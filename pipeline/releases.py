"""Versioned DuckDB release construction and staging-only promotion.

Warehouse candidates are copied from a verified backup, modified only at a new
``.partial`` path, validated, and atomically renamed. Promotion commands can only
manage the staging pointer below the selected data root; production is deliberately
not a supported environment in this milestone.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .acquisition import inspect_hospital_enrollments, pipeline_commit
from .discovery import safe_error, utc_now
from .manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)

WAREHOUSE_RELEASE_SCHEMA_VERSION = 1
PROMOTION_JOURNAL_SCHEMA_VERSION = 1
COPY_CHUNK_BYTES = 8 * 1024 * 1024
STAGING_ENVIRONMENT = "staging"
HOSPITAL_SOURCE_ID = "cms_hospital_enrollments"

HOSPITAL_COLUMN_MAP: tuple[tuple[str, str], ...] = (
    ("ENROLLMENT ID", "enrollment_id"),
    ("ENROLLMENT STATE", "enrollment_state"),
    ("PROVIDER TYPE CODE", "provider_type_code"),
    ("PROVIDER TYPE TEXT", "provider_type_text"),
    ("NPI", "npi"),
    ("MULTIPLE NPI FLAG", "multiple_npi_flag"),
    ("CCN", "ccn"),
    ("ASSOCIATE ID", "associate_id"),
    ("ORGANIZATION NAME", "organization_name"),
    ("DOING BUSINESS AS NAME", "doing_business_as_name"),
    ("INCORPORATION DATE", "incorporation_date"),
    ("INCORPORATION STATE", "incorporation_state"),
    ("ORGANIZATION TYPE STRUCTURE", "organization_type_structure"),
    ("ORGANIZATION OTHER TYPE TEXT", "organization_other_type_text"),
    ("PROPRIETARY NONPROFIT", "proprietary_nonprofit"),
    ("ADDRESS LINE 1", "address_line_1"),
    ("ADDRESS LINE 2", "address_line_2"),
    ("CITY", "city"),
    ("STATE", "state"),
    ("ZIP CODE", "zip_code"),
    ("PRACTICE LOCATION TYPE", "practice_location_type"),
    ("LOCATION OTHER TYPE TEXT", "location_other_type_text"),
    ("SUBGROUP - GENERAL", "subgroup_general"),
    ("SUBGROUP - ACUTE CARE", "subgroup_acute_care"),
    ("SUBGROUP - ALCOHOL DRUG", "subgroup_alcohol_drug"),
    ("SUBGROUP - CHILDRENS", "subgroup_childrens"),
    ("SUBGROUP - LONG-TERM", "subgroup_long_term"),
    ("SUBGROUP - PSYCHIATRIC", "subgroup_psychiatric"),
    ("SUBGROUP - REHABILITATION", "subgroup_rehabilitation"),
    ("SUBGROUP - SHORT-TERM", "subgroup_short_term"),
    ("SUBGROUP - SWING-BED APPROVED", "subgroup_swing_bed_approved"),
    ("SUBGROUP - PSYCHIATRIC UNIT", "subgroup_psychiatric_unit"),
    ("SUBGROUP - REHABILITATION UNIT", "subgroup_rehabilitation_unit"),
    ("SUBGROUP - SPECIALTY HOSPITAL", "subgroup_specialty_hospital"),
    ("SUBGROUP - OTHER", "subgroup_other"),
    ("SUBGROUP - OTHER TEXT", "subgroup_other_text"),
    ("REH CONVERSION FLAG", "reh_conversion_flag"),
    ("REH CONVERSION DATE", "reh_conversion_date"),
    ("CAH OR HOSPITAL CCN", "cah_or_hospital_ccn"),
)


class ReleaseError(RuntimeError):
    """A warehouse release could not be safely built or transitioned."""


@dataclass(slots=True)
class WarehouseRelease:
    warehouse_release_id: str
    created_at: str
    source_run_ids: tuple[str, ...]
    pipeline_code_commit: str
    baseline_path: str
    baseline_sha256: str
    database_path: str
    byte_size: int | None = None
    sha256: str | None = None
    table_counts: dict[str, int] = field(default_factory=dict)
    validation_state: ValidationState = ValidationState.NOT_RUN
    validation_timestamp: str | None = None
    promotion_state: PromotionState = PromotionState.NOT_PROMOTED
    promotion_timestamp: str | None = None
    rollback_timestamp: str | None = None
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if not self.warehouse_release_id or not self.source_run_ids:
            raise ValueError("warehouse_release_id and source_run_ids are required")
        if len(self.pipeline_code_commit) != 40 or any(
            character not in "0123456789abcdef"
            for character in self.pipeline_code_commit
        ):
            raise ValueError("pipeline_code_commit must be a full lowercase Git commit")
        if not _is_sha256(self.baseline_sha256):
            raise ValueError("baseline_sha256 must be a SHA-256 digest")
        if self.sha256 is not None and not _is_sha256(self.sha256):
            raise ValueError("sha256 must be a SHA-256 digest")
        if self.byte_size is not None and self.byte_size < 0:
            raise ValueError("byte_size cannot be negative")
        if any(not isinstance(value, int) or value < 0 for value in self.table_counts.values()):
            raise ValueError("table_counts values must be non-negative integers")
        if self.error_summary is not None:
            self.error_summary = safe_error(self.error_summary)

    def to_dict(self) -> dict:
        return {
            "warehouse_release_id": self.warehouse_release_id,
            "created_at": self.created_at,
            "source_run_ids": list(self.source_run_ids),
            "pipeline_code_commit": self.pipeline_code_commit,
            "baseline_path": self.baseline_path,
            "baseline_sha256": self.baseline_sha256,
            "database_path": self.database_path,
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "table_counts": dict(sorted(self.table_counts.items())),
            "validation_state": self.validation_state.value,
            "validation_timestamp": self.validation_timestamp,
            "promotion_state": self.promotion_state.value,
            "promotion_timestamp": self.promotion_timestamp,
            "rollback_timestamp": self.rollback_timestamp,
            "error_summary": self.error_summary,
        }

    @classmethod
    def from_dict(cls, value: dict) -> WarehouseRelease:
        if not isinstance(value, dict):
            raise ValueError("warehouse release must be an object")
        try:
            return cls(
                warehouse_release_id=value["warehouse_release_id"],
                created_at=value["created_at"],
                source_run_ids=tuple(value["source_run_ids"]),
                pipeline_code_commit=value["pipeline_code_commit"],
                baseline_path=value["baseline_path"],
                baseline_sha256=value["baseline_sha256"],
                database_path=value["database_path"],
                byte_size=value.get("byte_size"),
                sha256=value.get("sha256"),
                table_counts=value.get("table_counts") or {},
                validation_state=ValidationState(
                    value.get("validation_state", ValidationState.NOT_RUN.value)
                ),
                validation_timestamp=value.get("validation_timestamp"),
                promotion_state=PromotionState(
                    value.get("promotion_state", PromotionState.NOT_PROMOTED.value)
                ),
                promotion_timestamp=value.get("promotion_timestamp"),
                rollback_timestamp=value.get("rollback_timestamp"),
                error_summary=value.get("error_summary"),
            )
        except KeyError as error:
            raise ValueError(
                f"warehouse release is missing required field: {error.args[0]}"
            ) from error


@dataclass(slots=True)
class WarehouseReleaseDocument:
    releases: list[WarehouseRelease] = field(default_factory=list)
    schema_version: int = WAREHOUSE_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != WAREHOUSE_RELEASE_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported warehouse release schema_version {self.schema_version}"
            )

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "releases": [release.to_dict() for release in self.releases],
        }

    @classmethod
    def from_dict(cls, value: dict) -> WarehouseReleaseDocument:
        if not isinstance(value, dict):
            raise ValueError("warehouse release document must be an object")
        if value.get("schema_version") != WAREHOUSE_RELEASE_SCHEMA_VERSION:
            raise ValueError(
                "Unsupported warehouse release schema_version "
                f"{value.get('schema_version')!r}"
            )
        rows = value.get("releases")
        if not isinstance(rows, list):
            raise ValueError("warehouse release document is missing releases")
        return cls(
            releases=[WarehouseRelease.from_dict(row) for row in rows],
            schema_version=value["schema_version"],
        )


class WarehouseReleaseStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> WarehouseReleaseDocument:
        if not self.path.exists():
            return WarehouseReleaseDocument()
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid warehouse release JSON at {self.path}") from error
        return WarehouseReleaseDocument.from_dict(value)

    def save(self, document: WarehouseReleaseDocument) -> None:
        _atomic_write_json(self.path, document.to_dict())


@dataclass(frozen=True, slots=True)
class BuildResult:
    release: WarehouseRelease
    database_path: Path
    release_manifest_path: Path
    release_store_path: Path

    def to_dict(self) -> dict:
        return {
            "release": self.release.to_dict(),
            "database_path": str(self.database_path),
            "release_manifest_path": str(self.release_manifest_path),
            "release_store_path": str(self.release_store_path),
        }


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(COPY_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ReleaseError(f"Another staging operation holds {path}") from error
        yield


def _release_store_path(data_root: Path) -> Path:
    return data_root / "warehouse-releases.json"


def _release_manifest_path(data_root: Path, warehouse_release_id: str) -> Path:
    return data_root / "releases" / warehouse_release_id / "release.json"


def _save_release_document(data_root: Path, document: WarehouseReleaseDocument) -> None:
    WarehouseReleaseStore(_release_store_path(data_root)).save(document)
    for release in document.releases:
        _atomic_write_json(
            _release_manifest_path(data_root, release.warehouse_release_id),
            {
                "schema_version": WAREHOUSE_RELEASE_SCHEMA_VERSION,
                "release": release.to_dict(),
            },
        )


def make_warehouse_release_id(
    source_run_id: str,
    code_commit: str,
    now: datetime | None = None,
) -> str:
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    suffix = hashlib.sha256(
        f"{source_run_id}\0{code_commit}\0{uuid.uuid4().hex}".encode("utf-8")
    ).hexdigest()[:10]
    return f"warehouse-{timestamp:%Y%m%dT%H%M%SZ}-{suffix}"


def _source_manifest(data_root: Path, run_id: str) -> RunManifest:
    document = ManifestStore(data_root / "manifests.json").load()
    matches = [manifest for manifest in document.manifests if manifest.run_id == run_id]
    if len(matches) != 1:
        raise ReleaseError(f"Expected one source manifest for run {run_id}; found {len(matches)}")
    manifest = matches[0]
    if manifest.source_id != HOSPITAL_SOURCE_ID:
        raise ReleaseError(f"Warehouse build does not support source {manifest.source_id}")
    if manifest.validation_state != ValidationState.PASSED:
        raise ReleaseError(f"Source run {run_id} has not passed validation")
    if not manifest.sha256 or not manifest.source_encoding:
        raise ReleaseError(f"Source run {run_id} lacks checksum or encoding provenance")
    return manifest


def _verified_source_artifact(data_root: Path, manifest: RunManifest) -> Path:
    artifact = data_root / "runs" / manifest.source_id / manifest.run_id / "source.csv"
    if not artifact.is_file() or artifact.is_symlink():
        raise ReleaseError(f"Source artifact is missing or not a regular file: {artifact}")
    inspection = inspect_hospital_enrollments(artifact)
    if inspection.sha256 != manifest.sha256 or inspection.byte_size != manifest.byte_size:
        raise ReleaseError("Source artifact no longer matches its acquisition manifest")
    if inspection.schema_fingerprint != manifest.schema_fingerprint:
        raise ReleaseError("Source artifact schema fingerprint no longer matches its manifest")
    if inspection.source_encoding != manifest.source_encoding:
        raise ReleaseError("Source artifact encoding no longer matches its manifest")
    if inspection.row_count != manifest.row_counts.get("source_rows"):
        raise ReleaseError("Source artifact row count no longer matches its manifest")
    return artifact


def _load_backup_manifest(path: Path) -> tuple[Path, str, int | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseError(f"Backup manifest is unreadable: {safe_error(error)}") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ReleaseError("Backup manifest has an unsupported schema")
    backup_path = payload.get("backup_path")
    backup_sha = payload.get("sha256")
    if not isinstance(backup_path, str) or not _is_sha256(backup_sha):
        raise ReleaseError("Backup manifest lacks a valid path or SHA-256")
    backup = Path(backup_path)
    if not backup.is_file() or backup.is_symlink():
        raise ReleaseError("Backup database is missing, symlinked, or not a regular file")
    identity = payload.get("backup_identity") or {}
    byte_size = identity.get("byte_size")
    if byte_size is not None and (not isinstance(byte_size, int) or byte_size < 0):
        raise ReleaseError("Backup manifest has an invalid byte size")
    validation = payload.get("validation") or {}
    if validation.get("read_only_open") != "passed":
        raise ReleaseError("Backup manifest does not prove a successful read-only open")
    return backup, backup_sha, byte_size


def _copy_verified_baseline(
    baseline: Path,
    destination: Path,
    *,
    expected_sha256: str,
    expected_bytes: int | None,
) -> None:
    before = baseline.stat()
    digest = hashlib.sha256()
    byte_size = 0
    with baseline.open("rb") as source, destination.open("xb") as target:
        while chunk := source.read(COPY_CHUNK_BYTES):
            target.write(chunk)
            digest.update(chunk)
            byte_size += len(chunk)
        target.flush()
        os.fsync(target.fileno())
    after = baseline.stat()
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        raise ReleaseError("Backup database changed while the candidate was copied")
    if digest.hexdigest() != expected_sha256:
        raise ReleaseError("Backup database no longer matches its manifest SHA-256")
    if expected_bytes is not None and byte_size != expected_bytes:
        raise ReleaseError("Backup database no longer matches its manifest byte size")


def _create_raw_hospital_table(connection: duckdb.DuckDBPyConnection) -> None:
    columns = ",\n".join(f'"{target}" VARCHAR' for _, target in HOSPITAL_COLUMN_MAP)
    connection.execute("DROP TABLE IF EXISTS raw_hospital_enrollments")
    connection.execute(
        f"""
        CREATE TABLE raw_hospital_enrollments (
            {columns},
            source_run_id VARCHAR NOT NULL,
            source_release_id VARCHAR NOT NULL,
            source_data_period VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        )
        """
    )


def _load_hospital_rows(
    connection: duckdb.DuckDBPyConnection,
    artifact: Path,
    manifest: RunManifest,
) -> int:
    expected_header = tuple(source for source, _ in HOSPITAL_COLUMN_MAP)
    target_columns = tuple(target for _, target in HOSPITAL_COLUMN_MAP)
    inserted = 0
    ingested_at = utc_now()
    with artifact.open("r", encoding=manifest.source_encoding, newline="") as handle:
        reader = csv.reader(handle)
        header = tuple(next(reader, ()))
        if header != expected_header:
            missing = sorted(set(expected_header) - set(header))
            unexpected = sorted(set(header) - set(expected_header))
            detail = []
            if missing:
                detail.append("missing=" + ",".join(missing))
            if unexpected:
                detail.append("unexpected=" + ",".join(unexpected))
            if not detail:
                detail.append("column order changed")
            raise ReleaseError(
                "Hospital Enrollments header no longer matches the canonical raw schema: "
                + "; ".join(detail)
            )

        quoted = ", ".join(f'"{column}"' for column in target_columns)
        metadata_columns = "source_run_id, source_release_id, source_data_period, ingested_at"
        placeholders = ", ".join("?" for _ in range(len(target_columns) + 4))
        statement = (
            f"INSERT INTO raw_hospital_enrollments ({quoted}, {metadata_columns}) "
            f"VALUES ({placeholders})"
        )
        batch: list[tuple[str, ...]] = []
        for row in reader:
            batch.append(
                tuple(row)
                + (
                    manifest.run_id,
                    manifest.release_id,
                    manifest.source_data_period,
                    ingested_at,
                )
            )
            if len(batch) >= 1000:
                connection.executemany(statement, batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            connection.executemany(statement, batch)
            inserted += len(batch)
    connection.execute(
        "CREATE INDEX idx_raw_hospital_enrollments_npi "
        "ON raw_hospital_enrollments(npi)"
    )
    return inserted


def _validate_candidate(
    connection: duckdb.DuckDBPyConnection,
    expected_source_rows: int,
) -> dict[str, int]:
    core_providers = connection.execute("SELECT count(*) FROM core_providers").fetchone()[0]
    source_rows = connection.execute(
        "SELECT count(*) FROM raw_hospital_enrollments"
    ).fetchone()[0]
    invalid_npis = connection.execute(
        """
        SELECT count(*)
        FROM raw_hospital_enrollments
        WHERE NOT regexp_full_match(npi, '[0-9]{10}')
        """
    ).fetchone()[0]
    missing_organizations = connection.execute(
        """
        SELECT count(*)
        FROM raw_hospital_enrollments
        WHERE organization_name IS NULL OR trim(organization_name) = ''
        """
    ).fetchone()[0]
    distinct_hospital_npis = connection.execute(
        "SELECT count(DISTINCT npi) FROM raw_hospital_enrollments"
    ).fetchone()[0]
    table_count = connection.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ?",
        ["main"],
    ).fetchone()[0]
    if core_providers <= 0:
        raise ReleaseError("Candidate warehouse has no core providers")
    if source_rows != expected_source_rows:
        raise ReleaseError(
            f"Candidate raw_hospital_enrollments has {source_rows} rows; "
            f"expected {expected_source_rows}"
        )
    if invalid_npis:
        raise ReleaseError(f"Candidate warehouse contains {invalid_npis} invalid hospital NPIs")
    if missing_organizations:
        raise ReleaseError(
            f"Candidate warehouse contains {missing_organizations} blank hospital names"
        )
    if distinct_hospital_npis <= 0:
        raise ReleaseError("Candidate warehouse has no distinct hospital NPIs")
    return {
        "core_providers": int(core_providers),
        "raw_hospital_enrollments": int(source_rows),
        "distinct_hospital_npis": int(distinct_hospital_npis),
        "database_tables": int(table_count),
    }


def build_warehouse_release(
    *,
    data_root: Path,
    source_run_id: str,
    backup_manifest_path: Path,
    code_commit: str | None = None,
) -> BuildResult:
    """Build one immutable candidate without opening the active production database."""
    manifest = _source_manifest(data_root, source_run_id)
    artifact = _verified_source_artifact(data_root, manifest)
    baseline, baseline_sha, baseline_bytes = _load_backup_manifest(backup_manifest_path)
    commit = code_commit or pipeline_commit()
    if commit is None:
        raise ReleaseError("A full pipeline Git commit is required to build a release")
    warehouse_release_id = make_warehouse_release_id(source_run_id, commit)
    release_dir = data_root / "releases" / warehouse_release_id
    database_path = release_dir / "warehouse.duckdb"
    partial_path = release_dir / "warehouse.duckdb.partial"
    release_store_path = _release_store_path(data_root)
    relative_database_path = str(database_path.relative_to(data_root))

    with _exclusive_lock(data_root / "locks" / "build.lock"):
        release_dir.mkdir(parents=True, exist_ok=False)
        document = WarehouseReleaseStore(release_store_path).load()
        release = WarehouseRelease(
            warehouse_release_id=warehouse_release_id,
            created_at=utc_now(),
            source_run_ids=(source_run_id,),
            pipeline_code_commit=commit,
            baseline_path=str(baseline),
            baseline_sha256=baseline_sha,
            database_path=relative_database_path,
        )
        document.releases.append(release)
        _save_release_document(data_root, document)
        try:
            _copy_verified_baseline(
                baseline,
                partial_path,
                expected_sha256=baseline_sha,
                expected_bytes=baseline_bytes,
            )
            connection = duckdb.connect(str(partial_path), read_only=False)
            try:
                connection.execute("BEGIN TRANSACTION")
                _create_raw_hospital_table(connection)
                inserted = _load_hospital_rows(connection, artifact, manifest)
                if inserted != manifest.row_counts["source_rows"]:
                    raise ReleaseError(
                        f"Loaded {inserted} hospital rows; expected "
                        f"{manifest.row_counts['source_rows']}"
                    )
                table_counts = _validate_candidate(connection, inserted)
                connection.execute("COMMIT")
                connection.execute("CHECKPOINT")
            except Exception:
                try:
                    connection.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                raise
            finally:
                connection.close()

            release.byte_size = partial_path.stat().st_size
            release.sha256 = sha256_file(partial_path)
            release.table_counts = table_counts
            release.validation_state = ValidationState.PASSED
            release.validation_timestamp = utc_now()
            os.replace(partial_path, database_path)
            os.chmod(database_path, 0o440)
            _save_release_document(data_root, document)
        except Exception as error:
            release.validation_state = ValidationState.FAILED
            release.error_summary = safe_error(error)
            _save_release_document(data_root, document)
            raise ReleaseError(release.error_summary) from error

    return BuildResult(
        release=release,
        database_path=database_path,
        release_manifest_path=_release_manifest_path(data_root, warehouse_release_id),
        release_store_path=release_store_path,
    )


def _release_database(data_root: Path, release: WarehouseRelease) -> Path:
    root = data_root.resolve()
    database = (data_root / release.database_path).resolve()
    if not database.is_relative_to(root):
        raise ReleaseError("Warehouse release path escapes the staging data root")
    if not database.is_file() or database.is_symlink():
        raise ReleaseError(f"Warehouse release database is missing: {database}")
    return database


def _find_release(
    document: WarehouseReleaseDocument, warehouse_release_id: str
) -> WarehouseRelease:
    matches = [
        release
        for release in document.releases
        if release.warehouse_release_id == warehouse_release_id
    ]
    if len(matches) != 1:
        raise ReleaseError(
            f"Expected one warehouse release {warehouse_release_id}; found {len(matches)}"
        )
    return matches[0]


def _verify_promotable(data_root: Path, release: WarehouseRelease) -> Path:
    if release.validation_state != ValidationState.PASSED or not release.sha256:
        raise ReleaseError(f"Warehouse release {release.warehouse_release_id} is not validated")
    database = _release_database(data_root, release)
    if sha256_file(database) != release.sha256:
        raise ReleaseError("Warehouse release database no longer matches its SHA-256")
    return database


def _pointer_path(data_root: Path) -> Path:
    return data_root / STAGING_ENVIRONMENT / "warehouse-current"


def _read_pointer(pointer: Path) -> str | None:
    if not os.path.lexists(pointer):
        return None
    if not pointer.is_symlink():
        raise ReleaseError(f"Staging warehouse pointer is not a symlink: {pointer}")
    return os.readlink(pointer)


def _switch_pointer(pointer: Path, database: Path) -> None:
    pointer.parent.mkdir(parents=True, exist_ok=True)
    temporary = pointer.with_name(f".{pointer.name}.{uuid.uuid4().hex}.partial")
    target = os.path.relpath(database, pointer.parent)
    os.symlink(target, temporary)
    os.replace(temporary, pointer)


def _restore_pointer(pointer: Path, target: str | None) -> None:
    if target is not None:
        temporary = pointer.with_name(f".{pointer.name}.{uuid.uuid4().hex}.rollback")
        os.symlink(target, temporary)
        os.replace(temporary, pointer)
        return
    if os.path.lexists(pointer):
        retired = pointer.with_name(f".{pointer.name}.{uuid.uuid4().hex}.retired")
        os.replace(pointer, retired)
        retired.unlink()


def _pointer_release_id(
    data_root: Path,
    document: WarehouseReleaseDocument,
    target: str | None,
) -> str | None:
    if target is None:
        return None
    resolved = (_pointer_path(data_root).parent / target).resolve()
    root = data_root.resolve()
    matches: list[str] = []
    for release in document.releases:
        candidate = (data_root / release.database_path).resolve()
        if not candidate.is_relative_to(root):
            raise ReleaseError("Warehouse release path escapes the staging data root")
        if candidate == resolved:
            matches.append(release.warehouse_release_id)
    if len(matches) != 1:
        raise ReleaseError("Staging pointer does not identify exactly one managed release")
    return matches[0]


def _journal_path(data_root: Path) -> Path:
    return data_root / "promotion-journal.json"


def _load_journal(data_root: Path) -> dict:
    path = _journal_path(data_root)
    if not path.exists():
        return {"schema_version": PROMOTION_JOURNAL_SCHEMA_VERSION, "events": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ReleaseError("Promotion journal is invalid JSON") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != PROMOTION_JOURNAL_SCHEMA_VERSION
        or not isinstance(value.get("events"), list)
    ):
        raise ReleaseError("Promotion journal has an unsupported schema")
    if any(event.get("state") == "pending" for event in value["events"]):
        raise ReleaseError("Promotion journal contains an unresolved pending transaction")
    return value


def _save_journal(data_root: Path, journal: dict) -> None:
    _atomic_write_json(_journal_path(data_root), journal)


def _source_document(data_root: Path) -> ManifestDocument:
    return ManifestStore(data_root / "manifests.json").load()


def _activate_source_runs(
    document: ManifestDocument,
    release: WarehouseRelease,
    timestamp: str,
) -> None:
    target_run_ids = set(release.source_run_ids)
    targets = [manifest for manifest in document.manifests if manifest.run_id in target_run_ids]
    if len(targets) != len(target_run_ids):
        raise ReleaseError("Warehouse release source manifests are incomplete")
    target_source_ids = {manifest.source_id for manifest in targets}
    for manifest in document.manifests:
        if (
            manifest.source_id in target_source_ids
            and manifest.run_id not in target_run_ids
            and manifest.promotion_state == PromotionState.ACTIVE
        ):
            manifest.promotion_state = PromotionState.SUPERSEDED
            manifest.active_release_id = None
    for manifest in targets:
        if manifest.validation_state != ValidationState.PASSED:
            raise ReleaseError(f"Source run {manifest.run_id} is not validated")
        manifest.promotion_state = PromotionState.ACTIVE
        manifest.promotion_timestamp = timestamp
        manifest.active_release_id = manifest.release_id


def _rollback_source_runs(
    document: ManifestDocument,
    current: WarehouseRelease,
    previous: WarehouseRelease | None,
    timestamp: str,
) -> None:
    current_ids = set(current.source_run_ids)
    for manifest in document.manifests:
        if manifest.run_id in current_ids:
            manifest.promotion_state = PromotionState.ROLLED_BACK
            manifest.rollback_timestamp = timestamp
            manifest.active_release_id = None
    if previous is not None:
        _activate_source_runs(document, previous, timestamp)


def _transition_event(
    *,
    action: str,
    from_release_id: str | None,
    to_release_id: str | None,
    from_target: str | None,
    to_target: str | None,
) -> dict:
    return {
        "transaction_id": uuid.uuid4().hex,
        "environment": STAGING_ENVIRONMENT,
        "action": action,
        "state": "pending",
        "started_at": utc_now(),
        "completed_at": None,
        "from_release_id": from_release_id,
        "to_release_id": to_release_id,
        "from_target": from_target,
        "to_target": to_target,
        "error_summary": None,
    }


def _finish_event(data_root: Path, journal: dict, event: dict, state: str) -> None:
    event["state"] = state
    event["completed_at"] = utc_now()
    _save_journal(data_root, journal)


def promote_staging_release(data_root: Path, warehouse_release_id: str) -> dict:
    """Atomically activate a validated release in the isolated staging pointer."""
    with _exclusive_lock(data_root / "locks" / "promotion.lock"):
        release_store = WarehouseReleaseStore(_release_store_path(data_root))
        release_document = release_store.load()
        release = _find_release(release_document, warehouse_release_id)
        database = _verify_promotable(data_root, release)
        pointer = _pointer_path(data_root)
        previous_target = _read_pointer(pointer)
        previous_release_id = _pointer_release_id(
            data_root, release_document, previous_target
        )
        if previous_release_id == warehouse_release_id:
            raise ReleaseError(f"Warehouse release {warehouse_release_id} is already active")

        manifest_store = ManifestStore(data_root / "manifests.json")
        source_document = manifest_store.load()
        release_before = deepcopy(release_document)
        source_before = deepcopy(source_document)
        journal = _load_journal(data_root)
        event = _transition_event(
            action="promote",
            from_release_id=previous_release_id,
            to_release_id=warehouse_release_id,
            from_target=previous_target,
            to_target=os.path.relpath(database, pointer.parent),
        )
        journal["events"].append(event)
        _save_journal(data_root, journal)
        try:
            _switch_pointer(pointer, database)
            timestamp = utc_now()
            for candidate in release_document.releases:
                if candidate.promotion_state == PromotionState.ACTIVE:
                    candidate.promotion_state = PromotionState.SUPERSEDED
            release.promotion_state = PromotionState.ACTIVE
            release.promotion_timestamp = timestamp
            _activate_source_runs(source_document, release, timestamp)
            _save_release_document(data_root, release_document)
            manifest_store.save(source_document)
            _finish_event(data_root, journal, event, "completed")
        except Exception as error:
            _restore_pointer(pointer, previous_target)
            _save_release_document(data_root, release_before)
            manifest_store.save(source_before)
            event["error_summary"] = safe_error(error)
            _finish_event(data_root, journal, event, "rolled_back")
            raise ReleaseError(f"Staging promotion rolled back: {safe_error(error)}") from error

    return {
        "environment": STAGING_ENVIRONMENT,
        "action": "promote",
        "warehouse_release_id": warehouse_release_id,
        "previous_release_id": previous_release_id,
        "pointer": str(pointer),
        "database_path": str(database),
        "transaction_id": event["transaction_id"],
        "state": event["state"],
    }


def rollback_staging_release(data_root: Path) -> dict:
    """Roll back the isolated staging pointer to its previous managed target."""
    with _exclusive_lock(data_root / "locks" / "promotion.lock"):
        release_store = WarehouseReleaseStore(_release_store_path(data_root))
        release_document = release_store.load()
        pointer = _pointer_path(data_root)
        current_target = _read_pointer(pointer)
        current_release_id = _pointer_release_id(
            data_root, release_document, current_target
        )
        if current_release_id is None:
            raise ReleaseError("No staging warehouse release is active")
        current = _find_release(release_document, current_release_id)
        journal = _load_journal(data_root)
        promotions = [
            event
            for event in journal["events"]
            if event.get("action") == "promote"
            and event.get("state") == "completed"
            and event.get("to_release_id") == current_release_id
        ]
        if not promotions:
            raise ReleaseError("Promotion journal has no rollback target for the active release")
        promotion = promotions[-1]
        previous_release_id = promotion.get("from_release_id")
        previous_target = promotion.get("from_target")
        previous = (
            _find_release(release_document, previous_release_id)
            if previous_release_id is not None
            else None
        )
        if previous is not None:
            _verify_promotable(data_root, previous)

        manifest_store = ManifestStore(data_root / "manifests.json")
        source_document = manifest_store.load()
        release_before = deepcopy(release_document)
        source_before = deepcopy(source_document)
        event = _transition_event(
            action="rollback",
            from_release_id=current_release_id,
            to_release_id=previous_release_id,
            from_target=current_target,
            to_target=previous_target,
        )
        journal["events"].append(event)
        _save_journal(data_root, journal)
        try:
            _restore_pointer(pointer, previous_target)
            timestamp = utc_now()
            current.promotion_state = PromotionState.ROLLED_BACK
            current.rollback_timestamp = timestamp
            if previous is not None:
                previous.promotion_state = PromotionState.ACTIVE
                previous.promotion_timestamp = timestamp
            _rollback_source_runs(source_document, current, previous, timestamp)
            _save_release_document(data_root, release_document)
            manifest_store.save(source_document)
            _finish_event(data_root, journal, event, "completed")
        except Exception as error:
            _restore_pointer(pointer, current_target)
            _save_release_document(data_root, release_before)
            manifest_store.save(source_before)
            event["error_summary"] = safe_error(error)
            _finish_event(data_root, journal, event, "rolled_back")
            raise ReleaseError(f"Staging rollback was reverted: {safe_error(error)}") from error

    return {
        "environment": STAGING_ENVIRONMENT,
        "action": "rollback",
        "warehouse_release_id": current_release_id,
        "restored_release_id": previous_release_id,
        "pointer": str(pointer),
        "restored_target": previous_target,
        "transaction_id": event["transaction_id"],
        "state": event["state"],
    }
