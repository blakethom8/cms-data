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

WAREHOUSE_RELEASE_SCHEMA_VERSION = 2
SUPPORTED_WAREHOUSE_RELEASE_SCHEMA_VERSIONS = frozenset({1, 2})
PROMOTION_JOURNAL_SCHEMA_VERSION = 1
COMPARISON_SCHEMA_VERSION = 2
COPY_CHUNK_BYTES = 8 * 1024 * 1024
STAGING_ENVIRONMENT = "staging"
HOSPITAL_SOURCE_ID = "cms_hospital_enrollments"
FULL_CMS_SOURCE_IDS = frozenset(
    {
        "cms_physician_by_provider",
        "cms_physician_by_provider_and_service",
        "cms_part_d_by_provider",
        "cms_part_d_by_provider_and_drug",
        "cms_dme_by_referring_provider",
        "cms_qpp_experience",
        "cms_pecos_public_provider_enrollment",
        "cms_order_and_referring",
        HOSPITAL_SOURCE_ID,
        "cms_revalidation_group_reassignment",
    }
)
FULL_PLATFORM_WAREHOUSE_SOURCE_IDS = FULL_CMS_SOURCE_IDS | frozenset(
    {
        "nppes_monthly_v2",
        "nppes_weekly_incremental_v2",
        "open_payments_general",
        "open_payments_research",
        "open_payments_ownership",
    }
)
FULL_PLATFORM_SMOKE_TABLES = (
    "core_providers",
    "practice_locations",
    "utilization_metrics",
    "industry_relationships",
    "hospital_affiliations",
    "provider_service_detail",
    "provider_drug_detail",
    "provider_quality_scores",
    "order_referring_eligibility",
    "raw_physician_by_provider",
    "raw_physician_by_provider_and_service",
    "raw_part_d_by_provider",
    "raw_part_d_by_provider_and_drug",
    "raw_dme_by_referring_provider",
    "raw_qpp_experience",
    "raw_pecos_enrollment",
    "raw_order_and_referring",
    "raw_hospital_enrollments",
    "raw_reassignment",
    "raw_nppes",
    "nppes_radar_provider_state",
    "nppes_radar_events",
    "nppes_radar_releases",
    "raw_open_payments_general",
    "raw_open_payments_research",
    "raw_open_payments_ownership",
    "kol_summary",
)
AFFILIATION_MATCH_POLICY = "normalized_name_and_state_unique_hospital_npi_v1"
AFFILIATION_CHANGED_TABLES = frozenset(
    {"raw_hospital_enrollments", "hospital_affiliations"}
)
FULL_CMS_CHANGED_TABLES = frozenset(
    {
        "core_providers",
        "practice_locations",
        "utilization_metrics",
        "industry_relationships",
        "hospital_affiliations",
        "provider_service_detail",
        "provider_drug_detail",
        "provider_quality_scores",
        "order_referring_eligibility",
        "raw_physician_by_provider",
        "raw_physician_by_provider_and_service",
        "raw_part_d_by_provider",
        "raw_part_d_by_provider_and_drug",
        "raw_dme_by_referring_provider",
        "raw_qpp_experience",
        "raw_pecos_enrollment",
        "raw_order_and_referring",
        "raw_hospital_enrollments",
        "raw_reassignment",
    }
)
FULL_PLATFORM_CHANGED_TABLES = frozenset(FULL_PLATFORM_SMOKE_TABLES)

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
    duckdb_version: str | None = None
    byte_size: int | None = None
    sha256: str | None = None
    table_counts: dict[str, int] = field(default_factory=dict)
    validation_details: dict[str, object] = field(default_factory=dict)
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
        if not isinstance(self.validation_details, dict):
            raise ValueError("validation_details must be an object")
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
            "duckdb_version": self.duckdb_version,
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "table_counts": dict(sorted(self.table_counts.items())),
            "validation_details": self.validation_details,
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
                duckdb_version=value.get("duckdb_version"),
                byte_size=value.get("byte_size"),
                sha256=value.get("sha256"),
                table_counts=value.get("table_counts") or {},
                validation_details=value.get("validation_details") or {},
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
        if self.schema_version not in SUPPORTED_WAREHOUSE_RELEASE_SCHEMA_VERSIONS:
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
        if value.get("schema_version") not in SUPPORTED_WAREHOUSE_RELEASE_SCHEMA_VERSIONS:
            raise ValueError(
                "Unsupported warehouse release schema_version "
                f"{value.get('schema_version')!r}"
            )
        rows = value.get("releases")
        if not isinstance(rows, list):
            raise ValueError("warehouse release document is missing releases")
        return cls(
            releases=[WarehouseRelease.from_dict(row) for row in rows],
            schema_version=WAREHOUSE_RELEASE_SCHEMA_VERSION,
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


def _require_table_columns(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    required: set[str],
) -> None:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table],
    ).fetchall()
    present = {row[0] for row in rows}
    missing = sorted(required - present)
    if missing:
        raise ReleaseError(
            f"Candidate table {table} is missing required columns: {', '.join(missing)}"
        )


def _hospital_data_year(source_data_period: str) -> int:
    year = source_data_period[:4]
    if len(source_data_period) < 4 or not year.isdigit():
        raise ReleaseError(
            "Hospital source_data_period does not begin with a four-digit year"
        )
    return int(year)


def _rebuild_hospital_affiliations(
    connection: duckdb.DuckDBPyConnection,
    *,
    data_year: int,
) -> dict[str, int]:
    """Replace affiliations using only unambiguous normalized name/state keys.

    Practice locations currently lack usable city and ZIP provenance. A normalized
    name/state key is therefore accepted only when it identifies exactly one
    hospital NPI in the publisher snapshot. Ambiguous health-system names are
    excluded rather than fanned out across every hospital in the system.
    """
    _require_table_columns(
        connection,
        "practice_locations",
        {"npi", "group_pac_id", "group_legal_name", "group_state", "state"},
    )
    _require_table_columns(
        connection,
        "hospital_affiliations",
        {
            "npi",
            "hospital_npi",
            "hospital_ccn",
            "hospital_name",
            "hospital_city",
            "hospital_state",
            "hospital_zip",
            "hospital_subgroup",
            "affiliation_source",
            "confidence_level",
            "group_pac_id",
            "data_year",
        },
    )
    baseline_count = connection.execute(
        "SELECT count(*) FROM hospital_affiliations"
    ).fetchone()[0]

    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE hospital_affiliation_hospitals AS
        SELECT * EXCLUDE (preferred)
        FROM (
            SELECT *,
                row_number() OVER (
                    PARTITION BY npi
                    ORDER BY
                        CASE
                            WHEN upper(trim(coalesce(practice_location_type, '')))
                                IN ('MAIN', 'PRIMARY', 'PRIMARY PRACTICE LOCATION')
                            THEN 0 ELSE 1
                        END,
                        CASE WHEN nullif(trim(ccn), '') IS NOT NULL THEN 0 ELSE 1 END,
                        ccn NULLS LAST,
                        enrollment_id NULLS LAST,
                        address_line_1 NULLS LAST
                ) AS preferred
            FROM raw_hospital_enrollments
        )
        WHERE preferred = 1
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE hospital_affiliation_match_keys AS
        WITH names AS (
            SELECT
                npi AS hospital_npi,
                regexp_replace(upper(trim(organization_name)), '[^A-Z0-9]', '', 'g')
                    AS match_name,
                upper(trim(state)) AS match_state,
                1 AS match_priority
            FROM hospital_affiliation_hospitals
            WHERE nullif(trim(organization_name), '') IS NOT NULL
              AND nullif(trim(state), '') IS NOT NULL

            UNION ALL

            SELECT
                npi,
                regexp_replace(
                    upper(trim(doing_business_as_name)), '[^A-Z0-9]', '', 'g'
                ),
                upper(trim(state)),
                2
            FROM hospital_affiliation_hospitals
            WHERE nullif(trim(doing_business_as_name), '') IS NOT NULL
              AND nullif(trim(state), '') IS NOT NULL
        ),
        collapsed AS (
            SELECT hospital_npi, match_name, match_state, min(match_priority) match_priority
            FROM names
            WHERE match_name <> ''
            GROUP BY hospital_npi, match_name, match_state
        )
        SELECT
            match_name,
            match_state,
            min(hospital_npi) AS hospital_npi,
            min(match_priority) AS match_priority,
            count(DISTINCT hospital_npi) AS hospital_count
        FROM collapsed
        GROUP BY match_name, match_state
        """
    )

    connection.execute("DELETE FROM hospital_affiliations")
    connection.execute(
        """
        INSERT INTO hospital_affiliations (
            npi, hospital_npi, hospital_ccn, hospital_name,
            hospital_city, hospital_state, hospital_zip, hospital_subgroup,
            affiliation_source, confidence_level, group_pac_id, data_year
        )
        WITH practices AS (
            SELECT DISTINCT
                p.npi AS provider_npi,
                p.group_pac_id,
                regexp_replace(
                    upper(trim(p.group_legal_name)), '[^A-Z0-9]', '', 'g'
                ) AS match_name,
                upper(trim(coalesce(
                    nullif(trim(p.group_state), ''),
                    nullif(trim(p.state), '')
                ))) AS match_state
            FROM practice_locations p
            INNER JOIN core_providers c ON c.npi = p.npi
            WHERE nullif(trim(p.group_legal_name), '') IS NOT NULL
              AND coalesce(
                    nullif(trim(p.group_state), ''),
                    nullif(trim(p.state), '')
                  ) IS NOT NULL
        ),
        candidates AS (
            SELECT
                p.provider_npi,
                p.group_pac_id,
                k.hospital_npi,
                k.match_priority,
                row_number() OVER (
                    PARTITION BY p.provider_npi, k.hospital_npi
                    ORDER BY k.match_priority, p.group_pac_id NULLS LAST
                ) AS preferred_match
            FROM practices p
            INNER JOIN hospital_affiliation_match_keys k
                USING (match_name, match_state)
            WHERE k.hospital_count = 1
        )
        SELECT
            c.provider_npi,
            c.hospital_npi,
            nullif(trim(h.ccn), ''),
            h.organization_name,
            h.city,
            h.state,
            h.zip_code,
            CASE
                WHEN upper(h.subgroup_acute_care) = 'Y' THEN 'acute_care'
                WHEN upper(h.subgroup_psychiatric) = 'Y' THEN 'psychiatric'
                WHEN upper(h.subgroup_rehabilitation) = 'Y' THEN 'rehabilitation'
                WHEN upper(h.subgroup_long_term) = 'Y' THEN 'long_term'
                WHEN upper(h.subgroup_childrens) = 'Y' THEN 'childrens'
                WHEN upper(h.subgroup_specialty_hospital) = 'Y' THEN 'specialty'
                ELSE 'general'
            END,
            CASE c.match_priority
                WHEN 1 THEN 'cms_reassignment_legal_name_state'
                ELSE 'cms_reassignment_dba_name_state'
            END,
            CASE c.match_priority WHEN 1 THEN 'medium' ELSE 'low' END,
            c.group_pac_id,
            ?
        FROM candidates c
        INNER JOIN hospital_affiliation_hospitals h ON h.npi = c.hospital_npi
        WHERE c.preferred_match = 1
        """,
        [data_year],
    )

    stats = connection.execute(
        """
        SELECT
            count(*) FILTER (WHERE hospital_count = 1),
            count(*) FILTER (WHERE hospital_count > 1)
        FROM hospital_affiliation_match_keys
        """
    ).fetchone()
    return {
        "baseline_hospital_affiliations": int(baseline_count),
        "unambiguous_hospital_name_state_keys": int(stats[0]),
        "ambiguous_hospital_name_state_keys": int(stats[1]),
    }


def _validate_candidate(
    connection: duckdb.DuckDBPyConnection,
    expected_source_rows: int,
    transform_counts: dict[str, int],
) -> tuple[dict[str, int], dict[str, object]]:
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
    affiliation_rows = connection.execute(
        "SELECT count(*) FROM hospital_affiliations"
    ).fetchone()[0]
    affiliated_providers = connection.execute(
        "SELECT count(DISTINCT npi) FROM hospital_affiliations"
    ).fetchone()[0]
    affiliated_hospitals = connection.execute(
        "SELECT count(DISTINCT hospital_npi) FROM hospital_affiliations"
    ).fetchone()[0]
    duplicate_affiliations = connection.execute(
        """
        SELECT count(*) FROM (
            SELECT npi, hospital_npi
            FROM hospital_affiliations
            GROUP BY npi, hospital_npi
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    missing_providers = connection.execute(
        """
        SELECT count(*)
        FROM hospital_affiliations a
        LEFT JOIN core_providers p ON p.npi = a.npi
        WHERE p.npi IS NULL
        """
    ).fetchone()[0]
    missing_hospitals = connection.execute(
        """
        SELECT count(*)
        FROM hospital_affiliations a
        LEFT JOIN raw_hospital_enrollments h ON h.npi = a.hospital_npi
        WHERE h.npi IS NULL
        """
    ).fetchone()[0]
    invalid_affiliation_values = connection.execute(
        """
        SELECT count(*)
        FROM hospital_affiliations
        WHERE NOT regexp_full_match(npi, '[0-9]{10}')
           OR NOT regexp_full_match(hospital_npi, '[0-9]{10}')
           OR (affiliation_source = 'cms_reassignment_legal_name_state'
               AND confidence_level <> 'medium')
           OR (affiliation_source = 'cms_reassignment_dba_name_state'
               AND confidence_level <> 'low')
           OR affiliation_source NOT IN (
               'cms_reassignment_legal_name_state',
               'cms_reassignment_dba_name_state'
           )
        """
    ).fetchone()[0]
    source_breakdown = connection.execute(
        """
        SELECT affiliation_source, count(*)
        FROM hospital_affiliations
        GROUP BY affiliation_source
        ORDER BY affiliation_source
        """
    ).fetchall()
    representatives = connection.execute(
        """
        SELECT npi, hospital_npi, hospital_name, hospital_state,
               affiliation_source, confidence_level
        FROM hospital_affiliations
        ORDER BY npi, hospital_npi
        LIMIT 5
        """
    ).fetchall()
    table_count = connection.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_type = 'BASE TABLE'
        """,
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
    if affiliation_rows <= 0:
        raise ReleaseError("Candidate warehouse has no validated hospital affiliations")
    if duplicate_affiliations:
        raise ReleaseError(
            f"Candidate warehouse contains {duplicate_affiliations} duplicate affiliations"
        )
    if missing_providers:
        raise ReleaseError(
            f"Candidate warehouse contains {missing_providers} affiliations without providers"
        )
    if missing_hospitals:
        raise ReleaseError(
            f"Candidate warehouse contains {missing_hospitals} affiliations without hospitals"
        )
    if invalid_affiliation_values:
        raise ReleaseError(
            "Candidate warehouse contains "
            f"{invalid_affiliation_values} invalid affiliation values"
        )
    counts = {
        "core_providers": int(core_providers),
        "raw_hospital_enrollments": int(source_rows),
        "distinct_hospital_npis": int(distinct_hospital_npis),
        "hospital_affiliations": int(affiliation_rows),
        "affiliated_providers": int(affiliated_providers),
        "affiliated_hospitals": int(affiliated_hospitals),
        "database_tables": int(table_count),
        **transform_counts,
    }
    details: dict[str, object] = {
        "affiliation_match_policy": AFFILIATION_MATCH_POLICY,
        "affiliation_source_counts": {
            str(source): int(count) for source, count in source_breakdown
        },
        "integrity": {
            "duplicate_provider_hospital_pairs": int(duplicate_affiliations),
            "missing_core_providers": int(missing_providers),
            "missing_raw_hospitals": int(missing_hospitals),
            "invalid_affiliation_values": int(invalid_affiliation_values),
        },
        "representative_affiliations": [
            {
                "npi": row[0],
                "hospital_npi": row[1],
                "hospital_name": row[2],
                "hospital_state": row[3],
                "affiliation_source": row[4],
                "confidence_level": row[5],
            }
            for row in representatives
        ],
    }
    return counts, details


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
            duckdb_version=duckdb.__version__,
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
                transform_counts = _rebuild_hospital_affiliations(
                    connection,
                    data_year=_hospital_data_year(manifest.source_data_period),
                )
                table_counts, validation_details = _validate_candidate(
                    connection, inserted, transform_counts
                )
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
            release.validation_details = validation_details
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


def _period_year(manifest: RunManifest) -> int:
    value = manifest.source_data_period[:4]
    if len(value) != 4 or not value.isdigit():
        raise ReleaseError(
            f"Source {manifest.source_id} period does not begin with a four-digit year"
        )
    return int(value)


def _resolve_exact_source_set(
    data_root: Path,
    source_run_ids: tuple[str, ...],
    expected_source_ids: frozenset[str],
    *,
    label: str,
) -> dict[str, RunManifest]:
    if len(source_run_ids) != len(set(source_run_ids)):
        raise ReleaseError(f"{label} source run IDs must be unique")
    source_document = ManifestStore(data_root / "manifests.json").load()
    by_run_id = {manifest.run_id: manifest for manifest in source_document.manifests}
    try:
        manifests = tuple(by_run_id[run_id] for run_id in source_run_ids)
    except KeyError as error:
        raise ReleaseError(f"Source manifest is missing for run {error.args[0]}") from error
    by_source_id = {manifest.source_id: manifest for manifest in manifests}
    if len(by_source_id) != len(manifests):
        raise ReleaseError(f"{label} contains multiple runs for one source")
    missing = sorted(expected_source_ids - set(by_source_id))
    unexpected = sorted(set(by_source_id) - expected_source_ids)
    if missing or unexpected:
        detail = []
        if missing:
            detail.append("missing=" + ",".join(missing))
        if unexpected:
            detail.append("unexpected=" + ",".join(unexpected))
        raise ReleaseError(f"{label} source set is incomplete: " + "; ".join(detail))
    return by_source_id


def _load_full_cms_content(
    connection: duckdb.DuckDBPyConnection,
    *,
    data_root: Path,
    by_source_id: dict[str, RunManifest],
) -> tuple[dict[str, int], dict[str, object]]:
    from .candidate_sources import load_cms_raw_tables
    from .transform import clear_refresh_targets, transform_all

    hospital_manifest = _source_manifest(
        data_root, by_source_id[HOSPITAL_SOURCE_ID].run_id
    )
    hospital_artifact = _verified_source_artifact(data_root, hospital_manifest)
    non_hospital_run_ids = tuple(
        manifest.run_id
        for manifest in by_source_id.values()
        if manifest.source_id in FULL_CMS_SOURCE_IDS
        and manifest.source_id != HOSPITAL_SOURCE_ID
    )
    raw_counts = load_cms_raw_tables(
        connection,
        data_root=data_root,
        run_ids=non_hospital_run_ids,
    )
    connection.execute("BEGIN TRANSACTION")
    try:
        _create_raw_hospital_table(connection)
        hospital_rows = _load_hospital_rows(
            connection, hospital_artifact, hospital_manifest
        )
        clear_refresh_targets(connection, include_core_providers=False)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    # DuckDB's foreign-key indexes are updated only at the transaction boundary.
    # Delete the now-unreferenced parents separately before rebuilding children.
    connection.execute("DELETE FROM core_providers")
    connection.execute("BEGIN TRANSACTION")
    try:
        transform_counts = transform_all(
            connection,
            _period_year(by_source_id["cms_physician_by_provider"]),
            practice_year=_period_year(
                by_source_id["cms_revalidation_group_reassignment"]
            ),
            quality_year=_period_year(by_source_id["cms_qpp_experience"]),
            include_hospital_affiliations=False,
        )
        affiliation_counts = _rebuild_hospital_affiliations(
            connection,
            data_year=_period_year(hospital_manifest),
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    table_counts = {
        **raw_counts,
        "raw_hospital_enrollments": hospital_rows,
        **transform_counts,
        "hospital_affiliations": affiliation_counts["hospital_affiliations"],
    }
    required_nonempty = (
        "core_providers",
        "utilization_metrics",
        "practice_locations",
        "provider_quality_scores",
        "provider_service_detail",
        "provider_drug_detail",
        "order_referring_eligibility",
        "hospital_affiliations",
    )
    empty = [name for name in required_nonempty if table_counts.get(name, 0) <= 0]
    if empty:
        raise ReleaseError(
            "Full CMS candidate has empty required tables: " + ", ".join(empty)
        )
    details = {
        "affiliation_match_policy": AFFILIATION_MATCH_POLICY,
        "affiliation_counts": affiliation_counts,
    }
    return table_counts, details


def build_full_cms_warehouse_release(
    *,
    data_root: Path,
    source_run_ids: tuple[str, ...],
    backup_manifest_path: Path,
    code_commit: str | None = None,
) -> BuildResult:
    """Build all ten CMS sources into a new immutable warehouse candidate."""
    by_source_id = _resolve_exact_source_set(
        data_root,
        source_run_ids,
        FULL_CMS_SOURCE_IDS,
        label="Full CMS build",
    )
    baseline, baseline_sha, baseline_bytes = _load_backup_manifest(backup_manifest_path)
    commit = code_commit or pipeline_commit()
    if commit is None:
        raise ReleaseError("A full pipeline Git commit is required to build a release")
    identity = hashlib.sha256("\0".join(sorted(source_run_ids)).encode()).hexdigest()
    warehouse_release_id = make_warehouse_release_id(identity, commit)
    release_dir = data_root / "releases" / warehouse_release_id
    database_path = release_dir / "warehouse.duckdb"
    partial_path = release_dir / "warehouse.duckdb.partial"
    release_store_path = _release_store_path(data_root)

    with _exclusive_lock(data_root / "locks" / "build.lock"):
        release_dir.mkdir(parents=True, exist_ok=False)
        document = WarehouseReleaseStore(release_store_path).load()
        release = WarehouseRelease(
            warehouse_release_id=warehouse_release_id,
            created_at=utc_now(),
            source_run_ids=tuple(sorted(source_run_ids)),
            pipeline_code_commit=commit,
            baseline_path=str(baseline),
            baseline_sha256=baseline_sha,
            database_path=str(database_path.relative_to(data_root)),
            duckdb_version=duckdb.__version__,
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
                table_counts, cms_details = _load_full_cms_content(
                    connection, data_root=data_root, by_source_id=by_source_id
                )
                release.validation_details = {
                    "source_periods": {
                        source_id: manifest.source_data_period
                        for source_id, manifest in sorted(by_source_id.items())
                    },
                    **cms_details,
                }
                connection.execute("CHECKPOINT")
            except Exception:
                raise
            finally:
                connection.close()

            release.byte_size = partial_path.stat().st_size
            release.sha256 = sha256_file(partial_path)
            release.table_counts = dict(sorted(table_counts.items()))
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


def build_full_platform_warehouse_release(
    *,
    data_root: Path,
    source_run_ids: tuple[str, ...],
    backup_manifest_path: Path,
    code_commit: str | None = None,
) -> BuildResult:
    """Build all DuckDB-backed registered sources into one immutable candidate.

    AACT is prepared and restored as a separate PostgreSQL release because it is
    not a DuckDB source. Its run ID must therefore not be passed to this builder.
    """
    from .archive_sources import load_nppes_sources, load_open_payments_sources
    by_source_id = _resolve_exact_source_set(
        data_root,
        source_run_ids,
        FULL_PLATFORM_WAREHOUSE_SOURCE_IDS,
        label="Full platform warehouse build",
    )
    baseline, baseline_sha, baseline_bytes = _load_backup_manifest(backup_manifest_path)
    commit = code_commit or pipeline_commit()
    if commit is None:
        raise ReleaseError("A full pipeline Git commit is required to build a release")
    identity = hashlib.sha256("\0".join(sorted(source_run_ids)).encode()).hexdigest()
    warehouse_release_id = make_warehouse_release_id(identity, commit)
    release_dir = data_root / "releases" / warehouse_release_id
    database_path = release_dir / "warehouse.duckdb"
    partial_path = release_dir / "warehouse.duckdb.partial"
    release_store_path = _release_store_path(data_root)

    with _exclusive_lock(data_root / "locks" / "build.lock"):
        release_dir.mkdir(parents=True, exist_ok=False)
        document = WarehouseReleaseStore(release_store_path).load()
        release = WarehouseRelease(
            warehouse_release_id=warehouse_release_id,
            created_at=utc_now(),
            source_run_ids=tuple(sorted(source_run_ids)),
            pipeline_code_commit=commit,
            baseline_path=str(baseline),
            baseline_sha256=baseline_sha,
            database_path=str(database_path.relative_to(data_root)),
            duckdb_version=duckdb.__version__,
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
                cms_counts, cms_details = _load_full_cms_content(
                    connection, data_root=data_root, by_source_id=by_source_id
                )
                nppes_counts, nppes_details = load_nppes_sources(
                    connection,
                    data_root=data_root,
                    monthly_run_id=by_source_id["nppes_monthly_v2"].run_id,
                    weekly_run_id=by_source_id[
                        "nppes_weekly_incremental_v2"
                    ].run_id,
                )
                payments_counts, payments_details = load_open_payments_sources(
                    connection,
                    data_root=data_root,
                    run_ids=tuple(
                        by_source_id[source_id].run_id
                        for source_id in sorted(
                            {
                                "open_payments_general",
                                "open_payments_research",
                                "open_payments_ownership",
                            }
                        )
                    ),
                )
                table_counts = {**cms_counts, **nppes_counts, **payments_counts}
                required_nonempty = (
                    "raw_nppes",
                    "nppes_radar_provider_state",
                    "nppes_radar_releases",
                    "raw_open_payments_general",
                    "raw_open_payments_research",
                    "raw_open_payments_ownership",
                    "industry_relationships",
                    "kol_summary",
                )
                empty = [
                    name for name in required_nonempty if table_counts.get(name, 0) <= 0
                ]
                if empty:
                    raise ReleaseError(
                        "Full platform candidate has empty required tables: "
                        + ", ".join(empty)
                    )
                smoke_table_counts = {
                    table: int(
                        connection.execute(
                            f'SELECT count(*) FROM "{table}"'
                        ).fetchone()[0]
                    )
                    for table in FULL_PLATFORM_SMOKE_TABLES
                }
                release.validation_details = {
                    "source_periods": {
                        source_id: manifest.source_data_period
                        for source_id, manifest in sorted(by_source_id.items())
                    },
                    **cms_details,
                    "nppes": nppes_details,
                    "open_payments": payments_details,
                    "smoke_table_counts": smoke_table_counts,
                    "aact": {
                        "state": "external_postgresql_release_required",
                        "reason": "AACT is not stored in the DuckDB warehouse.",
                    },
                }
                connection.execute("CHECKPOINT")
            finally:
                connection.close()

            release.byte_size = partial_path.stat().st_size
            release.sha256 = sha256_file(partial_path)
            release.table_counts = dict(sorted(table_counts.items()))
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


def _database_table_counts(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    tables = [
        row[0]
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
    ]
    counts: dict[str, int] = {}
    for table in tables:
        quoted = table.replace('"', '""')
        counts[table] = int(
            connection.execute(f'SELECT count(*) FROM "{quoted}"').fetchone()[0]
        )
    return counts


def _comparison_policy(
    release: WarehouseRelease,
) -> tuple[str, frozenset[str], dict[str, int]]:
    """Select the exact source-owned table set for a release comparison."""
    source_periods = release.validation_details.get("source_periods")
    source_ids = set(source_periods) if isinstance(source_periods, dict) else set()
    if source_ids == FULL_PLATFORM_WAREHOUSE_SOURCE_IDS:
        evidence = release.validation_details.get("smoke_table_counts")
        if not isinstance(evidence, dict):
            raise ReleaseError(
                "Full platform release is missing exact smoke table-count evidence"
            )
        missing = sorted(set(FULL_PLATFORM_SMOKE_TABLES) - set(evidence))
        unexpected = sorted(set(evidence) - set(FULL_PLATFORM_SMOKE_TABLES))
        if missing or unexpected:
            raise ReleaseError(
                "Full platform smoke table-count evidence has the wrong table set: "
                f"missing={','.join(missing) or 'none'}; "
                f"unexpected={','.join(unexpected) or 'none'}"
            )
        try:
            expected_counts = {name: int(evidence[name]) for name in evidence}
        except (TypeError, ValueError) as error:
            raise ReleaseError(
                "Full platform smoke table-count evidence contains a non-integer count"
            ) from error
        if any(count < 0 for count in expected_counts.values()):
            raise ReleaseError(
                "Full platform smoke table-count evidence contains a negative count"
            )
        return "full_platform_v1", FULL_PLATFORM_CHANGED_TABLES, expected_counts
    if source_ids == FULL_CMS_SOURCE_IDS:
        return "full_cms_v1", FULL_CMS_CHANGED_TABLES, {}
    return "hospital_affiliations_v1", AFFILIATION_CHANGED_TABLES, {}


def compare_warehouse_release(
    *,
    data_root: Path,
    warehouse_release_id: str,
    backup_manifest_path: Path,
) -> dict:
    """Compare a validated candidate with its immutable baseline, read-only."""
    with _exclusive_lock(data_root / "locks" / "comparison.lock"):
        document = WarehouseReleaseStore(_release_store_path(data_root)).load()
        release = _find_release(document, warehouse_release_id)
        candidate = _verify_promotable(data_root, release)
        policy_name, allowed_changed_tables, expected_counts = _comparison_policy(
            release
        )
        baseline, baseline_sha, baseline_bytes = _load_backup_manifest(
            backup_manifest_path
        )
        if baseline.resolve() != Path(release.baseline_path).resolve():
            raise ReleaseError("Comparison backup path does not match the release baseline")
        if baseline_sha != release.baseline_sha256:
            raise ReleaseError("Comparison backup SHA-256 does not match the release baseline")
        if baseline_bytes is not None and baseline.stat().st_size != baseline_bytes:
            raise ReleaseError("Comparison backup byte size no longer matches its manifest")
        if sha256_file(baseline) != baseline_sha:
            raise ReleaseError("Comparison backup no longer matches its SHA-256")

        baseline_identity = (baseline.stat().st_size, baseline.stat().st_mtime_ns)
        candidate_identity = (candidate.stat().st_size, candidate.stat().st_mtime_ns)
        baseline_connection = duckdb.connect(str(baseline), read_only=True)
        candidate_connection = duckdb.connect(str(candidate), read_only=True)
        try:
            baseline_counts = _database_table_counts(baseline_connection)
            candidate_counts = _database_table_counts(candidate_connection)
            representatives = candidate_connection.execute(
                """
                SELECT npi, hospital_npi, hospital_name, hospital_state,
                       affiliation_source, confidence_level
                FROM hospital_affiliations
                ORDER BY npi, hospital_npi
                LIMIT 10
                """
            ).fetchall()
        finally:
            candidate_connection.close()
            baseline_connection.close()

        if baseline_identity != (baseline.stat().st_size, baseline.stat().st_mtime_ns):
            raise ReleaseError("Comparison baseline changed during its read-only inspection")
        if candidate_identity != (candidate.stat().st_size, candidate.stat().st_mtime_ns):
            raise ReleaseError("Comparison candidate changed during its read-only inspection")

        table_names = set(baseline_counts) | set(candidate_counts)
        invariant_tables = sorted(table_names - allowed_changed_tables)
        unexpected_differences = [
            {
                "table": table,
                "baseline_rows": baseline_counts.get(table),
                "candidate_rows": candidate_counts.get(table),
            }
            for table in invariant_tables
            if baseline_counts.get(table) != candidate_counts.get(table)
        ]
        changed_tables = {
            table: {
                "baseline_rows": baseline_counts.get(table),
                "candidate_rows": candidate_counts.get(table),
            }
            for table in sorted(allowed_changed_tables)
        }
        required_counts = {
            "core_providers": candidate_counts.get("core_providers", 0),
            "practice_locations": candidate_counts.get("practice_locations", 0),
            "hospital_affiliations": candidate_counts.get("hospital_affiliations", 0),
            "raw_hospital_enrollments": candidate_counts.get(
                "raw_hospital_enrollments", 0
            ),
        }
        failed_requirements = [
            table for table, count in required_counts.items() if count <= 0
        ]
        evidence_mismatches = [
            {
                "table": table,
                "expected_rows": expected,
                "candidate_rows": candidate_counts.get(table),
            }
            for table, expected in sorted(expected_counts.items())
            if candidate_counts.get(table) != expected
        ]
        state = (
            "passed"
            if not unexpected_differences
            and not failed_requirements
            and not evidence_mismatches
            else "failed"
        )
        payload = {
            "schema_version": COMPARISON_SCHEMA_VERSION,
            "generated_at": utc_now(),
            "state": state,
            "warehouse_release_id": warehouse_release_id,
            "pipeline_code_commit": release.pipeline_code_commit,
            "duckdb_version": release.duckdb_version,
            "comparison_policy": policy_name,
            "baseline": {
                "database_path": str(baseline),
                "sha256": baseline_sha,
                "byte_size": baseline.stat().st_size,
            },
            "candidate": {
                "database_path": str(candidate),
                "sha256": release.sha256,
                "byte_size": candidate.stat().st_size,
            },
            "unchanged_table_count": len(invariant_tables),
            "changed_tables": changed_tables,
            "required_candidate_counts": required_counts,
            "unexpected_differences": unexpected_differences,
            "failed_requirements": failed_requirements,
            "evidence_mismatches": evidence_mismatches,
            "representative_affiliations": [
                {
                    "npi": row[0],
                    "hospital_npi": row[1],
                    "hospital_name": row[2],
                    "hospital_state": row[3],
                    "affiliation_source": row[4],
                    "confidence_level": row[5],
                }
                for row in representatives
            ],
        }
        comparison_path = (
            data_root / "releases" / warehouse_release_id / "comparison.json"
        )
        _atomic_write_json(comparison_path, payload)
        payload["comparison_path"] = str(comparison_path)
        if state != "passed":
            raise ReleaseError(
                "Candidate comparison failed: "
                f"unexpected_differences={len(unexpected_differences)}, "
                f"failed_requirements={','.join(failed_requirements) or 'none'}, "
                f"evidence_mismatches={len(evidence_mismatches)}"
            )
        return payload


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
