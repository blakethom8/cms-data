"""Conservative retrospective provenance for an immutable warehouse release.

This command exists for legacy source files that predate the run-manifest model.
It validates declared publisher metadata against retained artifacts and read-only
database state, then writes candidate evidence outside the selected deployment.
It never edits a warehouse or production release pointer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import duckdb

from .discovery import safe_error, utc_now
from .manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)
from .source_registry import SOURCE_REGISTRY

AUDIT_SCHEMA_VERSION = 1


class BackfillError(RuntimeError):
    """Evidence could not safely prove the declared installed source."""


class AssessmentState(str, Enum):
    PROVEN = "proven"
    UNRESOLVED = "unresolved"
    NOT_INSTALLED = "not_installed"


class DatabaseKind(str, Enum):
    DUCKDB = "duckdb"
    AACT_POSTGRES = "aact_postgres"


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    source_id: str
    assessment: AssessmentState
    reason: str
    publisher_version: str | None = None
    source_data_period: str | None = None
    publisher_release_timestamp: str | None = None
    discovery_timestamp: str | None = None
    retrieval_timestamp: str | None = None
    source_url: str | None = None
    artifact_path: str | None = None
    byte_size: int | None = None
    sha256: str | None = None
    database_kind: DatabaseKind = DatabaseKind.DUCKDB
    table_counts: dict[str, int] = field(default_factory=dict)
    source_encoding: str | None = None
    pipeline_code_commit: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> SourceEvidence:
        if not isinstance(value, dict):
            raise BackfillError("Each source evidence row must be an object")
        try:
            return cls(
                source_id=str(value["source_id"]),
                assessment=AssessmentState(value["assessment"]),
                reason=str(value["reason"]),
                publisher_version=value.get("publisher_version"),
                source_data_period=value.get("source_data_period"),
                publisher_release_timestamp=value.get("publisher_release_timestamp"),
                discovery_timestamp=value.get("discovery_timestamp"),
                retrieval_timestamp=value.get("retrieval_timestamp"),
                source_url=value.get("source_url"),
                artifact_path=value.get("artifact_path"),
                byte_size=value.get("byte_size"),
                sha256=value.get("sha256"),
                database_kind=DatabaseKind(
                    value.get("database_kind", DatabaseKind.DUCKDB.value)
                ),
                table_counts=value.get("table_counts") or {},
                source_encoding=value.get("source_encoding"),
                pipeline_code_commit=value.get("pipeline_code_commit"),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise BackfillError(f"Malformed source evidence: {safe_error(error)}") from error


@dataclass(frozen=True, slots=True)
class BackfillSpec:
    target_warehouse_sha256: str
    promotion_timestamp: str
    sources: tuple[SourceEvidence, ...]
    schema_version: int = AUDIT_SCHEMA_VERSION

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> BackfillSpec:
        if not isinstance(value, dict):
            raise BackfillError("Backfill evidence must be a JSON object")
        if value.get("schema_version") != AUDIT_SCHEMA_VERSION:
            raise BackfillError(
                f"Unsupported backfill schema_version {value.get('schema_version')!r}"
            )
        rows = value.get("sources")
        if not isinstance(rows, list):
            raise BackfillError("Backfill evidence is missing the sources array")
        try:
            return cls(
                target_warehouse_sha256=str(value["target_warehouse_sha256"]),
                promotion_timestamp=str(value["promotion_timestamp"]),
                sources=tuple(SourceEvidence.from_dict(row) for row in rows),
            )
        except KeyError as error:
            raise BackfillError(
                f"Backfill evidence is missing required field: {error.args[0]}"
            ) from error


@dataclass(frozen=True, slots=True)
class BackfillResult:
    generated_at: str
    target_warehouse_sha256: str
    warehouse_path: str
    warehouse_sha256: str
    assessments: tuple[dict[str, Any], ...]
    manifest: ManifestDocument

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "generated_at": self.generated_at,
            "target_warehouse_sha256": self.target_warehouse_sha256,
            "warehouse_path": self.warehouse_path,
            "warehouse_sha256": self.warehouse_sha256,
            "summary": {
                state.value: sum(
                    row["assessment"] == state.value for row in self.assessments
                )
                for state in AssessmentState
            },
            "assessments": list(self.assessments),
            "candidate_manifest": self.manifest.to_dict(),
        }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _schema_fingerprint(table_schemas: dict[str, list[tuple[str, str]]]) -> str:
    normalized = {
        table: [[name, data_type] for name, data_type in columns]
        for table, columns in sorted(table_schemas.items())
    }
    payload = json.dumps(normalized, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _validate_declared_sha(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise BackfillError(f"{label} must be a lowercase SHA-256")


def _validate_timestamp(value: str, label: str) -> None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise BackfillError(f"{label} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise BackfillError(f"{label} must include a timezone")


def _validate_source_url(source: SourceEvidence) -> None:
    expected_host = (
        "aact.ctti-clinicaltrials.org"
        if source.source_id == "aact_clinical_trials_snapshot"
        else "download.cms.gov"
        if source.source_id.startswith(("nppes_", "open_payments_"))
        else "data.cms.gov"
    )
    parsed = urlsplit(str(source.source_url))
    if parsed.scheme != "https" or parsed.hostname != expected_host:
        raise BackfillError(
            f"{source.source_id} source_url must use official HTTPS host {expected_host}"
        )


def _validate_spec(spec: BackfillSpec, existing: ManifestDocument) -> None:
    _validate_declared_sha(spec.target_warehouse_sha256, "target_warehouse_sha256")
    _validate_timestamp(spec.promotion_timestamp, "promotion_timestamp")
    source_ids = [row.source_id for row in spec.sources]
    if len(source_ids) != len(set(source_ids)):
        raise BackfillError("Backfill evidence contains duplicate source IDs")
    unknown = sorted(set(source_ids) - set(SOURCE_REGISTRY))
    if unknown:
        raise BackfillError("Backfill evidence contains unknown source IDs: " + ", ".join(unknown))
    covered = set(source_ids) | {row.source_id for row in existing.manifests}
    missing = sorted(set(SOURCE_REGISTRY) - covered)
    if missing:
        raise BackfillError(
            "Backfill plus existing manifest does not assess every registry source: "
            + ", ".join(missing)
        )
    for row in spec.sources:
        if not row.reason.strip():
            raise BackfillError(f"{row.source_id} must include an evidence reason")
        if row.assessment != AssessmentState.PROVEN:
            continue
        required = {
            "publisher_version": row.publisher_version,
            "source_data_period": row.source_data_period,
            "publisher_release_timestamp": row.publisher_release_timestamp,
            "discovery_timestamp": row.discovery_timestamp,
            "retrieval_timestamp": row.retrieval_timestamp,
            "source_url": row.source_url,
            "artifact_path": row.artifact_path,
            "byte_size": row.byte_size,
            "sha256": row.sha256,
            "table_counts": row.table_counts,
        }
        missing_fields = [name for name, value in required.items() if value in (None, "", {})]
        if missing_fields:
            raise BackfillError(
                f"{row.source_id} proven evidence is missing: " + ", ".join(missing_fields)
            )
        if not isinstance(row.byte_size, int) or row.byte_size < 0:
            raise BackfillError(f"{row.source_id} byte_size must be non-negative")
        if not isinstance(row.sha256, str):
            raise BackfillError(f"{row.source_id} sha256 is required")
        _validate_declared_sha(row.sha256, f"{row.source_id} sha256")
        _validate_timestamp(
            str(row.publisher_release_timestamp),
            f"{row.source_id} publisher_release_timestamp",
        )
        _validate_timestamp(str(row.discovery_timestamp), f"{row.source_id} discovery_timestamp")
        _validate_timestamp(str(row.retrieval_timestamp), f"{row.source_id} retrieval_timestamp")
        _validate_source_url(row)
        if not isinstance(row.table_counts, dict):
            raise BackfillError(f"{row.source_id} table_counts must be an object")
        if any(
            not isinstance(count, int) or count < 0
            for count in row.table_counts.values()
        ):
            raise BackfillError(f"{row.source_id} table counts must be non-negative integers")


def _duckdb_evidence(
    connection: duckdb.DuckDBPyConnection,
    source: SourceEvidence,
) -> tuple[dict[str, int], dict[str, list[tuple[str, str]]]]:
    counts: dict[str, int] = {}
    schemas: dict[str, list[tuple[str, str]]] = {}
    available = {
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables where table_schema = 'main'"
        ).fetchall()
    }
    for table, expected in sorted(source.table_counts.items()):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise BackfillError(f"Unsafe DuckDB table name: {table}")
        if table not in available:
            raise BackfillError(f"{source.source_id} required table is missing: {table}")
        quoted = table.replace('"', '""')
        actual = int(connection.execute(f'select count(*) from "{quoted}"').fetchone()[0])
        if actual != expected:
            raise BackfillError(
                f"{source.source_id} {table} row count is {actual}; expected {expected}"
            )
        counts[table] = actual
        schemas[table] = [
            (str(row[1]), str(row[2]))
            for row in connection.execute(f"pragma table_info('{table}')").fetchall()
        ]
    return counts, schemas


def _postgres_evidence(
    database_url: str,
    source: SourceEvidence,
) -> tuple[dict[str, int], dict[str, list[tuple[str, str]]]]:
    try:
        import psycopg
    except ImportError as error:  # pragma: no cover - dependency is part of API runtime
        raise BackfillError("psycopg is required for AACT provenance") from error

    counts: dict[str, int] = {}
    schemas: dict[str, list[tuple[str, str]]] = {}
    try:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("show transaction_read_only")
                if cursor.fetchone()[0] != "on":
                    raise BackfillError("AACT provenance connection is not read-only")
                for table, expected in sorted(source.table_counts.items()):
                    if not table.startswith("ctgov.") or not all(
                        part.replace("_", "").isalnum() for part in table.split(".")
                    ):
                        raise BackfillError(f"Unsafe AACT table name: {table}")
                    schema_name, table_name = table.split(".", 1)
                    cursor.execute(
                        "select column_name, data_type from information_schema.columns "
                        "where table_schema = %s and table_name = %s order by ordinal_position",
                        (schema_name, table_name),
                    )
                    columns = [(str(name), str(data_type)) for name, data_type in cursor.fetchall()]
                    if not columns:
                        raise BackfillError(
                            f"{source.source_id} required table is missing: {table}"
                        )
                    cursor.execute(f'select count(*) from "{schema_name}"."{table_name}"')
                    actual = int(cursor.fetchone()[0])
                    if actual != expected:
                        raise BackfillError(
                            f"{source.source_id} {table} row count is {actual}; expected {expected}"
                        )
                    counts[table] = actual
                    schemas[table] = columns
    except BackfillError:
        raise
    except Exception as error:
        raise BackfillError(f"AACT provenance query failed: {safe_error(error)}") from error
    return counts, schemas


def build_backfill(
    spec: BackfillSpec,
    *,
    warehouse_path: Path,
    existing: ManifestDocument | None = None,
    aact_database_url: str | None = None,
) -> BackfillResult:
    """Validate retrospective evidence without opening any database for writes."""
    existing = existing or ManifestDocument()
    _validate_spec(spec, existing)
    if warehouse_path.is_symlink() or not warehouse_path.is_file():
        raise BackfillError("Warehouse must be a regular non-symlink file")
    actual_warehouse_sha = _sha256_file(warehouse_path)
    if actual_warehouse_sha != spec.target_warehouse_sha256:
        raise BackfillError(
            "Warehouse SHA-256 does not match the declared immutable release"
        )

    generated_at = utc_now()
    assessments: list[dict[str, Any]] = []
    generated_manifests: list[RunManifest] = []
    duckdb_connection: duckdb.DuckDBPyConnection | None = None
    try:
        for source in spec.sources:
            assessment = {
                "source_id": source.source_id,
                "assessment": source.assessment.value,
                "reason": safe_error(source.reason),
                "manifest_generated": False,
            }
            if source.assessment != AssessmentState.PROVEN:
                assessments.append(assessment)
                continue

            artifact_path = Path(str(source.artifact_path))
            if artifact_path.is_symlink() or not artifact_path.is_file():
                raise BackfillError(
                    f"{source.source_id} artifact must be a regular non-symlink file"
                )
            if artifact_path.stat().st_size != source.byte_size:
                raise BackfillError(f"{source.source_id} artifact byte size does not match")
            actual_source_sha = _sha256_file(artifact_path)
            if actual_source_sha != source.sha256:
                raise BackfillError(f"{source.source_id} artifact SHA-256 does not match")

            if source.database_kind == DatabaseKind.DUCKDB:
                if duckdb_connection is None:
                    duckdb_connection = duckdb.connect(str(warehouse_path), read_only=True)
                counts, schemas = _duckdb_evidence(duckdb_connection, source)
            else:
                if not aact_database_url:
                    raise BackfillError(
                        f"{source.source_id} requires a read-only AACT database URL"
                    )
                counts, schemas = _postgres_evidence(aact_database_url, source)

            source_release_id = f"{source.source_id}-{actual_source_sha[:16]}"
            generated_manifests.append(
                RunManifest(
                    run_id=f"backfill-{source.source_id}-{actual_source_sha[:12]}",
                    release_id=source_release_id,
                    source_id=source.source_id,
                    publisher=SOURCE_REGISTRY[source.source_id].publisher.value,
                    publisher_version=str(source.publisher_version),
                    source_data_period=str(source.source_data_period),
                    publisher_release_timestamp=source.publisher_release_timestamp,
                    discovery_timestamp=str(source.discovery_timestamp),
                    retrieval_timestamp=source.retrieval_timestamp,
                    source_url=source.source_url,
                    byte_size=source.byte_size,
                    sha256=source.sha256,
                    schema_fingerprint=_schema_fingerprint(schemas),
                    source_encoding=source.source_encoding,
                    row_counts=counts,
                    pipeline_code_commit=source.pipeline_code_commit,
                    validation_state=ValidationState.PASSED,
                    validation_timestamp=generated_at,
                    promotion_state=PromotionState.ACTIVE,
                    promotion_timestamp=spec.promotion_timestamp,
                    active_release_id=source_release_id,
                    operator_summary=(
                        "Retrospective provenance backfill for immutable warehouse sha256:"
                        f"{actual_warehouse_sha}; {source.reason}"
                    ),
                )
            )
            assessment["manifest_generated"] = True
            assessment["publisher_version"] = source.publisher_version
            assessment["row_counts"] = counts
            assessment["artifact_sha256"] = actual_source_sha
            assessments.append(assessment)
    finally:
        if duckdb_connection is not None:
            duckdb_connection.close()

    replaced = {manifest.source_id for manifest in generated_manifests}
    combined = [row for row in existing.manifests if row.source_id not in replaced]
    combined.extend(generated_manifests)
    combined.sort(key=lambda row: (row.source_id, row.run_id))
    return BackfillResult(
        generated_at=generated_at,
        target_warehouse_sha256=spec.target_warehouse_sha256,
        warehouse_path=str(warehouse_path),
        warehouse_sha256=actual_warehouse_sha,
        assessments=tuple(assessments),
        manifest=ManifestDocument(manifests=combined),
    )


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate legacy source provenance and emit staged candidate evidence"
    )
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument("--existing-manifest", type=Path)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--audit-output", type=Path)
    parser.add_argument(
        "--aact-dsn-env",
        default="AACT_DATABASE_URL",
        help="Environment variable containing the read-only AACT PostgreSQL URL",
    )
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        payload = json.loads(args.evidence.read_text(encoding="utf-8"))
        spec = BackfillSpec.from_dict(payload)
        existing = (
            ManifestStore(args.existing_manifest).load()
            if args.existing_manifest
            else ManifestDocument()
        )
        result = build_backfill(
            spec,
            warehouse_path=args.warehouse,
            existing=existing,
            aact_database_url=os.getenv(args.aact_dsn_env),
        )
        if args.manifest_output:
            ManifestStore(args.manifest_output).save(result.manifest)
        if args.audit_output:
            _write_json_atomic(args.audit_output, result.to_dict())
        if args.json or not (args.manifest_output or args.audit_output):
            print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return 0
    except (BackfillError, OSError, ValueError, json.JSONDecodeError) as error:
        message = safe_error(error)
        if args.json:
            print(json.dumps({"schema_version": 1, "error": message}, sort_keys=True))
        else:
            print(f"Provenance backfill failed: {message}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
