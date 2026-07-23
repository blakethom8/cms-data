"""Publish a release-pinned California reporting replica to PostgreSQL.

The publisher never mutates DuckDB. It reads an explicit validated warehouse release,
loads a build-specific PostgreSQL schema, validates it, and transactionally switches
stable Tableau-facing views only after every check passes.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .discovery import safe_error
from .manifests import ManifestStore, PromotionState, ValidationState
from .releases import WarehouseRelease, WarehouseReleaseStore, sha256_file
from .reporting_contract import (
    REPORTING_CONTRACT_VERSION,
    REPORTING_MODELS,
    REPORTING_SCOPE,
    REPORTING_STATE,
    SOURCE_DETAIL_MODELS,
    ReportingModel,
    SourceDetailModel,
)
from .source_registry import CMS_ATTRIBUTION, SOURCE_REGISTRY

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "reporting.sql"
MIN_FREE_BYTES_DEFAULT = 15 * 1024**3
COPY_CHUNK_BYTES = 8 * 1024 * 1024
SAFE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class ReportingError(RuntimeError):
    """The reporting replica could not be built or published safely."""


@dataclass(frozen=True, slots=True)
class ReleaseInput:
    warehouse_release_id: str
    database_path: Path
    sha256: str
    pipeline_code_commit: str | None
    source_run_ids: tuple[str, ...]
    manifest_path: Path | None = None


@dataclass(frozen=True, slots=True)
class ModelProfile:
    layer: str
    name: str
    grain: str
    scope_rule: str
    row_count: int
    column_count: int


@dataclass(frozen=True, slots=True)
class ReportingProfile:
    database_path: str
    scope_name: str
    state: str
    models: tuple[ModelProfile, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "database_path": self.database_path,
            "scope_name": self.scope_name,
            "state": self.state,
            "models": [asdict(model) for model in self.models],
        }


@dataclass(frozen=True, slots=True)
class PublishResult:
    snapshot_id: str
    warehouse_release_id: str
    build_schema: str
    row_counts: dict[str, int]
    validation_results: dict[str, Any]
    previous_snapshot_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ExtractedTable:
    layer: str
    name: str
    columns: tuple[str, ...]
    duckdb_types: tuple[str, ...]
    row_count: int
    csv_path: Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_identifier(value: str, label: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ReportingError(f"Invalid {label}: {value!r}")
    return value


def _duckdb_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _relation_exists(connection: duckdb.DuckDBPyConnection, relation: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [relation],
    ).fetchone()
    return bool(row and row[0])


def _describe_query(
    connection: duckdb.DuckDBPyConnection, query: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    try:
        cursor = connection.execute(f"SELECT * FROM ({query}) reporting_query LIMIT 0")
    except duckdb.Error as error:
        raise ReportingError(f"Reporting query contract failed: {safe_error(error)}") from error
    columns = tuple(description[0] for description in cursor.description)
    types = tuple(str(description[1]) for description in cursor.description)
    if not columns or len(set(columns)) != len(columns):
        raise ReportingError("Reporting query produced missing or duplicate column names")
    return columns, types


def _query_count(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    try:
        return int(
            connection.execute(f"SELECT COUNT(*) FROM ({query}) reporting_query").fetchone()[0]
        )
    except duckdb.Error as error:
        raise ReportingError(f"Reporting count query failed: {safe_error(error)}") from error


def profile_database(database_path: Path) -> ReportingProfile:
    if not database_path.is_file():
        raise ReportingError(f"DuckDB database not found: {database_path}")
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        profiles: list[ModelProfile] = []
        for model in REPORTING_MODELS:
            columns, _ = _describe_query(connection, model.query)
            profiles.append(
                ModelProfile(
                    layer="reporting",
                    name=model.name,
                    grain=model.grain,
                    scope_rule=model.scope_rule,
                    row_count=_query_count(connection, model.query),
                    column_count=len(columns),
                )
            )
        for model in SOURCE_DETAIL_MODELS:
            if not _relation_exists(connection, model.source_table):
                raise ReportingError(
                    f"Required source-detail table is missing: {model.source_table}"
                )
            query = model.query
            columns, _ = _describe_query(connection, query)
            profiles.append(
                ModelProfile(
                    layer="source_detail",
                    name=model.name,
                    grain=model.grain,
                    scope_rule=model.scope_rule,
                    row_count=_query_count(connection, query),
                    column_count=len(columns),
                )
            )
        return ReportingProfile(
            database_path=str(database_path),
            scope_name=REPORTING_SCOPE,
            state=REPORTING_STATE,
            models=tuple(profiles),
        )
    finally:
        connection.close()


def resolve_release(data_root: Path, warehouse_release_id: str) -> ReleaseInput:
    document = WarehouseReleaseStore(data_root / "warehouse-releases.json").load()
    matches = [
        release
        for release in document.releases
        if release.warehouse_release_id == warehouse_release_id
    ]
    if len(matches) != 1:
        raise ReportingError(
            f"Expected exactly one warehouse release {warehouse_release_id}; found {len(matches)}"
        )
    release: WarehouseRelease = matches[0]
    if release.validation_state != ValidationState.PASSED:
        raise ReportingError("Warehouse release has not passed validation")
    if release.promotion_state != PromotionState.ACTIVE:
        raise ReportingError("Warehouse release is not actively promoted")
    if not release.sha256:
        raise ReportingError("Warehouse release has no database checksum")
    database_path = Path(release.database_path)
    if not database_path.is_absolute():
        database_path = data_root / database_path
    if not database_path.is_file() or database_path.is_symlink():
        raise ReportingError("Warehouse release database must be an explicit regular file")
    if sha256_file(database_path) != release.sha256:
        raise ReportingError("Warehouse release database checksum does not match its manifest")
    return ReleaseInput(
        warehouse_release_id=release.warehouse_release_id,
        database_path=database_path,
        sha256=release.sha256,
        pipeline_code_commit=release.pipeline_code_commit,
        source_run_ids=release.source_run_ids,
    )


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise ReportingError(f"{label} must be an explicit regular file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReportingError(f"Could not read {label}: {safe_error(error)}") from error
    if not isinstance(payload, dict):
        raise ReportingError(f"{label} must contain a JSON object")
    return payload


def resolve_production_release(production_root: Path) -> ReleaseInput:
    """Resolve the one verified warehouse selected by the production control plane."""

    if not production_root.is_absolute() or production_root.is_symlink():
        raise ReportingError("Production root must be an explicit absolute directory")
    try:
        root = production_root.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ReportingError("Production root does not exist") from error
    if not root.is_dir() or root != production_root:
        raise ReportingError("Production root is not canonical")
    if (root / "transition-pending").exists() or (root / "transition-pending").is_symlink():
        raise ReportingError("Production has a pending transition")

    ledger = _load_json_object(root / "deployments.json", "production deployment ledger")
    selected_id = ledger.get("selected_deployment_id")
    deployments = ledger.get("deployments")
    if not isinstance(selected_id, str) or not re.fullmatch(
        r"deployment-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{10}", selected_id
    ):
        raise ReportingError("Production ledger has no valid selected deployment")
    if not isinstance(deployments, list):
        raise ReportingError("Production deployment ledger is malformed")
    selected = [
        deployment
        for deployment in deployments
        if isinstance(deployment, dict) and deployment.get("deployment_id") == selected_id
    ]
    if len(selected) != 1:
        raise ReportingError("Production ledger does not identify exactly one deployment")
    deployment = selected[0]
    if deployment.get("state") != "verified":
        raise ReportingError("Selected production deployment is not verified")

    selector = root / "release-current"
    if not selector.is_symlink():
        raise ReportingError("Production release-current selector is not a symlink")
    expected_bundle = root / "releases" / selected_id
    try:
        bundle = selector.resolve(strict=True)
        canonical_bundle = expected_bundle.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ReportingError("Selected production bundle cannot be resolved") from error
    if bundle != canonical_bundle or not bundle.is_dir() or bundle.is_symlink():
        raise ReportingError("Production selector does not match the verified deployment")

    warehouse_selector = bundle / "warehouse"
    if not warehouse_selector.is_symlink():
        raise ReportingError("Production warehouse target is not an immutable bundle link")
    try:
        database_path = warehouse_selector.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ReportingError("Production warehouse target cannot be resolved") from error
    if not database_path.is_file() or database_path.is_symlink():
        raise ReportingError("Production warehouse target must resolve to a regular file")

    release_id = deployment.get("warehouse_release_id")
    expected_sha256 = deployment.get("warehouse_sha256")
    if not isinstance(release_id, str) or not re.fullmatch(
        r"warehouse-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{10}", release_id
    ):
        raise ReportingError("Selected deployment has an invalid warehouse release ID")
    if not isinstance(expected_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_sha256
    ):
        raise ReportingError("Selected deployment has an invalid warehouse checksum")
    expected_size = deployment.get("warehouse_byte_size")
    if not isinstance(expected_size, int) or database_path.stat().st_size != expected_size:
        raise ReportingError("Production warehouse size does not match the deployment ledger")
    if sha256_file(database_path) != expected_sha256:
        raise ReportingError("Production warehouse checksum does not match the deployment ledger")

    evidence_root = root / "evidence" / selected_id
    release_evidence = _load_json_object(
        evidence_root / "warehouse-release.json", "production warehouse release evidence"
    )
    release = release_evidence.get("release")
    if not isinstance(release, dict):
        raise ReportingError("Production warehouse release evidence is malformed")
    if (
        release.get("warehouse_release_id") != release_id
        or release.get("sha256") != expected_sha256
    ):
        raise ReportingError("Production warehouse evidence does not match the selected deployment")
    source_run_ids = release.get("source_run_ids")
    if not isinstance(source_run_ids, list) or not all(
        isinstance(run_id, str) for run_id in source_run_ids
    ):
        raise ReportingError("Production warehouse evidence has invalid source run IDs")

    manifest_path = evidence_root / "source-manifests.json"
    manifest_evidence = _load_json_object(
        manifest_path, "production source manifest evidence"
    )
    if (
        manifest_evidence.get("deployment_id") != selected_id
        or manifest_evidence.get("warehouse_release_id") != release_id
    ):
        raise ReportingError("Production source manifests do not match the selected deployment")

    pipeline_commit = release.get("pipeline_code_commit")
    if pipeline_commit is not None and not (
        isinstance(pipeline_commit, str) and re.fullmatch(r"[0-9a-f]{40}", pipeline_commit)
    ):
        raise ReportingError("Production warehouse evidence has an invalid pipeline commit")
    return ReleaseInput(
        warehouse_release_id=release_id,
        database_path=database_path,
        sha256=expected_sha256,
        pipeline_code_commit=pipeline_commit,
        source_run_ids=tuple(source_run_ids),
        manifest_path=manifest_path,
    )


def _source_periods(data_root: Path, release: ReleaseInput) -> dict[str, str]:
    document = ManifestStore(release.manifest_path or data_root / "manifests.json").load()
    periods: dict[str, str] = {}
    for manifest in document.manifests:
        if manifest.run_id in release.source_run_ids or manifest.proves_active_installation:
            periods[manifest.source_id] = manifest.source_data_period
    for model in SOURCE_DETAIL_MODELS:
        periods.setdefault(model.source_dataset_id, "not_recorded")
    return periods


def _postgres_type(duckdb_type: str) -> str:
    normalized = duckdb_type.upper()
    if normalized in {"BOOLEAN"}:
        return "BOOLEAN"
    if normalized in {"TINYINT", "UTINYINT", "SMALLINT", "USMALLINT"}:
        return "SMALLINT"
    if normalized in {"INTEGER", "UINTEGER"}:
        return "INTEGER"
    if normalized in {"BIGINT", "UBIGINT", "HUGEINT", "UHUGEINT"}:
        return "BIGINT"
    if normalized in {"REAL", "FLOAT", "DOUBLE"}:
        return "DOUBLE PRECISION"
    if normalized.startswith("DECIMAL"):
        return normalized
    if normalized == "DATE":
        return "DATE"
    if normalized.startswith("TIMESTAMP WITH TIME ZONE") or normalized == "TIMESTAMPTZ":
        return "TIMESTAMPTZ"
    if normalized.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if normalized == "TIME":
        return "TIME"
    if normalized in {"BLOB", "BYTEA"}:
        return "BYTEA"
    return "TEXT"


def _snapshot_id(release_id: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", release_id).strip("-")[-36:]
    return f"{normalized}-{uuid.uuid4().hex[:10]}"


def _build_schema_name(snapshot_id: str) -> str:
    compact = re.sub(r"[^a-z0-9]+", "_", snapshot_id.lower()).strip("_")
    return _validate_identifier(f"reporting_build_{compact[-40:]}", "build schema")


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ReportingError(f"Another reporting build holds {path}") from error
        yield


def _extract_query(
    connection: duckdb.DuckDBPyConnection,
    *,
    layer: str,
    name: str,
    query: str,
    output_directory: Path,
) -> ExtractedTable:
    columns, types = _describe_query(connection, query)
    row_count = _query_count(connection, query)
    csv_path = output_directory / f"{layer}-{name}.csv"
    copy_query = (
        f"COPY ({query}) TO {_duckdb_literal(str(csv_path))} "
        "(FORMAT CSV, HEADER TRUE, NULL '')"
    )
    try:
        connection.execute(copy_query)
    except duckdb.Error as error:
        raise ReportingError(f"Failed to export {layer}.{name}: {safe_error(error)}") from error
    return ExtractedTable(
        layer=layer,
        name=name,
        columns=columns,
        duckdb_types=types,
        row_count=row_count,
        csv_path=csv_path,
    )


def _execute_sql_file(connection: psycopg.Connection[Any], path: Path) -> None:
    if not path.is_file():
        raise ReportingError(f"Reporting schema SQL not found: {path}")
    with connection.cursor() as cursor:
        cursor.execute(path.read_text(encoding="utf-8"))
    connection.commit()


def _create_and_load_table(
    connection: psycopg.Connection[Any],
    build_schema: str,
    extracted: ExtractedTable,
    key_columns: tuple[str, ...] = (),
) -> None:
    oversized_columns = [
        column for column in extracted.columns if len(column.encode("utf-8")) > 63
    ]
    if oversized_columns:
        raise ReportingError(
            f"Cannot publish {extracted.layer}.{extracted.name}: PostgreSQL identifiers "
            f"are limited to 63 bytes; oversized columns: {', '.join(oversized_columns)}"
        )
    table_identifier = sql.Identifier(build_schema, extracted.name)
    column_definitions = [
        sql.SQL("{} {}").format(sql.Identifier(column), sql.SQL(_postgres_type(data_type)))
        for column, data_type in zip(
            extracted.columns, extracted.duckdb_types, strict=True
        )
    ]
    create_statement = sql.SQL("CREATE TABLE {} ({})").format(
        table_identifier, sql.SQL(", ").join(column_definitions)
    )
    copy_statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)").format(
        table_identifier,
        sql.SQL(", ").join(sql.Identifier(column) for column in extracted.columns),
    )
    with connection.cursor() as cursor:
        cursor.execute(create_statement)
        with extracted.csv_path.open("rb") as source:
            with cursor.copy(copy_statement) as copy:
                while chunk := source.read(COPY_CHUNK_BYTES):
                    copy.write(chunk)
        if key_columns:
            cursor.execute(
                sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
                    table_identifier,
                    sql.SQL(", ").join(sql.Identifier(column) for column in key_columns),
                )
            )
        if "npi" in extracted.columns and "npi" not in key_columns:
            cursor.execute(
                sql.SQL("CREATE INDEX ON {} ({})").format(
                    table_identifier, sql.Identifier("npi")
                )
            )
    connection.commit()


def _period_semantics(source_id: str) -> str:
    spec = SOURCE_REGISTRY.get(source_id)
    if spec:
        return spec.source_period_semantics
    if source_id == "cms_dac_national_legacy":
        return "Doctors and Clinicians publisher snapshot; period not yet persisted by the legacy loader."
    return "See the source manifest and reporting model notes."


def _attribution(source_id: str) -> str:
    spec = SOURCE_REGISTRY.get(source_id)
    if spec:
        return spec.licensing_notes
    return CMS_ATTRIBUTION


def _insert_model_metadata(
    connection: psycopg.Connection[Any],
    snapshot_id: str,
    model: ReportingModel,
    extracted: ExtractedTable,
) -> None:
    source_ids = sorted({field.source_dataset_id for field in model.fields})
    semantics = "; ".join(
        f"{source_id}: {_period_semantics(source_id)}" for source_id in source_ids
    )
    attribution = " ".join(dict.fromkeys(_attribution(source_id) for source_id in source_ids))
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.model_catalog (
                snapshot_id, layer, model_name, source_dataset_id, source_tables,
                declared_grain, scope_rule, source_period_semantics, attribution,
                notes, row_count
            ) VALUES (%s, 'reporting', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_id,
                model.name,
                ", ".join(source_ids),
                list(model.source_tables),
                model.grain,
                model.scope_rule,
                semantics,
                attribution,
                model.notes,
                extracted.row_count,
            ),
        )
        for position, field in enumerate(model.fields, start=1):
            cursor.execute(
                """
                INSERT INTO control.column_lineage (
                    snapshot_id, layer, model_name, model_column, ordinal_position,
                    source_dataset_id, source_table, source_column, transformation,
                    declared_grain, scope_rule, source_period_semantics,
                    is_derived, is_inferred, notes
                ) VALUES (
                    %s, 'reporting', %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    snapshot_id,
                    model.name,
                    field.name,
                    position,
                    field.source_dataset_id,
                    field.source_table,
                    field.source_column,
                    field.transformation,
                    model.grain,
                    model.scope_rule,
                    _period_semantics(field.source_dataset_id),
                    field.derived,
                    field.inferred,
                    model.notes or None,
                ),
            )
    connection.commit()


def _insert_source_metadata(
    connection: psycopg.Connection[Any],
    snapshot_id: str,
    model: SourceDetailModel,
    extracted: ExtractedTable,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.model_catalog (
                snapshot_id, layer, model_name, source_dataset_id, source_tables,
                declared_grain, scope_rule, source_period_semantics, attribution,
                notes, row_count
            ) VALUES (%s, 'source_detail', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_id,
                model.name,
                model.source_dataset_id,
                [model.source_table],
                model.grain,
                model.scope_rule,
                model.source_period_semantics,
                model.attribution,
                model.notes,
                extracted.row_count,
            ),
        )
        for position, column in enumerate(extracted.columns, start=1):
            cursor.execute(
                """
                INSERT INTO control.column_lineage (
                    snapshot_id, layer, model_name, model_column, ordinal_position,
                    source_dataset_id, source_table, source_column, transformation,
                    declared_grain, scope_rule, source_period_semantics,
                    is_derived, is_inferred, notes
                ) VALUES (
                    %s, 'source_detail', %s, %s, %s, %s, %s, %s,
                    'Source-faithful loaded column', %s, %s, %s, FALSE, FALSE, %s
                )
                """,
                (
                    snapshot_id,
                    model.name,
                    column,
                    position,
                    model.source_dataset_id,
                    model.source_table,
                    model.source_column(column),
                    model.grain,
                    model.scope_rule,
                    model.source_period_semantics,
                    model.notes or None,
                ),
            )
    connection.commit()


def _scalar(connection: psycopg.Connection[Any], statement: sql.Composed) -> int:
    with connection.cursor() as cursor:
        cursor.execute(statement)
        return int(cursor.fetchone()[0])


def _validate_build(
    connection: psycopg.Connection[Any],
    build_schema: str,
    expected_counts: dict[str, int],
) -> dict[str, Any]:
    checks: dict[str, Any] = {"row_counts": {}, "key_uniqueness": {}, "orphans": {}}
    for model in REPORTING_MODELS:
        table = sql.Identifier(build_schema, model.name)
        actual_count = _scalar(
            connection, sql.SQL("SELECT COUNT(*) FROM {}").format(table)
        )
        expected = expected_counts[f"reporting.{model.name}"]
        if actual_count != expected:
            raise ReportingError(
                f"Row-count mismatch for {model.name}: DuckDB={expected}, PostgreSQL={actual_count}"
            )
        checks["row_counts"][f"reporting.{model.name}"] = actual_count
        if actual_count == 0 and model.name == "dim_provider":
            raise ReportingError("California provider dimension is empty")
        key_sql = sql.SQL(", ").join(sql.Identifier(key) for key in model.key_columns)
        duplicate_count = _scalar(
            connection,
            sql.SQL(
                "SELECT COUNT(*) FROM (SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1) d"
            ).format(key_sql, table, key_sql),
        )
        if duplicate_count:
            raise ReportingError(f"Duplicate declared grain in {model.name}")
        checks["key_uniqueness"][model.name] = "passed"

    dim_table = sql.Identifier(build_schema, "dim_provider")
    outside_state = _scalar(
        connection,
        sql.SQL("SELECT COUNT(*) FROM {} WHERE state <> 'CA' OR state IS NULL").format(
            dim_table
        ),
    )
    if outside_state:
        raise ReportingError("Provider dimension contains records outside California")
    location_table = sql.Identifier(build_schema, "bridge_provider_location")
    outside_location = _scalar(
        connection,
        sql.SQL("SELECT COUNT(*) FROM {} WHERE state <> 'CA' OR state IS NULL").format(
            location_table
        ),
    )
    if outside_location:
        raise ReportingError("Provider location bridge contains records outside California")
    enrollment_location_table = sql.Identifier(
        build_schema, "bridge_pecos_enrollment_location"
    )
    outside_enrollment_location = _scalar(
        connection,
        sql.SQL("SELECT COUNT(*) FROM {} WHERE state <> 'CA' OR state IS NULL").format(
            enrollment_location_table
        ),
    )
    if outside_enrollment_location:
        raise ReportingError(
            "PECOS enrollment location bridge contains records outside California"
        )
    checks["scope"] = {
        "dim_provider": "passed",
        "bridge_provider_location": "passed",
        "bridge_pecos_enrollment_location": "passed",
    }

    for model in REPORTING_MODELS:
        if model.name == "dim_provider" or not any(
            field.name == "npi" for field in model.fields
        ):
            continue
        table = sql.Identifier(build_schema, model.name)
        orphan_count = _scalar(
            connection,
            sql.SQL(
                "SELECT COUNT(*) FROM {} f LEFT JOIN {} d ON d.npi = f.npi "
                "WHERE d.npi IS NULL"
            ).format(table, dim_table),
        )
        if orphan_count:
            raise ReportingError(f"Orphan provider keys in {model.name}: {orphan_count}")
        checks["orphans"][model.name] = 0

    for model in SOURCE_DETAIL_MODELS:
        table = sql.Identifier(build_schema, model.name)
        actual_count = _scalar(
            connection, sql.SQL("SELECT COUNT(*) FROM {}").format(table)
        )
        expected = expected_counts[f"source_detail.{model.name}"]
        if actual_count != expected:
            raise ReportingError(
                f"Row-count mismatch for {model.name}: DuckDB={expected}, PostgreSQL={actual_count}"
            )
        checks["row_counts"][f"source_detail.{model.name}"] = actual_count
    checks["contract_version"] = REPORTING_CONTRACT_VERSION
    checks["state"] = REPORTING_STATE
    checks["status"] = "passed"
    return checks


def _role_exists(connection: psycopg.Connection[Any], role: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)", (role,))
        return bool(cursor.fetchone()[0])


def _switch_published_views(
    connection: psycopg.Connection[Any],
    *,
    snapshot_id: str,
    build_schema: str,
    reader_role: str | None,
) -> str | None:
    if reader_role:
        _validate_identifier(reader_role, "reader role")
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext('cms_reporting_publish'))")
            cursor.execute(
                "SELECT snapshot_id FROM control.reporting_snapshot "
                "WHERE status = 'active' FOR UPDATE"
            )
            row = cursor.fetchone()
            previous_snapshot_id = row[0] if row else None
            cursor.execute(
                "SELECT layer, model_name FROM control.model_catalog "
                "WHERE snapshot_id = %s ORDER BY layer, model_name",
                (snapshot_id,),
            )
            models = cursor.fetchall()
            for layer, model_name in models:
                _validate_identifier(layer, "published layer")
                _validate_identifier(model_name, "model name")
                cursor.execute(
                    sql.SQL("DROP VIEW IF EXISTS {}.{}").format(
                        sql.Identifier(layer), sql.Identifier(model_name)
                    )
                )
                cursor.execute(
                    sql.SQL("CREATE VIEW {}.{} AS SELECT * FROM {}.{}").format(
                        sql.Identifier(layer),
                        sql.Identifier(model_name),
                        sql.Identifier(build_schema),
                        sql.Identifier(model_name),
                    )
                )
            if previous_snapshot_id:
                cursor.execute(
                    "UPDATE control.reporting_snapshot SET status = 'superseded' "
                    "WHERE snapshot_id = %s",
                    (previous_snapshot_id,),
                )
            cursor.execute(
                """
                UPDATE control.reporting_snapshot
                SET status = 'active', published_at = NOW(), completed_at = NOW(),
                    previous_snapshot_id = %s
                WHERE snapshot_id = %s AND status = 'validated'
                """,
                (previous_snapshot_id, snapshot_id),
            )
            if cursor.rowcount != 1:
                raise ReportingError("Validated reporting snapshot was not publishable")
            if reader_role and _role_exists(connection, reader_role):
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA reporting, source_detail, control TO {}").format(
                        sql.Identifier(reader_role)
                    )
                )
                cursor.execute(
                    sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA reporting, source_detail TO {}").format(
                        sql.Identifier(reader_role)
                    )
                )
                cursor.execute(
                    sql.SQL(
                        "GRANT SELECT ON control.active_reporting_snapshot, "
                        "control.model_catalog, control.column_lineage TO {}"
                    ).format(sql.Identifier(reader_role))
                )
    return previous_snapshot_id


def _publish_release_unlocked(
    *,
    release: ReleaseInput,
    data_root: Path,
    postgres_dsn: str,
    temporary_root: Path | None = None,
    minimum_free_bytes: int = MIN_FREE_BYTES_DEFAULT,
    reader_role: str | None = "tableau_reader",
) -> PublishResult:
    disk_target = temporary_root or Path(tempfile.gettempdir())
    if shutil.disk_usage(disk_target).free < minimum_free_bytes:
        raise ReportingError(
            f"Insufficient free disk under {disk_target}; require at least "
            f"{minimum_free_bytes / 1024**3:.1f} GiB before extraction"
        )
    snapshot_id = _snapshot_id(release.warehouse_release_id)
    build_schema = _build_schema_name(snapshot_id)
    source_periods = _source_periods(data_root, release)
    started_at = utc_now()
    connection = psycopg.connect(postgres_dsn)
    duckdb_connection: duckdb.DuckDBPyConnection | None = None
    try:
        _execute_sql_file(connection, SCHEMA_PATH)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT snapshot_id, build_schema, table_row_counts,
                       validation_results, previous_snapshot_id
                FROM control.reporting_snapshot
                WHERE status = 'active'
                  AND warehouse_release_id = %s
                  AND warehouse_sha256 = %s
                  AND contract_version = %s
                  AND scope_name = %s
                """,
                (
                    release.warehouse_release_id,
                    release.sha256,
                    REPORTING_CONTRACT_VERSION,
                    REPORTING_SCOPE,
                ),
            )
            active = cursor.fetchone()
        if active:
            logger.info(
                "Reporting snapshot %s already publishes release %s",
                active[0],
                release.warehouse_release_id,
            )
            return PublishResult(
                snapshot_id=active[0],
                warehouse_release_id=release.warehouse_release_id,
                build_schema=active[1],
                row_counts=active[2],
                validation_results=active[3],
                previous_snapshot_id=active[4],
            )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.reporting_snapshot (
                    snapshot_id, contract_version, scope_name, scope_rule,
                    warehouse_release_id, warehouse_sha256, pipeline_code_commit,
                    build_schema, status, started_at, source_periods
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'building', %s, %s)
                """,
                (
                    snapshot_id,
                    REPORTING_CONTRACT_VERSION,
                    REPORTING_SCOPE,
                    "California source-specific practice-state membership",
                    release.warehouse_release_id,
                    release.sha256,
                    release.pipeline_code_commit,
                    build_schema,
                    started_at,
                    Jsonb(source_periods),
                ),
            )
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(build_schema)))
        connection.commit()

        duckdb_connection = duckdb.connect(str(release.database_path), read_only=True)
        expected_counts: dict[str, int] = {}
        with tempfile.TemporaryDirectory(
            prefix="cms-reporting-", dir=temporary_root
        ) as temporary_directory:
            output_directory = Path(temporary_directory)
            for model in REPORTING_MODELS:
                extracted = _extract_query(
                    duckdb_connection,
                    layer="reporting",
                    name=model.name,
                    query=model.query,
                    output_directory=output_directory,
                )
                _create_and_load_table(
                    connection,
                    build_schema,
                    extracted,
                    key_columns=model.key_columns,
                )
                _insert_model_metadata(connection, snapshot_id, model, extracted)
                expected_counts[f"reporting.{model.name}"] = extracted.row_count

            for model in SOURCE_DETAIL_MODELS:
                if not _relation_exists(duckdb_connection, model.source_table):
                    raise ReportingError(
                        f"Required source-detail table is missing: {model.source_table}"
                    )
                query = model.query
                extracted = _extract_query(
                    duckdb_connection,
                    layer="source_detail",
                    name=model.name,
                    query=query,
                    output_directory=output_directory,
                )
                _create_and_load_table(connection, build_schema, extracted)
                _insert_source_metadata(connection, snapshot_id, model, extracted)
                expected_counts[f"source_detail.{model.name}"] = extracted.row_count

        validation_results = _validate_build(
            connection, build_schema, expected_counts
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.reporting_snapshot
                SET status = 'validated', completed_at = NOW(),
                    table_row_counts = %s, validation_results = %s
                WHERE snapshot_id = %s AND status = 'building'
                """,
                (Jsonb(expected_counts), Jsonb(validation_results), snapshot_id),
            )
        connection.commit()
        previous_snapshot_id = _switch_published_views(
            connection,
            snapshot_id=snapshot_id,
            build_schema=build_schema,
            reader_role=reader_role,
        )
        return PublishResult(
            snapshot_id=snapshot_id,
            warehouse_release_id=release.warehouse_release_id,
            build_schema=build_schema,
            row_counts=expected_counts,
            validation_results=validation_results,
            previous_snapshot_id=previous_snapshot_id,
        )
    except Exception as error:
        connection.rollback()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE control.reporting_snapshot
                    SET status = 'failed', completed_at = NOW(), error_summary = %s
                    WHERE snapshot_id = %s AND status IN ('building', 'validated')
                    """,
                    (safe_error(error), snapshot_id),
                )
            connection.commit()
        except Exception:
            connection.rollback()
        if isinstance(error, ReportingError):
            raise
        raise ReportingError(safe_error(error)) from error
    finally:
        if duckdb_connection is not None:
            duckdb_connection.close()
        connection.close()


def publish_release(
    *,
    release: ReleaseInput,
    data_root: Path,
    postgres_dsn: str,
    temporary_root: Path | None = None,
    minimum_free_bytes: int = MIN_FREE_BYTES_DEFAULT,
    reader_role: str | None = "tableau_reader",
) -> PublishResult:
    lock_path = data_root / "locks" / "tableau-reporting.lock"
    with _exclusive_lock(lock_path):
        return _publish_release_unlocked(
            release=release,
            data_root=data_root,
            postgres_dsn=postgres_dsn,
            temporary_root=temporary_root,
            minimum_free_bytes=minimum_free_bytes,
            reader_role=reader_role,
        )


def rollback_snapshot(
    *,
    postgres_dsn: str,
    snapshot_id: str,
    reader_role: str | None = "tableau_reader",
) -> str | None:
    connection = psycopg.connect(postgres_dsn)
    try:
        _execute_sql_file(connection, SCHEMA_PATH)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT build_schema, status FROM control.reporting_snapshot "
                "WHERE snapshot_id = %s",
                (snapshot_id,),
            )
            row = cursor.fetchone()
        if not row:
            raise ReportingError(f"Unknown reporting snapshot: {snapshot_id}")
        build_schema, status = row
        if status not in {"active", "superseded"}:
            raise ReportingError("Only a previously published snapshot can be restored")
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE control.reporting_snapshot SET status = 'validated' "
                "WHERE snapshot_id = %s",
                (snapshot_id,),
            )
        connection.commit()
        return _switch_published_views(
            connection,
            snapshot_id=snapshot_id,
            build_schema=build_schema,
            reader_role=reader_role,
        )
    except Exception as error:
        connection.rollback()
        if isinstance(error, ReportingError):
            raise
        raise ReportingError(safe_error(error)) from error
    finally:
        connection.close()


def _dsn_from_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ReportingError(f"Required PostgreSQL DSN environment variable is unset: {name}")
    return value


def _json_print(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the isolated California PostgreSQL reporting replica"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    profile_parser = subparsers.add_parser(
        "profile", help="Profile reporting contracts against DuckDB without writing"
    )
    profile_parser.add_argument("--duckdb", type=Path, required=True)
    profile_parser.add_argument("--json", action="store_true")

    publish_parser = subparsers.add_parser(
        "publish", help="Build, validate, and atomically publish a reporting snapshot"
    )
    publish_parser.add_argument("--data-root", type=Path, required=True)
    release_group = publish_parser.add_mutually_exclusive_group(required=True)
    release_group.add_argument("--production-root", type=Path)
    release_group.add_argument("--warehouse-release-id")
    publish_parser.add_argument("--postgres-dsn-env", default="CMS_REPORTING_DSN")
    publish_parser.add_argument("--temporary-root", type=Path)
    publish_parser.add_argument("--minimum-free-gb", type=float, default=15.0)
    publish_parser.add_argument("--reader-role", default="tableau_reader")

    rollback_parser = subparsers.add_parser(
        "rollback", help="Transactionally restore a previously published snapshot"
    )
    rollback_parser.add_argument("--snapshot-id", required=True)
    rollback_parser.add_argument("--postgres-dsn-env", default="CMS_REPORTING_DSN")
    rollback_parser.add_argument("--reader-role", default="tableau_reader")

    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.command == "profile":
        profile = profile_database(args.duckdb)
        if args.json:
            _json_print(profile.to_dict())
        else:
            for model in profile.models:
                print(
                    f"{model.layer}.{model.name}: {model.row_count:,} rows, "
                    f"{model.column_count} columns — {model.grain}"
                )
        return 0
    if args.command == "publish":
        release = (
            resolve_production_release(args.production_root)
            if args.production_root
            else resolve_release(args.data_root, args.warehouse_release_id)
        )
        result = publish_release(
            release=release,
            data_root=args.data_root,
            postgres_dsn=_dsn_from_environment(args.postgres_dsn_env),
            temporary_root=args.temporary_root,
            minimum_free_bytes=int(args.minimum_free_gb * 1024**3),
            reader_role=args.reader_role or None,
        )
        _json_print(result.to_dict())
        return 0
    previous = rollback_snapshot(
        postgres_dsn=_dsn_from_environment(args.postgres_dsn_env),
        snapshot_id=args.snapshot_id,
        reader_role=args.reader_role or None,
    )
    _json_print({"restored_snapshot_id": args.snapshot_id, "replaced_snapshot_id": previous})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
