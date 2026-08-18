"""Independent immutable DuckDB releases for utilization discovery.

The utilization artifact is intentionally self-contained.  It copies the
small provider/search dimension and Part B facts from one validated warehouse,
rebuilds the Part D brand/generic grain from that warehouse's sealed raw input,
and never rebuilds unrelated provider evidence marts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

import duckdb


SCHEMA_VERSION = 1
COMPARISON_SCHEMA_VERSION = 1
RELEASE_PATTERN = re.compile(
    r"^utilization-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{10}$"
)
NPI_PATTERN = r"^[0-9]{10}$"
REQUIRED_TABLES = (
    "serving_practice_nppes_provider_sites",
    "utilization_metrics",
    "provider_service_detail",
    "provider_drug_detail",
    "utilization_procedure_dictionary",
    "utilization_drug_dictionary",
)
TAXONOMY_TABLES = (
    "utilization_procedure_taxonomy",
    "utilization_drug_classes",
    "utilization_drug_class_members",
)


class UtilizationReleaseError(RuntimeError):
    """A utilization release invariant was not satisfied."""


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_error(error: BaseException) -> str:
    text = " ".join(str(error).split())
    return (text or error.__class__.__name__)[:500]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _load_json(path: Path, label: str) -> dict:
    if path.is_symlink() or not path.is_file():
        raise UtilizationReleaseError(f"{label} must be a regular file: {path}")
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise UtilizationReleaseError(f"Could not read {label}: {safe_error(error)}") from error
    if not isinstance(value, dict):
        raise UtilizationReleaseError(f"{label} must contain a JSON object")
    return value


def _canonical_file(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise UtilizationReleaseError(f"{label} must be an absolute path")
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise UtilizationReleaseError(f"{label} does not resolve: {path}") from error
    if resolved.is_symlink() or not resolved.is_file():
        raise UtilizationReleaseError(f"{label} must resolve to a regular file")
    return resolved


def _canonical_directory(path: Path, label: str, *, create: bool = False) -> Path:
    if not path.is_absolute() or path == Path("/"):
        raise UtilizationReleaseError(f"{label} must be a specific absolute path")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise UtilizationReleaseError(f"{label} does not resolve: {path}") from error
    if path.is_symlink() or resolved != path or not path.is_dir():
        raise UtilizationReleaseError(f"{label} must be a canonical non-symlink directory")
    return path


def _git_commit() -> str:
    try:
        value = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise UtilizationReleaseError(f"Could not resolve pipeline commit: {safe_error(error)}")
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise UtilizationReleaseError("Pipeline checkout does not resolve to a full Git commit")
    return value


def _source_release(
    source_warehouse: Path, source_release_manifest: Path
) -> tuple[Path, dict]:
    source_warehouse = _canonical_file(source_warehouse, "source warehouse")
    document = _load_json(source_release_manifest, "source warehouse release manifest")
    if document.get("schema_version") != 2 or not isinstance(document.get("release"), dict):
        raise UtilizationReleaseError("Source warehouse release manifest is unsupported")
    release = document["release"]
    release_id = release.get("warehouse_release_id")
    if not isinstance(release_id, str) or not release_id.startswith("warehouse-"):
        raise UtilizationReleaseError("Source warehouse release ID is invalid")
    if release.get("validation_state") != "passed":
        raise UtilizationReleaseError("Source warehouse release validation has not passed")
    expected_sha = str(release.get("sha256", ""))
    expected_size = int(release.get("byte_size", -1))
    if source_warehouse.stat().st_size != expected_size:
        raise UtilizationReleaseError("Source warehouse byte size does not match release evidence")
    if sha256_file(source_warehouse) != expected_sha:
        raise UtilizationReleaseError("Source warehouse SHA-256 does not match release evidence")
    return source_warehouse, release


def _source_utilization_release(
    source_database: Path, source_release_manifest: Path
) -> tuple[Path, dict]:
    source_database = _canonical_file(source_database, "source utilization database")
    document = _load_json(source_release_manifest, "source utilization release manifest")
    release = document.get("release")
    if document.get("schema_version") != SCHEMA_VERSION or not isinstance(release, dict):
        raise UtilizationReleaseError("Source utilization release manifest is unsupported")
    if release.get("validation_state") != "passed":
        raise UtilizationReleaseError("Source utilization release validation has not passed")
    release_id = release.get("utilization_release_id")
    if not isinstance(release_id, str) or not RELEASE_PATTERN.fullmatch(release_id):
        raise UtilizationReleaseError("Source utilization release ID is invalid")
    if source_database.stat().st_size != int(release.get("byte_size", -1)):
        raise UtilizationReleaseError("Source utilization byte size does not match evidence")
    if sha256_file(source_database) != release.get("sha256"):
        raise UtilizationReleaseError("Source utilization SHA-256 does not match evidence")
    return source_database, release


def _taxonomy_reference(manifest_path: Path, source_release: dict) -> tuple[Path, dict]:
    manifest_path = _canonical_file(manifest_path, "taxonomy reference manifest")
    document = _load_json(manifest_path, "taxonomy reference manifest")
    reference = document.get("reference")
    if document.get("schema_version") != 1 or not isinstance(reference, dict):
        raise UtilizationReleaseError("Taxonomy reference manifest is unsupported")
    if reference.get("source_utilization_release_id") != source_release.get(
        "utilization_release_id"
    ):
        raise UtilizationReleaseError("Taxonomy reference targets a different utilization release")
    if reference.get("source_utilization_sha256") != source_release.get("sha256"):
        raise UtilizationReleaseError("Taxonomy reference source SHA-256 does not match")
    files = reference.get("files")
    if not isinstance(files, dict) or set(files) != {"procedures", "classes", "members"}:
        raise UtilizationReleaseError("Taxonomy reference files are incomplete")
    root = manifest_path.parent
    for label, evidence in files.items():
        if not isinstance(evidence, dict):
            raise UtilizationReleaseError(f"Taxonomy {label} file evidence is invalid")
        relative = Path(str(evidence.get("path", "")))
        if relative.is_absolute() or len(relative.parts) != 1:
            raise UtilizationReleaseError(f"Taxonomy {label} file path is not canonical")
        path = _canonical_file(root / relative, f"taxonomy {label} file")
        if path.parent != root:
            raise UtilizationReleaseError(f"Taxonomy {label} file escapes its release directory")
        if path.stat().st_size != int(evidence.get("byte_size", -1)):
            raise UtilizationReleaseError(f"Taxonomy {label} file byte size changed")
        if sha256_file(path) != evidence.get("sha256"):
            raise UtilizationReleaseError(f"Taxonomy {label} file SHA-256 changed")
    return root, reference


def _release_id(commit: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    identity = hashlib.sha256(f"{timestamp}\0{commit}\0utilization-v1".encode()).hexdigest()[:10]
    return f"utilization-{timestamp}-{identity}"


def _columns(connection: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    cursor = connection.execute(f"SELECT * FROM {relation} LIMIT 0")
    return {column[0] for column in cursor.description}


def _require_source_contract(connection: duckdb.DuckDBPyConnection) -> None:
    required = {
        "source.main.serving_practice_nppes_provider_sites": {
            "npi", "address", "city", "state", "zip5", "first_name", "last_name",
            "credentials", "specialties", "latitude", "longitude", "partb_services",
            "partb_payments", "partd_drug_cost", "data_year",
        },
        "source.main.utilization_metrics": {"npi", "metric_year", "rx_total_claims"},
        "source.main.provider_service_detail": {
            "npi", "hcpcs_code", "hcpcs_description", "hcpcs_drug_ind",
            "place_of_service", "tot_beneficiaries", "tot_services",
            "tot_bene_day_srvcs", "avg_submitted_chrg", "avg_medicare_allowed",
            "avg_medicare_payment", "avg_medicare_standardized", "data_year",
        },
        "source.main.raw_part_d_by_provider_and_drug": {
            "Prscrbr_NPI", "Brnd_Name", "Gnrc_Name", "Tot_Clms", "Tot_30day_Fills",
            "Tot_Day_Suply", "Tot_Drug_Cst", "Tot_Benes", "GE65_Tot_Clms",
            "GE65_Tot_Drug_Cst", "GE65_Tot_Benes", "source_run_id",
            "source_data_period",
        },
    }
    for relation, expected in required.items():
        try:
            actual = _columns(connection, relation)
        except duckdb.Error as error:
            raise UtilizationReleaseError(f"Source relation is unavailable: {relation}") from error
        missing = expected - actual
        if missing:
            raise UtilizationReleaseError(
                f"Source relation {relation} is missing columns: {', '.join(sorted(missing))}"
            )


def _configure(
    connection: duckdb.DuckDBPyConnection,
    *,
    memory_limit_gb: int,
    threads: int,
    spill_directory: Path,
) -> None:
    if memory_limit_gb < 1 or threads < 1:
        raise UtilizationReleaseError("Memory limit and thread count must be positive")
    spill_directory.mkdir(parents=True, exist_ok=False)
    connection.execute(f"SET memory_limit='{memory_limit_gb}GB'")
    connection.execute(f"SET threads={threads}")
    connection.execute("SET preserve_insertion_order=false")
    escaped = str(spill_directory).replace("'", "''")
    connection.execute(f"SET temp_directory='{escaped}'")


def _build_tables(
    connection: duckdb.DuckDBPyConnection,
    progress: Callable[[str, str], None] | None = None,
) -> None:
    statements = (
        (
            "provider_dimension",
            """
        CREATE TABLE serving_practice_nppes_provider_sites AS
        SELECT npi, first_name, last_name, credentials, specialties,
               address, city, state, zip5, latitude, longitude,
               partb_services, partb_payments, partd_drug_cost, data_year
        FROM source.main.serving_practice_nppes_provider_sites
        """,
        ),
        (
            "utilization_metrics",
            """
        CREATE TABLE utilization_metrics AS
        SELECT m.npi, m.metric_year, m.rx_total_claims
        FROM source.main.utilization_metrics m
        INNER JOIN serving_practice_nppes_provider_sites p ON p.npi = m.npi
        """,
        ),
        (
            "procedure_facts",
            """
        CREATE TABLE provider_service_detail AS
        SELECT s.npi, s.hcpcs_code, s.hcpcs_description, s.hcpcs_drug_ind,
               s.place_of_service, s.tot_beneficiaries, s.tot_services,
               s.tot_bene_day_srvcs, s.avg_submitted_chrg,
               s.avg_medicare_allowed, s.avg_medicare_payment,
               s.avg_medicare_standardized, s.data_year
        FROM source.main.provider_service_detail s
        INNER JOIN serving_practice_nppes_provider_sites p ON p.npi = s.npi
        """,
        ),
        (
            "drug_facts",
            """
        CREATE TABLE provider_drug_detail AS
        SELECT CAST(d.Prscrbr_NPI AS VARCHAR) npi,
               coalesce(nullif(trim(d.Brnd_Name), ''), trim(d.Gnrc_Name)) brand_name,
               trim(d.Gnrc_Name) generic_name,
               sum(TRY_CAST(d.Tot_Clms AS INTEGER)) tot_claims,
               sum(TRY_CAST(d.Tot_30day_Fills AS DECIMAL(15,2))) tot_30day_fills,
               sum(TRY_CAST(d.Tot_Day_Suply AS INTEGER)) tot_day_supply,
               sum(TRY_CAST(d.Tot_Drug_Cst AS DECIMAL(15,2))) tot_drug_cost,
               sum(TRY_CAST(d.Tot_Benes AS INTEGER)) tot_beneficiaries,
               sum(TRY_CAST(d.GE65_Tot_Clms AS INTEGER)) ge65_tot_claims,
               sum(TRY_CAST(d.GE65_Tot_Drug_Cst AS DECIMAL(15,2))) ge65_tot_drug_cost,
               sum(TRY_CAST(d.GE65_Tot_Benes AS INTEGER)) ge65_tot_benes,
               coalesce(try_cast(left(max(d.source_data_period), 4) AS INTEGER), 2024)
                   data_year
        FROM source.main.raw_part_d_by_provider_and_drug d
        INNER JOIN serving_practice_nppes_provider_sites p
          ON p.npi = CAST(d.Prscrbr_NPI AS VARCHAR)
        WHERE nullif(trim(d.Gnrc_Name), '') IS NOT NULL
        GROUP BY CAST(d.Prscrbr_NPI AS VARCHAR),
                 coalesce(nullif(trim(d.Brnd_Name), ''), trim(d.Gnrc_Name)),
                 trim(d.Gnrc_Name)
        """,
        ),
        (
            "procedure_dictionary",
            """
        CREATE TABLE utilization_procedure_dictionary AS
        SELECT hcpcs_code,
               arg_max(hcpcs_description, coalesce(tot_services, 0)) hcpcs_description,
               arg_max(hcpcs_drug_ind, coalesce(tot_services, 0)) hcpcs_drug_ind,
               count(distinct npi)::INTEGER physician_count,
               coalesce(sum(tot_services), 0)::DOUBLE total_services,
               coalesce(sum(tot_services * avg_medicare_payment), 0)::DOUBLE total_payments,
               data_year
        FROM provider_service_detail
        GROUP BY hcpcs_code, data_year
        """,
        ),
        (
            "drug_dictionary",
            """
        CREATE TABLE utilization_drug_dictionary AS
        SELECT brand_name, generic_name, count(distinct npi)::INTEGER physician_count,
               coalesce(sum(tot_claims), 0)::BIGINT total_claims,
               coalesce(sum(tot_drug_cost), 0)::DOUBLE total_drug_cost, data_year
        FROM provider_drug_detail
        GROUP BY brand_name, generic_name, data_year
        """,
        ),
    )
    for stage, statement in statements:
        if progress:
            progress(stage, "started")
        connection.execute(statement)
        if progress:
            progress(stage, "completed")


def _build_indexes(connection: duckdb.DuckDBPyConnection) -> None:
    statements = (
        "CREATE UNIQUE INDEX idx_utilization_provider_npi "
        "ON serving_practice_nppes_provider_sites(npi)",
        "CREATE INDEX idx_utilization_provider_state "
        "ON serving_practice_nppes_provider_sites(state)",
        "CREATE INDEX idx_utilization_provider_zip "
        "ON serving_practice_nppes_provider_sites(zip5)",
        "CREATE INDEX idx_utilization_metrics_npi ON utilization_metrics(npi)",
        "CREATE INDEX idx_svc_detail_hcpcs ON provider_service_detail(hcpcs_code)",
        "CREATE INDEX idx_svc_detail_npi ON provider_service_detail(npi)",
        "CREATE INDEX idx_drug_detail_generic ON provider_drug_detail(generic_name)",
        "CREATE INDEX idx_drug_detail_brand ON provider_drug_detail(brand_name)",
        "CREATE INDEX idx_drug_detail_npi ON provider_drug_detail(npi)",
        "CREATE INDEX idx_utilization_procedure_code "
        "ON utilization_procedure_dictionary(hcpcs_code)",
        "CREATE INDEX idx_utilization_drug_brand "
        "ON utilization_drug_dictionary(brand_name)",
        "CREATE INDEX idx_utilization_drug_generic "
        "ON utilization_drug_dictionary(generic_name)",
    )
    for statement in statements:
        connection.execute(statement)


def _build_taxonomy_tables(connection: duckdb.DuckDBPyConnection, root: Path) -> None:
    for table in TAXONOMY_TABLES:
        connection.execute(f"DROP TABLE IF EXISTS {table}")
    procedure_path = str(root / "procedure_taxonomy.csv")
    classes_path = str(root / "drug_classes.csv")
    members_path = str(root / "drug_class_members.csv")
    connection.execute(
        """
        CREATE TABLE utilization_procedure_taxonomy AS
        SELECT hcpcs_code, rbcs_id, category_id, category_name,
               subcategory_id, subcategory_name, family_id, family_name,
               major_indicator, hcpcs_add_date, hcpcs_end_date,
               try_cast(rbcs_release_year AS INTEGER) rbcs_release_year
        FROM read_csv(?, header=true, all_varchar=true)
        """,
        [procedure_path],
    )
    connection.execute(
        """
        CREATE TABLE utilization_drug_classes AS
        SELECT source, class_type, class_id, class_name,
               nullif(parent_class_id, '') parent_class_id,
               nullif(parent_class_name, '') parent_class_name,
               try_cast("level" AS INTEGER) hierarchy_level
        FROM read_csv(?, header=true, all_varchar=true)
        """,
        [classes_path],
    )
    connection.execute(
        """
        CREATE TABLE utilization_drug_class_members AS
        SELECT source, class_type, class_id, generic_name, rxcui,
               concept_name, concept_tty, try_cast(match_score AS INTEGER) match_score,
               match_method, source_version
        FROM read_csv(?, header=true, all_varchar=true)
        """,
        [members_path],
    )
    for statement in (
        "CREATE UNIQUE INDEX idx_utilization_rbcs_code "
        "ON utilization_procedure_taxonomy(hcpcs_code)",
        "CREATE INDEX idx_utilization_rbcs_family "
        "ON utilization_procedure_taxonomy(family_id)",
        "CREATE UNIQUE INDEX idx_utilization_drug_class "
        "ON utilization_drug_classes(source, class_id)",
        "CREATE INDEX idx_utilization_drug_class_name "
        "ON utilization_drug_classes(class_name)",
        "CREATE UNIQUE INDEX idx_utilization_drug_class_member "
        "ON utilization_drug_class_members(source, class_id, generic_name)",
        "CREATE INDEX idx_utilization_drug_class_member_generic "
        "ON utilization_drug_class_members(generic_name)",
    ):
        connection.execute(statement)


def _validate_taxonomy(
    connection: duckdb.DuckDBPyConnection, source_release: dict
) -> tuple[dict[str, int], dict]:
    expected_tables = (*REQUIRED_TABLES, *TAXONOMY_TABLES)
    existing = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    missing = sorted(set(expected_tables) - existing)
    if missing:
        raise UtilizationReleaseError(f"Taxonomy candidate is missing tables: {', '.join(missing)}")
    counts = {
        table: _scalar(connection, f"SELECT count(*) FROM {table}")
        for table in expected_tables
    }
    if any(count <= 0 for count in counts.values()):
        raise UtilizationReleaseError("Taxonomy candidate contains an empty required table")
    source_counts = source_release.get("table_counts")
    if not isinstance(source_counts, dict):
        raise UtilizationReleaseError("Source utilization table counts are unavailable")
    changed_base = {
        table: (source_counts.get(table), counts[table])
        for table in REQUIRED_TABLES
        if source_counts.get(table) != counts[table]
    }
    if changed_base:
        raise UtilizationReleaseError(
            f"Taxonomy augmentation changed base table counts: {changed_base}"
        )

    checks = {
        "duplicate_rbcs_codes": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT hcpcs_code FROM utilization_procedure_taxonomy "
            "GROUP BY hcpcs_code HAVING count(*) > 1)",
        ),
        "invalid_rbcs_rows": _scalar(
            connection,
            "SELECT count(*) FROM utilization_procedure_taxonomy "
            "WHERE NOT regexp_matches(hcpcs_code, '^[A-Z0-9]{5}$') "
            "OR nullif(trim(category_name), '') IS NULL "
            "OR nullif(trim(subcategory_name), '') IS NULL "
            "OR nullif(trim(family_name), '') IS NULL",
        ),
        "invalid_drug_classes": _scalar(
            connection,
            "SELECT count(*) FROM utilization_drug_classes "
            "WHERE (source='ATC' AND class_type!='ATC') "
            "OR (source='FDASPL' AND class_type!='EPC') "
            "OR source NOT IN ('ATC', 'FDASPL') "
            "OR nullif(trim(class_id), '') IS NULL OR nullif(trim(class_name), '') IS NULL",
        ),
        "duplicate_drug_members": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT source, class_id, generic_name "
            "FROM utilization_drug_class_members GROUP BY ALL HAVING count(*) > 1)",
        ),
        "orphan_drug_members": _scalar(
            connection,
            "SELECT count(*) FROM utilization_drug_class_members m ANTI JOIN "
            "utilization_drug_classes c ON c.source=m.source AND c.class_id=m.class_id",
        ),
        "invalid_drug_members": _scalar(
            connection,
            "SELECT count(*) FROM utilization_drug_class_members "
            "WHERE match_score NOT IN (95, 100) OR nullif(trim(generic_name), '') IS NULL",
        ),
    }
    failed = {name: value for name, value in checks.items() if value != 0}
    if failed:
        raise UtilizationReleaseError(f"Taxonomy candidate checks failed: {failed}")

    overlap = {
        "procedure_codes": _scalar(
            connection,
            "SELECT count(DISTINCT t.hcpcs_code) FROM utilization_procedure_taxonomy t "
            "JOIN utilization_procedure_dictionary d ON d.hcpcs_code=t.hcpcs_code",
        ),
        "drug_generics": _scalar(
            connection,
            "SELECT count(DISTINCT m.generic_name) FROM utilization_drug_class_members m "
            "JOIN utilization_drug_dictionary d "
            "ON lower(d.generic_name)=lower(m.generic_name)",
        ),
        "procedure_families": _scalar(
            connection,
            "SELECT count(DISTINCT t.family_id) FROM utilization_procedure_taxonomy t "
            "JOIN utilization_procedure_dictionary d ON d.hcpcs_code=t.hcpcs_code",
        ),
        "drug_classes": _scalar(
            connection,
            "SELECT count(DISTINCT concat(m.source, ':', m.class_id)) "
            "FROM utilization_drug_class_members m JOIN utilization_drug_dictionary d "
            "ON lower(d.generic_name)=lower(m.generic_name)",
        ),
    }
    if any(value <= 0 for value in overlap.values()):
        raise UtilizationReleaseError(f"Taxonomy candidate has empty dictionary overlap: {overlap}")
    return counts, {"zero_failure_checks": checks, "dictionary_overlap": overlap}


def _scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(connection.execute(sql).fetchone()[0])


def _decimal_text(value: object) -> str:
    if value is None:
        return "0"
    return format(Decimal(str(value)), "f")


def _validate(connection: duckdb.DuckDBPyConnection) -> tuple[dict[str, int], dict]:
    existing = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    missing = sorted(set(REQUIRED_TABLES) - existing)
    if missing:
        raise UtilizationReleaseError(f"Candidate is missing tables: {', '.join(missing)}")
    stage_tables = sorted(name for name in existing if "build_stage" in name)
    if stage_tables:
        raise UtilizationReleaseError(
            f"Candidate retained staging tables: {', '.join(stage_tables)}"
        )

    counts = {
        table: _scalar(connection, f"SELECT count(*) FROM {table}")
        for table in REQUIRED_TABLES
    }
    if any(count <= 0 for count in counts.values()):
        raise UtilizationReleaseError("Candidate contains an empty required table")

    checks = {
        "invalid_provider_npis": _scalar(
            connection,
            f"SELECT count(*) FROM serving_practice_nppes_provider_sites "
            f"WHERE NOT regexp_matches(npi, '{NPI_PATTERN}')",
        ),
        "duplicate_provider_npis": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT npi FROM serving_practice_nppes_provider_sites "
            "GROUP BY npi HAVING count(*) > 1)",
        ),
        "invalid_provider_geography": _scalar(
            connection,
            "SELECT count(*) FROM serving_practice_nppes_provider_sites "
            "WHERE nullif(trim(city), '') IS NULL OR NOT regexp_matches(state, '^[A-Z]{2}$') "
            "OR NOT regexp_matches(zip5, '^[0-9]{5}$')",
        ),
        "service_duplicate_keys": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT npi, hcpcs_code, place_of_service, data_year "
            "FROM provider_service_detail GROUP BY ALL HAVING count(*) > 1)",
        ),
        "drug_duplicate_keys": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT npi, brand_name, generic_name, data_year "
            "FROM provider_drug_detail GROUP BY ALL HAVING count(*) > 1)",
        ),
        "service_orphan_npis": _scalar(
            connection,
            "SELECT count(*) FROM provider_service_detail s ANTI JOIN "
            "serving_practice_nppes_provider_sites p ON p.npi=s.npi",
        ),
        "drug_orphan_npis": _scalar(
            connection,
            "SELECT count(*) FROM provider_drug_detail d ANTI JOIN "
            "serving_practice_nppes_provider_sites p ON p.npi=d.npi",
        ),
        "invalid_service_codes": _scalar(
            connection,
            "SELECT count(*) FROM provider_service_detail "
            "WHERE nullif(trim(hcpcs_code), '') IS NULL",
        ),
        "invalid_drug_names": _scalar(
            connection,
            "SELECT count(*) FROM provider_drug_detail WHERE nullif(trim(brand_name), '') IS NULL "
            "OR nullif(trim(generic_name), '') IS NULL",
        ),
    }
    failed = {name: value for name, value in checks.items() if value != 0}
    if failed:
        raise UtilizationReleaseError(f"Candidate data checks failed: {failed}")

    source_counts = {
        "provider_dimension": _scalar(
            connection,
            "SELECT count(*) FROM source.main.serving_practice_nppes_provider_sites",
        ),
        "service_rows_eligible": _scalar(
            connection,
            "SELECT count(*) FROM source.main.provider_service_detail s INNER JOIN "
            "serving_practice_nppes_provider_sites p ON p.npi=s.npi",
        ),
        "drug_raw_rows_eligible": _scalar(
            connection,
            "SELECT count(*) FROM source.main.raw_part_d_by_provider_and_drug d INNER JOIN "
            "serving_practice_nppes_provider_sites p "
            "ON p.npi=CAST(d.Prscrbr_NPI AS VARCHAR) "
            "WHERE nullif(trim(d.Gnrc_Name), '') IS NOT NULL",
        ),
    }
    if counts["serving_practice_nppes_provider_sites"] != source_counts["provider_dimension"]:
        raise UtilizationReleaseError("Provider dimension count differs from source warehouse")
    if counts["provider_service_detail"] != source_counts["service_rows_eligible"]:
        raise UtilizationReleaseError("Service fact count differs from eligible source rows")

    source_totals = connection.execute(
        """
        SELECT coalesce(sum(TRY_CAST(d.Tot_Clms AS DECIMAL(38,2))), 0),
               coalesce(sum(TRY_CAST(d.Tot_Drug_Cst AS DECIMAL(38,2))), 0)
        FROM source.main.raw_part_d_by_provider_and_drug d
        INNER JOIN serving_practice_nppes_provider_sites p
          ON p.npi=CAST(d.Prscrbr_NPI AS VARCHAR)
        WHERE nullif(trim(d.Gnrc_Name), '') IS NOT NULL
        """
    ).fetchone()
    candidate_totals = connection.execute(
        "SELECT coalesce(sum(tot_claims),0), coalesce(sum(tot_drug_cost),0) "
        "FROM provider_drug_detail"
    ).fetchone()
    totals = {
        "source_drug_claims": _decimal_text(source_totals[0]),
        "candidate_drug_claims": _decimal_text(candidate_totals[0]),
        "source_drug_cost": _decimal_text(source_totals[1]),
        "candidate_drug_cost": _decimal_text(candidate_totals[1]),
    }
    if Decimal(totals["source_drug_claims"]) != Decimal(
        totals["candidate_drug_claims"]
    ):
        raise UtilizationReleaseError("Drug claim totals do not reconcile to source rows")
    if Decimal(totals["source_drug_cost"]) != Decimal(totals["candidate_drug_cost"]):
        raise UtilizationReleaseError("Drug cost totals do not reconcile to source rows")

    smoke = connection.execute(
        """
        SELECT d.hcpcs_code, p.state, count(distinct s.npi) physicians
        FROM utilization_procedure_dictionary d
        INNER JOIN provider_service_detail s ON s.hcpcs_code=d.hcpcs_code
          AND s.data_year=d.data_year
        INNER JOIN serving_practice_nppes_provider_sites p ON p.npi=s.npi
        WHERE nullif(trim(p.state), '') IS NOT NULL
        GROUP BY d.hcpcs_code, p.state
        ORDER BY physicians DESC, d.hcpcs_code, p.state LIMIT 1
        """
    ).fetchone()
    if smoke is None or int(smoke[2]) <= 0:
        raise UtilizationReleaseError("Procedure search smoke returned no providers")
    drug_smoke = connection.execute(
        """
        SELECT d.brand_name, d.generic_name, p.state, count(distinct f.npi) physicians
        FROM utilization_drug_dictionary d
        INNER JOIN provider_drug_detail f ON f.brand_name=d.brand_name
          AND f.generic_name=d.generic_name AND f.data_year=d.data_year
        INNER JOIN serving_practice_nppes_provider_sites p ON p.npi=f.npi
        WHERE nullif(trim(p.state), '') IS NOT NULL
        GROUP BY d.brand_name, d.generic_name, p.state
        ORDER BY physicians DESC, d.brand_name, d.generic_name, p.state LIMIT 1
        """
    ).fetchone()
    if drug_smoke is None or int(drug_smoke[3]) <= 0:
        raise UtilizationReleaseError("Drug search smoke returned no providers")

    details = {
        "zero_failure_checks": checks,
        "source_counts": source_counts,
        "drug_totals": totals,
        "query_smoke": {
            "procedure": {
                "hcpcs_code": smoke[0], "state": smoke[1], "physicians": int(smoke[2])
            },
            "drug": {
                "brand_name": drug_smoke[0], "generic_name": drug_smoke[1],
                "state": drug_smoke[2], "physicians": int(drug_smoke[3]),
            },
        },
    }
    return counts, details


def build_release(
    *,
    data_root: Path,
    source_warehouse: Path,
    source_release_manifest: Path,
    spill_root: Path,
    memory_limit_gb: int = 16,
    threads: int = 1,
    pipeline_code_commit: str | None = None,
) -> dict:
    """Build, validate, hash, and seal one independent utilization artifact."""
    data_root = _canonical_directory(data_root, "utilization data root", create=True)
    spill_root = _canonical_directory(spill_root, "utilization spill root", create=True)
    source_warehouse, source_release = _source_release(
        source_warehouse, source_release_manifest
    )
    commit = pipeline_code_commit or _git_commit()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise UtilizationReleaseError("Pipeline commit must be a full Git SHA")
    release_id = _release_id(commit)
    if not RELEASE_PATTERN.fullmatch(release_id):
        raise UtilizationReleaseError("Generated utilization release ID is invalid")
    release_dir = data_root / "utilization-releases" / release_id
    release_dir.mkdir(parents=True, exist_ok=False)
    partial = release_dir / "utilization.duckdb.partial"
    final = release_dir / "utilization.duckdb"
    manifest_path = release_dir / "release.json"
    comparison_path = release_dir / "comparison.json"
    spill_directory = spill_root / release_id
    relative_database = f"utilization-releases/{release_id}/utilization.duckdb"
    release = {
        "utilization_release_id": release_id,
        "created_at": utc_now(),
        "database_path": relative_database,
        "source_warehouse_release_id": source_release["warehouse_release_id"],
        "source_warehouse_sha256": source_release["sha256"],
        "source_warehouse_pipeline_commit": source_release.get("pipeline_code_commit"),
        "pipeline_code_commit": commit,
        "duckdb_version": duckdb.__version__,
        "validation_state": "building",
        "validation_timestamp": None,
        "byte_size": None,
        "sha256": None,
        "table_counts": {},
        "validation_details": {
            "resource_limits": {
                "memory_limit_gb": memory_limit_gb,
                "threads": threads,
                "preserve_insertion_order": False,
                "spill_directory": str(spill_directory),
            },
            "build_progress": {"current_stage": "configure", "completed_stages": []},
        },
        "error_summary": None,
    }

    def write_manifest() -> None:
        _write_json(manifest_path, {"schema_version": SCHEMA_VERSION, "release": release})

    def mark(stage: str, status: str) -> None:
        progress = release["validation_details"]["build_progress"]
        if status == "completed":
            progress["completed_stages"].append(stage)
        progress["current_stage"] = stage
        progress["current_stage_status"] = status
        progress["updated_at"] = utc_now()
        write_manifest()

    write_manifest()
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(partial))
        _configure(
            connection,
            memory_limit_gb=memory_limit_gb,
            threads=threads,
            spill_directory=spill_directory,
        )
        escaped_source = str(source_warehouse).replace("'", "''")
        connection.execute(f"ATTACH '{escaped_source}' AS source (READ_ONLY)")
        _require_source_contract(connection)
        mark("configure", "completed")

        _build_tables(connection, mark)

        mark("indexes", "started")
        _build_indexes(connection)
        mark("indexes", "completed")

        mark("validation", "started")
        counts, validation = _validate(connection)
        release["table_counts"] = counts
        release["validation_details"].update(validation)
        mark("validation", "completed")
        connection.execute("CHECKPOINT")
        connection.execute("DETACH source")
        connection.close()
        connection = None

        os.replace(partial, final)
        release["byte_size"] = final.stat().st_size
        release["sha256"] = sha256_file(final)
        release["validation_state"] = "passed"
        release["validation_timestamp"] = utc_now()
        release["validation_details"]["build_progress"]["current_stage"] = "complete"
        release["validation_details"]["build_progress"]["current_stage_status"] = "completed"
        comparison = {
            "schema_version": COMPARISON_SCHEMA_VERSION,
            "utilization_release_id": release_id,
            "source_warehouse_release_id": source_release["warehouse_release_id"],
            "pipeline_code_commit": commit,
            "comparison_policy": "independent_utilization_v1",
            "state": "passed",
            "failed_requirements": [],
            "unexpected_differences": [],
            "evidence_mismatches": [],
            "table_counts": counts,
            "source_counts": validation["source_counts"],
            "drug_totals": validation["drug_totals"],
        }
        write_manifest()
        _write_json(comparison_path, comparison)
        final.chmod(0o440)
        manifest_path.chmod(0o440)
        comparison_path.chmod(0o440)
        return {
            "state": "passed",
            "utilization_release_id": release_id,
            "database_path": str(final),
            "release_manifest": str(manifest_path),
            "comparison": str(comparison_path),
            "byte_size": release["byte_size"],
            "sha256": release["sha256"],
            "table_counts": counts,
        }
    except Exception as error:
        if connection is not None:
            connection.close()
        release["validation_state"] = "failed"
        release["error_summary"] = safe_error(error)
        progress = release["validation_details"]["build_progress"]
        progress["current_stage_status"] = "failed"
        progress["failed_at"] = utc_now()
        write_manifest()
        manifest_path.chmod(0o440)
        raise
    finally:
        if spill_directory.exists():
            shutil.rmtree(spill_directory)


def augment_release(
    *,
    data_root: Path,
    source_utilization: Path,
    source_release_manifest: Path,
    taxonomy_manifest: Path,
    pipeline_code_commit: str | None = None,
) -> dict:
    """Copy one sealed utilization release and add versioned taxonomy references."""
    data_root = _canonical_directory(data_root, "utilization data root", create=True)
    source_database, source_release = _source_utilization_release(
        source_utilization, source_release_manifest
    )
    taxonomy_root, reference = _taxonomy_reference(taxonomy_manifest, source_release)
    commit = pipeline_code_commit or _git_commit()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise UtilizationReleaseError("Pipeline commit must be a full Git SHA")
    release_id = _release_id(commit)
    if not RELEASE_PATTERN.fullmatch(release_id):
        raise UtilizationReleaseError("Generated utilization release ID is invalid")
    release_dir = data_root / "utilization-releases" / release_id
    release_dir.mkdir(parents=True, exist_ok=False)
    partial = release_dir / "utilization.duckdb.partial"
    final = release_dir / "utilization.duckdb"
    manifest_path = release_dir / "release.json"
    comparison_path = release_dir / "comparison.json"
    release = {
        "utilization_release_id": release_id,
        "created_at": utc_now(),
        "database_path": f"utilization-releases/{release_id}/utilization.duckdb",
        "source_warehouse_release_id": source_release.get("source_warehouse_release_id"),
        "source_warehouse_sha256": source_release.get("source_warehouse_sha256"),
        "source_warehouse_pipeline_commit": source_release.get(
            "source_warehouse_pipeline_commit"
        ),
        "source_utilization_release_id": source_release["utilization_release_id"],
        "source_utilization_sha256": source_release["sha256"],
        "taxonomy_reference_id": reference.get("taxonomy_reference_id"),
        "taxonomy_source_versions": {
            "rbcs_release_year": reference.get("rbcs", {}).get("release_year"),
            "rxclass": reference.get("rxclass", {}).get("versions", {}),
        },
        "pipeline_code_commit": commit,
        "duckdb_version": duckdb.__version__,
        "validation_state": "building",
        "validation_timestamp": None,
        "byte_size": None,
        "sha256": None,
        "table_counts": {},
        "validation_details": {
            "build_progress": {"current_stage": "copy", "completed_stages": []}
        },
        "error_summary": None,
    }

    def write_manifest() -> None:
        _write_json(manifest_path, {"schema_version": SCHEMA_VERSION, "release": release})

    def mark(stage: str, status: str) -> None:
        progress = release["validation_details"]["build_progress"]
        if status == "completed":
            progress["completed_stages"].append(stage)
        progress["current_stage"] = stage
        progress["current_stage_status"] = status
        progress["updated_at"] = utc_now()
        write_manifest()

    write_manifest()
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        shutil.copy2(source_database, partial)
        partial.chmod(0o640)
        mark("copy", "completed")
        mark("taxonomy_tables", "started")
        connection = duckdb.connect(str(partial))
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")
        _build_taxonomy_tables(connection, taxonomy_root)
        mark("taxonomy_tables", "completed")
        mark("validation", "started")
        counts, validation = _validate_taxonomy(connection, source_release)
        release["table_counts"] = counts
        release["validation_details"].update(validation)
        mark("validation", "completed")
        connection.execute("CHECKPOINT")
        connection.close()
        connection = None
        os.replace(partial, final)
        release["byte_size"] = final.stat().st_size
        release["sha256"] = sha256_file(final)
        release["validation_state"] = "passed"
        release["validation_timestamp"] = utc_now()
        release["validation_details"]["build_progress"].update(
            {"current_stage": "complete", "current_stage_status": "completed"}
        )
        comparison = {
            "schema_version": COMPARISON_SCHEMA_VERSION,
            "utilization_release_id": release_id,
            "source_warehouse_release_id": release.get("source_warehouse_release_id"),
            "source_utilization_release_id": source_release["utilization_release_id"],
            "taxonomy_reference_id": reference.get("taxonomy_reference_id"),
            "pipeline_code_commit": commit,
            "comparison_policy": "independent_utilization_v1",
            "augmentation_policy": "taxonomy_reference_v1",
            "state": "passed",
            "failed_requirements": [],
            "unexpected_differences": [],
            "evidence_mismatches": [],
            "table_counts": counts,
            "source_table_counts": source_release["table_counts"],
            "dictionary_overlap": validation["dictionary_overlap"],
        }
        write_manifest()
        _write_json(comparison_path, comparison)
        final.chmod(0o440)
        manifest_path.chmod(0o440)
        comparison_path.chmod(0o440)
        return {
            "state": "passed",
            "utilization_release_id": release_id,
            "database_path": str(final),
            "release_manifest": str(manifest_path),
            "comparison": str(comparison_path),
            "byte_size": release["byte_size"],
            "sha256": release["sha256"],
            "table_counts": counts,
        }
    except Exception as error:
        if connection is not None:
            connection.close()
        release["validation_state"] = "failed"
        release["error_summary"] = safe_error(error)
        progress = release["validation_details"]["build_progress"]
        progress["current_stage_status"] = "failed"
        progress["failed_at"] = utc_now()
        write_manifest()
        manifest_path.chmod(0o440)
        raise


def verify_release(data_root: Path, release_id: str) -> dict:
    """Re-open a sealed release and verify its identity and zero-failure evidence."""
    data_root = _canonical_directory(data_root, "utilization data root")
    if not RELEASE_PATTERN.fullmatch(release_id):
        raise UtilizationReleaseError("Utilization release ID is invalid")
    release_dir = data_root / "utilization-releases" / release_id
    document = _load_json(release_dir / "release.json", "utilization release manifest")
    comparison = _load_json(release_dir / "comparison.json", "utilization comparison")
    release = document.get("release")
    if document.get("schema_version") != SCHEMA_VERSION or not isinstance(release, dict):
        raise UtilizationReleaseError("Utilization release manifest is unsupported")
    if release.get("utilization_release_id") != release_id:
        raise UtilizationReleaseError("Utilization release ID does not match its directory")
    if release.get("validation_state") != "passed" or comparison.get("state") != "passed":
        raise UtilizationReleaseError("Utilization release validation has not passed")
    database = release_dir / "utilization.duckdb"
    if database.is_symlink() or not database.is_file():
        raise UtilizationReleaseError("Utilization database is missing")
    if database.stat().st_size != int(release.get("byte_size", -1)):
        raise UtilizationReleaseError("Utilization database byte size changed")
    if sha256_file(database) != release.get("sha256"):
        raise UtilizationReleaseError("Utilization database SHA-256 changed")
    connection = duckdb.connect(str(database), read_only=True)
    try:
        recorded_counts = release.get("table_counts")
        if not isinstance(recorded_counts, dict) or not set(REQUIRED_TABLES).issubset(
            recorded_counts
        ):
            raise UtilizationReleaseError("Utilization table count evidence is incomplete")
        if any(not re.fullmatch(r"[a-z][a-z0-9_]*", table) for table in recorded_counts):
            raise UtilizationReleaseError("Utilization table count evidence has an invalid name")
        counts = {
            table: _scalar(connection, f"SELECT count(*) FROM {table}")
            for table in recorded_counts
        }
    finally:
        connection.close()
    if counts != release.get("table_counts"):
        raise UtilizationReleaseError("Utilization table counts differ from release evidence")
    return {
        "state": "passed",
        "utilization_release_id": release_id,
        "database_path": str(database),
        "sha256": release["sha256"],
        "byte_size": release["byte_size"],
        "table_counts": counts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Independent utilization DuckDB releases")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--data-root", type=Path, required=True)
    build.add_argument("--source-warehouse", type=Path, required=True)
    build.add_argument("--source-release-manifest", type=Path, required=True)
    build.add_argument("--spill-root", type=Path, required=True)
    build.add_argument("--memory-limit-gb", type=int, default=16)
    build.add_argument("--threads", type=int, default=1)
    build.add_argument("--json", action="store_true")
    augment = subparsers.add_parser("augment-taxonomy")
    augment.add_argument("--data-root", type=Path, required=True)
    augment.add_argument("--source-utilization", type=Path, required=True)
    augment.add_argument("--source-release-manifest", type=Path, required=True)
    augment.add_argument("--taxonomy-manifest", type=Path, required=True)
    augment.add_argument("--json", action="store_true")
    verify = subparsers.add_parser("verify")
    verify.add_argument("--data-root", type=Path, required=True)
    verify.add_argument("--utilization-release-id", required=True)
    verify.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "build":
            result = build_release(
                data_root=args.data_root,
                source_warehouse=args.source_warehouse,
                source_release_manifest=args.source_release_manifest,
                spill_root=args.spill_root,
                memory_limit_gb=args.memory_limit_gb,
                threads=args.threads,
            )
        elif args.command == "augment-taxonomy":
            result = augment_release(
                data_root=args.data_root,
                source_utilization=args.source_utilization,
                source_release_manifest=args.source_release_manifest,
                taxonomy_manifest=args.taxonomy_manifest,
            )
        else:
            result = verify_release(args.data_root, args.utilization_release_id)
    except Exception as error:
        payload = {"state": "error", "error_summary": safe_error(error)}
        if getattr(args, "json", False):
            print(json.dumps(payload, sort_keys=True))
        else:
            print(f"Utilization release error: {payload['error_summary']}", file=sys.stderr)
        return 4
    if getattr(args, "json", False):
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Utilization release: {result['utilization_release_id']}")
        print(f"State: {result['state']}")
        print(f"Database: {result['database_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
