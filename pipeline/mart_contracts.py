"""Declared contracts and fail-closed validation for curated warehouse marts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import duckdb


class MartContractError(RuntimeError):
    """One or more materialized marts violated their declared contract."""


@dataclass(frozen=True, slots=True)
class MartSpec:
    """The stable grain, dependencies, provenance, and consumers of one mart."""

    table: str
    transform_ids: tuple[str, ...]
    grain: str
    key_columns: tuple[str, ...]
    upstream_tables: tuple[str, ...]
    source_ids: tuple[str, ...]
    required_columns: tuple[str, ...]
    source_period_policy: str
    provenance_scope: str
    kind: str = "mart"
    npi_parent_table: str | None = "core_providers"
    authorized_routes: tuple[str, ...] = ()
    require_nonempty: bool = True
    row_validations: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.table or not self.transform_ids or not self.key_columns:
            raise ValueError("Mart table, transform IDs, and key columns are required")
        if self.kind not in {"mart", "summary", "serving"}:
            raise ValueError(f"Unsupported mart kind: {self.kind}")
        required = set(self.required_columns)
        missing_keys = set(self.key_columns) - required
        if missing_keys:
            raise ValueError(
                f"Mart {self.table} key columns must be required: "
                + ", ".join(sorted(missing_keys))
            )


MART_CONTRACTS: tuple[MartSpec, ...] = (
    MartSpec(
        table="core_providers",
        transform_ids=("build_core_providers", "enrich_core_providers_from_nppes"),
        grain="one current provider identity row per NPI",
        key_columns=("npi",),
        upstream_tables=("raw_physician_by_provider", "raw_pecos_enrollment", "raw_nppes"),
        source_ids=(
            "cms_physician_by_provider",
            "cms_pecos_public_provider_enrollment",
            "nppes_monthly_v2",
            "nppes_weekly_incremental_v2",
        ),
        required_columns=("npi", "last_org_name", "entity_type_code", "data_year"),
        source_period_policy="release manifest by contributing source; data_year is the CMS year",
        provenance_scope="release_manifest",
        npi_parent_table=None,
        authorized_routes=("/match", "/unified-search", "/practices/capabilities"),
    ),
    MartSpec(
        table="practice_locations",
        transform_ids=("build_practice_locations",),
        grain="one provider-to-group relationship in one warehouse year",
        key_columns=("location_id",),
        upstream_tables=("raw_reassignment", "core_providers"),
        source_ids=("cms_revalidation_group_reassignment",),
        required_columns=("location_id", "npi", "data_year"),
        source_period_policy="release manifest; data_year is the reassignment period year",
        provenance_scope="release_manifest",
        authorized_routes=("/match", "/explorer/provider-evidence"),
    ),
    MartSpec(
        table="serving_practice_provider_sites",
        transform_ids=("build_serving_practice_provider_sites",),
        grain="one normalized DAC site and organization-or-solo key per provider NPI",
        key_columns=("site_key", "npi"),
        upstream_tables=(
            "raw_dac_national",
            "raw_physician_by_provider",
            "raw_part_d_by_provider",
            "address_geocode",
        ),
        source_ids=(
            "cms_dac_national",
            "cms_physician_by_provider",
            "cms_part_d_by_provider",
        ),
        required_columns=(
            "site_key",
            "addr_key",
            "group_key",
            "npi",
            "address",
            "city",
            "state",
            "zip5",
            "specialties",
            "dac_source_data_periods",
            "dac_source_run_ids",
            "partb_source_data_periods",
            "partb_source_run_ids",
            "partd_source_data_periods",
            "partd_source_run_ids",
            "data_year",
        ),
        source_period_policy=(
            "DAC, Part B, and Part D retain row period/run IDs and require "
            "managed release-manifest periods"
        ),
        provenance_scope="row_and_release_manifest",
        kind="serving",
        npi_parent_table=None,
        authorized_routes=("/practices/search",),
        row_validations=(
            (
                "invalid_state_or_zip",
                "NOT regexp_full_match(state, '^[A-Z]{2}$') OR "
                "NOT regexp_full_match(zip5, '^[0-9]{5}$')",
            ),
            ("empty_specialties", "len(specialties) = 0"),
            (
                "missing_dac_provenance",
                "len(dac_source_data_periods) = 0 OR len(dac_source_run_ids) = 0",
            ),
            (
                "partb_value_without_provenance",
                "partb_payments IS NOT NULL AND "
                "(len(partb_source_data_periods) = 0 OR len(partb_source_run_ids) = 0)",
            ),
            (
                "partd_value_without_provenance",
                "partd_drug_cost IS NOT NULL AND "
                "(len(partd_source_data_periods) = 0 OR len(partd_source_run_ids) = 0)",
            ),
            (
                "invalid_group_identity",
                "(org_pac_id IS NULL AND group_key <> 'SOLO') OR "
                "(org_pac_id IS NOT NULL AND group_key <> org_pac_id)",
            ),
        ),
    ),
    MartSpec(
        table="serving_practice_nppes_provider_sites",
        transform_ids=("build_serving_practice_nppes_tables",),
        grain="one deterministic primary NPPES site per Medicare-participating provider NPI",
        key_columns=("npi",),
        upstream_tables=(
            "raw_nppes",
            "raw_physician_by_provider",
            "raw_part_d_by_provider",
            "address_geocode",
        ),
        source_ids=(
            "nppes_monthly_v2",
            "nppes_weekly_incremental_v2",
            "cms_physician_by_provider",
            "cms_part_d_by_provider",
        ),
        required_columns=(
            "npi",
            "addr_key",
            "addr_norm",
            "address",
            "city",
            "state",
            "zip5",
            "specialties",
            "nppes_source_data_period",
            "nppes_source_run_id",
            "partb_source_data_periods",
            "partb_source_run_ids",
            "partd_source_data_periods",
            "partd_source_run_ids",
            "data_year",
        ),
        source_period_policy=(
            "selected NPPES row and Part B/Part D values retain row run/period identity; "
            "all contributing sources require release-manifest periods"
        ),
        provenance_scope="row_and_release_manifest",
        kind="serving",
        npi_parent_table=None,
        authorized_routes=("/practices/search",),
        row_validations=(
            (
                "invalid_state_or_zip",
                "NOT regexp_full_match(state, '^[A-Z]{2}$') OR "
                "NOT regexp_full_match(zip5, '^[0-9]{5}$')",
            ),
            ("empty_specialties", "len(specialties) = 0"),
            (
                "partb_value_without_provenance",
                "partb_payments IS NOT NULL AND "
                "(len(partb_source_data_periods) = 0 OR len(partb_source_run_ids) = 0)",
            ),
            (
                "partd_value_without_provenance",
                "partd_drug_cost IS NOT NULL AND "
                "(len(partd_source_data_periods) = 0 OR len(partd_source_run_ids) = 0)",
            ),
        ),
    ),
    MartSpec(
        table="serving_practice_nppes_org_memberships",
        transform_ids=("build_serving_practice_nppes_tables",),
        grain="one provider NPI by NPPES primary site and CMS organization context",
        key_columns=("addr_key", "npi", "org_pac_id"),
        upstream_tables=(
            "serving_practice_nppes_provider_sites",
            "raw_dac_national",
        ),
        source_ids=(
            "nppes_monthly_v2",
            "nppes_weekly_incremental_v2",
            "cms_dac_national",
        ),
        required_columns=(
            "addr_key",
            "npi",
            "org_pac_id",
            "primary_address_match",
            "dac_source_data_periods",
            "dac_source_run_ids",
            "data_year",
        ),
        source_period_policy=(
            "membership retains DAC row run/period identity and the parent site retains "
            "selected NPPES identity"
        ),
        provenance_scope="row_and_release_manifest",
        kind="serving",
        npi_parent_table="serving_practice_nppes_provider_sites",
        authorized_routes=("/practices/search",),
        row_validations=(
            (
                "missing_dac_provenance",
                "len(dac_source_data_periods) = 0 OR len(dac_source_run_ids) = 0",
            ),
        ),
    ),
    MartSpec(
        table="utilization_metrics",
        transform_ids=("build_utilization_metrics",),
        grain="one provider NPI by metric year",
        key_columns=("npi", "metric_year"),
        upstream_tables=(
            "raw_physician_by_provider",
            "raw_part_d_by_provider",
            "raw_dme_by_referring_provider",
        ),
        source_ids=(
            "cms_physician_by_provider",
            "cms_part_d_by_provider",
            "cms_dme_by_referring_provider",
        ),
        required_columns=("npi", "metric_year"),
        source_period_policy="release manifest by source; metric_year is the CMS measurement year",
        provenance_scope="release_manifest",
        authorized_routes=("/match", "/unified-search"),
    ),
    MartSpec(
        table="industry_relationships",
        transform_ids=("build_industry_relationships",),
        grain="one provider NPI by program year and paying company",
        key_columns=("npi", "payment_year", "paying_company_name"),
        upstream_tables=("raw_open_payments_general",),
        source_ids=("open_payments_general",),
        required_columns=(
            "npi",
            "payment_year",
            "paying_company_name",
            "total_amount_received",
        ),
        source_period_policy="release manifest; payment_year is the Open Payments program year",
        provenance_scope="release_manifest",
    ),
    MartSpec(
        table="hospital_affiliations",
        transform_ids=("build_hospital_affiliations",),
        grain="one conservatively inferred provider-to-hospital relationship",
        key_columns=("npi", "hospital_npi"),
        upstream_tables=("raw_reassignment", "raw_hospital_enrollments", "core_providers"),
        source_ids=(
            "cms_revalidation_group_reassignment",
            "cms_hospital_enrollments",
        ),
        required_columns=(
            "npi",
            "hospital_npi",
            "affiliation_source",
            "data_year",
        ),
        source_period_policy="release manifest by source; data_year is the hospital period year",
        provenance_scope="release_manifest",
        authorized_routes=("/profiles/hospital-affiliations", "/explorer/provider-evidence"),
    ),
    MartSpec(
        table="provider_service_detail",
        transform_ids=("build_provider_service_detail",),
        grain="one provider NPI by HCPCS, place of service, and data year",
        key_columns=("npi", "hcpcs_code", "place_of_service", "data_year"),
        upstream_tables=("raw_physician_by_provider_and_service", "core_providers"),
        source_ids=("cms_physician_by_provider_and_service",),
        required_columns=("npi", "hcpcs_code", "place_of_service", "data_year"),
        source_period_policy="release manifest; data_year is the CMS measurement year",
        provenance_scope="release_manifest",
    ),
    MartSpec(
        table="provider_drug_detail",
        transform_ids=("build_provider_drug_detail",),
        grain="one provider NPI by generic drug and data year",
        key_columns=("npi", "generic_name", "data_year"),
        upstream_tables=("raw_part_d_by_provider_and_drug", "core_providers"),
        source_ids=("cms_part_d_by_provider_and_drug",),
        required_columns=("npi", "generic_name", "data_year"),
        source_period_policy="release manifest; data_year is the Part D measurement year",
        provenance_scope="release_manifest",
    ),
    MartSpec(
        table="provider_quality_scores",
        transform_ids=("build_provider_quality_scores",),
        grain="one selected quality record per provider NPI",
        key_columns=("npi",),
        upstream_tables=("raw_qpp_experience", "core_providers"),
        source_ids=("cms_qpp_experience",),
        required_columns=("npi", "data_year"),
        source_period_policy="release manifest; data_year is the QPP performance year",
        provenance_scope="release_manifest",
    ),
    MartSpec(
        table="order_referring_eligibility",
        transform_ids=("build_order_referring_eligibility",),
        grain="one current order-and-referring eligibility row per provider NPI",
        key_columns=("npi",),
        upstream_tables=("raw_order_and_referring", "core_providers"),
        source_ids=("cms_order_and_referring",),
        required_columns=("npi",),
        source_period_policy=(
            "release manifest; publisher interval is a snapshot, not an ingestion date"
        ),
        provenance_scope="release_manifest",
    ),
    MartSpec(
        table="kol_summary",
        transform_ids=("build_kol_summary",),
        grain="one provider NPI above the declared all-year payment threshold",
        key_columns=("npi",),
        upstream_tables=("industry_relationships", "core_providers"),
        source_ids=("open_payments_general",),
        required_columns=(
            "npi",
            "unique_companies",
            "total_payments_all_years",
            "total_payment_count",
            "most_recent_year",
            "kol_tier",
        ),
        source_period_policy="release manifest; most_recent_year is a derived program-year maximum",
        provenance_scope="release_manifest",
        kind="summary",
    ),
    MartSpec(
        table="nppes_radar_provider_state",
        transform_ids=("process_nppes_radar",),
        grain="one current reconciled NPPES state row per provider NPI",
        key_columns=("npi",),
        upstream_tables=("raw_nppes",),
        source_ids=("nppes_monthly_v2", "nppes_weekly_incremental_v2"),
        required_columns=(
            "npi",
            "record_fingerprint",
            "source_release_id",
            "source_data_period",
            "first_seen_at",
            "last_seen_at",
        ),
        source_period_policy="row source_data_period plus release manifest",
        provenance_scope="row_and_release_manifest",
        npi_parent_table=None,
        authorized_routes=("/radar/providers",),
    ),
    MartSpec(
        table="nppes_radar_events",
        transform_ids=("process_nppes_radar",),
        grain="one immutable NPPES change event",
        key_columns=("event_id",),
        upstream_tables=("nppes_radar_provider_state", "raw_nppes"),
        source_ids=("nppes_monthly_v2", "nppes_weekly_incremental_v2"),
        required_columns=(
            "event_id",
            "npi",
            "event_type",
            "effective_date",
            "detected_at",
            "source_release_id",
            "source_data_period",
        ),
        source_period_policy="row source_data_period plus release manifest",
        provenance_scope="row_and_release_manifest",
        kind="summary",
        npi_parent_table="nppes_radar_provider_state",
        authorized_routes=("/radar/providers",),
    ),
)

MART_CONTRACT_BY_TABLE = {spec.table: spec for spec in MART_CONTRACTS}

if len(MART_CONTRACT_BY_TABLE) != len(MART_CONTRACTS):
    raise ValueError("Mart contract table names must be unique")


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _available_columns(
    connection: duckdb.DuckDBPyConnection, table: str
) -> set[str] | None:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table],
    ).fetchall()
    return {str(row[0]) for row in rows} if rows else None


def inspect_mart_contracts(
    connection: duckdb.DuckDBPyConnection,
    contracts: Iterable[MartSpec] = MART_CONTRACTS,
) -> dict[str, object]:
    """Return cheap schema-only mart readiness without claiming row validation."""

    reports: list[dict[str, object]] = []
    for spec in contracts:
        columns = _available_columns(connection, spec.table)
        missing = sorted(set(spec.required_columns) - (columns or set()))
        reports.append(
            {
                "table": spec.table,
                "kind": spec.kind,
                "grain": spec.grain,
                "available": columns is not None,
                "schema_valid": columns is not None and not missing,
                "missing_required_columns": missing,
                "serving_authorized": bool(spec.authorized_routes),
                "authorized_routes": list(spec.authorized_routes),
            }
        )
    return _summary(reports, validation_scope="schema_only")


def validate_mart_contracts(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_periods: dict[str, str],
    contracts: Iterable[MartSpec] = MART_CONTRACTS,
    raise_on_error: bool = True,
) -> dict[str, object]:
    """Validate physical mart data and provenance during an offline release build."""

    reports: list[dict[str, object]] = []
    for spec in contracts:
        columns = _available_columns(connection, spec.table)
        missing = sorted(set(spec.required_columns) - (columns or set()))
        issues: list[str] = []
        row_count: int | None = None
        duplicate_keys: int | None = None
        required_null_rows: int | None = None
        invalid_npis: int | None = None
        orphan_npis: int | None = None
        row_validation_failures: dict[str, int] = {}

        if columns is None:
            issues.append("table_missing")
        elif missing:
            issues.append("required_columns_missing:" + ",".join(missing))
        else:
            table = _quote(spec.table)
            row_count = int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            if spec.require_nonempty and row_count == 0:
                issues.append("table_empty")

            keys = ", ".join(_quote(column) for column in spec.key_columns)
            duplicate_keys = int(
                connection.execute(
                    f"""
                    SELECT count(*) FROM (
                        SELECT {keys}
                        FROM {table}
                        GROUP BY {keys}
                        HAVING count(*) > 1
                    ) duplicate_groups
                    """
                ).fetchone()[0]
            )
            if duplicate_keys:
                issues.append(f"duplicate_key_groups:{duplicate_keys}")

            null_predicate = " OR ".join(
                f"{_quote(column)} IS NULL" for column in spec.required_columns
            )
            required_null_rows = int(
                connection.execute(
                    f"SELECT count(*) FROM {table} WHERE {null_predicate}"
                ).fetchone()[0]
            )
            if required_null_rows:
                issues.append(f"required_null_rows:{required_null_rows}")

            if "npi" in columns:
                invalid_npis = int(
                    connection.execute(
                        f"""
                        SELECT count(*) FROM {table}
                        WHERE npi IS NULL
                           OR NOT regexp_full_match(CAST(npi AS VARCHAR), '^[0-9]{{10}}$')
                        """
                    ).fetchone()[0]
                )
                if invalid_npis:
                    issues.append(f"invalid_npis:{invalid_npis}")

            if spec.npi_parent_table is not None and "npi" in columns:
                parent = _quote(spec.npi_parent_table)
                orphan_npis = int(
                    connection.execute(
                        f"""
                        SELECT count(*)
                        FROM {table} child
                        LEFT JOIN {parent} parent
                          ON CAST(parent.npi AS VARCHAR) = CAST(child.npi AS VARCHAR)
                        WHERE parent.npi IS NULL
                        """
                    ).fetchone()[0]
                )
                if orphan_npis:
                    issues.append(f"orphan_npis:{orphan_npis}")

            for label, predicate in spec.row_validations:
                failure_count = int(
                    connection.execute(
                        f"SELECT count(*) FROM {table} WHERE {predicate}"
                    ).fetchone()[0]
                )
                row_validation_failures[label] = failure_count
                if failure_count:
                    issues.append(f"{label}:{failure_count}")

        missing_source_periods = sorted(
            source_id
            for source_id in spec.source_ids
            if not isinstance(source_periods.get(source_id), str)
            or not source_periods[source_id].strip()
        )
        if missing_source_periods:
            issues.append(
                "source_periods_missing:" + ",".join(missing_source_periods)
            )

        reports.append(
            {
                "table": spec.table,
                "kind": spec.kind,
                "grain": spec.grain,
                "key_columns": list(spec.key_columns),
                "source_ids": list(spec.source_ids),
                "source_period_policy": spec.source_period_policy,
                "provenance_scope": spec.provenance_scope,
                "available": columns is not None,
                "schema_valid": columns is not None and not missing,
                "data_valid": not issues,
                "row_count": row_count,
                "duplicate_key_groups": duplicate_keys,
                "required_null_rows": required_null_rows,
                "invalid_npis": invalid_npis,
                "orphan_npis": orphan_npis,
                "row_validation_failures": row_validation_failures,
                "missing_required_columns": missing,
                "missing_source_periods": missing_source_periods,
                "serving_authorized": bool(spec.authorized_routes),
                "authorized_routes": list(spec.authorized_routes),
                "issues": issues,
            }
        )

    result = _summary(reports, validation_scope="row_and_schema")
    if raise_on_error and not result["passed"]:
        failed = [
            f"{report['table']} ({'; '.join(report.get('issues', []))})"
            for report in reports
            if not report.get("data_valid")
        ]
        raise MartContractError("Mart contract validation failed: " + ", ".join(failed))
    return result


def _summary(
    reports: list[dict[str, object]], *, validation_scope: str
) -> dict[str, object]:
    registered = len(reports)
    available = sum(report.get("available") is True for report in reports)
    schema_valid = sum(report.get("schema_valid") is True for report in reports)
    data_valid_reports = [report for report in reports if "data_valid" in report]
    data_valid = sum(report.get("data_valid") is True for report in data_valid_reports)
    readiness_field = "data_valid" if data_valid_reports else "schema_valid"
    return {
        "schema_version": 1,
        "validation_scope": validation_scope,
        "registered_count": registered,
        "available_count": available,
        "schema_valid_count": schema_valid,
        "data_valid_count": data_valid if data_valid_reports else None,
        "serving_authorized_count": sum(
            report.get("serving_authorized") is True
            and report.get(readiness_field) is True
            for report in reports
        ),
        "passed": (
            data_valid == registered
            if data_valid_reports
            else schema_valid == registered
        ),
        "marts": reports,
    }
