"""Verified loading for publisher ZIP sources into an isolated DuckDB candidate.

Every input is rechecked against its immutable acquisition manifest before a
single named member is streamed to short-lived staging storage.  The loaders
never extract an archive tree and never open the selected production warehouse.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import tempfile
import zipfile
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Iterator

import duckdb

from .archive_acquisition import ARCHIVE_PROFILES, inspect_archive
from .manifests import ManifestStore, RunManifest, ValidationState
from .nppes import enrich_core_providers, map_taxonomy_to_specialty
from .nppes_radar import NppesRadarRelease, process_nppes_provider_file
from .releases import ReleaseError


NPPES_SOURCE_IDS = frozenset(
    {"nppes_monthly_v2", "nppes_weekly_incremental_v2"}
)
OPEN_PAYMENTS_TABLES = {
    "open_payments_general": "raw_open_payments_general",
    "open_payments_research": "raw_open_payments_research",
    "open_payments_ownership": "raw_open_payments_ownership",
}
ARCHIVE_WAREHOUSE_SOURCE_IDS = NPPES_SOURCE_IDS | frozenset(OPEN_PAYMENTS_TABLES)

NPPES_MEMBER_PATTERN = r"(^|/)npidata_pfile_\d{8}-\d{8}\.csv$"
OPEN_PAYMENTS_MEMBER_PATTERNS = {
    "open_payments_general": r"(^|/).*GNRL.*\.csv$",
    "open_payments_research": r"(^|/).*RSRCH.*\.csv$",
    "open_payments_ownership": r"(^|/).*(OWNRSHP|OWNERSHIP).*\.csv$",
}

NPPES_COLUMNS: tuple[tuple[str, str], ...] = (
    ("NPI", "npi"),
    ("Entity Type Code", "entity_type"),
    ("Provider First Name", "first_name"),
    ("Provider Last Name (Legal Name)", "last_name"),
    ("Provider Middle Name", "middle_name"),
    ("Provider Name Prefix Text", "name_prefix"),
    ("Provider Name Suffix Text", "name_suffix"),
    ("Provider Credential Text", "credentials"),
    ("Provider Gender Code", "gender"),
    ("Provider Enumeration Date", "enumeration_date"),
    ("NPI Deactivation Date", "deactivation_date"),
    ("NPI Reactivation Date", "reactivation_date"),
    ("Is Sole Proprietor", "sole_proprietor"),
    (
        "Provider First Line Business Practice Location Address",
        "practice_address_1",
    ),
    (
        "Provider Second Line Business Practice Location Address",
        "practice_address_2",
    ),
    ("Provider Business Practice Location Address City Name", "practice_city"),
    ("Provider Business Practice Location Address State Name", "practice_state"),
    ("Provider Business Practice Location Address Postal Code", "practice_zip"),
    (
        "Provider Business Practice Location Address Country Code (If outside U.S.)",
        "practice_country",
    ),
    (
        "Provider Business Practice Location Address Telephone Number",
        "practice_phone",
    ),
    ("Healthcare Provider Taxonomy Code_1", "taxonomy_1"),
    ("Healthcare Provider Taxonomy Code_2", "taxonomy_2"),
    ("Healthcare Provider Taxonomy Code_3", "taxonomy_3"),
    ("Healthcare Provider Primary Taxonomy Switch_1", "taxonomy_primary_1"),
    ("Healthcare Provider Primary Taxonomy Switch_2", "taxonomy_primary_2"),
    ("Healthcare Provider Primary Taxonomy Switch_3", "taxonomy_primary_3"),
)
NPPES_GENDER_COLUMNS = ("Provider Sex Code", "Provider Gender Code")

OPEN_PAYMENTS_REQUIRED_COLUMNS = {
    "open_payments_general": frozenset(
        {
            "Covered_Recipient_NPI",
            "Covered_Recipient_Profile_ID",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
            "Total_Amount_of_Payment_USDollars",
            "Nature_of_Payment_or_Transfer_of_Value",
            "Program_Year",
            "Record_ID",
        }
    ),
    "open_payments_research": frozenset(
        {
            "Covered_Recipient_NPI",
            "Total_Amount_of_Payment_USDollars",
            "Program_Year",
            "Record_ID",
        }
    ),
    "open_payments_ownership": frozenset(
        {
            "Physician_NPI",
            "Total_Amount_Invested_USDollars",
            "Value_of_Interest",
            "Program_Year",
            "Record_ID",
        }
    ),
}

OPEN_PAYMENTS_NUMERIC_COLUMNS = {
    "open_payments_general": ("Total_Amount_of_Payment_USDollars",),
    "open_payments_research": ("Total_Amount_of_Payment_USDollars",),
    "open_payments_ownership": (
        "Total_Amount_Invested_USDollars",
        "Value_of_Interest",
    ),
}


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _manifest_map(data_root: Path) -> dict[str, RunManifest]:
    result: dict[str, RunManifest] = {}
    for manifest in ManifestStore(data_root / "manifests.json").load().manifests:
        if manifest.run_id in result:
            raise ReleaseError(f"Duplicate source manifest run ID: {manifest.run_id}")
        result[manifest.run_id] = manifest
    return result


def verified_archive_runs(
    data_root: Path,
    run_ids: tuple[str, ...],
    *,
    allowed_sources: frozenset[str],
) -> tuple[tuple[RunManifest, Path], ...]:
    """Resolve and fully revalidate immutable ZIP runs."""
    if not run_ids or len(run_ids) != len(set(run_ids)):
        raise ReleaseError("Archive source run IDs must be non-empty and unique")
    available = _manifest_map(data_root)
    verified: list[tuple[RunManifest, Path]] = []
    seen_sources: set[str] = set()
    for run_id in run_ids:
        manifest = available.get(run_id)
        if manifest is None:
            raise ReleaseError(f"Source manifest is missing for run {run_id}")
        if manifest.source_id not in allowed_sources:
            raise ReleaseError(f"Archive loader does not support source {manifest.source_id}")
        if manifest.source_id in seen_sources:
            raise ReleaseError(
                f"Candidate contains more than one run for source {manifest.source_id}"
            )
        seen_sources.add(manifest.source_id)
        if manifest.validation_state != ValidationState.PASSED:
            raise ReleaseError(f"Source run {run_id} has not passed validation")
        if (
            not manifest.sha256
            or manifest.source_encoding != "binary:zip"
            or not manifest.retrieval_timestamp
        ):
            raise ReleaseError(
                f"Source run {run_id} lacks archive checksum or retrieval provenance"
            )
        artifact = data_root / "runs" / manifest.source_id / run_id / "source.zip"
        if not artifact.is_file() or artifact.is_symlink():
            raise ReleaseError(f"Archive artifact is missing or unsafe: {artifact}")
        inspection = inspect_archive(artifact, ARCHIVE_PROFILES[manifest.source_id])
        if (
            inspection.sha256 != manifest.sha256
            or inspection.byte_size != manifest.byte_size
            or inspection.schema_fingerprint != manifest.schema_fingerprint
            or inspection.member_count != manifest.row_counts.get("archive_members")
            or inspection.uncompressed_bytes
            != manifest.row_counts.get("uncompressed_bytes")
        ):
            raise ReleaseError(
                f"Archive artifact no longer matches acquisition manifest for run {run_id}"
            )
        verified.append((manifest, artifact))
    return tuple(sorted(verified, key=lambda item: item[0].source_id))


@contextmanager
def extracted_member(
    data_root: Path,
    manifest: RunManifest,
    archive_path: Path,
    pattern: str,
    *,
    suffix: str,
) -> Iterator[Path]:
    """Stream exactly one safe archive member to a short-lived regular file."""
    staging = data_root / "staging" / "extracts"
    staging.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with zipfile.ZipFile(archive_path) as archive:
            matches = [
                member
                for member in archive.infolist()
                if not member.is_dir() and re.search(pattern, member.filename, re.I)
            ]
            if len(matches) != 1:
                raise ReleaseError(
                    f"Expected one archive member for {manifest.source_id}; found {len(matches)}"
                )
            member = matches[0]
            if member.flag_bits & 0x1:
                raise ReleaseError(f"Archive member is encrypted: {member.filename}")
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f"{manifest.run_id}-",
                suffix=f"{suffix}.partial",
                dir=staging,
                delete=False,
            ) as target:
                temporary = Path(target.name)
                with archive.open(member) as source:
                    shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            if temporary.stat().st_size != member.file_size:
                raise ReleaseError(
                    f"Extracted member size changed for {manifest.source_id}"
                )
        final = temporary.with_suffix("")
        os.replace(temporary, final)
        temporary = final
        yield final
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        if isinstance(error, ReleaseError):
            raise
        raise ReleaseError(f"Could not extract {manifest.source_id}: {error}") from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _period(value: str) -> tuple[date, date]:
    parts = value.split("/", 1)
    try:
        start = date.fromisoformat(parts[0])
        end = date.fromisoformat(parts[-1])
    except ValueError as error:
        raise ReleaseError(f"Invalid source data period: {value}") from error
    if start > end:
        raise ReleaseError(f"Invalid source data period: {value}")
    return start, end


def _load_nppes_raw_file(
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
    manifest: RunManifest,
    *,
    baseline: bool,
) -> int:
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as source:
            header = next(csv.reader(source))
    except (OSError, UnicodeError, StopIteration, csv.Error) as error:
        raise ReleaseError(f"NPPES header is unreadable: {error}") from error
    if len(header) != len(set(header)):
        raise ReleaseError("NPPES header contains duplicate column names")
    header_set = set(header)
    gender_matches = [name for name in NPPES_GENDER_COLUMNS if name in header_set]
    if len(gender_matches) != 1:
        raise ReleaseError(
            "NPPES must contain exactly one recognized sex/gender column; found "
            f"{len(gender_matches)}"
        )
    selected_columns = tuple(
        (gender_matches[0] if target == "gender" else source, target)
        for source, target in NPPES_COLUMNS
    )
    missing = sorted(source for source, _ in selected_columns if source not in header_set)
    if missing:
        raise ReleaseError("NPPES is missing required columns: " + ", ".join(missing))
    source_columns = ",\n".join(
        f"NULLIF(TRIM({_quote(source)}), '') AS {_quote(target)}"
        for source, target in selected_columns
    )
    connection.execute("DROP TABLE IF EXISTS _raw_nppes_incoming")
    connection.execute(
        f"""
        CREATE TEMP TABLE _raw_nppes_incoming AS
        SELECT
            {source_columns},
            ?::VARCHAR AS source_run_id,
            ?::VARCHAR AS source_release_id,
            ?::VARCHAR AS source_data_period,
            ?::TIMESTAMPTZ AS ingested_at
        FROM read_csv(
            ?, header = true, all_varchar = true, strict_mode = true,
            ignore_errors = false, encoding = 'utf-8'
        )
        """,
        [
            manifest.run_id,
            manifest.release_id,
            manifest.source_data_period,
            manifest.retrieval_timestamp,
            str(csv_path),
        ],
    )
    invalid = connection.execute(
        """
        SELECT npi FROM _raw_nppes_incoming
        WHERE npi IS NULL OR NOT regexp_matches(npi, '^[0-9]{10}$')
        LIMIT 1
        """
    ).fetchone()
    if invalid is not None:
        raise ReleaseError(f"NPPES contains an invalid NPI: {invalid[0]!r}")
    duplicate = connection.execute(
        """
        SELECT npi FROM _raw_nppes_incoming
        GROUP BY npi HAVING count(*) > 1 LIMIT 1
        """
    ).fetchone()
    if duplicate is not None:
        raise ReleaseError(f"NPPES contains duplicate NPI {duplicate[0]}")

    if baseline:
        connection.execute("DROP TABLE IF EXISTS raw_nppes")
        connection.execute(
            """
            CREATE TABLE raw_nppes AS
            SELECT * FROM _raw_nppes_incoming WHERE entity_type = '1'
            """
        )
    else:
        connection.execute(
            "DELETE FROM raw_nppes WHERE npi IN (SELECT npi FROM _raw_nppes_incoming)"
        )
        connection.execute(
            "INSERT INTO raw_nppes SELECT * FROM _raw_nppes_incoming WHERE entity_type = '1'"
        )
    connection.execute("DROP TABLE _raw_nppes_incoming")
    return int(connection.execute("SELECT count(*) FROM raw_nppes").fetchone()[0])


def load_nppes_sources(
    connection: duckdb.DuckDBPyConnection,
    *,
    data_root: Path,
    monthly_run_id: str,
    weekly_run_id: str,
) -> tuple[dict[str, int], dict[str, object]]:
    """Install a monthly NPPES baseline and overlay one verified weekly release."""
    verified = verified_archive_runs(
        data_root,
        (monthly_run_id, weekly_run_id),
        allowed_sources=NPPES_SOURCE_IDS,
    )
    by_source = {manifest.source_id: (manifest, path) for manifest, path in verified}
    if set(by_source) != NPPES_SOURCE_IDS:
        raise ReleaseError("NPPES load requires one monthly and one weekly V2 run")

    monthly_manifest, monthly_archive = by_source["nppes_monthly_v2"]
    weekly_manifest, weekly_archive = by_source["nppes_weekly_incremental_v2"]
    with ExitStack() as stack:
        monthly_csv = stack.enter_context(
            extracted_member(
                data_root,
                monthly_manifest,
                monthly_archive,
                NPPES_MEMBER_PATTERN,
                suffix=".csv",
            )
        )
        weekly_csv = stack.enter_context(
            extracted_member(
                data_root,
                weekly_manifest,
                weekly_archive,
                NPPES_MEMBER_PATTERN,
                suffix=".csv",
            )
        )
        connection.execute("BEGIN TRANSACTION")
        try:
            _load_nppes_raw_file(
                connection, monthly_csv, monthly_manifest, baseline=True
            )
            raw_count = _load_nppes_raw_file(
                connection, weekly_csv, weekly_manifest, baseline=False
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

        # Radar owns its own transactions. A full candidate always rebuilds it
        # from the same monthly baseline before applying weekly changes.
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute("DELETE FROM nppes_radar_events")
            connection.execute("DELETE FROM nppes_radar_releases")
            connection.execute("DELETE FROM nppes_radar_provider_state")
            connection.execute("COMMIT")
        except duckdb.CatalogException:
            connection.execute("ROLLBACK")

        month_start, month_end = _period(monthly_manifest.source_data_period)
        week_start, week_end = _period(weekly_manifest.source_data_period)
        monthly_result = process_nppes_provider_file(
            connection,
            monthly_csv,
            NppesRadarRelease(
                source_release_id=monthly_manifest.release_id,
                source_id=monthly_manifest.source_id,
                release_kind="monthly_full",
                period_start=month_start,
                period_end=month_end,
            ),
            baseline=True,
        )
        weekly_result = process_nppes_provider_file(
            connection,
            weekly_csv,
            NppesRadarRelease(
                source_release_id=weekly_manifest.release_id,
                source_id=weekly_manifest.source_id,
                release_kind="weekly_incremental",
                period_start=week_start,
                period_end=week_end,
            ),
        )

    enrichment = enrich_core_providers(connection)
    specialty_updates = map_taxonomy_to_specialty(connection)
    counts = {
        "raw_nppes": raw_count,
        "nppes_radar_provider_state": int(
            connection.execute("SELECT count(*) FROM nppes_radar_provider_state").fetchone()[0]
        ),
        "nppes_radar_events": int(
            connection.execute("SELECT count(*) FROM nppes_radar_events").fetchone()[0]
        ),
        "nppes_radar_releases": int(
            connection.execute("SELECT count(*) FROM nppes_radar_releases").fetchone()[0]
        ),
        "core_providers": int(
            connection.execute("SELECT count(*) FROM core_providers").fetchone()[0]
        ),
    }
    details = {
        "monthly": asdict(monthly_result),
        "weekly": asdict(weekly_result),
        "core_provider_enrichment": enrichment,
        "taxonomy_specialty_updates": specialty_updates,
    }
    return counts, details


def _table_columns(
    connection: duckdb.DuckDBPyConnection, table: str
) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(
            f"PRAGMA table_info({_quote(table)})"
        ).fetchall()
    }


def _load_open_payments_table(
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
    manifest: RunManifest,
) -> int:
    table = OPEN_PAYMENTS_TABLES[manifest.source_id]
    incoming = f"_{table}_incoming"
    candidate = f"_{table}_candidate"
    connection.execute(f"DROP TABLE IF EXISTS {_quote(incoming)}")
    connection.execute(f"DROP TABLE IF EXISTS {_quote(candidate)}")
    connection.execute(
        f"""
        CREATE TEMP TABLE {_quote(incoming)} AS
        SELECT * FROM read_csv(
            ?, header = true, all_varchar = true, strict_mode = true,
            ignore_errors = false, encoding = 'utf-8'
        )
        """,
        [str(csv_path)],
    )
    columns = _table_columns(connection, incoming)
    missing = sorted(OPEN_PAYMENTS_REQUIRED_COLUMNS[manifest.source_id] - columns)
    if missing:
        raise ReleaseError(
            f"{manifest.source_id} is missing required columns: {', '.join(missing)}"
        )
    collisions = {
        "source_run_id",
        "source_release_id",
        "source_data_period",
        "ingested_at",
    } & columns
    if collisions:
        raise ReleaseError(
            f"{manifest.source_id} publisher columns collide with provenance fields"
        )
    row_count = int(
        connection.execute(f"SELECT count(*) FROM {_quote(incoming)}").fetchone()[0]
    )
    if row_count <= 0:
        raise ReleaseError(f"{manifest.source_id} contains no data rows")

    numeric = OPEN_PAYMENTS_NUMERIC_COLUMNS[manifest.source_id]
    for column in numeric:
        invalid = int(
            connection.execute(
                f"""
                SELECT count(*) FROM {_quote(incoming)}
                WHERE nullif(trim({_quote(column)}), '') IS NOT NULL
                  AND try_cast({_quote(column)} AS DECIMAL(18,2)) IS NULL
                """
            ).fetchone()[0]
        )
        if invalid:
            raise ReleaseError(
                f"{manifest.source_id} has {invalid} invalid values in {column}"
            )
    invalid_years = int(
        connection.execute(
            f"""
            SELECT count(*) FROM {_quote(incoming)}
            WHERE try_cast({_quote('Program_Year')} AS INTEGER) IS NULL
               OR try_cast({_quote('Program_Year')} AS INTEGER) <> ?
            """,
            [int(manifest.source_data_period[:4])],
        ).fetchone()[0]
    )
    if invalid_years:
        raise ReleaseError(
            f"{manifest.source_id} has {invalid_years} rows outside its source period"
        )

    replacements = [
        f"try_cast({_quote(column)} AS DECIMAL(18,2)) AS {_quote(column)}"
        for column in numeric
    ]
    replacements.append(
        f"try_cast({_quote('Program_Year')} AS INTEGER) AS {_quote('Program_Year')}"
    )
    connection.execute(
        f"""
        CREATE TABLE {_quote(candidate)} AS
        SELECT
            * REPLACE ({', '.join(replacements)}),
            ?::VARCHAR AS source_run_id,
            ?::VARCHAR AS source_release_id,
            ?::VARCHAR AS source_data_period,
            ?::TIMESTAMPTZ AS ingested_at
        FROM {_quote(incoming)}
        """,
        [
            manifest.run_id,
            manifest.release_id,
            manifest.source_data_period,
            manifest.retrieval_timestamp,
        ],
    )
    connection.execute(f"DROP TABLE IF EXISTS {_quote(table)}")
    connection.execute(f"ALTER TABLE {_quote(candidate)} RENAME TO {_quote(table)}")
    connection.execute(f"DROP TABLE {_quote(incoming)}")
    return row_count


def _rebuild_industry_relationships(
    connection: duckdb.DuckDBPyConnection, year: int
) -> tuple[int, int]:
    connection.execute("DELETE FROM industry_relationships")
    missing_providers = int(
        connection.execute(
            """
            SELECT count(*)
            FROM raw_open_payments_general op
            LEFT JOIN core_providers cp
              ON cp.npi = trim(cast(op.Covered_Recipient_NPI AS VARCHAR))
            WHERE nullif(trim(cast(op.Covered_Recipient_NPI AS VARCHAR)), '') IS NOT NULL
              AND cp.npi IS NULL
            """
        ).fetchone()[0]
    )
    connection.execute(
        """
        INSERT INTO industry_relationships (
            npi, payment_year, paying_company_name, total_amount_received,
            payment_count, nature_of_payments, top_paying_company_flag
        )
        SELECT
            cp.npi,
            op.Program_Year,
            trim(op.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name),
            sum(op.Total_Amount_of_Payment_USDollars),
            count(*),
            string_agg(
                DISTINCT nullif(trim(op.Nature_of_Payment_or_Transfer_of_Value), ''),
                '; ' ORDER BY nullif(trim(op.Nature_of_Payment_or_Transfer_of_Value), '')
            ),
            false
        FROM raw_open_payments_general op
        JOIN core_providers cp
          ON cp.npi = trim(cast(op.Covered_Recipient_NPI AS VARCHAR))
        WHERE op.Total_Amount_of_Payment_USDollars > 0
          AND nullif(
              trim(op.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name), ''
          ) IS NOT NULL
        GROUP BY cp.npi, op.Program_Year,
                 trim(op.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)
        """
    )
    connection.execute(
        """
        UPDATE industry_relationships AS target
        SET top_paying_company_flag = true
        FROM (
            SELECT npi, payment_year, max(total_amount_received) AS maximum
            FROM industry_relationships GROUP BY npi, payment_year
        ) AS ranked
        WHERE target.npi = ranked.npi
          AND target.payment_year = ranked.payment_year
          AND target.total_amount_received = ranked.maximum
        """
    )
    count = int(
        connection.execute("SELECT count(*) FROM industry_relationships").fetchone()[0]
    )
    if count <= 0:
        raise ReleaseError("Open Payments aggregation produced no industry relationships")
    connection.execute("DROP TABLE IF EXISTS kol_summary")
    connection.execute(
        """
        CREATE TABLE kol_summary AS
        WITH payer_totals AS (
            SELECT npi, paying_company_name,
                   sum(total_amount_received) AS payer_total
            FROM industry_relationships
            GROUP BY npi, paying_company_name
        ),
        ranked_payers AS (
            SELECT *, row_number() OVER (
                PARTITION BY npi
                ORDER BY payer_total DESC, paying_company_name
            ) AS payer_rank
            FROM payer_totals
        ),
        top_payers AS (
            SELECT npi,
                   string_agg(
                       paying_company_name, '; '
                       ORDER BY payer_total DESC, paying_company_name
                   ) FILTER (WHERE payer_rank <= 3) AS top_3_payers
            FROM ranked_payers GROUP BY npi
        ),
        provider_totals AS (
            SELECT npi,
                   count(DISTINCT paying_company_name) AS unique_companies,
                   sum(total_amount_received) AS total_payments_all_years,
                   sum(payment_count) AS total_payment_count,
                   max(payment_year) AS most_recent_year,
                   string_agg(DISTINCT nature_of_payments, '; ') AS payment_natures
            FROM industry_relationships
            GROUP BY npi
        )
        SELECT totals.npi,
               providers.first_name,
               providers.last_org_name AS last_name,
               providers.provider_type AS specialty,
               providers.state,
               providers.city,
               totals.unique_companies,
               totals.total_payments_all_years,
               totals.total_payment_count,
               totals.most_recent_year,
               payers.top_3_payers,
               totals.payment_natures,
               CASE
                   WHEN totals.total_payments_all_years > 100000 THEN 'tier_1'
                   WHEN totals.total_payments_all_years > 50000 THEN 'tier_2'
                   ELSE 'tier_3'
               END AS kol_tier
        FROM provider_totals totals
        JOIN core_providers providers ON providers.npi = totals.npi
        LEFT JOIN top_payers payers ON payers.npi = totals.npi
        WHERE totals.total_payments_all_years > 10000
        ORDER BY totals.total_payments_all_years DESC, totals.npi
        """
    )
    kol_count = int(connection.execute("SELECT count(*) FROM kol_summary").fetchone()[0])
    return count, kol_count


def load_open_payments_sources(
    connection: duckdb.DuckDBPyConnection,
    *,
    data_root: Path,
    run_ids: tuple[str, ...],
) -> tuple[dict[str, int], dict[str, object]]:
    """Replace all three publisher-shaped Open Payments tables transactionally."""
    allowed = frozenset(OPEN_PAYMENTS_TABLES)
    verified = verified_archive_runs(
        data_root, run_ids, allowed_sources=allowed
    )
    if {manifest.source_id for manifest, _ in verified} != allowed:
        raise ReleaseError("Open Payments load requires general, research, and ownership runs")
    years = {int(manifest.source_data_period[:4]) for manifest, _ in verified}
    if len(years) != 1:
        raise ReleaseError("Open Payments sources must use one program year")
    year = years.pop()

    counts: dict[str, int] = {}
    connection.execute("BEGIN TRANSACTION")
    try:
        with ExitStack() as stack:
            extracted = [
                (
                    manifest,
                    stack.enter_context(
                        extracted_member(
                            data_root,
                            manifest,
                            archive,
                            OPEN_PAYMENTS_MEMBER_PATTERNS[manifest.source_id],
                            suffix=".csv",
                        )
                    ),
                )
                for manifest, archive in verified
            ]
            for manifest, csv_path in extracted:
                counts[OPEN_PAYMENTS_TABLES[manifest.source_id]] = (
                    _load_open_payments_table(connection, csv_path, manifest)
                )
            relationships, kol_count = _rebuild_industry_relationships(connection, year)
            counts["industry_relationships"] = relationships
            counts["kol_summary"] = kol_count
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    missing_providers = int(
        connection.execute(
            """
            SELECT count(*) FROM raw_open_payments_general op
            LEFT JOIN core_providers cp
              ON cp.npi = trim(cast(op.Covered_Recipient_NPI AS VARCHAR))
            WHERE nullif(trim(cast(op.Covered_Recipient_NPI AS VARCHAR)), '') IS NOT NULL
              AND cp.npi IS NULL
            """
        ).fetchone()[0]
    )
    return dict(sorted(counts.items())), {
        "program_year": year,
        "general_rows_without_core_provider": missing_providers,
    }
