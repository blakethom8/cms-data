"""Build durable NPPES provider-change events for New Provider Radar.

This module intentionally owns public-source state only. Workspace markets,
read/dismissed state, and rep actions remain in the Provider Search application.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

import duckdb

logger = logging.getLogger(__name__)

DDL_PATH = Path(__file__).resolve().parent.parent / "schema" / "ddl.sql"
MONTHLY_SOURCE_ID = "nppes_monthly_v2"
WEEKLY_SOURCE_ID = "nppes_weekly_incremental_v2"
ReleaseKind = Literal["monthly_full", "weekly_incremental"]


class NppesRadarError(RuntimeError):
    """An NPPES Radar release could not be validated or applied safely."""


@dataclass(frozen=True, slots=True)
class NppesRadarRelease:
    source_release_id: str
    source_id: str
    release_kind: ReleaseKind
    period_start: date
    period_end: date

    @property
    def source_data_period(self) -> str:
        if self.period_start == self.period_end:
            return self.period_end.isoformat()
        return f"{self.period_start.isoformat()}/{self.period_end.isoformat()}"

    def validate(self) -> None:
        if not self.source_release_id.strip():
            raise NppesRadarError("source_release_id is required")
        if self.period_start > self.period_end:
            raise NppesRadarError("period_start cannot be after period_end")
        expected_source = (
            MONTHLY_SOURCE_ID
            if self.release_kind == "monthly_full"
            else WEEKLY_SOURCE_ID
        )
        if self.source_id != expected_source:
            raise NppesRadarError(
                f"{self.release_kind} releases must use source_id {expected_source}"
            )


@dataclass(frozen=True, slots=True)
class NppesRadarProcessResult:
    source_release_id: str
    provider_row_count: int
    event_row_count: int
    is_baseline: bool
    already_processed: bool = False


def ensure_radar_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the warehouse schema, including the Radar state and event tables."""
    connection.execute(DDL_PATH.read_text())


def _sql_date(column: str) -> str:
    return (
        f"COALESCE(TRY_CAST(NULLIF(TRIM({column}), '') AS DATE), "
        f"CAST(TRY_STRPTIME(NULLIF(TRIM({column}), ''), '%m/%d/%Y') AS DATE))"
    )


def _taxonomy_column(position: int) -> str:
    return f'"Healthcare Provider Taxonomy Code_{position}"'


def _taxonomy_primary_column(position: int) -> str:
    return f'"Healthcare Provider Primary Taxonomy Switch_{position}"'


def _stage_provider_file(
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
) -> int:
    """Normalize the Type 1 rows from one monthly or weekly NPPES main CSV."""
    if not csv_path.is_file():
        raise NppesRadarError(f"NPPES provider CSV not found: {csv_path}")

    escaped_path = str(csv_path.resolve()).replace("'", "''")
    taxonomy_values = ", ".join(
        f"NULLIF(TRIM({_taxonomy_column(position)}), '')"
        for position in range(1, 16)
    )
    primary_taxonomy_cases = "\n".join(
        "WHEN UPPER(NULLIF(TRIM(" + _taxonomy_primary_column(position) + "), '')) = 'Y' "
        "THEN NULLIF(TRIM(" + _taxonomy_column(position) + "), '')"
        for position in range(1, 16)
    )

    connection.execute("DROP TABLE IF EXISTS _nppes_radar_incoming")
    try:
        connection.execute(
            f"""
            CREATE TEMP TABLE _nppes_radar_incoming AS
            WITH normalized AS (
                SELECT
                    NULLIF(TRIM("NPI"), '') AS npi,
                    NULLIF(TRIM("Provider First Name"), '') AS first_name,
                    NULLIF(TRIM("Provider Last Name (Legal Name)"), '') AS last_name,
                    NULLIF(TRIM("Provider Credential Text"), '') AS credentials,
                    {_sql_date('"Provider Enumeration Date"')} AS enumeration_date,
                    {_sql_date('"Last Update Date"')} AS source_last_updated_date,
                    {_sql_date('"NPI Deactivation Date"')} AS deactivation_date,
                    {_sql_date('"NPI Reactivation Date"')} AS reactivation_date,
                    COALESCE(
                        CASE
                            {primary_taxonomy_cases}
                        END,
                        NULLIF(TRIM({_taxonomy_column(1)}), '')
                    ) AS primary_taxonomy_code,
                    list_filter(
                        [{taxonomy_values}],
                        taxonomy_code -> taxonomy_code IS NOT NULL
                    ) AS taxonomy_codes,
                    NULLIF(TRIM(
                        "Provider First Line Business Practice Location Address"
                    ), '') AS practice_address_1,
                    NULLIF(TRIM(
                        "Provider Second Line Business Practice Location Address"
                    ), '') AS practice_address_2,
                    NULLIF(TRIM(
                        "Provider Business Practice Location Address City Name"
                    ), '') AS practice_city,
                    NULLIF(TRIM(
                        "Provider Business Practice Location Address State Name"
                    ), '') AS practice_state,
                    NULLIF(
                        REGEXP_EXTRACT(
                            TRIM(
                                "Provider Business Practice Location Address Postal Code"
                            ),
                            '^([0-9]{{5}})',
                            1
                        ),
                        ''
                    ) AS practice_zip5,
                    NULLIF(TRIM(
                        "Provider Business Practice Location Address Telephone Number"
                    ), '') AS practice_phone
                FROM read_csv_auto(
                    '{escaped_path}',
                    header = true,
                    all_varchar = true,
                    ignore_errors = false
                )
                WHERE TRIM("Entity Type Code") = '1'
            )
            SELECT
                *,
                MD5(CONCAT_WS(
                    '|',
                    COALESCE(npi, ''),
                    COALESCE(first_name, ''),
                    COALESCE(last_name, ''),
                    COALESCE(credentials, ''),
                    COALESCE(CAST(enumeration_date AS VARCHAR), ''),
                    COALESCE(CAST(source_last_updated_date AS VARCHAR), ''),
                    COALESCE(CAST(deactivation_date AS VARCHAR), ''),
                    COALESCE(CAST(reactivation_date AS VARCHAR), ''),
                    COALESCE(primary_taxonomy_code, ''),
                    COALESCE(ARRAY_TO_STRING(taxonomy_codes, ','), ''),
                    COALESCE(practice_address_1, ''),
                    COALESCE(practice_address_2, ''),
                    COALESCE(practice_city, ''),
                    COALESCE(practice_state, ''),
                    COALESCE(practice_zip5, ''),
                    COALESCE(practice_phone, '')
                )) AS record_fingerprint
            FROM normalized
            """
        )
    except duckdb.Error as error:
        raise NppesRadarError(
            "NPPES provider CSV does not match the expected V2 main-file schema: "
            f"{error}"
        ) from error

    provider_count = connection.execute(
        "SELECT COUNT(*) FROM _nppes_radar_incoming"
    ).fetchone()[0]
    invalid_npi = connection.execute(
        """
        SELECT npi
        FROM _nppes_radar_incoming
        WHERE npi IS NULL OR NOT REGEXP_MATCHES(npi, '^[0-9]{10}$')
        LIMIT 1
        """
    ).fetchone()
    if invalid_npi is not None:
        raise NppesRadarError(f"Invalid Type 1 NPI in provider file: {invalid_npi[0]!r}")
    duplicate_npi = connection.execute(
        """
        SELECT npi
        FROM _nppes_radar_incoming
        GROUP BY npi
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if duplicate_npi is not None:
        raise NppesRadarError(f"Duplicate Type 1 NPI in provider file: {duplicate_npi[0]}")
    return provider_count


def _previous_result(
    connection: duckdb.DuckDBPyConnection,
    source_release_id: str,
) -> NppesRadarProcessResult | None:
    row = connection.execute(
        """
        SELECT provider_row_count, event_row_count, is_baseline
        FROM nppes_radar_releases
        WHERE source_release_id = ?
        """,
        [source_release_id],
    ).fetchone()
    if row is None:
        return None
    return NppesRadarProcessResult(
        source_release_id=source_release_id,
        provider_row_count=row[0],
        event_row_count=row[1],
        is_baseline=row[2],
        already_processed=True,
    )


def _validate_release_order(
    connection: duckdb.DuckDBPyConnection,
    release: NppesRadarRelease,
) -> None:
    latest_period_end = connection.execute(
        "SELECT MAX(period_end) FROM nppes_radar_releases"
    ).fetchone()[0]
    if latest_period_end is not None and release.period_end < latest_period_end:
        raise NppesRadarError(
            "NPPES Radar releases must be applied in source-period order; "
            f"latest installed period ends {latest_period_end}"
        )


def _insert_event_sql(
    connection: duckdb.DuckDBPyConnection,
    *,
    release: NppesRadarRelease,
    event_type: str,
    predicate: str,
    effective_date: str,
    old_zip5: str = "c.practice_zip5",
    new_zip5: str = "i.practice_zip5",
    old_taxonomy: str = "c.primary_taxonomy_code",
    new_taxonomy: str = "i.primary_taxonomy_code",
) -> None:
    connection.execute(
        f"""
        INSERT INTO nppes_radar_events
        SELECT
            MD5(CONCAT_WS(
                '|',
                ?,
                i.npi,
                '{event_type}',
                CAST({effective_date} AS VARCHAR),
                COALESCE({old_zip5}, ''),
                COALESCE({new_zip5}, ''),
                COALESCE({old_taxonomy}, ''),
                COALESCE({new_taxonomy}, '')
            )) AS event_id,
            i.npi,
            '{event_type}' AS event_type,
            {effective_date} AS effective_date,
            ? AS detected_at,
            ? AS source_release_id,
            ? AS source_data_period,
            {old_zip5} AS old_zip5,
            {new_zip5} AS new_zip5,
            {old_taxonomy} AS old_primary_taxonomy_code,
            {new_taxonomy} AS new_primary_taxonomy_code,
            i.source_last_updated_date,
            i.deactivation_date,
            i.reactivation_date
        FROM _nppes_radar_incoming i
        LEFT JOIN nppes_radar_provider_state c ON c.npi = i.npi
        WHERE {predicate}
        """,
        [
            release.source_release_id,
            datetime.now(timezone.utc),
            release.source_release_id,
            release.source_data_period,
        ],
    )


def _insert_events(
    connection: duckdb.DuckDBPyConnection,
    release: NppesRadarRelease,
) -> int:
    before_count = connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_events"
    ).fetchone()[0]
    period_end = f"DATE '{release.period_end.isoformat()}'"

    _insert_event_sql(
        connection,
        release=release,
        event_type="newly_enumerated",
        predicate=(
            "c.npi IS NULL "
            "AND i.deactivation_date IS NULL "
            f"AND i.enumeration_date BETWEEN DATE '{release.period_start.isoformat()}' "
            f"AND {period_end}"
        ),
        effective_date="i.enumeration_date",
        old_zip5="NULL",
        old_taxonomy="NULL",
    )
    _insert_event_sql(
        connection,
        release=release,
        event_type="practice_location_changed",
        predicate=(
            "c.npi IS NOT NULL "
            "AND i.deactivation_date IS NULL "
            "AND i.practice_zip5 IS DISTINCT FROM c.practice_zip5"
        ),
        effective_date=f"COALESCE(i.source_last_updated_date, {period_end})",
    )
    _insert_event_sql(
        connection,
        release=release,
        event_type="primary_taxonomy_changed",
        predicate=(
            "c.npi IS NOT NULL "
            "AND i.deactivation_date IS NULL "
            "AND i.primary_taxonomy_code IS DISTINCT FROM c.primary_taxonomy_code"
        ),
        effective_date=f"COALESCE(i.source_last_updated_date, {period_end})",
    )
    _insert_event_sql(
        connection,
        release=release,
        event_type="reactivated",
        predicate=(
            "c.npi IS NOT NULL "
            "AND i.reactivation_date IS NOT NULL "
            "AND (i.reactivation_date IS DISTINCT FROM c.reactivation_date "
            "OR c.deactivation_date IS NOT NULL)"
        ),
        effective_date="i.reactivation_date",
    )
    _insert_event_sql(
        connection,
        release=release,
        event_type="deactivated",
        predicate=(
            "c.npi IS NOT NULL "
            "AND i.deactivation_date IS NOT NULL "
            "AND i.deactivation_date IS DISTINCT FROM c.deactivation_date"
        ),
        effective_date="i.deactivation_date",
        new_zip5="COALESCE(i.practice_zip5, c.practice_zip5)",
        new_taxonomy="COALESCE(i.primary_taxonomy_code, c.primary_taxonomy_code)",
    )

    after_count = connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_events"
    ).fetchone()[0]
    return after_count - before_count


def _replace_current_state(
    connection: duckdb.DuckDBPyConnection,
    release: NppesRadarRelease,
    processed_at: datetime,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO nppes_radar_provider_state
        SELECT
            i.npi,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.first_name, i.first_name) ELSE i.first_name END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.last_name, i.last_name) ELSE i.last_name END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.credentials, i.credentials) ELSE i.credentials END,
            COALESCE(i.enumeration_date, c.enumeration_date),
            COALESCE(i.source_last_updated_date, c.source_last_updated_date),
            i.deactivation_date,
            i.reactivation_date,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.primary_taxonomy_code, i.primary_taxonomy_code)
                ELSE i.primary_taxonomy_code END,
            CASE WHEN i.deactivation_date IS NOT NULL AND LEN(i.taxonomy_codes) = 0
                THEN c.taxonomy_codes ELSE i.taxonomy_codes END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.practice_address_1, i.practice_address_1)
                ELSE i.practice_address_1 END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.practice_address_2, i.practice_address_2)
                ELSE i.practice_address_2 END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.practice_city, i.practice_city) ELSE i.practice_city END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.practice_state, i.practice_state) ELSE i.practice_state END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.practice_zip5, i.practice_zip5) ELSE i.practice_zip5 END,
            CASE WHEN i.deactivation_date IS NOT NULL
                THEN COALESCE(c.practice_phone, i.practice_phone) ELSE i.practice_phone END,
            i.record_fingerprint,
            ?,
            ?,
            COALESCE(c.first_seen_at, ?),
            ?
        FROM _nppes_radar_incoming i
        LEFT JOIN nppes_radar_provider_state c ON c.npi = i.npi
        """,
        [
            release.source_release_id,
            release.source_data_period,
            processed_at,
            processed_at,
        ],
    )


def process_nppes_provider_file(
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
    release: NppesRadarRelease,
    *,
    baseline: bool = False,
) -> NppesRadarProcessResult:
    """Apply one normalized NPPES main file and emit immutable Radar events.

    The first installed release must be a monthly baseline. Reprocessing the same
    release is a successful no-op, while out-of-order releases are rejected.
    """
    release.validate()
    ensure_radar_schema(connection)

    previous = _previous_result(connection, release.source_release_id)
    if previous is not None:
        return previous

    state_count = connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_provider_state"
    ).fetchone()[0]
    if baseline and release.release_kind != "monthly_full":
        raise NppesRadarError("Only a monthly full release can establish the baseline")
    if baseline and state_count:
        raise NppesRadarError("Cannot establish an NPPES baseline over existing Radar state")
    if not baseline and not state_count:
        raise NppesRadarError("Install a monthly NPPES baseline before applying change files")
    _validate_release_order(connection, release)

    processed_at = datetime.now(timezone.utc)
    connection.execute("BEGIN TRANSACTION")
    try:
        provider_count = _stage_provider_file(connection, csv_path)
        event_count = 0 if baseline else _insert_events(connection, release)
        _replace_current_state(connection, release, processed_at)
        connection.execute(
            """
            INSERT INTO nppes_radar_releases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                release.source_release_id,
                release.source_id,
                release.release_kind,
                release.source_data_period,
                release.period_start,
                release.period_end,
                processed_at,
                provider_count,
                event_count,
                baseline,
            ],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.execute("DROP TABLE IF EXISTS _nppes_radar_incoming")

    logger.info(
        "Applied NPPES Radar release %s: providers=%d events=%d baseline=%s",
        release.source_release_id,
        provider_count,
        event_count,
        baseline,
    )
    return NppesRadarProcessResult(
        source_release_id=release.source_release_id,
        provider_row_count=provider_count,
        event_row_count=event_count,
        is_baseline=baseline,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply an NPPES file to New Provider Radar")
    parser.add_argument("--csv", type=Path, required=True, help="Extracted NPPES main CSV")
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Explicit staging candidate DuckDB path",
    )
    parser.add_argument("--release-id", required=True)
    parser.add_argument(
        "--kind", choices=("monthly_full", "weekly_incremental"), required=True
    )
    parser.add_argument("--period-start", type=date.fromisoformat, required=True)
    parser.add_argument("--period-end", type=date.fromisoformat, required=True)
    parser.add_argument("--baseline", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    source_id = MONTHLY_SOURCE_ID if args.kind == "monthly_full" else WEEKLY_SOURCE_ID
    release = NppesRadarRelease(
        source_release_id=args.release_id,
        source_id=source_id,
        release_kind=args.kind,
        period_start=args.period_start,
        period_end=args.period_end,
    )
    connection = duckdb.connect(str(args.db))
    try:
        result = process_nppes_provider_file(
            connection,
            args.csv,
            release,
            baseline=args.baseline,
        )
    except (duckdb.Error, OSError, NppesRadarError) as error:
        logger.error("NPPES Radar processing failed: %s", error)
        return 1
    finally:
        connection.close()
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
