"""Export DuckDB tables to CSV for Supabase sync.

Exports analytical tables from the DuckDB database to CSV files
that can be loaded into Supabase PostgreSQL via psql COPY.
"""

import logging
from pathlib import Path

import duckdb

from .config import DATA_DIR, DB_PATH

logger = logging.getLogger(__name__)

EXPORT_DIR = DATA_DIR / "exports"

# Tables to export and their row-count thresholds for sanity checks
EXPORTABLE_TABLES = {
    "core_providers": 100_000,          # expect 1M+ (Phase 1) or 8M+ (Phase 2)
    "utilization_metrics": 100_000,     # expect 1M+
    "practice_locations": 50_000,       # expect 1M+
    "hospital_affiliations": 1_000,     # expect 100K+
    "provider_quality_scores": 50_000,  # expect 500K+
    "targeting_scores": 100_000,        # expect 1M+
    "industry_relationships": 0,        # may be empty until Open Payments loaded
    "kol_summary": 0,                   # may be empty until Open Payments loaded
}


def export_table(con: duckdb.DuckDBPyConnection, table: str, output_dir: Path | None = None) -> Path:
    """Export a single DuckDB table to CSV.

    Returns path to the CSV file.
    """
    output_dir = output_dir or EXPORT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"{table}.csv"

    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except duckdb.CatalogException:
        logger.warning("Table %s does not exist, skipping export", table)
        return csv_path

    if count == 0:
        logger.info("Table %s is empty, skipping export", table)
        return csv_path

    logger.info("Exporting %s (%d rows) → %s", table, count, csv_path)
    con.execute(f"COPY {table} TO '{csv_path}' (HEADER, DELIMITER ',')")

    size_mb = csv_path.stat().st_size / (1024**2)
    logger.info("Exported %s: %.1f MB", table, size_mb)
    return csv_path


def export_all(
    con: duckdb.DuckDBPyConnection | None = None,
    tables: list[str] | None = None,
    output_dir: Path | None = None,
) -> dict[str, Path]:
    """Export multiple tables to CSV. Returns {table: csv_path}.

    If tables is None, exports all EXPORTABLE_TABLES.
    """
    from .load import get_connection

    own_con = con is None
    if own_con:
        con = get_connection()

    tables = tables or list(EXPORTABLE_TABLES.keys())
    results = {}

    for table in tables:
        try:
            results[table] = export_table(con, table, output_dir)
        except Exception:
            logger.exception("Failed to export %s", table)

    if own_con:
        con.close()

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Export DuckDB tables to CSV")
    parser.add_argument("--tables", nargs="+", help="Tables to export (default: all)")
    parser.add_argument("--output", type=str, default=str(EXPORT_DIR), help="Output directory")
    parser.add_argument("--db", type=str, default=str(DB_PATH), help="DuckDB path")
    args = parser.parse_args()

    from .load import get_connection

    con = get_connection(Path(args.db))
    export_all(con, args.tables, Path(args.output))
    con.close()
