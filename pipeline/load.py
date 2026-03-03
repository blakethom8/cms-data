"""Load raw CSVs into DuckDB tables using read_csv_auto for memory efficiency."""

import logging
from pathlib import Path

import duckdb

from .config import DATASETS, DB_PATH, DatasetConfig

logger = logging.getLogger(__name__)


def get_connection(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open (or create) the DuckDB database."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def load_csv_to_raw(
    con: duckdb.DuckDBPyConnection,
    dataset: DatasetConfig,
    overwrite: bool = False,
) -> int:
    """Load a single CSV into a raw DuckDB table via read_csv_auto.

    Returns the number of rows loaded.
    """
    csv_path = dataset.csv_path
    if not csv_path.exists():
        logger.warning("CSV not found: %s (run acquire first)", csv_path)
        return 0

    table = dataset.raw_table

    if not overwrite:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count > 0:
                logger.info("Table %s already has %d rows (skipping)", table, count)
                return count
        except duckdb.CatalogException:
            pass  # table doesn't exist yet

    logger.info("Loading %s -> %s", csv_path.name, table)
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"""
        CREATE TABLE {table} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info("Loaded %s: %d rows", table, count)
    return count


def load_all(con: duckdb.DuckDBPyConnection | None = None, overwrite: bool = False) -> dict[str, int]:
    """Load all downloaded CSVs into DuckDB raw tables.

    Returns {table_name: row_count}.
    """
    own_con = con is None
    if own_con:
        con = get_connection()

    results = {}
    for name, ds in DATASETS.items():
        if ds.csv_path.exists():
            results[ds.raw_table] = load_csv_to_raw(con, ds, overwrite=overwrite)
        else:
            logger.info("Skipping %s (CSV not downloaded)", name)

    if own_con:
        con.close()

    return results


def run_ddl(con: duckdb.DuckDBPyConnection, ddl_path: Path | None = None):
    """Execute the schema DDL to create target tables."""
    if ddl_path is None:
        ddl_path = Path(__file__).resolve().parent.parent / "schema" / "ddl.sql"

    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")

    ddl = ddl_path.read_text()
    # DuckDB can execute multi-statement SQL
    con.execute(ddl)
    logger.info("DDL executed successfully from %s", ddl_path)
