"""Open Payments (Sunshine Act) pipeline.

Downloads annual general payment files from CMS, aggregates by NPI,
and loads into the industry_relationships table for sales targeting.

Data source: https://openpaymentsdata.cms.gov/
License: Public domain (Open Government Directive). Attribution required.
         Must display accuracy disclaimer for self-reported data.

Key fields:
  - Which pharma/device companies are paying a provider
  - How much (total dollars)
  - For what (consulting, speaking, meals, research, ownership)
  - KOL flag: providers receiving >$10K are likely Key Opinion Leaders
"""

import logging
import os
import re
import zipfile
from pathlib import Path

import duckdb

from .config import DATA_DIR, DB_PATH, RAW_DIR

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

OPEN_PAYMENTS_RAW_DIR = RAW_DIR / "open_payments"

# CMS Open Payments download base URL
# Files are named: OP_DTL_GNRL_PGYR{year}_P{publish_date}.csv
# The publish date changes annually; we'll discover it from the ZIP contents.
OPEN_PAYMENTS_DOWNLOAD_BASE = "https://download.cms.gov/openpayments"

# Default years to download (most recent 3)
DEFAULT_YEARS = [2022, 2023, 2024]

# Payment natures we care about for targeting
PAYMENT_NATURES = {
    "Consulting Fee": "consulting",
    "Compensation for services other than consulting, including serving as faculty or as a speaker at a venue other than a continuing education program": "speaking",
    "Food and Beverage": "meals",
    "Travel and Lodging": "travel",
    "Education": "education",
    "Research": "research",
    "Gift": "gift",
    "Honoraria": "honoraria",
    "Charitable Contribution": "charitable",
    "Royalty or License": "royalty",
    "Current or prospective ownership or investment interest": "ownership",
    "Grant": "grant",
    "Entertainment": "entertainment",
    "Space rental or facility fees (teaching hospital only)": "facility",
}

# KOL threshold: providers receiving more than this across all years
KOL_THRESHOLD_USD = 10_000


# ── Download ──────────────────────────────────────────────────────────────────


def download_open_payments_year(year: int, overwrite: bool = False) -> Path:
    """Download Open Payments general payment file for a single year.

    Returns path to the downloaded ZIP file.
    """
    import urllib.request

    OPEN_PAYMENTS_RAW_DIR.mkdir(parents=True, exist_ok=True)

    zip_path = OPEN_PAYMENTS_RAW_DIR / f"open_payments_{year}.zip"

    if zip_path.exists() and not overwrite:
        size_mb = zip_path.stat().st_size / (1024**2)
        logger.info("Open Payments %d ZIP exists: %.0f MB (skipping)", year, size_mb)
        return zip_path

    # Try common URL patterns (publish date varies by year)
    url_patterns = [
        f"{OPEN_PAYMENTS_DOWNLOAD_BASE}/PGYR{year}_P01202026.ZIP",
        f"{OPEN_PAYMENTS_DOWNLOAD_BASE}/PGYR{year}_P06302025.ZIP",
        f"{OPEN_PAYMENTS_DOWNLOAD_BASE}/PGYR{year}_P01172025.ZIP",
        f"{OPEN_PAYMENTS_DOWNLOAD_BASE}/PGYR{year}_P06302026.ZIP",
    ]

    for url in url_patterns:
        logger.info("Trying Open Payments %d: %s", year, url)
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=1800) as resp:
                total = int(resp.headers.get("content-length", 0))
                logger.info("Downloading Open Payments %d (%.0f MB)...", year, total / (1024**2))

                with open(zip_path, "wb") as f:
                    downloaded = 0
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0 and downloaded % (50 * 1024 * 1024) < 1024 * 1024:
                            logger.info("  %.0f%% (%.0f MB)", (downloaded / total) * 100, downloaded / (1024**2))

            logger.info("Downloaded Open Payments %d: %s", year, zip_path)
            return zip_path

        except Exception as e:
            logger.warning("URL failed: %s (%s)", url, e)
            continue

    raise RuntimeError(f"Could not download Open Payments for {year}. Tried: {url_patterns}")


def download_all_years(years: list[int] | None = None, overwrite: bool = False) -> dict[int, Path]:
    """Download Open Payments for multiple years. Returns {year: zip_path}."""
    years = years or DEFAULT_YEARS
    results = {}
    for year in years:
        try:
            results[year] = download_open_payments_year(year, overwrite=overwrite)
        except Exception:
            logger.exception("Failed to download Open Payments for %d", year)
    return results


# ── Extract ───────────────────────────────────────────────────────────────────


def extract_general_payments(year: int, overwrite: bool = False) -> Path:
    """Extract the general payments CSV from a year's ZIP file.

    The ZIP contains multiple files; we want OP_DTL_GNRL_*.csv (general payments).
    Returns path to the extracted CSV.
    """
    zip_path = OPEN_PAYMENTS_RAW_DIR / f"open_payments_{year}.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}. Run download first.")

    # Check for existing extraction
    existing = list(OPEN_PAYMENTS_RAW_DIR.glob(f"OP_DTL_GNRL_PGYR{year}*.csv"))
    if existing and not overwrite:
        logger.info("General payments CSV already extracted for %d: %s", year, existing[0])
        return existing[0]

    logger.info("Extracting general payments from %s...", zip_path)
    with zipfile.ZipFile(zip_path, "r") as z:
        # Find the general payments file
        general_files = [
            f for f in z.namelist()
            if re.match(rf"OP_DTL_GNRL_PGYR{year}.*\.csv", f, re.IGNORECASE)
        ]

        if not general_files:
            # Try broader pattern
            general_files = [f for f in z.namelist() if "GNRL" in f.upper() and f.endswith(".csv")]

        if not general_files:
            raise ValueError(f"No general payments CSV found in ZIP. Contents: {z.namelist()[:10]}")

        target = general_files[0]
        logger.info("Extracting %s...", target)
        z.extract(target, OPEN_PAYMENTS_RAW_DIR)

    csv_path = OPEN_PAYMENTS_RAW_DIR / target
    size_mb = csv_path.stat().st_size / (1024**2)
    logger.info("Extracted: %s (%.0f MB)", csv_path, size_mb)
    return csv_path


# ── Load into DuckDB ──────────────────────────────────────────────────────────


def load_year_raw(con: duckdb.DuckDBPyConnection, year: int, csv_path: Path, overwrite: bool = False) -> int:
    """Load a single year's general payments into a raw DuckDB table.

    Only keeps rows with a valid NPI (Covered_Recipient_NPI).
    Returns row count.
    """
    table_name = f"raw_open_payments_{year}"

    if not overwrite:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if count > 0:
                logger.info("%s already has %d rows (skipping)", table_name, count)
                return count
        except duckdb.CatalogException:
            pass

    logger.info("Loading Open Payments %d into DuckDB...", year)

    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT
            "Covered_Recipient_NPI"::VARCHAR                    AS npi,
            "Covered_Recipient_First_Name"                      AS recipient_first_name,
            "Covered_Recipient_Last_Name"                       AS recipient_last_name,
            "Covered_Recipient_Type"                             AS recipient_type,
            "Covered_Recipient_Specialty_1"                     AS recipient_specialty,
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" AS payer_name,
            TRY_CAST("Total_Amount_of_Payment_USDollars" AS DECIMAL(15,2)) AS payment_amount,
            "Date_of_Payment"                                   AS payment_date,
            "Nature_of_Payment_or_Transfer_of_Value"            AS payment_nature,
            "Form_of_Payment_or_Transfer_of_Value"              AS payment_form,
            "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1" AS product_category,
            "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1"  AS product_name,
            {year}                                              AS payment_year
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        WHERE "Covered_Recipient_NPI" IS NOT NULL
          AND "Covered_Recipient_NPI" != ''
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info("Loaded %s: %d payment records", table_name, count)
    return count


# ── Aggregate & Build industry_relationships ──────────────────────────────────


def build_industry_relationships(con: duckdb.DuckDBPyConnection, years: list[int] | None = None) -> int:
    """Aggregate raw payment data into the industry_relationships table.

    Groups by (NPI, year, paying company) with total amounts and payment counts.
    Flags top-paying companies and KOLs.

    Returns total rows inserted.
    """
    years = years or DEFAULT_YEARS

    # Build UNION ALL across available year tables
    union_parts = []
    for year in years:
        try:
            con.execute(f"SELECT 1 FROM raw_open_payments_{year} LIMIT 1")
            union_parts.append(f"SELECT * FROM raw_open_payments_{year}")
        except duckdb.CatalogException:
            logger.warning("raw_open_payments_%d not found, skipping", year)

    if not union_parts:
        logger.warning("No Open Payments data loaded. Run load_year_raw() first.")
        return 0

    union_sql = " UNION ALL ".join(union_parts)

    logger.info("Aggregating Open Payments data (%d years)...", len(union_parts))

    # Clear existing data for these years
    year_list = ",".join(str(y) for y in years)
    con.execute(f"DELETE FROM industry_relationships WHERE payment_year IN ({year_list})")

    # Aggregate by NPI + year + company
    con.execute(f"""
        INSERT INTO industry_relationships (
            npi, payment_year, paying_company_name,
            total_amount_received, payment_count, nature_of_payments,
            top_paying_company_flag
        )
        SELECT
            npi,
            payment_year,
            payer_name AS paying_company_name,
            SUM(payment_amount) AS total_amount_received,
            COUNT(*) AS payment_count,
            STRING_AGG(DISTINCT payment_nature, '; ' ORDER BY payment_nature) AS nature_of_payments,
            FALSE AS top_paying_company_flag
        FROM ({union_sql})
        WHERE npi IS NOT NULL
          AND payer_name IS NOT NULL
          AND payment_amount > 0
        GROUP BY npi, payment_year, payer_name
    """)

    total = con.execute(
        f"SELECT COUNT(*) FROM industry_relationships WHERE payment_year IN ({year_list})"
    ).fetchone()[0]
    logger.info("Inserted %d industry_relationships rows", total)

    # Flag top-paying company per provider per year
    con.execute(f"""
        UPDATE industry_relationships ir
        SET top_paying_company_flag = TRUE
        WHERE payment_year IN ({year_list})
          AND (npi, payment_year, total_amount_received) IN (
              SELECT npi, payment_year, MAX(total_amount_received)
              FROM industry_relationships
              WHERE payment_year IN ({year_list})
              GROUP BY npi, payment_year
          )
    """)

    flagged = con.execute(
        f"SELECT COUNT(*) FROM industry_relationships WHERE top_paying_company_flag = TRUE AND payment_year IN ({year_list})"
    ).fetchone()[0]
    logger.info("Flagged %d top-paying company relationships", flagged)

    return total


# ── KOL Summary View ──────────────────────────────────────────────────────────


def build_kol_summary(con: duckdb.DuckDBPyConnection, years: list[int] | None = None) -> int:
    """Create a KOL (Key Opinion Leader) summary table.

    Aggregates across all years to identify providers with significant
    industry relationships (>$10K total payments).

    Returns count of KOLs identified.
    """
    years = years or DEFAULT_YEARS
    year_list = ",".join(str(y) for y in years)

    logger.info("Building KOL summary (threshold: $%d)...", KOL_THRESHOLD_USD)

    con.execute("DROP TABLE IF EXISTS kol_summary")
    con.execute(f"""
        CREATE TABLE kol_summary AS
        SELECT
            ir.npi,
            cp.first_name,
            cp.last_org_name AS last_name,
            cp.provider_type AS specialty,
            cp.state,
            cp.city,
            COUNT(DISTINCT ir.paying_company_name) AS unique_companies,
            SUM(ir.total_amount_received) AS total_payments_all_years,
            SUM(ir.payment_count) AS total_payment_count,
            MAX(ir.payment_year) AS most_recent_year,
            -- Top 3 payers (by total amount across years)
            (
                SELECT STRING_AGG(sub.paying_company_name, '; ' ORDER BY sub.total DESC)
                FROM (
                    SELECT paying_company_name, SUM(total_amount_received) AS total
                    FROM industry_relationships
                    WHERE npi = ir.npi AND payment_year IN ({year_list})
                    GROUP BY paying_company_name
                    ORDER BY total DESC
                    LIMIT 3
                ) sub
            ) AS top_3_payers,
            -- Primary payment nature
            (
                SELECT STRING_AGG(DISTINCT sub2.payment_nature, '; ')
                FROM (
                    SELECT UNNEST(STRING_SPLIT(nature_of_payments, '; ')) AS payment_nature
                    FROM industry_relationships
                    WHERE npi = ir.npi AND payment_year IN ({year_list})
                    LIMIT 10
                ) sub2
            ) AS payment_natures,
            CASE
                WHEN SUM(ir.total_amount_received) > 100000 THEN 'tier_1'
                WHEN SUM(ir.total_amount_received) > 50000  THEN 'tier_2'
                WHEN SUM(ir.total_amount_received) > {KOL_THRESHOLD_USD} THEN 'tier_3'
            END AS kol_tier
        FROM industry_relationships ir
        LEFT JOIN core_providers cp ON ir.npi = cp.npi
        WHERE ir.payment_year IN ({year_list})
        GROUP BY ir.npi, cp.first_name, cp.last_org_name, cp.provider_type, cp.state, cp.city
        HAVING SUM(ir.total_amount_received) > {KOL_THRESHOLD_USD}
        ORDER BY total_payments_all_years DESC
    """)

    kol_count = con.execute("SELECT COUNT(*) FROM kol_summary").fetchone()[0]
    logger.info("Identified %d KOLs (>$%d total payments)", kol_count, KOL_THRESHOLD_USD)

    # Log tier distribution
    tiers = con.execute("""
        SELECT kol_tier, COUNT(*), ROUND(AVG(total_payments_all_years), 0)
        FROM kol_summary
        GROUP BY kol_tier
        ORDER BY kol_tier
    """).fetchall()
    for tier, count, avg_pay in tiers:
        logger.info("  %s: %d KOLs (avg $%.0f)", tier, count, avg_pay)

    return kol_count


# ── Orchestrator ──────────────────────────────────────────────────────────────


def run_open_payments_pipeline(
    con: duckdb.DuckDBPyConnection | None = None,
    years: list[int] | None = None,
    download: bool = True,
    overwrite: bool = False,
) -> dict:
    """Run the full Open Payments pipeline: download → extract → load → aggregate.

    Returns summary dict.
    """
    from .load import get_connection, run_ddl

    years = years or DEFAULT_YEARS
    own_con = con is None
    if own_con:
        con = get_connection()

    run_ddl(con)
    results = {"years": years}

    # Step 1: Download
    if download:
        zip_paths = download_all_years(years, overwrite=overwrite)
        results["downloaded"] = {y: str(p) for y, p in zip_paths.items()}

    # Step 2: Extract + Load each year
    raw_counts = {}
    for year in years:
        try:
            csv_path = extract_general_payments(year, overwrite=overwrite)
            raw_counts[year] = load_year_raw(con, year, csv_path, overwrite=overwrite)
        except Exception:
            logger.exception("Failed to process Open Payments %d", year)
    results["raw_rows"] = raw_counts

    # Step 3: Aggregate into industry_relationships
    results["industry_relationships"] = build_industry_relationships(con, years)

    # Step 4: Build KOL summary
    results["kol_count"] = build_kol_summary(con, years)

    if own_con:
        con.close()

    logger.info("Open Payments pipeline complete: %s", results)
    return results


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Open Payments (Sunshine Act) pipeline")
    parser.add_argument("--download", action="store_true", help="Download Open Payments ZIPs")
    parser.add_argument("--load", action="store_true", help="Extract and load into DuckDB")
    parser.add_argument("--aggregate", action="store_true", help="Aggregate into industry_relationships")
    parser.add_argument("--kol", action="store_true", help="Build KOL summary")
    parser.add_argument("--all", action="store_true", help="Run full pipeline")
    parser.add_argument("--years", type=int, nargs="+", default=DEFAULT_YEARS, help="Years to process")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing data")
    parser.add_argument("--db", type=str, default=str(DB_PATH), help="DuckDB path")
    args = parser.parse_args()

    if args.all:
        args.download = args.load = args.aggregate = args.kol = True

    if not any([args.download, args.load, args.aggregate, args.kol]):
        parser.print_help()
        sys.exit(1)

    from .load import get_connection, run_ddl

    con = get_connection(Path(args.db))
    run_ddl(con)

    if args.download:
        download_all_years(args.years, overwrite=args.overwrite)

    if args.load:
        for year in args.years:
            try:
                csv_path = extract_general_payments(year, overwrite=args.overwrite)
                load_year_raw(con, year, csv_path, overwrite=args.overwrite)
            except Exception:
                logger.exception("Failed year %d", year)

    if args.aggregate:
        build_industry_relationships(con, args.years)

    if args.kol:
        build_kol_summary(con, args.years)

    con.close()
    logger.info("Done.")
