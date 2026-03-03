"""NPPES (National Plan & Provider Enumeration System) pipeline.

Downloads the monthly NPPES bulk file (~9GB ZIP, ~25GB CSV),
filters to Type 1 (Individual) NPIs, and either:
  1. Enriches existing core_providers with gender, taxonomy, credentials
  2. Inserts non-Medicare NPIs to expand from 1.2M → ~8M providers

Data source: https://download.cms.gov/nppes/NPI_Files.html
License: Public domain, no restrictions on use or redistribution.
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

NPPES_DOWNLOAD_URL = os.getenv(
    "NPPES_URL",
    "https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_2026.zip",
)

NPPES_ZIP_PATH = RAW_DIR / "nppes.zip"
NPPES_RAW_DIR = RAW_DIR / "nppes"

# Columns we actually need (out of 329 in the full file)
NPPES_KEEP_COLUMNS = [
    "NPI",
    "Entity Type Code",
    "Provider First Name",
    "Provider Last Name (Legal Name)",
    "Provider Middle Name",
    "Provider Name Prefix Text",
    "Provider Name Suffix Text",
    "Provider Credential Text",
    "Provider Gender Code",
    "Provider Enumeration Date",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
    "Is Sole Proprietor",
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Telephone Number",
    "Healthcare Provider Taxonomy Code_1",
    "Healthcare Provider Taxonomy Code_2",
    "Healthcare Provider Taxonomy Code_3",
    "Healthcare Provider Primary Taxonomy Switch_1",
    "Healthcare Provider Primary Taxonomy Switch_2",
    "Healthcare Provider Primary Taxonomy Switch_3",
]


# ── Download ──────────────────────────────────────────────────────────────────


def download_nppes(overwrite: bool = False) -> Path:
    """Download the NPPES bulk ZIP file (~9GB).

    Supports resume via Range headers if the download is interrupted.
    Returns the path to the downloaded ZIP.
    """
    import urllib.request

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if NPPES_ZIP_PATH.exists() and not overwrite:
        size_gb = NPPES_ZIP_PATH.stat().st_size / (1024**3)
        logger.info("NPPES ZIP already exists: %s (%.1f GB, skipping)", NPPES_ZIP_PATH, size_gb)
        return NPPES_ZIP_PATH

    logger.info("Downloading NPPES from %s (~9GB, this will take 20-30 min)...", NPPES_DOWNLOAD_URL)

    # Check for partial download to support resume
    partial_path = NPPES_ZIP_PATH.with_suffix(".zip.partial")
    start_byte = partial_path.stat().st_size if partial_path.exists() else 0

    req = urllib.request.Request(NPPES_DOWNLOAD_URL)
    if start_byte > 0:
        req.add_header("Range", f"bytes={start_byte}-")
        logger.info("Resuming download from byte %d (%.1f GB)", start_byte, start_byte / (1024**3))

    with urllib.request.urlopen(req, timeout=3600) as resp:
        total = int(resp.headers.get("content-length", 0)) + start_byte
        mode = "ab" if start_byte > 0 else "wb"

        with open(partial_path, mode) as f:
            downloaded = start_byte
            chunk_size = 1024 * 1024  # 1MB chunks
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0 and downloaded % (100 * 1024 * 1024) < chunk_size:
                    pct = (downloaded / total) * 100
                    logger.info("  %.1f%% (%.1f / %.1f GB)", pct, downloaded / (1024**3), total / (1024**3))

    # Rename partial → final
    partial_path.rename(NPPES_ZIP_PATH)
    logger.info("NPPES download complete: %s", NPPES_ZIP_PATH)
    return NPPES_ZIP_PATH


# ── Extract ───────────────────────────────────────────────────────────────────


def extract_nppes(overwrite: bool = False) -> Path:
    """Extract the main NPI data file from the NPPES ZIP.

    Returns the path to the extracted CSV (the main npidata_pfile_*.csv).
    """
    NPPES_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Check if already extracted
    existing = list(NPPES_RAW_DIR.glob("npidata_pfile_*.csv"))
    if existing and not overwrite:
        logger.info("NPPES CSV already extracted: %s", existing[0])
        return existing[0]

    if not NPPES_ZIP_PATH.exists():
        raise FileNotFoundError(f"NPPES ZIP not found: {NPPES_ZIP_PATH}. Run download_nppes() first.")

    logger.info("Extracting NPPES ZIP (~25GB uncompressed, ~5 min)...")
    with zipfile.ZipFile(NPPES_ZIP_PATH, "r") as z:
        # Find the main data file (npidata_pfile_YYYYMMDD-YYYYMMDD.csv)
        npi_files = [f for f in z.namelist() if re.match(r"npidata_pfile_\d{8}-\d{8}\.csv", f)]
        if not npi_files:
            # Fallback: any file starting with npidata
            npi_files = [f for f in z.namelist() if f.startswith("npidata") and f.endswith(".csv")]

        if not npi_files:
            raise ValueError(f"No NPI data file found in ZIP. Contents: {z.namelist()[:10]}")

        target = npi_files[0]
        logger.info("Extracting %s...", target)
        z.extract(target, NPPES_RAW_DIR)

    csv_path = NPPES_RAW_DIR / target
    size_gb = csv_path.stat().st_size / (1024**3)
    logger.info("Extracted: %s (%.1f GB)", csv_path, size_gb)
    return csv_path


# ── Load into DuckDB ──────────────────────────────────────────────────────────


def load_nppes_raw(con: duckdb.DuckDBPyConnection, csv_path: Path, overwrite: bool = False) -> int:
    """Load NPPES CSV into a raw DuckDB table, filtered to Type 1 (Individual) only.

    Only loads the columns we need (30 out of 329) for memory efficiency.
    Returns row count.
    """
    if not overwrite:
        try:
            count = con.execute("SELECT COUNT(*) FROM raw_nppes").fetchone()[0]
            if count > 0:
                logger.info("raw_nppes already has %d rows (skipping)", count)
                return count
        except duckdb.CatalogException:
            pass

    logger.info("Loading NPPES into DuckDB (filtered to Type 1, ~15 min)...")

    # Build column selection SQL
    # DuckDB read_csv_auto handles the 329-column file efficiently
    con.execute("DROP TABLE IF EXISTS raw_nppes")
    con.execute(f"""
        CREATE TABLE raw_nppes AS
        SELECT
            "NPI"                                       AS npi,
            "Entity Type Code"                          AS entity_type,
            "Provider First Name"                       AS first_name,
            "Provider Last Name (Legal Name)"           AS last_name,
            "Provider Middle Name"                      AS middle_name,
            "Provider Name Prefix Text"                 AS name_prefix,
            "Provider Name Suffix Text"                 AS name_suffix,
            "Provider Credential Text"                  AS credentials,
            "Provider Gender Code"                      AS gender,
            "Provider Enumeration Date"                 AS enumeration_date,
            "NPI Deactivation Date"                     AS deactivation_date,
            "NPI Reactivation Date"                     AS reactivation_date,
            "Is Sole Proprietor"                        AS sole_proprietor,
            "Provider First Line Business Practice Location Address"  AS practice_address_1,
            "Provider Second Line Business Practice Location Address" AS practice_address_2,
            "Provider Business Practice Location Address City Name"   AS practice_city,
            "Provider Business Practice Location Address State Name"  AS practice_state,
            "Provider Business Practice Location Address Postal Code" AS practice_zip,
            "Provider Business Practice Location Address Country Code (If outside U.S.)" AS practice_country,
            "Provider Business Practice Location Address Telephone Number" AS practice_phone,
            "Healthcare Provider Taxonomy Code_1"       AS taxonomy_1,
            "Healthcare Provider Taxonomy Code_2"       AS taxonomy_2,
            "Healthcare Provider Taxonomy Code_3"       AS taxonomy_3,
            "Healthcare Provider Primary Taxonomy Switch_1" AS taxonomy_primary_1,
            "Healthcare Provider Primary Taxonomy Switch_2" AS taxonomy_primary_2,
            "Healthcare Provider Primary Taxonomy Switch_3" AS taxonomy_primary_3
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        WHERE "Entity Type Code" = '1'
    """)

    count = con.execute("SELECT COUNT(*) FROM raw_nppes").fetchone()[0]
    logger.info("raw_nppes loaded: %d Type 1 (Individual) NPIs", count)
    return count


# ── Enrich core_providers ─────────────────────────────────────────────────────


def enrich_core_providers(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Enrich existing core_providers with NPPES data and insert non-Medicare NPIs.

    Returns dict with counts: {enriched, inserted, total}.
    """
    results = {}

    # Step 1: Enrich existing Medicare providers with gender + taxonomy
    logger.info("Enriching existing core_providers with NPPES data...")
    con.execute("""
        UPDATE core_providers
        SET
            gender = n.gender,
            primary_taxonomy_code = COALESCE(
                CASE WHEN n.taxonomy_primary_1 = 'Y' THEN n.taxonomy_1
                     WHEN n.taxonomy_primary_2 = 'Y' THEN n.taxonomy_2
                     WHEN n.taxonomy_primary_3 = 'Y' THEN n.taxonomy_3
                     ELSE n.taxonomy_1
                END,
                core_providers.primary_taxonomy_code
            )
        FROM raw_nppes n
        WHERE core_providers.npi = n.npi
    """)

    enriched = con.execute("""
        SELECT COUNT(*) FROM core_providers
        WHERE gender IS NOT NULL AND primary_taxonomy_code IS NOT NULL
    """).fetchone()[0]
    results["enriched"] = enriched
    logger.info("Enriched %d existing providers with gender + taxonomy", enriched)

    # Step 2: Insert non-Medicare NPIs (providers not in current core_providers)
    logger.info("Inserting non-Medicare NPIs...")

    # Get the data_year from existing data (use max)
    data_year = con.execute("SELECT MAX(data_year) FROM core_providers").fetchone()[0] or 2024

    con.execute("""
        INSERT INTO core_providers (
            npi, last_org_name, first_name, middle_initial, credentials,
            entity_type_code, gender, primary_taxonomy_code,
            street_address_1, street_address_2, city, state, zip5, country,
            medicare_participating, data_year
        )
        SELECT
            n.npi,
            n.last_name,
            n.first_name,
            LEFT(n.middle_name, 1),
            n.credentials,
            'I',
            n.gender,
            CASE WHEN n.taxonomy_primary_1 = 'Y' THEN n.taxonomy_1
                 WHEN n.taxonomy_primary_2 = 'Y' THEN n.taxonomy_2
                 WHEN n.taxonomy_primary_3 = 'Y' THEN n.taxonomy_3
                 ELSE n.taxonomy_1
            END,
            n.practice_address_1,
            n.practice_address_2,
            n.practice_city,
            n.practice_state,
            LEFT(n.practice_zip, 5),
            COALESCE(n.practice_country, 'US'),
            'N',  -- not Medicare participating (no claims data)
            ?
        FROM raw_nppes n
        WHERE n.npi NOT IN (SELECT npi FROM core_providers)
          AND n.deactivation_date IS NULL  -- exclude deactivated NPIs
          AND n.practice_state IS NOT NULL  -- must have a practice location
    """, [data_year])

    inserted = con.execute("""
        SELECT COUNT(*) FROM core_providers WHERE medicare_participating = 'N'
    """).fetchone()[0]
    results["inserted_non_medicare"] = inserted
    logger.info("Inserted %d non-Medicare NPIs", inserted)

    total = con.execute("SELECT COUNT(*) FROM core_providers").fetchone()[0]
    results["total_providers"] = total
    logger.info("Total core_providers: %d", total)

    return results


# ── Taxonomy Lookup ───────────────────────────────────────────────────────────

# Common taxonomy → specialty mapping for non-Medicare providers
# (Medicare providers already have provider_type from CMS data)
TAXONOMY_TO_SPECIALTY = {
    "207RC0000X": "Cardiovascular Disease",
    "207RI0011X": "Interventional Cardiology",
    "207RE0101X": "Endocrinology, Diabetes & Metabolism",
    "207RG0100X": "Gastroenterology",
    "207RH0003X": "Hematology & Oncology",
    "207RN0300X": "Nephrology",
    "207RP1001X": "Pulmonary Disease",
    "207RR0500X": "Rheumatology",
    "208600000X": "Surgery",
    "207X00000X": "Orthopaedic Surgery",
    "207T00000X": "Neurological Surgery",
    "208C00000X": "Colon & Rectal Surgery",
    "207V00000X": "Obstetrics & Gynecology",
    "207W00000X": "Ophthalmology",
    "207Y00000X": "Otolaryngology",
    "208200000X": "Plastic Surgery",
    "208G00000X": "Thoracic Surgery",
    "208100000X": "Physical Medicine & Rehabilitation",
    "2084N0400X": "Neurology",
    "2084P0800X": "Psychiatry",
    "207Q00000X": "Family Medicine",
    "207R00000X": "Internal Medicine",
    "208D00000X": "General Practice",
    "207L00000X": "Anesthesiology",
    "2086S0120X": "Pediatric Surgery",
    "2080N0001X": "Neonatal-Perinatal Medicine",
    "208000000X": "Pediatrics",
    "2085R0001X": "Radiation Oncology",
    "2086S0122X": "Surgical Oncology",
    "207RX0202X": "Medical Oncology",
    "207ND0101X": "Dermatopathology",
    "207N00000X": "Dermatology",
    "207U00000X": "Nuclear Medicine",
    "2085R0202X": "Diagnostic Radiology",
    "2085H0002X": "Hospice and Palliative Medicine",
    "363L00000X": "Nurse Practitioner",
    "363A00000X": "Physician Assistant",
    "174400000X": "Optometrist",
    "1223G0001X": "General Dentistry",
}


def map_taxonomy_to_specialty(con: duckdb.DuckDBPyConnection) -> int:
    """Map NPPES taxonomy codes to provider_type for non-Medicare providers.

    Only updates providers where provider_type is NULL (non-Medicare).
    Returns count of providers updated.
    """
    logger.info("Mapping taxonomy codes to specialties for non-Medicare providers...")

    # Build CASE statement from mapping
    cases = "\n".join(
        f"        WHEN primary_taxonomy_code = '{code}' THEN '{specialty}'"
        for code, specialty in TAXONOMY_TO_SPECIALTY.items()
    )

    con.execute(f"""
        UPDATE core_providers
        SET provider_type = CASE
{cases}
            ELSE 'Other'
        END
        WHERE provider_type IS NULL
          AND primary_taxonomy_code IS NOT NULL
    """)

    updated = con.execute("""
        SELECT COUNT(*) FROM core_providers
        WHERE provider_type IS NOT NULL AND medicare_participating = 'N'
    """).fetchone()[0]
    logger.info("Mapped specialty for %d non-Medicare providers", updated)
    return updated


# ── Orchestrator ──────────────────────────────────────────────────────────────


def run_nppes_pipeline(
    con: duckdb.DuckDBPyConnection | None = None,
    download: bool = True,
    overwrite: bool = False,
) -> dict:
    """Run the full NPPES pipeline: download → extract → load → enrich.

    Returns summary dict with counts.
    """
    from .load import get_connection, run_ddl

    own_con = con is None
    if own_con:
        con = get_connection()

    # Ensure schema exists
    run_ddl(con)

    results = {}

    # Step 1: Download
    if download:
        zip_path = download_nppes(overwrite=overwrite)
        results["zip_path"] = str(zip_path)

    # Step 2: Extract
    csv_path = extract_nppes(overwrite=overwrite)
    results["csv_path"] = str(csv_path)

    # Step 3: Load into DuckDB
    results["raw_rows"] = load_nppes_raw(con, csv_path, overwrite=overwrite)

    # Step 4: Enrich core_providers
    enrich_results = enrich_core_providers(con)
    results.update(enrich_results)

    # Step 5: Map taxonomy → specialty
    results["taxonomy_mapped"] = map_taxonomy_to_specialty(con)

    if own_con:
        con.close()

    logger.info("NPPES pipeline complete: %s", results)
    return results


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="NPPES pipeline")
    parser.add_argument("--download", action="store_true", help="Download NPPES ZIP")
    parser.add_argument("--extract", action="store_true", help="Extract NPPES CSV from ZIP")
    parser.add_argument("--load", action="store_true", help="Load NPPES into DuckDB")
    parser.add_argument("--enrich", action="store_true", help="Enrich core_providers from NPPES")
    parser.add_argument("--all", action="store_true", help="Run full pipeline")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing data")
    parser.add_argument("--db", type=str, default=str(DB_PATH), help="DuckDB path")
    args = parser.parse_args()

    if args.all:
        args.download = args.extract = args.load = args.enrich = True

    if not any([args.download, args.extract, args.load, args.enrich]):
        parser.print_help()
        sys.exit(1)

    from .load import get_connection, run_ddl

    con = get_connection(Path(args.db))
    run_ddl(con)

    if args.download:
        download_nppes(overwrite=args.overwrite)

    if args.extract:
        csv_path = extract_nppes(overwrite=args.overwrite)

    if args.load:
        csv_path = extract_nppes()  # get path without re-extracting
        load_nppes_raw(con, csv_path, overwrite=args.overwrite)

    if args.enrich:
        enrich_core_providers(con)
        map_taxonomy_to_specialty(con)

    con.close()
    logger.info("Done.")
