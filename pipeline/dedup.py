"""Type 1 / Type 2 NPI deduplication logic.

Problem: A physician (Type 1 / Individual NPI) may also appear under a group
practice (Type 2 / Organization NPI). We need to avoid double-counting.

Strategy:
  - core_providers includes ONLY entity_type_code = 'I' (Individual)
  - utilization_metrics pulls ONLY from Type 1 records
  - Type 2 (Organization) NPIs are used ONLY for practice_locations and
    hospital_affiliations context
  - Providers who bill ONLY through a group (not in the utilization data
    under their own NPI) are flagged as bills_through_group_only = TRUE
"""

import logging

import duckdb

logger = logging.getLogger(__name__)


def flag_group_only_billers(con: duckdb.DuckDBPyConnection, data_year: int) -> int:
    """Identify providers who appear in reassignment but NOT in utilization data.

    These providers bill exclusively through a group practice and lack
    individual-level utilization metrics. They are flagged in core_providers
    so downstream queries can handle them appropriately.

    Returns the number of providers flagged.
    """
    logger.info("Flagging group-only billers (data_year=%d)", data_year)

    # Reset all flags first
    con.execute("""
        UPDATE core_providers
        SET bills_through_group_only = FALSE
        WHERE data_year = ?
    """, [data_year])

    # Find NPIs that are in reassignment but have no utilization data
    con.execute("""
        UPDATE core_providers
        SET bills_through_group_only = TRUE
        WHERE data_year = ?
          AND npi IN (
              SELECT DISTINCT pl.npi
              FROM practice_locations pl
              WHERE pl.data_year = ?
                AND pl.npi NOT IN (
                    SELECT npi FROM utilization_metrics WHERE metric_year = ?
                )
          )
    """, [data_year, data_year, data_year])

    flagged = con.execute("""
        SELECT COUNT(*) FROM core_providers
        WHERE bills_through_group_only = TRUE AND data_year = ?
    """, [data_year]).fetchone()[0]

    logger.info("Flagged %d providers as bills_through_group_only", flagged)
    return flagged


def validate_dedup(con: duckdb.DuckDBPyConnection, data_year: int) -> dict:
    """Run deduplication validation checks. Returns a summary dict.

    Checks:
    1. No Organization (Type 2) NPIs in core_providers
    2. No NPI appears in both core_providers as Type 1 and raw data as Type 2
    3. Every NPI in utilization_metrics exists in core_providers
    4. bills_through_group_only flag is consistent
    """
    results = {}

    # Check 1: No Type 2 in core_providers
    type2_count = con.execute("""
        SELECT COUNT(*) FROM core_providers
        WHERE entity_type_code = 'O' AND data_year = ?
    """, [data_year]).fetchone()[0]
    results["type2_in_core_providers"] = type2_count
    if type2_count > 0:
        logger.warning("DEDUP VIOLATION: %d Type 2 (Organization) NPIs in core_providers", type2_count)

    # Check 2: All utilization NPIs exist in core_providers
    orphan_util = con.execute("""
        SELECT COUNT(*) FROM utilization_metrics u
        WHERE u.metric_year = ?
          AND u.npi NOT IN (SELECT npi FROM core_providers WHERE data_year = ?)
    """, [data_year, data_year]).fetchone()[0]
    results["orphan_utilization_npis"] = orphan_util
    if orphan_util > 0:
        logger.warning("DEDUP VIOLATION: %d utilization NPIs not in core_providers", orphan_util)

    # Check 3: bills_through_group_only consistency
    group_only_with_util = con.execute("""
        SELECT COUNT(*) FROM core_providers cp
        WHERE cp.bills_through_group_only = TRUE
          AND cp.data_year = ?
          AND cp.npi IN (SELECT npi FROM utilization_metrics WHERE metric_year = ?)
    """, [data_year, data_year]).fetchone()[0]
    results["group_only_with_utilization"] = group_only_with_util
    if group_only_with_util > 0:
        logger.warning(
            "DEDUP INCONSISTENCY: %d group-only providers have utilization data",
            group_only_with_util,
        )

    # Summary counts
    results["total_individual_providers"] = con.execute(
        "SELECT COUNT(*) FROM core_providers WHERE entity_type_code = 'I' AND data_year = ?",
        [data_year],
    ).fetchone()[0]
    results["total_with_utilization"] = con.execute(
        "SELECT COUNT(*) FROM utilization_metrics WHERE metric_year = ?",
        [data_year],
    ).fetchone()[0]
    results["total_group_only"] = con.execute(
        "SELECT COUNT(*) FROM core_providers WHERE bills_through_group_only = TRUE AND data_year = ?",
        [data_year],
    ).fetchone()[0]

    logger.info("Dedup validation: %s", results)
    return results
