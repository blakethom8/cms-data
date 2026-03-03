"""Targeting Score calculation: multi-dimensional 0-100 score for sales prioritization.

Phase 1 (catalog-only): Uses claims volume, payment volume, beneficiary reach,
prescribing volume, and quality gap (low MIPS = opportunity).

All percentiles are within-specialty so a cardiologist is compared to cardiologists.
"""

import logging

import duckdb

logger = logging.getLogger(__name__)

TARGETING_SCORE_SQL = """
CREATE OR REPLACE TABLE targeting_scores AS
WITH specialty_percentiles AS (
    SELECT
        cp.npi,
        cp.provider_type,
        cp.last_org_name,
        cp.first_name,
        cp.state,
        cp.city,
        cp.zip5,
        PERCENT_RANK() OVER (
            PARTITION BY cp.provider_type ORDER BY COALESCE(u.tot_services, 0)
        ) AS claims_pctile,
        PERCENT_RANK() OVER (
            PARTITION BY cp.provider_type ORDER BY COALESCE(u.tot_medicare_payment, 0)
        ) AS payment_pctile,
        PERCENT_RANK() OVER (
            PARTITION BY cp.provider_type ORDER BY COALESCE(u.tot_unique_beneficiaries, 0)
        ) AS bene_pctile,
        PERCENT_RANK() OVER (
            PARTITION BY cp.provider_type ORDER BY COALESCE(u.rx_total_claims, 0)
        ) AS rx_pctile,
        CASE
            WHEN q.final_mips_score IS NOT NULL
            THEN 1.0 - (q.final_mips_score / 100.0)
            ELSE 0.5
        END AS quality_opportunity
    FROM core_providers cp
    JOIN utilization_metrics u ON cp.npi = u.npi
    LEFT JOIN provider_quality_scores q ON cp.npi = q.npi
    WHERE cp.entity_type_code = 'I'
)
SELECT
    npi,
    provider_type,
    last_org_name,
    first_name,
    state,
    city,
    zip5,
    claims_pctile,
    payment_pctile,
    bene_pctile,
    rx_pctile,
    quality_opportunity,
    ROUND(
        (claims_pctile * 40) +
        (payment_pctile * 25) +
        (bene_pctile * 15) +
        (rx_pctile * 10) +
        (quality_opportunity * 10),
        1
    ) AS targeting_score
FROM specialty_percentiles
"""


def compute_targeting_scores(con: duckdb.DuckDBPyConnection) -> int:
    """Compute targeting scores for all individual providers.

    Creates/replaces the targeting_scores table.
    Returns the number of scored providers.
    """
    logger.info("Computing targeting scores")
    con.execute(TARGETING_SCORE_SQL)

    count = con.execute("SELECT COUNT(*) FROM targeting_scores").fetchone()[0]
    logger.info("Targeting scores computed for %d providers", count)

    # Log distribution summary
    stats = con.execute("""
        SELECT
            MIN(targeting_score) AS min_score,
            ROUND(AVG(targeting_score), 1) AS avg_score,
            MEDIAN(targeting_score) AS median_score,
            MAX(targeting_score) AS max_score
        FROM targeting_scores
    """).fetchone()
    if stats:
        logger.info(
            "Score distribution: min=%.1f, avg=%.1f, median=%.1f, max=%.1f",
            stats[0], stats[1], stats[2], stats[3],
        )

    return count


def get_top_targets(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    provider_type: str | None = None,
    city: str | None = None,
    zip_prefix: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Query top-scored providers with optional geographic/specialty filters.

    Returns list of dicts with provider info and targeting_score.
    """
    conditions = []
    params = []

    if state:
        conditions.append("t.state = ?")
        params.append(state)
    if provider_type:
        conditions.append("t.provider_type = ?")
        params.append(provider_type)
    if city:
        conditions.append("UPPER(t.city) = UPPER(?)")
        params.append(city)
    if zip_prefix:
        conditions.append("t.zip5 LIKE ?")
        params.append(f"{zip_prefix}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    query = f"""
        SELECT
            t.npi,
            t.last_org_name,
            t.first_name,
            t.provider_type,
            t.state,
            t.city,
            t.zip5,
            t.targeting_score,
            t.claims_pctile,
            t.payment_pctile,
            t.bene_pctile,
            t.rx_pctile,
            t.quality_opportunity,
            u.tot_services,
            u.tot_medicare_payment,
            u.tot_unique_beneficiaries,
            u.rx_total_claims,
            q.final_mips_score
        FROM targeting_scores t
        LEFT JOIN utilization_metrics u ON t.npi = u.npi
        LEFT JOIN provider_quality_scores q ON t.npi = q.npi
        {where}
        ORDER BY t.targeting_score DESC
        LIMIT ?
    """

    rows = con.execute(query, params).fetchall()
    columns = [
        "npi", "last_org_name", "first_name", "provider_type", "state", "city",
        "zip5", "targeting_score", "claims_pctile", "payment_pctile",
        "bene_pctile", "rx_pctile", "quality_opportunity", "tot_services",
        "tot_medicare_payment", "tot_unique_beneficiaries", "rx_total_claims",
        "final_mips_score",
    ]
    return [dict(zip(columns, row)) for row in rows]
