"""DME (Durable Medical Equipment) targeting queries.

For device sales reps targeting physicians who refer DME orders.
"""

import duckdb


def high_volume_dme_referrers(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    provider_type: str | None = None,
    min_claims: int = 10,
    limit: int = 50,
) -> list[dict]:
    """Find physicians with highest DME referral volumes."""
    conditions = ["u.dme_total_claims >= ?"]
    params = [min_claims]

    if state:
        conditions.append("cp.state = ?")
        params.append(state)
    if provider_type:
        conditions.append("cp.provider_type = ?")
        params.append(provider_type)

    # Only include providers eligible to order DME
    conditions.append("ore.dme = 'Y'")

    params.append(limit)
    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            cp.npi,
            cp.last_org_name || ', ' || COALESCE(cp.first_name, '') AS provider_name,
            cp.provider_type,
            cp.city,
            cp.state,
            u.dme_total_claims,
            u.dme_medicare_payment,
            u.tot_unique_beneficiaries,
            ts.targeting_score
        FROM core_providers cp
        JOIN utilization_metrics u ON cp.npi = u.npi
        JOIN order_referring_eligibility ore ON cp.npi = ore.npi
        LEFT JOIN targeting_scores ts ON cp.npi = ts.npi
        WHERE {where}
        ORDER BY u.dme_total_claims DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "npi", "provider_name", "provider_type", "city", "state",
        "dme_total_claims", "dme_medicare_payment", "tot_unique_beneficiaries",
        "targeting_score",
    ]
    return [dict(zip(columns, row)) for row in rows]


def dme_eligible_by_specialty(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
) -> list[dict]:
    """Summary of DME-eligible providers by specialty and state."""
    conditions = ["ore.dme = 'Y'"]
    params = []

    if state:
        conditions.append("cp.state = ?")
        params.append(state)

    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            cp.provider_type,
            COUNT(*) AS eligible_count,
            COUNT(CASE WHEN u.dme_total_claims > 0 THEN 1 END) AS active_referrers,
            ROUND(AVG(CASE WHEN u.dme_total_claims > 0 THEN u.dme_total_claims END), 0) AS avg_claims,
            ROUND(SUM(u.dme_medicare_payment), 0) AS total_dme_payment
        FROM core_providers cp
        JOIN order_referring_eligibility ore ON cp.npi = ore.npi
        LEFT JOIN utilization_metrics u ON cp.npi = u.npi
        WHERE {where}
        GROUP BY cp.provider_type
        ORDER BY active_referrers DESC
    """, params).fetchall()

    columns = [
        "provider_type", "eligible_count", "active_referrers",
        "avg_claims", "total_dme_payment",
    ]
    return [dict(zip(columns, row)) for row in rows]
