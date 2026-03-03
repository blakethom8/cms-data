"""Pharma-specific targeting queries.

Find prescribers of specific drugs, high-volume prescribers by therapeutic area,
and providers with high brand vs. generic ratios (potential switch targets).
"""

import duckdb


def find_prescribers_of_drug(
    con: duckdb.DuckDBPyConnection,
    drug_name: str,
    state: str | None = None,
    search_brand: bool = True,
    limit: int = 50,
) -> list[dict]:
    """Find all prescribers of a specific drug by brand or generic name.

    Requires provider_drug_detail table (from Part D by provider and drug).
    """
    if search_brand:
        drug_condition = "UPPER(dd.brand_name) LIKE UPPER(?)"
    else:
        drug_condition = "UPPER(dd.generic_name) LIKE UPPER(?)"

    params = [f"%{drug_name}%"]
    conditions = [drug_condition]

    if state:
        conditions.append("cp.state = ?")
        params.append(state)

    params.append(limit)
    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            cp.npi,
            cp.last_org_name || ', ' || COALESCE(cp.first_name, '') AS provider_name,
            cp.provider_type,
            cp.city,
            cp.state,
            dd.brand_name,
            dd.generic_name,
            dd.tot_claims,
            dd.tot_drug_cost,
            dd.tot_beneficiaries,
            ts.targeting_score
        FROM provider_drug_detail dd
        JOIN core_providers cp ON dd.npi = cp.npi
        LEFT JOIN targeting_scores ts ON dd.npi = ts.npi
        WHERE {where}
        ORDER BY dd.tot_claims DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "npi", "provider_name", "provider_type", "city", "state",
        "brand_name", "generic_name", "tot_claims", "tot_drug_cost",
        "tot_beneficiaries", "targeting_score",
    ]
    return [dict(zip(columns, row)) for row in rows]


def high_brand_ratio_prescribers(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    provider_type: str | None = None,
    min_total_claims: int = 100,
    limit: int = 50,
) -> list[dict]:
    """Find prescribers with high brand-to-generic ratio (potential generic switch targets)."""
    conditions = ["u.rx_total_claims >= ?"]
    params = [min_total_claims]

    if state:
        conditions.append("cp.state = ?")
        params.append(state)
    if provider_type:
        conditions.append("cp.provider_type = ?")
        params.append(provider_type)

    # Exclude rows where brand or generic claims are NULL/zero
    conditions.append("COALESCE(u.rx_brand_claims, 0) > 0")
    conditions.append("COALESCE(u.rx_generic_claims, 0) > 0")

    params.append(limit)
    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            cp.npi,
            cp.last_org_name || ', ' || COALESCE(cp.first_name, '') AS provider_name,
            cp.provider_type,
            cp.city,
            cp.state,
            u.rx_total_claims,
            u.rx_brand_claims,
            u.rx_generic_claims,
            ROUND(u.rx_brand_claims * 100.0 / (u.rx_brand_claims + u.rx_generic_claims), 1) AS brand_pct,
            u.rx_total_drug_cost,
            ts.targeting_score
        FROM core_providers cp
        JOIN utilization_metrics u ON cp.npi = u.npi
        LEFT JOIN targeting_scores ts ON cp.npi = ts.npi
        WHERE {where}
        ORDER BY brand_pct DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "npi", "provider_name", "provider_type", "city", "state",
        "rx_total_claims", "rx_brand_claims", "rx_generic_claims",
        "brand_pct", "rx_total_drug_cost", "targeting_score",
    ]
    return [dict(zip(columns, row)) for row in rows]


def opioid_prescribers(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    min_rate: float = 0,
    limit: int = 50,
) -> list[dict]:
    """Find prescribers by opioid prescribing rate (for compliance/monitoring)."""
    conditions = ["u.rx_opioid_prescriber_rate >= ?"]
    params = [min_rate]

    if state:
        conditions.append("cp.state = ?")
        params.append(state)

    params.append(limit)
    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            cp.npi,
            cp.last_org_name || ', ' || COALESCE(cp.first_name, '') AS provider_name,
            cp.provider_type,
            cp.city,
            cp.state,
            u.rx_opioid_prescriber_rate,
            u.rx_total_claims,
            u.rx_total_drug_cost,
            ts.targeting_score
        FROM core_providers cp
        JOIN utilization_metrics u ON cp.npi = u.npi
        LEFT JOIN targeting_scores ts ON cp.npi = ts.npi
        WHERE {where}
        ORDER BY u.rx_opioid_prescriber_rate DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "npi", "provider_name", "provider_type", "city", "state",
        "rx_opioid_prescriber_rate", "rx_total_claims", "rx_total_drug_cost",
        "targeting_score",
    ]
    return [dict(zip(columns, row)) for row in rows]
