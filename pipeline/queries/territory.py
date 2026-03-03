"""Geographic territory filtering queries.

Filter the ~1.2M provider universe down to a manageable territory
(typically 2,000-5,000 providers for a single-state specialty).
"""

import duckdb


def filter_territory(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    provider_types: list[str] | None = None,
    city: str | None = None,
    zip_prefix: str | None = None,
    entity_type: str = "I",
) -> int:
    """Create a territory_providers temp table from core_providers.

    Returns the number of providers in the territory.
    """
    conditions = [f"entity_type_code = '{entity_type}'"]
    params = []

    if state:
        conditions.append("state = ?")
        params.append(state)
    if provider_types:
        placeholders = ", ".join(["?"] * len(provider_types))
        conditions.append(f"provider_type IN ({placeholders})")
        params.extend(provider_types)
    if city:
        conditions.append("UPPER(city) = UPPER(?)")
        params.append(city)
    if zip_prefix:
        conditions.append("zip5 LIKE ?")
        params.append(f"{zip_prefix}%")

    where = " AND ".join(conditions)

    con.execute("DROP TABLE IF EXISTS territory_providers")
    con.execute(f"""
        CREATE TEMP TABLE territory_providers AS
        SELECT * FROM core_providers WHERE {where}
    """, params)

    count = con.execute("SELECT COUNT(*) FROM territory_providers").fetchone()[0]
    return count


def territory_summary(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Summary statistics for the current territory."""
    rows = con.execute("""
        SELECT
            tp.provider_type,
            COUNT(*) AS provider_count,
            ROUND(AVG(u.tot_medicare_payment), 0) AS avg_medicare_payment,
            ROUND(AVG(u.tot_services), 0) AS avg_services,
            ROUND(AVG(u.tot_unique_beneficiaries), 0) AS avg_beneficiaries
        FROM territory_providers tp
        LEFT JOIN utilization_metrics u ON tp.npi = u.npi
        GROUP BY tp.provider_type
        ORDER BY provider_count DESC
    """).fetchall()

    columns = ["provider_type", "provider_count", "avg_medicare_payment", "avg_services", "avg_beneficiaries"]
    return [dict(zip(columns, row)) for row in rows]
