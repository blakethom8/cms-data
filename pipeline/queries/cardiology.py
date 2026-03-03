"""Cardiology-specific targeting queries.

Useful for device reps (cardiac devices, stents, ablation catheters)
and pharma reps (anticoagulants, statins, heart failure drugs).
"""

import duckdb

# HCPCS codes commonly associated with interventional cardiology
CARDIAC_PROCEDURE_CODES = [
    "93458",  # Left heart catheterization
    "93459",  # Left heart cath with left ventriculography
    "93460",  # Right and left heart cath
    "93461",  # Right and left heart cath with left ventriculography
    "92928",  # PCI single vessel with stent
    "92929",  # PCI additional branch stent
    "33208",  # Pacemaker insertion (dual chamber)
    "33249",  # ICD insertion
    "93656",  # Ablation (atrial fibrillation)
]


def high_volume_cardiologists(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    min_services: int = 100,
    limit: int = 50,
) -> list[dict]:
    """Find high-volume cardiologists ranked by targeting score."""
    conditions = ["t.provider_type = 'Cardiology'"]
    params = []

    if state:
        conditions.append("t.state = ?")
        params.append(state)

    conditions.append("u.tot_services >= ?")
    params.append(min_services)
    params.append(limit)

    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            t.npi,
            t.last_org_name || ', ' || COALESCE(t.first_name, '') AS provider_name,
            t.city,
            t.state,
            t.targeting_score,
            u.tot_services,
            u.tot_medicare_payment,
            u.tot_unique_beneficiaries,
            u.cc_heart_failure_pct,
            u.cc_atrial_fib_pct,
            u.cc_ischemic_heart_pct,
            q.final_mips_score
        FROM targeting_scores t
        JOIN utilization_metrics u ON t.npi = u.npi
        LEFT JOIN provider_quality_scores q ON t.npi = q.npi
        WHERE {where}
        ORDER BY t.targeting_score DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "npi", "provider_name", "city", "state", "targeting_score",
        "tot_services", "tot_medicare_payment", "tot_unique_beneficiaries",
        "cc_heart_failure_pct", "cc_atrial_fib_pct", "cc_ischemic_heart_pct",
        "final_mips_score",
    ]
    return [dict(zip(columns, row)) for row in rows]


def interventional_cardiologists(
    con: duckdb.DuckDBPyConnection,
    state: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Find cardiologists performing interventional procedures (caths, stents, ablations)."""
    placeholders = ", ".join(["?"] * len(CARDIAC_PROCEDURE_CODES))
    params = list(CARDIAC_PROCEDURE_CODES)

    conditions = ["cp.provider_type = 'Cardiology'"]
    if state:
        conditions.append("cp.state = ?")
        params.append(state)

    params.append(limit)
    where = " AND ".join(conditions)

    rows = con.execute(f"""
        SELECT
            cp.npi,
            cp.last_org_name || ', ' || COALESCE(cp.first_name, '') AS provider_name,
            cp.city,
            cp.state,
            COUNT(DISTINCT sd.hcpcs_code) AS interventional_procedure_count,
            SUM(sd.tot_services) AS total_interventional_services,
            SUM(sd.tot_beneficiaries) AS total_interventional_benes,
            ts.targeting_score
        FROM core_providers cp
        JOIN provider_service_detail sd ON cp.npi = sd.npi
        LEFT JOIN targeting_scores ts ON cp.npi = ts.npi
        WHERE sd.hcpcs_code IN ({placeholders})
          AND {where}
        GROUP BY cp.npi, cp.last_org_name, cp.first_name, cp.city, cp.state, ts.targeting_score
        ORDER BY total_interventional_services DESC
        LIMIT ?
    """, params).fetchall()

    columns = [
        "npi", "provider_name", "city", "state",
        "interventional_procedure_count", "total_interventional_services",
        "total_interventional_benes", "targeting_score",
    ]
    return [dict(zip(columns, row)) for row in rows]
