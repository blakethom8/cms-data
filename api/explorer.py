"""
Data Explorer API — curated, read-only views into the CMS warehouse.

Powers the mydoclist /data-explorer dev page: a dataset catalog (with live row
counts), per-dataset sample rows, and "showcase" queries that demonstrate the
depth of each dataset for a metro area (default: Los Angeles, CA).

Everything here is WHITELISTED — no arbitrary SQL crosses this boundary. The
only user inputs are city/state, always passed as bound parameters.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/explorer", tags=["Data Explorer"])

# --------------------------------------------------------------------------
# Dataset catalog — the curated map of the warehouse.
# --------------------------------------------------------------------------

CATALOG: list[dict] = [
    {
        "key": "dac_national",
        "table": "raw_dac_national",
        "title": "Doctors & Clinicians (DAC)",
        "domain": "identity",
        "grain": "one row per clinician × practice address",
        "description": "CMS's clinician directory: every Medicare clinician with name, "
                       "specialty, practice addresses, phone, and their group "
                       "(org_pac_id + group size). The bridge between people, places, and orgs. "
                       "NOTE: hospital systems anchor most clinicians at the flagship address.",
        "join_keys": ["NPI", "org_pac_id", "address"],
    },
    {
        "key": "nppes",
        "table": "raw_nppes",
        "title": "NPPES (NPI Registry)",
        "domain": "identity",
        "grain": "one row per NPI (individuals + organizations)",
        "description": "The master NPI registry: 7M+ providers/orgs with taxonomy codes, "
                       "credentials, and practice/mailing addresses. The universe of NPIs.",
        "join_keys": ["NPI"],
    },
    {
        "key": "physician_by_provider",
        "table": "raw_physician_by_provider",
        "title": "Medicare Utilization (per provider)",
        "domain": "money",
        "grain": "one row per NPI per year",
        "description": "Annual Medicare volume per clinician: total services, unique "
                       "beneficiaries, payments, plus patient demographics and chronic-condition mix.",
        "join_keys": ["Rndrng_NPI"],
    },
    {
        "key": "physician_by_service",
        "table": "raw_physician_by_provider_and_service",
        "title": "Procedures (per provider × HCPCS)",
        "domain": "money",
        "grain": "one row per NPI × procedure code × place of service",
        "description": "The procedure-level detail: which HCPCS/CPT codes each clinician "
                       "bills, how often, at what average payment, split by facility (F) vs "
                       "office (O) site of service.",
        "join_keys": ["Rndrng_NPI", "HCPCS_Cd"],
    },
    {
        "key": "part_d_by_drug",
        "table": "raw_part_d_by_provider_and_drug",
        "title": "Part D Prescribing (per provider × drug)",
        "domain": "rx",
        "grain": "one row per prescriber × drug",
        "description": "Drug-level prescribing: brand + generic name, claim counts, day "
                       "supply, and total drug cost per prescriber. 26M+ rows — the "
                       "pharma-targeting goldmine.",
        "join_keys": ["Prscrbr_NPI", "Brnd_Name/Gnrc_Name"],
    },
    {
        "key": "open_payments_general",
        "table": "raw_open_payments_general",
        "title": "Open Payments — General (Sunshine Act)",
        "domain": "industry",
        "grain": "one row per payment (manufacturer → clinician)",
        "description": "Every industry payment to a clinician: manufacturer, dollar amount, "
                       "nature (consulting, speaking, food, travel...), and the drug/device "
                       "associated. 14.7M payments.",
        "join_keys": ["Covered_Recipient_NPI"],
    },
    {
        "key": "open_payments_research",
        "table": "raw_open_payments_research",
        "title": "Open Payments — Research",
        "domain": "industry",
        "grain": "one row per research payment",
        "description": "Industry research funding: sponsor, study context, and principal "
                       "investigators. Identifies clinical-trial-active physicians.",
        "join_keys": ["Covered_Recipient_NPI"],
    },
    {
        "key": "open_payments_ownership",
        "table": "raw_open_payments_ownership",
        "title": "Open Payments — Ownership",
        "domain": "industry",
        "grain": "one row per physician ownership/investment interest",
        "description": "Physician ownership stakes in manufacturers/GPOs: amount invested "
                       "and value of interest. A small but potent conflict-of-interest signal.",
        "join_keys": ["Physician_NPI"],
    },
    {
        "key": "reassignment",
        "table": "raw_reassignment",
        "title": "Reassignment (clinician → group)",
        "domain": "org",
        "grain": "one row per clinician × group employment",
        "description": "Who bills through which group: individual NPI → group PAC ID with "
                       "the group's legal name and total size. The org-chart of Medicare.",
        "join_keys": ["Individual NPI", "Group PAC ID"],
    },
    {
        "key": "mips_performance",
        "table": "raw_mips_performance",
        "title": "MIPS Quality Scores",
        "domain": "quality",
        "grain": "one row per NPI (per program year)",
        "description": "Merit-based Incentive Payment System: quality / promoting-"
                       "interoperability / improvement / cost category scores and the final "
                       "MIPS score per clinician.",
        "join_keys": ["NPI", "Org_PAC_ID"],
    },
    {
        "key": "dme_referring",
        "table": "raw_dme_by_referring_provider",
        "title": "DME Referrals",
        "domain": "money",
        "grain": "one row per referring provider",
        "description": "Durable medical equipment ordering: which clinicians refer DME, "
                       "supplier counts, claims, and Medicare payments. Device-targeting signal.",
        "join_keys": ["Rfrg_NPI"],
    },
    {
        "key": "address_geocode",
        "table": "address_geocode",
        "title": "Address Geocodes (ours)",
        "domain": "geo",
        "grain": "one row per distinct practice address",
        "description": "Our derived table: 233K practice addresses geocoded via the US "
                       "Census API (88% coverage). Enables proximity search and mapping.",
        "join_keys": ["addr_key = street|zip5"],
    },
]

_row_counts: dict[str, int] = {}

# Per-dataset sample queries. City/state are bound params (%s style via ?);
# every query returns a curated, human-readable column subset.
SAMPLES: dict[str, str] = {
    "dac_national": """
        select "NPI" npi, "Provider First Name" first_name, "Provider Last Name" last_name,
               pri_spec specialty, "Facility Name" group_name, num_org_mem group_size,
               adr_ln_1 address, "City/Town" city, left(CAST("ZIP Code" AS VARCHAR),5) zip
        from raw_dac_national
        where "City/Town" ilike ? and "State" = ? limit 8""",
    "nppes": """
        select npi, last_name, first_name, credentials,
               taxonomy_1 primary_taxonomy, practice_address_1 address, practice_city city,
               practice_state state
        from raw_nppes
        where practice_city ilike ? and practice_state = ? limit 8""",
    "physician_by_provider": """
        select "Rndrng_NPI" npi, "Rndrng_Prvdr_Last_Org_Name" last_name,
               "Rndrng_Prvdr_Type" specialty, "Tot_Benes" beneficiaries,
               "Tot_Srvcs" services, round("Tot_Mdcr_Pymt_Amt") medicare_payments,
               "Bene_Avg_Age" avg_patient_age
        from raw_physician_by_provider
        where "Rndrng_Prvdr_City" ilike ? and "Rndrng_Prvdr_State_Abrvtn" = ?
        order by "Tot_Mdcr_Pymt_Amt" desc limit 8""",
    "physician_by_service": """
        select "Rndrng_NPI" npi, "Rndrng_Prvdr_Last_Org_Name" last_name,
               "HCPCS_Cd" hcpcs, left("HCPCS_Desc", 60) procedure_desc,
               "Place_Of_Srvc" place_of_service, "Tot_Srvcs" services,
               round("Avg_Mdcr_Pymt_Amt", 2) avg_payment
        from raw_physician_by_provider_and_service
        where "Rndrng_Prvdr_City" ilike ? and "Rndrng_Prvdr_State_Abrvtn" = ?
        order by "Tot_Srvcs" desc limit 8""",
    "part_d_by_drug": """
        select "Prscrbr_NPI" npi, "Prscrbr_Last_Org_Name" last_name,
               "Prscrbr_Type" specialty, "Brnd_Name" brand, "Gnrc_Name" generic,
               "Tot_Clms" claims, round("Tot_Drug_Cst") drug_cost
        from raw_part_d_by_provider_and_drug
        where "Prscrbr_City" ilike ? and "Prscrbr_State_Abrvtn" = ?
        order by "Tot_Drug_Cst" desc limit 8""",
    "open_payments_general": """
        select "Covered_Recipient_NPI" npi, "Covered_Recipient_Last_Name" last_name,
               "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name" manufacturer,
               "Nature_of_Payment_or_Transfer_of_Value" nature,
               round("Total_Amount_of_Payment_USDollars", 2) amount,
               left(coalesce("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1", ''), 40) product
        from raw_open_payments_general
        where "Recipient_City" ilike ? and "Recipient_State" = ?
        order by "Total_Amount_of_Payment_USDollars" desc limit 8""",
    "open_payments_research": """
        select "Covered_Recipient_NPI" npi, "Covered_Recipient_Last_Name" last_name,
               "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name" sponsor,
               left(coalesce("Name_of_Study", ''), 60) study,
               round("Total_Amount_of_Payment_USDollars", 2) amount
        from raw_open_payments_research
        where "Recipient_City" ilike ? and "Recipient_State" = ?
        order by "Total_Amount_of_Payment_USDollars" desc limit 8""",
    "open_payments_ownership": """
        select "Physician_NPI" npi, "Physician_Last_Name" last_name,
               left("Physician_Specialty", 50) specialty,
               round("Total_Amount_Invested_USDollars") invested,
               round(TRY_CAST("Value_of_Interest" AS DOUBLE)) value_of_interest
        from raw_open_payments_ownership
        where "Recipient_City" ilike ? and "Recipient_State" = ?
        order by "Total_Amount_Invested_USDollars" desc nulls last limit 8""",
    "reassignment": """
        select "Individual NPI" npi, "Individual First Name" first_name,
               "Individual Last Name" last_name, "Individual Specialty Description" specialty,
               "Group Legal Business Name" group_name,
               "Group Reassignments and Physician Assistants" group_size
        from raw_reassignment
        where "Individual State Code" = ? and "Group State Code" = ? limit 8""",
    "mips_performance": """
        select m."NPI" npi, m."Provider Last Name" last_name, d.pri_spec specialty,
               m."Quality_category_score" quality_score, m."Cost_category_score" cost_score,
               m."final_MIPS_score" final_score
        from raw_mips_performance m
        join raw_dac_national d on CAST(m."NPI" AS VARCHAR) = CAST(d."NPI" AS VARCHAR)
        where d."City/Town" ilike ? and d."State" = ?
        group by all limit 8""",
    "dme_referring": """
        select "Rfrg_NPI" npi, "Rfrg_Prvdr_Last_Name_Org" last_name,
               "Rfrg_Prvdr_Spclty_Desc" specialty, "Tot_Suplrs" suppliers,
               "Tot_Suplr_Clms" claims, round("Suplr_Mdcr_Pymt_Amt") dme_payments
        from raw_dme_by_referring_provider
        where "Rfrg_Prvdr_City" ilike ? and "Rfrg_Prvdr_State_Abrvtn" = ?
        order by "Suplr_Mdcr_Pymt_Amt" desc nulls last limit 8""",
    "address_geocode": """
        select street, city, state, zip5, round(lat, 5) lat, round(lng, 5) lng, match_type
        from address_geocode
        where city ilike ? and state = ? and lat is not null limit 8""",
}

# Reassignment has no city column — it binds (state, state) instead.
STATE_ONLY_SAMPLES = {"reassignment"}

# --------------------------------------------------------------------------
# Showcases — "what can this data answer?" for a metro.
# --------------------------------------------------------------------------

SHOWCASES: dict[str, dict] = {
    "top_drugs": {
        "title": "Top drugs by Medicare spend — cardiologists",
        "question": "What do this metro's cardiologists actually prescribe, and what does it cost?",
        "dataset": "part_d_by_drug",
        "sql": """
            select "Brnd_Name" brand, "Gnrc_Name" generic,
                   count(distinct "Prscrbr_NPI") prescribers,
                   sum("Tot_Clms") claims, round(sum("Tot_Drug_Cst")) total_cost
            from raw_part_d_by_provider_and_drug
            where "Prscrbr_City" ilike ? and "Prscrbr_State_Abrvtn" = ?
              and "Prscrbr_Type" ilike '%cardio%'
            group by 1, 2 order by total_cost desc limit 10""",
    },
    "top_procedures": {
        "title": "Top procedures (HCPCS) — cardiologists",
        "question": "Which procedures dominate, and are they done in facilities (F) or offices (O)?",
        "dataset": "physician_by_service",
        "sql": """
            select "HCPCS_Cd" code, left(any_value("HCPCS_Desc"), 55) procedure_desc,
                   "Place_Of_Srvc" pos, count(distinct "Rndrng_NPI") clinicians,
                   sum("Tot_Srvcs") services, round(avg("Avg_Mdcr_Pymt_Amt"), 2) avg_payment
            from raw_physician_by_provider_and_service
            where "Rndrng_Prvdr_City" ilike ? and "Rndrng_Prvdr_State_Abrvtn" = ?
              and "Rndrng_Prvdr_Type" ilike '%cardio%'
            group by 1, 3 order by services desc limit 10""",
    },
    "pharma_by_manufacturer": {
        "title": "Industry money by manufacturer (Sunshine Act)",
        "question": "Which pharma/device companies spend the most on this metro's clinicians?",
        "dataset": "open_payments_general",
        "sql": """
            select "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name" manufacturer,
                   count(*) payments, count(distinct "Covered_Recipient_NPI") clinicians,
                   round(sum("Total_Amount_of_Payment_USDollars")) total
            from raw_open_payments_general
            where "Recipient_City" ilike ? and "Recipient_State" = ?
            group by 1 order by total desc limit 10""",
    },
    "payment_nature": {
        "title": "Industry money by payment type",
        "question": "Lunches vs consulting vs speaking — how deep do industry ties run?",
        "dataset": "open_payments_general",
        "sql": """
            select "Nature_of_Payment_or_Transfer_of_Value" nature, count(*) payments,
                   count(distinct "Covered_Recipient_NPI") clinicians,
                   round(sum("Total_Amount_of_Payment_USDollars")) total,
                   round(avg("Total_Amount_of_Payment_USDollars"), 2) avg_payment
            from raw_open_payments_general
            where "Recipient_City" ilike ? and "Recipient_State" = ?
            group by 1 order by total desc limit 10""",
    },
    "biggest_groups": {
        "title": "Largest cardiology groups",
        "question": "Which organizations employ the most cardiologists here?",
        "dataset": "dac_national",
        "sql": """
            select any_value("Facility Name") group_name,
                   count(distinct "NPI") cardiologists,
                   max(num_org_mem) total_group_size
            from raw_dac_national
            where "City/Town" ilike ? and "State" = ?
              and pri_spec ilike '%cardio%' and org_pac_id is not null
            group by org_pac_id order by cardiologists desc limit 10""",
    },
    "ownership_stakes": {
        "title": "Physician ownership stakes (Sunshine Act)",
        "question": "Which physicians hold investment interests in manufacturers?",
        "dataset": "open_payments_ownership",
        "sql": """
            select left("Physician_Specialty", 55) specialty, count(*) interests,
                   round(sum("Total_Amount_Invested_USDollars")) invested
            from raw_open_payments_ownership
            where "Recipient_City" ilike ? and "Recipient_State" = ?
            group by 1 order by invested desc nulls last limit 10""",
    },
    "top_earners": {
        "title": "Highest-billing clinicians (all specialties)",
        "question": "Who bills Medicare the most in this metro, and for how many patients?",
        "dataset": "physician_by_provider",
        "sql": """
            select "Rndrng_NPI" npi, "Rndrng_Prvdr_Last_Org_Name" last_name,
                   "Rndrng_Prvdr_Type" specialty, "Tot_Benes" patients,
                   "Tot_Srvcs" services, round("Tot_Mdcr_Pymt_Amt") medicare_payments
            from raw_physician_by_provider
            where "Rndrng_Prvdr_City" ilike ? and "Rndrng_Prvdr_State_Abrvtn" = ?
            order by "Tot_Mdcr_Pymt_Amt" desc limit 10""",
    },
}


class TableData(BaseModel):
    columns: list[str]
    rows: list[list]


class CatalogEntry(BaseModel):
    key: str
    table: str
    title: str
    domain: str
    grain: str
    description: str
    join_keys: list[str]
    row_count: int


class ShowcaseResult(BaseModel):
    key: str
    title: str
    question: str
    dataset: str
    city: str
    state: str
    data: TableData


def _run(conn, sql: str, params: list) -> TableData:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [list(r) for r in cur.fetchmany(50)]
    return TableData(columns=cols, rows=rows)


def get_explorer_router(get_conn):
    @router.get("/catalog", response_model=list[CatalogEntry])
    async def catalog():
        conn = get_conn()
        out = []
        for entry in CATALOG:
            table = entry["table"]
            if table not in _row_counts:
                _row_counts[table] = conn.execute(
                    f'select count(*) from "{table}"'
                ).fetchone()[0]
            out.append(CatalogEntry(**entry, row_count=_row_counts[table]))
        return out

    @router.get("/sample/{key}", response_model=TableData)
    async def sample(key: str, city: str = "Los Angeles", state: str = "CA"):
        sql = SAMPLES.get(key)
        if not sql:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{key}'")
        params = [state.upper(), state.upper()] if key in STATE_ONLY_SAMPLES else [city, state.upper()]
        try:
            return _run(get_conn(), sql, params)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Sample query failed: {e}")

    @router.get("/showcase/{key}", response_model=ShowcaseResult)
    async def showcase(key: str, city: str = "Los Angeles", state: str = "CA"):
        sc = SHOWCASES.get(key)
        if not sc:
            raise HTTPException(status_code=404, detail=f"Unknown showcase '{key}'")
        try:
            data = _run(get_conn(), sc["sql"], [city, state.upper()])
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Showcase query failed: {e}")
        return ShowcaseResult(
            key=key, title=sc["title"], question=sc["question"],
            dataset=sc["dataset"], city=city, state=state.upper(), data=data,
        )

    @router.get("/showcases")
    async def showcases_index():
        return [
            {"key": k, "title": v["title"], "question": v["question"], "dataset": v["dataset"]}
            for k, v in SHOWCASES.items()
        ]

    return router
