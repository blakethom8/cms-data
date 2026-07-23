"""
Data Explorer API — curated, read-only views into the CMS warehouse.

Powers the mydoclist /data-explorer dev page: a dataset catalog (with live row
counts), per-dataset sample rows, and "showcase" queries that demonstrate the
depth of each dataset for a metro area (default: Los Angeles, CA).

Everything here is WHITELISTED — no arbitrary SQL crosses this boundary. The
only user inputs are city/state and bounded sample sizes, all passed as bound
parameters. Full-row samples can select only physical tables named in CATALOG.
"""
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

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
        "grain": "one row per Type 1 individual NPI in the loaded subset",
        "description": "The loaded individual-provider subset of the NPI registry, with "
                       "taxonomy codes, credentials, and registered practice/mailing addresses. "
                       "Type 2 organization NPIs are not yet loaded into this table.",
        "join_keys": ["NPI"],
    },
    {
        "key": "pecos_enrollment",
        "table": "raw_pecos_enrollment",
        "title": "PECOS Public Provider Enrollment",
        "domain": "identity",
        "grain": "one Medicare enrollment record",
        "description": "CMS's quarterly public enrollment snapshot for Medicare "
                       "fee-for-service providers and organizations. Use the enrollment "
                       "ID to connect enrollment records; an enrollment is not evidence "
                       "of employment, a billing reassignment, or a primary practice site.",
        "join_keys": ["NPI", "ENRLMT_ID"],
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
        "grain": "one row per clinician × group reassignment record",
        "description": "Which group receives a clinician's reassigned Medicare benefits: "
                       "individual NPI → group PAC ID with the group's legal name and size. "
                       "Ordinary reassignment records do not establish employment.",
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
_col_counts: dict[str, int] = {}

# Per-dataset sample queries. City/state are bound params (%s style via ?);
# every query returns a curated, human-readable column subset.
SAMPLES: dict[str, str] = {
    "dac_national": """
        select "NPI" npi, "Provider First Name" first_name, "Provider Last Name" last_name,
               pri_spec specialty, "Facility Name" group_name, num_org_mem group_size,
               adr_ln_1 address, "City/Town" city, left(CAST("ZIP Code" AS VARCHAR),5) zip
        from raw_dac_national
        where "City/Town" ilike ? and "State" = ?""",
    "nppes": """
        select npi, last_name, first_name, credentials,
               taxonomy_1 primary_taxonomy, practice_address_1 address, practice_city city,
               practice_state state
        from raw_nppes
        where practice_city ilike ? and practice_state = ?""",
    "pecos_enrollment": """
        select "NPI" npi, "ENRLMT_ID" enrollment_id,
               coalesce(nullif("ORG_NAME", ''), concat_ws(' ', "FIRST_NAME", "MDL_NAME", "LAST_NAME")) provider_name,
               "PROVIDER_TYPE_DESC" provider_type, "STATE_CD" state
        from raw_pecos_enrollment
        where "STATE_CD" = ?""",
    "physician_by_provider": """
        select "Rndrng_NPI" npi, "Rndrng_Prvdr_Last_Org_Name" last_name,
               "Rndrng_Prvdr_Type" specialty, "Tot_Benes" beneficiaries,
               "Tot_Srvcs" services, round("Tot_Mdcr_Pymt_Amt") medicare_payments,
               "Bene_Avg_Age" avg_patient_age
        from raw_physician_by_provider
        where "Rndrng_Prvdr_City" ilike ? and "Rndrng_Prvdr_State_Abrvtn" = ?
        order by "Tot_Mdcr_Pymt_Amt" desc""",
    "physician_by_service": """
        select "Rndrng_NPI" npi, "Rndrng_Prvdr_Last_Org_Name" last_name,
               "HCPCS_Cd" hcpcs, left("HCPCS_Desc", 60) procedure_desc,
               "Place_Of_Srvc" place_of_service, "Tot_Srvcs" services,
               round("Avg_Mdcr_Pymt_Amt", 2) avg_payment
        from raw_physician_by_provider_and_service
        where "Rndrng_Prvdr_City" ilike ? and "Rndrng_Prvdr_State_Abrvtn" = ?
        order by "Tot_Srvcs" desc""",
    "part_d_by_drug": """
        select "Prscrbr_NPI" npi, "Prscrbr_Last_Org_Name" last_name,
               "Prscrbr_Type" specialty, "Brnd_Name" brand, "Gnrc_Name" generic,
               "Tot_Clms" claims, round("Tot_Drug_Cst") drug_cost
        from raw_part_d_by_provider_and_drug
        where "Prscrbr_City" ilike ? and "Prscrbr_State_Abrvtn" = ?
        order by "Tot_Drug_Cst" desc""",
    "open_payments_general": """
        select "Covered_Recipient_NPI" npi, "Covered_Recipient_Last_Name" last_name,
               "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name" manufacturer,
               "Nature_of_Payment_or_Transfer_of_Value" nature,
               round("Total_Amount_of_Payment_USDollars", 2) amount,
               left(coalesce("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1", ''), 40) product
        from raw_open_payments_general
        where "Recipient_City" ilike ? and "Recipient_State" = ?
        order by "Total_Amount_of_Payment_USDollars" desc""",
    "open_payments_research": """
        select "Covered_Recipient_NPI" npi, "Covered_Recipient_Last_Name" last_name,
               "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name" sponsor,
               left(coalesce("Name_of_Study", ''), 60) study,
               round("Total_Amount_of_Payment_USDollars", 2) amount
        from raw_open_payments_research
        where "Recipient_City" ilike ? and "Recipient_State" = ?
        order by "Total_Amount_of_Payment_USDollars" desc""",
    "open_payments_ownership": """
        select "Physician_NPI" npi, "Physician_Last_Name" last_name,
               left("Physician_Specialty", 50) specialty,
               round("Total_Amount_Invested_USDollars") invested,
               round(TRY_CAST("Value_of_Interest" AS DOUBLE)) value_of_interest
        from raw_open_payments_ownership
        where "Recipient_City" ilike ? and "Recipient_State" = ?
        order by "Total_Amount_Invested_USDollars" desc nulls last""",
    "reassignment": """
        select "Individual NPI" npi, "Individual First Name" first_name,
               "Individual Last Name" last_name, "Individual Specialty Description" specialty,
               "Group Legal Business Name" group_name,
               "Group Reassignments and Physician Assistants" group_size
        from raw_reassignment
        where "Individual State Code" = ? and "Group State Code" = ?""",
    "mips_performance": """
        select m."NPI" npi, m."Provider Last Name" last_name, d.pri_spec specialty,
               m."Quality_category_score" quality_score, m."Cost_category_score" cost_score,
               m."final_MIPS_score" final_score
        from raw_mips_performance m
        join raw_dac_national d on CAST(m."NPI" AS VARCHAR) = CAST(d."NPI" AS VARCHAR)
        where d."City/Town" ilike ? and d."State" = ?
        group by all""",
    "dme_referring": """
        select "Rfrg_NPI" npi, "Rfrg_Prvdr_Last_Name_Org" last_name,
               "Rfrg_Prvdr_Spclty_Desc" specialty, "Tot_Suplrs" suppliers,
               "Tot_Suplr_Clms" claims, round("Suplr_Mdcr_Pymt_Amt") dme_payments
        from raw_dme_by_referring_provider
        where "Rfrg_Prvdr_City" ilike ? and "Rfrg_Prvdr_State_Abrvtn" = ?
        order by "Suplr_Mdcr_Pymt_Amt" desc nulls last""",
    "address_geocode": """
        select street, city, state, zip5, round(lat, 5) lat, round(lng, 5) lng, match_type
        from address_geocode
        where city ilike ? and state = ? and lat is not null""",
}

# Reassignment has no city column — it binds (state, state) instead.
STATE_ONLY_SAMPLES = {"pecos_enrollment", "reassignment"}


# Provider-first source comparison. These are intentionally static, reviewed SQL
# fragments: callers may select NPIs and a bounded row limit, but never a table,
# column, or SQL expression.
PROVIDER_EVIDENCE_SOURCES: tuple[dict, ...] = (
    {
        "key": "nppes",
        "table": "raw_nppes",
        "title": "NPPES registry",
        "grain": "one row per NPI in the loaded registry subset",
        "relationship": "provider identity and registered primary practice address",
        "proves": "The NPI holder's registry identity, taxonomy, and registered location.",
        "does_not_prove": "Employment, billing-group membership, or care delivered at the address.",
        "sql": 'SELECT * FROM raw_nppes WHERE CAST(npi AS VARCHAR) = ?',
        "required_tables": ("raw_nppes",),
    },
    {
        "key": "dac_national",
        "table": "raw_dac_national",
        "title": "Doctors & Clinicians (DAC)",
        "grain": "one clinician enrollment × organization × practice address",
        "relationship": "Medicare clinician, organization, and practice-location association",
        "proves": "CMS publishes the clinician at this enrollment, organization PAC ID, and address grain.",
        "does_not_prove": "Employment, exclusive affiliation, or payment share by organization.",
        "sql": 'SELECT * FROM raw_dac_national WHERE CAST("NPI" AS VARCHAR) = ?',
        "required_tables": ("raw_dac_national",),
    },
    {
        "key": "revalidation_reassignment",
        "table": "raw_reassignment",
        "title": "Revalidation group reassignment",
        "grain": "one clinician × group reassignment record",
        "relationship": "individual clinician to Medicare group receiving reassigned benefits",
        "proves": "A published Medicare reassignment or physician-assistant employment association.",
        "does_not_prove": "A primary employer, exclusive group, or exact practice site.",
        "sql": 'SELECT * FROM raw_reassignment WHERE CAST("Individual NPI" AS VARCHAR) = ?',
        "required_tables": ("raw_reassignment",),
    },
    {
        "key": "pecos_enrollment",
        "table": "raw_pecos_enrollment",
        "title": "PECOS public enrollment",
        "grain": "one Medicare enrollment record",
        "relationship": "clinician or organization Medicare enrollment identity",
        "proves": "The provider's current public Medicare enrollment identifiers and enrollment attributes.",
        "does_not_prove": "Which organization receives this clinician's reassigned benefits.",
        "sql": 'SELECT * FROM raw_pecos_enrollment WHERE CAST("NPI" AS VARCHAR) = ?',
        "required_tables": ("raw_pecos_enrollment",),
    },
    {
        "key": "ppef_reassignment",
        "table": "raw_pecos_reassignment",
        "title": "PPEF reassignment",
        "grain": "one reassigning enrollment × receiving enrollment relationship",
        "relationship": "clinician enrollment to organization enrollment receiving benefits",
        "proves": "The explicit PPEF benefit-reassignment link between two Medicare enrollments.",
        "does_not_prove": "Employment, exclusivity, or which receiving location is primary.",
        "sql": """
            SELECT r.*
            FROM raw_pecos_reassignment r
            JOIN raw_pecos_enrollment e
              ON CAST(e.ENRLMT_ID AS VARCHAR) = CAST(r.REASGN_BNFT_ENRLMT_ID AS VARCHAR)
            WHERE CAST(e.NPI AS VARCHAR) = ?
        """,
        "required_tables": ("raw_pecos_reassignment", "raw_pecos_enrollment"),
    },
    {
        "key": "ppef_practice_location",
        "table": "raw_pecos_practice_location",
        "title": "PPEF receiving practice locations",
        "grain": "one receiving Medicare enrollment × practice location",
        "relationship": "practice locations attached to the organization receiving reassigned benefits",
        "proves": "The receiving enrollment's published Medicare practice locations.",
        "does_not_prove": "Which location the clinician personally uses most often.",
        "sql": """
            SELECT p.*
            FROM raw_pecos_practice_location p
            JOIN raw_pecos_reassignment r
              ON CAST(r.RCV_BNFT_ENRLMT_ID AS VARCHAR) = CAST(p.ENRLMT_ID AS VARCHAR)
            JOIN raw_pecos_enrollment e
              ON CAST(e.ENRLMT_ID AS VARCHAR) = CAST(r.REASGN_BNFT_ENRLMT_ID AS VARCHAR)
            WHERE CAST(e.NPI AS VARCHAR) = ?
        """,
        "required_tables": (
            "raw_pecos_practice_location",
            "raw_pecos_reassignment",
            "raw_pecos_enrollment",
        ),
    },
    {
        "key": "curated_pecos_organization_bridge",
        "table": "pecos_provider_organizations",
        "title": "Curated PECOS provider–organization bridge",
        "grain": "one provider enrollment × receiving enrollment relationship",
        "relationship": "provider NPI to the enrollment receiving reassigned Medicare benefits",
        "proves": "How the datamart resolves PPEF enrollment keys to provider and receiving-organization attributes.",
        "does_not_prove": "Employment, exclusivity, a primary billing organization, or payment share.",
        "sql": "SELECT * FROM pecos_provider_organizations WHERE CAST(npi AS VARCHAR) = ?",
        "required_tables": ("pecos_provider_organizations",),
        "layer": "curated",
        "evidence_kind": "derived",
    },
    {
        "key": "curated_pecos_location_bridge",
        "table": "pecos_enrollment_practice_locations",
        "title": "Curated PECOS receiving-location bridge",
        "grain": "one provider-to-receiving-enrollment relationship × receiving location",
        "relationship": "provider NPI to practice locations published for the receiving enrollment",
        "proves": "How the datamart joins PPEF benefit reassignment to the receiving enrollment's locations.",
        "does_not_prove": "The clinician's primary location or the site where a specific service was rendered.",
        "sql": """
            SELECT r.npi, l.receiving_enrollment_id,
                   l.receiving_organization_name, l.city, l.state, l.zip_code
            FROM pecos_provider_organizations r
            JOIN pecos_enrollment_practice_locations l
              ON l.receiving_enrollment_id = r.receiving_enrollment_id
            WHERE CAST(r.npi AS VARCHAR) = ?
        """,
        "required_tables": (
            "pecos_provider_organizations",
            "pecos_enrollment_practice_locations",
        ),
        "layer": "curated",
        "evidence_kind": "derived",
    },
    {
        "key": "medicare_provider_year",
        "table": "raw_physician_by_provider",
        "title": "Medicare utilization by provider",
        "grain": "one rendering provider × source year",
        "relationship": "provider-level Medicare services and payment totals",
        "proves": "The clinician's published annual Medicare utilization totals.",
        "does_not_prove": "Which organization received those dollars.",
        "sql": 'SELECT * FROM raw_physician_by_provider WHERE CAST("Rndrng_NPI" AS VARCHAR) = ?',
        "required_tables": ("raw_physician_by_provider",),
    },
    {
        "key": "facility_affiliation",
        "table": "raw_dac_facility_affiliations",
        "title": "CMS facility affiliations",
        "grain": "one clinician × facility certification relationship",
        "relationship": "clinician affiliation to a CMS-certified facility",
        "proves": "CMS publishes a facility affiliation for the clinician.",
        "does_not_prove": "Billing-group membership, employment, or hospital privileges beyond the published relationship.",
        "sql": 'SELECT * FROM raw_dac_facility_affiliations WHERE CAST("NPI" AS VARCHAR) = ?',
        "required_tables": ("raw_dac_facility_affiliations",),
    },
    {
        "key": "curated_practice_bridge",
        "table": "practice_locations",
        "title": "Curated provider–practice bridge",
        "grain": "one clinician × group relationship × warehouse year",
        "relationship": "warehouse-normalized group relationship from revalidation data",
        "proves": "How the current datamart represents each published clinician-to-group relationship.",
        "does_not_prove": "A true primary location; the current primary flag is a largest-group selection heuristic.",
        "sql": "SELECT * FROM practice_locations WHERE CAST(npi AS VARCHAR) = ?",
        "required_tables": ("practice_locations",),
        "layer": "curated",
        "evidence_kind": "derived",
    },
    {
        "key": "curated_hospital_bridge",
        "table": "hospital_affiliations",
        "title": "Curated hospital-affiliation bridge",
        "grain": "one clinician × inferred hospital relationship",
        "relationship": "warehouse-inferred hospital association from group reassignment and hospital enrollment",
        "proves": "That the warehouse inference rule found one unambiguous hospital match.",
        "does_not_prove": "Publisher-asserted hospital privileges, employment, or exclusive affiliation.",
        "sql": "SELECT * FROM hospital_affiliations WHERE CAST(npi AS VARCHAR) = ?",
        "required_tables": ("hospital_affiliations",),
        "layer": "curated",
        "evidence_kind": "inferred",
    },
)

DEFAULT_PROVIDER_EVIDENCE_NPIS: tuple[str, ...] = (
    "1710390513",  # Lauren DeStefano
    "1962509216",  # Robert Vescio
    "1740218155",  # Joshua Scott
    "1659383891",  # Jonathan Weiner
)
NPI_PATTERN = re.compile(r"^[0-9]{10}$")

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
    column_count: int


class ColumnInfo(BaseModel):
    name: str
    type: str


class ColumnsResponse(BaseModel):
    key: str
    table: str
    columns: list[ColumnInfo]


class ShowcaseResult(BaseModel):
    key: str
    title: str
    question: str
    dataset: str
    city: str
    state: str
    data: TableData


class ProviderEvidenceSource(BaseModel):
    key: str
    table: str
    title: str
    grain: str
    relationship: str
    proves: str
    does_not_prove: str
    layer: str
    evidence_kind: str
    availability: str
    missing_tables: list[str]
    providers: dict[str, TableData]


class ProviderEvidenceResponse(BaseModel):
    npis: list[str]
    sources: list[ProviderEvidenceSource]


def _run(conn, sql: str, params: list, limit: int = 50) -> TableData:
    bounded_sql = f"SELECT * FROM ({sql.strip().rstrip(';')}) AS sample_rows LIMIT ?"
    cur = conn.execute(bounded_sql, [*params, limit])
    cols = [d[0] for d in cur.description]
    rows = [list(r) for r in cur.fetchall()]
    return TableData(columns=cols, rows=rows)


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _run_json_safe(conn, sql: str, params: list, limit: int = 50) -> TableData:
    """Return raw rows while keeping timezone values portable in minimal runtimes."""
    schema_cursor = conn.execute(
        f"SELECT * FROM ({sql.strip().rstrip(';')}) AS provider_rows LIMIT 0",
        params,
    )
    projection = []
    for description in schema_cursor.description:
        name = description[0]
        identifier = _quoted_identifier(name)
        if "TIME ZONE" in str(description[1]).upper():
            projection.append(f"CAST({identifier} AS VARCHAR) AS {identifier}")
        else:
            projection.append(identifier)
    projected_sql = (
        f"SELECT {', '.join(projection)} "
        f"FROM ({sql.strip().rstrip(';')}) AS provider_rows"
    )
    return _run(conn, projected_sql, params, limit)


def _physical_tables(conn) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }


def get_explorer_router(get_conn):
    router = APIRouter(prefix="/explorer", tags=["Data Explorer"])

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
            if table not in _col_counts:
                _col_counts[table] = len(conn.execute(f"pragma table_info('{table}')").fetchall())
            out.append(CatalogEntry(**entry, row_count=_row_counts[table],
                                    column_count=_col_counts[table]))
        return out

    @router.get("/columns/{key}", response_model=ColumnsResponse)
    async def columns(key: str):
        """Full column list (name + type) for one catalog dataset."""
        entry = next((e for e in CATALOG if e["key"] == key), None)
        if not entry:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{key}'")
        rows = get_conn().execute(f"pragma table_info('{entry['table']}')").fetchall()
        return ColumnsResponse(
            key=key, table=entry["table"],
            columns=[ColumnInfo(name=r[1].strip(), type=r[2]) for r in rows],
        )

    @router.get("/sample/{key}", response_model=TableData)
    async def sample(
        key: str,
        city: str = "Los Angeles",
        state: str = "CA",
        limit: int = Query(50, ge=1, le=200),
    ):
        sql = SAMPLES.get(key)
        if not sql:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{key}'")
        params = [state.upper(), state.upper()] if key in STATE_ONLY_SAMPLES else [city, state.upper()]
        try:
            return _run(get_conn(), sql, params, limit)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Sample query failed: {e}")

    @router.get("/sample-all/{key}", response_model=TableData)
    async def sample_all(key: str, limit: int = Query(50, ge=1, le=200)):
        """Return bounded raw rows containing every physical column for a catalog table."""
        entry = next((entry for entry in CATALOG if entry["key"] == key), None)
        if not entry:
            raise HTTPException(status_code=404, detail=f"Unknown dataset '{key}'")
        table = entry["table"]
        try:
            return _run(get_conn(), f'SELECT * FROM "{table}"', [], limit)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"All-column sample query failed: {e}")

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

    @router.get("/provider-evidence", response_model=ProviderEvidenceResponse)
    async def provider_evidence(
        npis: str = ",".join(DEFAULT_PROVIDER_EVIDENCE_NPIS),
        limit: int = Query(10, ge=1, le=25),
    ):
        """Compare bounded, source-faithful rows for the same providers.

        The endpoint is deliberately provider-first and read-only. Dataset SQL,
        joins, and relationship descriptions are server-owned; the caller can
        provide only up to ten valid NPIs and a bounded per-source row limit.
        Missing optional source tables are reported as unavailable so a staged
        PPEF rollout remains inspectable before and after promotion.
        """
        requested_npis = list(dict.fromkeys(part.strip() for part in npis.split(",") if part.strip()))
        if not requested_npis or len(requested_npis) > 10:
            raise HTTPException(status_code=422, detail="Provide between 1 and 10 NPIs")
        invalid_npis = [npi for npi in requested_npis if not NPI_PATTERN.fullmatch(npi)]
        if invalid_npis:
            raise HTTPException(status_code=422, detail=f"Invalid NPI value: {invalid_npis[0]}")

        conn = get_conn()
        available_tables = _physical_tables(conn)
        sources: list[ProviderEvidenceSource] = []

        for source in PROVIDER_EVIDENCE_SOURCES:
            missing_tables = [table for table in source["required_tables"] if table not in available_tables]
            provider_rows: dict[str, TableData] = {}
            availability = "unavailable" if missing_tables else "available"

            if not missing_tables:
                try:
                    for npi in requested_npis:
                        provider_rows[npi] = _run_json_safe(conn, source["sql"], [npi], limit)
                except Exception as error:
                    availability = "query_error"
                    provider_rows = {}
                    missing_tables = [f"Query failed: {error}"]

            sources.append(
                ProviderEvidenceSource(
                    key=source["key"],
                    table=source["table"],
                    title=source["title"],
                    grain=source["grain"],
                    relationship=source["relationship"],
                    proves=source["proves"],
                    does_not_prove=source["does_not_prove"],
                    layer=source.get("layer", "raw"),
                    evidence_kind=source.get("evidence_kind", "publisher_asserted"),
                    availability=availability,
                    missing_tables=missing_tables,
                    providers=provider_rows,
                )
            )

        return ProviderEvidenceResponse(npis=requested_npis, sources=sources)

    return router
