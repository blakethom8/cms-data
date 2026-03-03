"""Dataset configuration: UUIDs, URLs, column mappings, and acquisition methods."""

from dataclasses import dataclass, field
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "provider_searcher.duckdb"

# ── CMS API ───────────────────────────────────────────────────────────────────
CMS_API_BASE = "https://data.cms.gov/data-api/v1/dataset"
CMS_API_PAGE_SIZE = 5000


@dataclass
class DatasetConfig:
    """Configuration for a single CMS dataset."""

    name: str
    uuid: str
    acquisition: str  # 'csv' or 'api'
    description: str = ""
    csv_url: str | None = None
    raw_table: str = ""  # table name when loaded into DuckDB raw layer

    @property
    def api_url(self) -> str:
        return f"{CMS_API_BASE}/{self.uuid}/data"

    @property
    def csv_path(self) -> Path:
        return RAW_DIR / f"{self.name}.csv"

    def __post_init__(self):
        if not self.raw_table:
            self.raw_table = f"raw_{self.name}"


# ── Dataset Registry ──────────────────────────────────────────────────────────
DATASETS: dict[str, DatasetConfig] = {
    # ── #1  Physician by Provider (foundational) ──────────────────────────────
    "physician_by_provider": DatasetConfig(
        name="physician_by_provider",
        uuid="8889d81e-2ee7-448f-8713-f071038289b5",
        acquisition="csv",
        description="NPI + name + address + specialty + utilization + chronic conditions",
    ),
    # ── #2  Physician by Provider and Service ─────────────────────────────────
    "physician_by_provider_and_service": DatasetConfig(
        name="physician_by_provider_and_service",
        uuid="92396110-2aed-4d63-a6a2-5d6207d46a29",
        acquisition="csv",
        description="NPI + HCPCS drill-down for procedure-level targeting",
    ),
    # ── #3  Part D Prescribers by Provider ────────────────────────────────────
    "part_d_by_provider": DatasetConfig(
        name="part_d_by_provider",
        uuid="14d8e8a9-7e9b-4370-a044-bf97c46b4b44",
        acquisition="csv",
        description="NPI-level prescribing: total claims, drug cost, brand/generic split",
    ),
    # ── #4  Part D Prescribers by Provider and Drug ───────────────────────────
    "part_d_by_provider_and_drug": DatasetConfig(
        name="part_d_by_provider_and_drug",
        uuid="9552739e-3d05-4c1b-8eff-ecabf391e2e5",
        acquisition="csv",
        description="NPI + specific drug name and volume",
    ),
    # ── #5  Reassignment (Individual -> Group Practice) ───────────────────────
    "reassignment": DatasetConfig(
        name="reassignment",
        uuid="e1f1fa9a-d6b4-417e-948a-c72dead8a41c",
        acquisition="csv",
        description="Maps individual NPI -> group practice (PAC ID, size, state)",
    ),
    # ── #6  Quality Payment Program Experience ────────────────────────────────
    "qpp_experience": DatasetConfig(
        name="qpp_experience",
        uuid="7adb8b1b-b85c-4ed3-b314-064776e50180",
        acquisition="csv",
        description="MIPS scores, payment adjustments, rural/HPSA status",
    ),
    # ── #7  PECOS Enrollment ──────────────────────────────────────────────────
    "pecos_enrollment": DatasetConfig(
        name="pecos_enrollment",
        uuid="2457ea29-fc82-48b0-86ec-3b0755de7515",
        acquisition="csv",
        description="Authoritative enrollment validation, multiple_npi_flag",
    ),
    # ── #8  DME by Referring Provider ─────────────────────────────────────────
    "dme_by_referring_provider": DatasetConfig(
        name="dme_by_referring_provider",
        uuid="f8603e5b-9c47-4c52-9b47-a4ef92dfada4",
        acquisition="csv",
        description="NPI-level DME referral volume for device sales targeting",
    ),
    # ── #9  Order and Referring ────────────────────────────────────────────────
    "order_and_referring": DatasetConfig(
        name="order_and_referring",
        uuid="c99b5865-1119-4436-bb80-c5af2773ea1f",
        acquisition="csv",
        description="Eligibility flags: Part B / DME / HHA / Hospice ordering",
    ),
    # ── #10 Hospital Enrollments ──────────────────────────────────────────────
    "hospital_enrollments": DatasetConfig(
        name="hospital_enrollments",
        uuid="f6f6505c-e8b0-4d57-b258-e2b94133aaf2",
        acquisition="api",
        description="Hospital NPI, CCN, address, subgroup type (~8K rows)",
    ),
}


# ── Column Mappings ───────────────────────────────────────────────────────────
# Maps raw CMS column names -> our schema column names.

PHYSICIAN_BY_PROVIDER_COLUMNS = {
    "rndrng_npi": "npi",
    "rndrng_prvdr_last_org_name": "last_org_name",
    "rndrng_prvdr_first_name": "first_name",
    "rndrng_prvdr_mi": "middle_initial",
    "rndrng_prvdr_crdntls": "credentials",
    "rndrng_prvdr_ent_cd": "entity_type_code",
    "rndrng_prvdr_st1": "street_address_1",
    "rndrng_prvdr_st2": "street_address_2",
    "rndrng_prvdr_city": "city",
    "rndrng_prvdr_state_abrvtn": "state",
    "rndrng_prvdr_zip5": "zip5",
    "rndrng_prvdr_cntry": "country",
    "rndrng_prvdr_ruca": "ruca_code",
    "rndrng_prvdr_type": "provider_type",
    "rndrng_prvdr_mdcr_prtcptg_ind": "medicare_participating",
}

UTILIZATION_PART_B_COLUMNS = {
    "rndrng_npi": "npi",
    "tot_hcpcs_cds": "tot_hcpcs_codes",
    "tot_srvcs": "tot_services",
    "tot_benes": "tot_unique_beneficiaries",
    "tot_sbmtd_chrg": "tot_submitted_charges",
    "tot_mdcr_alowd_amt": "tot_medicare_allowed",
    "tot_mdcr_pymt_amt": "tot_medicare_payment",
    "tot_mdcr_stdzd_amt": "tot_medicare_standardized",
    "drug_tot_srvcs": "drug_services",
    "med_tot_srvcs": "medical_services",
    "bene_avg_age": "bene_avg_age",
    "bene_avg_risk_scre": "bene_avg_risk_score",
    "bene_dual_cnt": "bene_dual_eligible_count",
    "bene_cc_ph_diabetes_v2_pct": "cc_diabetes_pct",
    "bene_cc_ph_hypertension_v2_pct": "cc_hypertension_pct",
    "bene_cc_ph_hf_nonihd_v2_pct": "cc_heart_failure_pct",
    "bene_cc_ph_ckd_v2_pct": "cc_ckd_pct",
    "bene_cc_ph_copd_v2_pct": "cc_copd_pct",
    "bene_cc_ph_cancer6_v2_pct": "cc_cancer_pct",
    "bene_cc_bh_depress_v1_pct": "cc_depression_pct",
    "bene_cc_bh_alz_nonalzdem_v2_pct": "cc_alzheimers_pct",
    "bene_cc_ph_afib_v2_pct": "cc_atrial_fib_pct",
    "bene_cc_ph_hyperlipidemia_v2_pct": "cc_hyperlipidemia_pct",
    "bene_cc_ph_ischemicheart_v2_pct": "cc_ischemic_heart_pct",
    "bene_cc_ph_osteoporosis_v2_pct": "cc_osteoporosis_pct",
    "bene_cc_ph_arthritis_v2_pct": "cc_arthritis_pct",
    "bene_cc_ph_stroke_tia_v2_pct": "cc_stroke_tia_pct",
}

PART_D_PROVIDER_COLUMNS = {
    "prscrbr_npi": "npi",
    "tot_clms": "rx_total_claims",
    "tot_drug_cst": "rx_total_drug_cost",
    "brnd_tot_clms": "rx_brand_claims",
    "gnrc_tot_clms": "rx_generic_claims",
    "opioid_prscrbr_rate": "rx_opioid_prescriber_rate",
}

DME_REFERRING_COLUMNS = {
    "rfrg_npi": "npi",
    "tot_suplr_clms": "dme_total_claims",
    "suplr_mdcr_pymt_amt": "dme_medicare_payment",
}

REASSIGNMENT_COLUMNS = {
    "group pac id": "group_pac_id",
    "group enrollment id": "group_enrollment_id",
    "group legal business name": "group_legal_name",
    "group state code": "group_state",
    "group reassignments and physician assistants": "group_practice_size",
    "individual npi": "npi",
    "individual first name": "first_name",
    "individual last name": "last_name",
    "individual state code": "state",
    "individual specialty description": "specialty",
}

HOSPITAL_ENROLLMENT_COLUMNS = {
    "npi": "hospital_npi",
    "ccn": "hospital_ccn",
    "organization name": "hospital_name",
    "city": "hospital_city",
    "state": "hospital_state",
    "zip code": "hospital_zip",
    "enrollment id": "enrollment_id",
    "enrollment state": "enrollment_state",
}

# Hospital subgroup columns to check for affiliation type
HOSPITAL_SUBGROUP_COLUMNS = [
    "subgroup - acute care",
    "subgroup - psychiatric",
    "subgroup - rehabilitation",
    "subgroup - long-term",
    "subgroup - childrens",
    "subgroup - short-term",
    "subgroup - swing-bed approved",
    "subgroup - psychiatric unit",
    "subgroup - rehabilitation unit",
    "subgroup - specialty hospital",
]

QPP_COLUMNS = {
    "npi": "npi",
    "practice state or us territory": "practice_state",
    "practice size": "practice_size",
    "clinician type": "clinician_type",
    "clinician specialty": "clinician_specialty",
    "years in medicare": "years_in_medicare",
    "participation option": "participation_option",
    "small practice status": "small_practice_status",
    "rural status": "rural_status",
    "health professional shortage area status": "hpsa_status",
    "hospital-based status": "hospital_based_status",
    "facility-based status": "facility_based_status",
    "dual eligibility ratio": "dual_eligibility_ratio",
    "final score": "final_mips_score",
    "payment adjustment percentage": "payment_adjustment_pct",
    "complex patient bonus": "complex_patient_bonus",
    "quality category score": "quality_category_score",
    "quality category weight": "quality_category_weight",
    "promoting interoperability (pi) category score": "pi_category_score",
    "promoting interoperability (pi) category weight": "pi_category_weight",
    "improvement activities (ia) category score": "ia_category_score",
    "improvement activities (ia) category weight": "ia_category_weight",
    "cost category score": "cost_category_score",
    "cost category weight": "cost_category_weight",
}
