"""Explicit contracts for the California Tableau reporting replica.

The reporting replica intentionally publishes both relationship-safe analytical models
and source-faithful detail tables.  DuckDB remains canonical; these contracts only
describe a downstream, read-only export.
"""

from __future__ import annotations

from dataclasses import dataclass

from .source_registry import CMS_ATTRIBUTION, NPPES_ATTRIBUTION, SOURCE_REGISTRY

REPORTING_SCOPE = "California"
REPORTING_STATE = "CA"
REPORTING_CONTRACT_VERSION = 1


@dataclass(frozen=True, slots=True)
class FieldContract:
    name: str
    expression: str
    source_dataset_id: str
    source_table: str
    source_column: str
    transformation: str = "Direct projection"
    derived: bool = False
    inferred: bool = False


@dataclass(frozen=True, slots=True)
class ReportingModel:
    name: str
    grain: str
    scope_rule: str
    source_tables: tuple[str, ...]
    key_columns: tuple[str, ...]
    from_sql: str
    fields: tuple[FieldContract, ...]
    notes: str = ""

    @property
    def query(self) -> str:
        select_list = ",\n            ".join(
            f"{field.expression} AS \"{field.name}\"" for field in self.fields
        )
        return f"SELECT\n            {select_list}\n        {self.from_sql}"


@dataclass(frozen=True, slots=True)
class SourceDetailModel:
    name: str
    source_dataset_id: str
    source_table: str
    grain: str
    scope_rule: str
    predicate_sql: str
    source_period_semantics: str
    attribution: str
    notes: str = ""


def _field(
    name: str,
    expression: str,
    source_dataset_id: str,
    source_table: str,
    source_column: str,
    transformation: str = "Direct projection",
    *,
    derived: bool = False,
    inferred: bool = False,
) -> FieldContract:
    return FieldContract(
        name=name,
        expression=expression,
        source_dataset_id=source_dataset_id,
        source_table=source_table,
        source_column=source_column,
        transformation=transformation,
        derived=derived,
        inferred=inferred,
    )


CORE_SOURCE = "cms_physician_by_provider + nppes_monthly_v2"

REPORTING_MODELS: tuple[ReportingModel, ...] = (
    ReportingModel(
        name="dim_provider",
        grain="one current individual provider NPI",
        scope_rule="core_providers.state = 'CA'",
        source_tables=("core_providers",),
        key_columns=("npi",),
        from_sql="FROM core_providers cp WHERE UPPER(cp.state) = 'CA'",
        notes=(
            "Curated provider identity. Medicare providers are enriched from NPPES; "
            "non-Medicare NPPES Type 1 providers can also be present."
        ),
        fields=(
            _field("npi", "CAST(cp.npi AS VARCHAR)", CORE_SOURCE, "core_providers", "npi"),
            _field("last_or_organization_name", "cp.last_org_name", CORE_SOURCE, "core_providers", "last_org_name"),
            _field("first_name", "cp.first_name", CORE_SOURCE, "core_providers", "first_name"),
            _field("middle_initial", "cp.middle_initial", CORE_SOURCE, "core_providers", "middle_initial"),
            _field("credentials", "cp.credentials", CORE_SOURCE, "core_providers", "credentials"),
            _field("provider_type", "cp.provider_type", "cms_physician_by_provider", "core_providers", "provider_type"),
            _field("gender", "cp.gender", "nppes_monthly_v2", "core_providers", "gender"),
            _field("primary_taxonomy_code", "cp.primary_taxonomy_code", "nppes_monthly_v2", "core_providers", "primary_taxonomy_code"),
            _field("street_address_1", "cp.street_address_1", CORE_SOURCE, "core_providers", "street_address_1"),
            _field("street_address_2", "cp.street_address_2", CORE_SOURCE, "core_providers", "street_address_2"),
            _field("city", "cp.city", CORE_SOURCE, "core_providers", "city"),
            _field("state", "UPPER(cp.state)", CORE_SOURCE, "core_providers", "state", "Upper-case state normalization", derived=True),
            _field("zip5", "LEFT(CAST(cp.zip5 AS VARCHAR), 5)", CORE_SOURCE, "core_providers", "zip5", "Five-digit ZIP normalization", derived=True),
            _field("ruca_code", "cp.ruca_code", "cms_physician_by_provider", "core_providers", "ruca_code"),
            _field("medicare_participating", "cp.medicare_participating", "cms_physician_by_provider", "core_providers", "medicare_participating"),
            _field("pecos_enrollment_id", "cp.pecos_enrollment_id", "cms_pecos_public_provider_enrollment", "core_providers", "pecos_enrollment_id"),
            _field("multiple_npi_flag", "cp.multiple_npi_flag", "cms_pecos_public_provider_enrollment", "core_providers", "multiple_npi_flag"),
            _field("bills_through_group_only", "cp.bills_through_group_only", "cms_revalidation_group_reassignment", "core_providers", "bills_through_group_only", "Derived by group-only billing deduplication", derived=True),
            _field("provider_data_year", "cp.data_year", "cms_physician_by_provider", "core_providers", "data_year"),
        ),
    ),
    ReportingModel(
        name="bridge_provider_location",
        grain="one provider NPI by DAC-reported practice address and organization",
        scope_rule='raw_dac_national."State" = \'CA\'',
        source_tables=("raw_dac_national",),
        key_columns=("location_key",),
        from_sql='FROM raw_dac_national d WHERE UPPER(d."State") = \'CA\'',
        notes=(
            "DAC practice-location bridge. A provider can have several rows; do not "
            "physically join this table to provider-level measures before aggregation."
        ),
        fields=(
            _field("location_key", "MD5(CONCAT_WS('|', CAST(d.\"NPI\" AS VARCHAR), COALESCE(CAST(d.org_pac_id AS VARCHAR), ''), UPPER(COALESCE(d.adr_ln_1, '')), LEFT(CAST(d.\"ZIP Code\" AS VARCHAR), 5)))", "cms_dac_national_legacy", "raw_dac_national", "NPI + org_pac_id + adr_ln_1 + ZIP Code", "Stable hash of the reported location grain", derived=True),
            _field("npi", 'CAST(d."NPI" AS VARCHAR)', "cms_dac_national_legacy", "raw_dac_national", "NPI"),
            _field("organization_pac_id", "CAST(d.org_pac_id AS VARCHAR)", "cms_dac_national_legacy", "raw_dac_national", "org_pac_id"),
            _field("facility_name", 'd."Facility Name"', "cms_dac_national_legacy", "raw_dac_national", "Facility Name"),
            _field("organization_member_count", "TRY_CAST(d.num_org_mem AS BIGINT)", "cms_dac_national_legacy", "raw_dac_national", "num_org_mem", "Safe integer cast", derived=True),
            _field("primary_specialty", "d.pri_spec", "cms_dac_national_legacy", "raw_dac_national", "pri_spec"),
            _field("street_address_1", "d.adr_ln_1", "cms_dac_national_legacy", "raw_dac_national", "adr_ln_1"),
            _field("city", 'd."City/Town"', "cms_dac_national_legacy", "raw_dac_national", "City/Town"),
            _field("state", 'UPPER(d."State")', "cms_dac_national_legacy", "raw_dac_national", "State", "Upper-case state normalization", derived=True),
            _field("zip5", 'LEFT(CAST(d."ZIP Code" AS VARCHAR), 5)', "cms_dac_national_legacy", "raw_dac_national", "ZIP Code", "Five-digit ZIP normalization", derived=True),
        ),
    ),
    ReportingModel(
        name="fact_provider_metrics_year",
        grain="one provider NPI by Medicare metric year",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("utilization_metrics", "core_providers"),
        key_columns=("npi", "metric_year"),
        from_sql=(
            "FROM utilization_metrics u JOIN core_providers cp ON cp.npi = u.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        fields=(
            _field("npi", "CAST(u.npi AS VARCHAR)", "cms_physician_by_provider", "utilization_metrics", "npi"),
            _field("metric_year", "u.metric_year", "cms_physician_by_provider", "utilization_metrics", "metric_year"),
            *tuple(
                _field(column, f"u.{column}", source_id, "utilization_metrics", column)
                for column, source_id in (
                    ("tot_hcpcs_codes", "cms_physician_by_provider"),
                    ("tot_services", "cms_physician_by_provider"),
                    ("tot_unique_beneficiaries", "cms_physician_by_provider"),
                    ("tot_submitted_charges", "cms_physician_by_provider"),
                    ("tot_medicare_allowed", "cms_physician_by_provider"),
                    ("tot_medicare_payment", "cms_physician_by_provider"),
                    ("tot_medicare_standardized", "cms_physician_by_provider"),
                    ("drug_services", "cms_physician_by_provider"),
                    ("medical_services", "cms_physician_by_provider"),
                    ("rx_total_claims", "cms_part_d_by_provider"),
                    ("rx_total_drug_cost", "cms_part_d_by_provider"),
                    ("rx_brand_claims", "cms_part_d_by_provider"),
                    ("rx_generic_claims", "cms_part_d_by_provider"),
                    ("rx_opioid_prescriber_rate", "cms_part_d_by_provider"),
                    ("dme_total_claims", "cms_dme_by_referring_provider"),
                    ("dme_medicare_payment", "cms_dme_by_referring_provider"),
                    ("bene_avg_age", "cms_physician_by_provider"),
                    ("bene_avg_risk_score", "cms_physician_by_provider"),
                    ("bene_dual_eligible_count", "cms_physician_by_provider"),
                    ("cc_diabetes_pct", "cms_physician_by_provider"),
                    ("cc_hypertension_pct", "cms_physician_by_provider"),
                    ("cc_heart_failure_pct", "cms_physician_by_provider"),
                    ("cc_ckd_pct", "cms_physician_by_provider"),
                    ("cc_copd_pct", "cms_physician_by_provider"),
                    ("cc_cancer_pct", "cms_physician_by_provider"),
                    ("cc_depression_pct", "cms_physician_by_provider"),
                )
            ),
        ),
    ),
    ReportingModel(
        name="fact_provider_quality_year",
        grain="one provider NPI by available QPP data year",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("provider_quality_scores", "core_providers"),
        key_columns=("npi", "data_year"),
        from_sql=(
            "FROM provider_quality_scores q JOIN core_providers cp ON cp.npi = q.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        fields=(
            _field("npi", "CAST(q.npi AS VARCHAR)", "cms_qpp_experience", "provider_quality_scores", "npi"),
            *tuple(
                _field(column, f"q.{column}", "cms_qpp_experience", "provider_quality_scores", column)
                for column in (
                    "data_year",
                    "practice_state",
                    "practice_size",
                    "clinician_type",
                    "clinician_specialty",
                    "years_in_medicare",
                    "participation_option",
                    "small_practice_status",
                    "rural_status",
                    "hpsa_status",
                    "hospital_based_status",
                    "facility_based_status",
                    "dual_eligibility_ratio",
                    "final_mips_score",
                    "payment_adjustment_pct",
                    "quality_category_score",
                    "pi_category_score",
                    "ia_category_score",
                    "cost_category_score",
                )
            ),
        ),
    ),
    ReportingModel(
        name="bridge_provider_hospital",
        grain="one provider NPI by inferred hospital NPI and data year",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("hospital_affiliations", "core_providers"),
        key_columns=("npi", "hospital_npi", "data_year"),
        from_sql=(
            "FROM hospital_affiliations h JOIN core_providers cp ON cp.npi = h.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes="Affiliations are inferred; confidence and source fields must remain visible.",
        fields=(
            _field("npi", "CAST(h.npi AS VARCHAR)", "cms_revalidation_group_reassignment", "hospital_affiliations", "npi", "Provider side of inferred affiliation", inferred=True),
            _field("hospital_npi", "CAST(h.hospital_npi AS VARCHAR)", "cms_hospital_enrollments", "hospital_affiliations", "hospital_npi", "Hospital side of inferred affiliation", inferred=True),
            *tuple(
                _field(column, f"h.{column}", "cms_hospital_enrollments", "hospital_affiliations", column, "Derived affiliation attribute", inferred=True)
                for column in (
                    "hospital_ccn",
                    "hospital_name",
                    "hospital_city",
                    "hospital_state",
                    "hospital_zip",
                    "hospital_subgroup",
                    "affiliation_source",
                    "confidence_level",
                    "group_pac_id",
                    "data_year",
                )
            ),
        ),
    ),
)


SOURCE_DETAIL_MODELS: tuple[SourceDetailModel, ...] = (
    SourceDetailModel(
        name="source_nppes_provider",
        source_dataset_id="nppes_monthly_v2",
        source_table="raw_nppes",
        grain="one Type 1 NPI in the loaded NPPES subset",
        scope_rule="raw_nppes.practice_state = 'CA'",
        predicate_sql="UPPER(practice_state) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["nppes_monthly_v2"].source_period_semantics,
        attribution=NPPES_ATTRIBUTION,
        notes="The DuckDB raw_nppes loader retains a selected subset of the 329-column publisher file.",
    ),
    SourceDetailModel(
        name="source_dac_clinician_location",
        source_dataset_id="cms_dac_national_legacy",
        source_table="raw_dac_national",
        grain="one clinician NPI by DAC practice address",
        scope_rule='raw_dac_national."State" = \'CA\'',
        predicate_sql='UPPER("State") = \'CA\'',
        source_period_semantics="Doctors and Clinicians publisher snapshot period; legacy loader does not yet persist a manifest.",
        attribution=CMS_ATTRIBUTION,
        notes="Source-faithful loaded columns; source period may be unavailable until its acquisition is migrated to manifests.",
    ),
    SourceDetailModel(
        name="source_medicare_provider_year",
        source_dataset_id="cms_physician_by_provider",
        source_table="raw_physician_by_provider",
        grain="one rendering provider NPI by annual source record",
        scope_rule='raw_physician_by_provider."Rndrng_Prvdr_State_Abrvtn" = \'CA\'',
        predicate_sql='UPPER("Rndrng_Prvdr_State_Abrvtn") = \'CA\'',
        source_period_semantics=SOURCE_REGISTRY[
            "cms_physician_by_provider"
        ].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
)


def model_by_name(name: str) -> ReportingModel:
    for model in REPORTING_MODELS:
        if model.name == name:
            return model
    raise KeyError(name)


def source_detail_by_name(name: str) -> SourceDetailModel:
    for model in SOURCE_DETAIL_MODELS:
        if model.name == name:
            return model
    raise KeyError(name)
