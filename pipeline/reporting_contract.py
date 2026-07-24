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
REPORTING_CONTRACT_VERSION = 4


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
    projection_sql: str = "*"
    column_aliases: tuple[tuple[str, str], ...] = ()

    @property
    def query(self) -> str:
        return (
            f'SELECT {self.projection_sql} FROM "{self.source_table}" '
            f"WHERE {self.predicate_sql}"
        )

    def source_column(self, output_column: str) -> str:
        aliases = dict(self.column_aliases)
        return aliases.get(output_column, output_column)


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


def _direct_fields(
    source_dataset_id: str,
    source_table: str,
    columns: tuple[str, ...],
    source_alias: str | None = None,
) -> tuple[FieldContract, ...]:
    alias = source_alias or source_table[0]
    return tuple(
        _field(column, f"{alias}.{column}", source_dataset_id, source_table, column)
        for column in columns
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
        grain="one provider enrollment by DAC address identifier and organization",
        scope_rule=(
            'raw_dac_national."State" = \'CA\' and provider belongs to '
            "core_providers.state = 'CA'"
        ),
        source_tables=("raw_dac_national", "core_providers"),
        key_columns=("location_key",),
        from_sql=(
            "FROM raw_dac_national d "
            'JOIN core_providers cp ON CAST(cp.npi AS VARCHAR) = CAST(d."NPI" AS VARCHAR) '
            'WHERE UPPER(d."State") = \'CA\' AND UPPER(cp.state) = \'CA\''
        ),
        notes=(
            "DAC practice-location bridge. Enrollment and publisher address identifiers "
            "distinguish otherwise identical address rows. A provider can have several rows; "
            "do not physically join this table to provider-level measures before aggregation. "
            "California DAC rows without a California dim_provider match remain available in "
            "source_detail.source_dac_clinician_location."
        ),
        fields=(
            _field("location_key", "MD5(CONCAT_WS('|', CAST(d.\"NPI\" AS VARCHAR), COALESCE(CAST(d.\"Ind_PAC_ID\" AS VARCHAR), ''), COALESCE(CAST(d.\"Ind_enrl_ID\" AS VARCHAR), ''), COALESCE(CAST(d.org_pac_id AS VARCHAR), ''), COALESCE(CAST(d.adrs_id AS VARCHAR), '')))", "cms_dac_national_legacy", "raw_dac_national", "NPI + Ind_PAC_ID + Ind_enrl_ID + org_pac_id + adrs_id", "Stable hash of the source enrollment-location grain", derived=True),
            _field("npi", 'CAST(d."NPI" AS VARCHAR)', "cms_dac_national_legacy", "raw_dac_national", "NPI"),
            _field("individual_pac_id", 'CAST(d."Ind_PAC_ID" AS VARCHAR)', "cms_dac_national_legacy", "raw_dac_national", "Ind_PAC_ID"),
            _field("individual_enrollment_id", 'CAST(d."Ind_enrl_ID" AS VARCHAR)', "cms_dac_national_legacy", "raw_dac_national", "Ind_enrl_ID"),
            _field("organization_pac_id", "CAST(d.org_pac_id AS VARCHAR)", "cms_dac_national_legacy", "raw_dac_national", "org_pac_id"),
            _field("address_id", "CAST(d.adrs_id AS VARCHAR)", "cms_dac_national_legacy", "raw_dac_national", "adrs_id"),
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
    ReportingModel(
        name="bridge_provider_hospital_evidence",
        grain="one provider NPI by hospital NPI and source-preserving evidence record",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("provider_hospital_evidence", "core_providers"),
        key_columns=("evidence_key",),
        from_sql=(
            "FROM provider_hospital_evidence e "
            "JOIN core_providers cp ON cp.npi = e.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes=(
            "Provider-hospital evidence layer. PECOS receiving-NPI matches, "
            "reassignment name/state matches, and DAC name/address matches remain "
            "separate rows. No record proves employment, exclusivity, or a primary "
            "hospital."
        ),
        fields=tuple(
            _field(
                column,
                f"e.{column}",
                "provider_hospital_evidence",
                "provider_hospital_evidence",
                column,
                "Source-preserving provider-hospital evidence build",
                derived=True,
            )
            for column in (
                "evidence_key",
                "npi",
                "hospital_npi",
                "hospital_ccn",
                "hospital_name",
                "hospital_city",
                "hospital_state",
                "hospital_zip",
                "evidence_method",
                "confidence_level",
                "group_pac_id",
                "organization_pac_id",
                "dac_address_id",
                "provider_enrollment_id",
                "receiving_enrollment_id",
                "source_data_period",
                "data_year",
            )
        ),
    ),
    ReportingModel(
        name="bridge_provider_practice",
        grain="one provider NPI by group practice relationship and warehouse year",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("practice_locations", "core_providers"),
        key_columns=("location_id",),
        from_sql=(
            "FROM practice_locations p JOIN core_providers cp ON cp.npi = p.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes=(
            "Curated provider-to-group relationship. A provider can have several rows; "
            "relate this bridge to dim_provider rather than physically joining it to facts."
        ),
        fields=(
            _field("location_id", "p.location_id", "cms_revalidation_group_reassignment", "practice_locations", "location_id", "Warehouse surrogate relationship key", derived=True),
            *_direct_fields(
                "cms_revalidation_group_reassignment",
                "practice_locations",
                (
                    "npi", "group_pac_id", "group_enrollment_id", "group_legal_name",
                    "group_state", "group_practice_size", "street_address_1", "city",
                    "state", "zip5", "google_place_id", "latitude", "longitude",
                    "is_primary_location", "location_type", "data_year",
                ),
            ),
        ),
    ),
    ReportingModel(
        name="bridge_provider_pecos_organization",
        grain="one provider enrollment by receiving benefit enrollment",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("pecos_provider_organizations", "core_providers"),
        key_columns=("relationship_key",),
        from_sql=(
            "FROM pecos_provider_organizations p "
            "JOIN core_providers cp ON cp.npi = p.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes=(
            "PPEF benefit-reassignment bridge. The receiving enrollment is the entity "
            "receiving reassigned Medicare benefits; this does not prove employment, "
            "exclusivity, or a primary organization."
        ),
        fields=(
            _field("relationship_key", "p.relationship_key", "cms_pecos_reassignment", "pecos_provider_organizations", "relationship_key", "Stable hash of both enrollment IDs", derived=True),
            _field("npi", "p.npi", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "npi"),
            _field("provider_enrollment_id", "p.provider_enrollment_id", "cms_pecos_reassignment", "pecos_provider_organizations", "provider_enrollment_id"),
            _field("receiving_enrollment_id", "p.receiving_enrollment_id", "cms_pecos_reassignment", "pecos_provider_organizations", "receiving_enrollment_id"),
            _field("receiving_npi", "p.receiving_npi", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "receiving_npi"),
            _field("receiving_organization_name", "p.receiving_organization_name", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "receiving_organization_name"),
            _field("receiving_entity_kind", "p.receiving_entity_kind", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "receiving_entity_kind", "Derived from organization-name presence", derived=True),
            _field("receiving_provider_type_code", "p.receiving_provider_type_code", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "receiving_provider_type_code"),
            _field("receiving_provider_type_desc", "p.receiving_provider_type_desc", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "receiving_provider_type_desc"),
            _field("receiving_state", "p.receiving_state", "cms_pecos_public_provider_enrollment", "pecos_provider_organizations", "receiving_state"),
            _field("source_data_period", "p.source_data_period", "cms_pecos_reassignment", "pecos_provider_organizations", "source_data_period"),
        ),
    ),
    ReportingModel(
        name="bridge_pecos_enrollment_location",
        grain="one receiving enrollment by published practice location",
        scope_rule="pecos_enrollment_practice_locations.state = 'CA'",
        source_tables=("pecos_enrollment_practice_locations",),
        key_columns=("location_key",),
        from_sql=(
            "FROM pecos_enrollment_practice_locations p "
            "WHERE UPPER(p.state) = 'CA'"
        ),
        notes=(
            "Relate this model to bridge_provider_pecos_organization on "
            "receiving_enrollment_id. Locations belong to the receiving PECOS enrollment; "
            "they are not claim-level sites and no location is labeled primary."
        ),
        fields=(
            _field("location_key", "p.location_key", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "location_key", "Stable hash of enrollment and location fields", derived=True),
            _field("receiving_enrollment_id", "p.receiving_enrollment_id", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "receiving_enrollment_id"),
            _field("receiving_npi", "p.receiving_npi", "cms_pecos_public_provider_enrollment", "pecos_enrollment_practice_locations", "receiving_npi"),
            _field("receiving_organization_name", "p.receiving_organization_name", "cms_pecos_public_provider_enrollment", "pecos_enrollment_practice_locations", "receiving_organization_name"),
            _field("receiving_entity_kind", "p.receiving_entity_kind", "cms_pecos_public_provider_enrollment", "pecos_enrollment_practice_locations", "receiving_entity_kind", "Derived from organization-name presence", derived=True),
            _field("city", "p.city", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "city"),
            _field("state", "p.state", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "state"),
            _field("zip_code", "p.zip_code", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "zip_code"),
            _field("zip5", "p.zip5", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "zip5", "Five-digit ZIP projection", derived=True),
            _field("source_data_period", "p.source_data_period", "cms_pecos_practice_location", "pecos_enrollment_practice_locations", "source_data_period"),
        ),
    ),
    ReportingModel(
        name="bridge_provider_taxonomy",
        grain="one provider NPI by distinct NPPES taxonomy code",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("nppes_radar_provider_state", "core_providers"),
        key_columns=("npi", "taxonomy_code"),
        from_sql=(
            "FROM nppes_radar_provider_state r "
            "JOIN core_providers cp ON cp.npi = r.npi "
            "CROSS JOIN UNNEST(list_distinct(r.taxonomy_codes)) AS t(taxonomy_code) "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes=(
            "All taxonomy codes retained by NPPES Radar. is_primary identifies the current "
            "primary code; an NPI can legitimately have several taxonomy rows."
        ),
        fields=(
            _field("npi", "CAST(r.npi AS VARCHAR)", "nppes_monthly_v2", "nppes_radar_provider_state", "npi"),
            _field("taxonomy_code", "CAST(t.taxonomy_code AS VARCHAR)", "nppes_monthly_v2", "nppes_radar_provider_state", "taxonomy_codes", "Unnest distinct NPPES taxonomy list", derived=True),
            _field("is_primary", "CAST(t.taxonomy_code AS VARCHAR) = r.primary_taxonomy_code", "nppes_monthly_v2", "nppes_radar_provider_state", "primary_taxonomy_code", "Compare taxonomy code to current primary taxonomy", derived=True),
            _field("source_release_id", "r.source_release_id", "nppes_monthly_v2", "nppes_radar_provider_state", "source_release_id"),
            _field("source_data_period", "r.source_data_period", "nppes_monthly_v2", "nppes_radar_provider_state", "source_data_period"),
        ),
    ),
    ReportingModel(
        name="fact_provider_drug_year",
        grain="one provider NPI by generic drug and Part D data year",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("provider_drug_detail", "core_providers"),
        key_columns=("npi", "generic_name", "data_year"),
        from_sql=(
            "FROM provider_drug_detail d JOIN core_providers cp ON cp.npi = d.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        fields=_direct_fields(
            "cms_part_d_by_provider_and_drug",
            "provider_drug_detail",
            (
                "npi", "brand_name", "generic_name", "tot_claims", "tot_30day_fills",
                "tot_day_supply", "tot_drug_cost", "tot_beneficiaries", "ge65_tot_claims",
                "ge65_tot_drug_cost", "ge65_tot_benes", "data_year",
            ),
            "d",
        ),
    ),
    ReportingModel(
        name="dim_provider_order_referring",
        grain="one current provider NPI eligibility record",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("order_referring_eligibility", "core_providers"),
        key_columns=("npi",),
        from_sql=(
            "FROM order_referring_eligibility o JOIN core_providers cp ON cp.npi = o.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes="Current publisher eligibility flags; this source is a snapshot rather than a historical fact.",
        fields=_direct_fields(
            "cms_order_and_referring",
            "order_referring_eligibility",
            ("npi", "last_name", "first_name", "partb", "dme", "hha", "pmd", "hospice"),
        ),
    ),
    ReportingModel(
        name="fact_provider_industry_payment_year",
        grain="one provider NPI by program year and paying company",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("industry_relationships", "core_providers"),
        key_columns=("npi", "payment_year", "paying_company_name"),
        from_sql=(
            "FROM industry_relationships i JOIN core_providers cp ON cp.npi = i.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes=(
            "Reported Open Payments transfers of value. These records do not establish "
            "endorsement, causation, or misconduct."
        ),
        fields=_direct_fields(
            "open_payments_general",
            "industry_relationships",
            (
                "npi", "payment_year", "paying_company_name", "total_amount_received",
                "payment_count", "nature_of_payments", "top_paying_company_flag",
            ),
        ),
    ),
    ReportingModel(
        name="provider_industry_summary",
        grain="one provider NPI with all-year Open Payments summary",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("kol_summary", "core_providers"),
        key_columns=("npi",),
        from_sql=(
            "FROM kol_summary k JOIN core_providers cp ON cp.npi = k.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes=(
            "Derived Open Payments summary for exploration. KOL tier is an internal analytical "
            "classification, not a publisher designation."
        ),
        fields=tuple(
            _field(
                column,
                f"k.{column}",
                "open_payments_general",
                "kol_summary",
                column,
                "Derived all-year Open Payments summary",
                derived=True,
            )
            for column in (
                "npi", "first_name", "last_name", "specialty", "state", "city",
                "unique_companies", "total_payments_all_years", "total_payment_count",
                "most_recent_year", "top_3_payers", "payment_natures", "kol_tier",
            )
        ),
    ),
    ReportingModel(
        name="fact_provider_radar_event",
        grain="one immutable NPPES change event",
        scope_rule="event NPI belongs to core_providers.state = 'CA'",
        source_tables=("nppes_radar_events", "core_providers"),
        key_columns=("event_id",),
        from_sql=(
            "FROM nppes_radar_events e JOIN core_providers cp ON cp.npi = e.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes="Events are detected from NPPES releases and do not independently prove licensure or practice status.",
        fields=_direct_fields(
            "nppes_weekly_incremental_v2",
            "nppes_radar_events",
            (
                "event_id", "npi", "event_type", "effective_date", "detected_at",
                "source_release_id", "source_data_period", "old_zip5", "new_zip5",
                "old_primary_taxonomy_code", "new_primary_taxonomy_code",
                "source_last_updated_date", "deactivation_date", "reactivation_date",
            ),
            "e",
        ),
    ),
    ReportingModel(
        name="dim_provider_radar_state",
        grain="one current NPPES Radar state row per provider NPI",
        scope_rule="provider NPI belongs to core_providers.state = 'CA'",
        source_tables=("nppes_radar_provider_state", "core_providers"),
        key_columns=("npi",),
        from_sql=(
            "FROM nppes_radar_provider_state r JOIN core_providers cp ON cp.npi = r.npi "
            "WHERE UPPER(cp.state) = 'CA'"
        ),
        notes="Current reconciled NPPES state; monthly snapshots establish the baseline and weekly files advance it.",
        fields=_direct_fields(
            "nppes_monthly_v2",
            "nppes_radar_provider_state",
            (
                "npi", "first_name", "last_name", "credentials", "enumeration_date",
                "source_last_updated_date", "deactivation_date", "reactivation_date",
                "primary_taxonomy_code", "taxonomy_codes", "practice_address_1",
                "practice_address_2", "practice_city", "practice_state", "practice_zip5",
                "practice_phone", "record_fingerprint", "source_release_id",
                "source_data_period", "first_seen_at", "last_seen_at",
            ),
            "r",
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
    SourceDetailModel(
        name="source_practice_reassignment",
        source_dataset_id="cms_revalidation_group_reassignment",
        source_table="raw_reassignment",
        grain="one publisher reassignment record",
        scope_rule='raw_reassignment."Individual State Code" = \'CA\'',
        predicate_sql='UPPER("Individual State Code") = \'CA\'',
        source_period_semantics=SOURCE_REGISTRY["cms_revalidation_group_reassignment"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_part_d_provider_year",
        source_dataset_id="cms_part_d_by_provider",
        source_table="raw_part_d_by_provider",
        grain="one prescriber NPI by annual Part D source record",
        scope_rule="raw_part_d_by_provider.Prscrbr_State_Abrvtn = 'CA'",
        predicate_sql="UPPER(Prscrbr_State_Abrvtn) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["cms_part_d_by_provider"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_part_d_provider_drug_year",
        source_dataset_id="cms_part_d_by_provider_and_drug",
        source_table="raw_part_d_by_provider_and_drug",
        grain="one prescriber NPI by drug annual source record",
        scope_rule="raw_part_d_by_provider_and_drug.Prscrbr_State_Abrvtn = 'CA'",
        predicate_sql="UPPER(Prscrbr_State_Abrvtn) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["cms_part_d_by_provider_and_drug"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_dme_referring_provider_year",
        source_dataset_id="cms_dme_by_referring_provider",
        source_table="raw_dme_by_referring_provider",
        grain="one referring provider NPI by annual DME source record",
        scope_rule="raw_dme_by_referring_provider.Rfrg_Prvdr_State_Abrvtn = 'CA'",
        predicate_sql="UPPER(Rfrg_Prvdr_State_Abrvtn) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["cms_dme_by_referring_provider"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_order_referring",
        source_dataset_id="cms_order_and_referring",
        source_table="raw_order_and_referring",
        grain="one publisher eligibility row per NPI",
        scope_rule="source NPI belongs to core_providers.state = 'CA'",
        predicate_sql=(
            "CAST(NPI AS VARCHAR) IN "
            "(SELECT CAST(npi AS VARCHAR) FROM core_providers WHERE UPPER(state) = 'CA')"
        ),
        source_period_semantics=SOURCE_REGISTRY["cms_order_and_referring"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_hospital_enrollment",
        source_dataset_id="cms_hospital_enrollments",
        source_table="raw_hospital_enrollments",
        grain="one publisher hospital enrollment record",
        scope_rule="raw_hospital_enrollments.state = 'CA'",
        predicate_sql="UPPER(state) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["cms_hospital_enrollments"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_qpp_experience",
        source_dataset_id="cms_qpp_experience",
        source_table="raw_qpp_experience",
        grain="one provider key by QPP performance-year source record",
        scope_rule='raw_qpp_experience."practice state or us territory" = \'CA\'',
        predicate_sql='UPPER("practice state or us territory") = \'CA\'',
        source_period_semantics=SOURCE_REGISTRY["cms_qpp_experience"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_pecos_enrollment",
        source_dataset_id="cms_pecos_public_provider_enrollment",
        source_table="raw_pecos_enrollment",
        grain="one PECOS public enrollment source record",
        scope_rule="raw_pecos_enrollment.STATE_CD = 'CA'",
        predicate_sql="UPPER(STATE_CD) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["cms_pecos_public_provider_enrollment"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
    ),
    SourceDetailModel(
        name="source_pecos_reassignment",
        source_dataset_id="cms_pecos_reassignment",
        source_table="raw_pecos_reassignment",
        grain="one reassigning enrollment by receiving enrollment",
        scope_rule="reassigning enrollment belongs to raw_pecos_enrollment.STATE_CD = 'CA'",
        predicate_sql=(
            "REASGN_BNFT_ENRLMT_ID IN "
            "(SELECT ENRLMT_ID FROM raw_pecos_enrollment WHERE UPPER(STATE_CD) = 'CA')"
        ),
        source_period_semantics=SOURCE_REGISTRY["cms_pecos_reassignment"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
        notes="Benefit reassignment is not proof of employment or exclusivity.",
    ),
    SourceDetailModel(
        name="source_pecos_practice_location",
        source_dataset_id="cms_pecos_practice_location",
        source_table="raw_pecos_practice_location",
        grain="one enrollment by city, state, and ZIP",
        scope_rule="raw_pecos_practice_location.STATE_CD = 'CA'",
        predicate_sql="UPPER(STATE_CD) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["cms_pecos_practice_location"].source_period_semantics,
        attribution=CMS_ATTRIBUTION,
        notes=(
            "Enrollment location rows do not identify claim service sites or a primary "
            "provider location."
        ),
    ),
    SourceDetailModel(
        name="source_open_payments_general",
        source_dataset_id="open_payments_general",
        source_table="raw_open_payments_general",
        grain="one Open Payments General transaction record",
        scope_rule="raw_open_payments_general.Recipient_State = 'CA'",
        predicate_sql="UPPER(Recipient_State) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["open_payments_general"].source_period_semantics,
        attribution=SOURCE_REGISTRY["open_payments_general"].licensing_notes,
        notes="Reported transfers of value; do not imply endorsement, causation, or misconduct.",
        projection_sql=(
            '* RENAME ('
            '"Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" '
            'AS payment_maker_country, '
            '"Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value" '
            'AS third_party_recipient_name)'
        ),
        column_aliases=(
            ("payment_maker_country", "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"),
            ("third_party_recipient_name", "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value"),
        ),
    ),
    SourceDetailModel(
        name="source_open_payments_research",
        source_dataset_id="open_payments_research",
        source_table="raw_open_payments_research",
        grain="one Open Payments Research transaction record",
        scope_rule="recipient or principal investigator state = 'CA'",
        predicate_sql=(
            "UPPER(Recipient_State) = 'CA' OR UPPER(Principal_Investigator_1_State) = 'CA' "
            "OR UPPER(Principal_Investigator_2_State) = 'CA' "
            "OR UPPER(Principal_Investigator_3_State) = 'CA' "
            "OR UPPER(Principal_Investigator_4_State) = 'CA' "
            "OR UPPER(Principal_Investigator_5_State) = 'CA'"
        ),
        source_period_semantics=SOURCE_REGISTRY["open_payments_research"].source_period_semantics,
        attribution=SOURCE_REGISTRY["open_payments_research"].licensing_notes,
        notes="Reported research payments; do not imply endorsement, causation, or misconduct.",
        projection_sql=(
            '* RENAME ('
            '"Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" '
            'AS payment_maker_country)'
        ),
        column_aliases=(("payment_maker_country", "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"),),
    ),
    SourceDetailModel(
        name="source_open_payments_ownership",
        source_dataset_id="open_payments_ownership",
        source_table="raw_open_payments_ownership",
        grain="one Open Payments Ownership and Investment Interest record",
        scope_rule="raw_open_payments_ownership.Recipient_State = 'CA'",
        predicate_sql="UPPER(Recipient_State) = 'CA'",
        source_period_semantics=SOURCE_REGISTRY["open_payments_ownership"].source_period_semantics,
        attribution=SOURCE_REGISTRY["open_payments_ownership"].licensing_notes,
        notes="Reported ownership interests; do not imply endorsement, causation, or misconduct.",
        projection_sql=(
            '* RENAME ('
            '"Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" '
            'AS payment_maker_country)'
        ),
        column_aliases=(("payment_maker_country", "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"),),
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
