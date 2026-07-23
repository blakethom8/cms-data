from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.manifests import PromotionState, ValidationState
from pipeline.releases import WarehouseRelease, WarehouseReleaseDocument, WarehouseReleaseStore
from pipeline.reporting_contract import REPORTING_MODELS, SOURCE_DETAIL_MODELS
from pipeline.reporting_export import (
    ReportingError,
    _build_schema_name,
    _postgres_type,
    main,
    profile_database,
    publish_release,
    resolve_production_release,
    resolve_release,
    sha256_file,
)


def _warehouse(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    connection.execute(
        """
        CREATE TABLE core_providers (
            npi VARCHAR, last_org_name VARCHAR, first_name VARCHAR,
            middle_initial VARCHAR, credentials VARCHAR, entity_type_code VARCHAR,
            provider_type VARCHAR, gender VARCHAR, primary_taxonomy_code VARCHAR,
            street_address_1 VARCHAR, street_address_2 VARCHAR, city VARCHAR,
            state VARCHAR, zip5 VARCHAR, country VARCHAR, ruca_code VARCHAR,
            medicare_participating VARCHAR, pecos_enrollment_id VARCHAR,
            multiple_npi_flag VARCHAR, bills_through_group_only BOOLEAN,
            data_year INTEGER, last_updated TIMESTAMP
        );
        INSERT INTO core_providers VALUES
            ('1000000001', 'Alpha', 'Ana', NULL, 'MD', 'I', 'Cardiology', 'F',
             '207RC0000X', '1 Main', NULL, 'Los Angeles', 'CA', '90001', 'US',
             '1', 'Y', 'E1', 'N', FALSE, 2024, '2026-07-01'),
            ('1000000002', 'Beta', 'Ben', NULL, 'DO', 'I', 'Internal Medicine', 'M',
             '207R00000X', '2 Main', NULL, 'Pasadena', 'ca', '91101', 'US',
             '1', 'Y', 'E2', 'N', FALSE, 2024, '2026-07-01'),
            ('1000000003', 'Texas', 'Tia', NULL, 'MD', 'I', 'Cardiology', 'F',
             '207RC0000X', '3 Main', NULL, 'Austin', 'TX', '73301', 'US',
             '1', 'Y', 'E3', 'N', FALSE, 2024, '2026-07-01');

        CREATE TABLE utilization_metrics (
            npi VARCHAR, metric_year INTEGER, tot_hcpcs_codes INTEGER,
            tot_services DECIMAL(15,2), tot_unique_beneficiaries INTEGER,
            tot_submitted_charges DECIMAL(15,2), tot_medicare_allowed DECIMAL(15,2),
            tot_medicare_payment DECIMAL(15,2), tot_medicare_standardized DECIMAL(15,2),
            drug_services DECIMAL(15,2), medical_services DECIMAL(15,2),
            rx_total_claims INTEGER, rx_total_drug_cost DECIMAL(15,2),
            rx_brand_claims INTEGER, rx_generic_claims INTEGER,
            rx_opioid_prescriber_rate DECIMAL(5,2), dme_total_claims INTEGER,
            dme_medicare_payment DECIMAL(15,2), bene_avg_age DECIMAL(5,2),
            bene_avg_risk_score DECIMAL(5,3), bene_dual_eligible_count INTEGER,
            cc_diabetes_pct DECIMAL(5,2), cc_hypertension_pct DECIMAL(5,2),
            cc_heart_failure_pct DECIMAL(5,2), cc_ckd_pct DECIMAL(5,2),
            cc_copd_pct DECIMAL(5,2), cc_cancer_pct DECIMAL(5,2),
            cc_depression_pct DECIMAL(5,2)
        );
        INSERT INTO utilization_metrics
        SELECT '1000000001', 2024, 12, 100, 50, 1000, 800, 700, 690,
               10, 90, 40, 4000, 10, 30, 0.5, 5, 200, 72, 1.1, 10,
               20, 30, 4, 5, 6, 7, 8;
        INSERT INTO utilization_metrics
        SELECT '1000000002', 2024, 5, 25, 12, 300, 250, 210, 205,
               1, 24, 5, 500, 1, 4, 0.1, 0, 0, 65, 0.9, 1,
               10, 15, 1, 2, 3, 4, 5;
        INSERT INTO utilization_metrics
        SELECT '1000000003', 2024, 8, 60, 30, 600, 500, 450, 445,
               3, 57, 20, 2000, 5, 15, 0.2, 2, 80, 68, 1.0, 5,
               12, 18, 2, 3, 4, 5, 6;

        CREATE TABLE provider_quality_scores (
            npi VARCHAR, practice_state VARCHAR, practice_size VARCHAR,
            clinician_type VARCHAR, clinician_specialty VARCHAR,
            years_in_medicare VARCHAR, participation_option VARCHAR,
            small_practice_status BOOLEAN, rural_status BOOLEAN,
            hpsa_status BOOLEAN, hospital_based_status BOOLEAN,
            facility_based_status BOOLEAN, dual_eligibility_ratio DECIMAL(5,3),
            final_mips_score DECIMAL(7,2), payment_adjustment_pct DECIMAL(7,4),
            quality_category_score DECIMAL(7,2), pi_category_score DECIMAL(7,2),
            ia_category_score DECIMAL(7,2), cost_category_score DECIMAL(7,2),
            data_year INTEGER
        );
        INSERT INTO provider_quality_scores VALUES
            ('1000000001', 'CA', 'small', 'physician', 'Cardiology', '10+',
             'individual', TRUE, FALSE, FALSE, FALSE, FALSE, 0.1, 90, 1,
             88, 92, 80, 85, 2024),
            ('1000000003', 'TX', 'small', 'physician', 'Cardiology', '10+',
             'individual', TRUE, FALSE, FALSE, FALSE, FALSE, 0.1, 80, 0,
             78, 82, 70, 75, 2024);

        CREATE TABLE hospital_affiliations (
            npi VARCHAR, hospital_npi VARCHAR, hospital_ccn VARCHAR,
            hospital_name VARCHAR, hospital_city VARCHAR, hospital_state VARCHAR,
            hospital_zip VARCHAR, hospital_subgroup VARCHAR,
            affiliation_source VARCHAR, confidence_level VARCHAR,
            group_pac_id VARCHAR, data_year INTEGER
        );
        INSERT INTO hospital_affiliations VALUES
            ('1000000001', '2000000001', 'CCN1', 'Alpha Hospital', 'Los Angeles',
             'CA', '90001', 'acute_care', 'reassignment', 'medium', 'PAC1', 2024),
            ('1000000003', '2000000002', 'CCN2', 'Texas Hospital', 'Austin',
             'TX', '73301', 'acute_care', 'reassignment', 'medium', 'PAC2', 2024);

        CREATE TABLE raw_dac_national (
            "NPI" VARCHAR, "Ind_PAC_ID" VARCHAR, "Ind_enrl_ID" VARCHAR,
            org_pac_id VARCHAR, adrs_id VARCHAR, "Facility Name" VARCHAR,
            num_org_mem VARCHAR, pri_spec VARCHAR, adr_ln_1 VARCHAR,
            "City/Town" VARCHAR, "State" VARCHAR, "ZIP Code" VARCHAR,
            extra_source_column VARCHAR
        );
        INSERT INTO raw_dac_national VALUES
            ('1000000001', 'IPAC1', 'ENRL1', 'PAC1', 'ADDR1', 'Alpha Group', '10', 'Cardiology', '1 Main',
             'Los Angeles', 'CA', '90001', 'preserved'),
            ('1000000002', 'IPAC2', 'ENRL2', 'PAC2', 'ADDR2', 'Beta Group', '2', 'Internal Medicine', '2 Main',
             'Pasadena', 'ca', '91101', 'preserved'),
            ('1000000003', 'IPAC3', 'ENRL3', 'PAC3', 'ADDR3', 'Texas Group', '4', 'Cardiology', '3 Main',
             'Austin', 'TX', '73301', 'excluded');

        CREATE TABLE raw_nppes (
            npi VARCHAR, first_name VARCHAR, last_name VARCHAR,
            practice_state VARCHAR, source_only_field VARCHAR
        );
        INSERT INTO raw_nppes VALUES
            ('1000000001', 'Ana', 'Alpha', 'CA', 'keep'),
            ('1000000002', 'Ben', 'Beta', 'ca', 'keep'),
            ('1000000003', 'Tia', 'Texas', 'TX', 'exclude');

        CREATE TABLE raw_physician_by_provider (
            "Rndrng_NPI" VARCHAR, "Rndrng_Prvdr_State_Abrvtn" VARCHAR,
            "Tot_Srvcs" DECIMAL(15,2), source_only_measure DECIMAL(15,2)
        );
        INSERT INTO raw_physician_by_provider VALUES
            ('1000000001', 'CA', 100, 999),
            ('1000000002', 'ca', 25, 888),
            ('1000000003', 'TX', 60, 777);

        CREATE TABLE practice_locations (
            location_id INTEGER, npi VARCHAR, group_pac_id VARCHAR,
            group_enrollment_id VARCHAR, group_legal_name VARCHAR,
            group_state VARCHAR, group_practice_size INTEGER,
            street_address_1 VARCHAR, city VARCHAR, state VARCHAR, zip5 VARCHAR,
            google_place_id VARCHAR, latitude DOUBLE, longitude DOUBLE,
            is_primary_location BOOLEAN, location_type VARCHAR, data_year INTEGER
        );
        INSERT INTO practice_locations VALUES
            (1, '1000000001', 'PAC1', 'GE1', 'Alpha Group', 'CA', 10,
             '1 Main', 'Los Angeles', 'CA', '90001', NULL, NULL, NULL, TRUE, NULL, 2024),
            (2, '1000000002', 'PAC2', 'GE2', 'Beta Group', 'CA', 2,
             NULL, NULL, 'CA', NULL, NULL, NULL, NULL, TRUE, NULL, 2024),
            (3, '1000000003', 'PAC3', 'GE3', 'Texas Group', 'TX', 4,
             NULL, NULL, 'TX', NULL, NULL, NULL, NULL, TRUE, NULL, 2024);

        CREATE TABLE pecos_provider_organizations (
            relationship_key VARCHAR, npi VARCHAR, provider_enrollment_id VARCHAR,
            receiving_enrollment_id VARCHAR, receiving_npi VARCHAR,
            receiving_organization_name VARCHAR, receiving_entity_kind VARCHAR,
            receiving_provider_type_code VARCHAR, receiving_provider_type_desc VARCHAR,
            receiving_state VARCHAR, source_data_period VARCHAR,
            relationship_source_run_id VARCHAR, enrollment_source_run_id VARCHAR
        );
        INSERT INTO pecos_provider_organizations VALUES
            ('rel1', '1000000001', 'E1', 'O1', '2000000001', 'Alpha Group',
             'organization', '12-00', 'Group Practice', 'CA', '2026-01-01/2026-03-31', 'r1', 'e1'),
            ('rel2', '1000000002', 'E2', 'O2', '2000000002', 'Beta Group',
             'organization', '12-00', 'Group Practice', 'CA', '2026-01-01/2026-03-31', 'r1', 'e1'),
            ('rel3', '1000000003', 'E3', 'O3', '2000000003', 'Texas Group',
             'organization', '12-00', 'Group Practice', 'TX', '2026-01-01/2026-03-31', 'r1', 'e1');

        CREATE TABLE pecos_enrollment_practice_locations (
            location_key VARCHAR, receiving_enrollment_id VARCHAR, receiving_npi VARCHAR,
            receiving_organization_name VARCHAR, receiving_entity_kind VARCHAR,
            city VARCHAR, state VARCHAR, zip_code VARCHAR, zip5 VARCHAR,
            source_data_period VARCHAR, location_source_run_id VARCHAR,
            enrollment_source_run_id VARCHAR
        );
        INSERT INTO pecos_enrollment_practice_locations VALUES
            ('loc1', 'O1', '2000000001', 'Alpha Group',
             'organization', 'Los Angeles', 'CA', '900010001', '90001',
             '2026-01-01/2026-03-31', 'l1', 'e1'),
            ('loc2', 'O2', '2000000002', 'Beta Group',
             'organization', 'Pasadena', 'CA', '911010001', '91101',
             '2026-01-01/2026-03-31', 'l1', 'e1'),
            ('loc3', 'O3', '2000000003', 'Texas Group',
             'organization', 'Austin', 'TX', '733010001', '73301',
             '2026-01-01/2026-03-31', 'l1', 'e1');

        CREATE TABLE nppes_radar_provider_state (
            npi VARCHAR, first_name VARCHAR, last_name VARCHAR, credentials VARCHAR,
            enumeration_date DATE, source_last_updated_date DATE,
            deactivation_date DATE, reactivation_date DATE,
            primary_taxonomy_code VARCHAR, taxonomy_codes VARCHAR[],
            practice_address_1 VARCHAR, practice_address_2 VARCHAR,
            practice_city VARCHAR, practice_state VARCHAR, practice_zip5 VARCHAR,
            practice_phone VARCHAR, record_fingerprint VARCHAR,
            source_release_id VARCHAR, source_data_period VARCHAR,
            first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ
        );
        INSERT INTO nppes_radar_provider_state VALUES
            ('1000000001', 'Ana', 'Alpha', 'MD', '2010-01-01', '2026-07-01',
             NULL, NULL, '207RC0000X', ['207RC0000X', '207R00000X'], '1 Main', NULL,
             'Los Angeles', 'CA', '90001', '555', 'fp1', 'rel1', '2026-07', NOW(), NOW()),
            ('1000000002', 'Ben', 'Beta', 'DO', '2011-01-01', '2026-07-01',
             NULL, NULL, '207R00000X', ['207R00000X'], '2 Main', NULL,
             'Pasadena', 'CA', '91101', '556', 'fp2', 'rel1', '2026-07', NOW(), NOW()),
            ('1000000003', 'Tia', 'Texas', 'MD', '2012-01-01', '2026-07-01',
             NULL, NULL, '207RC0000X', ['207RC0000X'], '3 Main', NULL,
             'Austin', 'TX', '73301', '557', 'fp3', 'rel1', '2026-07', NOW(), NOW());

        CREATE TABLE provider_drug_detail (
            npi VARCHAR, brand_name VARCHAR, generic_name VARCHAR,
            tot_claims INTEGER, tot_30day_fills DECIMAL(15,2), tot_day_supply INTEGER,
            tot_drug_cost DECIMAL(15,2), tot_beneficiaries INTEGER,
            ge65_tot_claims INTEGER, ge65_tot_drug_cost DECIMAL(15,2),
            ge65_tot_benes INTEGER, data_year INTEGER
        );
        INSERT INTO provider_drug_detail VALUES
            ('1000000001', 'Brand A', 'Drug A', 10, 11, 300, 1000, 8, 4, 300, 3, 2024),
            ('1000000002', NULL, 'Drug B', 5, 5, 120, 400, 4, 2, 150, 2, 2024),
            ('1000000003', NULL, 'Drug C', 7, 7, 200, 500, 6, 3, 200, 2, 2024);

        CREATE TABLE order_referring_eligibility (
            npi VARCHAR, last_name VARCHAR, first_name VARCHAR,
            partb VARCHAR, dme VARCHAR, hha VARCHAR, pmd VARCHAR, hospice VARCHAR
        );
        INSERT INTO order_referring_eligibility VALUES
            ('1000000001', 'Alpha', 'Ana', 'Y', 'Y', 'N', 'N', 'Y'),
            ('1000000002', 'Beta', 'Ben', 'Y', 'N', 'N', 'N', 'N'),
            ('1000000003', 'Texas', 'Tia', 'Y', 'Y', 'Y', 'N', 'N');

        CREATE TABLE industry_relationships (
            npi VARCHAR, payment_year INTEGER, paying_company_name VARCHAR,
            total_amount_received DECIMAL(15,2), payment_count INTEGER,
            nature_of_payments VARCHAR, top_paying_company_flag BOOLEAN
        );
        INSERT INTO industry_relationships VALUES
            ('1000000001', 2025, 'Company A', 100, 2, 'food', TRUE),
            ('1000000002', 2025, 'Company B', 50, 1, 'education', TRUE),
            ('1000000003', 2025, 'Company C', 75, 1, 'food', TRUE);

        CREATE TABLE kol_summary (
            npi VARCHAR, first_name VARCHAR, last_name VARCHAR, specialty VARCHAR,
            state VARCHAR, city VARCHAR, unique_companies INTEGER,
            total_payments_all_years DECIMAL(15,2), total_payment_count INTEGER,
            most_recent_year INTEGER, top_3_payers VARCHAR, payment_natures VARCHAR,
            kol_tier VARCHAR
        );
        INSERT INTO kol_summary VALUES
            ('1000000001', 'Ana', 'Alpha', 'Cardiology', 'CA', 'Los Angeles',
             1, 100, 2, 2025, 'Company A', 'food', 'emerging'),
            ('1000000003', 'Tia', 'Texas', 'Cardiology', 'TX', 'Austin',
             1, 75, 1, 2025, 'Company C', 'food', 'emerging');

        CREATE TABLE nppes_radar_events (
            event_id VARCHAR, npi VARCHAR, event_type VARCHAR, effective_date DATE,
            detected_at TIMESTAMPTZ, source_release_id VARCHAR,
            source_data_period VARCHAR, old_zip5 VARCHAR, new_zip5 VARCHAR,
            old_primary_taxonomy_code VARCHAR, new_primary_taxonomy_code VARCHAR,
            source_last_updated_date DATE, deactivation_date DATE, reactivation_date DATE
        );
        INSERT INTO nppes_radar_events VALUES
            ('event1', '1000000001', 'practice_location_changed', '2026-07-01', NOW(),
             'rel1', '2026-07', '90002', '90001', NULL, NULL, '2026-07-01', NULL, NULL),
            ('event2', '1000000003', 'newly_enumerated', '2026-07-01', NOW(),
             'rel1', '2026-07', NULL, '73301', NULL, '207RC0000X', '2026-07-01', NULL, NULL);

        CREATE TABLE raw_reassignment (
            "Individual NPI" VARCHAR, "Individual State Code" VARCHAR,
            "Group PAC ID" VARCHAR, source_only_field VARCHAR
        );
        INSERT INTO raw_reassignment VALUES
            ('1000000001', 'CA', 'PAC1', 'keep'),
            ('1000000003', 'TX', 'PAC3', 'exclude');

        CREATE TABLE raw_part_d_by_provider (
            PRSCRBR_NPI VARCHAR, Prscrbr_State_Abrvtn VARCHAR,
            Tot_Clms INTEGER, source_only_field VARCHAR
        );
        INSERT INTO raw_part_d_by_provider VALUES
            ('1000000001', 'CA', 10, 'keep'), ('1000000003', 'TX', 7, 'exclude');

        CREATE TABLE raw_part_d_by_provider_and_drug (
            Prscrbr_NPI VARCHAR, Prscrbr_State_Abrvtn VARCHAR,
            Gnrc_Name VARCHAR, Tot_Clms INTEGER, source_only_field VARCHAR
        );
        INSERT INTO raw_part_d_by_provider_and_drug VALUES
            ('1000000001', 'CA', 'Drug A', 10, 'keep'),
            ('1000000003', 'TX', 'Drug C', 7, 'exclude');

        CREATE TABLE raw_dme_by_referring_provider (
            Rfrg_NPI VARCHAR, Rfrg_Prvdr_State_Abrvtn VARCHAR,
            Tot_Suplr_Clms INTEGER, source_only_field VARCHAR
        );
        INSERT INTO raw_dme_by_referring_provider VALUES
            ('1000000001', 'CA', 5, 'keep'), ('1000000003', 'TX', 2, 'exclude');

        CREATE TABLE raw_order_and_referring (
            NPI VARCHAR, LAST_NAME VARCHAR, FIRST_NAME VARCHAR, PARTB VARCHAR,
            DME VARCHAR, HHA VARCHAR, PMD VARCHAR, HOSPICE VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_order_and_referring VALUES
            ('1000000001', 'Alpha', 'Ana', 'Y', 'Y', 'N', 'N', 'Y', 'keep'),
            ('1000000003', 'Texas', 'Tia', 'Y', 'Y', 'Y', 'N', 'N', 'exclude');

        CREATE TABLE raw_hospital_enrollments (
            enrollment_id VARCHAR, npi VARCHAR, organization_name VARCHAR,
            state VARCHAR, source_only_field VARCHAR
        );
        INSERT INTO raw_hospital_enrollments VALUES
            ('H1', '2000000001', 'Alpha Hospital', 'CA', 'keep'),
            ('H2', '2000000002', 'Texas Hospital', 'TX', 'exclude');

        CREATE TABLE raw_qpp_experience (
            npi VARCHAR, "practice state or us territory" VARCHAR,
            "final score" DECIMAL(7,2), source_only_field VARCHAR
        );
        INSERT INTO raw_qpp_experience VALUES
            ('1000000001', 'CA', 90, 'keep'), ('1000000003', 'TX', 80, 'exclude');

        CREATE TABLE raw_pecos_enrollment (
            NPI VARCHAR, STATE_CD VARCHAR, ENRLMT_ID VARCHAR, ORG_NAME VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_pecos_enrollment VALUES
            ('1000000001', 'CA', 'E1', NULL, 'keep'),
            ('1000000002', 'CA', 'E2', NULL, 'keep'),
            ('1000000003', 'TX', 'E3', NULL, 'exclude'),
            ('2000000001', 'CA', 'O1', 'Alpha Group', 'keep'),
            ('2000000002', 'CA', 'O2', 'Beta Group', 'keep'),
            ('2000000003', 'TX', 'O3', 'Texas Group', 'exclude');

        CREATE TABLE raw_pecos_reassignment (
            REASGN_BNFT_ENRLMT_ID VARCHAR, RCV_BNFT_ENRLMT_ID VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_pecos_reassignment VALUES
            ('E1', 'O1', 'keep'), ('E2', 'O2', 'keep'), ('E3', 'O3', 'exclude');

        CREATE TABLE raw_pecos_practice_location (
            ENRLMT_ID VARCHAR, CITY_NAME VARCHAR, STATE_CD VARCHAR, ZIP_CD VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_pecos_practice_location VALUES
            ('O1', 'Los Angeles', 'CA', '900010001', 'keep'),
            ('O2', 'Pasadena', 'CA', '911010001', 'keep'),
            ('O3', 'Austin', 'TX', '733010001', 'exclude');

        CREATE TABLE raw_open_payments_general (
            Covered_Recipient_NPI VARCHAR, Recipient_State VARCHAR,
            Total_Amount_of_Payment_USDollars DECIMAL(15,2),
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" VARCHAR,
            "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value" VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_open_payments_general VALUES
            ('1000000001', 'CA', 100, 'US', 'Recipient A', 'keep'),
            ('1000000003', 'TX', 75, 'US', 'Recipient C', 'exclude');

        CREATE TABLE raw_open_payments_research (
            Covered_Recipient_NPI VARCHAR, Recipient_State VARCHAR,
            Principal_Investigator_1_State VARCHAR,
            Principal_Investigator_2_State VARCHAR,
            Principal_Investigator_3_State VARCHAR,
            Principal_Investigator_4_State VARCHAR,
            Principal_Investigator_5_State VARCHAR,
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_open_payments_research VALUES
            ('1000000001', 'CA', NULL, NULL, NULL, NULL, NULL, 'US', 'keep'),
            ('1000000003', 'TX', NULL, NULL, NULL, NULL, NULL, 'US', 'exclude');

        CREATE TABLE raw_open_payments_ownership (
            Physician_NPI VARCHAR, Recipient_State VARCHAR,
            Value_of_Interest DECIMAL(15,2),
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country" VARCHAR,
            source_only_field VARCHAR
        );
        INSERT INTO raw_open_payments_ownership VALUES
            ('1000000001', 'CA', 1000, 'US', 'keep'),
            ('1000000003', 'TX', 500, 'US', 'exclude');
        """
    )
    connection.close()
    return path


def _release(data_root: Path, database_path: Path) -> WarehouseRelease:
    checksum = sha256_file(database_path)
    release = WarehouseRelease(
        warehouse_release_id="warehouse-test-ca",
        created_at="2026-07-22T00:00:00+00:00",
        source_run_ids=("run-test",),
        pipeline_code_commit="a" * 40,
        baseline_path="baseline.duckdb",
        baseline_sha256="b" * 64,
        database_path=str(database_path.relative_to(data_root)),
        duckdb_version=duckdb.__version__,
        byte_size=database_path.stat().st_size,
        sha256=checksum,
        validation_state=ValidationState.PASSED,
        promotion_state=PromotionState.ACTIVE,
    )
    WarehouseReleaseStore(data_root / "warehouse-releases.json").save(
        WarehouseReleaseDocument(releases=[release])
    )
    return release


def test_profile_preserves_grain_and_source_detail(tmp_path: Path) -> None:
    database = _warehouse(tmp_path / "warehouse.duckdb")
    before = (database.stat().st_size, database.stat().st_mtime_ns)

    profile = profile_database(database)

    counts = {(row.layer, row.name): row.row_count for row in profile.models}
    assert counts[("reporting", "dim_provider")] == 2
    assert counts[("reporting", "bridge_provider_location")] == 2
    assert counts[("reporting", "fact_provider_metrics_year")] == 2
    assert counts[("reporting", "fact_provider_quality_year")] == 1
    assert counts[("reporting", "bridge_provider_hospital")] == 1
    assert counts[("reporting", "bridge_provider_practice")] == 2
    assert counts[("reporting", "bridge_provider_pecos_organization")] == 2
    assert counts[("reporting", "bridge_pecos_enrollment_location")] == 2
    assert counts[("reporting", "bridge_provider_taxonomy")] == 3
    assert counts[("reporting", "fact_provider_drug_year")] == 2
    assert counts[("reporting", "dim_provider_order_referring")] == 2
    assert counts[("reporting", "fact_provider_industry_payment_year")] == 2
    assert counts[("reporting", "provider_industry_summary")] == 1
    assert counts[("reporting", "fact_provider_radar_event")] == 1
    assert counts[("reporting", "dim_provider_radar_state")] == 2
    assert counts[("source_detail", "source_nppes_provider")] == 2
    assert counts[("source_detail", "source_dac_clinician_location")] == 2
    assert counts[("source_detail", "source_medicare_provider_year")] == 2
    assert counts[("source_detail", "source_pecos_reassignment")] == 2
    assert counts[("source_detail", "source_pecos_practice_location")] == 2
    assert (database.stat().st_size, database.stat().st_mtime_ns) == before


def test_curated_location_bridge_excludes_orphans_preserved_in_source_detail(
    tmp_path: Path,
) -> None:
    database = _warehouse(tmp_path / "warehouse.duckdb")
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        INSERT INTO raw_dac_national VALUES
            ('1000000999', 'IPAC9', 'ENRL9', 'PAC9', 'ADDR9', 'Unmatched Group',
             '1', 'Cardiology', '9 Main', 'Los Angeles', 'CA', '90009', 'preserved')
        """
    )
    connection.close()

    profile = profile_database(database)
    counts = {(row.layer, row.name): row.row_count for row in profile.models}

    assert counts[("reporting", "bridge_provider_location")] == 2
    assert counts[("source_detail", "source_dac_clinician_location")] == 3


def test_profile_exposes_every_source_column(tmp_path: Path) -> None:
    database = _warehouse(tmp_path / "warehouse.duckdb")
    profile = profile_database(database)
    columns = {(row.layer, row.name): row.column_count for row in profile.models}

    assert columns[("source_detail", "source_nppes_provider")] == 5
    assert columns[("source_detail", "source_dac_clinician_location")] == 13
    assert columns[("source_detail", "source_medicare_provider_year")] == 4


def test_contract_has_lineage_for_every_curated_field() -> None:
    for model in REPORTING_MODELS:
        query_names = [field.name for field in model.fields]
        assert len(query_names) == len(set(query_names))
        assert set(model.key_columns).issubset(query_names)
        for field in model.fields:
            assert field.source_dataset_id
            assert field.source_table
            assert field.source_column
            assert field.transformation

    assert not any(
        field.name == "hcpcs_description"
        for model in REPORTING_MODELS
        for field in model.fields
    )
    location = next(model for model in REPORTING_MODELS if model.name == "bridge_provider_location")
    assert {"individual_pac_id", "individual_enrollment_id", "address_id"}.issubset(
        field.name for field in location.fields
    )


def test_source_detail_contracts_are_explicitly_scoped() -> None:
    assert {model.source_table for model in SOURCE_DETAIL_MODELS} == {
        "raw_nppes",
        "raw_dac_national",
        "raw_physician_by_provider",
        "raw_reassignment",
        "raw_part_d_by_provider",
        "raw_part_d_by_provider_and_drug",
        "raw_dme_by_referring_provider",
        "raw_order_and_referring",
        "raw_hospital_enrollments",
        "raw_qpp_experience",
        "raw_pecos_enrollment",
        "raw_pecos_reassignment",
        "raw_pecos_practice_location",
        "raw_open_payments_general",
        "raw_open_payments_research",
        "raw_open_payments_ownership",
    }
    assert all("CA" in model.scope_rule for model in SOURCE_DETAIL_MODELS)


def test_source_detail_aliases_preserve_upstream_lineage() -> None:
    general = next(
        model for model in SOURCE_DETAIL_MODELS
        if model.name == "source_open_payments_general"
    )
    assert general.source_column("payment_maker_country") == (
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"
    )
    assert general.source_column("third_party_recipient_name") == (
        "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value"
    )
    assert general.source_column("Covered_Recipient_NPI") == "Covered_Recipient_NPI"


def test_release_resolution_requires_active_checksum_matched_release(
    tmp_path: Path,
) -> None:
    database = _warehouse(tmp_path / "releases" / "test" / "warehouse.duckdb")
    release = _release(tmp_path, database)

    resolved = resolve_release(tmp_path, release.warehouse_release_id)

    assert resolved.database_path == database
    assert resolved.sha256 == release.sha256

    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE mutation(value INTEGER)")
    connection.close()
    with pytest.raises(ReportingError, match="checksum"):
        resolve_release(tmp_path, release.warehouse_release_id)


def _production_selection(root: Path, database: Path, *, state: str = "verified") -> Path:
    deployment_id = "deployment-20260722T120000Z-0123456789"
    release_id = "warehouse-20260722T110000Z-abcdef0123"
    checksum = sha256_file(database)
    bundle = root / "releases" / deployment_id
    evidence = root / "evidence" / deployment_id
    bundle.mkdir(parents=True)
    evidence.mkdir(parents=True)
    (bundle / "warehouse").symlink_to(database)
    (root / "release-current").symlink_to(bundle)
    (root / "deployments.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "selected_deployment_id": deployment_id,
                "deployments": [
                    {
                        "deployment_id": deployment_id,
                        "state": state,
                        "warehouse_release_id": release_id,
                        "warehouse_sha256": checksum,
                        "warehouse_byte_size": database.stat().st_size,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (evidence / "warehouse-release.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "release": {
                    "warehouse_release_id": release_id,
                    "sha256": checksum,
                    "pipeline_code_commit": "a" * 40,
                    "source_run_ids": ["run-production"],
                },
            }
        ),
        encoding="utf-8",
    )
    (evidence / "source-manifests.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "deployment_id": deployment_id,
                "warehouse_release_id": release_id,
                "manifests": [],
            }
        ),
        encoding="utf-8",
    )
    return database


def test_production_resolution_pins_verified_selected_bundle(tmp_path: Path) -> None:
    database = _warehouse(tmp_path / "artifacts" / "warehouse.duckdb")
    production_root = tmp_path / "production"
    _production_selection(production_root, database)

    release = resolve_production_release(production_root)

    assert release.database_path == database.resolve()
    assert release.warehouse_release_id == "warehouse-20260722T110000Z-abcdef0123"
    assert release.sha256 == sha256_file(database)
    assert release.manifest_path == (
        production_root
        / "evidence"
        / "deployment-20260722T120000Z-0123456789"
        / "source-manifests.json"
    )


def test_production_resolution_rejects_unverified_selection(tmp_path: Path) -> None:
    database = _warehouse(tmp_path / "artifacts" / "warehouse.duckdb")
    production_root = tmp_path / "production"
    _production_selection(production_root, database, state="selected")

    with pytest.raises(ReportingError, match="not verified"):
        resolve_production_release(production_root)


def test_profile_cli_outputs_machine_readable_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = _warehouse(tmp_path / "warehouse.duckdb")

    assert main(["profile", "--duckdb", str(database), "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["scope_name"] == "California"
    assert any(model["name"] == "dim_provider" for model in payload["models"])


@pytest.mark.parametrize(
    ("duckdb_type", "postgres_type"),
    [
        ("VARCHAR", "TEXT"),
        ("INTEGER", "INTEGER"),
        ("BIGINT", "BIGINT"),
        ("DOUBLE", "DOUBLE PRECISION"),
        ("DECIMAL(15,2)", "DECIMAL(15,2)"),
        ("DATE", "DATE"),
        ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"),
    ],
)
def test_duckdb_types_map_to_postgres(duckdb_type: str, postgres_type: str) -> None:
    assert _postgres_type(duckdb_type) == postgres_type


def test_build_schema_name_is_safe_and_bounded() -> None:
    name = _build_schema_name("warehouse:2026/07/22-" + "x" * 100)
    assert name.startswith("reporting_build_")
    assert len(name) <= 63
    assert name.replace("_", "").isalnum()


@pytest.mark.skipif(
    not os.getenv("CMS_REPORTING_TEST_DSN"),
    reason="CMS_REPORTING_TEST_DSN is required for PostgreSQL integration",
)
def test_postgres_publish_keeps_curated_and_source_layers_queryable(
    tmp_path: Path,
) -> None:
    database = _warehouse(tmp_path / "releases" / "test" / "warehouse.duckdb")
    release = _release(tmp_path, database)
    resolved = resolve_release(tmp_path, release.warehouse_release_id)
    dsn = os.environ["CMS_REPORTING_TEST_DSN"]

    result = publish_release(
        release=resolved,
        data_root=tmp_path,
        postgres_dsn=dsn,
        temporary_root=tmp_path,
        minimum_free_bytes=0,
        reader_role=None,
    )

    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM reporting.dim_provider")
            assert cursor.fetchone()[0] == 2
            cursor.execute(
                'SELECT source_only_field FROM source_detail.source_nppes_provider '
                "ORDER BY npi LIMIT 1"
            )
            assert cursor.fetchone()[0] == "keep"
            cursor.execute("SELECT COUNT(*) FROM reporting.bridge_provider_practice")
            assert cursor.fetchone()[0] == 2
            cursor.execute("SELECT COUNT(*) FROM reporting.bridge_provider_taxonomy")
            assert cursor.fetchone()[0] == 3
            cursor.execute("SELECT COUNT(*) FROM reporting.fact_provider_drug_year")
            assert cursor.fetchone()[0] == 2
            cursor.execute(
                "SELECT payment_maker_country, third_party_recipient_name "
                "FROM source_detail.source_open_payments_general"
            )
            assert cursor.fetchone() == ("US", "Recipient A")
            cursor.execute(
                "SELECT source_column FROM control.column_lineage "
                "WHERE snapshot_id = %s AND model_name = %s AND model_column = %s",
                (
                    result.snapshot_id,
                    "source_open_payments_general",
                    "payment_maker_country",
                ),
            )
            assert cursor.fetchone()[0] == (
                "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"
            )
            cursor.execute(
                "SELECT COUNT(*) FROM control.column_lineage "
                "WHERE snapshot_id = %s AND layer = 'reporting'",
                (result.snapshot_id,),
            )
            assert cursor.fetchone()[0] == sum(
                len(model.fields) for model in REPORTING_MODELS
            )
