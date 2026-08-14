import sys
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.transform import (
    build_pecos_provider_relationships,
    build_provider_evidence_outputs,
    build_practice_locations,
    build_provider_drug_detail,
    build_provider_quality_scores,
    clear_refresh_targets,
    build_serving_practice_provider_sites,
)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute((REPOSITORY_ROOT / "schema" / "ddl.sql").read_text())
    connection.execute(
        """
        insert into core_providers (
            npi, last_org_name, entity_type_code, data_year
        ) values ('1234567890', 'Example', 'I', 2024)
        """
    )
    return connection


def test_refresh_can_commit_child_deletes_before_deleting_core_providers() -> None:
    connection = _connection()
    try:
        connection.execute(
            """
            insert into industry_relationships (
                npi, payment_year, paying_company_name, total_amount_received
            ) values ('1234567890', 2025, 'Example', 1.00)
            """
        )
        connection.execute("BEGIN TRANSACTION")
        clear_refresh_targets(connection, include_core_providers=False)
        connection.execute("COMMIT")
        connection.execute("DELETE FROM core_providers")

        assert connection.execute(
            "SELECT count(*) FROM industry_relationships"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM core_providers"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_qpp_transform_handles_boolean_inference_and_selects_best_npi_row() -> None:
    connection = _connection()
    try:
        connection.execute(
            """
            create table raw_qpp_experience (
                "provider key" varchar,
                npi bigint,
                "practice state or us territory" varchar,
                "practice size" varchar,
                "clinician type" varchar,
                "clinician specialty" varchar,
                "years in medicare" varchar,
                "participation option" varchar,
                "small practice status" boolean,
                "rural status" boolean,
                "health professional shortage area status" boolean,
                "hospital-based status" boolean,
                "facility-based status" boolean,
                "dual eligibility ratio" varchar,
                "final score" varchar,
                "payment adjustment percentage" varchar,
                "complex patient bonus" varchar,
                "quality category score" varchar,
                "quality category weight" varchar,
                "promoting interoperability (pi) category score" varchar,
                "promoting interoperability (pi) category weight" varchar,
                "improvement activities (ia) category score" varchar,
                "improvement activities (ia) category weight" varchar,
                "cost category score" varchar,
                "cost category weight" varchar
            )
            """
        )
        values = (
            "?, '1234567890', 'CA', 'small', 'doctor', 'cardiology', '10', "
            """'individual', ?, false, true, false, true, '.2', ?, '1', '2',
               '3', '4', '5', '6', '7', '8', '9', '10'"""
        )
        connection.execute(f"insert into raw_qpp_experience values ({values})", ["a", True, "80"])
        connection.execute(f"insert into raw_qpp_experience values ({values})", ["b", False, "90"])

        count = build_provider_quality_scores(connection, 2024)
        row = connection.execute(
            "select final_mips_score, small_practice_status, rural_status "
            "from provider_quality_scores"
        ).fetchone()
    finally:
        connection.close()

    assert count == 1
    assert row == (90, False, False)


def test_drug_transform_aggregates_duplicate_generic_drug_rows() -> None:
    connection = _connection()
    try:
        connection.execute(
            """
            create table raw_part_d_by_provider_and_drug (
                Prscrbr_NPI bigint, Brnd_Name varchar, Gnrc_Name varchar,
                Tot_Clms varchar, Tot_30day_Fills varchar, Tot_Day_Suply varchar,
                Tot_Drug_Cst varchar, Tot_Benes varchar, GE65_Tot_Clms varchar,
                GE65_Tot_Drug_Cst varchar, GE65_Tot_Benes varchar
            )
            """
        )
        connection.execute(
            """
            insert into raw_part_d_by_provider_and_drug values
                ('1234567890', 'Brand A', 'Generic X', '2', '2.5', '30',
                 '10.25', '2', '1', '5.25', '1'),
                ('1234567890', 'Brand B', 'Generic X', '3', '3.5', '60',
                 '20.75', '3', '2', '10.75', '2')
            """
        )

        count = build_provider_drug_detail(connection, 2024)
        row = connection.execute(
            "select generic_name, tot_claims, tot_30day_fills, tot_drug_cost "
            "from provider_drug_detail"
        ).fetchone()
    finally:
        connection.close()

    assert count == 1
    assert row == ("Generic X", 5, 6, 31)


def test_practice_transform_matches_numeric_raw_npi_to_text_core_npi() -> None:
    connection = _connection()
    try:
        connection.execute(
            """
            create table raw_reassignment (
                "Individual NPI" bigint,
                "Group PAC ID" varchar,
                "Group Enrollment ID" varchar,
                "Group Legal Business Name" varchar,
                "Group State Code" varchar,
                "Group Reassignments and Physician Assistants" bigint,
                "Individual State Code" varchar
            )
            """
        )
        connection.execute(
            """
            insert into raw_reassignment values
                (1234567890, 'PAC-1', 'ENROLL-1', 'Example Group', 'CA', 8, 'CA')
            """
        )

        count = build_practice_locations(connection, 2024)
        row = connection.execute(
            "select npi, group_legal_name, group_practice_size from practice_locations"
        ).fetchone()
    finally:
        connection.close()

    assert count == 1
    assert row == ("1234567890", "Example Group", 8)


def test_serving_practice_transform_preserves_grain_metrics_and_provenance() -> None:
    connection = _connection()
    try:
        connection.execute(
            '''
            CREATE TABLE raw_dac_national (
                "NPI" VARCHAR, "Provider First Name" VARCHAR,
                "Provider Last Name" VARCHAR, pri_spec VARCHAR,
                "Facility Name" VARCHAR, org_pac_id VARCHAR,
                num_org_mem INTEGER, adr_ln_1 VARCHAR, "ZIP Code" VARCHAR,
                "City/Town" VARCHAR, "State" VARCHAR,
                "Telephone Number" VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_dac_national VALUES
                ('1234567890', 'Jamie', 'Rivera', 'Electrophysiology',
                 'Cardio Group', ' PAC-1 ', 20, '10 MAIN ST', '90001',
                 'Los Angeles', 'CA', '111', 'dac-run', '2026-07'),
                ('1234567890', 'Jamie', 'Rivera', 'Cardiology',
                 'Cardio Group', 'PAC-1', 20, '10 MAIN ST', '90001',
                 'Los Angeles', 'CA', '111', 'dac-run', '2026-07');

            CREATE TABLE raw_physician_by_provider (
                "Rndrng_NPI" VARCHAR, "Tot_Mdcr_Pymt_Amt" DOUBLE,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_physician_by_provider VALUES
                ('1234567890', 125.25, 'partb-run', '2024'),
                ('1234567890', 125.25, 'partb-run', '2024');

            CREATE TABLE raw_part_d_by_provider (
                "PRSCRBR_NPI" VARCHAR, "Tot_Drug_Cst" DOUBLE,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_part_d_by_provider VALUES
                ('1234567890', 50.75, 'partd-run', '2024');

            CREATE TABLE address_geocode (addr_key VARCHAR, lat DOUBLE, lng DOUBLE);
            INSERT INTO address_geocode VALUES
                ('10 MAIN ST|90001', 34.1, -118.2),
                ('10 MAIN ST|90001', 34.2, -118.1);
            '''
        )

        count = build_serving_practice_provider_sites(connection, 2026)
        row = connection.execute(
            """
            SELECT addr_key, group_key, npi, specialties, latitude, longitude,
                   partb_payments, partd_drug_cost, dac_source_run_ids,
                   partb_source_data_periods, partd_source_run_ids, data_year
            FROM serving_practice_provider_sites
            """
        ).fetchone()
    finally:
        connection.close()

    assert count == 1
    assert row == (
        "10 MAIN ST|90001",
        "PAC-1",
        "1234567890",
        ["Cardiology", "Electrophysiology"],
        34.1,
        -118.2,
        125.25,
        50.75,
        ["dac-run"],
        ["2024"],
        ["partd-run"],
        2026,
    )


def test_serving_practice_transform_rejects_missing_dac_provenance() -> None:
    connection = _connection()
    try:
        connection.execute(
            '''
            CREATE TABLE raw_dac_national (
                "NPI" VARCHAR, "Provider First Name" VARCHAR,
                "Provider Last Name" VARCHAR, pri_spec VARCHAR,
                "Facility Name" VARCHAR, org_pac_id VARCHAR,
                num_org_mem INTEGER, adr_ln_1 VARCHAR, "ZIP Code" VARCHAR,
                "City/Town" VARCHAR, "State" VARCHAR,
                "Telephone Number" VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_dac_national VALUES
                ('1234567890', 'Jamie', 'Rivera', 'Cardiology', NULL, NULL, 1,
                 '10 MAIN ST', '90001', 'Los Angeles', 'CA', NULL, NULL, '2026-07');
            CREATE TABLE raw_physician_by_provider (
                "Rndrng_NPI" VARCHAR, "Tot_Mdcr_Pymt_Amt" DOUBLE,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            CREATE TABLE raw_part_d_by_provider (
                "PRSCRBR_NPI" VARCHAR, "Tot_Drug_Cst" DOUBLE,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            CREATE TABLE address_geocode (addr_key VARCHAR, lat DOUBLE, lng DOUBLE);
            '''
        )

        with pytest.raises(ValueError, match="without source provenance: 1"):
            build_serving_practice_provider_sites(connection, 2026)
    finally:
        connection.close()


def test_provider_evidence_outputs_keep_address_and_organization_sources_separate() -> None:
    connection = _connection()
    try:
        connection.execute(
            """
            CREATE TABLE raw_nppes (
                npi VARCHAR, practice_address_1 VARCHAR, practice_address_2 VARCHAR,
                practice_city VARCHAR, practice_state VARCHAR, practice_zip VARCHAR,
                practice_country VARCHAR, source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_nppes VALUES
                ('1234567890', '101 Registry Way', NULL, 'Los Angeles', 'CA', '90001',
                 'US', 'nppes-run', '2026-07-01');

            CREATE TABLE raw_dac_national (
                "NPI" VARCHAR, adr_ln_1 VARCHAR, adr_ln_2 VARCHAR, "City/Town" VARCHAR,
                "State" VARCHAR, "ZIP Code" VARCHAR, adrs_id VARCHAR, org_pac_id VARCHAR,
                "Facility Name" VARCHAR, source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_dac_national VALUES
                ('1234567890', '202 DAC Way', 'Suite 2', 'Pasadena', 'CA', '91101', 'DAC-ADDR',
                 'ORG-PAC-1', 'Example DAC Group', 'dac-run', '2026-07-01');

            CREATE TABLE raw_physician_by_provider (
                Rndrng_NPI VARCHAR, Rndrng_Prvdr_City VARCHAR,
                Rndrng_Prvdr_State_Abrvtn VARCHAR, Rndrng_Prvdr_Zip5 VARCHAR,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_physician_by_provider VALUES
                ('1234567890', 'Santa Monica', 'CA', '90401', 'medicare-run', '2024');

            CREATE TABLE raw_open_payments_general (
                Covered_Recipient_NPI VARCHAR,
                Recipient_Primary_Business_Street_Address_Line1 VARCHAR,
                Recipient_Primary_Business_Street_Address_Line2 VARCHAR,
                Recipient_City VARCHAR, Recipient_State VARCHAR,
                Recipient_Zip_Code VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_open_payments_general VALUES
                ('1234567890', '303 Payments Way', NULL, 'Burbank', 'CA', '91501',
                 'payments-run', '2023');

            CREATE TABLE raw_reassignment (
                "Individual NPI" VARCHAR, "Group PAC ID" VARCHAR,
                "Group Legal Business Name" VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_reassignment VALUES
                ('1234567890', 'GROUP-PAC-1', 'Example Legal Group', 'reassign-run', '2026-07-01');

            CREATE TABLE raw_dac_facility_affiliations (
                "NPI" VARCHAR, "Facility Affiliations Certification Number" VARCHAR,
                "Facility Type Certification Number" VARCHAR
            );
            INSERT INTO raw_dac_facility_affiliations VALUES
                ('1234567890', '050001', NULL);

            INSERT INTO pecos_provider_organizations VALUES
                ('relationship-1', '1234567890', 'IND-1', 'ORG-ENROLL-1', '1098765432',
                 'Example PECOS Group', 'organization', '12-00', 'Group Practice', 'CA',
                 '2026-07-01', 'pecos-reassign-run', 'pecos-enroll-run');
            INSERT INTO pecos_enrollment_practice_locations VALUES
                ('pecos-location-1', 'ORG-ENROLL-1', '1098765432', 'Example PECOS Group',
                 'organization', 'Long Beach', 'CA', '90802', '90802', '2026-07-01',
                 'pecos-location-run', 'pecos-enroll-run');

            INSERT INTO provider_hospital_evidence VALUES
                ('hospital-evidence-1', '1234567890', '1098765432', '050001',
                 'Example Hospital', 'Los Angeles', 'CA', '90033', 'reassignment', 'medium',
                 'GROUP-PAC-1', NULL, NULL, NULL, NULL, '2026-07-01', 2026);
            """
        )

        counts = build_provider_evidence_outputs(connection, 2026)
        assert build_provider_evidence_outputs(connection, 2026) == counts
        addresses = connection.execute(
            """
            SELECT relationship_type, source_tables, data_year
            FROM provider_address_evidence ORDER BY relationship_type
            """
        ).fetchall()
        organizations = connection.execute(
            """
            SELECT organization_identifier_type, organization_identifier, organization_name,
                   evidence_kind
            FROM provider_organization_evidence
            ORDER BY organization_identifier_type
            """
        ).fetchall()
        pecos_provenance = connection.execute(
            """
            SELECT source_data_period, source_run_id, source_data_periods, source_run_ids
            FROM provider_address_evidence
            WHERE relationship_type = 'receiving_organization_published_location'
            """
        ).fetchone()
        indexes = {
            row[0]
            for row in connection.execute(
                """
                SELECT index_name FROM duckdb_indexes()
                WHERE table_name IN (
                    'provider_address_evidence', 'provider_organization_evidence'
                )
                """
            ).fetchall()
        }
    finally:
        connection.close()

    assert counts == {
        "provider_address_evidence": 5,
        "provider_organization_evidence": 5,
    }
    assert addresses == [
        ("clinician_practice_address", "raw_dac_national", 2026),
        ("medicare_rendering_location", "raw_physician_by_provider", 2024),
        ("payment_recipient_business_address", "raw_open_payments_general", 2023),
        (
            "receiving_organization_published_location",
            "raw_pecos_reassignment + raw_pecos_practice_location",
            2026,
        ),
        ("registered_practice_address", "raw_nppes", 2026),
    ]
    assert ("group_pac_id", "GROUP-PAC-1", "Example Legal Group", "publisher_asserted") in organizations
    assert ("hospital_npi", "1098765432", "Example Hospital", "derived_or_inferred") in organizations
    assert pecos_provenance == (
        "2026-07-01",
        "pecos-location-run",
        ["2026-07-01"],
        ["pecos-enroll-run", "pecos-location-run", "pecos-reassign-run"],
    )
    assert indexes == {
        "idx_provider_address_evidence_location",
        "idx_provider_address_evidence_npi",
        "idx_provider_organization_evidence_identifier",
        "idx_provider_organization_evidence_npi",
    }


def test_pecos_relationship_transform_preserves_assignment_and_location_grains() -> None:
    connection = _connection()
    try:
        connection.execute(
            """
            CREATE TABLE raw_pecos_enrollment (
                NPI VARCHAR, ENRLMT_ID VARCHAR, ORG_NAME VARCHAR,
                PROVIDER_TYPE_CD VARCHAR, PROVIDER_TYPE_DESC VARCHAR,
                STATE_CD VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_pecos_enrollment VALUES
                ('1234567890', 'I20031103000001', NULL, '14-00', 'Physician',
                 'CA', 'enrollment-run', '2026-01-01/2026-03-31'),
                ('1234567891', 'I20031103000002', NULL, '14-00', 'Physician',
                 'CA', 'enrollment-run', '2026-01-01/2026-03-31'),
                ('1098765432', 'O20031216000213', 'Example Group', '12-00',
                 'Group Practice', 'CA', 'enrollment-run', '2026-01-01/2026-03-31');

            CREATE TABLE raw_pecos_reassignment (
                REASGN_BNFT_ENRLMT_ID VARCHAR, RCV_BNFT_ENRLMT_ID VARCHAR,
                source_run_id VARCHAR, source_data_period VARCHAR
            );
            INSERT INTO raw_pecos_reassignment VALUES
                ('I20031103000001', 'O20031216000213', 'relationship-run',
                 '2026-01-01/2026-03-31'),
                ('I20031103000002', 'O20031216000213', 'relationship-run',
                 '2026-01-01/2026-03-31');

            CREATE TABLE raw_pecos_practice_location (
                ENRLMT_ID VARCHAR, CITY_NAME VARCHAR, STATE_CD VARCHAR,
                ZIP_CD VARCHAR, source_run_id VARCHAR,
                source_data_period VARCHAR
            );
            INSERT INTO raw_pecos_practice_location VALUES
                ('O20031216000213', 'LOS ANGELES', 'CA', '900480001',
                 'location-run', '2026-01-01/2026-03-31'),
                ('O20031216000213', 'PASADENA', 'CA', '911010001',
                 'location-run', '2026-01-01/2026-03-31');
            """
        )

        counts = build_pecos_provider_relationships(connection)
        organization = connection.execute(
            """
            SELECT npi, provider_enrollment_id, receiving_enrollment_id,
                   receiving_organization_name, receiving_entity_kind
            FROM pecos_provider_organizations
            ORDER BY npi
            """
        ).fetchone()
        location = connection.execute(
            """
            SELECT receiving_enrollment_id, city, state, zip_code, zip5
            FROM pecos_enrollment_practice_locations
            ORDER BY city
            """
        ).fetchone()
    finally:
        connection.close()

    assert counts == {
        "pecos_provider_organizations": 2,
        "pecos_enrollment_practice_locations": 2,
    }
    assert organization == (
        "1234567890",
        "I20031103000001",
        "O20031216000213",
        "Example Group",
        "organization",
    )
    assert location == (
        "O20031216000213",
        "LOS ANGELES",
        "CA",
        "900480001",
        "90048",
    )
