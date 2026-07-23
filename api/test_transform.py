import sys
from pathlib import Path

import duckdb

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.transform import (
    build_pecos_provider_relationships,
    build_practice_locations,
    build_provider_drug_detail,
    build_provider_quality_scores,
    clear_refresh_targets,
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
