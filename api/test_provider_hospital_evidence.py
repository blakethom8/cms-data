import sys
from pathlib import Path

import duckdb

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.transform import build_provider_hospital_evidence


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE core_providers (npi VARCHAR PRIMARY KEY)")
    connection.execute("INSERT INTO core_providers VALUES ('1111111111')")
    connection.execute(
        """
        CREATE TABLE raw_hospital_enrollments (
            enrollment_id VARCHAR,
            npi VARCHAR,
            ccn VARCHAR,
            organization_name VARCHAR,
            doing_business_as_name VARCHAR,
            address_line_1 VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO raw_hospital_enrollments VALUES
            ('H-1', '9999999991', 'CCN-1', 'Metro Hospital', 'Metro',
             '1 Main St', 'Los Angeles', 'CA', '90001'),
            ('H-2', '9999999992', 'CCN-2', 'Shared Hospital', NULL,
             '2 Main St', 'Los Angeles', 'CA', '90002'),
            ('H-3', '9999999993', 'CCN-3', 'Shared Hospital', NULL,
             '2 Main St', 'Los Angeles', 'CA', '90002')
        """
    )
    connection.execute(
        """
        CREATE TABLE hospital_affiliations (
            npi VARCHAR,
            hospital_npi VARCHAR,
            hospital_ccn VARCHAR,
            hospital_name VARCHAR,
            hospital_city VARCHAR,
            hospital_state VARCHAR,
            hospital_zip VARCHAR,
            hospital_subgroup VARCHAR,
            affiliation_source VARCHAR,
            confidence_level VARCHAR,
            group_pac_id VARCHAR,
            data_year INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO hospital_affiliations VALUES
            ('1111111111', '9999999991', 'CCN-1', 'Metro Hospital',
             'Los Angeles', 'CA', '90001', 'acute_care',
             'cms_reassignment_legal_name_state', 'medium', 'GROUP-1', 2026)
        """
    )
    connection.execute(
        """
        CREATE TABLE pecos_provider_organizations (
            npi VARCHAR,
            provider_enrollment_id VARCHAR,
            receiving_enrollment_id VARCHAR,
            receiving_npi VARCHAR,
            source_data_period VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO pecos_provider_organizations VALUES
            ('1111111111', 'PROV-1', 'REC-1', '9999999991', '2026-Q1')
        """
    )
    connection.execute(
        """
        CREATE TABLE raw_dac_national (
            "NPI" VARCHAR,
            org_pac_id VARCHAR,
            "Facility Name" VARCHAR,
            adrs_id VARCHAR,
            adr_ln_1 VARCHAR,
            "City/Town" VARCHAR,
            "State" VARCHAR,
            "ZIP Code" VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO raw_dac_national VALUES
            ('1111111111', 'ORG-1', 'Metro Hospital', 'DAC-1',
             '1 Main St', 'Los Angeles', 'CA', '90001'),
            ('1111111111', 'ORG-2', 'Shared Hospital', 'DAC-2',
             '2 Main St', 'Los Angeles', 'CA', '90002')
        """
    )
    return connection


def test_provider_hospital_evidence_preserves_methods_and_excludes_ambiguous_dac() -> None:
    connection = _connection()
    try:
        assert build_provider_hospital_evidence(connection, data_year=2026) == 3
        rows = connection.execute(
            """
            SELECT evidence_method, confidence_level, hospital_npi,
                   group_pac_id, organization_pac_id, dac_address_id,
                   provider_enrollment_id, receiving_enrollment_id
            FROM provider_hospital_evidence
            ORDER BY evidence_method
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        (
            "cms_reassignment_legal_name_state",
            "medium",
            "9999999991",
            "GROUP-1",
            None,
            None,
            None,
            None,
        ),
        (
            "dac_hospital_organization_name_address",
            "medium",
            "9999999991",
            None,
            "ORG-1",
            "DAC-1",
            None,
            None,
        ),
        (
            "pecos_receiving_npi_match",
            "high",
            "9999999991",
            None,
            None,
            None,
            "PROV-1",
            "REC-1",
        ),
    ]
