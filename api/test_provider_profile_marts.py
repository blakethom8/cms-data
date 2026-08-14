"""S3 provider-profile serving tables preserve the existing core/access response."""

import sys
from pathlib import Path

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

import profiles
from profiles import (
    CRED,
    TELE,
    _affiliation_groups,
    _profile_header,
    _profile_claims_mart_is_available,
    _profile_claims_summary,
    _profile_locations,
    _profile_mart_is_available,
    _profile_top_drugs,
    _profile_top_procedures,
    get_profiles_router,
)
from pipeline.transform import (
    build_serving_provider_profile_claims_tables,
    build_serving_provider_profile_core_tables,
)


def _database() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(
        f"""
        CREATE TABLE raw_nppes (
            npi VARCHAR, first_name VARCHAR, last_name VARCHAR,
            credentials VARCHAR, practice_address_1 VARCHAR,
            practice_address_2 VARCHAR, practice_city VARCHAR,
            practice_state VARCHAR, practice_zip VARCHAR,
            practice_phone VARCHAR, taxonomy_1 VARCHAR,
            source_run_id VARCHAR, source_data_period VARCHAR
        );
        CREATE TABLE raw_dac_national (
            "NPI" VARCHAR, "Provider First Name" VARCHAR,
            "Provider Last Name" VARCHAR, {CRED} VARCHAR,
            pri_spec VARCHAR, sec_spec_all VARCHAR, "City/Town" VARCHAR,
            "State" VARCHAR, Med_sch VARCHAR, Grd_yr INTEGER,
            {TELE} VARCHAR, "Facility Name" VARCHAR, org_pac_id VARCHAR,
            num_org_mem INTEGER, adr_ln_1 VARCHAR, adr_ln_2 VARCHAR,
            "ZIP Code" VARCHAR, "Telephone Number" VARCHAR,
            source_run_id VARCHAR, source_data_period VARCHAR
        );
        CREATE TABLE raw_reassignment (
            "Individual NPI" VARCHAR, "Group PAC ID" VARCHAR,
            "Group Legal Business Name" VARCHAR,
            "Group Reassignments and Physician Assistants" BIGINT,
            source_run_id VARCHAR, source_data_period VARCHAR
        );
        CREATE TABLE nucc_taxonomy (
            taxonomy_code VARCHAR, classification VARCHAR,
            specialization VARCHAR
        );
        CREATE TABLE address_geocode (addr_key VARCHAR, lat DOUBLE, lng DOUBLE);

        INSERT INTO nucc_taxonomy VALUES
            ('207RC0000X', 'Internal Medicine', 'Cardiovascular Disease');
        INSERT INTO raw_nppes VALUES
            ('1111111111', 'JANE', 'SMITH', 'M.D.', '10 MAIN ST', 'SUITE 200',
             'LOS ANGELES', 'CA', '90001', '111', '207RC0000X',
             'nppes-run', '2026-07'),
            ('2222222222', 'ALEX', 'RIVER', 'DO', '10 MAIN ST', NULL,
             'LOS ANGELES', 'CA', '90001', '222', '207RC0000X',
             'nppes-run', '2026-07');
        INSERT INTO raw_dac_national VALUES
            ('1111111111', 'JANE', 'SMITH', 'MD', 'Cardiology',
             'Internal Medicine', 'LOS ANGELES', 'CA', 'UCLA', 2005, 'Y',
             'CARDIO GROUP', 'PAC-1', 20, '10 MAIN ST', 'SUITE 200',
             '90001', '111', 'dac-run', '2026-07'),
            ('1111111111', 'JANE', 'SMITH', 'MD', 'Internal Medicine',
             'Cardiology', 'PASADENA', 'CA', 'USC', 2004, 'N',
             'CARDIO GROUP', 'PAC-1', 20, '10 MAIN ST', 'SUITE 200',
             '90001', '111', 'dac-run', '2026-07'),
            ('2222222222', 'ALEX', 'RIVER', 'DO', 'Cardiology', NULL,
             'LOS ANGELES', 'CA', 'USC', 2010, 'N', 'CARDIO GROUP',
             'PAC-1', 20, '10 MAIN ST', NULL, '90001', '222',
             'dac-run', '2026-07');
        INSERT INTO raw_reassignment VALUES
            ('1111111111', 'PAC-1', 'Cardio Group Legal', 25,
             'reassign-run', '2026-07'),
            ('1111111111', 'PAC-1', 'Cardio Group Legal', 50,
             'reassign-run', '2026-07'),
            ('1111111111', 'PAC-0', 'Another Regional Medical Group', 100,
             'reassign-run', '2026-07'),
            ('1111111111', 'PAC-2', 'Regional Medical Group', 100,
             'reassign-run', '2026-07');
        INSERT INTO address_geocode VALUES ('10 MAIN ST|90001', 34.1, -118.2);
        """
    )
    return connection


def _claims_database() -> duckdb.DuckDBPyConnection:
    connection = _database()
    connection.execute(
        """
        CREATE TABLE raw_physician_by_provider (
            Rndrng_NPI VARCHAR, Rndrng_Prvdr_Ent_Cd VARCHAR,
            Tot_Benes BIGINT, Tot_Srvcs DOUBLE, Tot_Mdcr_Alowd_Amt DOUBLE,
            Drug_Mdcr_Pymt_Amt DOUBLE, Bene_Avg_Age BIGINT,
            Bene_Age_75_84_Cnt BIGINT, Bene_Age_GT_84_Cnt BIGINT,
            Bene_Feml_Cnt BIGINT, Bene_Dual_Cnt BIGINT,
            Bene_Avg_Risk_Scre DOUBLE,
            Bene_CC_PH_Hypertension_V2_Pct DOUBLE,
            Bene_CC_PH_Hyperlipidemia_V2_Pct DOUBLE,
            Bene_CC_PH_Diabetes_V2_Pct DOUBLE,
            Bene_CC_PH_IschemicHeart_V2_Pct DOUBLE,
            Bene_CC_PH_HF_NonIHD_V2_Pct DOUBLE,
            Bene_CC_PH_Afib_V2_Pct DOUBLE, Bene_CC_PH_CKD_V2_Pct DOUBLE,
            Bene_CC_PH_COPD_V2_Pct DOUBLE,
            Bene_CC_BH_Depress_V1_Pct DOUBLE,
            source_run_id VARCHAR, source_data_period VARCHAR
        );
        CREATE TABLE raw_physician_by_provider_and_service (
            Rndrng_NPI VARCHAR, Rndrng_Prvdr_Type VARCHAR, HCPCS_Cd VARCHAR,
            HCPCS_Desc VARCHAR, HCPCS_Drug_Ind VARCHAR, Place_Of_Srvc VARCHAR,
            Tot_Srvcs DOUBLE, Tot_Benes BIGINT, Avg_Mdcr_Pymt_Amt DOUBLE,
            source_run_id VARCHAR, source_data_period VARCHAR
        );
        CREATE TABLE raw_part_d_by_provider (
            Prscrbr_NPI VARCHAR, Tot_Clms BIGINT, Tot_Benes BIGINT,
            Tot_Drug_Cst DOUBLE, Brnd_Tot_Clms BIGINT, Gnrc_Tot_Clms BIGINT,
            Brnd_Tot_Drug_Cst DOUBLE, Opioid_Prscrbr_Rate DOUBLE,
            LIS_Tot_Clms BIGINT, Bene_Avg_Age DOUBLE,
            Bene_Avg_Risk_Scre DOUBLE, source_run_id VARCHAR,
            source_data_period VARCHAR
        );
        CREATE TABLE raw_part_d_by_provider_and_drug (
            Prscrbr_NPI VARCHAR, Brnd_Name VARCHAR, Gnrc_Name VARCHAR,
            Tot_Clms BIGINT, Tot_Benes BIGINT, Tot_Drug_Cst DOUBLE,
            Tot_Day_Suply BIGINT, source_run_id VARCHAR,
            source_data_period VARCHAR
        );

        INSERT INTO raw_physician_by_provider VALUES
            ('1111111111', 'I', 100, 250.0, 50000.4, 1200.6, 76,
             30, 20, 60, 25, 1.7, 80, 70, 30, 20, 15, 10, 12, 8, 22,
             'partb-provider-run', '2024'),
            ('3333333333', 'I', 10, 20.0, 100.0, 0.0, 70,
             2, 1, 5, 1, 1.1, 10, 10, 10, 10, 10, 10, 10, 10, 10,
             'partb-provider-run', '2024');
        INSERT INTO raw_physician_by_provider_and_service VALUES
            ('1111111111', 'Cardiology', '99213', 'Z description', 'N', 'F',
             10.0, 9, 10.0, 'partb-service-run', '2024'),
            ('1111111111', 'Cardiology', '99213', 'A description', 'N', 'O',
             5.0, 7, 20.0, 'partb-service-run', '2024'),
            ('1111111111', 'Cardiology', '93000', 'ECG', 'Y', 'O',
             20.0, 15, 5.0, 'partb-service-run', '2024');
        INSERT INTO raw_part_d_by_provider VALUES
            ('1111111111', 50, 20, 1500.4, 10, 40, 1000.0, 2.0, 25,
             75.5, 1.8, 'partd-provider-run', '2024');
        INSERT INTO raw_part_d_by_provider_and_drug VALUES
            ('1111111111', 'Brand Z', 'Generic Z', 10, 5, 1000.4, 300,
             'partd-drug-run', '2024'),
            ('1111111111', 'Brand A', 'Generic A', 20, 10, 500.4, 600,
             'partd-drug-run', '2024');
        """
    )
    build_serving_provider_profile_core_tables(connection, 2026)
    return connection


def test_profile_core_marts_are_idempotent_and_byte_shape_equivalent() -> None:
    connection = _database()
    try:
        raw = {
            "header": _profile_header(connection, "1111111111"),
            "locations": _profile_locations(connection, "1111111111"),
            "groups": _affiliation_groups(connection, "1111111111"),
        }

        counts = build_serving_provider_profile_core_tables(connection, 2026)
        assert build_serving_provider_profile_core_tables(connection, 2026) == counts

        mart = {
            "header": _profile_header(connection, "1111111111", backend="mart"),
            "locations": _profile_locations(
                connection, "1111111111", backend="mart"
            ),
            "groups": _affiliation_groups(
                connection, "1111111111", backend="mart"
            ),
        }
    finally:
        connection.close()

    assert [group["group_id"] for group in raw["groups"]] == [
        "PAC-1",
        "PAC-0",
        "PAC-2",
    ]
    assert raw["header"]["specialty"] == "Cardiology"
    assert raw["header"]["secondary_specialties"] == "Internal Medicine"
    assert raw["header"]["med_school"] == "UCLA"
    assert counts == {
        "serving_provider_profile_headers": 2,
        "serving_provider_profile_locations": 2,
        "serving_provider_profile_groups": 4,
    }
    assert mart == raw


def test_profile_core_marts_preserve_separate_grains_and_provenance() -> None:
    connection = _database()
    try:
        build_serving_provider_profile_core_tables(connection, 2026)
        header = connection.execute(
            """
            SELECT npi, nppes_source_run_ids, dac_source_run_ids, data_year
            FROM serving_provider_profile_headers WHERE npi = '1111111111'
            """
        ).fetchone()
        location = connection.execute(
            """
            SELECT npi, addr_key, roster_size, sources,
                   nppes_source_data_periods, dac_source_data_periods
            FROM serving_provider_profile_locations WHERE npi = '1111111111'
            """
        ).fetchone()
        groups = connection.execute(
            """
            SELECT group_id, sources, dac_source_run_ids,
                   reassignment_source_run_ids
            FROM serving_provider_profile_groups
            WHERE npi = '1111111111' ORDER BY group_id
            """
        ).fetchall()
    finally:
        connection.close()

    assert header == ("1111111111", ["nppes-run"], ["dac-run"], 2026)
    assert location == (
        "1111111111",
        "10 MAIN ST|90001",
        2,
        "dac + nppes",
        ["2026-07"],
        ["2026-07"],
    )
    assert groups == [
        ("PAC-0", "reassignment", [], ["reassign-run"]),
        ("PAC-1", "dac + reassignment", ["dac-run"], ["reassign-run"]),
        ("PAC-2", "reassignment", [], ["reassign-run"]),
    ]


def test_profile_claims_marts_are_idempotent_and_response_shape_equivalent() -> None:
    connection = _claims_database()
    try:
        raw = {
            **_profile_claims_summary(connection, "1111111111"),
            "top_procedures": _profile_top_procedures(connection, "1111111111"),
            "top_drugs": _profile_top_drugs(connection, "1111111111"),
        }
        counts = build_serving_provider_profile_claims_tables(connection, 2026)
        assert build_serving_provider_profile_claims_tables(connection, 2026) == counts
        mart = {
            **_profile_claims_summary(connection, "1111111111", backend="mart"),
            "top_procedures": _profile_top_procedures(
                connection, "1111111111", backend="mart"
            ),
            "top_drugs": _profile_top_drugs(
                connection, "1111111111", backend="mart"
            ),
        }
    finally:
        connection.close()

    assert counts == {
        "serving_provider_profile_claims_summary": 1,
        "serving_provider_profile_top_services": 2,
        "serving_provider_profile_top_drugs": 2,
    }
    assert raw["top_procedures"][0]["description"] == "A description"
    assert raw == mart


def test_profile_claims_marts_preserve_provenance_and_nppes_only_defaults() -> None:
    connection = _claims_database()
    try:
        build_serving_provider_profile_claims_tables(connection, 2026)
        summary = connection.execute(
            """
            SELECT part_b_provider_source_run_ids,
                   part_b_service_source_run_ids,
                   part_d_provider_source_run_ids, data_year
            FROM serving_provider_profile_claims_summary
            WHERE npi = '1111111111'
            """
        ).fetchone()
        service = connection.execute(
            """
            SELECT service_rank, source_run_ids
            FROM serving_provider_profile_top_services
            WHERE npi = '1111111111' ORDER BY service_rank
            """
        ).fetchall()
        drug = connection.execute(
            """
            SELECT drug_rank, source_run_ids
            FROM serving_provider_profile_top_drugs
            WHERE npi = '1111111111' ORDER BY drug_rank
            """
        ).fetchall()
        raw_missing = _profile_claims_summary(connection, "2222222222")
        mart_missing = _profile_claims_summary(
            connection, "2222222222", backend="mart"
        )
    finally:
        connection.close()

    assert summary == (
        ["partb-provider-run"], ["partb-service-run"],
        ["partd-provider-run"], 2026,
    )
    assert service == [
        (1, ["partb-service-run"]), (2, ["partb-service-run"]),
    ]
    assert drug == [(1, ["partd-drug-run"]), (2, ["partd-drug-run"])]
    assert mart_missing == raw_missing


def test_profile_claims_mart_capability_requires_all_three_complete_tables() -> None:
    connection = _claims_database()
    try:
        assert _profile_claims_mart_is_available(connection) is False
        build_serving_provider_profile_claims_tables(connection, 2026)
        assert _profile_claims_mart_is_available(connection) is True
        connection.execute("DROP TABLE serving_provider_profile_top_drugs")
        connection.execute(
            "CREATE TABLE serving_provider_profile_top_drugs "
            "(npi VARCHAR, drug_rank INTEGER)"
        )
        assert _profile_claims_mart_is_available(connection) is False
    finally:
        connection.close()


def test_profile_claims_marts_fail_closed_on_missing_provenance() -> None:
    connection = _claims_database()
    try:
        connection.execute(
            "UPDATE raw_part_d_by_provider_and_drug SET source_run_id = NULL"
        )
        with pytest.raises(
            ValueError, match="Part D drug rows without source provenance: 2"
        ):
            build_serving_provider_profile_claims_tables(connection, 2026)
    finally:
        connection.close()


def test_profile_claims_marts_fail_closed_on_duplicate_source_grain() -> None:
    connection = _claims_database()
    try:
        connection.execute(
            """
            INSERT INTO raw_part_d_by_provider_and_drug
            SELECT * FROM raw_part_d_by_provider_and_drug LIMIT 1
            """
        )
        with pytest.raises(
            ValueError, match="Part D drug NPI/brand/generic grain: 1 duplicate"
        ):
            build_serving_provider_profile_claims_tables(connection, 2026)
    finally:
        connection.close()


def test_profile_mart_capability_requires_all_three_complete_tables() -> None:
    connection = _database()
    try:
        assert _profile_mart_is_available(connection) is False
        build_serving_provider_profile_core_tables(connection, 2026)
        assert _profile_mart_is_available(connection) is True
        connection.execute(
            "DROP TABLE serving_provider_profile_groups"
        )
        connection.execute(
            "CREATE TABLE serving_provider_profile_groups "
            "(npi VARCHAR, group_id VARCHAR)"
        )
        assert _profile_mart_is_available(connection) is False
    finally:
        connection.close()


@pytest.mark.parametrize(("build_marts", "expected_backend"), [(False, "raw"), (True, "mart")])
def test_profile_auto_selector_falls_back_or_uses_the_complete_capability(
    monkeypatch: pytest.MonkeyPatch, build_marts: bool, expected_backend: str
) -> None:
    connection = _database()
    if build_marts:
        build_serving_provider_profile_core_tables(connection, 2026)
    selected: list[str] = []

    def header(_connection, npi: str, *, backend: str = "raw") -> dict:
        selected.append(backend)
        return {"npi": npi, "name": "JANE SMITH", "state": "CA"}

    def locations(_connection, _npi: str, *, backend: str = "raw") -> list[dict]:
        selected.append(backend)
        return []

    def groups(_connection, _npi: str, *, backend: str = "raw") -> list[dict]:
        selected.append(backend)
        return []

    monkeypatch.setattr(profiles, "_profile_header", header)
    monkeypatch.setattr(profiles, "_profile_locations", locations)
    monkeypatch.setattr(profiles, "_affiliation_groups", groups)
    monkeypatch.setattr(profiles, "_hospital_affiliations", lambda *_args: [])
    monkeypatch.setattr(profiles, "industry_summary", lambda *_args: None)
    monkeypatch.setattr(profiles, "_row", lambda *_args: None)
    monkeypatch.setattr(profiles, "_rows", lambda *_args: [])
    profiles._mips_stats.clear()

    app = FastAPI()
    app.include_router(
        get_profiles_router(lambda: connection, provider_profile_backend="auto")
    )
    try:
        response = TestClient(app).get("/profiles/1111111111")
    finally:
        connection.close()

    assert response.status_code == 200
    assert selected == [expected_backend, expected_backend, expected_backend]


@pytest.mark.parametrize(
    ("build_claims_marts", "expected_claims_backend"),
    [(False, "raw"), (True, "mart")],
)
def test_profile_auto_selector_resolves_claims_capability_independently(
    monkeypatch: pytest.MonkeyPatch,
    build_claims_marts: bool,
    expected_claims_backend: str,
) -> None:
    connection = _claims_database()
    if build_claims_marts:
        build_serving_provider_profile_claims_tables(connection, 2026)
    selected: list[tuple[str, str]] = []

    def header(_connection, npi: str, *, backend: str = "raw") -> dict:
        selected.append(("core", backend))
        return {"npi": npi, "name": "JANE SMITH", "state": "CA"}

    def claims(_connection, _npi: str, *, backend: str = "raw") -> dict:
        selected.append(("claims", backend))
        return {"panel": None, "clinical": None, "prescribing": None}

    def procedures(_connection, _npi: str, *, backend: str = "raw") -> list:
        selected.append(("procedures", backend))
        return []

    def drugs(_connection, _npi: str, *, backend: str = "raw") -> list:
        selected.append(("drugs", backend))
        return []

    monkeypatch.setattr(profiles, "_profile_header", header)
    monkeypatch.setattr(profiles, "_profile_claims_summary", claims)
    monkeypatch.setattr(profiles, "_profile_top_procedures", procedures)
    monkeypatch.setattr(profiles, "_profile_top_drugs", drugs)
    monkeypatch.setattr(profiles, "_profile_locations", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(profiles, "_affiliation_groups", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(profiles, "_hospital_affiliations", lambda *_args: [])
    monkeypatch.setattr(profiles, "industry_summary", lambda *_args: None)
    monkeypatch.setattr(profiles, "_row", lambda *_args: None)
    monkeypatch.setattr(profiles, "_rows", lambda *_args: [])
    profiles._mips_stats.clear()

    app = FastAPI()
    app.include_router(
        get_profiles_router(lambda: connection, provider_profile_backend="auto")
    )
    try:
        response = TestClient(app).get("/profiles/1111111111")
    finally:
        connection.close()

    assert response.status_code == 200
    assert selected == [
        ("core", "mart"),
        ("claims", expected_claims_backend),
        ("procedures", expected_claims_backend),
        ("drugs", expected_claims_backend),
    ]


@pytest.mark.parametrize(
    ("table", "run_column", "predicate", "label"),
    [
        ("raw_nppes", "source_run_id", "npi = '1111111111'", "NPPES"),
        (
            "raw_dac_national",
            "source_run_id",
            '"NPI" = \'1111111111\' AND pri_spec = \'Cardiology\'',
            "DAC",
        ),
        (
            "raw_reassignment",
            "source_run_id",
            '"Group PAC ID" = \'PAC-2\'',
            "reassignment",
        ),
    ],
)
def test_profile_core_marts_fail_closed_on_missing_provenance(
    table: str, run_column: str, predicate: str, label: str
) -> None:
    connection = _database()
    try:
        connection.execute(
            f"UPDATE {table} SET {run_column} = NULL WHERE {predicate}"
        )
        with pytest.raises(
            ValueError, match=f"{label} rows without source provenance: 1"
        ):
            build_serving_provider_profile_core_tables(connection, 2026)
    finally:
        connection.close()
