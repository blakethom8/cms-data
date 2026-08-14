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
    _profile_locations,
    _profile_mart_is_available,
    get_profiles_router,
)
from pipeline.transform import build_serving_provider_profile_core_tables


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
