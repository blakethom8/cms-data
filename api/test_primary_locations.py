"""NPPES-primary location attribution contracts for Medicare exploration."""

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from practices import get_practices_router


def _database() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(
        '''
        create table raw_physician_by_provider (
            "Rndrng_NPI" varchar,
            "Rndrng_Prvdr_Type" varchar,
            "Tot_Mdcr_Pymt_Amt" double,
            "Tot_Srvcs" double,
            "Tot_Benes" double
        )
        '''
    )
    connection.execute(
        """
        create table raw_nppes (
            npi varchar,
            first_name varchar,
            last_name varchar,
            credentials varchar,
            practice_address_1 varchar,
            practice_city varchar,
            practice_state varchar,
            practice_zip varchar,
            practice_phone varchar,
            deactivation_date varchar
        )
        """
    )
    connection.execute(
        '''create table raw_part_d_by_provider (
            "PRSCRBR_NPI" varchar, "Tot_Drug_Cst" double, "Tot_Clms" double
        )'''
    )
    connection.execute(
        '''create table raw_open_payments_general (
            "Covered_Recipient_NPI" varchar,
            "Total_Amount_of_Payment_USDollars" double,
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" varchar
        )'''
    )
    connection.execute(
        '''create table raw_physician_by_provider_and_service (
            "Rndrng_NPI" varchar, "HCPCS_Cd" varchar, "HCPCS_Desc" varchar,
            "Tot_Srvcs" double, "Avg_Mdcr_Pymt_Amt" double, "Tot_Benes" double
        )'''
    )
    connection.execute(
        '''create table raw_part_d_by_provider_and_drug (
            "Prscrbr_NPI" varchar, "Brnd_Name" varchar, "Gnrc_Name" varchar,
            "Tot_Drug_Cst" double, "Tot_Clms" double
        )'''
    )
    connection.execute(
        "create table address_geocode (addr_key varchar, lat double, lng double)"
    )
    connection.executemany(
        "insert into raw_physician_by_provider values (?, ?, ?, ?, ?)",
        [
            ("1111111111", "Cardiology", 100.0, 10.0, 8.0),
            ("2222222222", "Cardiology", 200.0, 20.0, 12.0),
        ],
    )
    connection.executemany(
        "insert into raw_nppes values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111111111",
                "Jamie",
                "Rivera",
                "MD",
                "100 PRIMARY ST",
                "Denver",
                "CO",
                "80202",
                "3035550100",
                None,
            ),
            (
                "2222222222",
                "Alex",
                "Morgan",
                "DO",
                "100 PRIMARY ST",
                "Denver",
                "CO",
                "80202",
                "3035550100",
                None,
            ),
        ],
    )
    connection.executemany(
        'insert into raw_part_d_by_provider values (?, ?, ?)',
        [("1111111111", 30.0, 3.0), ("2222222222", 40.0, 4.0)],
    )
    connection.execute(
        "insert into raw_open_payments_general values ('1111111111', 25, 'Acme')"
    )
    connection.execute(
        "insert into raw_physician_by_provider_and_service values "
        "('1111111111', '99213', 'Office visit', 10, 50, 8)"
    )
    connection.execute(
        "insert into raw_part_d_by_provider_and_drug values "
        "('1111111111', 'Example', 'example', 30, 3)"
    )
    connection.execute(
        "insert into address_geocode values ('100 PRIMARY ST|80202', 39.74, -104.99)"
    )
    return connection


def _client(connection: duckdb.DuckDBPyConnection) -> TestClient:
    app = FastAPI()
    app.include_router(get_practices_router(lambda: connection))
    return TestClient(app)


def test_specialties_lists_the_distinct_cms_provider_types():
    connection = _database()
    connection.execute(
        "insert into raw_physician_by_provider values "
        "('3333333333', 'Family Practice', 50, 5, 3)"
    )

    response = _client(connection).get("/practices/specialties")

    assert response.status_code == 200
    assert response.json() == {"specialties": ["Cardiology", "Family Practice"]}


def test_primary_search_attributes_each_npi_total_once_to_nppes_address():
    connection = _database()
    response = _client(connection).get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zips": "80202",
            "location_basis": "nppes_primary",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["location_basis"] == "nppes_primary"
    assert payload["total"] == 1
    location = payload["results"][0]
    assert location["address"] == "100 PRIMARY ST"
    assert location["zip5"] == "80202"
    assert location["providers_here"] == 2
    assert location["partb_payments"] == 300.0
    assert location["partd_drug_cost"] == 70.0
    assert location["location_basis"] == "nppes_primary"


def test_primary_search_filters_on_nppes_zip_and_rejects_invalid_boundaries():
    connection = _database()
    client = _client(connection)

    outside = client.get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zips": "80203",
            "location_basis": "nppes_primary",
        },
    )
    invalid = client.get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zips": "8020",
            "location_basis": "nppes_primary",
        },
    )

    assert outside.status_code == 200
    assert outside.json()["total"] == 0
    assert invalid.status_code == 422
    assert invalid.json()["detail"] == "ZIP codes must be five digits"


def test_primary_roster_and_profile_use_the_same_nppes_location_membership():
    connection = _database()
    client = _client(connection)
    params = {
        "street": "100 PRIMARY ST",
        "zip": "80202",
        "specialty": "Cardiology",
        "location_basis": "nppes_primary",
    }

    roster = client.get("/practices/providers", params=params)
    profile = client.get("/practices/site-profile", params=params)

    assert roster.status_code == 200
    assert roster.json()["location_basis"] == "nppes_primary"
    assert {provider["npi"] for provider in roster.json()["providers"]} == {
        "1111111111",
        "2222222222",
    }
    assert profile.status_code == 200
    assert profile.json()["location_basis"] == "nppes_primary"
    assert profile.json()["roster_size"] == 2
    assert profile.json()["partb_payments"] == 300.0
