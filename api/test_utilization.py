"""Contract tests for code-first utilization dictionaries and ranked NPI search."""

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from utilization import get_utilization_router


def _client() -> TestClient:
    connection = duckdb.connect(":memory:")
    connection.execute("""
        create table utilization_procedure_dictionary (
          hcpcs_code varchar, hcpcs_description varchar, hcpcs_drug_ind varchar,
          physician_count integer, total_services double, total_payments double,
          data_year integer
        );
        create table utilization_drug_dictionary (
          brand_name varchar, generic_name varchar, physician_count integer,
          total_claims bigint, total_drug_cost double, data_year integer
        );
        create table provider_service_detail (
          npi varchar, hcpcs_code varchar, hcpcs_description varchar,
          hcpcs_drug_ind varchar, place_of_service varchar,
          tot_beneficiaries integer, tot_services double,
          avg_medicare_payment double, data_year integer
        );
        create table provider_drug_detail (
          npi varchar, brand_name varchar, generic_name varchar,
          tot_claims integer, tot_drug_cost double, data_year integer
        );
        create table serving_practice_nppes_provider_sites (
          npi varchar, address varchar, city varchar, state varchar, zip5 varchar,
          first_name varchar, last_name varchar, credentials varchar,
          specialties varchar[], latitude double, longitude double,
          partb_payments double, partb_services double,
          partb_beneficiaries double, partd_drug_cost double
        );
        create table utilization_metrics (
          npi varchar, metric_year integer, rx_total_claims integer
        );
        """)
    connection.executemany(
        "insert into utilization_procedure_dictionary values (?, ?, ?, ?, ?, ?, ?)",
        [
            ("33249", "Insert ICD system", "N", 2, 180, 27000, 2024),
            ("J9999", "Example injection", "Y", 1, 20, 500, 2024),
        ],
    )
    connection.executemany(
        "insert into utilization_drug_dictionary values (?, ?, ?, ?, ?, ?)",
        [
            ("Eliquis", "Apixaban", 2, 140, 14000, 2024),
            ("Jardiance", "Empagliflozin", 1, 30, 3000, 2024),
        ],
    )
    connection.executemany(
        "insert into provider_service_detail values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111111111", "33249", "Insert ICD system", "N", "F", 20, 100, 150, 2024),
            ("2222222222", "33249", "Insert ICD system", "N", "F", 10, 80, 150, 2024),
            ("3333333333", "33249", "Insert ICD system", "N", "F", 5, 40, 150, 2024),
            ("1111111111", "J9999", "Example injection", "Y", "O", 4, 20, 25, 2024),
        ],
    )
    connection.executemany(
        "insert into provider_drug_detail values (?, ?, ?, ?, ?, ?)",
        [
            ("1111111111", "Eliquis", "Apixaban", 100, 10000, 2024),
            ("2222222222", "Eliquis", "Apixaban", 40, 4000, 2024),
            ("3333333333", "Jardiance", "Empagliflozin", 30, 3000, 2024),
        ],
    )
    connection.executemany(
        "insert into serving_practice_nppes_provider_sites values "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111111111",
                "1 Main St",
                "Denver",
                "CO",
                "80202",
                "Alice",
                "Able",
                "MD",
                ["Cardiology"],
                39.74,
                -104.99,
                30000,
                200,
                50,
                20000,
            ),
            (
                "2222222222",
                "2 Main St",
                "Denver",
                "CO",
                "80203",
                "Bob",
                "Baker",
                "DO",
                ["Cardiology"],
                39.75,
                -104.98,
                20000,
                160,
                30,
                8000,
            ),
            (
                "3333333333",
                "3 Far Rd",
                "Boulder",
                "CO",
                "80301",
                "Cara",
                "Cole",
                "MD",
                ["Endocrinology"],
                40.02,
                -105.27,
                10000,
                100,
                20,
                5000,
            ),
        ],
    )
    connection.executemany(
        "insert into utilization_metrics values (?, ?, ?)",
        [
            ("1111111111", 2024, 200),
            ("2222222222", 2024, 100),
            ("3333333333", 2024, 60),
        ],
    )
    app = FastAPI()
    app.include_router(get_utilization_router(lambda: connection))
    app.state.connection = connection
    return TestClient(app)


client = _client()


def test_procedure_options_redact_license_sensitive_descriptions(monkeypatch):
    monkeypatch.delenv("HCPCS_DESCRIPTIONS_ENABLED", raising=False)
    response = client.get("/utilization/procedures/options", params={"q": "332"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["descriptions_enabled"] is False
    assert payload["results"][0]["value"] == "33249"
    assert payload["results"][0]["description"] is None


def test_procedure_description_search_requires_explicit_release_gate(monkeypatch):
    monkeypatch.setenv("HCPCS_DESCRIPTIONS_ENABLED", "true")
    response = client.get("/utilization/procedures/options", params={"q": "ICD"})

    assert response.status_code == 200
    assert response.json()["results"][0]["description"] == "Insert ICD system"


def test_procedure_search_ranks_selected_volume_and_preserves_scope(monkeypatch):
    monkeypatch.delenv("HCPCS_DESCRIPTIONS_ENABLED", raising=False)
    response = client.get(
        "/utilization/procedures/search",
        params={"hcpcs": "33249", "city": "Denver", "state": "CO", "min_services": 90},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["metric_scope"] == "national_npi_totals"
    assert payload["total"] == payload["returned_count"] == 1
    row = payload["results"][0]
    assert row["npi"] == "1111111111"
    assert row["selected_services"] == 100
    assert row["selected_payments"] == 15000
    assert row["selected_service_share"] == 0.5
    assert row["matched_codes"][0]["description"] is None


def test_procedure_search_supports_radius_and_specialty():
    response = client.get(
        "/utilization/procedures/search",
        params={
            "hcpcs": "33249",
            "lat": 39.74,
            "lng": -104.99,
            "radius_miles": 5,
            "specialty": "Cardiology",
        },
    )

    assert response.status_code == 200
    assert [row["npi"] for row in response.json()["results"]] == [
        "1111111111",
        "2222222222",
    ]


def test_drug_options_and_ranked_search_return_selected_and_denominator_metrics():
    options = client.get("/utilization/drugs/options", params={"q": "apix"})
    search = client.get(
        "/utilization/drugs/search",
        params={"brands": "Eliquis", "city": "Denver", "state": "CO"},
    )

    assert options.status_code == 200
    assert options.json()["results"][0]["brand"] == "Eliquis"
    assert search.status_code == 200
    payload = search.json()
    assert [row["npi"] for row in payload["results"]] == ["1111111111", "2222222222"]
    assert payload["results"][0]["selected_claim_share"] == 0.5
    assert payload["results"][0]["selected_cost_share"] == 0.5
    assert payload["results"][0]["matched_drugs"] == [
        {"brand": "Eliquis", "generic": "Apixaban", "claims": 100, "drug_cost": 10000}
    ]


def test_search_rejects_missing_basket_geo_and_oversized_basket():
    missing_basket = client.get(
        "/utilization/procedures/search", params={"city": "Denver", "state": "CO"}
    )
    missing_geo = client.get("/utilization/drugs/search", params={"brands": "Eliquis"})
    oversized = client.get(
        "/utilization/procedures/search",
        params=[*(("hcpcs", str(index)) for index in range(51)), ("zip", "80202")],
    )

    assert missing_basket.status_code == 422
    assert missing_geo.status_code == 422
    assert oversized.status_code == 422
