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
        create table utilization_procedure_taxonomy (
          hcpcs_code varchar, rbcs_id varchar, category_id varchar, category_name varchar,
          subcategory_id varchar, subcategory_name varchar, family_id varchar,
          family_name varchar, major_indicator varchar, hcpcs_add_date varchar,
          hcpcs_end_date varchar, rbcs_release_year integer
        );
        create table utilization_drug_classes (
          source varchar, class_type varchar, class_id varchar, class_name varchar,
          parent_class_id varchar, parent_class_name varchar, hierarchy_level integer
        );
        create table utilization_drug_class_members (
          source varchar, class_type varchar, class_id varchar, generic_name varchar,
          rxcui varchar, concept_name varchar, concept_tty varchar, match_score integer,
          match_method varchar, source_version varchar
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
        "insert into utilization_procedure_taxonomy values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "33249", "MC010N", "M", "Procedures", "MC", "Cardiovascular",
                "MC-010", "Device Implantation", "N", "01/01/2000", "12/31/9999", 2025,
            ),
            (
                "J9999", "PM015N", "M", "Procedures", "PM", "Musculoskeletal",
                "PM-015", "Joint Injection", "N", "01/01/2000", "12/31/9999", 2025,
            ),
        ],
    )
    connection.executemany(
        "insert into utilization_drug_classes values (?, ?, ?, ?, ?, ?, ?)",
        [
            ("ATC", "ATC", "B", "Blood and blood forming organs", None, None, 1),
            ("ATC", "ATC", "B01", "Antithrombotic agents", "B", "Blood and blood forming organs", 2),
            ("ATC", "ATC", "B01AF", "Direct factor Xa inhibitors", "B01", "Antithrombotic agents", 3),
            ("FDASPL", "EPC", "N1", "Factor Xa Inhibitor", None, None, 1),
        ],
    )
    connection.executemany(
        "insert into utilization_drug_class_members values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("ATC", "ATC", "B01AF", "Apixaban", "1364430", "apixaban", "IN", 100, "exact_normalized", "v1"),
            ("FDASPL", "EPC", "N1", "Apixaban", "1364430", "apixaban", "IN", 100, "exact_normalized", "v1"),
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


def test_procedure_catalog_pages_in_sequential_order_and_keeps_total(monkeypatch):
    monkeypatch.delenv("HCPCS_DESCRIPTIONS_ENABLED", raising=False)
    first = client.get(
        "/utilization/procedures/catalog", params={"limit": 1, "offset": 0}
    )
    second = client.get(
        "/utilization/procedures/catalog", params={"limit": 1, "offset": 1}
    )
    beyond = client.get(
        "/utilization/procedures/catalog", params={"limit": 1, "offset": 5}
    )

    assert first.status_code == second.status_code == beyond.status_code == 200
    assert first.json() == {
        "contract_version": 1,
        "query": None,
        "prefix": None,
        "data_year": 2024,
        "ordering": "hcpcs_code",
        "descriptions_enabled": False,
        "total": 2,
        "offset": 0,
        "limit": 1,
        "returned_count": 1,
        "has_more": True,
        "results": [
            {
                "value": "33249",
                "description": None,
                "is_drug_code": False,
                "physician_count": 2,
                "total_services": 180.0,
                "total_payments": 27000.0,
            }
        ],
    }
    assert second.json()["results"][0]["value"] == "J9999"
    assert second.json()["has_more"] is False
    assert beyond.json()["total"] == 2
    assert beyond.json()["results"] == []


def test_procedure_catalog_supports_strict_prefix_and_gated_description_search(
    monkeypatch,
):
    monkeypatch.setenv("HCPCS_DESCRIPTIONS_ENABLED", "true")
    prefix = client.get(
        "/utilization/procedures/catalog", params={"prefix": "j"}
    )
    description = client.get(
        "/utilization/procedures/catalog", params={"q": "ICD"}
    )

    assert prefix.status_code == description.status_code == 200
    assert prefix.json()["prefix"] == "J"
    assert [row["value"] for row in prefix.json()["results"]] == ["J9999"]
    assert description.json()["results"][0]["description"] == "Insert ICD system"


def test_v2_procedure_catalog_uses_snapshot_keyset_cursors_and_anchor(monkeypatch):
    monkeypatch.delenv("HCPCS_DESCRIPTIONS_ENABLED", raising=False)
    first = client.get("/utilization/v2/procedures/catalog", params={"limit": 1})
    payload = first.json()
    second = client.get(
        "/utilization/v2/procedures/catalog",
        params={"after": payload["window"]["next_cursor"], "limit": 1},
    )
    previous = client.get(
        "/utilization/v2/procedures/catalog",
        params={"before": second.json()["window"]["previous_cursor"], "limit": 1},
    )
    anchored = client.get(
        "/utilization/v2/procedures/catalog",
        params={"anchor": "hcpcs:J9999", "limit": 1},
    )

    assert first.status_code == second.status_code == previous.status_code == anchored.status_code == 200
    assert payload["contract_version"] == 2
    assert payload["snapshot"] == {
        "id": "utilization-data-year-2024",
        "data_year": 2024,
        "ordering": "hcpcs_code_v1",
    }
    assert payload["count"] == {"value": 2, "relation": "exact"}
    assert payload["results"][0]["row_key"] == "hcpcs:33249"
    assert second.json()["results"][0]["row_key"] == "hcpcs:J9999"
    assert previous.json()["results"][0]["row_key"] == "hcpcs:33249"
    assert anchored.json()["window"]["anchor_resolution"] == "exact"
    assert anchored.json()["window"]["start_index"] == 1


def test_v2_catalog_rejects_mismatched_cursor_and_resolves_bounded_basket(monkeypatch):
    monkeypatch.delenv("HCPCS_DESCRIPTIONS_ENABLED", raising=False)
    page = client.get("/utilization/v2/procedures/catalog", params={"limit": 1}).json()
    invalid = client.get(
        "/utilization/v2/procedures/catalog",
        params={"after": page["window"]["next_cursor"], "prefix": "J"},
    )
    resolved = client.post(
        "/utilization/v2/catalog/resolve",
        json={"keys": ["hcpcs:33249", "brand:Eliquis", "generic:missing"]},
    )
    oversized = client.post(
        "/utilization/v2/catalog/resolve",
        json={"keys": ["hcpcs:33249"] * 51},
    )

    assert invalid.status_code == 422
    assert invalid.json() == {"detail": {"reason": "catalog.invalid_cursor"}}
    assert resolved.status_code == 200
    assert [row["available"] for row in resolved.json()["results"]] == [True, True, False]
    assert resolved.json()["results"][0]["description"] is None
    assert oversized.status_code == 422
    assert oversized.json() == {"detail": {"reason": "catalog.invalid_request"}}


def test_v2_drug_catalog_uses_collision_safe_rows_and_nearest_anchor():
    page = client.get("/utilization/v2/drugs/catalog", params={"limit": 1})
    row = page.json()["results"][0]
    next_page = client.get(
        "/utilization/v2/drugs/catalog",
        params={"after": page.json()["window"]["next_cursor"], "limit": 1},
    )
    anchor = client.get(
        "/utilization/v2/drugs/catalog",
        params={"anchor": row["row_key"], "limit": 1},
    )

    assert page.status_code == next_page.status_code == anchor.status_code == 200
    assert row["row_key"].startswith("drug:")
    assert next_page.json()["results"][0]["brand"] == "Jardiance"
    assert anchor.json()["window"]["anchor_resolution"] == "exact"


def test_v2_hierarchy_member_routes_reuse_bounded_catalog_paging():
    family = client.get(
        "/utilization/v2/procedures/families/PM-015/members", params={"limit": 1}
    )
    drug_class = client.get(
        "/utilization/v2/drugs/classes/ATC/B/members", params={"limit": 1}
    )

    assert family.status_code == drug_class.status_code == 200
    assert family.json()["scope"]["family_id"] == "PM-015"
    assert family.json()["results"][0]["row_key"] == "hcpcs:J9999"
    assert drug_class.json()["scope"] == {
        "query": None,
        "prefix": None,
        "code_from": None,
        "code_to": None,
        "family_id": None,
        "category_id": None,
        "subcategory_id": None,
        "class_source": "ATC",
        "class_id": "B",
        "parent_class_id": None,
    }
    assert drug_class.json()["results"][0]["generic"] == "Apixaban"


def test_v2_hierarchy_discovery_uses_independent_signed_keysets():
    families = client.get("/utilization/v2/procedures/taxonomy", params={"limit": 1})
    family_next = client.get(
        "/utilization/v2/procedures/taxonomy",
        params={"after": families.json()["window"]["next_cursor"], "limit": 1},
    )
    classes = client.get("/utilization/v2/drugs/classes", params={"limit": 1})
    class_next = client.get(
        "/utilization/v2/drugs/classes",
        params={"after": classes.json()["window"]["next_cursor"], "limit": 1},
    )
    mismatched = client.get(
        "/utilization/v2/drugs/classes",
        params={"after": families.json()["window"]["next_cursor"]},
    )

    assert families.status_code == family_next.status_code == classes.status_code == class_next.status_code == 200
    assert families.json()["snapshot"]["ordering"] == "procedure_family_v1"
    assert families.json()["count"] == {"value": 2, "relation": "exact"}
    assert families.json()["results"][0]["family_id"] != family_next.json()["results"][0]["family_id"]
    assert classes.json()["snapshot"]["ordering"] == "drug_class_v1"
    assert classes.json()["results"][0]["class_id"] != class_next.json()["results"][0]["class_id"]
    assert mismatched.status_code == 422
    assert mismatched.json() == {"detail": {"reason": "catalog.invalid_cursor"}}


def test_v2_member_routes_distinguish_missing_references_from_empty_filters():
    missing_family = client.get("/utilization/v2/procedures/families/ZZ-999/members")
    missing_class = client.get("/utilization/v2/drugs/classes/ATC/NOPE/members")
    empty_filter = client.get(
        "/utilization/v2/procedures/families/PM-015/members", params={"q": "absent"}
    )
    brand_match = client.get(
        "/utilization/v2/drugs/classes/ATC/B/members", params={"q": "eliquis"}
    )
    missing_anchor = client.get(
        "/utilization/v2/drugs/classes/ATC/B/members",
        params={"anchor": "generic:missing"},
    )

    assert missing_family.status_code == missing_class.status_code == 404
    assert missing_family.json() == missing_class.json() == {
        "detail": {"reason": "catalog.reference_unavailable"}
    }
    assert empty_filter.status_code == 200
    assert empty_filter.json()["results"] == []
    assert brand_match.status_code == 200
    assert brand_match.json()["results"][0]["generic"] == "Apixaban"
    assert missing_anchor.status_code == 200
    assert missing_anchor.json()["window"]["anchor_resolution"] == "start"
    assert missing_anchor.json()["window"]["anchor_key"] in {
        row["row_key"] for row in missing_anchor.json()["results"]
    }


def test_procedure_taxonomy_finds_family_by_clinical_label(monkeypatch):
    monkeypatch.setenv("HCPCS_DESCRIPTIONS_ENABLED", "true")
    browse = client.get("/utilization/procedures/taxonomy", params={"q": "joint"})
    detail = client.get("/utilization/procedures/families/PM-015")

    assert browse.status_code == 200
    assert browse.json()["results"][0]["family_name"] == "Joint Injection"
    assert browse.json()["results"][0]["available_code_count"] == 1
    assert detail.status_code == 200
    assert detail.json()["members"] == [
        {
            "value": "J9999",
            "description": "Example injection",
            "is_drug_code": True,
            "physician_count": 1,
            "total_services": 20.0,
            "total_payments": 500.0,
        }
    ]


def test_drug_class_browse_rolls_leaf_members_into_atc_parent():
    browse = client.get("/utilization/drugs/classes", params={"q": "apixaban"})
    detail = client.get("/utilization/drugs/classes/ATC/B01")

    assert browse.status_code == 200
    payload = browse.json()
    assert payload["source"] == "ATC"
    assert payload["attribution"].startswith("This product uses publicly available data")
    assert [row["class_id"] for row in payload["results"]] == ["B01AF", "B01", "B"]
    assert detail.status_code == 200
    assert detail.json()["members"][0]["generic"] == "Apixaban"
    assert detail.json()["members"][0]["brands"] == ["Eliquis"]


def test_drug_class_detail_resolves_broad_atc_root_directly():
    detail = client.get("/utilization/drugs/classes/ATC/B")

    assert detail.status_code == 200
    assert detail.json()["drug_class"]["class_id"] == "B"
    assert detail.json()["members"][0]["generic"] == "Apixaban"


def test_drug_class_source_separates_atc_from_fda_epc():
    response = client.get(
        "/utilization/drugs/classes", params={"source": "FDASPL", "q": "factor xa"}
    )

    assert response.status_code == 200
    assert response.json()["results"][0]["class_type"] == "EPC"
    assert response.json()["results"][0]["class_id"] == "N1"


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


def test_drug_catalog_supports_stable_paging_search_and_prefix():
    first = client.get("/utilization/drugs/catalog", params={"limit": 1})
    second = client.get(
        "/utilization/drugs/catalog", params={"limit": 1, "offset": 1}
    )
    prefix = client.get("/utilization/drugs/catalog", params={"prefix": "eli"})
    generic = client.get("/utilization/drugs/catalog", params={"q": "empagliflozin"})

    assert first.status_code == second.status_code == 200
    assert first.json()["ordering"] == "brand_generic"
    assert first.json()["total"] == 2
    assert first.json()["has_more"] is True
    assert first.json()["results"][0]["brand"] == "Eliquis"
    assert second.json()["results"][0]["brand"] == "Jardiance"
    assert second.json()["has_more"] is False
    assert prefix.json()["prefix"] == "eli"
    assert [row["brand"] for row in prefix.json()["results"]] == ["Eliquis"]
    assert [row["generic"] for row in generic.json()["results"]] == [
        "Empagliflozin"
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
