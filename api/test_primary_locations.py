"""NPPES-primary location attribution contracts for Medicare exploration."""

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from practices import get_practices_router


def _database() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        create table core_providers (
            npi varchar,
            provider_type varchar
        )
        """
    )
    connection.execute(
        "create index idx_core_providers_provider_type on core_providers(provider_type)"
    )
    connection.execute(
        """
        create table raw_physician_by_provider (
            "Rndrng_NPI" varchar,
            "Rndrng_Prvdr_Type" varchar,
            "Tot_Mdcr_Pymt_Amt" double,
            "Tot_Srvcs" double,
            "Tot_Benes" double
        )
        """
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
        """create table raw_part_d_by_provider (
            "PRSCRBR_NPI" varchar, "Tot_Drug_Cst" double, "Tot_Clms" double
        )"""
    )
    connection.execute(
        """create table raw_open_payments_general (
            "Covered_Recipient_NPI" varchar,
            "Total_Amount_of_Payment_USDollars" double,
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name" varchar
        )"""
    )
    connection.execute(
        """create table raw_physician_by_provider_and_service (
            "Rndrng_NPI" varchar, "HCPCS_Cd" varchar, "HCPCS_Desc" varchar,
            "Tot_Srvcs" double, "Avg_Mdcr_Pymt_Amt" double, "Tot_Benes" double
        )"""
    )
    connection.execute(
        """create table raw_part_d_by_provider_and_drug (
            "Prscrbr_NPI" varchar, "Brnd_Name" varchar, "Gnrc_Name" varchar,
            "Tot_Drug_Cst" double, "Tot_Clms" double
        )"""
    )
    connection.execute(
        "create table address_geocode (addr_key varchar, lat double, lng double)"
    )
    connection.execute(
        """
        create table raw_dac_national (
            "NPI" varchar,
            "Provider First Name" varchar,
            "Provider Last Name" varchar,
            "Cred\t\t\t\t" varchar,
            pri_spec varchar,
            "Facility Name" varchar,
            org_pac_id varchar,
            num_org_mem integer,
            adr_ln_1 varchar,
            "ZIP Code" varchar,
            "City/Town" varchar,
            "State" varchar,
            "Telephone Number" varchar
        )
        """
    )
    connection.executemany(
        "insert into core_providers values (?, ?)",
        [
            ("1111111111", "Cardiology"),
            ("2222222222", "Cardiology"),
            ("3333333333", "Dermatology"),
            ("4444444444", "Cardiology"),
            ("5555555555", "Dermatology"),
        ],
    )
    connection.executemany(
        "insert into raw_physician_by_provider values (?, ?, ?, ?, ?)",
        [
            ("1111111111", "Cardiology", 100.0, 10.0, 8.0),
            # Repeated source rows must never duplicate one NPI's national totals.
            ("1111111111", "Cardiology", 100.0, 10.0, 8.0),
            ("2222222222", "Cardiology", 200.0, 20.0, 12.0),
            ("3333333333", "Dermatology", 50.0, 5.0, 4.0),
            ("4444444444", "Cardiology", 75.0, 7.0, 5.0),
            ("5555555555", "Dermatology", 25.0, 2.0, 2.0),
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
            (
                "3333333333",
                "Dana",
                "Lee",
                "MD",
                "200 NEAR ST",
                "Denver",
                "CO",
                "80203",
                "3035550200",
                None,
            ),
            (
                "4444444444",
                "Taylor",
                "Park",
                "MD",
                "300 CORNER ST",
                "Denver",
                "CO",
                "80204",
                "3035550300",
                None,
            ),
            (
                "5555555555",
                "Robin",
                "Shah",
                "DO",
                "200 NEAR ST",
                "Denver",
                "CO",
                "80203",
                "3035550200",
                None,
            ),
        ],
    )
    connection.executemany(
        "insert into raw_part_d_by_provider values (?, ?, ?)",
        [
            ("1111111111", 30.0, 3.0),
            ("1111111111", 30.0, 3.0),
            ("2222222222", 40.0, 4.0),
            ("3333333333", 10.0, 1.0),
            ("4444444444", 15.0, 1.0),
            ("5555555555", 5.0, 1.0),
        ],
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
    connection.executemany(
        "insert into address_geocode values (?, ?, ?)",
        [
            ("100 PRIMARY ST|80202", 39.74, -104.99),
            ("200 NEAR ST|80203", 39.76, -104.99),
            # Inside a 5-mile bounding box, but outside the exact 5-mile circle.
            ("300 CORNER ST|80204", 39.81, -104.90),
        ],
    )
    connection.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111111111",
                "Jamie",
                "Rivera",
                "MD",
                "Cardiology",
                "Cardio Group",
                "PAC-A",
                10,
                "100 PRIMARY ST",
                "80202",
                "Denver",
                "CO",
                "3035550100",
            ),
            # A second PAC is context only and must not duplicate Jamie's metrics.
            (
                "1111111111",
                "Jamie",
                "Rivera",
                "MD",
                "Cardiology",
                "Regional Cardio",
                "PAC-B",
                100,
                "999 OTHER ST",
                "80210",
                "Denver",
                "CO",
                "3035550999",
            ),
            (
                "2222222222",
                "Alex",
                "Morgan",
                "DO",
                "Cardiology",
                None,
                None,
                None,
                "100 PRIMARY ST",
                "80202",
                "Denver",
                "CO",
                "3035550100",
            ),
            (
                "3333333333",
                "Dana",
                "Lee",
                "MD",
                "Dermatology",
                None,
                None,
                None,
                "200 NEAR ST",
                "80203",
                "Denver",
                "CO",
                "3035550200",
            ),
            (
                "4444444444",
                "Taylor",
                "Park",
                "MD",
                "Cardiology",
                None,
                None,
                None,
                "300 CORNER ST",
                "80204",
                "Denver",
                "CO",
                "3035550300",
            ),
            (
                "5555555555",
                "Robin",
                "Shah",
                "DO",
                "Dermatology",
                None,
                None,
                None,
                "200 NEAR ST",
                "80203",
                "Denver",
                "CO",
                "3035550200",
            ),
        ],
    )
    return connection


def _client(connection: duckdb.DuckDBPyConnection) -> TestClient:
    app = FastAPI()
    app.include_router(get_practices_router(lambda: connection))
    return TestClient(app)


def test_specialties_uses_normalized_core_provider_catalog_and_caches_it():
    connection = _database()
    connection.execute(
        "insert into raw_physician_by_provider values "
        "('3333333333', 'Family Practice', 50, 5, 3)"
    )
    client = _client(connection)

    response = client.get("/practices/specialties")

    assert response.status_code == 200
    assert response.json() == {"specialties": ["Cardiology", "Dermatology"]}

    connection.execute(
        "insert into core_providers values ('6666666666', 'Family Practice')"
    )
    cached = client.get("/practices/specialties")
    assert cached.json() == response.json()


def test_capabilities_advertise_the_complete_v2_contract():
    response = _client(_database()).get("/practices/capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "contract_version": 2,
        "capabilities": [
            "multi_zip",
            "nppes_primary",
            "exact_radius",
            "multi_specialty",
            "practice_specialties",
            "scoped_metrics",
        ],
    }


def test_capabilities_fails_closed_when_specialty_catalog_is_missing_or_empty():
    missing_connection = duckdb.connect(":memory:")
    missing = _client(missing_connection).get("/practices/capabilities")

    assert missing.status_code == 503
    assert missing.json() == {"detail": "CMS specialty catalog is unavailable"}

    empty_connection = _database()
    empty_connection.execute("delete from core_providers")
    empty = _client(empty_connection).get("/practices/capabilities")

    assert empty.status_code == 503
    assert empty.json() == {"detail": "CMS specialty catalog is unavailable"}


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
    assert payload["contract_version"] == 2
    assert payload["requested_specialties"] == ["Cardiology"]
    assert payload["population_scope"] == "selected_specialties"
    assert payload["metric_scope"] == "national_npi_totals"
    assert payload["location_basis"] == "nppes_primary"
    assert payload["total"] == 1
    assert payload["returned_count"] == 1
    assert payload["truncated"] is False
    location = payload["results"][0]
    assert location["address"] == "100 PRIMARY ST"
    assert location["zip5"] == "80202"
    assert location["providers_here"] == 2
    assert location["roster_npi_count"] == 2
    assert location["partb_payments"] == 300.0
    assert location["partd_drug_cost"] == 70.0
    assert location["location_basis"] == "nppes_primary"
    assert location["site_id"] == "nppes_primary:100 PRIMARY ST|80202"
    assert location["organization_scope"] == "nppes_primary_address"
    assert location["site_classification"] == "organization_context"
    assert location["unaffiliated_provider_count"] == 1
    assert location["solo_provider_name"] is None
    assert [item["org_pac_id"] for item in location["organization_contexts"]] == [
        "PAC-A",
        "PAC-B",
    ]
    assert location["organization_contexts"][0]["primary_address_match_count"] == 1
    assert location["organization_contexts"][1]["primary_address_match_count"] == 0


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
    assert roster.json()["contract_version"] == 2
    assert roster.json()["site_id"] == "nppes_primary:100 PRIMARY ST|80202"
    assert roster.json()["population_scope"] == "selected_specialties"
    assert roster.json()["total"] == roster.json()["roster_npi_count"] == 2
    assert {provider["npi"] for provider in roster.json()["providers"]} == {
        "1111111111",
        "2222222222",
    }
    assert profile.status_code == 200
    assert profile.json()["location_basis"] == "nppes_primary"
    assert profile.json()["roster_size"] == 2
    assert profile.json()["roster_npi_count"] == 2
    assert profile.json()["site_id"] == "nppes_primary:100 PRIMARY ST|80202"
    assert profile.json()["site_classification"] == "organization_context"
    assert profile.json()["partb_payments"] == 300.0


def test_roster_count_remains_the_full_membership_when_rows_are_limited():
    response = _client(_database()).get(
        "/practices/providers",
        params={
            "street": "100 PRIMARY ST",
            "zip": "80202",
            "specialty": "Cardiology",
            "limit": 1,
            "location_basis": "nppes_primary",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["providers"]) == 1
    assert payload["total"] == payload["roster_npi_count"] == 2


def test_multi_specialty_union_keeps_one_nppes_site_and_one_metric_copy_per_npi():
    response = _client(_database()).get(
        "/practices/search",
        params={
            "specialties": "Cardiology,Dermatology,cardiology",
            "zips": "80202,80203",
            "location_basis": "nppes_primary",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["requested_specialties"] == ["Cardiology", "Dermatology"]
    assert payload["total"] == 2
    locations = {item["site_id"]: item for item in payload["results"]}
    primary = locations["nppes_primary:100 PRIMARY ST|80202"]
    shared = locations["nppes_primary:200 NEAR ST|80203"]
    assert primary["providers_here"] == 2
    assert primary["partb_payments"] == 300.0
    assert primary["partd_drug_cost"] == 70.0
    assert shared["providers_here"] == 2
    assert shared["partb_payments"] == 75.0
    assert shared["site_classification"] == "shared_unaffiliated"
    assert shared["unaffiliated_provider_count"] == 2


def test_single_unaffiliated_npi_is_a_named_solo_site():
    response = _client(_database()).get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zip": "80204",
            "location_basis": "nppes_primary",
        },
    )

    assert response.status_code == 200
    location = response.json()["results"][0]
    assert location["site_classification"] == "solo"
    assert location["solo_provider_name"] == "Taylor Park"
    assert location["organization_contexts"] == []


def test_exact_radius_filters_bbox_corner_before_limit_for_both_location_bases():
    client = _client(_database())
    common = {
        "specialty": "Cardiology",
        "lat": 39.74,
        "lng": -104.99,
        "radius_miles": 5,
        "limit": 20,
    }

    primary = client.get(
        "/practices/search",
        params={**common, "location_basis": "nppes_primary"},
    )
    enrollment = client.get(
        "/practices/search",
        params={**common, "location_basis": "cms_enrollment"},
    )

    assert primary.status_code == 200
    assert [item["site_id"] for item in primary.json()["results"]] == [
        "nppes_primary:100 PRIMARY ST|80202"
    ]
    assert enrollment.status_code == 200
    assert {item["address"] for item in enrollment.json()["results"]} == {
        "100 PRIMARY ST"
    }
    assert all(item["distance_miles"] <= 5 for item in enrollment.json()["results"])


def test_proximity_requires_paired_coordinates_and_a_positive_bounded_radius():
    client = _client(_database())
    base = {"specialty": "Cardiology", "location_basis": "nppes_primary"}

    missing_lng = client.get("/practices/search", params={**base, "lat": 39.74})
    zero_radius = client.get(
        "/practices/search",
        params={**base, "lat": 39.74, "lng": -104.99, "radius_miles": 0},
    )
    excessive_radius = client.get(
        "/practices/search",
        params={**base, "lat": 39.74, "lng": -104.99, "radius_miles": 251},
    )

    assert missing_lng.status_code == 422
    assert missing_lng.json()["detail"] == "lat and lng must be provided together"
    assert zero_radius.status_code == 422
    assert excessive_radius.status_code == 422


def test_unfiltered_profile_declares_all_specialties_scope_and_validates_site_id():
    client = _client(_database())
    params = {
        "street": "200 NEAR ST",
        "zip": "80203",
        "location_basis": "nppes_primary",
    }

    profile = client.get("/practices/site-profile", params=params)
    mismatch = client.get(
        "/practices/site-profile",
        params={**params, "site_id": "nppes_primary:WRONG|80203"},
    )

    assert profile.status_code == 200
    assert profile.json()["population_scope"] == "all_specialties"
    assert profile.json()["requested_specialties"] == []
    assert profile.json()["roster_size"] == profile.json()["roster_npi_count"] == 2
    assert mismatch.status_code == 422
    assert mismatch.json()["detail"] == "site_id does not match the requested site"


def test_search_and_roster_expose_honest_truncation_counts():
    client = _client(_database())
    search = client.get(
        "/practices/search",
        params={
            "specialties": "Cardiology,Dermatology",
            "zips": "80202,80203",
            "limit": 1,
            "location_basis": "nppes_primary",
        },
    )
    roster = client.get(
        "/practices/providers",
        params={
            "street": "100 PRIMARY ST",
            "zip": "80202",
            "specialty": "Cardiology",
            "limit": 1,
            "location_basis": "nppes_primary",
        },
    )

    assert search.status_code == 200
    assert search.json()["total"] == 2
    assert search.json()["returned_count"] == 1
    assert search.json()["truncated"] is True
    assert roster.status_code == 200
    assert roster.json()["total"] == roster.json()["roster_npi_count"] == 2
    assert roster.json()["returned_count"] == 1
    assert roster.json()["truncated"] is True


def test_multi_specialty_roster_and_profile_share_the_union_scope():
    client = _client(_database())
    params = {
        "street": "200 NEAR ST",
        "zip": "80203",
        "specialties": "Cardiology,Dermatology",
        "location_basis": "nppes_primary",
    }

    roster = client.get("/practices/providers", params=params)
    profile = client.get("/practices/site-profile", params=params)

    assert roster.status_code == 200
    assert roster.json()["requested_specialties"] == ["Cardiology", "Dermatology"]
    assert {item["npi"] for item in roster.json()["providers"]} == {
        "3333333333",
        "5555555555",
    }
    assert profile.status_code == 200
    assert profile.json()["requested_specialties"] == ["Cardiology", "Dermatology"]
    assert profile.json()["roster_npi_count"] == 2
    assert profile.json()["partb_payments"] == 75.0


def test_malformed_source_location_is_excluded_instead_of_poisoning_the_response():
    connection = _database()
    connection.execute(
        "insert into raw_physician_by_provider values "
        "('6666666666', 'Cardiology', 90, 9, 6)"
    )
    connection.execute(
        "insert into raw_nppes values "
        "('6666666666', 'Bad', 'Location', 'MD', '600 BROKEN ST', '', 'CO', "
        "'80209', '3035550600', null)"
    )
    connection.execute(
        "insert into raw_dac_national values "
        "('7777777777', 'Broken', 'Enrollment', 'MD', 'Cardiology', null, null, "
        "null, '700 BROKEN ST', '80209', '', 'CO', '3035550700')"
    )

    client = _client(connection)
    response = client.get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zip": "80209",
            "location_basis": "nppes_primary",
        },
    )

    assert response.status_code == 200
    assert response.json()["total"] == 0
    enrollment = client.get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zip": "80209",
            "location_basis": "cms_enrollment",
        },
    )
    assert enrollment.status_code == 200
    assert enrollment.json()["total"] == 0


def test_wildcard_specialties_are_rejected_and_stale_profiles_are_not_found():
    client = _client(_database())

    wildcard = client.get(
        "/practices/search",
        params={"specialty": "%", "zip": "80202", "location_basis": "nppes_primary"},
    )
    stale = client.get(
        "/practices/site-profile",
        params={
            "street": "404 MISSING ST",
            "zip": "80202",
            "location_basis": "nppes_primary",
        },
    )

    assert wildcard.status_code == 422
    assert stale.status_code == 404
    assert stale.json()["detail"] == "Practice site not found"


def test_search_requires_a_bounded_geography():
    response = _client(_database()).get(
        "/practices/search",
        params={"specialty": "Cardiology", "location_basis": "nppes_primary"},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Choose a city, state, ZIP boundary, or radius origin"
    )


def test_detail_routes_require_an_exact_five_digit_zip():
    client = _client(_database())
    common = {
        "street": "100 PRIMARY ST",
        "location_basis": "nppes_primary",
    }

    for path in ("/practices/providers", "/practices/site-profile"):
        zip_plus_four = client.get(path, params={**common, "zip": "80202-1234"})
        trailing_text = client.get(path, params={**common, "zip": "80202junk"})

        assert zip_plus_four.status_code == 422
        assert trailing_text.status_code == 422
        assert zip_plus_four.json()["detail"] == "ZIP codes must be five digits"


def test_enrollment_drill_down_uses_the_same_normalized_pac_identity_as_search():
    connection = _database()
    connection.execute(
        "update raw_dac_national set org_pac_id = ' PAC-A ' where \"NPI\" = '1111111111' "
        "and trim(org_pac_id) = 'PAC-A'"
    )
    connection.execute(
        "update raw_dac_national set org_pac_id = '   ' where \"NPI\" = '2222222222'"
    )
    client = _client(connection)
    search = client.get(
        "/practices/search",
        params={
            "specialty": "Cardiology",
            "zip": "80202",
            "location_basis": "cms_enrollment",
        },
    )

    assert search.status_code == 200
    sites = {site["org_pac_id"] or "SOLO": site for site in search.json()["results"]}
    assert set(sites) == {"PAC-A", "SOLO"}

    for expected_npi, site in (
        ("1111111111", sites["PAC-A"]),
        ("2222222222", sites["SOLO"]),
    ):
        params = {
            "street": site["address"],
            "zip": site["zip5"],
            "site_id": site["site_id"],
            "specialty": "Cardiology",
            "location_basis": "cms_enrollment",
        }
        if site["org_pac_id"]:
            params["org_pac_id"] = site["org_pac_id"]

        roster = client.get("/practices/providers", params=params)
        profile = client.get("/practices/site-profile", params=params)

        assert roster.status_code == 200
        assert {provider["npi"] for provider in roster.json()["providers"]} == {
            expected_npi
        }
        assert profile.status_code == 200
        assert profile.json()["roster_npi_count"] == 1


def test_router_factory_keeps_each_app_bound_to_its_own_database():
    first_connection = _database()
    second_connection = _database()
    second_connection.execute("delete from core_providers")
    second_connection.execute(
        "insert into core_providers values ('9999999999', 'Neurology')"
    )

    first = _client(first_connection).get("/practices/specialties")
    second = _client(second_connection).get("/practices/specialties")

    assert first.json()["specialties"] == ["Cardiology", "Dermatology"]
    assert second.json()["specialties"] == ["Neurology"]
