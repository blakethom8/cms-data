import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from industry import get_industry_router


def _build_client() -> TestClient:
    connection = duckdb.connect(":memory:")
    connection.execute(
        '''
        create table raw_dac_national (
          "NPI" varchar,
          "Provider First Name" varchar,
          "Provider Last Name" varchar,
          "Cred\t\t\t\t" varchar,
          pri_spec varchar,
          "Facility Name" varchar,
          "City/Town" varchar,
          "State" varchar,
          adr_ln_1 varchar,
          "ZIP Code" varchar
        )
        '''
    )
    connection.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111111111",
                "Alice",
                "Overall",
                "MD",
                "Orthopedic Surgery",
                "Westside Orthopedics",
                "Santa Monica",
                "CA",
                "1 Main St",
                "90401",
            ),
            (
                "2222222222",
                "Bob",
                "Matched",
                "MD",
                "Orthopedic Surgery",
                "Westside Orthopedics",
                "Santa Monica",
                "CA",
                "2 Main St",
                "90401",
            ),
        ],
    )
    connection.execute(
        "create table core_providers (npi varchar primary key, provider_type varchar)"
    )
    connection.executemany(
        "insert into core_providers values (?, ?)",
        [
            ("1111111111", "Orthopedics"),
            ("2222222222", "Orthopedics"),
        ],
    )
    connection.execute(
        '''
        create table raw_open_payments_general (
          Covered_Recipient_NPI varchar,
          Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name varchar,
          Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 varchar,
          Nature_of_Payment_or_Transfer_of_Value varchar,
          Total_Amount_of_Payment_USDollars double,
          Covered_Recipient_First_Name varchar,
          Covered_Recipient_Last_Name varchar,
          Recipient_City varchar,
          Recipient_State varchar
        )
        '''
    )
    connection.executemany(
        "insert into raw_open_payments_general values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111111111", "Stryker Corporation", "MAKO", "Food and Beverage", 70,
             "Alice", "Overall", "Santa Monica", "CA"),
            ("1111111111", "Acme Medical", "Persona", "Consulting Fee", 30_000,
             "Alice", "Overall", "Santa Monica", "CA"),
            ("2222222222", "Stryker Corporation", "MAKO", "Consulting Fee", 6_000,
             "Bob", "Matched", "Santa Monica", "CA"),
        ],
    )
    connection.execute("create table address_geocode (addr_key varchar, lat double, lng double)")
    connection.executemany(
        "insert into address_geocode values (?, ?, ?)",
        [("1 MAIN ST|90401", 34.01, -118.49), ("2 MAIN ST|90401", 34.02, -118.48)],
    )

    app = FastAPI()
    app.include_router(get_industry_router(lambda: connection))
    app.state.connection = connection
    return TestClient(app)


client = _build_client()


def test_selected_relationship_is_the_default_threshold_scope():
    response = client.get(
        "/industry/search",
        params={"manufacturer": "Stryker Corporation", "min_tier": 4},
    )

    assert response.status_code == 200
    assert response.json()["total"] == 0


def test_all_activity_scope_can_qualify_a_small_matched_relationship():
    response = client.get(
        "/industry/search",
        params={
            "manufacturer": "Stryker Corporation",
            "min_tier": 4,
            "threshold_scope": "all",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["results"][0]["npi"] == "1111111111"
    assert payload["results"][0]["matched_total_usd"] == 70
    assert payload["results"][0]["total_usd"] == 30_070


def test_relationship_detail_honors_manufacturer_and_product_filters():
    response = client.get(
        "/industry/1111111111/detail",
        params={"manufacturer": "Stryker Corporation", "product": "MAKO"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["payment_count"] == 1
    assert payload["total_usd"] == 70
    assert payload["by_nature"] == [
        {"label": "Food and Beverage", "payment_count": 1, "total_usd": 70}
    ]
    assert payload["manufacturers"] == [
        {
            "manufacturer": "Stryker Corporation",
            "payment_count": 1,
            "total_usd": 70,
            "products": ["MAKO"],
        }
    ]


def test_options_use_the_same_threshold_scope_as_search():
    matched_response = client.get(
        "/industry/options",
        params={"field": "manufacturer", "q": "Stryker", "min_tier": 4},
    )
    all_response = client.get(
        "/industry/options",
        params={
            "field": "manufacturer",
            "q": "Stryker",
            "min_tier": 4,
            "threshold_scope": "all",
        },
    )

    assert matched_response.status_code == 200
    assert matched_response.json()["total_values"] == 0
    assert all_response.status_code == 200
    assert all_response.json()["options"] == [
        {
            "value": "Stryker Corporation",
            "physician_count": 1,
            "payment_count": 1,
            "total_usd": 70,
        }
    ]


def test_search_uses_stable_representatives_and_tie_breakers():
    connection = client.app.state.connection
    connection.execute("begin transaction")
    try:
        connection.execute(
            "insert into raw_dac_national values "
            "('2222222222', 'Aaron', 'Alternate', 'DO', 'A Specialty', "
            "'A Practice', 'A City', 'AZ', '0 Main St', '85001')"
        )
        connection.execute(
            "insert into raw_open_payments_general values "
            "('2222222222', 'Acme Medical', 'ALPHA', 'Consulting Fee', 6000, "
            "'Bob', 'Matched', 'Santa Monica', 'CA')"
        )

        response = client.get(
            "/industry/search", params={"min_tier": 1, "sort": "payments"}
        )

        assert response.status_code == 200
        row = next(
            item for item in response.json()["results"] if item["npi"] == "2222222222"
        )
        assert row["name"] == "Aaron Alternate"
        assert row["practice_name"] == "A Practice"
        assert row["top_manufacturer"] == "Acme Medical"
        assert row["top_product"] == "ALPHA"
    finally:
        connection.execute("rollback")


def test_search_uses_catalog_specialty_not_raw_dac_specialty():
    response = client.get("/industry/search", params={"specialty": "orthopedics"})

    assert response.status_code == 200
    payload = response.json()
    assert {row["npi"] for row in payload["results"]} == {"1111111111", "2222222222"}
    assert {row["specialty"] for row in payload["results"]} == {"Orthopedics"}

    raw_dac_label = client.get(
        "/industry/search", params={"specialty": "Orthopedic Surgery"}
    )
    assert raw_dac_label.status_code == 200
    assert raw_dac_label.json()["total"] == 0


def test_unmatched_open_payments_recipient_is_retained_only_without_specialty_filter():
    connection = client.app.state.connection
    connection.execute("begin transaction")
    try:
        connection.execute(
            "insert into raw_open_payments_general values "
            "('3333333333', 'Uncataloged Corp', 'Unknown Device', 'Food and Beverage', 10, "
            "'Uma', 'Unmatched', 'Los Angeles', 'CA')"
        )

        unfiltered = client.get("/industry/search")
        selected_specialty = client.get(
            "/industry/search", params={"specialty": "ORTHOPEDICS"}
        )

        assert unfiltered.status_code == 200
        unmatched = next(
            row for row in unfiltered.json()["results"] if row["npi"] == "3333333333"
        )
        assert unmatched["name"] == "Uma Unmatched"
        assert unmatched["specialty"] is None
        assert {row["npi"] for row in selected_specialty.json()["results"]} == {
            "1111111111",
            "2222222222",
        }
    finally:
        connection.execute("rollback")


def test_specialty_options_and_live_open_payments_facets_use_their_respective_keys():
    specialty = client.get("/industry/options", params={"field": "specialty"})
    manufacturers = client.get(
        "/industry/options",
        params={"field": "manufacturer", "specialty": "ORTHOPEDICS", "sort": "alpha"},
    )
    products = client.get(
        "/industry/options",
        params={"field": "product", "manufacturer": "Stryker Corporation"},
    )

    assert specialty.status_code == 200
    assert specialty.json()["options"] == [
        {
            "value": "Orthopedics",
            "physician_count": 2,
            "payment_count": 3,
            "total_usd": 36070,
        }
    ]
    assert [option["value"] for option in manufacturers.json()["options"]] == [
        "Acme Medical",
        "Stryker Corporation",
    ]
    assert products.json()["options"] == [
        {"value": "MAKO", "physician_count": 2, "payment_count": 2, "total_usd": 6070}
    ]
