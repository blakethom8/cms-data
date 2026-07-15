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
        '''
        create table raw_open_payments_general (
          Covered_Recipient_NPI varchar,
          Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name varchar,
          Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 varchar,
          Nature_of_Payment_or_Transfer_of_Value varchar,
          Total_Amount_of_Payment_USDollars double
        )
        '''
    )
    connection.executemany(
        "insert into raw_open_payments_general values (?, ?, ?, ?, ?)",
        [
            ("1111111111", "Stryker Corporation", "MAKO", "Food and Beverage", 70),
            ("1111111111", "Acme Medical", "Persona", "Consulting Fee", 30_000),
            ("2222222222", "Stryker Corporation", "MAKO", "Consulting Fee", 6_000),
        ],
    )
    connection.execute("create table address_geocode (addr_key varchar, lat double, lng double)")
    connection.executemany(
        "insert into address_geocode values (?, ?, ?)",
        [("1 MAIN ST|90401", 34.01, -118.49), ("2 MAIN ST|90401", 34.02, -118.48)],
    )

    app = FastAPI()
    app.include_router(get_industry_router(lambda: connection))
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
