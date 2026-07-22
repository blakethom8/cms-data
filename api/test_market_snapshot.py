"""Market-snapshot contract: linked org/site/provider blocks from one scan."""

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from market_snapshot import get_market_snapshot_router
from practices import site_identifier


def _database() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
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
    connection.execute(
        """create table raw_physician_by_provider (
            "Rndrng_NPI" varchar,
            "Rndrng_Prvdr_Type" varchar,
            "Tot_Mdcr_Pymt_Amt" double,
            "Tot_Srvcs" double,
            "Tot_Benes" double
        )"""
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
        "create table address_geocode (addr_key varchar, lat double, lng double)"
    )

    dac_rows = [
        # NPI 1: org 111 at two Denver doors (multi-door within one org). Its
        # NPPES primary address matches door D2.
        ("1", "AVA", "ARNOLD", "MD", "CARDIOLOGY", "HEART GROUP", "111", 40,
         "1 MAIN ST", "802010000", "DENVER", "CO", "3030000001"),
        ("1", "AVA", "ARNOLD", "MD", "CARDIOLOGY", "HEART GROUP", "111", 40,
         "2 OAK AVE", "80202", "DENVER", "CO", "3030000002"),
        # Duplicate source row must not double anything.
        ("1", "AVA", "ARNOLD", "MD", "CARDIOLOGY", "HEART GROUP", "111", 40,
         "1 MAIN ST", "80201", "DENVER", "CO", "3030000001"),
        # NPI 1 also enrolls out of the queried city — must not leak in.
        ("1", "AVA", "ARNOLD", "MD", "CARDIOLOGY", "OTHER ORG", "999", 10,
         "9 FAR RD", "80301", "BOULDER", "CO", "3030000009"),
        # NPI 2: same org, door D1 only.
        ("2", "BEN", "BROOK", "DO", "INTERVENTIONAL CARDIOLOGY", "HEART GROUP",
         "111", 40, "1 MAIN ST", "80201", "DENVER", "CO", "3030000001"),
        # NPI 3: independent (no org) at its own door -> solo site.
        ("3", "CARA", "CRUZ", "MD", "CARDIOLOGY", None, None, None,
         "3 ELM CT", "80203", "DENVER", "CO", "3030000003"),
        # NPI 4: org 111 with a blank address -> provider counts, no door.
        ("4", "DEV", "DOSHI", "MD", "CARDIOLOGY", "HEART GROUP", "111", 40,
         "", "80201", "DENVER", "CO", None),
        # Non-matching specialty stays out entirely.
        ("5", "EMA", "EAST", "MD", "DERMATOLOGY", "SKIN GROUP", "222", 5,
         "5 PINE LN", "80201", "DENVER", "CO", "3030000005"),
    ]
    connection.executemany(
        "insert into raw_dac_national values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        dac_rows,
    )
    connection.executemany(
        "insert into raw_physician_by_provider values (?, ?, ?, ?, ?)",
        [
            ("1", "Cardiology", 100.0, 10.0, 8.0),
            # Repeated national row must not duplicate NPI 1's total.
            ("1", "Cardiology", 100.0, 10.0, 8.0),
            ("2", "Interventional Cardiology", 200.0, 20.0, 12.0),
            ("3", "Cardiology", 50.0, 5.0, 4.0),
        ],
    )
    connection.executemany(
        "insert into raw_part_d_by_provider values (?, ?, ?)",
        [("1", 10.0, 4.0), ("3", 30.0, 6.0)],
    )
    connection.executemany(
        "insert into raw_open_payments_general values (?, ?, ?)",
        [("1", 5.0, "ACME"), ("1", 5.0, "ACME"), ("2", 7.0, "ZENITH")],
    )
    connection.executemany(
        "insert into raw_nppes values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1", "AVA", "ARNOLD", "MD", "2 Oak Ave", "DENVER", "CO",
             "802020000", "3030000002", None),
            ("2", "BEN", "BROOK", "DO", "UNMATCHABLE BLVD", "DENVER", "CO",
             "80299", "3030000001", None),
            ("3", "CARA", "CRUZ", "MD", "3 Elm Ct", "DENVER", "CO",
             "80203", "3030000003", None),
        ],
    )
    connection.executemany(
        "insert into address_geocode values (?, ?, ?)",
        [
            ("1 MAIN ST|80201", 39.74, -104.99),
            ("2 OAK AVE|80202", 39.75, -104.98),
            ("3 ELM CT|80203", 39.76, -104.97),
        ],
    )
    return connection


def _client() -> TestClient:
    connection = _database()
    app = FastAPI()
    app.include_router(get_market_snapshot_router(lambda: connection))
    return TestClient(app)


D1 = site_identifier("cms_enrollment", "1 MAIN ST", "80201", "111")
D2 = site_identifier("cms_enrollment", "2 OAK AVE", "80202", "111")
D3 = site_identifier("cms_enrollment", "3 ELM CT", "80203", None)


def _snapshot(client, **overrides):
    params = {"specialty": "cardiology", "city": "Denver", "state": "CO"}
    params.update(overrides)
    response = client.get("/practices/market-snapshot", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_totals_and_block_shapes():
    body = _snapshot(_client())
    assert body["contract_version"] == 1
    assert body["location_basis"] == "cms_enrollment"
    assert body["metric_scope"] == "national_npi_totals"
    assert body["totals"] == {
        "organizations": 1,
        "sites": 3,
        "providers": 4,
        # Distinct-NPI sum: 100 + 200 + 50 (NPI 4 has no Part B row).
        "partb_payments": 350.0,
        "partd_drug_cost": 40.0,
    }


def test_org_rollup_deduplicates_multi_door_npis():
    body = _snapshot(_client())
    assert len(body["organizations"]) == 1
    org = body["organizations"][0]
    assert org["org_pac_id"] == "111"
    assert org["name"] == "HEART GROUP"
    # NPI 1 (two doors), NPI 2, and door-less NPI 4 are each one provider.
    assert org["provider_count"] == 3
    assert org["site_count"] == 2
    # 100 + 200, with NPI 1 counted once despite two doors.
    assert org["partb_payments"] == 300.0
    assert org["open_payments_total"] == 17.0
    assert org["group_size_national"] == 40


def test_sites_keep_roster_power_and_solo_naming():
    body = _snapshot(_client())
    by_id = {site["site_id"]: site for site in body["sites"]}
    assert set(by_id) == {D1, D2, D3}

    main_st = by_id[D1]
    assert main_st["providers_here"] == 2
    assert main_st["partb_payments"] == 300.0  # roster power: 100 + 200
    assert main_st["site_classification"] == "organization_context"
    assert main_st["practice_name"] == "HEART GROUP"
    assert main_st["billing_artifact"] is False
    assert main_st["lat"] == 39.74

    solo = by_id[D3]
    assert solo["org_pac_id"] is None
    assert solo["site_classification"] == "solo"
    assert solo["solo_provider_name"] == "CARA CRUZ"
    assert solo["partb_payments"] == 50.0


def test_provider_doors_and_nppes_primary_ordering():
    body = _snapshot(_client())
    by_npi = {p["npi"]: p for p in body["providers"]}
    assert set(by_npi) == {"1", "2", "3", "4"}

    ava = by_npi["1"]
    # Boulder door filtered out; NPPES-primary door (D2) listed first.
    assert ava["site_ids"] == [D2, D1]
    assert ava["door_count"] == 2
    assert ava["has_nppes_primary_door"] is True
    assert ava["org_pac_ids"] == ["111"]
    assert ava["partb_payments"] == 100.0
    assert ava["open_payments_total"] == 10.0

    ben = by_npi["2"]
    assert ben["site_ids"] == [D1]
    assert ben["has_nppes_primary_door"] is False

    doorless = by_npi["4"]
    assert doorless["site_ids"] == []
    assert doorless["door_count"] == 0

    # Sorted by Part B descending, unknown dollars last.
    ordered = [p["npi"] for p in body["providers"]]
    assert ordered == ["2", "1", "3", "4"]


def test_independent_bucket_rolls_up_solo_clinicians():
    body = _snapshot(_client())
    independent = body["independent"]
    assert independent == {
        "provider_count": 1,
        "site_count": 1,
        "partb_payments": 50.0,
        "partd_drug_cost": 30.0,
        "open_payments_total": None,
    }


def test_zip_boundary_scopes_doors_and_providers():
    body = _snapshot(_client(), city=None, state=None, zips="80201,80202")
    site_ids = {site["site_id"] for site in body["sites"]}
    assert site_ids == {D1, D2}
    # NPI 4's blank-street row still carries ZIP 80201, so the provider stays
    # in scope (door-less) — same behavior as the city boundary.
    npis = {p["npi"] for p in body["providers"]}
    assert npis == {"1", "2", "4"}
    assert body["independent"] is None


def test_missing_specialty_is_rejected():
    client = _client()
    response = client.get(
        "/practices/market-snapshot", params={"city": "Denver", "state": "CO"}
    )
    assert response.status_code == 422


def test_wildcard_specialty_is_rejected():
    client = _client()
    response = client.get(
        "/practices/market-snapshot",
        params={"specialty": "card%", "city": "Denver", "state": "CO"},
    )
    assert response.status_code == 422
