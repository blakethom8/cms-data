import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from explorer import get_explorer_router


def _client() -> TestClient:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE raw_nppes (
            npi VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            practice_address_1 VARCHAR
        );
        CREATE TABLE raw_dac_national (
            "NPI" VARCHAR,
            org_pac_id VARCHAR,
            "Facility Name" VARCHAR,
            adrs_id VARCHAR
        );
        CREATE TABLE raw_reassignment (
            "Individual NPI" VARCHAR,
            "Group PAC ID" VARCHAR,
            "Group Legal Business Name" VARCHAR
        );
        CREATE TABLE raw_pecos_enrollment (
            "NPI" VARCHAR,
            ENRLMT_ID VARCHAR,
            ORG_NAME VARCHAR
        );
        CREATE TABLE raw_physician_by_provider (
            "Rndrng_NPI" VARCHAR,
            "Tot_Mdcr_Pymt_Amt" DOUBLE
        );
        CREATE TABLE raw_dac_facility_affiliations (
            "NPI" VARCHAR,
            facility_type VARCHAR,
            "Facility Affiliations Certification Number" VARCHAR
        );
        CREATE TABLE raw_pecos_reassignment (
            REASGN_BNFT_ENRLMT_ID VARCHAR,
            RCV_BNFT_ENRLMT_ID VARCHAR
        );
        CREATE TABLE raw_pecos_practice_location (
            ENRLMT_ID VARCHAR,
            CITY_NAME VARCHAR,
            STATE_CD VARCHAR,
            ZIP_CD VARCHAR
        );
        CREATE TABLE pecos_provider_organizations (
            npi VARCHAR,
            provider_enrollment_id VARCHAR,
            receiving_enrollment_id VARCHAR,
            receiving_organization_name VARCHAR
        );
        CREATE TABLE pecos_enrollment_practice_locations (
            receiving_enrollment_id VARCHAR,
            receiving_organization_name VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR
        )
        """
    )
    connection.execute(
        "INSERT INTO raw_nppes VALUES ('1710390513', 'LAUREN', 'DESTEFANO', '8700 BEVERLY BLVD')"
    )
    connection.execute(
        "INSERT INTO raw_dac_national VALUES ('1710390513', 'ORG-1', 'CEDARS-SINAI', 'ADDR-1')"
    )
    connection.execute(
        "INSERT INTO raw_reassignment VALUES ('1710390513', 'GROUP-1', 'CEDARS GROUP')"
    )
    connection.executemany(
        "INSERT INTO raw_pecos_enrollment VALUES (?, ?, ?)",
        [
            ("1710390513", "IND-1", None),
            ("1999999999", "ORG-ENROLL-1", "CEDARS GROUP"),
        ],
    )
    connection.execute("INSERT INTO raw_pecos_reassignment VALUES ('IND-1', 'ORG-ENROLL-1')")
    connection.execute(
        "INSERT INTO raw_pecos_practice_location VALUES ('ORG-ENROLL-1', 'LOS ANGELES', 'CA', '90048')"
    )
    connection.execute(
        "INSERT INTO pecos_provider_organizations "
        "VALUES ('1710390513', 'IND-1', 'ORG-ENROLL-1', 'CEDARS GROUP')"
    )
    connection.execute(
        "INSERT INTO pecos_enrollment_practice_locations "
        "VALUES ('ORG-ENROLL-1', 'CEDARS GROUP', 'LOS ANGELES', 'CA', '90048')"
    )
    connection.execute("INSERT INTO raw_physician_by_provider VALUES ('1710390513', 125000.0)")
    connection.execute(
        "INSERT INTO raw_dac_facility_affiliations VALUES ('1710390513', 'Hospital', '050625')"
    )

    app = FastAPI()
    app.include_router(get_explorer_router(lambda: connection))
    return TestClient(app)


def _source(payload: dict, key: str) -> dict:
    return next(source for source in payload["sources"] if source["key"] == key)


def test_provider_evidence_follows_one_npi_across_source_grains() -> None:
    response = _client().get("/explorer/provider-evidence?npis=1710390513&limit=5")

    assert response.status_code == 200
    payload = response.json()
    assert payload["npis"] == ["1710390513"]

    dac = _source(payload, "dac_national")
    assert dac["grain"] == "one clinician enrollment × organization × practice address"
    assert dac["providers"]["1710390513"]["rows"] == [
        ["1710390513", "ORG-1", "CEDARS-SINAI", "ADDR-1"]
    ]

    ppef = _source(payload, "ppef_reassignment")
    assert ppef["providers"]["1710390513"]["rows"] == [["IND-1", "ORG-ENROLL-1"]]

    locations = _source(payload, "ppef_practice_location")
    assert locations["providers"]["1710390513"]["rows"] == [
        ["ORG-ENROLL-1", "LOS ANGELES", "CA", "90048"]
    ]

    organization_bridge = _source(payload, "curated_pecos_organization_bridge")
    assert organization_bridge["providers"]["1710390513"]["rows"] == [
        ["1710390513", "IND-1", "ORG-ENROLL-1", "CEDARS GROUP"]
    ]

    location_bridge = _source(payload, "curated_pecos_location_bridge")
    assert location_bridge["providers"]["1710390513"]["rows"] == [
        [
            "1710390513",
            "ORG-ENROLL-1",
            "CEDARS GROUP",
            "LOS ANGELES",
            "CA",
            "90048",
        ]
    ]


def test_provider_evidence_reports_optional_tables_as_unavailable() -> None:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE raw_nppes (npi VARCHAR)")
    app = FastAPI()
    app.include_router(get_explorer_router(lambda: connection))

    payload = TestClient(app).get("/explorer/provider-evidence?npis=1710390513").json()

    ppef = _source(payload, "ppef_reassignment")
    assert ppef["availability"] == "unavailable"
    assert ppef["missing_tables"] == ["raw_pecos_reassignment", "raw_pecos_enrollment"]
    assert ppef["providers"] == {}
    source_keys = {source["key"] for source in payload["sources"]}
    assert {
        "part_d_provider_year",
        "part_d_provider_drug",
        "open_payments_general",
        "open_payments_research",
        "open_payments_ownership",
        "mips_performance",
        "dme_referring",
        "order_referring",
    } <= source_keys


def test_provider_evidence_validates_npis_and_limits() -> None:
    client = _client()

    assert client.get("/explorer/provider-evidence?npis=not-an-npi").status_code == 422
    assert client.get("/explorer/provider-evidence?npis=1710390513&limit=26").status_code == 422
    assert client.get("/explorer/provider-evidence?npis=").status_code == 422


def test_provider_evidence_exposes_reviewed_npi_addressable_raw_sources() -> None:
    connection = duckdb.connect(":memory:")
    connection.execute('CREATE TABLE raw_part_d_by_provider ("Prscrbr_NPI" VARCHAR, claims INTEGER)')
    connection.execute('CREATE TABLE raw_dme_by_referring_provider ("Rfrg_NPI" VARCHAR, suppliers INTEGER)')
    connection.execute('CREATE TABLE raw_order_and_referring ("NPI" VARCHAR, last_name VARCHAR)')
    connection.execute("INSERT INTO raw_part_d_by_provider VALUES ('1710390513', 42)")
    connection.execute("INSERT INTO raw_dme_by_referring_provider VALUES ('1710390513', 3)")
    connection.execute("INSERT INTO raw_order_and_referring VALUES ('1710390513', 'DESTEFANO')")
    app = FastAPI()
    app.include_router(get_explorer_router(lambda: connection))

    payload = TestClient(app).get("/explorer/provider-evidence?npis=1710390513").json()

    assert _source(payload, "part_d_provider_year")["providers"]["1710390513"]["rows"] == [["1710390513", 42]]
    assert _source(payload, "dme_referring")["providers"]["1710390513"]["rows"] == [["1710390513", 3]]
    assert _source(payload, "order_referring")["providers"]["1710390513"]["rows"] == [["1710390513", "DESTEFANO"]]
