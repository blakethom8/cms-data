import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from explorer import get_explorer_router


def _client() -> TestClient:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE raw_physician_by_provider_and_service (
            "Rndrng_NPI" VARCHAR,
            "Rndrng_Prvdr_Last_Org_Name" VARCHAR,
            "HCPCS_Cd" VARCHAR,
            "HCPCS_Desc" VARCHAR,
            "Place_Of_Srvc" VARCHAR,
            "Tot_Srvcs" DOUBLE,
            "Avg_Mdcr_Pymt_Amt" DOUBLE,
            "Rndrng_Prvdr_City" VARCHAR,
            "Rndrng_Prvdr_State_Abrvtn" VARCHAR,
            source_marker VARCHAR
        )
        """
    )
    connection.executemany(
        """
        INSERT INTO raw_physician_by_provider_and_service
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(1000000000 + index),
                f"Provider {index}",
                f"{9000 + index}",
                f"Procedure {index}",
                "O",
                float(200 - index),
                42.5,
                "Los Angeles",
                "CA",
                f"raw-{index}",
            )
            for index in range(75)
        ],
    )

    app = FastAPI()
    app.include_router(get_explorer_router(lambda: connection))
    return TestClient(app)


def test_curated_sample_returns_requested_number_of_rows() -> None:
    response = _client().get(
        "/explorer/sample/physician_by_service?city=Los%20Angeles&state=CA&limit=50"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["rows"]) == 50
    assert payload["columns"] == [
        "npi",
        "last_name",
        "hcpcs",
        "procedure_desc",
        "place_of_service",
        "services",
        "avg_payment",
    ]


def test_all_column_sample_returns_every_physical_column() -> None:
    response = _client().get(
        "/explorer/sample-all/physician_by_service?limit=25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["rows"]) == 25
    assert payload["columns"] == [
        "Rndrng_NPI",
        "Rndrng_Prvdr_Last_Org_Name",
        "HCPCS_Cd",
        "HCPCS_Desc",
        "Place_Of_Srvc",
        "Tot_Srvcs",
        "Avg_Mdcr_Pymt_Amt",
        "Rndrng_Prvdr_City",
        "Rndrng_Prvdr_State_Abrvtn",
        "source_marker",
    ]
    assert payload["rows"][0][-1] == "raw-0"


def test_sample_limits_are_bounded() -> None:
    client = _client()

    assert client.get("/explorer/sample/physician_by_service?limit=201").status_code == 422
    assert client.get("/explorer/sample-all/physician_by_service?limit=0").status_code == 422


def test_all_column_sample_rejects_unknown_dataset() -> None:
    response = _client().get("/explorer/sample-all/not-a-dataset")

    assert response.status_code == 404
