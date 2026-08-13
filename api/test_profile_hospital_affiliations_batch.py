import duckdb
import pytest
from fastapi import HTTPException

from profiles import _hospital_affiliations_response


@pytest.fixture
def connection():
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE raw_dac_facility_affiliations (
            "NPI" VARCHAR,
            facility_type VARCHAR,
            "Facility Affiliations Certification Number" VARCHAR
        );
        CREATE TABLE raw_hospital_general_info (
            "Facility ID" VARCHAR,
            "Facility Name" VARCHAR,
            "Address" VARCHAR,
            "City/Town" VARCHAR,
            "State" VARCHAR,
            "ZIP Code" VARCHAR,
            "Hospital Type" VARCHAR
        );
        INSERT INTO raw_dac_facility_affiliations VALUES
            ('1111111111', 'Hospital', '050001'),
            ('2222222222', 'Hospital', '050002');
        INSERT INTO raw_hospital_general_info VALUES
            ('050001', 'Alpha Hospital', '1 Main St', 'Los Angeles', 'CA', '90001-1234', 'Acute'),
            ('050002', 'Beta Hospital', '2 Main St', 'Burbank', 'CA', '91501', 'Acute');
        """
    )
    yield conn
    conn.close()


@pytest.mark.anyio
async def test_batch_hospital_affiliations_are_typed_bounded_and_grouped(connection) -> None:
    response = _hospital_affiliations_response(
        connection, "1111111111,2222222222,3333333333"
    )

    assert set(response["providers"]) == {"1111111111", "2222222222", "3333333333"}
    assert response["providers"]["1111111111"] == [
        {
            "facility_type": "Hospital",
            "ccn": "050001",
            "name": "Alpha Hospital",
            "address": "1 Main St",
            "city": "Los Angeles",
            "state": "CA",
            "zip5": "90001",
            "hospital_type": "Acute",
        }
    ]
    assert response["providers"]["3333333333"] == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "npis",
    [
        "",
        "not-an-npi",
        "1111111111,not-an-npi",
        ",".join(f"{npi:010d}" for npi in range(1, 52)),
    ],
)
async def test_batch_hospital_affiliations_reject_invalid_or_excessive_input(
    connection, npis: str
) -> None:
    with pytest.raises(HTTPException) as denied:
        _hospital_affiliations_response(connection, npis)
    assert denied.value.status_code == 422


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
