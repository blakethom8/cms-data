from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient

from clinical_trials import (
    _build_study_payloads,
    _facility_tokens,
    _parse_geo_filter,
    get_clinical_trials_router,
)


def test_geo_and_facility_inputs_are_normalized() -> None:
    assert _parse_geo_filter("distance(34.1478,-118.1445,50mi)") == (
        34.1478,
        -118.1445,
        50.0,
    )
    assert _facility_tokens("City of Hope Medical Center") == ["city", "of", "hope"]


def test_aact_rows_build_the_app_compatible_study_shape() -> None:
    payload = _build_study_payloads(
        nct_ids=["NCT00000001"],
        core=[
            {
                "nct_id": "NCT00000001",
                "brief_title": "Local lung cancer trial",
                "official_title": "A Local Lung Cancer Trial",
                "overall_status": "RECRUITING",
                "phase": "PHASE2",
                "study_type": "INTERVENTIONAL",
                "enrollment": 120,
                "start_date": date(2026, 1, 1),
                "completion_date": None,
                "primary_completion_date": None,
                "last_update_posted_date": date(2026, 7, 14),
            }
        ],
        conditions=[{"nct_id": "NCT00000001", "name": "Lung Cancer"}],
        interventions=[
            {
                "nct_id": "NCT00000001",
                "name": "Example Drug",
                "intervention_type": "DRUG",
            }
        ],
        sponsors=[
            {
                "nct_id": "NCT00000001",
                "name": "Example Sponsor",
                "agency_class": "INDUSTRY",
            }
        ],
        facilities=[
            {
                "id": 10,
                "nct_id": "NCT00000001",
                "status": "RECRUITING",
                "name": "City of Hope Medical Center",
                "city": "Duarte",
                "state": "California",
                "zip": "91010",
                "country": "United States",
                "latitude": 34.13,
                "longitude": -117.97,
            }
        ],
        facility_people=[
            {
                "facility_id": 10,
                "nct_id": "NCT00000001",
                "name": "Jane Investigator, MD",
                "role": "PRINCIPAL_INVESTIGATOR",
            }
        ],
        officials=[],
        references=[
            {
                "nct_id": "NCT00000001",
                "pmid": "40123456",
                "reference_type": "RESULT",
                "citation": "Example result.",
            }
        ],
    )

    protocol = payload[0]["protocolSection"]
    assert protocol["identificationModule"]["nctId"] == "NCT00000001"
    assert protocol["statusModule"]["lastUpdatePostDateStruct"]["date"] == "2026-07-14"
    assert protocol["contactsLocationsModule"]["locations"][0]["contacts"] == [
        {"name": "Jane Investigator, MD", "role": "PRINCIPAL_INVESTIGATOR"}
    ]
    assert protocol["referencesModule"]["references"][0]["pmid"] == "40123456"


def test_router_translates_v2_query_parameters_for_the_store() -> None:
    class FakeStore:
        def __init__(self) -> None:
            self.request = None

        async def version(self):
            return {"apiVersion": "AACT test", "dataTimestamp": "2026-07-14"}

        async def search(self, **kwargs):
            self.request = kwargs
            return {"studies": [], "totalCount": 0, "nextPageToken": None}

    store = FakeStore()
    app = FastAPI()
    app.include_router(get_clinical_trials_router(store))  # type: ignore[arg-type]
    client = TestClient(app)

    response = client.get(
        "/clinical-trials/studies",
        params={
            "query.cond": "lung cancer",
            "filter.geo": "distance(34.1478,-118.1445,50mi)",
            "filter.overallStatus": "RECRUITING|ACTIVE_NOT_RECRUITING",
            "pageSize": 100,
        },
    )

    assert response.status_code == 200
    assert store.request == {
        "query": "lung cancer",
        "search_type": "condition",
        "statuses": ["RECRUITING", "ACTIVE_NOT_RECRUITING"],
        "page_size": 100,
        "geo": (34.1478, -118.1445, 50.0),
        "facility": None,
        "city": None,
        "states": [],
    }
