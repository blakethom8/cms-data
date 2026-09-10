import csv
import json
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.nppes_radar import (
    MONTHLY_SOURCE_ID,
    WEEKLY_SOURCE_ID,
    NppesRadarError,
    NppesRadarRelease,
    ensure_radar_schema,
    process_nppes_provider_file,
)
import radar as radar_module
from radar import (
    RadarHydrateRequest,
    RadarMatchScopesRequest,
    RadarMatchScopesResponse,
    RadarScopeMatch,
    get_radar_router,
)

CONTRACT_FIXTURE = Path(__file__).parent / "fixtures" / "radar_release_contract_v1.json"


BASE_HEADERS = [
    "NPI",
    "Entity Type Code",
    "Provider First Name",
    "Provider Last Name (Legal Name)",
    "Provider Credential Text",
    "Provider Enumeration Date",
    "Last Update Date",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Telephone Number",
]


def test_radar_schema_install_is_limited_to_radar_owned_tables() -> None:
    connection = duckdb.connect(":memory:")
    ensure_radar_schema(connection)

    tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert tables == {
        "nppes_radar_provider_state",
        "nppes_radar_events",
        "nppes_radar_releases",
    }
    connection.close()
HEADERS = BASE_HEADERS + [
    field
    for position in range(1, 16)
    for field in (
        f"Healthcare Provider Taxonomy Code_{position}",
        f"Healthcare Provider Primary Taxonomy Switch_{position}",
    )
]


def _provider(
    npi: str,
    *,
    first_name: str,
    last_name: str,
    enumeration_date: str,
    last_update_date: str,
    zip5: str,
    taxonomy: str,
    deactivation_date: str = "",
    reactivation_date: str = "",
) -> dict[str, str]:
    row = dict.fromkeys(HEADERS, "")
    row.update(
        {
            "NPI": npi,
            "Entity Type Code": "1",
            "Provider First Name": first_name,
            "Provider Last Name (Legal Name)": last_name,
            "Provider Credential Text": "MD",
            "Provider Enumeration Date": enumeration_date,
            "Last Update Date": last_update_date,
            "NPI Deactivation Date": deactivation_date,
            "NPI Reactivation Date": reactivation_date,
            "Provider First Line Business Practice Location Address": "1 Main St",
            "Provider Business Practice Location Address City Name": "Denver",
            "Provider Business Practice Location Address State Name": "CO",
            "Provider Business Practice Location Address Postal Code": zip5,
            "Provider Business Practice Location Address Telephone Number": "3035550100",
            "Healthcare Provider Taxonomy Code_1": taxonomy,
            "Healthcare Provider Primary Taxonomy Switch_1": "Y",
        }
    )
    return row


def _write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _release(
    release_id: str,
    *,
    kind: str,
    period_start: date,
    period_end: date,
) -> NppesRadarRelease:
    return NppesRadarRelease(
        source_release_id=release_id,
        source_id=MONTHLY_SOURCE_ID if kind == "monthly_full" else WEEKLY_SOURCE_ID,
        release_kind=kind,  # type: ignore[arg-type]
        period_start=period_start,
        period_end=period_end,
    )


@pytest.fixture
def radar_connection(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    baseline_csv = _write_csv(
        tmp_path / "baseline.csv",
        [
            _provider(
                "1111111111",
                first_name="Alice",
                last_name="Move",
                enumeration_date="01/10/2010",
                last_update_date="06/10/2026",
                zip5="80206",
                taxonomy="207RC0000X",
            ),
            _provider(
                "2222222222",
                first_name="Bob",
                last_name="Taxonomy",
                enumeration_date="02/10/2011",
                last_update_date="06/10/2026",
                zip5="94110",
                taxonomy="207R00000X",
            ),
            _provider(
                "4444444444",
                first_name="Dana",
                last_name="Reactivate",
                enumeration_date="03/10/2012",
                last_update_date="06/15/2026",
                zip5="80220",
                taxonomy="207RG0100X",
                deactivation_date="06/15/2026",
            ),
        ],
    )
    baseline = process_nppes_provider_file(
        connection,
        baseline_csv,
        _release(
            "NPPES_Data_Dissemination_July_2026_V2",
            kind="monthly_full",
            period_start=date(2026, 7, 13),
            period_end=date(2026, 7, 13),
        ),
        baseline=True,
    )
    assert baseline.event_row_count == 0

    weekly_csv = _write_csv(
        tmp_path / "weekly.csv",
        [
            _provider(
                "1111111111",
                first_name="Alice",
                last_name="Move",
                enumeration_date="01/10/2010",
                last_update_date="07/15/2026",
                zip5="80220",
                taxonomy="207RC0000X",
            ),
            _provider(
                "2222222222",
                first_name="Bob",
                last_name="Taxonomy",
                enumeration_date="02/10/2011",
                last_update_date="07/16/2026",
                zip5="94110",
                taxonomy="207RG0100X",
            ),
            _provider(
                "3333333333",
                first_name="Cara",
                last_name="New",
                enumeration_date="07/17/2026",
                last_update_date="07/17/2026",
                zip5="80220",
                taxonomy="207RC0000X",
            ),
            _provider(
                "4444444444",
                first_name="Dana",
                last_name="Reactivate",
                enumeration_date="03/10/2012",
                last_update_date="07/18/2026",
                zip5="80220",
                taxonomy="207RG0100X",
                reactivation_date="07/18/2026",
            ),
        ],
    )
    result = process_nppes_provider_file(
        connection,
        weekly_csv,
        _release(
            "NPPES_Data_Dissemination_071326_071926_Weekly_V2",
            kind="weekly_incremental",
            period_start=date(2026, 7, 13),
            period_end=date(2026, 7, 19),
        ),
    )
    assert result.provider_row_count == 4
    assert result.event_row_count == 4
    yield connection
    connection.close()


def test_release_processing_classifies_changes_and_is_idempotent(
    radar_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    event_types = dict(
        radar_connection.execute(
            "SELECT npi, event_type FROM nppes_radar_events ORDER BY npi"
        ).fetchall()
    )
    assert event_types == {
        "1111111111": "practice_location_changed",
        "2222222222": "primary_taxonomy_changed",
        "3333333333": "newly_enumerated",
        "4444444444": "reactivated",
    }
    move = radar_connection.execute(
        "SELECT old_zip5, new_zip5 FROM nppes_radar_events WHERE npi = '1111111111'"
    ).fetchone()
    assert move == ("80206", "80220")

    replay_csv = _write_csv(
        tmp_path / "replay.csv",
        [
            _provider(
                "3333333333",
                first_name="Cara",
                last_name="New",
                enumeration_date="07/17/2026",
                last_update_date="07/17/2026",
                zip5="80220",
                taxonomy="207RC0000X",
            )
        ],
    )
    replay = process_nppes_provider_file(
        radar_connection,
        replay_csv,
        _release(
            "NPPES_Data_Dissemination_071326_071926_Weekly_V2",
            kind="weekly_incremental",
            period_start=date(2026, 7, 13),
            period_end=date(2026, 7, 19),
        ),
    )
    assert replay.already_processed is True
    assert radar_connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_events"
    ).fetchone()[0] == 4


def test_radar_api_filters_by_market_zip_event_and_taxonomy(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)

    response = client.get(
        "/radar/providers",
        params=[
            ("zip5", "80220"),
            ("since", "2026-07-13"),
            ("until", "2026-07-19"),
            ("taxonomy_code", "207RC0000X"),
        ],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["source_fresh_through"] == "2026-07-19"
    assert {event["event_type"] for event in payload["events"]} == {
        "newly_enumerated",
        "practice_location_changed",
    }
    assert {event["npi"] for event in payload["events"]} == {
        "1111111111",
        "3333333333",
    }


def test_radar_api_city_scope_normalizes_exact_match_and_preserves_shape(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    common = [("since", "2026-07-13"), ("until", "2026-07-19")]

    city_response = client.get(
        "/radar/providers",
        params=[("city", "  denver "), ("state", " co "), *common],
    )
    zip_response = client.get(
        "/radar/providers",
        params=[("zip5", "80220"), *common],
    )

    assert city_response.status_code == 200
    city_payload = city_response.json()
    assert city_payload["total"] == 2
    assert {event["npi"] for event in city_payload["events"]} == {
        "1111111111",
        "3333333333",
    }
    assert city_payload.keys() == zip_response.json().keys()


@pytest.mark.parametrize(
    "params",
    [
        [],
        [("city", "Denver")],
        [("state", "CO")],
        [("zip5", "80220"), ("city", "Denver"), ("state", "CO")],
        [("city", "   "), ("state", "CO")],
        [("city", "Denver"), ("state", "Colorado")],
    ],
)
def test_radar_api_requires_one_complete_valid_scope(
    radar_connection: duckdb.DuckDBPyConnection,
    params: list[tuple[str, str]],
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    response = TestClient(app).get("/radar/providers", params=params)

    assert response.status_code == 422


def test_shared_release_fixture_validates_the_paired_contract() -> None:
    fixture = json.loads(CONTRACT_FIXTURE.read_text(encoding="utf-8"))
    request = RadarMatchScopesRequest.model_validate(fixture["match_request"])
    response = RadarMatchScopesResponse.model_validate(fixture["match_response"])
    hydration = RadarHydrateRequest.model_validate(fixture["hydrate_request"])

    assert request.source_release_id == response.source_release_id
    assert [scope.scope_key for scope in request.scopes] == [
        scope.scope_key for scope in response.scopes
    ]
    assert [row.event_id for row in hydration.references] == [
        "evt-fixture-1",
        "evt-fixture-2",
    ]
    assert not {"workspace_id", "customer_organization_id", "user_id"} & set(
        fixture["match_request"]
    )


def test_radar_release_and_batch_match_are_pinned_and_scope_fair(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    weekly_release = "NPPES_Data_Dissemination_071326_071926_Weekly_V2"

    release = client.get("/radar/providers/release")
    assert release.status_code == 200
    assert release.json() == {
        "contract_version": 1,
        "source_release_id": weekly_release,
        "source_data_period": "2026-07-13/2026-07-19",
        "source_fresh_through": "2026-07-19",
    }

    response = client.post(
        "/radar/providers/match-scopes",
        json={
            "source_release_id": weekly_release,
            "scopes": [
                {
                    "scope_key": "denver-core",
                    "zip_codes": ["80220"],
                    "taxonomy_codes": ["207RC0000X"],
                    "event_types": [
                        "newly_enumerated",
                        "practice_location_changed",
                    ],
                },
                {
                    "scope_key": "denver-city",
                    "city": " denver ",
                    "state": "co",
                    "event_types": ["practice_location_changed"],
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_release_id"] == weekly_release
    assert [row["scope_key"] for row in payload["scopes"]] == [
        "denver-core",
        "denver-city",
    ]
    core = payload["scopes"][0]["matches"]
    assert len(core) == 2
    assert {
        row["location_change"]
        for row in core
        if row["event_id"]
        == radar_connection.execute(
            "SELECT event_id FROM nppes_radar_events "
            "WHERE event_type = 'practice_location_changed'"
        ).fetchone()[0]
    } == {"entered_market"}
    assert payload["scopes"][1]["matches"][0]["location_change"] is None


def test_batch_match_rejects_a_changed_release_and_tenant_context(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    scope = {
        "scope_key": "scope-1",
        "zip_codes": ["80220"],
        "event_types": ["newly_enumerated"],
    }

    changed = client.post(
        "/radar/providers/match-scopes",
        json={"source_release_id": "retired-release", "scopes": [scope]},
    )
    widened = client.post(
        "/radar/providers/match-scopes",
        json={"scopes": [{**scope, "workspace_id": "must-not-cross"}]},
    )

    assert changed.status_code == 409
    assert changed.json()["detail"]["code"] == "radar_release_changed"
    assert widened.status_code == 422


def test_reconciliation_requests_reject_duplicate_opaque_keys(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    scope = {
        "scope_key": "scope-1",
        "zip_codes": ["80220"],
        "event_types": ["newly_enumerated"],
    }
    release = "NPPES_Data_Dissemination_071326_071926_Weekly_V2"
    event_id = radar_connection.execute(
        "SELECT event_id FROM nppes_radar_events LIMIT 1"
    ).fetchone()[0]

    duplicate_scopes = client.post(
        "/radar/providers/match-scopes",
        json={"scopes": [scope, scope]},
    )
    duplicate_references = client.post(
        "/radar/providers/hydrate",
        json={
            "references": [
                {"event_id": event_id, "source_release_id": release},
                {"event_id": event_id, "source_release_id": release},
            ]
        },
    )

    assert duplicate_scopes.status_code == 422
    assert duplicate_references.status_code == 422


def test_batch_match_enforces_a_request_wide_result_ceiling(
    radar_connection: duckdb.DuckDBPyConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)

    def oversized_scope_matches(
        connection: duckdb.DuckDBPyConnection,
        scope: radar_module.RadarMatchScope,
    ) -> list[RadarScopeMatch]:
        del connection
        return [
            RadarScopeMatch(event_id=f"{scope.scope_key}-{index}")
            for index in range(2_501)
        ]

    monkeypatch.setattr(radar_module, "_scope_matches", oversized_scope_matches)
    response = client.post(
        "/radar/providers/match-scopes",
        json={
            "scopes": [
                {
                    "scope_key": key,
                    "zip_codes": ["80220"],
                    "event_types": ["newly_enumerated"],
                }
                for key in ("scope-1", "scope-2")
            ]
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "radar_request_too_broad",
        "maximum_matches": radar_module.MAX_MATCHES_PER_REQUEST,
    }


def test_creation_baseline_excludes_events_first_detected_after_watch_creation(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    scope = {
        "scope_key": "creation-baseline",
        "zip_codes": ["80220"],
        "event_types": ["newly_enumerated", "practice_location_changed"],
    }

    before = client.post(
        "/radar/providers/match-scopes",
        json={"scopes": [{**scope, "baseline_as_of": "2000-01-01T00:00:00Z"}]},
    )
    after = client.post(
        "/radar/providers/match-scopes",
        json={"scopes": [{**scope, "baseline_as_of": "2100-01-01T00:00:00Z"}]},
    )

    assert before.status_code == 200
    assert before.json()["scopes"][0]["matches"] == []
    assert len(after.json()["scopes"][0]["matches"]) == 2


def test_hydration_preserves_reference_order_and_observation_release(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    events = radar_connection.execute(
        "SELECT event_id FROM nppes_radar_events "
        "WHERE event_type IN ('newly_enumerated', 'practice_location_changed') "
        "ORDER BY event_type"
    ).fetchall()
    event_ids = [row[0] for row in reversed(events)]
    weekly_release = "NPPES_Data_Dissemination_071326_071926_Weekly_V2"
    references = [
        {"event_id": event_id, "source_release_id": weekly_release}
        for event_id in event_ids
    ]

    response = client.post(
        "/radar/providers/hydrate",
        json={"references": references},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["references"] == references
    assert [event["event_id"] for event in payload["events"]] == event_ids
    assert payload["source_fresh_through"] == "2026-07-19"
    assert all(event["reason"] for event in payload["events"])


def test_hydration_fails_closed_for_impossible_or_missing_references(
    radar_connection: duckdb.DuckDBPyConnection,
) -> None:
    app = FastAPI()
    app.include_router(get_radar_router(lambda: radar_connection))
    client = TestClient(app)
    event_id = radar_connection.execute(
        "SELECT event_id FROM nppes_radar_events WHERE event_type = 'newly_enumerated'"
    ).fetchone()[0]
    baseline_release = "NPPES_Data_Dissemination_July_2026_V2"

    too_early = client.post(
        "/radar/providers/hydrate",
        json={
            "references": [
                {"event_id": event_id, "source_release_id": baseline_release}
            ]
        },
    )
    missing = client.post(
        "/radar/providers/hydrate",
        json={
            "references": [
                {"event_id": "missing-event", "source_release_id": baseline_release}
            ]
        },
    )

    assert too_early.status_code == 409
    assert too_early.json()["detail"]["code"] == "radar_event_reference_unavailable"
    assert missing.status_code == 409


@pytest.mark.parametrize(
    ("method", "path", "json_body"),
    [
        ("get", "/radar/providers/release", None),
        (
            "post",
            "/radar/providers/match-scopes",
            {
                "scopes": [
                    {
                        "scope_key": "missing-tables",
                        "zip_codes": ["80220"],
                        "event_types": ["newly_enumerated"],
                    }
                ]
            },
        ),
        (
            "post",
            "/radar/providers/hydrate",
            {
                "references": [
                    {"event_id": "event-1", "source_release_id": "release-1"}
                ]
            },
        ),
    ],
)
def test_reconciliation_endpoints_fail_closed_when_radar_is_not_installed(
    method: str, path: str, json_body: dict | None
) -> None:
    connection = duckdb.connect(":memory:")
    app = FastAPI()
    app.include_router(get_radar_router(lambda: connection))
    client = TestClient(app)

    response = getattr(client, method)(path, json=json_body) if json_body else client.get(path)

    assert response.status_code == 503
    assert "not been installed" in response.json()["detail"]
    connection.close()


def test_weekly_release_requires_a_monthly_baseline(tmp_path: Path) -> None:
    connection = duckdb.connect(":memory:")
    weekly_csv = _write_csv(
        tmp_path / "weekly_without_baseline.csv",
        [
            _provider(
                "3333333333",
                first_name="Cara",
                last_name="New",
                enumeration_date="07/17/2026",
                last_update_date="07/17/2026",
                zip5="80220",
                taxonomy="207RC0000X",
            )
        ],
    )

    with pytest.raises(NppesRadarError, match="monthly NPPES baseline"):
        process_nppes_provider_file(
            connection,
            weekly_csv,
            _release(
                "weekly-without-baseline",
                kind="weekly_incremental",
                period_start=date(2026, 7, 13),
                period_end=date(2026, 7, 19),
            ),
        )
    connection.close()


def test_out_of_order_release_is_rejected(
    radar_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    older_csv = _write_csv(
        tmp_path / "older.csv",
        [
            _provider(
                "5555555555",
                first_name="Evan",
                last_name="Older",
                enumeration_date="07/10/2026",
                last_update_date="07/10/2026",
                zip5="80220",
                taxonomy="207RC0000X",
            )
        ],
    )

    with pytest.raises(NppesRadarError, match="source-period order"):
        process_nppes_provider_file(
            radar_connection,
            older_csv,
            _release(
                "older-weekly-release",
                kind="weekly_incremental",
                period_start=date(2026, 7, 6),
                period_end=date(2026, 7, 12),
            ),
        )
    assert radar_connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_releases"
    ).fetchone()[0] == 2


def test_invalid_npi_rolls_back_release_and_state(tmp_path: Path) -> None:
    connection = duckdb.connect(":memory:")
    invalid_csv = _write_csv(
        tmp_path / "invalid.csv",
        [
            _provider(
                "123",
                first_name="Invalid",
                last_name="NPI",
                enumeration_date="07/13/2026",
                last_update_date="07/13/2026",
                zip5="80220",
                taxonomy="207RC0000X",
            )
        ],
    )

    with pytest.raises(NppesRadarError, match="Invalid Type 1 NPI"):
        process_nppes_provider_file(
            connection,
            invalid_csv,
            _release(
                "invalid-monthly-release",
                kind="monthly_full",
                period_start=date(2026, 7, 13),
                period_end=date(2026, 7, 13),
            ),
            baseline=True,
        )
    assert connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_provider_state"
    ).fetchone()[0] == 0
    assert connection.execute(
        "SELECT COUNT(*) FROM nppes_radar_releases"
    ).fetchone()[0] == 0
    connection.close()


def test_historical_reactivation_date_does_not_repeat_in_later_weeklies(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    provider = _provider(
        "1111111111",
        first_name="Ada",
        last_name="Historical",
        enumeration_date="01/01/2006",
        last_update_date="02/01/2020",
        zip5="80220",
        taxonomy="207RC0000X",
        deactivation_date="01/01/2020",
        reactivation_date="02/01/2020",
    )
    baseline = _write_csv(tmp_path / "baseline.csv", [provider])
    process_nppes_provider_file(
        connection,
        baseline,
        _release(
            "monthly-release",
            kind="monthly_full",
            period_start=date(2026, 7, 13),
            period_end=date(2026, 7, 13),
        ),
        baseline=True,
    )
    periods = (
        (date(2026, 7, 14), date(2026, 7, 20)),
        (date(2026, 7, 21), date(2026, 7, 27)),
    )
    for index, (period_start, period_end) in enumerate(periods, start=1):
        weekly = _write_csv(tmp_path / f"weekly-{index}.csv", [provider])
        process_nppes_provider_file(
            connection,
            weekly,
            _release(
                f"weekly-release-{index}",
                kind="weekly_incremental",
                period_start=period_start,
                period_end=period_end,
            ),
        )

    assert connection.execute(
        "SELECT count(*) FROM nppes_radar_events WHERE event_type = 'reactivated'"
    ).fetchone() == (0,)
    connection.close()
