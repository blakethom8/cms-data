import csv
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
    process_nppes_provider_file,
)
from radar import get_radar_router


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
