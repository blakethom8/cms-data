import json
import sys
from pathlib import Path

import duckdb

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import serving_plan_capture as capture


def test_profiling_connection_records_exact_sql_parameters_and_plan(tmp_path: Path) -> None:
    database = tmp_path / "fixture.duckdb"
    setup = duckdb.connect(str(database))
    setup.execute("create table facts(id integer, value varchar)")
    setup.execute("insert into facts values (1, 'one'), (2, 'two')")
    setup.close()
    records: list[dict] = []
    connection = capture.ProfilingConnection(database, records)
    token = capture._ACTIVE_CASE.set("fixture-case")
    try:
        rows = connection.execute(
            "select value from facts where id = ?", [2]
        ).fetchall()
    finally:
        capture._ACTIVE_CASE.reset(token)
        connection.close()

    assert rows == [("two",)]
    assert len(records) == 1
    assert records[0]["sql"] == "select value from facts where id = ?"
    assert records[0]["parameters"] == [2]
    assert records[0]["plan_error"] is None
    assert records[0]["summary"]["rows_scanned"] == 2
    assert records[0]["plan_sha256"]


def test_response_variations_distinguish_order_content_and_values() -> None:
    payloads = [
        {"items": [{"id": 1}, {"id": 2}], "total": 2, "label": "a"},
        {"items": [{"id": 2}, {"id": 1}], "total": 3, "label": "a"},
    ]
    assert capture.response_variations(payloads) == [
        {"path": "items", "kind": "list_order"},
        {"path": "total", "kind": "value"},
    ]

    nested = [
        {"results": [{"npi": "1", "specialties": ["A", "B"]}]},
        {"results": [{"npi": "1", "specialties": ["B", "A"]}]},
    ]
    assert capture.response_variations(nested) == [
        {"path": "results[npi=1].specialties", "kind": "list_order"}
    ]


def test_plan_summary_is_compact_and_does_not_invent_temp_storage() -> None:
    plan = {
        "latency": 0.1,
        "cpu_time": 0.2,
        "rows_returned": 3,
        "children": [
            {
                "operator_name": "SEQ_SCAN",
                "operator_timing": 0.05,
                "operator_rows_scanned": 10,
                "operator_cardinality": 3,
                "result_set_size": 24,
                "children": [],
            }
        ],
    }
    summary = capture.summarize_plan(plan)
    assert summary["latency_ms"] == 100.0
    assert summary["rows_scanned"] == 10
    assert summary["slowest_operator"] == "SEQ_SCAN"
    assert summary["peak_temporary_storage_bytes"] is None
    assert "children" not in json.dumps(summary)
