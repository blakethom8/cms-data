import hashlib
import json
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import serving_diagnostics as diagnostics


def test_committed_case_manifest_is_bounded_and_covers_s2_families() -> None:
    path = REPOSITORY_ROOT / "docs/operations/workloads/s2-canonical-cases-v1.json"
    cases, digest = diagnostics.load_cases(path)
    assert len(cases) == 14
    assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert {case.family for case in cases} >= {
        "provider-profile", "practice-search", "market-snapshot", "industry-search",
        "industry-options", "industry-detail", "radar", "explorer-evidence",
    }
    assert {case.name for case in cases} >= {
        "profile-missing", "practice-cms-empty", "practice-cms-state",
        "practice-nppes-state", "explorer-ten-npi",
    }


def test_manifest_rejects_unknown_fields_and_unsafe_paths(tmp_path: Path) -> None:
    for entry in (
        {"name": "safe-name", "family": "profile", "path": "https://bad.test", "purpose": "x"},
        {"name": "safe-name", "family": "profile", "path": "/ok", "purpose": "x", "extra": 1},
    ):
        path = tmp_path / "cases.json"
        path.write_text(json.dumps({"schema_version": 1, "cases": [entry]}))
        with pytest.raises(ValueError):
            diagnostics.load_cases(path)


def test_result_count_is_explicit_and_fail_closed() -> None:
    assert diagnostics._result_count({"returned_count": 3}, "returned_count") == 3
    assert diagnostics._result_count({"results": [1, 2]}, "results") == 2
    assert diagnostics._result_count([1, 2], None) == 2
    with pytest.raises(ValueError):
        diagnostics._result_count({}, "returned_count")


def test_run_records_identity_and_never_serializes_key(monkeypatch) -> None:
    case = diagnostics.DiagnosticCase(
        "profile", "provider-profile", "/profiles/1", 200, None, "profile"
    )
    monkeypatch.setattr(
        diagnostics, "_request_case",
        lambda origin, case, api_key, timeout: {
            "name": case.name, "passed": True, "api_key_seen": api_key == "secret",
            "response_sha256": "d" * 64, "elapsed_ms": 10.0,
        },
    )
    evidence = diagnostics.run_diagnostics(
        origin="http://127.0.0.1:8080", api_key="secret", cases=[case],
        manifest_sha256="a" * 64, deployment_id="deployment-1",
        warehouse_release_id="warehouse-1", warehouse_sha256="b" * 64,
        code_commit="c" * 40, executor_settings={"threads": 8}, timeout_seconds=30,
    )
    assert evidence["failure_count"] == 0
    assert evidence["status_failure_count"] == 0
    assert evidence["stability_failure_count"] == 0
    assert evidence["trial_count"] == 3
    assert evidence["cases"][0]["median_elapsed_ms"] == 10.0
    assert evidence["identity"]["executor_settings"] == {"threads": 8}
    assert "secret" not in json.dumps(evidence)
