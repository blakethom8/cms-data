import json
import os
import stat
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import production_cutover as cutover
from pipeline import production_smoke as smoke


def _successful_request(base_url, method, path, api_key, payload=None):
    if path == "/health":
        return 200, {"status": "ok", "core_providers": 1196535}
    if path == "/practices/capabilities" and api_key is None:
        return 401, {"detail": "Invalid or missing API key"}
    if path == "/practices/capabilities":
        return 200, {
            "contract_version": 2,
            "capabilities": [
                "multi_zip",
                "nppes_primary",
                "exact_radius",
                "multi_specialty",
                "practice_specialties",
                "scoped_metrics",
            ],
        }
    if path.startswith("/practices/search"):
        return 200, {
            "total": 3,
            "results": [
                {
                    "contract_version": 2,
                    "site_id": "site-1",
                    "organization_scope": "single_organization",
                    "address": "1 Main St",
                    "city": "Cleveland",
                    "state": "OH",
                    "zip5": "44101",
                    "providers_here": 3,
                }
            ],
        }
    if path.startswith("/profiles/"):
        return 200, {
            "npi": "1003005257",
            "header": {},
            "panel": {},
            "clinical": {},
            "prescribing": {},
            "industry": {},
            "research": {},
            "locations": [],
            "groups": [],
            "mips": [],
        }
    if path.startswith("/industry/options"):
        return 200, {
            "options": [
                {
                    "value": "Example Manufacturer",
                    "physician_count": 10,
                    "payment_count": 20,
                    "total_usd": 1000.0,
                }
            ]
        }
    if path.startswith("/industry/") and path.endswith("/detail"):
        return 200, {
            "npi": "1003005257",
            "payment_count": 1,
            "total_usd": 100.0,
            "nonfood_usd": 50.0,
            "consulting_speaking_usd": 0.0,
            "by_nature": [],
            "manufacturers": [],
            "products": [],
        }
    if path.startswith("/industry/search"):
        return 200, {
            "total": 1,
            "results": [
                {
                    "npi": "1003005257",
                    "name": "Example Doctor",
                    "payment_count": 1,
                    "total_usd": 100.0,
                    "matched_payment_count": 1,
                    "matched_total_usd": 100.0,
                    "n_manufacturers": 1,
                    "tier": 2,
                }
            ],
        }
    if path == "/research/investigators":
        return 200, {
            "investigators": [],
            "source": "CMS Open Payments Research",
        }
    if path == "/clinical-trials/version":
        return 200, {"snapshotDate": "2026-07-20", "studyCount": 600000}
    if path == "/explorer/catalog":
        return 200, [{"id": "example"}]
    if path == "/tables":
        return 200, {
            "tables": [
                {"name": "core_providers", "approx_rows": 1196535},
                {"name": "hospital_affiliations", "approx_rows": 146970},
                {"name": "raw_hospital_enrollments", "approx_rows": 9175},
            ]
        }
    if path == "/query":
        return 200, {"rows": [[1196535, 9175, 146970, 118864]]}
    raise AssertionError((method, path, payload))


def _run(monkeypatch: pytest.MonkeyPatch) -> dict:
    monkeypatch.setattr(smoke, "_request", _successful_request)
    monkeypatch.setattr(
        smoke,
        "_process_identity",
        lambda process_id, production_root, deployment_id, release_bundle=None: (
            True,
            {"process_id": process_id, "warehouse_open": True},
        ),
    )
    return smoke.run_smoke(
        base_url="http://127.0.0.1:8080",
        deployment_id="deployment-20260721T120000Z-abcdef1234",
        api_key="secret-not-for-evidence",
        expected_core_providers=1196535,
        expected_hospital_affiliations=146970,
        expected_affiliated_providers=118864,
        expected_raw_hospital_enrollments=9175,
        representative_npi="1003005257",
        process_id=123,
        production_root=Path("/srv/cms-data-platform/production"),
    )


def test_smoke_validates_required_contracts_and_exact_counts(monkeypatch: pytest.MonkeyPatch):
    evidence = _run(monkeypatch)

    assert evidence["state"] == "passed"
    assert all(check["state"] == "passed" for check in evidence["checks"])
    assert "secret-not-for-evidence" not in json.dumps(evidence)
    assert {check["name"] for check in evidence["checks"]} >= {
        "process_identity",
        "practice_capabilities",
        "provider_profile",
        "industry_detail",
        "research",
        "clinical_trials",
        "warehouse_counts",
    }


def test_smoke_requires_exact_aact_snapshot_and_study_count(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(smoke, "_request", _successful_request)
    monkeypatch.setattr(
        smoke,
        "_process_identity",
        lambda *args, **kwargs: (True, {"warehouse_open": True}),
    )

    evidence = smoke.run_smoke(
        base_url="http://127.0.0.1:8080",
        deployment_id="deployment-20260721T120000Z-abcdef1234",
        api_key="secret-not-for-evidence",
        expected_core_providers=1196535,
        expected_hospital_affiliations=146970,
        expected_affiliated_providers=118864,
        expected_raw_hospital_enrollments=9175,
        expected_aact_study_count=600001,
        expected_aact_snapshot_date="2026-07-21",
        representative_npi="1003005257",
        process_id=123,
        production_root=Path("/srv/cms-data-platform/production"),
    )

    assert evidence["state"] == "failed"
    clinical = next(
        check for check in evidence["checks"] if check["name"] == "clinical_trials"
    )
    assert clinical["state"] == "failed"
    assert clinical["summary"]["study_count"] == 600000
    assert clinical["summary"]["expected_study_count"] == 600001


def test_runtime_identity_accepts_selected_bundle_path_without_prefix_collisions():
    selector_runtime = Path("/srv/cms-data-platform/production/release-current/runtime")

    assert smoke._references_runtime(
        [str(selector_runtime / "bin/python"), "-m", "uvicorn"],
        [selector_runtime],
    )
    assert not smoke._references_runtime(
        [str(selector_runtime) + "-unrelated/bin/python"],
        [selector_runtime],
    )


def test_smoke_fails_on_exact_count_change(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(smoke, "_request", _successful_request)
    monkeypatch.setattr(
        smoke,
        "_process_identity",
        lambda *args, **kwargs: (True, {"warehouse_open": True}),
    )
    evidence = smoke.run_smoke(
        base_url="http://127.0.0.1:8080",
        deployment_id="deployment-20260721T120000Z-abcdef1234",
        api_key="secret-not-for-evidence",
        expected_core_providers=1196535,
        expected_hospital_affiliations=1,
        expected_affiliated_providers=118864,
        expected_raw_hospital_enrollments=9175,
        representative_npi="1003005257",
        process_id=123,
        production_root=Path("/srv/cms-data-platform/production"),
    )

    assert evidence["state"] == "failed"
    assert "warehouse_counts" in evidence["error_summary"]


def test_smoke_validates_release_wide_exact_table_counts(
    monkeypatch: pytest.MonkeyPatch,
):
    def request(base_url, method, path, api_key, payload=None):
        if path == "/tables":
            return 200, {
                "tables": [
                    {"name": "core_providers", "approx_rows": 1196535},
                    {"name": "hospital_affiliations", "approx_rows": 146970},
                    {"name": "raw_hospital_enrollments", "approx_rows": 9175},
                    {"name": "raw_nppes", "approx_rows": 2},
                    {"name": "raw_open_payments_general", "approx_rows": 1},
                ]
            }
        if path == "/query":
            assert "raw_nppes" in payload["sql"]
            assert "raw_open_payments_general" in payload["sql"]
            return 200, {"rows": [[1196535, 9175, 146970, 2, 1, 118864]]}
        return _successful_request(base_url, method, path, api_key, payload)

    monkeypatch.setattr(smoke, "_request", request)
    monkeypatch.setattr(
        smoke,
        "_process_identity",
        lambda *args, **kwargs: (True, {"warehouse_open": True}),
    )
    evidence = smoke.run_smoke(
        base_url="http://127.0.0.1:8080",
        deployment_id="deployment-20260721T120000Z-abcdef1234",
        api_key="secret-not-for-evidence",
        expected_core_providers=1196535,
        expected_hospital_affiliations=146970,
        expected_affiliated_providers=118864,
        expected_raw_hospital_enrollments=9175,
        expected_table_counts={
            "core_providers": 1196535,
            "raw_nppes": 2,
            "raw_open_payments_general": 1,
        },
        representative_npi="1003005257",
        process_id=123,
        production_root=Path("/srv/cms-data-platform/production"),
    )

    assert evidence["state"] == "passed"
    counts = next(
        check for check in evidence["checks"] if check["name"] == "warehouse_counts"
    )
    assert counts["summary"]["raw_nppes"] == 2


def test_release_manifest_table_counts_are_loaded_for_smoke(tmp_path: Path):
    release = tmp_path / "release.json"
    release.write_text(
        json.dumps(
            {
                "release": {
                    "validation_details": {
                        "smoke_table_counts": {
                            "raw_nppes": 10,
                            "raw_open_payments_general": 20,
                        }
                    }
                }
            }
        )
    )

    assert smoke._load_expected_table_counts(release) == {
        "raw_nppes": 10,
        "raw_open_payments_general": 20,
    }


def test_smoke_can_verify_known_rollback_detail_absence(monkeypatch: pytest.MonkeyPatch):
    def rollback_request(base_url, method, path, api_key, payload=None):
        if path.startswith("/industry/") and path.endswith("/detail"):
            return 404, {"detail": "Not Found"}
        return _successful_request(base_url, method, path, api_key, payload)

    monkeypatch.setattr(smoke, "_request", rollback_request)
    monkeypatch.setattr(
        smoke,
        "_process_identity",
        lambda *args, **kwargs: (True, {"warehouse_open": True}),
    )
    evidence = smoke.run_smoke(
        base_url="http://127.0.0.1:8080",
        deployment_id="legacy-20260721T120000Z-abcdef1234",
        api_key="secret-not-for-evidence",
        expected_core_providers=1196535,
        expected_hospital_affiliations=146970,
        expected_affiliated_providers=118864,
        expected_raw_hospital_enrollments=9175,
        expected_industry_detail_status=404,
        representative_npi="1003005257",
        process_id=123,
        production_root=Path("/srv/cms-data-platform/production"),
    )

    detail = next(check for check in evidence["checks"] if check["name"] == "industry_detail")
    assert detail["state"] == "passed"
    assert detail["summary"]["expected_status"] == 404


def test_smoke_rejects_non_loopback_before_sending_api_key(monkeypatch: pytest.MonkeyPatch):
    called = False

    def request(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("request must not be sent")

    monkeypatch.setattr(smoke, "_request", request)
    with pytest.raises(ValueError, match="loopback"):
        smoke.run_smoke(
            base_url="https://example.test:8080",
            deployment_id="deployment-20260721T120000Z-abcdef1234",
            api_key="secret",
            expected_core_providers=1,
            expected_hospital_affiliations=1,
            expected_affiliated_providers=1,
            expected_raw_hospital_enrollments=1,
            representative_npi="1003005257",
            process_id=123,
            production_root=Path("/srv/cms-data-platform/production"),
        )
    assert called is False


def test_smoke_error_replaces_old_passing_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    output = tmp_path / "evidence" / "smoke.json"
    output.parent.mkdir()
    output.write_text(json.dumps({"state": "passed"}))
    monkeypatch.setenv("CMS_API_KEY", "secret")
    monkeypatch.setattr(smoke, "_request", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("down")))

    exit_code = smoke.main(
        [
            "--deployment-id",
            "deployment-20260721T120000Z-abcdef1234",
            "--production-root",
            str(tmp_path / "production"),
            "--process-id",
            "123",
            "--expected-core-providers",
            "1",
            "--expected-hospital-affiliations",
            "1",
            "--expected-affiliated-providers",
            "1",
            "--expected-raw-hospital-enrollments",
            "1",
            "--expected-aact-study-count",
            "600000",
            "--expected-aact-snapshot-date",
            "2026-07-20",
            "--output",
            str(output),
        ]
    )

    assert exit_code == 2
    assert json.loads(output.read_text())["state"] == "failed"
    assert stat.S_IMODE(output.stat().st_mode) == 0o440


def test_failed_candidate_smoke_automatically_restarts_and_verifies_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    candidate_id = "deployment-20260721T120000Z-abcdef1234"
    rollback_id = "legacy-20260721T110000Z-fedcba4321"
    calls: list[str] = []

    def activate(root, deployment_id, dry_run=False):
        calls.append("activate-dry" if dry_run else "activate")
        return SimpleNamespace(deployment_id=candidate_id)

    monkeypatch.setattr(cutover.os, "geteuid", lambda: 0)
    monkeypatch.setattr(cutover.manager, "activate_release", activate)
    monkeypatch.setattr(
        cutover.manager,
        "rollback_release",
        lambda root: calls.append("rollback") or SimpleNamespace(deployment_id=rollback_id),
    )
    monkeypatch.setattr(
        cutover.manager,
        "verify_release",
        lambda root, deployment_id, evidence: calls.append(f"verify:{deployment_id}"),
    )
    monkeypatch.setattr(
        cutover,
        "_restart_service",
        lambda service: calls.append("restart") or (100 + calls.count("restart")),
    )
    monkeypatch.setattr(cutover, "_wait_ready", lambda base_url: calls.append("ready"))

    def record(**kwargs):
        deployment_id = kwargs["deployment_id"]
        calls.append(f"smoke:{deployment_id}")
        state = "failed" if deployment_id == candidate_id else "passed"
        return (
            {"state": state, "error_summary": "candidate failed" if state == "failed" else None},
            tmp_path / deployment_id / "smoke.json",
        )

    monkeypatch.setattr(cutover, "_run_smoke_and_record", record)
    counts = cutover.ExpectedCounts(1, 2, 3, 4)
    result = cutover.execute_cutover(
        production_root=tmp_path / "production",
        deployment_id=candidate_id,
        service="cms-api.service",
        base_url="http://127.0.0.1:8080",
        api_key="secret",
        candidate_counts=counts,
        rollback_counts=counts,
    )

    assert result["state"] == "rolled_back"
    assert result["selected_deployment_id"] == rollback_id
    assert calls == [
        "activate-dry",
        "activate",
        "restart",
        "ready",
        f"smoke:{candidate_id}",
        "rollback",
        "restart",
        "ready",
        f"smoke:{rollback_id}",
        f"verify:{rollback_id}",
    ]
