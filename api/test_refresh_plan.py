import json
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.manifests import ManifestDocument, RunManifest, ValidationState
from pipeline.refresh_plan import (
    RefreshPlanError,
    build_refresh_plan,
    main,
    parse_exceptions,
)
from pipeline.source_registry import SOURCE_REGISTRY


STALE_SOURCE_IDS = {
    "aact_clinical_trials_snapshot",
    "cms_dme_by_referring_provider",
    "cms_hospital_enrollments",
    "cms_order_and_referring",
    "cms_part_d_by_provider",
    "cms_pecos_practice_location",
    "cms_pecos_public_provider_enrollment",
    "cms_pecos_reassignment",
    "cms_revalidation_group_reassignment",
    "nppes_monthly_v2",
    "nppes_weekly_incremental_v2",
}


def _period(source_id: str) -> str:
    if source_id == "nppes_monthly_v2":
        return "2026-08-10"
    if source_id == "nppes_weekly_incremental_v2":
        return "2026-08-03/2026-08-09"
    if source_id == "aact_clinical_trials_snapshot":
        return "2026-08-13"
    return "2026-01-01/2026-12-31"


def _status() -> dict:
    sources = []
    for source_id in sorted(SOURCE_REGISTRY):
        stale = source_id in STALE_SOURCE_IDS
        sources.append(
            {
                "source_id": source_id,
                "freshness_status": "stale" if stale else "current",
                "latest_publisher_version": f"latest-{source_id}",
                "installed_version": (
                    f"installed-{source_id}" if stale else f"latest-{source_id}"
                ),
                "source_data_period": _period(source_id),
                "installed_source_data_period": "2025-01-01/2025-12-31",
            }
        )
    return {
        "schema_version": 1,
        "generated_at": "2026-08-13T06:22:10+00:00",
        "production": {"selected_deployment_id": "deployment-current"},
        "sources": sources,
    }


def _manifest(source_id: str, publisher_version: str) -> RunManifest:
    return RunManifest(
        run_id=f"run-{source_id}-{publisher_version[:8]}",
        release_id=f"release-{source_id}-{publisher_version[:8]}",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version=publisher_version,
        source_data_period=_period(source_id),
        discovery_timestamp="2026-08-13T06:22:10+00:00",
        retrieval_timestamp="2026-08-13T07:00:00+00:00",
        validation_state=ValidationState.PASSED,
        validation_timestamp="2026-08-13T07:10:00+00:00",
    )


def _all_latest_manifests() -> ManifestDocument:
    return ManifestDocument(
        manifests=[
            _manifest(source_id, f"latest-{source_id}")
            for source_id in sorted(SOURCE_REGISTRY)
        ]
    )


def test_live_stale_shape_chooses_three_independent_candidate_lanes() -> None:
    plan = build_refresh_plan(_status(), ManifestDocument())

    assert plan["read_only"] is True
    assert plan["summary"]["stale"] == 11
    assert plan["summary"]["acquire"] == 10
    assert plan["summary"]["candidate_input_restore"] == 5
    assert plan["summary"]["covered"] == 1
    assert "nppes_monthly_v2" in plan["acquisition_order"]
    assert "nppes_weekly_incremental_v2" not in plan["acquisition_order"]
    lanes = {lane["lane_id"]: lane for lane in plan["candidate_lanes"]}
    assert set(lanes) == {"aact_postgres", "nppes_radar", "full_cms"}
    assert set(lanes["full_cms"]["required_source_ids"]) == {
        source_id for source_id in SOURCE_REGISTRY if source_id.startswith("cms_")
    }
    assert all(lane["state"] == "awaiting_validated_runs" for lane in lanes.values())
    assert set(plan["candidate_input_restore_ids"]) == {
        "cms_dac_national",
        "cms_part_d_by_provider_and_drug",
        "cms_physician_by_provider",
        "cms_physician_by_provider_and_service",
        "cms_qpp_experience",
    }
    assert plan["promotion"] == "manual_approval_required"


def test_validated_runs_make_lanes_ready_and_monthly_supersedes_old_weekly() -> None:
    plan = build_refresh_plan(_status(), _all_latest_manifests())

    assert plan["summary"]["acquire"] == 0
    assert plan["summary"]["candidate_input_restore"] == 0
    assert plan["summary"]["staged"] == 10
    assert plan["summary"]["covered"] == 1
    lanes = {lane["lane_id"]: lane for lane in plan["candidate_lanes"]}
    assert all(lane["state"] == "ready" for lane in lanes.values())
    assert lanes["nppes_radar"]["selected_run_ids"] == (
        "run-nppes_monthly_v2-latest-n",
    )
    assert lanes["nppes_radar"]["missing_source_ids"] == ()


def test_exception_requires_a_reason_and_reuses_installed_evidence() -> None:
    status = _status()
    source_id = "cms_dme_by_referring_provider"
    manifests = _all_latest_manifests()
    manifests.manifests.append(_manifest(source_id, f"installed-{source_id}"))

    plan = build_refresh_plan(
        status,
        manifests,
        exceptions={source_id: "Publisher correction requires clinical review."},
    )

    action = next(item for item in plan["sources"] if item["source_id"] == source_id)
    assert action["action"] == "exception"
    assert plan["exceptions"][source_id].startswith("Publisher correction")
    assert plan["summary"]["exceptions"] == 1

    with pytest.raises(RefreshPlanError, match="non-empty reason"):
        parse_exceptions([f"{source_id}="])


def test_exception_without_installed_staging_evidence_blocks_the_plan() -> None:
    source_id = "cms_dme_by_referring_provider"

    plan = build_refresh_plan(
        _status(),
        ManifestDocument(),
        exceptions={source_id: "Awaiting clinical review."},
    )

    action = next(item for item in plan["sources"] if item["source_id"] == source_id)
    assert action["action"] == "blocked"
    assert source_id in plan["blockers"]
    assert plan["planning_state"] == "blocked"


def test_unknown_source_blocks_the_plan() -> None:
    status = _status()
    status["sources"][0]["freshness_status"] = "unknown"

    plan = build_refresh_plan(status, ManifestDocument())

    assert plan["planning_state"] == "blocked"
    assert plan["blockers"] == [status["sources"][0]["source_id"]]


def test_cli_is_read_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status_path = tmp_path / "status.json"
    manifest_path = tmp_path / "manifests.json"
    status_path.write_text(json.dumps(_status()), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(_all_latest_manifests().to_dict()), encoding="utf-8"
    )
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (status_path, manifest_path)
    }

    exit_code = main(
        [
            "--status-json",
            str(status_path),
            "--staging-manifest",
            str(manifest_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["planning_state"] == "actionable"
    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (status_path, manifest_path)
    } == before
