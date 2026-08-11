import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.manifests import ManifestDocument, ManifestStore, RunManifest, ValidationState
from pipeline.radar_reconciliation import (
    reconcile_radar_candidate,
    select_reconciliation_runs,
)
from pipeline.releases import FULL_PLATFORM_WAREHOUSE_SOURCE_IDS, ReleaseError
from pipeline.source_registry import SOURCE_REGISTRY


CODE_COMMIT = "a" * 40


def _manifest(source_id: str, run_id: str, period: str) -> RunManifest:
    return RunManifest(
        run_id=run_id,
        release_id=f"{run_id}-release",
        source_id=source_id,
        publisher=SOURCE_REGISTRY[source_id].publisher.value,
        publisher_version=f"{run_id}-version",
        source_data_period=period,
        discovery_timestamp="2026-08-01T00:00:00+00:00",
        retrieval_timestamp="2026-08-01T00:01:00+00:00",
        validation_state=ValidationState.PASSED,
        validation_timestamp="2026-08-01T00:02:00+00:00",
    )


def _platform_manifests() -> list[RunManifest]:
    rows = [
        _manifest(source_id, f"{source_id}-run", "2026-07-01")
        for source_id in sorted(FULL_PLATFORM_WAREHOUSE_SOURCE_IDS)
        if source_id != "nppes_weekly_incremental_v2"
    ]
    rows.extend(
        [
            _manifest(
                "nppes_weekly_incremental_v2",
                "weekly-one",
                "2026-07-01/2026-07-07",
            ),
            _manifest(
                "nppes_weekly_incremental_v2",
                "weekly-two",
                "2026-07-08/2026-07-14",
            ),
        ]
    )
    return rows


def test_reconciliation_selects_monthly_and_all_consecutive_weeklies(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=_platform_manifests())
    )

    selected = select_reconciliation_runs(data_root)

    assert "weekly-one" in selected
    assert "weekly-two" in selected
    assert len(selected) == len(FULL_PLATFORM_WAREHOUSE_SOURCE_IDS) + 1


def test_reconciliation_fails_closed_when_a_platform_source_is_missing(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    rows = [
        row
        for row in _platform_manifests()
        if row.source_id != "open_payments_research"
    ]
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=rows)
    )

    with pytest.raises(ReleaseError, match="open_payments_research"):
        select_reconciliation_runs(data_root)


def test_reconciliation_builds_and_compares_without_promoting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pipeline import radar_reconciliation

    data_root = tmp_path / "data"
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=_platform_manifests())
    )
    backup_manifest = tmp_path / "backup.json"
    calls: list[str] = []

    def fake_build(**kwargs):
        assert kwargs["data_root"] == data_root
        assert kwargs["backup_manifest_path"] == backup_manifest
        calls.append("build")
        return SimpleNamespace(
            release=SimpleNamespace(warehouse_release_id="radar-candidate")
        )

    def fake_compare(**kwargs):
        assert kwargs["warehouse_release_id"] == "radar-candidate"
        calls.append("compare")
        return {"state": "passed"}

    monkeypatch.setattr(
        radar_reconciliation, "build_full_platform_warehouse_release", fake_build
    )
    monkeypatch.setattr(
        radar_reconciliation, "compare_warehouse_release", fake_compare
    )

    result = reconcile_radar_candidate(
        data_root=data_root,
        backup_manifest_path=backup_manifest,
        code_commit=CODE_COMMIT,
    )

    assert calls == ["build", "compare"]
    assert result["status"] == "candidate_ready"
    assert result["promoted"] is False
