import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import duckdb

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline.manifests import ManifestDocument, ManifestStore, RunManifest, ValidationState
from pipeline.radar_reconciliation import (
    reconcile_radar_candidate,
    select_reconciliation_runs,
)
from pipeline.releases import (
    ReleaseError,
    build_radar_warehouse_release,
    sha256_file,
)
from pipeline.source_registry import SOURCE_REGISTRY
from test_archive_sources import _nppes_csv, _stage_archive


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
        _manifest("nppes_monthly_v2", "monthly-run", "2026-07-01")
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
    assert selected == ("monthly-run", "weekly-one", "weekly-two")


def test_reconciliation_fails_closed_when_a_platform_source_is_missing(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    rows = [
        row
        for row in _platform_manifests()
        if row.source_id != "nppes_monthly_v2"
    ]
    ManifestStore(data_root / "manifests.json").save(
        ManifestDocument(manifests=rows)
    )

    with pytest.raises(ReleaseError, match="nppes_monthly_v2"):
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
        assert kwargs["monthly_run_id"] == "monthly-run"
        assert kwargs["weekly_run_ids"] == ("weekly-one", "weekly-two")
        calls.append("build")
        return SimpleNamespace(
            release=SimpleNamespace(warehouse_release_id="radar-candidate")
        )

    def fake_compare(**kwargs):
        assert kwargs["warehouse_release_id"] == "radar-candidate"
        calls.append("compare")
        return {"state": "passed"}

    monkeypatch.setattr(
        radar_reconciliation, "build_radar_warehouse_release", fake_build
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


def test_targeted_radar_release_preserves_baseline_and_installs_two_weeklies(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    monthly = _stage_archive(
        data_root,
        "nppes_monthly_v2",
        "monthly-run",
        "2026-07-01",
        {
            "npidata_pfile_20050523-20260630.csv": _nppes_csv(
                [{"NPI": "1111111111", "Provider Last Name (Legal Name)": "Base"}]
            )
        },
    )
    weekly_one = _stage_archive(
        data_root,
        "nppes_weekly_incremental_v2",
        "weekly-one",
        "2026-07-01/2026-07-07",
        {
            "npidata_pfile_20260701-20260707.csv": _nppes_csv(
                [
                    {
                        "NPI": "2222222222",
                        "Provider Last Name (Legal Name)": "First",
                        "Provider Enumeration Date": "07/02/2026",
                    }
                ]
            )
        },
    )
    weekly_two = _stage_archive(
        data_root,
        "nppes_weekly_incremental_v2",
        "weekly-two",
        "2026-07-08/2026-07-14",
        {
            "npidata_pfile_20260708-20260714.csv": _nppes_csv(
                [
                    {
                        "NPI": "2222222222",
                        "Provider Last Name (Legal Name)": "First",
                        "Provider Business Practice Location Address Postal Code": "90002",
                        "Last Update Date": "07/10/2026",
                    }
                ]
            )
        },
    )
    backup = tmp_path / "backup" / "warehouse.duckdb"
    backup.parent.mkdir()
    connection = duckdb.connect(str(backup))
    connection.execute((REPOSITORY_ROOT / "schema/ddl.sql").read_text(encoding="utf-8"))
    connection.execute(
        """
        INSERT INTO core_providers
            (npi, last_org_name, entity_type_code, data_year)
        VALUES ('1111111111', 'BASE', 'I', 2026)
        """
    )
    connection.execute("CREATE TABLE baseline_marker (value VARCHAR)")
    connection.execute("INSERT INTO baseline_marker VALUES ('preserved')")
    connection.execute("CHECKPOINT")
    connection.close()
    baseline_digest = sha256_file(backup)
    backup_manifest = backup.parent / "backup-manifest.json"
    backup_manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "backup_path": str(backup),
                "backup_identity": {"byte_size": backup.stat().st_size},
                "sha256": baseline_digest,
                "validation": {"read_only_open": "passed"},
            }
        ),
        encoding="utf-8",
    )

    result = build_radar_warehouse_release(
        data_root=data_root,
        monthly_run_id=monthly.run_id,
        weekly_run_ids=(weekly_one.run_id, weekly_two.run_id),
        backup_manifest_path=backup_manifest,
        code_commit=CODE_COMMIT,
    )

    assert sha256_file(backup) == baseline_digest
    assert result.release.validation_state == ValidationState.PASSED
    assert result.release.validation_details["nppes"]["reconciliation"][
        "weekly_release_rows"
    ] == 2
    candidate = duckdb.connect(str(result.database_path), read_only=True)
    try:
        assert candidate.execute(
            "SELECT value FROM baseline_marker"
        ).fetchone() == ("preserved",)
        assert candidate.execute(
            "SELECT count(*) FROM nppes_radar_releases"
        ).fetchone() == (3,)
    finally:
        candidate.close()
