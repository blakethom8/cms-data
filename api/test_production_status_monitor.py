import json
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import production_status_monitor as monitor
from pipeline.data_platform import EXIT_DISCOVERY_FAILURE, EXIT_STALE_OR_UNKNOWN
from pipeline.source_registry import SOURCE_REGISTRY


FIXTURES = REPOSITORY_ROOT / "pipeline" / "fixtures" / "publisher_metadata"


def _healthy_status(deployment_id: str) -> dict:
    return {
        "healthy": True,
        "selected_deployment_id": deployment_id,
        "selected_code_commit": "a" * 40,
        "selected_warehouse_release_id": "warehouse-20990101T000000Z-abcdef",
        "last_verified_at": "2099-01-01T00:00:00+00:00",
    }


def test_monitor_uses_only_selected_deployment_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    selected = "deployment-20990101T000000Z-aaaaaaaaaa"
    stale = "deployment-20980101T000000Z-bbbbbbbbbb"
    monkeypatch.setattr(monitor, "production_status", lambda root: _healthy_status(selected))
    stale_path = monitor.selected_manifest_path(tmp_path, stale)
    stale_path.parent.mkdir(parents=True)
    stale_path.write_text(json.dumps({"schema_version": 1, "manifests": []}))

    exit_code = monitor.main(
        ["--production-root", str(tmp_path), "--offline", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_STALE_OR_UNKNOWN
    assert payload["production"]["selected_deployment_id"] == selected
    assert payload["production"]["source_manifest_present"] is False
    assert payload["manifest_path"].endswith(f"{selected}/source-manifests.json")
    assert payload["summary"]["unknown"] == len(SOURCE_REGISTRY)


def test_unhealthy_control_plane_fails_without_publisher_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        monitor,
        "production_status",
        lambda root: {"healthy": False, "selected_deployment_id": "candidate"},
    )
    monkeypatch.setattr(
        monitor,
        "discover_all",
        lambda **kwargs: pytest.fail("publisher discovery must not run"),
    )

    exit_code = monitor.main(
        ["--production-root", str(tmp_path), "--offline", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_DISCOVERY_FAILURE
    assert "not healthy" in payload["error"]


def test_malformed_selected_manifest_is_discovery_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    selected = "deployment-20990101T000000Z-aaaaaaaaaa"
    monkeypatch.setattr(monitor, "production_status", lambda root: _healthy_status(selected))
    manifest_path = monitor.selected_manifest_path(tmp_path, selected)
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("not json")

    exit_code = monitor.main(
        ["--production-root", str(tmp_path), "--offline", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == EXIT_DISCOVERY_FAILURE
    assert "Invalid manifest JSON" in payload["error"]


def test_systemd_monitor_is_read_only_and_scheduled_daily() -> None:
    service = (REPOSITORY_ROOT / "deploy/systemd/cms-data-status.service").read_text()
    timer = (REPOSITORY_ROOT / "deploy/systemd/cms-data-status.timer").read_text()

    assert "release-current/runtime/bin/python" in service
    assert "-m pipeline.production_status_monitor" in service
    assert "--json --timeout 30" in service
    assert "PYTHONDONTWRITEBYTECODE=1" in service
    assert "ProtectSystem=strict" in service
    assert not any(
        command in service
        for command in (" acquire ", "build-release", " promote ", " rollback ")
    )
    assert "OnCalendar=*-*-* 06:15:00 UTC" in timer
    assert "RandomizedDelaySec=15m" in timer
    assert "Persistent=true" in timer
