import json
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import production_manager as production
from pipeline import retention


def _deployment(
    platform_root: Path,
    number: int,
    state: production.DeploymentState,
    previous: str | None,
) -> production.ProductionDeployment:
    timestamp = f"2026-08-{number:02d}T12:00:00+00:00"
    deployment_id = f"deployment-202608{number:02d}T120000Z-{'a' * 9}{number}"
    artifact_root = platform_root / "production-artifacts"
    release_id = f"warehouse-202608{number:02d}T120000Z-release{number}"
    selected_at = timestamp if state != production.DeploymentState.PREPARED else None
    verified_at = timestamp if state in {
        production.DeploymentState.VERIFIED,
        production.DeploymentState.SUPERSEDED,
    } else None
    return production.ProductionDeployment(
        deployment_id=deployment_id,
        deployment_kind="legacy_baseline" if previous is None else "warehouse_release",
        state=state,
        targets=production.ReleaseTargets(
            code=str(artifact_root / "code" / f"commit-{number}"),
            warehouse=str(
                artifact_root / "warehouses" / release_id / "warehouse.duckdb"
            ),
            runtime=str(artifact_root / "runtimes" / f"runtime-{number}"),
        ),
        artifact_root=str(artifact_root),
        warehouse_sha256=str(number) * 64,
        warehouse_byte_size=1024,
        code_fingerprint=f"sha256:{str(number) * 64}",
        runtime_fingerprint=f"sha256:{str(number) * 64}",
        prepared_at=timestamp,
        code_commit=str(number) * 40,
        warehouse_release_id=release_id,
        warehouse_pipeline_commit=str(number) * 40,
        previous_deployment_id=previous,
        selected_at=selected_at,
        verified_at=verified_at,
        superseded_at=(timestamp if state == production.DeploymentState.SUPERSEDED else None),
        verification_summary=({"state": "passed"} if verified_at else {}),
    )


def _platform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    deployment_count: int = 4,
) -> tuple[Path, list[production.ProductionDeployment]]:
    platform_root = tmp_path.resolve()
    production_root = platform_root / "production"
    artifact_root = platform_root / "production-artifacts"
    production_root.mkdir()
    artifact_root.mkdir()
    deployments: list[production.ProductionDeployment] = []
    previous = None
    for number in range(1, deployment_count + 1):
        state = (
            production.DeploymentState.VERIFIED
            if number == deployment_count
            else production.DeploymentState.SUPERSEDED
        )
        item = _deployment(platform_root, number, state, previous)
        deployments.append(item)
        previous = item.deployment_id
        for target in item.targets.to_bundle_map().values():
            path = Path(target)
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"x" * number)
            else:
                path.mkdir(parents=True, exist_ok=True)
        bundle = production_root / "releases" / item.deployment_id
        bundle.mkdir(parents=True)
        for name, target in item.targets.to_bundle_map().items():
            (bundle / name).symlink_to(target)
        evidence = production_root / "evidence" / item.deployment_id / "smoke.json"
        evidence.parent.mkdir(parents=True)
        evidence.write_text("{}")
        release = platform_root / "data" / "releases" / item.warehouse_release_id
        release.mkdir(parents=True)
        (release / "warehouse.duckdb").write_bytes(b"staging")
    orphan = artifact_root / "warehouses" / "warehouse-orphan"
    orphan.mkdir(parents=True)
    (orphan / "warehouse.duckdb").write_bytes(b"orphan")
    backup = platform_root / "backups" / "backup-1"
    backup.mkdir(parents=True)
    (backup / "warehouse.duckdb").write_bytes(b"backup")
    refresh = platform_root / "refresh-20260801"
    refresh.mkdir()
    (refresh / "scratch.bin").write_bytes(b"scratch")
    ledger = {
        "schema_version": 1,
        "selected_deployment_id": deployments[-1].deployment_id,
        "deployments": [item.to_dict() for item in deployments],
    }
    (production_root / "deployments.json").write_text(json.dumps(ledger))
    (production_root / production.RELEASE_POINTER).symlink_to(
        production_root / "releases" / deployments[-1].deployment_id
    )
    monkeypatch.setattr(production, "_require_control_ownership", lambda path: None)
    return platform_root, deployments


def test_preview_protects_active_and_two_predecessors_and_names_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    platform_root, deployments = _platform(tmp_path, monkeypatch)

    report = retention.build_retention_preview(platform_root)

    assert report["read_only"] is True
    assert report["rollback_policy"]["floor_met"] is True
    assert report["rollback_policy"]["protected_deployment_ids"] == [
        deployments[3].deployment_id,
        deployments[2].deployment_id,
        deployments[1].deployment_id,
    ]
    items = {item["path"]: item for item in report["items"]}
    old_warehouse = Path(deployments[0].targets.warehouse).parent
    active_warehouse = Path(deployments[3].targets.warehouse).parent
    orphan = platform_root / "production-artifacts" / "warehouses" / "warehouse-orphan"
    backup = platform_root / "backups" / "backup-1"
    refresh = platform_root / "refresh-20260801"
    assert items[str(old_warehouse)]["disposition"] == "review_candidate"
    assert items[str(active_warehouse)]["disposition"] == "protected"
    assert items[str(orphan)]["disposition"] == "review_candidate"
    assert items[str(backup)]["disposition"] == "protected"
    assert items[str(refresh)]["disposition"] == "review_candidate"
    assert report["summary"]["confirmed_reclaimable_bytes"] == 0


def test_preview_blocks_artifact_candidates_when_rollback_floor_is_not_met(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    platform_root, _ = _platform(tmp_path, monkeypatch, deployment_count=2)

    report = retention.build_retention_preview(platform_root)

    assert report["rollback_policy"]["floor_met"] is False
    assert report["promotion_capacity_gate"]["allowed"] is False
    artifact_items = [
        item
        for item in report["items"]
        if item["path"].startswith(str(platform_root / "production-artifacts"))
    ]
    assert artifact_items
    assert all(item["disposition"] in {"protected", "blocked"} for item in artifact_items)


def test_preview_rejects_unsafe_threshold_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    platform_root, _ = _platform(tmp_path, monkeypatch)

    with pytest.raises(retention.RetentionError, match="disk thresholds"):
        retention.build_retention_preview(
            platform_root,
            warning_percent=80,
            critical_percent=70,
        )


def test_preview_fails_closed_when_selected_pointer_does_not_match_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    platform_root, deployments = _platform(tmp_path, monkeypatch)
    pointer = platform_root / "production" / production.RELEASE_POINTER
    pointer.unlink()
    pointer.symlink_to(
        platform_root / "production" / "releases" / deployments[0].deployment_id
    )

    with pytest.raises(retention.RetentionError, match="release-current"):
        retention.build_retention_preview(platform_root)
