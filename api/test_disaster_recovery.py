import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import disaster_recovery as recovery
from pipeline.production_manager import REQUIRED_VERIFICATION_CHECKS


DEPLOYMENT_ID = "deployment-20260811T155814Z-6baa26aa69"
WAREHOUSE_RELEASE_ID = "warehouse-20260811T021837Z-f44c147e30"
REPRESENTATIVE_NPI = "1003005257"


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "off-host-bundle"
    bundle.mkdir()
    warehouse = bundle / "warehouse" / "warehouse.duckdb"
    warehouse.parent.mkdir()
    connection = duckdb.connect(str(warehouse))
    connection.execute("CREATE TABLE core_providers (npi VARCHAR PRIMARY KEY, name VARCHAR)")
    connection.execute(
        "INSERT INTO core_providers VALUES (?, 'Representative')",
        [REPRESENTATIVE_NPI],
    )
    connection.execute("CREATE TABLE hospital_affiliations (npi VARCHAR)")
    connection.execute("INSERT INTO hospital_affiliations VALUES (?)", [REPRESENTATIVE_NPI])
    connection.execute("CHECKPOINT")
    connection.close()
    warehouse_sha = recovery.sha256_file(warehouse)
    warehouse_size = warehouse.stat().st_size

    deployments = bundle / "control" / "deployments.json"
    _write_json(
        deployments,
        {
            "schema_version": 1,
            "selected_deployment_id": DEPLOYMENT_ID,
            "deployments": [
                {
                    "deployment_id": DEPLOYMENT_ID,
                    "warehouse_release_id": WAREHOUSE_RELEASE_ID,
                    "warehouse_sha256": warehouse_sha,
                    "warehouse_byte_size": warehouse_size,
                }
            ],
        },
    )
    warehouse_release = bundle / "evidence" / "warehouse-release.json"
    _write_json(
        warehouse_release,
        {
            "schema_version": 2,
            "release": {
                "warehouse_release_id": WAREHOUSE_RELEASE_ID,
                "sha256": warehouse_sha,
                "byte_size": warehouse_size,
            },
        },
    )
    source_manifests = bundle / "evidence" / "source-manifests.json"
    _write_json(
        source_manifests,
        {
            "schema_version": 1,
            "deployment_id": DEPLOYMENT_ID,
            "warehouse_release_id": WAREHOUSE_RELEASE_ID,
            "manifests": [],
        },
    )
    files = []
    for role, path in (
        ("warehouse", warehouse),
        ("deployments", deployments),
        ("warehouse_release", warehouse_release),
        ("source_manifests", source_manifests),
    ):
        files.append(
            {
                "role": role,
                "path": path.relative_to(bundle).as_posix(),
                "sha256": recovery.sha256_file(path),
                "byte_size": path.stat().st_size,
            }
        )
    _write_json(
        bundle / recovery.MANIFEST_NAME,
        {
            "schema_version": 1,
            "backup_id": "cms-prod-20260813",
            "created_at": "2026-08-13T08:00:00+00:00",
            "deployment_id": DEPLOYMENT_ID,
            "warehouse_release_id": WAREHOUSE_RELEASE_ID,
            "warehouse_sha256": warehouse_sha,
            "warehouse_byte_size": warehouse_size,
            "retention": {
                "approved_copy_count": 3,
                "location": "approved-off-host-store",
                "owner": "CMS data-platform operator",
                "approved_at": "2026-08-13T08:00:00+00:00",
                "next_drill_date": "2026-11-13",
            },
            "control_plane_coverage": {
                "kind": "both",
                "reference": "provider-snapshot-123 plus control/deployments.json",
                "captured_at": "2026-08-13T08:00:00+00:00",
            },
            "files": files,
            "representative_table_counts": {
                "core_providers": 1,
                "hospital_affiliations": 1,
            },
            "representative_npi": REPRESENTATIVE_NPI,
            "failure_modes_exercised": ["checksum_mismatch", "stale_smoke"],
        },
    )
    return bundle


def _smoke(path: Path, generated_at: datetime) -> Path:
    _write_json(
        path,
        {
            "schema_version": 1,
            "deployment_id": DEPLOYMENT_ID,
            "state": "passed",
            "generated_at": generated_at.isoformat(),
            "checks": [
                {"name": name, "state": "passed"}
                for name in sorted(REQUIRED_VERIFICATION_CHECKS)
            ],
        },
    )
    return path


def test_materialize_and_verify_isolated_restore(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    restore = tmp_path / "isolated-restore"
    started = datetime.now(timezone.utc) - timedelta(seconds=2)

    materialized = recovery.materialize_restore(bundle, restore)
    smoke = _smoke(tmp_path / "restored-smoke.json", datetime.now(timezone.utc))
    result = recovery.verify_restore(restore, smoke, restored_after=started)

    assert materialized["state"] == "materialized"
    assert materialized["materialized_at"].endswith("+00:00")
    assert materialized["application_smoke"].startswith("required")
    assert result["state"] == "passed"
    assert result["deployment_id"] == DEPLOYMENT_ID
    assert result["checks"]["duckdb_read_only_open"] == "passed"
    assert result["checks"]["representative_table_counts"] == {
        "core_providers": 1,
        "hospital_affiliations": 1,
    }
    assert result["retention"]["approved_copy_count"] == 3


def test_materialize_refuses_an_existing_restore_target(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    restore = tmp_path / "existing"
    restore.mkdir()

    with pytest.raises(recovery.DisasterRecoveryError, match="must not already exist"):
        recovery.materialize_restore(bundle, restore)


def test_materialize_detects_source_corruption_before_copy(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    warehouse = bundle / "warehouse" / "warehouse.duckdb"
    warehouse.write_bytes(warehouse.read_bytes() + b"corrupt")

    with pytest.raises(recovery.DisasterRecoveryError, match="byte size"):
        recovery.materialize_restore(bundle, tmp_path / "restore")


def test_verify_rejects_smoke_that_predates_restore(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    restore = tmp_path / "restore"
    recovery.materialize_restore(bundle, restore)
    started = datetime.now(timezone.utc)
    smoke = _smoke(tmp_path / "stale-smoke.json", started - timedelta(seconds=1))

    with pytest.raises(recovery.DisasterRecoveryError, match="predates"):
        recovery.verify_restore(restore, smoke, restored_after=started)


def test_verify_requires_every_canonical_smoke_check(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    restore = tmp_path / "restore"
    recovery.materialize_restore(bundle, restore)
    started = datetime.now(timezone.utc) - timedelta(seconds=1)
    smoke = _smoke(tmp_path / "incomplete-smoke.json", datetime.now(timezone.utc))
    payload = json.loads(smoke.read_text())
    payload["checks"] = payload["checks"][1:]
    _write_json(smoke, payload)

    with pytest.raises(recovery.DisasterRecoveryError, match="lacks passed checks"):
        recovery.verify_restore(restore, smoke, restored_after=started)


def test_manifest_rejects_traversal_path(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    manifest_path = bundle / recovery.MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["path"] = "../warehouse.duckdb"
    _write_json(manifest_path, manifest)

    with pytest.raises(recovery.DisasterRecoveryError, match="confined"):
        recovery.materialize_restore(bundle, tmp_path / "restore")
