import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import production_manager as production


COMMIT = "8" * 40
WAREHOUSE_COMMIT = "9" * 40
RELEASE_ID = "warehouse-20260721T120000Z-abcdef"
LEGACY_BYTES = b"legacy warehouse"
CANDIDATE_BYTES = b"candidate warehouse"
LEGACY_SHA = hashlib.sha256(LEGACY_BYTES).hexdigest()
CANDIDATE_SHA = hashlib.sha256(CANDIDATE_BYTES).hexdigest()


@pytest.fixture(autouse=True)
def _restore_test_permissions(tmp_path: Path):
    yield
    for path in sorted(tmp_path.rglob("*"), reverse=True):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o700)
        elif path.exists():
            path.chmod(0o600)


def _fingerprint(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _write_immutable(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    path.chmod(0o440)
    return path


def _paths(tmp_path: Path) -> dict[str, Path]:
    artifact_root = tmp_path / "artifacts"
    paths = {
        "production": tmp_path / "production",
        "artifacts": artifact_root,
        "data": tmp_path / "staging-data",
        "legacy_code": artifact_root / "code" / "legacy",
        "candidate_code": artifact_root / "code" / COMMIT,
        "legacy_runtime": artifact_root / "runtimes" / "legacy",
        "candidate_runtime": artifact_root / "runtimes" / "candidate",
        "legacy_db": artifact_root / "warehouses" / "legacy" / "warehouse.duckdb",
        "candidate_db": artifact_root / "warehouses" / RELEASE_ID / "warehouse.duckdb",
    }
    paths["production"].mkdir()
    paths["data"].mkdir()
    for key in ("legacy_code", "candidate_code", "legacy_runtime", "candidate_runtime"):
        paths[key].mkdir(parents=True)
    _write_immutable(paths["legacy_db"], LEGACY_BYTES)
    _write_immutable(paths["candidate_db"], CANDIDATE_BYTES)
    return paths


def _stub_platform_security(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(production, "_require_control_ownership", lambda path: None)
    monkeypatch.setattr(
        production,
        "_require_root_owned",
        lambda path, artifact_root, *, sealed_parent: None,
    )
    monkeypatch.setattr(production, "_require_root_operator", lambda: None)
    monkeypatch.setattr(
        production,
        "_inspect_legacy_code",
        lambda path, artifact_root: _fingerprint(f"legacy:{Path(path).name}"),
    )
    monkeypatch.setattr(
        production,
        "_inspect_git_code",
        lambda path, artifact_root: (COMMIT, _fingerprint(f"code:{Path(path).name}")),
    )
    monkeypatch.setattr(
        production,
        "_inspect_runtime",
        lambda path, artifact_root, expected: _fingerprint(f"runtime:{Path(path).name}"),
    )


def _write_release(
    paths: dict[str, Path],
    *,
    comparison_state: str = "passed",
    pipeline_commit: str = COMMIT,
    comparison_schema_version: int = 2,
    comparison_policy: str = "full_platform_v1",
) -> None:
    release_dir = paths["data"] / "releases" / RELEASE_ID
    staging_database = _write_immutable(
        release_dir / "warehouse.duckdb", CANDIDATE_BYTES
    )
    release = {
        "schema_version": 2,
        "release": {
            "warehouse_release_id": RELEASE_ID,
            "pipeline_code_commit": pipeline_commit,
            "database_path": f"releases/{RELEASE_ID}/warehouse.duckdb",
            "duckdb_version": "1.4.4",
            "byte_size": staging_database.stat().st_size,
            "sha256": CANDIDATE_SHA,
            "validation_state": "passed",
        },
    }
    comparison = {
        "schema_version": comparison_schema_version,
        "warehouse_release_id": RELEASE_ID,
        "pipeline_code_commit": pipeline_commit,
        "state": comparison_state,
        "failed_requirements": [],
        "unexpected_differences": [],
        "evidence_mismatches": [],
        "comparison_policy": comparison_policy,
        "candidate": {"sha256": CANDIDATE_SHA},
    }
    if comparison_policy == "serving_practice_managed_dac_v1":
        counts = {
            "raw_dac_national": 2,
            "serving_practice_provider_sites": 1,
        }
        release["release"]["table_counts"] = counts
        release["release"]["validation_details"] = {
            "comparison_policy": comparison_policy,
            "changed_tables": list(counts),
            "mart_contract_validation": {
                "passed": True,
                "marts": [
                    {
                        "table": "serving_practice_provider_sites",
                        "schema_valid": True,
                        "data_valid": True,
                        "issues": [],
                        "row_count": 1,
                        "row_validation_failures": {
                            "invalid_state_or_zip": 0,
                            "empty_specialties": 0,
                        },
                    }
                ],
            },
        }
        comparison["changed_tables"] = {
            table: {"baseline_rows": None, "candidate_rows": count}
            for table, count in counts.items()
        }
    if comparison_policy == "serving_practice_nppes_additive_v1":
        counts = {
            "serving_practice_nppes_provider_sites": 2,
            "serving_practice_nppes_org_memberships": 3,
        }
        release["release"]["table_counts"] = counts
        release["release"]["validation_details"] = {
            "comparison_policy": comparison_policy,
            "release_scope": "targeted_additive",
            "changed_tables": list(counts),
            "mart_contract_validation": {
                "passed": True,
                "registered_count": 2,
                "available_count": 2,
                "schema_valid_count": 2,
                "data_valid_count": 2,
                "serving_authorized_count": 2,
                "marts": [
                    {
                        "table": table,
                        "schema_valid": True,
                        "data_valid": True,
                        "serving_authorized": True,
                        "issues": [],
                        "row_count": count,
                        "key_columns": (
                            ["addr_key", "npi", "org_pac_id"]
                            if table.endswith("org_memberships")
                            else ["npi"]
                        ),
                        "duplicate_key_groups": 0,
                        "invalid_npis": 0,
                        "required_null_rows": 0,
                        "orphan_npis": 0 if table.endswith("org_memberships") else None,
                        "row_validation_failures": (
                            {"missing_dac_provenance": 0}
                            if table.endswith("org_memberships")
                            else {
                                "empty_specialties": 0,
                                "invalid_state_or_zip": 0,
                                "partb_value_without_provenance": 0,
                                "partd_value_without_provenance": 0,
                            }
                        ),
                    }
                    for table, count in counts.items()
                ],
            },
        }
        comparison["changed_tables"] = {
            table: {"baseline_rows": None, "candidate_rows": count}
            for table, count in counts.items()
        }
        comparison["invariant_logical_fingerprints_checked"] = 42
        comparison["unchanged_table_count"] = 42
    (release_dir / "release.json").write_text(json.dumps(release))
    (release_dir / "comparison.json").write_text(json.dumps(comparison))


@pytest.mark.parametrize(
    "comparison_policy", ("ppef_additive_v1", "nppes_radar_targeted_v1")
)
def test_prepare_accepts_targeted_comparison_policy(
    comparison_policy: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths, comparison_policy=comparison_policy)

    deployment = production.prepare_release(
        paths["production"],
        paths["artifacts"],
        paths["data"],
        paths["candidate_code"],
        paths["candidate_runtime"],
        paths["candidate_db"],
        RELEASE_ID,
    )

    assert deployment.warehouse_release_id == RELEASE_ID
    assert deployment.state == production.DeploymentState.PREPARED


def test_prepare_accepts_validated_s2_managed_dac_policy_after_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(
        paths, comparison_policy="serving_practice_managed_dac_v1"
    )

    deployment = production.prepare_release(
        paths["production"],
        paths["artifacts"],
        paths["data"],
        paths["candidate_code"],
        paths["candidate_runtime"],
        paths["candidate_db"],
        RELEASE_ID,
    )

    assert deployment.warehouse_release_id == RELEASE_ID
    assert deployment.state == production.DeploymentState.PREPARED


def test_prepare_accepts_validated_s2_nppes_practice_policy_after_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(
        paths, comparison_policy="serving_practice_nppes_additive_v1"
    )

    deployment = production.prepare_release(
        paths["production"],
        paths["artifacts"],
        paths["data"],
        paths["candidate_code"],
        paths["candidate_runtime"],
        paths["candidate_db"],
        RELEASE_ID,
    )

    assert deployment.warehouse_release_id == RELEASE_ID
    assert deployment.state == production.DeploymentState.PREPARED


def test_prepare_rejects_provider_profile_policy_before_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(
        paths, comparison_policy="serving_provider_profile_core_additive_v1"
    )

    with pytest.raises(
        production.ProductionError,
        match="Warehouse comparison has an unsupported policy",
    ):
        production.prepare_release(
            paths["production"],
            paths["artifacts"],
            paths["data"],
            paths["candidate_code"],
            paths["candidate_runtime"],
            paths["candidate_db"],
            RELEASE_ID,
        )


@pytest.mark.parametrize(
    "invalid_field",
    ("comparison_changed_tables", "release_changed_tables", "mart_validation"),
)
def test_prepare_rejects_incomplete_s2_managed_dac_authorization_evidence(
    invalid_field: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths, comparison_policy="serving_practice_managed_dac_v1")
    release_path = paths["data"] / "releases" / RELEASE_ID / "release.json"
    comparison_path = paths["data"] / "releases" / RELEASE_ID / "comparison.json"
    release = json.loads(release_path.read_text())
    comparison = json.loads(comparison_path.read_text())
    if invalid_field == "comparison_changed_tables":
        comparison["changed_tables"].pop("raw_dac_national")
        expected = "changed-table scope"
    elif invalid_field == "release_changed_tables":
        release["release"]["validation_details"]["changed_tables"] = [
            "serving_practice_provider_sites"
        ]
        expected = "release validation scope"
    else:
        release["release"]["validation_details"]["mart_contract_validation"][
            "passed"
        ] = False
        expected = "serving-mart validation"
    release_path.write_text(json.dumps(release))
    comparison_path.write_text(json.dumps(comparison))

    with pytest.raises(production.ProductionError, match=expected):
        production.prepare_release(
            paths["production"],
            paths["artifacts"],
            paths["data"],
            paths["candidate_code"],
            paths["candidate_runtime"],
            paths["candidate_db"],
            RELEASE_ID,
        )


@pytest.mark.parametrize(
    "invalid_field",
    (
        "comparison_changed_tables",
        "release_changed_tables",
        "table_counts",
        "baseline_rows",
        "invariant_fingerprints",
        "mart_validation",
        "mart_type",
        "mart_set",
        "mart_rows",
        "mart_keys",
        "row_validation_shape",
        "row_validation",
        "membership_orphans",
    ),
)
def test_prepare_rejects_incomplete_s2_nppes_practice_authorization_evidence(
    invalid_field: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths, comparison_policy="serving_practice_nppes_additive_v1")
    release_path = paths["data"] / "releases" / RELEASE_ID / "release.json"
    comparison_path = paths["data"] / "releases" / RELEASE_ID / "comparison.json"
    release = json.loads(release_path.read_text())
    comparison = json.loads(comparison_path.read_text())
    details = release["release"]["validation_details"]
    mart_validation = details["mart_contract_validation"]
    provider_table = "serving_practice_nppes_provider_sites"
    membership_table = "serving_practice_nppes_org_memberships"
    if invalid_field == "comparison_changed_tables":
        comparison["changed_tables"].pop(provider_table)
        expected = "comparison changed-table scope"
    elif invalid_field == "release_changed_tables":
        details["changed_tables"] = [provider_table]
        expected = "release validation scope"
    elif invalid_field == "table_counts":
        release["release"]["table_counts"].pop(provider_table)
        expected = "release table-count scope"
    elif invalid_field == "baseline_rows":
        comparison["changed_tables"][provider_table]["baseline_rows"] = 1
        expected = "candidate table counts"
    elif invalid_field == "invariant_fingerprints":
        comparison["invariant_logical_fingerprints_checked"] = 0
        expected = "invariant-table evidence"
    elif invalid_field == "mart_validation":
        mart_validation["passed"] = False
        expected = "serving-mart validation"
    elif invalid_field == "mart_type":
        mart_validation["marts"] = {"unexpected": "mapping"}
        expected = "serving-mart validation"
    elif invalid_field == "mart_set":
        mart_validation["marts"] = mart_validation["marts"][:1]
        expected = "serving-mart validation"
    elif invalid_field == "mart_rows":
        mart_validation["marts"][0]["row_count"] = 999
        expected = "serving-mart validation"
    elif invalid_field == "mart_keys":
        mart_validation["marts"][0]["key_columns"] = ["npi", "addr_key"]
        expected = "serving-mart validation"
    elif invalid_field == "row_validation_shape":
        mart_validation["marts"][0]["row_validation_failures"].pop(
            "empty_specialties"
        )
        expected = "serving-mart validation"
    elif invalid_field == "row_validation":
        mart_validation["marts"][0]["row_validation_failures"][
            "empty_specialties"
        ] = 1
        expected = "serving-mart validation"
    else:
        membership = next(
            mart for mart in mart_validation["marts"]
            if mart["table"] == membership_table
        )
        membership["orphan_npis"] = 1
        expected = "serving-mart validation"
    release_path.write_text(json.dumps(release))
    comparison_path.write_text(json.dumps(comparison))

    with pytest.raises(production.ProductionError, match=expected):
        production.prepare_release(
            paths["production"],
            paths["artifacts"],
            paths["data"],
            paths["candidate_code"],
            paths["candidate_runtime"],
            paths["candidate_db"],
            RELEASE_ID,
        )


def _write_evidence(
    paths: dict[str, Path],
    deployment: production.ProductionDeployment,
    *,
    generated_at: str | None = None,
    names: set[str] | None = None,
    state: str = "passed",
) -> Path:
    evidence = (
        paths["production"]
        / "evidence"
        / deployment.deployment_id
        / "smoke.json"
    )
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.unlink(missing_ok=True)
    evidence.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "deployment_id": deployment.deployment_id,
                "state": state,
                "generated_at": generated_at or production.utc_now(),
                "base_url": "http://127.0.0.1:8080",
                "checks": [
                    {"name": name, "state": "passed"}
                    for name in sorted(names or production.REQUIRED_VERIFICATION_CHECKS)
                ],
            }
        )
    )
    evidence.chmod(0o440)
    return evidence


def _bootstrap_selected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, Path], production.ProductionDeployment]:
    paths = _paths(tmp_path)
    _stub_platform_security(monkeypatch)
    baseline = production.bootstrap_legacy(
        paths["production"],
        paths["artifacts"],
        paths["legacy_code"],
        paths["legacy_db"],
        paths["legacy_runtime"],
        LEGACY_SHA,
    )
    return paths, baseline


def _bootstrap_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, Path], production.ProductionDeployment]:
    paths, baseline = _bootstrap_selected(tmp_path, monkeypatch)
    production.verify_release(
        paths["production"], baseline.deployment_id, _write_evidence(paths, baseline)
    )
    return paths, baseline


def _prepare(paths: dict[str, Path]) -> production.ProductionDeployment:
    _write_release(paths)
    return production.prepare_release(
        paths["production"],
        paths["artifacts"],
        paths["data"],
        paths["candidate_code"],
        paths["candidate_runtime"],
        paths["candidate_db"],
        RELEASE_ID,
    )


def test_atomic_bundle_activation_verification_and_complete_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, baseline = _bootstrap_verified(tmp_path, monkeypatch)
    deployment = _prepare(paths)
    legacy_before = paths["legacy_db"].read_bytes()
    candidate_before = paths["candidate_db"].read_bytes()

    assert (paths["production"] / production.RELEASE_POINTER).resolve().name == baseline.deployment_id
    assert not any(
        (paths["production"] / name).exists()
        for name in ("code-current", "warehouse-current", "runtime-current")
    )

    selected = production.activate_release(paths["production"], deployment.deployment_id)
    assert selected.state == production.DeploymentState.SELECTED
    bundle = (paths["production"] / production.RELEASE_POINTER).resolve()
    assert bundle.name == deployment.deployment_id
    assert {item.name for item in bundle.iterdir()} == {"code", "warehouse", "runtime"}
    assert production.production_status(paths["production"])["healthy"] is False

    verified = production.verify_release(
        paths["production"], deployment.deployment_id, _write_evidence(paths, deployment)
    )
    assert verified.state == production.DeploymentState.VERIFIED
    assert production.production_status(paths["production"])["healthy"] is True

    rollback = production.rollback_release(paths["production"])
    assert rollback.deployment_id == baseline.deployment_id
    assert (paths["production"] / production.RELEASE_POINTER).resolve().name == baseline.deployment_id
    assert production.production_status(paths["production"])["healthy"] is False
    production.verify_release(
        paths["production"], baseline.deployment_id, _write_evidence(paths, rollback)
    )
    assert production.production_status(paths["production"])["healthy"] is True
    assert paths["legacy_db"].read_bytes() == legacy_before
    assert paths["candidate_db"].read_bytes() == candidate_before

    journal = json.loads((paths["production"] / "deployment-journal.json").read_text())
    assert [event["action"] for event in journal["events"]] == [
        "bootstrap",
        "activate",
        "rollback",
    ]
    assert all(event["state"] == "completed" for event in journal["events"])


def test_atomic_control_writes_inherit_the_control_root_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "control" / "deployments.json"
    target.parent.mkdir()
    ownership_calls: list[tuple[Path, int, int, bool]] = []
    monkeypatch.setattr(production.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        production.os,
        "chown",
        lambda path, uid, gid, *, follow_symlinks: ownership_calls.append(
            (Path(path), uid, gid, follow_symlinks)
        ),
    )

    production._write_json_atomic(target, {"schema_version": 1})

    assert json.loads(target.read_text()) == {"schema_version": 1}
    assert len(ownership_calls) == 1
    assert ownership_calls[0][0].parent == target.parent
    assert ownership_calls[0][1:] == (0, target.parent.stat().st_gid, False)


def test_activation_replaces_only_one_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    deployment = _prepare(paths)
    real_replace = production.os.replace
    selector_replacements: list[Path] = []

    def recording_replace(source, destination):
        destination = Path(destination)
        if destination == paths["production"] / production.RELEASE_POINTER:
            selector_replacements.append(destination)
        return real_replace(source, destination)

    monkeypatch.setattr(production.os, "replace", recording_replace)
    production.activate_release(paths["production"], deployment.deployment_id)

    assert selector_replacements == [paths["production"] / production.RELEASE_POINTER]


def test_failed_activation_restores_pointer_and_exact_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, baseline = _bootstrap_verified(tmp_path, monkeypatch)
    deployment = _prepare(paths)
    ledger_path = paths["production"] / "deployments.json"
    ledger_before = ledger_path.read_bytes()
    real_write = production._write_deployments
    failed = False

    def fail_candidate_write(root, document, deployments):
        nonlocal failed
        if document.get("selected_deployment_id") == deployment.deployment_id and not failed:
            failed = True
            raise OSError("simulated durable ledger failure")
        return real_write(root, document, deployments)

    monkeypatch.setattr(production, "_write_deployments", fail_candidate_write)
    with pytest.raises(production.ProductionError, match="activate failed"):
        production.activate_release(paths["production"], deployment.deployment_id)

    assert ledger_path.read_bytes() == ledger_before
    assert (paths["production"] / production.RELEASE_POINTER).resolve().name == baseline.deployment_id
    assert not (paths["production"] / production.TRANSITION_SENTINEL).exists()
    journal = json.loads((paths["production"] / "deployment-journal.json").read_text())
    assert journal["events"][-1]["state"] == "failed"


def test_startup_check_refuses_incomplete_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, baseline = _bootstrap_verified(tmp_path, monkeypatch)
    sentinel = paths["production"] / production.TRANSITION_SENTINEL
    sentinel.write_text("f" * 32 + "\n")

    with pytest.raises(production.ProductionError, match="sentinel"):
        production.startup_check(paths["production"])

    sentinel.unlink()
    assert production.startup_check(paths["production"])["selected_deployment_id"] == baseline.deployment_id


def test_stale_or_incomplete_smoke_evidence_cannot_verify(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    deployment = _prepare(paths)
    production.activate_release(paths["production"], deployment.deployment_id)
    stale = (datetime.now(timezone.utc) - timedelta(minutes=20)).replace(
        microsecond=0
    ).isoformat()

    with pytest.raises(production.ProductionError, match="stale"):
        production.verify_release(
            paths["production"],
            deployment.deployment_id,
            _write_evidence(paths, deployment, generated_at=stale),
        )

    with pytest.raises(production.ProductionError, match="canonical checks"):
        production.verify_release(
            paths["production"],
            deployment.deployment_id,
            _write_evidence(paths, deployment, names={"health"}),
        )


def test_prepare_dry_run_does_not_change_control_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths)
    before = {
        path.relative_to(paths["production"]): path.read_bytes()
        for path in paths["production"].rglob("*")
        if path.is_file() and not path.is_symlink()
    }

    result = production.prepare_release(
        paths["production"],
        paths["artifacts"],
        paths["data"],
        paths["candidate_code"],
        paths["candidate_runtime"],
        paths["candidate_db"],
        RELEASE_ID,
        dry_run=True,
    )

    after = {
        path.relative_to(paths["production"]): path.read_bytes()
        for path in paths["production"].rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    assert result.state == production.DeploymentState.PREPARED
    assert after == before


def test_prepare_tracks_serving_and_warehouse_pipeline_commits_separately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths, pipeline_commit=WAREHOUSE_COMMIT)

    deployment = production.prepare_release(
        paths["production"],
        paths["artifacts"],
        paths["data"],
        paths["candidate_code"],
        paths["candidate_runtime"],
        paths["candidate_db"],
        RELEASE_ID,
        dry_run=True,
    )

    assert deployment.code_commit == COMMIT
    assert deployment.warehouse_pipeline_commit == WAREHOUSE_COMMIT


def test_prepare_rejects_failed_release_comparison(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths, comparison_state="failed")

    with pytest.raises(production.ProductionError, match="comparison has not passed"):
        production.prepare_release(
            paths["production"],
            paths["artifacts"],
            paths["data"],
            paths["candidate_code"],
            paths["candidate_runtime"],
            paths["candidate_db"],
            RELEASE_ID,
        )


def test_prepare_rejects_legacy_release_comparison_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    paths, _ = _bootstrap_verified(tmp_path, monkeypatch)
    _write_release(paths, comparison_schema_version=1)

    with pytest.raises(production.ProductionError, match="comparison schema version"):
        production.prepare_release(
            paths["production"],
            paths["artifacts"],
            paths["data"],
            paths["candidate_code"],
            paths["candidate_runtime"],
            paths["candidate_db"],
            RELEASE_ID,
        )
