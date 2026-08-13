"""Materialize and verify an isolated CMS disaster-recovery bundle.

The bundle transport and storage provider are deliberately outside this module.
Materialization copies only manifest-declared regular files into a new target;
verification proves identity, checksums, DuckDB read-only access, representative
data, control-plane evidence, retention policy, and fresh application smoke.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath

import duckdb

from .production_manager import REQUIRED_VERIFICATION_CHECKS


SCHEMA_VERSION = 1
MANIFEST_NAME = "disaster-recovery.json"
REQUIRED_FILE_ROLES = {
    "warehouse",
    "deployments",
    "warehouse_release",
    "source_manifests",
}
DEPLOYMENT_PATTERN = re.compile(r"^[a-z]+-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{10}$")
WAREHOUSE_RELEASE_PATTERN = re.compile(
    r"^warehouse-[0-9]{8}T[0-9]{6}Z-[a-z0-9]{6,32}$"
)


class DisasterRecoveryError(RuntimeError):
    """Restore materialization or verification failed closed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timestamp(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as error:
        raise DisasterRecoveryError(f"{label} is not an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise DisasterRecoveryError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_directory(path: Path, label: str) -> Path:
    if not path.is_absolute() or path == Path("/"):
        raise DisasterRecoveryError(f"{label} must be a specific absolute path: {path}")
    if path.is_symlink() or not path.is_dir():
        raise DisasterRecoveryError(f"{label} must be a non-symlink directory: {path}")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise DisasterRecoveryError(f"{label} must be canonical: {path}")
    return resolved


def _load_json(path: Path, label: str) -> dict:
    if path.is_symlink() or not path.is_file():
        raise DisasterRecoveryError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DisasterRecoveryError(f"Could not read {label}: {error}") from error
    if not isinstance(value, dict):
        raise DisasterRecoveryError(f"{label} must contain a JSON object")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative_path(value: object, label: str) -> Path:
    if not isinstance(value, str):
        raise DisasterRecoveryError(f"{label} path must be a string")
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        raise DisasterRecoveryError(f"{label} path must be a confined relative path")
    return Path(*pure.parts)


def _validate_manifest(value: dict) -> dict[str, dict]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise DisasterRecoveryError("Unsupported disaster-recovery manifest schema version")
    backup_id = value.get("backup_id")
    if not isinstance(backup_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{5,100}", backup_id):
        raise DisasterRecoveryError("Disaster-recovery manifest has an invalid backup_id")
    _timestamp(value.get("created_at"), "backup created_at")
    deployment_id = value.get("deployment_id")
    release_id = value.get("warehouse_release_id")
    if not isinstance(deployment_id, str) or not DEPLOYMENT_PATTERN.fullmatch(deployment_id):
        raise DisasterRecoveryError("Disaster-recovery manifest has an invalid deployment ID")
    if not isinstance(release_id, str) or not WAREHOUSE_RELEASE_PATTERN.fullmatch(release_id):
        raise DisasterRecoveryError(
            "Disaster-recovery manifest has an invalid warehouse release ID"
        )
    warehouse_sha = value.get("warehouse_sha256")
    warehouse_bytes = value.get("warehouse_byte_size")
    if not isinstance(warehouse_sha, str) or not re.fullmatch(
        r"[0-9a-f]{64}", warehouse_sha
    ):
        raise DisasterRecoveryError("Disaster-recovery manifest has an invalid warehouse SHA-256")
    if not isinstance(warehouse_bytes, int) or warehouse_bytes <= 0:
        raise DisasterRecoveryError("Disaster-recovery manifest has an invalid warehouse byte size")
    retention = value.get("retention")
    if not isinstance(retention, dict):
        raise DisasterRecoveryError("Disaster-recovery manifest has no retention decision")
    if (
        not isinstance(retention.get("approved_copy_count"), int)
        or retention["approved_copy_count"] < 1
    ):
        raise DisasterRecoveryError("Off-host retention copy count must be positive")
    for field in ("location", "owner", "approved_at", "next_drill_date"):
        if not isinstance(retention.get(field), str) or not retention[field].strip():
            raise DisasterRecoveryError(f"Retention decision is missing {field}")
    _timestamp(retention["approved_at"], "retention approved_at")
    try:
        date.fromisoformat(retention["next_drill_date"])
    except ValueError as error:
        raise DisasterRecoveryError("next_drill_date is not an ISO date") from error
    coverage = value.get("control_plane_coverage")
    if not isinstance(coverage, dict) or coverage.get("kind") not in {
        "provider_snapshot",
        "off_host_bundle",
        "both",
    }:
        raise DisasterRecoveryError("Control-plane backup coverage is not recorded")
    if not isinstance(coverage.get("reference"), str) or not coverage["reference"].strip():
        raise DisasterRecoveryError("Control-plane coverage reference is missing")
    _timestamp(coverage.get("captured_at"), "control-plane captured_at")

    files = value.get("files")
    if not isinstance(files, list):
        raise DisasterRecoveryError("Disaster-recovery manifest is missing files")
    by_role: dict[str, dict] = {}
    paths: set[Path] = set()
    for item in files:
        if not isinstance(item, dict) or not isinstance(item.get("role"), str):
            raise DisasterRecoveryError("Disaster-recovery file record is malformed")
        role = item["role"]
        if role in by_role:
            raise DisasterRecoveryError(f"Duplicate disaster-recovery file role: {role}")
        relative = _relative_path(item.get("path"), role)
        if relative in paths:
            raise DisasterRecoveryError(f"Duplicate disaster-recovery path: {relative}")
        expected_sha = item.get("sha256")
        expected_bytes = item.get("byte_size")
        if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
            raise DisasterRecoveryError(f"Disaster-recovery file has invalid SHA-256: {role}")
        if not isinstance(expected_bytes, int) or expected_bytes < 0:
            raise DisasterRecoveryError(f"Disaster-recovery file has invalid byte size: {role}")
        by_role[role] = {**item, "relative_path": relative}
        paths.add(relative)
    missing = sorted(REQUIRED_FILE_ROLES - set(by_role))
    if missing:
        raise DisasterRecoveryError("Disaster-recovery bundle lacks roles: " + ", ".join(missing))
    expected_counts = value.get("representative_table_counts")
    if not isinstance(expected_counts, dict) or not expected_counts:
        raise DisasterRecoveryError("Representative table counts are missing")
    if any(
        not isinstance(table, str)
        or not re.fullmatch(r"[a-z][a-z0-9_]*", table)
        or not isinstance(count, int)
        or count < 0
        for table, count in expected_counts.items()
    ):
        raise DisasterRecoveryError("Representative table counts are malformed")
    npi = value.get("representative_npi")
    if not isinstance(npi, str) or not re.fullmatch(r"[0-9]{10}", npi):
        raise DisasterRecoveryError("Representative NPI must contain ten digits")
    return by_role


def _verify_files(root: Path, manifest: dict, by_role: dict[str, dict]) -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for role, item in by_role.items():
        path = root / item["relative_path"]
        if path.is_symlink() or not path.is_file():
            raise DisasterRecoveryError(f"Restored {role} is not a regular file: {path}")
        if path.resolve(strict=True).parent != path.parent.resolve(strict=True):
            raise DisasterRecoveryError(f"Restored {role} escapes its declared path")
        if path.stat().st_size != item["byte_size"]:
            raise DisasterRecoveryError(f"Restored {role} byte size does not match")
        if sha256_file(path) != item["sha256"]:
            raise DisasterRecoveryError(f"Restored {role} SHA-256 does not match")
        resolved[role] = path
    warehouse = resolved["warehouse"]
    if warehouse.stat().st_size != manifest.get("warehouse_byte_size"):
        raise DisasterRecoveryError("Warehouse byte size differs from backup identity")
    if by_role["warehouse"]["sha256"] != manifest.get("warehouse_sha256"):
        raise DisasterRecoveryError("Warehouse SHA-256 differs from backup identity")
    return resolved


def materialize_restore(bundle_root: Path, restore_root: Path) -> dict:
    """Copy a verified logical bundle into one new isolated restore directory."""
    bundle_root = _canonical_directory(bundle_root, "backup bundle root")
    if not restore_root.is_absolute() or restore_root == Path("/"):
        raise DisasterRecoveryError("restore root must be a specific absolute path")
    if restore_root.exists() or restore_root.is_symlink():
        raise DisasterRecoveryError("restore root must not already exist")
    parent = _canonical_directory(restore_root.parent, "restore parent")
    if restore_root.is_relative_to(bundle_root) or bundle_root.is_relative_to(restore_root):
        raise DisasterRecoveryError("backup bundle and restore root must be disjoint")
    manifest_path = bundle_root / MANIFEST_NAME
    manifest = _load_json(manifest_path, "disaster-recovery manifest")
    by_role = _validate_manifest(manifest)
    _verify_files(bundle_root, manifest, by_role)

    temporary = parent / f".{restore_root.name}.{uuid.uuid4().hex}.partial"
    temporary.mkdir(mode=0o700)
    try:
        shutil.copyfile(manifest_path, temporary / MANIFEST_NAME)
        for item in by_role.values():
            source = bundle_root / item["relative_path"]
            destination = temporary / item["relative_path"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            os.chmod(destination, 0o440)
        restored_manifest = _load_json(temporary / MANIFEST_NAME, "restored manifest")
        restored_roles = _validate_manifest(restored_manifest)
        _verify_files(temporary, restored_manifest, restored_roles)
        os.chmod(temporary / MANIFEST_NAME, 0o440)
        os.replace(temporary, restore_root)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {
        "schema_version": SCHEMA_VERSION,
        "state": "materialized",
        "materialized_at": _utc_now(),
        "backup_id": manifest["backup_id"],
        "restore_root": str(restore_root),
        "file_count": len(by_role),
        "verified_before_copy": True,
        "verified_after_copy": True,
        "application_smoke": "required_before_restore_proof_passes",
    }


def _selected_deployment(document: dict, deployment_id: str) -> dict:
    if document.get("schema_version") != 1:
        raise DisasterRecoveryError("Restored deployment ledger schema is unsupported")
    if document.get("selected_deployment_id") != deployment_id:
        raise DisasterRecoveryError("Restored deployment ledger selects another deployment")
    deployments = document.get("deployments")
    if not isinstance(deployments, list):
        raise DisasterRecoveryError("Restored deployment ledger is malformed")
    matches = [
        item
        for item in deployments
        if isinstance(item, dict) and item.get("deployment_id") == deployment_id
    ]
    if len(matches) != 1:
        raise DisasterRecoveryError("Restored deployment ledger lacks the selected deployment")
    return matches[0]


def _verify_evidence(manifest: dict, paths: dict[str, Path]) -> None:
    deployment = _selected_deployment(
        _load_json(paths["deployments"], "restored deployment ledger"),
        manifest["deployment_id"],
    )
    if (
        deployment.get("warehouse_release_id") != manifest["warehouse_release_id"]
        or deployment.get("warehouse_sha256") != manifest["warehouse_sha256"]
        or deployment.get("warehouse_byte_size") != manifest["warehouse_byte_size"]
    ):
        raise DisasterRecoveryError("Restored deployment identity does not match the bundle")
    release_document = _load_json(paths["warehouse_release"], "warehouse release evidence")
    release = release_document.get("release")
    if not isinstance(release, dict):
        raise DisasterRecoveryError("Warehouse release evidence is malformed")
    if (
        release.get("warehouse_release_id") != manifest["warehouse_release_id"]
        or release.get("sha256") != manifest["warehouse_sha256"]
        or release.get("byte_size") != manifest["warehouse_byte_size"]
    ):
        raise DisasterRecoveryError("Warehouse release evidence does not match the bundle")
    source_manifests = _load_json(paths["source_manifests"], "source manifest evidence")
    if source_manifests.get("schema_version") != 1 or not isinstance(
        source_manifests.get("manifests"), list
    ):
        raise DisasterRecoveryError("Source manifest evidence is malformed")
    for optional, expected in (
        ("deployment_id", manifest["deployment_id"]),
        ("warehouse_release_id", manifest["warehouse_release_id"]),
    ):
        if optional in source_manifests and source_manifests[optional] != expected:
            raise DisasterRecoveryError(f"Source manifest {optional} does not match")


def _verify_warehouse(manifest: dict, warehouse: Path) -> dict[str, int]:
    connection = duckdb.connect(str(warehouse), read_only=True)
    counts: dict[str, int] = {}
    try:
        tables = {
            row[0]
            for row in connection.execute("SHOW TABLES").fetchall()
        }
        expected_counts = manifest["representative_table_counts"]
        missing = sorted(set(expected_counts) - tables)
        if missing:
            raise DisasterRecoveryError("Restored warehouse lacks tables: " + ", ".join(missing))
        for table, expected in sorted(expected_counts.items()):
            actual = int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
            counts[table] = actual
            if actual != expected:
                raise DisasterRecoveryError(
                    f"Restored warehouse count mismatch for {table}: {actual} != {expected}"
                )
        if "core_providers" not in tables:
            raise DisasterRecoveryError("Restored warehouse lacks core_providers")
        found = connection.execute(
            "SELECT count(*) FROM core_providers WHERE npi = ?",
            [manifest["representative_npi"]],
        ).fetchone()[0]
        if int(found) != 1:
            raise DisasterRecoveryError("Representative NPI is not unique in restored warehouse")
    finally:
        connection.close()
    return counts


def _verify_smoke(path: Path, deployment_id: str, restored_after: datetime) -> dict:
    smoke = _load_json(path, "isolated application smoke evidence")
    if smoke.get("deployment_id") != deployment_id or smoke.get("state") != "passed":
        raise DisasterRecoveryError("Application smoke did not pass for the restored deployment")
    generated_at = _timestamp(smoke.get("generated_at"), "application smoke generated_at")
    if generated_at < restored_after:
        raise DisasterRecoveryError("Application smoke predates the isolated restore")
    checks = smoke.get("checks")
    if not isinstance(checks, list):
        raise DisasterRecoveryError("Application smoke checks are missing")
    passed = {
        item.get("name")
        for item in checks
        if isinstance(item, dict) and item.get("state") == "passed"
    }
    missing = sorted(REQUIRED_VERIFICATION_CHECKS - passed)
    if missing:
        raise DisasterRecoveryError("Application smoke lacks passed checks: " + ", ".join(missing))
    return {"generated_at": generated_at.isoformat(), "passed_checks": len(passed)}


def verify_restore(
    restore_root: Path,
    application_smoke: Path,
    *,
    restored_after: datetime,
) -> dict:
    """Verify one isolated restore and return durable drill evidence."""
    restore_root = _canonical_directory(restore_root, "restore root")
    manifest = _load_json(restore_root / MANIFEST_NAME, "disaster-recovery manifest")
    by_role = _validate_manifest(manifest)
    paths = _verify_files(restore_root, manifest, by_role)
    _verify_evidence(manifest, paths)
    counts = _verify_warehouse(manifest, paths["warehouse"])
    smoke = _verify_smoke(application_smoke, manifest["deployment_id"], restored_after)
    completed_at = datetime.now(timezone.utc).replace(microsecond=0)
    duration = max(0, int((completed_at - restored_after).total_seconds()))
    return {
        "schema_version": SCHEMA_VERSION,
        "state": "passed",
        "verified_at": completed_at.isoformat(),
        "backup_id": manifest["backup_id"],
        "deployment_id": manifest["deployment_id"],
        "warehouse_release_id": manifest["warehouse_release_id"],
        "warehouse_sha256": manifest["warehouse_sha256"],
        "restore_root": str(restore_root),
        "restore_duration_seconds": duration,
        "checks": {
            "checksums": "passed",
            "control_plane_identity": "passed",
            "warehouse_release_identity": "passed",
            "source_provenance": "passed",
            "duckdb_read_only_open": "passed",
            "representative_table_counts": counts,
            "representative_npi": "passed",
            "application_smoke": smoke,
        },
        "retention": manifest["retention"],
        "control_plane_coverage": manifest["control_plane_coverage"],
        "failure_modes_exercised": manifest.get("failure_modes_exercised", []),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CMS disaster-recovery restore verification")
    subparsers = parser.add_subparsers(dest="command", required=True)
    materialize = subparsers.add_parser("materialize")
    materialize.add_argument("--bundle-root", required=True, type=Path)
    materialize.add_argument("--restore-root", required=True, type=Path)
    materialize.add_argument("--json", action="store_true")
    verify = subparsers.add_parser("verify")
    verify.add_argument("--restore-root", required=True, type=Path)
    verify.add_argument("--application-smoke", required=True, type=Path)
    verify.add_argument("--restored-after", required=True)
    verify.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "materialize":
            result = materialize_restore(args.bundle_root, args.restore_root)
        else:
            result = verify_restore(
                args.restore_root,
                args.application_smoke,
                restored_after=_timestamp(args.restored_after, "restored_after"),
            )
    except (OSError, ValueError, duckdb.Error, DisasterRecoveryError) as error:
        payload = {"state": "failed", "error": str(error)}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Disaster-recovery verification failed: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
