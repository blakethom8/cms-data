"""Crash-safe production release selection and verification.

The manager never opens DuckDB, restarts a service, reads secrets, or mutates
staging manifests.  It validates root-owned immutable artifacts, selects one
code/runtime/warehouse bundle through a single atomic symlink, journals every
selection, and records loopback smoke evidence separately from staging state.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Iterator
from urllib.parse import urlsplit


SCHEMA_VERSION = 1
RELEASE_POINTER = "release-current"
TRANSITION_SENTINEL = "transition-pending"
BUNDLE_CODE = "code"
BUNDLE_WAREHOUSE = "warehouse"
BUNDLE_RUNTIME = "runtime"
BUNDLE_NAMES = (BUNDLE_CODE, BUNDLE_WAREHOUSE, BUNDLE_RUNTIME)
BLOCKING_JOURNAL_STATES = {"pending", "recovery_required"}
TERMINAL_JOURNAL_STATES = {"completed", "failed", "recovered"}
MAX_EVIDENCE_AGE = timedelta(minutes=15)
RELEASE_ID_PATTERN = re.compile(r"^warehouse-[0-9]{8}T[0-9]{6}Z-[a-z0-9]{6,32}$")
DEPLOYMENT_ID_PATTERN = re.compile(r"^[a-z]+-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{10}$")
REQUIRED_VERIFICATION_CHECKS = {
    "process_identity",
    "health",
    "authentication_required",
    "practice_capabilities",
    "practice_search",
    "provider_profile",
    "industry_search",
    "industry_options",
    "industry_exact_option_round_trip",
    "industry_detail",
    "research",
    "clinical_trials",
    "explorer_catalog",
    "required_tables",
    "warehouse_counts",
}


class ProductionError(RuntimeError):
    """A production deployment invariant was not satisfied."""


class DeploymentState(str, Enum):
    PREPARED = "prepared"
    SELECTED = "selected"
    VERIFIED = "verified"
    SUPERSEDED = "superseded"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


@dataclass(frozen=True)
class ReleaseTargets:
    code: str
    warehouse: str
    runtime: str

    def to_bundle_map(self) -> dict[str, str]:
        return {
            BUNDLE_CODE: self.code,
            BUNDLE_WAREHOUSE: self.warehouse,
            BUNDLE_RUNTIME: self.runtime,
        }


@dataclass
class ProductionDeployment:
    deployment_id: str
    deployment_kind: str
    state: DeploymentState
    targets: ReleaseTargets
    artifact_root: str
    warehouse_sha256: str
    warehouse_byte_size: int
    code_fingerprint: str
    runtime_fingerprint: str
    prepared_at: str
    code_commit: str | None = None
    warehouse_release_id: str | None = None
    warehouse_pipeline_commit: str | None = None
    previous_deployment_id: str | None = None
    selected_at: str | None = None
    verified_at: str | None = None
    superseded_at: str | None = None
    rollback_at: str | None = None
    verification_summary: dict = field(default_factory=dict)
    error_summary: str | None = None

    def to_dict(self) -> dict:
        value = asdict(self)
        value["state"] = self.state.value
        return value

    @classmethod
    def from_dict(cls, value: dict) -> ProductionDeployment:
        try:
            targets = ReleaseTargets(**value["targets"])
            return cls(
                deployment_id=value["deployment_id"],
                deployment_kind=value["deployment_kind"],
                state=DeploymentState(value["state"]),
                targets=targets,
                artifact_root=value["artifact_root"],
                warehouse_sha256=value["warehouse_sha256"],
                warehouse_byte_size=int(value["warehouse_byte_size"]),
                code_fingerprint=value["code_fingerprint"],
                runtime_fingerprint=value["runtime_fingerprint"],
                prepared_at=value["prepared_at"],
                code_commit=value.get("code_commit"),
                warehouse_release_id=value.get("warehouse_release_id"),
                warehouse_pipeline_commit=value.get("warehouse_pipeline_commit"),
                previous_deployment_id=value.get("previous_deployment_id"),
                selected_at=value.get("selected_at"),
                verified_at=value.get("verified_at"),
                superseded_at=value.get("superseded_at"),
                rollback_at=value.get("rollback_at"),
                verification_summary=value.get("verification_summary") or {},
                error_summary=value.get("error_summary"),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ProductionError("Production deployment record is malformed") from error


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_error(error: BaseException) -> str:
    text = " ".join(str(error).split())
    return (text or error.__class__.__name__)[:500]


def _parse_timestamp(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as error:
        raise ProductionError(f"{label} is not an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ProductionError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_absolute(path: Path, label: str) -> Path:
    if not path.is_absolute() or path == Path("/"):
        raise ProductionError(f"{label} must be a specific absolute path: {path}")
    return path


def _canonical_directory(path: Path, label: str) -> Path:
    _require_absolute(path, label)
    if path.is_symlink() or not path.is_dir():
        raise ProductionError(f"{label} must be a non-symlink directory: {path}")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise ProductionError(f"{label} must be canonical: {path}")
    return resolved


def _require_root_operator() -> None:
    if os.geteuid() != 0:
        raise ProductionError("Production mutations must run as root")


def _require_control_ownership(path: Path) -> None:
    details = path.lstat()
    if details.st_uid != 0:
        raise ProductionError(f"Production control path is not root-owned: {path}")
    if details.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ProductionError(f"Production control path is service-writable: {path}")


def _canonical_control_root(path: Path) -> Path:
    path = _canonical_directory(path, "production root")
    _require_control_ownership(path)
    return path


def _inherit_control_group(path: Path, root: Path) -> None:
    if os.geteuid() == 0:
        os.chown(path, 0, root.stat().st_gid, follow_symlinks=False)


def _confined(path: Path, root: Path, label: str) -> Path:
    root = _canonical_directory(root, "production artifact root")
    _require_absolute(path, label)
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ProductionError(f"{label} does not resolve: {path}") from error
    if not resolved.is_relative_to(root):
        raise ProductionError(f"{label} escapes the production artifact root: {path}")
    return resolved


def _require_root_owned(path: Path, artifact_root: Path, *, sealed_parent: bool) -> None:
    root = artifact_root.resolve(strict=True)
    current = path.resolve(strict=True)
    first_parent = current.parent if current.is_file() else current
    while True:
        details = current.stat()
        if details.st_uid != 0:
            raise ProductionError(f"Production artifact is not root-owned: {current}")
        if details.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ProductionError(f"Production artifact is service-writable: {current}")
        if sealed_parent and current == first_parent and details.st_mode & stat.S_IWUSR:
            raise ProductionError(f"Production artifact parent is replaceable: {current}")
        if current == root:
            break
        current = current.parent


def _require_immutable_file(path: Path, artifact_root: Path, label: str) -> os.stat_result:
    path = _confined(path, artifact_root, label)
    if path.is_symlink() or not path.is_file():
        raise ProductionError(f"{label} must be a regular non-symlink file: {path}")
    details = path.stat()
    if details.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise ProductionError(f"{label} must not have write permission: {path}")
    if details.st_nlink != 1:
        raise ProductionError(f"{label} must be an independent inode, not a hard link: {path}")
    _require_root_owned(path, artifact_root, sealed_parent=True)
    return details


def _require_immutable_directory(path: Path, artifact_root: Path, label: str) -> Path:
    path = _confined(path, artifact_root, label)
    if path.is_symlink() or not path.is_dir():
        raise ProductionError(f"{label} must be a non-symlink directory: {path}")
    for candidate in [path, *path.rglob("*")]:
        if candidate.is_symlink():
            link_details = candidate.lstat()
            if link_details.st_uid != 0:
                raise ProductionError(f"{label} contains a non-root-owned symlink: {candidate}")
            try:
                resolved = candidate.resolve(strict=True)
            except (FileNotFoundError, OSError) as error:
                raise ProductionError(f"{label} has a dangling symlink: {candidate}") from error
            if resolved.is_dir() and not resolved.is_relative_to(path):
                raise ProductionError(f"{label} has an external directory symlink: {candidate}")
            if resolved.is_file() and not resolved.is_relative_to(path):
                target_details = resolved.stat()
                if target_details.st_uid != 0 or target_details.st_mode & (
                    stat.S_IWGRP | stat.S_IWOTH
                ):
                    raise ProductionError(
                        f"{label} has an unsafe external file symlink: {candidate}"
                    )
            continue
        details = candidate.stat()
        mode = details.st_mode
        if details.st_uid != 0:
            raise ProductionError(f"{label} contains a non-root-owned path: {candidate}")
        if mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise ProductionError(f"{label} contains a writable path: {candidate}")
    _require_root_owned(path, artifact_root, sealed_parent=False)
    return path


def _tree_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    for candidate in sorted(path.rglob("*")):
        relative = candidate.relative_to(path)
        if ".git" in relative.parts:
            continue
        if candidate.is_symlink():
            resolved = candidate.resolve(strict=True)
            digest.update(b"L\0" + str(relative).encode() + b"\0")
            digest.update(os.readlink(candidate).encode() + b"\0")
            digest.update(str(resolved).encode() + b"\0")
            if resolved.is_file():
                with resolved.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
        elif candidate.is_file():
            details = candidate.stat()
            digest.update(b"F\0" + str(relative).encode() + b"\0")
            digest.update(str(details.st_size).encode() + b"\0")
            digest.update(oct(stat.S_IMODE(details.st_mode)).encode() + b"\0")
            with candidate.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _inspect_git_code(path: Path, artifact_root: Path) -> tuple[str, str]:
    path = _require_immutable_directory(path, artifact_root, "code target")
    command = ["git", "-c", f"safe.directory={path}", "-C", str(path)]
    git_environment = {**os.environ, "GIT_OPTIONAL_LOCKS": "0"}
    try:
        commit = subprocess.run(
            [*command, "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=git_environment,
        ).stdout.strip()
        status_output = subprocess.run(
            [*command, "status", "--porcelain", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=git_environment,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise ProductionError(f"Could not validate code target: {safe_error(error)}") from error
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ProductionError("Code target does not resolve to a full Git commit")
    if status_output:
        raise ProductionError("Code target is not a clean Git checkout")
    return commit, _tree_fingerprint(path)


def _inspect_legacy_code(path: Path, artifact_root: Path) -> str:
    path = _require_immutable_directory(path, artifact_root, "legacy code target")
    return _tree_fingerprint(path)


def _inspect_runtime(
    path: Path,
    artifact_root: Path,
    expected_duckdb_version: str | None,
) -> str:
    path = _require_immutable_directory(path, artifact_root, "runtime target")
    python = path / "bin" / "python"
    if not python.is_file() or not os.access(python, os.X_OK):
        raise ProductionError(f"Runtime Python is missing or not executable: {python}")
    probe = (
        "import importlib.metadata as m,json,platform,sys;"
        "names=('duckdb','fastapi','uvicorn','pydantic','psycopg');"
        "print(json.dumps({'python':platform.python_version(),'prefix':sys.prefix,"
        "'packages':{n:m.version(n) for n in names}},sort_keys=True))"
    )
    try:
        child_environment = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
        }
        result = subprocess.run(
            [str(python), "-I", "-c", probe],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=child_environment,
        )
        details = json.loads(result.stdout)
        freeze = subprocess.run(
            [str(python), "-I", "-m", "pip", "freeze", "--all"],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
            env=child_environment,
        ).stdout
    except (
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ) as error:
        raise ProductionError(f"Runtime validation failed: {safe_error(error)}") from error
    if Path(details.get("prefix", "")).resolve() != path:
        raise ProductionError("Runtime Python resolves to a different environment prefix")
    actual_duckdb = details.get("packages", {}).get("duckdb")
    if expected_duckdb_version and actual_duckdb != expected_duckdb_version:
        raise ProductionError(
            f"Runtime DuckDB {actual_duckdb!r} does not match release {expected_duckdb_version!r}"
        )
    digest = hashlib.sha256()
    digest.update(json.dumps(details, sort_keys=True).encode())
    digest.update(b"\n" + freeze.encode() + b"\n")
    digest.update(_tree_fingerprint(path).encode())
    return f"sha256:{digest.hexdigest()}"


def _load_json(path: Path, label: str) -> dict:
    if path.is_symlink():
        raise ProductionError(f"{label} must not be a symlink")
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ProductionError(f"Could not read {label}: {safe_error(error)}") from error
    if not isinstance(value, dict):
        raise ProductionError(f"{label} must contain a JSON object")
    return value


def _validate_warehouse_release(
    data_root: Path,
    warehouse_release_id: str,
    production_warehouse: Path,
    artifact_root: Path,
) -> tuple[Path, dict]:
    data_root = _canonical_directory(data_root, "warehouse data root")
    artifact_root = _canonical_directory(artifact_root, "production artifact root")
    if artifact_root.is_relative_to(data_root) or data_root.is_relative_to(artifact_root):
        raise ProductionError("Production artifacts and staging data must use disjoint roots")
    if not RELEASE_ID_PATTERN.fullmatch(warehouse_release_id):
        raise ProductionError(f"Invalid warehouse release ID: {warehouse_release_id!r}")
    release_dir = data_root / "releases" / warehouse_release_id
    if release_dir.is_symlink() or release_dir.resolve(strict=True) != release_dir:
        raise ProductionError("Warehouse release directory is not canonical")
    release_document = _load_json(release_dir / "release.json", "warehouse release")
    if release_document.get("schema_version") != 2:
        raise ProductionError("Unsupported warehouse release schema version")
    release = release_document.get("release")
    if not isinstance(release, dict):
        raise ProductionError("Warehouse release document is missing release data")
    if release.get("warehouse_release_id") != warehouse_release_id:
        raise ProductionError("Warehouse release ID does not match its directory")
    if release.get("validation_state") != "passed":
        raise ProductionError("Warehouse release validation has not passed")
    pipeline_commit = release.get("pipeline_code_commit")
    if not isinstance(pipeline_commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}", pipeline_commit
    ):
        raise ProductionError("Warehouse release has an invalid pipeline commit")
    canonical_relative = Path("releases") / warehouse_release_id / "warehouse.duckdb"
    if Path(str(release.get("database_path", ""))) != canonical_relative:
        raise ProductionError("Warehouse release database path is not canonical")
    staging_database = data_root / canonical_relative
    if staging_database.is_symlink() or not staging_database.is_file():
        raise ProductionError("Staging warehouse is not a regular file")
    expected_size = int(release.get("byte_size", -1))
    expected_sha = str(release.get("sha256", ""))
    if staging_database.stat().st_size != expected_size:
        raise ProductionError("Warehouse byte size does not match release evidence")
    if sha256_file(staging_database) != expected_sha:
        raise ProductionError("Warehouse SHA-256 does not match release evidence")
    comparison = _load_json(release_dir / "comparison.json", "release comparison")
    if comparison.get("schema_version") != 1:
        raise ProductionError("Unsupported release comparison schema version")
    if comparison.get("warehouse_release_id") != warehouse_release_id:
        raise ProductionError("Comparison release ID does not match")
    if comparison.get("pipeline_code_commit") != pipeline_commit:
        raise ProductionError("Comparison pipeline commit does not match the warehouse release")
    if comparison.get("state") != "passed":
        raise ProductionError("Warehouse comparison has not passed")
    failed_requirements = comparison.get("failed_requirements")
    unexpected_differences = comparison.get("unexpected_differences")
    if not isinstance(failed_requirements, list) or not isinstance(
        unexpected_differences, list
    ):
        raise ProductionError("Warehouse comparison safety fields are missing or malformed")
    if failed_requirements != [] or unexpected_differences != []:
        raise ProductionError("Warehouse comparison contains failed requirements or differences")
    comparison_candidate = comparison.get("candidate")
    if not isinstance(comparison_candidate, dict):
        raise ProductionError("Warehouse comparison candidate is missing or malformed")
    if comparison_candidate.get("sha256") != expected_sha:
        raise ProductionError("Comparison candidate SHA-256 does not match")
    target_details = _require_immutable_file(
        production_warehouse, artifact_root, "production warehouse target"
    )
    production_warehouse = production_warehouse.resolve(strict=True)
    if production_warehouse.samefile(staging_database):
        raise ProductionError("Production warehouse must be an independent copy of staging")
    if target_details.st_size != expected_size:
        raise ProductionError("Production warehouse copy has the wrong byte size")
    if sha256_file(production_warehouse) != expected_sha:
        raise ProductionError("Production warehouse copy SHA-256 does not match")
    return production_warehouse, release


def _state_path(root: Path) -> Path:
    return root / "deployments.json"


def _journal_path(root: Path) -> Path:
    return root / "deployment-journal.json"


def _sentinel_path(root: Path) -> Path:
    return root / TRANSITION_SENTINEL


def _empty_state() -> dict:
    return {"schema_version": SCHEMA_VERSION, "selected_deployment_id": None, "deployments": []}


def _empty_journal() -> dict:
    return {"schema_version": SCHEMA_VERSION, "events": []}


def _read_document(path: Path, empty: dict) -> dict:
    if not path.exists() and not path.is_symlink():
        return empty
    if path.is_symlink() or not path.is_file():
        raise ProductionError(f"Production control document is not a regular file: {path}")
    _require_control_ownership(path)
    value = _load_json(path, path.name)
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ProductionError(f"Unsupported {path.name} schema version")
    return value


def _validate_deployment(deployment: ProductionDeployment) -> None:
    if not DEPLOYMENT_ID_PATTERN.fullmatch(deployment.deployment_id):
        raise ProductionError("Production deployment record has an invalid ID")
    if deployment.deployment_kind not in {"legacy_baseline", "warehouse_release"}:
        raise ProductionError("Production deployment record has an invalid kind")
    for label, value in deployment.targets.to_bundle_map().items():
        if not Path(value).is_absolute():
            raise ProductionError(f"Production deployment {label} target is not absolute")
    if not Path(deployment.artifact_root).is_absolute():
        raise ProductionError("Production deployment artifact root is not absolute")
    if not re.fullmatch(r"[0-9a-f]{64}", deployment.warehouse_sha256):
        raise ProductionError("Production deployment has an invalid warehouse SHA-256")
    if deployment.warehouse_byte_size <= 0:
        raise ProductionError("Production deployment has an invalid warehouse byte size")
    for fingerprint in (deployment.code_fingerprint, deployment.runtime_fingerprint):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint):
            raise ProductionError("Production deployment has an invalid artifact fingerprint")
    prepared = _parse_timestamp(deployment.prepared_at, "deployment prepared_at")
    timestamps: dict[str, datetime] = {}
    for name in ("selected_at", "verified_at", "superseded_at", "rollback_at"):
        value = getattr(deployment, name)
        if value is not None:
            timestamps[name] = _parse_timestamp(value, f"deployment {name}")
            if timestamps[name] < prepared:
                raise ProductionError(f"Production deployment {name} precedes preparation")
    if deployment.state == DeploymentState.PREPARED and deployment.selected_at is not None:
        raise ProductionError("Prepared deployment already has a selection timestamp")
    if deployment.state in {DeploymentState.SELECTED, DeploymentState.VERIFIED} and not deployment.selected_at:
        raise ProductionError("Selected deployment is missing selected_at")
    if deployment.state == DeploymentState.VERIFIED:
        if not deployment.verified_at or not deployment.verification_summary:
            raise ProductionError("Verified deployment is missing verification evidence")
    if deployment.state == DeploymentState.SUPERSEDED:
        if not deployment.selected_at or not deployment.verified_at or not deployment.superseded_at:
            raise ProductionError("Superseded deployment has incomplete timestamps")
    if deployment.state == DeploymentState.ROLLED_BACK and not deployment.rollback_at:
        raise ProductionError("Rolled-back deployment is missing rollback_at")
    if deployment.deployment_kind == "warehouse_release":
        if not re.fullmatch(r"[0-9a-f]{40}", deployment.code_commit or ""):
            raise ProductionError("Warehouse deployment has an invalid code commit")
        if not RELEASE_ID_PATTERN.fullmatch(deployment.warehouse_release_id or ""):
            raise ProductionError("Warehouse deployment has an invalid warehouse release ID")
        if not re.fullmatch(
            r"[0-9a-f]{40}", deployment.warehouse_pipeline_commit or ""
        ):
            raise ProductionError("Warehouse deployment has an invalid pipeline commit")
        if not deployment.previous_deployment_id:
            raise ProductionError("Warehouse deployment has no predecessor")


def _validate_ledger(document: dict, deployments: list[ProductionDeployment]) -> None:
    for deployment in deployments:
        _validate_deployment(deployment)
    identifiers = {item.deployment_id for item in deployments}
    if len(identifiers) != len(deployments):
        raise ProductionError("Production deployment store has duplicate IDs")
    for deployment in deployments:
        predecessor = deployment.previous_deployment_id
        if predecessor is not None and (predecessor not in identifiers or predecessor == deployment.deployment_id):
            raise ProductionError("Production deployment has an invalid predecessor")
        seen = {deployment.deployment_id}
        cursor = predecessor
        while cursor is not None:
            if cursor in seen:
                raise ProductionError("Production deployment predecessor graph contains a cycle")
            seen.add(cursor)
            cursor = _deployment_by_id(deployments, cursor).previous_deployment_id
    selected_id = document.get("selected_deployment_id")
    selected_states = [
        item for item in deployments if item.state in {DeploymentState.SELECTED, DeploymentState.VERIFIED}
    ]
    if selected_id is None:
        if selected_states:
            raise ProductionError("Production ledger has selected state without a selected ID")
    elif (
        selected_id not in identifiers
        or len(selected_states) != 1
        or selected_states[0].deployment_id != selected_id
    ):
        raise ProductionError("Production selected deployment state is inconsistent")


def _read_deployments(root: Path) -> tuple[dict, list[ProductionDeployment]]:
    document = _read_document(_state_path(root), _empty_state())
    values = document.get("deployments")
    if not isinstance(values, list):
        raise ProductionError("Production deployment store is malformed")
    deployments = [ProductionDeployment.from_dict(value) for value in values]
    _validate_ledger(document, deployments)
    return document, deployments


def _serialize_state(document: dict, deployments: list[ProductionDeployment]) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "selected_deployment_id": document.get("selected_deployment_id"),
        "deployments": [deployment.to_dict() for deployment in deployments],
    }


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ProductionError(f"State path must not be a symlink: {path}")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _write_deployments(root: Path, document: dict, deployments: list[ProductionDeployment]) -> None:
    _validate_ledger(document, deployments)
    _write_json_atomic(_state_path(root), _serialize_state(document, deployments))


@contextmanager
def _lock(root: Path) -> Iterator[None]:
    lock_path = root / "locks" / "production.lock"
    if lock_path.parent.exists() or lock_path.parent.is_symlink():
        if lock_path.parent.is_symlink() or not lock_path.parent.is_dir():
            raise ProductionError("Production lock directory is invalid")
    else:
        lock_path.parent.mkdir(mode=0o750)
        _inherit_control_group(lock_path.parent, root)
    os.chmod(lock_path.parent, 0o750)
    _require_control_ownership(lock_path.parent)
    if lock_path.is_symlink():
        raise ProductionError("Production lock file must not be a symlink")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(lock_path, flags, 0o640)
    with os.fdopen(descriptor, "a+") as handle:
        _inherit_control_group(lock_path, root)
        os.chmod(lock_path, 0o640)
        _require_control_ownership(lock_path)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def _deployment_by_id(
    deployments: list[ProductionDeployment], deployment_id: str
) -> ProductionDeployment:
    for deployment in deployments:
        if deployment.deployment_id == deployment_id:
            return deployment
    raise ProductionError(f"Unknown production deployment: {deployment_id}")


def _bundle_path(root: Path, deployment_id: str) -> Path:
    if not DEPLOYMENT_ID_PATTERN.fullmatch(deployment_id):
        raise ProductionError(f"Invalid deployment ID: {deployment_id!r}")
    return root / "releases" / deployment_id


def _create_bundle(root: Path, deployment: ProductionDeployment) -> Path:
    releases = root / "releases"
    if releases.exists() or releases.is_symlink():
        if releases.is_symlink() or not releases.is_dir():
            raise ProductionError("Production releases directory is invalid")
    else:
        releases.mkdir()
        _inherit_control_group(releases, root)
    os.chmod(releases, 0o750)
    _require_control_ownership(releases)
    bundle = _bundle_path(root, deployment.deployment_id)
    if bundle.exists() or bundle.is_symlink():
        raise ProductionError(f"Production release bundle already exists: {bundle}")
    bundle.mkdir(mode=0o750)
    _inherit_control_group(bundle, root)
    try:
        for name, target in deployment.targets.to_bundle_map().items():
            os.symlink(target, bundle / name)
        actual = _read_bundle_targets(bundle)
        if actual != deployment.targets.to_bundle_map():
            raise ProductionError("Production release bundle targets do not match")
        os.chmod(bundle, 0o550)
        _require_control_ownership(bundle)
        _fsync_directory(bundle)
        _fsync_directory(releases)
    except Exception:
        os.chmod(bundle, 0o750)
        for child in bundle.iterdir():
            child.unlink(missing_ok=True)
        bundle.rmdir()
        raise
    return bundle


def _read_bundle_targets(bundle: Path) -> dict[str, str]:
    if bundle.is_symlink() or not bundle.is_dir():
        raise ProductionError(f"Release bundle is not a directory: {bundle}")
    values: dict[str, str] = {}
    for name in BUNDLE_NAMES:
        link = bundle / name
        if not link.is_symlink():
            raise ProductionError(f"Release bundle is missing symlink: {link}")
        try:
            values[name] = str(link.resolve(strict=True))
        except (FileNotFoundError, OSError) as error:
            raise ProductionError(f"Release bundle has a dangling target: {link}") from error
    return values


def _read_selected_bundle(root: Path) -> Path | None:
    pointer = root / RELEASE_POINTER
    if not pointer.exists() and not pointer.is_symlink():
        return None
    if not pointer.is_symlink():
        raise ProductionError(f"Production release pointer is not a symlink: {pointer}")
    try:
        bundle = pointer.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ProductionError("Production release pointer is dangling") from error
    releases_path = root / "releases"
    if releases_path.is_symlink() or not releases_path.is_dir():
        raise ProductionError("Production releases directory is invalid")
    _require_control_ownership(releases_path)
    releases = releases_path.resolve(strict=True)
    if bundle.parent != releases:
        raise ProductionError("Production release pointer escapes the bundle directory")
    _read_bundle_targets(bundle)
    return bundle


def _atomic_bundle_pointer(root: Path, bundle: Path | None) -> None:
    pointer = root / RELEASE_POINTER
    if bundle is None:
        if pointer.is_symlink():
            pointer.unlink()
            _fsync_directory(root)
        elif pointer.exists():
            raise ProductionError("Cannot remove a non-symlink production release pointer")
        return
    if bundle.parent != root / "releases" or not bundle.is_dir():
        raise ProductionError("Refusing to select an invalid production bundle")
    temporary = root / f".{RELEASE_POINTER}.{uuid.uuid4().hex}.partial"
    os.symlink(str(bundle), temporary)
    try:
        os.replace(temporary, pointer)
        _fsync_directory(root)
    finally:
        temporary.unlink(missing_ok=True)


def _journal_events(root: Path) -> list[dict]:
    journal = _read_document(_journal_path(root), _empty_journal())
    events = journal.get("events")
    if not isinstance(events, list):
        raise ProductionError("Production deployment journal is malformed")
    transaction_ids: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            raise ProductionError("Production deployment journal contains a malformed event")
        transaction_id = event.get("transaction_id")
        if not isinstance(transaction_id, str) or not re.fullmatch(r"[0-9a-f]{32}", transaction_id):
            raise ProductionError("Production deployment journal has an invalid transaction ID")
        if transaction_id in transaction_ids:
            raise ProductionError("Production deployment journal has duplicate transaction IDs")
        transaction_ids.add(transaction_id)
        if event.get("state") not in BLOCKING_JOURNAL_STATES | TERMINAL_JOURNAL_STATES:
            raise ProductionError("Production deployment journal has an invalid event state")
        if event.get("action") not in {"bootstrap", "activate", "rollback"}:
            raise ProductionError("Production deployment journal has an invalid action")
    return events


def _blocking_events(root: Path) -> list[dict]:
    events = _journal_events(root)
    return [event for event in events if event.get("state") in BLOCKING_JOURNAL_STATES]


def _append_event(root: Path, event: dict) -> None:
    journal = _read_document(_journal_path(root), _empty_journal())
    events = journal.setdefault("events", [])
    if not isinstance(events, list):
        raise ProductionError("Production deployment journal is malformed")
    events.append(event)
    # Validate the full document before it becomes durable.
    transaction_ids = [item.get("transaction_id") for item in events if isinstance(item, dict)]
    if len(transaction_ids) != len(events) or len(transaction_ids) != len(set(transaction_ids)):
        raise ProductionError("Production deployment journal event is malformed or duplicated")
    _write_json_atomic(_journal_path(root), journal)


def _update_event(root: Path, transaction_id: str, updates: dict) -> None:
    journal = _read_document(_journal_path(root), _empty_journal())
    events = journal.get("events")
    if not isinstance(events, list) or any(not isinstance(event, dict) for event in events):
        raise ProductionError("Production deployment journal is malformed")
    for event in events:
        if event.get("transaction_id") == transaction_id:
            event.update(updates)
            _write_json_atomic(_journal_path(root), journal)
            return
    raise ProductionError(f"Production transaction is missing: {transaction_id}")


def _write_sentinel(root: Path, transaction_id: str) -> None:
    sentinel = _sentinel_path(root)
    if sentinel.exists() or sentinel.is_symlink():
        raise ProductionError("A production transition sentinel already exists")
    with sentinel.open("x", encoding="ascii") as handle:
        handle.write(transaction_id + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(sentinel, 0o640)
    _fsync_directory(root)


def _remove_sentinel(root: Path) -> None:
    sentinel = _sentinel_path(root)
    if sentinel.is_symlink():
        raise ProductionError("Production transition sentinel must not be a symlink")
    sentinel.unlink(missing_ok=True)
    _fsync_directory(root)


def _require_clean_control_state(root: Path) -> None:
    blocking = _blocking_events(root)
    if blocking:
        raise ProductionError(
            f"Production journal has a blocking transaction: {blocking[-1].get('transaction_id')}"
        )
    if _sentinel_path(root).exists() or _sentinel_path(root).is_symlink():
        raise ProductionError("Production transition sentinel requires operator recovery")


def _validate_targets(deployment: ProductionDeployment) -> None:
    artifact_root = Path(deployment.artifact_root)
    code = Path(deployment.targets.code)
    runtime = Path(deployment.targets.runtime)
    warehouse = Path(deployment.targets.warehouse)
    if deployment.deployment_kind == "legacy_baseline":
        code_fingerprint = _inspect_legacy_code(code, artifact_root)
    else:
        commit, code_fingerprint = _inspect_git_code(code, artifact_root)
        if commit != deployment.code_commit:
            raise ProductionError("Code target commit changed after preparation")
    if code_fingerprint != deployment.code_fingerprint:
        raise ProductionError("Code target fingerprint changed after preparation")
    details = _require_immutable_file(warehouse, artifact_root, "warehouse target")
    if details.st_size != deployment.warehouse_byte_size:
        raise ProductionError("Warehouse target byte size changed after preparation")
    if sha256_file(warehouse) != deployment.warehouse_sha256:
        raise ProductionError("Warehouse target SHA-256 changed after preparation")
    runtime_fingerprint = _inspect_runtime(runtime, artifact_root, None)
    if runtime_fingerprint != deployment.runtime_fingerprint:
        raise ProductionError("Runtime target fingerprint changed after preparation")


def _validate_selected(
    root: Path,
    document: dict,
    deployments: list[ProductionDeployment],
) -> ProductionDeployment:
    selected_id = document.get("selected_deployment_id")
    if not selected_id:
        raise ProductionError("Production has no selected deployment")
    deployment = _deployment_by_id(deployments, selected_id)
    bundle = _read_selected_bundle(root)
    if bundle != _bundle_path(root, selected_id):
        raise ProductionError("Production bundle pointer does not match the ledger")
    if _read_bundle_targets(bundle) != deployment.targets.to_bundle_map():
        raise ProductionError("Selected bundle targets do not match the ledger")
    return deployment


def _new_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}-{uuid.uuid4().hex[:10]}"


def _transition(
    root: Path,
    *,
    action: str,
    target: ProductionDeployment,
    document: dict,
    deployments: list[ProductionDeployment],
    mutate_ledger: Callable[[], None],
) -> None:
    ledger_before = _serialize_state(document, deployments)
    from_bundle = _read_selected_bundle(root)
    to_bundle = _bundle_path(root, target.deployment_id)
    transaction_id = uuid.uuid4().hex
    event = {
        "transaction_id": transaction_id,
        "action": action,
        "state": "pending",
        "deployment_id": target.deployment_id,
        "from_bundle": str(from_bundle) if from_bundle else None,
        "to_bundle": str(to_bundle),
        "ledger_before": ledger_before,
        "started_at": utc_now(),
    }
    _write_sentinel(root, transaction_id)
    try:
        _append_event(root, event)
        _atomic_bundle_pointer(root, to_bundle)
        if _read_selected_bundle(root) != to_bundle:
            raise ProductionError("Atomic production bundle selection did not persist")
        mutate_ledger()
        _write_deployments(root, document, deployments)
        _update_event(root, transaction_id, {"state": "completed", "completed_at": utc_now()})
    except Exception as error:
        restore_errors: list[str] = []
        try:
            _atomic_bundle_pointer(root, from_bundle)
        except Exception as restore_error:
            restore_errors.append(safe_error(restore_error))
        try:
            _write_json_atomic(_state_path(root), ledger_before)
        except Exception as restore_error:
            restore_errors.append(safe_error(restore_error))
        state = "recovery_required" if restore_errors else "failed"
        journal_recorded = False
        try:
            _update_event(
                root,
                transaction_id,
                {
                    "state": state,
                    "failed_at": utc_now(),
                    "error_summary": safe_error(error),
                    "restore_errors": restore_errors,
                },
            )
            journal_recorded = True
        except Exception as journal_error:
            restore_errors.append(safe_error(journal_error))
            state = "recovery_required"
        if not restore_errors and journal_recorded:
            _remove_sentinel(root)
        raise ProductionError(
            f"Production {action} failed ({state}): {safe_error(error)}"
        ) from error
    try:
        _remove_sentinel(root)
    except Exception as error:
        raise ProductionError(
            "Production selection completed, but the startup sentinel remains; run recover"
        ) from error


def _bootstrap_preconditions(root: Path) -> None:
    root = _canonical_control_root(root)
    _require_clean_control_state(root)
    document, deployments = _read_deployments(root)
    if deployments or document.get("selected_deployment_id") or _read_selected_bundle(root):
        raise ProductionError("Production has already been bootstrapped")


def bootstrap_legacy(
    production_root: Path,
    artifact_root: Path,
    code_path: Path,
    warehouse_path: Path,
    runtime_path: Path,
    expected_warehouse_sha256: str,
    *,
    dry_run: bool = False,
) -> ProductionDeployment:
    production_root = _canonical_control_root(production_root)
    artifact_root = _canonical_directory(artifact_root, "production artifact root")
    _bootstrap_preconditions(production_root)
    code_path = _confined(code_path, artifact_root, "legacy code target")
    runtime_path = _confined(runtime_path, artifact_root, "legacy runtime target")
    warehouse_path = _confined(warehouse_path, artifact_root, "legacy warehouse target")
    code_fingerprint = _inspect_legacy_code(code_path, artifact_root)
    warehouse_details = _require_immutable_file(
        warehouse_path, artifact_root, "legacy warehouse target"
    )
    warehouse_sha = sha256_file(warehouse_path)
    if warehouse_sha != expected_warehouse_sha256:
        raise ProductionError("Legacy warehouse SHA-256 does not match the approved baseline")
    runtime_fingerprint = _inspect_runtime(runtime_path, artifact_root, None)
    deployment = ProductionDeployment(
        deployment_id=_new_id("legacy"),
        deployment_kind="legacy_baseline",
        state=DeploymentState.SELECTED,
        targets=ReleaseTargets(str(code_path), str(warehouse_path), str(runtime_path)),
        artifact_root=str(artifact_root),
        warehouse_sha256=warehouse_sha,
        warehouse_byte_size=warehouse_details.st_size,
        code_fingerprint=code_fingerprint,
        runtime_fingerprint=runtime_fingerprint,
        prepared_at=utc_now(),
    )
    if dry_run:
        return deployment
    _require_root_operator()
    with _lock(production_root):
        _bootstrap_preconditions(production_root)
        _validate_targets(deployment)
        _create_bundle(production_root, deployment)
        document, deployments = _read_deployments(production_root)

        def update() -> None:
            deployment.selected_at = utc_now()
            document["selected_deployment_id"] = deployment.deployment_id
            deployments.append(deployment)

        _transition(
            production_root,
            action="bootstrap",
            target=deployment,
            document=document,
            deployments=deployments,
            mutate_ledger=update,
        )
    return deployment


def _prepare_validation(
    production_root: Path,
    artifact_root: Path,
    data_root: Path,
    code_path: Path,
    runtime_path: Path,
    warehouse_path: Path,
    warehouse_release_id: str,
) -> tuple[ProductionDeployment, dict, list[ProductionDeployment]]:
    production_root = _canonical_control_root(production_root)
    artifact_root = _canonical_directory(artifact_root, "production artifact root")
    _require_clean_control_state(production_root)
    document, deployments = _read_deployments(production_root)
    active = _validate_selected(production_root, document, deployments)
    if active.state != DeploymentState.VERIFIED:
        raise ProductionError("Selected production deployment must be verified before preparation")
    code_path = _confined(code_path, artifact_root, "code target")
    runtime_path = _confined(runtime_path, artifact_root, "runtime target")
    code_commit, code_fingerprint = _inspect_git_code(code_path, artifact_root)
    warehouse_path, release = _validate_warehouse_release(
        data_root,
        warehouse_release_id,
        warehouse_path,
        artifact_root,
    )
    runtime_fingerprint = _inspect_runtime(
        runtime_path, artifact_root, release.get("duckdb_version")
    )
    deployment = ProductionDeployment(
        deployment_id=_new_id("deployment"),
        deployment_kind="warehouse_release",
        state=DeploymentState.PREPARED,
        targets=ReleaseTargets(str(code_path), str(warehouse_path), str(runtime_path)),
        artifact_root=str(artifact_root),
        warehouse_sha256=release["sha256"],
        warehouse_byte_size=warehouse_path.stat().st_size,
        code_fingerprint=code_fingerprint,
        runtime_fingerprint=runtime_fingerprint,
        prepared_at=utc_now(),
        code_commit=code_commit,
        warehouse_release_id=warehouse_release_id,
        warehouse_pipeline_commit=release.get("pipeline_code_commit"),
        previous_deployment_id=active.deployment_id,
    )
    return deployment, document, deployments


def prepare_release(
    production_root: Path,
    artifact_root: Path,
    data_root: Path,
    code_path: Path,
    runtime_path: Path,
    warehouse_path: Path,
    warehouse_release_id: str,
    *,
    dry_run: bool = False,
) -> ProductionDeployment:
    deployment, _, _ = _prepare_validation(
        production_root,
        artifact_root,
        data_root,
        code_path,
        runtime_path,
        warehouse_path,
        warehouse_release_id,
    )
    if dry_run:
        return deployment
    _require_root_operator()
    with _lock(production_root):
        deployment, document, deployments = _prepare_validation(
            production_root,
            artifact_root,
            data_root,
            code_path,
            runtime_path,
            warehouse_path,
            warehouse_release_id,
        )
        _create_bundle(production_root, deployment)
        deployments.append(deployment)
        _write_deployments(production_root, document, deployments)
    return deployment


def _activation_validation(
    root: Path, deployment_id: str
) -> tuple[dict, list[ProductionDeployment], ProductionDeployment, ProductionDeployment]:
    root = _canonical_control_root(root)
    _require_clean_control_state(root)
    document, deployments = _read_deployments(root)
    deployment = _deployment_by_id(deployments, deployment_id)
    if deployment.state != DeploymentState.PREPARED:
        raise ProductionError("Only a prepared deployment can be selected")
    active = _validate_selected(root, document, deployments)
    if active.state != DeploymentState.VERIFIED:
        raise ProductionError("Current production must be verified before another selection")
    if deployment.previous_deployment_id != active.deployment_id:
        raise ProductionError("Prepared deployment no longer targets the selected predecessor")
    if _bundle_path(root, deployment_id) == _read_selected_bundle(root):
        raise ProductionError("Prepared deployment is already selected")
    if _read_bundle_targets(_bundle_path(root, deployment_id)) != deployment.targets.to_bundle_map():
        raise ProductionError("Prepared bundle targets do not match the ledger")
    _validate_targets(deployment)
    return document, deployments, deployment, active


def activate_release(root: Path, deployment_id: str, *, dry_run: bool = False) -> ProductionDeployment:
    validated = _activation_validation(root, deployment_id)
    if dry_run:
        return validated[2]
    _require_root_operator()
    with _lock(root):
        document, deployments, deployment, active = _activation_validation(root, deployment_id)

        def update() -> None:
            timestamp = utc_now()
            active.state = DeploymentState.SUPERSEDED
            active.superseded_at = timestamp
            deployment.state = DeploymentState.SELECTED
            deployment.selected_at = timestamp
            document["selected_deployment_id"] = deployment.deployment_id

        _transition(
            root,
            action="activate",
            target=deployment,
            document=document,
            deployments=deployments,
            mutate_ledger=update,
        )
    return deployment


def _validate_evidence(
    root: Path,
    deployment: ProductionDeployment,
    evidence_path: Path,
) -> tuple[dict, list[dict]]:
    evidence_path = _require_absolute(evidence_path, "verification evidence")
    if evidence_path.is_symlink():
        raise ProductionError("Verification evidence must not be a symlink")
    expected_root = root / "evidence" / deployment.deployment_id
    try:
        resolved = evidence_path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ProductionError("Verification evidence does not exist") from error
    if resolved.parent != expected_root or resolved.name != "smoke.json":
        raise ProductionError("Verification evidence is outside its deployment directory")
    details = resolved.stat()
    _require_control_ownership(resolved)
    if details.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise ProductionError("Verification evidence must be root-owned and immutable")
    _require_control_ownership(resolved.parent)
    evidence = _load_json(resolved, "verification evidence")
    if evidence.get("schema_version") != 1 or evidence.get("state") != "passed":
        raise ProductionError("Verification evidence has not passed")
    if evidence.get("deployment_id") != deployment.deployment_id:
        raise ProductionError("Verification evidence deployment ID does not match")
    checks = evidence.get("checks")
    if not isinstance(checks, list) or not checks:
        raise ProductionError("Verification evidence has no checks")
    if any(not isinstance(item, dict) or item.get("state") != "passed" for item in checks):
        raise ProductionError("Verification evidence contains failed checks")
    names = [item.get("name") for item in checks]
    if any(not isinstance(name, str) for name in names):
        raise ProductionError("Verification evidence has malformed check names")
    if len(names) != len(set(names)) or not REQUIRED_VERIFICATION_CHECKS.issubset(set(names)):
        raise ProductionError("Verification evidence is missing canonical checks")
    parsed_url = urlsplit(str(evidence.get("base_url", "")))
    try:
        evidence_port = parsed_url.port
    except ValueError as error:
        raise ProductionError("Verification evidence has an invalid loopback port") from error
    if (
        parsed_url.scheme != "http"
        or parsed_url.hostname not in {"127.0.0.1", "localhost", "::1"}
        or evidence_port is None
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.path not in {"", "/"}
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise ProductionError("Verification evidence must use an exact HTTP loopback origin")
    generated = _parse_timestamp(evidence.get("generated_at"), "evidence generated_at")
    selected = _parse_timestamp(deployment.selected_at, "deployment selected_at")
    now = datetime.now(timezone.utc)
    if (
        generated < selected
        or generated < now - MAX_EVIDENCE_AGE
        or generated > now + timedelta(minutes=5)
    ):
        raise ProductionError("Verification evidence is stale or future-dated")
    return evidence, checks


def _verification_validation(
    root: Path,
    deployment_id: str,
    evidence_path: Path,
) -> tuple[dict, list[ProductionDeployment], ProductionDeployment, dict, list[dict]]:
    root = _canonical_control_root(root)
    _require_clean_control_state(root)
    document, deployments = _read_deployments(root)
    deployment = _validate_selected(root, document, deployments)
    if deployment.deployment_id != deployment_id:
        raise ProductionError("Verification target is not selected")
    if deployment.state not in {DeploymentState.SELECTED, DeploymentState.VERIFIED}:
        raise ProductionError("Only a selected deployment can be verified")
    _validate_targets(deployment)
    evidence, checks = _validate_evidence(root, deployment, evidence_path)
    return document, deployments, deployment, evidence, checks


def verify_release(
    root: Path,
    deployment_id: str,
    evidence_path: Path,
    *,
    dry_run: bool = False,
) -> ProductionDeployment:
    validated = _verification_validation(root, deployment_id, evidence_path)
    if dry_run:
        return validated[2]
    _require_root_operator()
    with _lock(root):
        document, deployments, deployment, evidence, checks = _verification_validation(
            root, deployment_id, evidence_path
        )
        deployment.state = DeploymentState.VERIFIED
        deployment.verified_at = utc_now()
        deployment.verification_summary = {
            "generated_at": evidence.get("generated_at"),
            "check_count": len(checks),
            "check_names": sorted(item["name"] for item in checks),
            "evidence_path": str(evidence_path.resolve(strict=True)),
            "evidence_sha256": sha256_file(evidence_path.resolve(strict=True)),
        }
        _write_deployments(root, document, deployments)
    return deployment


def _rollback_validation(
    root: Path,
) -> tuple[dict, list[ProductionDeployment], ProductionDeployment, ProductionDeployment]:
    root = _canonical_control_root(root)
    _require_clean_control_state(root)
    document, deployments = _read_deployments(root)
    current = _validate_selected(root, document, deployments)
    if current.state not in {DeploymentState.SELECTED, DeploymentState.VERIFIED}:
        raise ProductionError("Only a selected deployment can be rolled back")
    if not current.previous_deployment_id:
        raise ProductionError("Selected deployment has no rollback target")
    previous = _deployment_by_id(deployments, current.previous_deployment_id)
    if previous.verified_at is None or previous.state != DeploymentState.SUPERSEDED:
        raise ProductionError("Rollback target is not a previously verified deployment")
    if _read_bundle_targets(_bundle_path(root, previous.deployment_id)) != previous.targets.to_bundle_map():
        raise ProductionError("Rollback bundle targets do not match the ledger")
    _validate_targets(previous)
    return document, deployments, current, previous


def rollback_release(
    root: Path,
    *,
    dry_run: bool = False,
    deployment_id: str | None = None,
) -> ProductionDeployment:
    if deployment_id is not None:
        if not dry_run:
            raise ProductionError("A rollback deployment ID is valid only for dry-run rehearsal")
        _, _, _, predecessor = _activation_validation(root, deployment_id)
        return predecessor
    validated = _rollback_validation(root)
    if dry_run:
        return validated[3]
    _require_root_operator()
    with _lock(root):
        document, deployments, current, previous = _rollback_validation(root)

        def update() -> None:
            timestamp = utc_now()
            current.state = DeploymentState.ROLLED_BACK
            current.rollback_at = timestamp
            previous.state = DeploymentState.SELECTED
            previous.selected_at = timestamp
            previous.superseded_at = None
            document["selected_deployment_id"] = previous.deployment_id

        _transition(
            root,
            action="rollback",
            target=previous,
            document=document,
            deployments=deployments,
            mutate_ledger=update,
        )
    return previous


def _recover_validation(root: Path) -> tuple[dict | None, str | None]:
    root = _canonical_control_root(root)
    blocking = _blocking_events(root)
    sentinel = _sentinel_path(root)
    sentinel_id = None
    if sentinel.exists() and not sentinel.is_symlink():
        sentinel_id = sentinel.read_text().strip()
    elif sentinel.is_symlink():
        raise ProductionError("Transition sentinel must not be a symlink")
    if len(blocking) > 1:
        raise ProductionError("More than one production transaction requires recovery")
    if blocking:
        if sentinel.exists() and sentinel_id != blocking[0].get("transaction_id"):
            raise ProductionError("Transition sentinel does not match the blocking transaction")
        return blocking[0], sentinel_id
    if sentinel_id:
        matches = [event for event in _journal_events(root) if event.get("transaction_id") == sentinel_id]
        if len(matches) > 1:
            raise ProductionError("Startup sentinel identifies duplicate transactions")
        if matches and matches[0].get("state") not in TERMINAL_JOURNAL_STATES:
            raise ProductionError("Startup sentinel identifies a non-terminal transaction")
        document, deployments = _read_deployments(root)
        if document.get("selected_deployment_id"):
            selected = _validate_selected(root, document, deployments)
            _validate_targets(selected)
        elif deployments or _read_selected_bundle(root):
            raise ProductionError("Orphan sentinel has inconsistent production state")
        return None, sentinel_id
    raise ProductionError("Production has no transaction requiring recovery")


def recover_pending(root: Path, *, dry_run: bool = False) -> dict:
    event, sentinel_id = _recover_validation(root)
    if dry_run:
        return event or {"transaction_id": sentinel_id, "state": "completed_sentinel"}
    _require_root_operator()
    with _lock(root):
        event, sentinel_id = _recover_validation(root)
        if event is None:
            _remove_sentinel(root)
            return {"transaction_id": sentinel_id, "state": "sentinel_cleared"}
        ledger_before = event.get("ledger_before")
        if not isinstance(ledger_before, dict):
            raise ProductionError("Recovery transaction has no complete ledger snapshot")
        if ledger_before.get("schema_version") != SCHEMA_VERSION:
            raise ProductionError("Recovery ledger snapshot has an unsupported schema version")
        values = ledger_before.get("deployments")
        if not isinstance(values, list):
            raise ProductionError("Recovery ledger snapshot is malformed")
        deployments = [ProductionDeployment.from_dict(value) for value in values]
        _validate_ledger(ledger_before, deployments)
        selected_id = ledger_before.get("selected_deployment_id")
        from_bundle_value = event.get("from_bundle")
        if selected_id:
            selected = _deployment_by_id(deployments, selected_id)
            _validate_targets(selected)
            from_bundle = _bundle_path(root, selected_id)
            if str(from_bundle) != from_bundle_value:
                raise ProductionError("Recovery bundle does not match the prior ledger")
            if _read_bundle_targets(from_bundle) != selected.targets.to_bundle_map():
                raise ProductionError("Recovery bundle targets do not match the prior ledger")
        else:
            from_bundle = None
            if from_bundle_value is not None:
                raise ProductionError("Recovery snapshot and prior bundle disagree")
        try:
            _atomic_bundle_pointer(root, from_bundle)
            _write_json_atomic(_state_path(root), ledger_before)
            _update_event(
                root,
                event["transaction_id"],
                {"state": "recovered", "recovered_at": utc_now()},
            )
            _remove_sentinel(root)
        except Exception as error:
            try:
                _update_event(
                    root,
                    event["transaction_id"],
                    {
                        "state": "recovery_required",
                        "error_summary": safe_error(error),
                    },
                )
            finally:
                raise ProductionError(f"Production recovery failed: {safe_error(error)}") from error
        return event


def production_status(root: Path) -> dict:
    root = _require_absolute(root, "production root")
    if not root.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "healthy": False,
            "control_plane_healthy": False,
            "selected_deployment_id": None,
            "selected_state": None,
            "pointer_matches_ledger": False,
            "blocking_transactions": 0,
            "transition_sentinel": False,
            "artifact_integrity": "unknown",
            "deployment_count": 0,
            "service_observation": "not_checked",
        }
    root = _canonical_control_root(root)
    document, deployments = _read_deployments(root)
    selected_id = document.get("selected_deployment_id")
    selected = _deployment_by_id(deployments, selected_id) if selected_id else None
    bundle = _read_selected_bundle(root)
    pointer_match = selected is not None and bundle == _bundle_path(root, selected.deployment_id)
    integrity = "unknown"
    error_summary = None
    if selected and pointer_match:
        try:
            if _read_bundle_targets(bundle) != selected.targets.to_bundle_map():
                raise ProductionError("Bundle targets differ from the ledger")
            _validate_targets(selected)
            integrity = "passed"
        except ProductionError as error:
            integrity = "failed"
            error_summary = safe_error(error)
    blocking = len(_blocking_events(root))
    sentinel = _sentinel_path(root).exists() or _sentinel_path(root).is_symlink()
    healthy = bool(
        selected
        and selected.state == DeploymentState.VERIFIED
        and pointer_match
        and integrity == "passed"
        and blocking == 0
        and not sentinel
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "healthy": healthy,
        "control_plane_healthy": healthy,
        "selected_deployment_id": selected_id,
        "selected_state": selected.state.value if selected else None,
        "selected_code_commit": selected.code_commit if selected else None,
        "selected_warehouse_release_id": selected.warehouse_release_id if selected else None,
        "last_verified_at": selected.verified_at if selected else None,
        "bundle_path": str(bundle) if bundle else None,
        "pointer_matches_ledger": pointer_match,
        "blocking_transactions": blocking,
        "transition_sentinel": sentinel,
        "artifact_integrity": integrity,
        "error_summary": error_summary,
        "deployment_count": len(deployments),
        "service_observation": "not_checked",
    }


def startup_check(root: Path) -> dict:
    """Fail closed before systemd starts a selected application bundle."""
    root = _canonical_control_root(root)
    _require_clean_control_state(root)
    document, deployments = _read_deployments(root)
    selected = _validate_selected(root, document, deployments)
    if selected.state not in {DeploymentState.SELECTED, DeploymentState.VERIFIED}:
        raise ProductionError("Production startup target is not selected")
    _validate_targets(selected)
    return {
        "schema_version": SCHEMA_VERSION,
        "state": "passed",
        "selected_deployment_id": selected.deployment_id,
        "selected_state": selected.state.value,
    }


def _print_result(value: object, as_json: bool) -> None:
    if as_json:
        if isinstance(value, ProductionDeployment):
            value = value.to_dict()
        print(json.dumps(value, indent=2, sort_keys=True))
        return
    if isinstance(value, ProductionDeployment):
        print(f"Deployment: {value.deployment_id}")
        print(f"State: {value.state.value}")
        print(f"Code: {value.code_commit or value.targets.code}")
        print(f"Warehouse: {value.warehouse_release_id or value.targets.warehouse}")
    else:
        print(json.dumps(value, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crash-safe production release manager")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--production-root", type=Path, required=True)
    status.add_argument("--json", action="store_true")

    startup = subparsers.add_parser("startup-check")
    startup.add_argument("--production-root", type=Path, required=True)
    startup.add_argument("--json", action="store_true")

    bootstrap = subparsers.add_parser("bootstrap")
    bootstrap.add_argument("--production-root", type=Path, required=True)
    bootstrap.add_argument("--artifact-root", type=Path, required=True)
    bootstrap.add_argument("--code-path", type=Path, required=True)
    bootstrap.add_argument("--warehouse-path", type=Path, required=True)
    bootstrap.add_argument("--warehouse-sha256", required=True)
    bootstrap.add_argument("--runtime-path", type=Path, required=True)
    bootstrap.add_argument("--dry-run", action="store_true")
    bootstrap.add_argument("--json", action="store_true")

    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--production-root", type=Path, required=True)
    prepare.add_argument("--artifact-root", type=Path, required=True)
    prepare.add_argument("--data-root", type=Path, required=True)
    prepare.add_argument("--code-path", type=Path, required=True)
    prepare.add_argument("--runtime-path", type=Path, required=True)
    prepare.add_argument("--warehouse-path", type=Path, required=True)
    prepare.add_argument("--warehouse-release-id", required=True)
    prepare.add_argument("--dry-run", action="store_true")
    prepare.add_argument("--json", action="store_true")

    for name in ("activate", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--production-root", type=Path, required=True)
        command.add_argument("--deployment-id", required=True)
        if name == "verify":
            command.add_argument("--evidence", type=Path, required=True)
        command.add_argument("--dry-run", action="store_true")
        command.add_argument("--json", action="store_true")

    for name in ("rollback", "recover"):
        command = subparsers.add_parser(name)
        command.add_argument("--production-root", type=Path, required=True)
        command.add_argument("--dry-run", action="store_true")
        command.add_argument("--json", action="store_true")
        if name == "rollback":
            command.add_argument("--deployment-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "status":
            result = production_status(args.production_root)
            exit_code = 0 if result["healthy"] else 1
        elif args.command == "startup-check":
            result = startup_check(args.production_root)
            exit_code = 0
        elif args.command == "bootstrap":
            result = bootstrap_legacy(
                args.production_root,
                args.artifact_root,
                args.code_path,
                args.warehouse_path,
                args.runtime_path,
                args.warehouse_sha256,
                dry_run=args.dry_run,
            )
            exit_code = 0
        elif args.command == "prepare":
            result = prepare_release(
                args.production_root,
                args.artifact_root,
                args.data_root,
                args.code_path,
                args.runtime_path,
                args.warehouse_path,
                args.warehouse_release_id,
                dry_run=args.dry_run,
            )
            exit_code = 0
        elif args.command == "activate":
            result = activate_release(
                args.production_root, args.deployment_id, dry_run=args.dry_run
            )
            exit_code = 0
        elif args.command == "verify":
            result = verify_release(
                args.production_root,
                args.deployment_id,
                args.evidence,
                dry_run=args.dry_run,
            )
            exit_code = 0
        elif args.command == "rollback":
            result = rollback_release(
                args.production_root,
                dry_run=args.dry_run,
                deployment_id=args.deployment_id,
            )
            exit_code = 0
        else:
            result = recover_pending(args.production_root, dry_run=args.dry_run)
            exit_code = 0
    except Exception as error:
        if getattr(args, "json", False):
            print(json.dumps({"state": "error", "error_summary": safe_error(error)}))
        else:
            print(f"Production error: {safe_error(error)}", file=sys.stderr)
        return 2
    _print_result(result, getattr(args, "json", False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
