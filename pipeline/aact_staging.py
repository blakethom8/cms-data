"""Restore and validate an immutable AACT artifact into a new candidate database.

This module deliberately has no promotion or drop operation.  A failed restore
leaves the explicitly named candidate available for inspection and keeps the
active ``aact`` database untouched.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .aact_releases import AACT_RELEASE_SCHEMA_VERSION
from .discovery import safe_error, utc_now
from .releases import ReleaseError, sha256_file


DOCKER = "/usr/bin/docker"
POSTGRES_IMAGE = "postgres:17-bookworm"
DEFAULT_CONTAINER = "aact-postgres"
DEFAULT_READER_ROLE = "aact_reader"
PASSWORD_FILE = Path("/etc/aact/postgres_password")
DATABASE_PATTERN = re.compile(r"^aact_candidate_[0-9]{8}_[a-f0-9]{10}$")


@dataclass(frozen=True, slots=True)
class AactStageResult:
    aact_release_id: str
    candidate_database: str
    source_data_period: str
    study_count: int
    latest_update_date: str
    ctgov_table_count: int
    reader_count: int
    restored_at: str
    validation_state: str = "passed"

    def to_dict(self) -> dict:
        return {
            "aact_release_id": self.aact_release_id,
            "candidate_database": self.candidate_database,
            "source_data_period": self.source_data_period,
            "study_count": self.study_count,
            "latest_update_date": self.latest_update_date,
            "ctgov_table_count": self.ctgov_table_count,
            "reader_count": self.reader_count,
            "restored_at": self.restored_at,
            "validation_state": self.validation_state,
        }


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def candidate_database_name(aact_release_id: str) -> str:
    match = re.fullmatch(r"aact-([0-9]{8})-([a-f0-9]{10})", aact_release_id)
    if not match:
        raise ReleaseError("AACT release ID cannot produce a safe candidate database name")
    return f"aact_candidate_{match.group(1)}_{match.group(2)}"


def _load_release(path: Path) -> tuple[dict, Path]:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ReleaseError("AACT release manifest must be an absolute regular file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseError(f"AACT release manifest is unreadable: {safe_error(error)}") from error
    if payload.get("schema_version") != AACT_RELEASE_SCHEMA_VERSION:
        raise ReleaseError("AACT release manifest has an unsupported schema")
    release = payload.get("release")
    if not isinstance(release, dict):
        raise ReleaseError("AACT release manifest is missing its release object")
    for commit_field in ("source_pipeline_code_commit", "preparation_code_commit"):
        value = release.get(commit_field)
        if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{40}", value):
            raise ReleaseError(f"AACT release manifest lacks {commit_field}")
    release_directory = path.parent.resolve(strict=True)
    for prefix in ("dump", "data_dictionary"):
        relative = release.get(f"{prefix}_path")
        expected_size = release.get(f"{prefix}_byte_size")
        expected_sha = release.get(f"{prefix}_sha256")
        if not isinstance(relative, str) or Path(relative).name != relative:
            raise ReleaseError(f"AACT release has an unsafe {prefix} path")
        artifact = release_directory / relative
        if artifact.is_symlink() or not artifact.is_file():
            raise ReleaseError(f"AACT release {prefix} artifact is missing")
        if artifact.stat().st_size != expected_size or sha256_file(artifact) != expected_sha:
            raise ReleaseError(f"AACT release {prefix} artifact changed after preparation")
    return release, release_directory


def _run(
    runner: CommandRunner,
    command: list[str],
    *,
    capture: bool = False,
    log_handle=None,
) -> subprocess.CompletedProcess[str]:
    try:
        return runner(
            command,
            check=True,
            text=True,
            capture_output=capture,
            stdout=None if capture else log_handle,
            stderr=None if capture else subprocess.STDOUT,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ReleaseError(f"AACT staging command failed: {safe_error(error)}") from error


def _psql(
    container: str,
    database: str,
    sql: str,
    *,
    user: str = "aact_admin",
) -> list[str]:
    return [
        DOCKER,
        "exec",
        container,
        "psql",
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        user,
        "-d",
        database,
        "-Atc",
        sql,
    ]


def stage_aact_database(
    *,
    release_manifest_path: Path,
    restore_log_path: Path,
    container: str = DEFAULT_CONTAINER,
    reader_role: str = DEFAULT_READER_ROLE,
    minimum_study_count: int = 500_000,
    runner: CommandRunner = subprocess.run,
) -> AactStageResult:
    """Restore a sealed dump into a new database and validate read-only access."""
    release, release_directory = _load_release(release_manifest_path)
    database = candidate_database_name(str(release.get("aact_release_id", "")))
    if not DATABASE_PATTERN.fullmatch(database):
        raise ReleaseError("Unsafe AACT candidate database name")
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", reader_role):
        raise ReleaseError("Unsafe AACT reader role")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", container):
        raise ReleaseError("Unsafe AACT container name")
    if minimum_study_count <= 0:
        raise ReleaseError("AACT minimum study count must be positive")
    if not restore_log_path.is_absolute() or restore_log_path.exists():
        raise ReleaseError("AACT restore log must be a new absolute path")
    if not PASSWORD_FILE.is_file():
        raise ReleaseError("AACT PostgreSQL password file is unavailable")

    exists = _run(
        runner,
        _psql(
            container,
            "postgres",
            f"SELECT 1 FROM pg_database WHERE datname = '{database}'",
        ),
        capture=True,
    ).stdout.strip()
    if exists:
        raise ReleaseError(f"AACT candidate database already exists: {database}")

    restore_log_path.parent.mkdir(parents=True, exist_ok=True)
    with restore_log_path.open("x", encoding="utf-8") as log:
        _run(
            runner,
            [DOCKER, "exec", container, "createdb", "-U", "aact_admin", database],
            log_handle=log,
        )
        _run(
            runner,
            [
                DOCKER,
                "run",
                "--rm",
                "--network",
                "host",
                "-v",
                f"{release_directory}:/snapshot:ro",
                "-v",
                f"{PASSWORD_FILE}:/run/secrets/postgres_password:ro",
                POSTGRES_IMAGE,
                "bash",
                "-ceu",
                (
                    'export PGPASSWORD="$(cat /run/secrets/postgres_password)"; '
                    'export PGOPTIONS="-c synchronous_commit=off"; '
                    "pg_restore --host 127.0.0.1 --port 5433 --username aact_admin "
                    '--dbname "$1" --no-owner --no-privileges --exit-on-error '
                    "--jobs=4 /snapshot/postgres.dmp"
                ),
                "bash",
                database,
            ],
            log_handle=log,
        )
        _run(
            runner,
            [
                DOCKER,
                "exec",
                container,
                "vacuumdb",
                "-U",
                "aact_admin",
                "-d",
                database,
                "--analyze-in-stages",
                "--jobs=4",
            ],
            log_handle=log,
        )

    study_count = int(
        _run(
            runner,
            _psql(container, database, "SELECT count(*) FROM ctgov.studies"),
            capture=True,
        ).stdout.strip()
    )
    latest_update = _run(
        runner,
        _psql(
            container,
            database,
            "SELECT max(last_update_posted_date) FROM ctgov.studies",
        ),
        capture=True,
    ).stdout.strip()
    table_count = int(
        _run(
            runner,
            _psql(
                container,
                database,
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'ctgov'",
            ),
            capture=True,
        ).stdout.strip()
    )
    if study_count < minimum_study_count:
        raise ReleaseError(f"AACT candidate has only {study_count} studies")
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", latest_update):
        raise ReleaseError("AACT candidate has no valid latest update date")
    if table_count < 25:
        raise ReleaseError(f"AACT candidate has only {table_count} ctgov tables")

    grants = (
        f"GRANT USAGE ON SCHEMA ctgov TO {reader_role}; "
        f"GRANT SELECT ON ALL TABLES IN SCHEMA ctgov TO {reader_role}; "
        f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ctgov TO {reader_role};"
    )
    _run(runner, _psql(container, database, grants), capture=True)
    _run(
        runner,
        _psql(
            container,
            "postgres",
            f"REVOKE CONNECT ON DATABASE {database} FROM PUBLIC; "
            f"GRANT CONNECT ON DATABASE {database} TO {reader_role};",
        ),
        capture=True,
    )
    reader_count = int(
        _run(
            runner,
            _psql(
                container,
                database,
                "SELECT count(*) FROM ctgov.studies",
                user=reader_role,
            ),
            capture=True,
        ).stdout.strip()
    )
    if reader_count != study_count:
        raise ReleaseError("AACT reader validation does not match the restored study count")
    os.chmod(restore_log_path, 0o440)
    return AactStageResult(
        aact_release_id=release["aact_release_id"],
        candidate_database=database,
        source_data_period=release["source_data_period"],
        study_count=study_count,
        latest_update_date=latest_update,
        ctgov_table_count=table_count,
        reader_count=reader_count,
        restored_at=utc_now(),
    )


def write_stage_evidence(path: Path, result: AactStageResult) -> None:
    if not path.is_absolute() or path.exists():
        raise ReleaseError("AACT stage evidence must be a new absolute path")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(
            {
                "schema_version": 1,
                "result": result.to_dict(),
            },
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o440)
    os.replace(temporary, path)
