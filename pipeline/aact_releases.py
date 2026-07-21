"""Prepare immutable AACT restore artifacts from a verified publisher archive."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from .acquisition import pipeline_commit
from .archive_sources import extracted_member, verified_archive_runs
from .discovery import utc_now
from .releases import ReleaseError, sha256_file


AACT_SOURCE_ID = "aact_clinical_trials_snapshot"
AACT_RELEASE_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class AactRelease:
    aact_release_id: str
    source_run_id: str
    source_release_id: str
    source_data_period: str
    source_pipeline_code_commit: str
    preparation_code_commit: str
    prepared_at: str
    dump_path: str
    dump_byte_size: int
    dump_sha256: str
    data_dictionary_path: str
    data_dictionary_byte_size: int
    data_dictionary_sha256: str
    validation_state: str = "passed"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AactPrepareResult:
    release: AactRelease
    release_directory: Path
    release_manifest_path: Path

    def to_dict(self) -> dict:
        return {
            "schema_version": AACT_RELEASE_SCHEMA_VERSION,
            "release": self.release.to_dict(),
            "release_directory": str(self.release_directory),
            "release_manifest_path": str(self.release_manifest_path),
        }


def _atomic_json(path: Path, value: dict) -> None:
    serialized = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _release_id(
    source_data_period: str, source_release_id: str, preparation_commit: str
) -> str:
    compact = source_data_period.replace("-", "")
    suffix = hashlib.sha256(
        f"{source_release_id}\0{preparation_commit}".encode()
    ).hexdigest()[:10]
    return f"aact-{compact}-{suffix}"


def prepare_aact_release(
    *,
    data_root: Path,
    source_run_id: str,
    output_root: Path,
) -> AactPrepareResult:
    """Extract the restore dump and dictionary into a sealed versioned directory."""
    ((manifest, archive),) = verified_archive_runs(
        data_root,
        (source_run_id,),
        allowed_sources=frozenset({AACT_SOURCE_ID}),
    )
    if not manifest.pipeline_code_commit:
        raise ReleaseError("AACT source manifest lacks a pipeline code commit")
    preparation_commit = pipeline_commit()
    if not preparation_commit:
        raise ReleaseError("AACT preparation requires a full pipeline Git commit")
    release_id = _release_id(
        manifest.source_data_period, manifest.release_id, preparation_commit
    )
    releases_root = output_root / "aact-releases"
    release_directory = releases_root / release_id
    partial = releases_root / f".{release_id}.partial"
    if release_directory.exists() or partial.exists():
        raise ReleaseError(f"AACT release already exists: {release_id}")
    releases_root.mkdir(parents=True, exist_ok=True)
    partial.mkdir(mode=0o750)
    try:
        with extracted_member(
            data_root,
            manifest,
            archive,
            r"(^|/)postgres\.dmp$",
            suffix=".dmp",
        ) as source_dump:
            dump = partial / "postgres.dmp"
            with source_dump.open("rb") as source, dump.open("xb") as target:
                for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
        with dump.open("rb") as handle:
            dump_magic = handle.read(5)
        if dump_magic != b"PGDMP":
            raise ReleaseError("AACT postgres.dmp does not have PostgreSQL custom-dump magic")

        with extracted_member(
            data_root,
            manifest,
            archive,
            r"(^|/)data_dictionary\.csv$",
            suffix=".csv",
        ) as source_dictionary:
            dictionary = partial / "data_dictionary.csv"
            with source_dictionary.open("rb") as source, dictionary.open("xb") as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
        with dictionary.open("rb") as handle:
            dictionary_prefix = handle.read(100)
        if not dictionary_prefix.strip():
            raise ReleaseError("AACT data dictionary is empty")

        release = AactRelease(
            aact_release_id=release_id,
            source_run_id=manifest.run_id,
            source_release_id=manifest.release_id,
            source_data_period=manifest.source_data_period,
            source_pipeline_code_commit=manifest.pipeline_code_commit,
            preparation_code_commit=preparation_commit,
            prepared_at=utc_now(),
            dump_path="postgres.dmp",
            dump_byte_size=dump.stat().st_size,
            dump_sha256=sha256_file(dump),
            data_dictionary_path="data_dictionary.csv",
            data_dictionary_byte_size=dictionary.stat().st_size,
            data_dictionary_sha256=sha256_file(dictionary),
        )
        release_manifest = partial / "release.json"
        _atomic_json(
            release_manifest,
            {
                "schema_version": AACT_RELEASE_SCHEMA_VERSION,
                "release": release.to_dict(),
            },
        )
        for path in (dump, dictionary, release_manifest):
            os.chmod(path, 0o440)
        os.replace(partial, release_directory)
        os.chmod(release_directory, 0o550)
    except Exception:
        if partial.exists():
            partial.chmod(0o700)
            for path in partial.glob("*"):
                path.chmod(0o600)
                path.unlink()
            partial.rmdir()
        raise

    return AactPrepareResult(
        release=release,
        release_directory=release_directory,
        release_manifest_path=release_directory / "release.json",
    )
