"""Immutable ZIP acquisition for NPPES and AACT publisher snapshots."""

from __future__ import annotations

import hashlib
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .acquisition import (
    AcquisitionError,
    AcquisitionResult,
    _save_manifest,
    download_artifact,
    make_run_id,
    pipeline_commit,
    release_id,
)
from .discovery import ReleaseMetadata, safe_error, utc_now
from .manifests import PromotionState, RunManifest, ValidationState
from .source_registry import SOURCE_REGISTRY


@dataclass(frozen=True, slots=True)
class ArchiveProfile:
    label: str
    required_member_patterns: tuple[str, ...]
    max_download_bytes: int
    max_uncompressed_bytes: int


GIB = 1024 * 1024 * 1024
ARCHIVE_PROFILES = {
    "nppes_monthly_v2": ArchiveProfile(
        "NPPES monthly V2 archive",
        (r"(^|/)npidata_pfile_\d{8}-\d{8}\.csv$",),
        3 * GIB,
        40 * GIB,
    ),
    "nppes_weekly_incremental_v2": ArchiveProfile(
        "NPPES weekly V2 archive",
        (r"(^|/)npidata_pfile_\d{8}-\d{8}\.csv$",),
        2 * GIB,
        10 * GIB,
    ),
    "aact_clinical_trials_snapshot": ArchiveProfile(
        "AACT snapshot archive",
        (r"(^|/)postgres\.dmp$", r"(^|/)data_dictionary\.csv$"),
        5 * GIB,
        20 * GIB,
    ),
    "open_payments_general": ArchiveProfile(
        "Open Payments archive",
        (r"(^|/).*GNRL.*\.csv$",),
        3 * GIB,
        30 * GIB,
    ),
    "open_payments_research": ArchiveProfile(
        "Open Payments archive",
        (r"(^|/).*RSRCH.*\.csv$",),
        3 * GIB,
        30 * GIB,
    ),
    "open_payments_ownership": ArchiveProfile(
        "Open Payments archive",
        (r"(^|/).*(OWNRSHP|OWNERSHIP).*\.csv$",),
        3 * GIB,
        30 * GIB,
    ),
}
SUPPORTED_ARCHIVE_ACQUISITION_SOURCES = frozenset(ARCHIVE_PROFILES)


@dataclass(frozen=True, slots=True)
class ArchiveInspection:
    byte_size: int
    sha256: str
    schema_fingerprint: str
    member_count: int
    uncompressed_bytes: int


def inspect_archive(path: Path, profile: ArchiveProfile) -> ArchiveInspection:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if not members:
                raise AcquisitionError(f"{profile.label} contains no members")
            names = []
            uncompressed = 0
            for member in members:
                parts = PurePosixPath(member.filename).parts
                if member.filename.startswith("/") or ".." in parts:
                    raise AcquisitionError(f"{profile.label} contains an unsafe member path")
                if member.flag_bits & 0x1:
                    raise AcquisitionError(f"{profile.label} contains an encrypted member")
                names.append(member.filename)
                uncompressed += member.file_size
            if uncompressed > profile.max_uncompressed_bytes:
                raise AcquisitionError(f"{profile.label} exceeds its uncompressed size ceiling")
            for pattern in profile.required_member_patterns:
                if not any(re.search(pattern, name, re.I) for name in names):
                    raise AcquisitionError(
                        f"{profile.label} is missing required member pattern: {pattern}"
                    )
            bad = archive.testzip()
            if bad is not None:
                raise AcquisitionError(f"{profile.label} failed CRC validation at {bad}")
    except zipfile.BadZipFile as error:
        raise AcquisitionError(f"{profile.label} is not a valid ZIP archive") from error
    payload = json.dumps(sorted(names), separators=(",", ":"))
    return ArchiveInspection(
        byte_size=size,
        sha256=digest.hexdigest(),
        schema_fingerprint="sha256:" + hashlib.sha256(payload.encode()).hexdigest(),
        member_count=len(members),
        uncompressed_bytes=uncompressed,
    )


def acquire_archive_release(
    release: ReleaseMetadata,
    *,
    discovery_timestamp: str,
    data_root: Path,
    manifest_path: Path,
    max_bytes: int | None = None,
    timeout: float = 60.0,
    code_commit: str | None = None,
) -> AcquisitionResult:
    if release.source_id not in ARCHIVE_PROFILES:
        raise AcquisitionError(f"Archive acquisition is not implemented for {release.source_id}")
    profile = ARCHIVE_PROFILES[release.source_id]
    run_id = make_run_id()
    run_directory = data_root / "runs" / release.source_id / run_id
    artifact = run_directory / "source.zip"
    run_manifest = run_directory / "manifest.json"
    run_directory.mkdir(parents=True, exist_ok=False)
    manifest = RunManifest(
        run_id=run_id,
        release_id=release_id(release.source_id, release.publisher_version),
        source_id=release.source_id,
        publisher=SOURCE_REGISTRY[release.source_id].publisher.value,
        publisher_version=release.publisher_version,
        source_data_period=release.source_data_period,
        publisher_release_timestamp=release.publisher_release_timestamp,
        discovery_timestamp=discovery_timestamp,
        source_url=release.source_url,
        pipeline_code_commit=code_commit or pipeline_commit(),
        promotion_state=PromotionState.NOT_PROMOTED,
    )
    try:
        size, digest = download_artifact(
            release,
            artifact,
            max_bytes=profile.max_download_bytes if max_bytes is None else max_bytes,
            timeout=timeout,
        )
        manifest.retrieval_timestamp = utc_now()
        manifest.byte_size = size
        manifest.sha256 = digest
        inspection = inspect_archive(artifact, profile)
        if inspection.byte_size != size or inspection.sha256 != digest:
            raise AcquisitionError("Archive changed between retrieval and validation")
        manifest.schema_fingerprint = inspection.schema_fingerprint
        manifest.source_encoding = "binary:zip"
        manifest.row_counts = {
            "archive_members": inspection.member_count,
            "uncompressed_bytes": inspection.uncompressed_bytes,
        }
        manifest.validation_state = ValidationState.PASSED
        manifest.validation_timestamp = utc_now()
    except (AcquisitionError, OSError, ValueError) as error:
        manifest.validation_state = ValidationState.FAILED
        manifest.failure_timestamp = utc_now()
        manifest.error_summary = safe_error(error)
        _save_manifest(manifest, run_manifest, manifest_path)
        raise AcquisitionError(manifest.error_summary) from error
    _save_manifest(manifest, run_manifest, manifest_path)
    return AcquisitionResult(manifest, run_directory, artifact, run_manifest, manifest_path)
