"""Immutable source acquisition for staged data-platform runs.

The first supported source is CMS Hospital Enrollments. Downloads are bounded,
streamed to a ``.partial`` file, and atomically renamed only after the response is
complete. This module never opens DuckDB and never promotes a release.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from .discovery import ReleaseMetadata, safe_error, utc_now
from .manifests import (
    ManifestDocument,
    ManifestStore,
    RunManifest,
    ValidationState,
)
from .source_registry import SOURCE_REGISTRY

SUPPORTED_ACQUISITION_SOURCES = frozenset({"cms_hospital_enrollments"})
DEFAULT_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
USER_AGENT = "cms-data-platform-acquisition/1.0 (+staged immutable download)"

HOSPITAL_REQUIRED_COLUMNS = (
    "ENROLLMENT ID",
    "NPI",
    "CCN",
    "ORGANIZATION NAME",
    "STATE",
)


class AcquisitionError(RuntimeError):
    """The publisher artifact could not be safely retrieved or validated."""


@dataclass(frozen=True, slots=True)
class AcquisitionResult:
    manifest: RunManifest
    run_directory: Path
    artifact_path: Path
    run_manifest_path: Path
    manifest_store_path: Path

    def to_dict(self) -> dict:
        return {
            "manifest": self.manifest.to_dict(),
            "run_directory": str(self.run_directory),
            "artifact_path": str(self.artifact_path),
            "run_manifest_path": str(self.run_manifest_path),
            "manifest_store_path": str(self.manifest_store_path),
        }


@dataclass(frozen=True, slots=True)
class ArtifactInspection:
    byte_size: int
    sha256: str
    schema_fingerprint: str
    source_encoding: str
    row_count: int


def make_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return f"{timestamp:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def release_id(source_id: str, publisher_version: str) -> str:
    digest = hashlib.sha256(
        f"{source_id}\0{publisher_version}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{source_id}-{digest}"


def pipeline_commit(repository_root: Path | None = None) -> str | None:
    """Return the exact checked-out commit without requiring a writable repository."""
    root = repository_root or Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip().lower()
    if len(commit) == 40 and all(character in "0123456789abcdef" for character in commit):
        return commit
    return None


def _validate_source_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "data.cms.gov":
        raise AcquisitionError(
            "Hospital Enrollments artifact must use HTTPS on data.cms.gov"
        )
    return parsed.hostname


def _response_length(headers: object) -> int | None:
    value = headers.get("Content-Length") if hasattr(headers, "get") else None
    if value is None:
        return None
    try:
        length = int(value)
    except (TypeError, ValueError) as error:
        raise AcquisitionError("Publisher response has an invalid Content-Length") from error
    if length < 0:
        raise AcquisitionError("Publisher response has a negative Content-Length")
    return length


def download_artifact(
    release: ReleaseMetadata,
    destination: Path,
    *,
    max_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    timeout: float = 60.0,
) -> tuple[int, str]:
    """Stream one publisher artifact to an atomic immutable destination."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    expected_host = _validate_source_url(release.source_url)
    if destination.exists():
        raise AcquisitionError(f"Refusing to overwrite existing artifact: {destination}")
    partial = destination.with_name(destination.name + ".partial")
    if partial.exists():
        raise AcquisitionError(f"Refusing to overwrite partial artifact: {partial}")

    request = urllib.request.Request(
        release.source_url,
        headers={"User-Agent": USER_AGENT, "Accept": "text/csv,*/*;q=0.1"},
    )
    digest = hashlib.sha256()
    byte_size = 0
    announced_size: int | None = None
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            final_url = response.geturl()
            final_host = urlparse(final_url).hostname
            if urlparse(final_url).scheme != "https" or final_host != expected_host:
                raise AcquisitionError("Publisher redirected the artifact outside data.cms.gov")
            announced_size = _response_length(response.headers)
            if announced_size is not None and announced_size > max_bytes:
                raise AcquisitionError(
                    f"Publisher artifact exceeds the {max_bytes} byte acquisition limit"
                )
            with partial.open("xb") as handle:
                while True:
                    chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                    if not chunk:
                        break
                    byte_size += len(chunk)
                    if byte_size > max_bytes:
                        raise AcquisitionError(
                            f"Publisher artifact exceeds the {max_bytes} byte acquisition limit"
                        )
                    digest.update(chunk)
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
    except urllib.error.URLError as error:
        raise AcquisitionError(f"Publisher artifact unavailable: {safe_error(error)}") from error

    if byte_size == 0:
        raise AcquisitionError("Publisher returned an empty artifact")
    if announced_size is not None and byte_size != announced_size:
        raise AcquisitionError(
            f"Publisher response ended at {byte_size} bytes; expected {announced_size}"
        )
    os.replace(partial, destination)
    return byte_size, digest.hexdigest()


def inspect_hospital_enrollments(path: Path) -> ArtifactInspection:
    """Validate the Hospital Enrollments CSV and derive durable artifact metrics."""
    digest = hashlib.sha256()
    byte_size = 0
    with path.open("rb") as raw:
        while chunk := raw.read(DOWNLOAD_CHUNK_BYTES):
            byte_size += len(chunk)
            digest.update(chunk)

    source_encoding: str | None = None
    for candidate in ("utf-8-sig", "cp1252"):
        try:
            with path.open("r", encoding=candidate, newline="") as handle:
                while handle.read(DOWNLOAD_CHUNK_BYTES):
                    pass
        except UnicodeDecodeError:
            continue
        source_encoding = candidate
        break
    if source_encoding is None:
        raise AcquisitionError(
            "Hospital Enrollments CSV is neither valid UTF-8 nor Windows-1252"
        )

    try:
        with path.open("r", encoding=source_encoding, newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise AcquisitionError("Hospital Enrollments CSV has no header")
            if any(not column.strip() for column in header):
                raise AcquisitionError("Hospital Enrollments CSV has a blank column name")
            if len(header) != len(set(header)):
                raise AcquisitionError("Hospital Enrollments CSV has duplicate column names")
            missing = [column for column in HOSPITAL_REQUIRED_COLUMNS if column not in header]
            if missing:
                raise AcquisitionError(
                    "Hospital Enrollments CSV is missing required columns: "
                    + ", ".join(missing)
                )
            npi_index = header.index("NPI")
            row_count = 0
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise AcquisitionError(
                        f"Hospital Enrollments CSV row {line_number} has {len(row)} fields; "
                        f"expected {len(header)}"
                    )
                npi = row[npi_index].strip()
                if not (len(npi) == 10 and npi.isdigit()):
                    raise AcquisitionError(
                        f"Hospital Enrollments CSV row {line_number} has an invalid NPI"
                    )
                row_count += 1
    except csv.Error as error:
        raise AcquisitionError(f"Hospital Enrollments CSV is malformed: {safe_error(error)}") from error

    if row_count == 0:
        raise AcquisitionError("Hospital Enrollments CSV contains no data rows")
    schema_payload = json.dumps(header, ensure_ascii=False, separators=(",", ":"))
    schema_fingerprint = "sha256:" + hashlib.sha256(
        schema_payload.encode("utf-8")
    ).hexdigest()
    return ArtifactInspection(
        byte_size=byte_size,
        sha256=digest.hexdigest(),
        schema_fingerprint=schema_fingerprint,
        source_encoding=source_encoding,
        row_count=row_count,
    )


def _save_manifest(manifest: RunManifest, run_path: Path, store_path: Path) -> None:
    run_document = ManifestDocument(manifests=[manifest])
    ManifestStore(run_path).save(run_document)
    store = ManifestStore(store_path)
    document = store.load()
    if any(existing.run_id == manifest.run_id for existing in document.manifests):
        raise AcquisitionError(f"Manifest store already contains run {manifest.run_id}")
    document.manifests.append(manifest)
    store.save(document)


def acquire_release(
    release: ReleaseMetadata,
    *,
    discovery_timestamp: str,
    data_root: Path,
    manifest_path: Path,
    max_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    timeout: float = 60.0,
    code_commit: str | None = None,
) -> AcquisitionResult:
    """Acquire and validate one supported release into a new immutable run directory."""
    if release.source_id not in SUPPORTED_ACQUISITION_SOURCES:
        raise AcquisitionError(
            f"Immutable acquisition is not implemented for {release.source_id}"
        )
    spec = SOURCE_REGISTRY[release.source_id]
    run_id = make_run_id()
    run_directory = data_root / "runs" / release.source_id / run_id
    artifact_path = run_directory / "source.csv"
    run_manifest_path = run_directory / "manifest.json"
    run_directory.mkdir(parents=True, exist_ok=False)

    manifest = RunManifest(
        run_id=run_id,
        release_id=release_id(release.source_id, release.publisher_version),
        source_id=release.source_id,
        publisher=spec.publisher.value,
        publisher_version=release.publisher_version,
        source_data_period=release.source_data_period,
        publisher_release_timestamp=release.publisher_release_timestamp,
        discovery_timestamp=discovery_timestamp,
        source_url=release.source_url,
        pipeline_code_commit=code_commit or pipeline_commit(),
    )
    try:
        downloaded_size, downloaded_sha = download_artifact(
            release,
            artifact_path,
            max_bytes=max_bytes,
            timeout=timeout,
        )
        manifest.retrieval_timestamp = utc_now()
        manifest.byte_size = downloaded_size
        manifest.sha256 = downloaded_sha
        inspection = inspect_hospital_enrollments(artifact_path)
        if inspection.byte_size != downloaded_size or inspection.sha256 != downloaded_sha:
            raise AcquisitionError("Artifact changed between retrieval and validation")
        manifest.schema_fingerprint = inspection.schema_fingerprint
        manifest.source_encoding = inspection.source_encoding
        manifest.row_counts = {"source_rows": inspection.row_count}
        manifest.validation_state = ValidationState.PASSED
        manifest.validation_timestamp = utc_now()
    except (AcquisitionError, OSError, ValueError) as error:
        manifest.validation_state = ValidationState.FAILED
        manifest.failure_timestamp = utc_now()
        manifest.error_summary = safe_error(error)
        _save_manifest(manifest, run_manifest_path, manifest_path)
        raise AcquisitionError(manifest.error_summary) from error

    _save_manifest(manifest, run_manifest_path, manifest_path)
    return AcquisitionResult(
        manifest=manifest,
        run_directory=run_directory,
        artifact_path=artifact_path,
        run_manifest_path=run_manifest_path,
        manifest_store_path=manifest_path,
    )
