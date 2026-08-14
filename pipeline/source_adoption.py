"""Safely adopt a retained validated CMS source run into managed staging.

This is a reconciliation path for source bytes that already passed acquisition
validation and were used by production before the current managed staging store
was established.  It never downloads data, changes a warehouse, or promotes a
release.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .acquisition import AcquisitionError, CMS_CSV_PROFILES, inspect_cms_csv
from .manifests import (
    ManifestDocument,
    ManifestStore,
    PromotionState,
    RunManifest,
    ValidationState,
)

COPY_CHUNK_BYTES = 8 * 1024 * 1024


class SourceAdoptionError(RuntimeError):
    """A retained source run could not be safely adopted into staging."""


@dataclass(frozen=True, slots=True)
class SourceAdoptionResult:
    source_id: str
    run_id: str
    artifact_path: Path
    run_manifest_path: Path
    manifest_store_path: Path
    byte_size: int
    sha256: str
    source_data_period: str
    production_release_id: str
    adopted: bool

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "artifact_path": str(self.artifact_path),
            "run_manifest_path": str(self.run_manifest_path),
            "manifest_store_path": str(self.manifest_store_path),
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "source_data_period": self.source_data_period,
            "production_release_id": self.production_release_id,
            "adopted": self.adopted,
        }


def _require_absolute_regular_file(path: Path, label: str) -> None:
    if not path.is_absolute():
        raise SourceAdoptionError(f"{label} must be an absolute path")
    if path.is_symlink() or not path.is_file():
        raise SourceAdoptionError(f"{label} must be a regular non-symlink file")
    if path.resolve(strict=True) != path:
        raise SourceAdoptionError(f"{label} must not traverse symlinked directories")


def _ensure_managed_directory(path: Path, label: str) -> None:
    if not path.is_absolute():
        raise SourceAdoptionError(f"{label} must be an absolute path")
    path.mkdir(exist_ok=True)
    if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
        raise SourceAdoptionError(f"{label} must be a non-symlink directory")


def _require_safe_path_segment(value: str, label: str) -> None:
    if not value or value in {".", ".."} or Path(value).name != value:
        raise SourceAdoptionError(f"{label} must be one safe path segment")


def _exact_manifest(
    path: Path,
    *,
    source_id: str,
    run_id: str,
    label: str,
) -> RunManifest:
    _require_absolute_regular_file(path, label)
    matches = [
        manifest
        for manifest in ManifestStore(path).load().manifests
        if manifest.source_id == source_id and manifest.run_id == run_id
    ]
    if len(matches) != 1:
        raise SourceAdoptionError(
            f"{label} must contain exactly one manifest for {source_id}/{run_id}; "
            f"found {len(matches)}"
        )
    return matches[0]


def _immutable_provenance(manifest: RunManifest) -> dict:
    value = manifest.to_dict()
    for field in (
        "promotion_state",
        "promotion_timestamp",
        "active_release_id",
        "failure_timestamp",
        "rollback_timestamp",
        "operator_summary",
        "error_summary",
    ):
        value.pop(field)
    return value


def _verify_manifest_pair(
    acquisition: RunManifest,
    production: RunManifest,
) -> None:
    if acquisition.validation_state != ValidationState.PASSED:
        raise SourceAdoptionError("Retained acquisition manifest has not passed validation")
    if acquisition.promotion_state != PromotionState.NOT_PROMOTED:
        raise SourceAdoptionError(
            "Retained acquisition manifest must preserve its original not_promoted state"
        )
    if not production.proves_active_installation:
        raise SourceAdoptionError(
            "Production evidence does not prove an active validated installation"
        )
    if _immutable_provenance(acquisition) != _immutable_provenance(production):
        raise SourceAdoptionError(
            "Production evidence does not match the retained acquisition manifest"
        )
    if (
        acquisition.source_id not in CMS_CSV_PROFILES
        or not acquisition.sha256
        or acquisition.byte_size is None
        or not acquisition.schema_fingerprint
        or not acquisition.source_encoding
        or not acquisition.retrieval_timestamp
        or "source_rows" not in acquisition.row_counts
    ):
        raise SourceAdoptionError(
            "Retained acquisition manifest lacks required validation provenance"
        )


def _verify_source_bytes(path: Path, manifest: RunManifest) -> None:
    try:
        inspection = inspect_cms_csv(
            path,
            profile=CMS_CSV_PROFILES[manifest.source_id],
        )
    except AcquisitionError as error:
        raise SourceAdoptionError(
            "Retained source artifact does not match its acquisition manifest"
        ) from error
    if (
        inspection.byte_size != manifest.byte_size
        or inspection.sha256 != manifest.sha256
        or inspection.schema_fingerprint != manifest.schema_fingerprint
        or inspection.source_encoding != manifest.source_encoding
        or inspection.row_count != manifest.row_counts.get("source_rows")
        or inspection.invalid_identifier_rows
        != manifest.row_counts.get("invalid_identifier_rows", 0)
    ):
        raise SourceAdoptionError(
            "Retained source artifact does not match its acquisition manifest"
        )


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
            0o660,
        )
    except OSError as error:
        raise SourceAdoptionError(
            f"Source-adoption lock is not a safe regular file: {path}"
        ) from error
    with os.fdopen(descriptor, "a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SourceAdoptionError(
                f"Another source-adoption operation holds {path}"
            ) from error
        yield


def _copy_verified(source: Path, destination: Path, manifest: RunManifest) -> None:
    partial = destination.with_name(destination.name + ".partial")
    if partial.exists() or partial.is_symlink():
        if partial.is_symlink() or not partial.is_file():
            raise SourceAdoptionError(
                f"Unsafe interrupted source-adoption artifact: {partial}"
            )
        partial.unlink()
    digest = hashlib.sha256()
    copied = 0
    try:
        with source.open("rb") as source_handle, partial.open("xb") as target_handle:
            while chunk := source_handle.read(COPY_CHUNK_BYTES):
                target_handle.write(chunk)
                digest.update(chunk)
                copied += len(chunk)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        if copied != manifest.byte_size or digest.hexdigest() != manifest.sha256:
            raise SourceAdoptionError(
                "Retained source changed while it was copied into managed staging"
            )
        os.chmod(partial, 0o440)
        try:
            os.link(partial, destination)
        except FileExistsError as error:
            raise SourceAdoptionError(
                f"Managed source destination appeared during adoption: {destination}"
            ) from error
        partial.unlink()
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        partial.unlink(missing_ok=True)


def _same_manifest(existing: RunManifest, expected: RunManifest) -> bool:
    return existing.to_dict() == expected.to_dict()


def adopt_validated_source_run(
    *,
    data_root: Path,
    source_manifest_path: Path,
    source_artifact_path: Path,
    production_evidence_path: Path,
    expected_source_id: str,
    expected_run_id: str,
) -> SourceAdoptionResult:
    """Copy an exact retained run into staging after two-source provenance proof."""
    _require_safe_path_segment(expected_source_id, "Expected source ID")
    _require_safe_path_segment(expected_run_id, "Expected run ID")
    if expected_source_id not in CMS_CSV_PROFILES:
        raise SourceAdoptionError(f"Unsupported CMS source: {expected_source_id}")
    acquisition = _exact_manifest(
        source_manifest_path,
        source_id=expected_source_id,
        run_id=expected_run_id,
        label="Retained acquisition manifest",
    )
    production = _exact_manifest(
        production_evidence_path,
        source_id=expected_source_id,
        run_id=expected_run_id,
        label="Production source-manifest evidence",
    )
    _verify_manifest_pair(acquisition, production)
    _require_absolute_regular_file(source_artifact_path, "Retained source artifact")
    _verify_source_bytes(source_artifact_path, acquisition)

    manifest_store_path = data_root / "manifests.json"
    run_directory = data_root / "runs" / expected_source_id / expected_run_id
    artifact_path = run_directory / "source.csv"
    run_manifest_path = run_directory / "manifest.json"
    adopted = False

    _ensure_managed_directory(data_root, "Managed data root")
    _ensure_managed_directory(data_root / "locks", "Managed lock directory")
    with _exclusive_lock(data_root / "locks" / "source-adoption.lock"):
        _ensure_managed_directory(data_root / "runs", "Managed runs directory")
        _ensure_managed_directory(
            data_root / "runs" / expected_source_id,
            "Managed source directory",
        )
        _ensure_managed_directory(run_directory, "Managed run directory")
        if artifact_path.exists() or artifact_path.is_symlink():
            if artifact_path.is_symlink() or not artifact_path.is_file():
                raise SourceAdoptionError(
                    f"Managed source destination is not a regular file: {artifact_path}"
                )
            _verify_source_bytes(artifact_path, acquisition)
        else:
            _copy_verified(source_artifact_path, artifact_path, acquisition)
            adopted = True

        if run_manifest_path.is_symlink() or (
            run_manifest_path.exists() and not run_manifest_path.is_file()
        ):
            raise SourceAdoptionError(
                "Managed per-run manifest path is not a regular file"
            )
        run_document = ManifestStore(run_manifest_path).load()
        if run_document.manifests:
            if (
                len(run_document.manifests) != 1
                or not _same_manifest(run_document.manifests[0], acquisition)
            ):
                raise SourceAdoptionError(
                    "Managed per-run manifest conflicts with retained provenance"
                )
        else:
            ManifestStore(run_manifest_path).save(
                ManifestDocument(manifests=[acquisition])
            )
            adopted = True

        if manifest_store_path.is_symlink() or (
            manifest_store_path.exists() and not manifest_store_path.is_file()
        ):
            raise SourceAdoptionError(
                "Managed manifest store path is not a regular file"
            )
        store = ManifestStore(manifest_store_path)
        document = store.load()
        matches = [
            manifest
            for manifest in document.manifests
            if manifest.run_id == expected_run_id
        ]
        if matches:
            if len(matches) != 1 or not _same_manifest(matches[0], acquisition):
                raise SourceAdoptionError(
                    "Managed manifest store conflicts with retained provenance"
                )
        else:
            document.manifests.append(acquisition)
            store.save(document)
            adopted = True

    return SourceAdoptionResult(
        source_id=acquisition.source_id,
        run_id=acquisition.run_id,
        artifact_path=artifact_path,
        run_manifest_path=run_manifest_path,
        manifest_store_path=manifest_store_path,
        byte_size=acquisition.byte_size,
        sha256=acquisition.sha256,
        source_data_period=acquisition.source_data_period,
        production_release_id=production.active_release_id or production.release_id,
        adopted=adopted,
    )
