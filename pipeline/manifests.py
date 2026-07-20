"""Versioned local run-manifest model for immutable data-platform releases."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .discovery import safe_error
from .source_registry import SOURCE_REGISTRY

MANIFEST_SCHEMA_VERSION = 1


class ValidationState(str, Enum):
    NOT_RUN = "not_run"
    PASSED = "passed"
    FAILED = "failed"


class PromotionState(str, Enum):
    NOT_PROMOTED = "not_promoted"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass(slots=True)
class RunManifest:
    run_id: str
    release_id: str
    source_id: str
    publisher: str
    publisher_version: str
    source_data_period: str
    discovery_timestamp: str
    publisher_release_timestamp: str | None = None
    retrieval_timestamp: str | None = None
    source_url: str | None = None
    byte_size: int | None = None
    sha256: str | None = None
    schema_fingerprint: str | None = None
    row_counts: dict[str, int] = field(default_factory=dict)
    pipeline_code_commit: str | None = None
    validation_state: ValidationState = ValidationState.NOT_RUN
    validation_timestamp: str | None = None
    promotion_state: PromotionState = PromotionState.NOT_PROMOTED
    promotion_timestamp: str | None = None
    active_release_id: str | None = None
    failure_timestamp: str | None = None
    rollback_timestamp: str | None = None
    operator_summary: str | None = None
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id or not self.release_id:
            raise ValueError("run_id and release_id are required")
        if self.source_id not in SOURCE_REGISTRY:
            raise ValueError(f"Unknown source_id: {self.source_id}")
        if not self.publisher_version:
            raise ValueError("publisher_version is required")
        if self.byte_size is not None and self.byte_size < 0:
            raise ValueError("byte_size cannot be negative")
        if self.sha256 is not None and not _is_sha256(self.sha256):
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        if any(not isinstance(value, int) or value < 0 for value in self.row_counts.values()):
            raise ValueError("row_counts values must be non-negative integers")
        if self.error_summary is not None:
            self.error_summary = safe_error(self.error_summary)
        if self.operator_summary is not None:
            self.operator_summary = safe_error(self.operator_summary)

    @property
    def proves_active_installation(self) -> bool:
        """Whether this record is sufficient evidence of the installed version."""
        return (
            self.validation_state == ValidationState.PASSED
            and self.promotion_state == PromotionState.ACTIVE
            and bool(self.retrieval_timestamp)
            and bool(self.active_release_id)
            and self.release_id == self.active_release_id
        )

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "release_id": self.release_id,
            "source_id": self.source_id,
            "publisher": self.publisher,
            "publisher_version": self.publisher_version,
            "source_data_period": self.source_data_period,
            "publisher_release_timestamp": self.publisher_release_timestamp,
            "discovery_timestamp": self.discovery_timestamp,
            "retrieval_timestamp": self.retrieval_timestamp,
            "source_url": self.source_url,
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "schema_fingerprint": self.schema_fingerprint,
            "row_counts": dict(sorted(self.row_counts.items())),
            "pipeline_code_commit": self.pipeline_code_commit,
            "validation_state": self.validation_state.value,
            "validation_timestamp": self.validation_timestamp,
            "promotion_state": self.promotion_state.value,
            "promotion_timestamp": self.promotion_timestamp,
            "active_release_id": self.active_release_id,
            "failure_timestamp": self.failure_timestamp,
            "rollback_timestamp": self.rollback_timestamp,
            "operator_summary": self.operator_summary,
            "error_summary": self.error_summary,
        }

    @classmethod
    def from_dict(cls, value: dict) -> RunManifest:
        if not isinstance(value, dict):
            raise ValueError("manifest row must be an object")
        try:
            return cls(
                run_id=value["run_id"],
                release_id=value["release_id"],
                source_id=value["source_id"],
                publisher=value["publisher"],
                publisher_version=value["publisher_version"],
                source_data_period=value["source_data_period"],
                publisher_release_timestamp=value.get("publisher_release_timestamp"),
                discovery_timestamp=value["discovery_timestamp"],
                retrieval_timestamp=value.get("retrieval_timestamp"),
                source_url=value.get("source_url"),
                byte_size=value.get("byte_size"),
                sha256=value.get("sha256"),
                schema_fingerprint=value.get("schema_fingerprint"),
                row_counts=value.get("row_counts") or {},
                pipeline_code_commit=value.get("pipeline_code_commit"),
                validation_state=ValidationState(
                    value.get("validation_state", ValidationState.NOT_RUN.value)
                ),
                validation_timestamp=value.get("validation_timestamp"),
                promotion_state=PromotionState(
                    value.get("promotion_state", PromotionState.NOT_PROMOTED.value)
                ),
                promotion_timestamp=value.get("promotion_timestamp"),
                active_release_id=value.get("active_release_id"),
                failure_timestamp=value.get("failure_timestamp"),
                rollback_timestamp=value.get("rollback_timestamp"),
                operator_summary=value.get("operator_summary"),
                error_summary=value.get("error_summary"),
            )
        except KeyError as error:
            raise ValueError(f"manifest row is missing required field: {error.args[0]}") from error


@dataclass(slots=True)
class ManifestDocument:
    manifests: list[RunManifest] = field(default_factory=list)
    schema_version: int = MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MANIFEST_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported manifest schema_version {self.schema_version}; "
                f"expected {MANIFEST_SCHEMA_VERSION}"
            )

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "manifests": [manifest.to_dict() for manifest in self.manifests],
        }

    @classmethod
    def from_dict(cls, value: dict) -> ManifestDocument:
        if not isinstance(value, dict):
            raise ValueError("manifest document must be an object")
        version = value.get("schema_version")
        if version != MANIFEST_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported manifest schema_version {version!r}; "
                f"expected {MANIFEST_SCHEMA_VERSION}"
            )
        rows = value.get("manifests")
        if not isinstance(rows, list):
            raise ValueError("manifest document is missing the manifests array")
        return cls(
            manifests=[RunManifest.from_dict(row) for row in rows],
            schema_version=version,
        )

    def proven_active(self, source_id: str) -> tuple[RunManifest | None, str | None]:
        candidates = [
            manifest
            for manifest in self.manifests
            if manifest.source_id == source_id and manifest.proves_active_installation
        ]
        if not candidates:
            return None, "No validated active manifest proves the installed publisher version."
        release_ids = {manifest.release_id for manifest in candidates}
        if len(release_ids) != 1:
            return None, "Multiple active release IDs make installed provenance ambiguous."
        return max(
            candidates,
            key=lambda manifest: (
                manifest.promotion_timestamp or "",
                manifest.retrieval_timestamp or "",
                manifest.run_id,
            ),
        ), None


class ManifestStore:
    """JSON-backed manifest store; reading a missing path is non-mutating and empty."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> ManifestDocument:
        if not self.path.exists():
            return ManifestDocument()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid manifest JSON at {self.path}") from error
        return ManifestDocument.from_dict(payload)

    def save(self, document: ManifestDocument) -> None:
        """Atomically save local manifest state without touching a warehouse file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(document.to_dict(), indent=2, sort_keys=True) + "\n"
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=self.path.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)


def _is_sha256(value: str) -> bool:
    return bool(len(value) == 64 and all(character in "0123456789abcdef" for character in value))
