"""Release identity for the serving process.

Resolves which promoted release this process is serving, using evidence the
production control plane already places on the box: the `release-current`
bundle pointer (whose target directory name is the deployment ID), the
deployment ledger, and the deployment-scoped source-manifest snapshot.

Everything here is read-only observation. This module never takes
control-plane locks, never writes, and never opens DuckDB. When evidence is
absent or unreadable it degrades to partial metadata or `503`, never a guess.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, HTTPException

from pipeline.manifests import ManifestStore

logger = logging.getLogger(__name__)

# Names the shape of data responses served by this API. Bump whenever any
# endpoint's response shape changes; consumers cache responses keyed on
# (release_id, representation_version), so a shape change without a bump would
# serve wrong data from a correct cache.
REPRESENTATION_VERSION = 1

# Matches pipeline/production_manager.py DEPLOYMENT_ID_PATTERN.
DEPLOYMENT_ID_PATTERN = re.compile(r"^[a-z]+-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{10}$")

PRODUCTION_LEDGER_NAME = "deployments.json"
SOURCE_MANIFEST_EVIDENCE = "source-manifests.json"


@dataclass(frozen=True)
class ReleaseMetadata:
    release_id: str
    promoted_at: str | None = None
    verified_at: str | None = None
    warehouse_release_id: str | None = None
    checksum: str | None = None
    pipeline_ref: str | None = None
    source_vintages: dict[str, str] = field(default_factory=dict)
    compatibility: str = "current"

    def to_payload(self) -> dict:
        return {
            "release_id": self.release_id,
            "promoted_at": self.promoted_at,
            "verified_at": self.verified_at,
            "representation_version": REPRESENTATION_VERSION,
            "source_vintages": dict(sorted(self.source_vintages.items())),
            "build": {
                "checksum": self.checksum,
                "pipeline_ref": self.pipeline_ref,
                "warehouse_release_id": self.warehouse_release_id,
            },
            "compatibility": self.compatibility,
        }


def _ledger_record(production_root: Path, deployment_id: str) -> dict:
    """Return the ledger record for one deployment, or {} when unreadable."""

    path = production_root / PRODUCTION_LEDGER_NAME
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        logger.warning("Production ledger is unavailable at %s: %s", path, error)
        return {}
    deployments = payload.get("deployments") if isinstance(payload, dict) else None
    if not isinstance(deployments, list):
        logger.warning("Production ledger at %s is malformed", path)
        return {}
    for record in deployments:
        if isinstance(record, dict) and record.get("deployment_id") == deployment_id:
            return record
    return {}


def _source_vintages(production_root: Path, deployment_id: str) -> dict[str, str]:
    """Return proven-active source periods from the deployment-scoped snapshot."""

    path = production_root / "evidence" / deployment_id / SOURCE_MANIFEST_EVIDENCE
    try:
        document = ManifestStore(path).load()
    except (OSError, ValueError) as error:
        logger.warning("Source-manifest evidence is unavailable at %s: %s", path, error)
        return {}
    vintages: dict[str, str] = {}
    for source_id in sorted({manifest.source_id for manifest in document.manifests}):
        manifest, _ = document.proven_active(source_id)
        if manifest is not None:
            vintages[source_id] = manifest.source_data_period
    return vintages


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _bundle_release(duckdb_path: str) -> ReleaseMetadata | None:
    """Derive release identity from the production bundle containing the warehouse.

    The deployed `DUCKDB_PATH` is `<production-root>/release-current/warehouse`;
    the pointer resolves into `<production-root>/releases/<deployment-id>/`, so
    the bundle directory name is the release identity even when the ledger and
    evidence snapshot are unreadable.
    """

    try:
        bundle = Path(duckdb_path).parent.resolve(strict=True)
    except OSError:
        return None
    if not DEPLOYMENT_ID_PATTERN.fullmatch(bundle.name) or bundle.parent.name != "releases":
        return None
    production_root = bundle.parent.parent
    record = _ledger_record(production_root, bundle.name)
    return ReleaseMetadata(
        release_id=bundle.name,
        promoted_at=_optional_str(record.get("selected_at")),
        verified_at=_optional_str(record.get("verified_at")),
        warehouse_release_id=_optional_str(record.get("warehouse_release_id")),
        checksum=_optional_str(record.get("warehouse_sha256")),
        pipeline_ref=_optional_str(record.get("warehouse_pipeline_commit")),
        source_vintages=_source_vintages(production_root, bundle.name),
    )


def _override_release(path: Path) -> ReleaseMetadata | None:
    """Load explicit release metadata stamped by a deploy step (non-bundle boxes)."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        logger.warning("Release metadata override at %s could not be read: %s", path, error)
        return None
    if not isinstance(payload, dict) or not _optional_str(payload.get("release_id")):
        logger.warning("Release metadata override at %s is malformed", path)
        return None
    build = payload.get("build") if isinstance(payload.get("build"), dict) else {}
    vintages = payload.get("source_vintages")
    return ReleaseMetadata(
        release_id=payload["release_id"],
        promoted_at=_optional_str(payload.get("promoted_at")),
        verified_at=_optional_str(payload.get("verified_at")),
        warehouse_release_id=_optional_str(build.get("warehouse_release_id")),
        checksum=_optional_str(build.get("checksum")),
        pipeline_ref=_optional_str(build.get("pipeline_ref")),
        source_vintages={
            key: value
            for key, value in (vintages or {}).items()
            if isinstance(key, str) and isinstance(value, str)
        }
        if isinstance(vintages, dict)
        else {},
        compatibility=_optional_str(payload.get("compatibility")) or "current",
    )


def load_release_metadata(duckdb_path: str) -> ReleaseMetadata | None:
    """Resolve the served release, or None when no evidence names one."""

    override = os.getenv("CMS_RELEASE_METADATA_PATH", "")
    if override:
        return _override_release(Path(override))
    return _bundle_release(duckdb_path)


def make_release_resolver(duckdb_path: str) -> Callable[[], ReleaseMetadata | None]:
    """Cache a successful resolution for the process lifetime.

    A release changes only through the production cutover, which restarts the
    service, so a resolved identity is stable until shutdown. Failed
    resolutions are retried per request so a box repaired in place recovers
    without a restart.
    """

    cache: list[ReleaseMetadata] = []

    def resolve() -> ReleaseMetadata | None:
        if cache:
            return cache[0]
        metadata = load_release_metadata(duckdb_path)
        if metadata is not None:
            cache.append(metadata)
        return metadata

    return resolve


def get_release_router(
    resolve_metadata: Callable[[], ReleaseMetadata | None],
) -> APIRouter:
    """Create the read-only release-manifest router."""

    router = APIRouter(tags=["Release"])

    @router.get("/release")
    async def release():
        metadata = resolve_metadata()
        if metadata is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Release metadata is unavailable for the warehouse this process "
                    "serves; the release manifest cannot be reported."
                ),
            )
        return metadata.to_payload()

    return router
