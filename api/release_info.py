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
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from pipeline.manifests import ManifestStore

logger = logging.getLogger(__name__)

# Names the shape of data responses served by this API. Bump whenever any
# endpoint's response shape changes; consumers cache responses keyed on
# (release_id, representation_version), so a shape change without a bump would
# serve wrong data from a correct cache.
#
# v2 — /profiles/{npi} affiliation breadth (81fcf37): group rows gained
# `reassignment_size` and `sources`, and the payload gained a top-level
# `hospital_affiliations` list. The endpoint is untyped in OpenAPI, so the
# snapshot comparison cannot see this change; the bump follows the operating
# doctrine ("any response-shape change must bump") rather than the snapshot.
#
# v3 — NPPES-first provider discovery and profiles. `/profiles/search` now
# discovers through NPPES and uses DAC as enrichment; source values are
# `nppes`, `nppes + medicare`, or the rare DAC-only fallback `medicare`.
# `/profiles/{npi}` accepts NPPES-only clinicians, and each locations[] row
# gains `sources` (`dac` / `nppes` / `dac + nppes`) via a DAC ⟕ NPPES join.
# v4 — `/practices/capabilities` advertises the release-gated
# `utilization_browse_v2` capability. Provider Search uses this as a fail-closed
# preflight before it enables the snapshot-pinned reference browser.
# v5 — `/industry/search` accepts an exact repeated ZIP5 scope and returns
# `applied_scope.zip_codes` as proof that the adapter applied the requested
# CMS-primary-practice boundary. Legacy city/state responses omit the field.
REPRESENTATION_VERSION = 5

# Matches pipeline/production_manager.py DEPLOYMENT_ID_PATTERN.
DEPLOYMENT_ID_PATTERN = re.compile(r"^[a-z]+-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{10}$")

PRODUCTION_LEDGER_NAME = "deployments.json"
SOURCE_MANIFEST_EVIDENCE = "source-manifests.json"


class ReleaseBuild(BaseModel):
    """Provenance of the warehouse this process serves."""

    checksum: str | None = None
    pipeline_ref: str | None = None
    warehouse_release_id: str | None = None


class ReleaseManifest(BaseModel):
    """The `GET /release` payload.

    Declared as a model so the response shape is published in the OpenAPI
    document and covered by the `representation_version` snapshot; a payload
    change here is then a test failure rather than a silent cache hazard.
    """

    release_id: str
    promoted_at: str | None = None
    verified_at: str | None = None
    representation_version: int
    source_vintages: dict[str, str] = Field(default_factory=dict)
    build: ReleaseBuild
    compatibility: str


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


def _bundle_verified_at(duckdb_path: str, release_id: str) -> str | None:
    """Refresh verification only when the bundle still names the cached release."""

    try:
        bundle = Path(duckdb_path).parent.resolve(strict=True)
    except OSError:
        return None
    if bundle.name != release_id or bundle.parent.name != "releases":
        return None
    record = _ledger_record(bundle.parent.parent, release_id)
    return _optional_str(record.get("verified_at"))


def make_release_resolver(duckdb_path: str) -> Callable[[], ReleaseMetadata | None]:
    """Cache stable release identity while completing pending verification metadata.

    A release changes only through the production cutover, which restarts the
    service, so a resolved identity is stable until shutdown. The cutover
    verifies the selected deployment after smoke; until that transition is
    recorded, refresh only ``verified_at`` from the same bundle's ledger.
    Failed resolutions are retried per request so a box repaired in place
    recovers without a restart.
    """

    cache: list[ReleaseMetadata] = []

    def resolve() -> ReleaseMetadata | None:
        if cache:
            metadata = cache[0]
            if metadata.promoted_at is not None and metadata.verified_at is None:
                verified_at = _bundle_verified_at(duckdb_path, metadata.release_id)
                if verified_at is not None:
                    metadata = replace(metadata, verified_at=verified_at)
                    cache[0] = metadata
            return metadata
        metadata = load_release_metadata(duckdb_path)
        if metadata is not None:
            cache.append(metadata)
        return metadata

    return resolve


# Responses may be stored but must be revalidated before reuse; within one
# immutable release the revalidation is a free 304.
CACHE_CONTROL = "private, no-cache"

# Process-status and documentation surfaces are not immutable data responses.
_VALIDATOR_EXEMPT_PATHS = frozenset(
    {"/health", "/release", "/openapi.json", "/redoc"}
)


def release_etag(metadata: ReleaseMetadata) -> str:
    """Strong validator naming both the data (release) and its shape."""

    return f'"{metadata.release_id}:{REPRESENTATION_VERSION}"'


def _if_none_match_matches(header: str | None, etag: str) -> bool:
    if not header:
        return False
    if header.strip() == "*":
        return True
    candidates = (candidate.strip() for candidate in header.split(","))
    return etag in {
        candidate[2:] if candidate.startswith("W/") else candidate
        for candidate in candidates
    }


class ReleaseCacheMiddleware(BaseHTTPMiddleware):
    """Attach release-keyed cache validators and honor conditional requests.

    A GET whose `If-None-Match` names the current release returns 304 without
    entering the route — no DuckDB query runs. The short-circuit applies only
    to authorized requests: auth dependencies run inside routing, after this
    middleware, so an unauthorized conditional request must fall through to
    receive its normal 401 rather than a confirmation that its validator is
    current.
    """

    def __init__(
        self,
        app,
        resolve_metadata: Callable[[], ReleaseMetadata | None],
        is_authorized: Callable[[Request], bool],
    ) -> None:
        super().__init__(app)
        self._resolve_metadata = resolve_metadata
        self._is_authorized = is_authorized

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if (
            request.method != "GET"
            or path in _VALIDATOR_EXEMPT_PATHS
            or path.startswith("/docs")
        ):
            return await call_next(request)
        metadata = self._resolve_metadata()
        if metadata is None or not self._is_authorized(request):
            return await call_next(request)
        etag = release_etag(metadata)
        if _if_none_match_matches(request.headers.get("if-none-match"), etag):
            return Response(
                status_code=304,
                headers={"ETag": etag, "Cache-Control": CACHE_CONTROL},
            )
        response = await call_next(request)
        if response.status_code == 200:
            response.headers["ETag"] = etag
            response.headers["Cache-Control"] = CACHE_CONTROL
        return response


def get_release_router(
    resolve_metadata: Callable[[], ReleaseMetadata | None],
) -> APIRouter:
    """Create the read-only release-manifest router."""

    router = APIRouter(tags=["Release"])

    @router.get("/release", response_model=ReleaseManifest)
    async def release(response: Response):
        metadata = resolve_metadata()
        if metadata is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Release metadata is unavailable for the warehouse this process "
                    "serves; the release manifest cannot be reported."
                ),
            )
        response.headers["Cache-Control"] = "no-store"
        return metadata.to_payload()

    return router
