"""Read-only operating evidence for the internal data command center.

This router deliberately exposes observation, not execution. Acquisition, release
construction, comparison, promotion, and rollback remain operator-controlled
pipeline actions outside the serving process.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from fastapi import APIRouter, Query

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
from pipeline.manifests import ManifestDocument, ManifestStore, RunManifest
from pipeline.source_registry import SOURCE_REGISTRY, SourceSpec


logger = logging.getLogger(__name__)


DEFAULT_MANIFEST_PATH = REPOSITORY_ROOT / "data" / "manifests.json"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _configured_manifest_path() -> Path:
    return Path(os.getenv("CMS_MANIFEST_PATH", str(DEFAULT_MANIFEST_PATH)))


def _load_manifests(path: Path) -> tuple[ManifestDocument, str | None]:
    try:
        return ManifestStore(path).load(), None
    except (OSError, ValueError) as error:
        logger.warning("Manifest evidence is unavailable at %s: %s", path, error)
        return (
            ManifestDocument(),
            "Manifest evidence exists but could not be read or validated.",
        )


def _sort_timestamp(manifest: RunManifest) -> str:
    return max(
        manifest.promotion_timestamp or "",
        manifest.validation_timestamp or "",
        manifest.retrieval_timestamp or "",
        manifest.discovery_timestamp,
    )


def _latest_by_source(document: ManifestDocument) -> dict[str, RunManifest]:
    latest: dict[str, RunManifest] = {}
    for manifest in document.manifests:
        existing = latest.get(manifest.source_id)
        if existing is None or _sort_timestamp(manifest) > _sort_timestamp(existing):
            latest[manifest.source_id] = manifest
    return latest


def _manifest_summary(manifest: RunManifest) -> dict:
    return {
        "run_id": manifest.run_id,
        "release_id": manifest.release_id,
        "publisher_version": manifest.publisher_version,
        "source_data_period": manifest.source_data_period,
        "discovery_timestamp": manifest.discovery_timestamp,
        "retrieval_timestamp": manifest.retrieval_timestamp,
        "validation_state": manifest.validation_state.value,
        "validation_timestamp": manifest.validation_timestamp,
        "promotion_state": manifest.promotion_state.value,
        "promotion_timestamp": manifest.promotion_timestamp,
        "active_release_id": manifest.active_release_id,
        "schema_fingerprint": manifest.schema_fingerprint,
        "row_counts": dict(sorted(manifest.row_counts.items())),
        "pipeline_code_commit": manifest.pipeline_code_commit,
        "operator_summary": manifest.operator_summary,
        "error_summary": manifest.error_summary,
    }


def _evidence_status(
    spec: SourceSpec,
    document: ManifestDocument,
    latest: RunManifest | None,
) -> tuple[str, str]:
    active, reason = document.proven_active(spec.source_id)
    if active is not None:
        return "validated_active", "Validated active manifest proves the installed release."
    if latest is None:
        return "missing", "No run manifest has been recorded for this source."
    if latest.validation_state.value == "failed" or latest.promotion_state.value == "failed":
        return "failed", latest.error_summary or "The latest recorded run failed."
    return "unverified", reason or "The latest run does not prove an active installation."


def _source_contract(
    spec: SourceSpec,
    document: ManifestDocument,
    latest: RunManifest | None,
) -> dict:
    status, reason = _evidence_status(spec, document, latest)
    return {
        "source_id": spec.source_id,
        "title": spec.title,
        "publisher": spec.publisher.value,
        "cadence": spec.cadence.value,
        "discovery_mechanism": spec.discovery.value,
        "source_period_semantics": spec.source_period_semantics,
        "downstream_tables": list(spec.downstream_tables),
        "licensing_notes": spec.licensing_notes,
        "evidence_status": status,
        "evidence_reason": reason,
        "latest_manifest": _manifest_summary(latest) if latest else None,
    }


def get_operations_router(
    get_conn: Callable,
    manifest_path: Path | None = None,
) -> APIRouter:
    """Create the read-only operations router bound to one warehouse connection."""

    router = APIRouter(prefix="/operations", tags=["Data Operations"])

    def evidence() -> tuple[ManifestDocument, str | None]:
        return _load_manifests(manifest_path or _configured_manifest_path())

    @router.get("/overview")
    async def overview():
        connection = get_conn()
        table_rows = connection.execute(
            """
            SELECT table_name, estimated_size
            FROM duckdb_tables()
            WHERE schema_name = 'main'
            ORDER BY table_name
            """
        ).fetchall()
        tables = {name: int(size or 0) for name, size in table_rows}
        raw_tables = {name for name in tables if name.startswith("raw_")}
        registered_marts = {
            table
            for spec in SOURCE_REGISTRY.values()
            for table in spec.downstream_tables
            if "." not in table and not table.startswith("raw_")
        }
        available_marts = registered_marts.intersection(tables)

        document, evidence_error = evidence()
        active_sources = sum(
            1
            for source_id in SOURCE_REGISTRY
            if document.proven_active(source_id)[0] is not None
        )
        failed_runs = sum(
            1
            for manifest in document.manifests
            if manifest.validation_state.value == "failed"
            or manifest.promotion_state.value == "failed"
        )
        latest_observed_at = max(
            (_sort_timestamp(manifest) for manifest in document.manifests),
            default=None,
        )

        version_row = connection.execute("SELECT version()").fetchone()
        return {
            "schema_version": 1,
            "generated_at": _utc_now(),
            "warehouse": {
                "duckdb_version": version_row[0] if version_row else None,
                "table_count": len(tables),
                "raw_table_count": len(raw_tables),
                "data_mart_count": len(available_marts),
                "estimated_rows": sum(tables.values()),
            },
            "contracts": {
                "registered_sources": len(SOURCE_REGISTRY),
                "sources_with_active_evidence": active_sources,
                "manifest_run_count": len(document.manifests),
                "failed_run_count": failed_runs,
                "latest_observed_at": latest_observed_at,
                "evidence_error": evidence_error,
            },
            "control_plane": {
                "manual_refresh_enabled": False,
                "mode": "observation_only",
                "reason": (
                    "The serving API is read-only. Refresh execution requires a separately "
                    "approved operator service with locking, preview, audit, and confirmation."
                ),
                "safe_sequence": [
                    "discover",
                    "preview",
                    "acquire",
                    "validate",
                    "build_candidate",
                    "compare",
                    "approve",
                    "promote",
                ],
            },
        }

    @router.get("/sources")
    async def sources():
        document, evidence_error = evidence()
        latest = _latest_by_source(document)
        return {
            "schema_version": 1,
            "generated_at": _utc_now(),
            "evidence_error": evidence_error,
            "sources": [
                _source_contract(spec, document, latest.get(source_id))
                for source_id, spec in sorted(SOURCE_REGISTRY.items())
            ],
        }

    @router.get("/runs")
    async def runs(limit: int = Query(50, ge=1, le=200)):
        document, evidence_error = evidence()
        ordered = sorted(document.manifests, key=_sort_timestamp, reverse=True)
        return {
            "schema_version": 1,
            "generated_at": _utc_now(),
            "evidence_error": evidence_error,
            "runs": [
                {
                    "source_id": manifest.source_id,
                    **_manifest_summary(manifest),
                }
                for manifest in ordered[:limit]
            ],
        }

    return router
