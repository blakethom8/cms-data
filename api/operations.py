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
from pipeline.lineage import TRANSFORMS, raw_table_for_source, table_kind
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


def _lineage_inventory(connection) -> dict[str, int]:
    """Return observed table row estimates without assuming the main schema."""

    rows = connection.execute(
        """
        SELECT
            CASE WHEN schema_name = 'main' THEN table_name
                ELSE schema_name || '.' || table_name END AS table_name,
            estimated_size
        FROM duckdb_tables()
        """
    ).fetchall()
    return {str(name): int(size or 0) for name, size in rows}


def _lineage_payload(
    connection,
    document: ManifestDocument,
    evidence_error: str | None,
) -> dict:
    """Build declared topology and annotate it with live read-only evidence.

    Table presence is observed inventory. Transform edges remain declared even when
    their input and output tables are present: the API cannot prove a particular
    execution from table existence alone.
    """

    inventory = _lineage_inventory(connection)
    latest = _latest_by_source(document)
    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    def add_table(table: str) -> str:
        node_id = f"table:{table}"
        if node_id not in nodes:
            observed = table in inventory
            nodes[node_id] = {
                "id": node_id,
                "label": table,
                "kind": table_kind(table),
                "table": table,
                "declared": True,
                "observed": {"table_present": observed, "approx_rows": inventory.get(table)},
                "evidence_status": "observed" if observed else "unavailable",
                "details": {
                    "evidence_note": (
                        "Observed in the active warehouse inventory."
                        if observed
                        else "Declared in pipeline lineage but not observed in the active warehouse inventory."
                    )
                },
            }
        return node_id

    def add_edge(
        source: str,
        target: str,
        *,
        kind: str,
        label: str,
        evidence_status: str = "declared",
        transform_id: str | None = None,
    ) -> None:
        edge_id = f"{source}>{target}:{kind}:{transform_id or ''}"
        if any(edge["id"] == edge_id for edge in edges):
            return
        edges.append(
            {
                "id": edge_id,
                "source": source,
                "target": target,
                "kind": kind,
                "label": label,
                "evidence_status": evidence_status,
                "transform_id": transform_id,
            }
        )

    transform_outputs = {table for transform in TRANSFORMS for table in transform.outputs}
    for source_id, spec in sorted(SOURCE_REGISTRY.items()):
        source_node_id = f"source:{source_id}"
        status, reason = _evidence_status(spec, document, latest.get(source_id))
        nodes[source_node_id] = {
            "id": source_node_id,
            "label": spec.title,
            "kind": "source",
            "source_id": source_id,
            "declared": True,
            "evidence_status": status,
            "latest_manifest": _manifest_summary(latest[source_id]) if source_id in latest else None,
            "details": {
                "publisher": spec.publisher.value,
                "cadence": spec.cadence.value,
                "discovery_mechanism": spec.discovery.value,
                "source_period_semantics": spec.source_period_semantics,
                "evidence_note": reason,
            },
        }

        raw_table = raw_table_for_source(source_id)
        declared_outputs = set(spec.downstream_tables)
        if raw_table:
            declared_outputs.add(raw_table)

        for table in sorted(declared_outputs):
            table_node_id = add_table(table)
            if table == raw_table or table.startswith("raw_"):
                source_is_observed = (
                    status == "validated_active"
                    and bool(nodes[table_node_id]["observed"]["table_present"])
                )
                add_edge(
                    source_node_id,
                    table_node_id,
                    kind="lands_in",
                    label="lands in",
                    evidence_status="observed" if source_is_observed else "declared",
                )
            elif table not in transform_outputs:
                add_edge(
                    source_node_id,
                    table_node_id,
                    kind="published_surface",
                    label="published surface",
                )

    for transform in TRANSFORMS:
        transform_node_id = f"transform:{transform.transform_id}"
        nodes[transform_node_id] = {
            "id": transform_node_id,
            "label": transform.label,
            "kind": "transform",
            "transform_id": transform.transform_id,
            "declared": True,
            "evidence_status": "declared",
            "details": {"description": transform.description},
        }
        for table in transform.inputs:
            add_edge(
                add_table(table),
                transform_node_id,
                kind="reads_from",
                label="reads",
                transform_id=transform.transform_id,
            )
        for table in transform.outputs:
            add_edge(
                transform_node_id,
                add_table(table),
                kind="materializes",
                label="materializes",
                transform_id=transform.transform_id,
            )

    ordered_nodes = sorted(
        nodes.values(),
        key=lambda node: (node["kind"], node["label"].casefold()),
    )
    kind_counts = {
        kind: sum(1 for node in ordered_nodes if node["kind"] == kind)
        for kind in ("source", "raw", "transform", "bridge", "mart", "summary")
    }
    return {
        "schema_version": 1,
        "generated_at": _utc_now(),
        "evidence_error": evidence_error,
        "summary": {
            **kind_counts,
            "observed_tables": sum(
                1
                for node in ordered_nodes
                if node["kind"] != "source"
                and node.get("observed", {}).get("table_present") is True
            ),
            "observed_source_landings": sum(
                1 for edge in edges if edge["evidence_status"] == "observed"
            ),
            "declared_edges": len(edges),
        },
        "nodes": ordered_nodes,
        "edges": edges,
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

    @router.get("/lineage")
    async def lineage():
        document, evidence_error = evidence()
        return _lineage_payload(connection=get_conn(), document=document, evidence_error=evidence_error)

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
