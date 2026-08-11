"""Scheduled NPPES Radar reconciliation into an immutable staging candidate."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .acquisition import pipeline_commit
from .manifests import ManifestStore, RunManifest, ValidationState
from .releases import (
    ReleaseError,
    WarehouseReleaseStore,
    build_radar_warehouse_release,
    compare_warehouse_release,
)


def _period(value: str) -> tuple[date, date]:
    parts = value.split("/", 1)
    try:
        return date.fromisoformat(parts[0]), date.fromisoformat(parts[-1])
    except ValueError as error:
        raise ReleaseError(f"Invalid source data period: {value}") from error


def select_reconciliation_runs(data_root: Path) -> tuple[str, ...]:
    """Select latest validated platform inputs plus weeklies after the monthly base."""
    manifests = [
        manifest
        for manifest in ManifestStore(data_root / "manifests.json").load().manifests
        if manifest.validation_state == ValidationState.PASSED
        and manifest.source_id
        in {"nppes_monthly_v2", "nppes_weekly_incremental_v2"}
    ]
    grouped = {
        source_id: [
            manifest for manifest in manifests if manifest.source_id == source_id
        ]
        for source_id in {"nppes_monthly_v2", "nppes_weekly_incremental_v2"}
    }
    missing = sorted(source_id for source_id, rows in grouped.items() if not rows)
    if missing:
        raise ReleaseError(
            "Radar reconciliation lacks validated source runs: " + ", ".join(missing)
        )

    def latest(rows: list[RunManifest]) -> RunManifest:
        return max(
            rows,
            key=lambda manifest: (
                manifest.source_data_period,
                manifest.retrieval_timestamp or "",
                manifest.run_id,
            ),
        )

    selected = {"nppes_monthly_v2": latest(grouped["nppes_monthly_v2"])}
    monthly_start, _ = _period(
        selected["nppes_monthly_v2"].source_data_period
    )
    weekly_by_version: dict[str, RunManifest] = {}
    for manifest in grouped["nppes_weekly_incremental_v2"]:
        weekly_start, _ = _period(manifest.source_data_period)
        if weekly_start < monthly_start:
            continue
        existing = weekly_by_version.get(manifest.publisher_version)
        if existing is None or (
            manifest.retrieval_timestamp or "", manifest.run_id
        ) > (existing.retrieval_timestamp or "", existing.run_id):
            weekly_by_version[manifest.publisher_version] = manifest
    weeklies = sorted(
        weekly_by_version.values(),
        key=lambda manifest: (*_period(manifest.source_data_period), manifest.run_id),
    )
    if not weeklies:
        raise ReleaseError(
            "Radar reconciliation has no weekly release at or after the monthly baseline"
        )
    return (selected["nppes_monthly_v2"].run_id,) + tuple(
        manifest.run_id for manifest in weeklies
    )


def reconcile_radar_candidate(
    *,
    data_root: Path,
    backup_manifest_path: Path,
    code_commit: str | None = None,
) -> dict[str, object]:
    """Build and compare a staging candidate, or no-op for identical evidence."""
    run_ids = select_reconciliation_runs(data_root)
    commit = code_commit or pipeline_commit()
    if commit is None:
        raise ReleaseError("A full pipeline Git commit is required for reconciliation")
    store = WarehouseReleaseStore(data_root / "warehouse-releases.json")
    for release in reversed(store.load().releases):
        comparison_path = (
            data_root / "releases" / release.warehouse_release_id / "comparison.json"
        )
        if (
            release.validation_state == ValidationState.PASSED
            and release.pipeline_code_commit == commit
            and set(release.source_run_ids) == set(run_ids)
            and comparison_path.is_file()
        ):
            try:
                comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if comparison.get("state") == "passed":
                return {
                    "status": "no_op",
                    "reason": "source_runs_already_reconciled",
                    "warehouse_release_id": release.warehouse_release_id,
                    "source_run_ids": list(run_ids),
                    "wrote_candidate": False,
                }

    build = build_radar_warehouse_release(
        data_root=data_root,
        monthly_run_id=run_ids[0],
        weekly_run_ids=run_ids[1:],
        backup_manifest_path=backup_manifest_path,
        code_commit=commit,
    )
    comparison = compare_warehouse_release(
        data_root=data_root,
        warehouse_release_id=build.release.warehouse_release_id,
        backup_manifest_path=backup_manifest_path,
    )
    return {
        "status": "candidate_ready",
        "warehouse_release_id": build.release.warehouse_release_id,
        "source_run_ids": list(run_ids),
        "comparison": comparison,
        "wrote_candidate": True,
        "promoted": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build and compare an NPPES Radar staging reconciliation"
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--backup-manifest", required=True, type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = reconcile_radar_candidate(
            data_root=args.data_root,
            backup_manifest_path=args.backup_manifest,
        )
    except (OSError, ValueError, ReleaseError) as error:
        payload = {"status": "failed", "error": str(error)}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Radar reconciliation failed: {error}")
        return 1
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
