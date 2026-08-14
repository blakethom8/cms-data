"""Read-only storage inventory and retention planning for the CMS data platform.

This module deliberately has no delete operation.  It derives the protected
production rollback set from the validated deployment ledger, inventories
known storage roots without following symlinks, and names paths that an
operator may review in a separate, approved cleanup change.
"""

from __future__ import annotations

import argparse
import json
import shutil
import stat
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from . import production_manager as production


SCHEMA_VERSION = 1
DEFAULT_KEEP_PREVIOUS = 2
DEFAULT_WARNING_PERCENT = 70.0
DEFAULT_CRITICAL_PERCENT = 80.0
DEFAULT_PROMOTION_BLOCK_PERCENT = 85.0


class RetentionError(RuntimeError):
    """The retention preview could not prove a safe result."""


@dataclass(frozen=True, slots=True)
class StorageItem:
    path: str
    allocated_bytes: int
    disposition: str
    reason: str
    references: tuple[str, ...] = ()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _canonical_root(path: Path, label: str) -> Path:
    if not path.is_absolute() or path == Path("/"):
        raise RetentionError(f"{label} must be a specific absolute path: {path}")
    if path.is_symlink() or not path.is_dir():
        raise RetentionError(f"{label} must be a non-symlink directory: {path}")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise RetentionError(f"{label} must be canonical: {path}")
    return resolved


def allocated_bytes(path: Path) -> int:
    """Return allocated bytes below *path* without crossing mounts or symlinks."""
    try:
        root_details = path.lstat()
    except OSError as error:
        raise RetentionError(f"Could not inspect storage path {path}: {error}") from error
    root_device = root_details.st_dev
    seen: set[tuple[int, int]] = set()

    def visit(candidate: Path) -> int:
        try:
            details = candidate.lstat()
        except OSError as error:
            raise RetentionError(f"Could not inspect storage path {candidate}: {error}") from error
        identity = (details.st_dev, details.st_ino)
        if identity in seen:
            return 0
        seen.add(identity)
        total = details.st_blocks * 512
        if not stat.S_ISDIR(details.st_mode) or details.st_dev != root_device:
            return total
        try:
            children = tuple(candidate.iterdir())
        except OSError as error:
            raise RetentionError(f"Could not list storage path {candidate}: {error}") from error
        for child in children:
            total += visit(child)
        return total

    return visit(path)


def _children(path: Path) -> tuple[Path, ...]:
    if not path.exists():
        return ()
    if path.is_symlink() or not path.is_dir():
        raise RetentionError(f"Managed storage root is not a directory: {path}")
    return tuple(sorted(path.iterdir(), key=lambda item: item.name))


def _is_within(target: str, parent: Path) -> bool:
    try:
        return Path(target).resolve(strict=False).is_relative_to(parent.resolve(strict=False))
    except (OSError, RuntimeError):
        return False


def _protected_deployments(
    deployments: list[production.ProductionDeployment],
    selected_id: str,
    keep_previous: int,
) -> tuple[list[production.ProductionDeployment], list[str]]:
    by_id = {deployment.deployment_id: deployment for deployment in deployments}
    selected = by_id[selected_id]
    protected = [selected]
    problems: list[str] = []
    if selected.state != production.DeploymentState.VERIFIED:
        problems.append("selected deployment is not verified")
    cursor = selected.previous_deployment_id
    while cursor is not None and len(protected) <= keep_previous:
        predecessor = by_id[cursor]
        if predecessor.verified_at is None or predecessor.state not in {
            production.DeploymentState.SUPERSEDED,
            production.DeploymentState.VERIFIED,
        }:
            problems.append(
                f"deployment {predecessor.deployment_id} is not a validated rollback predecessor"
            )
            break
        protected.append(predecessor)
        cursor = predecessor.previous_deployment_id
    if len(protected) != keep_previous + 1:
        problems.append(
            f"rollback floor requires active + {keep_previous} previous validated deployments; "
            f"only {len(protected)} can be proven"
        )
    return protected, problems


def _artifact_items(
    artifact_root: Path,
    deployments: list[production.ProductionDeployment],
    protected_ids: set[str],
    *,
    rollback_floor_met: bool,
) -> list[StorageItem]:
    references: dict[str, list[str]] = {}
    protected_targets: set[str] = set()
    for deployment in deployments:
        for target in deployment.targets.to_bundle_map().values():
            references.setdefault(target, []).append(deployment.deployment_id)
            if deployment.deployment_id in protected_ids:
                protected_targets.add(target)

    items: list[StorageItem] = []
    for kind in ("code", "runtimes", "warehouses"):
        for candidate in _children(artifact_root / kind):
            candidate_references = sorted(
                deployment_id
                for target, deployment_ids in references.items()
                if _is_within(target, candidate)
                for deployment_id in deployment_ids
            )
            is_protected = any(_is_within(target, candidate) for target in protected_targets)
            if is_protected:
                disposition = "protected"
                reason = "referenced by the active rollback-retention set"
            elif not rollback_floor_met:
                disposition = "blocked"
                reason = (
                    "rollback floor is not proven; no production artifact is a cleanup candidate"
                )
            elif candidate_references:
                disposition = "review_candidate"
                reason = "referenced only by deployments outside the rollback-retention set"
            else:
                disposition = "review_candidate"
                reason = "not referenced by any deployment in the production ledger"
            items.append(
                StorageItem(
                    path=str(candidate),
                    allocated_bytes=allocated_bytes(candidate),
                    disposition=disposition,
                    reason=reason,
                    references=tuple(candidate_references),
                )
            )
    return items


def _workspace_items(platform_root: Path, protected_release_ids: set[str]) -> list[StorageItem]:
    items: list[StorageItem] = []
    for refresh_root in sorted(platform_root.glob("refresh-*")):
        if refresh_root.is_dir() and not refresh_root.is_symlink():
            items.append(
                StorageItem(
                    path=str(refresh_root),
                    allocated_bytes=allocated_bytes(refresh_root),
                    disposition="review_candidate",
                    reason=(
                        "refresh workspace; confirm no active job and preserve required evidence"
                    ),
                )
            )

    for release in _children(platform_root / "data" / "releases"):
        if release.name in protected_release_ids:
            disposition = "protected"
            reason = "warehouse release belongs to the active rollback-retention set"
        else:
            disposition = "manual_review_only"
            reason = "staging release; validate manifest and promotion provenance before cleanup"
        items.append(
            StorageItem(str(release), allocated_bytes(release), disposition, reason)
        )

    for backup in _children(platform_root / "backups"):
        items.append(
            StorageItem(
                path=str(backup),
                allocated_bytes=allocated_bytes(backup),
                disposition="protected",
                reason=(
                    "verified baseline; retain until off-host recovery policy "
                    "and restore proof exist"
                ),
            )
        )
    return items


def build_retention_preview(
    platform_root: Path,
    *,
    production_root: Path | None = None,
    keep_previous: int = DEFAULT_KEEP_PREVIOUS,
    warning_percent: float = DEFAULT_WARNING_PERCENT,
    critical_percent: float = DEFAULT_CRITICAL_PERCENT,
    promotion_block_percent: float = DEFAULT_PROMOTION_BLOCK_PERCENT,
    candidate_bytes: int | None = None,
) -> dict:
    """Build a deterministic, read-only retention and promotion-capacity report."""
    if keep_previous < 2:
        raise RetentionError("keep_previous must be at least 2")
    if not 0 < warning_percent < critical_percent < promotion_block_percent < 100:
        raise RetentionError(
            "disk thresholds must satisfy 0 < warning < critical < promotion block < 100"
        )
    platform_root = _canonical_root(platform_root, "platform root")
    production_root = _canonical_root(
        production_root or platform_root / "production", "production root"
    )
    if not production_root.is_relative_to(platform_root):
        raise RetentionError("production root is outside the platform root")
    try:
        production._require_control_ownership(production_root)
        document, deployments = production._read_deployments(production_root)
    except production.ProductionError as error:
        raise RetentionError(str(error)) from error
    selected_id = document.get("selected_deployment_id")
    if not isinstance(selected_id, str):
        raise RetentionError("production ledger has no selected deployment")
    protected, problems = _protected_deployments(deployments, selected_id, keep_previous)
    protected_ids = {deployment.deployment_id for deployment in protected}
    selected_bundle = production._read_selected_bundle(production_root)
    expected_selected_bundle = production_root / "releases" / selected_id
    if selected_bundle != expected_selected_bundle:
        raise RetentionError("release-current does not match the selected production deployment")
    for deployment in protected:
        bundle = production_root / "releases" / deployment.deployment_id
        try:
            actual_targets = production._read_bundle_targets(bundle)
        except production.ProductionError as error:
            raise RetentionError(str(error)) from error
        if actual_targets != deployment.targets.to_bundle_map():
            raise RetentionError(
                f"release bundle targets do not match the ledger: {deployment.deployment_id}"
            )
    rollback_floor_met = not problems

    deployment_items: list[StorageItem] = []
    for deployment in deployments:
        bundle = production_root / "releases" / deployment.deployment_id
        evidence = production_root / "evidence" / deployment.deployment_id
        size = sum(
            allocated_bytes(path)
            for path in (bundle, evidence)
            if path.exists() or path.is_symlink()
        )
        if deployment.deployment_id in protected_ids:
            disposition = "protected"
            reason = "active deployment or validated rollback predecessor"
        elif rollback_floor_met:
            disposition = "review_candidate"
            reason = "outside the active + previous validated deployment floor"
        else:
            disposition = "blocked"
            reason = "rollback floor is not proven"
        deployment_items.append(
            StorageItem(
                path=str(bundle),
                allocated_bytes=size,
                disposition=disposition,
                reason=reason,
                references=(deployment.deployment_id,),
            )
        )

    artifact_roots = {Path(deployment.artifact_root) for deployment in deployments}
    if len(artifact_roots) != 1:
        raise RetentionError("production ledger does not identify one artifact root")
    artifact_root = _canonical_root(artifact_roots.pop(), "production artifact root")
    if not artifact_root.is_relative_to(platform_root):
        raise RetentionError("production artifact root is outside the platform root")
    artifact_items = _artifact_items(
        artifact_root,
        deployments,
        protected_ids,
        rollback_floor_met=rollback_floor_met,
    )
    protected_release_ids = {
        deployment.warehouse_release_id
        for deployment in protected
        if deployment.warehouse_release_id is not None
    }
    workspace_items = _workspace_items(platform_root, protected_release_ids)
    items = deployment_items + artifact_items + workspace_items

    disk = shutil.disk_usage(platform_root)
    used_percent = disk.used / disk.total * 100
    selected = next(item for item in deployments if item.deployment_id == selected_id)
    required_candidate_bytes = (
        selected.warehouse_byte_size if candidate_bytes is None else candidate_bytes
    )
    if required_candidate_bytes < 0:
        raise RetentionError("candidate_bytes must be non-negative")
    projected_used_percent = (disk.used + required_candidate_bytes) / disk.total * 100
    if used_percent >= critical_percent:
        disk_state = "critical"
    elif used_percent >= warning_percent:
        disk_state = "warning"
    else:
        disk_state = "healthy"
    promotion_allowed = (
        rollback_floor_met
        and disk.free >= required_candidate_bytes
        and projected_used_percent < promotion_block_percent
    )
    review_candidate_bytes = sum(
        item.allocated_bytes for item in items if item.disposition == "review_candidate"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "read_only": True,
        "platform_root": str(platform_root),
        "production_root": str(production_root),
        "rollback_policy": {
            "keep_active": 1,
            "keep_previous_validated": keep_previous,
            "floor_met": rollback_floor_met,
            "protected_deployment_ids": [item.deployment_id for item in protected],
            "problems": problems,
        },
        "disk": {
            "total_bytes": disk.total,
            "used_bytes": disk.used,
            "free_bytes": disk.free,
            "used_percent": round(used_percent, 2),
            "state": disk_state,
            "thresholds_percent": {
                "warning": warning_percent,
                "critical": critical_percent,
                "promotion_block": promotion_block_percent,
            },
        },
        "promotion_capacity_gate": {
            "allowed": promotion_allowed,
            "required_candidate_bytes": required_candidate_bytes,
            "projected_used_percent": round(projected_used_percent, 2),
            "reason": (
                "capacity and rollback floor satisfy the configured gate"
                if promotion_allowed
                else "rollback floor, free bytes, or projected utilization blocks promotion"
            ),
        },
        "summary": {
            "review_candidate_count": sum(
                item.disposition == "review_candidate" for item in items
            ),
            "review_candidate_allocated_bytes": review_candidate_bytes,
            "confirmed_reclaimable_bytes": 0,
            "note": "candidate bytes are allocated under review paths, not guaranteed reclaimable",
        },
        "items": [asdict(item) for item in items],
    }


def render_human(report: dict) -> str:
    disk = report["disk"]
    rollback = report["rollback_policy"]
    gate = report["promotion_capacity_gate"]
    lines = [
        "CMS data platform retention preview (read-only)",
        f"Platform: {report['platform_root']}",
        f"Disk: {disk['used_percent']:.2f}% used ({disk['state']})",
        f"Rollback floor met: {'yes' if rollback['floor_met'] else 'no'}",
        f"Promotion capacity gate: {'allow' if gate['allowed'] else 'block'}",
        "",
        "Protected deployments:",
    ]
    lines.extend(f"  {deployment_id}" for deployment_id in rollback["protected_deployment_ids"])
    for problem in rollback["problems"]:
        lines.append(f"  PROBLEM: {problem}")
    lines.extend(["", "Storage paths:"])
    for item in report["items"]:
        gib = item["allocated_bytes"] / 1024**3
        lines.append(
            f"  [{item['disposition']}] {gib:8.2f} GiB  {item['path']} — {item['reason']}"
        )
    lines.extend(
        [
            "",
            f"Review paths: {report['summary']['review_candidate_count']}",
            "No files were changed. This command has no delete mode.",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only CMS storage retention planning")
    subparsers = parser.add_subparsers(dest="command", required=True)
    preview = subparsers.add_parser("preview", help="inventory storage and name review candidates")
    preview.add_argument("--platform-root", required=True, type=Path)
    preview.add_argument("--production-root", type=Path)
    preview.add_argument("--keep-previous", type=int, default=DEFAULT_KEEP_PREVIOUS)
    preview.add_argument(
        "--candidate-bytes",
        type=int,
        help=(
            "additional candidate bytes not already allocated on this filesystem; "
            "use 0 after the immutable production artifact has been copied"
        ),
    )
    preview.add_argument("--warning-percent", type=float, default=DEFAULT_WARNING_PERCENT)
    preview.add_argument("--critical-percent", type=float, default=DEFAULT_CRITICAL_PERCENT)
    preview.add_argument(
        "--promotion-block-percent", type=float, default=DEFAULT_PROMOTION_BLOCK_PERCENT
    )
    preview.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = build_retention_preview(
            args.platform_root,
            production_root=args.production_root,
            keep_previous=args.keep_previous,
            warning_percent=args.warning_percent,
            critical_percent=args.critical_percent,
            promotion_block_percent=args.promotion_block_percent,
            candidate_bytes=args.candidate_bytes,
        )
    except (OSError, RetentionError) as error:
        payload = {"read_only": True, "error": str(error)}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Retention preview failed: {error}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(render_human(report))
    return 0 if report["promotion_capacity_gate"]["allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
