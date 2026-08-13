"""Read-only planning for publisher-freshness recovery.

The planner consumes a captured production status result and the staging manifest
store.  It names acquisition work, selects already validated runs, and chooses
the narrowest available candidate lane.  It never downloads, builds, promotes,
or mutates either input.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path

from .manifests import ManifestDocument, ManifestStore, RunManifest, ValidationState
from .releases import (
    FULL_CMS_SOURCE_IDS,
    FULL_PLATFORM_WAREHOUSE_SOURCE_IDS,
    HOSPITAL_SOURCE_ID,
    PPEF_SOURCE_IDS,
    RADAR_SOURCE_IDS,
)
from .source_registry import SOURCE_REGISTRY


SCHEMA_VERSION = 1
AACT_SOURCE_ID = "aact_clinical_trials_snapshot"
OPEN_PAYMENTS_SOURCE_IDS = frozenset(
    {
        "open_payments_general",
        "open_payments_research",
        "open_payments_ownership",
    }
)
TARGETED_CMS_SOURCE_IDS = PPEF_SOURCE_IDS | {HOSPITAL_SOURCE_ID}


class RefreshPlanError(RuntimeError):
    """The captured evidence cannot produce a trustworthy refresh plan."""


@dataclass(frozen=True, slots=True)
class SourceAction:
    source_id: str
    family: str
    freshness_status: str
    action: str
    publisher_version: str | None
    installed_version: str | None
    source_data_period: str | None
    selected_run_ids: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class CandidateLane:
    lane_id: str
    build_command: str
    trigger_source_ids: tuple[str, ...]
    required_source_ids: tuple[str, ...]
    selected_run_ids: tuple[str, ...]
    missing_source_ids: tuple[str, ...]
    state: str
    promotion: str = "manual_approval_required"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_status(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise RefreshPlanError(f"status JSON must be a regular non-symlink file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RefreshPlanError(f"Could not read status JSON: {error}") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise RefreshPlanError("status JSON has an unsupported schema version")
    sources = payload.get("sources")
    if not isinstance(sources, list):
        raise RefreshPlanError("status JSON is missing its sources array")
    identifiers = [item.get("source_id") for item in sources if isinstance(item, dict)]
    if len(identifiers) != len(sources) or set(identifiers) != set(SOURCE_REGISTRY):
        raise RefreshPlanError("status JSON does not contain the exact registered source set")
    if len(identifiers) != len(set(identifiers)):
        raise RefreshPlanError("status JSON contains duplicate sources")
    return payload


def parse_exceptions(values: list[str]) -> dict[str, str]:
    exceptions: dict[str, str] = {}
    for value in values:
        source_id, separator, reason = value.partition("=")
        source_id = source_id.strip()
        reason = " ".join(reason.split())
        if not separator or source_id not in SOURCE_REGISTRY or not reason:
            raise RefreshPlanError(
                "exceptions must use a registered source_id=non-empty reason"
            )
        if source_id in exceptions:
            raise RefreshPlanError(f"duplicate refresh exception: {source_id}")
        exceptions[source_id] = reason
    return exceptions


def _family(source_id: str) -> str:
    if source_id == AACT_SOURCE_ID:
        return "aact"
    if source_id in RADAR_SOURCE_IDS:
        return "nppes"
    if source_id in FULL_CMS_SOURCE_IDS:
        return "cms"
    if source_id in OPEN_PAYMENTS_SOURCE_IDS:
        return "open_payments"
    raise RefreshPlanError(f"Source has no refresh family: {source_id}")


def _period_bounds(value: str | None) -> tuple[date, date] | None:
    if not value:
        return None
    parts = value.split("/", 1)
    try:
        start = date.fromisoformat(parts[0])
        end = date.fromisoformat(parts[-1])
    except ValueError:
        return None
    return (start, end) if start <= end else None


def _latest_matching_runs(
    manifests: ManifestDocument,
    source_id: str,
    publisher_version: str | None,
) -> tuple[RunManifest, ...]:
    if publisher_version is None:
        return ()
    matches = [
        manifest
        for manifest in manifests.manifests
        if manifest.source_id == source_id
        and manifest.publisher_version == publisher_version
        and manifest.validation_state == ValidationState.PASSED
    ]
    if not matches:
        return ()
    latest = max(
        matches,
        key=lambda item: (item.retrieval_timestamp or "", item.run_id),
    )
    return (latest,)


def _source_actions(
    status_sources: list[dict],
    manifests: ManifestDocument,
    exceptions: dict[str, str],
) -> list[SourceAction]:
    actions: list[SourceAction] = []
    for source in status_sources:
        source_id = source["source_id"]
        freshness = source.get("freshness_status")
        if freshness not in {"current", "stale", "unknown", "unavailable"}:
            raise RefreshPlanError(f"Source has invalid freshness status: {source_id}")
        if freshness in {"current", "stale"} and (
            not isinstance(source.get("latest_publisher_version"), str)
            or not isinstance(source.get("installed_version"), str)
        ):
            raise RefreshPlanError(
                f"Source is missing comparable publisher versions: {source_id}"
            )
        if source_id in exceptions and freshness != "stale":
            raise RefreshPlanError(
                f"Refresh exceptions are valid only for stale sources: {source_id}"
            )
        target_version = (
            source.get("installed_version")
            if source_id in exceptions
            else source.get("latest_publisher_version")
        )
        matching = _latest_matching_runs(manifests, source_id, target_version)
        if freshness == "current":
            action = "no_action"
            reason = "Production matches the latest publisher evidence."
        elif freshness in {"unknown", "unavailable"}:
            action = "blocked"
            reason = "Publisher or installed provenance is not sufficient to plan acquisition."
        elif source_id in exceptions and matching:
            action = "exception"
            reason = exceptions[source_id]
        elif source_id in exceptions:
            action = "blocked"
            reason = (
                f"Exception lacks a validated staging run for installed version: "
                f"{exceptions[source_id]}"
            )
        elif matching:
            action = "staged"
            reason = "A validated staging run matches the latest publisher version."
        else:
            action = "acquire"
            reason = "No validated staging run matches the latest publisher version."
        actions.append(
            SourceAction(
                source_id=source_id,
                family=_family(source_id),
                freshness_status=freshness,
                action=action,
                publisher_version=source.get("latest_publisher_version"),
                installed_version=source.get("installed_version"),
                source_data_period=source.get("source_data_period"),
                selected_run_ids=tuple(item.run_id for item in matching),
                reason=reason,
            )
        )
    return actions


def _apply_family_coverage(actions: list[SourceAction]) -> list[SourceAction]:
    by_id = {action.source_id: action for action in actions}
    monthly = by_id["nppes_monthly_v2"]
    weekly = by_id["nppes_weekly_incremental_v2"]
    monthly_bounds = _period_bounds(monthly.source_data_period)
    weekly_bounds = _period_bounds(weekly.source_data_period)
    if (
        monthly.freshness_status == "stale"
        and monthly.action not in {"blocked", "exception"}
        and weekly.freshness_status == "stale"
        and weekly.action not in {"blocked", "exception"}
        and monthly_bounds is not None
        and weekly_bounds is not None
        and monthly_bounds[1] >= weekly_bounds[1]
    ):
        covered = replace(
            weekly,
            action="covered_by_planned_monthly",
            selected_run_ids=(),
            reason=(
                "The planned monthly NPPES baseline covers the latest weekly period "
                f"ending {weekly_bounds[1].isoformat()}; do not replay that weekly."
            ),
        )
        return [covered if item.source_id == covered.source_id else item for item in actions]
    return actions


def _selected_run(
    source: dict,
    manifests: ManifestDocument,
    exceptions: dict[str, str],
) -> str | None:
    target_version = (
        source.get("installed_version")
        if source["source_id"] in exceptions
        else source.get("latest_publisher_version")
    )
    matches = _latest_matching_runs(manifests, source["source_id"], target_version)
    return matches[0].run_id if matches else None


def _lane(
    lane_id: str,
    command: str,
    triggers: set[str],
    required: set[str],
    by_status: dict[str, dict],
    manifests: ManifestDocument,
    exceptions: dict[str, str],
) -> CandidateLane:
    selected: list[str] = []
    missing: list[str] = []
    for source_id in sorted(required):
        run_id = _selected_run(by_status[source_id], manifests, exceptions)
        if run_id is None:
            missing.append(source_id)
        else:
            selected.append(run_id)
    return CandidateLane(
        lane_id=lane_id,
        build_command=command,
        trigger_source_ids=tuple(sorted(triggers)),
        required_source_ids=tuple(sorted(required)),
        selected_run_ids=tuple(selected),
        missing_source_ids=tuple(missing),
        state="ready" if not missing else "awaiting_validated_runs",
    )


def _nppes_lane(
    triggers: set[str],
    by_status: dict[str, dict],
    manifests: ManifestDocument,
) -> CandidateLane:
    monthly_status = by_status["nppes_monthly_v2"]
    weekly_status = by_status["nppes_weekly_incremental_v2"]
    monthly_runs = _latest_matching_runs(
        manifests,
        "nppes_monthly_v2",
        monthly_status.get("latest_publisher_version"),
    )
    selected = [item.run_id for item in monthly_runs]
    missing = [] if monthly_runs else ["nppes_monthly_v2"]
    monthly_bounds = _period_bounds(monthly_status.get("source_data_period"))
    weekly_bounds = _period_bounds(weekly_status.get("source_data_period"))
    weekly_is_after_baseline = bool(
        monthly_bounds
        and weekly_bounds
        and weekly_bounds[0] >= monthly_bounds[0]
    )
    if weekly_is_after_baseline:
        weekly_runs = _latest_matching_runs(
            manifests,
            "nppes_weekly_incremental_v2",
            weekly_status.get("latest_publisher_version"),
        )
        if weekly_runs:
            selected.extend(item.run_id for item in weekly_runs)
        else:
            missing.append("nppes_weekly_incremental_v2")
    return CandidateLane(
        lane_id="nppes_radar",
        build_command="pipeline.radar_reconciliation",
        trigger_source_ids=tuple(sorted(triggers)),
        required_source_ids=tuple(sorted(RADAR_SOURCE_IDS)),
        selected_run_ids=tuple(selected),
        missing_source_ids=tuple(missing),
        state="ready" if not missing else "awaiting_validated_runs",
    )


def _candidate_lanes(
    actions: list[SourceAction],
    status_sources: list[dict],
    manifests: ManifestDocument,
    exceptions: dict[str, str],
) -> list[CandidateLane]:
    by_status = {source["source_id"]: source for source in status_sources}
    actionable = {
        action.source_id
        for action in actions
        if action.freshness_status == "stale" and action.action != "exception"
    }
    lanes: list[CandidateLane] = []
    if AACT_SOURCE_ID in actionable:
        lanes.append(
            _lane(
                "aact_postgres",
                "prepare-aact-release + stage-aact-database",
                {AACT_SOURCE_ID},
                {AACT_SOURCE_ID},
                by_status,
                manifests,
                exceptions,
            )
        )

    warehouse_actionable = actionable & FULL_PLATFORM_WAREHOUSE_SOURCE_IDS
    if warehouse_actionable & OPEN_PAYMENTS_SOURCE_IDS:
        lanes.append(
            _lane(
                "full_platform",
                "pipeline.data_platform build-platform-release",
                warehouse_actionable,
                set(FULL_PLATFORM_WAREHOUSE_SOURCE_IDS),
                by_status,
                manifests,
                exceptions,
            )
        )
        return lanes

    nppes = warehouse_actionable & RADAR_SOURCE_IDS
    if nppes:
        lanes.append(_nppes_lane(nppes, by_status, manifests))

    cms = warehouse_actionable & FULL_CMS_SOURCE_IDS
    broad_cms = cms - TARGETED_CMS_SOURCE_IDS
    if broad_cms:
        lanes.append(
            _lane(
                "full_cms",
                "pipeline.data_platform build-cms-release",
                cms,
                set(FULL_CMS_SOURCE_IDS),
                by_status,
                manifests,
                exceptions,
            )
        )
    else:
        ppef = cms & PPEF_SOURCE_IDS
        if ppef:
            lanes.append(
                _lane(
                    "ppef",
                    "pipeline.data_platform build-ppef-release",
                    ppef,
                    set(PPEF_SOURCE_IDS),
                    by_status,
                    manifests,
                    exceptions,
                )
            )
        if HOSPITAL_SOURCE_ID in cms:
            lanes.append(
                _lane(
                    "hospital_enrollments",
                    "pipeline.data_platform build-release",
                    {HOSPITAL_SOURCE_ID},
                    {HOSPITAL_SOURCE_ID},
                    by_status,
                    manifests,
                    exceptions,
                )
            )
    return lanes


def _acquisition_order(
    actions: list[SourceAction],
    candidate_input_restore_ids: set[str],
) -> list[str]:
    pending = [
        action
        for action in actions
        if action.action == "acquire"
        or action.source_id in candidate_input_restore_ids
    ]

    def key(action: SourceAction) -> tuple[str, str, str, str]:
        bounds = _period_bounds(action.source_data_period)
        period = bounds[0].isoformat() if bounds else "9999-12-31"
        nppes_order = {
            "nppes_monthly_v2": "0",
            "nppes_weekly_incremental_v2": "1",
        }.get(action.source_id, "0")
        return action.family, nppes_order, period, action.source_id

    return [action.source_id for action in sorted(pending, key=key)]


def build_refresh_plan(
    status: dict,
    staging_manifests: ManifestDocument,
    *,
    exceptions: dict[str, str] | None = None,
) -> dict:
    exceptions = exceptions or {}
    if any(
        source_id not in SOURCE_REGISTRY or not " ".join(reason.split())
        for source_id, reason in exceptions.items()
    ):
        raise RefreshPlanError("exceptions require a registered source and non-empty reason")
    status_sources = status.get("sources")
    if not isinstance(status_sources, list) or not all(
        isinstance(source, dict) for source in status_sources
    ):
        raise RefreshPlanError("status evidence is missing its sources array")
    identifiers = [source.get("source_id") for source in status_sources]
    if set(identifiers) != set(SOURCE_REGISTRY) or len(identifiers) != len(set(identifiers)):
        raise RefreshPlanError("status evidence does not contain the exact registered source set")
    actions = _apply_family_coverage(
        _source_actions(status_sources, staging_manifests, exceptions)
    )
    lanes = _candidate_lanes(
        actions, status_sources, staging_manifests, exceptions
    )
    actions_by_id = {action.source_id: action for action in actions}
    candidate_input_restore_ids = {
        source_id
        for lane in lanes
        for source_id in lane.missing_source_ids
        if actions_by_id[source_id].action == "no_action"
    }
    blockers = sorted(
        action.source_id for action in actions if action.action == "blocked"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "read_only": True,
        "status_generated_at": status.get("generated_at"),
        "selected_deployment_id": (status.get("production") or {}).get(
            "selected_deployment_id"
        ),
        "planning_state": "blocked" if blockers else "actionable",
        "promotion": "manual_approval_required",
        "summary": {
            "stale": sum(action.freshness_status == "stale" for action in actions),
            "acquire": sum(action.action == "acquire" for action in actions),
            "candidate_input_restore": len(candidate_input_restore_ids),
            "staged": sum(action.action == "staged" for action in actions),
            "covered": sum(
                action.action == "covered_by_planned_monthly" for action in actions
            ),
            "exceptions": len(exceptions),
            "blocked": len(blockers),
            "candidate_lanes": len(lanes),
            "ready_lanes": sum(lane.state == "ready" for lane in lanes),
        },
        "blockers": blockers,
        "exceptions": dict(sorted(exceptions.items())),
        "candidate_input_restore_ids": sorted(candidate_input_restore_ids),
        "acquisition_order": _acquisition_order(
            actions, candidate_input_restore_ids
        ),
        "sources": [asdict(action) for action in actions],
        "candidate_lanes": [asdict(lane) for lane in lanes],
    }


def render_human(plan: dict) -> str:
    lines = [
        "CMS production freshness recovery plan (read-only)",
        f"State: {plan['planning_state']}",
        f"Selected deployment: {plan['selected_deployment_id'] or '-'}",
        "",
        "Acquisition order:",
    ]
    lines.extend(
        f"  {index}. {source_id}"
        for index, source_id in enumerate(plan["acquisition_order"], 1)
    )
    if not plan["acquisition_order"]:
        lines.append("  none")
    lines.extend(["", "Candidate lanes:"])
    for lane in plan["candidate_lanes"]:
        lines.append(
            f"  [{lane['state']}] {lane['lane_id']} — {lane['build_command']}"
        )
        if lane["missing_source_ids"]:
            lines.append("    missing: " + ", ".join(lane["missing_source_ids"]))
    lines.extend(
        [
            "",
            "Promotion: manual approval required",
            "No acquisition, build, promotion, or service action was performed.",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan production freshness recovery without changing data"
    )
    parser.add_argument("--status-json", required=True, type=Path)
    parser.add_argument("--staging-manifest", required=True, type=Path)
    parser.add_argument(
        "--exception",
        action="append",
        default=[],
        help="Record an intentional stale-source exception as source_id=reason",
    )
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        status = _load_status(args.status_json)
        if args.staging_manifest.is_symlink() or not args.staging_manifest.is_file():
            raise RefreshPlanError(
                "staging manifest must be a regular non-symlink file: "
                f"{args.staging_manifest}"
            )
        manifests = ManifestStore(args.staging_manifest).load()
        plan = build_refresh_plan(
            status,
            manifests,
            exceptions=parse_exceptions(args.exception),
        )
    except (OSError, ValueError, RefreshPlanError) as error:
        payload = {"read_only": True, "error": str(error)}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Refresh planning failed: {error}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(plan, indent=2, sort_keys=True))
    else:
        print(render_human(plan))
    return 1 if plan["planning_state"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
