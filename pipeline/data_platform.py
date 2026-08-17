"""Public healthcare data-platform status and staged acquisition commands.

Status is strictly read-only. Acquisition writes only immutable run artifacts and
manifests under the selected data root; it never opens or promotes a DuckDB file.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date
from enum import Enum
from pathlib import Path

from .acquisition import (
    SUPPORTED_ACQUISITION_SOURCES,
    AcquisitionError,
    acquire_release,
    make_run_id,
    release_id,
)
from .archive_acquisition import (
    SUPPORTED_ARCHIVE_ACQUISITION_SOURCES,
    acquire_archive_release,
)
from .aact_releases import prepare_aact_release
from .aact_staging import stage_aact_database, write_stage_evidence
from .discovery import (
    DiscoveryResult,
    DiscoveryState,
    ReleaseMetadata,
    discover_all,
    discover_source,
    safe_error,
    utc_now,
)
from .manifests import ManifestDocument, ManifestStore, RunManifest, ValidationState
from .releases import (
    STAGING_ENVIRONMENT,
    ReleaseError,
    build_full_cms_warehouse_release,
    build_full_platform_warehouse_release,
    build_managed_dac_serving_practice_warehouse_release,
    build_nppes_serving_practice_warehouse_release,
    build_ppef_warehouse_release,
    build_provider_profile_core_warehouse_release,
    build_provider_profile_warehouse_release,
    build_radar_warehouse_release,
    build_serving_practice_warehouse_release,
    build_warehouse_release,
    compare_warehouse_release,
    promote_staging_release,
    rollback_staging_release,
)
from .source_adoption import SourceAdoptionError, adopt_validated_source_run
from .source_registry import SOURCE_REGISTRY, SourceSpec

DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parent.parent / "data" / "manifests.json"
DEFAULT_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "publisher_metadata"

EXIT_HEALTHY = 0
EXIT_STALE_OR_UNKNOWN = 1
EXIT_DISCOVERY_FAILURE = 2
EXIT_ACQUISITION_FAILURE = 3
EXIT_RELEASE_FAILURE = 4


class FreshnessStatus(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class SourceStatus:
    source_id: str
    title: str
    publisher: str
    publisher_cadence: str
    discovery_mechanism: str
    discovery_state: str
    latest_publisher_version: str | None
    installed_version: str | None
    source_data_period: str | None
    installed_source_data_period: str | None
    publisher_release_timestamp: str | None
    ingestion_timestamp: str | None
    active_release_id: str | None
    freshness_status: FreshnessStatus
    reason: str
    source_url: str | None

    def to_dict(self) -> dict:
        value = asdict(self)
        value["freshness_status"] = self.freshness_status.value
        return value


@dataclass(frozen=True, slots=True)
class StatusReport:
    schema_version: int
    generated_at: str
    manifest_path: str
    discovery_mode: str
    sources: tuple[SourceStatus, ...]

    def to_dict(self) -> dict:
        counts = {status.value: 0 for status in FreshnessStatus}
        for source in self.sources:
            counts[source.freshness_status.value] += 1
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "manifest_path": self.manifest_path,
            "discovery_mode": self.discovery_mode,
            "summary": counts,
            "sources": [source.to_dict() for source in self.sources],
        }

    @property
    def exit_code(self) -> int:
        statuses = {source.freshness_status for source in self.sources}
        if FreshnessStatus.UNAVAILABLE in statuses:
            return EXIT_DISCOVERY_FAILURE
        if FreshnessStatus.STALE in statuses or FreshnessStatus.UNKNOWN in statuses:
            return EXIT_STALE_OR_UNKNOWN
        return EXIT_HEALTHY


def evaluate_source(
    spec: SourceSpec,
    discovery: DiscoveryResult,
    manifests: ManifestDocument,
) -> SourceStatus:
    release = discovery.release
    installed, provenance_reason = manifests.proven_active(spec.source_id)
    if discovery.state != DiscoveryState.AVAILABLE or release is None:
        reason = discovery.error_summary or "Publisher discovery returned no usable release."
        return SourceStatus(
            source_id=spec.source_id,
            title=spec.title,
            publisher=spec.publisher.value,
            publisher_cadence=spec.cadence.value,
            discovery_mechanism=spec.discovery.value,
            discovery_state=discovery.state.value,
            latest_publisher_version=None,
            installed_version=installed.publisher_version if installed else None,
            source_data_period=None,
            installed_source_data_period=(
                installed.source_data_period if installed else None
            ),
            publisher_release_timestamp=None,
            ingestion_timestamp=installed.retrieval_timestamp if installed else None,
            active_release_id=installed.active_release_id if installed else None,
            freshness_status=FreshnessStatus.UNAVAILABLE,
            reason=reason,
            source_url=None,
        )

    if installed is None:
        return SourceStatus(
            source_id=spec.source_id,
            title=spec.title,
            publisher=spec.publisher.value,
            publisher_cadence=spec.cadence.value,
            discovery_mechanism=spec.discovery.value,
            discovery_state=discovery.state.value,
            latest_publisher_version=release.publisher_version,
            installed_version=None,
            source_data_period=release.source_data_period,
            installed_source_data_period=None,
            publisher_release_timestamp=release.publisher_release_timestamp,
            ingestion_timestamp=None,
            active_release_id=None,
            freshness_status=FreshnessStatus.UNKNOWN,
            reason=provenance_reason
            or "Installed provenance is not sufficient to compare versions.",
            source_url=release.source_url,
        )

    is_current = installed.publisher_version == release.publisher_version
    if is_current:
        freshness = FreshnessStatus.CURRENT
        reason = "Validated active manifest matches the latest publisher version."
    else:
        freshness = FreshnessStatus.STALE
        reason = (
            f"Validated active manifest has {installed.publisher_version}; publisher lists "
            f"{release.publisher_version}."
        )
    return SourceStatus(
        source_id=spec.source_id,
        title=spec.title,
        publisher=spec.publisher.value,
        publisher_cadence=spec.cadence.value,
        discovery_mechanism=spec.discovery.value,
        discovery_state=discovery.state.value,
        latest_publisher_version=release.publisher_version,
        installed_version=installed.publisher_version,
        source_data_period=release.source_data_period,
        installed_source_data_period=installed.source_data_period,
        publisher_release_timestamp=release.publisher_release_timestamp,
        ingestion_timestamp=installed.retrieval_timestamp,
        active_release_id=installed.active_release_id,
        freshness_status=freshness,
        reason=reason,
        source_url=release.source_url,
    )


def build_status_report(
    discoveries: dict[str, DiscoveryResult],
    manifests: ManifestDocument,
    *,
    manifest_path: Path,
    discovery_mode: str,
) -> StatusReport:
    sources = reconcile_source_family_statuses(
        tuple(
            evaluate_source(
                SOURCE_REGISTRY[source_id], discoveries[source_id], manifests
            )
            for source_id in sorted(SOURCE_REGISTRY)
        )
    )
    return StatusReport(
        schema_version=1,
        generated_at=utc_now(),
        manifest_path=str(manifest_path),
        discovery_mode=discovery_mode,
        sources=sources,
    )


def _period_end(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value.split("/", 1)[-1])
    except ValueError:
        return None


def reconcile_source_family_statuses(
    sources: tuple[SourceStatus, ...],
) -> tuple[SourceStatus, ...]:
    """Apply publisher-family coverage rules after per-source evaluation."""
    by_id = {source.source_id: source for source in sources}
    monthly = by_id.get("nppes_monthly_v2")
    weekly = by_id.get("nppes_weekly_incremental_v2")
    monthly_period = (
        _period_end(monthly.installed_source_data_period) if monthly else None
    )
    weekly_period = _period_end(weekly.source_data_period) if weekly else None
    if (
        monthly is not None
        and weekly is not None
        and monthly.freshness_status == FreshnessStatus.CURRENT
        and weekly.freshness_status == FreshnessStatus.STALE
        and monthly_period is not None
        and weekly_period is not None
        and monthly_period >= weekly_period
    ):
        covered = replace(
            weekly,
            freshness_status=FreshnessStatus.CURRENT,
            reason=(
                "Latest validated monthly NPPES baseline covers the latest published "
                f"weekly period ending {weekly_period.isoformat()}."
            ),
        )
        return tuple(
            covered if source.source_id == covered.source_id else source
            for source in sources
        )
    return sources


def _short(value: str | None, width: int) -> str:
    if not value:
        return "-"
    return value if len(value) <= width else value[: width - 1] + "…"


def render_human(report: StatusReport) -> str:
    lines = [
        f"Data platform status ({report.discovery_mode})",
        f"Manifest: {report.manifest_path}",
        "",
        f"{'SOURCE':42} {'CADENCE':28} {'STATUS':11} {'PUBLISHER VERSION':32} {'INSTALLED':24}",
        f"{'-' * 42} {'-' * 28} {'-' * 11} {'-' * 32} {'-' * 24}",
    ]
    for source in report.sources:
        lines.append(
            f"{_short(source.source_id, 42):42} "
            f"{_short(source.publisher_cadence, 28):28} "
            f"{source.freshness_status.value:11} "
            f"{_short(source.latest_publisher_version, 32):32} "
            f"{_short(source.installed_version, 24):24}"
        )
        lines.append(
            f"  publisher_period={source.source_data_period or '-'} "
            f"installed_period={source.installed_source_data_period or '-'} "
            f"ingested={source.ingestion_timestamp or '-'}"
        )
        lines.append(f"  why: {source.reason}")
    summary = report.to_dict()["summary"]
    lines.extend(
        [
            "",
            "Summary: "
            + ", ".join(f"{name}={count}" for name, count in summary.items()),
        ]
    )
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Public healthcare data-platform operations")
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser(
        "status", help="Discover publisher releases and report freshness without downloading data"
    )
    status.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    status.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Local versioned manifest JSON (missing means provenance unknown)",
    )
    fixture_group = status.add_mutually_exclusive_group()
    fixture_group.add_argument(
        "--fixtures",
        type=Path,
        help="Use publisher metadata fixtures from this directory; make no network requests",
    )
    fixture_group.add_argument(
        "--offline",
        action="store_true",
        help="Use the checked-in publisher metadata fixtures; make no network requests",
    )
    status.add_argument(
        "--timeout", type=float, default=30.0, help="Per-metadata-request timeout in seconds"
    )
    acquire = subparsers.add_parser(
        "acquire",
        help="Discover and immutably acquire a supported source into staging",
    )
    acquire.add_argument(
        "source_id",
        choices=sorted(
            SUPPORTED_ACQUISITION_SOURCES | SUPPORTED_ARCHIVE_ACQUISITION_SOURCES
        ),
    )
    acquire.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_MANIFEST_PATH.parent,
        help="Staging data root; run artifacts are written below runs/<source>/<run-id>",
    )
    acquire.add_argument(
        "--manifest",
        type=Path,
        help="Manifest store path (defaults to <data-root>/manifests.json)",
    )
    acquire.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    acquire.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and show the proposed run without downloading or writing files",
    )
    acquire.add_argument(
        "--fixtures",
        type=Path,
        help="Use checked-in-style publisher metadata fixtures; intended for dry-run tests",
    )
    acquire.add_argument(
        "--timeout", type=float, default=60.0, help="Publisher request timeout in seconds"
    )
    acquire.add_argument(
        "--max-bytes",
        type=int,
        help="Optional hard transfer limit; defaults to the source-specific safety ceiling",
    )
    adopt_source = subparsers.add_parser(
        "adopt-validated-source-run",
        help="Adopt a retained, production-proven validated CMS run into staging",
    )
    adopt_source.add_argument("--source-manifest", required=True, type=Path)
    adopt_source.add_argument("--source-artifact", required=True, type=Path)
    adopt_source.add_argument("--production-evidence", required=True, type=Path)
    adopt_source.add_argument("--expected-source-id", required=True)
    adopt_source.add_argument("--expected-run-id", required=True)
    adopt_source.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    adopt_source.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    adopt_source.add_argument("--json", action="store_true")
    build = subparsers.add_parser(
        "build-release",
        help="Build and validate a versioned DuckDB candidate in staging",
    )
    build.add_argument("--source-run-id", required=True)
    build.add_argument("--backup-manifest", required=True, type=Path)
    build.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build.add_argument("--json", action="store_true")
    build_cms = subparsers.add_parser(
        "build-cms-release",
        help="Build all registered validated CMS source runs into one staging candidate",
    )
    build_cms.add_argument("--source-run-id", action="append", required=True)
    build_cms.add_argument("--backup-manifest", required=True, type=Path)
    build_cms.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_cms.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_cms.add_argument(
        "--memory-limit-gb", type=int, default=12,
        help="DuckDB memory ceiling for the full CMS build (default: 12 GiB)",
    )
    build_cms.add_argument(
        "--threads", type=int, default=1,
        help="DuckDB worker threads for the full CMS build (default: 1)",
    )
    build_cms.add_argument(
        "--spill-root", type=Path,
        help=(
            "Existing filesystem root for release-scoped DuckDB temporary files; "
            "defaults to data-root/staging/duckdb-spill"
        ),
    )
    build_cms.add_argument("--json", action="store_true")
    build_ppef = subparsers.add_parser(
        "build-ppef-release",
        help="Build only PPEF reassignment, practice-location, and relationship tables",
    )
    build_ppef.add_argument("--source-run-id", action="append", required=True)
    build_ppef.add_argument("--backup-manifest", required=True, type=Path)
    build_ppef.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_ppef.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_ppef.add_argument(
        "--memory-limit-gb", type=int, default=12,
        help="DuckDB memory ceiling for the targeted build (default: 12 GiB)",
    )
    build_ppef.add_argument(
        "--threads", type=int, default=1,
        help="DuckDB worker threads for the targeted build (default: 1)",
    )
    build_ppef.add_argument("--json", action="store_true")
    build_serving = subparsers.add_parser(
        "build-serving-practice-release",
        help="Build only the provider-search practice serving mart",
    )
    build_serving.add_argument("--baseline-warehouse-release-id", required=True)
    build_serving.add_argument("--backup-manifest", required=True, type=Path)
    build_serving.add_argument("--data-year", required=True, type=int)
    build_serving.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_serving.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_serving.add_argument(
        "--memory-limit-gb",
        type=int,
        default=12,
        help="DuckDB memory ceiling for the targeted build (default: 12 GiB)",
    )
    build_serving.add_argument(
        "--threads",
        type=int,
        default=1,
        help="DuckDB worker threads for the targeted build (default: 1)",
    )
    build_serving.add_argument("--json", action="store_true")
    build_managed_dac_serving = subparsers.add_parser(
        "build-managed-dac-serving-practice-release",
        help="Load one managed DAC run and build the practice serving mart",
    )
    build_managed_dac_serving.add_argument(
        "--baseline-warehouse-release-id", required=True
    )
    build_managed_dac_serving.add_argument("--dac-source-run-id", required=True)
    build_managed_dac_serving.add_argument(
        "--backup-manifest", required=True, type=Path
    )
    build_managed_dac_serving.add_argument("--data-year", required=True, type=int)
    build_managed_dac_serving.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_managed_dac_serving.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_managed_dac_serving.add_argument(
        "--memory-limit-gb",
        type=int,
        default=12,
        help="DuckDB memory ceiling for the targeted build (default: 12 GiB)",
    )
    build_managed_dac_serving.add_argument(
        "--threads",
        type=int,
        default=1,
        help="DuckDB worker threads for the targeted build (default: 1)",
    )
    build_managed_dac_serving.add_argument("--json", action="store_true")
    build_nppes_serving = subparsers.add_parser(
        "build-nppes-serving-practice-release",
        help="Build only the NPPES-primary practice serving tables",
    )
    build_nppes_serving.add_argument(
        "--baseline-warehouse-release-id", required=True
    )
    build_nppes_serving.add_argument("--backup-manifest", required=True, type=Path)
    build_nppes_serving.add_argument("--data-year", required=True, type=int)
    build_nppes_serving.add_argument(
        "--code-commit",
        help="Exact 40-character Git commit for a sealed source archive",
    )
    build_nppes_serving.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_nppes_serving.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_nppes_serving.add_argument(
        "--memory-limit-gb",
        type=int,
        default=12,
        help="DuckDB memory ceiling for the targeted build (default: 12 GiB)",
    )
    build_nppes_serving.add_argument(
        "--threads",
        type=int,
        default=1,
        help="DuckDB worker threads for the targeted build (default: 1)",
    )
    build_nppes_serving.add_argument("--json", action="store_true")
    build_profile_core = subparsers.add_parser(
        "build-provider-profile-core-release",
        help="Build only the provider-profile header, location, and group serving tables",
    )
    build_profile_core.add_argument(
        "--baseline-warehouse-release-id", required=True
    )
    build_profile_core.add_argument("--backup-manifest", required=True, type=Path)
    build_profile_core.add_argument("--data-year", required=True, type=int)
    build_profile_core.add_argument(
        "--reassignment-run-id",
        help=(
            "Verified managed run used to reconcile missing baseline "
            "reassignment provenance"
        ),
    )
    build_profile_core.add_argument(
        "--code-commit",
        help="Exact 40-character Git commit for a sealed source archive",
    )
    build_profile_core.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_profile_core.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_profile_core.add_argument(
        "--memory-limit-gb",
        type=int,
        default=12,
        help="DuckDB memory ceiling for the targeted build (default: 12 GiB)",
    )
    build_profile_core.add_argument(
        "--threads",
        type=int,
        default=1,
        help="DuckDB worker threads for the targeted build (default: 1)",
    )
    build_profile_core.add_argument("--json", action="store_true")
    build_profile = subparsers.add_parser(
        "build-provider-profile-release",
        help="Build the complete provider-profile core and claims serving capability",
    )
    build_profile.add_argument("--baseline-warehouse-release-id", required=True)
    build_profile.add_argument("--backup-manifest", required=True, type=Path)
    build_profile.add_argument("--data-year", required=True, type=int)
    build_profile.add_argument(
        "--reassignment-run-id",
        help=(
            "Verified managed run used to reconcile missing baseline "
            "reassignment provenance"
        ),
    )
    build_profile.add_argument(
        "--claims-service-run-id",
        help=(
            "Verified managed provider-and-service run used to reconcile "
            "missing baseline claims-detail provenance"
        ),
    )
    build_profile.add_argument(
        "--claims-drug-run-id",
        help=(
            "Verified managed provider-and-drug run used to reconcile "
            "missing baseline claims-detail provenance"
        ),
    )
    build_profile.add_argument(
        "--code-commit",
        help="Exact 40-character Git commit for a sealed source archive",
    )
    build_profile.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_profile.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_profile.add_argument(
        "--memory-limit-gb",
        type=int,
        default=12,
        help="DuckDB memory ceiling for the targeted build (default: 12 GiB)",
    )
    build_profile.add_argument(
        "--threads",
        type=int,
        default=1,
        help="DuckDB worker threads for the targeted build (default: 1)",
    )
    build_profile.add_argument("--json", action="store_true")
    build_platform = subparsers.add_parser(
        "build-platform-release",
        help="Build all CMS, NPPES, and Open Payments runs into one staging candidate",
    )
    build_platform.add_argument("--source-run-id", action="append", required=True)
    build_platform.add_argument("--backup-manifest", required=True, type=Path)
    build_platform.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_platform.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_platform.add_argument("--json", action="store_true")
    build_radar = subparsers.add_parser(
        "build-radar-release",
        help="Build a targeted NPPES Radar staging candidate",
    )
    build_radar.add_argument("--monthly-run-id", required=True)
    build_radar.add_argument("--weekly-run-id", action="append", default=[])
    build_radar.add_argument("--backup-manifest", required=True, type=Path)
    build_radar.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    build_radar.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    build_radar.add_argument("--json", action="store_true")
    prepare_aact = subparsers.add_parser(
        "prepare-aact-release",
        help="Prepare a sealed AACT PostgreSQL restore artifact in staging",
    )
    prepare_aact.add_argument("--source-run-id", required=True)
    prepare_aact.add_argument("--output-root", required=True, type=Path)
    prepare_aact.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    prepare_aact.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    prepare_aact.add_argument("--json", action="store_true")
    stage_aact = subparsers.add_parser(
        "stage-aact-database",
        help="Restore a sealed AACT artifact into a new validated PostgreSQL candidate",
    )
    stage_aact.add_argument("--release-manifest", required=True, type=Path)
    stage_aact.add_argument("--restore-log", required=True, type=Path)
    stage_aact.add_argument("--evidence", required=True, type=Path)
    stage_aact.add_argument("--minimum-study-count", type=int, default=500_000)
    stage_aact.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    stage_aact.add_argument("--json", action="store_true")
    compare = subparsers.add_parser(
        "compare-release",
        help="Compare a validated staging candidate with its immutable baseline",
    )
    compare.add_argument("--warehouse-release-id", required=True)
    compare.add_argument("--backup-manifest", required=True, type=Path)
    compare.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    compare.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    compare.add_argument("--json", action="store_true")
    promote = subparsers.add_parser(
        "promote",
        help="Atomically activate a validated release in staging only",
    )
    promote.add_argument("--warehouse-release-id", required=True)
    promote.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    promote.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    promote.add_argument("--json", action="store_true")
    rollback = subparsers.add_parser(
        "rollback",
        help="Roll back the isolated staging warehouse pointer",
    )
    rollback.add_argument(
        "--data-root", type=Path, default=DEFAULT_MANIFEST_PATH.parent
    )
    rollback.add_argument(
        "--environment", choices=[STAGING_ENVIRONMENT], required=True
    )
    rollback.add_argument("--json", action="store_true")
    return parser


def _discover_for_acquisition(
    source_id: str, *, fixture_dir: Path | None, timeout: float
) -> DiscoveryResult:
    if fixture_dir is not None:
        return discover_all(fixture_dir=fixture_dir, timeout=timeout)[source_id]
    return discover_source(source_id, timeout=timeout)


def _render_acquisition(payload: dict, *, dry_run: bool) -> str:
    if dry_run:
        return "\n".join(
            [
                "Immutable acquisition dry run",
                f"Source: {payload['source_id']}",
                f"Publisher version: {payload['publisher_version']}",
                f"Source period: {payload['source_data_period']}",
                f"Source URL: {payload['source_url']}",
                f"Proposed run: {payload['proposed_run_directory']}",
                "No files were downloaded or written.",
            ]
        )
    manifest = payload["manifest"]
    if payload.get("status") == "no_op":
        return "\n".join(
            [
                "Immutable acquisition no-op",
                f"Source: {manifest['source_id']}",
                f"Publisher version: {manifest['publisher_version']}",
                f"Existing run: {manifest['run_id']}",
                "No files were downloaded or written.",
            ]
        )
    return "\n".join(
        [
            "Immutable acquisition completed",
            f"Source: {manifest['source_id']}",
            f"Publisher version: {manifest['publisher_version']}",
            f"Source period: {manifest['source_data_period']}",
            f"Rows: {manifest['row_counts'].get('source_rows', 0)}",
            f"Bytes: {manifest['byte_size']}",
            f"SHA-256: {manifest['sha256']}",
            f"Run directory: {payload['run_directory']}",
            "Promotion state: not_promoted",
        ]
    )


def _completed_nppes_acquisition(
    manifest_path: Path, data_root: Path, release: ReleaseMetadata
) -> RunManifest | None:
    source_id = release.source_id
    publisher_version = release.publisher_version
    if source_id not in {"nppes_monthly_v2", "nppes_weekly_incremental_v2"}:
        return None
    matches = [
        manifest
        for manifest in ManifestStore(manifest_path).load().manifests
        if manifest.source_id == source_id
        and manifest.publisher_version == publisher_version
        and manifest.validation_state == ValidationState.PASSED
        and manifest.sha256
        and manifest.artifact_checksums.get("npidata_pfile.csv")
        and (
            data_root
            / "runs"
            / source_id
            / manifest.run_id
            / "npidata_pfile.csv"
        ).is_file()
    ]
    if not matches:
        return None
    return max(
        matches,
        key=lambda manifest: (manifest.retrieval_timestamp or "", manifest.run_id),
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command in {
        "adopt-validated-source-run",
        "build-release",
        "build-cms-release",
        "build-ppef-release",
        "build-serving-practice-release",
        "build-managed-dac-serving-practice-release",
        "build-nppes-serving-practice-release",
        "build-provider-profile-core-release",
        "build-provider-profile-release",
        "build-platform-release",
        "build-radar-release",
        "prepare-aact-release",
        "stage-aact-database",
        "compare-release",
        "promote",
        "rollback",
    }:
        try:
            if args.command == "adopt-validated-source-run":
                payload = adopt_validated_source_run(
                    data_root=args.data_root,
                    source_manifest_path=args.source_manifest,
                    source_artifact_path=args.source_artifact,
                    production_evidence_path=args.production_evidence,
                    expected_source_id=args.expected_source_id,
                    expected_run_id=args.expected_run_id,
                ).to_dict()
                heading = "Validated retained source run adopted into staging"
            elif args.command == "build-release":
                payload = build_warehouse_release(
                    data_root=args.data_root,
                    source_run_id=args.source_run_id,
                    backup_manifest_path=args.backup_manifest,
                ).to_dict()
                heading = "Staging warehouse release built"
            elif args.command == "build-cms-release":
                payload = build_full_cms_warehouse_release(
                    data_root=args.data_root,
                    source_run_ids=tuple(args.source_run_id),
                    backup_manifest_path=args.backup_manifest,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                    spill_root=args.spill_root,
                ).to_dict()
                heading = "Full CMS staging warehouse release built"
            elif args.command == "build-ppef-release":
                payload = build_ppef_warehouse_release(
                    data_root=args.data_root,
                    source_run_ids=tuple(args.source_run_id),
                    backup_manifest_path=args.backup_manifest,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                ).to_dict()
                heading = "Targeted PPEF staging warehouse release built"
            elif args.command == "build-serving-practice-release":
                payload = build_serving_practice_warehouse_release(
                    data_root=args.data_root,
                    baseline_warehouse_release_id=(
                        args.baseline_warehouse_release_id
                    ),
                    backup_manifest_path=args.backup_manifest,
                    data_year=args.data_year,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                ).to_dict()
                heading = "Targeted practice serving-mart release built"
            elif args.command == "build-managed-dac-serving-practice-release":
                payload = build_managed_dac_serving_practice_warehouse_release(
                    data_root=args.data_root,
                    baseline_warehouse_release_id=(
                        args.baseline_warehouse_release_id
                    ),
                    dac_source_run_id=args.dac_source_run_id,
                    backup_manifest_path=args.backup_manifest,
                    data_year=args.data_year,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                ).to_dict()
                heading = "Managed DAC practice serving-mart release built"
            elif args.command == "build-nppes-serving-practice-release":
                payload = build_nppes_serving_practice_warehouse_release(
                    data_root=args.data_root,
                    baseline_warehouse_release_id=(
                        args.baseline_warehouse_release_id
                    ),
                    backup_manifest_path=args.backup_manifest,
                    data_year=args.data_year,
                    code_commit=args.code_commit,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                ).to_dict()
                heading = "NPPES-primary practice serving-mart release built"
            elif args.command == "build-provider-profile-core-release":
                payload = build_provider_profile_core_warehouse_release(
                    data_root=args.data_root,
                    baseline_warehouse_release_id=(
                        args.baseline_warehouse_release_id
                    ),
                    backup_manifest_path=args.backup_manifest,
                    data_year=args.data_year,
                    reassignment_run_id=args.reassignment_run_id,
                    code_commit=args.code_commit,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                ).to_dict()
                heading = "Provider-profile core serving-mart release built"
            elif args.command == "build-provider-profile-release":
                payload = build_provider_profile_warehouse_release(
                    data_root=args.data_root,
                    baseline_warehouse_release_id=(
                        args.baseline_warehouse_release_id
                    ),
                    backup_manifest_path=args.backup_manifest,
                    data_year=args.data_year,
                    reassignment_run_id=args.reassignment_run_id,
                    claims_service_run_id=args.claims_service_run_id,
                    claims_drug_run_id=args.claims_drug_run_id,
                    code_commit=args.code_commit,
                    memory_limit_gb=args.memory_limit_gb,
                    threads=args.threads,
                ).to_dict()
                heading = "Complete provider-profile serving-mart release built"
            elif args.command == "build-platform-release":
                payload = build_full_platform_warehouse_release(
                    data_root=args.data_root,
                    source_run_ids=tuple(args.source_run_id),
                    backup_manifest_path=args.backup_manifest,
                ).to_dict()
                heading = "Full platform staging warehouse release built"
            elif args.command == "build-radar-release":
                payload = build_radar_warehouse_release(
                    data_root=args.data_root,
                    monthly_run_id=args.monthly_run_id,
                    weekly_run_ids=tuple(args.weekly_run_id),
                    backup_manifest_path=args.backup_manifest,
                ).to_dict()
                heading = "NPPES Radar staging warehouse release built"
            elif args.command == "prepare-aact-release":
                payload = prepare_aact_release(
                    data_root=args.data_root,
                    source_run_id=args.source_run_id,
                    output_root=args.output_root,
                ).to_dict()
                heading = "AACT staging restore release prepared"
            elif args.command == "stage-aact-database":
                result = stage_aact_database(
                    release_manifest_path=args.release_manifest,
                    restore_log_path=args.restore_log,
                    minimum_study_count=args.minimum_study_count,
                )
                write_stage_evidence(args.evidence, result)
                payload = {
                    "result": result.to_dict(),
                    "evidence": str(args.evidence),
                    "restore_log": str(args.restore_log),
                }
                heading = "AACT PostgreSQL candidate restored and validated"
            elif args.command == "compare-release":
                payload = compare_warehouse_release(
                    data_root=args.data_root,
                    warehouse_release_id=args.warehouse_release_id,
                    backup_manifest_path=args.backup_manifest,
                )
                heading = "Staging warehouse release comparison passed"
            elif args.command == "promote":
                payload = promote_staging_release(
                    args.data_root, args.warehouse_release_id
                )
                heading = "Staging warehouse release promoted"
            else:
                payload = rollback_staging_release(args.data_root)
                heading = "Staging warehouse release rolled back"
        except (OSError, ValueError, ReleaseError, SourceAdoptionError) as error:
            payload = {
                "environment": STAGING_ENVIRONMENT,
                "command": args.command,
                "error": safe_error(error),
            }
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"Release operation failed: {payload['error']}", file=sys.stderr)
            return EXIT_RELEASE_FAILURE
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(heading)
            print(json.dumps(payload, indent=2, sort_keys=True))
        return EXIT_HEALTHY

    if args.command == "acquire":
        if args.fixtures is not None and not args.dry_run:
            message = "Fixture metadata is allowed only with --dry-run"
            if args.json:
                print(json.dumps({"error": message}, indent=2, sort_keys=True))
            else:
                print(f"Acquisition failed: {message}", file=sys.stderr)
            return EXIT_ACQUISITION_FAILURE
        try:
            discovery = _discover_for_acquisition(
                args.source_id,
                fixture_dir=args.fixtures,
                timeout=args.timeout,
            )
        except (OSError, ValueError) as error:
            discovery = DiscoveryResult(
                source_id=args.source_id,
                state=DiscoveryState.ERROR,
                discovered_at=utc_now(),
                error_summary=safe_error(error),
            )
        if discovery.state != DiscoveryState.AVAILABLE or discovery.release is None:
            payload = {
                "source_id": args.source_id,
                "discovery_state": discovery.state.value,
                "error": discovery.error_summary
                or "Publisher discovery returned no usable release.",
            }
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"Acquisition unavailable: {payload['error']}", file=sys.stderr)
            return EXIT_DISCOVERY_FAILURE

        release = discovery.release
        manifest_path = args.manifest or args.data_root / "manifests.json"
        if args.dry_run:
            proposed_run_id = make_run_id()
            payload = {
                "dry_run": True,
                "source_id": release.source_id,
                "publisher_version": release.publisher_version,
                "release_id": release_id(release.source_id, release.publisher_version),
                "source_data_period": release.source_data_period,
                "publisher_release_timestamp": release.publisher_release_timestamp,
                "source_url": release.source_url,
                "manifest_path": str(manifest_path),
                "proposed_run_directory": str(
                    args.data_root / "runs" / release.source_id / proposed_run_id
                ),
                "wrote_files": False,
            }
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(_render_acquisition(payload, dry_run=True))
            return EXIT_HEALTHY

        try:
            completed = _completed_nppes_acquisition(
                manifest_path, args.data_root, release
            )
        except (OSError, ValueError) as error:
            payload = {"source_id": args.source_id, "error": safe_error(error)}
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"Acquisition failed: {payload['error']}", file=sys.stderr)
            return EXIT_ACQUISITION_FAILURE
        if completed is not None:
            payload = {
                "status": "no_op",
                "reason": "publisher_version_already_acquired",
                "manifest": completed.to_dict(),
                "wrote_files": False,
            }
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(_render_acquisition(payload, dry_run=False))
            return EXIT_HEALTHY

        try:
            acquire_function = (
                acquire_archive_release
                if release.source_id in SUPPORTED_ARCHIVE_ACQUISITION_SOURCES
                else acquire_release
            )
            result = acquire_function(
                release,
                discovery_timestamp=discovery.discovered_at,
                data_root=args.data_root,
                manifest_path=manifest_path,
                max_bytes=args.max_bytes,
                timeout=args.timeout,
            )
        except (AcquisitionError, OSError, ValueError) as error:
            payload = {"source_id": args.source_id, "error": safe_error(error)}
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(f"Acquisition failed: {payload['error']}", file=sys.stderr)
            return EXIT_ACQUISITION_FAILURE
        payload = result.to_dict()
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(_render_acquisition(payload, dry_run=False))
        return EXIT_HEALTHY

    fixture_dir = DEFAULT_FIXTURE_DIR if args.offline else args.fixtures
    discovery_mode = f"fixtures:{fixture_dir}" if fixture_dir else "live"
    try:
        manifests = ManifestStore(args.manifest).load()
    except (OSError, ValueError) as error:
        payload = {
            "schema_version": 1,
            "generated_at": utc_now(),
            "manifest_path": str(args.manifest),
            "discovery_mode": discovery_mode,
            "error": safe_error(error),
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Status failed: {payload['error']}", file=sys.stderr)
        return EXIT_DISCOVERY_FAILURE

    discoveries = discover_all(fixture_dir=fixture_dir, timeout=args.timeout)
    report = build_status_report(
        discoveries,
        manifests,
        manifest_path=args.manifest,
        discovery_mode=discovery_mode,
    )
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_human(report))
    return report.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
