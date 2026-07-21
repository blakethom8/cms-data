"""Read-only publisher freshness monitoring for the selected production release.

The selected deployment ID comes from the validated production bundle pointer. Its
source provenance is read from deployment-scoped evidence, so a newer staging
manifest can never make an older production warehouse appear current.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .data_platform import (
    DEFAULT_FIXTURE_DIR,
    EXIT_DISCOVERY_FAILURE,
    StatusReport,
    build_status_report,
    render_human,
)
from .discovery import discover_all, safe_error, utc_now
from .manifests import ManifestStore
from .production_manager import ProductionError, production_status


SOURCE_MANIFEST_EVIDENCE = "source-manifests.json"


def selected_manifest_path(production_root: Path, selected_deployment_id: str) -> Path:
    """Return the provenance snapshot selected by the one production bundle pointer."""
    return (
        production_root
        / "evidence"
        / selected_deployment_id
        / SOURCE_MANIFEST_EVIDENCE
    )


def build_production_status(
    production_root: Path,
    *,
    fixture_dir: Path | None = None,
    timeout: float = 30.0,
) -> tuple[dict, int, StatusReport | None]:
    """Build one monitoring result without downloading data or opening DuckDB."""
    control = production_status(production_root)
    selected_id = control.get("selected_deployment_id")
    discovery_mode = f"fixtures:{fixture_dir}" if fixture_dir else "live"
    if not control.get("healthy") or not isinstance(selected_id, str):
        payload = {
            "schema_version": 1,
            "generated_at": utc_now(),
            "discovery_mode": discovery_mode,
            "error": "Production control plane is not healthy; publisher status was not inferred.",
            "production": control,
        }
        return payload, EXIT_DISCOVERY_FAILURE, None

    manifest_path = selected_manifest_path(production_root, selected_id)
    try:
        manifests = ManifestStore(manifest_path).load()
    except (OSError, ProductionError, ValueError) as error:
        payload = {
            "schema_version": 1,
            "generated_at": utc_now(),
            "manifest_path": str(manifest_path),
            "discovery_mode": discovery_mode,
            "error": safe_error(error),
            "production": control,
        }
        return payload, EXIT_DISCOVERY_FAILURE, None

    discoveries = discover_all(fixture_dir=fixture_dir, timeout=timeout)
    report = build_status_report(
        discoveries,
        manifests,
        manifest_path=manifest_path,
        discovery_mode=discovery_mode,
    )
    payload = report.to_dict()
    payload["production"] = {
        "control_plane_healthy": True,
        "selected_deployment_id": selected_id,
        "selected_code_commit": control.get("selected_code_commit"),
        "selected_warehouse_release_id": control.get("selected_warehouse_release_id"),
        "last_verified_at": control.get("last_verified_at"),
        "source_manifest_evidence": str(manifest_path),
        "source_manifest_present": manifest_path.is_file(),
    }
    return payload, report.exit_code, report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monitor publisher freshness for the selected production release"
    )
    parser.add_argument("--production-root", required=True, type=Path)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    fixtures = parser.add_mutually_exclusive_group()
    fixtures.add_argument("--fixtures", type=Path)
    fixtures.add_argument(
        "--offline",
        action="store_true",
        help="Use checked-in publisher fixtures and make no network requests",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    fixture_dir = DEFAULT_FIXTURE_DIR if args.offline else args.fixtures
    try:
        payload, exit_code, report = build_production_status(
            args.production_root,
            fixture_dir=fixture_dir,
            timeout=args.timeout,
        )
    except (OSError, ValueError) as error:
        payload = {
            "schema_version": 1,
            "generated_at": utc_now(),
            "error": safe_error(error),
        }
        exit_code = EXIT_DISCOVERY_FAILURE
        report = None

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    elif report is not None:
        production = payload["production"]
        print(
            "Production deployment: "
            f"{production['selected_deployment_id']} "
            f"(warehouse {production['selected_warehouse_release_id'] or '-'})"
        )
        print(render_human(report))
    else:
        print(f"Production status monitoring failed: {payload['error']}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
