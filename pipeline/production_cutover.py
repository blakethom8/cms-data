"""One-shot production cutover with automatic restart and rollback.

This command is intentionally narrow: select one prepared deployment, restart
one systemd service, run the bounded smoke suite, and either verify the selected
deployment or restore and verify its predecessor.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import production_manager as manager
from . import production_smoke as smoke


SYSTEMCTL = "/usr/bin/systemctl"


class CutoverError(RuntimeError):
    """The cutover could not produce a verified candidate or rollback."""


@dataclass(frozen=True)
class ExpectedCounts:
    core_providers: int
    hospital_affiliations: int
    affiliated_providers: int
    raw_hospital_enrollments: int
    aact_study_count: int = 0
    aact_snapshot_date: str = ""
    table_counts: dict[str, int] = field(default_factory=dict)


def _restart_service(service: str) -> int:
    if not re.fullmatch(r"[A-Za-z0-9_.@-]+", service):
        raise CutoverError("Invalid systemd service name")
    subprocess.run([SYSTEMCTL, "restart", service], check=True, timeout=90)
    result = subprocess.run(
        [SYSTEMCTL, "show", service, "--property", "MainPID", "--value"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    try:
        process_id = int(result.stdout.strip())
    except ValueError as error:
        raise CutoverError("systemd returned an invalid service process ID") from error
    if process_id <= 0:
        raise CutoverError("systemd service has no running process")
    return process_id


def _wait_ready(base_url: str, timeout_seconds: int = 60) -> None:
    smoke._validate_loopback_url(base_url)
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status, body = smoke._request(base_url, "GET", "/health", None)
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                return
        except Exception as error:
            last_error = error
        time.sleep(1)
    summary = manager.safe_error(last_error) if last_error else "health endpoint did not become ready"
    raise CutoverError(f"Service did not become ready: {summary}")


def _failed_evidence(deployment_id: str, base_url: str, error: BaseException) -> dict:
    return {
        "schema_version": smoke.SMOKE_SCHEMA_VERSION,
        "deployment_id": deployment_id,
        "generated_at": smoke.utc_now(),
        "base_url": base_url,
        "state": "failed",
        "checks": [],
        "error_summary": manager.safe_error(error),
    }


def _run_smoke_and_record(
    *,
    production_root: Path,
    deployment_id: str,
    base_url: str,
    api_key: str,
    process_id: int,
    counts: ExpectedCounts,
    representative_npi: str,
    expected_industry_detail_status: int,
) -> tuple[dict, Path]:
    evidence_path = production_root / "evidence" / deployment_id / "smoke.json"
    try:
        evidence = smoke.run_smoke(
            base_url=base_url,
            deployment_id=deployment_id,
            api_key=api_key,
            expected_core_providers=counts.core_providers,
            expected_hospital_affiliations=counts.hospital_affiliations,
            expected_affiliated_providers=counts.affiliated_providers,
            expected_raw_hospital_enrollments=counts.raw_hospital_enrollments,
            expected_aact_study_count=counts.aact_study_count,
            expected_aact_snapshot_date=counts.aact_snapshot_date,
            expected_table_counts=counts.table_counts,
            expected_industry_detail_status=expected_industry_detail_status,
            representative_npi=representative_npi,
            process_id=process_id,
            production_root=production_root,
        )
    except Exception as error:
        evidence = _failed_evidence(deployment_id, base_url, error)
    smoke._write_json_atomic(evidence_path, evidence)
    return evidence, evidence_path


def execute_cutover(
    *,
    production_root: Path,
    deployment_id: str,
    service: str,
    base_url: str,
    api_key: str,
    candidate_counts: ExpectedCounts,
    rollback_counts: ExpectedCounts,
    representative_npi: str = "1003005257",
    candidate_industry_detail_status: int = 200,
    rollback_industry_detail_status: int = 200,
) -> dict:
    """Select and verify a candidate, or restore and verify its predecessor."""
    if os.geteuid() != 0:
        raise CutoverError("Production cutover must run as root")
    smoke._validate_loopback_url(base_url)
    manager.activate_release(production_root, deployment_id, dry_run=True)
    selected = manager.activate_release(production_root, deployment_id)
    try:
        process_id = _restart_service(service)
        _wait_ready(base_url)
        evidence, evidence_path = _run_smoke_and_record(
            production_root=production_root,
            deployment_id=selected.deployment_id,
            base_url=base_url,
            api_key=api_key,
            process_id=process_id,
            counts=candidate_counts,
            representative_npi=representative_npi,
            expected_industry_detail_status=candidate_industry_detail_status,
        )
        if evidence.get("state") != "passed":
            raise CutoverError(evidence.get("error_summary") or "Candidate smoke failed")
        manager.verify_release(production_root, selected.deployment_id, evidence_path)
        return {
            "state": "promoted",
            "selected_deployment_id": selected.deployment_id,
            "smoke_evidence": str(evidence_path),
            "rollback_available": True,
        }
    except Exception as candidate_error:
        try:
            rollback = manager.rollback_release(production_root)
            rollback_pid = _restart_service(service)
            _wait_ready(base_url)
            rollback_evidence, rollback_path = _run_smoke_and_record(
                production_root=production_root,
                deployment_id=rollback.deployment_id,
                base_url=base_url,
                api_key=api_key,
                process_id=rollback_pid,
                counts=rollback_counts,
                representative_npi=representative_npi,
                expected_industry_detail_status=rollback_industry_detail_status,
            )
            if rollback_evidence.get("state") != "passed":
                raise CutoverError(
                    rollback_evidence.get("error_summary") or "Rollback smoke failed"
                )
            manager.verify_release(production_root, rollback.deployment_id, rollback_path)
        except Exception as rollback_error:
            raise CutoverError(
                "Candidate failed and rollback could not be verified: "
                f"candidate={manager.safe_error(candidate_error)}; "
                f"rollback={manager.safe_error(rollback_error)}"
            ) from rollback_error
        return {
            "state": "rolled_back",
            "selected_deployment_id": rollback.deployment_id,
            "failed_deployment_id": selected.deployment_id,
            "candidate_error": manager.safe_error(candidate_error),
            "smoke_evidence": str(rollback_path),
            "rollback_available": True,
        }


def _counts(prefix: str, args: argparse.Namespace) -> ExpectedCounts:
    return ExpectedCounts(
        core_providers=getattr(args, f"{prefix}_core_providers"),
        hospital_affiliations=getattr(args, f"{prefix}_hospital_affiliations"),
        affiliated_providers=getattr(args, f"{prefix}_affiliated_providers"),
        raw_hospital_enrollments=getattr(args, f"{prefix}_raw_hospital_enrollments"),
        aact_study_count=getattr(args, f"{prefix}_aact_study_count"),
        aact_snapshot_date=getattr(args, f"{prefix}_aact_snapshot_date"),
        table_counts=smoke._load_expected_table_counts(
            getattr(args, f"{prefix}_table_counts")
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select, restart, smoke, and verify one release")
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--service", default="cms-api.service")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--api-key-env", default="CMS_API_KEY")
    parser.add_argument("--representative-npi", default="1003005257")
    parser.add_argument(
        "--candidate-industry-detail-status", type=int, choices=(200, 404), default=200
    )
    parser.add_argument(
        "--rollback-industry-detail-status", type=int, choices=(200, 404), default=200
    )
    for prefix in ("candidate", "rollback"):
        parser.add_argument(f"--{prefix}-core-providers", type=int, required=True)
        parser.add_argument(f"--{prefix}-hospital-affiliations", type=int, required=True)
        parser.add_argument(f"--{prefix}-affiliated-providers", type=int, required=True)
        parser.add_argument(f"--{prefix}-raw-hospital-enrollments", type=int, required=True)
        parser.add_argument(f"--{prefix}-aact-study-count", type=int, required=True)
        parser.add_argument(f"--{prefix}-aact-snapshot-date", required=True)
        parser.add_argument(
            f"--{prefix}-table-counts",
            type=Path,
            help="Release JSON containing validation_details.smoke_table_counts",
        )
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    api_key = os.getenv(args.api_key_env, "")
    if not api_key:
        error = f"API key environment variable is empty: {args.api_key_env}"
        if args.json:
            print(json.dumps({"state": "error", "error_summary": error}))
        else:
            print(f"Cutover error: {error}", file=sys.stderr)
        return 2
    try:
        result = execute_cutover(
            production_root=args.production_root,
            deployment_id=args.deployment_id,
            service=args.service,
            base_url=args.base_url,
            api_key=api_key,
            candidate_counts=_counts("candidate", args),
            rollback_counts=_counts("rollback", args),
            representative_npi=args.representative_npi,
            candidate_industry_detail_status=args.candidate_industry_detail_status,
            rollback_industry_detail_status=args.rollback_industry_detail_status,
        )
    except Exception as error:
        if args.json:
            print(json.dumps({"state": "error", "error_summary": manager.safe_error(error)}))
        else:
            print(f"Cutover error: {manager.safe_error(error)}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Cutover: {result['state']}")
        print(f"Selected deployment: {result['selected_deployment_id']}")
        print(f"Smoke evidence: {result['smoke_evidence']}")
    return 0 if result["state"] == "promoted" else 1


if __name__ == "__main__":
    raise SystemExit(main())
