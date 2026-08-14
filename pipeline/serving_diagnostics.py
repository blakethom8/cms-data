"""Read-only canonical API diagnostic runner for serving-mart comparisons."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import statistics
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


DIAGNOSTIC_SCHEMA_VERSION = 1
IDENTITY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")


@dataclass(frozen=True)
class DiagnosticCase:
    name: str
    family: str
    path: str
    expected_status: int
    result_count_field: str | None
    purpose: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_error(error: BaseException) -> str:
    return (" ".join(str(error).split()) or error.__class__.__name__)[:300]


def _validate_origin(origin: str) -> str:
    parsed = urlsplit(origin)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Base URL must be an exact HTTP(S) origin without credentials")
    return origin.rstrip("/")


def load_cases(path: Path) -> tuple[list[DiagnosticCase], str]:
    raw = path.read_bytes()
    document = json.loads(raw)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("Diagnostic manifest schema_version must be 1")
    entries = document.get("cases")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Diagnostic manifest must contain a non-empty cases list")

    cases: list[DiagnosticCase] = []
    names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Every diagnostic case must be an object")
        allowed = {
            "name", "family", "path", "expected_status", "result_count_field", "purpose"
        }
        if set(entry) - allowed:
            raise ValueError(f"Unknown diagnostic case fields: {sorted(set(entry) - allowed)}")
        name = entry.get("name")
        family = entry.get("family")
        request_path = entry.get("path")
        status = entry.get("expected_status", 200)
        count_field = entry.get("result_count_field")
        purpose = entry.get("purpose")
        if not isinstance(name, str) or not IDENTITY_PATTERN.fullmatch(name) or name in names:
            raise ValueError("Case names must be safe, non-empty, and unique")
        if not isinstance(family, str) or not IDENTITY_PATTERN.fullmatch(family):
            raise ValueError(f"Case family is invalid: {name}")
        if (
            not isinstance(request_path, str)
            or not request_path.startswith("/")
            or request_path.startswith("//")
            or "#" in request_path
        ):
            raise ValueError(f"Case path must be an absolute-origin path: {name}")
        if not isinstance(status, int) or isinstance(status, bool) or not 100 <= status <= 599:
            raise ValueError(f"Case expected_status is invalid: {name}")
        if count_field is not None and (
            not isinstance(count_field, str)
            or not count_field
            or any(not part for part in count_field.split("."))
        ):
            raise ValueError(f"Case result_count_field is invalid: {name}")
        if not isinstance(purpose, str) or not purpose.strip():
            raise ValueError(f"Case purpose is required: {name}")
        names.add(name)
        cases.append(
            DiagnosticCase(name, family, request_path, status, count_field, purpose.strip())
        )
    return cases, hashlib.sha256(raw).hexdigest()


def _result_count(payload: object, field: str | None) -> int | None:
    if field is None:
        if isinstance(payload, list):
            return len(payload)
        return None
    value = payload
    for part in field.split("."):
        if not isinstance(value, dict) or part not in value:
            raise ValueError(f"Response does not contain count field {field}")
        value = value[part]
    if isinstance(value, list):
        return len(value)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"Response count field is not a non-negative integer or list: {field}")
    return value


def _request_case(
    origin: str, case: DiagnosticCase, api_key: str, timeout_seconds: float
) -> dict:
    request = urllib.request.Request(
        f"{origin}{case.path}",
        headers={"Accept": "application/json", "X-API-Key": api_key},
        method="GET",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as error:
        status = error.code
        body = error.read()
    elapsed_ms = (time.perf_counter() - started) * 1000
    payload = json.loads(body)
    count = _result_count(payload, case.result_count_field)
    return {
        "name": case.name,
        "family": case.family,
        "path": case.path,
        "purpose": case.purpose,
        "expected_status": case.expected_status,
        "status_code": status,
        "passed": status == case.expected_status,
        "elapsed_ms": round(elapsed_ms, 2),
        "response_bytes": len(body),
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "result_count": count,
    }


def run_diagnostics(
    *, origin: str, api_key: str, cases: list[DiagnosticCase], manifest_sha256: str,
    deployment_id: str, warehouse_release_id: str, warehouse_sha256: str,
    code_commit: str, executor_settings: dict[str, str | int | float],
    timeout_seconds: float, trials: int = 3,
) -> dict:
    origin = _validate_origin(origin)
    if trials < 1:
        raise ValueError("trials must be positive")
    trial_results = [
        [_request_case(origin, case, api_key, timeout_seconds) for case in cases]
        for _ in range(trials)
    ]
    results: list[dict] = []
    for index, case in enumerate(cases):
        observations = [trial[index] for trial in trial_results]
        response_digests = {row["response_sha256"] for row in observations}
        results.append(
            {
                "name": case.name,
                "family": case.family,
                "path": case.path,
                "purpose": case.purpose,
                "expected_status": case.expected_status,
                "passed": all(row["passed"] for row in observations)
                and len(response_digests) == 1,
                "response_stable": len(response_digests) == 1,
                "median_elapsed_ms": round(
                    statistics.median(row["elapsed_ms"] for row in observations), 2
                ),
                "trials": observations,
            }
        )
    return {
        "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "identity": {
            "deployment_id": deployment_id,
            "warehouse_release_id": warehouse_release_id,
            "warehouse_sha256": warehouse_sha256,
            "code_commit": code_commit,
            "executor_settings": executor_settings,
        },
        "base_origin": origin,
        "case_manifest_sha256": manifest_sha256,
        "timeout_seconds": timeout_seconds,
        "trial_count": trials,
        "case_count": len(results),
        "status_failure_count": sum(
            any(not trial["passed"] for trial in result["trials"])
            for result in results
        ),
        "stability_failure_count": sum(
            not result["response_stable"] for result in results
        ),
        "failure_count": sum(not result["passed"] for result in results),
        "cases": results,
    }


def _executor_settings(value: str) -> dict[str, str | int | float]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict) or not parsed:
        raise argparse.ArgumentTypeError("executor settings must be a non-empty JSON object")
    if any(
        not isinstance(key, str)
        or not isinstance(item, (str, int, float))
        or isinstance(item, bool)
        for key, item in parsed.items()
    ):
        raise argparse.ArgumentTypeError("executor settings must contain scalar values")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--cases", required=True, type=Path)
    parser.add_argument("--api-key-env", default="CMS_API_KEY")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--warehouse-release-id", required=True)
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument("--warehouse-sha256", required=True)
    parser.add_argument("--code-commit", required=True)
    parser.add_argument("--executor-settings", required=True, type=_executor_settings)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        parser.error(f"API key environment variable is empty: {args.api_key_env}")
    try:
        if args.timeout_seconds <= 0 or args.trials <= 0:
            raise ValueError("timeout and trials must be positive")
        if args.warehouse.is_symlink() or not args.warehouse.is_file():
            raise ValueError("Warehouse must be a regular non-symlink file")
        if args.warehouse.stat().st_mode & 0o222:
            raise ValueError("Warehouse must have no write permission bits")
        actual_sha256 = _sha256_file(args.warehouse)
        if actual_sha256 != args.warehouse_sha256:
            raise ValueError("Warehouse SHA-256 does not match the declared identity")
        cases, manifest_sha256 = load_cases(args.cases)
        evidence = run_diagnostics(
            origin=args.base_url, api_key=api_key, cases=cases,
            manifest_sha256=manifest_sha256, deployment_id=args.deployment_id,
            warehouse_release_id=args.warehouse_release_id,
            warehouse_sha256=actual_sha256, code_commit=args.code_commit,
            executor_settings=args.executor_settings, timeout_seconds=args.timeout_seconds,
            trials=args.trials,
        )
    except (OSError, ValueError, urllib.error.URLError) as error:
        parser.error(_safe_error(error))
    rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    return int(evidence["failure_count"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
