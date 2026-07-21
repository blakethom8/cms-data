"""Read-only production API smoke checks with safe JSON evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


SMOKE_SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_error(error: BaseException) -> str:
    return (" ".join(str(error).split()) or error.__class__.__name__)[:500]


def _validate_loopback_url(base_url: str) -> None:
    parsed = urlsplit(base_url)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("Smoke base URL has an invalid port") from error
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Smoke base URL must be an exact HTTP loopback origin")


def _request(
    base_url: str,
    method: str,
    path: str,
    api_key: str | None,
    payload: dict | None = None,
) -> tuple[int, object]:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = None
        return error.code, body


def _check(name: str, condition: bool, status: int | None, summary: dict | None = None) -> dict:
    return {
        "name": name,
        "state": "passed" if condition else "failed",
        "status_code": status,
        "summary": summary or {},
    }


def _references_runtime(command: list[str], roots: list[Path]) -> bool:
    for value in command:
        for root in roots:
            root_text = str(root)
            if value == root_text or value.startswith(root_text + os.sep):
                return True
    return False


def _process_identity(
    process_id: int,
    production_root: Path,
    deployment_id: str,
    release_bundle: Path | None = None,
) -> tuple[bool, dict]:
    if process_id <= 0 or not production_root.is_absolute():
        return False, {"process_id": process_id}
    selector = production_root / "release-current"
    bundle_path = release_bundle or selector
    if release_bundle is None and not selector.is_symlink():
        return False, {"process_id": process_id}
    try:
        bundle = bundle_path.resolve(strict=True)
        if bundle.name != deployment_id or bundle.is_symlink() or not bundle.is_dir():
            return False, {"process_id": process_id, "bundle": str(bundle)}
        code_pointer = bundle / "code"
        warehouse_pointer = bundle / "warehouse"
        runtime_pointer = bundle / "runtime"
        if not all(pointer.is_symlink() for pointer in (code_pointer, warehouse_pointer, runtime_pointer)):
            return False, {"process_id": process_id, "bundle": str(bundle)}
        expected_code = code_pointer.resolve(strict=True)
        expected_warehouse = warehouse_pointer.resolve(strict=True)
        expected_runtime = runtime_pointer.resolve(strict=True)
        actual_code = Path(f"/proc/{process_id}/cwd").resolve(strict=True)
        open_targets: set[Path] = set()
        for descriptor in Path(f"/proc/{process_id}/fd").iterdir():
            try:
                open_targets.add(descriptor.resolve(strict=True))
            except (FileNotFoundError, OSError):
                continue
        command = Path(f"/proc/{process_id}/cmdline").read_bytes().split(b"\0")
        command_text = [part.decode(errors="replace") for part in command if part]
    except (FileNotFoundError, OSError):
        return False, {"process_id": process_id}
    runtime_roots = [runtime_pointer, expected_runtime]
    try:
        if selector.is_symlink() and selector.resolve(strict=True) == bundle:
            runtime_roots.append(selector / "runtime")
    except (FileNotFoundError, OSError):
        pass
    runtime_referenced = _references_runtime(command_text, runtime_roots)
    passed = (
        actual_code == expected_code
        and expected_warehouse in open_targets
        and runtime_referenced
    )
    return passed, {
        "process_id": process_id,
        "deployment_id": deployment_id,
        "bundle": str(bundle),
        "code_target": str(actual_code),
        "warehouse_open": expected_warehouse in open_targets,
        "runtime_referenced": runtime_referenced,
    }


def run_smoke(
    *,
    base_url: str,
    deployment_id: str,
    api_key: str,
    expected_core_providers: int,
    expected_hospital_affiliations: int,
    expected_affiliated_providers: int,
    expected_raw_hospital_enrollments: int,
    expected_industry_detail_status: int = 200,
    representative_npi: str,
    process_id: int,
    production_root: Path,
    release_bundle: Path | None = None,
    expected_table_counts: dict[str, int] | None = None,
) -> dict:
    """Run bounded read-only checks and return evidence without response bodies."""
    _validate_loopback_url(base_url)
    if expected_industry_detail_status not in {200, 404}:
        raise ValueError("Expected industry detail status must be 200 or 404")
    checks: list[dict] = []

    status, health = _request(base_url, "GET", "/health", api_key)
    health_ok = (
        status == 200
        and isinstance(health, dict)
        and health.get("status") == "ok"
        and health.get("core_providers") == expected_core_providers
    )
    checks.append(
        _check(
            "health",
            health_ok,
            status,
            {"core_providers": health.get("core_providers") if isinstance(health, dict) else None},
        )
    )

    process_ok, process_summary = _process_identity(
        process_id, production_root, deployment_id, release_bundle
    )
    checks.append(_check("process_identity", process_ok, None, process_summary))

    status, _ = _request(base_url, "GET", "/practices/capabilities", None)
    checks.append(_check("authentication_required", status == 401, status))

    status, capabilities = _request(
        base_url, "GET", "/practices/capabilities", api_key
    )
    required_capabilities = {
        "multi_zip",
        "nppes_primary",
        "exact_radius",
        "multi_specialty",
        "practice_specialties",
        "scoped_metrics",
    }
    advertised = set(capabilities.get("capabilities", [])) if isinstance(capabilities, dict) else set()
    capabilities_ok = (
        status == 200
        and isinstance(capabilities, dict)
        and capabilities.get("contract_version") == 2
        and required_capabilities.issubset(advertised)
    )
    checks.append(
        _check(
            "practice_capabilities",
            capabilities_ok,
            status,
            {
                "contract_version": capabilities.get("contract_version")
                if isinstance(capabilities, dict)
                else None
            },
        )
    )

    status, practices = _request(
        base_url,
        "GET",
        "/practices/search?specialty=Cardiology&state=OH&limit=3",
        api_key,
    )
    practices_ok = (
        status == 200
        and isinstance(practices, dict)
        and isinstance(practices.get("results"), list)
        and practices.get("total", 0) > 0
        and bool(practices.get("results"))
        and isinstance(practices["results"][0], dict)
        and {
            "contract_version",
            "site_id",
            "organization_scope",
            "address",
            "city",
            "state",
            "zip5",
            "providers_here",
        }.issubset(practices["results"][0])
    )
    checks.append(
        _check(
            "practice_search",
            practices_ok,
            status,
            {
                "total": practices.get("total") if isinstance(practices, dict) else None,
                "returned": len(practices.get("results", []))
                if isinstance(practices, dict)
                else None,
            },
        )
    )

    status, profile = _request(
        base_url, "GET", f"/profiles/{representative_npi}", api_key
    )
    profile_ok = (
        status == 200
        and isinstance(profile, dict)
        and str(profile.get("npi")) == representative_npi
        and {
            "header",
            "panel",
            "clinical",
            "prescribing",
            "industry",
            "research",
            "locations",
            "groups",
            "mips",
        }.issubset(profile)
    )
    checks.append(_check("provider_profile", profile_ok, status, {"npi": representative_npi}))

    status, industry = _request(
        base_url, "GET", "/industry/search?state=OH&limit=3", api_key
    )
    industry_results = industry.get("results", []) if isinstance(industry, dict) else []
    industry_ok = (
        status == 200
        and isinstance(industry_results, list)
        and bool(industry_results)
        and isinstance(industry_results[0], dict)
        and {
            "npi",
            "name",
            "payment_count",
            "total_usd",
            "matched_payment_count",
            "matched_total_usd",
            "n_manufacturers",
            "tier",
        }.issubset(industry_results[0])
    )
    checks.append(
        _check(
            "industry_search",
            industry_ok,
            status,
            {
                "total": industry.get("total") if isinstance(industry, dict) else None,
                "returned": len(industry_results),
            },
        )
    )

    status, options = _request(
        base_url,
        "GET",
        "/industry/options?field=manufacturer&state=OH&limit=1",
        api_key,
    )
    option_values = options.get("options", []) if isinstance(options, dict) else []
    options_ok = (
        status == 200
        and isinstance(option_values, list)
        and bool(option_values)
        and isinstance(option_values[0], dict)
        and {"value", "physician_count", "payment_count", "total_usd"}.issubset(
            option_values[0]
        )
    )
    checks.append(_check("industry_options", options_ok, status, {"returned": len(option_values)}))

    if options_ok:
        first = option_values[0]
        option_value = first.get("value") if isinstance(first, dict) else first
        query = urllib.parse.urlencode(
            {
                "state": "OH",
                "manufacturer": option_value,
                "threshold_scope": "matched",
                "limit": 3,
            }
        )
        exact_status, exact_result = _request(
            base_url, "GET", f"/industry/search?{query}", api_key
        )
        exact_ok = (
            exact_status == 200
            and isinstance(exact_result, dict)
            and isinstance(exact_result.get("results"), list)
            and bool(exact_result.get("results"))
        )
        checks.append(_check("industry_exact_option_round_trip", exact_ok, exact_status))
    else:
        checks.append(_check("industry_exact_option_round_trip", False, None))

    if industry_results:
        detail_npi = str(industry_results[0].get("npi", ""))
        detail_status, detail = _request(
            base_url, "GET", f"/industry/{detail_npi}/detail", api_key
        )
        if expected_industry_detail_status == 200:
            detail_ok = (
                detail_status == 200
                and isinstance(detail, dict)
                and str(detail.get("npi")) == detail_npi
                and {
                    "payment_count",
                    "total_usd",
                    "nonfood_usd",
                    "consulting_speaking_usd",
                    "by_nature",
                    "manufacturers",
                    "products",
                }.issubset(detail)
            )
        else:
            detail_ok = (
                detail_status == 404
                and isinstance(detail, dict)
                and detail.get("detail") == "Not Found"
            )
        checks.append(
            _check(
                "industry_detail",
                detail_ok,
                detail_status,
                {"npi": detail_npi, "expected_status": expected_industry_detail_status},
            )
        )
    else:
        checks.append(_check("industry_detail", False, None))

    status, research = _request(
        base_url,
        "POST",
        "/research/investigators",
        api_key,
        {"npis": [representative_npi], "active_nct_ids": []},
    )
    research_ok = (
        status == 200
        and isinstance(research, dict)
        and isinstance(research.get("investigators"), list)
        and research.get("source") == "CMS Open Payments Research"
    )
    checks.append(_check("research", research_ok, status))

    status, clinical = _request(
        base_url, "GET", "/clinical-trials/version", api_key
    )
    clinical_ok = (
        status == 200
        and isinstance(clinical, dict)
        and clinical.get("studyCount") is not None
        and clinical.get("snapshotDate") is not None
    )
    checks.append(
        _check(
            "clinical_trials",
            clinical_ok,
            status,
            {
                "snapshot_date": clinical.get("snapshotDate")
                if isinstance(clinical, dict)
                else None,
                "study_count": clinical.get("studyCount")
                if isinstance(clinical, dict)
                else None,
            },
        )
    )

    status, explorer = _request(base_url, "GET", "/explorer/catalog", api_key)
    explorer_count = (
        len(explorer)
        if isinstance(explorer, list)
        else len(explorer.get("datasets", []))
        if isinstance(explorer, dict)
        else None
    )
    explorer_ok = status == 200 and explorer_count is not None and explorer_count > 0
    checks.append(
        _check("explorer_catalog", explorer_ok, status, {"dataset_count": explorer_count})
    )

    status, tables = _request(base_url, "GET", "/tables", api_key)
    table_values = tables.get("tables", []) if isinstance(tables, dict) else []
    table_counts = {
        item.get("name"): item.get("approx_rows")
        for item in table_values
        if isinstance(item, dict)
    }
    base_expected_counts = {
        "core_providers": expected_core_providers,
        "raw_hospital_enrollments": expected_raw_hospital_enrollments,
        "hospital_affiliations": expected_hospital_affiliations,
    }
    extra_expected_counts = expected_table_counts or {}
    for table, expected in extra_expected_counts.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise ValueError(f"Invalid expected table name: {table!r}")
        if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
            raise ValueError(f"Invalid expected count for table {table}")
        if table in base_expected_counts and base_expected_counts[table] != expected:
            raise ValueError(f"Conflicting expected count for table {table}")
    all_expected_counts = {**base_expected_counts, **extra_expected_counts}
    required_tables = set(all_expected_counts)
    tables_ok = status == 200 and required_tables.issubset(table_counts)
    checks.append(
        _check(
            "required_tables",
            tables_ok,
            status,
            {
                "present": sorted(required_tables & set(table_counts)),
            },
        )
    )

    count_names = [
        "core_providers",
        "raw_hospital_enrollments",
        "hospital_affiliations",
    ] + sorted(set(extra_expected_counts) - set(base_expected_counts))
    count_sql = ", ".join(
        f'(SELECT COUNT(*) FROM "{name}") AS "{name}"' for name in count_names
    )
    count_sql += (
        ", (SELECT COUNT(DISTINCT npi) FROM hospital_affiliations) "
        "AS affiliated_providers"
    )
    status, count_query = _request(
        base_url,
        "POST",
        "/query",
        api_key,
        {
            "sql": "SELECT " + count_sql
        },
    )
    rows = count_query.get("rows", []) if isinstance(count_query, dict) else []
    expected_values = [all_expected_counts[name] for name in count_names] + [
        expected_affiliated_providers
    ]
    actual_counts = rows[0] if rows and isinstance(rows[0], list) else []
    if len(actual_counts) != len(expected_values):
        actual_counts = [None] * len(expected_values)
    counts_ok = (
        status == 200
        and actual_counts == expected_values
    )
    checks.append(
        _check(
            "warehouse_counts",
            counts_ok,
            status,
            {
                **dict(zip(count_names, actual_counts[:-1])),
                "affiliated_providers": actual_counts[-1],
            },
        )
    )

    failed = [check["name"] for check in checks if check["state"] != "passed"]
    return {
        "schema_version": SMOKE_SCHEMA_VERSION,
        "deployment_id": deployment_id,
        "generated_at": utc_now(),
        "base_url": base_url,
        "state": "failed" if failed else "passed",
        "checks": checks,
        "error_summary": f"Failed checks: {', '.join(failed)}" if failed else None,
    }


def _write_json_atomic(path: Path, value: dict) -> None:
    if not path.is_absolute():
        raise ValueError("Evidence output path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o440)
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _load_expected_table_counts(path: Path | None) -> dict[str, int]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Expected table-count evidence is unreadable: {error}") from error
    candidates = [
        payload.get("smoke_table_counts") if isinstance(payload, dict) else None,
        (payload.get("validation_details") or {}).get("smoke_table_counts")
        if isinstance(payload, dict)
        else None,
        ((payload.get("release") or {}).get("validation_details") or {}).get(
            "smoke_table_counts"
        )
        if isinstance(payload, dict)
        else None,
    ]
    counts = next((value for value in candidates if isinstance(value, dict)), None)
    if not counts:
        raise ValueError("Expected table-count evidence has no smoke_table_counts object")
    validated: dict[str, int] = {}
    for name, count in counts.items():
        if not isinstance(name, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*", name
        ):
            raise ValueError("Expected table-count evidence has an invalid table name")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError(f"Expected table-count evidence has an invalid count for {name}")
        validated[name] = count
    return validated


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run bounded read-only production API checks")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--release-bundle", type=Path)
    parser.add_argument("--process-id", type=int, required=True)
    parser.add_argument("--api-key-env", default="CMS_API_KEY")
    parser.add_argument("--expected-core-providers", type=int, required=True)
    parser.add_argument("--expected-hospital-affiliations", type=int, required=True)
    parser.add_argument("--expected-affiliated-providers", type=int, required=True)
    parser.add_argument("--expected-raw-hospital-enrollments", type=int, required=True)
    parser.add_argument(
        "--expected-table-counts",
        type=Path,
        help="Release JSON containing validation_details.smoke_table_counts",
    )
    parser.add_argument(
        "--expected-industry-detail-status", type=int, choices=(200, 404), default=200
    )
    parser.add_argument("--representative-npi", default="1003005257")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        _validate_loopback_url(args.base_url)
        api_key = os.getenv(args.api_key_env, "")
        if not api_key:
            raise ValueError(f"API key environment variable is empty: {args.api_key_env}")
        evidence = run_smoke(
            base_url=args.base_url,
            deployment_id=args.deployment_id,
            api_key=api_key,
            expected_core_providers=args.expected_core_providers,
            expected_hospital_affiliations=args.expected_hospital_affiliations,
            expected_affiliated_providers=args.expected_affiliated_providers,
            expected_raw_hospital_enrollments=args.expected_raw_hospital_enrollments,
            expected_table_counts=_load_expected_table_counts(
                args.expected_table_counts
            ),
            expected_industry_detail_status=args.expected_industry_detail_status,
            representative_npi=args.representative_npi,
            process_id=args.process_id,
            production_root=args.production_root,
            release_bundle=args.release_bundle,
        )
        _write_json_atomic(args.output, evidence)
    except Exception as error:
        evidence = {
            "schema_version": SMOKE_SCHEMA_VERSION,
            "deployment_id": args.deployment_id,
            "generated_at": utc_now(),
            "base_url": args.base_url,
            "state": "failed",
            "checks": [],
            "error_summary": safe_error(error),
        }
        try:
            _write_json_atomic(args.output, evidence)
        except Exception:
            pass
        print(f"Production smoke error: {safe_error(error)}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(evidence, indent=2, sort_keys=True))
    else:
        print(f"Production smoke: {evidence['state']}")
        print(f"Evidence: {args.output}")
        for check in evidence["checks"]:
            print(f"- {check['name']}: {check['state']}")
    return 0 if evidence["state"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
