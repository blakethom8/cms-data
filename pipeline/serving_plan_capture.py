"""Capture exact route SQL and DuckDB plans against an immutable warehouse."""

from __future__ import annotations

import argparse
import contextvars
import hashlib
import importlib.util
import json
import os
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

from .serving_diagnostics import DiagnosticCase, _sha256_file, load_cases


PLAN_CAPTURE_SCHEMA_VERSION = 1
CASE_HEADER = "X-CMS-Diagnostic-Case"
_ACTIVE_CASE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "cms_diagnostic_case", default=None
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_value(value), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def summarize_plan(plan: dict) -> dict:
    operator_counts: Counter[str] = Counter()
    operator_time = 0.0
    rows_scanned = 0
    max_cardinality = 0
    result_set_bytes = 0
    max_operator: tuple[str | None, float] = (None, 0.0)

    def visit(node: Any) -> None:
        nonlocal operator_time, rows_scanned, max_cardinality, result_set_bytes
        nonlocal max_operator
        if not isinstance(node, dict):
            return
        name = str(node.get("operator_name") or node.get("operator_type") or "QUERY").strip()
        timing = float(node.get("operator_timing") or 0.0)
        if "operator_name" in node or "operator_type" in node:
            operator_counts[name] += 1
            operator_time += timing
            if timing > max_operator[1]:
                max_operator = (name, timing)
        rows_scanned += int(node.get("operator_rows_scanned") or 0)
        max_cardinality = max(max_cardinality, int(node.get("operator_cardinality") or 0))
        result_set_bytes += int(node.get("result_set_size") or 0)
        for child in node.get("children") or []:
            visit(child)

    visit(plan)
    return {
        "latency_ms": round(float(plan.get("latency") or 0.0) * 1000, 3),
        "cpu_time_ms": round(float(plan.get("cpu_time") or 0.0) * 1000, 3),
        "operator_time_ms": round(operator_time * 1000, 3),
        "rows_returned": int(plan.get("rows_returned") or 0),
        "rows_scanned": rows_scanned,
        "max_operator_cardinality": max_cardinality,
        "operator_result_bytes_sum": result_set_bytes,
        "operator_counts": dict(sorted(operator_counts.items())),
        "slowest_operator": max_operator[0],
        "slowest_operator_ms": round(max_operator[1] * 1000, 3),
        "peak_buffer_memory_bytes": (
            int(plan["system_peak_buffer_memory"])
            if "system_peak_buffer_memory" in plan
            else None
        ),
        "peak_temporary_storage_bytes": (
            int(plan["system_peak_temp_dir_size"])
            if "system_peak_temp_dir_size" in plan
            else None
        ),
        "total_bytes_read": (
            int(plan["total_bytes_read"]) if "total_bytes_read" in plan else None
        ),
        "total_bytes_written": (
            int(plan["total_bytes_written"]) if "total_bytes_written" in plan else None
        ),
    }


class ProfilingConnection:
    """DuckDB-compatible execute proxy with a separate read-only planner."""

    def __init__(self, database: Path, records: list[dict]):
        self.database = database
        self.records = records
        self.execution = duckdb.connect(str(database), read_only=True)
        self.planner = duckdb.connect(str(database), read_only=True)

    def execute(self, sql: str, parameters: Any = None):
        case_name = _ACTIVE_CASE.get()
        if case_name is not None:
            params = [] if parameters is None else _json_value(parameters)
            started = time.perf_counter()
            record = {
                "case_name": case_name,
                "sequence": 1 + sum(
                    row["case_name"] == case_name for row in self.records
                ),
                "sql": sql,
                "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                "parameters": params,
                "parameters_sha256": _sha256(params),
            }
            try:
                plan_row = self.planner.execute(
                    f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}", parameters
                ).fetchone()
                plan = json.loads(plan_row[1])
                record.update(
                    {
                        "plan": plan,
                        "plan_sha256": _sha256(plan),
                        "summary": summarize_plan(plan),
                        "plan_error": None,
                    }
                )
            except Exception as error:  # preserve the real route execution
                record.update(
                    {
                        "plan": None,
                        "plan_sha256": None,
                        "summary": None,
                        "plan_error": (" ".join(str(error).split()))[:500],
                    }
                )
            record["capture_elapsed_ms"] = round(
                (time.perf_counter() - started) * 1000, 3
            )
            self.records.append(record)
        if parameters is None:
            return self.execution.execute(sql)
        return self.execution.execute(sql, parameters)

    def close(self) -> None:
        self.planner.close()
        self.execution.close()

    def __getattr__(self, name: str):
        return getattr(self.execution, name)


class _CaseContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        token = _ACTIVE_CASE.set(request.headers.get(CASE_HEADER))
        try:
            return await call_next(request)
        finally:
            _ACTIVE_CASE.reset(token)


def response_variations(payloads: list[Any]) -> list[dict]:
    """Return only changed JSON paths, distinguishing list order from content."""
    if len(payloads) < 2:
        return []
    changes: list[dict] = []

    def visit(values: list[Any], path: str) -> None:
        if all(value == values[0] for value in values[1:]):
            return
        if all(isinstance(value, dict) for value in values):
            keys = sorted(set().union(*(value.keys() for value in values)))
            for key in keys:
                present = [key in value for value in values]
                child_path = f"{path}.{key}" if path else str(key)
                if not all(present):
                    changes.append({"path": child_path, "kind": "field_presence"})
                else:
                    visit([value[key] for value in values], child_path)
            return
        if all(isinstance(value, list) for value in values):
            canonical = [[_sha256(item) for item in value] for value in values]
            multisets = [Counter(items) for items in canonical]
            if all(multiset == multisets[0] for multiset in multisets[1:]):
                changes.append({"path": path or "$", "kind": "list_order"})
                return
            if all(all(isinstance(item, dict) for item in value) for value in values):
                identity = _list_identity_key(values)
                if identity is not None:
                    maps = [
                        {str(item[identity]): item for item in value} for value in values
                    ]
                    for item_id in sorted(maps[0]):
                        visit(
                            [mapping[item_id] for mapping in maps],
                            f"{path or '$'}[{identity}={item_id}]",
                        )
                    return
            changes.append({"path": path or "$", "kind": "list_content"})
            return
        changes.append({"path": path or "$", "kind": "value"})

    visit(payloads, "")
    return changes


def _list_identity_key(values: list[list[dict]]) -> str | None:
    if not values or not values[0]:
        return None
    common = set(values[0][0])
    for rows in values:
        for row in rows:
            common &= set(row)
    preferred = (
        "npi", "site_id", "event_id", "source_id", "key", "id", "name",
        "manufacturer", "manufacturer_name", "product", "label", "code",
    )
    candidates = [key for key in preferred if key in common]
    candidates.extend(sorted(common - set(candidates)))
    for key in candidates:
        identities: list[set[str]] = []
        valid = True
        for rows in values:
            items = [row[key] for row in rows]
            if any(isinstance(item, (dict, list)) or item is None for item in items):
                valid = False
                break
            normalized = {str(item) for item in items}
            if len(normalized) != len(items):
                valid = False
                break
            identities.append(normalized)
        if valid and all(value == identities[0] for value in identities[1:]):
            return key
    return None


def _load_api(api_root: Path, database: Path, api_key: str):
    main_path = api_root / "main.py"
    if not main_path.is_file():
        raise ValueError(f"API root does not contain main.py: {api_root}")
    os.environ["DUCKDB_PATH"] = str(database)
    os.environ["CMS_API_KEY"] = api_key
    sys.path.insert(0, str(api_root))
    spec = importlib.util.spec_from_file_location("cms_s2_capture_main", main_path)
    if spec is None or spec.loader is None:
        raise ValueError("Could not load API module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def capture_cases(
    *, api_root: Path, database: Path, api_key: str, cases: list[DiagnosticCase],
    case_manifest_sha256: str, identity: dict, response_trials: int = 3,
) -> dict:
    if response_trials < 1:
        raise ValueError("response_trials must be positive")
    records: list[dict] = []
    connection = ProfilingConnection(database, records)
    module = _load_api(api_root, database, api_key)
    module._conn = connection
    if hasattr(module, "database_pool"):
        module.database_pool._connection_factory = lambda: ProfilingConnection(
            database, records
        )
    app: FastAPI = module.app
    app.add_middleware(_CaseContextMiddleware)
    case_results: list[dict] = []
    with TestClient(app) as client:
        for case in cases:
            payloads: list[Any] = []
            responses: list[dict] = []
            for trial in range(response_trials):
                headers = {"X-API-Key": api_key}
                if trial == 0:
                    headers[CASE_HEADER] = case.name
                started = time.perf_counter()
                response = client.get(case.path, headers=headers)
                responses.append(
                    {
                        "status_code": response.status_code,
                        "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                        "response_bytes": len(response.content),
                        "response_sha256": hashlib.sha256(response.content).hexdigest(),
                    }
                )
                payloads.append(response.json())
            case_results.append(
                {
                    "name": case.name,
                    "expected_status": case.expected_status,
                    "statuses_passed": all(
                        row["status_code"] == case.expected_status for row in responses
                    ),
                    "response_stable": len(
                        {row["response_sha256"] for row in responses}
                    ) == 1,
                    "response_variations": response_variations(payloads),
                    "responses": responses,
                    "query_count": sum(row["case_name"] == case.name for row in records),
                }
            )
    return {
        "schema_version": PLAN_CAPTURE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "identity": identity,
        "case_manifest_sha256": case_manifest_sha256,
        "duckdb_version": duckdb.__version__,
        "response_trials": response_trials,
        "case_count": len(case_results),
        "query_count": len(records),
        "plan_error_count": sum(row["plan_error"] is not None for row in records),
        "status_failure_count": sum(not row["statuses_passed"] for row in case_results),
        "unstable_response_count": sum(not row["response_stable"] for row in case_results),
        "cases": case_results,
        "queries": records,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-root", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--warehouse-sha256", required=True)
    parser.add_argument("--cases", required=True, type=Path)
    parser.add_argument("--api-key-env", default="CMS_API_KEY")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--warehouse-release-id", required=True)
    parser.add_argument("--code-commit", required=True)
    parser.add_argument("--executor-settings", required=True)
    parser.add_argument("--response-trials", type=int, default=3)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        parser.error(f"API key environment variable is empty: {args.api_key_env}")
    try:
        database = args.database.resolve(strict=True)
        if not database.is_file() or database.is_symlink() or database.stat().st_mode & 0o222:
            raise ValueError("Database must be a non-writable regular file")
        actual_sha256 = _sha256_file(database)
        if actual_sha256 != args.warehouse_sha256:
            raise ValueError("Warehouse SHA-256 does not match")
        executor_settings = json.loads(args.executor_settings)
        if not isinstance(executor_settings, dict) or not executor_settings:
            raise ValueError("Executor settings must be a non-empty JSON object")
        cases, manifest_sha256 = load_cases(args.cases)
        evidence = capture_cases(
            api_root=args.api_root, database=database, api_key=api_key, cases=cases,
            case_manifest_sha256=manifest_sha256,
            identity={
                "deployment_id": args.deployment_id,
                "warehouse_release_id": args.warehouse_release_id,
                "warehouse_sha256": actual_sha256,
                "code_commit": args.code_commit,
                "executor_settings": executor_settings,
            },
            response_trials=args.response_trials,
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        parser.error((" ".join(str(error).split()))[:500])
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return int(
        evidence["plan_error_count"] > 0 or evidence["status_failure_count"] > 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
