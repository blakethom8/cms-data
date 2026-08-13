"""Repeatable read-only benchmark for the CMS serving API."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


BENCHMARK_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WorkloadRequest:
    name: str
    path: str
    weight: int


@dataclass(frozen=True)
class RequestSample:
    name: str
    status_code: int | None
    elapsed_ms: float
    response_bytes: int
    error: str | None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_error(error: BaseException) -> str:
    return (" ".join(str(error).split()) or error.__class__.__name__)[:300]


def _validate_base_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
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
    return base_url.rstrip("/")


def load_workload(path: Path) -> tuple[list[WorkloadRequest], str]:
    raw = path.read_bytes()
    payload = json.loads(raw)
    entries = payload.get("requests") if isinstance(payload, dict) else None
    if not isinstance(entries, list) or not entries:
        raise ValueError("Workload must contain a non-empty requests list")

    requests: list[WorkloadRequest] = []
    names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Every workload request must be an object")
        name = entry.get("name")
        request_path = entry.get("path")
        weight = entry.get("weight", 1)
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("Workload request names must be non-empty and unique")
        if (
            not isinstance(request_path, str)
            or not request_path.startswith("/")
            or request_path.startswith("//")
            or "#" in request_path
        ):
            raise ValueError("Workload paths must be absolute-origin paths")
        if not isinstance(weight, int) or isinstance(weight, bool) or not 1 <= weight <= 100:
            raise ValueError("Workload weights must be integers from 1 through 100")
        names.add(name)
        requests.append(WorkloadRequest(name=name, path=request_path, weight=weight))
    return requests, hashlib.sha256(raw).hexdigest()


def _request_once(
    base_url: str,
    request: WorkloadRequest,
    api_key: str,
    timeout_seconds: float,
) -> RequestSample:
    started = time.perf_counter()
    status_code: int | None = None
    response_bytes = 0
    error_text: str | None = None
    http_request = urllib.request.Request(
        f"{base_url}{request.path}",
        headers={"Accept": "application/json", "X-API-Key": api_key},
        method="GET",
    )
    try:
        with urllib.request.urlopen(http_request, timeout=timeout_seconds) as response:
            status_code = response.status
            response_bytes = len(response.read())
    except urllib.error.HTTPError as error:
        status_code = error.code
        response_bytes = len(error.read())
        error_text = f"HTTP {error.code}"
    except (OSError, TimeoutError) as error:
        error_text = _safe_error(error)
    return RequestSample(
        name=request.name,
        status_code=status_code,
        elapsed_ms=(time.perf_counter() - started) * 1000,
        response_bytes=response_bytes,
        error=error_text,
    )


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * percentile + 0.5)))
    return round(ordered[index], 2)


def summarize_samples(samples: list[RequestSample], elapsed_seconds: float) -> dict:
    latencies = [sample.elapsed_ms for sample in samples]
    status_counts: dict[str, int] = {}
    error_count = 0
    for sample in samples:
        key = str(sample.status_code) if sample.status_code is not None else "transport_error"
        status_counts[key] = status_counts.get(key, 0) + 1
        error_count += int(sample.error is not None or sample.status_code != 200)
    return {
        "requests": len(samples),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "throughput_rps": round(len(samples) / elapsed_seconds, 2),
        "response_bytes": sum(sample.response_bytes for sample in samples),
        "failures": error_count,
        "status_counts": status_counts,
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
            "max": round(max(latencies), 2) if latencies else None,
        },
    }


def _process_snapshot(process_id: int | None) -> dict | None:
    if not process_id:
        return None
    proc = Path(f"/proc/{process_id}")
    try:
        fields = (proc / "stat").read_text().split()
        status = (proc / "status").read_text().splitlines()
        ticks = os.sysconf("SC_CLK_TCK")
        rss_kib = next(
            int(line.split()[1]) for line in status if line.startswith("VmRSS:")
        )
        return {
            "cpu_seconds": (int(fields[13]) + int(fields[14])) / ticks,
            "rss_bytes": rss_kib * 1024,
        }
    except (FileNotFoundError, OSError, StopIteration, ValueError):
        return None


class _ProcessSampler:
    def __init__(self, process_id: int | None):
        self.process_id = process_id
        self.samples: list[dict] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        while not self._stop.is_set():
            snapshot = _process_snapshot(self.process_id)
            if snapshot is not None:
                self.samples.append(snapshot)
            self._stop.wait(0.05)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_args):
        self._stop.set()
        self._thread.join()
        snapshot = _process_snapshot(self.process_id)
        if snapshot is not None:
            self.samples.append(snapshot)

    def summary(self, elapsed_seconds: float) -> dict | None:
        if not self.samples:
            return None
        cpu_delta = self.samples[-1]["cpu_seconds"] - self.samples[0]["cpu_seconds"]
        return {
            "process_id": self.process_id,
            "cpu_percent": round(100 * cpu_delta / elapsed_seconds, 2),
            "rss_start_bytes": self.samples[0]["rss_bytes"],
            "rss_peak_bytes": max(sample["rss_bytes"] for sample in self.samples),
            "rss_end_bytes": self.samples[-1]["rss_bytes"],
        }


def run_level(
    *,
    base_url: str,
    api_key: str,
    workload: list[WorkloadRequest],
    concurrency: int,
    request_count: int,
    timeout_seconds: float,
    server_process_id: int | None,
) -> dict:
    weighted = [request for request in workload for _ in range(request.weight)]
    scheduled = [weighted[index % len(weighted)] for index in range(request_count)]
    sampler = _ProcessSampler(server_process_id)
    started = time.perf_counter()
    with sampler, concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        samples = list(
            executor.map(
                lambda request: _request_once(base_url, request, api_key, timeout_seconds),
                scheduled,
            )
        )
    elapsed_seconds = time.perf_counter() - started
    result = summarize_samples(samples, elapsed_seconds)
    result.update(
        {
            "concurrency": concurrency,
            "server_process": sampler.summary(elapsed_seconds),
            # The baseline server has no explicit pool yet. Do not report
            # request latency as pool wait or invent a zero measurement.
            "pool_wait_ms": None,
            "by_request": {
                request.name: summarize_samples(
                    [sample for sample in samples if sample.name == request.name],
                    elapsed_seconds,
                )
                for request in workload
            },
        }
    )
    return result


def run_benchmark(
    *,
    base_url: str,
    api_key: str,
    workload: list[WorkloadRequest],
    workload_sha256: str,
    concurrency_levels: list[int],
    requests_per_level: int,
    timeout_seconds: float,
    server_process_id: int | None,
) -> dict:
    base_url = _validate_base_url(base_url)
    for request in workload:
        warmup = _request_once(base_url, request, api_key, timeout_seconds)
        if warmup.status_code != 200:
            raise RuntimeError(f"Warmup failed for {request.name}: {warmup.error}")
    levels = [
        run_level(
            base_url=base_url,
            api_key=api_key,
            workload=workload,
            concurrency=concurrency,
            request_count=requests_per_level,
            timeout_seconds=timeout_seconds,
            server_process_id=server_process_id,
        )
        for concurrency in concurrency_levels
    ]
    return {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "base_origin": base_url,
        "workload_sha256": workload_sha256,
        "requests_per_level": requests_per_level,
        "timeout_seconds": timeout_seconds,
        "concurrency_levels": concurrency_levels,
        "levels": levels,
    }


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--api-key-env", default="CMS_API_KEY")
    parser.add_argument("--concurrency", default="1,2,4,8,12")
    parser.add_argument("--requests-per-level", type=_positive_int, default=60)
    parser.add_argument("--timeout-seconds", type=_positive_float, default=30.0)
    parser.add_argument("--server-process-id", type=_positive_int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    api_key = os.getenv(args.api_key_env)
    if not api_key:
        parser.error(f"API key environment variable is empty: {args.api_key_env}")
    try:
        concurrency_levels = [_positive_int(value) for value in args.concurrency.split(",")]
        workload, workload_sha256 = load_workload(args.workload)
        evidence = run_benchmark(
            base_url=args.base_url,
            api_key=api_key,
            workload=workload,
            workload_sha256=workload_sha256,
            concurrency_levels=concurrency_levels,
            requests_per_level=args.requests_per_level,
            timeout_seconds=args.timeout_seconds,
            server_process_id=args.server_process_id,
        )
    except (OSError, ValueError, RuntimeError) as error:
        parser.error(_safe_error(error))
    rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    return int(any(level["failures"] for level in evidence["levels"]))


if __name__ == "__main__":
    raise SystemExit(main())
