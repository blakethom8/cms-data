import json
import sys
from collections import Counter
from email.message import Message
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from pipeline import serving_benchmark as benchmark


def test_load_workload_is_bounded_and_reproducible(tmp_path: Path) -> None:
    path = tmp_path / "workload.json"
    path.write_text(
        json.dumps(
            {
                "requests": [
                    {"name": "profile", "path": "/profiles/1003005257", "weight": 2},
                    {"name": "radar", "path": "/radar/providers?state=OH", "weight": 1},
                ]
            }
        )
    )

    requests, digest = benchmark.load_workload(path)

    assert requests == [
        benchmark.WorkloadRequest("profile", "/profiles/1003005257", 2),
        benchmark.WorkloadRequest("radar", "/radar/providers?state=OH", 1),
    ]
    assert len(digest) == 64


@pytest.mark.parametrize(
    "entry",
    [
        {"name": "bad", "path": "https://example.test/profiles/1"},
        {"name": "bad", "path": "//example.test/profiles/1"},
        {"name": "bad", "path": "/profiles/1#fragment"},
        {"name": "bad", "path": "/profiles/1", "weight": 0},
    ],
)
def test_load_workload_rejects_unsafe_or_unbounded_entries(
    tmp_path: Path, entry: dict
) -> None:
    path = tmp_path / "workload.json"
    path.write_text(json.dumps({"requests": [entry]}))

    with pytest.raises(ValueError):
        benchmark.load_workload(path)


def test_summary_records_latency_throughput_bytes_and_failure_behavior() -> None:
    samples = [
        benchmark.RequestSample("profile", 200, 10.0, 100, None),
        benchmark.RequestSample("profile", 200, 20.0, 120, None),
        benchmark.RequestSample("radar", 503, 30.0, 40, "HTTP 503"),
    ]

    summary = benchmark.summarize_samples(samples, elapsed_seconds=0.5)

    assert summary["throughput_rps"] == 6.0
    assert summary["response_bytes"] == 260
    assert summary["failures"] == 1
    assert summary["status_counts"] == {"200": 2, "503": 1}
    assert summary["latency_ms"] == {"p50": 20.0, "p95": 30.0, "p99": 30.0, "max": 30.0}
    assert summary["pool_wait_ms"] == {
        "samples": 0,
        "p50": None,
        "p95": None,
        "p99": None,
        "max": None,
    }


def test_pool_wait_parser_reads_only_the_named_server_timing_metric() -> None:
    headers = Message()
    headers["Server-Timing"] = "cache;dur=2.1, duckdb_pool;dur=18.42"

    assert benchmark._pool_wait_ms(headers) == 18.42

    headers.replace_header("Server-Timing", "cache;dur=2.1")
    assert benchmark._pool_wait_ms(headers) is None


def test_base_url_never_accepts_embedded_credentials() -> None:
    with pytest.raises(ValueError):
        benchmark._validate_base_url("https://secret@example.test")

    assert benchmark._validate_base_url("http://127.0.0.1:8080/") == "http://127.0.0.1:8080"


def test_benchmark_is_deterministic_and_never_serializes_the_key(monkeypatch) -> None:
    calls: list[str] = []

    def request_once(base_url, request, api_key, timeout_seconds):
        assert base_url == "http://127.0.0.1:8080"
        assert api_key == "operator-secret"
        assert timeout_seconds == 5
        calls.append(request.name)
        return benchmark.RequestSample(request.name, 200, 10.0, 50, None)

    monkeypatch.setattr(benchmark, "_request_once", request_once)
    workload = [
        benchmark.WorkloadRequest("profile", "/profiles/1003005257", 2),
        benchmark.WorkloadRequest("radar", "/radar/providers?zip5=44101", 1),
    ]

    evidence = benchmark.run_benchmark(
        base_url="http://127.0.0.1:8080",
        api_key="operator-secret",
        workload=workload,
        workload_sha256="abc123",
        concurrency_levels=[1, 2],
        requests_per_level=3,
        timeout_seconds=5,
        server_process_id=None,
    )

    assert calls[:2] == ["profile", "radar"]
    assert Counter(calls) == {"profile": 5, "radar": 3}
    assert [level["concurrency"] for level in evidence["levels"]] == [1, 2]
    assert all(
        level["pool_wait_ms"]["samples"] == 0 for level in evidence["levels"]
    )
    assert "operator-secret" not in json.dumps(evidence)
