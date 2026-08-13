# API concurrency benchmark

Use this read-only harness to establish and compare CMS serving performance at concurrency
1/2/4/8/12. It records latency, throughput, response bytes, status/failure behavior, and—when a
local Linux server PID is supplied—server CPU and RSS. The current baseline has no explicit query
pool, so `pool_wait_ms` is deliberately `null` rather than an invented zero.

## Workload

Create a JSON file containing representative, non-secret GET paths for the exact release under
test. Keep provider identifiers and geographic filters stable between runs.

```json
{
  "requests": [
    {"name": "provider_search", "path": "/profiles/search?q=Smith&state=OH&limit=10"},
    {"name": "provider_profile", "path": "/profiles/1003005257", "weight": 2},
    {"name": "practice_search", "path": "/practices/search?specialty=Cardiology&state=OH&limit=25"},
    {"name": "radar", "path": "/radar/providers?city=Cleveland&state=OH&limit=100"},
    {"name": "explorer", "path": "/explorer/provider-evidence?npis=1003005257"}
  ]
}
```

Weights are integers from 1 through 100. The runner cycles the weighted request list
deterministically. Absolute URLs, protocol-relative paths, fragments, request bodies, and methods
other than GET are not accepted.

## Run

Start the API against an immutable read-only release, warm it, and keep other host work quiet. Put
the scoped key in the environment; never pass it on the command line or include it in the workload.

```bash
export CMS_API_KEY='<scoped-key>'
.venv/bin/python -m pipeline.serving_benchmark \
  --base-url http://127.0.0.1:8080 \
  --workload /absolute/path/workload.json \
  --server-process-id <uvicorn-pid> \
  --requests-per-level 60 \
  --output /absolute/path/evidence.json
```

The runner performs one warmup request per workload entry, then measures each configured level. A
non-200 response or transport error is recorded and makes the command exit nonzero. The output
contains the workload SHA-256 so later runs can prove they used the same request mix. The API key
is never written to evidence.

Run at least three trials per candidate and retain the median trial alongside the deployment or
rehearsal record. Compare p50/p95/p99, throughput, CPU, RSS peak, status counts, and response bytes.
Once the bounded connection pool exists, add its measured queue wait to this schema rather than
deriving it from total request latency.
