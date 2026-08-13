# CMS API concurrency baseline — 2026-08-13

## Result

The current production server is stable under this bounded workload but does not convert additional
request concurrency into throughput. Across three trials, median throughput stayed between 2.59
and 2.60 requests/second while mixed-workload median p50 latency rose from 238 ms at concurrency 1
to 4.63 seconds at concurrency 12. This is consistent with synchronous request handlers contending
for the process-global DuckDB connection.

No production configuration, code, credentials, service state, or data changed during this run.

## Test identity

- Trial times: `2026-08-13T23:10:50Z`, `2026-08-13T23:17:30Z`, and
  `2026-08-13T23:20:12Z`
- Serving release: `deployment-20260811T155814Z-6baa26aa69`
- API origin: loopback-only `http://127.0.0.1:8080`
- API process: PID `3240475`
- Workload SHA-256: `783dfa5e70fb5a05e46f14a3f581a23d1eccbc460ba5355d804de08aef65146c`
- Concurrency levels: 1, 2, 4, 8, 12
- Requests per level: 60; total measured requests: 900 across three trials
- Timeout: 30 seconds per request
- Result: 900 HTTP 200 responses; zero failures and zero timeouts
- Pool wait: unavailable because the baseline server has no explicit query pool

The deterministic six-request mix was provider search (weight 1), provider profile (weight 2),
practice search (weight 1), Radar (weight 1), and explorer provider evidence (weight 1). Each level
therefore ran 10 requests for each route except provider profile, which ran 20.

## Three-trial medians

The table below takes the median of the three independently measured values at each concurrency
level. It does not pool individual request samples across trials.

| Concurrency | Elapsed (s) | Throughput (rps) | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | CPU (%) | RSS peak (bytes) | Failures |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 23.121 | 2.59 | 237.91 | 1,435.49 | 1,467.16 | 1,493.56 | 567.44 | 2,608,881,664 | 0 |
| 2 | 23.091 | 2.60 | 405.26 | 1,611.08 | 1,633.82 | 1,633.84 | 567.11 | 2,655,379,456 | 0 |
| 4 | 23.193 | 2.59 | 1,787.88 | 2,043.66 | 2,137.49 | 2,137.52 | 566.80 | 2,671,292,416 | 0 |
| 8 | 23.079 | 2.60 | 2,821.60 | 4,038.95 | 4,078.22 | 4,163.21 | 562.53 | 2,673,889,280 | 0 |
| 12 | 23.151 | 2.59 | 4,627.60 | 5,305.14 | 6,049.52 | 7,043.82 | 560.95 | 2,678,689,792 | 0 |

## Trial 1 measurements

| Concurrency | Elapsed (s) | Throughput (rps) | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | CPU (%) | RSS start (bytes) | RSS peak (bytes) | RSS end (bytes) | Failures |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 23.121 | 2.59 | 237.91 | 1,435.49 | 1,467.16 | 1,493.56 | 561.82 | 2,479,431,680 | 2,590,081,024 | 2,571,378,688 | 0 |
| 2 | 22.676 | 2.65 | 400.40 | 1,588.68 | 1,595.38 | 1,595.42 | 561.95 | 2,571,378,688 | 2,617,237,504 | 2,600,931,328 | 0 |
| 4 | 23.193 | 2.59 | 1,787.88 | 2,087.19 | 2,193.41 | 2,193.80 | 567.64 | 2,600,931,328 | 2,624,663,552 | 2,605,383,680 | 0 |
| 8 | 23.073 | 2.60 | 2,821.60 | 3,998.51 | 4,057.31 | 4,070.99 | 562.53 | 2,605,383,680 | 2,644,733,952 | 2,593,587,200 | 0 |
| 12 | 23.151 | 2.59 | 4,592.07 | 5,305.14 | 5,455.47 | 7,043.82 | 559.49 | 2,593,587,200 | 2,631,180,288 | 2,597,949,440 | 0 |

CPU percentage is process CPU across all cores, so values near 560% mean roughly 5.6 cores were
busy. RSS peaked at about 2.46 GiB. Host-level available memory was about 23.9 GiB before the run
and 23.5 GiB immediately afterward.

## Median route latency

| Route | C1 p50 (ms) | C1 p95 (ms) | C12 p50 (ms) | C12 p95 (ms) | C12 max (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Provider search | 53.24 | 62.48 | 4,663.20 | 6,049.52 | 6,049.52 |
| Provider profile | 259.24 | 271.80 | 4,678.28 | 4,890.36 | 5,098.55 |
| Practice search | 157.74 | 168.57 | 4,677.16 | 5,455.47 | 5,455.47 |
| Radar | 161.07 | 167.02 | 4,606.85 | 5,451.92 | 5,451.92 |
| Explorer evidence | 1,431.19 | 1,493.56 | 4,654.63 | 7,043.82 | 7,043.82 |

The cheap provider-search route is the clearest head-of-line blocking signal: its median p50
increased by about 88 times even though total throughput did not improve. Explorer evidence is the
dominant single-request cost at concurrency 1, but all route classes converge toward multi-second
latency as contention increases.

## Safety checks

Before every trial, `cms-api.service` was active on the same PID with zero restarts and at least
23.6 GB memory available. After every trial:

- `cms-api.service` remained active on the same PID with zero restarts;
- `/health` returned 200 with `7,395,713` core providers;
- the one-minute host load average remained below 5 on the 16-vCPU host and declined after load;
- at least 23.3 GB memory remained available; and
- the CMS service journal contained no warning-or-higher entries during each benchmark window.

## Decision

Proceed with a small bounded executor using independently owned read-only DuckDB connections. Move
synchronous database work off the async event loop, expose real pool-wait measurements, and return a
controlled overload response when acquisition exceeds a short deadline. Start conservatively and
use this exact workload to compare candidates; do not increase concurrency merely because the host
has spare RAM.

The required three-trial median production baseline is complete. Use these medians as the comparison
point for the bounded-executor candidate; retain the same release data, workload hash, weights,
request counts, timeout, and concurrency levels.
