# CMS API concurrency baseline — 2026-08-13

## Result

The current production server is stable under this bounded workload but does not convert additional
request concurrency into throughput. Throughput stayed between 2.59 and 2.65 requests/second while
mixed-workload p50 latency rose from 238 ms at concurrency 1 to 4.59 seconds at concurrency 12.
This is consistent with synchronous request handlers contending for the process-global DuckDB
connection.

No production configuration, code, credentials, service state, or data changed during this run.

## Test identity

- Run time: `2026-08-13T23:10:50Z`
- Serving release: `deployment-20260811T155814Z-6baa26aa69`
- API origin: loopback-only `http://127.0.0.1:8080`
- API process: PID `3240475`
- Workload SHA-256: `783dfa5e70fb5a05e46f14a3f581a23d1eccbc460ba5355d804de08aef65146c`
- Concurrency levels: 1, 2, 4, 8, 12
- Requests per level: 60; total measured requests: 300
- Timeout: 30 seconds per request
- Result: 300 HTTP 200 responses; zero failures and zero timeouts
- Pool wait: unavailable because the baseline server has no explicit query pool

The deterministic six-request mix was provider search (weight 1), provider profile (weight 2),
practice search (weight 1), Radar (weight 1), and explorer provider evidence (weight 1). Each level
therefore ran 10 requests for each route except provider profile, which ran 20.

## Overall measurements

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

## Route latency

| Route | C1 p50 (ms) | C1 p95 (ms) | C12 p50 (ms) | C12 p95 (ms) | C12 max (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Provider search | 52.92 | 62.48 | 4,614.91 | 5,305.14 | 5,305.14 |
| Provider profile | 254.78 | 271.80 | 4,583.65 | 4,781.35 | 5,051.59 |
| Practice search | 155.82 | 163.27 | 4,678.59 | 5,455.47 | 5,455.47 |
| Radar | 161.07 | 167.02 | 4,578.04 | 5,451.92 | 5,451.92 |
| Explorer evidence | 1,427.78 | 1,493.56 | 4,601.73 | 7,043.82 | 7,043.82 |

The cheap provider-search route is the clearest head-of-line blocking signal: its p50 increased by
about 87 times even though total throughput did not improve. Explorer evidence is the dominant
single-request cost at concurrency 1, but all route classes converge toward multi-second latency as
contention increases.

## Safety checks

Before the run, `cms-api.service` was active with zero restarts, host load averages were zero, and
about 23.9 GiB memory was available. After the run:

- `cms-api.service` remained active on the same PID with zero restarts;
- `/health` returned 200 with `7,395,713` core providers;
- the host load average was `2.06, 1.08, 0.42` and declining;
- about 23.5 GiB memory remained available; and
- the CMS service journal contained no warning-or-higher entries during the benchmark window.

## Decision

Proceed with a small bounded executor using independently owned read-only DuckDB connections. Move
synchronous database work off the async event loop, expose real pool-wait measurements, and return a
controlled overload response when acquisition exceeds a short deadline. Start conservatively and
use this exact workload to compare candidates; do not increase concurrency merely because the host
has spare RAM.

Run two additional baseline trials before selecting final production parameters. The current trial
is sufficient to establish the bottleneck, but three trials are required by the benchmark runbook
for a median production baseline.
