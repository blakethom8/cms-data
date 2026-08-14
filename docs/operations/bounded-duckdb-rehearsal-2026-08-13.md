# Bounded DuckDB executor rehearsal — 2026-08-13

## Result

The bounded executor is a viable concurrency improvement, but the code defaults are not the right
production parameters for this host. The initial `4` connections × `4` DuckDB threads with a
2-second acquisition deadline improved high-concurrency throughput but slowed isolated requests and
returned a median 7 overload responses at concurrency 12.

The best measured candidate used:

```text
DUCKDB_POOL_SIZE=2
DUCKDB_THREADS=8
DUCKDB_POOL_ACQUIRE_SECONDS=4
DUCKDB_MEMORY_LIMIT=2GB
```

Across three trials it completed all 900 measured requests. At concurrency 12, median throughput
increased from 2.59 to 4.25 requests/second, p50 latency fell from 4.63 to 2.40 seconds, and p95 fell
from 5.31 to 3.90 seconds. At concurrency 1, throughput fell from 2.59 to 2.46 requests/second and p50
rose from 238 to 335 ms, although p95 improved from 1.44 to 1.24 seconds. This is a tradeoff, not an
unqualified win: high-concurrency behavior is substantially better, while the common isolated path
still has a measurable median cost.

No live service, production configuration, deployment selection, warehouse, or release pointer was
changed. The rehearsal ran on loopback port 18080 and was stopped afterward.

## Identity and comparison limits

- Trial times: `2026-08-14T00:00:58Z`, `2026-08-14T00:02:41Z`, and
  `2026-08-14T00:04:18Z`.
- Candidate code: merge commit `ef457f669bbc986f15cdb9862e17620caa518e78`.
- Sealed code artifact:
  `/srv/cms-data-platform/production-artifacts/code/ef457f669bbc986f15cdb9862e17620caa518e78-bounded-executor-rehearsal`.
- Artifact archive SHA-256: `ca50d61a2fe4d3f6e1829c2d3c8fb0af151d42ebf390e21882bc32864e73b3ad`.
- Serving release: `deployment-20260811T155814Z-6baa26aa69`.
- Warehouse release: `warehouse-20260811T021837Z-f44c147e30`.
- Candidate origin: loopback-only `http://127.0.0.1:18080`.
- Candidate PID for the final trials: `3805210`.
- Concurrency levels: 1, 2, 4, 8, 12; 60 requests per level; 30-second request timeout.
- Result: 900 HTTP 200 responses, zero failures, and zero timeouts.

The baseline workload file was temporary and was not retained. Its recorded raw SHA-256 was
`783dfa5e70fb5a05e46f14a3f581a23d1eccbc460ba5355d804de08aef65146c`. The request names, paths,
weights, counts, and timeout were preserved in the benchmark documentation and were used here, but
the old raw bytes cannot be reconstructed honestly. The now-committed canonical workload has SHA-256
`0956223308be22f6807c33bd230df941c7a2b22e7c6949c0692d9946bc0eb8f0`. Future runs must use that
committed file so both semantic identity and raw SHA remain reproducible.

## Three-trial median comparison

Medians are taken across the three independently measured trial summaries at each concurrency
level; request samples are not pooled across trials.

| Concurrency | Baseline rps | Candidate rps | Change | Baseline p50 (ms) | Candidate p50 (ms) | Change | Baseline p95 (ms) | Candidate p95 (ms) | Change | Failures |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.59 | 2.46 | -5.0% | 237.91 | 334.86 | +40.8% | 1,435.49 | 1,237.23 | -13.8% | 0 |
| 2 | 2.60 | 4.24 | +63.1% | 405.26 | 340.91 | -15.9% | 1,611.08 | 1,348.55 | -16.3% | 0 |
| 4 | 2.59 | 4.28 | +65.3% | 1,787.88 | 919.00 | -48.6% | 2,043.66 | 1,679.58 | -17.8% | 0 |
| 8 | 2.60 | 4.26 | +63.8% | 2,821.60 | 1,679.48 | -40.5% | 4,038.95 | 2,675.51 | -33.8% | 0 |
| 12 | 2.59 | 4.25 | +64.1% | 4,627.60 | 2,395.01 | -48.2% | 5,305.14 | 3,897.38 | -26.5% | 0 |

## Candidate resource and queue measurements

| Concurrency | Pool wait p50 (ms) | Pool wait p95 (ms) | Pool wait p99 (ms) | Pool wait max (ms) | CPU (%) | RSS peak (bytes) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.03 | 0.04 | 0.06 | 0.07 | 454.02 | 1,831,460,864 |
| 2 | 0.03 | 0.05 | 0.06 | 0.06 | 758.82 | 1,895,272,448 |
| 4 | 403.09 | 863.58 | 889.11 | 917.33 | 759.70 | 1,903,353,856 |
| 8 | 1,335.32 | 1,399.64 | 1,487.74 | 1,508.95 | 754.35 | 1,900,261,376 |
| 12 | 2,142.18 | 2,568.36 | 2,610.47 | 2,611.18 | 754.38 | 1,910,837,248 |

CPU is process CPU across all cores. The candidate used roughly 7.5 cores once saturated, compared
with roughly 5.6 cores in the baseline. Candidate RSS peaked near 1.78 GiB versus the baseline's
approximately 2.49 GiB, but the baseline process was long-lived and the candidate was fresh, so RSS
should be treated as a safety observation rather than a durable memory-savings claim.

## Route latency

| Route | Baseline C1 p50 (ms) | Candidate C1 p50 (ms) | Baseline C12 p50 (ms) | Candidate C12 p50 (ms) |
| --- | ---: | ---: | ---: | ---: |
| Provider search | 53.24 | 69.53 | 4,663.20 | 2,324.96 |
| Provider profile | 259.24 | 345.79 | 4,678.28 | 2,354.48 |
| Practice search | 157.74 | 192.74 | 4,677.16 | 2,467.95 |
| Radar | 161.07 | 238.06 | 4,606.85 | 2,683.94 |
| Explorer evidence | 1,431.19 | 1,235.72 | 4,654.63 | 3,884.35 |

The cheap provider-search route remains the clearest improvement under contention: C12 p50 fell by
about half. Its isolated p50 rose by about 31% but remained approximately 70 ms.

## Default candidate and tuning learnings

The code defaults (`4` pooled connections, `4` DuckDB threads, 2-second acquisition) were measured
for three trials before tuning. Their medians were:

| Concurrency | Throughput (rps) | p50 (ms) | p95 (ms) | Pool wait p50 (ms) | Pool wait p95 (ms) | Failures |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.77 | 582.61 | 1,446.78 | 0.03 | 0.05 | 0 |
| 2 | 2.83 | 618.64 | 1,504.64 | 0.03 | 0.05 | 0 |
| 4 | 4.09 | 1,005.86 | 1,892.61 | 0.03 | 0.04 | 0 |
| 8 | 4.04 | 1,604.56 | 3,002.54 | 965.60 | 1,275.72 | 0 |
| 12 | 5.05 | 2,140.62 | 3,329.22 | 1,455.63 | 2,000.00 | 7 |

A pool-size-1 diagnostic at four DuckDB threads produced essentially the same C1 result, ruling out
connection rotation as the isolated-latency cause. Increasing the per-query ceiling to eight threads
improved C1 p50 from about 577 to 329 ms. The important capacity variable is therefore the product of
active queries and per-query DuckDB threads, not pool size alone. Two connections × eight threads
keeps nominal saturation aligned with the 16-vCPU host. Extending acquisition from two to four
seconds removed failures from the representative C12 workload while remaining finite.

## Safety and overload gates

- Production remained active on PID `3240475` with zero restarts throughout the rehearsal.
- The selected deployment, warehouse release, release pointer, and deployment ledger were unchanged;
  the production manager remained healthy with zero blocking transactions.
- Authenticated capability returned 200 and pool timing; an ETag revalidation returned 304 without
  pool timing; an unauthenticated database route returned 401 without pool timing.
- A separate 20-request heavy overload probe returned 8 HTTP 200 and 12 controlled HTTP 503
  responses. Every observed 503 carried `duckdb_pool;dur=4000.00`; automated middleware coverage
  separately verifies `Retry-After: 1` on that response.
- Candidate and production journals had no warning-or-higher entries during the recorded window.
- Available host memory was about 23.0 GiB after the candidate stopped; port 18080 was released.
- The sealed candidate contained no bytecode directories and no group- or world-writable paths.
- The unused pre-fix candidate artifact was removed after the corrected artifact passed the exact
  production-root import check. It is reproducible from Git commit `7f3ab9a` if ever needed.

The full production smoke process-identity check was not claimed: this rehearsal intentionally used
a standalone sealed code artifact with the currently selected immutable warehouse rather than a
prepared deployment bundle. Preparing a candidate bundle, running the full smoke suite against that
exact bundle, and exercising the formal rollback gate remain required before any cutover.

## Evidence

Raw summary evidence is committed beside this document:

- `evidence/bounded-executor-2026-08-13/final-trial-1.json` — SHA-256
  `1e42f83782731453361db00154b0305522bcb9cc7b2073f2e309f9617e8c4253`
- `evidence/bounded-executor-2026-08-13/final-trial-2.json` — SHA-256
  `4c844daa7a42c131eb3c18b5cd34e07a7f6ff9212007ab790fe9dea56c39a673`
- `evidence/bounded-executor-2026-08-13/final-trial-3.json` — SHA-256
  `52161d5cb548e22a58f86c59c5e46ca3eb30e8ed81970077ee803d78a26d6c66`

The three default-candidate trial files are retained in the same directory so the rejected
configuration and its overload behavior remain auditable rather than being discarded.

## Decision

Do not deploy the code defaults. Carry `pool size 2`, `DuckDB threads 8`, and a 4-second acquisition
deadline into a formal immutable deployment candidate. Before cutover, run the complete production
smoke suite, response and ETag gates, resource checks, and rollback rehearsal against that exact
bundle. Treat the C1 median regression as an explicit canary guardrail; if production traffic is
predominantly isolated, test a modest per-query thread increase before accepting it.
