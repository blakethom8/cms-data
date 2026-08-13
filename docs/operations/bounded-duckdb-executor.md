# Bounded DuckDB request executor

Status: implementation candidate. This design is not a production rollout record.

## Why this exists

The 2026-08-13 production baseline showed a stable release completing every request while throughput
stayed near 2.6 requests per second and latency increased sharply with concurrency. Concurrency-1
and concurrency-12 timings came from the same release, so the difference is evidence of queueing and
event-loop blocking under simultaneous DuckDB work—not evidence that a deployment made an isolated
request slower. Preserve that distinction in every future report.

The first executor increment makes the queue explicit and bounded. Its goals are to keep synchronous
DuckDB work off the async event loop, give each active request exclusive ownership of an independent
read-only connection, protect the server with a finite queue wait, and expose wait time separately
from query and response time.

## Candidate behavior

- `DUCKDB_POOL_SIZE` controls the number of independent read-only connections (default `4`).
- `DUCKDB_POOL_ACQUIRE_SECONDS` bounds how long a request waits for a connection (default `2`).
- A request that cannot acquire capacity receives `503`, `Retry-After: 1`, and a `Server-Timing`
  `duckdb_pool` duration. It is not silently dropped.
- Successful pooled requests receive the same `Server-Timing` metric and access logs include
  `pool_wait_ms` and `pool_result`.
- Invalid or missing API keys do not consume a pool slot. Conditional cache hits are resolved before
  pool acquisition. Static metadata routes are excluded from the pool.
- Each leased connection has one request owner at a time. The serving database remains read-only;
  refresh, promotion, and rollback remain pipeline operations.

The initial scope is the synchronous, database-backed routes under `/profiles`, `/practices`,
`/radar`, and `/explorer`. Other route families continue to use the compatibility connection until
they are migrated and benchmarked. This is intentionally an incremental boundary, not a claim that
all API database access is pooled.

## What to measure

Use the procedure in [API concurrency benchmark](api-concurrency-benchmark.md). Run at least three
trials for both baseline and candidate, then compare median trials with the same immutable database,
release metadata, workload hash, weights, request counts, timeouts, and concurrency levels.

Record:

- total p50/p95/p99 latency and throughput;
- pool-wait p50/p95/p99/max independently from total latency;
- per-route latency, status counts, timeouts, and `503` overload responses;
- CPU, RSS peak, response bytes, and release identity.

A useful result is not necessarily “no queue.” A bounded queue that preserves event-loop
responsiveness and fails predictably can be safer than accepting unlimited work. Tune the pool from
measured CPU, memory, throughput, and tail latency rather than increasing it until overload moves to
the host.

## Rollout and rollback gates

Before production rollout:

1. Run the complete API test suite and response-shape compatibility checks.
2. Rehearse the candidate against an immutable copy of the production release; do not overwrite the
   active DuckDB file.
3. Verify auth failures and cache hits do not lease connections, and verify controlled overload.
4. Compare three candidate trials with the three-trial baseline under the fixed comparison contract.
5. Confirm host CPU and RSS headroom, then canary with release-aware logs and dashboards.

Rollback is the prior application release and its prior executor settings. Database rollback remains
an atomic pointer or path change to a previously validated immutable release. Do not modify or replace
the active database in place.

## Learnings log

- 2026-08-13: Baseline tests demonstrated same-release concurrency queueing: throughput remained
  roughly flat while total latency rose. The reported concurrency values must not be described as a
  before/after latency regression.
- 2026-08-13: Pool wait needs its own signal; otherwise a latency change cannot be separated into
  capacity wait versus query execution and response work.
- 2026-08-13: Capacity protection must sit behind auth and behind conditional-cache short circuits so
  rejected or already-satisfied requests cannot deplete scarce query slots.
- 2026-08-13: The first migration covers the benchmarked route families only. Remaining database
  routes require explicit conversion and measurement rather than an unverified bulk switch.
- 2026-08-13: Candidate preflight must exercise the production launch shape from the release root,
  not only the repository's `api/` test working directory. The first rehearsal preflight caught and
  corrected an import-order mismatch before a candidate process was started.
