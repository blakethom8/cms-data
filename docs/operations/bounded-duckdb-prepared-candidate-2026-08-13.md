# Bounded DuckDB prepared-candidate record — 2026-08-13

## Outcome

The bounded DuckDB executor is prepared and has passed its isolated production-host rehearsal. The
candidate is **not selected**, its temporary service is stopped, and no production cutover was
performed. A cutover remains a separate approval-gated operation.

The prepared deployment is `deployment-20260814T002255Z-11131e3630`, built from code commit
`5d9572f2e177b8f4c853d4c10dcc72a5636de9ce`. The still-selected verified predecessor is
`deployment-20260811T155814Z-6baa26aa69`. Both deployments reference warehouse release
`warehouse-20260811T021837Z-f44c147e30`; this is a code-only serving candidate.

## Bound configuration and immutable artifacts

The candidate process was observed with these effective settings:

- `DUCKDB_MEMORY_LIMIT=2GB`
- `DUCKDB_THREADS=8`
- `DUCKDB_POOL_SIZE=2`
- `DUCKDB_POOL_ACQUIRE_SECONDS=4`
- `DUCKDB_PATH` resolved to the prepared deployment bundle, not the selected release

Artifact identity:

| Item | Identity |
| --- | --- |
| Code fingerprint | `sha256:49e3642ad260beaaa12d17077d9dec53db677a17e1dcfc7abdcd106408b655b9` |
| Runtime fingerprint | `sha256:82370f7e4b25f1a907a92eda5c1097302a6f88936ad59319206b4ade3cc7c347` |
| Warehouse SHA-256 | `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2` |
| Warehouse bytes | `20,569,665,536` |
| Serving config SHA-256 | `a2d87a9072db29ba39aa9ea0dc32cb024c3373247cc9317102b34ea5e3cca15c` |
| Source manifests SHA-256 | `fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244` |

The prepared bundle reused the selected runtime because `api/requirements.txt` is unchanged from
the live code commit. Preparation and both transition directions passed the production manager's
immutable-artifact validation after the rehearsal cleanup described below.

## Functional, authorization, and cache checks

The canonical production smoke suite passed all 15 checks. It covered health, process identity,
authentication, practice capabilities/search, provider profile, industry search/options/exact
round trip/detail, research, clinical trials, explorer catalog, required tables, and exact warehouse
counts. The AACT snapshot was `2026-07-21` with 594,772 studies.

The scoped-key and cache matrix also passed:

- the Command Center scoped key received `200` on its allowed query;
- the Provider Search shared key received `403` on that scoped query;
- a missing key received `401`;
- a valid uncached response included `duckdb_pool;dur=0.05`;
- conditional revalidation returned `304` without acquiring the DuckDB pool;
- invalid authentication returned `401` without acquiring the pool; and
- the candidate ETag was `"deployment-20260814T002255Z-11131e3630:3"`.

## Exact-bundle serving canary

The prepared bundle ran on isolated loopback port `18080` while the production unit continued on its
existing port and PID. The committed canonical workload SHA-256 was
`0956223308be22f6807c33bd230df941c7a2b22e7c6949c0692d9946bc0eb8f0`.

| Concurrency | Requests | Successes | Throughput | p50 | p95 | Peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 60 | 60 | 2.50 req/s | 328 ms | 1,220 ms | 1.99 GB |
| 12 | 60 | 60 | 4.32 req/s | 2,351 ms | 3,751 ms | 2.07 GB |

The C12 result closely reproduces the three-run tuned rehearsal: all requests completed, throughput
increased materially over the single-connection baseline, and memory remained bounded near the 2 GB
DuckDB limit plus process overhead. This does not mean individual requests became single-digit
milliseconds; the improvement is concurrency throughput and lower loaded latency relative to the
old single-connection behavior.

At C20, a 40-request mixed overload run completed 39 requests and rejected one with `503` after the
finite four-second acquisition wait. A focused 20-request explorer burst produced 7 `200` responses
and 13 `503` responses; all 13 rejections included `Retry-After: 1`. This proves explicit backpressure
rather than unbounded queuing.

## Capacity and lifecycle gates

The read-only retention preview reported:

- 72.5% current disk use with 84,916,256,768 bytes free;
- 78.18% conservatively projected use when charging the full warehouse size again;
- promotion capacity gate `allowed=true`;
- active plus two validated predecessors protected; and
- 95,406,465,024 allocated bytes identified only as review candidates, with zero bytes declared
  automatically reclaimable.

The host is therefore above the warning threshold but below the promotion block. No cleanup was
performed. Because the candidate reuses the existing warehouse, the projection is deliberately more
conservative than the actual code-only storage requirement.

Both commands below passed with `--dry-run`; neither changed the ledger or selector:

- activation of `deployment-20260814T002255Z-11131e3630`;
- rollback from that candidate to `deployment-20260811T155814Z-6baa26aa69`.

At the end of the rehearsal, production remained active with PID `3240475`, `NRestarts=0`, a healthy
control plane, no transition sentinel, no warning-level journal events during the rehearsal, and the
verified predecessor still selected. The candidate unit was inactive and port `18080` was free.

## Rehearsal findings incorporated into procedure

Three operational traps were found before cutover:

1. A normal `git status` after sealing may refresh `.git/index` and make the checkout fail the
   immutable-artifact permission check. Use `GIT_OPTIONAL_LOCKS=0 git status --porcelain=v1` for a
   post-seal cleanliness check, or complete Git inspection before sealing.
2. A transient systemd `--setenv DUCKDB_PATH=...` did not override the unit's later
   `EnvironmentFile` assignment. An isolated candidate must bind its bundle path at `ExecStart`
   after environment files load, then prove the effective value from `/proc/PID/environ` and
   `GET /release`.
3. Running Python tooling from an immutable code checkout without
   `PYTHONDONTWRITEBYTECODE=1` can create writable `__pycache__` directories. This occurred once in
   the candidate artifact and once during a probe of the selected artifact. Only the generated cache
   directories were removed; both checkouts were proven clean and sealed afterward. All future
   smoke, benchmark, and diagnostic invocations against a release checkout must export
   `PYTHONDONTWRITEBYTECODE=1` before Python starts.

These failures were fail-closed: the manager refused the transition dry-run until artifact integrity
was restored. The live service was not restarted, and `release-current` never moved.

## Sealed evidence

Evidence remains on the production host under
`/srv/cms-data-platform/production/evidence/deployment-20260814T002255Z-11131e3630/`, owned by
`root:dataops` and mode `0440`:

| File | SHA-256 |
| --- | --- |
| `smoke.json` | `29503299dac1bb886c08a41b291eeef5bb22f55881b1ca74b04f176d4f10d5a0` |
| `serving-canary.json` | `71544999dcd047ae38189c5566ef45d1947e22aebd16294f794fd8cf66c74386` |
| `serving-overload.json` | `d33f747f512ce04511644091a292142312287c6340ff902bb0de867e93494356` |
| `serving-config.env` | `a2d87a9072db29ba39aa9ea0dc32cb024c3373247cc9317102b34ea5e3cca15c` |
| `source-manifests.json` | `fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244` |

## Remaining approval boundary

No additional preparation gate is known to be open for this code-only candidate. The remaining step
is the controlled cutover in Phase 3 of the production promotion runbook: reconfirm state, install
the reviewed unit/environment configuration, select and restart once through the cutover command,
run complete smoke, and automatically restore the verified predecessor on failure. Do not begin that
phase without separate explicit approval.
