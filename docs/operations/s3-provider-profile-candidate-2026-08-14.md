# S3 provider-profile staging-candidate evaluation — 2026-08-14

> **Last reviewed: 2026-08-14** · **Status: evaluated; no cutover**
>
> **Decision: do not prepare, authorize, or cut over this candidate.** The three core/access marts
> are valid and materially faster, but the complete provider-profile route misses both performance
> acceptance gates. A later determinism fix also supersedes this candidate's header contents.
> Production remained unchanged throughout the evaluation.

## Identity and boundary

The isolated candidate is warehouse `warehouse-20260814T222518Z-62c1707278`, built from selected
baseline `warehouse-20260814T183948Z-e5ff46dce9`. It is 24,472,203,264 bytes with SHA-256
`70315198133da2f083f820ba761f1640f32bcfe1f4b1c48df2afbed5b78829a7`, passed release validation,
and remains `not_promoted`. The build used pipeline commit
`70ccca16cf766e5594d00a149bc19c8cb73397af` with one DuckDB thread, a 12 GiB memory ceiling, and
the explicitly adopted reassignment run `20260721T220859Z-0353abdb`.

Production stayed on deployment `deployment-20260814T201311Z-0325c353c9` and warehouse
`warehouse-20260814T183948Z-e5ff46dce9`. The final manager check reported a healthy control plane,
passed artifact integrity, the same selected pointer and ledger, API PID `4041825`, and zero
restarts. No production artifact, deployment bundle, authorization, route switch, or Provider
Search RPC change was created.

The sealed server-side audit is
`/srv/cms-data-platform/audits/s3-provider-profile-20260814T220852Z/evidence`. It contains 43
root-owned, read-only files and a verified `SHA256SUMS` index. Evidence includes source adoption,
both build attempts, candidate comparison, parity runs, raw/mart plans, six HTTP benchmark trials,
capacity, final integrity, and the decision summary.

## Build and contract result

The first build failed closed because two legitimate NPPES international/territory addresses have
a street but no ZIP. Their visible null ZIP is part of the raw response contract, but the original
hidden mart address key also became null. PR
[#69](https://github.com/blakethom8/cms-data/pull/69) added source-qualified internal missing-ZIP
keys while preserving every visible field and raw null-join semantics. The incomplete 21.7 GB
partial file was proved unopen and removed by exact path after its failed manifest was retained.

The corrected build produced:

| Mart | Rows | Contract result |
| --- | ---: | --- |
| `serving_provider_profile_headers` | 7,404,664 | passed |
| `serving_provider_profile_locations` | 10,078,566 | passed |
| `serving_provider_profile_groups` | 3,444,000 | passed |

All three marts had zero duplicate keys, invalid NPIs, required nulls, or missing provenance. The
candidate comparison found exactly the three allowlisted tables changed and no differences across
44 invariant logical fingerprints. The release records the reassignment source period, run, row
count, and artifact checksum under `reconciled_source_runs`.

## Production-data parity findings

The first 22-provider component comparison found exact header and location parity. Four providers
with 119–141 affiliations had identical group rows in a different order because tied groups lacked
a final sort key. PR [#70](https://github.com/blakethom8/cms-data/pull/70) added `group_id` as the
stable raw and mart tie-breaker. The repeated component run then passed all 22 cases byte for byte,
including both null-ZIP providers and the high-location/high-group cases.

The group population reconciles exactly:

- raw DAC/reassignment union: 3,444,407 NPI/group rows;
- reachable through a provider-profile header: 3,444,000;
- mart rows: 3,444,000; and
- excluded rows: 407 reassignment-only NPIs with no header, zero DAC-backed exclusions, and zero
  invalid NPIs.

Paired complete-route plan capture then exposed a separate raw-oracle defect. Rich provider
`1811967433` has two DAC primary-specialty rows, and `any_value` alternated between cardiology and
internal medicine across read-only connections. Independent aggregates could also combine fields
that never appeared on one source row. PR
[#71](https://github.com/blakethom8/cms-data/pull/71) now selects one coherent DAC row with a stable
specialty-first order while retaining telehealth across all rows and complete source provenance.

That fix is merged as `95399c712b3122174e76973f83a210ae35da7296`, after this candidate was built.
The candidate therefore must not be paired with current code for cutover evidence: its materialized
header froze the earlier arbitrary value. The next candidate must rebuild the header with the
deterministic transform.

The wider canonical diagnostic also found a pre-existing, backend-independent industry-search
nondeterminism: NPI `1407378078` can return either of two city values from an un-tied aggregate.
That route is outside these profile marts and is tracked as a follow-up rather than hidden inside
this evaluation.

## HTTP performance

Raw and mart APIs ran as separate loopback-only processes against the same immutable candidate and
merged query code `b3dc9651d7926955987c9bc80adbb5168fcfb1e4`. Both used four read-only DuckDB
connections, four threads per connection, a 2 GiB per-connection memory ceiling, and a two-second
pool-acquisition deadline. The committed mixed workload ran 60 requests at each concurrency level
for three trials per backend. The table reports the median of the three trial metrics; failures are
the three-trial mixed-workload totals at that level.

| Concurrency | Raw profile p50 | Mart profile p50 | Raw profile p95 | Mart profile p95 | Raw / mart throughput | Raw / mart failures |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 581.71 ms | 451.26 ms | 592.96 ms | 461.94 ms | 2.17 / 2.39 rps | 0 / 0 |
| 2 | 1,029.98 ms | 855.74 ms | 1,084.34 ms | 917.37 ms | 2.95 / 3.25 rps | 0 / 0 |
| 4 | 1,324.84 ms | 993.22 ms | 1,533.78 ms | 1,231.35 ms | 4.13 / 4.54 rps | 0 / 0 |
| 8 | 2,380.26 ms | 1,933.85 ms | 2,747.57 ms | 2,270.56 ms | 4.12 / 4.52 rps | 0 / 0 |
| 12 | 2,787.70 ms | 2,478.17 ms | 3,181.89 ms | 2,909.13 ms | 4.72 / 4.98 rps | 21 / 10 |

No provider-profile request failed. At concurrency 12, other requests in the mixed workload hit the
intentional two-second pool deadline: 21 raw-side and 10 mart-side HTTP 503s across three trials.
The mart reduced overloads by 52%, but did not eliminate them. Median mixed throughput improved by
about 10% through concurrency 8 and 5.5% at concurrency 12. Median process CPU and RSS stayed in the
same envelope; mart RSS was lower at concurrency 4–12.

## Plan and acceptance-gate decision

The exact three-case workload is
[`s3-provider-profile-cases-v1.json`](workloads/s3-provider-profile-cases-v1.json). Both plan
captures had zero plan or status errors. The complete request still executes 15–16 SQL statements.

| Complete profile | Operator-time reduction | Rows-scanned reduction | Capture-wall reduction |
| --- | ---: | ---: | ---: |
| Rich | 24.18% | 6.77% | 16.88% |
| Standard | 17.97% | 6.78% | 25.17% |

The first slice does not pass either route-switch option:

1. The latency gate requires at least 20% lower provider-profile p95 at concurrency 1 and 12. The
   measured improvements were 22.10% and 8.57%, respectively.
2. The alternate gate requires at least 30% lower operator work/CPU/spill with p95 no worse than
   5%. Operator time fell only 24.18% for the rich profile and 17.97% for the standard profile.
3. Current code/candidate correctness is also blocked until a new candidate materializes the
   deterministic header selection.

The result is still useful: precomputing header, locations, and groups improves provider latency
and throughput, but the remaining claims, drug, industry, research, quality, and hospital work
dominates the complete route.

## Capacity and next work

After evaluation, the filesystem was 77.54% used with 66,666,803,200 bytes free. One additional
candidate-sized copy projects 84.29%, only 0.71 percentage points below the 85% block. The
active-plus-two validated rollback floor passes, but that margin is not sufficient for another
same-sized experiment plus normal growth.

The superseded candidate database was subsequently removed through the controlled retention
procedure recorded in
[`s3-provider-profile-candidate-retention-cleanup-2026-08-14.md`](s3-provider-profile-candidate-retention-cleanup-2026-08-14.md).
The release and comparison records remain, production stayed on its verified deployment, and the
post-cleanup preview reports 70.79% used with 91,137,449,984 bytes free. A conservative
24,696,061,952-byte candidate now projects 77.60% use and passes the capacity gate.

The next sequence is:

1. Retain this evidence and the three-table design, but keep the profile backend on `raw`.
2. The exact superseded staging database has been reclaimed without touching the active or two
   rollback warehouses.
3. The next provider slice is implemented and merged in PR
   [#73](https://github.com/blakethom8/cms-data/pull/73): utilization/prescribing summary, top
   services, and top drugs, together with the deterministic header fix. Its contracts retain
   separate claims source periods and provenance, and its combined release policy allows exactly
   six profile tables to differ.
4. Produce one new isolated six-table candidate and repeat component/full-response parity, plans,
   three concurrency trials, capacity, isolated smoke, and rollback rehearsal.
5. Stop again before production preparation or authorization. Consider cutover only if the combined
   slice clears every existing gate.

Merged work supporting this evaluation is in PRs
[#68](https://github.com/blakethom8/cms-data/pull/68),
[#69](https://github.com/blakethom8/cms-data/pull/69),
[#70](https://github.com/blakethom8/cms-data/pull/70), and
[#71](https://github.com/blakethom8/cms-data/pull/71). The combined claims-mart implementation is
in [#73](https://github.com/blakethom8/cms-data/pull/73).

The next six-table candidate subsequently passed its correctness and performance gates but remains
blocked from production preparation by the exact-copy capacity gate. See the
[`complete candidate evaluation`](s3-provider-profile-complete-candidate-2026-08-15.md).
