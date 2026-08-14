# S2 NPPES-primary production-data candidate — 2026-08-14

> **Decision:** the data and performance gates pass, but production preparation and cutover are
> blocked by storage capacity. Production was not changed.

## Outcome

The isolated staging release `warehouse-20260814T183948Z-e5ff46dce9` adds the two NPPES-primary
practice serving tables to the exact selected warehouse baseline:

- candidate SHA-256:
  `2bcc92d44014b62e2bc0c4c42d3c1b814827668ed653b13ffe565ceea7aac9d3`;
- size: 21,513,908,224 bytes, 562,561,024 bytes larger than the baseline;
- build code: `55d31add1ad2751ba803486d6a22ce45bc0aa840`;
- `serving_practice_nppes_provider_sites`: 1,229,202 rows;
- `serving_practice_nppes_org_memberships`: 1,361,659 rows; and
- validation passed, state `not_promoted`.

The selected production deployment remained
`deployment-20260814T172445Z-3cd965d04e`, code
`b80d56510757770f1f6f6d90492053948567b08b`, and warehouse
`warehouse-20260814T025428Z-5dac630227`. The live API stayed healthy on PID `4002795` with zero
restarts and 7,395,713 core providers. No production artifact was copied, no deployment was
prepared, and no selector, ledger, route configuration, or service process changed.

## Build and comparison

The candidate was built from the retained staging copy of the selected warehouse, whose
20,951,347,200-byte file matched production at SHA-256
`bf7d2381c8c9a683497a7dcc5d64c87ccfb3359fe5ee77d61e74ca3bffb1fa02`.
Its managed source periods are NPPES monthly `2026-07-13`, NPPES weekly through
`2026-07-27/2026-08-02`, DAC `2026-07-31`, and full-year 2024 Part B and Part D.

Both serving-mart contracts passed with zero duplicate keys, required nulls, invalid NPIs,
invalid state/ZIP values, empty specialty arrays, source-provenance failures, or membership-parent
orphans. The `serving_practice_nppes_additive_v1` comparison passed: 42 invariant tables had the
same schema and order-independent logical fingerprints, and only the two allowlisted serving tables
were added.

Three fail-closed build learnings are retained:

1. A sealed source archive has no `.git`, so the first invocation rejected the missing code
   identity before allocating a release.
2. The builder now requires an explicit exact 40-character hexadecimal `--code-commit` for sealed
   archives; missing, abbreviated, and malformed values still fail before allocation.
3. The first retry as `dataops` could not read the existing root-owned `0600` staging release
   ledger. The successful invocation followed the existing release-builder operating boundary and
   ran as root with bytecode disabled. No partial candidate was allocated by either failure.

## Response correctness

The focused four-case manifest covers state, multi-specialty/multi-ZIP, proximity/limit, and empty
NPPES-primary searches. Raw and mart instances used the same candidate, a 2 GB DuckDB limit, four
threads, pool size four, a two-second acquisition limit, and three sequential trials. Every trial
matched byte-for-byte, including HTTP status, response digest, result count and order, totals,
truncation, and empty behavior.

| Case | Raw median | Mart median | Result |
| --- | ---: | ---: | --- |
| State | 1,527.22 ms | 146.61 ms | exact |
| Multi-ZIP / specialty | 1,542.93 ms | 250.40 ms | exact |
| Proximity | 1,456.78 ms | 102.31 ms | exact |
| Empty | 1,233.17 ms | 13.75 ms | exact |

The fourteen-case cross-route corpus returned expected statuses and stable responses within both
backend runs. Thirteen cases matched across the independently started processes. `profile-rich`
used different byte order in the first cross-process capture, but the route does not consume the
NPPES practice backend selector. A direct recheck from the still-running raw-flag and mart-flag
instances was byte-identical at 8,611 bytes and SHA-256
`9a1869888d143dce56ccfc6ca17accbc5f1e00f773e6fa0b518d9c9d84d82e3d`. This is retained as a
pre-existing process-order caveat, not counted as NPPES route parity.

## Query plans and concurrency

Paired `EXPLAIN ANALYZE` captures ran each focused route query against the same immutable candidate.
Both backends captured four queries with zero plan or status failures. The mart plan scanned
46,201,038 operator rows per case versus 204,418,117 for raw, a 77.4% reduction. Capture wall time
fell from 369–759 ms for raw to 14–388 ms for mart. DuckDB's JSON plan reported zero peak-buffer
values, so the independent process RSS measurements remain the memory evidence.

The three-trial workload issued 60 requests per backend at concurrency 1, 2, 4, 8, and 12. Values
below are medians of the three trial summaries.

| Concurrency | Raw p95 | Mart p95 | p95 reduction | Raw req/s | Mart req/s | Raw / mart failures |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,579.14 ms | 257.33 ms | 83.7% | 0.68 | 6.56 | 0 / 0 |
| 2 | 2,961.65 ms | 369.44 ms | 87.5% | 0.88 | 9.81 | 0 / 0 |
| 4 | 4,426.42 ms | 516.85 ms | 88.3% | 1.23 | 13.94 | 0 / 0 |
| 8 | 5,315.39 ms | 810.53 ms | 84.8% | 2.28 | 14.00 | 28 / 0 |
| 12 | 5,083.93 ms | 1,014.84 ms | 80.0% | 3.67 | 13.93 | 41 / 0 |

The mart completed all 900 measured requests without failure. Raw remained failure-free through
concurrency four, then hit the existing two-second pool-acquisition boundary at eight and twelve.
Mart peak RSS was approximately 505–519 MB; raw peak RSS was approximately 1.03–1.07 GB. These
results pass the correctness, performance, bounded-overload, and memory gates.

## Capacity decision

> **Resolved later on 2026-08-14:** the approved
> [July refresh workspace compaction](july-refresh-workspace-compaction-2026-08-14.md) retained all
> 18 checksum-valid source snapshots while reclaiming 26,169,270,272 filesystem bytes. The fresh
> exact 21,513,908,224-byte preview now allows the production copy at 83.64% projected use. The
> candidate remains staging-only; this resolution is not cutover authorization.

After the staging candidate was allocated, the host used 307,955,982,336 of 362,633,863,168 bytes
(84.92%) with 39,886,811,136 bytes free. The active-plus-two verified rollback floor passes.

A zero-additional-byte preview also passes at 84.92%, but it is not a production-preparation gate:
the candidate exists only in staging and policy requires a distinct immutable production artifact.
Supplying the candidate's exact 21,513,908,224 bytes projects 90.85% use, above the 85% promotion
block, and exits nonzero. Therefore:

- do not copy the candidate into `production-artifacts`;
- do not prepare or select a deployment; and
- perform an explicitly reviewed retention cleanup or add storage, then rerun the exact-byte
  preview before any cutover request.

The route code itself is ready for an eventual deployment-local `auto` switch. Both serving tables
must be present and complete or the existing raw implementation remains the fallback. The API
contract and Provider Search RPCs do not change. The follow-on production-manager change accepts
`serving_practice_nppes_additive_v1` only when the release has the exact two-table additive scope,
matching positive counts, complete invariant-fingerprint evidence, passed validation for both
marts, and zero row or membership-parent failures. This code authorization does not copy an
artifact, prepare a deployment, or authorize selection; the corresponding sealed operations
package is not installed while capacity blocks preparation.

## Handoff

The next safe sequence is:

1. review the retention planner's named candidates and authorize only verified, unreferenced
   deletions outside the rollback floor, or expand the volume;
2. require enough headroom for the distinct production copy and normal operating growth rather
   than targeting the 85% boundary;
3. ~~Add and test the exact two-table production-manager policy authorization.~~ Completed in code;
   its sealed operations package remains uninstalled.
4. rerun the exact-byte capacity gate and confirm production identity and health; and
5. only then install the reviewed operations package, copy and prepare an immutable deployment, run
   full isolated smoke and rollback dry-runs, and request separate authorization for selection.

Machine-readable evidence is committed under
[`evidence/s2-nppes-primary-2026-08-14`](evidence/s2-nppes-primary-2026-08-14/) and sealed on the
server under `/srv/cms-data-platform/audits/s2-nppes-candidate-20260814T183600Z/evidence` as
`root:dataops` mode `0440`.
