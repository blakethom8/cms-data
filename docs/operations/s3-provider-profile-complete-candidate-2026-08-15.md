# S3 complete provider-profile candidate evaluation — 2026-08-15

> **Last reviewed: 2026-08-15** · **Status: evaluation passed; production copy blocked**
>
> **Decision: do not prepare, authorize, or cut over production yet.** The six-table candidate
> passes response parity, invariant comparison, plan, latency, throughput, isolated smoke, and
> staging rollback gates. A required separate production copy would exceed the 85% capacity block.

## Candidate identity and boundary

The isolated candidate is `warehouse-20260814T235921Z-dc0bde25f1`, built from selected baseline
`warehouse-20260814T183948Z-e5ff46dce9`. It is 26,034,057,216 bytes with SHA-256
`de70e24bdb3f3c2d2e41f956510330673c71fbcdff74e71754b5ed77f20fca87` and DuckDB 1.4.4.
The build used commit `dbac0f1284605596939aa4f11c2a2ef167a52c0f`, one DuckDB thread, a 12 GiB
memory ceiling, and no spill. It completed in 9 minutes 18 seconds with 13,485,416 KiB peak RSS,
zero swap, and validation `passed`.

API evaluation used commit `c1c07c59aedb5acfa55232e7cf8228de0a9fea70`, which contains the response-
type compatibility correction found during parity testing. Production remained on verified
deployment `deployment-20260814T201311Z-0325c353c9`, warehouse
`warehouse-20260814T183948Z-e5ff46dce9`, code commit
`ef9a94fef246011ffa4b7410dd6b31c25ddd148d`, and the `raw` profile backend. The final manager
check reported passed artifact integrity, a matching pointer and ledger, zero blocking
transactions, no transition sentinel, and a healthy control plane.

No production artifact, deployment bundle, authorization, profile route switch, Provider Search
RPC change, or production pointer mutation was created. The checksummed, read-only 37-file audit is
`/srv/cms-data-platform/audits/s3-provider-profile-complete-20260814T2350Z/evidence`.

## Provenance recovery and build result

The first combined build stopped before allocation because the older baseline release recorded
the provider-level Part B and Part D periods but not the distinct service-detail and drug-detail
source IDs. PR [#75](https://github.com/blakethom8/cms-data/pull/75) added explicit, staging-only
reconciliation for verified managed detail runs. The retained artifacts, original passed
manifests, production evidence, baseline raw-table run/period, and row count had to agree.

The successful candidate records three reconciliations:

| Source | Run | Raw rows | Period |
| --- | --- | ---: | --- |
| Group reassignment | `20260721T220859Z-0353abdb` | 3,361,139 | `2026-07-01/2026-07-31` |
| Part B provider and service | `20260721T221019Z-dacc7a22` | 9,781,673 | `2024-01-01/2024-12-31` |
| Part D provider and drug | `20260721T221302Z-0102130a` | 28,023,892 | `2024-01-01/2024-12-31` |

Two provider-level source runs were also adopted into the managed staging manifest store before
the rollback rehearsal because the baseline referenced their active July 21 identities while the
store retained older July 20 entries. Adoption copied and revalidated the exact retained bytes; it
did not alter the warehouse or production.

All six contracts passed with zero duplicate keys, invalid NPIs, orphan NPIs, missing required
values, missing source periods, or row-level provenance failures:

| Serving table | Rows |
| --- | ---: |
| `serving_provider_profile_headers` | 7,404,664 |
| `serving_provider_profile_locations` | 10,078,566 |
| `serving_provider_profile_groups` | 3,444,000 |
| `serving_provider_profile_claims_summary` | 1,784,629 |
| `serving_provider_profile_top_services` | 6,328,077 |
| `serving_provider_profile_top_drugs` | 7,494,398 |

The comparison policy allowed exactly those six tables to differ. It checked 44 invariant logical
fingerprints, found no unexpected differences or evidence mismatches, and passed.

## Exact response parity learning

The first production-data response capture found equal JSON values but different bytes for nine
chronic-condition percentages. Raw returned the publisher's integer type, such as `16`, while the
first mart schema returned `16.0`. PR [#76](https://github.com/blakethom8/cms-data/pull/76) changed
future physical columns to `BIGINT` and added an API compatibility cast for this candidate.

After the fix, rich, standard, and missing-provider cases matched raw exactly in HTTP status,
response byte length, and response SHA-256 across three trials. Both raw and mart captures had zero
plan errors, status failures, response variations, or unstable responses. The complete rich and
standard profile routes execute two fewer SQL statements under the mart backend.

## Plan evidence

| Complete profile | Raw / mart queries | Operator-time reduction | Rows-scanned reduction | Capture-wall reduction | Spill |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rich | 16 / 14 | 58.88% | 45.58% | 33.70% | 0 |
| Standard | 15 / 13 | 44.64% | 45.63% | 41.34% | 0 |

The missing-provider case also retained exact status/body semantics while eliminating the raw
header scan. These figures are query-plan measurements, not HTTP latency claims.

## Concurrency result

Three raw and three mart trials used the same immutable database, API code, two-second pool wait,
four-connection pool, committed mixed workload SHA-256
`0956223308be22f6807c33bd230df941c7a2b22e7c6949c0692d9946bc0eb8f0`, and 60 requests per
concurrency level. Values below are medians across trials.

| Concurrency | Raw profile p95 | Mart profile p95 | Profile p95 improvement | Raw / mart mixed throughput | Raw / mart mixed failures |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 601.26 ms | 298.04 ms | 50.43% | 2.16 / 2.76 rps | 0 / 0 |
| 2 | 1,089.70 ms | 643.98 ms | 40.90% | 2.96 / 3.75 rps | 0 / 0 |
| 4 | 1,471.74 ms | 811.83 ms | 44.84% | 4.12 / 5.24 rps | 0 / 0 |
| 8 | 2,714.37 ms | 1,555.92 ms | 42.68% | 4.11 / 5.24 rps | 0 / 0 |
| 12 | 3,086.30 ms | 2,325.79 ms | 24.64% | 4.70 / 5.22 rps | 7 / 0 |

The candidate passes the primary route-switch gate because provider-profile p95 improves by at
least 20% at both concurrency 1 and 12. Mixed throughput improves 11.06% to 27.78%, and the mart
eliminates the median seven raw-side overload failures at concurrency 12. Provider-profile requests
themselves had zero failures in both modes.

## Isolated smoke and rollback

The first staging promotion attempt failed closed without changing the pointer because two active
provider-level source manifests were absent from the managed staging store. After their verified
adoption, transaction `6e154eebafc04c66b8e74bc0d756c72b` pointed only
`data/staging/warehouse-current` at the candidate. The three-case mart smoke matched raw exactly.
Rollback transaction `4b895bfdeb4f49bd9578cab9daad044e` restored the prior staging warehouse
`warehouse-20260720T235355Z-684f3cd62d`. No production pointer participated.

## Capacity and decision

After the candidate and four retained source copies were allocated, the official preview reports
291,182,084,096 bytes used (80.30%, `critical`) and 56,660,709,376 bytes free. The active-plus-two
validated production rollback floor remains intact and a zero-additional-byte operation passes.

A separate production copy of the exact 26,034,057,216-byte candidate would project 87.48% use,
above the 85% hard block. Production preparation must therefore remain stopped. In addition, the
production manager intentionally does not yet authorize
`serving_provider_profile_complete_additive_v1`.

The recommended next sequence is:

1. Review exact, unreferenced retention candidates or add storage until a fresh full-copy preview
   passes with useful operating margin; preserve the active and two validated rollback warehouses.
2. Decide whether to rebuild once with the corrected physical percentage types or retain this
   validated candidate with the tested API compatibility cast.
3. Add the six-table policy to production authorization only after the capacity decision and an
   independent final evidence review.
4. Re-run the exact production-copy preview, preparation dry run, live smoke, and rollback gate.
5. Cut over only if every gate still passes; otherwise keep production on `raw`.
