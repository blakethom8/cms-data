# S3 provider-profile production-data preflight — 2026-08-14

> **Decision update:** source readiness, capacity, and explicit reassignment-source provenance now
> pass. The first production-data build failed closed on two NPPES international addresses with no
> ZIP; its incomplete copy was removed after preserving failure evidence. Production traffic, the
> selected deployment, and its warehouse were not changed. No valid staging candidate, production
> artifact, deployment, or route switch was created.

## Starting identity

PR [#66](https://github.com/blakethom8/cms-data/pull/66) merged the three-table provider-profile
serving implementation as `babca70ca080c8a1b0a1012a5e897272b6d946ea`. The selected production
deployment remained `deployment-20260814T201311Z-0325c353c9`, serving warehouse
`warehouse-20260814T183948Z-e5ff46dce9` with SHA-256
`2bcc92d44014b62e2bc0c4c42d3c1b814827668ed653b13ffe565ceea7aac9d3`. The production manager
reported passed artifact integrity, a matching pointer and ledger, zero blocking transactions, no
transition sentinel, and a healthy verified control plane.

## Initial capacity gate

The official read-only retention preview reported:

- total filesystem bytes: 362,633,863,168;
- used bytes: 303,327,387,648 (83.65%, `critical`);
- available bytes: 44,515,405,824;
- active-plus-two validated rollback floor: passed; and
- zero-additional-byte gate: allowed at 83.65%.

The selected warehouse is 21,513,908,224 bytes before adding the provider-profile tables. Using
that size as a conservative lower bound for one new immutable staging copy projects 89.58%, above
the 85% promotion block. The actual candidate will be larger, and a later distinct production copy
would require its full exact size again. Therefore the preflight did not invoke
`build-provider-profile-core-release`.

## Source readiness

Read-only queries used one DuckDB thread and a 2 GiB memory ceiling against the selected immutable
warehouse. Every required column exists in `raw_nppes`, `raw_dac_national`, `raw_reassignment`,
`nucc_taxonomy`, and `address_geocode`.

| Source | Rows scanned | Eligible rows missing run/period provenance |
| --- | ---: | ---: |
| NPPES | 7,404,601 | 0 |
| National DAC | 3,388,151 | 0 |
| Group reassignment | 3,361,139 | 0 |

Estimated serving-table grains, calculated without writing data, are:

| Serving table | Expected rows |
| --- | ---: |
| `serving_provider_profile_headers` | 7,404,664 |
| `serving_provider_profile_locations` | 10,078,566 |
| `serving_provider_profile_groups` | 3,444,406 |

NPPES has no duplicate NPI rows, geocodes have no duplicate address keys, DAC has no empty PAC IDs,
and reassignment has no missing PAC IDs. Those results remove four important parity and uniqueness
risks before materialization.

## Parity learning

The preflight found 125,907 NPI/PAC pairs with more than one published reassignment-size value. The
raw API query resolves those values with DuckDB `any_value`, while the initial serving transform
used `max`. That could change the `reassignment_size` response for otherwise identical groups.

The follow-up parity fix restores the raw oracle's aggregation for DAC group size and reassignment
size and adds a duplicate-value fixture. PR #67 merged that fix as
`d2d08c484884deb7ce3d9433e4ff83b3849aacce`; its CI and the local full suite passed. Production
remains on the raw profile backend regardless.

## Reviewed capacity-recovery candidates

The retention planner reports 71,757,832,192 allocated bytes across 60 review candidates but zero
bytes as automatically reclaimable. No deletion was performed. Three superseded derived warehouse
artifacts are the most useful reviewed set:

| Production artifact | Allocated bytes | Recovery evidence |
| --- | ---: | --- |
| `warehouse-20260720T235355Z-684f3cd62d` | 6,519,279,616 | Exact staging counterpart has the same 6,519,271,424-byte file and SHA-256 `7975434c…12c26` |
| `warehouse-20260722T010607Z-ce61bf5add` | 20,093,358,080 | All referencing deployments are superseded; the retained July source snapshots and release manifest can rebuild it, but no exact database counterpart remains |
| `warehouse-20260811T021837Z-f44c147e30` | 20,569,673,728 | Exact staging counterpart has the same 20,569,665,536-byte file and SHA-256 `91e2ee4e…5ef2` |

Together they account for 47,182,311,424 allocated bytes. The active warehouse and both validated
rollback warehouses are not in this set. The legacy 6.4 GB production artifact is also deliberately
excluded because this preflight did not identify an exact retained counterpart.

If separately approved and removed through quarantine with pre/post health checks, this set would
reduce used bytes to approximately 256.1 GB before candidate allocation. Two additional baseline-
sized copies would project roughly 82.5% use, leaving about 9.1 GB below the 85% threshold for the
duplicated provider-profile mart growth. This is only a planning estimate: after building the
staging candidate, its exact byte size must be used for a fresh production-copy preview. A failed
preview still stops preparation.

## Approved cleanup outcome

The named three-artifact set was subsequently approved, quarantined by exact path, health-checked,
and removed. No active or rollback-floor warehouse was included. Before deletion, each artifact was
proved unopen and either checksum-identical to a retained staging database or rebuildable from the
retained July source snapshots and release evidence.

The cleanup reclaimed approximately 47,182,209,024 filesystem bytes. The post-cleanup filesystem
reported 256,145,379,328 used bytes and 91,697,414,144 available bytes. The official retention
preview passed the active-plus-two rollback floor and reported 70.63% use. One baseline-sized
candidate projects 76.57%; two baseline-sized copies project 82.50%, both below the 85% promotion
block. The selected deployment remained `deployment-20260814T201311Z-0325c353c9`; its service PID,
restart count, integrity check, and health endpoint remained stable.

## Reassignment provenance reconciliation

The selected staging release records complete NPPES and DAC periods but omits
`cms_revalidation_group_reassignment` from both `source_periods` and `source_run_ids`, even though
its `raw_reassignment` table contains exactly one run and period:

- run `20260721T220859Z-0353abdb`;
- period `2026-07-01/2026-07-31`; and
- 3,361,139 rows.

The original passed acquisition manifest and its 534,858,072-byte retained `source.csv` remain in
the July refresh workspace. The artifact SHA-256 is
`6b1443ae43a35855a3119fc5732b24c279320da60a43910e185bd65e5b514aed`. The selected production
deployment's immutable source-manifest evidence independently records that same run as active.

The safe recovery path is therefore adoption, not release-metadata editing. The staging-only
`adopt-validated-source-run` command requires the original passed manifest, matching active
production evidence, the exact expected source/run identifiers, and source bytes that reproduce the
manifest's size, checksum, encoding, schema fingerprint, row count, and invalid-identifier count.
It copies the artifact into an immutable managed run path and is idempotent. The provider-profile
builder's optional `--reassignment-run-id` then revalidates that managed artifact and requires the
baseline raw table's sole run, period, and row count to match before it can fill the missing release
provenance. The candidate records the reconciliation in `reconciled_source_runs`.

The adoption completed for run `20260721T220859Z-0353abdb`, and an immediate replay returned
`adopted: false`. The managed artifact is root-owned, read-only, 534,858,072 bytes, and retains the
expected checksum. A fresh full hash and read-only open of baseline
`warehouse-20260814T183948Z-e5ff46dce9` passed with 7,395,713 core providers and 44 tables. After
adoption, the exact baseline-sized capacity preview reported 70.78% current use, 76.72% projected
use, and a passed rollback floor.

## First production-data build learning

The first reconciled build allocated failed release
`warehouse-20260814T221250Z-6fc15bc3b0` and stopped on the warehouse constraint
`serving_provider_profile_locations.addr_key NOT NULL`. The build transaction rolled back, no
completed `warehouse.duckdb` was created, and the 21,735,944,192-byte partial copy was preserved long
enough to archive its failed release manifest, proved unopen, then removed by its exact filename.

Read-only diagnostics found the full cause:

- DAC rows with a non-empty street and null ZIP: 0;
- NPPES rows with a non-empty street and null ZIP: 2;
- affected NPIs: `1306373501` and `1760673693`; and
- each affected NPI has exactly one NPPES location and no DAC location.

The raw profile query includes both legitimate international/territory addresses and returns
`zip5: null`; only its hidden concatenated address key becomes null. Excluding those rows or
inventing a ZIP would break the raw oracle. The follow-up fix therefore uses source-qualified,
internal missing-ZIP sentinels only for the mart key. The visible street, city, state, phone, and
null ZIP remain unchanged; DAC and NPPES null keys cannot incorrectly merge; and a null-key DAC row
still cannot acquire roster evidence through a join that is null in the raw query.

## Next safe sequence

1. Merge and seal the missing-ZIP key fix after the full suite and CI pass.
2. Re-run the exact candidate-capacity preview and build one isolated provider-profile candidate
   with the already adopted explicit reassignment run.
3. Validate its exact three-table scope, contracts, invariant fingerprints, actual size, raw/mart
   parity, query plans, and concurrency results.
4. Stop before production preparation, policy authorization, or cutover and present the evidence.
