# S3 provider-profile production-data preflight — 2026-08-14

> **Decision:** source readiness passes, but capacity blocks candidate allocation. Production was
> not changed. No staging candidate, production artifact, deployment, or route switch was created.

## Starting identity

PR [#66](https://github.com/blakethom8/cms-data/pull/66) merged the three-table provider-profile
serving implementation as `babca70ca080c8a1b0a1012a5e897272b6d946ea`. The selected production
deployment remained `deployment-20260814T201311Z-0325c353c9`, serving warehouse
`warehouse-20260814T183948Z-e5ff46dce9` with SHA-256
`2bcc92d44014b62e2bc0c4c42d3c1b814827668ed653b13ffe565ceea7aac9d3`. The production manager
reported passed artifact integrity, a matching pointer and ledger, zero blocking transactions, no
transition sentinel, and a healthy verified control plane.

## Capacity gate

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
size and adds a duplicate-value fixture. This fix must merge and pass CI before a candidate build.
Production remains on the raw profile backend regardless.

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

## Next safe sequence

1. Merge the raw-oracle aggregation fix and rerun the full suite.
2. Obtain explicit approval for the named three-artifact cleanup, or add storage instead.
3. Quarantine only the approved artifacts, verify production identity and health, then remove them
   and record exact reclaimed filesystem bytes.
4. Re-run the zero-byte and baseline-size capacity previews.
5. Build one isolated provider-profile candidate from the selected staging release.
6. Validate its exact three-table scope, contracts, invariant fingerprints, actual size, raw/mart
   parity, query plans, and concurrency results.
7. Stop before production preparation, policy authorization, or cutover and present the evidence.
