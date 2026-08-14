# S2 canonical query-plan baseline — 2026-08-14

## Outcome

An isolated in-process copy of the selected API captured every DuckDB `execute` call for the
fourteen canonical cases. Each exact SQL statement and bound parameter set was run through
`EXPLAIN (ANALYZE, FORMAT JSON)` on a separate read-only connection before the route executed it
normally. The run captured 452 queries with zero plan errors and zero status failures.

The live service was not instrumented, restarted, or reconfigured. The deployment selector,
warehouse, prepared bounded-executor candidate, and all routes remained unchanged.

## Identity and interpretation

- Selected deployment: `deployment-20260811T155814Z-6baa26aa69`
- API code: `bcd338fa8670caa2c533ff47aa551b77077503d4`
- Warehouse release: `warehouse-20260811T021837Z-f44c147e30`
- Warehouse SHA-256: `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`
- DuckDB: `1.4.4`
- Case manifest SHA-256: `d61bc78e3ec116d4831345df2c9e5deaabad432dc98eccdef14e5b69dce6c762`
- Capture tool SHA-256: `86818eac56990738fe3d9fe2fada8b6ea75ffea6f9349719ca7fe0146c7f1552`
- Raw evidence SHA-256: `293b854b55e22bf107d2d4250eaa865bf4f9ac46666a4c8aa943578ecb772534`

`capture_ms` is wall time for the isolated `EXPLAIN ANALYZE`. Operator time is summed over plan
operators and can exceed wall time when DuckDB runs operators in parallel. Summed scanned rows can
also include repeated scans and work across operators. DuckDB reported zero peak buffer and
temporary-storage bytes in these JSON plans; this is not a substitute for process RSS evidence.

## Route-family work

| Case | Queries | Unique SQL | Plan wall time | Summed scanned rows |
| --- | ---: | ---: | ---: | ---: |
| Rich provider profile | 16 | 16 | 649 ms | 2.51B |
| Standard provider profile | 15 | 15 | 244 ms | 2.51B |
| Missing provider | 1 | 1 | 11 ms | 43.0M |
| CMS-enrollment practice state search | 1 | 1 | 166 ms | 74.7M |
| NPPES-primary practice state search | 1 | 1 | 497 ms | 193.2M |
| CMS multi-ZIP/multi-specialty search | 1 | 1 | 150 ms | 74.7M |
| Empty CMS practice search | 1 | 1 | 130 ms | 74.7M |
| State market snapshot | 2 | 2 | 177 ms | 494.3M |
| Industry search | 1 | 1 | 509 ms | 1.08B |
| Industry manufacturer options | 1 | 1 | 203 ms | 559.2M |
| Industry provider detail | 5 | 5 | 141 ms | 1.29B |
| Radar city search | 3 | 3 | 170 ms | 481.1M |
| Radar ZIP/taxonomy search | 3 | 3 | 2,476 ms | 30.2M |
| Ten-NPI Explorer evidence | 401 | 41 | 12,416 ms | 3.75B |

These figures support the planned CMS-enrollment practice serving mart: the raw query scans DAC,
provider-level Part B, provider-level Part D, and geocode inputs for every boundary, including an
empty result. The NPPES-primary route is materially more expensive but remains a separate semantic
path and is not included in the first mart switch.

## Findings

### Explorer fanout

The ten-NPI Explorer request executes one setup query plus forty source queries per NPI: 401 total
queries, with forty SQL shapes repeated ten times. This is the dominant query-count problem. The
correct optimization is source-faithful batching—one bounded query per source for the requested NPI
set—not a summary mart that collapses publisher rows.

### Selective Radar cost

The Radar ZIP/taxonomy case spends about 1.88 seconds in its event-row query and about 0.58 seconds
in its count query. Each scans about 15.1M rows. Radar already has release-built state/event tables;
inspect filter layout and physical ordering before proposing another mart.

### Industry aggregation

Industry search scans about 1.08B operator rows and spends most operator time in grouping. Industry
manufacturer options scans about 559M. These results justify a later provider summary and facet
slice, but they do not displace the measured practice slice.

### Current response nondeterminism

The refined three-response comparison identified five unstable cases in this run; industry search,
which drifted in the earlier baseline, happened to be stable and remains an intermittent concern.

- Practice search changes `partb_payments` and `partd_drug_cost` at otherwise identical site IDs.
  The raw SQL sums floating-point provider totals; aggregation order can change low-order digits.
- Market provider `org_pac_ids` and `site_ids` reorder because they are accumulated from an unordered
  result set. Site phone and occasional city values change because the first membership row wins at
  a site shared by providers with different publisher values.
- Rich profile location order changes; research sponsors use an unordered `string_agg(distinct ...)`.
- Explorer PPEF reassignment rows reorder while their source-native contents remain the same.

These are existing raw-route behaviors. A serving mart must not silently choose new semantics.
Before parity approval, define deterministic ordering and site representative-value rules, and
round published monetary aggregates to the route's declared precision.

## Next implementation boundary

Proceed with two independently reviewable changes:

1. Stabilize existing response semantics: ordered list/string aggregates, deterministic market site
   representative values, explicit provider/site ID ordering, and published monetary precision.
   This prerequisite is implemented in the follow-up stabilization change and must pass a fresh
   three-trial canonical comparison before it becomes the mart parity oracle.
2. Build `serving_practice_provider_sites` in an isolated candidate while retaining the stabilized
   raw query as the parity oracle.

Neither change authorizes a production route switch or cutover.
