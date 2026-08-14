# S2 canonical behavior baseline — 2026-08-14

## Outcome

The first S2.2 diagnostic pass completed against the selected immutable warehouse. All 42 requests
(14 cases × 3 trials) returned the expected status, and every explicitly counted result stayed
constant across trials. Six cases produced different response bytes for identical requests. Those
ordering/determinism gaps are now explicit parity blockers; they are not treated as mart failures or
smoothed into latency claims.

No route, service, deployment selector, candidate, warehouse, or production configuration changed.
The prepared bounded-executor candidate remained stopped and unselected.

## Identity

- Generated: `2026-08-14T01:26:50Z`
- Selected deployment: `deployment-20260811T155814Z-6baa26aa69`
- API code: `bcd338fa8670caa2c533ff47aa551b77077503d4`
- Warehouse release: `warehouse-20260811T021837Z-f44c147e30`
- Warehouse SHA-256: `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`
- Executor: one process-global DuckDB connection with library-default configuration
- Case manifest SHA-256: `d61bc78e3ec116d4831345df2c9e5deaabad432dc98eccdef14e5b69dce6c762`
- Evidence SHA-256: `24223869ebf8a061deb196de172383c979846a2c23e5023344c5492410719086`

These are sequential isolated-request medians, not mixed-load or bounded-executor measurements.
Use the committed concurrency workload for queueing and throughput comparisons.

## Three-trial medians and determinism

| Case | Median | Count each trial | Stable response bytes |
| --- | ---: | ---: | :---: |
| Rich provider profile | 313 ms | — | no |
| Standard provider profile | 235 ms | — | yes |
| Missing provider | 12 ms | — | yes |
| CMS-enrollment practice state search | 161 ms | 25 | no |
| NPPES-primary practice state search | 509 ms | 25 | no |
| CMS multi-ZIP/multi-specialty search | 146 ms | 2 | yes |
| Empty CMS practice search | 130 ms | 0 | yes |
| State market snapshot | 232 ms | — | no |
| Industry search | 488 ms | 100 | no |
| Industry manufacturer options | 202 ms | — | yes |
| Industry provider detail | 140 ms | — | yes |
| Radar city search | 140 ms | 100 | yes |
| Radar ZIP/taxonomy search | 2,405 ms | 1 | yes |
| Ten-NPI Explorer evidence | 11,103 ms | — | no |

The ten-NPI Explorer case and selective Radar ZIP/taxonomy case are the most expensive isolated
paths in this corpus. Explorer remains source-faithful and outside summary-mart replacement. Radar
already uses release-built state/event tables; the result only justifies plan inspection, not a new
mart by assumption.

## Required follow-up before a practice route switch

The first two investigations are complete in the
[S2 query-plan baseline](s2-query-plan-baseline-2026-08-14.md). It captured all 452 route queries
and identified the exact unstable response fields. Remaining work is:

1. Define and test the intended ordering, representative-value, and monetary-precision semantics
   before using exact response digests as a parity gate. **Complete:** the
   [stabilization record](s2-parity-oracle-stabilization-2026-08-14.md) reports all fourteen cases
   byte-stable across three trials.
2. Add discovered practice roster and site-profile cases after the search response selects a real
   site, so drill-downs use the same site identity rather than a hand-maintained address.
3. Re-run the corpus against an isolated serving-mart candidate on the same warehouse. A mart route
   cannot pass while status, counts, ordering, null behavior, or response shape differ.

HTTP timing is not operator-plan evidence. No serving-mart DDL or route implementation should be
chosen from this table alone.
