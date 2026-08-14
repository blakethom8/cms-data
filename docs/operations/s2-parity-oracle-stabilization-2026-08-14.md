# S2 parity-oracle stabilization — 2026-08-14

## Outcome

The existing raw-route responses are now stable enough to act as the exact oracle for the first S2
serving mart. All fourteen canonical cases returned their expected status and identical response
bytes in three consecutive trials: 42 successful requests, zero status failures, and zero response-
stability failures.

The verification used an isolated loopback-only API process. It did not select a deployment, change
a route, modify the warehouse, restart production, or authorize a cutover.

## Identity and isolation

- Stabilization code: `b996f19088e2e280e8f5c1008ed15d8cea39d913`
- Diagnostic identity: `deployment-20260814T-stable-b996f19088` (an isolated invocation label, not
  a prepared or selectable deployment bundle)
- Selected production deployment, unchanged:
  `deployment-20260811T155814Z-6baa26aa69`
- Warehouse release: `warehouse-20260811T021837Z-f44c147e30`
- Warehouse SHA-256: `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`
- DuckDB: `1.4.4`
- Isolated listener: `127.0.0.1:18081`, stopped after the run
- Executor: `2GB` memory limit, `8` DuckDB threads per connection, pool size `2`, four-second pool
  acquisition timeout, sequential diagnostic requests
- Case manifest SHA-256: `d61bc78e3ec116d4831345df2c9e5deaabad432dc98eccdef14e5b69dce6c762`
- Diagnostic evidence SHA-256: `618aab54de9a56d5ea2bdf2bdf92ee5c98b25cd4a4b119997bb7fa25d893549b`

The selected immutable warehouse file was opened read-only and retained mode `0440`. The temporary
process used the selected production runtime but the exact archived stabilization commit. Timings
from this sequential determinism run are not a throughput or serving-mart performance comparison.

## Stabilized semantics

The response shape and representation version remain unchanged. The stabilization defines:

- cent precision for practice-search monetary aggregates;
- explicit order for profile locations, suites, IDs, distinct strings, industry ranks, facets,
  detail rows, and pagination ties;
- coherent deterministic DAC representatives for industry discovery;
- lexical minimum non-null publisher values as stable market-site display representatives;
- provider and site ID ordering with primary sites retained before other sites; and
- full exposed-column ordering for source-faithful Explorer evidence, with duplicate receiving
  practice locations collapsed at the endpoint's declared enrollment/location grain.

These rules eliminate accidental aggregation- and scan-order behavior. They do not assert that a
lexically selected publisher value is more authoritative, and they do not collapse evidence across
different publisher grains.

## Verification and evidence

Focused tests cover cent rounding, market representatives, profile ordering, industry tie-breakers,
and Explorer reassignment/location behavior. The complete API suite passed with 454 tests, one
intentional skip, and no failures.

Sealed evidence is retained on the production host at:

`/srv/cms-data-platform/production/evidence/s2-parity-oracle-stabilization-20260814/`

Both evidence files are owned by `root:dataops` and mode `0440`. After the run, port `18081` was
closed. Production remained active at PID `3240475`, with zero restarts and the same selected
deployment.

## Next boundary

Build `serving_practice_provider_sites` only in an isolated additive candidate. Compare the mart
implementation against this stabilized raw route over the complete parity corpus. Do not switch the
route, select the candidate, or cut over production without a separate approval.
