# Command Center source observability deployment — 2026-08-11

> **Status: PROMOTED AND VERIFIED** — API deployment
> `deployment-20260811T155814Z-6baa26aa69`, static dashboard release
> `command-center-20260811T1550Z-bcd338f`, and the temporary password gate are live.

## Outcome and boundary

This work improves the Source Contracts and Source-to-Mart Lineage workspaces and gives “last ran”
an evidence-backed meaning. It does not refresh, build, mutate, copy over, or replace the selected
DuckDB warehouse.

The displayed time concepts are intentionally separate:

- **Last ran** is the latest recorded pipeline lifecycle event for a source: promotion, validation,
  retrieval, or discovery.
- **Source period** is the publisher data vintage represented by that run.
- **Observed at** is when the browser requested the current read-only API state.

The live selected API deployment remains
`deployment-20260811T145509Z-187674a921`, with unchanged warehouse
`warehouse-20260811T021837Z-f44c147e30`. The candidate reuses that exact immutable warehouse and
runtime `runtime-candidate-8985e8a-c26024b3`.

## Shipped outside the API approval gate

Commit `b39ab3c` added HTTP Basic Auth to the HTTPS dashboard gateway. The shared account name is
`dashboard`; the password and bcrypt record remain root-owned outside Git. After reload,
unauthenticated HTTPS returned `401`, HTTP continued to redirect with `301`, nginx was running with
restart count 0, and the private API health route remained healthy.

Commit `bcd338f` added source-contract summary metrics, pipeline last-run evidence, cadence and run
identity, clearer lineage evidence counts, and a CSP-compatible graph layout. Browser QA found that
the prior graph used inline position styles that the strict Content Security Policy blocked, causing
nodes to stack at the graph origin. The replacement uses a fixed CSS grid and keeps the CSP strict.
Desktop Contracts, desktop Lineage, filtering/navigation semantics, and a 390×844 Contracts view
were checked without browser errors.

The sealed static release is selected at:

```text
/srv/cms-data-platform/command-center/releases/command-center-20260811T1550Z-bcd338f
```

Its gateway PID is `3236771`. The artifact has zero writable paths and zero bytecode/cache paths.
The predecessor `command-center-20260811T1510Z-18bfc9f` remains intact for rollback.

## Why an API candidate is required

The selected deployment already has a sealed, reconciled `source-manifests.json` with 20 runs and
18 proven-active source contracts. `/release` reads that deployment-scoped evidence and correctly
reports 18 source vintages. The `/operations/*` router instead defaulted to the checkout-local
`data/manifests.json`, which is absent from the sealed production code artifact. The dashboard
therefore reported all 18 source contracts as missing even though the selected-release evidence was
present.

Commit `1e2bcad` fixes only that lookup. When `CMS_MANIFEST_PATH` is not explicitly configured, the
read-only operations router now derives the selected deployment from `DUCKDB_PATH` and reads:

```text
production/evidence/<selected-deployment-id>/source-manifests.json
```

The explicit override remains available for non-bundle installations. No response field was added
or removed, so `representation_version` remains `3`. A regression test proves that the router
follows the selected immutable bundle.

## Commit and test record

All commits are pushed to `origin/main` through `bcd338f`.

Before `b39ab3c feat(command-center): require dashboard authentication`, the required full suite
returned:

```text
371 passed, 1 skipped, 135 warnings in 10.22s
```

Before `1e2bcad fix(api): bind operations evidence to selected release`, the required full suite
returned:

```text
372 passed, 1 skipped, 135 warnings in 10.26s
```

Before `bcd338f feat(command-center): clarify source freshness and lineage`, the final required full
suite returned:

```text
372 passed, 1 skipped, 135 warnings in 10.63s
```

`node --check dashboard/command-center/app.js` also passed before the dashboard commit.

## Code-only candidate preparation

Preconditions passed: manager healthy, artifact integrity passed, zero blocking transactions, no
transition sentinel, `release-current` matched the verified ledger, port 18080 was free, the API PID
was `3226584`, and approximately 80 GiB remained free.

The clean detached artifact is:

```text
/srv/cms-data-platform/production-artifacts/code/
  bcd338fa8670caa2c533ff47aa551b77077503d4-operations-evidence-1
```

It is sealed `root:dataops`, has zero writable or bytecode/cache paths, and has tree fingerprint
`sha256:48c205e2bd46cdfa017b49727352cf27e34fcacc57aebdbd215759e74b4935b0`.
The dependency diff against served commit `18bfc9f` was empty, so the selected runtime was reused.

Prepare dry-run passed. The first real prepare attempt was stopped by a 90-second command timeout
while re-hashing the 20.6 GB warehouse. It produced no candidate record or manager mutation. A
post-timeout audit found the original selector unchanged, no sentinel, no port listener, a healthy
manager, and zero blocking transactions. The clean retry with a longer validation window passed and
created:

```text
deployment_id: deployment-20260811T155814Z-6baa26aa69
state: prepared
previous_deployment_id: deployment-20260811T145509Z-187674a921
warehouse_release_id: warehouse-20260811T021837Z-f44c147e30
warehouse_sha256: 91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2
warehouse_byte_size: 20569665536
runtime_fingerprint: sha256:82370f7e4b25f1a907a92eda5c1097302a6f88936ad59319206b4ade3cc7c347
error_summary: null
```

The predecessor snapshot was copied to candidate-scoped evidence as required for a code-only
deployment. Both files have SHA-256
`fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244`; the candidate copy is
`root:dataops` mode `0440`, 32,909 bytes.

## Rehearsal evidence

The candidate ran as `dataops` with `PYTHONDONTWRITEBYTECODE=1`, Python `-B`, its immutable bundle,
and loopback port 18080. The complete smoke output was:

```text
Production smoke: passed
Evidence: /srv/cms-data-platform/production/evidence/deployment-20260811T155814Z-6baa26aa69/smoke.json
- health: passed
- process_identity: passed
- authentication_required: passed
- practice_capabilities: passed
- practice_search: passed
- provider_profile: passed
- industry_search: passed
- industry_options: passed
- industry_exact_option_round_trip: passed
- industry_detail: passed
- research: passed
- clinical_trials: passed
- explorer_catalog: passed
- required_tables: passed
- warehouse_counts: passed
```

The smoke evidence SHA-256 is
`22ad524cbb612976228781dc75d9b241b31f3ad1f6b5e7a1716084ddb9f6883c`.

Serving-contract checks returned:

```text
release_id: deployment-20260811T155814Z-6baa26aa69
representation_version: 3
source_vintages: 18
warehouse_release_id: warehouse-20260811T021837Z-f44c147e30
etag: "deployment-20260811T155814Z-6baa26aa69:3"
conditional_status: 304
```

Source-observability checks returned:

```text
registered_sources: 18
active_evidence: 18
manifest_runs: 20
latest_observed_at: 2026-08-11T02:41:41+00:00
evidence_error: None
source_contracts: 18
validated_active: 18
nppes_last_run: 2026-08-11T02:41:41+00:00
nppes_source_period: 2026-07-13
nppes_run_id: 20260811T015237Z-9b77e0e8
lineage_observed_source_landings: 16
lineage_observed_tables: 33
lineage_declared_edges: 83
lineage_evidence_error: None
```

The rehearsal process stopped cleanly, port 18080 was released, and the sealed API artifact still
has zero writable and zero bytecode/cache paths. Activate and rollback dry-runs both returned exit
code 0 with `error_summary: null`. The live selector still resolves to the verified predecessor.

## Approval gate and completed cutover

Blake explicitly approved the cutover after reviewing the candidate evidence. Final preconditions
matched: the verified predecessor remained selected on API PID `3226584`, manager health and
artifact integrity passed, there were zero blocking transactions, no transition sentinel, no port
18080 listener, approximately 80 GiB free, the candidate snapshot and rehearsal smoke hashes were
unchanged, the candidate artifact had zero writable/cache paths, and the predecessor bundle was
intact.

The first `pipeline.production_cutover` invocation exited with code 2 before selection and reported:

```text
{"state": "error", "error_summary": "API key environment variable is empty: CMS_API_KEY"}
```

The shell had not loaded the required environment file. No secret was printed. A post-exit audit
proved the selector, API PID, ledger, and sentinel were unchanged and the manager remained healthy.
The command was rerun after loading `/etc/cms-data/cms-api.env` and `/etc/aact/reader.env` with
`set -a; . file; set +a`, as required. It returned exit code 0:

```json
{
  "rollback_available": true,
  "selected_deployment_id": "deployment-20260811T155814Z-6baa26aa69",
  "smoke_evidence": "/srv/cms-data-platform/production/evidence/deployment-20260811T155814Z-6baa26aa69/smoke.json",
  "state": "promoted"
}
```

Post-cutover manager status is healthy and verified at `2026-08-11T16:06:39+00:00`, with artifact
integrity passed, zero blocking transactions, pointer matching the ledger, no transition sentinel,
and selected code commit `bcd338fa8670caa2c533ff47aa551b77077503d4`. The API PID is `3240475`.
The unchanged immutable warehouse remains `warehouse-20260811T021837Z-f44c147e30`.

The cutover-owned smoke evidence is:

```text
/srv/cms-data-platform/production/evidence/deployment-20260811T155814Z-6baa26aa69/smoke.json
SHA-256 d7e925274854bb35b5f16dd5d346059587fb0787d43d679e2a6c6be243b4ee14
```

Live serving-contract checks returned representation version 3, 18 source vintages, warehouse
release `warehouse-20260811T021837Z-f44c147e30`, ETag
`"deployment-20260811T155814Z-6baa26aa69:3"`, and a `304` conditional round trip.

Live source evidence returned 18 registered and 18 proven-active sources, 20 manifest runs, latest
pipeline event `2026-08-11T02:41:41+00:00`, 16 proven source landings, 33 observed warehouse tables,
83 declared lineage edges, and no evidence error. The Command Center gateway independently returned
18 source contracts with all 18 validated active. Unauthenticated public HTTPS still returned 401.
Provider Search `https://mydoclist.com/ready` returned 200 with `cms_data.status=ok`.

The predecessor `deployment-20260811T145509Z-187674a921` remains intact with its original sealed
code artifact, runtime, and unchanged warehouse link for rollback.
