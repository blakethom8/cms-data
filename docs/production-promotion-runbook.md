# Production Cutover Runbook

> **Last reviewed: 2026-08-18** · **Status: current approval-gated production procedure**

## Scope

This runbook performs one reversible serving cutover. It does not download source data, rebuild a
warehouse, run refresh jobs, or overwrite a DuckDB file. Staging remains under
`/srv/cms-data-platform/data`; production artifacts are separate immutable copies.

The cutover unit is one bundle:

```text
/srv/cms-data-platform/
  production-artifacts/                 root:dataops; never service-writable
    code/<code-id>/
    runtimes/<runtime-id>/
    warehouses/<warehouse-id>/warehouse.duckdb
    utilization/<utilization-id>/utilization.duckdb   # optional independent sidecar
  production-ops/
    <ops-id>/                            immutable control and smoke code
    current -> <ops-id>
  production/                            root:dataops 0750
    releases/<deployment-id>/            root:dataops 0550
      code -> production-artifacts/code/<code-id>
      runtime -> production-artifacts/runtimes/<runtime-id>
      warehouse -> production-artifacts/warehouses/<warehouse-id>/warehouse.duckdb
      utilization -> production-artifacts/utilization/<utilization-id>/utilization.duckdb
    release-current -> releases/<deployment-id>
    deployments.json
    deployment-journal.json
    transition-pending                   exists only during a pointer transaction
    evidence/<deployment-id>/smoke.json
```

`release-current` is the only serving selector. Activation and rollback replace that symlink once;
the internal artifact links never change. `utilization` is optional so older rollback bundles
remain valid. The API user can read but cannot modify the control
tree or artifacts. All manager and cutover commands run as root from `production-ops/current`, not
from the selected application runtime.

## Stop conditions

Stop before selection or restart if any of these is true:

- the approved code ID, warehouse release ID, runtime ID, byte size, or SHA-256 differs;
- candidate release validation or `comparison.json` is missing, failed, or names another commit;
- a production artifact is writable, service-owned, hard-linked, or resolves into staging;
- the rollback copy differs from the currently served database baseline;
- a journal event or `transition-pending` requires recovery;
- the candidate cannot pass the complete loopback smoke suite;
- the current service/database differs from the Phase 2 baseline; or
- disk headroom cannot retain both complete releases.

File modification time is not provenance. Never place credentials in code, manifests, evidence, or
command output.

## Phase 2: stage and rehearse without changing the live service

1. Reconfirm the current service PID, working directory, executable, open DuckDB path, database
   SHA-256, unit/drop-ins, runtime package versions, candidate evidence, and free disk. Record the
   values without printing secret environment contents.

2. Install the Phase 1 operations package as an immutable root-owned tree and point the root-owned
   `production-ops/current` symlink at it. Record a SHA-256 tree fingerprint. This pointer is not used
   by the live service during Phase 2.

3. Create the control and artifact roots before bootstrap:

```bash
install -d -o root -g dataops -m 0750 /srv/cms-data-platform/production
install -d -o root -g dataops -m 0750 /srv/cms-data-platform/production-artifacts
install -d -o root -g dataops -m 0750 /srv/cms-data-platform/production-artifacts/code
install -d -o root -g dataops -m 0750 /srv/cms-data-platform/production-artifacts/runtimes
install -d -o root -g dataops -m 0750 /srv/cms-data-platform/production-artifacts/warehouses
```

4. Build separate rollback artifacts. Copy the current database to a new `.partial` file, fsync it,
   confirm a different device/inode from both the active database and staging, verify the approved
   SHA-256, atomically rename it, then seal its parent `0550` and file `0440` as `root:dataops`.
   Copy served code without `.env*`, data, logs, caches, or virtual environments. Build the rollback
   runtime at its final versioned path from the captured package lock; do not relocate an existing
   virtual environment. Rehearse the resulting rollback bundle before sealing code/runtime trees
   `0550`/`0440` or `0550` for executables.

5. Create candidate code and runtime artifacts in the same way. Copy the validated staging DuckDB to
   a distinct production inode, verify its byte size and release SHA-256, then seal it. Neither
   production database may be the active database, staging database, or a hard link to either.
   Record the serving-code and warehouse-pipeline commits independently: the warehouse release and
   comparison must agree on the pipeline commit, while the bounded API smoke suite proves that the
   selected serving commit is compatible with that immutable warehouse.

6. Bootstrap the rollback bundle. The production root must already exist and mutations run as root:

```bash
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  bootstrap \
  --production-root /srv/cms-data-platform/production \
  --artifact-root /srv/cms-data-platform/production-artifacts \
  --code-path /srv/cms-data-platform/production-artifacts/code/ROLLBACK_CODE_ID \
  --warehouse-path /srv/cms-data-platform/production-artifacts/warehouses/ROLLBACK_WAREHOUSE_ID/warehouse.duckdb \
  --warehouse-sha256 ROLLBACK_SHA256 \
  --runtime-path /srv/cms-data-platform/production-artifacts/runtimes/ROLLBACK_RUNTIME_ID \
  --dry-run --json
```

Repeat without `--dry-run`. This creates control-plane state only; the existing systemd unit still
serves its original paths.

7. Start the rollback bundle on an unused loopback port, using its code/runtime/database paths. Run
   the complete smoke suite with the bundle path so process identity is checked against that exact
   bundle:

```bash
/srv/cms-data-platform/production-artifacts/runtimes/ROLLBACK_RUNTIME_ID/bin/python \
  /srv/cms-data-platform/production-ops/current/pipeline/production_smoke.py \
  --base-url http://127.0.0.1:18080 \
  --deployment-id ROLLBACK_DEPLOYMENT_ID \
  --production-root /srv/cms-data-platform/production \
  --release-bundle /srv/cms-data-platform/production/releases/ROLLBACK_DEPLOYMENT_ID \
  --process-id ROLLBACK_REHEARSAL_PID \
  --api-key-env CMS_SMOKE_API_KEY \
  --expected-core-providers ROLLBACK_CORE_COUNT \
  --expected-hospital-affiliations ROLLBACK_AFFILIATION_COUNT \
  --expected-affiliated-providers ROLLBACK_AFFILIATED_PROVIDER_COUNT \
  --expected-raw-hospital-enrollments ROLLBACK_RAW_HOSPITAL_COUNT \
  --expected-aact-study-count ROLLBACK_AACT_STUDY_COUNT \
  --expected-aact-snapshot-date ROLLBACK_AACT_SNAPSHOT_DATE \
  --expected-table-counts ROLLBACK_RELEASE_JSON \
  --expected-industry-detail-status ROLLBACK_INDUSTRY_DETAIL_STATUS \
  --output /srv/cms-data-platform/production/evidence/ROLLBACK_DEPLOYMENT_ID/smoke.json

/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  verify \
  --production-root /srv/cms-data-platform/production \
  --deployment-id ROLLBACK_DEPLOYMENT_ID \
  --evidence /srv/cms-data-platform/production/evidence/ROLLBACK_DEPLOYMENT_ID/smoke.json \
  --json
```

Stop the temporary process without touching the live service.

8. Prepare the candidate; this validates the staging release and comparison evidence against the
   independent production copy:

```bash
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  prepare \
  --production-root /srv/cms-data-platform/production \
  --artifact-root /srv/cms-data-platform/production-artifacts \
  --data-root /srv/cms-data-platform/data \
  --code-path /srv/cms-data-platform/production-artifacts/code/CANDIDATE_COMMIT \
  --runtime-path /srv/cms-data-platform/production-artifacts/runtimes/CANDIDATE_RUNTIME_ID \
  --warehouse-path /srv/cms-data-platform/production-artifacts/warehouses/CANDIDATE_WAREHOUSE_ID/warehouse.duckdb \
  --warehouse-release-id WAREHOUSE_RELEASE_ID \
  --utilization-data-root /mnt/UTILIZATION_VOLUME/cms-data-utilization \
  --utilization-path /srv/cms-data-platform/production-artifacts/utilization/UTILIZATION_RELEASE_ID/utilization.duckdb \
  --utilization-release-id UTILIZATION_RELEASE_ID \
  --dry-run --json
```

Repeat without `--dry-run`, then start the prepared candidate bundle on a second unused loopback
port and run the same smoke command with candidate counts and `--release-bundle` pointing to the
prepared bundle. Stop the temporary process after it passes. Export `PYTHONDONTWRITEBYTECODE=1` for
every Python smoke, benchmark, or diagnostic command that imports from an immutable release checkout.
The smoke key must resolve to a named consumer authorized by `CMS_QUERY_CONSUMERS` (normally
`command-center`); the legacy shared key can prove normal routes but correctly receives `403` from
the bounded warehouse-count query. Load the named key from the protected environment into
`CMS_SMOKE_API_KEY` without printing it, and pass `--api-key-env CMS_SMOKE_API_KEY` to rehearsal and
cutover.

The three utilization arguments are all-or-none. Before `prepare`, independently run
`pipeline.utilization_releases verify`, copy the sidecar to a distinct production inode, confirm its
sealed manifest SHA-256 and byte size, and set the copy to `root:dataops` mode `0440`. The smoke
runner detects the bundle's `utilization` link automatically; a sidecar deployment cannot verify if
the process does not have that exact file open or any of the four utilization contract checks are
skipped.

Targeted release manifests must retain the baseline's query-authorized `smoke_table_counts` even
when their own `table_counts` contains only changed tables. Do not add raw or serving-only tables to
the arbitrary-SQL smoke count set merely to count them: the query boundary should continue to reject
those tables. Validate targeted changed-table counts from the sealed release/comparison evidence and
a separate direct read-only candidate check instead.
After sealing, check Git cleanliness with `GIT_OPTIONAL_LOCKS=0 git status --porcelain=v1`; a normal
status invocation may refresh `.git/index` and violate the no-writable-path invariant.

When systemd starts the isolated candidate, do not assume a transient `--setenv DUCKDB_PATH=...`
overrides a later `EnvironmentFile` value. Bind the candidate bundle's `DUCKDB_PATH` at `ExecStart`
after environment files have loaded. Before smoke, prove both the effective process environment from
`/proc/PID/environ` and candidate identity from `GET /release`.

9. Rehearse both transition directions without changing `release-current`:

```bash
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  activate --production-root /srv/cms-data-platform/production \
  --deployment-id CANDIDATE_DEPLOYMENT_ID --dry-run --json

/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  rollback --production-root /srv/cms-data-platform/production \
  --deployment-id CANDIDATE_DEPLOYMENT_ID --dry-run --json
```

Phase 2 ends with the old live unit untouched and `release-current` still selecting the verified
rollback bundle.

## Phase 3: controlled cutover

Immediately before changes, reconfirm the live PID, current database path/SHA-256, rollback artifact
hashes, candidate artifact hashes, free disk, and clean production journal. Any mismatch stops.

Run the retention and capacity preview from the selected immutable bundle, supplying only candidate
bytes that are not already allocated on this filesystem. Use the exact candidate byte size before
the independent production copy exists, and `0` after that immutable copy has been created and
verified. Counting an already allocated artifact again produces a false projection. Exit 1 blocks
promotion because the rollback floor or projected disk gate is not satisfied; exit 2 means the
preview itself could not prove a safe result. The command is read-only and has no delete mode.

```bash
cd /srv/cms-data-platform/production/release-current/code
PYTHONDONTWRITEBYTECODE=1 \
  /srv/cms-data-platform/production/release-current/runtime/bin/python -B \
  -m pipeline.retention preview \
  --platform-root /srv/cms-data-platform \
  --candidate-bytes ADDITIONAL_UNALLOCATED_CANDIDATE_BYTES \
  --json
```

Archive checksummed copies of the current systemd unit, environment-file metadata, and all drop-ins.
Inspect `systemctl cat cms-api.service`; remove or neutralize only the known obsolete AACT drop-in
after confirming the checked-in unit loads `/etc/aact/reader.env` directly. Install the checked-in
unit and non-secret environment file, then run `systemctl daemon-reload` without restarting.

The unit reads only:

- `release-current/code` as its working directory;
- `release-current/runtime/bin/python` as its executable; and
- `release-current/warehouse` as `DUCKDB_PATH`.

Its root-run `ExecStartPre` executes `production_manager.py startup-check`; a sentinel, blocking
journal event, ledger mismatch, or changed artifact prevents startup.

Run the one-shot cutover. It dry-validates and selects the candidate, restarts once, waits for the
loopback health endpoint, records the complete smoke suite, and verifies the candidate. If restart,
readiness, smoke, or verification fails, it atomically selects the predecessor, restarts, runs the
rollback counts, and verifies the rollback before returning exit code `1`.

The smoke base URL must be an exact loopback HTTP origin such as `http://127.0.0.1:8080`; a private
interface address is rejected before candidate selection even when it routes to the same service.

```bash
cd /srv/cms-data-platform/production-ops/current
PYTHONPATH=/srv/cms-data-platform/production-ops/current \
  /srv/cms-data-platform/production/release-current/runtime/bin/python \
  -m pipeline.production_cutover \
  --production-root /srv/cms-data-platform/production \
  --deployment-id CANDIDATE_DEPLOYMENT_ID \
  --api-key-env CMS_SMOKE_API_KEY \
  --candidate-core-providers CANDIDATE_CORE_COUNT \
  --candidate-hospital-affiliations CANDIDATE_AFFILIATION_COUNT \
  --candidate-affiliated-providers CANDIDATE_AFFILIATED_PROVIDER_COUNT \
  --candidate-raw-hospital-enrollments CANDIDATE_RAW_HOSPITAL_COUNT \
  --candidate-aact-study-count CANDIDATE_AACT_STUDY_COUNT \
  --candidate-aact-snapshot-date CANDIDATE_AACT_SNAPSHOT_DATE \
  --candidate-table-counts CANDIDATE_RELEASE_JSON \
  --rollback-core-providers ROLLBACK_CORE_COUNT \
  --rollback-hospital-affiliations ROLLBACK_AFFILIATION_COUNT \
  --rollback-affiliated-providers ROLLBACK_AFFILIATED_PROVIDER_COUNT \
  --rollback-raw-hospital-enrollments ROLLBACK_RAW_HOSPITAL_COUNT \
  --rollback-aact-study-count ROLLBACK_AACT_STUDY_COUNT \
  --rollback-aact-snapshot-date ROLLBACK_AACT_SNAPSHOT_DATE \
  --rollback-table-counts ROLLBACK_RELEASE_JSON \
  --rollback-industry-detail-status ROLLBACK_INDUSTRY_DETAIL_STATUS \
  --json
```

Do not declare success from `systemctl is-active` alone. Record the final selected deployment,
service PID, resolved code/runtime/database identity, smoke evidence path/hash, journal state, and
availability of the untouched other release.

When the candidate also includes a new AACT PostgreSQL snapshot, restore and validate it first with
`pipeline.data_platform stage-aact-database`; never point a rehearsal process at the active `aact`
database by accident. The temporary API must receive a candidate-only `AACT_DATABASE_URL` and pass
the same clinical-trials smoke check.

A combined cutover uses an API-stopped coherence boundary because PostgreSQL database rename and a
filesystem symlink replacement are not one cross-system atomic operation. Before stopping the API,
record the current `aact` study count, latest update date, exact bytes and SHA-256 of
`/srv/aact/CURRENT_SNAPSHOT`, and the validated candidate database name. Then:

1. Create `/srv/cms-data-platform/production/aact-transition-pending` as a root-owned `0640` regular
   file and fsync the production directory. The checked-in systemd unit refuses to start while this
   sentinel exists.
2. Stop the API and terminate only its remaining sessions to the current AACT database. Rename the
   current `aact` database to a unique rollback name; never drop it. Rename the validated versioned
   candidate database to `aact`.
3. Write the candidate snapshot date to a new marker file, fsync it, atomically replace
   `/srv/aact/CURRENT_SNAPSHOT`, and fsync `/srv/aact`. Select the DuckDB release bundle while the API
   remains stopped.
4. Recheck both selectors, remove and fsync the AACT transition sentinel, start the API once, and run
   the complete smoke suite with the exact DuckDB and AACT counts.

If smoke fails, recreate the sentinel before stopping the API, restore the previous DuckDB bundle,
rename the new `aact` database back to its versioned candidate name, rename the untouched rollback
database back to `aact`, atomically restore the previous snapshot marker, remove the sentinel, then
start and smoke the rollback. An interruption leaves the API stopped or startup-blocked; recovery
must inspect both database names, the marker, the bundle pointer, and the sentinel before taking any
action. PostgreSQL rename, marker replacement, and rollback commands remain approval-gated server
operations; `stage-aact-database` intentionally has no rename or drop capability.

## Interrupted transition recovery

The sentinel is written before the journal or pointer changes, and systemd will not start while it
exists. Inspect state first, then rehearse recovery:

```bash
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  recover --production-root /srv/cms-data-platform/production --dry-run --json
```

Actual recovery restores the complete recorded predecessor ledger and the one bundle pointer. After
recovery, restart and smoke the selected predecessor. Never edit the ledger, journal, or symlink by
hand and never copy a database over an existing artifact.

## Code-only serving deployment

> Added 2026-08-03 for the serving-contract upgrade (first use: serving commit
> `2427719ed9be17754e6e46258cd871b980a99c99`, which adds `GET /release` and release-keyed
> cache validators). Use this flow whenever new API code ships against the **unchanged**
> selected warehouse. It is the same prepare → rehearse → cutover mechanism with the
> data-refresh steps removed; there is still no in-place "update production" path.

**Consumer note:** a code-only deployment creates a new deployment ID, so `GET /release`
reports a new `release_id` and every `ETag` changes. Consumer caches keyed on
`release_id` + `representation_version` invalidate on the next revalidation — safe
over-invalidation, never stale data. Bump `representation_version` in `api/release_info.py`
in the same commit whenever the code changes any response shape.

1. Record current state and reusable targets:

```bash
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  status --production-root /srv/cms-data-platform/production --json

readlink /srv/cms-data-platform/production/releases/CURRENT_DEPLOYMENT_ID/warehouse
readlink /srv/cms-data-platform/production/releases/CURRENT_DEPLOYMENT_ID/runtime
```

Record `selected_deployment_id` (the rollback target) and `selected_warehouse_release_id`.
The staging data root must still retain `releases/WAREHOUSE_RELEASE_ID/` with its
`release.json` and `comparison.json`; prepare revalidates that evidence.

2. Create the candidate code artifact exactly as in Phase 2 step 5: a clean detached checkout
   of the approved full commit at `production-artifacts/code/CANDIDATE_COMMIT`, without
   `.env*`, data, logs, caches, or virtual environments, sealed `root:dataops`.

3. Reuse the selected runtime artifact only after proving the dependency set is unchanged
   between the served commit and the candidate commit:

```bash
git diff --stat SERVED_COMMIT..CANDIDATE_COMMIT -- api/requirements.txt
```

Any change means building a new versioned runtime artifact instead; never mutate the
existing one.

4. Prepare the candidate with the **current** warehouse and runtime targets and the new code
   path (dry-run first, then real). This is the Phase 2 step 8 command with
   `--warehouse-path`, `--warehouse-release-id`, and `--runtime-path` taken from step 1's
   recorded values. Record the returned `CANDIDATE_DEPLOYMENT_ID`.

5. Seal the deployment-scoped provenance snapshot. The warehouse is byte-identical to the
   predecessor's, so the predecessor's reconciled snapshot **is** the candidate's:

```bash
install -d -o root -g dataops -m 0750 \
  /srv/cms-data-platform/production/evidence/CANDIDATE_DEPLOYMENT_ID
install -o root -g dataops -m 0440 \
  /srv/cms-data-platform/production/evidence/CURRENT_DEPLOYMENT_ID/source-manifests.json \
  /srv/cms-data-platform/production/evidence/CANDIDATE_DEPLOYMENT_ID/source-manifests.json
```

6. Rehearse the prepared bundle on an unused loopback port as the production service account,
   with `PYTHONDONTWRITEBYTECODE=1` and Python's `-B` flag, then run the complete smoke suite
   with the candidate bundle and IDs. Running rehearsal as root can create writable
   `__pycache__` content inside an otherwise sealed artifact and invalidate its immutability;
   after stopping rehearsal, confirm the code artifact contains no `__pycache__` directories or
   `.pyc` files before transition dry-runs. Do not run standalone `production_manager.py verify`
   against the prepared candidate: verification is a selected-deployment state transition,
   and `production_cutover` performs it after selection. The warehouse is unchanged, so
   **candidate counts equal rollback counts** and both
   `--expected-table-counts` arguments point at the same
   `releases/WAREHOUSE_RELEASE_ID/release.json`. While the rehearsal process is up, also
   check the serving contract against the rehearsal port: `GET /release` must return the
   candidate deployment ID (the resolver derives it from the bundle directory), and a data
   GET must carry `ETag: "CANDIDATE_DEPLOYMENT_ID:REPRESENTATION_VERSION"`.

7. Reconfirm live PID, hashes, journal, sentinel, and disk as in Phase 3, then run the
   one-shot `pipeline.production_cutover` from the Phase 3 section with identical candidate
   and rollback count values.

8. Post-cutover serving-contract checks (in addition to the recorded smoke evidence; load
   the key from the environment file without printing it):

```bash
set -a; . /etc/cms-data/cms-api.env; set +a
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:8080/release
curl -fsSi -o /dev/null -D - -H "X-API-Key: $CMS_API_KEY" \
  http://127.0.0.1:8080/practices/capabilities | grep -i '^etag'
curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $CMS_API_KEY" \
  -H "If-None-Match: \"CANDIDATE_DEPLOYMENT_ID:1\"" \
  http://127.0.0.1:8080/practices/capabilities
```

Expect: `release_id` equal to the new deployment ID with `representation_version` `1`, an
`ETag` of `"CANDIDATE_DEPLOYMENT_ID:1"`, and `304` from the conditional request. Then the
one-time §8.5 check from the serving-contract proposal: if `/release` reports `promoted_at`
and `build.checksum` as `null`, `production/deployments.json` is not group-readable by the
service user — the endpoint stays correct via the bundle name, but record the finding and
decide (owner) between loosening that one file's group mode and stamping
`CMS_RELEASE_METADATA_PATH` at deploy time.

The one-shot flow runs smoke before it records manager verification. The release resolver caches
immutable release identity but, while a selected bundle still has no verification timestamp,
refreshes only `verified_at` from that same deployment's ledger. It refuses to refresh from a
repointed bundle, so an old process cannot absorb a new deployment's metadata during transition.
`GET /release` is non-cacheable operational metadata and does not use the immutable data-route ETag,
so clients cannot retain the pre-verification representation through a conditional `304`.
Older serving bundles may continue returning `verified_at: null` until their next normal code
deployment. Treat that as a recorded metadata discrepancy: prove the ledger state independently and
confirm release ID, artifact hashes, ETag, and route behavior. Do not add an unplanned restart solely
to refresh the informational field.

Rollback is unchanged: the cutover auto-selects and re-verifies the predecessor on any
required failure, and manual `rollback` restores the prior bundle pointer, which also
restores the prior `/release` identity.

## Recurring staging-to-production promotion

After the initial cutover, every refresh uses the same release mechanism; there is no in-place
"update production" path:

1. Classify the candidate before building it: targeted additive, source-family refresh, or full
   reconciliation. Record the triggering source runs, exact dependency closure, expected changed
   tables, comparison allowlist, validation gates, and resource budget. Do not select a full rebuild
   merely because it is the only available command.
2. Run publisher discovery. Proceed only for an explicit newer version and only after the
   source-specific gate in the operating model passes.
3. Acquire into a new immutable staging run, record the complete source manifest, and build a new
   DuckDB candidate from a checksum-verified production baseline copy. A targeted or source-family
   candidate loads and rebuilds only its declared dependency closure; a full reconciliation rebuilds
   the complete bundle.
4. Run source validation, complete-warehouse comparison, API contract tests, and the full temporary
   loopback smoke suite. Do not reuse evidence from another deployment.
5. Create separate immutable production code, runtime, and warehouse artifacts. Prepare a new
   deployment bundle while the live bundle remains selected.
6. Reconcile the candidate's source manifests to the contents of that exact warehouse. Write the
   resulting document as `root:dataops` mode `0440`, in a `root:dataops` mode `0750` directory, at
   `production/evidence/<candidate-deployment-id>/source-manifests.json`. Validate it with fixture
   status and, when publisher metadata is reachable, live status. Missing provenance stays unknown.
7. Reconfirm the selected release, hashes, journal, transition sentinel, rollback artifact, and disk
   headroom immediately before cutover.
8. Run `pipeline.production_cutover` once. It atomically selects the complete candidate bundle,
   restarts, creates fresh smoke evidence, and verifies. Any required failure selects, restarts, and
   smoke-tests the complete predecessor.
9. Retain the selected release and at least two prior validated releases. Prune only an explicitly
   identified superseded artifact after its hashes and rollback retention requirements are reviewed.

### Scoped-build resource guardrail

Build work must not starve the serving API or administrative access. Before a candidate starts,
reserve enough RAM, CPU, and disk for the selected API bundle, its predecessor, the candidate copy,
and any reporting export. Configure DuckDB's thread and memory limits plus a candidate-only spill
directory, or move the build to an isolated worker, when the host cannot preserve that reserve.

If a candidate causes sustained API, SSH, or reverse-proxy degradation, stop only the identified
candidate process, leave `release-current` untouched, and inspect the partial candidate before
retrying. Do not kill the API or overwrite a release to recover capacity. The interrupted candidate
remains unpromoted and must be rebuilt from the verified baseline after the resource plan is fixed.

For PPEF specifically, use `build-ppef-release` with the two same-period PPEF run IDs and the
checksum-verified production baseline manifest. Its defaults are `--memory-limit-gb 12 --threads 1`;
lower the memory limit when the serving reserve requires it, but do not raise either limit without a
new resource review. The full-platform command is not an acceptable operational substitute.

The daily `cms-data-status.timer` is advisory discovery monitoring. A stale result opens an operator
workflow; it does not authorize acquisition, candidate construction, restart, or promotion. Inspect
the latest structured result with:

```bash
systemctl show cms-data-status.service -p Result -p ExecMainStatus
journalctl -u cms-data-status.service -n 200 --no-pager
```

Before acquisition, save a fresh structured monitor result in the audit directory and generate the
read-only execution plan. The plan names exact acquisition order, validated run IDs, missing inputs,
candidate lanes, and any explicit exceptions. Current sources required only to complete an expanded
candidate are listed under `candidate_input_restore_ids`. The planner does not execute those actions.

```bash
mkdir -p /srv/cms-data-platform/audits/REFRESH_AUDIT_ID
/srv/cms-data-platform/production/release-current/runtime/bin/python \
  -m pipeline.production_status_monitor \
  --production-root /srv/cms-data-platform/production --json \
  > /srv/cms-data-platform/audits/REFRESH_AUDIT_ID/freshness-status.json

/srv/cms-data-platform/production/release-current/runtime/bin/python \
  -m pipeline.refresh_plan \
  --status-json /srv/cms-data-platform/audits/REFRESH_AUDIT_ID/freshness-status.json \
  --staging-manifest /srv/cms-data-platform/data/manifests.json \
  --json
```

Add `--exception 'SOURCE_ID=REVIEWED_REASON'` only for an approved intentional deferral and retain
that plan with the refresh evidence. Exit `2` means the inputs are malformed; exit `1` means source
provenance blocks planning; exit `0` means the plan is actionable, not that acquisition or promotion
has been authorized. Review every `awaiting_validated_runs` lane before running the named acquire
commands in publisher-period order.

### Retrospective source provenance

Use retrospective backfill only for retained legacy source artifacts that predate manifests. Keep
the evidence and outputs outside the selected deployment, use the canonical immutable warehouse
artifact rather than `release-current`, and load the AACT reader environment without printing it:

```bash
set -a
. /etc/aact/reader.env
set +a
python -m pipeline.provenance_backfill \
  --evidence /srv/cms-data-platform/audits/<audit-id>/evidence.json \
  --warehouse /srv/cms-data-platform/production-artifacts/warehouses/<release-id>/warehouse.duckdb \
  --existing-manifest /srv/cms-data-platform/production/evidence/<deployment-id>/source-manifests.json \
  --manifest-output /srv/cms-data-platform/audits/<audit-id>/source-manifests.candidate.json \
  --audit-output /srv/cms-data-platform/audits/<audit-id>/audit.json
```

Review `audit.json` and run fixture plus live status against the candidate manifest. Do not copy it
over the selected deployment's evidence. It may enter production only as sealed evidence belonging
to a newly prepared deployment whose exact warehouse hash matches the audit, followed by the normal
atomic cutover and rollback procedure.
