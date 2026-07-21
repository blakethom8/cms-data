# Production Cutover Runbook

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
  production-ops/
    <ops-id>/                            immutable control and smoke code
    current -> <ops-id>
  production/                            root:dataops 0750
    releases/<deployment-id>/            root:dataops 0550
      code -> production-artifacts/code/<code-id>
      runtime -> production-artifacts/runtimes/<runtime-id>
      warehouse -> production-artifacts/warehouses/<warehouse-id>/warehouse.duckdb
    release-current -> releases/<deployment-id>
    deployments.json
    deployment-journal.json
    transition-pending                   exists only during a pointer transaction
    evidence/<deployment-id>/smoke.json
```

`release-current` is the only serving selector. Activation and rollback replace that symlink once;
the three internal artifact links never change. The API user can read but cannot modify the control
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
  --expected-core-providers ROLLBACK_CORE_COUNT \
  --expected-hospital-affiliations ROLLBACK_AFFILIATION_COUNT \
  --expected-affiliated-providers ROLLBACK_AFFILIATED_PROVIDER_COUNT \
  --expected-raw-hospital-enrollments ROLLBACK_RAW_HOSPITAL_COUNT \
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
  --dry-run --json
```

Repeat without `--dry-run`, then start the prepared candidate bundle on a second unused loopback
port and run the same smoke command with candidate counts and `--release-bundle` pointing to the
prepared bundle. Stop the temporary process after it passes.

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

```bash
cd /srv/cms-data-platform/production-ops/current
PYTHONPATH=/srv/cms-data-platform/production-ops/current \
  /srv/cms-data-platform/production/release-current/runtime/bin/python \
  -m pipeline.production_cutover \
  --production-root /srv/cms-data-platform/production \
  --deployment-id CANDIDATE_DEPLOYMENT_ID \
  --candidate-core-providers CANDIDATE_CORE_COUNT \
  --candidate-hospital-affiliations CANDIDATE_AFFILIATION_COUNT \
  --candidate-affiliated-providers CANDIDATE_AFFILIATED_PROVIDER_COUNT \
  --candidate-raw-hospital-enrollments CANDIDATE_RAW_HOSPITAL_COUNT \
  --rollback-core-providers ROLLBACK_CORE_COUNT \
  --rollback-hospital-affiliations ROLLBACK_AFFILIATION_COUNT \
  --rollback-affiliated-providers ROLLBACK_AFFILIATED_PROVIDER_COUNT \
  --rollback-raw-hospital-enrollments ROLLBACK_RAW_HOSPITAL_COUNT \
  --rollback-industry-detail-status ROLLBACK_INDUSTRY_DETAIL_STATUS \
  --json
```

Do not declare success from `systemctl is-active` alone. Record the final selected deployment,
service PID, resolved code/runtime/database identity, smoke evidence path/hash, journal state, and
availability of the untouched other release.

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

## Recurring staging-to-production promotion

After the initial cutover, every refresh uses the same release mechanism; there is no in-place
"update production" path:

1. Run publisher discovery. Proceed only for an explicit newer version and only after the
   source-specific gate in the operating model passes.
2. Acquire into a new immutable staging run, record the complete source manifest, and build a new
   DuckDB candidate from a checksum-verified production baseline copy.
3. Run source validation, complete-warehouse comparison, API contract tests, and the full temporary
   loopback smoke suite. Do not reuse evidence from another deployment.
4. Create separate immutable production code, runtime, and warehouse artifacts. Prepare a new
   deployment bundle while the live bundle remains selected.
5. Reconcile the candidate's source manifests to the contents of that exact warehouse. Write the
   resulting document as `root:dataops` mode `0440`, in a `root:dataops` mode `0750` directory, at
   `production/evidence/<candidate-deployment-id>/source-manifests.json`. Validate it with fixture
   status and, when publisher metadata is reachable, live status. Missing provenance stays unknown.
6. Reconfirm the selected release, hashes, journal, transition sentinel, rollback artifact, and disk
   headroom immediately before cutover.
7. Run `pipeline.production_cutover` once. It atomically selects the complete candidate bundle,
   restarts, creates fresh smoke evidence, and verifies. Any required failure selects, restarts, and
   smoke-tests the complete predecessor.
8. Retain the selected release and at least two prior validated releases. Prune only an explicitly
   identified superseded artifact after its hashes and rollback retention requirements are reviewed.

The daily `cms-data-status.timer` is advisory discovery monitoring. A stale result opens an operator
workflow; it does not authorize acquisition, candidate construction, restart, or promotion. Inspect
the latest structured result with:

```bash
systemctl show cms-data-status.service -p Result -p ExecMainStatus
journalctl -u cms-data-status.service -n 200 --no-pager
```

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
