# Profile affiliations code-only deployment — 2026-08-11

> **Status: COMPLETE — recovery deployment `deployment-20260811T135930Z-2c3fb4878d`
> was selected and verified on 2026-08-11 after Blake's explicit approval. The intact
> predecessor `deployment-20260811T031052Z-73cea84b1b` remains available for rollback.**

## Background — what this ships and why

Provider Search's dossier (`/provider?npi=…`) shows an Access lens built from
`GET /profiles/{npi}`. Until `81fcf37`, that response exposed only DAC-derived group rows
and locations, which under-reports a clinician's real footprint. Motivating case, NPI
`1154580017` (Trevan Fischer, MD — general surgery, Los Angeles):

- DAC publishes exactly one door (Cedars-Sinai Medical Care Foundation, 11800 Wilshire
  Blvd) — verified `row_count: 1, truncated: false` against the live warehouse;
- Medicare reassignment carries **three** groups (Cedars-Sinai 2,066; Providence Saint
  Johns 456; SCPMG 13,886), two of which were invisible to the product;
- DAC facility affiliations carry **two** hospitals (CCNs 050290, 050069 → Saint John's
  Health Center, Providence St. Joseph), also invisible;
- every activity signal (Medicare billing ZIP, Part D, Open Payments) points at Santa
  Monica — the door the product did not show.

`81fcf37` (already on `main`, tested) expands the read-only profile response:

- `groups` becomes DAC ∪ reassignment per PAC ID with provenance
  (`sources`: `dac + reassignment` | `dac` | `reassignment`), DAC figures
  (`group_size`, `n_addresses`) and `reassignment_size` carried separately,
  door-bearing groups sorted first;
- new `hospital_affiliations`: DAC facility affiliations left-joined to
  `raw_hospital_general_info` (facility_type, ccn, facility_name, city, state);
  unresolved CCNs keep their row.

All four source tables (`raw_dac_national`, `raw_reassignment`,
`raw_dac_facility_affiliations`, `raw_hospital_general_info`) already exist in the
selected production warehouse — confirmed by live queries on 2026-08-10. **No warehouse
change is needed or permitted for this deployment.** This is exactly the runbook's
"Code-only serving deployment" case (`production-promotion-runbook.md`).

`7285b4b` bumps `representation_version` 1 → 2 as the serving contract requires for a
response-shape change. The endpoint is untyped in OpenAPI, so the snapshot gate could not
see the change; the bump follows doctrine, and post-deploy ETags become
`"<deployment-id>:2"`.

The downstream consumer is already merged and back-compatible: provider-search's Access
lens renders the new fields when present and older payloads unchanged
(`web/src/__tests__/accessLensAffiliations.test.tsx`). Deploy order is therefore free.

## Candidate identity

| Item | Value |
| --- | --- |
| Candidate code commit | `7285b4bab8969bcbd3cd4b00415149f15756bc8d` (origin/main) |
| Candidate deployment ID | `deployment-20260811T050116Z-3eb99c92bf` (state: prepared) |
| Rollback target (selected, live) | `deployment-20260811T031052Z-73cea84b1b` (state: verified) |
| Served code commit at prep time | `fa4bcdd78ffc3ac3c60b2d63f7187035258a7417` |
| Warehouse (unchanged) | `warehouse-20260811T021837Z-f44c147e30` · sha256 `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2` · 20,569,665,536 bytes |
| Runtime (reused) | `runtime-candidate-8985e8a-c26024b3` — `git diff fa4bcdd..7285b4b -- api/requirements.txt` is empty |
| Code artifact | `production-artifacts/code/7285b4bab8969bcbd3cd4b00415149f15756bc8d-safe-rehearsal1` |
| Code fingerprint (prepare echo) | `sha256:c11716672cb61387eb8644f72f043ea5eede420cf351473bfd04bf572c4ddb04` |

Local verification before any box work: `363 passed, 1 skipped` from `api/` at `7285b4b`.

## Completed steps (live service untouched throughout)

1. Live pre-state recorded: PID 3091528 (started 2026-08-11 03:16:00 UTC), cwd =
   `fa4bcdd` code artifact, open DuckDB = the selected warehouse artifact; control plane
   healthy, 0 blocking transactions, no sentinel, pointer matches ledger; loopback
   rehearsal port 18080 free; ~80 G disk free.
2. Candidate code artifact created and sealed: clean detached checkout of `7285b4b` at
   `production-artifacts/code/7285b4bab8969bcbd3cd4b00415149f15756bc8d`, `git status`
   clean, no `.env*`/data/logs/caches/venvs, `root:dataops`, read-only — matching the
   served `fa4bcdd` artifact's convention.
3. `prepare --dry-run` then real `prepare` succeeded →
   `deployment-20260811T041229Z-8eee0d7afd`, state `prepared`, previous
   `deployment-20260811T031052Z-73cea84b1b`.
4. Provenance-source identity confirmed:
   `releases/deployment-20260811T023712Z-b68e0ca9c3/warehouse` and the candidate's
   `warehouse` link both resolve to
   `production-artifacts/warehouses/warehouse-20260811T021837Z-f44c147e30/warehouse.duckdb`,
   so the 02:37Z predecessor's reconciled `source-manifests.json` is valid for the
   candidate (step R1 below copies it).

## Finding recorded in passing

`production/evidence/deployment-20260811T031052Z-73cea84b1b/` (the **selected**
deployment) contains only `smoke.json` — its `source-manifests.json` was never sealed, so
`GET /release` currently serves `source_vintages: {}`. This deployment's candidate
evidence copies from the 02:37Z predecessor instead and fixes the observable gap for the
next release. No action against the selected deployment is required or authorized.

## R1–R3 attempt and stop record — 2026-08-11 04:36 UTC

The required preconditions matched this document before rehearsal:

- `production_manager.py status` reported `healthy: true`, `control_plane_healthy: true`,
  `blocking_transactions: 0`, `pointer_matches_ledger: true`, and no transition sentinel;
- `release-current` still selected
  `deployment-20260811T031052Z-73cea84b1b`, with live PID `3091528` active;
- candidate code, runtime, and warehouse links resolved to the identities in the candidate
  table above; port 18080 was free; and the host retained approximately 80 GiB free.

R1 sealed the candidate provenance snapshot as `root:dataops` mode `0440`, 32,909 bytes.
Its SHA-256 matched the reconciled predecessor snapshot exactly:

```text
fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244
```

R2 started candidate rehearsal PID `3106172` on `127.0.0.1:18080`. Health returned:

```json
{"status":"ok","core_providers":7395713}
```

R3 smoke passed all 15 canonical checks with the documented counts:

```text
Production smoke: passed
Evidence: /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd/smoke.json
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

The smoke file is `root:root` mode `0440`, 3,497 bytes, with SHA-256:

```text
fe4f0c2bb9fcace05bc12c3f299f6f43019af719303b1d74823cdea94d56bd26
```

The immediately following documented `production_manager.py verify` command returned exit
code `2` instead of marking the candidate verified:

```json
{"state": "error", "error_summary": "Verification target is not selected"}
```

This is a stop condition. The checked-in manager's `_verification_validation` explicitly
requires the verification target to be the deployment currently selected by
`release-current`; the candidate is correctly still only `prepared`. No alternative flag or
undocumented command was substituted. R4–R6 were not run during that attempt. Rehearsal PID `3106172` was stopped,
port 18080 was released, and a post-stop status check confirmed the healthy live selection is
still `deployment-20260811T031052Z-73cea84b1b` with zero blocking transactions and no sentinel.

Before resuming, reconcile the working procedure's pre-cutover verification requirement with
the manager's selected-only verification state machine. Do not treat the passing smoke file as
cutover approval.

Blake authorized that procedure correction after reviewing the stop evidence. The corrected
code-only flow treats the complete candidate smoke suite as the pre-selection evidence and leaves
the selected-deployment `verify` transition to `production_cutover`, which performs it after
selection. This does not authorize R7 or any production selection change.

## R4–R6 resumed attempt and second stop record — 2026-08-11 04:45 UTC

Preconditions were reconfirmed before resuming: production was healthy with zero blocking
transactions, no sentinel, PID `3091528` still active, `release-current` still selecting
`deployment-20260811T031052Z-73cea84b1b`, and port 18080 free. The same immutable candidate
bundle started as rehearsal PID `3108460` and returned the expected health response.

R4 passed all serving-contract expectations. `GET /release` returned:

```text
release_id: deployment-20260811T041229Z-8eee0d7afd
representation_version: 2
source_vintages: 18 non-empty entries
build.checksum: 91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2
build.warehouse_release_id: warehouse-20260811T021837Z-f44c147e30
```

The validator checks returned exactly:

```text
etag: "deployment-20260811T041229Z-8eee0d7afd:2"
304
```

R5 passed both fixed feature cases:

```text
[('CEDARS-SINAI MEDICAL CARE FOUNDATION', 'dac + reassignment'),
 ('Southern California Permanente Medical Group', 'reassignment'),
 ('Providence Saint Johns Medical Foundation', 'reassignment')]
[('PROVIDENCE ST. JOSEPH HOSPITAL', '050069'),
 ("SAINT JOHN'S HEALTH CENTER", '050290')]
2 groups, 4 hospitals
```

Rehearsal PID `3108460` then stopped cleanly and port 18080 was released. The R6 activate
dry-run returned exit code `2`:

```json
{"state": "error", "error_summary": "code target contains a writable path: /srv/cms-data-platform/production-artifacts/code/7285b4bab8969bcbd3cd4b00415149f15756bc8d/api/__pycache__"}
```

Read-only inspection found a root-owned mode `0755` `api/__pycache__` directory containing
18 root-owned mode `0644` `.pyc` files, all timestamped `2026-08-11 04:35:16 UTC`, when the
first root-run rehearsal imported the API. The documented R2 command ran Python as root from
the sealed artifact, allowing Python to create bytecode despite the artifact's ordinary mode
bits. This invalidates the candidate's immutability check. Nothing was deleted, resealed, or
changed in place, and the rollback dry-run was not attempted.

The candidate deployment and code artifact must now be treated as contaminated and retained
only as failed evidence. Resume requires a fresh versioned code artifact and prepared deployment,
followed by rehearsal as the service user with bytecode writes disabled. Re-run smoke, R4, R5,
and both R6 dry-runs against the new deployment ID. Do not repair or reuse the existing artifact.

A post-stop check confirmed production remains healthy on
`deployment-20260811T031052Z-73cea84b1b`, with zero blocking transactions, no sentinel, and
port 18080 free.

## Replacement candidate and completed R1–R6 — 2026-08-11 05:07 UTC

Blake authorized creation of a fresh immutable artifact and repetition of the pre-cutover
checks. The contaminated artifact and deployment were left intact as failed evidence; neither
was repaired or reused.

A clean, detached, no-hardlink checkout of the exact approved commit was created at
`production-artifacts/code/7285b4bab8969bcbd3cd4b00415149f15756bc8d-safe-rehearsal1`.
It contained no environment files, data, databases, virtual environments, logs, Python bytecode,
or cache directories, and was sealed `root:dataops`. Prepare dry-run and real prepare succeeded,
creating `deployment-20260811T050116Z-3eb99c92bf` with the unchanged runtime and warehouse.
The resulting code fingerprint is
`sha256:c11716672cb61387eb8644f72f043ea5eede420cf351473bfd04bf572c4ddb04`.

R1 copied and sealed the reconciled provenance snapshot as `root:dataops` mode `0440`,
32,909 bytes. Its SHA-256 remained:

```text
fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244
```

R2 ran rehearsal PID `3111481` as `dataops`, with `PYTHONDONTWRITEBYTECODE=1` and Python's
`-B` flag. Health returned `{"status":"ok","core_providers":7395713}`. R3 then passed all
15 canonical smoke checks:

```text
Production smoke: passed
Evidence: /srv/cms-data-platform/production/evidence/deployment-20260811T050116Z-3eb99c92bf/smoke.json
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

The smoke file is `root:root` mode `0440`, 3,513 bytes, with SHA-256:

```text
7aa667808617f481bba582515bd5f4b2b421938ba86b52d5c659c2767adb0409
```

No standalone verify was run: as corrected above, verification is selected-only and belongs
to the approval-gated one-shot cutover. R4 returned the four serving-contract results:

```text
release_id: deployment-20260811T050116Z-3eb99c92bf
representation_version: 2
source_vintages: 18 non-empty entries
build.checksum: 91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2
build.warehouse_release_id: warehouse-20260811T021837Z-f44c147e30
etag: "deployment-20260811T050116Z-3eb99c92bf:2"
304
```

R5 returned the exact expected feature evidence:

```text
[('CEDARS-SINAI MEDICAL CARE FOUNDATION', 'dac + reassignment'), ('Southern California Permanente Medical Group', 'reassignment'), ('Providence Saint Johns Medical Foundation', 'reassignment')]
[('PROVIDENCE ST. JOSEPH HOSPITAL', '050069'), ("SAINT JOHN'S HEALTH CENTER", '050290')]
2 groups, 4 hospitals
```

The rehearsal stopped cleanly, port 18080 was released, and a scan confirmed the fresh sealed
artifact still contained zero `__pycache__` or `.pyc` paths. R6 activate dry-run returned the
replacement deployment in `prepared` state with `error_summary: null`, the code fingerprint
above, and the unchanged warehouse identity. Rollback dry-run returned exit code `0` and the
currently selected verified predecessor `deployment-20260811T031052Z-73cea84b1b` with
`error_summary: null`.

The final pre-gate status is healthy with `blocking_transactions: 0`,
`control_plane_healthy: true`, `pointer_matches_ledger: true`, and no transition sentinel.
`release-current` still selects `deployment-20260811T031052Z-73cea84b1b`; port 18080 is free.

## Validated steps and remaining gate

The blocks below preserve the exact commands used for R1–R6 and the command reserved for R7.
R1–R6 are complete; none touched the live service. R7 remains approval-gated.

### R1 — seal the candidate provenance snapshot

```bash
install -d -o root -g dataops -m 0750 \
  /srv/cms-data-platform/production/evidence/deployment-20260811T050116Z-3eb99c92bf
install -o root -g dataops -m 0440 \
  /srv/cms-data-platform/production/evidence/deployment-20260811T023712Z-b68e0ca9c3/source-manifests.json \
  /srv/cms-data-platform/production/evidence/deployment-20260811T050116Z-3eb99c92bf/source-manifests.json
```

### R2 — start the rehearsal process on loopback 18080

Load env without printing it. The AACT reader env is required — the smoke suite's
clinical-trials check queries the AACT PostgreSQL database.

```bash
set -a; . /etc/cms-data/cms-api.env; . /etc/aact/reader.env; set +a
cd /srv/cms-data-platform/production/releases/deployment-20260811T050116Z-3eb99c92bf/code
PYTHONDONTWRITEBYTECODE=1 \
DUCKDB_PATH=/srv/cms-data-platform/production/releases/deployment-20260811T050116Z-3eb99c92bf/warehouse \
  /usr/bin/setpriv --reuid=dataops --regid=dataops --init-groups \
  /srv/cms-data-platform/production/releases/deployment-20260811T050116Z-3eb99c92bf/runtime/bin/python \
  -B -m uvicorn api.main:app --host 127.0.0.1 --port 18080 &
REHEARSAL_PID=$!
for i in $(seq 1 60); do
  curl -fsS -o /dev/null http://127.0.0.1:18080/health -H "X-API-Key: $CMS_API_KEY" && break
  sleep 1
done
```

### R3 — full pre-selection smoke suite

Warehouse unchanged ⇒ candidate counts equal the rollback counts (values below are from
the selected deployment's verified smoke evidence and the warehouse release evidence).

```bash
/srv/cms-data-platform/production-artifacts/runtimes/runtime-candidate-8985e8a-c26024b3/bin/python \
  /srv/cms-data-platform/production-ops/current/pipeline/production_smoke.py \
  --base-url http://127.0.0.1:18080 \
  --deployment-id deployment-20260811T050116Z-3eb99c92bf \
  --production-root /srv/cms-data-platform/production \
  --release-bundle /srv/cms-data-platform/production/releases/deployment-20260811T050116Z-3eb99c92bf \
  --process-id $REHEARSAL_PID \
  --expected-core-providers 7395713 \
  --expected-hospital-affiliations 139775 \
  --expected-affiliated-providers 111881 \
  --expected-raw-hospital-enrollments 9175 \
  --expected-aact-study-count 594772 \
  --expected-aact-snapshot-date 2026-07-21 \
  --expected-table-counts /srv/cms-data-platform/data/releases/warehouse-20260811T021837Z-f44c147e30/release.json \
  --expected-industry-detail-status 200 \
  --output /srv/cms-data-platform/production/evidence/deployment-20260811T050116Z-3eb99c92bf/smoke.json

```

Do not run standalone `production_manager.py verify` here. The manager only verifies the
currently selected deployment; the approval-gated `production_cutover` command owns candidate
selection followed by verification.

### R4 — serving-contract checks against the rehearsal port

```bash
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:18080/release
# expect: release_id deployment-20260811T050116Z-3eb99c92bf, representation_version 2,
#         non-empty source_vintages (proves R1 worked)
curl -fsSi -o /dev/null -D - -H "X-API-Key: $CMS_API_KEY" \
  http://127.0.0.1:18080/practices/capabilities | grep -i '^etag'
# expect: ETag: "deployment-20260811T050116Z-3eb99c92bf:2"
curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $CMS_API_KEY" \
  -H 'If-None-Match: "deployment-20260811T050116Z-3eb99c92bf:2"' \
  http://127.0.0.1:18080/practices/capabilities
# expect: 304
```

### R5 — feature checks (the point of the deployment)

```bash
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:18080/profiles/1154580017 \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); \
print([ (g["group_name"], g["sources"]) for g in d["groups"] ]); \
print([ (h["facility_name"], h["ccn"]) for h in d["hospital_affiliations"] ])'
# expect 3 groups: Cedars-Sinai (dac + reassignment), SCPMG (reassignment),
#   Providence Saint Johns (reassignment) — door-bearing group first
# expect 2 hospitals: SAINT JOHN'S HEALTH CENTER (050290),
#   PROVIDENCE ST. JOSEPH HOSPITAL (050069)

curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:18080/profiles/1881985521 \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); \
print(len(d["groups"]), "groups,", len(d["hospital_affiliations"]), "hospitals")'
# expect 2 groups, 4 hospitals (Duc Do — UCLA + County of LA; regression case)

kill $REHEARSAL_PID
sleep 2; ss -ltn | grep 18080 || echo "rehearsal stopped, port released"
```

### R6 — transition rehearsals (no selection change)

```bash
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  activate --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T050116Z-3eb99c92bf --dry-run --json
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  rollback --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T050116Z-3eb99c92bf --dry-run --json
```

### R7 — ⛔ APPROVAL GATE, then the one-shot cutover

**Stop here. Present the R3–R6 evidence to Blake and obtain his explicit approval before
running the command below.** Passing rehearsal does not imply approval.

Immediately before cutover, reconfirm: live PID unchanged, no sentinel, clean journal,
`release-current` still `deployment-20260811T031052Z-73cea84b1b`, disk headroom intact.

```bash
cd /srv/cms-data-platform/production-ops/current
PYTHONPATH=/srv/cms-data-platform/production-ops/current \
  /srv/cms-data-platform/production/release-current/runtime/bin/python \
  -m pipeline.production_cutover \
  --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T050116Z-3eb99c92bf \
  --candidate-core-providers 7395713 \
  --candidate-hospital-affiliations 139775 \
  --candidate-affiliated-providers 111881 \
  --candidate-raw-hospital-enrollments 9175 \
  --candidate-aact-study-count 594772 \
  --candidate-aact-snapshot-date 2026-07-21 \
  --candidate-table-counts /srv/cms-data-platform/data/releases/warehouse-20260811T021837Z-f44c147e30/release.json \
  --rollback-core-providers 7395713 \
  --rollback-hospital-affiliations 139775 \
  --rollback-affiliated-providers 111881 \
  --rollback-raw-hospital-enrollments 9175 \
  --rollback-aact-study-count 594772 \
  --rollback-aact-snapshot-date 2026-07-21 \
  --rollback-table-counts /srv/cms-data-platform/data/releases/warehouse-20260811T021837Z-f44c147e30/release.json \
  --rollback-industry-detail-status 200 \
  --json
```

The cutover auto-selects, restarts, smoke-tests, and re-verifies the predecessor on any
required failure. Manual rollback afterward, if ever needed:
`production_manager.py rollback --production-root /srv/cms-data-platform/production --deployment-id deployment-20260811T050116Z-3eb99c92bf --json`
(then confirm the predecessor serves and re-runs smoke).

### R8 — post-cutover checks and record

```bash
set -a; . /etc/cms-data/cms-api.env; set +a
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:8080/release
# expect the candidate deployment ID, representation_version 2, non-empty source_vintages
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:8080/profiles/1154580017 \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["groups"]), len(d["hospital_affiliations"]))'
# expect: 3 2
```

Then update this document: mark the status line complete, record the final selected
deployment, service PID, smoke evidence path + SHA-256, journal state, and the untouched
rollback bundle, following the pattern in `new-provider-radar-execution.md`. Commit as
`docs(platform): record profile-affiliations cutover` and push.

## R7–R8 completion evidence — 2026-08-11 05:11 UTC

Blake explicitly approved the production cutover of
`deployment-20260811T050116Z-3eb99c92bf`. The immediately preceding checks found the live
service healthy on PID `3091528`, zero blocking transactions, artifact integrity passed, no
transition sentinel, no warning-level journal entries since 05:00 UTC, 80 GiB free, the sealed
candidate still free of runtime cache paths, and `release-current` still selecting the expected
predecessor.

The exact documented R7 `pipeline.production_cutover` command returned exit code `0`:

```json
{
  "rollback_available": true,
  "selected_deployment_id": "deployment-20260811T050116Z-3eb99c92bf",
  "smoke_evidence": "/srv/cms-data-platform/production/evidence/deployment-20260811T050116Z-3eb99c92bf/smoke.json",
  "state": "promoted"
}
```

R8 confirmed the live serving contract:

```text
release_id: deployment-20260811T050116Z-3eb99c92bf
representation_version: 2
source_vintages: 18 non-empty entries
build.checksum: 91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2
build.warehouse_release_id: warehouse-20260811T021837Z-f44c147e30
etag: "deployment-20260811T050116Z-3eb99c92bf:2"
304
```

The live feature checks returned:

```text
[('CEDARS-SINAI MEDICAL CARE FOUNDATION', 'dac + reassignment'), ('Southern California Permanente Medical Group', 'reassignment'), ('Providence Saint Johns Medical Foundation', 'reassignment')]
[('PROVIDENCE ST. JOSEPH HOSPITAL', '050069'), ("SAINT JOHN'S HEALTH CENTER", '050290')]
2 groups, 4 hospitals
```

Final production state:

- selected and verified deployment: `deployment-20260811T050116Z-3eb99c92bf`;
- selected code commit: `7285b4bab8969bcbd3cd4b00415149f15756bc8d`;
- service: PID `3113223`, active/running, cwd resolves to the sealed replacement code artifact;
- warehouse: unchanged `warehouse-20260811T021837Z-f44c147e30` with SHA-256
  `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`;
- verified smoke evidence:
  `/srv/cms-data-platform/production/evidence/deployment-20260811T050116Z-3eb99c92bf/smoke.json`,
  `root:root` mode `0440`, 3,512 bytes, SHA-256
  `31d5c830bb38cde3d17ddbfc8daba98bf4902be814eb31b92edfec0af5987262`;
- manager: healthy, artifact integrity passed, zero blocking transactions, pointer matches
  ledger, selected state `verified`, verified at `2026-08-11T05:10:55+00:00`;
- journal: no warning-level entries since 05:00 UTC; transition sentinel absent;
- rollback: predecessor bundle `deployment-20260811T031052Z-73cea84b1b` remains intact with
  its original code, reused runtime, and unchanged warehouse links.

## Command Center preflight incident and recovery — 2026-08-11 13:51–14:03 UTC

While preparing the separately approved Command Center publication, a root-run read-only
authentication diagnostic imported `api/auth.py` from the selected code artifact. Python wrote
`api/__pycache__/auth.cpython-312.pyc`, violating the immutable-artifact invariant. The subsequent
API restart was correctly rejected by the systemd startup integrity guard. The exact original
`/etc/cms-data/cms-api.env` was restored from its root-only backup; the proposed scoped Command
Center key is not active. No warehouse, release ledger, or bundle pointer was edited by hand.

The first actual rollback invocation incorrectly included `--deployment-id`, which the manager
rejects outside dry-run; it made no state change. The documented actual rollback without that flag
then selected the intact predecessor `deployment-20260811T031052Z-73cea84b1b`. A fresh full smoke
run passed all 15 checks, and manager verification restored this state:

- manager healthy and control plane healthy; artifact integrity passed;
- selected state `verified` at `2026-08-11T13:53:05+00:00`;
- zero blocking transactions, pointer matches ledger, and no transition sentinel;
- `cms-api` active/running on PID `3208720` with
  `{"status":"ok","core_providers":7395713}`;
- rollback smoke evidence SHA-256
  `5ada62285772be8e64b433f82284a22a4c1858039f6a5065888652eae5b80a3a`.

Two unsuccessful replacement-artifact constructions were retained and never prepared:
`safe-recovery2` lacked Git metadata, and `safe-recovery3`/`safe-recovery4` were rejected during
dry-run because their checkout modes were not clean/immutable. `safe-recovery5` is a clean detached
checkout of exact commit `7285b4bab8969bcbd3cd4b00415149f15756bc8d`, sealed without changing
Git-tracked executable modes. Two identical prepare operations outlived their SSH client sessions
and created prepared audit records; only the first,
`deployment-20260811T135930Z-2c3fb4878d`, is the recovery candidate. The duplicate remains
unselected and must not be used.

Recovery R1–R6 evidence for `deployment-20260811T135930Z-2c3fb4878d`:

- unchanged warehouse `warehouse-20260811T021837Z-f44c147e30` and reused runtime
  `runtime-candidate-8985e8a-c26024b3`;
- provenance snapshot: 32,909 bytes, `root:dataops` mode `0440`, SHA-256
  `fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244`;
- all 15 production smoke checks passed; smoke SHA-256
  `2a7bc83a09afdb880b8e5fb6ceb12e1e1668cf9ce5ddc0eacecd80074922c049`;
- `/release`: candidate ID, representation version 2, 18 source-vintage entries, and unchanged
  warehouse checksum; ETag `"deployment-20260811T135930Z-2c3fb4878d:2"`; conditional GET `304`;
- fixed features: Fischer returned 3 groups / 2 hospitals and Do returned 2 groups / 4 hospitals;
- rehearsal stopped, port 18080 released, and the sealed artifact retained zero bytecode paths;
- activate and rollback dry-runs both returned exit code 0 with `error_summary: null`.

This is a new deployment ID, so the earlier approval does not authorize its selection. Stop here
and obtain Blake's explicit approval before running a one-shot cutover targeting
`deployment-20260811T135930Z-2c3fb4878d`. Dashboard publication remains paused until the affiliation
API is restored and verified.

### Recovery R7–R8 completion evidence — 2026-08-11 14:11 UTC

Blake explicitly approved production cutover to
`deployment-20260811T135930Z-2c3fb4878d`. The first invocation stopped before mutation because
`CMS_API_KEY` was not loaded; it returned
`API key environment variable is empty: CMS_API_KEY`. After loading the protected CMS API and AACT
reader environments without displaying them, the one-shot cutover returned exit code 0:

```json
{
  "rollback_available": true,
  "selected_deployment_id": "deployment-20260811T135930Z-2c3fb4878d",
  "smoke_evidence": "/srv/cms-data-platform/production/evidence/deployment-20260811T135930Z-2c3fb4878d/smoke.json",
  "state": "promoted"
}
```

R8 returned the expected release ID, representation version 2, 18 source-vintage entries, unchanged
warehouse checksum, ETag `"deployment-20260811T135930Z-2c3fb4878d:2"`, and a `304` conditional
round trip. The live feature checks again returned Fischer's 3 groups / 2 hospitals and Do's
2 groups / 4 hospitals.

Final recovery state:

- manager healthy and control plane healthy; selected state `verified` at
  `2026-08-11T14:10:55+00:00`; artifact integrity passed, zero blocking transactions, pointer
  matches ledger, and no transition sentinel;
- service PID `3213811`, active/running;
- selected code commit `7285b4bab8969bcbd3cd4b00415149f15756bc8d`;
- warehouse remains `warehouse-20260811T021837Z-f44c147e30` with checksum
  `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`;
- verified smoke evidence is `root:root` mode `0440`, 3,511 bytes, SHA-256
  `1f9c1b51569ff8af6fadd62ed09d0ebf87683fa426bf055ae5067d17c40b5828`;
- no warning-level `cms-api` journal entries during cutover;
- predecessor bundle `deployment-20260811T031052Z-73cea84b1b` remains intact for rollback.

## Stop conditions (from the runbook, instantiated)

Stop and report — do not improvise — if any of these holds:

- `production_manager.py status` is unhealthy, reports blocking transactions, or
  `release-current` no longer selects `deployment-20260811T031052Z-73cea84b1b`;
- a `transition-pending` sentinel exists;
- the candidate bundle's code/runtime/warehouse links do not resolve to the identities in
  the table above;
- any smoke check fails;
- the rehearsal `/release` does not report the candidate ID with
  `representation_version` 2;
- disk headroom cannot retain both complete releases.

A failed rehearsal leaves nothing to roll back — the live service was never touched. Kill
the rehearsal process, record the failure here, and stop.

## Downstream once live

`provider-search` needs no backend change (the medicare proxy is a passthrough). Its
Access lens renders the new fields and ships dark until the box serves them; older
payloads keep rendering. The dossier's Fischer case becomes the demo: three groups with
provenance labels and two named hospitals instead of one door.
