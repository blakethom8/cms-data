# Profile affiliations code-only deployment — 2026-08-11

> **Status: Phase 2 in progress — candidate prepared and provenance-confirmed; rehearsal,
> verification, and the approval-gated cutover remain. Cutover is NOT approved.**

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
| Candidate deployment ID | `deployment-20260811T041229Z-8eee0d7afd` (state: prepared) |
| Rollback target (selected, live) | `deployment-20260811T031052Z-73cea84b1b` (state: verified) |
| Served code commit at prep time | `fa4bcdd78ffc3ac3c60b2d63f7187035258a7417` |
| Warehouse (unchanged) | `warehouse-20260811T021837Z-f44c147e30` · sha256 `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2` · 20,569,665,536 bytes |
| Runtime (reused) | `runtime-candidate-8985e8a-c26024b3` — `git diff fa4bcdd..7285b4b -- api/requirements.txt` is empty |
| Code fingerprint (prepare echo) | `sha256:6f8fb93a15859e7fcd47d6c84066d9829381f1a492adb1546bde61a8da367ad6` |

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

## Remaining steps

Run in order. Each block is copy-paste safe; none touches the live service until the
approval-gated cutover in R7.

### R1 — seal the candidate provenance snapshot

```bash
install -d -o root -g dataops -m 0750 \
  /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd
install -o root -g dataops -m 0440 \
  /srv/cms-data-platform/production/evidence/deployment-20260811T023712Z-b68e0ca9c3/source-manifests.json \
  /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd/source-manifests.json
```

### R2 — start the rehearsal process on loopback 18080

Load env without printing it. The AACT reader env is required — the smoke suite's
clinical-trials check queries the AACT PostgreSQL database.

```bash
set -a; . /etc/cms-data/cms-api.env; . /etc/aact/reader.env; set +a
cd /srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd/code
DUCKDB_PATH=/srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd/warehouse \
  /srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd/runtime/bin/python \
  -m uvicorn api.main:app --host 127.0.0.1 --port 18080 &
REHEARSAL_PID=$!
for i in $(seq 1 60); do
  curl -fsS -o /dev/null http://127.0.0.1:18080/health -H "X-API-Key: $CMS_API_KEY" && break
  sleep 1
done
```

### R3 — full smoke suite + verify

Warehouse unchanged ⇒ candidate counts equal the rollback counts (values below are from
the selected deployment's verified smoke evidence and the warehouse release evidence).

```bash
/srv/cms-data-platform/production-artifacts/runtimes/runtime-candidate-8985e8a-c26024b3/bin/python \
  /srv/cms-data-platform/production-ops/current/pipeline/production_smoke.py \
  --base-url http://127.0.0.1:18080 \
  --deployment-id deployment-20260811T041229Z-8eee0d7afd \
  --production-root /srv/cms-data-platform/production \
  --release-bundle /srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd \
  --process-id $REHEARSAL_PID \
  --expected-core-providers 7395713 \
  --expected-hospital-affiliations 139775 \
  --expected-affiliated-providers 111881 \
  --expected-raw-hospital-enrollments 9175 \
  --expected-aact-study-count 594772 \
  --expected-aact-snapshot-date 2026-07-21 \
  --expected-table-counts /srv/cms-data-platform/data/releases/warehouse-20260811T021837Z-f44c147e30/release.json \
  --expected-industry-detail-status 200 \
  --output /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd/smoke.json

/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  verify --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T041229Z-8eee0d7afd \
  --evidence /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd/smoke.json \
  --json
```

### R4 — serving-contract checks against the rehearsal port

```bash
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:18080/release
# expect: release_id deployment-20260811T041229Z-8eee0d7afd, representation_version 2,
#         non-empty source_vintages (proves R1 worked)
curl -fsSi -o /dev/null -D - -H "X-API-Key: $CMS_API_KEY" \
  http://127.0.0.1:18080/practices/capabilities | grep -i '^etag'
# expect: ETag: "deployment-20260811T041229Z-8eee0d7afd:2"
curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $CMS_API_KEY" \
  -H 'If-None-Match: "deployment-20260811T041229Z-8eee0d7afd:2"' \
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
  --deployment-id deployment-20260811T041229Z-8eee0d7afd --dry-run --json
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  rollback --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T041229Z-8eee0d7afd --dry-run --json
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
  --deployment-id deployment-20260811T041229Z-8eee0d7afd \
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
`production_manager.py rollback --production-root /srv/cms-data-platform/production --deployment-id deployment-20260811T041229Z-8eee0d7afd --json`
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

## Stop conditions (from the runbook, instantiated)

Stop and report — do not improvise — if any of these holds:

- `production_manager.py status` is unhealthy, reports blocking transactions, or
  `release-current` no longer selects `deployment-20260811T031052Z-73cea84b1b`;
- a `transition-pending` sentinel exists;
- the candidate bundle's code/runtime/warehouse links do not resolve to the identities in
  the table above;
- any smoke check fails or `verify` does not mark the candidate verified;
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
