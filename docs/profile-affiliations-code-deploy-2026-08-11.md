# Profile affiliations code-only deployment — 2026-08-11

> **Status: Phase 2 in progress — stopped before rehearsal; cutover NOT approved**

Code-only serving deployment (per `production-promotion-runbook.md` §Code-only serving
deployment) to ship the expanded profile affiliation response (`81fcf37`) plus the
`representation_version` bump (`7285b4b`) against the **unchanged** selected warehouse.

## Candidate identity

| Item | Value |
| --- | --- |
| Candidate code commit | `7285b4bab8969bcbd3cd4b00415149f15756bc8d` (origin/main) |
| Candidate deployment ID | `deployment-20260811T041229Z-8eee0d7afd` (state: prepared) |
| Rollback target (selected) | `deployment-20260811T031052Z-73cea84b1b` (state: verified) |
| Served code commit at prep time | `fa4bcdd78ffc3ac3c60b2d63f7187035258a7417` |
| Warehouse (unchanged) | `warehouse-20260811T021837Z-f44c147e30` · sha256 `91e2ee4e…a345ef2` · 20,569,665,536 bytes |
| Runtime (reused) | `runtime-candidate-8985e8a-c26024b3` — `git diff fa4bcdd..7285b4b -- api/requirements.txt` is empty |
| Code fingerprint (dry-run echo) | `sha256:6f8fb93a15859e7fcd47d6c84066d9829381f1a492adb1546bde61a8da367ad6` |

Local verification before any box work: `363 passed, 1 skipped` from `api/` at `7285b4b`.

## Why representation_version 2

`81fcf37` changed the served `/profiles/{npi}` shape (group rows gained
`reassignment_size` and `sources`; payload gained `hospital_affiliations`) without a bump.
The endpoint is untyped in OpenAPI so the snapshot gate could not see it; `7285b4b`
records v2 per the serving-contract doctrine. Post-deploy ETags become
`"<deployment-id>:2"`.

## Completed steps (all without touching the live service)

1. Live pre-state recorded: PID 3091528 (started 03:16:00Z), cwd = `fa4bcdd` code artifact,
   open DuckDB = the selected warehouse artifact; control plane healthy, 0 blocking
   transactions, no sentinel; loopback rehearsal port 18080 free; 80 G disk free.
2. Candidate code artifact created and sealed: clean detached checkout of `7285b4b` at
   `production-artifacts/code/7285b4bab8969bcbd3cd4b00415149f15756bc8d`, no
   `.env*`/data/logs/caches/venvs, `root:dataops`, read-only (matches the `fa4bcdd`
   artifact convention).
3. `prepare --dry-run` then real `prepare` succeeded → `deployment-20260811T041229Z-8eee0d7afd`,
   state `prepared`, previous `deployment-20260811T031052Z-73cea84b1b`.

## Finding: selected deployment is missing its provenance snapshot

`production/evidence/deployment-20260811T031052Z-73cea84b1b/` contains only `smoke.json` —
no `source-manifests.json`. This is why `GET /release` serves `source_vintages: {}`. The
newest snapshot on the box belongs to `deployment-20260811T023712Z-b68e0ca9c3` (02:37Z).
Before copying it for the candidate, confirm that deployment served the same warehouse
(`readlink production/releases/deployment-20260811T023712Z-b68e0ca9c3/warehouse`).

## Remaining steps (operator commands, in order)

```bash
# 1. Confirm the 02:37Z predecessor served the same warehouse
readlink /srv/cms-data-platform/production/releases/deployment-20260811T023712Z-b68e0ca9c3/warehouse

# 2. Seal the candidate provenance snapshot from that reconciled predecessor
install -d -o root -g dataops -m 0750 \
  /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd
install -o root -g dataops -m 0440 \
  /srv/cms-data-platform/production/evidence/deployment-20260811T023712Z-b68e0ca9c3/source-manifests.json \
  /srv/cms-data-platform/production/evidence/deployment-20260811T041229Z-8eee0d7afd/source-manifests.json

# 3. Rehearse the prepared bundle on loopback 18080 (env loaded, never printed)
set -a; . /etc/cms-data/cms-api.env; . /etc/aact/reader.env; set +a
cd /srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd/code
DUCKDB_PATH=/srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd/warehouse \
  /srv/cms-data-platform/production/releases/deployment-20260811T041229Z-8eee0d7afd/runtime/bin/python \
  -m uvicorn api.main:app --host 127.0.0.1 --port 18080 &
REHEARSAL_PID=$!

# 4. Full smoke suite — warehouse unchanged, so candidate counts equal rollback counts
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

# 5. Serving-contract + feature checks against the rehearsal port (expect version 2)
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:18080/release
#   → release_id deployment-20260811T041229Z-8eee0d7afd, representation_version 2
curl -fsSi -o /dev/null -D - -H "X-API-Key: $CMS_API_KEY" \
  http://127.0.0.1:18080/practices/capabilities | grep -i '^etag'
#   → ETag: "deployment-20260811T041229Z-8eee0d7afd:2"
curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $CMS_API_KEY" \
  -H 'If-None-Match: "deployment-20260811T041229Z-8eee0d7afd:2"' \
  http://127.0.0.1:18080/practices/capabilities
#   → 304
curl -fsS -H "X-API-Key: $CMS_API_KEY" http://127.0.0.1:18080/profiles/1154580017 \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["groups"]), "groups;", len(d["hospital_affiliations"]), "hospitals")'
#   → 3 groups; 2 hospitals  (Fischer: Cedars-Sinai + SCPMG + Providence St Johns; St John's + Providence St Joseph)

kill $REHEARSAL_PID

# 6. Transition rehearsals (no selection change)
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  activate --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T041229Z-8eee0d7afd --dry-run --json
/usr/bin/python3 /srv/cms-data-platform/production-ops/current/pipeline/production_manager.py \
  rollback --production-root /srv/cms-data-platform/production \
  --deployment-id deployment-20260811T041229Z-8eee0d7afd --dry-run --json
```

## Approval gate

Cutover is one `pipeline.production_cutover` run with candidate counts equal to rollback
counts (identical values above, both `--*-table-counts` pointing at the same
`release.json`) and `--rollback-industry-detail-status 200`. **Do not run it without
Blake's explicit approval.** Rollback target `deployment-20260811T031052Z-73cea84b1b`
remains selected and untouched until then.

## Downstream once live

`provider-search` needs no backend change (the medicare proxy is a passthrough); its
Access lens renders the new fields and is already merged with tests
(`web/src/__tests__/accessLensAffiliations.test.tsx`). Older payloads keep rendering, so
deploy order is free.
