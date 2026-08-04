# Serving-contract upgrade — release identity, cache validators, scoped keys

**Status:** PROPOSED — implementation spec, ready to build
**Date:** 2026-08-04
**Authority:** `docs/data-platform-operating-model.md` governs discovery, manifests, validation,
promotion, and rollback; `AGENTS.md` governs conventions. This document adds serving-API
capability and changes neither.

> **Replaces** a withdrawn draft (`2026-08-target-architecture.md`, never committed). That draft
> was written with partial visibility into its consumer and several of its claims failed
> verification there; its cms-data section was directionally right and survives here as this
> narrower, self-contained spec. Everything an implementer needs is in this file — it deliberately
> links to nothing outside this repository.

## 1. Context and consumers

This API serves immutable, promoted DuckDB releases of public CMS data. Its consumers are
servers, not browsers:

| Consumer | How it calls | What it pins today |
|---|---|---|
| provider-search production | `/api/medicare/*` proxy → this API, `X-API-Key` header | `contract_version: 2` on practice-shaped responses (rejects mismatches with a 502) |
| provider-search development | same, via SSH tunnel (`localhost:9080`) | same |
| command-center dashboard | direct | none |

All consumers currently present **the same shared `X-API-Key`**, and data responses carry **no
cache validators** — every identical question is answered by a fresh DuckDB query, and no consumer
can tell which release answered it.

Releases themselves are already first-class in the *pipeline* layer — but in two distinct
tiers (corrected 2026-08-03 during in-repo verification; the original draft conflated them):
`pipeline/manifests.py` (`RunManifest.release_id`, `active_release_id`, `ManifestStore`) tracks
**source-run** releases, one per acquired source version. The **serving** release identity lives
in `pipeline/releases.py` (`WarehouseRelease`, with `release.json` beside each immutable
candidate) and `pipeline/production_manager.py` (the deployment ledger `deployments.json` and
the `release-current` bundle pointer, whose target directory name *is* the deployment ID). What
is missing is the last step: **the serving process cannot name the release it is serving.**
`api/main.py` resolves a bare `DUCKDB_PATH` and knows nothing else. (`api/operations.py` does
read manifest evidence at request time, but only source-run manifests — not the serving
release — and its default path is absent on the production box.)

Because releases are immutable — within one release, a given response cannot change — exposing
release identity makes consumer-side caching *correct by construction*: invalidation degenerates
to noticing that a pointer moved. That is the property this spec exploits. (Note the honest
framing: this API has no per-call licensing cost, so the win is latency, warehouse load, and
correctness — not dollars.)

## 2. The five changes

All five are **additive to a read-only serving API**. None touches the warehouse schema, the
pipeline, the active DuckDB file, or the promotion workflow.

### 2.1 `GET /release` — the release manifest endpoint

```json
{
  "release_id": "deployment-20260721T202014Z-28465a2bbf",
  "promoted_at": "2026-07-21T20:31:07Z",
  "representation_version": 1,
  "source_vintages": {"dac_national": "2026-06", "open_payments": "PY2024", "...": "..."},
  "build": {"checksum": "…", "pipeline_ref": "…"},
  "compatibility": "current"
}
```

- Served from the promotion metadata the production control plane already places on the box
  (verified 2026-08-03; no deploy-step change is needed): `DUCKDB_PATH` points at
  `production/release-current/warehouse`, which resolves into
  `production/releases/<deployment-id>/`, so the bundle directory name is the release
  (deployment) ID; `production/deployments.json` carries `warehouse_release_id`,
  `selected_at`, `verified_at`, and checksums; and
  `production/evidence/<deployment-id>/source-manifests.json` carries source vintages. If any
  of those are unreadable, the endpoint degrades honestly (partial fields or `503`) — never a
  guess. An explicit metadata-file override remains available for non-bundle deployments.
- `representation_version` is a serving-API constant (start at `1`): it names the **shape** of
  data responses, and is bumped whenever any endpoint's response shape changes. Data can change
  without code (a new release) and code can change without data (a deploy) — consumers need both
  dimensions.
- `compatibility` is free-text status (`current`, `superseded`, …) for operators; consumers key
  off `release_id` + `representation_version` only.
- Key-gated like every other data route. Cheap enough to poll: consumers will hit it daily and on
  their own deploys.

### 2.2 Cache validators on data responses

- Every data response carries `ETag: "<release_id>:<representation_version>"` (a strong
  validator; both parts, for the reason above) and a `Cache-Control` header permitting
  revalidation.
- A request with a matching `If-None-Match` returns **304 with no body and no DuckDB query**.
  That short-circuit is the entire point — implement it as shared middleware or dependency, not
  per-route.
- The existing `contract_version: 2` field inside practice-shaped payloads is orthogonal and
  **stays exactly as it is** — it is a payload-schema pin consumers validate after a 200;
  `representation_version` is a transport-level validator that avoids the 200 entirely.

### 2.3 Per-consumer scoped API keys

- Replace the single shared secret with named keys: `ps-prod`, `ps-dev`, `command-center`
  (extendable). An env-provided mapping is sufficient — no database, no auth framework.
- **Rotation with overlap:** each consumer name may temporarily have two valid keys, so a
  rotation is issue-new → migrate consumer → retire-old, with no simultaneous-break window.
  Revocation is removal from the mapping.
- Log the **key name** (never the value) with each request, and echo an inbound
  `X-Request-ID` header back in responses and logs so a consumer's request ids correlate across
  the wire.
- **Compatibility rule:** the current shared key keeps working until every consumer holds a
  scoped key; retiring it is the last step, coordinated by the owner, not the implementer.

### 2.4 Promotion notification — polling is the contract

Consumers learn of a new release by polling `GET /release` (daily and on their own deploys) and
by observing the ETag change. A push webhook is optional sugar for later; nothing may *depend* on
it, because a missed webhook must never strand a consumer on a stale pointer.

### 2.5 Serving-box invariants, written down

Add to `docs/data-platform-operating-model.md` (or a linked page) as explicit rules — these are
all true today by design; writing them down keeps a future change from silently breaking the
consumers now caching against them:

1. The serving process is read-only and holds **no publisher credentials**.
2. The serving box holds **zero client data** — worst-case compromise is a copy of public data.
3. Releases are immutable; the active production DuckDB is never modified in place.
4. The operator environment retains the **last N releases** off-box (natural disaster-recovery
   copy); rollback is repoint-to-previous-release, and the runbook says how.

## 3. Compatibility constraints (do not violate)

- **Additive only.** Existing consumers must work unchanged through every slice: same routes,
  same payloads, shared key still valid, `contract_version: 2` untouched.
- The serving process stays read-only; no API request handler writes anything, ever.
- Never modify the active production DuckDB in place.
- No credentials, data archives, DuckDB files, or release evidence in commits.
- Conventional commits, existing scoping (`feat(api): …`, `docs(platform): …`).
- Tests live beside `api/` modules and run from that directory (`cd api && pytest -q`).

## 4. Verify before building

These claims were established from the consumer side on 2026-08-03 by code inspection of this
repository. **Re-verify each in-repo before implementing** — if any is wrong, the plan changes
and the spec should be corrected rather than worked around:

1. One `check_api_key` dependency guards all secured surfaces (~13 mounts in `api/main.py`), and
   every consumer presents the same secret.
2. No `ETag`, `Cache-Control`, or conditional-request handling exists anywhere in `api/`.
3. The serving process performs no writes and needs no publisher credentials.
4. **Release identity is not currently reachable at request time** — `api/main.py` knows only
   `DUCKDB_PATH`. Confirm what promotion metadata the *deployed* box actually has adjacent to its
   DuckDB, and how the deploy step could stamp `release_id` where the API can read it. This is
   the load-bearing item: §2.1 and §2.2 both stand on it.
5. The only payload-level version pin any consumer relies on is `contract_version: 2` on
   practice-shaped responses.

### Verification results (in-repo, 2026-08-03)

1. **Confirmed.** One `check_api_key` (`api/main.py`) guards 12 router mounts plus the three
   direct data routes (`/query`, `/tables`, `/tables/{name}/schema`); a single shared
   `CMS_API_KEY` is the only secret. `/health` is unauthenticated and does query DuckDB —
   relevant precedent for §8 question 3.
2. **Confirmed.** No `ETag`, `Cache-Control`, or conditional-request handling anywhere in `api/`.
3. **Confirmed.** The DuckDB connection is opened `read_only=True`, `/query` rejects write
   statements, no handler writes files, and no publisher credentials exist (outbound third-party
   keys — Google Places, OpenAI, the AACT read-only Postgres URL — may be present in the service
   environment; they are not publisher credentials, but §2.5's invariant wording should not imply
   the box holds *zero* secrets beyond the API key).
4. **Confirmed, with two corrections folded into §1 and §2.1 above.** The serving release is
   unnameable at request time, but the promotion metadata is *already adjacent* on the deployed
   box via the `release-current` bundle structure, ledger, and evidence snapshot — no deploy-step
   change is required. The original draft's pointer to `pipeline/manifests.py` as the release
   layer was wrong (source-run tier, not serving tier). One residual unknown that cannot be
   proved from the repository: whether `production/deployments.json` (mode `0640`, control
   group) is group-readable by the `dataops` service user on the real box. S1 therefore treats
   ledger enrichment as optional and must never take control-plane locks.
5. **Confirmed, with a footnote.** `market_snapshot` payloads also carry a `contract_version: 1`
   field, but no consumer pins it; `practices.py` `CONTRACT_VERSION = 2` is the only relied-upon
   pin.

## 5. Deliberately not doing

Keep DuckDB; keep one serving box. No high availability, no replication, no schema/DDL changes,
no auth framework or gateway, no webhook-as-dependency, no per-consumer rate limiting (revisit if
a consumer misbehaves), no write paths of any kind. 163 MB of read-only data has headroom for
another 100× of growth, and retained-previous-release rollback already beats most HA setups.

## 6. What the primary consumer will build against this

So the implementer knows what the contract must hold up: provider-search intends to (a) poll
`/release` daily and on deploy; (b) send `If-None-Match` on data calls; (c) adopt `ps-prod` /
`ps-dev` keys when issued; (d) keep validating `contract_version: 2`; and (e) build a
release-keyed response cache whose keys include `release_id` **and** `representation_version` —
which is why any response-shape change without a `representation_version` bump would serve wrong
data from a correct cache. Treat that bump as non-optional, the same class of obligation as the
`contract_version` check.

## 7. Suggested slices (each additive, shippable, and independently revertable)

| Slice | Contents | Proof |
|---|---|---|
| **S1** | Release metadata reachable by the serving process + `GET /release` + tests | endpoint returns the active release on a box with promotion metadata; clean error (`503`, not a guess) without it |
| **S2** | ETag/`If-None-Match` middleware + `representation_version` constant + tests | 304 short-circuit verified to skip DuckDB; ETag stable within a release, changed across releases |
| **S3** | Scoped keys with overlap rotation + key-name logging + `X-Request-ID` echo | old shared key and new scoped keys valid simultaneously; log lines carry key name + request id |

> **S3 implementer note (added 2026-08-03, post-S2):** the auth predicate briefly existed in
> two places in `api/main.py` — the `check_api_key` dependency and the `is_authorized` lambda
> passed to `ReleaseCacheMiddleware` (needed so the 304 short-circuit cannot bypass
> key-gating). Scoped keys must change **both in lockstep**, so the shared predicate was
> extracted as S3's first step (done 2026-08-03: `api/auth.py`, both call sites rewired,
> lockstep covered by `api/test_auth.py`). The remainder of S3 — key mapping, overlap
> rotation, key-name logging, `X-Request-ID` echo — builds on `make_key_validator` and stays
> blocked on the §8 owner decisions.
| **S4** | Operating-model doc: invariants (§2.5), retention rule, rollback note | doc review |

S1 → S2 is the natural order (S2 needs release identity). S3 and S4 are independent of both.

## 8. Open questions for the owner

Findings recorded 2026-08-03 after S1/S2 implementation; items still needing an owner decision
are marked **OWNER**.

1. Retention: what is N for "retain the last N releases off-box"?
   *Finding:* the operating model already sets an on-box floor — lifecycle step 8 retains "the
   active and at least two previous validated releases", and verified baselines live under
   `backups/<backup-id>/`. **OWNER:** pick the off-box N (suggest N = 3 to match the on-box
   floor) and write it into the operating model with S4.
2. Key distribution: where do the scoped key values live on the consumer side, and who rotates
   them on what cadence?
   *Finding:* on the serving side the natural home already exists — the systemd unit loads
   secrets only from stable protected environment files (`/etc/cms-data/cms-api.env`), so the
   scoped-key mapping belongs there. The consumer side is invisible from this repository.
   **OWNER:** decide consumer-side storage and rotation cadence before S3 starts.
3. Should `/release` be reachable by an *unscoped* monitoring check (no key)?
   *Finding:* `/health` is already unauthenticated (and runs a DuckDB count), so an uptime
   probe needs no new surface; `/release` was implemented key-gated per the default here.
   **OWNER:** confirm key-gated stands.
4. When every consumer has scoped keys, who retires the shared key, and when?
   **OWNER:** unchanged; this is the explicit last step of §2.3 and is not an implementer call.

New questions raised by implementation:

5. **OWNER / one-time box check:** is `production/deployments.json` (mode `0640`, control
   group) group-readable by the `dataops` service user on the real box? The bundle-derived
   `release_id` needs only symlink traversal (which the service already has — it opens the
   warehouse through the same links), but `promoted_at`, checksums, and
   `warehouse_release_id` come from the ledger. If it is not readable, either loosen the group
   policy deliberately or have the deploy step stamp a `CMS_RELEASE_METADATA_PATH` JSON file;
   the endpoint already degrades to partial metadata rather than failing.
6. Conditional requests are honored on **GET only** (standard `If-None-Match` semantics);
   `POST /query` and any future POST data routes carry no validators. **OWNER/provider-search:**
   confirm all cache-worthy data calls are GETs, or say which POST responses need validators
   before the consumer cache is built.
7. Development serving (`ps-dev` tunnel): if the dev box serves a bare DuckDB path rather than
   a production bundle, `GET /release` returns `503` there by design. **OWNER:** either accept
   that in dev or stamp `CMS_RELEASE_METADATA_PATH` in the dev deploy.
8. ~~`representation_version` bump discipline: nothing mechanical enforces it.~~
   **Resolved 2026-08-04 by pinning response shapes per version.**
   `api/response_shapes/v<version>.json` records the resolved response schemas for each
   `representation_version`; `api/test_response_shapes.py` fails when an already-published
   operation's shape changes without a bump, and fails again until the bumped version gets
   its own snapshot. New operations are exempt — they cannot invalidate a cache nobody holds.
   Regenerate with `cd api && ../.venv/bin/python response_shapes.py --write`.
9. AACT and the ETag (raised 2026-08-04, post-deploy): clinical-trials responses are served
   from the separate AACT PostgreSQL database, which is *not* part of the DuckDB release the
   ETag names. Today this is sound — AACT swaps ride the combined-cutover coherence boundary
   with a restart and, in practice, a new deployment, so `release_id` rotates. But if AACT
   ever refreshes *independently* (the operating model's daily-refresh aspiration) without a
   new deployment, a matching `If-None-Match` on clinical-trials routes would 304 against
   changed data — stale answers from a correct cache. **OWNER:** before enabling independent
   AACT refreshes, either rule that every AACT swap rides a new deployment, or exclude
   clinical-trials routes from the cache validators.
