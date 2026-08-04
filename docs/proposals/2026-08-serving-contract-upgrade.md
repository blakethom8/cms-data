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

Releases themselves are already first-class in the *pipeline* layer: `pipeline/manifests.py`
defines `RunManifest` (with `release_id`), `PromotionState`, `active_release_id`, and a
`ManifestStore`. What is missing is the last step: **the serving process cannot name the release
it is serving.** `api/main.py` resolves a bare `DUCKDB_PATH` and knows nothing else.

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

- Served from the promotion metadata the pipeline already produces (`ManifestStore` /
  `active_release_id`). If the deployed box does not currently have that metadata adjacent to the
  DuckDB it serves, the deploy step must start placing it there — that is part of this change, not
  a blocker to route around.
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
| **S4** | Operating-model doc: invariants (§2.5), retention rule, rollback note | doc review |

S1 → S2 is the natural order (S2 needs release identity). S3 and S4 are independent of both.

## 8. Open questions for the owner

1. Retention: what is N for "retain the last N releases off-box"? (Pick and write it down.)
2. Key distribution: where do the scoped key values live on the consumer side, and who rotates
   them on what cadence?
3. Should `/release` be reachable by an *unscoped* monitoring check (no key) for uptime probes,
   or stay key-gated? (Default here: key-gated.)
4. When every consumer has scoped keys, who retires the shared key, and when?
