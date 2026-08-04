# Serving-contract upgrade — implementation status and consumer handoff

**Date:** 2026-08-03 · **Deployed to production: 2026-08-04**
**Spec:** `docs/proposals/2026-08-serving-contract-upgrade.md` (committed, verified, amended)
**Audience:** consumer-side implementers (provider-search) and the platform owner. This file is
self-contained: everything a consumer needs to build against the new contract is here.

> **LIVE IN PRODUCTION** since 2026-08-04T16:47:18Z as
> `deployment-20260804T163418Z-2ad954a774` (serving code `f081f8c`, warehouse
> `warehouse-20260723T000948Z-24c46c1cda` unchanged — a code-only deployment per the cutover
> runbook's new section). Full smoke suite passed at cutover; `/release`, the ETag validator,
> the 304 short-circuit, and 401-not-304 for bad keys are all verified against live traffic.
> Consumers can build against this contract now. §8.5 is answered by observation: the service
> user reads the deployment ledger, so `promoted_at`/`build.*` are populated — no permission
> change or metadata stamping needed.

## What shipped

| Slice | Status | Commit |
|---|---|---|
| Spec committed | done | `786b259` |
| §4 claims verified in-repo, spec corrected | done | `ba3c7fd` |
| **S1** — `GET /release` manifest endpoint | **done** | `fde2055` |
| **S2** — ETag / `If-None-Match` / 304 short-circuit | **done** | `617a48a` |
| §8 open questions answered / extended | done | `aff6ed3` |
| **S4** — serving-box invariants in the operating model | **done** | `83a7aa7` (+ `f49bb97` cleanup) |
| **S3** — scoped keys, rotation, `X-Request-ID` echo | **started: shared predicate extracted (`api/auth.py`); the rest blocked on owner** | `6bf6f81`; open: key distribution (§8.2), shared-key retirement (§8.4) |

All changes are additive. Existing routes, payloads, the shared `X-API-Key`, and
`contract_version: 2` are untouched; the serving process remains read-only. Test suite:
247 passed, 1 skipped (`cd api && pytest -q`).

## The contract, as implemented

### `GET /release` (key-gated, like every data route)

```json
{
  "release_id": "deployment-20260721T202014Z-28465a2bbf",
  "promoted_at": "2026-07-21T20:31:07+00:00",
  "verified_at": "2026-07-21T20:35:11+00:00",
  "representation_version": 1,
  "source_vintages": {"nppes_monthly_v2": "2026-06", "open_payments_general": "PY2024"},
  "build": {
    "checksum": "<warehouse sha256>",
    "pipeline_ref": "<full git commit that built the warehouse>",
    "warehouse_release_id": "warehouse-20260721T180000Z-…"
  },
  "compatibility": "current"
}
```

- `release_id` is the production **deployment ID** (the `release-current` bundle name). It
  names the deployed bundle (code + warehouse + runtime), so a code-only deploy also rotates
  it even though the data is unchanged — caches keyed on it over-invalidate safely, never
  serve stale.
- `representation_version` names the response **shape** and is bumped on any shape change.
  Consumers must key caches on `release_id` **and** `representation_version`; nothing else in
  this payload is a cache key. `compatibility` is operator free-text.
- Fields other than `release_id`, `representation_version`, and `compatibility` may be `null`
  when box evidence is partially unreadable — the endpoint reports what it can prove.
- **503** (with a JSON `detail`) when the process cannot name its release at all (e.g. a dev
  box serving a bare DuckDB path). Never a guessed identity. Poll daily and on your own
  deploys; it is cheap (no DuckDB query).
- **Consumer rule for 503:** treat a `/release` 503 as "no release-keyed caching available"
  and fall back to plain TTL caching — do not invent a release key and do not treat it as an
  outage (data routes still work). Note for provider-search's Tier-M cache (Gate D): the
  development tunnel serves a bare `DUCKDB_PATH` today and will sit in exactly this 503 state
  unless the dev deploy stamps `CMS_RELEASE_METADATA_PATH` (spec §8.7).

### Cache validators on data responses

- Every data **GET** (200) carries `ETag: "<release_id>:<representation_version>"` (strong)
  and `Cache-Control: private, no-cache` (store allowed, revalidate before reuse).
- Send `If-None-Match` with the stored ETag: a match returns **304, no body, no DuckDB
  query**. `W/"…"` weak forms, comma-separated lists, and `*` are honored.
- Conditional handling is **GET-only**. POST routes (e.g. `/query`) carry no validators —
  flagged to the owner as §8.6; say something if you cache POST responses.
- An invalid or missing API key gets **401, never 304**, even with a matching validator.
- `/health` (unauthenticated) and docs surfaces carry no validators.
- The `contract_version: 2` field inside practice-shaped payloads is unchanged and still must
  be validated after a 200 — it is orthogonal to the transport-level ETag.

### What did NOT change (S3 pending)

- One shared `X-API-Key` secret; no per-consumer keys yet, no key-name logging, no
  `X-Request-ID` echo. Do not build against those until S3 ships.
- S3's first step is done: the previously duplicated auth predicate is extracted to
  `api/auth.py` (`make_key_validator`), and both enforcement points — the `check_api_key`
  dependency and the cache middleware's `is_authorized` — consume it, with a lockstep test in
  `api/test_auth.py`. Scoped keys will change only the validator construction. No behavior
  change: shared key and open-access dev mode work exactly as before.

## Implementation notes (platform side)

- New module: `api/release_info.py` (resolver, dataclass, router, middleware,
  `REPRESENTATION_VERSION = 1`). Tests: `api/test_release_info.py` (14). Wiring:
  `api/main.py` only.
- Release identity is derived read-only from evidence the production control plane already
  places on the box — `DUCKDB_PATH` resolves through `production/release-current/` into
  `production/releases/<deployment-id>/`, so the bundle directory name is the identity;
  `deployments.json` and `evidence/<deployment-id>/source-manifests.json` enrich it. No
  locks taken, no writes, DuckDB never opened by the resolver. `CMS_RELEASE_METADATA_PATH`
  can point at an explicit JSON override for non-bundle deployments.
- A successful resolution is cached for the process lifetime (cutover restarts the service);
  failures are retried per request.
- Key spec corrections made during verification: `pipeline/manifests.py` is the *source-run*
  release tier, not the serving tier; and the promotion metadata was already adjacent to the
  deployed DuckDB, so no deploy-step change was needed.

## Open items for the owner (full text in spec §8)

1. Off-box retention count N (suggest 3; on-box floor of active + 2 previous already written).
2. Consumer-side scoped-key storage and rotation cadence — blocks S3.
3. Confirm `/release` stays key-gated (`/health` already serves as the unauthenticated probe).
4. Who retires the shared key after S3, and when.
5. ~~One-time box check: is `production/deployments.json` group-readable by `dataops`?~~
   **Answered 2026-08-04 in production:** yes — the live `/release` reports `promoted_at` and
   `build.*` from the ledger. No change needed.
6. GET-only conditional semantics acceptable, or do POST data routes need validators?
7. Dev boxes: stamp the metadata override, or accept 503 on `/release` in dev?
8. Enforcement mechanism for `representation_version` bump discipline.
