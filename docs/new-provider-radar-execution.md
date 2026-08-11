# New Provider Radar — cms-data execution handoff

> **Last reviewed: 2026-08-10** · **Status: implementation handoff — remaining data-plane work**

This is the build brief for finishing the cms-data side of New Provider Radar. Design authority
is [new-provider-radar.md](new-provider-radar.md) — event vocabulary, warehouse model, API
contract, and safety rules live there and are **not** renegotiated here. The product side
(the MD Watch page, saved markets, workspace state) is specified in the provider-search repo at
`docs/features/new-provider-radar.md` and is out of scope for this repo.

**How to use this doc in an agent session:** a thin prompt is enough — e.g. "Build T1 per
docs/new-provider-radar-execution.md." Standing repo rules are in `AGENTS.md`; follow
`data-platform-operating-model.md` for manifests, validation, promotion, and rollback. Never
overwrite the active production DuckDB in place; writes never happen in API request handlers.

## Current state (verified 2026-08-10)

Built and tested:

- `pipeline/nppes_radar.py` — baseline + weekly diff processor: `nppes_radar_provider_state`,
  `nppes_radar_events`, `nppes_radar_releases`; idempotent reapply, out-of-order rejection,
  single-transaction writes, baseline emits no events. CLI documented in the design doc.
- `api/radar.py` — `GET /radar/providers` (ZIP set + event type + taxonomy + date filters),
  wired secured in `api/main.py`; tests in `api/test_nppes_radar.py`.
- `pipeline/discovery.py` — already parses the official NPPES download index and recognizes the
  `nppes_monthly_v2` and `nppes_weekly_incremental_v2` filename shapes.

Not built (the gap this doc closes):

- No scheduled acquisition: the radar CLI takes a hand-extracted CSV and has only been run
  against staging candidates.
- Radar tables are not installed in the production warehouse; they are not part of the
  staging → promotion flow.
- No city/state query support on the API (new product requirement, T3).
- No precision measurement against a real market (T4).

## Tracks

Each track is independently shippable, in order. Conventional commits scoped by subsystem
(`feat(pipeline):`, `feat(api):`); tests run from `api/` per `AGENTS.md`.

### T1 — Automated weekly acquisition

Wire the two NPPES V2 sources into the standard acquire path: discovery (already working) →
download → archive validation → extraction → manifest record, so a scheduled run can go from
"publisher posted a new weekly file" to "extracted CSV ready for `pipeline.nppes_radar`"
without hands. Reuse the manifest/acquisition machinery (`pipeline/acquire.py`,
`pipeline/manifests.py`, `pipeline/archive_acquisition.py`) rather than the legacy
`pipeline/nppes.py` download path; keep immutable run artifacts under `data/runs/`.

Acceptance:

- [x] A single command (cron-able) discovers, acquires, and extracts a not-yet-seen weekly
      release; a re-run on the same publisher version is a recorded no-op.
- [x] A calendar date with no new publisher version produces no acquisition (never assume a
      date proves a release exists).
- [x] Manifest rows record source id, publisher version, checksums, and run id for each
      acquisition.
- [x] Fixture-based tests for the discovery→acquire handoff of both source ids.

Completed 2026-08-10: `pipeline.data_platform acquire nppes_weekly_incremental_v2` now uses the
discovered publisher version as its idempotency key, writes the validated archive and extracted
`npidata_pfile.csv` under one immutable run directory, and records SHA-256/byte-size evidence for
both artifacts. Monthly and weekly fixture handoffs cover the successful acquisition and
same-version no-op paths. No T1 blockers remain.

### T2 — Production install and promotion integration

Bring the radar tables into the versioned warehouse flow per
[production-promotion-runbook.md](production-promotion-runbook.md):

- Install the monthly baseline into a staging candidate, apply available weeklies, then promote
  through the approval-gated cutover — never in place.
- Add radar tables to the staging validation gates (row counts, release-ledger consistency,
  the design doc's safety properties).
- Define the monthly reconciliation run (monthly full vs. accumulated weeklies) as a scheduled
  job with the no-duplicate-events guarantee.

Acceptance:

- [ ] Production serves `/radar/providers` from promoted data with source freshness coming from
      `nppes_radar_releases`.
- [ ] Two consecutive weekly releases applied end-to-end (T1 acquisition → processor → promoted)
      with no manual file handling.
- [ ] A rehearsed rollback leaves the prior promoted database serving.

Implementation status (2026-08-10): the versioned full-platform builder now accepts one monthly
baseline plus all consecutive weekly runs selected after it, applies them in source-period order,
and records staging gates for baseline/event counts, release-ledger consistency, orphan release
references, ordering, and duplicate logical events. `pipeline.radar_reconciliation` selects only
validated publisher runs, builds and compares a fresh staging candidate, and records a no-op when
the identical source/run/commit evidence was already reconciled. The checked-in
`cms-nppes-radar-reconciliation.timer` polls publisher metadata and invokes that staging-only flow;
it contains no promotion or production-cutover command.

Blockers: the three acceptance items above require real monthly and two consecutive weekly
artifacts on the data server, a prepared/compared production candidate, authenticated production
smoke evidence, and a rollback rehearsal. Production selection/restart is the runbook's explicit
approval gate, so none is checked and no cutover has been attempted. T3 and T4 remain gated on T2.

### T3 — Contract addition: city/state scope

Product requirement from provider-search (ad-hoc "search a city" mode when a rep has no saved
ZIP market): extend `GET /radar/providers` to accept a city scope as an alternative to the ZIP
set.

- New params: `city` (string) + `state` (2-letter, required when `city` is present). Exactly one
  scope is required per request: `zip5[]` **or** `city`+`state`; both or neither is a 422.
- Match against the current primary practice city/state in `nppes_radar_provider_state`,
  normalized (trim, uppercase). No fuzzy matching in V1.
- All other filters (event types, taxonomy, dates, pagination, deactivated exclusion) behave
  identically. Response shape is unchanged.
- Known limitation to document in the API docstring: NPPES city strings are noisy
  (abbreviations, neighborhoods) and city scope misses suburbs. A ZCTA/metro crosswalk is the
  named upgrade path if the product needs it — do not build it speculatively.

Acceptance:

- [ ] City scope returns exactly the normalized-match rows; state without city and city without
      state are 422s; combined zip+city scope is a 422.
- [ ] Tests cover casing/whitespace normalization and the unchanged response shape.
- [ ] This doc and `new-provider-radar.md`'s API section updated with the final contract.

### T4 — Precision spike (R1 in the product plan)

Before anyone enables a digest (the MD Watch page itself shipped without one): replay the available historical
weekly releases against one or two real saved-market ZIP sets (get the actual ZIP lists from
Blake / the provider-search workspace; the Denver-area fixture market is the expected first
sample) and measure:

- candidate volume per market per week, split by event type;
- taxonomy distribution vs. the market's target specialties;
- duplicate-event rate across weekly + monthly reconciliation;
- rows with missing/unusable practice location;
- eyeballed false positives (address edits within the same ZIP, re-enumerations, etc.).

Deliverable: a short dated report committed under `docs/` (or an evidence file referenced from
this doc) with the numbers and a recommendation for default filters and digest safety. No
numeric success targets exist until this establishes the denominator.

## Explicitly deferred (do not build in these tracks)

From the design doc's later phases: secondary practice locations (Practice Location Reference
File), taxonomy-change/reactivation product surfacing, targeted Registry API verification
cache, Type 2 organization events. Also deferred: any ZCTA/metro crosswalk (see T3).

## Coordination with provider-search

**App-side status (2026-08-10):** R2 is BUILT — draft PR
[provider-search#201](https://github.com/blakethom8/provider-search/pull/201) ships the
`/md-watch` page ("MD Watch", Search rail), the proxy router, and the workspace state tables
(migration `20260810070000`, applied to the hosted dev project). The app is live against the
ZIP-mode contract and correctly renders "source data not available" until a release is
installed here. **What the app is waiting on from this repo: T1 + T2 (real data). T3 gates
only the not-yet-built ad-hoc city mode; T4 gates only the digest.** Build order T1 → T2
first — data is the thing MD Watch lacks — then T3, then T4.

- `event_id` is a durable foreign key for workspace state in the application — treat its
  determinism rule (release, provider, event type, effective date, before/after values) as a
  frozen contract; changing it orphans customer state. The application stores it in
  `workspace_radar_item_states` keyed `(workspace_id, event_id)`.
- Response-shape changes to `/radar/providers` need a matching update to the provider-search
  proxy (`docs/features/new-provider-radar.md` §6 there) — additive fields are fine, renames
  are not. The app also re-validates every returned event's resulting ZIP against the market
  boundary and fails the page closed on a stray row, so a boundary-semantics change here is a
  breaking change even if the shape is unchanged.
- The app calls this API with retries disabled (`max_attempts=1`): a 503 ("radar not
  installed") is treated as a durable verdict. Once T2 makes 503 a genuinely transient state,
  nothing breaks — the rep refreshes — but do not start returning 503 for momentary conditions
  by design.
- Progress and blockers: note them in commit messages and this doc's track checkboxes;
  provider-search sessions read this file to know track status.
