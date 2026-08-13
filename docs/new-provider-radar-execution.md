# New Provider Radar — cms-data execution handoff

> **Last reviewed: 2026-08-11** · **Status: T1–T4 complete; no cms-data blockers**

This is the build brief for finishing the cms-data side of New Provider Radar. Design authority
is [new-provider-radar.md](new-provider-radar.md) — event vocabulary, warehouse model, API
contract, and safety rules live there and are **not** renegotiated here. The product side
(the MD Watch page, saved markets, workspace state) is specified in the provider-search repo at
`docs/features/new-provider-radar.md` and is out of scope for this repo.

**How to use this doc in an agent session:** a thin prompt is enough — e.g. "Build T1 per
docs/new-provider-radar-execution.md." Standing repo rules are in `AGENTS.md`; follow
`data-platform-operating-model.md` for manifests, validation, promotion, and rollback. Never
overwrite the active production DuckDB in place; writes never happen in API request handlers.

## Current state (verified 2026-08-11)

Built and tested:

- `pipeline/nppes_radar.py` — baseline + weekly diff processor: `nppes_radar_provider_state`,
  `nppes_radar_events`, `nppes_radar_releases`; idempotent reapply, out-of-order rejection,
  single-transaction writes, baseline emits no events. CLI documented in the design doc.
- `api/radar.py` — `GET /radar/providers` (ZIP set + event type + taxonomy + date filters),
  wired secured in `api/main.py`; tests in `api/test_nppes_radar.py`.
- `pipeline/discovery.py` — already parses the official NPPES download index and recognizes the
  `nppes_monthly_v2` and `nppes_weekly_incremental_v2` filename shapes.

All four tracks are complete. Product follow-ups from the precision spike are recorded in
[new-provider-radar-precision-2026-08-11.md](new-provider-radar-precision-2026-08-11.md).

## Production handoff snapshot

This is the authoritative post-cutover identity for operations and downstream integration:

- selected deployment: `deployment-20260811T031052Z-73cea84b1b`;
- serving code: `fa4bcdd78ffc3ac3c60b2d63f7187035258a7417`;
- warehouse release: `warehouse-20260811T021837Z-f44c147e30`;
- warehouse pipeline commit: `3c3e761afcfb6aa8c5190e53985adfd50f8e0a51`;
- warehouse SHA-256: `91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`;
- runtime: `runtime-candidate-8985e8a-c26024b3`;
- immediate rollback deployment: `deployment-20260811T023712Z-b68e0ca9c3`;
- production smoke evidence:
  `/srv/cms-data-platform/production/evidence/deployment-20260811T031052Z-73cea84b1b/smoke.json`
  (SHA-256 `b3e46234169d1619c03d5dd9c899df1cf0db208ef8f6f57e5698d0d9eeb707cd`);
- cutover configuration audit:
  `/srv/cms-data-platform/audits/radar-t3-code-cutover-20260811T0314Z/`.

The serving-code commit and warehouse-pipeline commit intentionally differ: T3 was a read-only API
addition promoted against the already validated T2 warehouse. The release endpoint therefore
reports the warehouse pipeline commit, while production-manager status reports the serving-code
commit. Do not treat that expected split as drift. The warehouse file was not replaced or modified
during the T3 code-only cutover.

## Remaining cross-repo work

There are no remaining cms-data acceptance items in T1–T4. The next work belongs primarily in
provider-search and must preserve the API and event contracts in
[new-provider-radar.md](new-provider-radar.md):

- [ ] Opt the ad-hoc MD Watch search UI and proxy into the promoted `city` + `state` request mode;
      retain ZIP mode for saved-market boundaries and keep response validation fail-closed.
- [ ] Require at least one explicit taxonomy selection before notifications can be enabled; an
      unset taxonomy may power the interactive feed but must not mean an all-taxonomy digest.
- [ ] Keep email/digest delivery disabled while sampling a second real market that has configured
      target taxonomies; record the same volume, fit, duplicate, boundary, and location-quality
      measures used in the first precision report.
- [ ] Design same-address burst grouping or capping for a future digest without deleting or
      deduplicating durable feed events.
- [ ] Re-evaluate digest defaults only after the second-market evidence exists. Do not infer a
      universal volume threshold from the two-ZIP Denver QA sample.

No cms-data change is needed for those product tasks unless provider-search uncovers a concrete
contract or data-quality defect. Any future warehouse refresh or code promotion remains subject to
the staging, validation, rollback, and explicit approval gates in the production runbook.

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

- [x] Production serves `/radar/providers` from promoted data with source freshness coming from
      `nppes_radar_releases`.
- [x] Two consecutive weekly releases applied end-to-end (T1 acquisition → processor → promoted)
      with no manual file handling.
- [x] A rehearsed rollback leaves the prior promoted database serving.

Completed 2026-08-11: immutable acquisition runs installed a July monthly baseline and three
consecutive weekly releases through 2026-08-02. The targeted staging release
`warehouse-20260811T021837Z-f44c147e30` passed comparison policy
`nppes_radar_targeted_v1`, including zero duplicate logical events, zero orphan release
references, ordered release-ledger checks, and an event-ledger delta of zero. It contains 69,374
events across four source releases.

After explicit approval, the runbook cutover selected and verified production deployment
`deployment-20260811T023712Z-b68e0ca9c3` (warehouse SHA-256
`91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`). Both the candidate and
predecessor passed complete loopback smoke rehearsals; activation and rollback directions passed
manager dry-runs while the predecessor remained selected. The one-shot cutover then recorded
fresh authenticated smoke evidence and retained verified deployment
`deployment-20260804T163418Z-2ad954a774` as the immutable rollback bundle. A live Radar query for
ZIP 20852 returned 628 events and `source_fresh_through: 2026-08-02`, proving freshness is served
from the release ledger. No T2 blockers remain; T3 and T4 are now unblocked in track order.

Monthly rollover rule: the newest validated monthly archive is a complete baseline and may produce
a valid staging candidate before a later weekly archive exists. Reconciliation includes only weekly
periods beginning on or after that monthly baseline, in source-period order. Earlier weeklies are
superseded by the baseline and must not be replayed over it. A monthly-only candidate therefore has
one baseline release-ledger row, zero weekly rows, and zero baseline-generated events; it still runs
the complete comparison gates and remains unpromoted. Once that monthly baseline is selected, the
freshness monitor treats a latest weekly period ending on or before the installed monthly period as
covered rather than stale; the monitor reason records that family-level coverage explicitly.

The CMS data-platform operator owns the daily 07:15 UTC polling and staging-reconciliation cadence
defined by `cms-nppes-radar-reconciliation.timer`. Publisher-version no-ops are successful runs.
Candidate promotion is deliberately manual and remains owned by the approval-gated production
runbook; the timer never selects a production bundle or restarts the API.

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

- [x] City scope returns exactly the normalized-match rows; state without city and city without
      state are 422s; combined zip+city scope is a 422.
- [x] Tests cover casing/whitespace normalization and the unchanged response shape.
- [x] This doc and `new-provider-radar.md`'s API section updated with the final contract.

Completed 2026-08-11: `/radar/providers` now requires exactly one geographic scope: 1-100
`zip5` values or `city` plus two-letter `state`. City/state matching trims and uppercases both
request and current NPPES primary-practice values; ZIP filtering and every other filter retain
their existing semantics. Invalid partial or combined scopes fail with 422, and the response model
is unchanged. Implementation is committed at `fa4bcdd` and production now serves full commit
`fa4bcdd78ffc3ac3c60b2d63f7187035258a7417` through verified deployment
`deployment-20260811T031052Z-73cea84b1b`. The code-only promotion reused immutable warehouse
`warehouse-20260811T021837Z-f44c147e30` and runtime `runtime-candidate-8985e8a-c26024b3`; the
warehouse SHA-256 remained
`91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`.

The candidate passed the complete 15-check loopback smoke suite before selection. After the
approval-gated one-shot cutover, manager status reported the candidate verified with zero blocking
transactions and no transition sentinel; the live process resolved to the approved code artifact
and held the expected warehouse inode open. Production city/state smoke returned 170 Denver events
for the promoted source window, preserved the ZIP-mode response shape, and rejected combined scopes
with 422. Smoke evidence is recorded at
`/srv/cms-data-platform/production/evidence/deployment-20260811T031052Z-73cea84b1b/smoke.json`
(SHA-256 `b3e46234169d1619c03d5dd9c899df1cf0db208ef8f6f57e5698d0d9eeb707cd`). The immediate
predecessor `deployment-20260811T023712Z-b68e0ca9c3` remains intact for rollback. No T3 blockers
remain; provider-search may adopt the additive city/state mode.

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

Completed 2026-08-11: the three promoted weekly releases were replayed against the real hosted-dev
saved market **Radar QA — Denver** (`80206`, `80220`). The dated
[precision report](new-provider-radar-precision-2026-08-11.md) records weekly volume, taxonomy
distribution, duplicate and location-quality rates, manual noise review, and recommended defaults.
The data and boundary classifications were precise, but the market has no configured taxonomy
rules and 0 of 27 V1 candidates matched the expected cardiology family. Recommendation: keep the
digest disabled until taxonomy selection is required and a second targeted market is sampled. No
T4 implementation blocker remains; the no-digest verdict is the result of the spike.

## Explicitly deferred (do not build in these tracks)

From the design doc's later phases: secondary practice locations (Practice Location Reference
File), taxonomy-change/reactivation product surfacing, targeted Registry API verification
cache, Type 2 organization events. Also deferred: any ZCTA/metro crosswalk (see T3).

## Coordination with provider-search

**App-side status (2026-08-11):** R2 is BUILT — draft PR
[provider-search#201](https://github.com/blakethom8/provider-search/pull/201) ships the
`/md-watch` page ("MD Watch", Search rail), the proxy router, and the workspace state tables
(migration `20260810070000`, applied to the hosted dev project). The app shipped against the
ZIP-mode contract; T1 + T2 now provide its production data. **T3's additive city/state API mode is
now promoted and available for adoption. T4 is complete; its precision report recommends keeping
the digest disabled pending required taxonomy targeting and a second sampled market.**

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
- T3 is complete here, but provider-search still needs to opt its ad-hoc search UI into the
  additive `city` + `state` request mode. Existing ZIP-mode calls and the response shape are
  unchanged.
- Progress and blockers: note them in commit messages and this doc's track checkboxes;
  provider-search sessions read this file to know track status.
