# New Provider Radar — cms-data execution handoff

> **Last reviewed: 2026-09-21** · **Status: production current through 2026-09-20;
> daily staging reconciliation is installed and weekly promotion is operator-owned**

This is the build brief for finishing the cms-data side of New Provider Radar. Design authority
is [new-provider-radar.md](new-provider-radar.md) — event vocabulary, warehouse model, API
contract, and safety rules live there and are **not** renegotiated here. The product side
(the MD Watch page, saved markets, workspace state) is specified in the provider-search repo at
`docs/features/new-provider-radar.md` and is out of scope for this repo.

**How to use this doc in an agent session:** a thin prompt is enough — e.g. "Build T1 per
docs/new-provider-radar-execution.md." Standing repo rules are in `AGENTS.md`; follow
`data-platform-operating-model.md` for manifests, validation, promotion, and rollback. Never
overwrite the active production DuckDB in place; writes never happen in API request handlers.

## Current state (verified 2026-09-21)

Built and tested:

- `pipeline/nppes_radar.py` — baseline + weekly diff processor: `nppes_radar_provider_state`,
  `nppes_radar_events`, `nppes_radar_releases`; idempotent reapply, out-of-order rejection,
  single-transaction writes, baseline emits no events. CLI documented in the design doc.
- `api/radar.py` — `GET /radar/providers`, `GET /radar/providers/release`,
  `POST /radar/providers/match-scopes`, and `POST /radar/providers/hydrate`, wired secured in
  `api/main.py`; tests in `api/test_nppes_radar.py`.
- `pipeline/discovery.py` — already parses the official NPPES download index and recognizes the
  `nppes_monthly_v2` and `nppes_weekly_incremental_v2` filename shapes.

All four tracks are complete. Product follow-ups from the precision spike are recorded in
[new-provider-radar-precision-2026-08-11.md](new-provider-radar-precision-2026-08-11.md).

## Production handoff snapshot

The 2026-08-11 identity below is retained as historical handoff evidence. The current production
identity and recovery evidence are recorded in the recovery section below.

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

## 2026-09-20 recovery and 2026-09-21 cadence proof

Production had remained on `source_fresh_through: 2026-08-02` for 49 days. That blocked the
provider-search MD Watch paid-tier work in PR #884. Recovery acquired and replayed every missing
weekly release in order against the July baseline:

| Source period | Release ID | Provider rows | Event rows |
| --- | --- | ---: | ---: |
| 2026-08-03/2026-08-09 | `nppes_weekly_incremental_v2-44a36fd573304782` | 27,671 | 23,415 |
| 2026-08-10/2026-08-16 | `nppes_weekly_incremental_v2-6625ad3d24b62602` | 28,236 | 24,581 |
| 2026-08-17/2026-08-23 | `nppes_weekly_incremental_v2-7eba46ae009210d6` | 27,739 | 24,518 |
| 2026-08-24/2026-08-30 | `nppes_weekly_incremental_v2-42b4e493341846ef` | 29,451 | 25,487 |
| 2026-08-31/2026-09-06 | `nppes_weekly_incremental_v2-7302b2a97e19475e` | 28,799 | 24,967 |
| 2026-09-07/2026-09-13 | `nppes_weekly_incremental_v2-5e2e9f1689dad77f` | 22,779 | 19,678 |
| 2026-09-14/2026-09-20 | `nppes_weekly_incremental_v2-95ae8279bf1d200a` | 27,351 | 21,700 |

Re-running acquisition for all six versions produced only publisher-version no-ops. Staging
warehouse `warehouse-20260920T234445Z-6042e7a7ab` contained 212,020 events and ten release-ledger
rows and passed `nppes_radar_targeted_v1`: zero duplicate logical events, zero orphan release or
state rows, zero out-of-order releases, and zero event-ledger delta. After the approved cutover,
verified deployment `deployment-20260921T001737Z-4428d54844` selected warehouse SHA-256
`2681dea86f2c9bc68404489aa8cbd0da4467ba3b2265dfb6c970a81c1faa5808`. The authenticated live
release receipt for that approved catch-up was:

```json
{"contract_version":1,"source_release_id":"nppes_weekly_incremental_v2-5e2e9f1689dad77f","source_data_period":"2026-09-07/2026-09-13","source_fresh_through":"2026-09-13"}
```

On 2026-09-21 the installed timer path acquired the September monthly archive
(`nppes_monthly_v2-9f42862736f42fc4`) and the September 14–20 weekly release, then built and compared
staging warehouse `warehouse-20260921T152709Z-32ef406bf5`. The candidate retained all ten prior
release rows and all 212,020 prior event references, added two release rows and 21,700 events, and
passed comparison with zero failed requirements, evidence mismatches, or unexpected differences.
An immediate rerun was a one-second `source_runs_already_reconciled` no-op and wrote no candidate.

After the approved immutable copy, isolated 27-check smoke rehearsal, rollback dry run, and
cutover, verified deployment `deployment-20260921T155224Z-30a8db0951` selected warehouse SHA-256
`d39a294d430613ea246d279ead7985c29aeb95432b72c104b9d51c6251c028dc`. The live warehouse has
12 release rows, 233,720 event rows, and 7,482,386 provider-state rows. The authenticated live
receipt is now:

```json
{"contract_version":1,"source_release_id":"nppes_weekly_incremental_v2-95ae8279bf1d200a","source_data_period":"2026-09-14/2026-09-20","source_fresh_through":"2026-09-20"}
```

`cms-nppes-radar-reconciliation.timer` is installed, enabled, and active on the production host.
It polls daily at 07:15 UTC with randomized delay, respects the filesystem kill switch, and records
a dated `staging_reconciled` journal handoff owned by the **CMS data-platform operator on call**.
Unattended production selection remains disabled. The operator must inspect the candidate and
select a verified weekly release by Wednesday 18:00 UTC under
[the production promotion runbook](production-promotion-runbook.md), or record the blocking gate
and live freshness in issue #14 by that deadline. The post-cutover capacity preview passed at
84.56% used with 41,192,939,520 bytes free, but is in the critical band and only 0.44 percentage
points below the promotion block. The unit therefore does not automate the capacity, immutable-copy,
isolated-smoke, rollback, or live-probe gates.

Monthly state rebuilds restore every previously promoted release/event row, preserve the first
observed event set when a source is replayed, and fail comparison on any retired reference.
`/radar/providers/release` reports only the release actually selected.

## Remaining cross-repo work

There are no remaining cms-data acceptance items in T1–T4. The next work belongs primarily in
provider-search and must preserve the API and event contracts in
[new-provider-radar.md](new-provider-radar.md):

- [x] The ad-hoc MD Watch search UI and proxy adopted the promoted `city` + `state` request mode in
      provider-search PR #208; ZIP mode remains the saved-market boundary contract.
- [x] Notifications require an explicit taxonomy selection; an unset taxonomy is not treated as
      an all-taxonomy digest.
- [x] A second configured-taxonomy market was sampled and recorded in provider-search's
      `precision-market-2` document.
- [x] Same-address burst grouping/capping shipped as documented in provider-search's
      `digest-bursts` document without deleting durable feed events.
- [x] Digest defaults were re-evaluated from the second-market evidence rather than generalized
      from the original two-ZIP Denver sample.

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
As of 2026-09-20, the checked-in unit and timer are not installed on the production host; issue #14
tracks that open deployment. Until it is installed, the operator on call runs the same two acquire
commands and reconciliation manually after each publisher release. Candidate promotion remains a
manual runbook step owned by that operator; the timer never selects a production bundle or restarts
the API.

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

**Contract answers (2026-09-20):**

- **Hydrate retention:** V1 is append-only with no day or release-count expiry. Every release and
  event reference in a promoted warehouse remains servable through later weekly and monthly
  promotions, and candidate comparison hard-stops on a retired release or event reference.
  Provider-search keeps per-item `409 radar_reference_release_unavailable` and
  `radar_event_reference_unavailable` handling for rollback or operator-error defense.
- **Identity determinism:** yes. The release suffix is
  `sha256(source_id + NUL + publisher_version)[:16]`, so replaying the same official weekly file
  yields the same release ID (including `nppes_weekly_incremental_v2-5e2e9f1689dad77f`). Event IDs
  are deterministic from the release plus provider/event/effective-date/before-after tuple.
- **NULL prior ZIP:** yes, `practice_location_changed.old_zip5` can be `NULL` when the prior NPPES
  state had no usable five-digit practice ZIP. ZIP-scope matching classifies it as
  `entered_market`; the six recovered weeklies contained 2, 3, 3, 4, 5, and 1 such events.
- **Capacity:** there is no route-specific rate limiter. Production has a shared two-connection
  DuckDB pool and a four-second acquisition deadline; overload returns `503` with
  `Retry-After: 1`. `/radar/providers` is bounded to 250 rows, `/match-scopes` to 100 scopes and
  5,000 matches, and `/hydrate` to 100 references. Workspaces on a 900-second cadence should jitter
  starts and keep no more than one Radar request in flight per API instance.

**App-side status (2026-09-20):** [provider-search#201](https://github.com/blakethom8/provider-search/pull/201)
originally shipped the `/md-watch` page, proxy router, and workspace state tables. Provider-search
PR #208 later shipped city/state adoption and taxonomy gating; its `precision-market-2` and
`digest-bursts` documents record the second sample and burst design. PR #884 supplies the MD Watch
paid tier and depends on the weekly production feed remaining current.

- `event_id` is a durable foreign key for workspace state in the application — treat its
  determinism rule (release, provider, event type, effective date, before/after values) as a
  frozen contract; changing it orphans customer state. The application stores it in
  `workspace_radar_item_states` keyed `(workspace_id, event_id)`.
- Response-shape changes to `/radar/providers` need a matching update to the provider-search
  proxy (`docs/features/new-provider-radar.md` §6 there) — additive fields are fine, renames
  are not. The app also re-validates every returned event's resulting ZIP against the market
  boundary and fails the page closed on a stray row, so a boundary-semantics change here is a
  breaking change even if the shape is unchanged.
- The app keeps transport retries disabled (`max_attempts=1`). It replays a read-only Radar call
  exactly once only when cms-data returns an observed `503` with `Retry-After`, and otherwise
  preserves the explicit unavailable state.
- T3 is complete here and provider-search has adopted the additive `city` + `state` request mode.
  Existing ZIP-mode calls and the response shape are unchanged.
- Progress and blockers: note them in commit messages and this doc's track checkboxes;
  provider-search sessions read this file to know track status.
