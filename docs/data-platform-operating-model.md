# Provider Data Platform Operating Model

> **Last reviewed: 2026-07-22** · **Status: canonical operating policy**

## Decision

This repository is the canonical public-data plane for Provider Search. It owns source discovery,
bulk acquisition, validation, warehouse construction, snapshot promotion, rollback, and the secured
read-only API. The `provider-search` repository owns product behavior and consumes versioned API
contracts; it must not download or rebuild these bulk datasets.

The repository name remains `cms-data` for now, although the scope also includes NPPES, Open
Payments, and the AACT mirror of ClinicalTrials.gov. Renaming can wait until the operating model is
implemented and production is reproducible.

## Repository Boundaries

| Location | Responsibility |
| --- | --- |
| `pipeline/` | Source discovery, download, load, transform, validation, and future promotion CLI |
| `schema/ddl.sql` | Warehouse schema shared by pipeline and API |
| `api/` | Authenticated, read-only query contracts used by Provider Search |
| `docs/` | Source policies, runbooks, licensing notes, and architecture decisions |
| `cms-public-data-catalog` | Metadata reference only; never the runtime ingestion engine |
| `provider-search` | Downstream application, auth/plan gates, UI, and upstream-contract checks |
| `healthcare-ai` | Separate experimental/private-data work; not a public-data source of truth |

Private customer claims, PHI, and uploaded client datasets must not enter this warehouse. They need
separate storage, access controls, retention policy, and contractual review.

## Source Refresh Policies

Schedules should discover releases from publisher metadata and run only when a version changes.
Calendar timers are polling opportunities, not proof that a new file exists.

| Source family | Publisher cadence | Target policy |
| --- | --- | --- |
| CMS annual utilization, Part D, DME, and QPP | Annual | Check metadata daily; promote within 48 hours of a new version |
| CMS Order and Referring | About twice weekly | Check daily; promote changed versions within 48 hours |
| CMS hospital enrollments and reassignment | Monthly | Check daily; promote changed versions within 48 hours |
| CMS PECOS public enrollment | Quarterly | Check weekly; promote changed versions within 72 hours |
| NPPES | Monthly full V2 plus weekly increments; registry API daily | Apply weekly increments and replace the base from every monthly V2 release |
| Open Payments | New program year by June 30; correction refresh in January | Check weekly year-round and daily during June/July and January release windows |
| AACT / ClinicalTrials.gov | Daily | Run the existing staged AACT refresh daily after the upstream snapshot is available |

Official discovery should use `https://data.cms.gov/data.json`, the NPPES download index, the Open
Payments dataset-download index, and the AACT downloads page. Do not hard-code dated archive URLs.

### Operational refresh gates

A timer may discover metadata, but it must never start a refresh merely because a calendar boundary
was reached. Every source must pass these common gates before it can enter a production candidate:

1. **Version gate:** primary-publisher discovery returns a parseable version different from the
   deployment-scoped production manifest. `unknown`, `unavailable`, and `discovery_error` require
   operator review and never authorize acquisition.
2. **Acquisition gate:** the immutable source run records its publisher URL and version, source
   period, retrieval time, byte size, SHA-256, schema fingerprint, code commit, and row counts.
3. **Validation gate:** required columns, period semantics, row bounds, identifiers, uniqueness,
   referential integrity, and source-specific invariants pass against the staged artifact.
4. **Comparison gate:** a complete candidate DuckDB is compared with the selected production
   baseline. Only intended tables may change, and the Provider Search API contract suite must pass.
5. **Promotion gate:** the candidate has immutable code, runtime, DuckDB, and source-manifest
   evidence; a verified predecessor remains available; selection changes only `release-current`;
   and the bounded smoke/automatic rollback path is ready.

Source-specific gates add to, and never replace, those common gates:

| Source family | Required source-specific gate before candidate build |
| --- | --- |
| CMS annual utilization, Part D, DME, and QPP | Stable CMS dataset UUID resolves to a new complete CSV resource; calendar-year/performance-year semantics parse; required tables retain schema and bounded row-count deltas. HCPCS Level I content remains blocked unless the licensing gate is satisfied. |
| CMS Order and Referring | Snapshot interval advances; NPI shape and eligibility-domain checks pass; removal/addition deltas are reviewed because absence affects ordering/referring eligibility. |
| CMS Hospital Enrollments | Month-end period advances; the exact canonical header, source parity, hospital NPI/name rules, ambiguity exclusions, and affiliation comparison pass. |
| CMS Revalidation Group Reassignment | Month-end period advances; practitioner/group NPI shape, reassignment uniqueness, and practice/affiliation deltas pass without treating reassignment as asserted hospital privileges. |
| CMS PECOS Public Provider Enrollment | Quarter-end period advances; enrollment identifiers and provider-type distributions pass bounded-delta review. |
| NPPES monthly V2 | A newer full V2 publisher release is present; load into a fresh staging candidate; reconcile all NPIs and prior weekly events; validate NPI uniqueness, entity/taxonomy/location coverage, deletions/deactivations, and representative API/Radar queries before it becomes the new baseline. |
| NPPES weekly incremental V2 | The inclusive filename period follows the installed monthly/weekly watermark without an unexplained gap or overlap; apply idempotently to a fresh copy of the monthly baseline; validate changed NPIs and event counts. A weekly file never substitutes for the next monthly full reconciliation. |
| NPPES Registry API | Use only for daily targeted verification of already selected NPIs and confidence labeling. It is not a bulk source, does not advance the installed monthly/weekly version, and must not trigger a production warehouse promotion by itself. |
| Open Payments general, research, and ownership | Official index exposes a newer program-year/correction release; category and program year parse explicitly; duplicate/payment/provider identifiers and category row deltas pass. Preserve the three category versions independently while validating their shared publication cycle. |
| AACT / ClinicalTrials.gov | Export date advances monotonically; restore succeeds in isolated staging; required AACT tables, study identifiers, source date, counts, research endpoints, and clinical-trials version checks pass. |

NPPES therefore operates as weekly change detection plus monthly authoritative reconciliation, with
daily Registry API verification only for targeted product evidence. Weekly increments should be
processed in publisher-period order; a missing interval blocks the incremental chain until it is
resolved or the next full monthly snapshot establishes a new baseline.

The first discovery implementation lives in `pipeline/source_registry.py`,
`pipeline/discovery.py`, and `pipeline/data_platform.py`. It makes these concrete choices:

- CMS sources are matched by their stable dataset UUID in `data.json`; the newest complete CSV
  distribution supplies the version-specific resource UUID, source period, modified date, and URL.
- NPPES monthly and weekly files are parsed from the official index and accepted only when they
  match the documented V2 filename shapes. The monthly release date comes from the publisher label;
  the weekly period comes from the filename, and no unsupported weekly release timestamp is guessed.
- Open Payments program-year ZIPs are read from the official Dataset Explorer's compiled
  `datasetDownloads` configuration. The client-side download route currently returns 404 to direct
  metadata clients, so discovery starts at `/datasets` and follows only the official same-host
  `frontend/build/static/js/index.js` asset. This is intentionally conservative and fragile: a route,
  asset-path, or filename change becomes `discovery_error`, never `current`.
- AACT discovery selects the PostgreSQL snapshot card used by the existing restore workflow. Its
  HTML card classes, exported-date label, filename, or download-link shape changing becomes
  `discovery_error`.

All metadata responses are capped at 64 MiB. Network failures are `unavailable`; reachable metadata
that no longer satisfies a parser contract is `discovery_error`. Neither state can be reported as
current.

## Refresh Lifecycle

Every source follows the same state machine:

1. **Discover** the publisher's current version and compare it with the installed manifest.
2. **Download** to an immutable run directory using a partial filename and atomic rename.
3. **Record** source URL, publisher version, source period, retrieval time, byte size, SHA-256,
   schema fingerprint, and code commit.
4. **Stage** a new warehouse or source table without touching the active database.
5. **Validate** archive integrity, required columns, row-count bounds, source-period expectations,
   NPI shape, uniqueness, referential integrity, and representative API queries.
6. **Promote** by atomically switching a `current` symlink or equivalent release pointer.
7. **Verify** API health, `/practices/capabilities`, clinical-trials version, and Provider Search
   smoke queries.
8. **Retain** the active and at least two previous validated releases for rollback.

A failure leaves the active release unchanged and records a failed run with a safe diagnostic.

## Manifest and Status Contract

Add a durable manifest store with one row per source version and pipeline run. At minimum record:

- source ID, publisher, source version, data period, and expected cadence;
- discovery, download, validation, promotion, failure, and rollback timestamps;
- URL, checksum, byte size, schema fingerprint, row counts, and code commit;
- active release ID, validation results, and operator/error summary.

The API should expose a secured `/data-status` endpoint with source period, publisher release date,
ingestion date, active release, and validation state. Provider Search should display source periods
from this contract rather than inferring freshness from file modification times.

For the local milestone, the versioned JSON manifest document defaults to
`data/manifests.json`, which is gitignored with the rest of `data/`. Schema version 1 records run and
release IDs, publisher version and source period, publisher/discovery/retrieval timestamps, source
URL, byte size, SHA-256, schema fingerprint, per-table row counts, pipeline commit, validation and
promotion timestamps/states, active release ID, rollback/failure timestamps, and safe summaries.
The status command only reads this file. A manifest proves an installed version only when validation
passed, promotion state is active, retrieval is recorded, and `release_id` equals
`active_release_id`; file modification time is never provenance.

Legacy files that predate this manifest contract may be assessed with
`python -m pipeline.provenance_backfill`. A retrospective record is generated only when an explicit
evidence document identifies the exact publisher version and URL, retained artifact size and
SHA-256, immutable target-warehouse SHA-256, and exact read-only table counts. AACT additionally
requires a read-only PostgreSQL connection. The evidence document plus any existing selected
manifest must account for every registry source; sources that cannot be proved are recorded as
`unresolved` or `not_installed` and do not receive an active manifest. The command writes candidate
evidence only and never changes a warehouse, deployment pointer, or selected deployment evidence.

## Immutable Acquisition and Complete Candidate Builds

`pipeline/acquisition.py` and `pipeline/archive_acquisition.py` make lifecycle steps 2 and 3
concrete for every registered source. CMS CSVs use source-specific column, identifier, encoding,
row, and byte contracts. NPPES, Open Payments, and AACT archives use publisher-discovered URLs and
enforce safe member paths, required member patterns, CRC checks, encryption rejection, expansion
ceilings, and member-list fingerprints. The
`pipeline.data_platform acquire <source-id>` command:

- resolves the current CSV through live `data.json` discovery instead of a dated URL in code;
- accepts only HTTPS artifacts on `data.cms.gov` and rejects cross-host redirects;
- enforces a source-specific transfer ceiling while streaming to a `.partial` artifact;
- atomically renames the artifact only after the response completes and the file is flushed;
- validates the source-specific CSV or archive contract and records its encoding or archive shape;
- records actual bytes, SHA-256, an ordered-header schema fingerprint, source row count, discovery and
  retrieval timestamps, source period, publisher release/version, and Git commit; and
- writes a per-run manifest plus the local versioned manifest store with validation state `passed`
  and promotion state `not_promoted`.

Run directories use `data/runs/<source-id>/<UTC timestamp>-<random suffix>/` and are never reused or
overwritten. Failed retrieval or validation runs retain a safe failed manifest and leave the active
warehouse unchanged. `--dry-run` performs discovery and path planning without creating the data
root. Fixture metadata is deliberately accepted only with `--dry-run`, so fixture-derived publisher
versions cannot be acquired as promotion candidates.

`build-platform-release --environment staging` requires exactly the ten CMS, two NPPES, and three
Open Payments runs. It copies a checksum-verified baseline to a new candidate, strictly replaces raw
tables, rebuilds derived data, applies monthly NPPES before the weekly overlay, rebuilds Radar and
Open Payments summaries, and records exact counts for every table required by production smoke.
`prepare-aact-release --environment staging` revalidates the AACT archive and seals its PostgreSQL
custom dump and data dictionary as a separate immutable restore artifact. Neither operation opens or
overwrites the selected production DuckDB.

## Production Model

Use versioned releases on the Hetzner data server and systemd oneshot services plus timers. The API
service should open only the promoted DuckDB release in read-only mode. Production deployment must
be traceable to a Git commit; copying an unversioned working directory is not an acceptable steady
state. Do not add Airflow or another orchestration platform until systemd can no longer provide the
required scheduling, locking, retry, and observability.

The first non-production Hetzner layout is now established under `/srv/cms-data-platform/`:

```text
code/<full-git-commit>/    detached, clean, immutable code checkouts
staging/code-current      atomic symlink used only by staging commands
data/runs/<source>/<run>/ immutable acquired artifacts and per-run manifests
data/manifests.json       staging manifest store
backups/<backup-id>/      verified warehouse baselines and backup metadata
locks/                    advisory locks for one-at-a-time staging operations
rehearsal/                pointers and evidence used only for rollback drills
```

The first staged Hospital Enrollments run was executed from a full-commit checkout and validated. A
separate baseline copy of the active warehouse was checksum-matched and opened read-only, and
isolated staging pointers completed atomic switch/rollback rehearsals. No production service
references this staging root, and no production database pointer or service was changed.

`pipeline/releases.py` now implements the versioned staging warehouse path:

- `build-release --environment staging` requires a validated source run and a backup manifest that
  proves the baseline path, byte size, SHA-256, and successful read-only open;
- the baseline is streamed into a new `warehouse.duckdb.partial` while its checksum and stable file
  identity are revalidated, so the active `DUCKDB_PATH` is never opened;
- the candidate transactionally replaces `raw_hospital_enrollments`, retaining the complete
  baseline warehouse needed by the API and adding source run, source release, source period, and
  ingestion metadata to every raw row;
- the candidate replaces `hospital_affiliations` from `practice_locations` and the current hospital
  snapshot. Names are normalized to uppercase ASCII alphanumerics and joined with state, but a key
  is accepted only when it identifies exactly one hospital NPI. Legal-name matches are `medium`
  confidence, DBA-name matches are `low`, and ambiguous health-system names are excluded rather
  than expanded to every hospital in that system;
- the Hospital Enrollments header must exactly match the 39-column canonical mapping. A missing,
  extra, or reordered publisher column fails the release instead of being guessed;
- validation checks baseline providers, source row parity, NPI shape, hospital names, distinct NPIs,
  affiliation uniqueness and referential integrity, allowed confidence/source pairs, ambiguity
  counts, and representative affiliations before checkpointing and hashing the completed candidate;
- warehouse-release schema version 2 records the DuckDB runtime version and structured validation
  evidence while reading existing schema-version-1 records compatibly;
- `compare-release --environment staging` verifies both candidate and immutable-baseline checksums,
  opens both databases read-only, requires every table outside the two intended hospital tables to
  retain its row count, checks required API tables, and writes `comparison.json` beside the release;
- `promote --environment staging` verifies the candidate checksum and atomically changes only
  `data/staging/warehouse-current`; and
- `rollback --environment staging` restores the journaled prior staging pointer and source/release
  states. A failed metadata write restores both pointer and manifests, while an interrupted pending
  journal blocks further transitions for operator review.

Warehouse release records live in `data/warehouse-releases.json`, per-release evidence lives beside
each immutable database, and transition evidence lives in `data/promotion-journal.json`. Those
records remain staging-specific; production state is not written back into a source manifest or
guessed from the staging pointer.

`pipeline.production` implements the independent production-serving control plane. Its versioned
deployment ledger records code, warehouse, and Python-runtime targets and fingerprints; its separate
journal makes a pending or interrupted pointer transaction explicit. Bootstrap captures an immutable
legacy rollback release. Prepare validates a clean, read-only Git checkout, the warehouse release and
comparison evidence, the complete database checksum, and the runtime package fingerprint without
changing the serving selector. Activate and rollback atomically replace one `release-current` bundle
pointer under one lock, restore the complete prior ledger and selector on a handled failure, and never
open DuckDB. `pipeline.production_smoke` records bounded authenticated API and process-identity checks
before a release may be marked verified. `pipeline.production_cutover` owns the one restart and
automatically restarts and re-verifies the predecessor after a candidate failure.

The checked-in systemd definition under `deploy/systemd/` uses only the selected production bundle and
refuses startup while a transition sentinel or blocking journal event exists. It reads control logic
from a separate immutable operations package so rollback does not depend on candidate code, and it
loads secrets only from stable protected environment files. Staging and production therefore have
independent active states and rollback histories. This establishes reproducible serving and
promotion; refresh remains operator-triggered, and future timers must still build and validate in
staging before an explicitly approved production promotion.

`deploy/systemd/cms-data-status.timer` schedules live publisher-metadata discovery daily at 06:15
UTC with a randomized delay. Its oneshot validates the production control plane first, runs from the
immutable operations package with the selected runtime, and reads provenance only from
`production/evidence/<selected-deployment-id>/source-manifests.json`. The selected deployment ID is
derived from `release-current`; there is no second production selector. If that immutable snapshot
is absent, installed provenance remains `unknown`. Output and semantic exit status are retained by
the systemd journal: `0` is current, `1` is stale/unknown, and `2` is discovery or control-plane
failure. This monitor downloads no dataset, opens no DuckDB file, and never launches a refresh.

Before activating every future candidate, write a root-owned `root:dataops` mode `0440`
source-manifest snapshot into a root-owned `root:dataops` mode `0750` deployment evidence
directory. It must contain only validated active source
versions actually present in the candidate warehouse. Copying the mutable staging manifest without
reconciling it to the candidate is prohibited. Rollback automatically selects the predecessor's
deployment-scoped snapshot because it restores the complete bundle pointer.

The name/state affiliation rule is intentionally incomplete. The current `practice_locations`
snapshot has no populated city or ZIP values, so it cannot safely disambiguate a health system with
multiple hospital NPIs in one state. Those keys remain excluded until a higher-quality linkage key
is acquired and separately validated. The API and downstream product must present these rows as
inferred affiliations with their confidence level, not as publisher-asserted clinician privileges.

DuckDB is pinned to `1.4.4`, matching the runtime already used by the Hetzner API and the staging
release rehearsal. Candidate builds record that version so runtime drift is visible in release
evidence.

The name/state affiliation rule is intentionally incomplete. The current `practice_locations`
snapshot has no populated city or ZIP values, so it cannot safely disambiguate a health system with
multiple hospital NPIs in one state. Those keys remain excluded until a higher-quality linkage key
is acquired and separately validated. The API and downstream product must present these rows as
inferred affiliations with their confidence level, not as publisher-asserted clinician privileges.

DuckDB is pinned to `1.4.4`, matching the runtime already used by the Hetzner API and the staging
release rehearsal. Candidate builds record that version so runtime drift is visible in release
evidence.

## Data-Use Guardrails

CMS public-use data and FOIA-disclosable NPPES data generally do not require a business agreement,
but retain source attribution and never imply government endorsement. Open Payments must be
described as reported transfers of value, not proof of endorsement, causation, or misconduct.

ClinicalTrials.gov outputs must attribute ClinicalTrials.gov, remain current, display the processing
date, and disclose modifications. Do not use registry email addresses for marketing.

HCPCS Level I contains AMA CPT codes and descriptions. Do not expose or commercialize that content
until the organization has confirmed an appropriate AMA license or implemented an approved filter.
Keep this as an explicit release gate, not an informal documentation caveat.

## Initial Implementation Sequence

1. Add a tested source registry and publisher-release discovery layer.
2. Add manifest persistence and a `status` command before changing production data.
3. Implement immutable downloads, checksums, schema validation, and fixture-based tests.
4. Build versioned DuckDB staging, atomic promotion, and rollback.
5. Put AACT on a daily timer, then migrate NPPES, Open Payments, and CMS families.
6. Refresh stale production sources only after the new promotion path passes smoke tests.
7. Add `/data-status`, downstream contract tests, monitoring, and operator alerts.

The first milestone is complete when a dry run can discover all source versions, explain which are
stale, and produce a manifest without downloading large files or mutating the active warehouse.
