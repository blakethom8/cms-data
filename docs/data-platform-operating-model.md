# Provider Data Platform Operating Model

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

## First Immutable Acquisition

`pipeline/acquisition.py` makes lifecycle steps 2 and 3 concrete for CMS Hospital Enrollments. The
`pipeline.data_platform acquire cms_hospital_enrollments` command:

- resolves the current CSV through live `data.json` discovery instead of a dated URL in code;
- accepts only HTTPS artifacts on `data.cms.gov` and rejects cross-host redirects;
- enforces a 100 MiB default transfer ceiling while streaming to `source.csv.partial`;
- atomically renames the artifact only after the response completes and the file is flushed;
- validates a non-empty UTF-8 or Windows-1252 CSV, records the detected encoding, and checks unique
  column names, required enrollment/NPI/organization fields, exact row widths, and ten-digit NPIs;
- records actual bytes, SHA-256, an ordered-header schema fingerprint, source row count, discovery and
  retrieval timestamps, source period, publisher release/version, and Git commit; and
- writes a per-run manifest plus the local versioned manifest store with validation state `passed`
  and promotion state `not_promoted`.

Run directories use `data/runs/<source-id>/<UTC timestamp>-<random suffix>/` and are never reused or
overwritten. Failed retrieval or validation runs retain a safe failed manifest and leave the active
warehouse unchanged. `--dry-run` performs discovery and path planning without creating the data
root. Fixture metadata is deliberately accepted only with `--dry-run`, so fixture-derived publisher
versions cannot be acquired as promotion candidates.

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
- the Hospital Enrollments header must exactly match the 39-column canonical mapping. A missing,
  extra, or reordered publisher column fails the release instead of being guessed;
- validation checks baseline providers, source row parity, NPI shape, hospital names, distinct NPIs,
  and table counts before checkpointing and hashing the completed candidate;
- `promote --environment staging` verifies the candidate checksum and atomically changes only
  `data/staging/warehouse-current`; and
- `rollback --environment staging` restores the journaled prior staging pointer and source/release
  states. A failed metadata write restores both pointer and manifests, while an interrupted pending
  journal blocks further transitions for operator review.

Warehouse release records live in `data/warehouse-releases.json`, per-release evidence lives beside
each immutable database, and transition evidence lives in `data/promotion-journal.json`. Production
is not an accepted CLI environment. A production promotion command, systemd integration, and active
service change remain a separate approval-gated milestone.

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
