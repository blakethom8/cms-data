# Provider Intelligence Data Platform

> **Last reviewed: 2026-07-22** · **Status: production operating guide**

`cms-data` is the canonical public-data platform for Provider Search. It owns public CMS, NPPES,
Open Payments, and AACT / ClinicalTrials.gov discovery, staging, validation, immutable release
artifacts, and the secured read-only API. `provider-search` is the downstream product; it must not
become a bulk-data ingestion repository.

The active production DuckDB database is never modified in place. A complete deployment selects
immutable code, runtime, warehouse, and provenance evidence through one atomic `release-current`
pointer; the previous complete release remains available for rollback.

## Start here

| Need | Canonical document |
| --- | --- |
| Platform scope, source policy, validation, promotion, and rollback rules | [Operating model](docs/data-platform-operating-model.md) |
| Data marts, sources, cadence, and architecture | [Platform overview](docs/platform-overview.md) |
| One safe staging-to-production promotion | [Production promotion runbook](docs/production-promotion-runbook.md) |
| NPPES weekly/monthly/daily Radar model | [New Provider Radar](docs/new-provider-radar.md) |
| AACT runtime and refresh constraints | [AACT clinical-trials adapter](docs/aact-clinical-trials.md) |
| systemd release layout and read-only status monitoring | [systemd guide](deploy/systemd/README.md) |
| Documentation lifecycle and archive index | [Documentation guide](docs/README.md) |

## Local setup and tests

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -r api/requirements-dev.txt

cd api && ../.venv/bin/python -m pytest -q
```

## Internal Data Command Center

The internal Data Command Center lives in [`dashboard/command-center/`](dashboard/command-center/).
It combines the curated data catalog, columns and samples, source-to-mart lineage, contract evidence,
and manifest run history without adding pipeline writes to the serving API. Its operating model and
future approval-gated refresh boundary are documented in
[`docs/data-command-center.md`](docs/data-command-center.md).

Run the FastAPI service locally against an explicitly selected local DuckDB file only:

```bash
cd api
DUCKDB_PATH=../data/provider_searcher.duckdb \
  ../.venv/bin/python -m uvicorn main:app --reload --port 8080
```

## Read-only source status

Publisher discovery does not download bulk data or open a DuckDB file:

```bash
# Live publisher metadata
.venv/bin/python -m pipeline.data_platform status

# Machine-readable output
.venv/bin/python -m pipeline.data_platform status --json

# Checked-in publisher fixtures; no network access
.venv/bin/python -m pipeline.data_platform status --offline --json
```

Exit codes are `0` when every provable source is current, `1` for stale or unknown provenance, and
`2` for unavailable publisher metadata or a discovery-contract error. Unknown provenance is never
guessed to be current.

## Safe staging commands

Use a dry run to inspect a publisher release without downloads or writes:

```bash
.venv/bin/python -m pipeline.data_platform acquire nppes_weekly_incremental_v2 --dry-run
.venv/bin/python -m pipeline.data_platform acquire nppes_monthly_v2 --dry-run
```

Actual acquisitions and candidate builds are staging-only operations. Follow the operating model
and promotion runbook; do not point any command at the active production warehouse.

## Production model

Production has one daily `cms-data-status.timer`. It only discovers publisher metadata and compares
it with deployment-scoped provenance; it never downloads data, creates a candidate, restarts the
API, or promotes a release. A stale status opens an operator workflow rather than initiating an
automatic refresh.

The production API is secured with `X-API-Key`. Do not publish credentials, production paths, raw
data archives, DuckDB files, or release evidence in Git.

## Historical documentation

Pre-release implementation plans, legacy website material, and one-time deployment summaries are
kept under [docs/archive](docs/archive/README.md) for historical context only. They are not
operational instructions and must not override the documents listed above.

## License and data use

Repository code is private. Public-data attribution and use constraints, including the HCPCS Level I
licensing gate and Open Payments disclaimer requirements, are defined in the
[operating model](docs/data-platform-operating-model.md#data-use-guardrails).
