# Repository Guidelines

> **Last reviewed: 2026-07-22**

## Project Structure & Module Organization

`pipeline/` owns public-data acquisition and DuckDB transformations. `schema/ddl.sql` defines the
warehouse tables used by both pipeline code and the API. `api/` is a read-only FastAPI service over
the promoted DuckDB database; its route modules are imported with `api/` on `sys.path`, so run API
tests from that directory. `docs/` contains architecture and operating decisions. `dashboard/` and
`frontend/` are static legacy/explorer surfaces, not the Provider Search product UI.

`cms-public-data-catalog` is a metadata-only external gitlink. Do not add ingestion logic there or
treat it as the source of runtime data. Bulk data, DuckDB files, archives, credentials, and logs are
gitignored and must stay outside commits.

## Build, Test, and Development Commands

The repository has no root package/build configuration. A validated local setup is
`uv venv --python 3.13 .venv && uv pip install --python .venv/bin/python -r api/requirements-dev.txt`.

- Full tests: `cd api && ../.venv/bin/python -m pytest -q`
- Single file: `cd api && ../.venv/bin/python -m pytest test_clinical_trials.py -q`
- Local API: `cd api && DUCKDB_PATH=../data/provider_searcher.duckdb ../.venv/bin/python -m uvicorn main:app --reload --port 8080`
- NPPES CLI help: `.venv/bin/python -m pipeline.nppes --help`
- Open Payments CLI help: `.venv/bin/python -m pipeline.open_payments --help`

`pipeline.acquire`, `pipeline.load`, `pipeline.transform`, and `pipeline.scoring` currently expose
Python functions but no working CLI entry point; do not claim otherwise in documentation.

## Coding Style & Data Boundaries

Use modern Python type hints (`str | None`, `list[str]`), module loggers, and small source-specific
functions. Use relative imports inside `pipeline/`. Keep the serving process read-only: writes and
refreshes belong to staging pipeline runs, never API request handlers. Preserve NPI as the provider
identity key and keep source-period fields distinct from ingestion timestamps.

Follow `docs/data-platform-operating-model.md` for discovery, manifests, validation, promotion, and
rollback. Never overwrite the active production DuckDB file in place.

## Testing Guidelines

Tests use pytest and in-memory DuckDB fixtures. Add focused tests beside the API modules as
`api/test_*.py`. Pipeline changes must test source discovery and validation with small fixtures;
full public datasets are not test fixtures.

## Commit & Security Guidelines

Recent history uses conventional commits such as `feat(api):`, `fix(api):`, and `feat(industry):`.
Keep commits scoped by subsystem. Never commit API keys, environment files, downloaded public-data
archives, or generated databases. Treat CPT/HCPCS Level I commercialization as license-sensitive;
see the operating model before exposing procedure codes or descriptions.
