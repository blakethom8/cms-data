# Utilization search serving contract

> **Status:** independent utilization release and API contract active in the development serving
> environment; commercialization and licensing review remains a release gate

`/utilization/*` is the inverted Medicare discovery surface for Provider Search Cases. It ranks
individual NPIs by a selected HCPCS or Part D drug basket and assigns each NPI's national totals to
the one NPPES-primary location in `serving_practice_nppes_provider_sites`. The location is a map and
territory attribution, not a claim service location. Every search response carries
`metric_scope: national_npi_totals`.

The serving data lives in an immutable, self-contained `utilization.duckdb` sidecar. It contains
only the NPPES-primary provider/search dimension, utilization denominators, procedure and drug
facts, and the two typeahead dictionaries. The API opens it through a separate bounded connection
pool; unrelated routes continue to open the selected warehouse. This avoids rebuilding or changing
the large evidence marts in the main warehouse.

The compact `utilization_procedure_dictionary` and `utilization_drug_dictionary` tables own
typeahead aggregates. Search facts remain in the sidecar's `provider_service_detail` and
`provider_drug_detail`; there is no additional provider-level inverted fact table. Procedure
payments are estimates computed as services multiplied by average Medicare payment. J-code rows
carry `is_drug_code=true`, because their service units must not be described as cases. Part D
retains the publisher's suppression of small cells.

## Routes

- `GET /utilization/procedures/options?q=33249&limit=10`
- `GET /utilization/procedures/search?hcpcs=33249&city=Denver&state=CO`
- `GET /utilization/drugs/options?q=eliquis&limit=10`
- `GET /utilization/drugs/search?brands=Eliquis&city=Denver&state=CO`

Search requires a non-empty basket plus city/state, a ZIP boundary, or a latitude/longitude radius.
Baskets are capped at 50 values and results at 200 NPIs. Procedure and drug modes are separate.

## HCPCS description release gate

Numeric HCPCS Level I descriptions can contain AMA CPT content. The safe default therefore returns
no procedure descriptions and does not match description text. On 2026-08-18, the development
serving environment explicitly enabled `HCPCS_DESCRIPTIONS_ENABLED=true` so Provider Search can be
built and evaluated against the intended end-state procedure discovery experience. This
development opt-in does not settle commercialization rights: a formal licensing review or approved
description filter remains required before a commercial production release. Code lookup remains
available whenever the gate is closed.

## Confirmed non-goals in the active warehouse

- There is no physician-level ICD-10 diagnosis-volume fact or API. Chronic-condition percentages
  in `utilization_metrics` are panel mix, not reverse-searchable diagnosis claims.
- There is no versioned county, ZCTA-to-county, or MSA crosswalk. Geography remains city/state,
  ZIP boundaries, and radius.
- The active Part B service and Part D drug facts contain the 2024 measurement year only. The
  schema retains `data_year`, but there is no multiple-vintage series for adoption scoring.

Adding ICD, county, or longitudinal adoption requires separate source and serving contracts; these
routes intentionally expose none of those parameters.

## Independent release build

Build from one selected, validated warehouse. The source database is attached read-only, each
large `CREATE TABLE AS` commits independently, indexes are created after the loads, and build spill
must be placed on a non-production volume:

```bash
python -m pipeline.utilization_releases build \
  --data-root /mnt/UTILIZATION_VOLUME/cms-data-utilization \
  --source-warehouse /srv/cms-data-platform/production/release-current/warehouse \
  --source-release-manifest \
    /srv/cms-data-platform/data/releases/WAREHOUSE_RELEASE_ID/release.json \
  --spill-root /mnt/UTILIZATION_VOLUME/cms-data-utilization-spill \
  --memory-limit-gb 16 \
  --threads 1 \
  --json
```

The builder fails closed on source hash/size mismatch, missing schema, duplicate or orphan keys,
geography defects, aggregate reconciliation differences, empty query smoke, or any DuckDB error.
It writes a sealed `release.json`, `comparison.json`, and `utilization.duckdb`. A failed release is
never eligible for preparation:

```bash
python -m pipeline.utilization_releases verify \
  --data-root /mnt/UTILIZATION_VOLUME/cms-data-utilization \
  --utilization-release-id UTILIZATION_RELEASE_ID \
  --json
```

Promotion copies the sealed database to an independent immutable inode under
`production-artifacts/utilization/`, then adds an optional `utilization` link to the atomic release
bundle. The production manager verifies both staging and production hashes before preparation.
Serving code discovers that sibling link at process start; bundles without it fall back to the
warehouse, preserving rollback compatibility. Production smoke automatically exercises all four
utilization endpoints and proves that the selected process has the sidecar file open.
