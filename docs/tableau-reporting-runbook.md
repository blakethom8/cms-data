# Tableau Reporting Replica

## Purpose and boundary

The Tableau replica is a disposable, downstream data product for operator analysis. The promoted
DuckDB warehouse remains canonical, and FastAPI remains the Provider Search serving interface. The
reporting publisher never updates DuckDB, never runs inside an API request, and never reuses the AACT
PostgreSQL database or the Provider Search/Supabase database.

The first scope is California. Los Angeles County is deliberately deferred until a versioned county
boundary and address-geography matching policy are available. `City = 'Los Angeles'` is not a valid
county predicate.

```text
approved immutable DuckDB release (read-only)
    -> build-specific PostgreSQL schema
    -> row/grain/scope/relationship validation
    -> transactional view switch
    -> reporting + source_detail + control schemas
    -> SSH tunnel -> Tableau Desktop
```

## What Tableau receives

The `reporting` schema contains certified analytical models:

| View | Declared grain |
| --- | --- |
| `dim_provider` | one current individual provider NPI |
| `bridge_provider_location` | one provider enrollment by DAC address identifier and organization |
| `fact_provider_metrics_year` | one provider NPI by Medicare metric year |
| `fact_provider_quality_year` | one provider NPI by available QPP data year |
| `bridge_provider_hospital` | one provider NPI by inferred hospital NPI and data year |

The `source_detail` schema preserves every column loaded in the California slice of `raw_nppes`,
`raw_dac_national`, and `raw_physician_by_provider`. These are evidence tables, not automatically
join-safe facts. The NPPES raw loader itself retains a selected subset of the publisher's 329 columns;
the original downloaded artifact remains the complete source record.

The `control` schema exposes the active release, model catalog, declared grain, source period
semantics, and column-level lineage. Every curated column identifies its source table/column,
transformation, and whether it is derived or inferred.

## Preflight: no writes

Run profiling against the explicit immutable release database before provisioning PostgreSQL:

```bash
cd /srv/cms-data-platform/production/release-current
runtime/bin/python -m pipeline.reporting_export profile \
  --duckdb /srv/cms-data-platform/data/releases/WAREHOUSE_RELEASE_ID/warehouse.duckdb \
  --json
```

Confirm all expected models are present and non-empty where applicable. Record California row counts
and estimated export size. The publish command refuses to begin when the temporary filesystem has
less than 15 GiB free; operators must also preserve room for the active DuckDB release, reporting
build, prior reporting build, and warehouse rollback artifacts.

## Provision the isolated PostgreSQL service

1. Create `/srv/cms-data-platform/reporting/postgres` and
   `/srv/cms-data-platform/reporting/tmp`, owned by the reporting service account.
2. Install `/etc/cms-data/tableau-postgres.env` from
   `deploy/tableau-postgres/postgres.env.example`, generate a unique password, and set mode `0600`.
3. Review `deploy/tableau-postgres/docker-compose.yml`. It binds PostgreSQL only to
   `127.0.0.1:5434`; do not add a public firewall rule.
4. Start the container and verify the actual listener remains loopback-only.
5. Run `schema/reporting_roles.sql` as the database administrator, then set unique passwords for
   `cms_reporting_loader` and `tableau_reader` outside Git.
6. Transfer ownership of `control`, `reporting`, and `source_detail` to the loader, or run the first
   schema initialization as `cms_reporting_loader` after granting it database `CREATE`.

Do not reuse port `5433`, which belongs to the private AACT mirror.

## Manual first publication

Install `/etc/cms-data/tableau-reporting.env` from the example with the loader DSN in
`CMS_REPORTING_DSN`. The publisher resolves the one verified deployment selected by the immutable
production `release-current` control plane; it does not use the separately active staging release.

Then run the oneshot service manually:

```bash
sudo systemctl start cms-tableau-reporting.service
sudo journalctl -u cms-tableau-reporting.service -n 200 --no-pager
```

The publisher performs these gates:

1. requires a clean production control plane with no pending transition;
2. requires exactly one verified deployment selected by `release-current`;
3. verifies the selected bundle, release evidence, database byte size, and SHA-256;
4. opens DuckDB read-only and exports the declared California contracts;
5. loads a unique `reporting_build_*` PostgreSQL schema;
6. applies primary-key, row-count, California-scope, and orphan-NPI checks;
7. records model/column lineage and validation evidence; and
8. switches stable views in one PostgreSQL transaction.

A failure before step 8 leaves the prior Tableau views untouched and records a safe failure summary.
Re-running the same release/checksum/contract is an idempotent no-op.

Do not enable `cms-tableau-reporting.timer` until the manual publication, failure drill, and rollback
drill have passed. A reporting run can only follow a warehouse release after that release has passed
the production promotion process and become the verified `release-current` selection.

## Verification

As the loader/administrator:

```sql
SELECT snapshot_id, warehouse_release_id, warehouse_sha256, status, published_at
FROM control.active_reporting_snapshot;

SELECT layer, model_name, declared_grain, row_count, scope_rule
FROM control.model_catalog
WHERE snapshot_id = (SELECT snapshot_id FROM control.active_reporting_snapshot)
ORDER BY layer, model_name;

SELECT model_name, model_column, source_table, source_column,
       transformation, is_derived, is_inferred
FROM control.column_lineage
WHERE snapshot_id = (SELECT snapshot_id FROM control.active_reporting_snapshot)
ORDER BY model_name, ordinal_position;
```

Verify `tableau_reader` can select those three control surfaces and every published view, but cannot
create, update, truncate, or drop anything. Verify `ss -lntp` shows port `5434` only on `127.0.0.1`.

## Tableau connection

On the Mac:

```bash
ssh -NT -L 5434:127.0.0.1:5434 hetzner2
```

In Tableau Desktop use the native PostgreSQL connector:

```text
Server: localhost
Port: 5434
Database: cms_tableau
User: tableau_reader
```

Use Tableau relationships on `npi` (and the applicable year), not physical joins. Adding multiple
locations to a sheet must not multiply provider-level Medicare measures. Keep the source period or
metric year visible in analyses that combine NPPES, Medicare, QPP, or later Open Payments data.

## Rollback

Only previously published (`active` or `superseded`) snapshots are eligible:

```bash
runtime/bin/python -m pipeline.reporting_export rollback \
  --snapshot-id PREVIOUS_SNAPSHOT_ID \
  --postgres-dsn-env CMS_REPORTING_DSN
```

Rollback transactionally repoints all stable reporting and source-detail views. It does not alter
DuckDB. Retain the active and previous build schemas until the next release has been verified in
Tableau; removal of older build schemas is a separate, explicit capacity-management operation.

## Future LA County scope

Create LA County only after geocoded addresses are joined to a versioned authoritative county
boundary (FIPS `06037`). Preserve boundary version, match method, and match status. Unmatched
addresses must remain `unknown`, not be silently classified outside the county. Geography is
source-specific: NPPES practice address, DAC practice address, Medicare rendering address, and Open
Payments recipient address can disagree and must retain separate scope flags.
