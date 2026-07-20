# Production Warehouse Promotion Runbook

## Scope and Approval Gate

This runbook promotes an already validated immutable warehouse release. It does not download source
data, rebuild a release, or overwrite the active DuckDB file. The production pointer, systemd unit,
and API process must not be changed until an operator gives explicit approval for the named warehouse
release ID, database SHA-256, and pipeline Git commit.

The current service starts from a mutable checkout and reads its database path from a protected
environment file. The first production promotion therefore includes a one-time migration to
versioned code and warehouse pointers. Prepare and review that change separately from executing it.

## Required Evidence

Before requesting approval, capture all of the following in the handoff:

- the full pipeline Git commit and a clean immutable server checkout at that commit;
- the source run ID, publisher version, source period, source artifact SHA-256, and validation state;
- the warehouse release ID, DuckDB runtime version, byte size, SHA-256, and validation timestamp;
- `comparison.json` with state `passed`, no unexpected table-count differences, and representative
  hospital affiliations reviewed;
- a checksum-verified backup of the active database that opens read-only;
- staging API health, capability, representative provider, and affiliation-query results;
- production API results for the same read-only requests, with expected differences explained;
- available disk sufficient to retain the active release, the candidate, and rollback copies.

Any missing or contradictory item stops the promotion. File modification time is not provenance.

## Preflight

Run these checks from the immutable candidate code checkout. Substitute explicit, reviewed paths;
do not use a broad glob or an unresolved path as a copy, move, or link target.

```bash
python -m pipeline.data_platform compare-release \
  --environment staging \
  --data-root /srv/cms-data-platform/data \
  --warehouse-release-id WAREHOUSE_RELEASE_ID \
  --backup-manifest /srv/cms-data-platform/backups/BACKUP_ID/backup-manifest.json \
  --json
```

Confirm separately that:

1. the candidate and backup SHA-256 values equal their manifests;
2. the candidate database is a regular, immutable file rather than a symlink;
3. the staging pointer resolves to the approved candidate;
4. the production process still has the expected active database open;
5. no staging path is referenced by the production service;
6. `duckdb.__version__` is `1.4.4` in the API runtime; and
7. no pending promotion-journal transaction exists.

## Staging API Comparison

Start an ephemeral API process on a loopback-only unused port with `DUCKDB_PATH` set to the explicit
candidate file. Do not reuse or restart the production process. Compare these read-only requests
against the production API:

- `/health`;
- `/practices/capabilities`;
- `/tables`;
- a bounded `/query` for `core_providers`;
- a bounded `/query` joining `hospital_affiliations` to provider identity; and
- representative provider/profile requests for NPIs recorded in release validation evidence.

Record status codes and normalized response summaries. Expected hospital-table additions are allowed;
missing baseline capabilities, provider-count drift, 5xx responses, or unexplained schema differences
stop the promotion.

## One-Time Production Pointer Design

The approved target layout is:

```text
/srv/cms-data-platform/production/code-current       -> ../code/FULL_GIT_COMMIT
/srv/cms-data-platform/production/warehouse-current  -> ../data/releases/WAREHOUSE_RELEASE_ID/warehouse.duckdb
```

The systemd unit should use the immutable code pointer as its working directory and the immutable
warehouse pointer as `DUCKDB_PATH`. Apply both pointer changes atomically with temporary symlinks and
`rename(2)` semantics. Do not replace, truncate, copy over, or open the old active DuckDB read-write.

The unit change must be reviewed before installation. After installation, use `systemctl daemon-reload`
and one controlled API restart. A restart is the only expected production interruption; do not combine
it with source acquisition or candidate construction.

## Promotion Verification

Immediately after the approved restart:

1. verify the service is active and its PID is new;
2. verify the process resolves the approved code and warehouse pointers;
3. run the staging comparison request set against production;
4. confirm the source/status evidence identifies the approved release rather than file timestamps;
5. watch service logs for DuckDB, import, schema, authentication, and 5xx errors; and
6. preserve the prior code target, prior warehouse target, backup, and manifests for rollback.

Do not declare success based only on `systemctl is-active`; the read-only API requests are required.

## Rollback

Rollback is an atomic pointer reversal followed by one controlled API restart. Restore both the prior
code pointer and prior warehouse pointer recorded during preflight, then repeat the health and query
checks. If the original deployment did not use pointers, restore the protected environment and unit
configuration captured before migration; never copy the backup over the active database in place.

Record the failed release, safe error summary, rollback timestamp, restored targets, and verification
results. Keep the failed immutable candidate for diagnosis unless retention policy explicitly removes
it after the incident review.

## Stop Conditions

Stop without changing production when any of these is true:

- explicit approval names a different commit, release ID, or SHA-256;
- a checksum, source period, runtime version, or row-count comparison differs from the evidence;
- the candidate cannot answer the complete read-only smoke suite;
- the active production database or process changes during preflight;
- disk headroom is insufficient for both promotion and rollback; or
- the prior targets and rollback procedure are not independently verifiable.
