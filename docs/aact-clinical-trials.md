# AACT Clinical-Trials Adapter

> **Last reviewed: 2026-07-22** · **Status: current production adapter guide**

This service exposes the hosted AACT PostgreSQL mirror to Provider Search. It is the request-time
clinical-trials data plane; Provider Search does not call the public ClinicalTrials.gov search API.

## Runtime

- AACT PostgreSQL: `127.0.0.1:5433`, private to the data box
- Read-only connection environment: `/etc/aact/reader.env`
- Promoted snapshot marker: `/srv/aact/CURRENT_SNAPSHOT`
- API process: `cms-api.service`
- Refresh entry point: `/usr/local/sbin/aact-refresh` or `aact-refresh.service`

The `cms-api.service` systemd drop-in loads `/etc/aact/reader.env`. The application router is
included with the service's existing X-API-Key dependency.

## Endpoints

- `GET /clinical-trials/version`
- `GET /clinical-trials/studies`

`/studies` requires exactly one of `query.cond`, `query.intr`, or `query.term`. It accepts
`filter.overallStatus`, `pageSize`, and either a `filter.geo=distance(lat,lng,Nmi)` market filter
or the site filters `query.locn`, `query.city`, and `query.state`.

The response intentionally mirrors only the ClinicalTrials.gov v2 fields consumed by Provider
Search. Extend the adapter deliberately when the application needs another field; do not turn it
into an unrestricted SQL or database proxy.

## Query matching

`query.cond`, `query.intr`, and `query.term` use case-insensitive substring matching against AACT
tables. Before matching, the adapter expands common US/UK medical spelling variants (for example
`orthopedic` ↔ `orthopaedic`, `pediatric` ↔ `paediatric`) so American and British registry text
both hit. This is literal OR expansion of needles — not ClinicalTrials.gov Essie synonym search
and not a wild-card stemmer.

Provider Search may further expand specialty-like topics into multiple condition queries before
calling this adapter. That product interpretation lives in Provider Search, not here.

## Verification

```bash
docker exec aact-postgres pg_isready -U aact_reader -d aact
cat /srv/aact/CURRENT_SNAPSHOT
systemctl status cms-api.service --no-pager
curl -fsS http://127.0.0.1:8080/clinical-trials/version \
  -H "X-API-Key: ${CMS_API_KEY}" | jq
```

Also verify that the same endpoint without the header returns `401`. A normal version response
contains `apiVersion`, `dataTimestamp`, `snapshotDate`, `studyCount`, and `source`.

## Refresh safety

Refreshes must restore into staging, validate row counts and representative queries, and promote
only after validation succeeds. The current refresh is operator-triggered. Preserve the previous
snapshot until the replacement has passed application smoke tests.

The public-data acquisition and restore-artifact preparation steps are now repository-owned:

```bash
.venv/bin/python -m pipeline.data_platform acquire aact_clinical_trials_snapshot \
  --data-root <staging-data-root> --json

.venv/bin/python -m pipeline.data_platform prepare-aact-release \
  --environment staging \
  --source-run-id <validated-aact-run-id> \
  --data-root <staging-data-root> \
  --output-root <immutable-artifact-root> --json

.venv/bin/python -m pipeline.data_platform stage-aact-database \
  --environment staging \
  --release-manifest <immutable-artifact-root>/aact-releases/<release-id>/release.json \
  --restore-log <new-absolute-log-path> \
  --evidence <new-absolute-evidence-path> --json
```

Preparation revalidates the acquired ZIP, extracts only `postgres.dmp` and
`data_dictionary.csv`, verifies PostgreSQL custom-dump magic, hashes both files, and seals the
versioned directory. It does not connect to PostgreSQL or change `CURRENT_SNAPSHOT`.
`stage-aact-database` refuses an existing candidate name, restores only to a release-derived
`aact_candidate_*` database, grants the read-only role, validates study/table counts and the latest
update date, and records immutable evidence. It has no database-drop, rename, or promotion path.
Run representative searches and the temporary API smoke suite before any database rename. AACT
promotion is a coordinated PostgreSQL operation, not a DuckDB pointer change;
stop the API during a combined cutover so no mixed DuckDB/AACT release is served, and retain the
previous PostgreSQL database until the complete smoke suite passes.
