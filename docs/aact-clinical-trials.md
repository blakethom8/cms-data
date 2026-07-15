# AACT Clinical-Trials Adapter

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
