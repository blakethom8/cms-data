# Arbitrary SQL access boundary

> Issue: [#19](https://github.com/blakethom8/cms-data/issues/19)
> Status: implementation and consumer migration in progress
> Last reviewed: 2026-08-13

## Decision

`POST /query` is a legacy operator compatibility route, not a product API. A valid general API key
is no longer sufficient: the resolved scoped-key name must also appear in
`CMS_QUERY_CONSUMERS`. The deployment default is `command-center`; an empty value disables the
route for every consumer. The compatibility names `shared` and `open` are deliberately absent, so
neither the legacy shared credential nor unauthenticated development mode grants SQL access.

The endpoint accepts one parsed `SELECT`, applies unavoidable query-text, outer-row, and serialized
response-size limits, and rejects DuckDB control statements, unbounded row generators, extension
loading, attachment, filesystem/network readers, secret introspection, and multiple statements.
The serving connection also disables DuckDB external access below the SQL-text guard. This is
defense in depth for an operator-only route, not a reason to expose SQL to a browser or product
credential.

`GET /tables/{table_name}/schema` now confirms the requested name against DuckDB's `main`-schema
inventory before quoting it as an identifier. A caller cannot turn the path segment into SQL.

## Caller inventory and disposition

| Caller | Class | Credential/location | Disposition |
|---|---|---|---|
| `dashboard/command-center/dev_server.py` provider-evidence and sample compatibility bridges | local operator | server-side dedicated `command-center` scoped key | Temporarily allowed. These are static, server-authored queries used only while the deployed explorer API trails the Command Center checkout. Remove each bridge after its typed explorer/evidence route is deployed. |
| `provider-search/api/capture_lab/runner.py` facility-affiliation batch | product workflow | Provider Search `CMS_API_KEY` (`ps-prod` or `ps-dev`) | Replace with typed `GET /profiles/hospital-affiliations`; product keys must receive 403 from `/query`. |
| `dashboard/index.html` preset explorer queries and SQL console | legacy browser dashboard | browser-supplied/general key | Retire direct SQL. Presets move to curated explorer endpoints; the free-form console is operator tooling and must not be publicly published. |
| `dashboard/compare.html` provider comparison queries | legacy browser comparison page | browser/no dedicated operator identity | Obsolete as a serving contract. Use typed search, profile, and practice endpoints if the page is retained. |
| `frontend/access/index.html` `/query` example and endpoint card | obsolete public documentation | encourages browser/general-key use | Remove the advertised SQL contract; document reviewed typed endpoints instead. |
| `api/test_production_smoke.py` fake `/query` response | smoke fixture | no live consumer | Keep only while the rollout smoke explicitly verifies the operator allow/deny matrix; it does not justify product access. |

Repository-wide inspection found no other runtime `/query` call in Provider Search. Its generic
`CmsClient.query()` method exists only to support the facility-affiliation call above and should be
removed after that migration. References to the public CMS Provider Data Catalog URL ending in
`/datastore/query` are a different upstream API and are out of scope.

## Configuration contract

```text
CMS_API_KEYS=ps-prod:<value>,ps-dev:<value>,command-center:<value>
CMS_QUERY_CONSUMERS=command-center
MAX_QUERY_SECONDS=15
DUCKDB_MEMORY_LIMIT=2GB
DUCKDB_THREADS=4
DUCKDB_POOL_SIZE=4
DUCKDB_POOL_ACQUIRE_SECONDS=2
MAX_QUERY_SQL_CHARS=20000
MAX_QUERY_RESPONSE_BYTES=1000000
```

- Consumer names, never key values, appear in `CMS_QUERY_CONSUMERS`.
- `MAX_QUERY_SECONDS` interrupts runaway operator work; the default is 15 seconds.
- `DUCKDB_MEMORY_LIMIT` and `DUCKDB_THREADS` bound the serving process as a whole. DuckDB applies
  these settings database-wide, so they are configured once when the read-only connection opens.
- `DUCKDB_POOL_SIZE` bounds concurrent database-only route execution. Each leased read-only
  connection has one request owner, and `DUCKDB_POOL_ACQUIRE_SECONDS` bounds queue wait before a
  controlled 503 response.
- Provider Search keys authenticate typed endpoints but are not SQL operators.
- The Command Center key remains server-side. It must not be embedded in static HTML or returned to
  the browser.
- Do not add `shared`, `open`, `ps-prod`, or `ps-dev` to the SQL allowlist.

## Rollout and verification

1. Deploy the typed hospital-affiliation endpoint while existing `/query` behavior remains live.
2. Migrate Provider Search and verify its capture workflow uses only the typed GET endpoint.
3. Confirm request logs show no `ps-prod` or `ps-dev` calls to `/query` for an agreed observation
   window.
4. Confirm the Command Center uses its dedicated scoped key and its compatibility reads succeed.
5. Deploy the SQL boundary with `CMS_QUERY_CONSUMERS=command-center` explicitly set.
6. Require smoke evidence that `command-center` can run a bounded `SELECT`, `ps-prod` receives 403,
   an invalid key receives 401, and attach/file/extension/multi-statement probes receive 403.
7. Remove the legacy browser SQL documentation and surfaces from any published artifact.
8. After typed Command Center routes are deployed, set `CMS_QUERY_CONSUMERS` empty and remove the
   route in a later compatibility release.

No production configuration change, restart, or credential rotation is authorized by merging the
code. Those remain approval-gated operations.

## Rollback

If the product migration fails, roll Provider Search back to its preceding release while leaving the
SQL boundary undeployed. If the boundary itself fails after deployment, roll the CMS serving bundle
back to the preceding immutable release. Do not restore product access by adding `ps-prod`, `ps-dev`,
`shared`, or `open` to `CMS_QUERY_CONSUMERS`; that would recreate the security defect under incident
pressure. Preserve the failed release and request evidence for diagnosis.
