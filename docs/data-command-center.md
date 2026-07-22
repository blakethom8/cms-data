# Data Command Center

The Data Command Center is the internal observation surface for the CMS public-data platform. It
brings warehouse structure, data grain, source contracts, lineage, validation evidence, and run
history into one place. It is intentionally separate from the Provider Search product UI and from
the legacy public/explorer pages under `frontend/`.

## Current Scope

The static application lives in `dashboard/command-center/` and expects the existing same-origin
reverse-proxy contract: browser requests use `/api`, and the proxy supplies the CMS API credential.
The browser bundle never contains an API key.

The first version provides six workspaces:

1. **Overview** summarizes the live warehouse and available operating evidence.
2. **Provider Evidence** follows named clinicians across raw sources and curated relationship models
   without flattening their different grains into one ambiguous record.
3. **Data Catalog** shows each curated dataset's grain, join keys, columns, and sample rows.
4. **Lineage** connects registered publisher sources to downstream warehouse tables.
5. **Contracts** joins source-registry definitions to the latest local manifest evidence.
6. **Operations** shows recorded runs and the future approval-gated refresh sequence.

The application reads these authenticated endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | API and core-provider readiness |
| `GET /tables` | Live DuckDB table inventory and estimated row counts |
| `GET /explorer/catalog` | Curated dataset names, grain, join keys, and descriptions |
| `GET /explorer/columns/{key}` | Full column names and DuckDB types |
| `GET /explorer/sample/{key}` | Bound, limited sample query for one curated dataset |
| `GET /explorer/provider-evidence` | Whitelisted raw and curated evidence for up to ten NPIs |
| `GET /operations/overview` | Warehouse, manifest, contract, and control-plane summary |
| `GET /operations/sources` | Source contracts plus latest recorded evidence |
| `GET /operations/runs` | Newest-first manifest run ledger |

The `/operations/*` routes only read DuckDB metadata, the typed source registry, and the configured
manifest store. Set `CMS_MANIFEST_PATH` when the production manifest is not located at the repository
default `data/manifests.json`. The API does not return the manifest's filesystem path.

### Local live-data preview

With the existing Hetzner SSH tunnel listening on `127.0.0.1:9080`, run:

```bash
CMS_API_KEY=... python dashboard/command-center/dev_server.py
```

Then open `http://127.0.0.1:4199`. The local server serves the static application and proxies only
browser `GET` and `HEAD` requests under `/api/*` to the CMS API. For catalog samples and provider
evidence, it can translate those browser reads into static, server-owned calls to the deployed API's
read-only `/query` endpoint; this keeps a local dashboard preview usable before matching explorer
routes are deployed. It adds the API key server-side; the credential is never returned to or stored
by the browser application. Use `CMS_API_BASE_URL` to target a different tunnel endpoint.

## Evidence Model

The Command Center distinguishes facts from missing evidence:

- live DuckDB metadata proves which tables and approximate row counts the API can see;
- `pipeline/source_registry.py` defines publisher, cadence, discovery mechanism, source-period
  semantics, downstream tables, and data-use notes;
- a manifest is shown as `validated_active` only when validation passed, promotion is active,
  retrieval is recorded, and the active release ID matches the release ID;
- a missing manifest is reported as missing evidence, never as a successful or current refresh;
- publisher freshness still requires the separate discovery/status workflow. The request-serving API
does not perform live publisher discovery.

## Provider Evidence Workspace

The Provider Evidence workspace is designed to make source semantics visible rather than to invent
a single canonical employer field. It starts with four Cedars-Sinai examples and shows a
source-by-provider matrix. Selecting a cell opens the physical fields from each native row so that
addresses, organization names, enrollment identifiers, source periods, and duplicate rows can be
compared directly.

The sources intentionally remain separate because they make different claims:

- **NPPES** is a provider identity and registration record; its address is not necessarily a billing
  site or employer.
- **DAC National** is a clinician-to-practice/location assertion used by Medicare Care Compare.
- **Revalidation Reassignment** records Medicare benefit reassignment to an organization; it does
  not independently prove employment.
- **PECOS Enrollment** describes an enrolled individual or organization and connects enrollment IDs
  to organization identities.
- **PPEF Reassignment** and **PPEF Practice Location** preserve the assignment and location rows
  from the public PECOS files. The page reports them as unavailable until those optional tables are
  present in the promoted warehouse.
- **Medicare provider-year utilization** is provider-level billing evidence and does not retain a
  provider-by-organization allocation.
- **Practice** and **hospital bridges** are curated relationship models. Their derived or inferred
  status is shown explicitly rather than presenting them as raw source facts.

The browser cannot supply table names or SQL. `GET /explorer/provider-evidence` uses a static source
allowlist and bound NPI parameters, returns at most 25 rows per source/provider combination, and
reports optional missing tables instead of fabricating empty source coverage.

## Control Boundary

The current Command Center is an observation surface. Its refresh control is deliberately disabled.
Acquisition and warehouse mutation must not run inside a FastAPI request handler.

The future operating control plane should be a separately approved operator service that submits a
job and exposes its audit trail. The safe workflow is:

```text
discover -> preview -> acquire -> validate -> build candidate -> compare -> approve -> promote
```

Before enabling a manual refresh button, the control plane must provide:

- authenticated operator authorization distinct from ordinary API read access;
- one-at-a-time locks and idempotent job identifiers;
- a dry-run/preview response naming the exact source and expected release;
- immutable logs, checksums, validation evidence, and code commit provenance;
- explicit approval before promotion and a visible rollback target;
- asynchronous execution so browser or proxy timeouts cannot interrupt a pipeline run; and
- status polling that reports queued, running, failed, awaiting approval, promoted, and rolled-back
  states without guessing.

Until that boundary is implemented and approved, the serving API remains read-only and the Command
Center should explain why execution is unavailable.
