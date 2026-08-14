# S2 release-built serving marts plan

> Status: S2.1-S2.3 merged; S2.4 prerequisite in progress — managed DAC acquisition and an isolated
> same-source raw/mart candidate
>
> Production boundary: do not select, rebuild, amend, or supersede prepared S1 deployment
> `deployment-20260814T002255Z-11131e3630`. S2 candidates remain isolated and unselected until a
> separately approved cutover.

## Decision

The twelve registered warehouse marts and the proposed S2 serving marts are different layers.

The registered marts already exist in the current warehouse:

1. `core_providers`
2. `practice_locations`
3. `utilization_metrics`
4. `industry_relationships`
5. `hospital_affiliations`
6. `provider_service_detail`
7. `provider_drug_detail`
8. `provider_quality_scores`
9. `order_referring_eligibility`
10. `kol_summary`
11. `nppes_radar_provider_state`
12. `nppes_radar_events`

They are curated, source-oriented foundations. Their presence does not prove that provider,
practice, market, or industry routes should consume them. Several routes still perform large raw
joins and aggregations at request time. S2 will first give the twelve existing marts explicit,
machine-validated contracts, then add route-specific serving marts only where measured plans show a
material benefit.

Explorer evidence remains source-faithful and is not replaced by a summary mart. Clinical Trials is
an independent AACT/PostgreSQL plane and is outside this DuckDB work.

## Phase 0 — freeze identity and measure current paths

Leave the prepared S1 bundle and selected production release untouched. Against an immutable copy of
the current warehouse, capture canonical SQL and `EXPLAIN ANALYZE` evidence for:

- provider profile;
- practice search using both `cms_enrollment` and `nppes_primary` location bases;
- practice providers and site profile;
- market snapshot;
- industry search, options, and detail;
- Radar providers; and
- the committed mixed workload.

Each case records deployment/release identity, warehouse SHA-256, code commit, DuckDB version,
executor settings, exact parameter identity, SQL hash, result count/digest, operator time and
cardinality, spill/temp use, latency, response bytes, CPU, RSS, and pool wait. End-to-end comparisons
use the existing three-trial median contract at concurrency 1/2/4/8/12. SQL profiles diagnose cost;
they do not replace HTTP performance evidence.

The case corpus includes representative providers and release-selected adversarial cases:

- maximum Part B service fanout;
- maximum Part D drug fanout;
- maximum Open Payments fanout;
- maximum location/group/hospital fanout;
- NPPES-only provider with optional data absent;
- empty and missing-provider behavior;
- broad and boundary practice geography/specialty cases;
- a ten-NPI Explorer evidence request; and
- broad Radar ZIP/taxonomy filters.

## Phase 1 — contract and validate the twelve registered marts

Add `pipeline/mart_contracts.py` with one `MartSpec` per registered mart. Each contract declares:

- physical table and owning transform;
- exact grain and uniqueness key;
- upstream tables and source families;
- source-period and provenance policy;
- required columns and null semantics;
- uniqueness, cardinality, orphan, and value-domain validations;
- semantic kind: evidence-preserving, bridge, summary, or product-serving; and
- routes authorized to consume it.

Use the catalog as the authoritative mart registry:

- `pipeline/lineage.py` derives classification and graph edges from it;
- `pipeline/releases.py` runs every applicable validation and stores results in release
  `validation_details`;
- `api/operations.py` reports registered, available, validated, and route-authorized counts
  separately; and
- `docs/product-serving-marts.md` publishes a compact contract table.

Do not invent row-level provenance when only release/manifest provenance exists. Declare its actual
scope. Missing evidence is `unknown` and blocks route authorization; file modification time is never
used as a source period.

Phase 1 changes no API query path.

### Phase 1 tests

- `api/test_mart_contracts.py`: all twelve specs are unique, physical, keyed, lineage-complete, and
  documented.
- `api/test_transform.py`: small-fixture uniqueness, orphan, null, source-period, and provenance
  validation.
- `api/test_releases.py`: complete per-mart validation evidence and fail-closed candidate rejection.
- `api/test_operations.py`: distinct registered/current/validated/serving-authorized counts.

## Phase 2 — first vertical slice: default practice search

The first serving mart is `serving_practice_provider_sites`, initially for
`GET /practices/search?location_basis=cms_enrollment`. This path repeatedly scans and groups DAC,
Part B, Part D, and geocode data and can be switched independently while retaining the
`nppes_primary` implementation.

Grain: one row per `(site_key, npi)`. `site_key` includes normalized address, ZIP, and organization
or solo identity. The row contains the existing response inputs: organization identity/name,
address/phone/geography, provider specialty and identity, group size, Part B and Part D measures,
geocode, source periods, and source run IDs. Unknown values remain `NULL`; absence is not rewritten
as zero or empty text.

Implementation locations:

- `schema/ddl.sql`: table, key, and selective indexes;
- `pipeline/transform.py`: `build_serving_practice_provider_sites()`;
- `pipeline/mart_contracts.py` and `pipeline/lineage.py`: contract and dependencies;
- `pipeline/releases.py`: an explicit targeted-additive serving-mart build/comparison policy; and
- `api/practices.py`: mart-backed implementation behind an internal selector while retaining the raw
  implementation as the parity oracle.

The targeted release flow copies and verifies an immutable baseline, applies only serving-mart DDL,
builds only the dependency closure under bounded resources, validates the contract, proves every
non-allowlisted table unchanged, records complete comparison evidence, and seals a new candidate.
It never promotes or changes production.

Do not reuse an unrelated publisher-source comparison fallback. Add an explicit policy such as
`serving_marts_additive_v1` with an exact changed-table allowlist.

### Phase 2 parity cases

Tests cover duplicate DAC records, solo sites, group sites, multiple specialties, missing Part B,
missing Part D, missing geocode, malformed geography, normalized organization identifiers,
multi-door providers, empty results, maximum limits, proximity boundaries, ordering, totals,
pagination/truncation, site IDs, classifications, and null behavior.

Extend:

- `api/test_transform.py`;
- `api/test_practice_contract_invariants.py`;
- `api/test_primary_locations.py`;
- `api/test_market_snapshot.py` to prove unchanged market semantics;
- `api/test_releases.py`; and
- the complete API suite and response-shape snapshot tests.

## Route-switch acceptance gate

A route may consume a new serving mart only when all conditions pass:

1. Exact response status, shape, ordering, totals, pagination, truncation, source-period,
   suppression, and null-semantics parity over the complete corpus. Numeric tolerance is no wider
   than the route's published rounding precision.
2. Zero duplicate keys, required-column nulls, invalid NPIs/state/ZIP values, upstream orphans, or
   unexplained provenance gaps.
3. Three warm, comparable trials with identical release, executor settings, workload bytes/hash,
   request counts, concurrency levels, and timeout.
4. Either at least 20% lower route p95 at both concurrency 1 and 12 without throughput/failure
   regression, or at least 30% less operator work/CPU/spill with p95 no worse than 5%.
5. No new timeouts or 5xx responses; RSS remains inside the measured bounded-executor envelope.
6. Capacity projection remains below the configured promotion block.
7. Raw tables and evidence routes remain unchanged and queryable.
8. OpenAPI and response models remain unchanged; cache invalidation occurs only through normal
   release/deployment identity.

If a mart fails the performance gate, retain it only when it materially improves semantics or
governance. Otherwise do not ship it.

## Later vertical slices

Proceed one measured slice at a time:

1. NPPES-primary practice sites and provider-organization membership.
2. Provider header, location, group, and hospital projections.
3. Provider utilization summary, top services, and top drugs.
4. Industry provider summary and manufacturer/product/nature facets.
5. Market ZIP/specialty/provider-site rollups, preserving distinct-NPI attribution so multi-door
   providers are never double counted.

Radar already uses release-built state/event tables and receives another mart only if its measured
plan justifies one. Provider fuzzy search remains separate because precomputation must preserve its
ranking and discovery-universe semantics.

## Delivery sequence

- PR S2.1: mart contract catalog, twelve contracts, validation framework, operations reporting, and
  documentation.
- PR S2.2: checked-in canonical case manifest and baseline plan/performance evidence.
- PR S2.3: practice serving-mart DDL, transform, lineage, targeted release policy, and parity oracle.
- PR S2.4: isolated warehouse candidate, comparison evidence, exact-bundle smoke, and route-switch
  recommendation.

Every PR is independently testable and reversible. No S2 PR authorizes a production cutover.

S2.2 was delivered in two fail-closed increments: the first registered and captured the exact HTTP
case corpus with immutable warehouse identity; the second attached per-query `EXPLAIN ANALYZE`
evidence to those same case IDs. A response timing is never represented as an operator plan. The
result also established a response-stabilization prerequisite before the Phase 2 parity oracle can
be authoritative.

That prerequisite keeps the v3 response shape intact while defining cent precision, complete list
and pagination ordering, deterministic market-site representative values, and source-faithful
Explorer row ordering. It passed the full API suite and all fourteen canonical cases passed three-
trial byte-stability checks against the immutable selected warehouse. See the
[stabilization record](../operations/s2-parity-oracle-stabilization-2026-08-14.md). This acceptance
does not authorize a route switch, candidate selection, or production cutover.

S2.3 implements the serving table and indexes, an idempotent targeted transform, row and release
provenance validation, lineage registration, and an internal raw/mart API selector whose default
remains `raw`. Fixture parity covers state, multi-ZIP/multi-specialty, proximity/limit, and empty
results with byte-exact response comparison. The targeted additive release builder requires a
named validated baseline with a matching verified-backup digest, inherits its source-run identity,
and allowlists only the serving table. Its comparison scans schema and order-independent logical
row fingerprints for every non-allowlisted table in addition to row counts. This implementation is
not route-authorized; S2.4 must still produce and evaluate an isolated production-data candidate.

The first production audit found a fail-closed provenance prerequisite. The selected warehouse's
Part B and Part D raw tables carry managed run, release, period, and ingestion fields, but its legacy
`raw_dac_national` table carries none. No historical manifest records an exact DAC publisher period
or resource identity, so the missing values cannot be reconstructed honestly. S2.4 therefore first
registers the official CMS Provider Data Catalog dataset `mj5m-pzi6`, validates its exact 31-column
CSV contract, and acquires it as an immutable managed run. The isolated candidate will replace only
that raw DAC table, build `serving_practice_provider_sites` from DAC and the baseline's exact Part B
and Part D rows, and compare raw versus mart responses within the same candidate. This prerequisite
does not select a warehouse, change a route, or authorize cutover.
