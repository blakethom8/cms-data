# Product serving marts

> **Last reviewed: 2026-08-14** · **Status: S2 contract reference**

The warehouse keeps raw publisher-shaped evidence separate from curated marts. The twelve
source-oriented foundations below are joined by the first registered route-specific serving mart.
Registration does not automatically authorize replacement of request-time API logic. A route can
switch only after response parity and a material p95 or resource improvement are proven under the
S2 acceptance gates.

The executable source of truth is `pipeline/mart_contracts.py`. Release builds validate physical
grain, required values, NPI domains, parent relationships, and source-manifest coverage. The
read-only operations overview performs schema inspection only and labels that scope explicitly.
`GET /operations/marts` exposes the per-mart schema result and declared current consumers without
running row scans in the serving process.

| Mart | Grain / key | Period and provenance | Current serving use |
| --- | --- | --- | --- |
| `core_providers` | One provider identity per `npi` | Contributing source periods in the release manifest; `data_year` is the CMS year | Match, unified search, practice capabilities |
| `practice_locations` | One provider/group relationship per `location_id` | Reassignment manifest; `data_year` is its period year | Match and source evidence |
| `utilization_metrics` | `npi × metric_year` | Part B, Part D, and DME manifests; metric year retained | Match and unified search |
| `industry_relationships` | `npi × payment_year × paying_company_name` | Open Payments manifest; program year retained | Not yet authorized for the industry API |
| `hospital_affiliations` | `npi × hospital_npi` | Reassignment and hospital manifests; relationship method retained | Profile affiliation and source evidence |
| `provider_service_detail` | `npi × HCPCS × place_of_service × data_year` | Provider-and-service manifest; measurement year retained | Not yet authorized for profile serving |
| `provider_drug_detail` | `npi × generic_name × data_year` | Part D drug manifest; measurement year retained | Not yet authorized for profile serving |
| `provider_quality_scores` | One selected quality record per `npi` | QPP manifest; performance year retained | Not yet authorized for profile serving |
| `order_referring_eligibility` | One current eligibility row per `npi` | Order-and-referring snapshot interval in release manifest | Not yet authorized for profile serving |
| `kol_summary` | One qualifying all-year summary per `npi` | Open Payments manifest; most recent program year is derived | Not yet authorized for industry serving |
| `nppes_radar_provider_state` | One reconciled current state row per `npi` | Row release/period plus release manifest | Radar providers |
| `nppes_radar_events` | One immutable event per `event_id` | Row release/period plus release manifest | Radar providers |

The first route-specific contract is `serving_practice_provider_sites`, at one normalized DAC site
and organization-or-solo key per NPI. It retains ordered specialty values, national Part B and Part
D totals, geocodes, and row-level source period/run IDs. DAC, Part B, and Part D are registered
managed sources and require release-manifest provenance. The selected production baseline predates
managed DAC acquisition and its `raw_dac_national` table lacks those provenance columns, so it was
not eligible as an S2 candidate input. The managed-DAC candidate passed parity and performance and
was selected in production on 2026-08-14, so `/practices/search` now consumes the mart for
`cms_enrollment` searches. Runtime capability selection uses the mart only when the selected
immutable warehouse contains it; predecessors remain on the raw oracle without a global
environment change.

The additive offline builder is `python -m pipeline.data_platform
build-serving-practice-release`. It requires a named validated baseline release, a verified backup
manifest with the same SHA-256, and the mart data year. The production-proof form is
`build-managed-dac-serving-practice-release`; it additionally requires one exact validated DAC run,
derives the baseline Part B and Part D run-period pairs from their rows, replaces only
`raw_dac_national`, and then builds the mart. Its `serving_practice_managed_dac_v1` comparison policy
allowlists only that raw table and `serving_practice_provider_sites`; every other table is protected
by row counts, schema digests, and order-independent logical row fingerprints so equal counts cannot
mask drift. The production release manager accepts this policy only when the comparison and release
both contain the exact two-table scope, matching positive row counts, and passed serving-mart
validation. The first production cutover was separately authorized and completed on 2026-08-14.

## Validation states

- **Registered** means the table has a declared `MartSpec`.
- **Available** means a table with that name exists in the warehouse.
- **Schema valid** means every required contract column exists. The operations API may report this
  cheap read-only state; it does not imply row validation.
- **Data valid** means the offline release builder also proved non-empty requirements, unique grain,
  required-value presence, ten-digit NPI domains, declared parent coverage, and source-period
  evidence. Any violation rejects the release candidate.
- **Serving authorized** means the contract names an existing route consumer and the applicable
  schema or offline validation passed. New S2 consumers require parity and performance evidence
  before they can be added.

## Null and provenance rules

Contract keys and explicitly required columns cannot be null. Other columns retain source absence as
`NULL`; transforms must not silently rewrite unknown data to zero or an empty string. Release-scoped
provenance is declared honestly when the physical row has no source-run columns. File timestamps and
ingestion timestamps are never substituted for publisher periods.

## S2 route migration

The [S2 execution plan](proposals/2026-08-s2-serving-marts-plan.md) begins with measured canonical
plans, then builds the default practice-search serving mart as the first isolated vertical slice.
That slice is now live. Explorer remains source-faithful, Clinical Trials remains in
AACT/PostgreSQL, and every future mart cutover still requires separate authorization.
