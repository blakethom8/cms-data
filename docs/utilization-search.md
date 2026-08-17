# Utilization search serving contract

> **Status:** implemented locally; requires a warehouse candidate and code deployment before use

`/utilization/*` is the inverted Medicare discovery surface for Provider Search Cases. It ranks
individual NPIs by a selected HCPCS or Part D drug basket and assigns each NPI's national totals to
the one NPPES-primary location in `serving_practice_nppes_provider_sites`. The location is a map and
territory attribution, not a claim service location. Every search response carries
`metric_scope: national_npi_totals`.

The compact `utilization_procedure_dictionary` and `utilization_drug_dictionary` tables own
typeahead aggregates. Search facts remain in `provider_service_detail` and
`provider_drug_detail`; there is no duplicate provider-level inverted fact table. Procedure
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

Numeric HCPCS Level I descriptions can contain AMA CPT content. The API therefore returns no
procedure descriptions and does not match description text by default. Set
`HCPCS_DESCRIPTIONS_ENABLED=true` only after the organization confirms the required license or an
approved description filter. Code lookup remains available while that gate is closed.

## Confirmed non-goals in the active warehouse

- There is no physician-level ICD-10 diagnosis-volume fact or API. Chronic-condition percentages
  in `utilization_metrics` are panel mix, not reverse-searchable diagnosis claims.
- There is no versioned county, ZCTA-to-county, or MSA crosswalk. Geography remains city/state,
  ZIP boundaries, and radius.
- The active Part B service and Part D drug facts contain the 2024 measurement year only. The
  schema retains `data_year`, but there is no multiple-vintage series for adoption scoring.

Adding ICD, county, or longitudinal adoption requires separate source and serving contracts; these
routes intentionally expose none of those parameters.
