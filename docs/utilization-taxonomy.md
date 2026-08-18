# Utilization taxonomy reference contract

> **Status:** implementation contract for the Cases hierarchy candidate

Cases discovery keeps the published utilization facts at their original HCPCS and Part D drug
name grains. Clinical browsing is a separate, versioned reference layer in the utilization
sidecar. A taxonomy refresh therefore augments a sealed utilization release; it does not rebuild
or rewrite the 28-million-row drug fact.

## Sources and permitted hierarchy

- Procedures use the CMS Restructured BETOS Classification System (RBCS) taxonomy. The acquisition
  manifest records the exact CMS CSV URL, SHA-256, release year, row counts, and download time.
  The current assignment (`RBCS_Latest_Assignment = 1`) is authoritative. The hierarchy is
  category → subcategory → family → HCPCS code.
- Drugs use the U.S. National Library of Medicine RxClass API. The reference stores the exact ATC
  and FDASPL source versions reported by RxClass. The browse hierarchy exposes ATC therapeutic
  classes and FDA Established Pharmacologic Class (EPC) classes. RxNorm ingredient concepts are
  the bridge from the warehouse's published generic name to a class.
- Commercially curated procedure bundles and drug cohorts are a later product overlay. They must
  never be presented as publisher-authored CMS or NLM taxonomy.

RxClass-derived screens must include this attribution:

> This product uses publicly available data from the U.S. National Library of Medicine (NLM),
> National Institutes of Health, Department of Health and Human Services; NLM is not responsible
> for the product and does not endorse or recommend this or any other product.

## Immutable reference acquisition

`pipeline.utilization_taxonomy acquire` takes a sealed utilization database, downloads the RBCS
CSV, fetches the ATC catalog, and resolves every distinct warehouse generic against both ATC and
FDASPL. It writes immutable CSV reference files plus `manifest.json` under a caller-supplied,
non-production directory. Every file is hashed. RxClass calls are cached per generic and source so
an interrupted acquisition can resume without silently changing completed responses.

Ingredient matching is conservative. Exact normalized names and names differing only by common
salt words are accepted. Combination concepts must have the same normalized ingredient set; a
single-ingredient generic such as atorvastatin cannot inherit an atorvastatin/amlodipine class.
The manifest reports queried, mapped, and unmapped generic counts. Unmapped drugs remain available
through ordinary brand/generic search and are never guessed into a class.

## Sidecar tables

An augmented utilization release adds these small tables and indexes:

- `utilization_procedure_taxonomy`: one current RBCS assignment per HCPCS code, including category,
  subcategory, family, major indicator, active dates, and RBCS release.
- `utilization_drug_classes`: one ATC or EPC class, including ATC parent identifiers where present.
- `utilization_drug_class_members`: accepted warehouse generic → RxNorm ingredient → class mappings,
  with match method and score.

Browse summaries join those references to the existing dictionaries at request time. Metrics count
only codes or drugs present in the active utilization release. A taxonomy family may contain more
than the Cases basket limit; the API returns all available members for inspection, while the client
requires the user to refine the selection to at most 50 explicit codes or drug names before the
ranked NPI search. The existing search endpoints and contract remain backward compatible.

## API shape

- `GET /utilization/procedures/taxonomy?q=&category=&subcategory=&limit=` returns matching RBCS
  families, ranked by available utilization volume, plus category/subcategory context.
- `GET /utilization/procedures/families/{family_id}` returns the available HCPCS members and their
  dictionary metrics.
- `GET /utilization/drugs/classes?q=&source=ATC|FDASPL&limit=` returns matching ATC therapeutic or
  FDA EPC classes ranked by available claims.
- `GET /utilization/drugs/classes/{source}/{class_id}` returns distinct available generic members
  and their aggregate dictionary metrics.

Search treats a concept match differently from a literal substring match: families/classes appear
first, followed by individual codes/drugs. Numeric HCPCS neighborhood browsing remains a secondary
expert affordance, not the clinical source of truth.

## Release gates

The augmentation builder verifies the source utilization manifest, source database hash and size,
every reference file hash, one-current-assignment-per-HCPCS, valid class sources/types, unique class
memberships, non-empty overlap with the active dictionaries, and read-only query smokes. It seals a
new `utilization.duckdb`, `release.json`, and `comparison.json`. Preparation, rehearsal, transition
dry-runs, and controlled cutover remain unchanged. Any failed gate stops; activation is never
forced.

RBCS labels are CMS-authored and do not remove the separate CPT/HCPCS Level I description licensing
gate documented in `docs/utilization-search.md`. This work adds no diagnosis, county, or database
migration surface.
