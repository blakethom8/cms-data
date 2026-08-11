# Provider discovery, profile, and evidence serving contract

> **Last reviewed: 2026-08-11** · **Status: representation version 3 in production**

The serving API deliberately exposes three provider views. They share NPI as the identity key, but
they answer different questions and must not be collapsed into one oversized response.

| Endpoint | Job | Source treatment |
| --- | --- | --- |
| `GET /profiles/search` | Discover a clinician by name or exact NPI. | NPPES is the discovery universe. Medicare Doctors & Clinicians (DAC) can enrich a hit but cannot gate whether an NPPES clinician is found. |
| `GET /profiles/{npi}` | Build the curated product profile used by provider dossiers. | NPPES establishes identity and supports NPPES-only profiles. Medicare, Open Payments, AACT, reassignment, facility affiliation, and other loaded sources add optional lenses. |
| `GET /explorer/provider-evidence` | Inspect bounded source-native rows and curated relationships for audit and data-quality work. | Publisher rows stay separate. Absence in one source is reported as absence in that source, not filled from another publisher. |

This separation keeps discovery comprehensive, the product profile convenient, and the evidence
surface honest about grain and provenance. Provider Search should use discovery to choose an NPI,
then use the curated profile for the dossier. The Data Command Center can use discovery for the same
selection step and the evidence endpoint for source-by-source inspection.

## Discovery semantics

Name search runs against Type 1 NPPES records. State is a hard scope. City is a ranking boost, not a
filter, because publisher practice-city values may use suburbs or neighborhood names. Medicare DAC
is left-joined only after the NPPES name candidates have been selected.

Exact NPI search follows the same rule: use NPPES identity first and add DAC specialty and group
context when present. A DAC-only fallback remains for the rare case where the selected warehouse
contains an older Medicare row without its NPPES identity row.

The search result `source` field explains the evidence available for that result:

- `nppes`: NPPES supplied identity and no DAC row was found;
- `nppes + medicare`: NPPES supplied identity and DAC supplied Medicare enrichment; and
- `medicare`: the rare exact-NPI DAC fallback when no NPPES row is present.

`source` does not select which table was searched. Name discovery is always NPPES-first. It also
does not prove employment, billing at a specific door, network participation, or acceptance of new
patients.

## Curated profile semantics

`GET /profiles/{npi}` succeeds when the NPI is present in NPPES or DAC. The header prefers NPPES
name, credentials, city, and state; DAC contributes Medicare specialty, secondary specialties,
education, and telehealth fields when available. NPPES taxonomy supplies a readable specialty when
DAC specialty is absent.

The Access fields remain curated subcontracts:

- `locations` merges DAC practice doors with the NPPES primary practice address on normalized
  `street|zip5`. Each row has `sources`: `dac`, `nppes`, or `dac + nppes`. Different suite text in
  address line 1 remains a separate door rather than being guessed into one location.
- `groups` merges DAC organization rows with Medicare reassignment relationships. Each row retains
  `sources`: `dac`, `reassignment`, or `dac + reassignment`.
- `hospital_affiliations` preserves DAC facility-affiliation rows and resolves certification
  numbers to hospital names when the hospital file contains a match.

All other lenses are optional enrichment. An NPPES-only clinician can therefore have a valid header
and NPPES location while Medicare utilization, prescribing, group, hospital, quality, and industry
sections are empty. Empty enrichment is not a profile failure.

## Evidence semantics

The evidence endpoint is not a replacement for the curated profile. It is the audit surface for
questions such as “what did each publisher actually say?” Its allowlist, bounded NPI list, bound
parameters, and per-source row limits keep it read-only and reviewable. See
[provider-evidence-model.md](provider-evidence-model.md) for warehouse evidence grains and consumer
rules.

The Command Center labels discovery provenance, then retrieves raw source rows separately. It must
not flatten NPPES registration, DAC practice, Medicare reassignment, PECOS enrollment, utilization,
or derived bridges into a single employer claim.

## Serving and cache contract

These changes are representation version 3:

- search discovery and `source` semantics changed;
- NPPES-only NPIs can return a curated profile; and
- each `locations[]` row gained `sources` and can include an NPPES-only door.

Consumers must key caches on `release_id` plus `representation_version`. Deployment selection is an
immutable code-only bundle cutover; the active DuckDB is never overwritten and request handlers do
not write. A v3 candidate must prove Alicia Terando (`1396877080`) is NPPES-discovered with two
separate provenance-labelled doors, prove at least one real NPPES-only clinician can be searched and
profiled, retain the Fischer and Do affiliation counts, pass the provider-evidence check, and pass
the canonical production smoke and ETag/304 checks before approval.
