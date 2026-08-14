# S2 NPPES-primary serving slice implementation

> Date: 2026-08-14
>
> Scope: local implementation and fixture validation only
>
> Production mutation: none

## Decision

The next serving-mart slice is `/practices/search?location_basis=nppes_primary`. The canonical state
case took 497 ms and scanned 193.2 million rows, while the route already has a stable raw oracle and
the same external response contract as the live CMS-enrollment slice.

The slice uses two tables:

1. `serving_practice_nppes_provider_sites` has one deterministic active NPPES practice address per
   Medicare NPI. It retains ordered specialties, national Part B and Part D measures, geocodes,
   selected NPPES source identity, and claims source run-period arrays.
2. `serving_practice_nppes_org_memberships` has one provider/site/organization context. It retains
   DAC source run-period arrays and whether the CMS organization address matches the selected NPPES
   address.

The split is deliberate. Organization membership is non-additive: flattening it into the provider
row would multiply national payments and provider counts for NPIs with more than one CMS
organization.

## Safety and compatibility

- The existing raw SQL remains the parity oracle.
- The API response model, OpenAPI contract, route, and Provider Search RPC contract are unchanged.
- `NPPES_PRACTICE_SEARCH_BACKEND=auto` requires both serving tables and every query column. A
  missing or incomplete capability falls back to raw.
- An explicit mart selector is available for isolated testing only.
- Transforms reject eligible NPPES, Part B, Part D, or DAC rows whose required run/period identity is
  missing. Mart contracts also fail rows with invalid geography, empty specialties, missing claims
  provenance, duplicate keys, or parent orphans.
- Production remains on deployment `deployment-20260814T172445Z-3cd965d04e` and warehouse
  `warehouse-20260814T025428Z-5dac630227`; neither contains this candidate slice.

## Local evidence

Focused tests prove:

- byte-exact raw/mart responses for state, multi-specialty/multi-ZIP, proximity/limit, and empty
  searches;
- specialty preservation for a provider with more than one searchable specialty;
- raw fallback before the complete capability exists;
- mart-only execution after all raw dependencies are renamed away;
- fail-closed fallback when either table is incomplete; and
- provider-grain national measures remain separate from multiple organization memberships.

The focused API, transform, contract, and operations suite passed 50 tests. The complete API suite
passed 483 tests with one expected skip. The final diff check reported no whitespace errors.

The follow-on S2.6 builder adds `build-nppes-serving-practice-release` and the exact
`serving_practice_nppes_additive_v1` comparison policy. Fixture validation passed 27 release tests,
including rejection of a missing declared source period and a tampered changed-table allowlist.
The complete suite passed 487 tests with one expected skip. The builder remains staging-only and
the production manager does not accept its policy.

The first server invocation from a sealed code archive stopped before allocating a release because
the archive intentionally had no `.git` directory and the CLI could not infer a commit. The command
now accepts `--code-commit` for this case and validates an exact 40-character hexadecimal identity;
missing, abbreviated, or malformed values still fail closed. No warehouse copy or partial database
was created by the rejected invocation.

## Required before any production cutover

1. ~~Add an exact targeted-additive release policy whose allowlist contains only the two new
   tables.~~ Completed in S2.6.
2. Copy and verify the immutable selected baseline; never modify it in place.
3. Build and validate an isolated production-data candidate under bounded resources.
4. Prove all non-allowlisted tables unchanged with schema digests and logical row fingerprints.
5. Run the full canonical raw-versus-mart parity corpus against the same candidate.
6. Capture query profiles and three-trial HTTP medians at concurrency 1/2/4/8/12.
7. Pass the S2 correctness, performance, memory, capacity, smoke, and rollback gates.
8. Produce a written recommendation and obtain separate explicit authorization before selection.

This record does not authorize deployment, warehouse selection, or production cutover.
