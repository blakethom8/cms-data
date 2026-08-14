# S3 provider-profile serving marts plan

> **Last reviewed: 2026-08-14** · **Status: S3.4 claims slice implemented; production unchanged**
>
> **Production state:** unchanged. Provider-profile core queries still default to the raw oracle.
> One isolated candidate was built and evaluated, but was never prepared or selected. It missed the
> complete-route performance gates and is superseded by a deterministic-header fix. See the
> [candidate evaluation](../operations/s3-provider-profile-candidate-2026-08-14.md).

## Decision

Build the provider-profile serving layer as small projections with explicit grains, not as one wide
provider row. The first slice materializes the three request-time joins that define identity and
access while preserving the existing API response contract:

| Table | Grain | Replaces in `/profiles/{npi}` |
| --- | --- | --- |
| `serving_provider_profile_headers` | One NPPES-first identity row per NPI | NPPES, DAC, and taxonomy header join |
| `serving_provider_profile_locations` | One normalized DAC or NPPES address per NPI | Address merge, roster, and geocode join |
| `serving_provider_profile_groups` | One NPI × PAC organization context | DAC and reassignment affiliation merge |

Separate grains prevent a provider's locations and organizations from multiplying one another.
Every row retains the contributing source-period and source-run arrays. Unknown source values remain
null; absence is not rewritten as zero or an empty string.

Hospital affiliations remain on the existing raw/curated path in this slice. The legacy
`raw_dac_facility_affiliations` and `raw_hospital_general_info` inputs do not yet have the same
independently managed source-manifest coverage as NPPES, national DAC, and reassignment. Hiding that
provenance gap behind a new serving projection would make the contract look stronger than the
evidence.

## Measured reason to proceed

The 2026-08-14 canonical query-plan capture found 15–16 SQL statements per provider profile. The
rich profile executed about 2.51 billion operator rows and took 649 ms in the reproducible plan
capture; the standard profile took 244 ms with the same operator-row scale. Separate exploratory
runs observed roughly 777 ms and 592 ms respectively. These are diagnostic measurements against a
fixed warehouse, not promises about production latency.

The first three projections remove repeated identity, location, and group joins. The next three
response-exact projections now materialize utilization/prescribing summaries, top services, and
top drugs. Industry, research, quality, and hospital sections still use their existing paths, so
the combined six-table capability must be measured before any route switch.

## S3.1 — contract and implementation

Implemented in the repository:

- physical schemas and indexes for the three grains;
- one idempotent, provenance-validating transform;
- executable mart contracts, lineage, and source-registry dependencies;
- an immutable targeted-additive release builder with exact three-table scope;
- schema-complete `raw`, `mart`, and `auto` API selection while keeping `raw` as the default;
- exact fixture parity for header, locations, groups, ordering, nulls, provenance, and fallback; and
- invariant-table fingerprints in candidate comparison.

The builder command is:

```bash
.venv/bin/python -m pipeline.data_platform \
  build-provider-profile-core-release \
  --baseline-warehouse-release-id <validated-release-id> \
  --backup-manifest <verified-backup-manifest.json> \
  --data-year <year> \
  --reassignment-run-id <verified-managed-run-id> \
  --claims-service-run-id <verified-managed-run-id> \
  --claims-drug-run-id <verified-managed-run-id> \
  --code-commit <full-40-character-commit> \
  --environment staging \
  --json
```

It copies and verifies the named immutable baseline, applies only the three new tables, validates
their contracts in one transaction, and seals a candidate under bounded DuckDB resources. It does
not prepare or select a deployment. The production manager intentionally does not accept the new
comparison policy yet.

## S3.2 — production-data evaluation

Before considering authorization:

1. Confirm capacity is below the promotion block and retain a verified rollback artifact.
2. Build one isolated candidate from the exact selected production baseline.
3. Prove the changed-table set is exactly the three provider-profile tables and all invariant
   fingerprints match.
4. Run byte-exact raw-versus-mart parity over the canonical provider corpus, including missing,
   NPPES-only, high-location, and high-organization cases.
5. Capture paired `EXPLAIN ANALYZE` evidence for the three replaced query families and the complete
   provider-profile request.
6. Run three comparable HTTP trials at concurrency 1, 2, 4, 8, and 12 with the same release,
   executor settings, workload, and timeout.
7. Require zero new failures and either the S2 latency gate or the S2 operator-work gate.
8. Record candidate size, peak build storage, post-prepare capacity, and rollback rehearsal.

If only the three component queries improve but the full profile does not materially improve, keep
the code dormant and proceed to the next profile projection rather than cutting over prematurely.

### S3.2 result

That is the measured outcome. The core slice improved provider-profile p95 by 22.10% at concurrency
1 but only 8.57% at concurrency 12, versus a 20% requirement at both levels. Complete-profile
operator time fell 24.18% for the rich case and 17.97% for the standard case, below the alternate
30% gate. The mart cut mixed-workload concurrency-12 overloads from 21 to 10 across three trials,
but did not eliminate them. Production remains raw.

The evaluation also found and fixed stable group ordering and deterministic coherent DAC header
selection. Because the latter merged after the candidate build, the candidate is not compatible
with current parity semantics. The next candidate must include that fix plus the utilization,
top-services, and top-drugs slice before these gates are rerun.

## S3.4 — claims-side serving slice

Implemented in the repository, but not built against production data:

| Table | Grain | Existing response sections |
| --- | --- | --- |
| `serving_provider_profile_claims_summary` | One claims-bearing provider NPI | `panel`, `clinical`, and `prescribing` |
| `serving_provider_profile_top_services` | One NPI × deterministic service rank, maximum 10 | `top_procedures` |
| `serving_provider_profile_top_drugs` | One NPI × deterministic drug rank, maximum 10 | `top_drugs` |

The summary retains separate Part B provider, Part B service, and Part D provider run/period
arrays. The detail rows retain their contributing service or drug run/period. Source-grain
duplicates and missing provenance fail the build before any table is accepted. Stable HCPCS,
brand, and generic tie-breakers make repeated builds deterministic; service descriptions use a
deterministic minimum rather than `any_value`. This does not broaden the endpoint's existing HCPCS
description exposure, and the existing AMA licensing gate still applies to commercialization.

`auto` detects the three claims tables independently of the three core tables. An incomplete
claims capability therefore falls back to the raw claims oracle without disabling a complete core
capability. `raw` remains the deployment default.

The combined staging command is:

```bash
.venv/bin/python -m pipeline.data_platform \
  build-provider-profile-release \
  --baseline-warehouse-release-id <validated-release-id> \
  --backup-manifest <verified-backup-manifest.json> \
  --data-year <year> \
  --code-commit <full-40-character-commit> \
  --environment staging \
  --json
```

It builds all six tables in one transaction and uses
`serving_provider_profile_complete_additive_v1`, which permits exactly those six tables to differ
from the verified baseline and fingerprints every invariant table. When an older baseline omitted
the reassignment or claims-detail source IDs, each explicit managed run is revalidated against its
retained artifact and the baseline raw table's sole run, period, and row count before the candidate
can inherit it. The production manager does not authorize this policy. No successful candidate,
preparation, or cutover has occurred for this slice.

## S3.3 — explicit authorization and cutover

Only after S3.2 passes should a separate change authorize
`serving_provider_profile_core_additive_v1` in the production manager. A separately approved
deployment may then use deployment-local `auto` selection, run isolated smoke, rehearse rollback,
and atomically select the exact tested artifact. No Provider Search RPC or response-model change is
expected because the CMS endpoint contract remains unchanged.

## Next profile slices

Continue in measured order:

1. Evaluate the implemented utilization summary, top services, and top drugs with the core slice.
2. Industry summary and its high-fanout facets.
3. Quality and research summaries where their source semantics remain exact.
4. Hospital projection only after facility-affiliation and hospital source registration is complete.

Each slice needs its own grain, provenance contract, fixture parity, targeted release scope, and
production-data performance decision. Explorer remains source-faithful and is not replaced by these
summary projections.
