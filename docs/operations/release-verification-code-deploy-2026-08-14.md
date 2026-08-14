# Release-verification code deployment — 2026-08-14

> **Outcome:** the immutable code-only deployment completed successfully. Production selects
> `deployment-20260814T172445Z-3cd965d04e`; the immediately preceding S2 deployment remains
> verified and rollback-ready.

## Scope

PR #55 (`b80d56510757770f1f6f6d90492053948567b08b`) corrected the release metadata lifecycle found
during the S2 managed-DAC cutover. When a selected process initially observes its deployment before
manager verification, the resolver now refreshes only a missing `verified_at` value from the same
deployment's ledger. It refuses metadata from a repointed bundle. `GET /release` is now explicitly
non-cacheable with `Cache-Control: no-store` and no ETag, while immutable data routes retain their
deployment-scoped ETags and conditional `304` behavior.

This deployment changed only serving code. It reused:

- warehouse release `warehouse-20260814T025428Z-5dac630227`;
- warehouse SHA-256
  `bf7d2381c8c9a683497a7dcc5d64c87ccfb3359fe5ee77d61e74ca3bffb1fa02`;
- warehouse size 20,951,347,200 bytes;
- runtime `runtime-candidate-8985e8a-c26024b3`; and
- runtime fingerprint
  `sha256:82370f7e4b25f1a907a92eda5c1097302a6f88936ad59319206b4ade3cc7c347`.

There was no warehouse copy, data rebuild, schema change, dependency-lock change, API contract
change, or Provider Search RPC change.

## Immutable candidate and rehearsal

The clean detached code artifact is
`production-artifacts/code/b80d56510757770f1f6f6d90492053948567b08b-release-verification-1`.
Its fingerprint is
`sha256:b58e72bb30a0f4b15735d237e5a4fc315a7e2717d9e220608075f9afa07ca490`.
The artifact is root-owned and sealed with zero writable paths and zero bytecode/cache paths.

Preparation created `deployment-20260814T172445Z-3cd965d04e` at
`2026-08-14T17:24:45+00:00` with exact predecessor
`deployment-20260814T160153Z-45ab9d2d38`. The isolated candidate ran as `dataops` on loopback port
18080 using the exact candidate code, existing production runtime, and existing production
warehouse. Its complete 15-check smoke passed.

The first serving-contract diagnostic was retained because it appeared to fail even though the
conditional data request returned `304`. The diagnostic had converted response headers to a
case-sensitive dictionary and then looked up mixed-case names. Normalizing header names corrected
the observation; the repeated check passed without changing the candidate or production. This was a
test-harness issue, not a serving regression.

The corrected rehearsal proved:

- `/release` returned the candidate identity, `Cache-Control: no-store`, and no ETag;
- the candidate was correctly unverified before selection;
- a data route returned ETag `"deployment-20260814T172445Z-3cd965d04e:3"`;
- the matching data-route conditional request returned `304`; and
- a conditional `/release` request using that data ETag still returned a fresh `200` response.

Activation and rollback dry-runs passed. The rehearsal process stopped cleanly, port 18080 was
released, the production selector did not move, and the candidate artifact remained sealed.

## Production cutover

Immediately before selection, the manager reported a healthy control plane, matching pointer and
ledger, passed artifact integrity, no transition sentinel, and zero blocking transactions. The API
was active on predecessor PID `3990931` with zero restarts. The retention gate allowed promotion at
84.29% disk use with approximately 42.19 GB free and the active-plus-two rollback floor intact.

The approved one-shot cutover selected the candidate at `2026-08-14T17:33:41+00:00` and recorded
verification at `2026-08-14T17:34:46+00:00`. Final production smoke passed all 15 checks. The new
process is PID `4002795`, active with zero restarts, with its current working directory and open
DuckDB file resolving to the exact sealed code and warehouse artifacts.

Independent post-cutover checks proved:

- selected state `verified`, pointer/ledger agreement, and passed artifact integrity;
- `/release` reports both `promoted_at` and the expected non-null `verified_at`;
- `/release` remains non-cacheable and a conditional request returns fresh `200`;
- immutable data-route ETag and `304` semantics remain unchanged;
- all 15 smoke checks pass, including practice search, provider profile, industry, research,
  Clinical Trials, Explorer, required tables, and warehouse counts;
- Provider Search reports `ready` with its CMS data check `ok`; and
- the predecessor is `superseded`, verified, and available for rollback.

Post-cutover disk use remains 84.29%, below the 85% promotion block but above the 80% critical
threshold. No further large warehouse build or copy should proceed without a new retention review.

## Evidence

The machine-readable preparation, rehearsal, dry-run, cutover, smoke, serving-contract, process,
ledger, retention, journal, and downstream-readiness evidence is archived under
[`evidence/release-verification-code-deploy-2026-08-14`](evidence/release-verification-code-deploy-2026-08-14/).
