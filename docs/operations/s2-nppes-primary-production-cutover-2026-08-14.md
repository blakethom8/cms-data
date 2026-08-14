# S2 NPPES-primary production cutover — 2026-08-14

> **Outcome:** production now serves the two-table NPPES-primary practice mart through
> deployment-local `auto` selection. The one-shot cutover completed successfully, all 15 canonical
> smoke checks passed, Provider Search remained ready, and the exact predecessor remains verified
> and rollback-ready. No Provider Search RPC changed.

## Selected release

Production selected and verified deployment `deployment-20260814T201311Z-0325c353c9`:

- serving code commit: `ef9a94fef246011ffa4b7410dd6b31c25ddd148d`;
- warehouse release: `warehouse-20260814T183948Z-e5ff46dce9`;
- warehouse SHA-256:
  `2bcc92d44014b62e2bc0c4c42d3c1b814827668ed653b13ffe565ceea7aac9d3`;
- warehouse byte size: 21,513,908,224;
- warehouse pipeline commit: `55d31add1ad2751ba803486d6a22ce45bc0aa840`;
- runtime: existing sealed `runtime-candidate-8985e8a-c26024b3`; and
- selected and verified timestamps: `2026-08-14T20:21:26+00:00` and
  `2026-08-14T20:22:39+00:00`.

The selected warehouse adds only:

- `serving_practice_nppes_provider_sites`: 1,229,202 rows; and
- `serving_practice_nppes_org_memberships`: 1,361,659 rows.

The `/practices/search` request and response contract is unchanged. The API uses both NPPES tables
as one deployment-local capability for `location_basis=nppes_primary`; a warehouse missing either
complete contract continues to use the raw implementation.

## Preparation and rehearsal

The July workspace compaction reduced host use enough for the required independent production
copy. Immediately before that copy, the exact 21,513,908,224-byte preview passed at 83.64%
projected use with the active-plus-two rollback floor intact.

Two clean, immutable, root-owned checkouts at commit `ef9a94f` were installed separately for the
operations control plane and candidate serving code. The serving dependency files were unchanged
from the selected predecessor, so its sealed runtime was reused. The new production warehouse has
a distinct inode from staging, one link, mode `0440`, ownership `root:dataops`, and the exact release
checksum.

Manager preparation dry-run and mutation both accepted the strict
`serving_practice_nppes_additive_v1` evidence. The deployment inherited the predecessor's complete
source-manifest snapshot because the additive release changes no source vintage. Its separate
expected-count evidence preserves the query-authorized invariant smoke tables and the two changed
mart counts.

The prepared bundle ran as `dataops` on loopback port 18080. Its effective environment named the
candidate bundle's `warehouse` link, `/release` reported the prepared deployment and exact build
identity, and direct read-only counts matched both mart manifests. All 15 smoke checks passed,
including process identity, authentication, practice search, profiles, industry, research,
Clinical Trials, Explorer, required tables, and warehouse counts. Activation and rollback dry-runs
both passed before the temporary process was stopped.

## Controlled cutover

Immediately before selection:

- production manager status was healthy with passed artifact integrity;
- the predecessor was selected and verified;
- pointer and ledger matched;
- blocking transactions were zero and no transition sentinel existed;
- the candidate and predecessor production warehouses matched their approved SHA-256 values;
- the exact post-copy capacity preview allowed selection at 83.64%; and
- the API remained active on PID `4002795` with zero restarts.

The approved one-shot cutover selected the complete candidate bundle, restarted `cms-api.service`
once, waited for loopback readiness, ran the full smoke suite, and verified the deployment. The
automatic rollback path was armed but not needed.

## Post-cutover verification

The final manager status reports a healthy control plane, passed artifact integrity, matching
pointer and ledger, zero blocking transactions, no transition sentinel, and selected state
`verified`. The API is active on PID `4041825` with zero restarts; its working directory resolves to
the sealed `ef9a94f` code artifact and it holds the candidate DuckDB file open.

`GET /release` reports the selected deployment, warehouse ID, checksum, pipeline commit, promoted
timestamp, verified timestamp, representation version 3, and all 18 source vintages. Production
smoke passed all 15 checks with SHA-256
`c5a44b893c876ec8f769869dcf609290e66b973986c9a911db1ce64c17358f2d`. The CMS API error journal
contains no entries from the cutover window.

Provider Search `/ready` reports `ready` with `cms_data.status: ok`. A bounded live request for
California cardiologists through `location_basis=nppes_primary` returned 10 of 1,129 results in
203.5 ms. This is a single post-cutover availability sample, not a replacement for the prior
three-trial concurrency benchmark, which measured 80.0–88.3% lower p95 across concurrency 1–12
with zero mart failures.

The rollback floor now protects:

1. selected `deployment-20260814T201311Z-0325c353c9`;
2. predecessor `deployment-20260814T172445Z-3cd965d04e`; and
3. prior validated deployment `deployment-20260814T160153Z-45ab9d2d38`.

Current disk use is 83.64%, with 44,524,462,080 available bytes. The completed release is below the
85% promotion block, but the host remains in the critical storage band. Another large immutable
candidate must not be allocated without a fresh exact-byte retention preview and additional
cleanup or capacity.

## Evidence

Machine-readable preparation, rehearsal, selection, smoke, process, release, capacity, rollback,
journal, and downstream-readiness evidence is committed under
[`evidence/s2-nppes-production-2026-08-14`](evidence/s2-nppes-production-2026-08-14/) and sealed on
the server under
`/srv/cms-data-platform/production/evidence/deployment-20260814T201311Z-0325c353c9`.
