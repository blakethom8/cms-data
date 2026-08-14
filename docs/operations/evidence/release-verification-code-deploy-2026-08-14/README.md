# Release-verification code deployment evidence

This directory supports the
[`release-verification code deployment`](../../release-verification-code-deploy-2026-08-14.md)
record for `deployment-20260814T172445Z-3cd965d04e`.

- `deployment-ledger-excerpt.json` captures the selected deployment and its verified rollback
  predecessor.
- `source-manifests.json` and `expected-table-counts.json` preserve the inherited warehouse
  provenance and bounded smoke-count contract.
- `startup-check.json`, `rehearsal-smoke.json`, and `rehearsal-serving-contract.json` prove the
  exact immutable bundle before selection.
- `rehearsal-serving-contract-attempt1.json` retains the case-sensitive diagnostic-header issue;
  the actual data conditional status was already `304`, and the corrected diagnostic passed.
- `transition-dry-run-{activate,rollback}.json` preserve the passing no-op transition rehearsals.
- `pre-cutover-*` files preserve control-plane, process, service, non-secret environment metadata,
  retention, journal, and checksum evidence immediately before selection.
- `cutover-result.json` and `smoke.json` record the successful one-shot promotion and final
  15-check production smoke.
- `post-cutover-*` files prove the selected process and artifacts, manager state, release/data
  cache contract, clean journal, retention gate, and evidence checksums after selection.
- `provider-search-readiness.json` records the downstream application's `ready` result and healthy
  CMS data check.

The artifacts intentionally retain exact loopback origins, process IDs, artifact paths, release
IDs, hashes, and timestamps required for audit. Environment-file contents, API keys, downloaded
source data, and DuckDB files are not included.
