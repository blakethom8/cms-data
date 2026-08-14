# S2 NPPES-primary production cutover evidence

This directory supports the
[`S2 NPPES-primary production cutover`](../../s2-nppes-primary-production-cutover-2026-08-14.md)
record.

- `pre-cutover-*` proves the selected predecessor, service identity, exact warehouse hashes,
  systemd unit, rollback floor, and zero-additional-byte capacity decision immediately before
  selection.
- `expected-table-counts.json` preserves the query-authorized invariant smoke counts and separately
  names the two changed serving-table counts.
- `source-manifests.json` is the deployment-scoped source-provenance snapshot inherited from the
  exact warehouse baseline.
- `rehearsal-smoke.json` is the full isolated-bundle smoke result from loopback port 18080.
- `cutover-result.json` and `smoke.json` are the one-shot selector result and its canonical
  production verification evidence.
- `post-cutover-*` proves final manager state, release identity, process paths, absence of service
  errors, capacity, rollback retention, and a bounded live NPPES-primary practice query.
- `provider-search-readiness.json` records the downstream application's healthy CMS data check.
- `deployment-ledger-excerpt.json` contains the selected deployment and both protected validated
  predecessors.
- `SHA256SUMS` checksums the evidence files committed here.

No file contains credentials, environment-file contents, downloaded source payloads, or DuckDB
data.
