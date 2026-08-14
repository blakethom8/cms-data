# S2 NPPES-primary candidate evidence

This directory is the committed copy of the read-only and isolated evidence described in
[`../../s2-nppes-primary-candidate-2026-08-14.md`](../../s2-nppes-primary-candidate-2026-08-14.md).

- `build*.json`, `candidate-identity.json`, and `comparison-command.json` record the fail-closed
  attempts, successful release, mart contracts, and 42 invariant fingerprints.
- `raw-diagnostics.json`, `mart-diagnostics.json`, and `parity-summary.json` record exact focused
  response parity.
- `raw-canonical-diagnostics.json`, `mart-canonical-diagnostics.json`,
  `canonical-parity-summary.json`, and `profile-rich-recheck-summary.json` preserve the wider corpus
  and the unrelated cross-process profile-order caveat.
- `*-benchmark-trial-*.json` and `benchmark-summary.json` contain all three trials at every tested
  concurrency.
- `*-plan-capture.json` and `plan-capture-summary.json` contain the exact SQL, bound-parameter
  digests, JSON plans, and operator summaries.
- `post-capacity-preview.json` is the zero-additional-byte inventory. The authoritative preparation
  gate is `production-copy-capacity-preview.json`, which includes the required distinct production
  copy and blocks at a projected 90.85%.
- `final-*` records prove that the selected production deployment, service, and health stayed
  unchanged.

`SHA256SUMS` covers every server-produced file in this directory. The README is repository context
and is intentionally not part of that server-side checksum set.
