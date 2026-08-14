# S2 managed-DAC evidence bundle

This directory supports the
[`S2 managed-DAC practice candidate`](../../s2-managed-dac-candidate-2026-08-14.md) record.

- `acquisition-manifest.json` identifies the official CMS source artifact and validation result.
- `candidate-release.json` identifies the immutable warehouse, source runs, contract validation,
  and resource limits.
- `candidate-comparison.json` proves the 40 invariant tables and exact changed-table allowlist.
- `raw-diagnostics.json` and `mart-diagnostics.json` contain the three-trial canonical response
  evidence.
- `raw-benchmark-trial-*.json` and `mart-benchmark-trial-*.json` are the three comparable focused
  performance trials per backend.
- `retention-preview.json` records the initial capacity block and review-only inventory.
- `retention-preview-after-cleanup.json` records the passing gate after the failed candidate was
  explicitly reviewed and removed.
- `production-smoke.json` is the passing 15-check isolated production-bundle smoke. The three
  `production-smoke-attempt*.json` files retain the fail-closed evidence-shape, protected-table, and
  scoped-key learnings that preceded it.
- `production-expected-table-counts.json` is the bounded, query-authorized count contract used by
  the passing smoke; targeted raw/mart counts are verified separately.
- `production-candidate-mart-check.json` proves `auto` resolved to the complete mart contract and
  records exact raw/mart counts from the immutable production copy.
- `production-serving-contract.json` proves candidate release identity, provenance, ETag, and
  conditional `304` behavior.
- `production-source-status-{fixture,live}.json` validate the reconciled deployment-scoped source
  snapshot without changing the selected production snapshot.
- `production-transition-dry-run-{activate,rollback}.json` record both no-op transition rehearsals.
- `production-retention-preview.json` is the passing post-copy capacity and rollback-floor gate; its
  zero candidate bytes mean zero additional bytes remain to be allocated, not a zero-sized warehouse.

The generated artifacts intentionally retain exact loopback origins, process IDs, warehouse paths,
release IDs, hashes, and timestamps needed to audit the rehearsal. They contain no API keys or
downloaded source data.
