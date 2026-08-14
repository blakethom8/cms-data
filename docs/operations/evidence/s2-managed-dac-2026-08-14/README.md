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
- `retention-preview.json` records the read-only capacity block and review-only inventory.

The generated artifacts intentionally retain exact loopback origins, process IDs, warehouse paths,
release IDs, hashes, and timestamps needed to audit the rehearsal. They contain no API keys or
downloaded source data.
