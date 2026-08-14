# Production storage-retention cleanup evidence

This directory supports the
[`production storage-retention cleanup`](../../storage-retention-cleanup-2026-08-14.md) record.

- `pre-retention-preview.json` and `post-retention-preview.json` preserve the full deterministic
  storage inventory, protected rollback floor, and capacity result before and after deletion.
- `pre-candidate-stat.txt` records the exact directory and DuckDB inode, ownership, modes, logical
  bytes, and allocated blocks.
- `warehouse-copy-sha256.txt` proves the removed production artifact matched both retained copies.
- `staging-release.json`, `staging-comparison.json`, and `backup-manifest.json` preserve validation,
  promotion provenance, and retained-copy identity.
- `production-ledger-excerpt.json` contains every deployment that referenced the removed warehouse.
- `production-symlink-references.txt` and `pre-open-files.txt` preserve the reference/open-handle
  boundary before quarantine.
- `quarantine-*` files prove the manager, selected open warehouse, `/release`, and Provider Search
  remained healthy while the candidate path was reversibly renamed.
- `post-*` files prove the final manager, service, release, downstream, and capacity state.
- `evidence-sha256.txt` checksums the generated evidence bundle.

The directory contains no credentials, environment-file contents, downloaded source data, or
DuckDB files.
