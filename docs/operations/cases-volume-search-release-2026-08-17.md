# Cases utilization search release — 2026-08-17

> **Status:** release execution in progress; production remains unchanged until every promotion
> gate passes.

## Temporary capacity policy

This release uses an operator-approved `--promotion-block-percent 90` override. The checked-in
default remains 85%; the exception is scoped to this release attempt and does not weaken the
active-plus-two validated rollback floor, immutable staging/production-copy requirement, or any
other promotion gate.

The first corrected full-lifecycle preview failed at 92.15% projected use because the host must
reserve both a staging candidate and its separate production artifact. The preview used two times
the selected 21,513,908,224-byte warehouse as a conservative lower bound.

## Approved retention cleanup

The unpromoted, rolled-back provider-profile staging database was removed to restore headroom:

- release: `warehouse-20260814T235921Z-dc0bde25f1`;
- exact removed file: `data/releases/warehouse-20260814T235921Z-dc0bde25f1/warehouse.duckdb`;
- logical bytes: 26,034,057,216;
- SHA-256: `de70e24bdb3f3c2d2e41f956510330673c71fbcdff74e71754b5ed77f20fca87`;
- production references, symlinks, open files, and matching processes: none; and
- retained: source snapshots, `release.json`, comparison evidence, release ledger, and the sealed
  37-file audit under `/srv/cms-data-platform/audits/s3-provider-profile-complete-20260814T2350Z/evidence`.

The file was renamed to an exact quarantine path first. While quarantined, production-manager
artifact integrity, control-plane health, rollback-floor state, the selected deployment, systemd,
and loopback `/health` all passed. Only then was the quarantine deleted. The database is not
recoverable in place, but it remains reproducible from retained inputs and evidence.

After cleanup, current use was 73.10%. Reserving two selected-warehouse-sized allocations projected
84.97%, passing the temporary 90% gate with the active-plus-two rollback floor intact.

## Failed first build and N/N-1 repair

The first full-CMS candidate, `warehouse-20260817T221218Z-fd2eac47d6`, failed before validation
because the selected baseline predates `provider_address_evidence`. The failed release remained
unpromoted with a `.partial` database, and production stayed on
`deployment-20260814T201311Z-0325c353c9` with zero restarts.

The failure also exposed a second compatibility requirement: the selected baseline retains the old
generic-only `provider_drug_detail` primary key, which cannot represent the new brand/generic grain.
The full-CMS builder now prepares only its isolated candidate copy by recreating the derived Part D
and utilization-dictionary tables, applying current idempotent DDL for missing tables, and then
performing the complete rebuild. A regression test reconstructs that N-1 baseline shape and proves
the missing evidence/dictionary tables plus the four-column Part D primary key. Changed-file lint,
the focused regression/transform tests, and the full suite (`539 passed, 1 skipped`) pass.

## Required storage follow-up

The 90% exception is temporary. After this release, perform a separate evidence-backed retention
review or expand the volume, restore the normal 85% promotion block, and prove useful operating
headroom. Cleanup must continue to preserve active plus two validated predecessors, source
snapshots without durable off-host copies, backups required by recovery policy, and sealed audit
evidence.
