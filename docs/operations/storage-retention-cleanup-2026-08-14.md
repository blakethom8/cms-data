# Production storage-retention cleanup — 2026-08-14

> **Outcome:** one superseded warehouse artifact outside the active-plus-two rollback floor was
> removed after an evidence-backed quarantine check. Production disk use fell from 84.29% critical
> to 78.99% warning without changing the selected deployment, warehouse, service process, or
> Provider Search readiness.

## Pre-cleanup inventory

The read-only retention planner ran against selected deployment
`deployment-20260814T172445Z-3cd965d04e` with zero additional candidate bytes. It reported:

- 305,656,582,144 used bytes and 42,186,211,328 free bytes;
- 84.29% use, above the 80% critical threshold but below the 85% promotion block;
- 58 review candidates totaling 95,416,426,496 allocated bytes;
- a passing active-plus-two rollback floor; and
- zero automatically reclaimable bytes.

The largest item was the 42.54 GB `refresh-20260721` workspace. It contains source runs, AACT
snapshots, release evidence, and a staging warehouse, so it was deliberately retained pending a
category-specific refresh-workspace policy. The audit instead selected the narrower production
artifact
`production-artifacts/warehouses/warehouse-20260723T000948Z-24c46c1cda`, which occupied
19,228,606,464 allocated bytes.

## Candidate proof

The planner classified the exact artifact path as `review_candidate`. Its only production-ledger
references were these superseded deployments, all outside the protected rollback floor:

- `deployment-20260723T002222Z-31da868819`;
- `deployment-20260723T004113Z-2f24e8d935`; and
- `deployment-20260804T163418Z-2ad954a774`.

The protected floor remained:

1. `deployment-20260814T172445Z-3cd965d04e`;
2. `deployment-20260814T160153Z-45ab9d2d38`; and
3. `deployment-20260811T155814Z-6baa26aa69`.

The candidate DuckDB file was 20,091,252,736 logical bytes with SHA-256
`b8af099ef264883160797e8d5799b788681e085a53db912975de702e52f59ed7`. Two independently
allocated, byte-identical copies remain:

- the original validated staging release under
  `refresh-20260721/releases/warehouse-20260723T000948Z-24c46c1cda`; and
- validated backup `backups/20260811T015700Z-radar-serving-baseline`.

The retained staging `release.json`, passed comparison, root release ledger, backup manifest, and
production deployment records agree on release ID, byte size, and checksum. No active job or
process held the production artifact open. Its three bundle symlinks were recorded before cleanup;
they belong only to the superseded deployments listed above.

## Controlled deletion

The exact artifact directory was first renamed to the explicit quarantine path
`.retention-delete-warehouse-20260723T000948Z-24c46c1cda-20260814T175543Z`. A failure trap would
have restored the original path before deletion if any guard failed.

While quarantined, the production manager remained healthy with passed artifact integrity,
matching pointer and ledger, zero blocking transactions, and no transition sentinel. The active API
process continued to hold the selected S2 warehouse open. `/release` returned the selected verified
deployment, and Provider Search remained `ready` with CMS data `ok`. Only after those checks passed
was the quarantined directory permanently removed. The removed production copy is not recoverable
in place; the two exact retained copies above remain available.

## Post-cleanup result

The post-cleanup preview reports:

- 286,428,069,888 used bytes and 61,414,723,584 free bytes;
- 78.99% use, now in the warning state rather than critical;
- a passing active-plus-two rollback floor; and
- an allowed zero-additional-byte promotion gate.

The API stayed active on PID `4002795` with zero restarts. The selected deployment remained
`deployment-20260814T172445Z-3cd965d04e`, manager artifact integrity passed, `/release` retained its
expected verification timestamp, and Provider Search stayed ready.

A new full selected-warehouse-sized allocation would project approximately 84.76% use. That is
below the 85% block but leaves little margin, so every further warehouse build or production copy
still requires its own retention preview. The full refresh workspace and backups remained
untouched during that operation.

The refresh workspace was subsequently handled by the separately reviewed
[July refresh workspace compaction](july-refresh-workspace-compaction-2026-08-14.md). That operation
kept every checksum-valid source snapshot while deduplicating repeated payloads and retiring the
verified old staging warehouse; it did not modify or select production.

## Operator learnings

Two diagnostic mistakes were caught and corrected without restarting or changing production data:

1. A recursive text search entered a DuckDB file and outlived the interrupted SSH session. Its exact
   orphaned `grep` process was identified and terminated before the open-file gate was repeated.
   Future reference audits must restrict searches to small text/JSON files and inspect symlinks
   separately.
2. Importing `pipeline.retention` as root from the sealed selected code created a writable
   `pipeline/__pycache__`. The production manager correctly failed artifact integrity. Only the
   generated cache directory was removed, after which the original artifact integrity passed. All
   runbook invocations now use `PYTHONDONTWRITEBYTECODE=1` and Python `-B`.

## Evidence

The read-only previews, candidate identity, three-copy checksum proof, release/comparison evidence,
backup manifest, ledger excerpt, symlink and open-file checks, quarantine validation, post-cleanup
manager/service/release checks, and Provider Search readiness are archived under
[`evidence/retention-cleanup-2026-08-14`](evidence/retention-cleanup-2026-08-14/).
