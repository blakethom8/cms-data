# S3 provider-profile candidate retention cleanup — 2026-08-14

> **Last reviewed: 2026-08-14** · **Status: complete; production unchanged**

The superseded three-table S3 staging database was removed after its failed performance decision
and replacement implementation were durably recorded. This operation reclaimed one exact derived
database only. It did not alter a source snapshot, production artifact, deployment, selected
warehouse, release pointer, API route, or production authorization.

## Target and pre-removal proof

The exact target was:

- release: `warehouse-20260814T222518Z-62c1707278`;
- path: `/srv/cms-data-platform/data/releases/warehouse-20260814T222518Z-62c1707278/warehouse.duckdb`;
- logical file size: 24,472,203,264 bytes;
- SHA-256: `70315198133da2f083f820ba761f1640f32bcfe1f4b1c48df2afbed5b78829a7`;
- link count: one; and
- release state: validation `passed`, promotion `not_promoted`.

The release had been rejected for production because the complete provider-profile route missed
its concurrency and operator-work gates. Its header contents were also superseded by the later
deterministic selection fix. The database was rebuildable from the retained selected baseline,
source snapshots, release record, and merged pipeline code.

Immediately before mutation, the exact file had no open-file holder, matching process argument,
symlink, production deployment reference, or active/rollback-floor reference. The root release
ledger described it only as the unpromoted staging release. `release.json`, `comparison.json`, and
the sealed 43-file audit under
`/srv/cms-data-platform/audits/s3-provider-profile-20260814T220852Z/evidence` were retained.

The read-only retention preview reported 77.54% filesystem use, 66,665,275,392 bytes free, and a
passing active-plus-two rollback floor. A conservative 24,696,061,952-byte replacement candidate
would have projected 84.35% use, only 0.65 percentage points below the 85% promotion block.

## Controlled removal

The database was first renamed on the same filesystem to the exact quarantine path
`.retention-quarantine-warehouse-20260814T222518Z-62c1707278-20260814T2345Z.duckdb`. A shell trap
would have restored the original path on any subsequent guard failure.

While quarantined, the production manager proved all of the following before deletion:

- the selected deployment remained `deployment-20260814T201311Z-0325c353c9`;
- artifact integrity passed and the release pointer matched the ledger;
- the control plane was healthy;
- no transition sentinel existed; and
- there were zero blocking transactions.

Only then was the exact quarantined file removed. The two small per-release evidence files remain
in place. The deleted derived database is not recoverable in place, but its inputs and the evidence
needed to reproduce and audit it remain retained.

## Post-removal result

The final production-manager status remained healthy on verified warehouse
`warehouse-20260814T183948Z-e5ff46dce9` and selected code commit
`ef9a94fef246011ffa4b7410dd6b31c25ddd148d`. No production transition occurred.

The post-removal retention preview reports:

- 256,705,343,488 bytes used and 91,137,449,984 bytes free;
- 70.79% current use, down from 77.54%;
- the same protected active deployment and two validated predecessors;
- no rollback-policy problems; and
- 77.60% projected use for a conservative 24,696,061,952-byte candidate.

Capacity is therefore restored for one isolated six-table provider-profile candidate. The next
step is a staging-only build and evaluation of the core plus claims marts. Production preparation,
authorization, route switching, and cutover remain explicitly out of scope until that candidate
passes parity, plan, concurrency, capacity, isolated-smoke, and rollback-rehearsal gates.
