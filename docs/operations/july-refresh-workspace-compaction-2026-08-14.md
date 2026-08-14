# July refresh workspace compaction — 2026-08-14

> **Outcome:** the legacy July 21–23 refresh workspace was compacted without removing any unique
> source snapshot. The operation reclaimed 26,169,270,272 filesystem bytes, reduced host use from
> 84.92% to 77.71%, and changed the exact S2 production-copy capacity gate from blocked at 90.85%
> projected use to allowed at 83.64%. Production was not cut over or restarted.

## What the workspace was

`/srv/cms-data-platform/refresh-20260721` was the original full-platform rebuild workspace. It
contained 18 acquired public-data runs, two prepared copies of the same AACT dump, transformation
and rehearsal evidence, and the warehouse later extended by the PPEF additive work. It was not an
active serving path, an active rollback target, or temporary query scratch.

The pre-cleanup allocated size was 42,536,947,712 bytes:

- 20,091,387,904 bytes under `releases`, almost entirely the derived DuckDB warehouse;
- 17,435,865,088 bytes under `runs`, containing the original source payloads and manifests;
- 5,009,403,904 bytes under `aact-releases`, containing two prepared AACT copies; and
- less than one MiB of ledgers, comparisons, logs, and rehearsal evidence.

## Retention decision

Keep one checksum-valid copy of every exact source payload and keep all manifests and small
operating evidence. Derived or duplicate payload bytes do not require independent allocation in
this legacy workspace when their retained counterpart is immutable and checksum-identical.

The retained source set matters because publisher URLs are not a durable recovery guarantee. In
particular, weekly and daily snapshots can be replaced or retired even when their historical URL,
source period, byte count, and checksum remain in a manifest. The long-term destination for these
payloads should be off-host object storage; retaining them on this host is the interim recovery
floor.

## Proof and compaction boundary

Before mutation, the retention planner classified the exact workspace as `review_candidate` with
no production-ledger references. No process held a file below it open. Production manager status
was healthy with passed artifact integrity, a matching pointer and ledger, zero blocking
transactions, and no transition sentinel.

The following byte-identical files were deduplicated with hard links so that every original path
continues to exist:

- the two 2,504,635,698-byte AACT `postgres.dmp` files, SHA-256
  `eed4c4847a82423a0729aad8b21242f29e383c9efb8d96c3ce64d30cd77393c3`;
- the General, Research, and Ownership Open Payments 1,204,513,778-byte archives, SHA-256
  `03e143773654b26380c240f4111ed69b7834d4cbc7c6f4e29a22c76fd701f4de`;
- the July workspace's hospital-enrollment payload, matching the managed-data copy at SHA-256
  `220bf4e645fd2ec724fad576a79bc997553b80f7636b4fccaa957ecfc5258f0b`;
- the July workspace's monthly NPPES archive, matching the managed-data copy at SHA-256
  `82b43e03504550112bd375d66c3498a259dbaa2172824d57dc3f3241e9994adf`;
  and
- the July workspace's weekly NPPES archive, matching the managed-data copy at SHA-256
  `a96e5e822c6bb31186ab8679e0ea984a91171b605b6f1524136d14edf5ef6868`.

The old 20,091,252,736-byte workspace warehouse was retired after quarantine. Its SHA-256 was
`b8af099ef264883160797e8d5799b788681e085a53db912975de702e52f59ed7`, matching the protected
validated backup at
`/srv/cms-data-platform/backups/20260811T015700Z-radar-serving-baseline/warehouse.duckdb`. The
warehouse's `release.json`, `comparison.json`, root release ledger, and other small evidence remain.
The now-invalid legacy `staging/warehouse-current` symlink was removed.

Each target was renamed to an exact `.retention-quarantine-20260814` path first. Production health
and identity passed while the old warehouse was quarantined. Only then were the seven quarantined
payloads removed. Those independently allocated copies are not recoverable in place; their
checksum-identical retained files remain.

## Post-cleanup validation

The full source audit opened and hashed the payload for every one of the 18 manifest records:

- checked source runs: 18;
- checksum or path failures: 0; and
- total logical source payload bytes: 17,435,571,364.

Both AACT release paths and all three Open Payments run paths remain present and share their
expected immutable inode within each duplicate family. The compacted workspace reports
17,532,006,400 bytes when traversed independently; that number includes source inodes shared with
the managed-data tree, while filesystem capacity reflects their single physical allocation.

The exact 21,513,908,224-byte production-copy preview now reports:

- 281,786,916,864 used bytes and 66,055,876,608 available bytes;
- 77.71% current use (`warning` rather than `critical`);
- 83.64% projected use after the production copy;
- a passing active-plus-two rollback floor; and
- `promotion_capacity_gate.allowed: true`.

Production remained on deployment `deployment-20260814T172445Z-3cd965d04e`, warehouse
`warehouse-20260814T025428Z-5dac630227`, and API PID `4002795` with zero restarts. Manager artifact
integrity passed, CMS `/health` returned `ok` with 7,395,713 core providers, and Provider Search
`/ready` remained `ready` with `cms_data.status: ok`.

## Next boundary

This cleanup resolves the capacity prerequisite only. It does not copy the S2 candidate into the
production artifact store, install a new operations package, prepare or select a deployment, or
authorize a cutover. The next production step must begin with a fresh exact-byte preview and follow
the prepare, isolated smoke, rollback rehearsal, and separately authorized selection sequence.
