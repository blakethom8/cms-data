# CMS data-platform disaster recovery

> **Last reviewed: 2026-08-13** · **Status: restore contract and rehearsal procedure**

This runbook defines the proof required for an off-host CMS warehouse restore. It does not select a
storage vendor, create paid infrastructure, upload production data, or authorize a production
cutover. Those remain owner-approved operations. The serving host contains public data, but API
credentials and other protected configuration must remain in an encrypted provider snapshot or
secret manager; never place them in the logical bundle.

## Recovery set

Recovery uses two complementary layers:

1. A provider snapshot or equivalent infrastructure backup covers the operating system, systemd
   definitions, firewall and WireGuard configuration, encrypted secret files, AACT PostgreSQL, and
   other host state that is not part of a DuckDB release.
2. An off-host logical bundle covers one selected CMS DuckDB warehouse, its immutable warehouse
   release evidence, source-manifest provenance, and the deployment ledger that selects it.

Every logical bundle has a `disaster-recovery.json` at its root. Schema version 1 requires:

- `backup_id`, creation timestamp, deployment ID, warehouse release ID, warehouse SHA-256, and byte
  size;
- an approved off-host location, copy count, owner, approval timestamp, and next drill date;
- provider-snapshot/control-plane coverage kind, reference, and capture timestamp;
- exactly one file record for each required role: `warehouse`, `deployments`, `warehouse_release`,
  and `source_manifests`;
- relative, confined file paths plus the byte size and SHA-256 of every declared file;
- fixed representative table counts and one ten-digit representative NPI; and
- the failure modes exercised by the drill.

The approved copy count and storage location are owner decisions. Until real values replace the
manifest fields and the provider snapshot reference is verified in its control plane, the recovery
requirement remains open. Do not use placeholder values as restore evidence.

## Prepare the off-host bundle

Assemble the bundle in a new restricted directory from the exact selected deployment. Never follow
`release-current` while copying individual files; resolve and record the selected deployment once,
then copy that immutable warehouse and its matching evidence. Include no `.env` files or keys.

Use the production ledger and warehouse-release evidence to cross-check the deployment ID, release
ID, SHA-256, and byte size. Record hashes with `sha256sum` and allocated file sizes with `stat`; have a
second operator review `disaster-recovery.json` before transport. Transport must preserve the files
without rewriting them. Provider-side encryption, access logging, object versioning or immutability,
and deletion protection should be enabled where available.

The logical bundle is not itself a runnable server image. An isolated application smoke test also
needs a reviewed code/runtime artifact, protected configuration, and the AACT dependency restored
from their approved recovery layers.

## Materialize an isolated restore

Download one retained bundle to an isolated host or volume. The destination must be a new absolute
path on a filesystem with enough headroom. `materialize` verifies every source hash before copying,
copies only manifest-declared regular files, verifies every destination hash, and atomically names
the destination only after success. It refuses an existing destination and has no overwrite mode.

```bash
python -m pipeline.disaster_recovery materialize \
  --bundle-root /srv/restore-input/cms-prod-BACKUP_ID \
  --restore-root /srv/restore-drill/BACKUP_ID \
  --json
```

Record the drill start timestamp immediately before materialization. A failed partial copy is removed
from the tool-owned temporary directory; the source bundle remains unchanged.

## Verify data and application behavior

Start the restored API only on an isolated loopback port with the restored warehouse opened
read-only. Restore AACT separately when clinical-trial checks are in scope. Run the canonical
`pipeline.production_smoke` suite against that isolated process and write fresh smoke evidence
outside the restored bundle. The smoke timestamp must be later than the recorded restore start.

Then run the complete proof:

```bash
python -m pipeline.disaster_recovery verify \
  --restore-root /srv/restore-drill/BACKUP_ID \
  --application-smoke /srv/restore-drill-evidence/BACKUP_ID/smoke.json \
  --restored-after 2026-08-13T08:00:00+00:00 \
  --json \
  > /srv/restore-drill-evidence/BACKUP_ID/restore-proof.json
```

The verifier requires all of the following:

- every declared byte size and SHA-256 matches after restoration;
- the deployment ledger selects the declared deployment and agrees on warehouse identity;
- warehouse-release and source-manifest evidence agree with the deployment;
- DuckDB opens read-only, required tables have exact representative counts, and the representative
  NPI occurs exactly once;
- fresh application smoke passes every canonical verification check for the restored deployment;
- retention ownership, location, count, approval date, next drill date, and control-plane coverage
  are recorded; and
- restore duration and exercised failure modes appear in the generated proof.

Exit `0` is a passed proof. Exit `1` is a failed or incomplete proof. A successful logical restore
does not authorize replacing production.

## Required failure drills

At minimum, exercise and retain evidence for:

- a modified warehouse byte or mismatched hash, which must fail before materialization;
- stale application-smoke evidence, which must fail final verification;
- an existing restore destination, which must not be overwritten; and
- a missing control-plane snapshot or secret recovery layer, which keeps the full-host drill open.

After the drill, record observed duration, bottlenecks, corrective actions, the operator, and the next
drill date on issue #18. Remove the isolated restore only through a separately reviewed cleanup after
the evidence is retained. Keep at least one restore drill per quarter until measured RPO/RTO targets
and provider retention automation are approved.
