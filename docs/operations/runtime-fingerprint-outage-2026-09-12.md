# Runtime Fingerprint Outage Recovery — 2026-09-12

## Summary

`cms-api.service` stopped serving at 2026-09-12 06:37 UTC after an operating-system update changed
the bytes reached through the selected virtual environment's `bin/python3 -> /usr/bin/python3`
symlink. The root-run startup check correctly refused to start because the computed runtime
fingerprint no longer matched the verified deployment ledger. By inspection time the unit had
attempted more than 1,600 restarts.

All retained rollback deployments referenced the same runtime artifact, so selecting a predecessor
would have failed the same integrity check. No ledger, bundle link, fingerprint, or immutable
artifact was edited in place.

## Recovery

A new runtime was built from the selected deployment's captured package lock with
`python3 -m venv --copies`. Its Python launchers are regular root-owned files inside the sealed
runtime. The existing verified code, warehouse, and utilization artifacts were reused.

The replacement bundle `deployment-20260912T181713Z-00649363f8` passed:

- production-manager prepare dry-run and recorded prepare;
- an isolated `dataops` rehearsal on `127.0.0.1:18080`;
- all 27 production smoke checks, including process and open-file identity;
- activation and rollback transition dry-runs; and
- the one-shot production cutover, smoke, and verification workflow.

After recovery, production-manager status reported healthy control-plane and artifact integrity,
no blocking transaction or sentinel, and the selected state `verified`. `cms-api.service` was
`active/running` with zero restarts, and both remote loopback health and the existing local SSH
tunnel at `127.0.0.1:9080` returned `{"status":"ok","core_providers":7395713}`.

## Evidence

Host evidence is retained at
`/srv/cms-data-platform/incident-evidence/2026-09-12-runtime-fingerprint/`. It includes the pre- and
post-recovery manager status, systemd state, full outage journal, pre-recovery ledger copy, health
response, and checksum inventories. The failed deployment and runtime remain untouched.

The sealed rehearsal and cutover smoke evidence hashes are:

- `smoke.rehearsal.json`: `sha256:42d238561ee7ed82b20c3560d8c54fc6b03f448ada7f13e692a0822cca64a936`
- `smoke.json`: `sha256:9c543de1335521ef995838a9db6cd9dd1df91dde62af1345a0ef8eafc5b2af7b`

## Prevention

Production artifact validation now rejects every symlink that resolves outside its sealed tree.
This closes the fingerprint boundary over the immutable artifact and prevents a virtual environment
backed by mutable system interpreter files from being prepared.
