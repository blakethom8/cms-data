# CMS API systemd release layout

> **Last reviewed: 2026-08-18** · **Status: current production service and monitor layout**

`cms-api.service` reads code, warehouse, runtime, and an optional independent utilization sidecar
through one atomic `release-current` bundle
managed by `python -m pipeline.production`. Its root-run startup check rejects a pending transition,
a mismatched ledger, or changed artifacts before the API process starts. Secrets remain outside Git in
`/etc/cms-data/cms-api.env`; AACT read-only credentials remain in `/etc/aact/reader.env`; and the
checked-in `production-release.env` contains only non-secret release settings. The startup check runs
from the separate immutable `production-ops/current` checkout so rollback does not depend on a broken
candidate runtime.

The production API process binds only to the WireGuard address `10.77.0.1:8080` and requires
`wg-quick@wg-cms.service` plus the repository-owned private firewall unit. A socket-activated,
loopback-only `systemd-socket-proxyd` listener at `127.0.0.1:8080` preserves the existing cutover and
smoke contract without exposing Uvicorn on a public or Docker interface. See
`docs/operations/cms-private-network-runbook.md` for installation and rollback.

The checked-in `production-release.env` also binds the measured DuckDB executor settings to the
serving artifact. The main warehouse values are pool size `2`, DuckDB threads `8`, a 4-second
acquisition deadline, and a `2GB` DuckDB memory limit. The utilization sidecar has its own pool size
`2`, DuckDB threads `4`, 4-second acquisition deadline, and `2GB` memory limit. Rehearsals must load
this exact file after the
secret environment files. Installing it under `/etc/cms-data/` and restarting the service remain
cutover actions; merging or preparing a bundle does neither.

Install these files only after production has been bootstrapped to a verified legacy rollback
release. Preserve checksummed copies of the prior unit and environment files first. A unit install
requires `systemctl daemon-reload` and one controlled restart followed by
`python -m pipeline.production_smoke`. Do not install the unit while any production deployment
journal transaction is pending.

`cms-data-status.timer` runs the read-only publisher discovery monitor once each day at 06:15 UTC,
with up to 15 minutes of randomized delay. It resolves source provenance from
`production/evidence/<selected-deployment-id>/source-manifests.json`; because the deployment ID comes
from the one validated `release-current` bundle, a newer staging manifest cannot make the selected
production warehouse appear current. A missing snapshot deliberately reports installed versions as
`unknown`.

The selected deployment evidence directory must be `root:dataops` mode `0750`, and its manifest
snapshot must be `root:dataops` mode `0440`, so the unprivileged monitor can read but never replace
the evidence.

Install and verify the monitor without restarting the API:

```bash
install -o root -g root -m 0644 deploy/systemd/cms-data-status.service \
  /etc/systemd/system/cms-data-status.service
install -o root -g root -m 0644 deploy/systemd/cms-data-status.timer \
  /etc/systemd/system/cms-data-status.timer
systemctl daemon-reload
systemctl enable --now cms-data-status.timer
systemctl start cms-data-status.service
systemctl show cms-data-status.service -p Result -p ExecMainStatus
journalctl -u cms-data-status.service -n 200 --no-pager
```

Exit `0` means every source with provenance is current; `1` means at least one source is stale or
unknown; `2` means publisher discovery, manifest parsing, or production control-plane validation
failed. Nonzero results are monitoring signals, not reasons to auto-refresh. The timer will run again
after a failed oneshot. The service has a read-only filesystem view, makes only metadata requests,
does not load secrets, and never opens DuckDB.

## NPPES Radar staging reconciliation

`cms-nppes-radar-reconciliation.timer` gives the CMS data-platform operator a daily 07:15 UTC
polling opportunity. Its oneshot service idempotently acquires the latest monthly and weekly NPPES
archives, then builds and compares an immutable staging candidate. It never promotes a candidate,
changes `production/release-current`, or restarts the API. The backup-manifest path is configured in
the root-owned `/etc/cms-data/nppes-radar-reconciliation.env`; use the checked-in example as the
non-secret shape.

At a monthly rollover, the new monthly file is a complete baseline. The service may build a
monthly-only candidate when no weekly period begins on or after that baseline. Older weekly files
are superseded and are not replayed. A later run adds consecutive eligible weeklies in publisher
period order. After monthly promotion, the freshness monitor reports an older latest-weekly period
as covered when its end date is on or before the installed monthly period. A publisher-version no-op
exits successfully; a failed acquisition, validation, build, or comparison leaves production
unchanged and is visible in the unit result and journal.

Installation or enablement is a production change and requires the runbook's explicit approval.
After approval, install the service, timer, and environment file, then verify the first run without
promoting its output:

```bash
install -o root -g root -m 0644 deploy/systemd/cms-nppes-radar-reconciliation.service \
  /etc/systemd/system/cms-nppes-radar-reconciliation.service
install -o root -g root -m 0644 deploy/systemd/cms-nppes-radar-reconciliation.timer \
  /etc/systemd/system/cms-nppes-radar-reconciliation.timer
test -r /etc/cms-data/nppes-radar-reconciliation.env
systemctl daemon-reload
systemctl enable --now cms-nppes-radar-reconciliation.timer
systemctl start cms-nppes-radar-reconciliation.service
systemctl show cms-nppes-radar-reconciliation.service -p Result -p ExecMainStatus
journalctl -u cms-nppes-radar-reconciliation.service -n 200 --no-pager
```
