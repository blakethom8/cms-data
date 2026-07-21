# CMS API systemd release layout

`cms-api.service` reads code, warehouse, and runtime through one atomic `release-current` bundle
managed by `python -m pipeline.production`. Its root-run startup check rejects a pending transition,
a mismatched ledger, or changed artifacts before the API process starts. Secrets remain outside Git in
`/etc/cms-data/cms-api.env`; AACT read-only credentials remain in `/etc/aact/reader.env`; and the
checked-in `production-release.env` contains only non-secret release settings. The startup check runs
from the separate immutable `production-ops/current` checkout so rollback does not depend on a broken
candidate runtime.

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
