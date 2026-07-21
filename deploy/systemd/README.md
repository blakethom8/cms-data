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
