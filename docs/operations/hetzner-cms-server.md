# Hetzner CMS Production Server

> **Last reviewed: 2026-08-05** · **Status: current infrastructure inventory and transition guide**

This document describes the production host for the CMS/AACT data platform and its dependency with
Provider Search (MyDocList). It records infrastructure facts, not secret values. The repository's
operating model and production-promotion runbook remain authoritative for data releases.

## Intended Role

The server at `5.78.148.70` (SSH alias `hetzner2`, hostname `ubuntu-32gb-hil-2`) should be a narrowly
scoped production data host. Its durable responsibilities are:

- the immutable CMS DuckDB releases and refresh artifacts under `/srv/cms-data-platform`;
- the read-only `cms-api.service` used by Provider Search;
- the hosted AACT PostgreSQL mirror and clinical-trials adapter;
- the Tableau reporting PostgreSQL projection and approved reporting automation.

It should not be a general application or portfolio host. Legacy healthcare demonstrations and the
personal website are outside the CMS production boundary.

## Capacity and Access

| Item | Audited state |
| --- | --- |
| Operating system | Ubuntu 24.04.4 LTS; kernel 6.8.0-101 |
| Capacity | 16 vCPU, 30 GiB RAM, 338 GiB root disk |
| Utilization after public-site retirement | 176 GiB disk used; 24 GiB memory available; serving load average about 0.03 |
| Swap | None |
| Maintenance | Reboot required; security and kernel updates pending |
| SSH | Key-based root access is configured; password authentication remains enabled |
| Host firewall | nftables; inbound policy requires review because the base input policy is accept |
| Public listeners | SSH 22 only; public 80/443 have no listener |
| Restricted service | CMS API on WireGuard `10.77.0.1:8080`; loopback-only smoke proxy on `127.0.0.1:8080` |

Hetzner Cloud backups, snapshots, Cloud Firewall rules, plan metadata, and billing cannot be proven
from the guest. Confirm them in the Hetzner Console before infrastructure changes.

## Runtime and Data Layout

| Component | Runtime / path | Current state |
| --- | --- | --- |
| CMS API | `cms-api.service`, user `dataops`, `10.77.0.1:8080` | Active; private-only since 2026-08-05 |
| CMS reporting database | `cms-tableau-postgres`, loopback `:5434` | Healthy |
| AACT database | `aact-postgres`, loopback `:5433` | Healthy |
| CMS platform storage | `/srv/cms-data-platform` | Approximately 111 GiB at initial audit |
| AACT storage | `/srv/aact` | Approximately 53 GiB at initial audit |
| Former public proxy | `/opt/personal-website` Compose project | Down since 2026-08-05; files retained for reversible retirement |
| Former personal website | `blakethomson.studio` | Retired; no container or listener |

The CMS platform storage included approximately 50 GiB of production artifacts, 40 GiB in a dated
`refresh-20260721` tree, 9.8 GiB of reporting data, 6.1 GiB of working data, and 5.6 GiB of backups.
Do not remove release artifacts, refresh output, databases, or the observed backup until retention
and restore requirements are explicitly approved.

## Provider Search Dependency

Provider Search runs on `5.78.142.73` and calls this server's CMS API for Medicare provider,
practice, profile, and clinical-trial functionality over WireGuard. The fixed peer addresses are
`10.77.0.1/30` (CMS) and `10.77.0.2/30` (Provider Search); Uvicorn binds only to the CMS address.
The application sends a Provider Search-specific `X-API-Key`, and the host firewall admits TCP 8080
only from the Provider Search tunnel peer. A loopback systemd socket preserves on-host production
smoke checks without making the API reachable from a public or Docker interface.

The exposed shared key discovered in plaintext backups was rotated atomically on 2026-08-05. The
four obsolete `.env.production.bak-*` files and temporary rotation files were deleted from Provider
Search. The selected serving code now supports named `CMS_API_KEYS`, but production configuration
still carries the one Provider Search-exclusive value through legacy `CMS_API_KEY`. Move that same
value to the `ps-prod` scoped entry during a separately approved configuration restart; do not
rotate the value or change the Provider Search client merely to rename its server-side identity.
Never send MyDocList account, search-history, billing, or customer analytics data back into the CMS
warehouse.

## Retired Public Web and Proxy

The owner retired the archived `healthcaredataai.com` surface and `blakethomson.studio` on
2026-08-05. The `/opt/personal-website` Compose project is down, both containers and its Docker
network were removed, and public ports 80/443 have no listener. Certbot renewal is disabled because
the only remaining certificates belonged to these retired sites.

The deployment directory and certificates are retained for a reversible “for now” retirement. The
certificates expire on 2026-09-06 and will not renew while the timer is disabled. Do not restart the
Compose project without restoring renewal/health monitoring and intentionally republishing the
domains.

No CMS production route depends on this proxy. The public data-server proxy and its literal shared
key were removed on 2026-08-05 after the request log and repository audit found no surviving data
consumer. DNS for `healthcaredataai.com`, `blakethomson.studio`, and `www.blakethomson.studio` still
points to this host and must be removed at the DNS provider.

## Capacity Assessment

The host is CPU-oversized for steady serving but not obviously oversized for its combined serving,
refresh, immutable-release, AACT, and rollback role. At the 2026-08-05 sample, the 16-vCPU host had a
load average near 0.03; the API used about 0.5% CPU and roughly 0.4 GiB RSS; and it served 217 requests
over the preceding 24 hours. AACT PostgreSQL used about 4.2 GiB, reporting PostgreSQL about 0.4 GiB,
and the host had 24 GiB available.

Storage and batch headroom, not request traffic, set the current floor: 176 GiB of 338 GiB was used,
including 111 GiB under `/srv/cms-data-platform` and 53 GiB under `/srv/aact`. Production policy must
retain immutable candidates and rollback releases, while pipeline builds may use a 12 GiB DuckDB
memory limit. With no swap, a 16 GiB replacement would be unnecessarily tight during a refresh even
though it could serve current traffic.

Keep the present size until at least one full refresh/promotion has been measured and retention has
been approved. A later one-box target should retain at least 24 GiB RAM and 320 GiB usable storage;
32 GiB RAM is the safer floor if refreshes continue on the production host. The cleaner long-term
right-size is a smaller 4–8 vCPU, 16 GiB serving host plus separate/ephemeral 32 GiB build capacity
and durable release storage. Do not resize from a single steady-state sample or before confirming
snapshots, restore time, disk layout, and Hetzner Console peak graphs.

## Completed Decommissioning

On August 5, 2026, the owner approved complete removal of Harbor, EHI Ignite, Market Explorer, and
Voiceflow. Their Compose projects, containers, deployment directories, dedicated images, proxy
routes, TLS certificates, and residual password files were removed. No project-labeled Docker
volumes remained. The server copies are not locally recoverable; source is retained locally or in
GitHub.

The following DNS names still resolved to this server after removal and must be deleted at the DNS
provider:

- `ehi.healthcaredataai.com`
- `harbor.healthcaredataai.com`
- `market.healthcaredataai.com`
- `voiceflow.healthcaredataai.com`

Revoke external credentials that were exclusive to those deployments.

## Known Operational Work

1. Confirm Hetzner backups, Cloud Firewall configuration, and an off-host restore procedure.
2. Apply pending updates and perform a controlled reboot with pre/post tunnel and health verification.
3. Review SSH password authentication and move routine deployment away from direct root use.
4. Move the existing Provider Search-exclusive value from `CMS_API_KEY` to the `ps-prod`
   `CMS_API_KEYS` entry during an approved configuration restart, without changing its value.
5. Investigate `cms-data-status.service`: the audit reported 9 current and 9 stale sources.
6. Confirm whether the disabled `cms-tableau-reporting.timer` is intentional.
7. Configure Docker log rotation and system journal retention.
8. Define retention for dated refreshes, production artifacts, downloads, and older images.
9. Remove the retired public DNS records; after the chosen retention window, delete the retained
   `/opt/personal-website` deployment and expired certificates if restoration is no longer wanted.

## Verification Baseline

After the 2026-08-05 private-network cutover:

- `cms-api.service`, `wg-quick@wg-cms.service`, `cms-private-firewall.service`, and the loopback
  smoke socket were active;
- `cms-tableau-postgres` and `aact-postgres` were healthy;
- CMS health/release/capabilities/practice search returned 200 with the new key, while no key and the
  former key returned 401;
- Provider Search `/ready` reported CMS data `ok` over `10.77.0.1:8080`;
- public `5.78.148.70:8080` was unreachable;
- `healthcaredataai.com` and both `blakethomson.studio` names failed to connect as expected, while
  `mydoclist.com/ready` returned 200;
- public ports 80/443 had no listener and only the two database containers remained on the CMS host.

Re-run these checks after every networking or proxy change. Service activity alone is insufficient:
also validate authentication, an actual CMS query, MyDocList's consumer path, firewall behavior,
remaining public domains, and rollback readiness.
