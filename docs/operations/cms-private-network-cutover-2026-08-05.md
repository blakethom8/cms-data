# CMS Private Network Cutover Record

> **Executed: 2026-08-05** · **Outcome: complete**

This record separates completed guest/server work from external owner actions. It contains no
credential values, database contents, or production release changes.

## Audit conclusion

`healthcaredataai.com` served the archived `frontend/` pages, the legacy dashboard, and a public
`/api/*` proxy that inserted a literal shared API key. Provider Search did not use that domain: its
API and worker called the CMS public host port directly. Repository consumers are the Provider
Search Medicare, practice/profile, place-matching, research, and clinical-trials clients; the local
command center uses an operator-configured tunnel. No other deployed production consumer was found.

The preceding seven days of healthcare-domain logs contained no successful data-query consumer.
Successful API traffic was crawler/indexing traffic to docs, health, capabilities/table metadata,
plus automated scanning. The static website/dashboard are archived presentation surfaces, not the
Provider Search product UI or a production data requirement.

## Completed repository changes

- Added secret-free WireGuard peer templates for CMS and Provider Search.
- Added a dedicated nftables boundary and idempotent iptables-nft compatibility rule.
- Bound Uvicorn to `10.77.0.1:8080` and made the tunnel/firewall systemd dependencies explicit.
- Added a loopback-only systemd socket proxy so existing production cutover smoke checks continue to
  use `127.0.0.1:8080` without reopening Uvicorn.
- Added bounded systemd journal rate limits, focused deployment tests, and the durable deployment,
  verification, rollback, ownership, TLS, and personal-proxy retirement runbook.

## Completed server changes

- Installed `wireguard-tools` on both hosts without rebooting.
- Created and enabled `wg-cms` as `10.77.0.1/30` on CMS and `10.77.0.2/30` on Provider Search.
- Installed/enabled `cms-private-firewall.service`; UDP 51820 is limited to the Provider Search
  public source and TCP 8080 is limited to its WireGuard peer plus loopback smoke traffic.
- Removed the obsolete persistent TCP 8080 exceptions for the Provider Search public address and
  Docker subnets. Retained the terminal drop and loopback rule.
- Rehearsed the exact selected immutable release on a separate tunnel port before changing the live
  service. No DuckDB, release pointer, production artifact, AACT data, or reporting data changed.
- Rotated to a Provider Search-exclusive API key through a temporary private bridge because the
  selected serving release predates scoped-key support. Provider Search API/worker replacements were
  protected by a canary API container; two 80-probe public health windows had zero failures.
- Rebound `cms-api.service` to the WireGuard address and enabled the loopback smoke socket.
- Changed Provider Search's CMS base URL to `http://10.77.0.1:8080` and removed both temporary bridge
  processes after readiness passed.
- Deleted four explicitly identified `.env.production.bak-*` files and all temporary key/rotation
  files. Those plaintext backups are not locally recoverable.
- Replaced the healthcare Nginx data routes with explicit 410 responses and reloaded Nginx in place.
  `personal-website_nginx_1` and `personal-website_website_1` were not stopped or recreated.

## Final verification

- CMS `/health`, authenticated `/release`, authenticated `/practices/capabilities` contract v2, and
  a cardiology practice search returned 200; missing and former credentials returned 401.
- `cms-tableau-postgres` and `aact-postgres` were healthy and remained on loopback ports 5434/5433.
- The Python listener was `10.77.0.1:8080`; the systemd smoke listener was `127.0.0.1:8080`; public
  `5.78.148.70:8080` timed out.
- Provider Search `/ready` reported `cms_data.status=ok` on the final private base URL.
- `healthcaredataai.com/` and `/dashboard/` returned 200; `/api`, `/api/health`, and `/health`
  returned 410.
- The apex and `www` forms of `blakethomson.studio` returned 200 over verified HTTPS.
- `mydoclist.com/` and `/ready` returned 200 over verified HTTPS.
- The production selector remained
  `/srv/cms-data-platform/production/releases/deployment-20260804T163418Z-2ad954a774`, with no
  transition sentinel.

## Owner / external follow-up

No DNS or Hetzner Console action was performed. The owner must still:

- confirm Hetzner snapshots/backups, Cloud Firewall rules, and off-host restore capability;
- delete the four already retired DNS records listed in `hetzner-cms-server.md`;
- decide whether to move or retire the archived `healthcaredataai.com` static surface; and
- migrate or explicitly retire `blakethomson.studio` before stopping/removing the personal-site
  proxy, following the exact sequence in `cms-private-network-runbook.md`.

## Later same-day public-site retirement

The owner subsequently approved retiring both remaining sites. The personal-site Compose project
was brought down, its two containers and network were removed, public 80/443 were verified to have
no listener, and Certbot renewal was disabled. CMS and Provider Search remained healthy. DNS and
final retained deployment/certificate deletion remain external/retention follow-ups; see
`public-sites-retirement-2026-08-05.md`.
