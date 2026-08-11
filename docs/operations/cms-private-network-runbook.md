# CMS Private Network and Proxy Retirement Runbook

> **Last reviewed: 2026-08-05** · **Status: production operating procedure**

## Decision and ownership

The production CMS API is a private upstream for Provider Search. WireGuard provides the host-to-host
boundary because the two servers do not currently have Hetzner private-network interfaces. CMS owns
the tunnel endpoint, nftables policy, API unit, credentials, and this runbook. Provider Search owns
its tunnel peer, `CMS_API_BASE_URL`, `CMS_API_KEY`, and end-to-end readiness check.

The fixed tunnel addresses are `10.77.0.1/30` on CMS and `10.77.0.2/30` on Provider Search. CMS
listens for WireGuard only from Provider Search's public address on UDP 51820. Uvicorn binds only to
`10.77.0.1:8080`; DuckDB remains an in-process read-only file and both PostgreSQL containers remain
published only to `127.0.0.1`. The API still requires a Provider Search-specific `X-API-Key`.

`cms-api-loopback.socket` exposes `127.0.0.1:8080` only on the CMS host and forwards it to the
private bind with `systemd-socket-proxyd`. This preserves the production cutover tooling's
loopback-only smoke boundary. It is not reachable from Docker containers or a remote interface and
does not make Uvicorn a public listener.

TLS is not used on the private HTTP hop: WireGuard authenticates and encrypts the transport, while
the API key authenticates the application consumer. Do not publish `10.77.0.1:8080`, add a public
reverse proxy, or reuse the Provider Search key for another consumer. If Hetzner private networking
is attached later, rehearse it as a new path before replacing WireGuard.

`healthcaredataai.com` is not a production CMS dependency. Its root, `frontend/`, and `dashboard/`
are archived static surfaces. Provider Search calls the CMS host directly and does not use that
domain. During the 2026-08-05 audit, the public `/api/*` proxy injected a shared key; the preceding
seven days contained no successful data-query consumer traffic, only crawler/indexing requests to
docs, health, and table metadata. The public API must remain retired.

## Durable files and secrets

Install these repository-owned files:

| Repository file | CMS destination |
| --- | --- |
| `deploy/network/cms-wireguard.conf.example` | `/etc/wireguard/wg-cms.conf` after replacing placeholders; `root:root` `0600` |
| `deploy/network/cms-private-firewall.nft` | `/etc/cms-data/cms-private-firewall.nft`; `root:root` `0644` |
| `deploy/systemd/cms-private-firewall.service` | `/etc/systemd/system/cms-private-firewall.service`; `root:root` `0644` |
| `deploy/systemd/cms-api.service` | `/etc/systemd/system/cms-api.service`; `root:root` `0644` |
| `deploy/systemd/cms-api-loopback.socket` | `/etc/systemd/system/cms-api-loopback.socket`; `root:root` `0644` |
| `deploy/systemd/cms-api-loopback.service` | `/etc/systemd/system/cms-api-loopback.service`; `root:root` `0644` |

Install `deploy/network/provider-search-wireguard.conf.example` as
`/etc/wireguard/wg-cms.conf` on Provider Search after replacing placeholders. Generate each private
key on the host where it is used. Only exchange public keys. Never write WireGuard private keys or
API credentials to Git, shell history, logs, deployment evidence, or command output.

The CMS credential file is `/etc/cms-data/cms-api.env`. Production should use a scoped entry named
`ps-prod` in `CMS_API_KEYS`; the Provider Search deployment reads its matching value from
`/opt/provider-search/.env.production`. Keep both files `root`-owned and mode `0600`.

## Firewall and logging

`cms-private-firewall.service` loads a dedicated `inet cms_private` nftables table without flushing
Docker or host-managed rules. It accepts the UDP handshake only from `5.78.142.73`, accepts TCP 8080
only from `10.77.0.2` over `wg-cms` (plus on-host checks to the tunnel address), and drops other
traffic to those destinations. It also installs the same narrow accept ahead of the legacy
iptables-nft `INPUT` chain's terminal TCP 8080 drop; this is required because an accept verdict in
one nftables base chain does not bypass a later base chain. The Uvicorn bind is a second boundary:
public `5.78.148.70:8080`
must have no listener even if firewall state is lost.

The CMS systemd unit sends output to journald with a 30-second/1,000-message rate limit. Keep host
journal retention bounded separately (for example `SystemMaxUse=1G` and `MaxRetentionSec=14day`)
only after reviewing total host logging requirements. WireGuard does not require access logging.
Do not log API keys, request authorization headers, or query strings.

## Establish the replacement path

Stop if either production selector has a pending transition, either database is unhealthy, the
current API smoke check fails, or Provider Search readiness cannot reach the existing path.

1. Capture `systemctl cat cms-api.service`, nftables rules, listeners, WireGuard status, the selected
   deployment ID, and checksummed rollback copies of files that will change. Do not capture secret
   file contents in operator output.
2. Install `wireguard-tools` on both hosts without rebooting. Generate host-local keys, install the
   two `wg-cms.conf` files, install the CMS firewall file/unit, run `systemctl daemon-reload`, and
   enable/start `cms-private-firewall.service` and `wg-quick@wg-cms.service`.
3. Verify `wg show wg-cms` reports a recent handshake and that Provider Search can reach
   `10.77.0.1` over the tunnel. Before rebinding the live API, start the selected immutable API
   bundle temporarily on `10.77.0.1:18080`; verify `/health`, `/release`, authenticated
   `/practices/capabilities`, and unauthenticated rejection. Stop only that rehearsal process.
4. If the selected serving release supports `CMS_API_KEYS`, add a generated `ps-prod` scoped key
   while retaining the old shared key, restart once, and verify both credentials during the atomic
   overlap window. If the selected release predates scoped-key support, do not mutate its immutable
   code: run the same selected bundle temporarily on `10.77.0.1:18080` with only the new key, prove
   its health/release/query/auth behavior, and use that as the overlap bridge.
5. Change Provider Search's `CMS_API_BASE_URL` to `http://10.77.0.1:8080` and `CMS_API_KEY` to the
   new `ps-prod` value, recreate only its API and worker containers, and require `/ready` to report
   `cms_data.status=ok`. Roll back those two settings immediately if it does not.
6. Install the checked-in CMS API and loopback socket units, daemon-reload, enable the socket, and
   restart the API once. Verify that the Python process listens only on `10.77.0.1:8080` and the
   systemd socket only on `127.0.0.1:8080`, then repeat all CMS and Provider Search checks. If a
   single-key bridge was used, move Provider Search from port 18080 to 8080 before removing it.
7. Remove the old shared CMS key, restart once, and verify the new key still succeeds while the old
   key and an absent key both fail. Remove obsolete `.env.production.bak-*` files from Provider
   Search only after the new credential/path passes.
8. Replace `healthcaredataai.conf` with a static-only archived-site configuration that returns HTTP
   410 for `/api`, `/api/*`, and `/health`. Validate `nginx -t` inside the existing container and
   reload Nginx; do not stop or recreate it. This preserves `blakethomson.studio`.

## Required verification

Record status codes and identities, never response secrets:

- CMS `/health` and authenticated `/release` return 200 on `http://10.77.0.1:8080`;
- authenticated `/practices/capabilities` returns 200 and contract version 2;
- the same data route without a key returns 401 or 403;
- `cms-tableau-postgres` and `aact-postgres` report healthy and still publish only to loopback;
- Provider Search `/ready` reports `cms_data.status=ok`, exercising its real configured client;
- `ss` shows the Python process on `10.77.0.1:8080`, the systemd smoke socket on
  `127.0.0.1:8080`, and PostgreSQL only on loopback;
- a request to public `5.78.148.70:8080` fails, while UDP 51820 is not accepted from other sources;
- public TCP 80/443 have no listener on the CMS host, and the retired
  `healthcaredataai.com`/`blakethomson.studio` names do not unexpectedly serve from it; and
- `https://mydoclist.com/ready` returns 200 with the expected certificate name.

Also confirm the retired `personal-website` Compose containers remain absent. DNS for retired names
is an external cleanup item and is not evidence of a live service.

## Rollback

Keep checksummed pre-change unit, Nginx configuration, and non-secret metadata until verification is
complete. Roll back in reverse order:

1. Restore the static/API Nginx configuration and reload (never stop the proxy).
2. Restore Provider Search's previous base URL and credential, then recreate only API and worker;
   confirm `/ready` reaches the old path.
3. Restore the prior CMS unit binding and credential configuration, daemon-reload, and restart;
   confirm the loopback smoke checks and existing public-IP allowlist.
4. Stop/disable `wg-quick@wg-cms.service` and `cms-private-firewall.service` only after the old path
   is healthy. Do not remove packages during incident rollback.

Rollback never changes `release-current`, production artifacts, DuckDB, AACT, reporting PostgreSQL,
or source data. A credential rollback must restore a still-valid previous value; never invent or
print one during an incident.

## Retired personal-site retention cleanup

> **Completed on the guest 2026-08-05:** both sites were intentionally retired, the Compose project
> was brought down, public 80/443 were verified closed, and Certbot renewal was disabled. The
> deployment/certificates remain for reversible restoration. DNS removal and final retained-file
> deletion remain owner actions; see `public-sites-retirement-2026-08-05.md`.

The guest-side retirement is complete. Remaining work is limited to owner-controlled DNS and the
chosen restoration-retention window:

1. Remove retired DNS records at the authoritative DNS provider.
2. After the restoration window, approve deletion of the retained `/opt/personal-website` files and
   expired certificate lineages. Do not broadly prune Docker or unrelated certificates.
3. Reconfirm public 80/443 have no listener and CMS/Provider Search readiness remains healthy after
   retained-file cleanup.

Restoration before that deadline is an explicit publication decision: restore DNS intentionally,
re-enable certificate renewal, start only the retained Compose project, verify both sites, and
confirm CMS remains private and independent.

Hetzner Cloud Firewall, snapshots/backups, and future private-network attachment remain owner-run
Console actions because they cannot be proven or changed safely from the guest.
