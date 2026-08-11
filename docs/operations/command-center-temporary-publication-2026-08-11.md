# Command Center temporary publication — 2026-08-11

> **Status: BLOCKED pending profile-affiliations recovery cutover approval; dashboard and public
> listeners are not running.**

## Decision and boundary

The owner approved temporarily publishing the read-only Data Command Center at
`https://healthcaredataai.com` and will point the DNS record to the CMS Hetzner host. This is an
intentional reversal of the public-site retirement for this one surface; it does not restore the
archived website or its broad `/api/*` proxy.

This temporary path is not identity-authenticated. It is suitable only for the public CMS-derived
data and non-secret operating metadata already represented by the Command Center. GitHub issue #13
tracks replacement with Cloudflare Tunnel and Access. Do not add customer data, PHI, credentials,
write controls, arbitrary SQL, or other private material to this surface.

The serving boundary is:

```text
public HTTPS -> pinned nginx container -> 127.0.0.1:4199 Command Center gateway
                                      -> 127.0.0.1:8080 private CMS API
```

- The API remains bound only to WireGuard plus its loopback smoke socket.
- The browser never receives an API key.
- The gateway holds a dedicated scoped `command-center` key.
- Only reviewed GET/HEAD paths used by the dashboard are accepted. `/query`, API docs, release,
  Radar, and arbitrary proxy paths return 404 or 405.
- The dashboard cannot acquire, refresh, mutate, promote, or roll back data.

## Versioned components

- `deploy/systemd/cms-command-center.service`: unprivileged loopback gateway service.
- `deploy/command-center/nginx.conf`: HTTPS, security headers, connection/rate limits, and proxy
  boundary.
- `deploy/command-center/docker-compose.yml`: pinned nginx container with read-only filesystem.
- Dashboard code comes from a sealed commit artifact selected through
  `/srv/cms-data-platform/command-center/current`.
- Secrets remain root-owned outside Git in `/etc/cms-data/cms-command-center.env` and
  `/etc/cms-data/cms-api.env`.

## Publication checks

Before starting public listeners:

1. Confirm `cms-api`, WireGuard, the private firewall, production manager, and Provider Search
   readiness are healthy; record listeners and the selected production deployment.
2. Confirm the retained `healthcaredataai.com` certificate is valid and its private key is not
   printed or copied.
3. Generate a dedicated random `command-center` scoped key without displaying it. Install it in the
   gateway env and add the named key to `CMS_API_KEYS`; preserve every existing key.
4. Restart the API, confirm the selected bundle and PID, and prove both Provider Search readiness
   and the new scoped key work before proceeding.
5. Start the loopback gateway. Confirm static assets and every allowed route, and confirm `/query`,
   docs, release, Radar, encoded paths, POST, and hidden files are unavailable.
6. Validate nginx configuration, start only the dedicated nginx container, and test HTTPS using
   `curl --resolve healthcaredataai.com:443:127.0.0.1` before DNS changes.

After the owner points the apex A record to `5.78.148.70`, validate from outside the host:

- apex HTTP redirects to HTTPS;
- HTTPS certificate and security headers are valid;
- Overview, Catalog, Lineage, Contracts, Operations, and Provider Evidence load;
- blocked routes remain blocked;
- CMS API public port 8080 remains unreachable; and
- Provider Search readiness remains healthy.

The retained certificate expires 2026-09-06. After DNS resolves to this host, replace its stale
standalone renewal hooks with a reviewed renewal method and enable/test renewal. Do not enable the
old hook: it names the retired `personal-website_nginx_1` container.

## Preparation and stop record — 2026-08-11

GitHub issue [#13](https://github.com/blakethom8/cms-data/issues/13) tracks the deferred Cloudflare
Tunnel and Access hardening. The sealed dashboard artifact, loopback systemd unit, and dedicated
nginx configuration were installed on the Hetzner host. Systemd and nginx configuration validation
passed, but neither service was started and ports 80, 443, 4199, and 18080 were confirmed closed
after rehearsal.

Publication stopped during the scoped-key API restart because the selected profile-affiliations
code artifact failed the immutable startup check. The API environment was restored exactly, the
intact predecessor was selected through the production manager, and its full smoke and verification
passed. The proposed Command Center key is not active. Full incident and clean recovery-candidate
evidence are recorded in `docs/profile-affiliations-code-deploy-2026-08-11.md`.

Do not resume this runbook until the clean affiliation candidate is explicitly approved, selected,
and verified. Resumption must start again at publication check 1; it must not assume the earlier API
key or process state remains valid.

## Rollback

Rollback does not touch `release-current`, DuckDB, AACT, or production artifacts:

1. Stop and disable `cms-command-center.service`.
2. Run `docker compose down` only in `/srv/cms-data-platform/command-center/nginx`.
3. Confirm public ports 80/443 and loopback 4199 have no listeners.
4. Remove the `command-center` entry from `CMS_API_KEYS`, restart `cms-api`, and confirm Provider
   Search readiness and the selected deployment.
5. Point DNS away from the CMS host.

Retain the sealed dashboard artifact, deployment configuration, certificate lineage, and logs for
review unless the owner separately approves deletion.
