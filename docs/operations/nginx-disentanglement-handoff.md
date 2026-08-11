# CMS Proxy Disentanglement Handoff (historical)

> **Prepared: 2026-08-05** · **Status: superseded by completed cutover**

This prompt is retained as decision history only. Do not execute it as current instructions. The
private-network cutover and later public-site retirement are complete; use
[cms-private-network-runbook.md](cms-private-network-runbook.md) for current operations,
[cms-private-network-cutover-2026-08-05.md](cms-private-network-cutover-2026-08-05.md) for the
executed change record, and
[public-sites-retirement-2026-08-05.md](public-sites-retirement-2026-08-05.md) for the retirement
record.

Use the prompt below from a Codex session opened at the root of the `cms-data` repository.

## Prompt

We need to disentangle the production CMS infrastructure from a shared Nginx proxy currently owned
by the personal-website deployment.

First, read `AGENTS.md`, `docs/operations/hetzner-cms-server.md`,
`docs/data-platform-operating-model.md`, and `docs/production-promotion-runbook.md`. Treat the
server overview as the current infrastructure baseline and the operating model/runbook as binding
for production data. Preserve all unrelated working-tree changes.

### Objective

Make the CMS production path independent of `/opt/personal-website` without interrupting CMS,
Provider Search, or `blakethomson.studio`. Prefer a private CMS API with no public data-server proxy.
If a public endpoint is still required, create minimal CMS-owned proxy infrastructure in this
repository.

### Required work

1. Begin with a read-only audit of the repository, `cms-api.service`, nftables, Docker networks,
   `/opt/personal-website/docker-compose.yml`, and the two remaining Nginx configurations. Do not
   print environment values or secrets.
2. Determine exactly what `healthcaredataai.com` serves and identify every current consumer of its
   public routes. Distinguish the archived website/dashboard from production API requirements.
3. Design the target network boundary. Prefer a Hetzner private network or WireGuard between the CMS
   host (`5.78.148.70`, SSH alias `hetzner2`) and Provider Search (`5.78.142.73`). Bind the CMS API to
   the private interface and keep PostgreSQL/DuckDB inaccessible publicly.
4. Add durable, secret-free deployment configuration and an operational runbook to this repository.
   Include firewall rules, bounded logging, health checks, TLS handling if applicable, deployment,
   verification, rollback, and ownership.
5. Establish and validate the replacement path before stopping or changing the current proxy.
6. Rotate the exposed CMS API key atomically between CMS and Provider Search. Use a
   Provider Search-specific credential, remove obsolete plaintext `.env.production.bak-*` files
   from that server, and never display or commit credential values.
7. Verify the CMS health and release endpoints, a representative authenticated data request, an
   unauthenticated rejection, both PostgreSQL health checks, Provider Search's end-to-end CMS path,
   firewall behavior, and every surviving public domain.
8. Preserve `blakethomson.studio` until it is migrated or explicitly retired. Do not stop
   `personal-website_nginx_1` while it remains responsible for that site.
9. Once CMS is independent, document the exact remaining steps to relocate/remove the personal site
   and retire the personal-website proxy.

### Current state and constraints

- `cms-api.service` runs as `dataops` on host port 8080.
- CMS Tableau PostgreSQL and AACT PostgreSQL bind only to loopback and are healthy.
- Public ports 80/443 belong to `personal-website_nginx_1` from
  `/opt/personal-website/docker-compose.yml`.
- Active Nginx files are `healthcaredataai.conf` and `blakethomson.conf` under
  `/opt/personal-website/nginx/conf.d`.
- Harbor, EHI Ignite, Market Explorer, and Voiceflow are already fully decommissioned. Do not
  recreate them.
- Their four retired DNS records remain an external follow-up; do not mistake DNS resolution for a
  live application.
- Do not modify active DuckDB files, production release pointers, CMS/AACT data, or unrelated Docker
  artifacts.
- Do not broadly prune Docker or reboot during the proxy change.
- Use an explicit rollback plan and report completed server changes separately from DNS or Hetzner
  Console actions that still require the owner.
