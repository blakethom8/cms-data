# Command Center temporary publication — 2026-08-11

> **Status: PUBLICATION VALIDATED on combined NPPES-first v3 deployment
> `deployment-20260811T145509Z-187674a921`; DNS propagation is in progress.**

## Decision and boundary

The owner approved temporarily publishing the read-only Data Command Center at
`https://healthcaredataai.com` and will point the DNS record to the CMS Hetzner host. This is an
intentional reversal of the public-site retirement for this one surface; it does not restore the
archived website or its broad `/api/*` proxy.

This temporary path uses one shared HTTP Basic Auth account as a short-term access barrier. The
account name is `dashboard`; its password and bcrypt record remain root-owned outside Git in
`/etc/cms-data/command-center.htpasswd`. This is not individual identity, authorization, or an audit
trail. GitHub issue #13 tracks replacement with Cloudflare Tunnel and Access. Do not add customer
data, PHI, credentials, write controls, arbitrary SQL, or other private material to this surface.

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
  boundary, including the temporary HTTP Basic Auth challenge.
- `deploy/command-center/docker-compose.yml`: pinned nginx container with read-only filesystem.
- Dashboard code comes from a sealed commit artifact selected through
  `/srv/cms-data-platform/command-center/current`.
- Secrets remain root-owned outside Git in `/etc/cms-data/cms-command-center.env` and
  `/etc/cms-data/cms-api.env`; the Basic Auth credential file is also outside Git.

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

## Resumed preparation and exact-NPI search stop — 2026-08-11 14:11–14:21 UTC

After Blake explicitly approved the affiliation recovery deployment, the one-shot cutover selected
and verified `deployment-20260811T135930Z-2c3fb4878d`. The API was then restarted with a freshly
generated `command-center` scoped key while preserving the existing shared key. Both keys passed
health checks, the manager remained healthy and verified, and Provider Search `/ready` reported CMS
data `ok`.

The loopback gateway started as PID `3215082`. Static assets, health, tables, catalog, and all four
operations endpoints returned 200. The first exact-NPI dashboard request,
`GET /api/profiles/search?q=1154580017&state=CA&limit=1`, returned 500. The upstream API journal
showed DuckDB rejecting the unquoted result alias in `'medicare' source` because `source` is a
reserved token. This was a publication stop condition. The gateway was stopped again; nginx was
not started, and ports 80, 443, 4199, and 18080 remained closed.

Commit `1b15cca1872d23a71bed005bd4e3ade4e33ab0c8` quotes both exact-NPI source aliases and adds DAC
and registry fallback regression tests. Focused tests returned `7 passed in 0.30s`; the full API
suite before commit returned `366 passed, 1 skipped, 135 warnings in 10.99s`. The fix does not
change response shape, dependencies, or data, so representation version remains 2 and the existing
runtime and warehouse are reused.

A clean detached artifact was sealed at
`production-artifacts/code/1b15cca1872d23a71bed005bd4e3ade4e33ab0c8-command-center-search1`.
Prepare dry-run and real prepare passed, creating
`deployment-20260811T141921Z-b26c3f3d25` with code fingerprint
`sha256:d818c562feaac9d76e844584400ba4ee5aa6caad8814143652253b764d9f7402`.
The unchanged provenance snapshot is `root:dataops` mode `0440`, 32,909 bytes, SHA-256
`fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244`.

Pre-cutover evidence:

- all 15 production smoke checks passed; smoke SHA-256
  `d76f7a44348db4ee7072d07ecbbc9a2807879fcab0bc53ff2191f6669ec80d29`;
- exact-NPI regression returned `[('1154580017', 'medicare', 'TREVAN FISCHER')]`;
- `/release` returned the candidate ID, representation version 2, and 18 source-vintage entries;
- ETag `"deployment-20260811T141921Z-b26c3f3d25:2"` and conditional response `304`;
- Fischer returned 3 groups / 2 hospitals; Do returned 2 groups / 4 hospitals;
- rehearsal stopped, port 18080 released, and the artifact retained zero bytecode paths;
- activate and rollback dry-runs returned exit code 0 with `error_summary: null`;
- live selection remains verified `deployment-20260811T135930Z-2c3fb4878d`.

This candidate has its own approval-gated selection. Do not run the one-shot cutover or resume the
gateway/nginx checks until Blake explicitly approves production cutover to
`deployment-20260811T141921Z-b26c3f3d25`.

## Combined NPPES-first v3 candidate — 2026-08-11 14:55–15:00 UTC

The search-only candidate `deployment-20260811T141921Z-b26c3f3d25` is superseded for deployment
planning and must not be selected. It fixed exact-NPI SQL but still used DAC-first name discovery.
The combined code now uses NPPES as the discovery and profile-identity backbone, treats DAC as
optional Medicare enrichment, merges DAC and NPPES profile locations with per-row provenance, and
shows discovery provenance in the Command Center. The durable API separation is documented in
`docs/provider-serving-contract.md`.

Code commits were pushed to `origin/main` before preparation:

- `feb0c84` — `feat(api): make provider discovery NPPES-first`;
- `18bfc9f` — `feat(command-center): show discovery provenance`.

Before each commit, the full API suite returned respectively:

```text
371 passed, 1 skipped, 135 warnings in 10.46s
371 passed, 1 skipped, 135 warnings in 10.84s
```

The first sealed checkout path was rejected during prepare dry-run because a post-seal Git status
refresh recreated `.git/index` as writable. It was never prepared or referenced by a deployment.
The retained replacement artifact
`18bfc9f88c168af4a48cd1271761f1e589972d8b-provider-v3-1` is a clean detached checkout, has no
writable or cache paths, and has fingerprint
`sha256:b3389fa458dffdf0f0b9189a2db61cbdd9e3d1d428e9bd37f0188dd062ce21fa`.
Dependency diff against the selected serving commit was empty, so the existing runtime was reused.

Prepare dry-run and real prepare passed. The resulting immutable candidate is
`deployment-20260811T145509Z-187674a921`, reusing runtime
`runtime-candidate-8985e8a-c26024b3` and unchanged warehouse
`warehouse-20260811T021837Z-f44c147e30` (20,569,665,536 bytes, SHA-256
`91e2ee4e22fd7b7f612765635e19601ce081730c8b0ddc634dc54d891a345ef2`). Its sealed provenance
snapshot is `root:dataops` mode `0440`, 32,909 bytes, SHA-256
`fd426f571eaf700fd0d70567b128c7195b80a780e3f6d73cdc1e77003df80244`.

The first rehearsal start attempt failed before process launch because a root shell could not open a
temporary log after ownership changed under Linux protected-file rules. The second rehearsal ran
the full smoke suite successfully, then the follow-on ETag script stopped because it mistakenly
used HTTP `HEAD` on a GET-only route. Both attempts touched nothing live and their cleanup traps
released the loopback port. The corrected GET-header contract and feature rehearsal passed.

Canonical smoke verdict:

```text
Production smoke: passed
Evidence: /srv/cms-data-platform/production/evidence/deployment-20260811T145509Z-187674a921/smoke.json
- health: passed
- process_identity: passed
- authentication_required: passed
- practice_capabilities: passed
- practice_search: passed
- provider_profile: passed
- industry_search: passed
- industry_options: passed
- industry_exact_option_round_trip: passed
- industry_detail: passed
- research: passed
- clinical_trials: passed
- explorer_catalog: passed
- required_tables: passed
- warehouse_counts: passed
```

The smoke evidence is `root:root` mode `0440`, 3,511 bytes, SHA-256
`9e8c183c2c26021dc7c343c63fda0391cb6c65d55bfeebdbefd8e82b0259126c`.

Contract and feature evidence:

```text
release_id: deployment-20260811T145509Z-187674a921
representation_version: 3
source_vintages: 18
warehouse_release_id: warehouse-20260811T021837Z-f44c147e30
etag: "deployment-20260811T145509Z-187674a921:3"
conditional_status: 304

Alicia name search: 1396877080, ALICIA TERANDO, nppes + medicare, PASADENA,
  SURGICAL ONCOLOGY
Alicia exact NPI search: 1396877080, nppes + medicare, ALICIA TERANDO
NPPES-only search: 1003000100, nppes, GERARDO GOMEZ
Alicia locations: 625 S FAIR OAKS AVE / SUITE 100 / dac;
  625 S FAIR OAKS AVE STE 270 / nppes
NPPES-only profile: 1003000100, GERARDO GOMEZ, Case Manager/Care Coordinator,
  108 W VICTORIA ST / nppes
Fischer: 3 groups, 2 hospitals
Do: 2 groups, 4 hospitals
Provider evidence: NPPES available, 1 Alicia row
```

The temporary dashboard gateway returned 200 for the static app, JavaScript, health, Alicia search,
and Alicia provider evidence. It returned 404 for `/api/query`, `/api/release`, Radar, and `.env`.
Ports 4198 and 18080 were released, the sealed code artifact retained zero bytecode/cache paths,
and the production Command Center service remains inactive. Public ports 80/443 and loopback 4199
remain closed.

Activate and rollback dry-runs both returned exit code 0 with `error_summary: null`. Final manager
status is healthy with artifact integrity passed, zero blocking transactions, pointer matching the
ledger, and no transition sentinel. Live selection remains the verified predecessor
`deployment-20260811T135930Z-2c3fb4878d`.

Stop here. Production cutover to `deployment-20260811T145509Z-187674a921` requires Blake's explicit
approval naming this candidate. Only after successful cutover may the gateway and nginx publication
checks resume; DNS remains an owner action after local HTTPS validation.

## Combined v3 cutover and server publication — 2026-08-11 15:07–15:12 UTC

Blake explicitly approved production cutover to
`deployment-20260811T145509Z-187674a921`. The immediately preceding checks found the selected
predecessor healthy and verified on API PID `3214625`, with artifact integrity passed, zero blocking
transactions, no transition sentinels, 80 GiB free, the candidate smoke checksum intact, and no
candidate bytecode/cache paths.

The one-shot `pipeline.production_cutover` command returned exit code 0:

```json
{
  "rollback_available": true,
  "selected_deployment_id": "deployment-20260811T145509Z-187674a921",
  "smoke_evidence": "/srv/cms-data-platform/production/evidence/deployment-20260811T145509Z-187674a921/smoke.json",
  "state": "promoted"
}
```

Post-cutover manager status is healthy and verified at `2026-08-11T15:07:37+00:00`, with artifact
integrity passed, zero blocking transactions, pointer matching the ledger, no transition sentinel,
and selected code commit `18bfc9f88c168af4a48cd1271761f1e589972d8b`. The API is active on PID
`3226584`. The immutable warehouse remains
`warehouse-20260811T021837Z-f44c147e30` and was not rebuilt, copied over, or modified in place.

The cutover-owned verified smoke evidence is `root:root` mode `0440`, 3,510 bytes, SHA-256
`154901949140619619a63ab5c44b3e4009b79caea57d222fb8cbad22a45dfa37`. Live contract checks
returned representation version 3, 18 source vintages, ETag
`"deployment-20260811T145509Z-187674a921:3"`, and a 304 conditional round trip. Alicia name search,
her two DAC/NPPES doors, the NPPES-only Gerardo Gomez profile, Fischer's 3 groups / 2 hospitals, and
Do's 2 groups / 4 hospitals all passed against port 8080. Provider Search `/ready` reported CMS data
`ok`, and neither the API nor Command Center journal contained warning-level entries during the
cutover/publication window.

The predecessor `deployment-20260811T135930Z-2c3fb4878d` remains intact for rollback with its
original sealed code target, the reused runtime, and unchanged warehouse link.

The Command Center selector now points at sealed release
`command-center-20260811T1510Z-18bfc9f`, retaining
`command-center-20260811T-local-217a777f` as its rollback bundle. The loopback gateway is active on
PID `3227475`. All reviewed static, health, catalog, profile-search, provider-evidence, and operations
routes returned 200. Query, docs, OpenAPI, release metadata, Radar, path traversal, and hidden files
were blocked; POST was rejected.

The retained Let's Encrypt certificate is valid for `healthcaredataai.com` through
`2026-09-06T00:17:21Z`. The pinned nginx configuration passed `nginx -t`; container
`cms-command-center-nginx` is running with restart count 0 and owns ports 80/443. Local HTTPS tests
using `--resolve healthcaredataai.com:443:127.0.0.1` passed for the application, Alicia discovery,
provider evidence, and operations. Blocked routes remained blocked, HTTP redirected to HTTPS, and
Content Security Policy, Permissions Policy, Referrer Policy, HSTS, MIME-sniffing, and frame headers
were present. The gateway and nginx both emit some defense headers, so duplicate header lines are
expected and harmless.

At the end of the initial server checks, the apex A record still resolved to `204.168.128.251`; the
remaining owner action was to point `healthcaredataai.com` to `5.78.148.70`. The propagation and
external-validation record follows. The stale certificate renewal hook still needs replacement
before the September expiry, and GitHub issue #13 tracks the later Cloudflare Tunnel and Access
migration.

### DNS propagation and external validation — 2026-08-11

Blake changed the apex A record. Cloudflare (`1.1.1.1`), Google (`8.8.8.8`), and Quad9 (`9.9.9.9`)
all returned `5.78.148.70`. Some recursive/client caches still returned the former
`204.168.128.251` immediately afterward, so global propagation was not yet complete.

External checks pinned only DNS resolution to `5.78.148.70` while preserving the real
`healthcaredataai.com` host name and TLS verification. HTTPS and the Let's Encrypt certificate
validated successfully; HTTP redirected to HTTPS. The application, static assets, health, Alicia
NPPES-first search, Alicia provider evidence, and all four operations routes returned 200. Query,
docs, OpenAPI, release metadata, Radar, and hidden files returned 404; POST returned 403. All required
security headers were present. A direct connection to public port 8080 timed out, proving the CMS API
remained outside the public listener boundary. Provider Search `/ready` continued to report CMS data
`ok`.

No additional server change is required for DNS. Allow prior TTLs and local resolver/browser caches
to expire, then repeat an unpinned public request. Certificate-renewal remediation and Cloudflare
Access remain the two infrastructure follow-ups.

### Temporary password gate — 2026-08-11

Blake requested a simple shared password while Cloudflare Access remains deferred. Nginx Basic Auth
is enabled for the entire HTTPS server. The root-owned credential file is mounted read-only into the
pinned nginx container; neither its password nor its bcrypt record is stored in the repository,
deployment documentation, or container image.

Configuration validation passed before the gateway container was recreated. After reload,
unauthenticated HTTPS returned `401`, HTTP continued to redirect to HTTPS with `301`, the nginx
container was running with restart count 0, and the private CMS API `/health` endpoint remained
healthy. This gate reduces casual exposure but remains a shared-secret control: it does not replace
per-user identity, MFA, access policy, or attributable audit logs.

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
