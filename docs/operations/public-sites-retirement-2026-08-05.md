# Public Sites Retirement Record

> **Executed: 2026-08-05** · **Outcome: retired reversibly**

The owner approved retiring the archived `healthcaredataai.com` site and both the apex and `www`
forms of `blakethomson.studio`. CMS had already moved to its private WireGuard path, so neither site
was a production dependency.

## Completed guest changes

- Ran `docker compose down` in `/opt/personal-website`, stopping and removing
  `personal-website_nginx_1`, `personal-website_website_1`, and their Compose network.
- Verified public TCP 80 and 443 have no listener.
- Disabled and stopped `certbot.timer`. Its only remaining certificates were for the retired sites.
- Retained `/opt/personal-website` and both certificate lineages for possible restoration. No source,
  certificate, CMS data, Docker image, or unrelated artifact was deleted.

The retained certificates expire on 2026-09-06 and will not renew while Certbot is disabled.

## Verification

- CMS API, WireGuard, private firewall, and loopback smoke socket remained active.
- AACT and Tableau PostgreSQL remained healthy and loopback-only.
- Provider Search `/ready` continued to report `cms_data.status=ok`.
- The three retired HTTPS names failed to connect, as expected; `mydoclist.com/ready` returned 200.
- Only `cms-tableau-postgres` and `aact-postgres` remained as running containers on the CMS host.

## External and retention follow-up

DNS still points `healthcaredataai.com`, `blakethomson.studio`, and `www.blakethomson.studio` at
`5.78.148.70`. Remove those records at the DNS provider so the retirement is explicit rather than a
connection failure.

After the desired restoration window, delete `/opt/personal-website` and the two certificate
lineages if the sites will not return. Do not broadly prune Docker. If restoration is requested
before then, restore DNS intentionally, re-enable Certbot, run `docker compose up -d`, validate both
certificate names and sites, and reconfirm CMS remains private and independent.
