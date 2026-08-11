# Command Center catalog freshness deployment — 2026-08-11

> **Status: PROMOTED AND VERIFIED.**
>
> Candidate: `command-center-20260811T162950Z-3a2a81c`
>
> Live predecessor: `command-center-20260811T1550Z-bcd338f`

## Change and safety boundary

Commit `3a2a81c819d3ac2265cbfa60b087a7444b158c1c` adds a **Freshness & provenance**
section to every Data Catalog entry. The section reuses the selected deployment's read-only
`/operations/sources` and `/operations/lineage` responses. It preserves one card per contributing
source and shows publisher data period, publisher version, retrieval, validation, promotion, latest
lifecycle event, cadence, and selected-release evidence status.

The dashboard does not calculate a table-wide “last run.” If declared lineage does not connect a
catalog table to a registered source contract, it reports freshness as unknown instead of guessing
from a filename, table timestamp, row count, or similar source ID. The Contracts table label also
changes from **Last ran** to the more accurate **Latest event**.

This is a static Command Center deployment. It does not change API code or response shapes, restart
the API, build or refresh data, mutate DuckDB, change `release-current`, or alter the temporary
password gate. The selected API deployment and warehouse remain untouched.

## Commit and local verification

The feature commit is pushed to `origin/main`. Before that commit, the required full API suite
returned verbatim:

```text
372 passed, 1 skipped, 135 warnings in 10.60s
```

`node --check dashboard/command-center/app.js` and `git diff --check` passed. Browser QA against
the live selected-release evidence confirmed:

- NPPES displays distinct monthly and weekly source cards rather than one flattened date;
- both cards display their own publisher period and lifecycle timestamps;
- DAC National reports `No registered source path` because current declared lineage does not
  connect that catalog table to a source contract; and
- the desktop layout renders without overlap or truncation that hides evidence.

## Sealed candidate

The detached commit archive is sealed at:

```text
/srv/cms-data-platform/production-artifacts/code/
  3a2a81c-command-center-catalog-freshness-1
```

It is owned by `root:dataops`, has zero group/world-writable files, zero bytecode/cache paths, and
has aggregate file fingerprint:

```text
sha256:46dd7ba9ce98f31c8d81063577d2afa26cfefc8fd32786a1cae6b00d1d29d8d2
```

The immutable candidate release is:

```text
/srv/cms-data-platform/command-center/releases/
  command-center-20260811T162950Z-3a2a81c
```

Its `code` link selects the sealed artifact above. Its `runtime` link reuses unchanged runtime
`runtime-candidate-8985e8a-c26024b3`.

## Rehearsal evidence

The candidate ran as `dataops` with `PYTHONDONTWRITEBYTECODE=1`, Python `-B`, and loopback port
4198. Static HTML, JavaScript, CSS, health, catalog, source contracts, lineage, and run history all
returned 200. `/api/query`, API docs, release metadata, Radar, and `.env` returned 404.

An automated Chromium session loaded the candidate over an SSH loopback tunnel. NPPES showed both
current source cards with exact selected-release evidence. DAC showed the explicit unknown state.
The rehearsal process then stopped cleanly, the tunnel closed, and port 4198 was released. The
candidate artifact still has zero writable and zero bytecode/cache paths.

Post-rehearsal audit confirmed the live selector is still
`command-center-20260811T1550Z-bcd338f`, the live gateway remains PID `3236771`, and the candidate
and predecessor are both intact.

## Approval gate and post-approval plan — completed

Selection of `command-center-20260811T162950Z-3a2a81c` and restart of the public Command Center
gateway required Blake's explicit approval naming this candidate. That approval was received before
the selector changed.

After approval, atomically replace only `/srv/cms-data-platform/command-center/current`, restart
`cms-command-center.service`, and verify the new PID, loopback health, static asset cache keys,
allowed/blocked gateway routes, NPPES multi-source evidence, DAC unknown evidence, nginx Basic Auth,
external HTTPS, and Provider Search readiness. Confirm the predecessor release remains intact for
rollback. Rollback atomically restores the predecessor selector and restarts only the Command Center
gateway; it does not touch the API service, production deployment selector, or warehouse.

## Approved cutover and final evidence

Blake explicitly approved production cutover to
`command-center-20260811T162950Z-3a2a81c`. Final preconditions matched the rehearsal record: the
candidate fingerprint, immutable artifact permissions, code and runtime links, password-file entry,
nginx worker readability, and predecessor selector were intact. Nginx configuration passed, its
container restart count was zero, and rehearsal port 4198 was free. The API remained on
`deployment-20260811T155814Z-6baa26aa69` with PID `3240475`.

The first atomic selector attempt restarted the candidate, but its immediate health request ran
before loopback port 4199 began listening. The cutover safety handler atomically restored the
predecessor and restarted the gateway. The audit found the predecessor selected, service active on
new PID `3247382`, and health returning 200. No API or data selector changed. This was a readiness
probe race, not an application failure.

The approved cutover was retried with a bounded readiness loop. It succeeded and selected:

```text
command-center-20260811T162950Z-3a2a81c
```

The live Command Center gateway PID is `3247555`. Loopback HTML, JavaScript, CSS, health, tables,
catalog, operations overview, sources, lineage, and runs returned 200. Query, docs, OpenAPI, release
metadata, Radar, and hidden-file paths returned 404. Unauthenticated public HTTPS returned 401,
and the nginx container still has restart count zero.

An authenticated external Chromium session loaded `https://healthcaredataai.com`, confirmed both
NPPES monthly and weekly evidence cards with their individual publisher periods and lifecycle
timestamps, and confirmed DAC's `No registered source path` state. No browser console errors were
observed. Provider Search `https://mydoclist.com/ready` returned `status=ready` and
`checks.cms_data.status=ok`.

The gateway and nginx journals contain zero error-level entries for the verification window. Nginx
recorded 14 expected warnings for duplicate `Date` and `Server` headers from proxied API responses;
this pre-existing harmless behavior is documented in the temporary-publication record. The API
selector remains `deployment-20260811T155814Z-6baa26aa69` on PID `3240475`. The candidate artifact
fingerprint remains
`sha256:46dd7ba9ce98f31c8d81063577d2afa26cfefc8fd32786a1cae6b00d1d29d8d2`, and predecessor
`command-center-20260811T1550Z-bcd338f` remains intact for rollback.
