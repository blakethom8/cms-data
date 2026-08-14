# CMS data platform production roadmap

> **Last reviewed: 2026-08-13** · **Status: active execution authority**
> **Planning horizon:** August–October 2026

**Program issue:** [#24 — stabilize and scale the CMS data platform](https://github.com/blakethom8/cms-data/issues/24)
· **Stabilization milestone:** [CMS Platform Stabilization — 2026-09](https://github.com/blakethom8/cms-data/milestone/1)

## Outcome

Make the CMS data platform a current, observable, capacity-safe, and predictably fast public-data
plane for Provider Search before expanding its product surface substantially.

The platform already has the hard architectural foundations: immutable warehouse releases,
approval-gated promotion, atomic rollback, source manifests, a read-only API, a private network
boundary, and source-preserving evidence models. This roadmap does not replace those controls. It
organizes the remaining work around them.

## How this roadmap is managed

This document owns the program's reasoning, priorities, dependencies, acceptance gates, and current
scoreboard. GitHub issues own execution state. Pull requests own implementation and verification
evidence. Operational cutover records remain under `docs/operations/`.

Rules:

1. One roadmap item may map to one umbrella issue and several independently shippable issues.
2. Issues should point here rather than duplicate the design rationale.
3. Each implementation slice gets a scoped branch and pull request.
4. A checked roadmap gate must cite a merged change or dated operational evidence.
5. Production mutation, promotion, credential changes, DNS changes, and destructive retention work
   still require the approvals in the existing runbooks.
6. Review this scoreboard weekly while P0 or P1 work remains active and monthly afterward.

## Current baseline

The following facts were observed read-only on 2026-08-13:

- The serving host had 16 vCPU, 30 GiB RAM, about 23 GiB available memory, negligible CPU load,
  and no swap.
- Root storage was 245 GiB of 338 GiB used (76%), about 69 GiB more used than the August 5 audit.
- `cms-api.service` was healthy with zero recorded restarts since its August 11 activation.
- The daily publisher-freshness monitor reported 7 current and 11 stale registered sources and
  exited failed as designed.
- The API received 730 requests in the preceding 24 hours; 671 were practice-capability probes, so
  current product traffic does not justify a database-platform migration.
- Warm production probes were approximately 70 ms for provider search, 150 ms for practice search,
  160–180 ms for a 100-row Radar response, and 280–300 ms for a full provider profile.
- Four concurrent full-profile requests each took about 1.14 seconds. The one process-global
  synchronous DuckDB connection serialized the work and blocked the async request path.
- Structured `api.access` duration/correlation records were absent from the journal even though the
  request-context middleware is deployed.

Treat these numbers as a baseline, not a permanent performance claim. Replace them with repeatable
measurements once the performance harness exists.

## Priority model

- **P0 — protect correctness and recoverability:** stale data, unsafe capacity, security exposure,
  or an unproven recovery path.
- **P1 — make current product traffic predictable:** concurrency, latency, caching, observability,
  and environment/contract discipline.
- **P2 — increase product value:** new source families, richer identity and organization evidence,
  market benchmarks, and longitudinal intelligence.

P2 work should not displace unresolved P0 work. P1 performance changes should be evidence-led and
must preserve the immutable read-only serving model.

## Program scoreboard

| ID | Priority | Workstream | State | Exit gate | Existing issue |
| --- | --- | --- | --- | --- | --- |
| H1 | P0 | Restore source freshness | active | All expected sources are current or have an explicitly accepted exception; a promoted release passes canonical smoke | [#16](https://github.com/blakethom8/cms-data/issues/16); [#14](https://github.com/blakethom8/cms-data/issues/14) owns scheduling |
| H2 | P0 | Storage attribution and retention | active | Growth is attributed; thresholds and retention are approved; cleanup is rehearsed; promotion refuses unsafe headroom | [#17](https://github.com/blakethom8/cms-data/issues/17) |
| H3 | P0 | Backup and restore proof | active | Off-host retention is decided and a dated restore rehearsal records RTO, integrity checks, and result | [#18](https://github.com/blakethom8/cms-data/issues/18) |
| H4 | P0 | Restrict arbitrary SQL serving | active | Provider Search and public dashboard credentials cannot reach arbitrary SQL; approved operator access is separately bounded | [#19](https://github.com/blakethom8/cms-data/issues/19) |
| H5 | P0 | Host maintenance and access hardening | active | Controlled reboot passes pre/post smoke; SSH/firewall/log-retention decisions are recorded | [#20](https://github.com/blakethom8/cms-data/issues/20) |
| S1 | P1 | Bounded DuckDB concurrency | active | Repeatable load test proves concurrent requests no longer serialize on one shared connection and resource ceilings hold | [#21](https://github.com/blakethom8/cms-data/issues/21) |
| S2 | P1 | Product serving marts | active | Profile and practice queries use release-built marts where measurements prove benefit; provenance and raw evidence remain intact | [S2 plan](2026-08-s2-serving-marts-plan.md) |
| O1 | P1 | End-to-end request observability | active | Request IDs join Provider Search and CMS logs; route/status/consumer/release latency is queryable | [#22](https://github.com/blakethom8/cms-data/issues/22) |
| C1 | P1 | Release-aware consumer caching | active | Provider Search caches CMS GETs by release and representation, conditionally revalidates, and records release identity with derived evidence | [provider-search#344](https://github.com/blakethom8/provider-search/issues/344) |
| E1 | P1 | CMS development/pre-production lane | active | `ps-dev` and `ps-prod` are isolated; the exact tested candidate is promoted; N/N-1 and rollback tests pass | [#23](https://github.com/blakethom8/cms-data/issues/23) |
| C2 | P1 | Cross-repository contract gate | tracked with E1 | Provider Search request/response models and representative journeys run against every CMS serving candidate | [#23](https://github.com/blakethom8/cms-data/issues/23) |
| D1 | P2 | Complete NPPES identity/location plane | ready after H1 | Type 2, secondary practice, mailing classification, other names, endpoints, and deactivation evidence have explicit grains and provenance | [#15](https://github.com/blakethom8/cms-data/issues/15) covers the location portion |
| D2 | P2 | First-class Doctors & Clinicians sources | ready after H1 | National, facility-affiliation, and selected utilization inputs have registered discovery, manifests, lineage, and source periods | — |
| D3 | P2 | Ownership and organization change | discovery | Hospital ownership/CHOW and selected facility ownership sources produce source-native evidence and conservative relationships | — |
| D4 | P2 | ACO participation | discovery | ACO and participant data are source-managed and joined without implying employment or referral direction | — |
| D5 | P2 | Market benchmarks and service vocabulary | discovery | Approved geographic benchmarks and non-license-sensitive service categories support peer comparisons with scope caveats | — |
| D6 | P2 | Longitudinal provider intelligence | blocked by D1/D2 | Trends retain source period, population, geography, and method; current snapshot endpoints remain backward compatible | — |
| X1 | P1 | Command Center durable publication | active | Authenticated tunnel replaces the temporary publication while the CMS API remains private | [#13](https://github.com/blakethom8/cms-data/issues/13) |

`ready` means the next slice is sufficiently framed to open an execution issue. `discovery` means a
source/contract spike is required before estimating implementation. `blocked by` names a real
sequencing dependency, not merely a preference.

## Phase 1 — stabilize the platform

### H1. Restore source freshness

Goal: return the selected production bundle to an explained, current state.

Acceptance gates:

- [ ] Re-run publisher discovery and record the exact stale-source set.
- [ ] Establish the next NPPES monthly V2 baseline and apply subsequent weekly V2 increments in
  publisher-period order.
- [ ] Refresh the other changed source families through immutable acquisition, candidate build,
  comparison, approval, promotion, restart, and smoke.
- [ ] Record explicit exceptions for any intentionally unrefreshed source; never relabel it current.
- [ ] Make the freshness monitor's last success, last failure, and current stale count visible
  without reading raw journal output.
- [ ] Decide which sources may be scheduled through validation and which retain a manual promotion
  gate. Update issue #14 and the source-specific runbooks accordingly.

### H2. Storage attribution and retention

Goal: keep immutable safety without allowing staging and retained artifacts to exhaust the host.

Acceptance gates:

- [ ] Attribute growth by production deployments, warehouse releases, refresh workspaces, downloads,
  AACT snapshots, reporting exports, Docker data, logs, and other material directories.
- [x] Define minimum free-space and percentage thresholds for warning, critical, and promotion block.
- [ ] Define retention separately for active/rollback releases, verified baselines, downloads,
  failed candidates, refresh workspaces, reports, and logs.
- [x] Add a read-only retention preview that names exact candidate paths, sizes, evidence state, and
  recoverability before any deletion.
- [x] Rehearse the approved cleanup against non-production material, then perform production cleanup
  only with explicit approval and a dated record.

The first approved production cleanup is recorded in
[`operations/storage-retention-cleanup-2026-08-14.md`](../operations/storage-retention-cleanup-2026-08-14.md).
The remaining gates are comprehensive attribution outside the managed platform roots and
category-specific retention for downloads, failed candidates, refresh workspaces, reports, logs,
and off-host recovery copies.

### H3. Backup and restore proof

Goal: replace backup assumptions with demonstrated recovery.

Acceptance gates:

- [ ] Confirm Hetzner backup/snapshot coverage from the control plane.
- [ ] Decide off-host retention count and storage location.
- [ ] Restore a selected bundle and its evidence into an isolated target.
- [ ] Run checksum, read-only-open, release identity, representative query, and application smoke.
- [ ] Record restore duration, required credentials, failure modes, and the owner of the next drill.

### H4. Restrict arbitrary SQL serving

Goal: ensure product and browser consumers can invoke only reviewed, bounded contracts.

Acceptance gates:

- [ ] Inventory every current `/query` caller and classify it as product, dashboard, local operator,
  smoke, or obsolete.
- [ ] Remove `/query` access from Provider Search and browser-facing credentials, or disable the
  route in production after typed replacements exist.
- [ ] If SQL remains an operator feature, place it behind a separate credential and network/role
  boundary with statement, time, memory, row, and response-size limits.
- [ ] Validate table identifiers against an allowlist rather than interpolating arbitrary names.
- [ ] Add negative tests for file access, attach/install/load behavior, expensive queries, excessive
  responses, unauthorized keys, and dashboard proxy bypass.

### H5. Host maintenance and access hardening

Goal: close the known guest/server maintenance items without weakening the private boundary.

Acceptance gates:

- [ ] Apply pending security/kernel updates through a controlled reboot.
- [ ] Verify WireGuard, loopback smoke, CMS API, AACT, reporting, timers, and Provider Search readiness
  before and after the reboot.
- [ ] Review SSH password authentication, root use, host firewall policy, Docker log rotation, and
  system journal retention.
- [ ] Move the current Provider Search value into the named `ps-prod` scoped-key configuration and
  issue an independently rotatable `ps-dev` credential when the development lane is ready.

## Phase 2 — predictable serving

### S1. Bounded DuckDB concurrency

Goal: preserve DuckDB while removing event-loop blocking and one-connection serialization.

Design direction:

- execute synchronous DuckDB work outside the async event loop;
- use independent read-only connections for concurrently executing workers;
- enforce a small query semaphore/connection pool and explicit DuckDB thread and memory ceilings;
- keep cheap identity reads from queueing indefinitely behind large explorer or market queries; and
- compare this design with a small multi-process Uvicorn configuration before choosing production
  parameters.

Acceptance gates:

- [x] Commit a production-representative benchmark for search, profile, practice, Radar, explorer,
  and a mixed workload at concurrency 1/2/4/8/12.
- [x] Record baseline latency, throughput, pool wait, CPU, RSS, and failure behavior.
- [x] Prove no connection is used concurrently in an unsupported way.
- [x] Prove bounded overload returns a controlled failure instead of unbounded queueing.
- [x] Re-run response-shape, production smoke, ETag/304, rollback, and resource tests.
- [x] Record selected worker/pool/thread/memory values and why they fit the host.

Preparation evidence is complete in the
[2026-08-13 prepared-candidate record](../operations/bounded-duckdb-prepared-candidate-2026-08-13.md).
The code-only candidate remains unselected; controlled production cutover is a separate approval.

### S2. Release-built product marts

Goal: avoid repeatedly aggregating large raw tables for stable product representations.

Candidate marts include provider header, locations, affiliations, hospitals, utilization summary,
top services, top drugs, industry summary, practice-search rollups, and market ZIP/specialty
rollups. A mart is justified only by measured query cost or clearer semantics.

Acceptance gates:

- [x] Capture `EXPLAIN ANALYZE` for canonical slow paths before design. See the
  [S2 query-plan baseline](../operations/s2-query-plan-baseline-2026-08-14.md).
- [x] Define each mart's grain, keys, source periods, lineage, null semantics, and validation.
- [x] Preserve raw source evidence and the discovery/profile/evidence endpoint separation.
- [x] Compare the first practice slice against the existing implementation over the canonical
  corpus. See the [S2 candidate record](../operations/s2-managed-dac-candidate-2026-08-14.md).
- [x] Demonstrate a material p95 improvement before switching the first route. The route remains
  unswitched because the capacity and explicit cutover gates are separate.

### O1. End-to-end observability

Goal: answer what happened to one user-triggered query across both services without transmitting
user identity to the data box.

Acceptance gates:

- [ ] Verify the CMS structured access logger emits INFO records in production.
- [ ] Forward the validated Provider Search `X-Request-ID` through every CMS call path.
- [ ] Record normalized route, status, duration, response size, scoped key name, release ID, query
  pool wait, and timeout/overload result.
- [ ] Define initial availability, freshness, and latency SLOs and alerts.
- [ ] Add a dashboard view that separates readiness probes from product traffic.

### C1. Release-aware Provider Search caching

Goal: exploit immutable CMS releases without allowing derived evidence to outlive its source.

Acceptance gates:

- [ ] Consolidate CMS calls behind one production client policy.
- [ ] Cache GET representations by release ID, representation version, endpoint, and canonical
  parameters; support `If-None-Match` revalidation.
- [ ] Pin place-match crosswalks, agent caches, takeaway caches, and evidence receipts to the source
  release that produced them.
- [ ] Define negative caching and stale-if-error behavior per endpoint.
- [ ] Treat AACT as a separately changing source until its identity is safely included in the cache
  contract or every swap rotates the serving deployment.
- [ ] Prove rollback invalidates or reselects cached representations correctly.

### E1/C2. Environment and contract discipline

Goal: make cross-repository integration changes testable before production.

Acceptance gates:

- [ ] Provide fixture, CMS development, pre-production candidate, and production roles without
  treating them as interchangeable.
- [ ] Ensure Provider Search development cannot silently use the production CMS credential.
- [ ] Promote the exact tested artifact rather than rebuilding after acceptance.
- [ ] Run Provider Search's Pydantic response models and representative journeys against every CMS
  serving candidate.
- [ ] Require N/N-1 compatibility or an explicit coordinated cutover for contract changes.
- [ ] Rehearse CMS rollback while a Provider Search N and N-1 consumer remain available.

## Phase 3 — expand product intelligence

### D1. Complete NPPES identity and location evidence

Extend issue #15 into a complete NPPES V2 plan: Type 2 organizations, other names, secondary
practice locations, mailing-address classification, endpoints, and deactivation/reactivation
evidence. Keep `primary_practice`, `secondary_practice`, and `mailing` semantically distinct.

### D2. First-class Doctors & Clinicians management

Manage the National Downloadable File through official Provider Data Catalog discovery, immutable
acquisition, strict schema validation, and row/release provenance. This managed path is now the S2.4
prerequisite because the selected legacy `raw_dac_national` table has no recoverable source period
or run identity. Next, prove its clinician/enrollment/group/address grain in an isolated candidate,
then add Facility Affiliation Data and approved utilization inputs without inferring relationships.

### D3. Ownership and organizational change

Start with Hospital All Owners and Hospital Change of Ownership/Owner Information. Preserve
self-reported ownership, managerial-control, percentage, role, and effective-date claims as
source-native evidence. Extend to other facility types only when tied to a Provider Search use case.

### D4. ACO participation

Evaluate Medicare Shared Savings Program ACO and participant data for organization/network context.
Do not convert participation into employment, referral direction, or a primary-organization claim.

### D5. Market benchmarks and service vocabulary

Evaluate county/HRR geographic variation, provider/service geography tables, and the Restructured
BETOS Classification System. Prefer license-safe clinical categories over exposing CPT descriptions
without an approved AMA license path.

### D6. Longitudinal intelligence

Add current-versus-prior comparisons only after source periods and grains are fully managed.
Provider activity, prescribing, service mix, locations, affiliations, ownership, and ACO changes
must retain population and publisher caveats; absence in one period is not automatically cessation.

## Issue structure

The active first wave uses one thin umbrella issue and independently shippable execution issues.
Issues #13, #14, and #15 are reused rather than duplicated:

1. [#24](https://github.com/blakethom8/cms-data/issues/24) — program umbrella.
2. [#16](https://github.com/blakethom8/cms-data/issues/16) — H1 current freshness recovery;
   [#14](https://github.com/blakethom8/cms-data/issues/14) owns durable NPPES scheduling.
3. [#17](https://github.com/blakethom8/cms-data/issues/17) — H2 storage and retention.
4. [#18](https://github.com/blakethom8/cms-data/issues/18) — H3 off-host restore proof.
5. [#19](https://github.com/blakethom8/cms-data/issues/19) — H4 arbitrary SQL retirement.
6. [#20](https://github.com/blakethom8/cms-data/issues/20) — H5 host hardening.
7. [#21](https://github.com/blakethom8/cms-data/issues/21) — S1 benchmark and serving executor.
8. [#22](https://github.com/blakethom8/cms-data/issues/22) — O1 request correlation and metrics.
9. [provider-search#344](https://github.com/blakethom8/provider-search/issues/344) — C1
   release-aware caching.
10. [#23](https://github.com/blakethom8/cms-data/issues/23) — E1/C2 environment and contract lane.

Do not open all P2 implementation issues yet. Open discovery issues for D2–D5 only when they enter
the next planning horizon; otherwise the issue tracker becomes a second, stale roadmap.

## Suggested operating cadence

### Weekly platform review

- publisher freshness by source and oldest gap;
- disk use, weekly growth, and minimum free space;
- last successful backup and restore rehearsal date;
- API product traffic excluding probes, errors, p50/p95/p99, and slowest routes;
- open P0/P1 issues, blocked decisions, and next production approval;
- Provider Search contract or release dependencies.

### Per implementation slice

1. Reproduce or measure the baseline.
2. Define the contract and acceptance tests.
3. Implement in a candidate or fixture environment.
4. Run focused, full, comparison, and consumer-contract tests proportional to risk.
5. Record rollback and operator steps.
6. Merge code without implying production promotion.
7. Rehearse and stop at the approval gate.
8. Promote only after explicit approval; record dated evidence and update this scoreboard.

### Monthly roadmap review

- close completed issues and cite their evidence here;
- re-rank work from measured product usage and source risk;
- promote only the next actionable P2 discoveries into issues;
- archive this document when a successor becomes authoritative rather than maintaining two plans.

## Decisions required before execution

1. Is restoration of all 11 currently stale sources the immediate first production program, or
   should any source receive a documented temporary exception?
2. What off-host release/backup retention count and restore-time objective should the platform use?
3. Should production `/query` be removed entirely, or retained behind a separate operator-only
   boundary?
4. Is the initial CMS development environment a representative subset or a recent immutable
   production release copy?
5. Which first product expansion matters most after stabilization: complete NPPES identity/location,
   organization ownership, ACO participation, or market benchmarking?
