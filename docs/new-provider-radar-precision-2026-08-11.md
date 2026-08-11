# New Provider Radar precision spike — 2026-08-11

## Recommendation

Do not enable an all-taxonomy digest. Require an explicit market taxonomy selection before
notifications, retain the V1 `newly_enumerated` and `practice_location_changed` defaults, and run
one more sampled market before enabling email. The interactive feed is safe to expose: its source
and boundary behavior were precise, but an untargeted digest would deliver mostly irrelevant
specialties.

## Sample and method

The sample is the real hosted-development saved market **Radar QA — Denver**, containing ZIPs
`80206` and `80220`. The market had no row in `workspace_market_radar_rules`, so it had no configured
target taxonomies or event overrides. Market membership was read from provider-search's hosted
development Postgres; no customer data was copied and no writes were made.

The replay queried the immutable production deployment
`deployment-20260811T023712Z-b68e0ca9c3`, warehouse release
`warehouse-20260811T021837Z-f44c147e30`, across its three weekly NPPES periods. The monthly release
is the event-free baseline. V1 candidates are `newly_enumerated` plus
`practice_location_changed`, matching the product default. Taxonomy and location fields use the
provider state current through 2026-08-02, which is also how the read API represents older events.

## Candidate volume

| Source week | New NPI | Entered market | V1 total | Taxonomy-change events excluded from V1 |
| --- | ---: | ---: | ---: | ---: |
| 2026-07-13–2026-07-19 | 5 | 5 | 10 | 3 |
| 2026-07-20–2026-07-26 | 9 | 1 | 10 | 1 |
| 2026-07-27–2026-08-02 | 5 | 2 | 7 | 1 |
| **Total** | **19** | **8** | **27** | **5** |

The V1 feed averaged 9 candidates per week in this small two-ZIP boundary. All 27 candidates were
distinct providers within each week.

## Taxonomy fit

The saved market has no configured target specialty, so a literal target-match rate is undefined.
For the expected Denver-cardiology use case, the cardiology family was evaluated as `207RC0000X`,
`207RC0001X`, `207RI0011X`, and `2080P0202X`: **0 of 27 candidates matched**, either as primary or
as any recorded taxonomy.

The largest primary-taxonomy groups were:

| Primary taxonomy | Label | Candidates | Share of V1 |
| --- | --- | ---: | ---: |
| `106S00000X` | Behavior Technician | 6 | 22.2% |
| `171M00000X` | Case Manager/Care Coordinator | 4 | 14.8% |
| `101YM0800X` | Mental Health Counselor | 3 | 11.1% |
| `183500000X` | Pharmacist | 3 | 11.1% |
| Other (11 codes) | Mixed | 11 | 40.7% |

This is not a source false-positive problem: these are legitimate provider events inside the saved
ZIPs. It is a targeting problem. Defaulting an unset market to all taxonomies would make the digest
irrelevant for a specialty-focused rep.

## Safety and data quality

- **Duplicate rate:** 0 duplicate event IDs and 0 duplicate logical events across 69,374 events,
  one monthly baseline, and three weekly releases (0%).
- **Boundary precision:** all 8 V1 location-change candidates moved from a ZIP outside the saved
  market into `80206` or `80220`. There were 0 within-market moves, 0 missing prior ZIPs, and 0
  same-ZIP address edits.
- **New-NPI classification:** all 19 new candidates had effective dates equal to their enumeration
  dates. None carried a deactivation or reactivation date, so the sample exposed no re-enumeration
  artifacts.
- **Market location quality:** all 27 V1 candidates had usable address, city, two-letter state,
  ZIP, and phone fields. Across all 69,374 promoted events, 8 had an unusable resulting ZIP
  (0.012%) and 24 had an unusable current state (0.035%). Such rows cannot enter a valid ZIP-scoped
  feed.

## Manual noise review

Two addresses accounted for 11 of 19 new NPIs (57.9%). Six Behavior Technicians appeared at one
address in pairs across all three weeks; another address produced four Case Managers and one
Addiction Counselor. These look like legitimate batch staff enumeration at shared organizations,
not duplicate events, but they would read as a repetitive burst in email. The eight market-entry
events all crossed the boundary and showed no address-only or same-market false positives.

The practical false-positive result is therefore:

- **Source/boundary false positives observed:** 0 in the checked classifications.
- **Expected cardiology-target false positives without a taxonomy rule:** 27 of 27 (100%).
- **Digest repetition risk:** material, because 57.9% of new NPIs clustered at two addresses.

## Product defaults

1. Keep the existing V1 event defaults. Rank `newly_enumerated` before
   `practice_location_changed`; keep taxonomy-change and reactivation out of V1.
2. Require at least one explicit taxonomy code before `notifications_enabled` can become true.
   An unset rule may still power the interactive feed, but must not silently mean “email every
   taxonomy.”
3. In a digest, group or cap same-address bursts while preserving every event in the persistent
   feed. Do not deduplicate them as providers; the source events are distinct.
4. Keep the digest disabled until a second real saved market with configured target taxonomies is
   sampled. This report establishes the denominator but does not justify a universal volume target
   from one two-ZIP QA market and three weeks.

## Limitations

This is one market, three weekly releases, and 27 V1 candidates. The market is real saved state but
is named for QA and has no targeting rule. The cardiology comparison is an explicitly labeled
product-use-case check, not stored market configuration. The review did not perform live Registry
API verification of individual NPIs; it evaluated the promoted NPPES evidence and current state.
