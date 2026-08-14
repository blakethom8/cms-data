# S2 managed-DAC practice candidate — 2026-08-14

> **Decision:** response parity and the S2 performance threshold pass for the first
> `cms_enrollment` practice-search serving mart. Capacity does not pass. The candidate remains
> unselected, the route default remains `raw`, and no production cutover is authorized.

## Outcome

The corrected immutable warehouse candidate is
`warehouse-20260814T025428Z-5dac630227`:

- warehouse SHA-256:
  `bf7d2381c8c9a683497a7dcc5d64c87ccfb3359fe5ee77d61e74ca3bffb1fa02`;
- size: 20,951,347,200 bytes;
- release-build code: `bc7b114187071691f55bb18bf182c3a3477a78bd`;
- route-test code: `91174f823a7a936e16408a1fdc177442fb30add1`;
- `raw_dac_national`: 3,388,151 rows;
- `serving_practice_provider_sites`: 3,212,860 rows; and
- state: validation passed, promotion state `not_promoted`, serving authorization false.

The selected production deployment remained
`deployment-20260811T155814Z-6baa26aa69` throughout the rehearsal. Its API PID stayed `3240475`,
`NRestarts` stayed zero, and `/health` continued to report 7,395,713 core providers. Both isolated
services were bound to loopback, run at reduced CPU/I/O priority, and stopped after evidence
capture. Ports 18081 and 18082 were no longer listening at handoff.

## Managed source identity

The official CMS Provider Data Catalog resource `mj5m-pzi6` was acquired as immutable run
`20260814T023610Z-5acfb10c`. CMS released it on 2026-08-13 for source period 2026-07-31. The source
CSV is 839,308,285 bytes with SHA-256
`56996a2de21da047d416a4d44c2cbaa116f601e4f69ff314639018316b6c49c0`, 3,388,151 data rows,
UTF-8-with-BOM encoding, and schema fingerprint
`sha256:56b554f9023a465fddba31d357014c9f1b4ffa93f28fa0c25102965c8f1cc45a`. Acquisition validation
passed and the source release remains unpromoted.

The mart also retains the selected warehouse's exact 2024 full-year Part B run
`20260721T220808Z-eed38803` and Part D run `20260721T220935Z-988185f9`. Row and release provenance
validation passed with no missing DAC, Part B, or Part D provenance.

## Candidate comparison and validation

The `serving_practice_managed_dac_v1` comparison passed. It checked logical fingerprints for all
40 non-allowlisted tables and found no unexpected differences or evidence mismatches. The only
authorized changes were:

| Table | Baseline rows | Candidate rows |
|---|---:|---:|
| `raw_dac_national` | 2,686,173 | 3,388,151 |
| `serving_practice_provider_sites` | absent | 3,212,860 |

The serving-mart contract reported zero duplicate keys, required-column nulls, invalid NPIs,
invalid state/ZIP values, empty specialty lists, invalid organization identity rows, and unexplained
source-value provenance gaps. The candidate is 381,681,664 bytes (1.86%) larger than the selected
warehouse.

The first candidate, `warehouse-20260814T023853Z-9b87f3e486`, remains preserved and unselected. It
passed the warehouse comparison but failed API compatibility because the current CMS file publishes
`Cred` and `Telehlth` while the legacy physical table used whitespace-suffixed names. The managed
loader now preserves established physical names only when publisher columns differ by unambiguous
leading/trailing whitespace and fails closed on ambiguous normalization. The corrected candidate
was rebuilt rather than amended.

## Response parity

The committed fourteen-case corpus SHA-256 was
`d61bc78e3ec116d4831345df2c9e5deaabad432dc98eccdef14e5b69dce6c762`. Raw and mart instances used
the same candidate, 2 GB DuckDB limit, four DuckDB threads, pool size four, 60-second request
timeout, and three sequential trials.

All four practice cases matched byte-for-byte between raw and mart on every trial: expected HTTP
status, response bytes, response SHA-256, ordering, totals, truncation, and empty-result behavior.
The unchanged NPPES-primary practice case also matched exactly. Ten other cross-route cases had the
same response digest sets. `profile-rich` and `industry-search` each varied ordering across their
three trials on both backends, so the full diagnostic command correctly exited nonzero; this is a
shared cross-route determinism follow-up, not a raw-versus-mart difference. No case returned a 5xx
or unexpected status.

## Focused performance evidence

The committed `s2-practice-search-v1.json` workload contains state, bounded multi-ZIP/multi-
specialty, and empty CMS-enrollment searches with weights 3:2:1 and SHA-256
`0f2f9cad574dfa3e4cff2410e5976ad1c6b2cc9e702f104b2be7867b3d362969`. Each backend ran three warm
trials of 60 requests at concurrency 1 and 12. The table reports the median of the three trial
summaries.

| Backend | Concurrency | Throughput (req/s) | p50 (ms) | p95 (ms) | Failures |
|---|---:|---:|---:|---:|---:|
| raw | 1 | 3.28 | 303.49 | 348.07 | 0 |
| mart | 1 | 8.22 | 202.87 | 210.80 | 0 |
| raw | 12 | 6.95 | 1,692.02 | 1,886.56 | 0 |
| mart | 12 | 17.15 | 607.07 | 981.71 | 0 |

The mart lowers p95 by 39.4% at concurrency 1 and 48.0% at concurrency 12 while increasing
throughput by 151% and 147%, respectively. There were zero failures across all 720 raw and final-
mart measured requests. The multi-ZIP route p95 fell from 351.23 to 56.86 ms at concurrency 1.

An intermediate query initially regressed concurrency-1 p95 because DuckDB applied specialty-list
matching before a selective ZIP boundary. Making the location slice an explicit materialized CTE
keeps the same response semantics while allowing only the selected geography to reach list
matching. This was validated by 89 focused tests and the full 468-passed, 1-skipped API suite.

## Capacity gate and next action

The read-only retention preview exited 1, as designed. It measured 305,705,242,624 used bytes
(84.3%) and 42,137,550,848 free bytes. Supplying the exact candidate size projected 90.08% use,
above the configured 85% promotion block. The active-plus-two validated rollback floor passed.

The planner names 54 review candidates totaling 95,406,317,568 allocated bytes but confirms zero
automatically reclaimable bytes. Large items include a 42.5 GB legacy refresh workspace, three
legacy warehouse artifacts totaling about 45.7 GB, and the 20.95 GB failed first S2 candidate. Each
path needs explicit manifest/job/reference review and separately authorized cleanup; the final
candidate and rollback floor must remain protected.

After reviewed capacity cleanup, rerun the preview. A later PR may then authorize the exact route in
the mart contract and production comparison policy and prepare an immutable deployment containing
warehouse `warehouse-20260814T025428Z-5dac630227` plus route code at or after
`91174f823a7a936e16408a1fdc177442fb30add1`. That work still does not authorize selection or
cutover; those remain a separate explicit approval gate.

## Evidence

Machine-readable acquisition, release, comparison, raw/mart diagnostics, six final benchmark
trials, and the retention preview are retained under
[`evidence/s2-managed-dac-2026-08-14`](evidence/s2-managed-dac-2026-08-14/).
