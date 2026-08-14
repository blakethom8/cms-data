# S2 managed-DAC practice candidate — 2026-08-14

> **Decision:** production cutover completed successfully for the first `cms_enrollment`
> practice-search serving mart. The selected deployment is
> `deployment-20260814T160153Z-45ab9d2d38`; its predecessor remains rollback-ready.

## Outcome

The corrected immutable warehouse candidate is
`warehouse-20260814T025428Z-5dac630227`:

- warehouse SHA-256:
  `bf7d2381c8c9a683497a7dcc5d64c87ccfb3359fe5ee77d61e74ca3bffb1fa02`;
- size: 20,951,347,200 bytes;
- release-build code: `bc7b114187071691f55bb18bf182c3a3477a78bd`;
- authorized serving code: `7fb735cdf0dac96dd26201277564be2740810170`;
- `raw_dac_national`: 3,388,151 rows;
- `serving_practice_provider_sites`: 3,212,860 rows; and
- state: validation passed, selected, promoted, and verified.

The selected production deployment remained
`deployment-20260811T155814Z-6baa26aa69` throughout the rehearsal. Its API PID stayed `3240475`,
`NRestarts` stayed zero, and `/health` continued to report 7,395,713 core providers. Both isolated
services were bound to loopback, run at reduced CPU/I/O priority, and stopped after evidence
capture. Ports 18081 and 18082 were no longer listening at rehearsal handoff. The later approved
cutover replaced that process once; production now runs the selected candidate as PID `3990931`.

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

The first candidate, `warehouse-20260814T023853Z-9b87f3e486`, passed the warehouse comparison but
failed API compatibility because the current CMS file publishes `Cred` and `Telehlth` while the
legacy physical table used whitespace-suffixed names. The managed loader now preserves established
physical names only when publisher columns differ by unambiguous leading/trailing whitespace and
fails closed on ambiguous normalization. The corrected candidate was rebuilt rather than amended.
After the corrected candidate passed and the failed release was proven unpromoted, unreferenced,
closed by every process, and outside the rollback floor, explicit approval authorized deletion of
only the failed release directory.

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

## Capacity gate and route authorization

The initial read-only retention preview exited 1, as designed. It measured 305,705,242,624 used bytes
(84.3%) and 42,137,550,848 free bytes. Supplying the exact candidate size projected 90.08% use,
above the configured 85% promotion block. The active-plus-two validated rollback floor passed.

The planner names 54 review candidates totaling 95,406,317,568 allocated bytes but confirms zero
automatically reclaimable bytes. Large items include a 42.5 GB legacy refresh workspace, three
legacy warehouse artifacts totaling about 45.7 GB, and the 20.95 GB failed first S2 candidate. Each
path needed explicit manifest/job/reference review and separately authorized cleanup; the final
candidate and rollback floor remained protected.

After explicit approval, the failed unpromoted candidate
`warehouse-20260814T023853Z-9b87f3e486` was reverified by release ID, SHA-256, validation and
promotion state, open-file scan, symlink scan, systemd inventory, and production-ledger search. Only
that directory was deleted, recovering about 20.95 GB. Production stayed on PID 3240475 with zero
restarts and a healthy 7,395,713-provider response.

The post-cleanup preview exited zero: used capacity is 284,682,866,688 bytes (78.5%), free capacity
is 63,159,926,784 bytes, and adding the exact candidate size projects 84.28%, below the 85% block.
The active-plus-two rollback floor still passes.

The authorization change declares `/practices/search` as the mart consumer and permits
`serving_practice_managed_dac_v1` only with the exact raw-DAC/mart changed-table set, matching row
counts, and passed mart validation. The API's deployment-local `auto` selector uses the mart when
the selected immutable warehouse contains it and falls back to raw for predecessor warehouses.
This avoids a global configuration change that could break rollback. Preparation, selection, and
cutover remain separate steps.

## Immutable production bundle rehearsal

Authorization merged through PR #51 as serving commit
`7fb735cdf0dac96dd26201277564be2740810170`. The API dependency lock is unchanged from the selected
serving commit, so preparation reused runtime
`runtime-candidate-8985e8a-c26024b3` with fingerprint
`sha256:82370f7e4b25f1a907a92eda5c1097302a6f88936ad59319206b4ade3cc7c347`.
The clean detached code artifact is root-owned and sealed at
`production-artifacts/code/7fb735cdf0dac96dd26201277564be2740810170-s2-managed-dac-1`; its
fingerprint is `sha256:61173ba25fdde0f510f1ebf6d7cf2ac2a5dd604f59972f521951229ce67f2398`.

The staging warehouse was copied to a distinct, non-reflinked production inode at
`production-artifacts/warehouses/warehouse-20260814T025428Z-5dac630227/warehouse.duckdb`. The copy
is `root:dataops` mode `0440`, has link count one, and matches the approved size and SHA-256 above.
Prepare dry-run passed, then real preparation created unselected deployment
`deployment-20260814T160153Z-45ab9d2d38`, targeting verified predecessor
`deployment-20260811T155814Z-6baa26aa69`.

The deployment-scoped source snapshot contains the predecessor's complete 20-record provenance plus
the new active DAC run, rather than incorrectly reducing the snapshot to the targeted builder's
dependency closure. Its 21-record SHA-256 is
`ea88ca04e7be81288d44dd31e4034d2fe377dfe53674f423d197d8d4f0af19fb`. Offline fixture discovery
found all 19 source families and no unknown/unavailable sources; fixtures intentionally reported all
19 stale. Live discovery completed without error and reported 8 current, 11 stale, and zero unknown
or unavailable sources. Those 11 existing freshness findings are advisory and are not introduced by
the DAC candidate.

The isolated candidate ran as `dataops` on `127.0.0.1:18080` with its bundle warehouse path bound at
process execution. The final canonical smoke passed all 15 checks and is sealed with SHA-256
`5e38d8ccc5863b1b2021401e2c24f56cd7631fbcc941538ac0e1c26340f19b13`. A separate read-only check
proved configuration `auto` resolves to `mart`, all 17 required mart columns are present, and the raw
and mart row counts match release evidence; its SHA-256 is
`deab8de2dba8dc7f68c9d51c2e9fd7ce4ff1655a38574cc529f2502801610f32`. The serving-contract check
proved the candidate release ID, warehouse SHA/pipeline identity, 19 source vintages,
representation version 3, candidate ETag, and a `304` conditional response; its SHA-256 is
`e0596923a0df6c66b3ee3f3bc17d484247eaa618b96c65e7f2861b1cf526a09a`.

Three fail-closed smoke attempts were retained rather than hidden. The first showed that this
targeted release's manifest lacked inherited `smoke_table_counts`. The second correctly received
`403` after raw/mart tables were incorrectly added to the arbitrary-SQL count set. The third showed
that the legacy shared key cannot call `/query`; the passing run used the existing allowlisted
`command-center` scoped key without exposing its value. Follow-up hardening makes targeted builders
inherit query-authorized baseline smoke counts, requires a named smoke consumer in the runbook, and
treats `--candidate-bytes` as additional unallocated storage so a prepared artifact is not counted
twice.

Activation and rollback dry-runs both passed, with evidence SHA-256 values
`379a6b2510b1f1d27362e2253487fad86357bae208de876adfbd19558809e8ca` and
`5cfa961253725a724b0cb7011892311bf2648a043df6db2cb44e14c1cc7c0812`. The rehearsal process then
stopped, port 18080 was released, and the sealed code remained clean with zero writable or bytecode
paths. Production still selects the verified predecessor on PID `3240475` with zero restarts; the
control plane is healthy, the transition sentinel is absent, and `production-ops/current` was not
changed.

Promotion hardening merged through PR #52 as operations commit
`d8567a340ffc1f665c39c01671273784ff224174`. It is installed as sealed package
`production-ops/ops-d8567a340ffc1f665c39c01671273784ff224174-s2-hardening`; the approved cutover
atomically selected it through `production-ops/current`. The
corrected post-copy preview passed with SHA-256
`5ac73e742fd6184da37b012179b34f53da8ac44aba498d5391bbeac3ba1efbda`: actual and projected use are
84.29%, 42,195,087,360 bytes remain free, zero additional candidate bytes are required, and the
active-plus-two rollback floor passes. Disk state is `critical` under the 80% warning threshold but
remains below the separate 85% promotion block; there is insufficient headroom for another large
warehouse candidate without a fresh retention review.

## Approved production cutover

The explicitly approved cutover completed on 2026-08-14. Immediately before selection, the
candidate and predecessor artifact hashes, live PID and selector, absent transition sentinel,
zero blocking transactions, and capacity/rollback floor were reverified. The archived systemd
unit, environment-file metadata, and control-plane state preserve the pre-change boundary without
capturing secret values.

The first one-shot invocation used `http://10.77.0.1:8080` and failed closed before selection
because production smoke requires an exact HTTP loopback origin. It exited `2`; the selector,
PID `3240475`, and production traffic remained unchanged. Repeating the same approved procedure
with `http://127.0.0.1:8080` and the named `command-center` smoke credential completed successfully:

- selected deployment: `deployment-20260814T160153Z-45ab9d2d38`;
- predecessor: `deployment-20260811T155814Z-6baa26aa69`, now superseded and rollback-ready;
- selection and promotion time: `2026-08-14T16:47:27+00:00`;
- verification time: `2026-08-14T16:48:41+00:00`;
- production API PID: `3990931`, active with zero restarts;
- final production smoke: all 15 checks passed, SHA-256
  `c298ea18daa23806a9f172dbb618153ec725768e2f4ec5b417d974562b4a67a1`; and
- one-shot result SHA-256:
  `5b11071f62e84e98517155130b941db666d83818ac9e2342a1ee1d40063e8ab8`.

The process resolves the immutable candidate code, runtime, and warehouse artifacts; the open
database is the exact 20,951,347,200-byte candidate with the approved SHA-256. `GET /release`
reports the candidate deployment, warehouse release, pipeline commit, 19 source vintages, and
representation version 3. Its ETag is
`"deployment-20260814T160153Z-45ab9d2d38:3"`, conditional retrieval returns `304`, and a live
CMS-enrollment practice search returned `200` with the mart selected through deployment-local
`auto`. Provider Search readiness remained `ready` with its CMS data check `ok`.

The final retention preview reports 84.29% use with approximately 42.2 GB free, zero additional
candidate bytes, and the active-plus-two floor intact. This is below the 85% promotion block but
above the 80% warning threshold, so another large warehouse candidate requires a fresh retention
review.

One non-blocking metadata issue was captured. The control-plane ledger is verified at
`2026-08-14T16:48:41+00:00`, but the live `/release` response reports `verified_at: null`. Smoke
populated the process-local release metadata cache before the manager recorded verification, and
the successful value remains cached for that process lifetime. Served release identity, data,
hashes, ETag, and route behavior are correct. Production was not restarted a second time merely to
refresh this timestamp; a follow-up should make successful metadata resolution refresh when the
ledger transitions from unverified to verified.

## Evidence

Machine-readable acquisition, release, comparison, raw/mart diagnostics, six final benchmark
trials, preparation rehearsal, fail-closed smoke attempts, serving contract, transition dry-runs,
source status, cutover result, final production smoke, process and journal checks, Provider Search
readiness, and pre/post-cutover retention evidence are retained under
[`evidence/s2-managed-dac-2026-08-14`](evidence/s2-managed-dac-2026-08-14/).
