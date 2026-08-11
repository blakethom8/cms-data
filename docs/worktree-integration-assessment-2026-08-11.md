# Worktree integration assessment — 2026-08-11

> **Status: complete** · **Branch: `main`** · **Remote: `origin/main`**

This record explains how the previously uncommitted work was assessed after the New Provider Radar
delivery. It is an integration decision log, not a production-promotion authorization. No warehouse
was built, mutated, promoted, or cut over during this assessment.

## Constraints applied

- The serving API remains read-only; no request handler performs acquisition, refresh, or warehouse
  mutation.
- Warehouse changes execute only against staging candidates. The active production DuckDB is never
  overwritten in place.
- A future production candidate still requires the comparison, approval, and cutover sequence in
  `production-promotion-runbook.md`.
- Changes were separated by subsystem, tested with the full API suite before each commit, and pushed
  to `origin/main`.
- Generated databases, downloaded data, credentials, screenshots, and browser-QA artifacts were
  kept outside Git.

## Assessment decisions

### Private network and service deployment — accepted

The firewall, loopback socket, and systemd service definitions represented deployed infrastructure
that should not remain undocumented local state. Local files were checked against the production
host byte-for-byte, nftables and systemd definitions were validated, and the operating docs were
reconciled with the retired public site and current private WireGuard boundary.

Commit: `76ae580 feat(deploy): codify private CMS network boundary`

### Provider evidence warehouse model — accepted and hardened

The address and organization evidence outputs are important application data-plane work because
they preserve conflicting publisher assertions rather than guessing a primary address or employer.
The initial implementation was retained, with these corrections made during assessment:

- complete, sorted provenance arrays were added for multi-source PECOS relationships;
- the primary source period now determines `data_year` when it contains a parseable year;
- compatibility creation now includes all declared indexes and additive columns;
- full-release nonempty validation and production smoke inventory include both evidence tables;
- Medicare utilization and Open Payments branches gained fixture coverage; and
- the reporting contract and consumer documentation preserve the evidence grain and provenance.

Commit: `8e29a65 feat(pipeline): preserve provider evidence provenance`

### Profile affiliation response — accepted

The profile access lens previously exposed only DAC group rows. The accepted work merges DAC group
doors with Medicare reassignment relationships, labels the asserting source, and returns facility
affiliations while retaining unresolved but valid certification numbers. It is a bounded read-only
query change with focused tests for precedence, ordering, empty results, name resolution, and
deduplication.

Commit: `81fcf37 feat(api): expand provider affiliation evidence`

### Command Center evidence inspection — accepted and hardened

The raw-result ledger is a material usability improvement over numbered record tabs: analysts can
compare duplicate and multi-address rows in their source-native column context before opening the
complete field/value view. The review added single-focus grid navigation with
Up/Down/Home/End, retained explicit empty and unavailable states, and verified responsive layouts.

The proposed local `.env` convenience originally placed a credential file under the static document
root. Before integration, the loader was restricted to `CMS_API_KEY` and `CMS_API_BASE_URL`, shell
values were kept authoritative, and the HTTP server was changed to reject every literal or encoded
hidden-file path. Exact-NPI search fallback remains a bounded read-only evidence probe.

Commit: `3d300bf feat(command-center): improve evidence inspection`

## Verification record

The repository-required command was run from `api/` before each commit:

```text
../.venv/bin/python -m pytest -q
```

Verbatim summaries:

```text
Before 76ae580: 357 passed, 1 skipped, 135 warnings in 11.46s
Before 8e29a65: 362 passed, 1 skipped, 135 warnings in 12.62s
Before 81fcf37: 362 passed, 1 skipped, 135 warnings in 12.62s
Before 3d300bf: 363 passed, 1 skipped, 135 warnings in 11.06s
Before this documentation commit: 363 passed, 1 skipped, 135 warnings in 10.24s
```

The same green 362-test run covers both `8e29a65` and `81fcf37`; the working files included both
subsystems, and no code changed between that run and the second commit.

Additional checks:

- focused evidence tests: `43 passed, 1 skipped, 132 warnings in 2.30s`;
- focused Command Center server tests: `4 passed, 132 warnings in 0.27s`;
- `node --check dashboard/command-center/app.js` passed;
- `git diff --check` passed before each assessed commit;
- literal `/.env` and encoded `/%2Eenv` requests returned HTTP 404;
- browser QA used mocked, non-production API responses with 12 rows and 7 columns;
- desktop and 390 × 844 mobile viewport rendering were visually reviewed;
- End-key selection moved the ledger selection to row 12 and updated the detailed address from
  `11800 Wilshire Boulevard Suite 300` to `900 Medical Plaza`;
- unavailable-source state rendered its missing-table explanation; and
- exact-NPI fallback emitted the expected search, one-row evidence probe, and 25-row evidence load.

The pytest warning count is pre-existing and consists primarily of temporary-directory cleanup
warnings plus three duplicate FastAPI operation-ID warnings. It did not hide test failures, but it
remains cleanup work rather than part of these subsystems.

## Production boundary and next work

These commits publish code and documentation only. The new provider evidence tables will not appear
in the active warehouse until a new staging candidate is built, validated, compared, explicitly
approved, and promoted under the production runbook. Do not infer cutover approval from this
assessment.

At completion, no assessed source file remains uncommitted. A future agent should start by confirming
`git status --short` is empty and `git rev-parse HEAD` matches `git rev-parse origin/main` before
building on this state.

## Copyable agent handoff prompt

```text
Continue from cms-data origin/main after the 2026-08-11 worktree integration assessment.

Read AGENTS.md, docs/data-platform-operating-model.md,
docs/production-promotion-runbook.md, docs/provider-evidence-model.md, and
docs/worktree-integration-assessment-2026-08-11.md before changing anything.

Landed work:
- 76ae580 codifies the deployed private CMS firewall/systemd boundary.
- 8e29a65 adds source-preserving provider address and organization evidence outputs,
  complete provenance arrays, release validation, reporting models, and tests.
- 81fcf37 expands the read-only profile affiliation response across DAC,
  reassignment, and facility affiliations.
- 3d300bf adds the Command Center raw-result ledger, keyboard navigation,
  exact-NPI fallback, and hardened local .env handling.

The serving API must remain read-only. Never overwrite the active production DuckDB.
Any warehouse refresh must build a staging candidate. Do not run a production promotion
or approval-gated cutover without Blake's explicit approval.

First verify the worktree is clean and HEAD equals origin/main, then run the API suite from
api/ with ../.venv/bin/python -m pytest -q. If the next task is to publish the new evidence
tables, stop at the runbook approval gate after candidate build, validation, and comparison,
and present the evidence for explicit approval.
```
