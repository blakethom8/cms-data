# Documentation Guide

> **Last reviewed: 2026-08-05** · **Status: canonical index**

This directory is organized to keep operational guidance distinct from historical planning material.
When documents disagree, the operating model and the source code win.

## Current documentation

| Document | Use it for |
| --- | --- |
| [data-platform-operating-model.md](data-platform-operating-model.md) | Repository ownership, source policy, manifests, validation gates, production constraints, and data-use rules. |
| [platform-overview.md](platform-overview.md) | A readable product and architecture overview: data marts, source mapping, cadence, and release flow. |
| [production-promotion-runbook.md](production-promotion-runbook.md) | The approval-gated staging rehearsal and atomic cutover procedure. |
| [aact-clinical-trials.md](aact-clinical-trials.md) | Hosted AACT adapter runtime, verification, and refresh safety. |
| [new-provider-radar.md](new-provider-radar.md) | NPPES weekly change detection, monthly reconciliation, and Provider Search handoff. |
| [new-provider-radar-execution.md](new-provider-radar-execution.md) | Implementation handoff for the remaining Radar data-plane work: acquisition automation, production promotion, city-scope API, precision spike. |
| [provider-serving-contract.md](provider-serving-contract.md) | NPPES-first discovery, curated profile, raw evidence endpoint separation, provenance labels, and v3 cache contract. |
| [provider-evidence-model.md](provider-evidence-model.md) | Source-preserving provider address and organization evidence grains, provenance, refresh, and consumer rules. |
| [worktree-integration-assessment-2026-08-11.md](worktree-integration-assessment-2026-08-11.md) | Decision log, verification record, commit map, and agent handoff for the assessed deployment, evidence, API, and Command Center work. |
| [../deploy/systemd/README.md](../deploy/systemd/README.md) | Systemd release layout and the read-only publisher-status timer. |
| [operations/hetzner-cms-server.md](operations/hetzner-cms-server.md) | Current production-host inventory, Provider Search boundary, decommission record, and infrastructure work. |
| [operations/cms-private-network-runbook.md](operations/cms-private-network-runbook.md) | Current private WireGuard API boundary, verification, rollback, and retention procedure. |
| [operations/cms-private-network-cutover-2026-08-05.md](operations/cms-private-network-cutover-2026-08-05.md) | Executed private-network and credential cutover record. |
| [operations/public-sites-retirement-2026-08-05.md](operations/public-sites-retirement-2026-08-05.md) | Executed public-site retirement record and remaining owner actions. |
| [operations/command-center-temporary-publication-2026-08-11.md](operations/command-center-temporary-publication-2026-08-11.md) | Temporary direct HTTPS Command Center boundary, validation, DNS handoff, and rollback. |
| [operations/nginx-disentanglement-handoff.md](operations/nginx-disentanglement-handoff.md) | Historical implementation prompt, retained as superseded decision context. |

## Documentation rules

- Use `Last reviewed: YYYY-MM-DD` below the title for a document that is intended to guide current
  implementation or operations.
- Update the review date only after checking the described commands, paths, and architecture against
  the repository or a recorded production audit.
- Archive superseded plans instead of silently deleting them. Archived material is context, never an
  implementation instruction.
- Keep operational commands free of credentials and do not record live production secrets, raw data,
  DuckDB files, or mutable release evidence in Git.

## Archive

[archive/README.md](archive/README.md) explains the historical material retained in this repository.
