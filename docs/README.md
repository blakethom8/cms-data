# Documentation Guide

> **Last reviewed: 2026-07-22** · **Status: canonical index**

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
| [../deploy/systemd/README.md](../deploy/systemd/README.md) | Systemd release layout and the read-only publisher-status timer. |

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
