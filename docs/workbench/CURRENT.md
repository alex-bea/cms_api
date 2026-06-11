# CMS API Current Work

_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._

- Active task WIP: **2/3**

## Active Tasks

### [1.3.13] Approve Render RVU Geography Production Execution Runbook

- Status: `active`
- Roadmap: `CMS Data Pipeline`
- Epic: `CMS Geography Production Ingestion`
- Team: `data`
- Owner mode: `shared`
- Updated: `2026-06-11`
- Plan: [`docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`](DOC-cms-geography-production-ingestion-epic-brief.md)
- Current task: Active approval gate. The runbook exists, but production mutation remains blocked because operator approval has not been granted.
- Next action: Operator must approve or reject the Render execution runbook, target service/database, backup/rollback path, image SHA/digest, source digest, RVU release, latest-active geography behavior, and live smoke checklist.
- Resume from: This task is the gate before production mutation. Do not run Render production load commands until approval is recorded. If approved, the next separate execution task can deploy/load/smoke the live API.
- Linked outputs: [`docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`](DOC-cms-geography-production-ingestion-epic-brief.md), [`docs/workbench/DOC-render-rvu-geography-production-approval-gate.md`](DOC-render-rvu-geography-production-approval-gate.md)

### [1.4.1] Audit OPPS Source Contracts And Current Ingestion

- Status: `active`
- Roadmap: `CMS Data Pipeline`
- Epic: `CMS OPPS Production Readiness`
- Team: `data`
- Owner mode: `shared`
- Updated: `2026-06-11`
- Plan: [`docs/workbench/DOC-cms-opps-production-readiness-build-brief.md`](DOC-cms-opps-production-readiness-build-brief.md)
- Current task: Start by reading the existing OPPS PRDs/source docs and code paths, then write a gap report into the epic brief or a dedicated workbench doc.
- Next action: Identify the smallest safe OPPS path that mirrors RVU/geography: source pinning, local load, validation gates, smoke, Docker, and Render runbook.
- Resume from: The epic defines that stable OPPS source tables should be prepared ahead of time, while quarter selection, packaging decisions, wage/facility context, final amount, and trace refs should happen on request.
- Linked outputs: [`docs/workbench/DOC-cms-opps-production-readiness-epic-brief.md`](DOC-cms-opps-production-readiness-epic-brief.md), [`docs/workbench/DOC-cms-opps-production-readiness-build-brief.md`](DOC-cms-opps-production-readiness-build-brief.md)

## Blocked Tasks

- None.

## Queued For Merge

- None.
