# CMS API Work Roadmap

_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._

## [1] CMS Data Pipeline

- Status: `active`
- Team: `data`
- Owner mode: `shared`
- Updated: `2026-06-10`
- Plan: `None`
- Summary: Land, validate, normalize, enrich, publish, and select CMS reimbursement datasets for pricing.

### Epics

- [1] CMS RVU Ingestion And Snapshot Selection - `active` (1 parked, 6 done)
  Team: `data`
  Plan: [`docs/workbench/DOC-cms-rvu-ingestion-epic-brief.md`](DOC-cms-rvu-ingestion-epic-brief.md)
  Summary: Move live CMS RVU releases through real local/dev DB writes and ensure valuation-date snapshot lookup chooses the expected RVU and GPCI release.
- [2] CMS Geography Real Data Breadth - `done` (5 done)
  Team: `data`
  Plan: [`docs/workbench/DOC-cms-geography-real-data-breadth-epic-brief.md`](DOC-cms-geography-real-data-breadth-epic-brief.md)
  Summary: Loaded real public CMS ZIP-locality breadth into the runtime geography table, validated GPCI joins, and removed the RVU smoke dependency on a one-row local seed.
- [2] Epic Brief Driven Tracker Workflow - `done` (8 done)
  Team: `ops`
  Plan: [`docs/workbench/DOC-epic-brief-driven-workflow.md`](DOC-epic-brief-driven-workflow.md)
  Summary: Move tracker workflow toward Codex-v0 build briefs and epic briefs as the planning unit, with queued task slices under each epic and a staged harness that starts with approved plans and dry-run orchestration before mutating state.
- [3] CMS Geography Production Ingestion - `active` (1 active, 12 done)
  Team: `data`
  Plan: [`docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`](DOC-cms-geography-production-ingestion-epic-brief.md)
  Summary: Execution readiness is mostly complete: local and Docker RVU/geography production-style smoke passed, the Render execution runbook is drafted, and production mutation is now blocked on explicit operator approval.
  Current tasks: Approve Render RVU Geography Production Execution Runbook
- [4] CMS OPPS Production Readiness - `active` (1 active, 10 queued)
  Team: `data`
  Plan: [`docs/workbench/DOC-cms-opps-production-readiness-epic-brief.md`](DOC-cms-opps-production-readiness-epic-brief.md)
  Summary: Bring public CMS OPPS Addendum A/B and status-indicator data to the RVU/geography readiness bar with source pinning, validation gates, local/Docker smoke, request-time calculation boundaries, and a Render approval gate.
  Current tasks: Audit OPPS Source Contracts And Current Ingestion
