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
- [2] Epic Brief Driven Tracker Workflow - `done` (8 done)
  Team: `ops`
  Plan: [`docs/workbench/DOC-epic-brief-driven-workflow.md`](DOC-epic-brief-driven-workflow.md)
  Summary: Move tracker workflow toward Codex-v0 build briefs and epic briefs as the planning unit, with queued task slices under each epic and a staged harness that starts with approved plans and dry-run orchestration before mutating state.
