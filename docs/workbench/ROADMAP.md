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

- [1] CMS RVU Ingestion And Snapshot Selection - `active` (1 active, 1 done)
  Team: `data`
  Plan: [`docs/workbench/DOC-cms-rvu-local-db-load-status.md`](DOC-cms-rvu-local-db-load-status.md)
  Summary: Move live CMS RVU releases through real local/dev DB writes and ensure valuation-date snapshot lookup chooses the expected RVU and GPCI release.
  Current tasks: Wire RVU Loaded Data Into Pricing Usage
