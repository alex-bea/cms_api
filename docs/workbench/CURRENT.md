# CMS API Current Work

_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._

- Active task WIP: **1/3**

## Active Tasks

### [1.1.1] Wire RVU Loaded Data Into Pricing Usage

- Status: `active`
- Roadmap: `CMS Data Pipeline`
- Epic: `CMS RVU Ingestion And Snapshot Selection`
- Team: `data`
- Owner mode: `shared`
- Updated: `2026-06-10`
- Plan: [`docs/workbench/DOC-cms-rvu-local-db-load-status.md`](DOC-cms-rvu-local-db-load-status.md)
- Current task: Confirm the API pricing path can use the live-loaded RVU and GPCI snapshots for valuation-date selection.
- Next action: Run local API pricing smoke calls against the loaded rvu_2026_C data, then add a repeatable post-load smoke command if one is missing.
- Resume from: Start from scripts/load_latest_cms_rvu_local.py, DatasetSnapshotService.select_snapshot, and the MPFS pricing service path.
- Linked outputs: [`docs/workbench/DOC-cms-rvu-local-db-load-status.md`](DOC-cms-rvu-local-db-load-status.md)

## Blocked Tasks

- None.
