# CMS API Current Work

_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._

- Active task WIP: **1/3**

## Active Tasks

### [1.1.3] Load Or Map MPFS Conversion Factor

- Status: `active`
- Roadmap: `CMS Data Pipeline`
- Epic: `CMS RVU Ingestion And Snapshot Selection`
- Team: `data`
- Owner mode: `shared`
- Updated: `2026-06-11`
- Plan: [`docs/workbench/DOC-cms-rvu-ingestion-epic-brief.md`](DOC-cms-rvu-ingestion-epic-brief.md)
- Current task: Decide whether RVUItem.conversion_factor is the intended live MPFS CF source for loaded RVU pricing, or whether a companion conversion-factor snapshot/load path is required.
- Next action: Add explicit tests/documentation for CF source selection, effective date, release ID, and trace refs; do not leave CF provenance implicit in RVU row pricing.
- Resume from: Start from cms_pricing/engines/mpfs.py trace refs, scripts/load_latest_cms_rvu_local.py, and cms_pricing/ingestion/parsers/conversion_factor_parser.py.
- Linked outputs: [`docs/workbench/DOC-cms-rvu-ingestion-epic-brief.md`](DOC-cms-rvu-ingestion-epic-brief.md), [`docs/workbench/DOC-cms-rvu-local-db-load-status.md`](DOC-cms-rvu-local-db-load-status.md)

## Blocked Tasks

- None.

## Queued For Merge

- None.
