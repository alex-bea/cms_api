# CMS API Current Work

_Generated from `state/work/` by `tools/work_tracker.py`. Do not edit by hand._

- Active task WIP: **1/3**

## Active Tasks

### [1.3.3] Publish CMS ZIP Locality To Runtime Geography

- Status: `active`
- Roadmap: `CMS Data Pipeline`
- Epic: `CMS Geography Production Ingestion`
- Team: `data`
- Owner mode: `shared`
- Updated: `2026-06-11`
- Plan: [`docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`](DOC-cms-geography-production-ingestion-epic-brief.md)
- Current task: Replace database replay normalization with source parsing and add runtime geography publication semantics without changing unrelated ZIP9/nearest-ZIP behavior.
- Next action: Update CMSZipLocalityProductionIngester to parse landed CMS ZIP-locality source through cms_pricing.ingestion.parsers.cms_geography, publish ZIP5/ZIP9 rows to runtime geography with non-destructive default/scoped replace semantics, and register ZIP_LOCALITY snapshots.
- Resume from: Target runtime table is cms_pricing.models.geography.Geography; do not use cms_zip_locality as the pricing resolver source of truth. Shared parser is tested and should be reused rather than duplicating fixed-width parsing.
- Linked outputs: [`docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`](DOC-cms-geography-production-ingestion-epic-brief.md)

## Blocked Tasks

- None.

## Queued For Merge

- None.
