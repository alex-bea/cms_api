# Render RVU/Geography Production Approval Gate

**Status:** Pending operator approval
**Updated:** 2026-06-11
**Scope:** Approval or blockers for Render production data mutation
**Production mutation:** Blocked

## Decision Required

The Render production execution runbook is drafted, but no production load is
approved yet.

Runbook:
`docs/workbench/DOC-render-rvu-geography-production-execution-runbook.md`

## Approval Checklist

An operator must explicitly approve:

- Render service `cms-pricing-api`;
- Render database `cms-pricing-db`;
- approved image SHA or digest;
- database backup or restore point;
- latest-active/open-ended geography behavior for production valuation date
  `2026-07-01`;
- ZIP-locality source digest
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`;
- RVU release `rvu_2026_C`;
- scoped replace behavior for geography and RVU rows;
- live API smoke expectations for `94110` and `66012`.

## Current Decision

Approval: not granted.

Blocker: operator has not yet reviewed and approved the Render execution
runbook, backup/rollback path, exact Render target, image SHA/digest, and
latest-active/open-ended geography behavior.

## Next Step

Record an explicit approval or rejection in this document. Only after approval
should a separate production execution task be created for the Render load and
live API smoke.
