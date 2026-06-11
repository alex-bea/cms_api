# CMS RVU Ingestion And Snapshot Selection

**Status:** Active
**Updated:** 2026-06-11
**Tracker link:** `state/work/epics/cms-rvu-ingestion.yaml`

## Related Governance

- `prds/PRD-rvu-gpci-prd-v0.1.md`
- `prds/PRD-mpfs-prd-v1.0.md`
- `prds/SRC-cms-rvu.md`
- `prds/SRC-gpci.md`
- `prds/SRC-conversion-factor.md`
- `prds/SRC-locality.md`
- `prds/STD-data-architecture-prd-v1.0.md`
- `prds/STD-data-architecture-impl-v1.0.md`
- `prds/STD-codex-v0-build-workflow-prd-v1.0.md`
- `prds/STD-codex-v0-build-workflow-impl-v1.0.md`
- `docs/workbench/DOC-cms-rvu-local-db-load-status.md`

## Goal

Move live CMS RVU releases through the local/dev database path and make MPFS
pricing choose the correct RVU, GPCI, conversion-factor, and locality inputs for
a valuation date.

## Current State

The latest live CMS RVU release has been loaded through the local database path.
Snapshot selection now chooses `rvu_2026_C` and `gpci_2026_C` for valuation date
`2026-07-01`, and a local pricing smoke returned a positive MPFS allowed amount
with trace refs for RVU, GPCI, and conversion-factor source.

Two correctness risks remain active before the RVU-backed pricing path should be
treated as validated:

- the MPFS conversion-factor source must be explicit, tested, and documented;
- geography/locality resolution must map cleanly into loaded RVU/GPCI locality
  keys without accidental loss of leading-zero semantics or undocumented `00`
  to `01` normalization.

## Scope

In scope:

- Formalize whether `rvu_items.conversion_factor` is the canonical MPFS
  conversion-factor source for loaded RVU pricing, or identify the need for a
  companion conversion-factor snapshot/load path.
- Preserve release ID, effective date, and trace refs for RVU, GPCI, and
  conversion-factor selection.
- Compare geography resolution for ZIP `94110` against loaded RVU locality and
  GPCI keys.
- Add targeted tests for locality leading-zero preservation and explicit `00`
  versus `01` behavior.
- Keep the implementation constrained to existing ingestion, geography,
  pricing, and provenance patterns.

Out of scope:

- Rebuilding the full RVU ingestion pipeline.
- Adding a new production dependency.
- Broad API response schema changes beyond fields already needed for provenance.
- Replacing Codex v0 workflow orchestration with Hermes, IronClaw, or another
  harness.
- Bulk production data reloads or destructive local database resets.

## Acceptance Criteria

- `load-or-map-mpfs-conversion-factor` records the selected conversion-factor
  source in tests and documentation.
- Pricing results expose trace refs that distinguish RVU release, GPCI release,
  conversion-factor release, and conversion-factor source.
- If `rvu_items.conversion_factor` remains the source, tests prove its release
  and effective-date behavior for the RVU snapshot path.
- If `rvu_items.conversion_factor` is insufficient, the task stops with a
  documented decision to add a companion conversion-factor snapshot/load path.
- `normalize-rvu-locality-for-geography-resolution` proves whether ZIP `94110`
  resolves to locality keys that join to the loaded RVU/GPCI data.
- Locality normalization preserves leading zeros where the source/domain
  requires them.
- Any `00` versus `01` mapping is applied only when backed by an explicit CMS
  rule or source-table relationship.
- Existing pricing and geography behavior outside this RVU path remains
  unchanged.

## Validation

- `.venv/bin/python scripts/governance/check-work-tracker.py`
- `.venv/bin/python -m pytest tests/services/test_mpfs_component_pricing.py tests/services/test_pricing_provenance.py -q`
- `.venv/bin/python -m pytest tests/geography/test_geography_resolver.py -q`

## Privacy / Data Boundaries

This epic uses public CMS datasets, repository code, tracker YAML, and local/dev
database state. The sanitizer gate is normally a fast not-applicable check.

Stop for sanitizer review before implementation if a slice introduces private
customer/provider records, raw production database dumps, secrets, browser
state, finance/call artifacts, or external-memory ingestion.

## PRD / STD Impact

No new PRD is required before continuing the active task sequence.

Update governed PRD/source docs before closing the epic if the implementation
establishes a durable contract, including:

- `prds/SRC-conversion-factor.md` or `prds/PRD-mpfs-prd-v1.0.md` if
  `rvu_items.conversion_factor` becomes the canonical MPFS CF source for loaded
  RVU pricing;
- `prds/SRC-locality.md`, `prds/SRC-gpci.md`, or
  `prds/PRD-rvu-gpci-prd-v0.1.md` if a formal locality normalization rule is
  adopted.

## Known Risks

- A local API smoke can return a positive amount while still using the wrong
  locality key.
- `rvu_items.conversion_factor` may be adequate for current RVU rows but still
  hide a future need for separate CF release provenance.
- Normalizing locality IDs too aggressively can mask real CMS locality
  distinctions.
- Current DB-backed tests may require a local Postgres service and can fail in a
  sandbox even when the code path is correct.
- Legacy geography ingestion tests still skip when the old
  `cms_pricing.ingestion.geography` module is unavailable. The focused
  geography resolver suite is active and should remain the locality validation
  target for this RVU pricing path.

## Stop Conditions

- Stop if the conversion-factor source cannot be proven from existing RVU row
  data and trace refs.
- Stop if the task requires a new companion conversion-factor snapshot/load
  path; create a separate task before implementing that broader path.
- Stop if pricing cannot report RVU, GPCI, and conversion-factor release IDs
  without changing the public API contract.
- Stop if locality normalization would strip leading zeros or map `00` to `01`
  without explicit CMS source support.
- Stop if ZIP `94110` correctness depends on missing source rows that require a
  fresh ingestion run or destructive DB reset.
- Stop if validation fails for a reason outside the active slice.
- Stop if implementation needs external network access, production data, or
  secrets.
- Stop at the next PR boundary after either active task is complete and
  validated.

## Ordered Task Slices

1. Load the latest CMS RVU release through the local DB path.
   - Status: done.
   - Evidence: `docs/workbench/DOC-cms-rvu-local-db-load-status.md`.
2. Run live RVU pricing smoke.
   - Status: done.
   - Evidence: `run-live-rvu-pricing-smoke`.
3. Wire pricing to RVU snapshot selection.
   - Status: done.
   - Evidence: RVU/GPCI release trace refs in pricing results.
4. Load or map MPFS conversion factor.
   - Status: active.
   - Tracker task: `state/work/tasks/load-or-map-mpfs-conversion-factor.yaml`.
   - Validation: focused MPFS component pricing and provenance tests.
5. Normalize RVU locality for geography resolution.
   - Status: queued behind conversion-factor source formalization.
   - Tracker task:
     `state/work/tasks/normalize-rvu-locality-for-geography-resolution.yaml`.
   - Validation: focused geography resolver tests.
6. Add post-RVU-load API smoke command.
   - Status: queued.
   - Tracker task: `state/work/tasks/add-post-rvu-load-api-smoke-command.yaml`.
   - Validation: local smoke command checks health, geography, pricing,
     positive allowed amount, and expected trace refs.

## Deferred Slices

- Full automated production ingestion replay.
- Public API response redesign for richer provenance.
- Harness-managed implementation execution beyond Codex v0.
- Hermes, IronClaw, or other external harness migration.

## Notes

Use `run-epic --dry-run --epic-id cms-rvu-ingestion` for sequencing and
visibility. Use `run-epic --validate --epic-id cms-rvu-ingestion` after the
active and queued implementation slices have updated the focused tests.

The `tests/geography/test_geography_resolver.py` command is intentionally listed
as the locality validation target. It now runs independently of the legacy
geography ingestion module skip gate.
