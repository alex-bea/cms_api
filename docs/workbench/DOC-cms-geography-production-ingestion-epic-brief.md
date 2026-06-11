# CMS Geography Production Ingestion

**Status:** Active - Render approval gate
**Updated:** 2026-06-11
**Tracker link:** `state/work/epics/cms-geography-production-ingestion.yaml`

## Related Governance

- `docs/workbench/DOC-cms-geography-real-data-breadth-epic-brief.md`
- `docs/workbench/DOC-cms-pricing-pipeline-production-readiness-epic-brief.md`
- `docs/workbench/DOC-cms-pricing-pipeline-production-readiness-build-brief.md`
- `docs/workbench/DOC-cms-rvu-local-db-load-status.md`
- `prds/PRD-geography-locality-mapping-prd-v1.0.md`
- `prds/REF-geography-source-map-prd-v1.0.md`
- `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- `prds/REF-nearest-zip-resolver-prd-v1.0.md`
- `prds/STD-data-architecture-prd-v1.0.md`
- `prds/STD-data-architecture-impl-v1.0.md`
- `prds/SRC-locality.md`
- `prds/SRC-gpci.md`

## Goal

Promote the proven local/dev CMS geography loader into the production DIS
ingestion path so CMS ZIP-locality and ZIP9 source data can be discovered,
landed, validated, normalized, published to runtime `geography`, registered for
snapshot/provenance selection, and smoke-tested without relying on
`scripts/load_cms_geography_local.py` as the durable ingestion mechanism.

## Current State

The completed `cms-geography-real-data-breadth` epic proved that real public CMS
geography data is usable in local/dev:

- `scripts/load_cms_geography_local.py` parses the public CMS
  `zip-code-carrier-locality-file-revised-08/14/2025.zip` package;
- the source package contains `ZIP5_OCT2025.txt`, `ZIP5lyout.txt`,
  `ZIP9_OCT2025.txt`, and `ZIP9lyout.txt`;
- local/dev load inserted `1,118,970` runtime `geography` rows:
  `42,956` ZIP5 rows and `1,076,014` ZIP9 rows;
- the load preserved leading-zero ZIP, plus4, carrier, and locality strings;
- locality `00` was preserved as a real CMS value;
- `94110` resolves to `CA` locality `05`, carrier `01112`;
- MPFS pricing can join all active geography state/locality pairs to 2026 GPCI
  after the pricing-side special-state mapping.

PR #450 advanced the production ingestion path:

- `CMSZipLocalityProductionIngester` now parses the landed CMS ZIP-locality
  package through `cms_pricing.ingestion.parsers.cms_geography`;
- the production ZIP-locality ingester publishes source ZIP5 and ZIP9 rows into
  runtime `geography`;
- runtime publication is non-destructive by default and requires explicit scoped
  replace behavior for overlapping rows;
- `ZIP_LOCALITY` dataset snapshots are registered from the source digest and
  effective window;
- `CMSZip9Ingester` now discovers the same CMS ZIP-locality package and
  delegates fixed-width ZIP9 parsing to the shared geography parser while
  retaining its legacy `zip9_overrides` output strategy.

After PR #450 merged, the local/dev seedless pipeline passed end to end:

- the geography load reused the full `ZIP_LOCALITY` runtime geography snapshot:
  `1,118,970` rows, zero rejects, zero duplicate source keys, and
  `94110 -> CA/05/01112`;
- the RVU loader selected and refreshed `rvu_2026_C`;
- snapshot selection picked `rvu_items -> rvu_2026_C` and
  `gpci_indices -> gpci_2026_C` for valuation date `2026-07-01`;
- the post-RVU API smoke returned `status=ok`, `allowed_cents=11758`,
  `release_id=rvu_2026_C`, and RVU/GPCI/CF trace refs.

Production-readiness closure:

- source discovery and effective-date policy are documented in
  `docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`;
- dry-run-only production preflight is documented in
  `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`;
- local evidence is recorded in
  `docs/workbench/DOC-cms-rvu-geography-local-production-preflight-evidence.md`;
- strict source-window mode correctly blocks `2026-07-01` for the pinned
  `2025Q4` source, while explicit latest-active/open-ended local preflight
  passes;
- full local smoke passes for both `94110 -> CA/05/01112` and
  `66012 -> EK/00/05202`, with `release_id=rvu_2026_C` and accepted
  `proof_path=production_style_local_smoke`;
- Docker Compose production-style smoke passes against isolated compose database
  `cms_pricing_docker_smoke_20260611_01`;
- Docker evidence is recorded in
  `docs/workbench/DOC-cms-rvu-geography-docker-compose-production-style-smoke-evidence.md`;
- the Render production execution runbook is drafted in
  `docs/workbench/DOC-render-rvu-geography-production-execution-runbook.md`;
- production mutation remains blocked until operator approval is recorded.

## Scope

In scope:

- Reconcile the proven local/dev loader behavior with existing DIS geography
  ingesters and contracts.
- Extract or reuse shared CMS ZIP5/ZIP9 parsing and reporting code so local/dev
  and production ingestion do not diverge.
- Update production ZIP-locality/ZIP9 ingestion so landed CMS source rows can
  publish to runtime `geography`.
- Preserve source-native `state`, `zip5`, `plus4`, `carrier`, `locality_id`,
  `rural_flag`, year/quarter, effective dates, source filename, and digest.
- Register `DatasetSnapshot` or equivalent provenance rows for
  `ZIP_LOCALITY` releases.
- Add validation gates for source structure, row counts, duplicate active keys,
  leading-zero preservation, locality `00`, probe ZIPs, and GPCI join coverage.
- Make the effective-date policy explicit: strict source-quarter dating,
  latest-effective/open-ended behavior, or newer source discovery.
- Add a repeatable production/local-dev orchestration path and smoke workflow
  that runs after RVU/GPCI loads.
- Update PRD/source-map/runbook docs when production behavior is established.

Out of scope:

- Address-to-ZIP+4 enrichment.
- Private patient, provider, customer, or claims data.
- Production database mutation without an explicit migration/runbook and
  operator approval.
- Rebuilding nearest-ZIP geospatial fallback unless production validation proves
  it blocks geography publication.
- Changing MPFS pricing math outside the GPCI geography lookup mapping already
  proven in the breadth epic.

## Acceptance Criteria

- Production geography ingestion parses real CMS ZIP5 and ZIP9 source files
  from the landed CMS package instead of replaying database seed/sample rows.
- Runtime `geography` receives real CMS ZIP5 and ZIP9 rows with source-native
  strings and effective dates.
- `DatasetSnapshot` or equivalent provenance records identify the loaded
  `ZIP_LOCALITY` release, digest, source URL, effective window, and manifest.
- The production path is non-destructive by default and has explicit,
  scoped replace/reload semantics.
- The same fixed-width parsing rules are tested once and reused by the local
  loader and production ingester path.
- Validation fails clearly on malformed rows, duplicate active ZIP/plus4 keys,
  `00` to `01` normalization, missing required fields, or valuation-date
  coverage mismatches.
- GPCI join validation proves every active geography state/locality pair either
  joins directly or through the governed MPFS special-state mapping.
- `94110` resolves to CA locality `05` from production-loaded geography data.
- The post-RVU-load API smoke passes after production-style geography ingestion,
  without running the one-row seed helper.
- PRD/source-map/runbook docs describe the production command, source discovery,
  effective-date policy, validation gates, and remaining limitations.

## Validation

- `.venv/bin/python tools/work_tracker.py check`
- `.venv/bin/python tools/work_tracker.py check-views`
- `.venv/bin/python -m pytest tests/scripts/test_load_cms_geography_local.py -q`
- Focused parser/adapter tests for shared ZIP5/ZIP9 fixed-width parsing.
- Focused ingester tests for land/validate/normalize/publish into
  `geography`.
- Production-style dry run against the CMS ZIP-locality package that reports
  row counts, digest, effective window, rejects, duplicate active keys, locality
  counts, and probe ZIPs.
- Local/dev DB load through the production path against
  `postgresql://cms_user:cms_password@localhost:5432/cms_pricing`.
- SQL or scripted validation for row count parity, active-key uniqueness,
  locality `00` preservation, leading-zero preservation, and 119/119
  state/locality GPCI joins after mapping.
- `.venv/bin/python scripts/post_rvu_load_api_smoke.py --database-url <local-dev-postgres-url>`
  after RVU/GPCI and geography production-style loads.

## Privacy / Data Boundaries

This epic uses public CMS geography and fee schedule data. The sanitizer gate is
normally a fast not-applicable check.

Stop for sanitizer review before implementation if a slice introduces private
customer/provider records, raw production database dumps, secrets, browser
state, finance/call artifacts, or external-memory ingestion.

## PRD / STD Impact

Update governed docs when implementation establishes durable behavior:

- `PRD-geography-locality-mapping-prd-v1.0.md` for production ingestion,
  snapshot, effective-date, and GPCI join semantics.
- `REF-geography-source-map-prd-v1.0.md` and
  `REF-cms-pricing-source-map-prd-v1.0.md` for source discovery, artifact
  names, parser layout assumptions, and production status.
- `STD-data-architecture-impl-v1.0.md` if the implementation adds or changes
  DIS lifecycle conventions for geography datasets.
- `DOC-cms-rvu-local-db-load-status.md` if the post-RVU smoke runbook changes.

No new PRD is required before the first audit/contract slice. A PRD/source-map
update is required before the final production ingestion task is closed.

## Known Risks

- CMS source URLs and filenames may change across releases.
- The existing ZIP-locality and ZIP9 ingesters target different table families
  than runtime `geography`; updating them may expose ownership ambiguity.
- The 2026 RVU smoke still depends on explicit latest-effective semantics unless
  a newer ZIP-locality package is discovered and validated.
- Production replace semantics need careful scoping to avoid deleting unrelated
  geography rows.
- Current `geography` schema has no explicit release ID column; provenance is
  inferred from `dataset_id` and `dataset_digest`.
- Full ZIP9 loads are large enough that production path must use chunking and
  transactional boundaries deliberately.
- Existing repo commit hooks have unrelated markdown checkbox failures, which
  can obscure validation signal unless checked separately.

## Stop Conditions

- Stop if implementation would write to a production or non-local database
  without explicit approval and a runbook.
- Stop if the authoritative CMS ZIP-locality source cannot be discovered,
  downloaded, or associated with a release/effective window.
- Stop if the production ingester cannot preserve leading-zero ZIP5, plus4,
  carrier, or locality strings.
- Stop if any implementation normalizes CMS locality `00` to `01`.
- Stop if source parsing cannot validate active-key uniqueness for ZIP/plus4 and
  valuation date.
- Stop if GPCI join validation fails after applying the governed special-state
  mapping and the mismatch cannot be classified.
- Stop if production reload semantics would overwrite or delete existing
  geography rows without an explicit scoped replace flag.
- Stop if local/dev smoke can only pass by running
  `scripts/seed_post_rvu_load_local.py`.

## Ordered Task Slices

1. Audit production geography ingester contracts.
   - Status: done.
   - Tracker task:
     `state/work/tasks/audit-geography-production-ingester-contracts.yaml`.
   - Validation: gap matrix comparing local loader behavior, DIS contracts,
     ZIP-locality ingester, ZIP9 ingester, target tables, and effective-date
     policy.
2. Extract shared CMS geography parser.
   - Status: done.
   - Tracker task:
     `state/work/tasks/extract-shared-cms-geography-parser.yaml`.
   - Validation: shared parser tests cover ZIP5, ZIP9, source digest,
     year/quarter mapping, leading zeros, locality `00`, rejects, and duplicate
     active keys.
3. Publish CMS ZIP-locality ingestion to runtime geography.
   - Status: done.
   - Tracker task:
     `state/work/tasks/publish-cms-zip-locality-to-runtime-geography.yaml`.
   - Validation: production ingester can land, validate, normalize, and publish
     real rows into `geography` in a local/dev DB without the local loader.
4. Consolidate ZIP9 ingestion and source discovery.
   - Status: done.
   - Tracker task:
     `state/work/tasks/consolidate-cms-zip9-ingestion-and-source-discovery.yaml`.
   - Validation: ZIP9 rows no longer diverge between `zip9_overrides` and
     runtime `geography`, and source discovery reports the selected package.
5. Add production geography validation gates.
   - Status: done.
   - Tracker task:
     `state/work/tasks/add-production-geography-validation-gates.yaml`.
   - Validation: `cms_geography_readiness` gates fail on rejects, duplicate
     active keys, `00` locality loss, row-count regressions, probe mismatch,
     GPCI join misses, seed-helper proof paths, and valuation-date coverage
     mismatches. The real local CMS ZIP package dry-run passed all readiness
     gates with 1,118,970 rows and `94110 -> CA/05/01112`.
6. Wire production ingestion smoke and runbook.
   - Status: done.
   - Tracker task:
     `state/work/tasks/wire-production-geography-ingestion-smoke-and-runbook.yaml`.
   - Validation: `scripts/run_cms_pricing_local_smoke.py --dry-run-plan`
     emits the production-style local/dev sequence for geography readiness,
     RVU/GPCI load, default `94110` smoke, and special source-state `66012`
     smoke without the one-row seed helper.
   - Includes the inserted local DB isolation subtask: DB-backed resolver/MPFS
     tests must be deterministic even when real CMS geography rows are present
     in the shared local DB.
7. Document CMS ZIP-locality source discovery and effective-date policy.
   - Status: done.
   - Tracker task:
     `state/work/tasks/document-cms-zip-locality-source-discovery-effective-date-policy.yaml`.
   - Output:
     `docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`.
   - Validation: source package selection, digest expectations, strict quarter
     coverage, explicit latest-active/open-ended behavior, and blocking stop
     conditions are documented for production preflight.
8. Write CMS pricing production preflight runbook.
   - Status: done.
   - Tracker task:
     `state/work/tasks/write-cms-pricing-production-preflight-runbook.yaml`.
   - Output:
     `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`.
   - Validation: dry-run-only production preflight steps, approvals, rollback
     plan, scoped replace rules, readiness gates, and final smoke expectations
     are documented before any production mutation.
   - Completion benchmarks: strict-mode source-window block, latest-active pass,
     `94110 -> CA/05/01112`, `66012 -> EK/00/05202`, zero rejects, zero
     duplicate source keys, locality `00` preservation,
     `proof_path=production_style_local_smoke`, and no seed-helper evidence.
9. Execute RVU/geography local production preflight.
   - Status: done.
   - Tracker task:
     `state/work/tasks/execute-rvu-geography-local-production-preflight.yaml`.
   - Output:
     `docs/workbench/DOC-cms-rvu-geography-local-production-preflight-evidence.md`.
   - Validation: local evidence commands are run and recorded against the
     completion benchmarks from the runbook before final close-docs work.
   - Stop if local DB smoke can only pass through
     `scripts/seed_post_rvu_load_local.py`, if source readiness gates fail, or
     if the optional full local smoke cannot prove `rvu_2026_C` selection.
10. Promote geography production ingestion docs and close.
   - Status: done.
   - Tracker task:
     `state/work/tasks/promote-geography-production-ingestion-docs-and-close.yaml`.
   - Validation: PRD/source-map/workbench docs and tracker state describe the
     final production path, source/effective-date policy, production preflight
     runbook, local preflight evidence, known limitations, and follow-up work.
11. Run Docker Compose RVU/geography production-style smoke.
   - Status: done.
   - Tracker task:
     `state/work/tasks/run-docker-compose-rvu-geography-production-style-smoke.yaml`.
   - Output:
     `docs/workbench/DOC-cms-rvu-geography-docker-compose-production-style-smoke-evidence.md`.
   - Validation: the full production-style sequence runs inside Docker Compose
     against the compose Postgres database, proving the path is not dependent on
     host-only venv or database state.
   - Evidence: readiness gates passed, no seed helper was used, `94110` and
     `66012` smoke passed with `release_id=rvu_2026_C` and positive pricing.
12. Write Render RVU/geography production execution runbook.
   - Status: done.
   - Tracker task:
     `state/work/tasks/write-render-rvu-geography-production-execution-runbook.yaml`.
   - Output:
     `docs/workbench/DOC-render-rvu-geography-production-execution-runbook.md`.
   - Validation: runbook names Render target, source digest, RVU release,
     deploy/migration checks, scoped load commands, backup/rollback path, live
     smoke checks, and a pre-mutation approval checkpoint.
13. Approve Render RVU/geography production execution runbook.
   - Status: active.
   - Tracker task:
     `state/work/tasks/approve-render-rvu-geography-production-execution-runbook.yaml`.
   - Output:
     `docs/workbench/DOC-render-rvu-geography-production-approval-gate.md`.
   - Validation: operator approval or blockers are recorded before any Render
     production data mutation. Approval is required before a later execution
     task can load production data.

## Local Preflight Completion Benchmarks

The RVU/geography production preflight is locally complete only when:

- strict geography readiness dry run blocks `2026-07-01` under the pinned
  `2025Q4` source window;
- latest-active/open-ended readiness dry run passes and records
  `open_ended_latest=true`;
- source digest remains
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`;
- source scan reports zero rejects and zero duplicate source keys;
- locality `00` rows remain present and are not normalized to `01`;
- `94110` resolves to `CA/05/01112`;
- `66012` resolves to `EK/00/05202`;
- smoke evidence uses `proof_path=production_style_local_smoke`;
- post-RVU smoke does not depend on `scripts/seed_post_rvu_load_local.py`;
- full local DB smoke, when local Postgres is available, selects
  `rvu_2026_C` and returns positive pricing.

## Notes

Use Codex v0 as the harness and advance sequentially. Do not perform production
database writes in this epic without explicit user approval and a separate
runbook.

The next phase is execution readiness, not production execution. Sequence:

1. Docker Compose smoke against compose Postgres. Done.
2. Render production execution runbook. Done.
3. Operator approval gate. Active.
4. Separate production execution task only after approval.

## Parser Slice Result

The `extract-shared-cms-geography-parser` slice is complete.

Added `cms_pricing/ingestion/parsers/cms_geography.py` as the shared source
contract for CMS ZIP-locality parsing and reporting. The module now owns:

- ZIP5 and ZIP9 fixed-width line parsing;
- source ZIP digesting and member selection;
- source row iteration;
- source scan stats and duplicate/reject accounting;
- locality `00` preservation;
- year/quarter effective-window derivation;
- explicit open-ended/latest behavior;
- probe and valuation coverage reporting.

Updated `scripts/load_cms_geography_local.py` to delegate parsing, scanning,
digesting, and report construction to the shared module while retaining only
CLI, local DB safety, snapshot registration, and runtime `geography` loading
concerns.

Focused tests now import the shared parser directly. Real-source dry run still
reports `1,118,970` rows, zero rejects, zero duplicate source keys, and
`94110 -> CA/05/01112`.
