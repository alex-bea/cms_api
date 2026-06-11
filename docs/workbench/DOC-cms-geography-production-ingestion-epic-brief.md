# CMS Geography Production Ingestion

**Status:** Active
**Updated:** 2026-06-11
**Tracker link:** `state/work/epics/cms-geography-production-ingestion.yaml`

## Related Governance

- `docs/workbench/DOC-cms-geography-real-data-breadth-epic-brief.md`
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

The production ingestion path is still not equivalent:

- `CMSZipLocalityProductionIngester` downloads the CMS ZIP package but its
  validation is mocked and normalization reads existing `cms_zip_locality`
  rows instead of parsing the landed source package;
- `CMSZipLocalityProductionIngester` publishes to `cms_zip_locality`, not to
  runtime `geography`;
- `CMSZip9Ingester` parses ZIP9 rows but targets `zip9_overrides`, drops
  carrier from the runtime projection, and does not publish the data needed by
  the pricing resolver;
- source discovery is hard-coded to the 2025-08-14 package and does not yet
  select or validate newer CMS packages;
- latest-effective semantics are explicit only in the local/dev loader
  (`--open-ended-latest`) and are not yet a governed production policy;
- production validation gates do not yet cover the proven real-data checks:
  row counts, duplicate active keys, locality `00` preservation, 94110 probe,
  state/locality GPCI join coverage, and post-RVU API smoke.

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
   - Status: active.
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
   - Status: active.
   - Tracker task:
     `state/work/tasks/publish-cms-zip-locality-to-runtime-geography.yaml`.
   - Validation: production ingester can land, validate, normalize, and publish
     real rows into `geography` in a local/dev DB without the local loader.
4. Consolidate ZIP9 ingestion and source discovery.
   - Status: queued.
   - Tracker task:
     `state/work/tasks/consolidate-cms-zip9-ingestion-and-source-discovery.yaml`.
   - Validation: ZIP9 rows no longer diverge between `zip9_overrides` and
     runtime `geography`, and source discovery reports the selected package.
5. Add production geography validation gates.
   - Status: queued.
   - Tracker task:
     `state/work/tasks/add-production-geography-validation-gates.yaml`.
   - Validation: gates fail on malformed rows, duplicate active keys, `00`
     normalization, row-count regressions, GPCI join misses, and valuation-date
     coverage mismatches.
6. Wire production ingestion smoke and runbook.
   - Status: queued.
   - Tracker task:
     `state/work/tasks/wire-production-geography-ingestion-smoke-and-runbook.yaml`.
   - Validation: production-style geography ingestion plus RVU/GPCI load passes
     post-RVU API smoke without the one-row seed helper.
7. Promote geography production ingestion docs and close.
   - Status: queued.
   - Tracker task:
     `state/work/tasks/promote-geography-production-ingestion-docs-and-close.yaml`.
   - Validation: PRD/source-map/workbench docs and tracker state describe the
     final production path, known limitations, and follow-up work.

## Notes

Use Codex v0 as the harness and advance sequentially. Do not perform production
database writes in this epic without explicit user approval and a separate
runbook.

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
