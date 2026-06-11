# CMS Pricing Pipeline Production Readiness

**Status:** Active - Render approval gate
**Updated:** 2026-06-11
**Tracker link:** Existing implementation work continues through
`state/work/epics/cms-geography-production-ingestion.yaml`; create a dedicated
tracker epic only if this umbrella scope becomes larger than geography
productionization.

## Related Work

- `docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`
- `docs/workbench/DOC-cms-rvu-ingestion-epic-brief.md`
- `docs/workbench/DOC-cms-rvu-local-db-load-status.md`
- `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`
- `docs/workbench/DOC-cms-rvu-geography-local-production-preflight-evidence.md`
- `docs/workbench/DOC-cms-pricing-pipeline-production-readiness-build-brief.md`
- `docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`
- `prds/PRD-geography-locality-mapping-prd-v1.0.md`
- `prds/REF-geography-source-map-prd-v1.0.md`
- `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- `prds/SRC-cms-rvu.md`
- `prds/SRC-gpci.md`
- `prds/SRC-locality.md`

## Objective

Bring the public CMS RVU, GPCI, conversion-factor, and ZIP-locality pipeline to
a production-ready state without running a production load yet. Production-ready
means the system has repeatable source discovery, dry-run validation,
non-destructive reload controls, provenance, smoke checks, and an operator
runbook that make a future production cutover a deliberate approval step rather
than an engineering investigation.

## Current Checkpoint

PR #450 merged the core geography ingestion path. After it merged, the local/dev
seedless sequence passed end to end:

- geography load reused the full `ZIP_LOCALITY` runtime geography snapshot:
  `1,118,970` rows, `42,956` ZIP5 rows, `1,076,014` ZIP9 rows, zero rejects,
  zero duplicate source keys, `39,476` locality `00` rows, and digest
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`;
- probe ZIP `94110` resolved to `CA`, locality `05`, carrier `01112`;
- the geography load used open-ended latest semantics so valuation date
  `2026-07-01` is covered by the verified public `2025Q4` ZIP-locality source;
- RVU load selected `rvu_2026_C` from `https://www.cms.gov/files/zip/rvu26c.zip`;
- curated RVU output contained `pprrvu=19,358`, `gpci=109`,
  `oppscap=15,260`, `anescf=109`, and `localitycounty=117` rows;
- DB load contained `pprrvu=19,358`, `gpci=109`, `oppscap=15,260`,
  `anescf=109`, and `localitycounty=57` rows;
- snapshot selection at `2026-07-01` picked `rvu_items -> rvu_2026_C` and
  `gpci_indices -> gpci_2026_C`;
- post-load smoke returned `status=ok`, `allowed_cents=11758`,
  `release_id=rvu_2026_C`, and trace refs for RVU, GPCI, and CF provenance.

The production-readiness preflight is now locally complete:

- strict geography readiness blocks `2026-07-01` for the pinned `2025Q4`
  ZIP-locality source, as expected;
- explicit latest-active/open-ended readiness passes with the pinned digest;
- production-style smoke plan uses `proof_path=production_style_local_smoke`
  and does not invoke the seed helper;
- full local DB smoke passes for `94110 -> CA/05/01112` and
  `66012 -> EK/00/05202`, both selecting `rvu_2026_C` with positive pricing.

Docker execution readiness is also complete:

- Docker Compose smoke ran inside the documented `cms-api-fix` stack against
  isolated database `cms_pricing_docker_smoke_20260611_01`;
- consolidated report:
  `data/ingestion/local/reports/cms_pricing_docker_smoke_20260611_01.json`;
- evidence:
  `docs/workbench/DOC-cms-rvu-geography-docker-compose-production-style-smoke-evidence.md`;
- `94110` returned `allowed_cents=11758` and `66012` returned
  `allowed_cents=8902`, both through `rvu_2026_C`;
- the Render execution runbook is drafted in
  `docs/workbench/DOC-render-rvu-geography-production-execution-runbook.md`;
- production mutation is now blocked only on the operator approval gate.

## Production Boundary

This epic is not a production push. It must not mutate a production or
non-local database. Production mutation remains blocked until there is:

- a reviewed runbook;
- a source package and release selection report;
- a successful dry run;
- validation-gate evidence;
- explicit operator approval;
- rollback or scoped-reload instructions;
- a final smoke checklist.

## In Scope

- Geography source validation gates for the CMS ZIP-locality package.
- RVU/GPCI post-load validation gates needed to prove pricing readiness.
- Source discovery policy for ZIP-locality and RVU releases.
- Explicit effective-date policy for latest public geography data.
- Local/dev orchestration that mirrors production order without relying on
  one-off seed helpers.
- Provenance reporting for source URL, release ID, digest, effective window,
  row counts, dataset snapshots, and pricing trace refs.
- Runbook steps for dry-run, approval, scoped local/dev replace, production
  preflight, and rollback planning.
- Documentation updates to PRDs/source maps once behavior is durable.

## Out Of Scope

- Production database mutation.
- Private data ingestion.
- Address-to-ZIP+4 enrichment.
- Rebuilding geospatial nearest-ZIP fallback.
- Replacing Codex v0 with Hermes, Ironclaw, or another harness.
- Changing MPFS pricing math beyond already-proven RVU/GPCI/CF/geography
  selection.

## Acceptance Criteria

- A dry-run report can prove source identity, release, effective window, digest,
  row counts, reject count, duplicate active-key count, locality `00`
  preservation, and probe ZIP correctness.
- Runtime geography publication remains non-destructive by default and requires
  explicit scoped replace semantics.
- Validation gates fail clearly before publish when source structure, row count,
  duplicate key, valuation-date, or probe expectations are not met.
- GPCI join validation proves active geography state/locality pairs either join
  directly or are classified through the governed special-state mapping.
- RVU load validation proves selected releases, DB row counts, dataset snapshot
  effective dates, and pricing trace refs.
- The post-load smoke proves `94110` prices through `rvu_2026_C`,
  `gpci_2026_C`, and `rvu_items.conversion_factor` without the seed helper.
- The production runbook separates dry-run, approval, mutation, rollback, and
  final smoke steps.

## Stop Conditions

- Stop if any step would write to production without explicit approval.
- Stop if the authoritative source package cannot be discovered or tied to a
  release/effective window.
- Stop if ZIP, plus4, carrier, locality, or `00` values would be normalized
  away.
- Stop if validation can pass only by running
  `scripts/seed_post_rvu_load_local.py`.
- Stop if latest-effective/open-ended geography behavior is required but not
  documented as an explicit policy.
- Stop if production reload behavior could delete unrelated geography rows.

## Ordered Slices

1. Production validation gates.
   - Status: done locally.
   - Build brief:
     `docs/workbench/DOC-cms-pricing-pipeline-production-readiness-build-brief.md`.
   - Output: code-level gates plus tests for source report expectations,
     valuation-date coverage, probe behavior, and seed-helper refusal.
   - Evidence: real-source dry run passed all readiness gates with
     1,118,970 rows and `94110 -> CA/05/01112`.

2. Production-style smoke/runbook command.
   - Status: done.
   - Tracker task:
     `state/work/tasks/wire-production-geography-ingestion-smoke-and-runbook.yaml`.
   - Output: one repeatable local/dev command sequence that mirrors production
     order and emits a machine-readable evidence report through
     `scripts/run_cms_pricing_local_smoke.py`.
   - Includes local DB integration-test isolation so smoke evidence is not
     contaminated by persistent real CMS rows in the shared local DB.

3. Source discovery and effective-date policy.
   - Status: done.
   - Tracker task:
     `state/work/tasks/document-cms-zip-locality-source-discovery-effective-date-policy.yaml`.
   - Output:
     `docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`
     documents rules for selecting the latest verified CMS ZIP-locality package
     and deciding strict quarter vs latest-active behavior.

4. Production preflight runbook.
   - Status: done.
   - Tracker task:
     `state/work/tasks/write-cms-pricing-production-preflight-runbook.yaml`.
   - Output:
     `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`
     documents dry-run-only production preflight steps, required approvals,
     rollback plan, scoped replace instructions, and final smoke expectations.

5. Local production preflight evidence.
   - Status: done.
   - Tracker task:
     `state/work/tasks/execute-rvu-geography-local-production-preflight.yaml`.
   - Output:
     `docs/workbench/DOC-cms-rvu-geography-local-production-preflight-evidence.md`
     records local evidence against the completion benchmarks for strict-mode
     block, latest-active pass, source digest, row cleanliness, `94110`,
     `66012`, seedless proof path, and RVU release selection.

6. Governance documentation closure.
   - Status: done.
   - Tracker task:
     `state/work/tasks/promote-geography-production-ingestion-docs-and-close.yaml`.
   - Output: updates to PRDs/source maps/workbench docs that mark what is
     production-ready, what is local/dev only, and what remains deferred.

7. Docker Compose production-style smoke.
   - Status: done.
   - Tracker task:
     `state/work/tasks/run-docker-compose-rvu-geography-production-style-smoke.yaml`.
   - Output:
     `docs/workbench/DOC-cms-rvu-geography-docker-compose-production-style-smoke-evidence.md`
     records Docker evidence that the production-style sequence works against
     compose Postgres, not only host-local venv/Postgres state.

8. Render production execution runbook.
   - Status: done.
   - Tracker task:
     `state/work/tasks/write-render-rvu-geography-production-execution-runbook.yaml`.
   - Output:
     `docs/workbench/DOC-render-rvu-geography-production-execution-runbook.md`
     defines Render-specific deploy, migration, scoped load, rollback, and live
     smoke steps for the RVU/geography path.

9. Render production execution approval.
   - Status: active.
   - Tracker task:
     `state/work/tasks/approve-render-rvu-geography-production-execution-runbook.yaml`.
   - Output:
     `docs/workbench/DOC-render-rvu-geography-production-approval-gate.md`
     records that operator approval is pending before any production mutation.

## Local Completion Benchmarks

The production preflight is locally complete only when the runbook and evidence
cover:

- strict geography readiness dry run blocks `2026-07-01`;
- latest-active/open-ended readiness dry run passes with the pinned digest;
- source scan has zero rejects and zero duplicate source keys;
- locality `00` is preserved;
- probes resolve `94110 -> CA/05/01112` and `66012 -> EK/00/05202`;
- smoke evidence uses `proof_path=production_style_local_smoke`;
- seed-helper proof paths are refused;
- full local DB smoke, when run, selects `rvu_2026_C` and returns positive
  pricing.

## Deferred Questions

- Should `ZIP_LOCALITY` latest-effective behavior be approved for production,
  or should production wait for a CMS package whose source quarter covers the
  target valuation date?
- Should source discovery remain CMS-page scraping plus URL validation, or move
  to a maintained manifest of approved CMS source packages?
- Should the Render production execution runbook include a shadow load into a staging schema
  before replacing runtime geography rows?
- Should `geography` gain a release ID column, or is
  `dataset_id + dataset_digest + DatasetSnapshot` enough provenance for the
  production cutover?
