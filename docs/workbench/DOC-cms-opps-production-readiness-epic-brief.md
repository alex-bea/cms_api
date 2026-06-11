# CMS OPPS Production Readiness

**Status:** Active - planning/audit
**Updated:** 2026-06-11
**Tracker link:** `state/work/epics/cms-opps-production-readiness.yaml`

## Related Governance

- `docs/workbench/DOC-cms-geography-production-ingestion-epic-brief.md`
- `docs/workbench/DOC-cms-pricing-pipeline-production-readiness-epic-brief.md`
- `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`
- `docs/workbench/DOC-cms-opps-production-readiness-build-brief.md`
- `prds/PRD-opps-prd-v1.0.md`
- `prds/PRD-opps-scraper-prd-v1.0.md`
- `prds/SRC-opps.md`
- `prds/SRC-oppscap.md`
- `prds/REF-scraper-ingestor-integration-v1.0.md`
- `prds/STD-data-architecture-prd-v1.0.md`
- `prds/STD-data-architecture-impl-v1.0.md`
- `prds/STD-ingestor-config-prd-v1.0.md`
- `prds/PRD-cms-treatment-plan-api-prd-v0.1.md`

## Goal

Bring OPPS Addendum A/B and status-indicator data to the same readiness bar as
the RVU/geography path: repeatable public-source discovery, digest-pinned
artifacts, normalized tables, dataset snapshots, validation gates, local and
Docker smoke evidence, and a Render execution runbook. The first production
goal is not to mutate Render; it is to make OPPS ready for an explicit
operator-approved Render load.

## Current State

The OPPS tracker epic is active. Existing OPPS assets include:

- source and product docs in `prds/SRC-opps.md`,
  `prds/PRD-opps-prd-v1.0.md`, and
  `prds/PRD-opps-scraper-prd-v1.0.md`;
- scraper and ingester code in
  `cms_pricing/ingestion/scrapers/cms_opps_scraper.py` and
  `cms_pricing/ingestion/ingestors/opps_ingestor.py`;
- contracts for OPPS and status indicators in
  `cms_pricing/ingestion/contracts/cms_opps_v1.0.json` and
  `cms_pricing/ingestion/contracts/cms_opps_si_lookup_v1.0.json`;
- models for `opps_apc_payment`, `opps_hcpcs_crosswalk`,
  `opps_rates_enriched`, and `ref_si_lookup`;
- API and engine entry points in `cms_pricing/routers/opps.py` and
  `cms_pricing/engines/opps.py`;
- dry-run and sandbox helpers in `scripts/dry_run_opps.py` and
  `scripts/run_opps_sandbox.sh`;
- tests under `tests/ingestors`, `tests/scrapers`, and `tests/fixtures/opps`.

The RVU path already ingests OPPSCAP rows as part of MPFS/RVU support, but this
epic is for the full OPPS outpatient dataset path: Addendum A APC rates,
Addendum B HCPCS-to-APC/status-indicator mappings, status indicator lookup, and
the request-time OPPS pricing behavior that depends on them.

## Scope

In scope:

- Audit current OPPS source discovery, artifact contracts, models, tables,
  ingester behavior, and API/engine behavior.
- Pin one current public CMS OPPS release package with source URL, release ID,
  quarter, digest, file list, row counts, and effective window.
- Load OPPS Addendum A, Addendum B, and status indicator lookup into local/dev
  Postgres through the existing DIS ingestion path or a narrowly scoped runner.
- Register dataset snapshots using CMS release/effective dates, not ingestion
  run dates.
- Add validation gates for required artifacts, row counts, natural keys,
  HCPCS/APC referential integrity, status indicator domain coverage, money
  parsing, quarterly effective windows, and provenance.
- Add a post-load API smoke that proves OPPS pricing can select the intended
  release and return traceable output.
- Run a Docker Compose production-style smoke against compose Postgres.
- Write a Render execution runbook and approval gate before any production
  OPPS mutation.

Out of scope:

- Production mutation without explicit operator approval.
- Private claims, patient, provider, or payer data.
- Full claim adjudication, NCCI/MUE editing, or payer-specific benefit logic.
- Predictive pricing or negotiated/commercial rates.
- Rebuilding MPFS/RVU calculations except where OPPSCAP integration needs a
  read-only compatibility check.

## Calculation Boundary

Do ahead of time during ingestion or precompute jobs:

- Discover and pin official CMS OPPS release artifacts.
- Land raw files and manifests with SHA-256 digests.
- Normalize Addendum A APC rates, Addendum B HCPCS/APC/status-indicator rows,
  and status indicator lookup rows.
- Validate natural keys, required fields, status indicator domains, money
  parsing, row-count floors, source corrections, and effective windows.
- Register `DatasetSnapshot` rows for OPPS datasets with CMS effective dates.
- Build indexed lookup tables or curated views for latest-effective release
  selection by valuation date.
- Optionally materialize stable per-quarter joins from HCPCS -> APC -> APC
  payment rate when the source data is finite and context-independent.

Calculate on request:

- Valuation-date/quarter snapshot selection.
- HCPCS/modifier lookup and final APC/status-indicator selection.
- Packaging decisions that depend on the request/claim context, especially
  conditional packaging indicators such as `Q1`, `Q2`, `Q3`, and comprehensive
  APC behavior.
- Geographic or facility-specific adjustment if/when wage-index inputs and
  facility/CBSA context are available.
- Beneficiary cost sharing/copay presentation when the request requires it.
- Final allowed amount, packaged flags, warnings, and trace refs.

Do not precompute every OPPS price fact across all ZIPs, facilities, dates, and
claim contexts until request volume proves it is necessary. OPPS packaging is
context-sensitive enough that request-time calculation is the safer default.

## Acceptance Criteria

- A dry run identifies the OPPS release, source URLs, artifact filenames,
  digest, quarter, effective window, and intended tables.
- Required OPPS artifacts are present: Addendum A, Addendum B, and status
  indicator lookup or a documented equivalent.
- Local/dev Postgres load creates positive row counts for APC payments,
  HCPCS crosswalk, and status indicator lookup.
- Dataset snapshots exist for the loaded OPPS release and use CMS effective
  dates.
- Validation fails clearly on missing artifacts, malformed money fields,
  duplicate natural keys, missing APC references, unknown status indicators,
  or valuation-date coverage gaps.
- OPPS post-load smoke proves at least one representative HCPCS can select the
  expected OPPS release and produce traceable API output.
- Docker Compose smoke reproduces the local proof path against compose
  Postgres.
- The Render runbook separates deploy, migration, backup, scoped load,
  rollback, and final smoke steps.
- The production approval gate records that no Render mutation happens until
  an operator approves the target service/database, source digest, release,
  backup/rollback path, and smoke checklist.

## Validation

- `.venv/bin/python tools/work_tracker.py check`
- `.venv/bin/python tools/work_tracker.py check-views`
- Focused OPPS scraper/source-discovery tests.
- Focused OPPS ingester tests for artifact profile, Addendum A/B parsing,
  status indicator lookup, natural keys, and provenance.
- Local OPPS dry run that emits release, digest, artifact list, row counts,
  rejects, duplicate keys, and referential-integrity results.
- Local/dev Postgres load into an isolated database or rollback-backed test DB.
- Post-load OPPS API smoke with an authenticated request and trace refs.
- Docker Compose production-style OPPS smoke against compose Postgres.
- Render runbook dry-run review before any production mutation.

## Privacy / Data Boundaries

This epic uses public CMS OPPS artifacts. Sanitizer review should normally be a
fast not-applicable check.

Stop for sanitizer review before implementation if a slice introduces private
claims, provider/customer records, production database dumps, secrets, browser
state, finance/call artifacts, or external-memory ingestion.

## PRD / STD Impact

Update governed docs when implementation establishes durable behavior:

- `prds/SRC-opps.md` for verified current release, source URLs, artifact names,
  digest policy, effective-date policy, and operational status.
- `prds/PRD-opps-prd-v1.0.md` for production-readiness behavior and API
  surface changes.
- `prds/REF-cms-pricing-source-map-prd-v1.0.md` for OPPS source discovery and
  release status.
- `prds/STD-data-architecture-impl-v1.0.md` only if OPPS adds a new DIS
  lifecycle convention beyond the RVU/geography pattern.
- Render and production preflight docs once runbook behavior is ready.

No new PRD is required before the first audit slice. A PRD/source-map update is
required before the final production-readiness task is closed.

## Known Risks

- CMS OPPS pages and addendum filenames change across quarters and corrections.
- Some downloads may require disclaimer/AMA interstitial handling.
- Historical OPPS files may drift across CSV, XLSX, ZIP, and worksheet shapes.
- OPPS packaging rules are claim-context-sensitive and should not be flattened
  into a single precomputed fee table too early.
- Wage-index/facility adjustment may require additional public datasets and a
  separate readiness slice.
- Current contract/model naming may differ (`apc` vs `apc_code`,
  `hcpcs` vs `hcpcs_code`).
- Existing local OPPS dry-run evidence may be sandbox-only and not sufficient
  for Render readiness.

## Stop Conditions

- Stop if any command would write to a production or non-local database without
  explicit approval.
- Stop if source artifacts cannot be tied to an official CMS OPPS release,
  quarter, digest, and effective window.
- Stop if money values require binary floating point for persisted or returned
  payment amounts.
- Stop if Addendum B references APCs that are missing from Addendum A for the
  same release and the gap is not explicitly documented by CMS.
- Stop if unknown status indicators are treated as payable by default.
- Stop if a slice proposes precomputing context-sensitive packaging decisions
  without a request-context proof path.
- Stop if Render execution lacks a backup/rollback step and final live smoke
  checklist.

## Ordered Task Slices

1. Audit OPPS source contracts and current ingestion.
   - Tracker task:
     `state/work/tasks/audit-opps-source-contracts-and-current-ingestion.yaml`.
   - Output: gap report covering PRDs/source docs, scraper, ingester, models,
     contracts, tests, and current dry-run behavior.

2. Pin latest public OPPS source release.
   - Tracker task:
     `state/work/tasks/pin-latest-cms-opps-source-release.yaml`.
   - Output: source URL, release ID, quarter, artifact names, digest, effective
     window, and expected row-count evidence.

3. Normalize OPPS Addenda and status indicator contracts.
   - Tracker task:
     `state/work/tasks/normalize-opps-addenda-and-si-contracts.yaml`.
   - Output: Addendum A, Addendum B, and status-indicator source
     rows normalize into contract-backed frames without writing runtime tables.

4. Load latest OPPS release locally.
   - Tracker task:
     `state/work/tasks/load-latest-cms-opps-local-db.yaml`.
   - Output: local/dev Postgres load evidence and snapshot registration for
     OPPS tables.

5. Register OPPS snapshots and selection tests.
   - Tracker task:
     `state/work/tasks/register-opps-snapshots-and-selection-tests.yaml`.
   - Output: OPPS dataset snapshots use CMS effective dates and
     valuation-date selection chooses the intended OPPS release.

6. Add OPPS production validation gates.
   - Tracker task:
     `state/work/tasks/add-opps-production-validation-gates.yaml`.
   - Output: enforceable gates for artifacts, row counts, natural keys,
     referential integrity, status indicators, effective windows, and
     provenance.

7. Implement OPPS request-time pricing resolver.
   - Tracker task:
     `state/work/tasks/implement-opps-request-time-pricing-resolver.yaml`.
   - Output: narrow resolver behavior for valuation date, HCPCS/APC/SI lookup,
     packaged/payable classification, amount, warnings, and trace refs.

8. Wire OPPS runtime pricing smoke.
   - Tracker task:
     `state/work/tasks/wire-opps-runtime-pricing-smoke.yaml`.
   - Output: authenticated post-load API smoke showing request-time OPPS
     release selection, calculation, packaging signals, and trace refs.

9. Run Docker Compose OPPS production-style smoke.
   - Tracker task:
     `state/work/tasks/run-docker-compose-opps-production-style-smoke.yaml`.
   - Output: compose Postgres smoke evidence matching the local proof path.

10. Write Render OPPS production execution runbook.
   - Tracker task:
     `state/work/tasks/write-render-opps-production-execution-runbook.yaml`.
   - Output: Render deploy, migration, backup, scoped load, rollback, and live
     OPPS smoke instructions.

11. Approve Render OPPS production execution runbook.
   - Tracker task:
     `state/work/tasks/approve-render-opps-production-execution-runbook.yaml`.
   - Output: operator approval or blockers before any production mutation.
