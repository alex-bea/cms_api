# Codex Build Brief: CMS OPPS Production Readiness

**Status:** Active - planning/audit
**Updated:** 2026-06-11
**Parent epic:** `docs/workbench/DOC-cms-opps-production-readiness-epic-brief.md`
**Active tracker task:** `state/work/tasks/audit-opps-source-contracts-and-current-ingestion.yaml`

## Objective

Build the OPPS production-readiness path so the CMS pricing API can load,
validate, snapshot, smoke-test, and eventually serve public CMS OPPS Addendum A,
Addendum B, and status-indicator data through a repeatable local-to-Render
workflow.

This is for the backend CMS data ingestion and pricing workflow. The first goal
is production readiness evidence, not immediate Render mutation.

## Current State

The repo already has OPPS docs, source definitions, scraper/ingester code,
contracts, models, an engine/router surface, sandbox helpers, and tests. The
gap is that OPPS does not yet have the RVU/geography production-readiness path:
source pinning, digest evidence, local DB load evidence, dataset snapshot
registration, validation gates, Docker Compose smoke, and a Render execution
runbook with explicit approval.

The RVU path already loads OPPSCAP rows for MPFS support, but that is not the
same as a full OPPS outpatient dataset path. This build brief covers OPPS
Addendum A APC payment rates, Addendum B HCPCS/APC/status-indicator mappings,
status indicator lookup rows, and the request-time behavior that depends on
those datasets.

## Desired Behavior

When Codex runs this epic as the v0 harness, the workflow should:

1. Audit existing OPPS source docs, contracts, ingestion code, data models,
   pricing engine, API router, tests, and sandbox evidence before changing
   implementation.
2. Pin one current official CMS OPPS release with source URL, artifact names,
   release ID, quarter, digest, row counts, and CMS effective window.
3. Load OPPS Addendum A/B and status-indicator data into local/dev Postgres
   through the smallest safe existing ingestion path.
4. Register OPPS dataset snapshots with CMS effective dates rather than
   ingestion run dates.
5. Add production-readiness validation gates and machine-readable evidence.
6. Prove request-time OPPS selection and pricing behavior with a post-load API
   smoke.
7. Reproduce the proof path inside Docker Compose against compose Postgres.
8. Stop before Render mutation until an operator approves the production
   execution runbook.

## Scope

In scope:

- OPPS source and implementation audit.
- Official CMS release pinning and provenance.
- Local/dev OPPS load using public CMS data.
- Dataset snapshot registration and valuation-date selection evidence.
- Validation gates for artifacts, row counts, money parsing, natural keys,
  HCPCS/APC referential integrity, status indicator domains, effective windows,
  and provenance.
- OPPS API smoke with trace refs.
- Docker Compose production-style OPPS smoke.
- Render execution runbook and approval gate.

Out of scope:

- Production or Render database mutation without explicit approval.
- Private claims, patient, provider, payer, or customer data.
- Full claim adjudication, NCCI/MUE editing, payer-specific benefit logic, or
  predictive pricing.
- Rebuilding MPFS/RVU pricing except for read-only compatibility checks.
- Precomputing every context-sensitive OPPS price fact across ZIP, facility,
  valuation date, and claim context.

## Relevant Context

Use the existing codebase, patterns, database schema, APIs, and conventions.
Before proposing implementation changes, inspect the relevant files and identify
the smallest safe implementation path.

Relevant areas to inspect:

- `docs/workbench/DOC-cms-opps-production-readiness-epic-brief.md`
- `prds/SRC-opps.md`
- `prds/PRD-opps-prd-v1.0.md`
- `prds/PRD-opps-scraper-prd-v1.0.md`
- `prds/SRC-oppscap.md`
- `cms_pricing/ingestion/scrapers/cms_opps_scraper.py`
- `cms_pricing/ingestion/ingestors/opps_ingestor.py`
- `cms_pricing/ingestion/contracts/cms_opps_v1.0.json`
- `cms_pricing/ingestion/contracts/cms_opps_si_lookup_v1.0.json`
- `cms_pricing/models/opps/opps_apc_payment.py`
- `cms_pricing/models/opps/opps_hcpcs_crosswalk.py`
- `cms_pricing/models/opps/ref_si_lookup.py`
- `cms_pricing/engines/opps.py`
- `cms_pricing/routers/opps.py`
- `scripts/dry_run_opps.py`
- `scripts/run_opps_sandbox.sh`
- `tests/ingestors/test_opps_*`
- `tests/scrapers/test_opps_*`
- `tests/fixtures/opps/*`

## Requirements

1. OPPS audit and gap report.
   - Acceptance: a workbench note or epic update identifies current OPPS source
     discovery, artifact contracts, parser/load behavior, model/API readiness,
     test coverage, and the smallest safe next implementation slice.

2. Source release pinning.
   - Acceptance: the selected CMS OPPS release has an official source URL,
     artifact filenames, quarter/release ID, SHA-256 digest, effective window,
     and expected row-count evidence.

3. Contract-backed source normalization.
   - Acceptance: Addendum A, Addendum B, and status-indicator source rows
     normalize into contract-backed frames before any runtime table writes.

4. Local/dev load.
   - Acceptance: local/dev Postgres contains positive row counts for APC
     payments, HCPCS crosswalk rows, and status-indicator lookup rows for the
     pinned release.

5. Snapshot/effective-date behavior.
   - Acceptance: OPPS dataset snapshots use CMS effective dates and
     valuation-date lookup selects the intended release.

6. Production validation gates.
   - Acceptance: focused tests prove gates fail clearly for missing artifacts,
     malformed money values, duplicate natural keys, missing APC references,
     unknown status indicators, and valuation-date gaps.

7. Request-time pricing resolver.
   - Acceptance: the engine resolves valuation date, HCPCS/APC/status
     indicator, packaged/payable classification, amount, warnings, and trace
     refs from normalized source tables without precomputing context-sensitive
     claim prices.

8. Runtime OPPS smoke.
   - Acceptance: an authenticated local API smoke returns traceable OPPS output
     for at least one representative HCPCS, with release/snapshot refs and
     packaging signals.

9. Docker Compose proof path.
   - Acceptance: the production-style OPPS sequence passes against compose
     Postgres without depending on the shared local database.

10. Render approval gate.
   - Acceptance: the Render runbook records deploy, migration, backup, scoped
     load, rollback, final smoke, and explicit operator approval before
     production mutation.

## Calculation Boundary

Do ahead of time during ingestion or precompute jobs:

- Discover and pin official CMS OPPS release artifacts.
- Land raw files and manifests with SHA-256 digests.
- Normalize Addendum A APC rates, Addendum B HCPCS/APC/status-indicator rows,
  and status indicator lookup rows.
- Validate natural keys, required fields, status indicator domains, money
  parsing, row-count floors, source corrections, and effective windows.
- Register `DatasetSnapshot` rows for OPPS datasets with CMS effective dates.
- Build indexed lookup tables or curated latest-effective views.
- Optionally materialize stable per-quarter HCPCS to APC to APC payment rate
  joins when the source data is finite and context-independent.

Calculate on request:

- Valuation-date and quarter snapshot selection.
- HCPCS/modifier lookup and final APC/status-indicator selection.
- Packaging decisions that depend on request or claim context, including `Q1`,
  `Q2`, `Q3`, and comprehensive APC behavior.
- Geographic or facility-specific adjustment if wage-index, facility, or CBSA
  inputs are available.
- Beneficiary cost sharing or copay presentation when requested.
- Final allowed amount, packaged flags, warnings, and trace refs.

## Privacy / Sanitization Boundary

This epic uses public CMS data. The sanitizer check should normally be a fast
not-applicable gate.

Stop for sanitizer review before implementation if a slice introduces private
claims, patient/provider/customer records, production database dumps, secrets,
browser state, finance/call artifacts, or external-memory ingestion.

## PRD / STD Impact

Durable OPPS behavior should update the governed docs before the epic closes:

- `prds/SRC-opps.md` for verified source URLs, artifact names, digest policy,
  effective-date policy, and operational status.
- `prds/PRD-opps-prd-v1.0.md` for production-readiness behavior and any API or
  calculation contract changes.
- `prds/REF-cms-pricing-source-map-prd-v1.0.md` for OPPS source discovery and
  release status.
- `prds/STD-data-architecture-impl-v1.0.md` only if OPPS establishes a new DIS
  lifecycle convention beyond the RVU/geography pattern.

No new PRD is required before the first audit task.

## Tracker Mapping

- Roadmap: `state/work/roadmaps/cms-data-pipeline.yaml`
- Epic: `state/work/epics/cms-opps-production-readiness.yaml`
- Active slice:
  `state/work/tasks/audit-opps-source-contracts-and-current-ingestion.yaml`
- Queued slices:
  `state/work/tasks/pin-latest-cms-opps-source-release.yaml`,
  `state/work/tasks/normalize-opps-addenda-and-si-contracts.yaml`,
  `state/work/tasks/load-latest-cms-opps-local-db.yaml`,
  `state/work/tasks/register-opps-snapshots-and-selection-tests.yaml`,
  `state/work/tasks/add-opps-production-validation-gates.yaml`,
  `state/work/tasks/implement-opps-request-time-pricing-resolver.yaml`,
  `state/work/tasks/wire-opps-runtime-pricing-smoke.yaml`,
  `state/work/tasks/run-docker-compose-opps-production-style-smoke.yaml`,
  `state/work/tasks/write-render-opps-production-execution-runbook.yaml`, and
  `state/work/tasks/approve-render-opps-production-execution-runbook.yaml`.
- Deferred slices: wage-index/facility adjustment expansion, full claim
  packaging/adjudication, commercial payer behavior, and high-volume
  precomputed price fact tables.

## Constraints

- Keep implementation slices minimal.
- Reuse existing OPPS scraper, ingester, DIS stages, contracts, models, and API
  conventions before adding new abstractions.
- Do not add production dependencies without explicit approval.
- Do not create broad refactors.
- Do not change public APIs unless a task explicitly requires it.
- Preserve existing RVU, MPFS, geography, and OPPSCAP behavior outside the OPPS
  production-readiness path.
- Use `Decimal` or scaled integers for OPPS money values. Do not introduce
  binary floats for persisted or returned payment amounts.
- Preserve CMS release IDs, quarters, effective dates, APC IDs, HCPCS values,
  status indicators, and provenance as pricing-critical data.

## Do Not Touch

- Production or Render database configuration.
- Secrets or environment-specific deploy config.
- Generated CMS data artifacts unless a task explicitly creates local/dev
  evidence and the artifact is intentionally excluded from code PRs.
- Unrelated tracker/governance state.
- MPFS/RVU pricing math except for read-only compatibility checks.
- Public API schemas unless the active task calls for a reviewed contract
  change.

## Testing Policy

Start with the narrowest relevant checks for each slice:

```bash
.venv/bin/python -m pytest tests/scrapers/test_opps_* -q
.venv/bin/python -m pytest tests/ingestors/test_opps_* -q
.venv/bin/python -m pytest tests/api/test_health.py -q
```

For tracker/doc-only changes, run:

```bash
.venv/bin/python tools/work_tracker.py check
.venv/bin/python tools/work_tracker.py check-views
git diff --check
```

For local/dev DB slices, prefer isolated or Docker Compose databases over the
shared local database, and record row counts, release IDs, snapshot IDs, digest
refs, and smoke outputs.

## Requested Codex Output

Do not implement OPPS production mutation. First produce or update:

1. Implementation plan with files likely to change for the active slice.
2. Tracker tasks in checklist format, including done/active/queued status.
3. QA checklist with manual and automated verification steps.
4. Risks, assumptions, and open questions only if blocking.

After approval, implement the active slice, keep the diff minimal, run the
smallest relevant checks, and report commands and results.
