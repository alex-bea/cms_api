# CMS Pricing Production Preflight Runbook

**Status:** Local production-readiness runbook
**Updated:** 2026-06-11
**Scope:** RVU/GPCI plus CMS ZIP-locality geography preflight evidence
**Production mutation:** Blocked until explicit operator approval

## Purpose

This runbook defines the dry-run-only preflight sequence for moving the public
CMS RVU/geography pipeline toward production. It is intentionally a preflight
runbook, not a production execution runbook. Production database mutation
remains blocked until an operator reviews the source package, evidence reports,
reload scope, rollback plan, and final smoke checklist.

## Source And Mode

Use the governed ZIP-locality policy in
`docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`
as the source contract.

Current accepted geography source:

- CMS URL:
  `https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip`
- Dataset ID: `ZIP_LOCALITY`
- Release ID: `zip_locality_2025_Q4`
- Source files: `ZIP5_OCT2025.txt`, `ZIP9_OCT2025.txt`
- Source digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- Strict source window: `2025-10-01` through `2025-12-31`
- Local smoke valuation date: `2026-07-01`

Strict source-window mode is the production default. For the current verified
source, strict mode must block `2026-07-01` because the source quarter is
`2025Q4`.

Latest-active/open-ended mode is allowed for local/dev preflight only when it is
explicitly passed as `--open-ended-latest` and the report records
`open_ended_latest=true`. A future production mutation must either use strict
mode with a source package that covers the target valuation date or receive
explicit approval for latest-active behavior.

## Required Approvals Before Production Mutation

Before any production write, the operator must approve:

- CMS ZIP-locality source URL, digest, release ID, and source files;
- RVU release selection and CMS URL;
- valuation date and effective-date mode;
- scoped reload target and replace behavior;
- rollback or restore strategy;
- expected final smoke probes and trace refs;
- evidence report paths from this preflight.

Without those approvals, stop after dry-run/local evidence.

## Preflight Sequence

### 1. Strict Geography Readiness

Run strict mode first. For the current source and `2026-07-01`, this is
expected to block. Passing strict mode for this valuation date would mean either
a newer source package is in use or the policy has changed.

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /tmp/cms-geography-strict-readiness.json
```

Expected result:

- nonzero exit or report `status=blocked`;
- stop condition reflects valuation-date coverage;
- no database mutation.

### 2. Latest-Active Geography Readiness

Run the explicit local/dev latest-active mode.

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --open-ended-latest \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /tmp/cms-geography-open-ended-readiness.json
```

Expected result:

- `status=ok`;
- `open_ended_latest=true`;
- digest matches
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`;
- total rows `1,118,970`;
- ZIP5 rows `42,956`;
- ZIP9 rows `1,076,014`;
- rejected rows `0`;
- duplicate source keys `0`;
- locality `00` rows present;
- `94110 -> CA/05/01112`;
- readiness gates report no failures.

### 3. Production-Style Local Smoke Plan

Generate the local smoke command plan without mutating the database.

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --dry-run-plan \
  --report-json /tmp/cms-pricing-local-smoke-plan.json
```

Expected result:

- `status=planned`;
- database password masked in report output;
- `proof_path=production_style_local_smoke`;
- plan includes geography readiness, RVU local load, `94110` smoke, and `66012`
  smoke;
- no command invokes `scripts/seed_post_rvu_load_local.py`.

### 4. Optional Full Local DB Smoke

Run this only against a local/dev database. It mutates local/dev state by
loading geography and RVU data, but must refuse remote database URLs.

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --report-json /tmp/cms-pricing-local-smoke.json
```

Expected result:

- consolidated report `status=ok`;
- default smoke resolves `94110 -> CA/05/01112`;
- special-state smoke resolves `66012 -> EK/00/05202`;
- pricing uses `release_id=rvu_2026_C`;
- pricing allowed amount is positive;
- trace refs include RVU, GPCI, and conversion-factor provenance;
- proof path remains `production_style_local_smoke`.

If local Postgres is unavailable, record this as a local environment blocker,
not a production-readiness pass.

## Scoped Reload Rules

Production reload instructions are not authorized by this runbook. When a
production execution runbook is approved later, it must scope replacement by:

- dataset ID `ZIP_LOCALITY`;
- source digest;
- effective window or explicit latest-active mode;
- overlapping ZIP/plus4 active keys only;
- RVU release ID and dataset snapshot IDs.

Any broad table truncation, unscoped delete, or remote write outside that
approved scope is a stop condition.

## Rollback Plan Requirements

A future production mutation runbook must include one of:

- restore from a database backup or snapshot taken immediately before the load;
- restore previous `dataset_snapshots` pointers and scoped geography/RVU rows;
- shadow-load into staging tables and promote only after validation.

If no rollback path is named and tested for the target environment, stop before
production mutation.

## Stop Conditions

Stop the preflight or production approval if:

- source digest, source URL, source files, release ID, or row counts differ from
  the governed source policy without review;
- strict mode unexpectedly passes or fails for reasons other than the documented
  source-window mismatch;
- latest-active mode is needed but absent from the command or report;
- rejected rows or duplicate source keys are nonzero;
- locality `00` is lost or normalized to `01`;
- `94110` does not resolve to `CA/05/01112`;
- `66012` does not resolve to `EK/00/05202`;
- smoke evidence depends on `scripts/seed_post_rvu_load_local.py`;
- RVU smoke does not select `rvu_2026_C`;
- trace refs omit RVU, GPCI, or conversion-factor provenance;
- any command targets a non-local database without explicit approval and a
  production execution runbook.

## Local Completion Benchmarks

The preflight is locally complete only when the evidence task records:

- strict geography readiness blocks `2026-07-01`;
- latest-active/open-ended readiness passes;
- pinned digest matches;
- source scan has zero rejects and zero duplicate source keys;
- locality `00` is preserved;
- `94110 -> CA/05/01112`;
- `66012 -> EK/00/05202`;
- `proof_path=production_style_local_smoke`;
- no seed-helper evidence;
- full local DB smoke, when run, selects `rvu_2026_C` and returns positive
  pricing.
