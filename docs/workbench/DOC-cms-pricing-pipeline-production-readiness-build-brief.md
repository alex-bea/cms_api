# Codex Build Brief: CMS Pricing Pipeline Production Readiness Gates

**Status:** Implemented locally
**Updated:** 2026-06-11
**Parent epic:** `docs/workbench/DOC-cms-pricing-pipeline-production-readiness-epic-brief.md`
**Active tracker task:** `state/work/tasks/add-production-geography-validation-gates.yaml`

## Objective

Build validation gates that make the public CMS geography and RVU pipeline
close to production-ready without running a production load. The gates should
turn the successful local/dev checkpoint into repeatable pre-publish and
post-load evidence.

## Current State

The local/dev sequence works:

- `ZIP_LOCALITY` geography rows are present and reusable for the verified CMS
  package;
- `94110` resolves to `CA`, locality `05`, carrier `01112`;
- `rvu_2026_C` loads into local/dev DB with expected RVU/GPCI snapshots;
- post-RVU smoke returns `allowed_cents=11758` and RVU/GPCI/CF trace refs.

The current gap is that these facts are checkpoint evidence, not durable
validation gates. A production operator still needs code and runbook checks that
fail before mutation when source or loaded data is unsafe.

## Desired Behavior

When a production-style geography/RVU load is prepared, the system should:

1. Validate the CMS ZIP-locality source before publishing rows.
2. Refuse publication if source rejects, duplicate active keys, row-count
   regressions, `00` normalization, probe mismatch, or valuation-date coverage
   failures are present.
3. Validate loaded geography against RVU/GPCI expectations before post-load
   smoke is considered production-ready.
4. Refuse any workflow whose success depends on
   `scripts/seed_post_rvu_load_local.py`.
5. Emit machine-readable evidence that can be pasted into the production
   approval runbook.

## Scope

In scope:

- Add validation primitives or a validator module around
  `cms_pricing.ingestion.parsers.cms_geography.SourceStats`.
- Add configurable expected thresholds for the verified CMS ZIP-locality package:
  `rows_total=1,118,970`, `zip5_rows=42,956`, `zip9_rows=1,076,014`,
  `rejected_rows=0`, `duplicate_source_keys=0`, and
  `locality_00_rows=39,476`.
- Add probe validation for `94110 -> CA/05/01112`.
- Add valuation-date coverage validation with explicit open-ended/latest
  behavior.
- Add tests for pass and fail cases.
- Add or update smoke/reporting code so it can assert that the seed helper was
  not used as the proof path.
- Update workbench docs with gate names and evidence output.

Out of scope:

- Production database writes.
- New production dependencies.
- Full source-discovery rewrite.
- Public API response changes.
- Schema migration for `geography`.
- Replacing the local/dev loader with a deployment workflow.

## Relevant Areas To Inspect

- `cms_pricing/ingestion/parsers/cms_geography.py`
- `cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`
- `cms_pricing/ingestion/validators/cms_zip_locality_validator.py`
- `cms_pricing/engines/mpfs.py`
- `scripts/load_cms_geography_local.py`
- `scripts/post_rvu_load_api_smoke.py`
- `tests/scripts/test_load_cms_geography_local.py`
- `tests/ingestors/test_cms_zip_locality_production_ingester.py`
- `tests/services/test_mpfs_component_pricing.py`
- `tests/geography/test_geography_resolver.py`

## Requirements

1. Source package gate.
   - Acceptance: a test proves a valid `SourceStats` object passes with the
     checkpoint metrics and fails when rejects or duplicate active keys are
     nonzero.

2. Row-count and regression gate.
   - Acceptance: tests prove the gate accepts the checkpoint row counts and
     fails when ZIP5, ZIP9, total rows, or locality `00` counts fall below
     configured minimums.

3. Probe and source-string preservation gate.
   - Acceptance: tests prove `94110 -> CA/05/01112` passes and mismatched
     state, locality, or carrier fails; locality `00` remains a required source
     value, not a normalization target.

4. Valuation-date coverage gate.
   - Acceptance: tests prove strict quarter dating blocks `2026-07-01` for the
     current `2025Q4` package, while explicit open-ended/latest mode passes.

5. GPCI join readiness gate.
   - Acceptance: a focused test or validation helper proves active geography
     state/locality pairs are joinable to loaded GPCI rows directly or through
     the governed special-state mapping, and reports classified misses.

6. Seed-helper refusal.
   - Acceptance: smoke/runbook validation names the accepted proof path and
     fails or warns clearly when the only evidence is the one-row seed helper.

## Constraints

- Keep the implementation minimal.
- Reuse the shared geography parser and existing smoke command before adding
  new orchestration.
- Do not add a new production dependency.
- Do not change production publish targets in this slice.
- Do not perform production DB writes.
- Preserve existing behavior outside the validation path.
- Keep thresholds configurable so future CMS packages can be validated without
  hard-coding the current package forever.

## Do Not Touch

- Production database configuration.
- Secrets or environment-specific deploy config.
- MPFS pricing math.
- Public API schemas.
- Existing generated CMS data artifacts unless the command being run produces
  local/dev evidence intentionally.

## Testing Policy

Run the smallest relevant tests first:

```bash
.venv/bin/python -m pytest tests/scripts/test_load_cms_geography_local.py -q
.venv/bin/python -m pytest tests/ingestors/test_cms_zip_locality_production_ingester.py -q
.venv/bin/python -m pytest tests/geography/test_geography_resolver.py -q
.venv/bin/python -m pytest tests/services/test_mpfs_component_pricing.py -q
```

Then run:

```bash
.venv/bin/python tools/work_tracker.py check
.venv/bin/python tools/work_tracker.py check-views
git diff --check
```

Do not run production mutation commands. Local/dev DB checks are allowed only
when the command explicitly targets `localhost` or Docker Compose local
services.

## Requested Codex Output

Do not implement production mutation. First produce or update:

1. Validation gate implementation plan with files likely to change.
2. Tracker task update for `add-production-geography-validation-gates`.
3. QA checklist with automated checks and manual production-preflight checks.
4. Risks, assumptions, and open questions only if blocking.

After approval, implement the validation-gates slice, keep the diff minimal,
run the focused checks, and report commands and results.

## Implementation Result

This slice added `cms_pricing.ingestion.validators.cms_geography_readiness`
with reusable gates for:

- source package cleanliness: valid rows, zero rejects, zero duplicate active
  keys;
- configured row-count floors for the verified 2025Q4 CMS ZIP-locality package;
- locality `00` preservation;
- probe ZIP `94110 -> CA/05/01112`;
- valuation-date coverage with explicit strict vs open-ended/latest behavior;
- active geography state/locality join readiness against GPCI state/locality
  rows using the governed MPFS special-state mapping;
- refusal of post-RVU smoke evidence that depends on
  `scripts/seed_post_rvu_load_local.py`.

`scripts/load_cms_geography_local.py` now exposes an opt-in
`--production-readiness-gates` flag that appends a machine-readable
`production_readiness_gates` section and blocks when any gate fails.

`scripts/post_rvu_load_api_smoke.py` now emits a `proof_path` section and
refuses seed-helper proof paths.

## Validation Evidence

Passing:

```bash
.venv/bin/python -m pytest tests/ingestion/test_cms_geography_readiness.py -q
.venv/bin/python -m pytest tests/scripts/test_load_cms_geography_local.py -q
.venv/bin/python scripts/load_cms_geography_local.py --dry-run --open-ended-latest --require-valuation-date-coverage --production-readiness-gates --report-json /tmp/cms-geography-readiness-report.json
```

The real-source dry run returned `status=ok`, `failed_gates=[]`,
`rows_total=1,118,970`, `zip5_rows=42,956`, `zip9_rows=1,076,014`,
`rejected_rows=0`, `duplicate_source_keys=0`, `locality_00_rows=39,476`, and
probe `94110 -> CA/05/01112`.

DB-backed isolation rerun:

```bash
.venv/bin/python -m pytest tests/geography/test_geography_resolver.py tests/services/test_mpfs_component_pricing.py -q
```

This now passes with 51 tests after tightening local DB test isolation. The
shared `test_db_session` rolls back committed fixture rows after each test, and
the resolver fixture masks only its known fixture ZIPs inside that rollback
transaction so real CMS geography rows do not contaminate resolver assertions.

## Production-Style Local Smoke Runner

This slice added `scripts/run_cms_pricing_local_smoke.py` as the local/dev
harness command for repeatable evidence. It plans or runs the sequence in the
same order the production path will need:

1. CMS ZIP-locality load with production readiness gates.
2. Latest CMS RVU/GPCI local load.
3. Post-RVU API smoke for default `94110 -> CA/05/01112`.
4. Post-RVU API smoke for special source-state `66012 -> EK/00/05202`.

The dry-run mode produces a consolidated report without mutating the database
and masks database credentials:

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing --dry-run-plan --report-json /tmp/cms-pricing-local-smoke-plan.json
```

Validation:

```bash
.venv/bin/python -m pytest tests/scripts/test_run_cms_pricing_local_smoke.py -q
.venv/bin/python scripts/run_cms_pricing_local_smoke.py --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing --dry-run-plan --report-json /tmp/cms-pricing-local-smoke-plan.json
```

## Source Discovery and Effective-Date Policy

The governed CMS ZIP-locality policy now lives in
`docs/workbench/DOC-cms-zip-locality-source-discovery-effective-date-policy.md`.
It pins the current verified CMS package, digest, source files, strict
`2025Q4` effective window, explicit latest-active/open-ended exception, and
stop conditions for the production preflight runbook.
