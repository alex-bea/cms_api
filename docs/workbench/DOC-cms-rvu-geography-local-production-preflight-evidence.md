# CMS RVU/Geography Local Production Preflight Evidence

**Status:** Passed locally
**Updated:** 2026-06-11
**Parent runbook:** `docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`

## Summary

The local RVU/geography production preflight benchmarks passed against the
verified public CMS ZIP-locality source and the latest 2026 CMS RVU release.
No production database mutation was run.

The optional full local DB smoke initially exposed a local harness bug:
`scripts/post_rvu_load_api_smoke.py` imported the proof-path validator before
applying the explicit local database URL, which could bind the FastAPI app to a
pre-existing remote `DATABASE_URL` or `TEST_DATABASE_URL`. The preflight fixed
that by delaying the proof-path validator import until after
`configure_database_url()` and by making `configure_database_url()` override
both env vars, cached settings, and any imported session factory.

## Commands And Results

### Strict Geography Readiness

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /tmp/cms-geography-strict-readiness.json
```

Result: expected block.

- exit code: `1`
- report status: `blocked`
- failed gate: `valuation_date_coverage`
- effective window: `2025-10-01` through `2025-12-31`
- valuation date: `2026-07-01`
- source cleanliness gates still passed:
  - rows total: `1,118,970`
  - ZIP5 rows: `42,956`
  - ZIP9 rows: `1,076,014`
  - rejected rows: `0`
  - duplicate source keys: `0`
  - locality `00` rows: `39,476`
  - `94110 -> CA/05/01112`

### Latest-Active Geography Readiness

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --dry-run \
  --open-ended-latest \
  --require-valuation-date-coverage \
  --production-readiness-gates \
  --report-json /tmp/cms-geography-open-ended-readiness.json
```

Result: passed.

- exit code: `0`
- report status: `ok`
- `open_ended_latest=true`
- digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- failed gates: `[]`
- rejected rows: `0`
- duplicate source keys: `0`
- locality `00` rows: `39,476`
- `94110 -> CA/05/01112`

### Production-Style Local Smoke Plan

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --dry-run-plan \
  --report-json /tmp/cms-pricing-local-smoke-plan.json
```

Result: passed.

- report status: `planned`
- database password masked in report output
- `proof_path=production_style_local_smoke`
- planned steps:
  - `geography_readiness_load`
  - `rvu_local_load`
  - `post_rvu_smoke_94110`
  - `post_rvu_smoke_special_state_66012`
- no planned command invokes `scripts/seed_post_rvu_load_local.py`

### Full Local DB Smoke

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --report-json /tmp/cms-pricing-local-smoke.json
```

Result: passed after local DB override fix.

- report status: `ok`
- geography readiness:
  - action: `reuse_existing`
  - overlapping rows: `1,118,970`
  - same-digest rows: `1,118,970`
  - failed gates: `[]`
- RVU load:
  - selected release: `rvu_2026_C`
  - selected URL: `https://www.cms.gov/files/zip/rvu26c.zip`
  - DB rows:
    - `pprrvu`: `19,358`
    - `gpci`: `109`
    - `oppscap`: `15,260`
    - `anescf`: `109`
    - `localitycounty`: `57`
  - snapshot selection:
    - `rvu_items -> rvu_2026_C`
    - `gpci_indices -> gpci_2026_C`
- default smoke:
  - `94110 -> CA/05/01112`
  - `allowed_cents=11758`
  - `release_id=rvu_2026_C`
  - proof path accepted as `production_style_local_smoke`
- special-state smoke:
  - `66012 -> EK/00/05202`
  - `allowed_cents=8902`
  - `release_id=rvu_2026_C`
  - proof path accepted as `production_style_local_smoke`

## Completion Benchmarks

| Benchmark | Result |
|---|---|
| Strict readiness blocks `2026-07-01` | Pass |
| Latest-active readiness passes | Pass |
| Pinned digest matches | Pass |
| Zero rejected geography rows | Pass |
| Zero duplicate source keys | Pass |
| Locality `00` preserved | Pass |
| `94110 -> CA/05/01112` | Pass |
| `66012 -> EK/00/05202` | Pass |
| `proof_path=production_style_local_smoke` | Pass |
| No seed-helper evidence | Pass |
| Full local smoke selects `rvu_2026_C` | Pass |
| Full local smoke returns positive pricing | Pass |

## Follow-Up

Production mutation remains blocked until a production execution runbook names
the approved source package, effective-date mode, scoped reload strategy,
rollback plan, operator approval, and final production smoke checklist.
