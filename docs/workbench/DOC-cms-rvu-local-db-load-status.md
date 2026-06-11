# CMS RVU Local DB Load Status

**Status:** Current
**Updated:** 2026-06-11
**Tracker link:** `state/work/tasks/load-latest-cms-rvu-local-db.yaml`

## Completed

The latest live CMS RVU release was loaded through the real local/dev database
path, not the no-DB validation harness.

| Check | Result |
|---|---|
| Selected release | `rvu_2026_C` |
| CMS effective date | `2026-07-01` |
| Latest run date | `2026-06-11` |
| Snapshot date behavior | `dataset_snapshots.effective_from` uses the CMS effective date, not the run date |
| Snapshot selection | `rvu_items -> rvu_2026_C`, `gpci_indices -> gpci_2026_C` for valuation date `2026-07-01` |
| Commit | `fe76d98 Add local CMS RVU DB load command` |

## Checkpoint: 2026-06-11 Seedless Local Sequence

After PR #450 merged, the local/dev RVU pipeline was rerun without the
one-row geography seed helper.

### Geography prerequisite

Command:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --replace-existing \
  --open-ended-latest \
  --require-valuation-date-coverage
```

Result:

- status: `ok`
- action: `reuse_existing`
- runtime `geography` rows: `1,118,970`
- ZIP5 rows: `42,956`
- ZIP9 rows: `1,076,014`
- rejected rows: `0`
- duplicate source keys: `0`
- dataset digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- `ZIP_LOCALITY` release: `zip_locality_2025_Q4`
- effective window: `2025-10-01` through open-ended/latest
- valuation date `2026-07-01`: covered
- probe: `94110 -> CA`, locality `05`, carrier `01112`

### RVU local DB load

Command:

```bash
docker compose -p cms-api-fix run --rm api \
  python scripts/load_latest_cms_rvu_local.py \
  --start-year 2026 \
  --end-year 2026 \
  --release latest \
  --output-dir data/ingestion/local/rvu \
  --report-json data/ingestion/local/reports/cms_rvu_local_load_latest.json
```

Result:

- status: `success`
- selected release: `rvu_2026_C`
- selected URL: `https://www.cms.gov/files/zip/rvu26c.zip`
- curated rows:
  - `pprrvu`: `19,358`
  - `gpci`: `109`
  - `oppscap`: `15,260`
  - `anescf`: `109`
  - `localitycounty`: `117`
- DB rows:
  - `pprrvu`: `19,358`
  - `gpci`: `109`
  - `oppscap`: `15,260`
  - `anescf`: `109`
  - `localitycounty`: `57`
- snapshot selection for `2026-07-01`:
  - `rvu_items -> rvu_2026_C`
  - `gpci_indices -> gpci_2026_C`
- report:
  `data/ingestion/local/reports/cms_rvu_local_load_latest.json`

### Post-load smoke

Command:

```bash
.venv/bin/python scripts/post_rvu_load_api_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
```

Result:

- status: `ok`
- geography: `94110`, `CA`, locality `05`, carrier `01112`
- pricing: `allowed_cents=11758`, `release_id=rvu_2026_C`
- trace refs include:
  - `RVU:release:rvu_2026_C`
  - `GPCI:release:gpci_2026_C`
  - `CF:release:rvu_2026_C`
  - `CF:source:rvu_items.conversion_factor`

This checkpoint proves the local/dev pipeline can run from real public CMS
geography data plus live CMS RVU data without depending on
`scripts/seed_post_rvu_load_local.py`.

## Checkpoint: 2026-06-11 Production Preflight Local Evidence

The RVU/geography production preflight is documented in
`docs/workbench/DOC-cms-pricing-production-preflight-runbook.md`, with local
evidence recorded in
`docs/workbench/DOC-cms-rvu-geography-local-production-preflight-evidence.md`.

Result: local preflight passed.

- strict geography readiness dry run blocked `2026-07-01` as expected for the
  pinned `2025Q4` source window;
- explicit latest-active/open-ended readiness passed with digest
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`;
- source scan reported zero rejected rows and zero duplicate source keys;
- locality `00` remained present with `39,476` rows;
- production-style smoke plan used `proof_path=production_style_local_smoke`
  and did not invoke `scripts/seed_post_rvu_load_local.py`;
- full local DB smoke passed:
  - `94110 -> CA/05/01112`, `allowed_cents=11758`,
    `release_id=rvu_2026_C`;
  - `66012 -> EK/00/05202`, `allowed_cents=8902`,
    `release_id=rvu_2026_C`.

Production mutation remains blocked until an execution runbook names the
approved source, effective-date mode, scoped reload strategy, rollback plan,
operator approval, and final production smoke checklist.

## Curated Parquet Evidence

| Dataset | Rows |
|---|---:|
| `pprrvu` | 19358 |
| `gpci` | 109 |
| `oppscap` | 15260 |
| `anescf` | 109 |
| `localitycounty` | 117 |

## Database Evidence

| Table family | Rows loaded for release |
|---|---:|
| `pprrvu` | 19358 |
| `gpci` | 109 |
| `oppscap` | 15260 |
| `anescf` | 109 |
| `localitycounty` | 57 |

The `localitycounty` DB count is lower than the curated parquet count because the
current loader normalizes the source rows into the existing DB shape. It is
positive and no longer collapses rows by MAC only. Exact locality row parity
should be treated as follow-up if the downstream API needs county-level
selection rather than locality-level selection.

## Repeatable Command

```bash
docker compose -p cms-api-fix run --rm api \
  python scripts/load_latest_cms_rvu_local.py \
  --start-year 2026 \
  --end-year 2026 \
  --release latest \
  --output-dir data/ingestion/local/rvu \
  --report-json data/ingestion/local/reports/cms_rvu_local_load_latest.json
```

## Tests Run

```bash
.venv/bin/pytest \
  tests/scripts/test_load_latest_cms_rvu_local.py \
  tests/ingestors/test_rvu_loader_aliases.py \
  tests/ingestion/test_rvu_real_cms_ingest_validation.py \
  tests/ingestion/test_live_cms_validation_harness.py \
  -q
```

Result: `24 passed, 1 skipped`.

## Next

The loaded RVU and GPCI data are now usable from the MPFS pricing workflow when
`dataset_snapshots` rows are present. The local API smoke below ran against the
rebuilt Docker Compose API:

```bash
curl -H 'X-API-Key: dev-key-123' \
  'http://127.0.0.1:8000/pricing/codes/price?zip=94110&code=99213&setting=MPFS&year=2026&valuation_date=2026-07-01&pos=11'
```

Result: `200 OK`, `allowed_cents=10061`, `release_id=rvu_2026_C`, and trace refs
included `RVU:release:rvu_2026_C`, `GPCI:release:gpci_2026_C`,
`CF:release:rvu_2026_C`, and `CF:source:rvu_items.conversion_factor`.

## Geography Prerequisite

The preferred local/dev geography prerequisite is now the real CMS geography
load:

```bash
.venv/bin/python scripts/load_cms_geography_local.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --replace-existing \
  --open-ended-latest \
  --require-valuation-date-coverage
```

This replaces the previous one-row ZIP seed with the real public CMS
ZIP-locality package:

- source files: `ZIP5_OCT2025.txt` and `ZIP9_OCT2025.txt`
- rows loaded: `1,118,970`
- ZIP5 rows: `42,956`
- ZIP9 rows: `1,076,014`
- dataset digest:
  `b14c414de73256ac9594d7cb0a58a75214ba04f4fe043468ffda507c3dd75c2e`
- `94110` resolves to `CA` locality `05`, carrier `01112`

The command uses `--open-ended-latest` because the current verified public CMS
ZIP-locality package is `2025Q4`; this makes the latest public geography source
active for the 2026 RVU smoke date until a newer ZIP-locality package is
verified.

## Production-Style Local Smoke Runner

Use this local/dev runner when the harness needs one repeatable command plan for
geography readiness, RVU load, and post-load API smoke evidence:

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing \
  --dry-run-plan
```

The dry-run plan writes a consolidated report without mutating the database. It
prints the exact local/dev sequence and masks the database password in the
evidence report.

To execute the local/dev sequence:

```bash
.venv/bin/python scripts/run_cms_pricing_local_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
```

The runner intentionally refuses remote database URLs through the same local DB
guard used by the other local CMS scripts. It runs:

- `scripts/load_cms_geography_local.py` with `--production-readiness-gates`,
  `--open-ended-latest`, and valuation-date coverage checks;
- `scripts/load_latest_cms_rvu_local.py` for the selected RVU release window;
- `scripts/post_rvu_load_api_smoke.py` for default `94110 -> CA/05/01112`;
- `scripts/post_rvu_load_api_smoke.py` for special source-state ZIP `66012`
  with expected `EK/00/05202`.

The expected proof path is `production_style_local_smoke`. The runner does not
call `scripts/seed_post_rvu_load_local.py`; seed-helper proof remains rejected
by the post-load smoke validation.

The old local/dev seed command remains a narrow repair fallback:

```bash
.venv/bin/python scripts/seed_post_rvu_load_local.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
```

It seeds the public ZIP-locality row for `94110` when an active matching row is
absent:

- `zip5=94110`
- `state=CA`
- `locality_id=05`
- `carrier=01112`
- `effective_from=2026-01-01`
- `effective_to=2026-12-31`

It also registers the active `rvu_2026_C` and `gpci_2026_C`
`dataset_snapshots` rows when missing, using the already-loaded RVU/GPCI table
counts and `effective_from=2026-07-01` so the pricing engine selects the
RVU-backed path.

The repeatable smoke command passes after the real geography load:

```bash
.venv/bin/python scripts/post_rvu_load_api_smoke.py \
  --database-url postgresql://cms_user:cms_password@localhost:5432/cms_pricing
```

Result: `status=ok`, geography `locality_id=05`, `state=CA`,
`carrier=01112`, pricing `allowed_cents=11758`, `release_id=rvu_2026_C`, and
trace refs include `RVU:release:rvu_2026_C`,
`GPCI:release:gpci_2026_C`, `CF:release:rvu_2026_C`, and
`CF:source:rvu_items.conversion_factor`.
