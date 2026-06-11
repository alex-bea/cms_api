# CMS RVU Local DB Load Status

**Status:** Current
**Updated:** 2026-06-10
**Tracker link:** `state/work/tasks/load-latest-cms-rvu-local-db.yaml`

## Completed

The latest live CMS RVU release was loaded through the real local/dev database
path, not the no-DB validation harness.

| Check | Result |
|---|---|
| Selected release | `rvu_2026_C` |
| CMS effective date | `2026-07-01` |
| Run date | `2026-06-10` |
| Snapshot date behavior | `dataset_snapshots.effective_from` uses the CMS effective date, not the run date |
| Snapshot selection | `rvu_items -> rvu_2026_C`, `gpci_indices -> gpci_2026_C` for valuation date `2026-07-01` |
| Commit | `fe76d98 Add local CMS RVU DB load command` |

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

The geography resolver still falls back to benchmark locality `01` for ZIP
`94110` in the local DB, so locality normalization remains the next pricing
correctness risk to resolve before treating ZIP-specific California pricing as
validated.
