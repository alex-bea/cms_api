# Runbook: MPFS Ingestion (v1.0)

**Document Type:** Operational Runbook  
**Status:** Draft v1.0 (2025-11-04)  
**Owners:** Data Engineering (MPFS squad) + Platform Operations  
**Review Cadence:** Quarterly (aligned with MPFS release cadence)  
**Cross-References:**  
- `prds/DOC-master-catalog-prd-v1.0.md` (catalog entry §4)  
- `prds/PRD-mpfs-prd-v1.0.md` (product requirements)  
- `artifacts/mpfs_implementation_plan.md` (delivery plan & status)  
- `artifacts/ingestor_gap_analysis.md` (portfolio view)  

---

## Purpose

Provide an operational playbook for running and monitoring the Medicare Physician Fee Schedule (MPFS) ingestion pipeline. This runbook assumes the snapshot-based ingestor (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py`) is deployed and that RVU/GPCI snapshots already exist. It documents:

- Required pre-flight checks.
- How the asynchronous `ConversionFactorFetcher` behaves (cache + override workflow).
- Step-by-step execution commands.
- Verification, validation, and troubleshooting guidance.

---

## 1. Pre-Flight Checklist

### 1.1 Environment & Access

- Postgres instance reachable (ingestor uses `DATABASE_URL` / Alembic migrations current).
- File system access to `data/ingestion/mpfs` (verify ≥ 5 GB free for staging/curated parquet).
- Credentials/API keys set for downstream verification scripts (if any).

### 1.2 Snapshot Availability

```bash
python - <<'PY'
from cms_pricing.database import SessionLocal
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService

session = SessionLocal()
svc = DatasetSnapshotService(session)
for dataset in ("rvu_items", "gpci_indices"):
    snap = svc.get_latest_snapshot(dataset)
    if not snap:
        raise SystemExit(f"❌ Missing snapshot: {dataset}")
    print(f"✅ {dataset}: {snap.release_id} ({snap.effective_from} – {snap.effective_to})")
session.close()
PY
```

### 1.3 Conversion Factor Override Configuration (If Needed)

1. Preferred approach (planned): populate `config/ingestors/mpfs/cf_overrides/{release_id}.yaml`.
2. Interim approach (until config service lands): pass CLI args `--cf-override-path` / `--cf-expected-checksum` when invoking helper script (see §3.2).
3. Ensure override files are stored under `data/ingestion/mpfs/manual_overrides/` with restricted permissions.

---

## 2. ConversionFactorFetcher Primer

The ingestor downloads annual conversion factor artifacts on demand using `ConversionFactorFetcher`.

| Scenario | Behaviour | Operator Actions |
|----------|-----------|------------------|
| Cached file exists | Fetcher reuses artifact under `data/ingestion/mpfs/raw/{year}/`. Warns if checksum mismatch vs expected. | None (confirm log line `Reusing cached conversion factor artefact`). |
| No cache | Async download via `httpx`. Saves to year directory, computes SHA256, records manifest metadata. | Ensure outbound network path is allowed. |
| Manual override provided | Fetcher reads override path, hashes content, records `file://` source. | Maintain override file, include release_id in filename, update override config/CLI args. |
| Checksum mismatch | Raises `ValueError` unless `warn_only=True` triggered by cache reuse. | Replace artifact or update expected checksum. |

**Operator Notes**

- Logs appear with context `{url, target}` for downloads; tail ingestion logs to monitor progress.
- Async fetch occurs during **Land stage**. Expect one download attempt per run.
- For mid-year CF adjustments, supply override file + checksum and update config PR before executing.

---

## 3. Execution Steps

### 3.1 Prepare Working Directory

```bash
export MPFS_OUTPUT_DIR=${MPFS_OUTPUT_DIR:-./data/ingestion/mpfs}
mkdir -p "$MPFS_OUTPUT_DIR"/{raw,stage,curated,quarantine,logs}
```

Optional: archive previous run artefacts
```bash
timestamp=$(date +%Y%m%d_%H%M%S)
mv "$MPFS_OUTPUT_DIR"/raw "$MPFS_OUTPUT_DIR"/raw_$timestamp 2>/dev/null || true
mkdir -p "$MPFS_OUTPUT_DIR"/raw
```

### 3.2 Run Ingestor

Recommended helper script (`scripts/run_mpfs_ingestion.py`):

```bash
python scripts/run_mpfs_ingestion.py \
  --year 2025 \
  --quarter D \
  --output-dir "$MPFS_OUTPUT_DIR" \
  [--cf-override-path /path/to/cf.txt] \
  [--cf-expected-checksum <sha256>]
```

> **Note:** CLI flags currently wrap the ingestor entrypoint; configuration-service driven overrides will replace manual flags once implemented. Keep manual override path under version control until then.

### 3.3 Monitor Logs

```bash
tail -f "$MPFS_OUTPUT_DIR"/logs/mpfs_ingestion_*.log
```

Look for:
- `Starting MPFS land stage` / `Completed` with `downloads=1`.
- Conversion factor reuse/download lines.
- Validation summary (`issues=0` for critical).
- Publish stage summary with curated table counts.

---

## 4. Post-Run Verification

### 4.1 Manifest & Curated Outputs

```bash
MANIFEST=$(find "$MPFS_OUTPUT_DIR"/curated/mpfs -name manifest.json | sort | tail -1)
jq '.' "$MANIFEST"
```

Expected datasets:
- `mpfs_payment_curated`
- `mpfs_rvu`
- `mpfs_gpci`
- `mpfs_cf_vintage`
- `mpfs_indicators_all`
- `mpfs_locality`
- `mpfs_link_keys`

### 4.2 Payment Sanity Check

```bash
python - <<'PY'
import pandas as pd, json, sys, pathlib
manifest = pathlib.Path(sys.argv[1])
payment = pd.read_parquet(manifest.parent / "mpfs_payment_curated.parquet")
print(payment[["hcpcs_code","locality_id","payment_nonfacility","payment_facility"]]
      .head().to_markdown(index=False))
PY "$MANIFEST"
```

Optional: compare against PFREV sample (stored in `tests/fixtures/mpfs/pfrev_sample.json` when available).

### 4.3 API Contract Check

After publish completes and data is registered:
```bash
curl -H "X-Api-Key: $API_KEY" \
  "https://<host>/v1/mpfs?code=99213&zip=94110&year=2025" \
  | jq '.datasets_used'
```

Confirm entries include `mpfs_cf`, `mpfs_rvu`, `mpfs_gpci` with release_id/batch metadata.

---

## 5. Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| `Conversion factor artefact not found` | Missing override path or network block | Re-run with valid override path; verify CMS URL accessible. |
| `Checksum mismatch` | Override file out of sync | Recalculate SHA256; update CLI flag or config entry. |
| `ValueError: RVU snapshot not available` | RVU pipeline not run | Execute RVU ingestor or point to prior snapshot release. |
| `Publish stage raised FileNotFoundError` | Output dir perms / disk full | Free disk space; ensure ingestor has write permissions. |
| API missing `datasets_used` entries | Contract tests pending or publish not complete | Re-run API regression suite; inspect manifest metadata. |

---

## 6. Run Artifact Checklist

Record the following in `artifacts/mpfs_run_log.md` (or ops ticket):

- Run timestamp, operator, command used.
- Release ID / batch ID assigned by ingestor.
- Manifest path + curated row counts.
- Conversion factor source (`download` vs `override`).
- Validation warnings (if any).
- API verification sample (code/locality + key outputs).

---

## 7. References

- `cms_pricing/ingestion/ingestors/mpfs_ingestor.py`
- `cms_pricing/ingestion/services/conversion_factor_fetcher.py`
- `cms_pricing/ingestion/datasets/mpfs_builder.py`
- `artifacts/mpfs_implementation_plan.md` (Phase 7 progress)
- `prds/PRD-mpfs-prd-v1.0.md`
- `prds/REF-cms-pricing-source-map-prd-v1.0.md`

---

## 8. Change Log

| Version | Date | Summary | Author |
|---------|------|---------|--------|
| v1.0 | 2025-11-04 | Initial split from combined MPFS/OPPS runbook. Added ConversionFactorFetcher guidance and override workflow steps. | Codex Agent |
