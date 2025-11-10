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

**Note:** The RVU ingestor (`cms_pricing/ingestion/ingestors/rvu_ingestor.py`) now automatically registers snapshots for all curated datasets (`rvu_items`, `gpci_indices`, `anescf`, `localitycounty`, `oppscap`) during the publish stage. Each successful RVU ingestion run will populate the `dataset_snapshots` table with SHA256 digests, effective dates, and manifest links, enabling MPFS and other downstream consumers to automatically discover and use RVU snapshots without manual intervention.

Verify that RVU snapshots are available before running MPFS ingestion:

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

Release identifiers returned by the snapshot check should include dataset-specific prefixes (`rvu_YYYY_S`, `gpci_YYYY_S`, etc.). If a snapshot still reports the shared RVU base ID, rerun the RVU ingestor before starting MPFS so that per-dataset release IDs are registered. Snapshot metadata stores manifest URLs for provenance; `DatasetSnapshotService` resolves the actual parquet path automatically. Use `python -m cms_pricing.ops.audit_snapshot_paths --dataset-id gpci_indices` to inspect manifest-only entries when troubleshooting.

Immediately after the DB check, run the parquet-path audit and repair routine:

```bash
python -m cms_pricing.ops.audit_snapshot_paths --dataset-id rvu_items --show-all
python -m cms_pricing.ops.audit_snapshot_paths --dataset-id gpci_indices --show-all
```

- **Expected:** `status=ok` for every snapshot row (path points at `data/ingestion/.../*.parquet`).  
- **If `status=missing_target` or the path still shows `/var/.../manifest.json`:** run the repair script, which writes a CSV backup under `artifacts/snapshot_repairs/` and rewrites the manifest URL to the current parquet:

```bash
python -m cms_pricing.ops.repair_snapshot_paths --dataset-id rvu_items --confirm
python -m cms_pricing.ops.repair_snapshot_paths --dataset-id gpci_indices --confirm
```

Re-run the audit after repairs; do not launch MPFS ingestion until both datasets report `status=ok`. Capture the audit + repair output in the ops ticket for traceability.

### 1.3 Conversion Factor Override Configuration

**YAML Config Service (Primary Method - Once Production-Ready)**

The MPFS config service (`cms_pricing/ingestion/services/mpfs_config_service.py`) supports per-release YAML configuration files for conversion factor overrides.

**Location**: `cf_overrides/{release_id}.yaml` or year-level files such as `cf_overrides/mpfs_2025.yaml` that contain `default:` and `releases:` entries. Release aliases support suffixes (`A`, `B`, `AR`, etc.) and quarter tokens (`2025_Q2`, `Q3`). The service merges matching entries so you can specify checksum in one block and override path in another.

**YAML Schema** (top-level or under `releases` entries):
```yaml
manual_override_path: "/path/to/cf_2025.xlsx"
expected_checksum: "abc123def456..."
```

**Sample Config File** (`cf_overrides/mpfs_2025_D.yaml`):
```yaml
manual_override_path: "/data/ingestion/mpfs/manual_overrides/cf_2025_ar.xlsx"
expected_checksum: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
```

**Behavior**:
- Config service checks for YAML file matching `{release_id}.yaml` in `cf_overrides/` directory
- If YAML exists and is valid, overrides are applied (release-specific sections override defaults)
- If YAML missing or malformed, falls back to CLI flags (see below)
- Config is cached in-memory for process lifetime (restart required for updates)
- When no override is supplied, the ingestor derives the conversion factor directly from the RVU snapshot and records `conversion_factor_strategy=derive_from_rvu`. Use overrides only when CMS publishes a separate CF artefact or when Ops needs to supply a manual file.

**CLI Flags (Fallback Until YAML Service Production-Ready)**

**IMPORTANT**: CLI flags remain the primary/fallback mechanism until YAML service is live and production-ready. Once YAML service is stable, CLI flags become override/emergency only.

To use CLI flags, pass arguments when invoking the ingestion script:
```bash
python scripts/run_mpfs_ingestion.py \
  --cf-override-path /path/to/cf_2025.xlsx \
  --cf-expected-checksum abc123def456
```

**Migration Path**:
1. **Current State**: CLI flags are primary method
2. **After YAML Service Lands**: YAML config becomes primary, CLI flags become override/emergency only
3. **Recommended**: Store override files under `data/ingestion/mpfs/manual_overrides/` with restricted permissions

**Error Handling**:
- Missing YAML → WARN logged, fallback to CLI flags
- Malformed YAML → Error raised with file path and line number, fallback to CLI flags
- Invalid override path → FileNotFoundError raised with clear message

### 1.4 Quarter & Release Targeting

- CMS publishes four planned RVU refreshes (A/B/C/D) plus correction notices (AR/BR/CR/DR). Always ingest the corresponding RVU release **before** running MPFS.
- The MPFS ingestor accepts `--quarter` (or explicit release suffix) and resolves it to the correct snapshot (`rvu_2025_B`, `gpci_2025_B`, etc.). If the requested release is missing, the run fails fast so Ops can ingest RVU first.
- When no quarter is supplied, MPFS uses the latest registered snapshots. For backfills or mid-year reruns, pass the explicit quarter to keep lineage deterministic.
- Command-line helpers:
  ```bash
  # List registered snapshots for reference
  python scripts/list_snapshots.py --dataset rvu_items
  # Target the July (C) release
  python scripts/run_mpfs_ingestion.py --year 2025 --quarter C
  ```
- Manifest metadata now includes `target_release_suffix`, `requested_release_param`, and the resolved RVU/GPCI release IDs. Capture those values in change tickets when promoting data downstream.

### 1.5 Snapshot Health Tooling

- **Audit (read-only):** `python -m cms_pricing.ops.audit_snapshot_paths --dataset-id gpci_indices --show-all`
- **Repair (mutating, requires --confirm):** `python -m cms_pricing.ops.repair_snapshot_paths --dataset-id gpci_indices --confirm`
- **Parity check (weekly + CI):** `python tools/check_snapshot_release_parity.py --pairs rvu_items:gpci_indices`
- Always run the audit script before the repair utility; the CSV backup emitted by the repair script must be attached to the ops ticket.

### 1.6 Render Low-Memory Quick Start

Render’s starter dynos have 2 GB of RAM. Before running this runbook from the Render shell, complete the “Low-Memory Snapshot Loading (Render)” checklist in `RENDER_DATA_LOADING_GUIDE.md`:

1. **Locate the latest manifest**  
   ```bash
   manifest=$(ls -t /var/data/ingestion/production/curated/cms_rvu/*/manifest.json | head -n1)
   export manifest
   ```
2. **Repair snapshot rows (if needed)** – run the helper script to point `rvu_items`/`gpci_indices` at the `/var/data/ingestion/production/...` parquet files.
3. **Set env limits** – e.g., `export MAX_MPFS_SNAPSHOT_ROWS=10000` and `export MPFS_SNAPSHOT_BATCH_ROWS=10000` so PyArrow streams manageable batches.
4. **Run the ingest command** – reuse the snippet from the guide; watch for `Row limiting applied...` and confirm `conversion_factor_strategy`.
5. **Spot-check CF output** – read `mpfs_cf_vintage.parquet`; if empty, raise the row limit and rerun.

Document the env vars you used in the change ticket. Following these steps prevents pod OOMs and eliminates manual DB edits on Render.

---

## 2. ConversionFactorFetcher Primer

The ingestor downloads annual conversion factor artifacts on demand using `ConversionFactorFetcher`. Override configuration is managed via YAML config service (primary) or CLI flags (fallback).

**Configuration Priority**:
1. **YAML Config Service** (if `cf_overrides/{release_id}.yaml` exists and is valid)
2. **CLI Flags** (fallback until YAML service is production-ready)

| Scenario | Behaviour | Operator Actions |
|----------|-----------|------------------|
| Cached file exists | Fetcher reuses artifact under `data/ingestion/mpfs/raw/{year}/`. Warns if checksum mismatch vs expected. | None (confirm log line `Reusing cached conversion factor artefact`). |
| No cache | Async download via `httpx`. Saves to year directory, computes SHA256, records manifest metadata. | Ensure outbound network path is allowed. |
| YAML config override | Config service loads `cf_overrides/{release_id}.yaml`, fetcher uses `manual_override_path` and `expected_checksum` from config. | Create/update YAML config file for release. |
| CLI flag override | Fetcher reads override path from CLI args, hashes content, records `file://` source. | Pass `--cf-override-path` and `--cf-expected-checksum` flags. |
| Checksum mismatch | Raises `ValueError` unless `warn_only=True` triggered by cache reuse. | Replace artifact or update expected checksum in config/CLI. |

**Operator Notes**

- Logs appear with context `{url, target}` for downloads; tail ingestion logs to monitor progress.
- Async fetch occurs during **Land stage**. Expect one download attempt per run.
- For mid-year CF adjustments, supply override file + checksum and update YAML config (or CLI flags as fallback) before executing.
- Config service logs: `Using YAML config override for conversion factor` when YAML config is used.
- Config service logs: `Config service error, falling back to CLI flags` when YAML config is missing or invalid.

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

> **Notes:**  
> - `--quarter` accepts CMS suffixes (`A`, `B`, `C`, `D`, `AR`, etc.) or `Q1`–`Q4`. If omitted, the latest registered snapshot is used.  
> - CLI flags currently wrap the ingestor entrypoint; configuration-service driven overrides will replace manual flags once implemented. Keep manual override path under version control until then.

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

### 4.2 Snapshot Verification

```bash
python - <<'PY'
from cms_pricing.database import SessionLocal
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService

session = SessionLocal()
svc = DatasetSnapshotService(session)

datasets = (
    "mpfs_payment_curated",
    "mpfs_rvu",
    "mpfs_gpci",
    "mpfs_cf_vintage",
    "mpfs_indicators_all",
    "mpfs_locality",
    "mpfs_link_keys",
)

missing = False
for dataset in datasets:
    snap = svc.get_latest_snapshot(dataset)
    if not snap:
        print(f"❌ Snapshot missing: {dataset}")
        missing = True
    else:
        print(f"✅ {dataset}: {snap.release_id} ({snap.effective_from} – {snap.effective_to})")

session.close()

if missing:
    raise SystemExit("Snapshot verification failed")
PY
```

```bash
jq '.metadata | {target_release_suffix, requested_release_param, snapshot_release_ids, conversion_factor_strategy}' "$MANIFEST"
```

- Expect `snapshot_release_ids.rvu_items` / `gpci_indices` to match the requested quarter (e.g., `rvu_2025_B`).
- `conversion_factor_strategy` should read `derive_from_rvu` unless a manual override/download was used (`download`).
- Include these metadata values in the ops ticket/run log for provenance.

### 4.3 Payment Spot Check

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

### 4.4 API Contract Check

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

**Weekly parity check:** run `python tools/check_snapshot_release_parity.py --pairs rvu_items:gpci_indices` to ensure RVU/GPCI release suffixes stay aligned before scheduling MPFS ingestion. Use `--allow-missing` if staging snapshots are mid-promotion.

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
