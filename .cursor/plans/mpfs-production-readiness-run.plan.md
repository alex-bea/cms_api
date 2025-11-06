<!-- c9490d92-cff7-41d8-9f52-51c32bf89812 4b3337d7-a59c-4016-9bc1-3042e783fb4b -->
# MPFS Production Readiness Run Plan (MVP)

Execute Phase 8 production readiness tasks with MVP-focused approach for "ship today" execution.

## Quick Fixes Before You Run

### 1. Async Call Fix

The direct Python snippet uses `await` in a sync block. Wrap it:

```python
import asyncio
asyncio.run(ingestor.ingest(2025))
```

### 2. Database Requirement

**Important:** The pipeline depends on the Render Postgres schema and migrations. Render DB access (or another fully migrated Postgres instance) is required. SQLite is not supported.

### 3. CF Override

Conversion factor overrides are configured via:

- **YAML files:** `cf_overrides/{release_id}.yaml` with `manual_override_path` and `expected_checksum` keys
- **CLI flags:** `--cf-override-path` and `--cf-expected-checksum` (if supported by the ingestion entry point)

See `prds/RUN-mpfs-ingestion-v1.0.md` §1.3 for details.

### 4. Snapshot Names Fallback

The check uses `rvu_items` / `gpci_indices`. If your registry uses slightly different keys, use `list_snapshots(dataset_id=...)` to list snapshots for a specific dataset.

**Note:** RVU/GPCI snapshots must already exist; otherwise operators should run RVU ingestion first.

---

## Super-MVP "Happy Path" (Copy/Paste)

### 1. Connectivity

```bash
python - <<'PY'
from cms_pricing.database import SessionLocal
s=SessionLocal(); s.execute('SELECT 1'); print('✅ DB ok'); s.close()
PY
```

**Note:** Render Postgres access (or another fully migrated Postgres instance) is required. The pipeline depends on the Render Postgres schema and migrations.

### 2. Snapshot Presence (with graceful fallback)

```bash
python - <<'PY'
from cms_pricing.database import SessionLocal
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService

s=SessionLocal()
svc=DatasetSnapshotService(s)

def ck(name):
    snap=svc.get_latest_snapshot(name)
    print(f"{'✅' if snap else '❌'} {name}:", getattr(snap,'release_id',None))
    return snap

for ds in ("rvu_items","gpci_indices"):
    ck(ds)

# Optional: list recent snapshots to debug naming differences
print("\nAvailable snapshots for rvu_items:")
for snap in svc.list_snapshots(dataset_id="rvu_items", limit=5):
    print(f"• {snap.release_id} ({snap.effective_from} – {snap.effective_to})")

print("\nAvailable snapshots for gpci_indices:")
for snap in svc.list_snapshots(dataset_id="gpci_indices", limit=5):
    print(f"• {snap.release_id} ({snap.effective_from} – {snap.effective_to})")

s.close()
PY
```

**Note:** RVU/GPCI snapshots must already exist. If missing, run RVU ingestion first to create them.

### 3. Run Ingestion

**Recommended: Direct Python with async (canonical path)**

```bash
python - <<'PY'
import asyncio
from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor
from cms_pricing.database import SessionLocal

s=SessionLocal()
ing=MPFSIngestor(output_dir="./data/ingestion/mpfs", db_session=s)

async def go(): 
    # Quarter is optional; if None, ingestor will pick from latest snapshot
    result = await ing.ingest(2025, quarter="D")  # or None to auto-detect
    print("✅ MPFS ingest complete")
    return result

result = asyncio.run(go())
s.close()
PY
```

**Note:** Make quarter a parameter (D is great, but let the run pick A/B/C/D from latest snapshot to avoid a hard-coded mismatch). Pass `quarter=None` to auto-detect from latest snapshot.

### 4. Row Counts from Parquet Files

```bash
python - <<'PY'
import pandas as pd
from pathlib import Path

curated_dir = Path("data/ingestion/mpfs/curated")
for release_dir in sorted(curated_dir.glob("mpfs_*"), reverse=True):
    print(f"\n📁 {release_dir.name}:")
    for parquet_file in release_dir.glob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            print(f"  ✅ {parquet_file.name}: {len(df):,} rows")
        except Exception as e:
            print(f"  ❌ {parquet_file.name}: {e}")
    break  # Only check latest release
PY
```

**Note:** Row counts live in the Parquet files and manifest, not in relational tables. The curated output is stored as Parquet files in `data/ingestion/mpfs/curated/{release_id}/`.

### 5. API Smoke (only if server is up)

```bash
# Start if needed
# uvicorn cms_pricing.api.main:app --port 8000 --host 0.0.0.0

curl -s "http://localhost:8000/pricing/codes/price?zip=90210&code=99213&setting=office&year=2025" \
  -H "X-API-Key: $API_KEY" | jq '{datasets_used:.datasets_used, sample:.price}'
```

**Note:** Requires access to the configured database.

---

## Answers to Open Items

### A) Manual Override & Checksum Injection

- **Option 1:** YAML files in `cf_overrides/{release_id}.yaml` with `manual_override_path` and `expected_checksum` keys (via `MPFSConfigService`)
- **Option 2:** CLI flags `--cf-override-path` and `--cf-expected-checksum` (if supported by ingestion entry point)
- **Option 3:** Extend discovery `manifest.json` with `manual_overrides` → future-proof, auditable.

**Recommendation:** Use YAML config service (Option 1) for production; CLI flags as fallback.

### B) Conversion Factor Handling (physician vs anesthesia, mid-year)

- **Option 1 (MVP):** Parse physician CF only, store anesthesia as NULL if absent.
- **Option 2:** Parse both columns when present (`physician_cf`, `anesthesia_cf`).
- **Option 3:** Multi-row per year with `cf_type ∈ {physician, anesthesia, mid_year}`.

**Recommendation:** Option 2 now (tiny delta, useful later).

### C) API Contract Test for datasets_used

- **Option 1 (MVP):** Minimal test asserting `mpfs_cf`/`mpfs_rvu`/`mpfs_gpci` are present.
- **Option 2:** Golden sample with ±$0.01 parity and lineage verification.
- **Option 3:** JSON Schema for `datasets_used` across endpoints.

**Recommendation:** Option 1 today; add Option 2 next sprint.

### D) Runbook/Operator Clarity for Async Fetcher & CF Cache

- **Option 1 (MVP):** Add a small "CF artefact lifecycle" section (cache dir, when it downloads, how override wins).
- **Option 2:** Include example commands for async run and reading cache/logs.
- **Option 3:** A short "common failures" table (DNS, 403, checksum mismatch).

**Recommendation:** Option 1 + 2 now; the table can wait.

---

## Environment Setup

### 1.1 Database Access

- **Requirement**: Access to Render Postgres host `dpg-d3rtb40dl3ps73940evg-a.oregon-postgres.render.com` (or another fully migrated Postgres instance)
- **Important:** The pipeline depends on the Render Postgres schema and migrations. SQLite is not supported.
- **Options:**
        - Run from local machine with VPN/network access configured
        - Request temporary network approval for sandbox environment
        - Use alternative environment with database connectivity (dev/staging)

### 1.2 Environment Variables

- Ensure `DATABASE_URL` is set and points to Render Postgres (or fully migrated Postgres instance)
- **CF Override:** Configure via YAML files in `cf_overrides/{release_id}.yaml` or CLI flags (see §A above)

---

## Pre-Flight Snapshot Check

### 2.1 Run Snapshot Availability Check (with fallback)

Execute snapshot check with graceful fallback for naming mismatches:

```bash
python - <<'PY'
from cms_pricing.database import SessionLocal
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService

s=SessionLocal()
svc=DatasetSnapshotService(s)

def ck(name):
    snap=svc.get_latest_snapshot(name)
    print(f"{'✅' if snap else '❌'} {name}:", getattr(snap,'release_id',None))
    return snap

for ds in ("rvu_items","gpci_indices"):
    ck(ds)

# Optional: list recent snapshots to debug naming differences
print("\nAvailable snapshots for rvu_items:")
for snap in svc.list_snapshots(dataset_id="rvu_items", limit=5):
    print(f"• {snap.release_id} ({snap.effective_from} – {snap.effective_to})")

print("\nAvailable snapshots for gpci_indices:")
for snap in svc.list_snapshots(dataset_id="gpci_indices", limit=5):
    print(f"• {snap.release_id} ({snap.effective_from} – {snap.effective_to})")

s.close()
PY
```

**Success Criteria:**

- Both `rvu_items` and `gpci_indices` snapshots exist (or equivalent names found)
- Latest snapshots have valid `release_id`, `effective_from`, `effective_to`
- Capture output for documentation

**Note:** RVU/GPCI snapshots must already exist. If missing, run RVU ingestion first to create them.

### 2.2 Verify Snapshot Metadata

- Check snapshot paths are accessible
- Verify snapshot digests are populated
- Confirm effective date ranges are valid for current year
- **Note:** Let the run pick quarter (A/B/C/D) from latest snapshot to avoid hard-coded mismatch

---

## Execute MPFS Ingestion

### 3.1 Run MPFS Ingestion

**Recommended: Direct Python with async (canonical path)**

```bash
python - <<'PY'
import asyncio
from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor
from cms_pricing.database import SessionLocal

s=SessionLocal()
ing=MPFSIngestor(output_dir="./data/ingestion/mpfs", db_session=s)

async def go(): 
    # Quarter is optional; if None, ingestor will pick from latest snapshot
    result = await ing.ingest(2025, quarter="D")  # or None to auto-detect
    print("✅ MPFS ingest complete")
    return result

result = asyncio.run(go())
s.close()
PY
```

**Note:** Don't depend on `scripts/run_mpfs_ingestion.py` existing. Make quarter a parameter from latest snapshot (pass `quarter=None` to auto-detect).

### 3.2 Monitor Ingestion

- Watch logs for successful completion
- Verify no critical validation errors
- Confirm all curated Parquet files generated in `data/ingestion/mpfs/curated/{release_id}/`

### 3.3 Capture Artifacts

After successful ingestion, capture to single evidence folder:

```bash
# Create evidence folder
EVIDENCE_DIR="artifacts/mpfs_production_run_$(date +%Y-%m-%d)"
mkdir -p "$EVIDENCE_DIR"

# Copy manifest
find data/ingestion/mpfs/curated -name manifest.json | sort | tail -1 | xargs cp -t "$EVIDENCE_DIR/"

# Save row counts from Parquet files
python - <<'PY' > "$EVIDENCE_DIR/row_counts.txt"
import pandas as pd
from pathlib import Path

curated_dir = Path("data/ingestion/mpfs/curated")
for release_dir in sorted(curated_dir.glob("mpfs_*"), reverse=True):
    print(f"\n{release_dir.name}:")
    for parquet_file in release_dir.glob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            print(f"{parquet_file.name}: {len(df):,} rows")
        except Exception as e:
            print(f"{parquet_file.name}: ERROR - {e}")
    break  # Only check latest release
PY

# Save API response (if server is up)
curl -s "http://localhost:8000/pricing/codes/price?zip=90210&code=99213&setting=office&year=2025" \
  -H "X-API-Key: $API_KEY" | jq '.' > "$EVIDENCE_DIR/api_response.json" 2>/dev/null || echo "API not available"
```

**Evidence Folder Structure:**

```
artifacts/mpfs_production_run_2025-11-06/
├── manifest.json
├── row_counts.txt
├── api_response.json (if available)
└── logs/ (if captured)
```

---

## Execute MPFS Test Suite

### 4.1 Run Full MPFS Test Suite

Once ingestion completes successfully:

```bash
# Run all MPFS-related tests
pytest tests/ingestors/test_mpfs_ingestor_e2e.py -v
pytest tests/ingestion/services/test_mpfs_config_service.py -v
pytest tests/ingestion/datasets/test_mpfs_builder.py -v
pytest tests/services/test_pricing_provenance.py::test_mpfs_engine_returns_code_pricing_item -v
pytest tests/api/test_golden.py -k mpfs -v
```

**Note:** Requires access to the configured database.

**Capture Test Results:**

```bash
EVIDENCE_DIR="artifacts/mpfs_production_run_$(date +%Y-%m-%d)"
pytest tests/ingestion/services/test_mpfs_config_service.py \
  tests/ingestion/datasets/test_mpfs_builder.py \
  -v --tb=short > "$EVIDENCE_DIR/test_results.txt" 2>&1
```

### 4.2 Contract Tests Verification (MVP)

**Option 1 (MVP):** Minimal test asserting `mpfs_cf`/`mpfs_rvu`/`mpfs_gpci` are present:

```python
def test_mpfs_datasets_used_present():
    """Minimal contract test: datasets_used includes MPFS datasets."""
    response = client.get("/pricing/codes/price?zip=90210&code=99213&setting=office&year=2025")
    assert response.status_code == 200
    data = response.json()
    datasets_used = data.get("datasets_used", [])
    assert "mpfs_cf" in datasets_used
    assert "mpfs_rvu" in datasets_used
    assert "mpfs_gpci" in datasets_used
```

**Verify:**

- `/pricing/codes/price` endpoint returns `datasets_used` with `mpfs_cf`, `mpfs_rvu`, `mpfs_gpci`
- Provenance metadata includes `release_id`, `batch_id`, `dataset_digest`

---

## Document Results

### 5.1 Update Readiness Plan

Update `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` with:

**Section 5.1 (Data Quality & Provenance)** - Add run evidence:

```markdown
- ✅ **COMPLETE:** MPFS production ingestion run executed on [DATE] for vintage 2025[D]
  - Evidence folder: `artifacts/mpfs_production_run_[DATE]/`
  - Curated output directory: `data/ingestion/mpfs/curated/{release_id}/`
  - Curated table row counts (from Parquet files):
    - `mpfs_payment_curated`: [ROW_COUNT]
    - `mpfs_rvu`: [ROW_COUNT]
    - `mpfs_gpci`: [ROW_COUNT]
    - `mpfs_cf_vintage`: [ROW_COUNT]
  - Sample `datasets_used` from API: [JSON_SAMPLE]
  - Test results: See `artifacts/mpfs_production_run_[DATE]/test_results.txt`
```

### 5.2 Evidence Files

All evidence captured to single folder:

- `artifacts/mpfs_production_run_[DATE]/manifest.json`
- `artifacts/mpfs_production_run_[DATE]/row_counts.txt` (from Parquet inspection)
- `artifacts/mpfs_production_run_[DATE]/api_response.json` (if available)
- `artifacts/mpfs_production_run_[DATE]/test_results.txt`

### 5.3 Update Implementation Plan

Mark Phase 8.1 complete in `artifacts/mpfs_implementation_plan.md`:

- Update Phase 8.1 status to complete
- Document run date and key metrics
- Note any issues encountered
- Reference evidence folder path and curated output directory

---

## Troubleshooting

### 6.1 Database Connection Issues

- Verify `DATABASE_URL` environment variable is set correctly
- **Important:** Render Postgres (or fully migrated Postgres instance) is required. SQLite is not supported.
- Check network connectivity to Render Postgres host
- Verify credentials are valid
- Test connection with `psql` or `python` connection test

### 6.2 Missing Snapshots

- **If RVU/GPCI snapshots missing:** Run RVU ingestion first to create them
- Verify `dataset_snapshots` table has entries
- Check snapshot paths are accessible
- **Fallback:** Use `list_snapshots(dataset_id=...)` to list snapshots for a specific dataset (see §2.1)

### 6.3 Ingestion Failures

- Check logs for specific error messages
- Verify CF fetcher can download conversion factors (network access)
- **CF Override:** Use YAML files in `cf_overrides/{release_id}.yaml` or CLI flags if download fails
- Validate input data formats

### 6.4 Curated Output Verification

- Curated data is stored as Parquet files in `data/ingestion/mpfs/curated/{release_id}/`
- Row counts are in the Parquet files and manifest, not in relational tables
- Use pandas to inspect Parquet files (see §3.3)

---

## Files to Update

- `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` - Add run evidence section
- `artifacts/mpfs_implementation_plan.md` - Update Phase 8.1 completion status
- `artifacts/mpfs_production_run_[DATE]/` - Evidence folder with all artifacts

---

## Success Criteria

- [ ] Snapshot check passes (both RVU and GPCI snapshots available, with fallback for naming)
- [ ] MPFS ingestion completes successfully (using async path, quarter from snapshot)
- [ ] All curated Parquet files generated with non-zero row counts (verified via pandas inspection)
- [ ] Evidence captured to single folder (`artifacts/mpfs_production_run_[DATE]/`)
- [ ] Sample `datasets_used` from API includes `mpfs_cf`, `mpfs_rvu`, `mpfs_gpci`
- [ ] Minimal contract test added asserting `datasets_used` includes MPFS datasets
- [ ] Readiness plan updated with evidence
- [ ] Implementation plan Phase 8.1 marked complete

---

## MVP Recommendation (Scales Later)

1. **Run with Render PG (or fully migrated Postgres instance).**
2. **Use YAML config service for CF overrides (`cf_overrides/{release_id}.yaml`).**
3. **Use the direct async ingestion snippet; verify Parquet row counts; smoke the API.**
4. **Land a tiny contract test asserting `datasets_used` includes `mpfs_cf`/`mpfs_rvu`/`mpfs_gpci`.**
5. **Append one page of run evidence to your readiness doc and mark Phase 8.1 done.**

That should get live data flowing reliably in hours, while leaving clean seams for the more robust manifest/contract/golden-test setup you can add next.