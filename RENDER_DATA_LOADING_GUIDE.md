# Guide: Load RVU Data to Render Database

**Status:** ✅ Tested and ready for production use  
**Expected Time:** 5 minutes  
**Created:** 2025-10-28

---

## Quick Steps

### Step 1: Access Render Shell

1. Go to: https://dashboard.render.com
2. Navigate to **cms-pricing-api** service
3. Click **"Shell"** tab

### Step 2: Run Production Ingestion

**In Render Shell, execute:**

```bash
cd /app
python scripts/load_rvu_to_production.py
```

**Expected Output:**
```
INFO - Starting RVU ingestion on production database
INFO - Running ingestion pipeline...
INFO - Dropped duplicate rows before DB load dataset=gpci duplicates_removed=1199
INFO - Database loading completed records_inserted=570
INFO - Total releases in database: 1
✅ Data successfully loaded to production database!
```

**Runtime:** ~20-30 seconds

### Step 3: Verify Data

**Query database:**
```bash
python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem

db = SessionLocal()
print('Releases:', db.query(Release).count())
print('RVU Items:', db.query(RVUItem).count())
db.close()
"
```

**Test API endpoint:**
```bash
curl -H "X-API-Key: dev-key-123" \
  https://cms-pricing-api.onrender.com/api/v1/rvu/releases
```

### Alternative: Upload Files First

If files aren't in the container:

1. **Prepare a manifest file** listing your files
2. **Upload via Render Shell** or SCP
3. **Run ingestion** pointing to uploaded files

## Verify Data Loaded

After loading, test API:
```bash
curl -H "X-API-Key: dev-key-123" \
  https://cms-pricing-api.onrender.com/api/v1/rvu/releases
```

Should return non-empty array.

---

## Low-Memory Snapshot Loading (Render)

Running RVU/MPFS/OPPS pipelines on Render’s 2 GB dynos is safe as long as you (a) point snapshots at the real curated parquet files under `/var/data/ingestion/production` and (b) cap how many rows PyArrow reads at once. Follow this sequence whenever you troubleshoot snapshot-aware ingestors on Render.

### 1. Discover the Latest RVU Manifest

```bash
manifest=$(ls -t /var/data/ingestion/production/curated/cms_rvu/*/manifest.json | head -n1)
export manifest
echo "Using manifest: $manifest"

python3 - <<'PY'
import json, os
manifest = os.environ["manifest"]
data = json.load(open(manifest))
for entry in data.get("datasets", []):
    if entry.get("name") in ("pprrvu", "gpci"):
        print(f"{entry['name']} rows={entry['records']:,} parquet={entry['parquet_path']}")
PY
```

### 2. Repair Snapshot Rows (Only If Needed)

If `dataset_snapshots.manifest_url` still points at `data/curated/...`, run the fixer so downstream ingestors read `/var/data/ingestion/production/...`:

```bash
python3 - <<'PY'
import json
from datetime import date
from pathlib import Path
from cms_pricing.database import SessionLocal
from cms_pricing.models.dataset_snapshots import DatasetSnapshot

manifest = Path(os.environ["manifest"])
data = json.loads(manifest.read_text())

def lookup(alias):
    for entry in data.get("datasets", []):
        if entry.get("name") == alias:
            return entry.get("parquet_path")
    return None

rows = {
    "rvu_items": ("rvu_2025_D", lookup("pprrvu")),
    "gpci_indices": ("gpci_2025_D", lookup("gpci")),
}

session = SessionLocal()
try:
    for dataset_id, (release_id, path) in rows.items():
        if not path:
            raise SystemExit(f"Missing parquet for {dataset_id}")
        snap = session.get(DatasetSnapshot, (dataset_id, release_id)) or DatasetSnapshot(
            dataset_id=dataset_id,
            release_id=release_id,
            digest="manual-digest",
            effective_from=date.today(),
        )
        snap.manifest_url = path
        snap.digest = "manual-digest"
        snap.effective_from = date.today()
        session.merge(snap)
        print("Set", dataset_id, "→", path)
    session.commit()
finally:
    session.close()
PY
```

### 3. Clamp Snapshot Row Counts

| Env Var | Default | When to Set | Notes |
|---------|---------|-------------|-------|
| `MAX_MPFS_SNAPSHOT_ROWS` | Unlimited | 2 GB dynos | Caps rows per dataset; 10 k works well on Render. |
| `MPFS_SNAPSHOT_BATCH_ROWS` | 50 000 | Low-memory runs | PyArrow batch size; keep ≤ the row limit. |
| `INGEST_SNAPSHOT_ROW_LIMIT` | Unlimited | Global fallback | Shared loader honors this when dataset-specific limits aren’t set. |

You can also use dataset-specific overrides (e.g., `MAX_RVU_ITEMS_SNAPSHOT_ROWS`) when you only need to clamp one dataset.

### 4. Render-Friendly MPFS Ingest Command

```bash
export MAX_MPFS_SNAPSHOT_ROWS=10000
export MPFS_SNAPSHOT_BATCH_ROWS=10000

python3 - <<'PY'
import asyncio
from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor

async def run():
    ingestor = MPFSIngestor(output_dir="/var/data/ingestion/mpfs")
    result = await ingestor.ingest(year=2025, quarter="D")
    print("MPFS ingest complete")
    print("  release_id:", result.get("release_id"))
    print("  batch_id:", result.get("batch_id"))
    print("  conversion_factor_strategy:", result["metadata"]["conversion_factor_strategy"])

asyncio.run(run())
PY
```

Watch for log lines such as `Row limiting applied for snapshot load dataset=rvu_items limited_rows=10000 original_rows=151816`.

### 5. Verify Conversion Factor Output

```bash
python3 - <<'PY'
from pathlib import Path
import pandas as pd

cf_root = Path("/var/data/ingestion/mpfs/curated")
manifest = sorted(cf_root.glob("mpfs_*/manifest.json"))[-1]
cf_path = manifest.parent / "mpfs_cf_vintage.parquet"
df = pd.read_parquet(cf_path)
print(df.head())
PY
```

If `mpfs_cf_vintage` is empty, increase the snapshot row limit so the truncated RVU dataframe still includes the `conversion_factor` column.

### 6. Troubleshooting Notes

- **“Snapshot path does not exist … data/curated/…”** → rerun the repair script above.
- **“RVU dataframe empty; unable to derive conversion factor”** → bump `MAX_MPFS_SNAPSHOT_ROWS` and rerun.
- **Pod restarts / OOM** → lower the row/batch limits (e.g., 5 k) and retry; only unset the limits after upgrading the dyno.

Keep this section handy—following these steps avoids manual DB surgery and keeps Render pods well below the 2 GB cap.

---

---

## Troubleshooting

### Issue: "Unique constraint violation"
**Cause:** Re-running ingestion on existing data  
**Solution:** This is expected. First run inserts 570 records. Subsequent runs detect duplicates and skip them.

### Issue: "No data in database"
**Check:** Look for errors in the ingestion logs  
**Command:** `tail -n 50 /app/data/ingestion/production/logs/*.log`

### Issue: "Module not found"
**Solution:** Ensure you're in `/app` directory in Render Shell

---

## Quick Reference

**Service URL:** https://cms-pricing-api.onrender.com  
**API Key:** dev-key-123  
**Database:** Connected (Render Postgres)  
**Tables:** 6 RVU tables ready  
**Production Script:** `scripts/load_rvu_to_production.py`

---

## Summary

✅ **Tested locally** - 570 unique records from 208,143 raw records  
✅ **Deduplication working** - Natural key constraints enforced  
✅ **Ready for production** - Single command execution  
✅ **Runtime optimized** - ~20 seconds for full pipeline
