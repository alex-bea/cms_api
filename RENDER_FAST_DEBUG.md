# Fast Debug - Process 1000 Rows Only

This patches the adapter to limit rows to 1000, making testing ~20x faster:

```python
python3 << 'EOF'
import asyncio
from datetime import datetime
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.ingestion.datasets import rvu_adapter
from cms_pricing.database import SessionLocal
from sqlalchemy import text

# Save original adapter
_original_adapt = rvu_adapter.adapt_rvu_raw_data

# Patch to limit rows
def limited_adapt(raw_batch):
    result = _original_adapt(raw_batch)
    MAX_ROWS = 1000
    for name, df in result.dataframes.items():
        if df is not None and len(df) > MAX_ROWS:
            print(f"   ⚡ Limiting {name}: {len(df):,} → {MAX_ROWS} rows")
            result.dataframes[name] = df.head(MAX_ROWS)
    return result

rvu_adapter.adapt_rvu_raw_data = limited_adapt

async def test():
    ingestor = RVUIngestor("./data/rvu_scraper")
    release_id = f"fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print("⚡ Fast Debug - 1000 rows max per dataset")
    print("=" * 60)
    
    result = await ingestor.ingest(release_id, batch_id)
    
    print(f"\n📊 Results:")
    print(f"   Status: {result.get('status')}")
    print(f"   Files: {result.get('files_downloaded', 0)}")
    print(f"   Records: {result.get('total_records', 0):,}")
    
    # Check DB for latest release
    db = SessionLocal()
    try:
        from cms_pricing.models.rvu import Release
        latest = db.query(Release).order_by(Release.imported_at.desc()).first()
        if latest:
            count = db.execute(text("""
                SELECT COUNT(*) FROM rvu_items WHERE release_id = :rid
            """), {"rid": latest.id}).fetchone()[0]
            
            with_values = db.execute(text("""
                SELECT COUNT(*) FROM rvu_items 
                WHERE release_id = :rid AND work_rvu IS NOT NULL
            """), {"rid": latest.id}).fetchone()[0]
            
            print(f"\n📊 Database (Release {latest.id}):")
            print(f"   Total records: {count:,}")
            print(f"   With work_rvu: {with_values:,} ({with_values*100.0/count:.1f}%)")
            
            if with_values > 0:
                print(f"\n✅ SUCCESS: RVU values populated!")
            else:
                print(f"\n❌ Still NULL")
    finally:
        db.close()

asyncio.run(test())
EOF
```

**Time savings:**
- Full file: ~20 minutes
- 1000 rows: ~1 minute (20x faster)
- Adjust `MAX_ROWS` as needed (500, 2000, etc.)

---

## Cleanup & Preflight Tips

- **Reset stale releases before retrying:**  
  ```bash
  python3 tools/reset_rvu_release.py --latest --dry-run   # preview
  python3 tools/reset_rvu_release.py --latest             # delete most recent RVU_FULL release
  ```
  You can also target by source version (`--source fast_2025`) or specific UUID (`--release-id ...`).

- **Preflight guard (avoids duplicate source versions):**  
  ```bash
  python3 tools/preflight_rvu_release.py --source fast_2025Q4
  ```
  Fails fast if a matching release already exists and reminds you if `MAX_INGESTION_ROWS` is still set.

- **Full ingest reminder:** Unset any row caps before production runs:  
  ```bash
  unset MAX_INGESTION_ROWS
  ```
