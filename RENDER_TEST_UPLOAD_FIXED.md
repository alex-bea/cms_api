# Fixed Test Data Upload in Render Shell

The correct method is `ingest()`, not `ingest_from_scraped_data()`. Use this:

```python
python3 << 'EOF'
import asyncio
from datetime import datetime
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.database import SessionLocal
from sqlalchemy import text

async def test_upload():
    print("🚀 Testing Data Upload")
    print("=" * 60)
    
    # Check before
    db = SessionLocal()
    rvu_before = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
    releases_before = db.execute(text("SELECT COUNT(*) FROM releases")).fetchone()[0]
    db.close()
    
    print(f"📊 Before: RVU={rvu_before:,}, Releases={releases_before}")
    print()
    
    # Test ingestion
    release_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        ingestor = RVUIngestor("./data/rvu_scraper")
        print(f"📥 Starting ingestion...")
        print(f"   Release ID: {release_id}")
        print(f"   Batch ID: {batch_id}")
        print("   (This will discover, download, and process CMS RVU data)")
        print()
        
        result = await ingestor.ingest(
            release_id=release_id,
            batch_id=batch_id
        )
        
        print("=" * 60)
        print("📊 Ingestion Results:")
        print(f"   Status: {result.get('status', 'unknown')}")
        print(f"   Release ID: {result.get('release_id', 'N/A')}")
        print(f"   Batch ID: {result.get('batch_id', 'N/A')}")
        print(f"   Files Downloaded: {result.get('files_downloaded', 0)}")
        print(f"   Total Records: {result.get('total_records', 0):,}")
        
        if result.get('status') == 'success':
            print("\n✅ Ingestion successful!")
        elif result.get('status') == 'partial':
            print("\n⚠️  Ingestion completed with partial data")
        else:
            print(f"\n❌ Ingestion failed: {result.get('error', 'Unknown error')}")
        
        # Check after
        db = SessionLocal()
        rvu_after = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
        releases_after = db.execute(text("SELECT COUNT(*) FROM releases")).fetchone()[0]
        db.close()
        
        print()
        print(f"📊 After: RVU={rvu_after:,}, Releases={releases_after}")
        print(f"   Added: {rvu_after - rvu_before:,} records, {releases_after - releases_before} releases")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

asyncio.run(test_upload())
EOF
```

## Simpler Version (Just Test the Method)

If you want to test without waiting for the full download:

```python
python3 << 'EOF'
import asyncio
from datetime import datetime
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

async def test():
    ingestor = RVUIngestor("./data/rvu_scraper")
    release_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Testing ingest() with release_id={release_id}, batch_id={batch_id}")
    print("(This will take a few minutes as it downloads from CMS)...")
    
    result = await ingestor.ingest(release_id, batch_id)
    print(f"Status: {result.get('status')}")
    print(f"Records: {result.get('total_records', 0):,}")

asyncio.run(test())
EOF
```

