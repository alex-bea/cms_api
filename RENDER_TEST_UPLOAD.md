# Test Data Upload in Render Shell

## Quick Test - Check Current Data Before Upload

```python
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text
from datetime import datetime

db = SessionLocal()
try:
    # Get current counts
    rvu_before = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
    releases_before = db.execute(text("SELECT COUNT(*) FROM releases")).fetchone()[0]
    
    print(f"📊 Before upload:")
    print(f"   RVU Items: {rvu_before:,}")
    print(f"   Releases: {releases_before}")
    
    # Get latest release
    latest = db.execute(text("SELECT MAX(imported_at) FROM releases")).fetchone()[0]
    print(f"   Latest release: {latest}")
finally:
    db.close()
EOF
```

## Test RVU Ingestion (Small Test)

This will test the ingestion pipeline. **Note:** This downloads from CMS and may take a few minutes.

```python
python3 << 'EOF'
import asyncio
from datetime import datetime
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

async def test_ingestion():
    print("🚀 Starting RVU Ingestion Test")
    print("=" * 60)
    
    # Generate test IDs
    release_id = f"test_rvu_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Release ID: {release_id}")
    print(f"Batch ID: {batch_id}")
    print()
    
    try:
        # Initialize ingestor
        ingestor = RVUIngestor("./data/rvu_scraper")
        
        print("📥 Testing ingestion from scraped data...")
        print("   (This will download and process latest RVU data from CMS)")
        print()
        
        # Test ingestion - latest only, current year
        result = await ingestor.ingest_from_scraped_data(
            release_id=release_id,
            batch_id=batch_id,
            start_year=2025,
            end_year=2025,
            latest_only=True
        )
        
        print("=" * 60)
        print("📊 Ingestion Results:")
        print(f"   Status: {result.get('status', 'unknown')}")
        print(f"   Release ID: {result.get('release_id', 'N/A')}")
        print(f"   Batch ID: {result.get('batch_id', 'N/A')}")
        print(f"   Record Count: {result.get('record_count', 0):,}")
        print(f"   Quality Score: {result.get('quality_score', 0):.2f}")
        
        if result.get('status') == 'success':
            print("\n✅ Ingestion successful!")
        else:
            print("\n⚠️  Ingestion completed with warnings")
            
    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()

# Run the test
asyncio.run(test_ingestion())
EOF
```

## Test After Upload - Verify Data Was Added

```python
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    # Get current counts
    rvu_after = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
    releases_after = db.execute(text("SELECT COUNT(*) FROM releases")).fetchone()[0]
    
    print(f"📊 After upload:")
    print(f"   RVU Items: {rvu_after:,}")
    print(f"   Releases: {releases_after}")
    
    # Get latest release
    latest = db.execute(text("""
        SELECT id, type, source_version, imported_at 
        FROM releases 
        ORDER BY imported_at DESC 
        LIMIT 1
    """))
    
    release = latest.fetchone()
    if release:
        print(f"\n   Latest Release:")
        print(f"      ID: {release[0]}")
        print(f"      Type: {release[1]}")
        print(f"      Version: {release[2]}")
        print(f"      Imported: {release[3]}")
finally:
    db.close()
EOF
```

## Alternative: Test via API Endpoint (if API is running)

If your API service is running, you can also test via curl:

```bash
# Get your API URL (replace with your actual Render service URL)
API_URL="https://your-service.onrender.com"

# Test ingestion endpoint
curl -X POST "$API_URL/api/v1/rvu/scraper/ingest-from-scraped?release_id=test_$(date +%Y%m%d_%H%M%S)&batch_id=batch_$(date +%Y%m%d_%H%M%S)&latest_only=true" \
  -H "X-API-Key: YOUR_API_KEY"
```

## Simple One-Liner Test

Just check if ingestion can be imported and initialized:

```python
python3 -c "from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor; ingestor = RVUIngestor('./data/rvu_scraper'); print('✅ RVU Ingestor initialized successfully')"
```

