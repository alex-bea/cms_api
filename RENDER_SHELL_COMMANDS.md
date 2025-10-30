# Render Shell Commands - Run This in Render Shell

**Copy and paste these commands one at a time into Render Shell:**

## 1. Create the script

```bash
cat > /tmp/load_rvu.py << 'EOF'
#!/usr/bin/env python3
"""Load RVU data to Render production database."""

import sys
import logging
import time
from pathlib import Path

sys.path.insert(0, '/app')

from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.models.rvu import Release

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run RVU ingestion on production database."""
    output_dir = "data/ingestion/production"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    db_session = SessionLocal()
    
    try:
        logger.info("Starting RVU ingestion on production database")
        logger.info(f"Output directory: {output_dir}")
        
        ingestor = RVUIngestor(output_dir=output_dir, db_session=db_session)
        release_id = "rvu_2025_prod"
        batch_id = f"batch_prod_{int(time.time())}"
        
        logger.info(f"Running ingestion pipeline... (release_id={release_id}, batch_id={batch_id})")
        import asyncio
        result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
        
        logger.info(f"Ingestion completed: {result}")
        
        release_count = db_session.query(Release).count()
        logger.info(f"Total releases in database: {release_count}")
        
        if release_count > 0:
            logger.info("✅ Data successfully loaded to production database!")
            return 0
        else:
            logger.warning("⚠️  No releases found in database. Ingestion may have failed.")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}", exc_info=True)
        return 1
        
    finally:
        db_session.close()
        logger.info("Database session closed")


if __name__ == "__main__":
    sys.exit(main())
EOF
```

## 2. Make it executable

```bash
chmod +x /tmp/load_rvu.py
```

## 3. Run the script

```bash
python /tmp/load_rvu.py
```

## Expected Output

You should see:
- INFO - Starting RVU ingestion
- INFO - Downloading files from CMS.gov
- INFO - Dropped duplicate rows
- INFO - Database loading completed records_inserted=570
- INFO - Total releases in database: 1
- ✅ Data successfully loaded to production database!

## 4. Verify (Optional)

Test the API endpoint:
```bash
curl -H "X-API-Key: dev-key-123" https://cms-pricing-api.onrender.com/api/v1/rvu/releases
```


