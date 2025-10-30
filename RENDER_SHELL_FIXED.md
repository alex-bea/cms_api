# Fixed Render Shell Command

**The issue is file permissions. Run this fixed version:**

```bash
cd /app && python -c "
import sys, logging, time, asyncio
from pathlib import Path
from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.models.rvu import Release

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Use /tmp which is writable
output_dir = '/tmp/rvu_ingestion'
Path(output_dir).mkdir(parents=True, exist_ok=True)
db = SessionLocal()

try:
    logger.info('✅ Starting RVU ingestion...')
    ingestor = RVUIngestor(output_dir=output_dir, db_session=db)
    release_id, batch_id = 'rvu_2025_prod', f'batch_prod_{int(time.time())}'
    
    logger.info(f'Running pipeline: release={release_id}, batch={batch_id}')
    result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
    
    count = db.query(Release).count()
    logger.info(f'✅ Complete! Total releases: {count}')
    print(f'\n🎉 SUCCESS: {count} release(s) loaded to database')
    print(f'   Output dir: {output_dir}')
except Exception as e:
    logger.error(f'❌ Failed: {e}')
    import traceback
    traceback.print_exc()
finally:
    db.close()
"
```

**Key change:** Using `/tmp/rvu_ingestion` instead of `data/ingestion/production` because `/tmp` is writable in the Render container.


