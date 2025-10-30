# Workaround for Render - Bypass Schema Registration

**The issue:** Schema registry tries to write to read-only files in the container.

**Solution:** Use this workaround that skips the problematic initialization:

```bash
cd /app && python -c "
import sys, logging, time, asyncio, os
from pathlib import Path
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release

# Workaround: Monkey-patch the schema registry to skip writes
import cms_pricing.ingestion.contracts.schema_registry as sr
original_register = sr.SchemaRegistry.register_schema

def no_write_register(self, schema_contract):
    # Just skip the write operation
    pass

sr.SchemaRegistry.register_schema = no_write_register

# Now import after monkey-patch
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

output_dir = '/tmp/rvu_ingestion'
Path(output_dir).mkdir(parents=True, exist_ok=True)
db = SessionLocal()

try:
    logger.info('✅ Starting RVU ingestion...')
    ingestor = RVUIngestor(output_dir=output_dir, db_session=db)
    release_id, batch_id = 'rvu_2025_prod', f'batch_prod_{int(time.time())}'
    
    logger.info(f'Running pipeline: release={release_id}, batch={batch_id}')
    result = asyncio.run камни
(ingestor.ingest(release_id=release_id, batch_id=batch_id))
    
    count = db.query(Release).count()
    logger.info(f'✅ Complete! Total releases: {count}')
    print(f'\n🎉 SUCCESS: {count} release(s) loaded to database')
except Exception as e:
    logger.error(f'❌ Failed: {e}')
    import traceback
    traceback.print_exc()
finally:
    db.close()
"
```


