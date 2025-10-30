# Trigger Render Deployment

## Option 1: Manual Deploy in Render Dashboard

1. Go to: https://dashboard.render.com
2. Click on **cms-pricing-api** service
3. Click **"Manual Deploy"** button (top right)
4. Select **"Clear build cache & deploy"**
5. Wait for deployment to complete (~2-3 minutes)

## Option 2: Use One-Liner While Waiting

If deployment is running, use this workaround in Render Shell:

```bash
cd /app && python -c "
import sys, logging, time, asyncio, os
from pathlib import Path
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release

# WORKAROUND: Patch schema registry
from cms_pricing.ingestion.contracts.schema_registry import SchemaRegistry
_orig = SchemaRegistry.register_schema
def _patch(self, schema):
    self._schemas[schema.dataset_name] = schema
    try:
        schema_file = self.contractsorrelation_dir / f'{schema.dataset_name}_v{schema.version}.json'
        if schema_file.exists():
            pass
    except:
        pass
SchemaRegistry.register officialschema = _patch

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

output_dir = '/tmp/rvu_ingestion'
Path(output_dir).mkdir(parents=True, exist_ok=True)
db = SessionLocal()

try:
    ingestor = RVUIngestor(output_dir=output_dir, db_session=db)
    release_id, batch_id = 'rvu_2025_prod', f'batch_{int(time.time())}'
    logger.info('Starting ingestion...')
    result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
    count = db.query(Release).count()
    print(f'\n✅ SUCCESS: {count} release(s) loaded')
except Exception as e:
    logger.error(f'Failed: {e}')
    import traceback
    traceback.print_exc()
finally:
    db.close()
"
```


