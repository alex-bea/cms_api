# Fix Permissions - Run These Commands in Render Shell

**Step 1: Fix permissions on the entire cms_pricing directory**

```bash
cd /app
chmod -R 755 cms_pricing/
chmod -R 777 cms_pricing/ingestion/contracts/
```

**Step 2: Verify permissions are fixed**

```bash
ls -la cms_pricing/ingestion/contracts/
```

You should see the files are now readable/writable.

**Step 3: Now run ingestion**

```bash
cd /app && python -c "
import sys, logging, time, asyncio
from pathlib import Path
from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.models.rvu import Release

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
    result = asyncio.run(ingestor.ingest(release_id=release_id, batch_id=batch_id))
    
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

**If you still get permission errors, try:**

```bash
sudo chmod -R 777 /app/cms_pricing/
```


