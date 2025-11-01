# Check Ingestion Logs

We need to see what the ingestion logs say. Run this in Render and check for:

## 1. Check if GPCI was processed

Look for these log messages in the ingestion output:
- `"Loading GPCI data"` - Should show DataFrame columns
- `"Sample row columns"` - Should show sample values

## 2. Check ingestion summary

The `load_rvu_to_production.py` script should output a summary. Look for:
- How many files were discovered
- What datasets were processed
- Any errors or warnings

## 3. Check if GPCI files exist

```bash
# In Render shell:
find data -name "*GPCI*" -type f
find sample_data -name "*GPCI*" -type f
```

## 4. Quick SQL check

```python
python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release
db = SessionLocal()
releases = db.query(Release).order_by(Release.imported_at.desc()).limit(5).all()
print('Recent releases:')
for r in releases:
    print(f'  {r.id} - {r.source_version} - {r.imported_at}')
db.close()
"
```

If you can share:
1. The ingestion script output (especially any "Loading GPCI data" logs)
2. Whether it says GPCI was processed
3. Any errors or warnings

This will help identify if:
- GPCI files aren't being found
- The parser isn't being called
- The data is being lost somewhere in the pipeline

