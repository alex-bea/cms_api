# Verify Production Data

## Check Database Records

**In Render Shell, run:**

```bash
python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex

db = SessionLocal()

# Check counts
print('Releases:', db.query(Release).count())
print('RVU Items:', db.query(RVUItem).count())
print('GPCI Indices:', db.query(GPCIIndex).count())

# Show release details
releases = db.query(Release).all()
for r in releases:
    print(f'\nRelease: {r.source_version}')
    print(f'  Created: {r.created_at}')
    print(f'  Files: {r.source_file}')
    
db.close()
"
```

## Check Via API

**From any terminal:**

```bash
curl -H "X-API-Key: dev-key-123" \
  https://cms-pricing-api.onrender.com/api/v1/rvu/releases | jq
```

## Expected Results:

- ✅ **1 release record** (metadata only)
- ❌ **0 data records** (RVU items, GPCI, etc.)

**Reason:** The scraper is downloading HTML pages (189KB files) instead of actual ZIP files. This is because the deployed code is using the old scraper without the two-hop discovery fix.

## Next Steps:

We need to deploy the updated RVU scraper v2.0 that includes the two-hop discovery fix. The code is already committed, but we need to check if it was deployed.


