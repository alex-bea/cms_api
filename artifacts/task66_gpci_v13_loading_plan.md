# Task 66: GPCI v1.3 Production Loading Plan

**Status:** Ready to implement  
**Priority:** High  
**Estimated Time:** 2-3 hours  
**Date:** 2025-10-31

## Context

GPCI v1.3 data needs to be loaded into Render production database. Previous attempt was deferred due to Python environment segfault. Recent Phase 2 provenance changes add `release_id` and `batch_id` columns that need to be populated.

## Prerequisites Status

- ✅ Database schema deployed (includes provenance columns via migration `8d80f393d0ee`)
- ✅ Alembic migrations applied 
- ✅ GPCI v1.3 parser implemented and tested
- ❌ Python environment segfault issue (needs resolution/workaround)
- ⚠️ `backfill_gpci_v13.py` incomplete (Step 5 TODO - no actual loading logic)

## Impact of Recent Provenance Changes

The Phase 2 provenance implementation adds `release_id` and `batch_id` columns to the `gpci` table (and all fee schedule tables). This means:

1. **Migration already applied**: When you deploy to Render, the provenance migration runs automatically
2. **Loading must populate provenance**: Any data loading must include `release_id` and `batch_id` values
3. **Existing data will have NULL provenance**: This is acceptable until data is re-ingested

## Recommended Solution: Use RVU Ingestor (Most Complete)

The `RVUIngestor` is the most complete path and handles:
- Provenance metadata automatically
- GPCI parsing with v1.3 natural key (MAC + locality_code + effective_from)
- Data validation and quality checks
- Proper batch_id and release_id generation

### Option A: Use RVU Ingestor (Recommended)

**Pros:**
- ✅ Complete, tested implementation
- ✅ Handles provenance automatically
- ✅ Includes validation and error handling
- ✅ Generates proper batch_id/release_id
- ✅ Works with existing ingestion pipeline

**Cons:**
- Requires source files in expected location
- More setup than direct script

**Implementation:**
```bash
# 1. Ensure source files are available
# Place GPCI2025.txt in sample_data/rvu25d_0/ or use scraper output

# 2. Run RVU ingestion (loads all RVU datasets including GPCI)
python scripts/load_rvu_to_production.py --debug --output data/ingestion/production

# This will:
# - Parse GPCI with v1.3 parser (includes MAC in natural key)
# - Generate release_id and batch_id automatically
# - Load to database with provenance metadata
# - Create parquet files for verification
```

### Option B: Complete backfill_gpci_v13.py Script

**Pros:**
- Direct control over GPCI loading
- Can target specific release
- Simpler than full ingestion pipeline

**Cons:**
- Currently incomplete (Step 5 TODO)
- Needs to be updated for provenance support
- Must handle all edge cases manually

**What needs to be done:**
1. Complete Step 5 loading logic in `backfill_gpci_v13.py`
2. Add provenance metadata (release_id, batch_id) 
3. Use `DatabaseLoader.load_mpfs_data()` pattern or direct SQLAlchemy bulk insert
4. Map parsed GPCI columns to `gpci` table schema

**Implementation (needs code changes):**
```python
# In backfill_gpci_v13.py Step 5, replace TODO with:
from scripts.load_data import DatabaseLoader
from cms_pricing.models.fee_schedules import GPCI

# Map parsed data to database schema
df_db = result.data.copy()
df_db.rename(columns={
    'locality_code': 'locality_id',
    # ... other mappings
}, inplace=True)

# Add provenance
df_db['release_id'] = release_id
df_db['batch_id'] = f"gpci_v13_{datetime.now().isoformat()}"

# Use DatabaseLoader pattern for bulk insert
loader = DatabaseLoader()
# ... use _prepare_bulk_records and bulk_insert_mappings
```

### Option C: Use Render One-Off Job (Avoids Local Segfault)

**Pros:**
- ✅ Avoids local Python environment issues completely
- ✅ Uses clean Docker environment from service image
- ✅ Can run directly on Render infrastructure

**Cons:**
- Requires source files to be accessible from Render
- Need to upload files or use scraper discovery
- Less control over execution environment

**Implementation:**
```bash
# 1. Create One-Off Job in Render Dashboard
#    Command: python scripts/load_rvu_to_production.py
#    This runs in the service's clean Docker environment

# OR

# 2. Use Render API to trigger job
curl -X POST "https://api.render.com/v1/services/${RENDER_SERVICE_ID}/jobs" \
  -H "Authorization: Bearer ${RENDER_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"startCommand":"python scripts/load_rvu_to_production.py"}'
```

### Option D: Fix Local Environment First

**For local testing/development:**

1. **Create clean virtual environment:**
```bash
# Remove broken conda/venv
rm -rf .venv*

# Create fresh venv with system Python
python3 -m venv .venv-gpci
source .venv-gpci/bin/activate

# Install dependencies
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# Test
python -c "import pandas, sqlalchemy, structlog; print('✅ OK')"
```

2. **Or use Docker:**
```bash
docker-compose up -d
docker-compose exec api python scripts/backfill_gpci_v13.py --dry-run
```

## Recommended Action Plan

### Immediate (Next Deployment)

1. **Deploy Phase 2 migration** (already staged):
   - Migration `8d80f393d0ee` will run automatically via CI/CD
   - Adds `release_id` and `batch_id` to `gpci` table
   - No manual intervention needed

2. **Choose loading approach:**
   - **If you have RVU source files ready**: Use `load_rvu_to_production.py` (Option A)
   - **If you prefer direct GPCI loading**: Complete `backfill_gpci_v13.py` first (Option B)
   - **If local env is still broken**: Use Render One-Off Job (Option C)

### Within 1 Week

1. **Load GPCI v1.3 data:**
   ```bash
   # Option A (recommended if files available):
   python scripts/load_rvu_to_production.py
   
   # Option C (if local env broken):
   # Create Render One-Off Job via dashboard/API
   ```

2. **Verify data loaded:**
   ```bash
   psql $DATABASE_URL -c "
     SELECT 
       COUNT(*) as total_rows,
       COUNT(DISTINCT release_id) as releases,
       COUNT(DISTINCT batch_id) as batches,
       COUNT(DISTINCT locality_id) as localities
     FROM gpci;
   "
   
   # Should show:
   # - total_rows: ~109 (for 2025 data)
   # - releases: 1
   # - batches: 1
   # - localities: varies by vintage
   ```

3. **Verify provenance:**
   ```bash
   psql $DATABASE_URL -c "
     SELECT release_id, batch_id, COUNT(*) 
     FROM gpci 
     GROUP BY release_id, batch_id;
   "
   # Should show non-NULL values for both columns
   ```

4. **Test API endpoint:**
   ```bash
   curl https://your-api.render.com/api/v1/gpci?locality_id=00&year=2025
   # Should return GPCI values with provenance in response
   ```

## Verification Checklist

After loading, verify:

- [ ] Row count matches expected (~109 for RVU25D)
- [ ] No NULL release_id or batch_id values
- [ ] Natural key uniqueness verified (no duplicates on MAC + locality_id + effective_from)
- [ ] API queries return GPCI data correctly
- [ ] Provenance appears in API responses (`datasets_used` field)
- [ ] Pricing calculations work with new GPCI data

## Troubleshooting

### If Segfault Persists Locally

1. **Use Docker instead:**
   ```bash
   docker-compose exec api python scripts/load_rvu_to_production.py
   ```

2. **Use Render One-Off Job:**
   - Create job in Render dashboard
   - Upload source files to Render service filesystem (or use scraper discovery)
   - Run job

3. **Direct SQL loading (last resort):**
   ```bash
   # Export parsed data to CSV
   python -c "
   import pandas as pd
   from pathlib import Path
   # ... parse and export
   df.to_csv('gpci_data.csv', index=False)
   "
   
   # Load via psql
   psql $DATABASE_URL << 'SQL'
   \COPY gpci (locality_id, locality_name, gpci_work, gpci_pe, gpci_mp, year, effective_from, release_id, batch_id)
   FROM 'gpci_data.csv' CSV HEADER;
   SQL
   ```

### If Migration Not Applied

Check Render deployment logs - migration should run automatically. If not:

```bash
# Manual migration via Render One-Off Job
# Command: alembic upgrade head
```

## Related Files

- `scripts/load_rvu_to_production.py` - Complete RVU ingestion (recommended)
- `scripts/backfill_gpci_v13.py` - Direct GPCI backfill (needs completion)
- `scripts/load_data.py` - Database loader with provenance support
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py` - Main ingestion pipeline
- `cms_pricing/models/fee_schedules.py` - GPCI model with provenance columns
- `alembic/versions/8d80f393d0ee_*.py` - Provenance migration

## Next Steps

1. ✅ Migration will run automatically on next Render deployment
2. ⏳ Choose loading approach (A, B, or C)
3. ⏳ Load GPCI v1.3 data with provenance
4. ⏳ Verify data quality and API functionality

## Summary

The recent provenance changes **do not block** GPCI loading - they actually **require** it to populate the new columns. The segfault issue can be avoided by using:
- Docker environment locally
- Render One-Off Jobs (clean environment)
- Or fixing local Python environment first

**Recommended path:** Use `load_rvu_to_production.py` via Render One-Off Job to avoid local environment issues completely.

