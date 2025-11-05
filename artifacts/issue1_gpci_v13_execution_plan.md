# Issue 1: GPCI v1.3 Loading to Render - Execution Plan

**Status:** Ready to execute  
**Priority:** High  
**Estimated Time:** 1-2 hours (if Render environment works) or 2-3 hours (if fixes needed)

---

## Execution Strategy: Test First, Fix Later

Based on analysis, we'll test if Render's Docker environment works first (likely scenario), then diagnose if needed.

---

## Step 1: Pre-Flight Check (5 minutes)

### Verify Prerequisites

```bash
# Check that source files exist locally
ls -la sample_data/rvu25d_0/GPCI2025.txt

# Expected: File exists, non-zero size (~17KB)
```

**If files don't exist:** You'll need to upload them to Render or use scraper discovery.

### Verify Migration Status

The provenance migration should run automatically on next deploy. Verify it's ready:

```bash
# Check migration file exists
ls -la alembic/versions/8d80f393d0ee_*.py

# Verify it adds release_id and batch_id to gpci table
grep -A 5 "gpci" alembic/versions/8d80f393d0ee_*.py
```

✅ **Expected:** Migration adds `release_id` and `batch_id` columns to `gpci` table.

---

## Step 2: Test Render One-Off Job (15 minutes)

### Option A: Via Render Dashboard (Easiest)

1. **Go to Render Dashboard:**
   - Navigate to your web service (`cms-pricing-api`)
   - Open the **Jobs** tab
   - Click **+ Add Job** or use existing job

2. **Configure Job:**
   - **Name:** `load-gpci-v13-test`
   - **Start command:** 
   ```bash
   python scripts/load_rvu_to_production.py
   ```
   - **Base service:** Your API service (automatically uses latest build + environment)

3. **Run Job:**
   - Click **Run Job**
   - Monitor logs in real-time

4. **Expected Results:**
   - ✅ **Success:** Job completes, logs show "Data successfully loaded"
   - ❌ **Failure:** Capture full stack trace for diagnosis

### Option B: Via Render API

```bash
# Set your Render credentials
export RENDER_API_KEY="your-api-key"
export RENDER_SERVICE_ID="srv-xxxxx"

# Trigger job
curl -X POST "https://api.render.com/v1/services/${RENDER_SERVICE_ID}/jobs" \
  -H "Authorization: Bearer ${RENDER_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "startCommand": "python scripts/load_rvu_to_production.py"
  }'

# Poll for status
JOB_ID="<from-response>"
curl -s "https://api.render.com/v1/jobs/${JOB_ID}" \
  -H "Authorization: Bearer ${RENDER_API_KEY}" | jq '.status'
```

### Option C: Render Shell (if dashboard/API unavailable)

```bash
# Connect to Render shell
# Then run:
cd /app
python scripts/load_rvu_to_production.py
```

---

## Step 3: Verify Data Loaded (10 minutes)

### Check Database

```bash
# Connect to Render database
psql $DATABASE_URL

# Or via Render dashboard → Database → Connect
```

**Queries:**

```sql
-- Check GPCI row count
SELECT COUNT(*) as total_rows FROM gpci;
-- Expected: ~109 rows (for 2025 data)

-- Verify provenance columns populated
SELECT 
  COUNT(*) as total,
  COUNT(release_id) as has_release_id,
  COUNT(batch_id) as has_batch_id,
  COUNT(DISTINCT release_id) as unique_releases,
  COUNT(DISTINCT batch_id) as unique_batches
FROM gpci;

-- Expected output:
-- total: ~109
-- has_release_id: ~109 (all should have release_id)
-- has_batch_id: ~109 (all should have batch_id)
-- unique_releases: 1
-- unique_batches: 1

-- Verify natural key uniqueness (v1.3 with MAC)
SELECT 
  locality_id,
  effective_from,
  mac,
  COUNT(*) as count
FROM gpci
GROUP BY locality_id, effective_from, mac
HAVING COUNT(*) > 1;
-- Expected: 0 rows (no duplicates)

-- Check sample data
SELECT 
  locality_id,
  locality_name,
  gpci_work,
  gpci_pe,
  gpci_mp,
  release_id,
  batch_id
FROM gpci
LIMIT 5;
```

### Check API Endpoint

```bash
# Test API response includes GPCI data with provenance
curl https://your-api.render.com/api/v1/pricing/price \
  -H "X-API-Key: your-key" \
  -H "Content-Type: application/json" \
  -d '{
    "codes": ["99213"],
    "zip": "90210",
    "year": 2025
  }' | jq '.line_items[0].trace_refs'

# Should include GPCI provenance like:
# ["GPCI:release:rvu_2025_prod", "GPCI:batch:batch_prod_..."]
```

---

## Step 4: If Segfault Occurs (Diagnosis)

### Capture Information

1. **Full Stack Trace:**
   - Copy entire error output from Render job logs
   - Look for Python version, library versions

2. **Environment Info:**
   ```bash
   # In Render shell or One-Off Job:
   python --version
   python -c "import sys; print(sys.executable)"
   python -c "import pandas; print(pandas.__version__)"
   python -c "import numpy; print(numpy.__version__)"
   python -c "import sqlalchemy; print(sqlalchemy.__version__)"
   ```

3. **Test Minimal Import:**
   ```bash
   python -c "import pandas, sqlalchemy, structlog; print('OK')"
   ```

### Potential Fixes

#### Fix 1: Update Dockerfile Dependencies

**File:** `Dockerfile`

```dockerfile
# Add explicit pandas/pyarrow installation with platform flags
RUN pip install --no-cache-dir \
    pandas==2.1.0 \
    pyarrow==12.0.1 \
    sqlalchemy==2.0.20
```

#### Fix 2: Use Alternative Loading Script

If segfault is in specific code path, create minimal loader:

```python
# scripts/load_gpci_minimal.py
# Direct pandas → SQLAlchemy bulk insert
# Avoids potential problematic code paths
```

#### Fix 3: Use psql COPY Instead

As last resort, export to CSV and load via SQL:

```sql
\COPY gpci FROM 'gpci_data.csv' CSV HEADER;
```

---

## Step 5: Documentation Updates

After successful load:

1. **Update Runbook:**
   - File: `prds/RUN-render-deployment-prd-v1.0.md`
   - Add successful GPCI loading steps
   - Document any environment fixes if needed

2. **Update Readiness Plan:**
   - File: `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md`
   - Mark GPCI v1.3 data loading complete
   - Add verification steps

3. **Create CI Test:**
   - Add to `.github/workflows/` or `tests/integration/`
   - Test GPCI loading in containerized environment

---

## Success Criteria

- [x] GPCI v1.3 data loaded to `gpci` table
- [x] Row count matches expected (~109 for 2025 data)
- [x] Provenance columns populated (`release_id`, `batch_id`)
- [x] Natural key uniqueness verified (no duplicates on MAC + locality_id + effective_from)
- [x] API queries return GPCI data correctly
- [x] Provenance appears in API responses

---

## Troubleshooting Quick Reference

| Issue | Solution |
|-------|----------|
| **Segfault in Render** | Capture stack trace → Check Dockerfile dependencies → Update if needed |
| **No source files** | Upload to Render or configure scraper discovery |
| **Migration not applied** | Run `alembic upgrade head` via One-Off Job first |
| **Provenance NULL** | Verify `load_rvu_to_production.py` generates release_id/batch_id |
| **API doesn't return GPCI** | Check pricing engine queries gpci table correctly |

---

## Next Steps After Success

1. Mark Task 66 complete in GitHub project
2. Update deployment checklist
3. Move to Issue 2 (OPPS wage index)

---

## Estimated Timeline

- **Best case (Render works):** 30-45 minutes
- **Needs diagnosis:** 1-2 hours  
- **Needs fixes:** 2-3 hours

**Ready to execute!** Start with Step 2 (Render One-Off Job test).

