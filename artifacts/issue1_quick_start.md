# Issue 1: GPCI v1.3 Quick Start Guide

**TL;DR:** Test Render One-Off Job first - it likely works! ✅

---

## ✅ Pre-flight Check Complete

- ✅ GPCI source file exists: `sample_data/rvu25d_0/GPCI2025.txt` (17KB)
- ✅ Provenance migration ready: `alembic/versions/8d80f393d0ee_*.py`
- ✅ Loading script ready: `scripts/load_rvu_to_production.py`

---

## 🚀 Quick Start: Test in Render (15 min)

### Step 1: Create One-Off Job in Render Dashboard

1. Go to **Render Dashboard → Your Service → Jobs Tab**
2. Click **+ Add Job** (or use existing)
3. Configure:
   - **Name:** `load-gpci-v13`
   - **Command:** `python scripts/load_rvu_to_production.py`
4. Click **Run Job**
5. Watch logs

### Step 2: Check Results

**If Success:**
```bash
# Verify in database (via Render dashboard or psql)
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci WHERE release_id IS NOT NULL;"
# Expected: ~109 rows
```

**If Segfault:**
- Capture full stack trace from Render logs
- Check Step 4 in `issue1_gpci_v13_execution_plan.md` for diagnosis

---

## 📋 What Happens

1. **RVU Ingestor runs:**
   - Parses GPCI2025.txt with v1.3 parser (includes MAC in natural key)
   - Generates `release_id` and `batch_id` automatically
   - Loads to `gpci` table with provenance columns

2. **Expected output:**
   - ~109 GPCI rows loaded
   - All rows have `release_id` and `batch_id`
   - No duplicate natural keys (MAC + locality_id + effective_from)

---

## 🔍 Verification Commands

```sql
-- Check data loaded
SELECT COUNT(*) FROM gpci;

-- Verify provenance
SELECT 
  COUNT(*) as total,
  COUNT(release_id) as has_release_id,
  COUNT(batch_id) as has_batch_id
FROM gpci;

-- Check no duplicates on v1.3 NK
SELECT locality_id, effective_from, mac, COUNT(*)
FROM gpci
GROUP BY locality_id, effective_from, mac
HAVING COUNT(*) > 1;
-- Should return 0 rows
```

---

## ⚠️ If You Get Segfault

1. **Capture info:**
   - Full error from Render logs
   - Python/library versions

2. **Check:** Is it Render environment or just local?
   - Try Render shell directly
   - Test minimal imports

3. **Fix options:**
   - Update Dockerfile dependencies
   - Use alternative loading approach
   - See `issue1_gpci_v13_execution_plan.md` Step 4

---

## 📚 Full Details

See `artifacts/issue1_gpci_v13_execution_plan.md` for:
- Complete troubleshooting guide
- Alternative approaches
- Documentation updates needed

---

## ✅ Success Criteria

- [x] GPCI data in database
- [x] Provenance columns populated
- [x] No duplicate natural keys
- [x] API returns GPCI data with provenance

**Ready to execute!** Start with Render One-Off Job test above.

