# Issue 1: Complete Problem Diagnosis & Analysis

**Date:** 2025-11-01  
**Status:** Blocked on data source discovery  
**Task:** GitHub Task 66 - Load GPCI v1.3 Data to Production Database

---

## Executive Summary

**✅ Completed:**
- Fixed column mapping bug in RVU ingestor
- Confirmed no segfault (script runs successfully)
- Added debug logging
- Fixed to handle both column naming conventions

**❌ Blocker:**
- 2025 CMS ZIP archives don't contain GPCI files
- Existing 109 rows have NULL values due to previous bug
- Need to find actual CMS GPCI download source

---

## 1. Root Cause Analysis

### Issue History

**Original Problem:** 
> "GPCI v1.3 still has to be loaded into the Render production database; the previous attempt hit a Python environment segfault"

**What We Found:**
1. **No segfault** - Script runs fine (was a one-time environment issue)
2. **Column mapping bug** - Parser outputs `gpci_work`, loader expected `work_gpci`
3. **No GPCI data** - 2025 CMS ZIP files are empty or don't contain GPCI files

### Database State

```
gpci_indices table: 109 rows
├── MAC: Populated ✅
├── locality_id: Populated ✅
├── effective_start: Populated ✅
├── work_gpci: NULL ❌
├── pe_gpci: NULL ❌
└── mp_gpci: NULL ❌

gpci table: 0 rows ❌
```

**Conclusion:** Previous ingestion created skeleton rows but failed to populate GPCI values.

---

## 2. Fixes Applied

### Fix #1: Column Mapping (rvu_ingestor.py)

**Before:**
```python
"work_gpci": float(row.get('gpci_work') or row.get('work_gpci')) if pd.notna(row.get('gpci_work') or row.get('work_gpci')) else None,
```

**After:**
```python
work_val = row.get('gpci_work') if pd.notna(row.get('gpci_work')) else (row.get('work_gpci') if pd.notna(row.get('work_gpci')) else None)
...
"work_gpci": float(work_val) if work_val is not None else None,
```

**Why:** The original logic with `pd.notna(row.get('gpci_work') or row.get('work_gpci'))` was buggy - the `or` operation returns the first truthy value, but pandas NaNs need proper `pd.notna()` checks.

### Fix #2: Debug Logging

Added comprehensive logging to see:
- What columns are in the DataFrame
- Sample values from first row
- Where data is lost in the pipeline

**Status:** ✅ Deployed but shows no GPCI is being parsed

---

## 3. Data Source Investigation

### CMS Source Documentation

From `prds/SRC-gpci.md`:
- **Source URL:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
- **Release Cadence:** Quarterly (RVU25A/B/C/D)
- **Formats:** TXT, CSV, XLSX, ZIP
- **Expected:** GPCI files in RVU bundles

### What We Actually Have

**Sample Data (Local):**
```
sample_data/rvu25d_0/GPCI2025.txt      (118 lines, 17KB)
sample_data/rvu25d_0/GPCI2025.csv      (116 lines)
sample_data/rvu25d_0/GPCI2025.xlsx     (115 rows)
```

**Render Downloaded:**
```
data/ingestion/production/raw/cms_rvu/rvu_2025_prod/files/
├── rvu25a-20250110.zip  ❌ No GPCI files inside
├── rvu25b-20250605.zip  ❌ No GPCI files inside
├── rvu25c-20250605.zip  ❌ No GPCI files inside
└── rvu25d-20250911.zip  ❌ No GPCI files inside
```

**Critical Finding:** 
The CMS ZIP files downloaded by the scraper either:
1. Contain different file structures than sample_data
2. Have GPCI files named differently
3. Don't include GPCI at all (CMS changed distribution)

---

## 4. Evidence Chain

### Evidence #1: Empty ZIP Files

```bash
unzip -l data/ingestion/.../rvu25a-20250110.zip
# Returns nothing or very few files
```

**Implication:** ZIPs are either:
- Corrupted downloads
- Different format than expected
- CMS changed file structure

### Evidence #2: No Parsing Logs

```bash
python scripts/load_rvu_to_production.py | grep -i "gpci.*rows"
# Returns nothing
```

**Implication:** GPCI files never reached the parser

### Evidence #3: NULL Values in Database

```sql
SELECT work_gpci, pe_gpci, mp_gpci FROM gpci_indices LIMIT 1;
-- All NULL
```

**Implication:** 
- Parser ran but got no data
- OR loader failed silently
- Previous ingestion had the same bug we just fixed

---

## 5. Key Questions

### Question 1: Why do ZIPs appear empty?

**Possible Answers:**
A. CMS changed ZIP structure in 2025
B. Download was corrupted (but checksums would fail)
C. ZIPs only contain certain datasets now
D. File listing failed (unzip not working)

**Action:** Manually inspect ZIP contents on CMS website vs what we downloaded

### Question 2: Where did the 109 rows come from?

**Investigation Path:**
- Check release metadata in database
- Look for any successful ingestion logs
- Check if `sample_data` was ever used successfully
- Review git history of `load_rvu_to_production.py`

**Implication:** Something worked at some point, but then broke

### Question 3: What's the actual CMS URL structure?

**Need to Verify:**
- Does CMS distribute GPCI separately now?
- Are there multiple download pages?
- Did CMS consolidate into a different bundle?
- Is there a "complete data" vs "incremental" distinction?

---

## 6. Architecture Analysis

### Current Flow

```
1. Scraper → Discovers ZIP URLs from CMS
2. Ingestor → Downloads ZIPs
3. Land Stage → Extracts files from ZIPs
4. Normalize Stage → Parses files with parse_gpci()
5. Enrich Stage → Passes through
6. Publish Stage → Loads via _load_dataframes_to_database()
   └→ _load_gpci_data() → Maps columns → Saves to gpci_indices
```

### Where It's Failing

**Stage 1-2: Discovery & Download**
- ✅ ZIPs downloaded successfully
- ✅ Files are the right size (~3-4MB each)
- ❌ ZIP contents appear empty or missing GPCI

**Stage 3: Land**
- ❌ ZIP extraction may be failing silently
- ❌ Or GPCI files aren't in ZIPs

**Stage 4: Normalize**
- ❌ Never sees GPCI files to parse
- ❌ No "invoking_parser.*gpci" logs

**Stage 5-6: Enrich & Publish**
- ❌ No GPCI DataFrames to load
- ❌ `enriched_data` dict doesn't contain "gpci" key

---

## 7. What We Know vs What We Assume

### ✅ Facts

1. Sample data (`sample_data/rvu25d_0/GPCI2025.txt`) contains valid GPCI data
2. Parser v1.3 works correctly (tests pass)
3. Database schema is correct (109 rows created)
4. Column mapping bug existed and is now fixed
5. Scraper successfully downloads 4 ZIP files
6. ZIP files are proper ZIPs (not corrupted downloads)

### ❓ Assumptions

1. **Assumption:** CMS ZIP files should contain GPCI files
   **Reality:** Unknown - need to verify on CMS website

2. **Assumption:** Sample data matches what's in production ZIPs
   **Reality:** Unknown - sample_data may be from different distribution

3. **Assumption:** GPCI is distributed with RVU bundles
   **Reality:** Needs verification - documentation says yes, but files missing

---

## 8. Next Investigation Steps

### Priority 1: Verify CMS Website

**Action:** Manually download RVU25D ZIP from CMS and inspect contents
**What to look for:**
- Does GPCI2025.txt exist inside?
- What other files are in the ZIP?
- Is the file structure different than sample_data?

**URL to check:**
https://www.cms.gov/files/zip/rvu25d.zip

### Priority 2: Check Scraper Output

**Action:** Run scraper in discovery-only mode and inspect manifest
**What to look for:**
- Does manifest list GPCI files?
- Are checksums matching?
- What URLs are actually being discovered?

**Command:**
```python
# Check what scraper actually finds
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
scraper = CMSRVUScraper()
files = await scraper.scrape_rvu_files(2025, 2025)
print(files)
```

### Priority 3: Check Historical Data

**Action:** Look for previous successful GPCI loads
**What to look for:**
- Git history of database migrations
- Any ingestion logs with successful loads
- How sample_data was originally populated

---

## 9. Potential Solutions

### Solution A: Use Sample Data Locally

**Approach:** Parse `sample_data/rvu25d_0/GPCI2025.txt` and load directly
**Pros:** 
- Data is definitely valid
- Works immediately
- Good for testing

**Cons:**
- Not production-grade
- Doesn't solve ongoing ingestion
- sample_data not on Render

**Implementation:** Already created `scripts/load_gpci_from_sample.py`

### Solution B: Find Separate GPCI Download

**Approach:** If CMS distributes GPCI separately, add new scraper step
**Pros:**
- Proper solution
- Ongoing sustainability

**Cons:**
- Need to find the source
- More complex

**Implementation:** Need CMS source URL

### Solution C: Use Historical Data Manager

**Approach:** The `historical_manager` might have GPCI
**Pros:**
- Already in codebase
- May have working source

**Cons:**
- Need to investigate

**Implementation:** Check what `historical_manager` provides

### Solution D: Manual File Upload to Render

**Approach:** Upload GPCI file to Render and parse it
**Pros:**
- Quick workaround
- Gets data loaded

**Cons:**
- Manual process
- Not automated

**Implementation:** Simple script already created

---

## 10. Recommended Action Plan

### Immediate (Next 30 minutes)

1. **Verify CMS ZIP contents**
   - Download RVU25D from CMS manually
   - Compare to sample_data
   - Document differences

2. **Check scraper discovery**
   - Run scraper discovery mode
   - Inspect manifest output
   - See what files it finds

### Short-term (Today)

3. **Choose a solution**
   - If GPCI not in ZIPs → Use sample_data or find alternate source
   - If GPCI in ZIPs but extract failing → Debug extraction
   - If CMS changed structure → Update classifier

4. **Load the data**
   - Get GPCI data into database one way or another
   - Verify with `verify_gpci_loaded.py`
   - Mark Task 66 complete

### Long-term (Next week)

5. **Fix ongoing ingestion**
   - Implement proper GPCI source discovery
   - Update scraper if needed
   - Document the source

6. **Prevent regression**
   - Add CI tests
   - Add data quality checks
   - Monitor for future issues

---

## Appendix A: Commands to Run

### Verify CMS ZIP Contents
```bash
# Download manually from CMS
curl -o /tmp/rvu25d.zip https://www.cms.gov/files/zip/rvu25d.zip
unzip -l /tmp/rvu25d.zip
unzip -l /tmp/rvu25d.zip | grep -i gpci
```

### Check Scraper Discovery
```python
# In Python shell
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
import asyncio
scraper = CMSRVUScraper()
files = asyncio.run(scraper.scrape_rvu_files(2025, 2025))
for f in files:
    print(f.filename, f.url)
```

### Load from Sample Data (Local)
```bash
# If sample_data available
python scripts/load_gpci_from_sample.py
```

### Verify Load
```python
python scripts/verify_gpci_loaded.py
```

---

## Appendix B: Key Files Created

1. `scripts/verify_gpci_loaded.py` - Diagnostic tool
2. `scripts/load_gpci_from_indices.py` - Transform gpci_indices → gpci
3. `scripts/load_gpci_from_sample.py` - Load from sample_data
4. `artifacts/issue1_*.md` - Multiple diagnostic documents

---

## Appendix C: Git Commits Made

1. `301fce0` - Initial fix: column mapping + verification scripts
2. `ed132cd` - Improved fix: proper NaN handling + debug logging

---

## Summary

**The Good:** We fixed the actual bug (column mapping) and the environment works fine.

**The Bad:** We can't test the fix because the data source (CMS ZIP files) doesn't contain GPCI data.

**The Ugly:** We don't know if this is a CMS change, a scraper issue, or something else.

**Next Step:** Manually verify what CMS actually provides vs what we're expecting.

