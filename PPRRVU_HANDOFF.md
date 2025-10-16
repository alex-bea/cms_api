# PPRRVU Parser - Handoff Document

**Status:** 90% Complete, Ready for Final Debug  
**Last Updated:** 2025-10-16  
**Next Session:** 10-15 minutes to completion  

---

## 🎯 Quick Start (Next Session)

### Option A: Debug & Fix (Recommended, 15 min)

```bash
# 1. Add logging (see Step 1 in debug plan below)
# 2. Run test with logging
pytest tests/ingestion/test_pprrvu_parser.py::test_pprrvu_golden_fixture -xvs --log-cli-level=INFO 2>&1 | grep "\[DEBUG\]"

# 3. Fix based on output (likely one of these):
#    - Skip normalization for fixed-width (columns already canonical)
#    - Fix header row filtering
#    - Fix dtype casting

# 4. Verify
pytest tests/ingestion/test_pprrvu_parser.py -v

# 5. Commit & push
git add -A
git commit -m "fix(parser): PPRRVU fixed-width parsing - [describe fix]"
git push
```

### Option B: CSV-First (Fallback, 10 min)

```bash
# 1. Convert fixture to CSV
python << 'PYTHON'
import pandas as pd
df = pd.read_fwf('tests/fixtures/pprrvu/golden/PPRRVU2025_sample.txt', skiprows=7)
df.to_csv('tests/fixtures/pprrvu/golden/PPRRVU2025_sample.csv', index=False)
PYTHON

# 2. Update test to use CSV fixture
# 3. Simplify parser to CSV-only
# 4. Run tests - should pass
# 5. Commit & push
```

---

## 📊 Current State

### ✅ Completed & Committed

**3 Commits on GitHub:**
- `f362013` - Phase 0 lockdown (v0.1.0-phase0 tag)
- `0b6d892` - Phase 0 enhancements + PPRRVU fixtures
- `53c0886` - PPRRVU parser WIP (current commit)

**34 Tests Passing:**
- 26 Phase 0 parser kit tests
- 8 error type tests

**Infrastructure:**
- Custom error types (5 exceptions)
- Enhanced uniqueness check (configurable severity)
- Golden fixture (94 rows, SHA-256 pinned)
- 4 negative fixtures
- Complete documentation

### 🚧 In Progress

**PPRRVU Parser:** 420 lines, 90% complete

**Blocking Issue:** `KeyError: 'hcpcs'` after layout parsing

**7 Tests Written:** Awaiting parser fix

---

## 🔍 Debugging Plan

### Step 1: Add Logging (2 min)

Edit `cms_pricing/ingestion/parsers/pprrvu_parser.py`:

```python
# After Step 2 (line ~118):
df = _parse_fixed_width(content_clean, encoding, metadata)
logger.info(f"[DEBUG] After parsing: shape={df.shape}, columns={list(df.columns)}")

# After Step 3 (line ~128):
df = _normalize_column_names(df)
logger.info(f"[DEBUG] After normalize: shape={df.shape}, columns={list(df.columns)}")

# After Step 4 (line ~131):
df = _cast_dtypes(df, metadata)
logger.info(f"[DEBUG] After cast: shape={df.shape}, columns={list(df.columns)}")
```

In `_parse_fixed_width()` (line ~268):

```python
# Before return:
logger.info(f"[DEBUG] Parsed {len(records)} records")
if records:
    logger.info(f"[DEBUG] First record: {list(records[0].keys())[:5]}...")
logger.info(f"[DEBUG] DataFrame: shape={df.shape}, columns={list(df.columns)[:5]}...")
return df
```

### Step 2: Run & Analyze (2 min)

```bash
pytest tests/ingestion/test_pprrvu_parser.py::test_pprrvu_golden_fixture -xvs --log-cli-level=INFO 2>&1 | grep "\[DEBUG\]"
```

### Step 3: Apply Fix (3 min)

**Most likely:** Column normalization issue

Layout columns are already lowercase (`hcpcs`, not `HCPCS`), so normalization might break them or return empty result.

**Fix:**
```python
def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names - skip if already canonical."""
    # Check if columns already match schema (from fixed-width layout)
    canonical_cols = {'hcpcs', 'modifier', 'status_code', 'work_rvu', 'pe_rvu_nonfac', 'pe_rvu_fac', 'mp_rvu'}
    
    if canonical_cols.issubset(set(df.columns)):
        # Columns already canonical (from layout registry)
        logger.debug("Columns already canonical, skipping normalization")
        return df
    
    # Otherwise apply COLUMN_ALIASES (for CSV/XLSX)
    # ... existing logic ...
```

### Step 4-7: Verify, Test, Commit (5 min)

- Run golden test (should pass)
- Run all 7 tests (should pass)
- Commit with descriptive message
- Push to GitHub

---

## 📁 File Locations

**Parser:**
- `cms_pricing/ingestion/parsers/pprrvu_parser.py`

**Tests:**
- `tests/ingestion/test_pprrvu_parser.py`
- `tests/test_parser_kit_errors.py`

**Fixtures:**
- `tests/fixtures/pprrvu/golden/PPRRVU2025_sample.txt`
- `tests/fixtures/pprrvu/golden/README.md`
- `tests/fixtures/pprrvu/bad_layout.txt`
- `tests/fixtures/pprrvu/bad_dup_keys.txt`
- `tests/fixtures/pprrvu/bad_category.txt`
- `tests/fixtures/pprrvu/bad_schema_regression.csv`

**Documentation:**
- `README_PPRRVU.md`
- `CHANGELOG.md`
- `/tmp/PPRRVU_SESSION_SUMMARY.md`
- `/tmp/PPRRVU_DEBUG_PLAN.md` (this file)

---

## 🎓 Key Learnings

### Parser Error Types
- Create explicit exceptions (not generic ValueError)
- Store context: `DuplicateKeyError(msg, duplicates=[...])`
- Clear hierarchy: All inherit from `ParseError`

### Layout Registry
- Signature: `get_layout(product_year, quarter_vintage, dataset)`
- Lookup key: `(dataset, year, quarter)` tuple
- Quarter extraction: "2025Q4" → "Q4"
- Returns lowercase column names (already canonical)

### Schema Files
- Filename: `cms_pprrvu_v1.0.json`
- Internal version: `1.1`
- Parser strips minor: `cms_pprrvu_v1.1` → `cms_pprrvu_v1.0.json`

### Natural Key Severity
- PPRRVU: `severity=BLOCK` (hard-fail on duplicates)
- Other parsers: `severity=WARN` (soft-fail, return rejects)
- Configurable per dataset

---

## 📚 References

**Contracts:**
- STD-parser-contracts v1.2: `prds/STD-parser-contracts-prd-v1.0.md`
- Schema: `cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json`
- Layout: `cms_pricing/ingestion/parsers/layout_registry.py` (PPRRVU_2025D_LAYOUT)

**Natural Keys:**
```python
['hcpcs', 'modifier', 'status_code', 'effective_from']
```

**RVU Columns (precision=2, HALF_UP):**
```python
['work_rvu', 'pe_rvu_nonfac', 'pe_rvu_fac', 'mp_rvu']
```

---

## ⚡ Quick Diagnostic

Run this to see current state:

```bash
cd /Users/alexanderbea/Cursor/cms-api

# Check layout works
python -c "from cms_pricing.ingestion.parsers.layout_registry import get_layout; \
  layout = get_layout('2025', '2025Q4', 'pprrvu'); \
  print(f'Layout: {layout is not None}'); \
  print(f'Columns: {list(layout[\"columns\"].keys())[:5]}...')"

# Check schema works
python -c "import json; \
  s = json.load(open('cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json')); \
  print(f'Version: {s[\"version\"]}'); \
  print(f'Natural keys: {s[\"natural_keys\"]}')"

# Try minimal parse
python -c "
from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu
from datetime import datetime
metadata = {'release_id': 'test', 'product_year': '2025', 
            'quarter_vintage': '2025Q4', 'vintage_date': datetime(2025,10,1),
            'schema_id': 'cms_pprrvu_v1.1', 'file_sha256': 'abc', 
            'layout_version': 'v2025.4.0'}
with open('tests/fixtures/pprrvu/golden/PPRRVU2025_sample.txt', 'rb') as f:
    result = parse_pprrvu(f, 'test.txt', metadata)
print(f'Success: {len(result.data)} rows')
"
```

---

## 🚀 After PPRRVU Complete

**Then proceed with:**
1. Update STD-parser-contracts v1.3 (30 min) - Document error types, severity, schema naming
2. Conversion Factor parser (2 hours)
3. GPCI parser (1.25 hours)
4. ANES parser (1 hour)
5. OPPSCAP parser (1.25 hours)
6. Locality parser (1 hour)

**Total Phase 1:** ~8 hours remaining (after PPRRVU)

---

**You're ready! The hard work is done - just one final debug iteration needed.** 🎯

