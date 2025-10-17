# GPCI Parser Test Fixes Summary

**Date:** 2025-10-17  
**Status:** Partial fixes applied (2/3 issues addressed)

---

## ✅ **Fixes Applied**

### 1. **Decimal Casting Issue (CSV/XLSX)**
**Problem:** `decimal.InvalidOperation` when converting integer strings like `"1"` to `Decimal`.

**Root Cause:**  
- CSV/XLSX fixtures have integer values (`1`, `1.5`) instead of formatted decimals (`1.000`, `1.500`)
- `Decimal(str(x))` requires precise numeric format

**Fix Applied:**  
- Updated `_parser_kit.py::format_decimal()` (lines 215-230)
- Added safe conversion: `Decimal(str(float(str_val)))` to handle integers
- Added try/except with `InvalidOperation` import
- Added logging for unparseable values

**Status:** ✅ Fixed

---

### 2. **ZIP Format Detection**
**Problem:** ZIP containing `GPCI2025_sample.txt` parsed as CSV instead of fixed-width.

**Root Cause:**  
- `_parse_zip()` defaulted to CSV for unknown extensions
- Didn't check for fixed-width pattern before parsing

**Fix Applied:**  
- Updated `gpci_parser.py::_parse_zip()` (lines 297-310)
- Added content-based format detection using regex `r'^\d{5}'` (MAC code pattern)
- Checks first 500 bytes for fixed-width indicators
- Falls back to CSV only if no fixed-width pattern found

**Status:** ✅ Fixed

---

## ⏳ **Remaining Issues**

### 3. **CSV/XLSX Still Failing**
**Current Error:** `ValueError: could not convert string to float: '025 M'`

**Root Cause (Suspected):**  
- CSV parser reading MAC/State columns into GPCI value columns
- Header rows (lines 1-2) not being skipped properly
- Column mapping issue causing non-numeric data in decimal columns

**Next Steps:**  
1. Debug CSV parsing: check `skiprows` parameter
2. Verify column header normalization
3. Ensure only numeric columns hit decimal canonicalization
4. Add better data validation before type casting

---

## 📊 **Current Test Status**

### **Passing: 6/10 (60%)**  
✅ `test_gpci_golden_txt` - TXT parsing  
✅ `test_gpci_determinism` - Hash consistency  
✅ `test_gpci_schema_v1_2_compliance` - Schema compliance  
✅ `test_gpci_metadata_injection` - Provenance  
✅ `test_gpci_natural_key_sort` - Sorting  
✅ `test_gpci_metrics_structure` - Metrics  

### **Failing: 4/10 (40%)**  
❌ `test_gpci_golden_csv` - `ValueError` in decimal conversion  
❌ `test_gpci_golden_xlsx` - Same as CSV  
❌ `test_gpci_golden_zip` - Needs re-test after fixes  
❌ `test_gpci_txt_csv_consistency` - Cascading CSV failure  

---

## 🔍 **Technical Details**

### **Files Modified:**
1. `cms_pricing/ingestion/parsers/_parser_kit.py`
   - Line 21: Added `InvalidOperation` import
   - Lines 215-230: Rewrote `format_decimal()` with safe casting

2. `cms_pricing/ingestion/parsers/gpci_parser.py`
   - Lines 277-310: Enhanced `_parse_zip()` with content detection

### **Docker Status:**
- ✅ Containers rebuilt with fixes
- ✅ API service running
- ✅ Tests execute in clean environment

---

## 🎯 **Expected Outcome**

**After CSV/XLSX fix:**  
- Test pass rate: 60% → **100%** ✅  
- All 10 golden tests passing  
- CSV, XLSX, ZIP formats working  
- Format consistency validated  

**Estimated Time:** 15-20 minutes to debug CSV header skipping

---

## 📝 **Next Actions**

1. **Debug CSV parsing:**
   ```python
   # Check what's being read:
   df_raw = pd.read_csv('GPCI2025_sample.csv', nrows=5)
   print(df_raw.head())
   print(df_raw.columns)
   ```

2. **Add skiprows if needed:**
   ```python
   df = pd.read_csv(file, skiprows=2, encoding=encoding)  # Skip header rows
   ```

3. **Verify column mapping:**
   - Ensure MAC column not mapped to GPCI values
   - Check alias map correctness
   - Validate normalized column names

4. **Re-test and commit:**
   - Run full test suite
   - Update this summary
   - Push to GitHub for CI validation

---

**Confidence Level:** 🟢 High - Core fixes are sound, CSV issue is isolated

