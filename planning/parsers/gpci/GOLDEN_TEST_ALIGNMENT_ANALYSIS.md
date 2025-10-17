# Golden Test Alignment Analysis

**Date:** 2025-10-17  
**Issue:** Are GPCI golden tests aligned with STD-qa-testing-prd-v1.0.md standards?

---

## 🔍 **Comparison: CF Parser (Standard) vs GPCI Parser (Current)**

### **CF Parser Golden Tests (Reference Standard)**

**Pattern from `test_conversion_factor_parser_golden.py`:**
```python
assert len(result.data) == 2, "Expected 2 rows (physician + anesthesia)"
assert len(result.rejects) == 0, "No rejects expected for valid data"

# Exact value validation
physician = result.data[result.data['cf_type'] == 'physician'].iloc[0]
assert physician['cf_value'] == '32.3465'  # Exact match, deterministic
```

**Characteristics:**
- ✅ **Deterministic:** Exact row counts (`== 2`, not `>= 2`)
- ✅ **Clean fixtures:** No rejects expected (`== 0`)
- ✅ **Exact values:** Specific value assertions
- ✅ **Same across formats:** All formats produce identical output

---

### **GPCI Parser Golden Tests (Current)**

**Pattern from `test_gpci_parser_golden.py`:**
```python
# TXT
assert len(result.data) == 18, "Expected 18 rows (20 - 2 duplicates)"
assert len(result.rejects) == 2, "Expected 2 duplicate rejects"

# CSV
assert len(result.data) == 16, "Expected 16 rows (18 - 2 duplicates)"
assert len(result.rejects) == 2

# XLSX
assert len(result.data) >= 20, "At least 20 unique localities"  # ← FLEXIBLE
assert len(result.rejects) > 0

# Consistency
assert len(overlapping) >= 10  # ← VERY FLEXIBLE
```

**Characteristics:**
- ❌ **Non-deterministic:** Flexible counts (`>= 20`)
- ❌ **Dirty fixtures:** Rejects expected due to duplicates
- ⚠️ **Different data:** TXT has 20 rows, CSV has 18 rows, XLSX has 113 rows
- ⚠️ **Inconsistent:** Overlapping localities instead of exact match

---

## 🚨 **QA Standard Violations**

### **1. Fixtures Should Be Clean (No Expected Rejects)**

**STD-qa-testing-prd-v1.0.md §5.1:**
> "Maintain comprehensive golden datasets per dataset version with manifest.yaml.  
> Validate goldens against schema contracts before use."

**Current Issue:**
- Our TXT/CSV fixtures have **duplicate locality code 00** (Alabama + Arizona)
- Tests **expect rejects** (`assert len(result.rejects) == 2`)
- This violates golden test principle: clean input → clean output

**Standard Pattern:**
```python
# CF Parser (CORRECT)
assert len(result.rejects) == 0, "No rejects expected for valid data"

# GPCI Parser (WRONG)
assert len(result.rejects) == 2, "Expected 2 duplicate rejects"
```

---

### **2. Fixtures Should Be Identical Across Formats**

**STD-qa-testing-prd-v1.0.md §2 (Definitions):**
> "Golden Dataset: Canonical fixture archived under `/fixtures/golden/<domain>/` with manifest + checksum."

**Current Issue:**
- **TXT:** 20 rows (Alabama + Alaska + Arizona + others)
- **CSV:** 18 rows (different sample, missing 2 localities)
- **XLSX:** 113 rows (full dataset)
- ZIP: Contains TXT (20 rows)

**Standard Pattern:**
- All formats should have **identical data**
- Tests should verify **exact match across formats**
- Consistency tests should assert `txt_result.data == csv_result.data`

---

### **3. Test Expectations Should Be Deterministic**

**STD-qa-testing-prd-v1.0.md §1 (Goals):**
> "Ensure deterministic, reproducible test runs with source-controlled fixtures and golden data."

**Current Issue:**
```python
# GPCI (TOO FLEXIBLE)
assert len(result.data) >= 20  # Could be 20, 21, 100, etc.
assert len(overlapping) >= 10  # Could be 10, 11, 18, etc.

# CF Parser (DETERMINISTIC)
assert len(result.data) == 2  # Exactly 2, always
```

**Standard Pattern:**
- Use `==` for exact matches
- No `>=` or `<=` unless explicitly testing boundaries
- Fixed expectations enable regression detection

---

### **4. Skip Flags Violate Production Parity**

**Current Issue:**
```python
TEST_METADATA = {
    ...
    'skip_row_count_validation': True,  # ← TEST-ONLY FLAG
}
```

**Problem:**
- Test mode uses different code path than production
- Row count validation is bypassed in tests but runs in production
- Could miss bugs in validation logic

**Standard Pattern:**
- No test-only flags or modes
- Tests exercise exact production code path
- If validation is correct, it should pass on good fixtures

---

## ✅ **Recommended Fixes**

### **Fix 1: Clean Up Fixtures (Remove Duplicates)**

**Action:**
```bash
# Create clean fixtures without duplicate locality 00
# Remove Arizona locality 00, keep only Alabama
# Or remove both and add unique localities

# TXT: 18 unique rows (no duplicates)
# CSV: Same 18 rows as TXT
# XLSX: Can keep full dataset OR use same 18 rows
# ZIP: Contains same TXT as golden TXT
```

**Result:**
```python
# TXT
assert len(result.data) == 18, "Expected 18 rows"
assert len(result.rejects) == 0, "No rejects expected for clean fixture"

# CSV (same data as TXT)
assert len(result.data) == 18, "Expected 18 rows"
assert len(result.rejects) == 0

# Consistency
assert txt_result.data.equals(csv_result.data), "TXT and CSV must match exactly"
```

---

### **Fix 2: Remove Test-Only Flags**

**Action:**
```python
# Remove from TEST_METADATA
# 'skip_row_count_validation': True,  # ← DELETE THIS

# If 18 rows is valid, adjust row count threshold
# In gpci_parser.py:
if count < 10:  # Changed from 90 to allow small test fixtures
    raise ParseError(...)
elif count < 100:
    logger.warning(f"Low row count: {count} (expected 100-120)")
```

**Or better:** Keep validation strict, use realistic fixture size (100+ rows)

---

### **Fix 3: Make XLSX Fixture Match Others**

**Action:**
```bash
# Option A: Use same 18 rows across all formats
head -21 sample_data/rvu25d_0/GPCI2025.txt > GPCI2025_sample.txt  # 18 unique
# Convert to CSV with same 18 rows
# Create XLSX with same 18 rows

# Option B: Use full dataset across all formats (100+ rows)
cp sample_data/rvu25d_0/GPCI2025.txt tests/fixtures/gpci/golden/
# Convert to CSV/XLSX from full file
```

**Result:**
```python
# All formats have identical data
assert len(txt_result.data) == len(csv_result.data) == len(xlsx_result.data)
assert txt_localities == csv_localities == xlsx_localities
```

---

## 📊 **Alignment Assessment**

| Requirement | CF Parser (Standard) | GPCI Parser (Current) | Aligned? |
|-------------|---------------------|----------------------|----------|
| **Deterministic counts** | ✅ Exact (`== 2`) | ❌ Flexible (`>= 20`) | ❌ NO |
| **Clean fixtures** | ✅ No rejects | ❌ Expects rejects | ❌ NO |
| **Same data across formats** | ✅ Identical | ❌ Different rows | ❌ NO |
| **No test-only flags** | ✅ None | ❌ `skip_row_count_validation` | ❌ NO |
| **Exact value validation** | ✅ `== '32.3465'` | ✅ `== '1.500'` | ✅ YES |
| **Hash determinism** | ✅ Verified | ✅ Verified | ✅ YES |
| **Schema compliance** | ✅ All columns | ✅ All columns | ✅ YES |
| **Provenance injection** | ✅ Metadata cols | ✅ Metadata cols | ✅ YES |

**Alignment Score:** 4/8 (50%) ❌

---

## 🎯 **Recommended Action Plan**

### **Option A: Quick Fix (Minimal Changes - 30 min)**
1. Remove duplicate locality 00 from fixtures (pick Alabama OR Arizona, not both)
2. Make TXT/CSV have identical 18 rows
3. Update tests to expect `== 18` and `rejects == 0`
4. Remove `skip_row_count_validation` flag
5. Keep XLSX as full dataset (document as separate test)

**Pros:** Fast, maintains most current work  
**Cons:** Still not 100% aligned (XLSX different)

---

### **Option B: Full Alignment (Proper Fix - 1-2 hours)**
1. Create clean 18-row sample across ALL formats (TXT, CSV, XLSX, ZIP)
2. Ensure all formats have **identical data**
3. Update all tests to expect:
   - Exact row counts (`== 18`)
   - Zero rejects (`== 0`)
   - Identical output across formats
4. Remove all flexible assertions (`>=`, overlapping checks)
5. Remove `skip_row_count_validation` flag
6. Adjust row count threshold to `< 10` for test fixtures

**Pros:** 100% aligned with standards, proper golden test pattern  
**Cons:** More rework, need to recreate fixtures

---

## 💡 **Recommendation**

**I recommend Option B (Full Alignment)** because:

1. **Standards Compliance:** We have the standards written, we should follow them
2. **Future Parsers:** Establishes correct pattern for OPPSCAP, Locality, etc.
3. **Deterministic Testing:** Removes ambiguity, enables true regression detection
4. **PRD Improvement:** This validates our lesson #1 (Test fixtures must match format)

**Time Investment:** 1-2 hours now saves 2-3 hours per future parser

---

## 📋 **What This Means for PRD Improvements**

**Add to Task #1 (Format Verification):**
> "Golden fixtures must be clean (zero expected rejects) and identical across all formats (TXT, CSV, XLSX, ZIP). Document exact row counts and verify fixtures have no duplicates, missing values, or out-of-range data."

**Add to Task #2 (CMS Characteristics):**
> "Document which locality codes may appear as duplicates in raw CMS data (e.g., locality 00 shared by Alabama and Arizona). Golden fixtures should resolve these to unique entries or exclude them entirely."

**Add to Task #8 (Format Detection):**
> "Golden test fixtures must have identical data across all formats to enable exact output comparison and true format consistency validation."

---

**Decision Point:** Should we fix the GPCI fixtures now for proper alignment, or accept current state and apply learnings to next parser?

