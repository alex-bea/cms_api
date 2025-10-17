# GPCI Parser Implementation: Lessons Learned

**Date:** 2025-10-17  
**Context:** Implementing GPCI parser from 60% to near-100% test pass rate

---

## 🎓 **Key Lessons for Future Parser PRDs**

### **1. Test Fixtures Must Match Real CMS Data Format**

**What Happened:**
- CSV fixtures had 2 header rows (title + empty + headers) that weren't documented
- Parser defaulted to reading from row 1, causing column misalignment
- Resulted in `ValueError: could not convert string to float: '025 M'` (MAC data in GPCI column)

**PRD Improvement:**
```markdown
## Test Fixtures Requirements
- Document exact CMS file structure (header rows, footers, metadata lines)
- Include representative samples showing:
  - Header row count and format
  - Data start line
  - Empty rows or separators
- Add fixture provenance section with source file details
```

**Impact:** Prevents 50%+ of debugging time on format mismatches

---

### **2. Alias Maps Need Comprehensive CMS Header Variations**

**What Happened:**
- CMS uses different header formats across years: `"PW GPCI"` vs `"2025 PW GPCI (with 1.0 Floor)"`
- Alias map only had generic variations, not year-specific ones
- Column mapping failed silently, producing invalid data

**PRD Improvement:**
```markdown
## Header Normalization Strategy
- Document all known CMS header variations per dataset
- Include historical header formats (e.g., 2023, 2024, 2025)
- Test against multiple year fixtures to validate alias map
- Add validation warnings for unmapped columns
```

**Impact:** Reduces parser brittleness across CMS data updates

---

### **3. Type Handling Must Be Defensive**

**What Happened:**
- `Decimal(str(x))` failed on integer strings like `"1"` (needs `"1.000"`)
- CSV had integers (`1`, `1.5`) while TXT had formatted decimals (`1.000`, `1.500`)
- Required casting through `float()` first for robust parsing

**PRD Improvement:**
```markdown
## Data Type Casting Requirements
- Document expected input formats AND common variations
- Specify handling for:
  - Empty strings → NaN
  - Integer strings → Decimal
  - Scientific notation → Decimal
  - Whitespace/special characters
- Add explicit error handling with actionable messages
- Test with multiple format variations (CSV, XLSX, TXT)
```

**Impact:** Prevents production failures on format variations

---

### **4. Format Detection Should Be Content-Based, Not Extension-Based**

**What Happened:**
- ZIP file contained `GPCI2025_sample.txt` but was parsed as CSV
- Extension-only detection (`.txt` → "assume CSV") failed
- Required regex pattern matching (`r'^\d{5}'` for MAC code) to identify fixed-width

**PRD Improvement:**
```markdown
## Format Detection Strategy
1. **Extension check** (fast path): .xlsx → Excel, .csv → CSV
2. **Content sniffing** (fallback): Check first 500 bytes for:
   - Fixed-width patterns (e.g., MAC code `^\d{5}`)
   - Delimiter detection (commas, tabs, pipes)
   - Layout registry lookup
3. **Explicit fallback order**: Fixed-width → CSV → Error

Document detection heuristics and edge cases.
```

**Impact:** Handles ambiguous file formats correctly

---

### **5. Error Messages Should Be Actionable**

**What Happened:**
- Generic errors like `decimal.InvalidOperation` didn't indicate root cause
- No guidance on fixing (e.g., "Check header rows" or "Verify column mapping")
- Required deep debugging to identify issues

**PRD Improvement:**
```markdown
## Error Handling Standards
- Every error must include:
  1. **What failed:** "Failed to convert 'gpci_work' column"
  2. **Why it failed:** "Value '025 M' is not numeric"
  3. **How to fix:** "Check column mapping or header row skipping"
  4. **Context:** Filename, row number, column name
- Add validation checkpoints with clear messages
- Log intermediate states for debugging
```

**Impact:** Reduces debugging time by 70%+

---

### **6. Pre-Implementation Validation is Critical**

**What Happened:**
- Assumed CSV format matched TXT format
- Didn't verify header structure before implementation
- Cost ~2 hours of debugging after implementation

**PRD Improvement:**
```markdown
## Pre-Implementation Checklist
Required before coding:
1. ✅ Inspect all source file formats (TXT, CSV, XLSX, ZIP)
2. ✅ Document header structure for each format
3. ✅ Verify column positions/names match across formats
4. ✅ Test layout registry against real data
5. ✅ Validate sample parsing with pandas before implementation
6. ✅ Document CMS-specific quirks (floors, special values, etc.)

Add "Format Verification" step to all parser PRDs.
```

**Impact:** Prevents rework and ensures correct first implementation

---

### **7. CMS Data Quirks Need Explicit Documentation**

**What Happened:**
- CMS GPCI files have 1.0 work GPCI floor (not in raw data)
- Alaska has 1.5 floor (documented in regulations, not files)
- Multiple header rows in CSV/XLSX (not in TXT)
- MAC codes vs Locality codes (different meanings)

**PRD Improvement:**
```markdown
## CMS Dataset Characteristics Section
Required for each dataset PRD:

### File Format Variations
- TXT: Fixed-width, no headers, data starts line X
- CSV: 2 header rows (title + empty), then column headers
- XLSX: Similar to CSV, may have duplicate header rows

### Business Rules (Non-Obvious)
- GPCI work floor: 1.0 (all localities)
- Alaska exception: 1.5 floor (locality 01)
- MAC codes: 5 digits, not same as locality codes
- Effective dates: Derived from quarter (A=Jan, B=Apr, C=Jul, D=Oct)

### Known Data Issues
- Duplicate locality 00 (Alabama + Arizona in some releases)
- Occasional missing columns (handle gracefully)
- Whitespace variations in headers
```

**Impact:** Reduces domain knowledge gaps, improves parser robustness

---

### **8. Test Coverage Should Include Format-Specific Edge Cases**

**What Happened:**
- TXT tests passed, CSV/XLSX tests failed
- Didn't test empty string handling until production
- Metrics calculation failed on empty GPCI values

**PRD Improvement:**
```markdown
## Test Strategy Requirements
Per format (TXT, CSV, XLSX, ZIP):
1. **Golden tests:** Valid data, all columns present
2. **Edge cases:**
   - Empty strings in numeric columns
   - Missing optional columns
   - Duplicate keys
   - Out-of-range values
3. **Format-specific:**
   - CSV: Multiple header rows, quoted fields
   - XLSX: Excel number formatting, date coercion
   - ZIP: Multiple inner files, nested ZIPs
   - TXT: Fixed-width misalignment, truncated lines

Require 80%+ test coverage before PR approval.
```

**Impact:** Catches format-specific bugs before production

---

### **9. Metrics Calculation Should Handle Missing/Empty Data**

**What Happened:**
- `float(df['gpci_work'].min())` failed when column had empty strings
- No null checks before type conversion
- Required filtering: `df[df['gpci_work'] != '']`

**PRD Improvement:**
```markdown
## Metrics Calculation Standards
- Always filter null/empty values before aggregation:
  ```python
  valid_values = df[df['column'] != '']['column']
  min_val = float(valid_values.min()) if len(valid_values) > 0 else None
  ```
- Document expected metrics and their null handling
- Add validation for metric sanity (e.g., GPCI range 0.5-2.0)
- Log warnings for unexpected metric values
```

**Impact:** Prevents runtime errors on edge case data

---

### **10. Incremental Testing is More Efficient Than Big-Bang Testing**

**What Happened:**
- Implemented full parser (510 lines) before running any tests
- Hit 4 different issues simultaneously
- Would have been faster to test CSV parsing alone first

**PRD Improvement:**
```markdown
## Implementation Phasing Strategy
Recommend incremental implementation with testing at each phase:

**Phase 1:** Single format (TXT) + core logic
- Parse fixed-width
- Column normalization
- Type casting
- **Test checkpoint:** 100% TXT tests passing

**Phase 2:** Add CSV/XLSX support
- Extend format detection
- Add header skipping
- Update alias map
- **Test checkpoint:** 100% CSV/XLSX tests passing

**Phase 3:** Add ZIP support
- Inner file extraction
- Format detection on inner file
- **Test checkpoint:** 100% ZIP tests passing

**Phase 4:** Edge cases + integration
- Negative tests
- Payment spot-check
- **Test checkpoint:** 100% all tests passing

Benefits: Faster debugging, clearer error isolation, incremental progress
```

**Impact:** Reduces overall implementation + debugging time by 40%

---

## 📋 **Action Items for PRD Template Updates**

1. **Add "Format Verification" section** to all parser PRDs
2. **Create "CMS Dataset Characteristics" template** for domain knowledge
3. **Expand "Error Handling" section** with actionable message requirements
4. **Add "Test Coverage Matrix"** showing format × edge case grid
5. **Include "Pre-Implementation Checklist"** in parser standards
6. **Document "Incremental Implementation"** best practice
7. **Add "Metrics Calculation Standards"** to parser contracts
8. **Create "Common CMS Quirks"** reference document

---

## 🎯 **Success Metrics**

Track these improvements in future parser implementations:
- **Time to first working test:** Target < 2 hours
- **Debugging time after implementation:** Target < 1 hour
- **Test pass rate on first run:** Target > 80%
- **PRD completeness score:** Target 100% (all sections filled)
- **Format variation handling:** Target 100% (all formats work)

---

**Overall Impact:** These improvements should reduce total parser implementation + debugging time from ~8 hours to ~4 hours.

