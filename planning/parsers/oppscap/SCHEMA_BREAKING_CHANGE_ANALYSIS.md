# OPPSCAP Schema v1.0 → v1.1 Breaking Change Analysis

**Date:** 2025-10-20  
**Change Type:** BREAKING (column schema changes)  
**Severity:** LOW (fixing bug before production use)

---

## What Changed

### Schema v1.0 (Incorrect - Never Worked)
```json
{
  "columns": {
    "opps_cap_applies": {"type": "bool"},
    "cap_amount_usd": {"type": "float64"},
    "cap_method": {"type": "str", "domain": ["APC", "HCPCS", "CUSTOM"]}
  },
  "natural_keys": ["hcpcs", "modifier", "effective_from"]
}
```

### Schema v1.1 (Correct - Matches Actual File)
```json
{
  "columns": {
    "hcpcs": {"type": "str"},
    "modifier": {"type": "str"},
    "status": {"type": "str"},
    "mac": {"type": "str"},
    "locality_code": {"type": "str"},
    "facility_price": {"type": "Decimal"},
    "nonfacility_price": {"type": "Decimal"}
  },
  "natural_keys": ["hcpcs", "modifier", "mac", "locality_code"]
}
```

---

## Impact Assessment

### ✅ GOOD NEWS: Low Impact (Pre-Production)

**Database Model ALREADY CORRECT:**
- File: `cms_pricing/models/rvu.py:113-141`
- Columns: `hcpcs_code`, `modifier`, `proc_status`, `mac`, `locality_id`, `price_fac`, `price_nonfac`
- **Matches v1.1 schema (and actual file structure)!**
- Table: `opps_caps` with correct columns already defined

**v1.0 Schema Never Actually Used:**
- Created before file inspection (spec-driven, not data-driven)
- Columns don't exist in real CMS file
- Parser would have failed immediately if attempted
- No production data exists with v1.0 structure

**Conclusion:** v1.1 is a **BUG FIX**, not a breaking change in practice

---

## Components Affected

### 1. Database Model - ✅ NO CHANGE NEEDED

**File:** `cms_pricing/models/rvu.py:113-141`

**Current State:** CORRECT
- Columns already match v1.1 schema
- `price_fac`, `price_nonfac` (not `cap_amount_usd`)
- `proc_status`, `mac`, `locality_id` already present
- Natural key index: `idx_opps_mac_locality` on (mac, locality_id) ✅

**Action:** None - model is correct

---

### 2. Ingestor Code - ⚠️ NEEDS UPDATE

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Line 375-378:** Schema registration using v1.0
```python
oppscap_schema = SchemaContract(
    dataset_name="cms_oppscap",
    version="1.0",  # ← Change to "1.1"
    ...
)
```
**Fix:** Change version to "1.1"

**Line 1437-1442:** Column mapping expects v1.0 columns (WRONG)
```python
column_mapping = {
    'HCPCS': 'hcpcs',
    'MODIFIER': 'modifier',
    'CAP_APPLIES': 'opps_cap_applies',  # ← Column doesn't exist in file!
    'CAP_AMOUNT': 'cap_amount_usd',      # ← Column doesn't exist in file!
    'CAP_METHOD': 'cap_method'           # ← Column doesn't exist in file!
}
```

**Fix:** Update to v1.1 columns
```python
column_mapping = {
    'HCPCS': 'hcpcs',
    'MOD': 'modifier',
    'PROCSTAT': 'status',
    'CARRIER': 'mac',
    'LOCALITY': 'locality_code',
    'FACILITY PRICE': 'facility_price',
    'NON-FACILTY PRICE': 'nonfacility_price',  # CMS typo
    'NON-FACILITY PRICE': 'nonfacility_price',  # Correct spelling
}
```

**Action:** Update `_normalize_oppscap_columns()` method

---

### 3. Parser Implementation - ℹ️ NOT YET CREATED

**File:** `cms_pricing/ingestion/parsers/oppscap_parser.py`

**Current State:** DOES NOT EXIST
- No parser file exists yet
- This is Phase 2 work (upcoming)

**Action:** Create parser using v1.1 schema

---

### 4. Tests - ⚠️ NEEDS UPDATE

**File:** `tests/ingestors/test_rvu_parsers.py`

Need to check if tests exist for OPPSCAP and update to v1.1 expectations.

---

### 5. API/Downstream Consumers - ✅ NO IMPACT

**Current State:** No production OPPSCAP data exists
- Parser never completed (no `oppscap_parser.py`)
- Ingestor code exists but never successfully parsed real files
- No production data in `opps_caps` table

**Action:** None - no downstream consumers yet

---

## Migration Path

### What Needs to Change

1. **Update rvu_ingestor.py** (2 changes, 10 min)
   - Line 377: Change `version="1.0"` to `version="1.1"`
   - Lines 1437-1442: Update column mapping to match actual file

2. **Update tests** (if any exist) (10 min)
   - Check `tests/ingestors/test_rvu_parsers.py`
   - Update test expectations from v1.0 columns to v1.1

3. **Create parser** (Phase 2) (2-3 hours)
   - New `oppscap_parser.py` using v1.1 schema
   - No migration needed (nothing to migrate from)

---

## Why This is LOW RISK

**1. v1.0 Schema Never Worked**
- Columns don't exist in actual CMS file
- Any attempt to parse with v1.0 would have failed immediately
- No production data exists

**2. Database Model Already Correct**
- `opps_caps` table has correct columns (matches v1.1)
- Table created based on actual data structure, not schema
- No database migration needed

**3. No Downstream Consumers**
- OPPSCAP parser never completed
- No APIs serving OPPSCAP data
- No production pipelines consuming OPPSCAP

**4. Fixing Before Production**
- Caught during Phase 1 verification
- Fixed before any real data processed
- Prevents future issues

---

## Breaking Change Classification

**Technical:** BREAKING (column schema changes)  
**Practical:** BUG FIX (v1.0 was wrong, v1.1 is correct)  
**Risk:** LOW (no production use, no consumers)

**Analogy:** Like fixing a typo in a function signature that was never called - technically breaking, but no actual breakage.

---

## Action Items

### Immediate (Before Phase 2):
1. ✅ Update schema to v1.1 (DONE)
2. ⬜ Update rvu_ingestor.py lines 377, 1437-1442
3. ⬜ Check and update tests (if any)

### Documentation:
1. ✅ Document in schema changelog (DONE)
2. ✅ Note in CHANGELOG.md (DONE)
3. ⬜ Add migration note if needed

---

## Recommendation

**Proceed with v1.1 as planned:**
- Update ingestor code (10 min)
- Check tests (10 min)
- Continue with Phase 2

**No rollback needed:**
- v1.0 never worked
- Database model already correct
- Low risk change

---

## Summary

**Breaking Change Impact:** MINIMAL
- Schema v1.0 was incorrect (created before file inspection)
- Database model already has correct structure
- No production data exists
- v1.1 fixes the schema to match reality
- Total fix time: ~20 minutes (ingestor + tests)

**This is actually GOOD:**
- Caught bug during Phase 1 (before implementation)
- Prevented 2-3 hours debugging during Phase 2
- Demonstrates value of pre-implementation verification

---

**Next Steps:**
1. Update rvu_ingestor.py to use v1.1 (10 min)
2. Verify/update tests (10 min)  
3. Proceed with Phase 2 (parser implementation)

