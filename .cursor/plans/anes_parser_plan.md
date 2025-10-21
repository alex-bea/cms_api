# ANES Parser Implementation Plan

## Overview
Build ANES (Anesthesia Conversion Factor) parser to complete the parser module (6/6 parsers). Copy GPCI template structure and adapt for simpler ANES data format.

**Disambiguation:** ANES = Anesthesia Conversion Factor (not American National Election Studies)

**Time Estimate:** 90 minutes  
**Priority:** HIGH (completes parser module)

---

## Pre-Flight Checks (10 minutes)

### Verify Infrastructure
1. **Schema Contract**: `cms_pricing/ingestion/contracts/cms_anescf_v1.0.json`
   - Current NK: `['locality_code', 'effective_from']` (SAME BUG AS GPCI v1.2!)
   - **Action Required**: Bump to v1.1, add `mac` to NK
   - Updated NK: `['mac', 'locality_code', 'effective_from']`
   - Update primary_keys to match

2. **Layout Registry**: `cms_pricing/ingestion/parsers/layout_registry.py`
   - `ANES_2025D_LAYOUT` already exists (lines 117-127)
   - Columns: `mac`, `locality_id`, `locality_name`, `anesthesia_cf`
   - Fixed-width positions verified
   - **Note:** NO `state_fips` column in ANES layout (unlike GPCI)

3. **Sample Data**: 
   - `sample_data/rvu25d_0/ANES2025.txt` (real CMS file, ~112 rows)
   - Format: `10112       00    ALABAMA                                                1931`
   - MAC (5 digits), locality (2 digits), name (40 chars), CF (4 digits in CENTS)
   - **Units:** Raw CF values are integer cents (1931 = $19.31 USD)

4. **Database Model**: `cms_pricing/models/rvu.py` (lines 144-165)
   - Table: `anes_cfs`
   - Already has `mac` column
   - Needs unique index on `(mac, locality_id, effective_from)`
   - **Note:** Use `effective_from` (not `effective_start`) for consistency with schema

---

## Step 1: Create ANES Schema v1.1 (10 minutes)

### File: `cms_pricing/ingestion/contracts/cms_anescf_v1.1.json` (NEW)

**Action:** Create new file (don't mutate v1.0)

**Changes from v1.0:**

1. **Version:** `"1.1"`

2. **Natural Keys:** (CORRECTED - includes MAC)
   ```json
   "natural_keys": [
     "mac",
     "locality_code",
     "effective_from"
   ]
   ```

3. **Primary Keys:** (Match NK)
   ```json
   "primary_keys": [
     "mac",
     "locality_code",
     "effective_from"
   ]
   ```

4. **Column Order:** (NO state_fips - not in ANES layout)
   ```json
   "column_order": [
     "mac",
     "locality_code",
     "anesthesia_cf_usd",
     "effective_from"
   ]
   ```

5. **MAC Column:** (NEW)
   ```json
   "mac": {
     "name": "mac",
     "type": "str",
     "nullable": false,
     "description": "5-digit Medicare Administrative Contractor code",
     "pattern": "^\\d{5}$",
     "sample_values": ["01112", "02102", "03102"]
   }
   ```

6. **Anesthesia CF Column:** (UNITS & SCALING)
   ```json
   "anesthesia_cf_usd": {
     "name": "anesthesia_cf_usd",
     "type": "float64",
     "precision": 2,
     "scale": 2,
     "rounding_mode": "HALF_UP",
     "multipleOf": 0.01,
     "nullable": false,
     "description": "Anesthesia conversion factor in USD (scaled from integer cents)",
     "unit": "USD",
     "domain": null,
     "min_value": 0.01,
     "max_value": 100.00,
     "pattern": null,
     "sample_values": [19.31, 27.86, 20.04],
     "notes": "Raw CMS file values are integer cents (e.g., 1931 → $19.31). Parser scales by dividing by 100.0"
   }
   ```

7. **Changelog:**
   ```json
   {
     "version": "1.1",
     "date": "2025-10-20",
     "changes": [
       "BREAKING: Corrected natural key from ['locality_code', 'effective_from'] to ['mac', 'locality_code', 'effective_from']",
       "Rationale: locality_code='00' appears in multiple states; MAC disambiguates",
       "Prevents false duplicates (same issue found in GPCI v1.2 → v1.3)",
       "Added mac column definition with validation pattern",
       "Updated primary_keys to match natural key",
       "Removed state_fips from schema (not in ANES layout)",
       "Added units & scaling: raw cents → USD with precision (2dp), rounding (HALF_UP)",
       "Set min_value to 0.01 (CF must be positive, not zero)"
     ]
   }
   ```

**Note:** Create NEW file `cms_anescf_v1.1.json` (clean version history)

---

## Step 2: Create ANES Parser (40 minutes)

### File: `cms_pricing/ingestion/parsers/anes_parser.py` (NEW)

**Copy from:** `gpci_parser.py` (639 lines)  
**Adapt for:** ANES (simpler structure, 4 columns vs 7)

### Key Differences from GPCI:
1. **Columns**: `mac`, `locality_code`, `anesthesia_cf_usd` (not 3 GPCI values)
2. **Validation**: Single CF value > 0 (not 3 GPCI ranges)
3. **Schema ID**: `cms_anescf_v1.1` (not v1.3)
4. **Natural Keys**: `['mac', 'locality_code', 'effective_from']`

### Structure (12-step template with scaling):
```python
# Header
PARSER_VERSION = "1.0.0"
SCHEMA_ID = "cms_anescf_v1.1"
NATURAL_KEYS = ["mac", "locality_code", "effective_from"]

# Alias map
ALIAS_MAP = {
    'medicare administrative contractor (mac)': 'mac',
    'mac': 'mac',
    'locality number': 'locality_code',
    'locality': 'locality_code',
    'loc': 'locality_code',
    'locality_id': 'locality_code',
    'anesthesia_cf': 'anesthesia_cf_raw',  # Raw cents value
    'anesthesia cf': 'anesthesia_cf_raw',
    'anes_cf': 'anesthesia_cf_raw',
}

# Main parse function
def parse_anes(file_obj, filename, metadata) -> ParseResult:
    # Step 1: Detect encoding
    # Step 2: Parse by format (TXT fixed-width, CSV, XLSX, ZIP)
    # Step 3: Normalize column names
    # Step 3.5: Normalize string columns
    # Step 3.6: Filter CMS footer rows (MAC != 5 digits)
    # Step 3.7: Load schema
    # Step 4: Map columns (locality_id → locality_code)
    # Step 4.5: Scale CF from cents to USD (1931 → 19.31) ← NEW
    # Step 5: Derive effective dates from filename/metadata ← ENHANCED
    # Step 6: Add metadata fields
    # Step 7: Validate & reject (CF range, MAC pattern, locality pattern)
    # Step 8: NK uniqueness (QUARANTINE duplicates, not keep='first') ← STRICT
    # Step 9: Row count validation (expect 100-120, FAIL <50) ← UPDATED
    # Step 10: Sort by NK
    # Step 11: Hash rows
    # Step 12: Return ParseResult

# NEW: Scaling function
def _scale_cf_to_usd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw integer cents to USD decimal.
    
    CMS format: 1931 (cents) → 19.31 (USD)
    Precision: 2 decimal places
    Rounding: HALF_UP per schema
    """
    df['anesthesia_cf_usd'] = (
        pd.to_numeric(df['anesthesia_cf_raw'], errors='coerce') / 100.0
    ).round(2)
    return df

# NEW: Effective date derivation
def _derive_effective_dates(df: pd.DataFrame, metadata: dict, filename: str) -> pd.DataFrame:
    """
    Derive effective_from/to from filename or metadata.
    
    Rules:
    1. Extract year from filename (ANES2025 → 2025)
    2. effective_from = Jan 1 of year
    3. effective_to = Dec 31 of year (or None if current/future)
    """
    import re
    year_match = re.search(r'ANES(\d{4})', filename, re.IGNORECASE)
    if year_match:
        year = int(year_match.group(1))
    else:
        year = int(metadata.get('product_year', metadata['vintage_date'].year))
    
    df['effective_from'] = pd.to_datetime(f'{year}-01-01')
    
    current_year = datetime.now().year
    if year < current_year:
        df['effective_to'] = pd.to_datetime(f'{year}-12-31')
    else:
        df['effective_to'] = pd.NaT
    
    return df

# Validation function
def _validate_and_reject(df, metadata, logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Range validation: anesthesia_cf_usd (AFTER scaling)
    # WARN: [15.00, 35.00] (log unusual, don't reject)
    # HARD: [0.01, 100.00] (reject out of bounds, must be positive)
    # Explicit zero/negative check: <= 0
    # Pattern validation: mac (5 digits), locality_code (2 digits)

# Helper functions (copy from GPCI)
def _parse_zip(...)
def _parse_xlsx(...)
def _parse_csv(...)
def _parse_fixed_width(...)
def _normalize_column_names(...)
def _load_schema(...)
def _validate_row_count(...)
```

### Validation Ranges (AFTER scaling to USD):
- **WARN Band**: [15.00, 35.00] USD (typical range, log outliers)
- **HARD Range**: [0.01, 100.00] USD (reject beyond, must be positive)
- **Explicit Zero/Negative**: <= 0 (always reject, CF must be positive)

### Expected Row Counts (STRICTER):
- **Normal**: 100-120 rows (matches GPCI locality count)
- **WARN**: 50-99 rows (incomplete, log warning)
- **Critical Fail**: < 50 rows (too few, reject file)

---

## Step 3: Create Test Suite (20 minutes)

### File: `tests/ingestion/test_anes_parser_golden.py` (NEW)

**Copy from:** `test_gpci_parser_golden.py` (509 lines)

**Tests (13 total - added 2 new):**
1. `test_anes_metadata_injection` - Verify metadata fields
2. `test_anes_row_hashing` - Verify SHA-256 hashing
3. `test_anes_natural_key_sort` - Verify sort by 3-field NK
4. `test_anes_schema_compliance` - Verify v1.1 columns (NO state_fips)
5. `test_anes_golden_txt` - Parse TXT (fixed-width)
6. `test_anes_golden_csv` - Parse CSV
7. `test_anes_golden_xlsx` - Parse XLSX (with footer filtering)
8. `test_anes_golden_zip` - Parse ZIP
9. `test_anes_reject_handling` - Verify rejects DataFrame
10. `test_anes_consistency_txt_csv` - Cross-format parity
11. `test_anes_duplicate_locality_00` - Verify different MACs = unique
12. **`test_anes_units_scaling_from_txt`** - NEW: Verify 1931 → 19.31 conversion
13. **`test_anes_effective_dates_from_metadata`** - NEW: Verify date derivation from filename

**Key Assertions:**
- Sort: `['mac', 'locality_code', 'effective_from']`
- Columns: `mac`, `locality_code`, `anesthesia_cf_usd`, `effective_from`, `effective_to`, `row_content_hash`
- **NO state_fips** (not in ANES layout)
- CF range: 15.00 ≤ CF ≤ 35.00 (typical, after scaling)
- CF > 0 (all values, must be positive)
- No NaN in required columns
- Units: USD (not cents)

### File: `tests/ingestion/test_anes_parser_negatives.py` (NEW)

**Copy from:** `test_gpci_parser_negatives.py`

**Tests (6 total):**
1. `test_anes_cf_out_of_range_rejects` - CF > 100 rejected (after scaling)
2. `test_anes_cf_negative_rejected` - CF < 0 rejected
3. `test_anes_cf_zero_rejected` - CF = 0 rejected (must be positive)
4. `test_anes_duplicate_natural_keys` - Duplicate NK QUARANTINED (strict policy)
5. `test_anes_row_count_below_minimum_fails` - < 50 rows fails (updated threshold)
6. `test_anes_missing_required_column_fails` - Missing `mac` fails

### File: `tests/fixtures/anes/golden/` (NEW)

**Create fixtures:**
- `ANES2025_sample.txt` (copy first 18 rows from `sample_data/rvu25d_0/ANES2025.txt`)
- `ANES2025_sample.csv` (convert TXT to CSV with headers)
- `ANES2025_sample.xlsx` (convert CSV to XLSX)
- `ANES2025_sample.zip` (zip the TXT file)
- `README.md` (document fixture structure)

### File: `tests/fixtures/anes/negatives/` (NEW)

**Create negative fixtures (raw cents values):**
- `out_of_range.csv` (CF = 15000 cents = $150.00, exceeds 100.00)
- `negative_cf.csv` (CF = -1000 cents = $-10.00)
- `zero_cf.csv` (CF = 0 cents = $0.00, must be positive)
- `duplicate_keys.csv` (same MAC+locality+date twice)
- `too_few_rows.csv` (30 rows, < 50 threshold)
- `missing_mac.csv` (no MAC column)

All negative fixtures: 2-line CMS header + MAC column + raw cents

---

## Step 4: Integration & Documentation (5 minutes)

### Update Parser Routing
**File:** `cms_pricing/ingestion/parsers/__init__.py`

Add:
```python
from .anes_parser import parse_anes
```

### Update CHANGELOG
**File:** `CHANGELOG.md`

Add entry:
```markdown
- **ANES Parser v1.0 - Anesthesia Conversion Factor Parser** - Complete parser module (6/6)
  - **Disambiguation:** ANES = Anesthesia Conversion Factor (not American National Election Studies)
  - **Schema:** Created `cms_anescf_v1.1.json` (new file, clean version history)
  - **Natural Key:** Corrected from ['locality_code', 'effective_from'] to ['mac', 'locality_code', 'effective_from']
  - **Rationale:** Same false duplicate bug as GPCI v1.2; locality_code='00' appears in multiple states
  - **Prevention:** Fixed NK before first release to avoid GPCI-style migration
  - **Schema Changes:**
    - Added MAC column with 5-digit validation pattern
    - Removed state_fips (not in ANES layout, caused schema mismatch)
    - Units & Scaling: Raw cents → USD (1931 → $19.31) with precision (2dp), rounding (HALF_UP)
    - Primary keys match natural key
  - **Validation (Strict Policy):**
    - Units: Raw CMS file values are integer cents; parser scales to USD
    - WARN range [15.00, 35.00] USD (typical CF values, after scaling)
    - HARD range [0.01, 100.00] USD (must be positive, zero rejected)
    - Explicit zero/negative check (CF must be > 0)
    - Duplicate handling: QUARANTINE (not keep='first', stricter than GPCI)
    - Row count: <50=FAIL, 50-99=WARN, 100-120=normal (tighter than GPCI)
    - CMS footer filtering (invalid MAC patterns)
  - **Effective Dates:** Derived from filename (ANES2025 → 2025-01-01) or metadata
  - **Test Results:** 19/19 tests passing (100%) - 13 golden + 6 negative
    - Added test_anes_units_scaling_from_txt (verify cents → USD)
    - Added test_anes_effective_dates_from_metadata (verify date logic)
  - **Real Data:** ~110 localities, 0 rejects, 0 duplicates, ~0.03s parse time
  - **Format Support:** TXT (fixed-width), CSV, XLSX, ZIP
  - **Parser Module:** 6/6 parsers complete (PPRRVU, CF, Locality, OPPSCAP, GPCI, ANES) ✅
  - **Time:** 90 minutes (schema v1.1 + scaling logic + parser + tests + docs)
```

### Update GitHub Tasks
**File:** `github_tasks_plan.md`

Update:
```markdown
- ✅ `anes_parser.py` - ANES parsing contract (v1.0 COMPLETE - 110 localities, 19/19 tests, schema v1.1)
```

Update progress:
```markdown
- ✅ 6 of 6 parsers complete (PPRRVU, CF, Locality, OPPSCAP, GPCI, ANES)
- ✅ All parsers use corrected 3-field NK including MAC (prevents false duplicates)
```

---

## Step 5: Smoke Test & Validation (5 minutes)

### Run Test Suite
```bash
# All ANES tests
pytest tests/ingestion/test_anes_parser_golden.py tests/ingestion/test_anes_parser_negatives.py -v

# Expect: 19/19 passing (100%)
# - 13 golden tests (includes units scaling + date derivation)
# - 6 negative tests (includes stricter row count threshold)
```

### Smoke Test on Real Data
```python
from cms_pricing.ingestion.parsers.anes_parser import parse_anes

metadata = {
    'release_id': 'ANES_2025D',
    'schema_id': 'cms_anescf_v1.1',
    'product_year': '2025',
    'quarter_vintage': 'D',
    'vintage_date': datetime(2025, 10, 1),
    'file_sha256': 'test',
    'source_uri': 'sample_data/rvu25d_0/ANES2025.txt',
    'source_release': 'RVU25D',
}

with open('sample_data/rvu25d_0/ANES2025.txt', 'rb') as f:
    result = parse_anes(f, 'ANES2025.txt', metadata)

# Verify:
# - 100-120 rows parsed
# - 0 rejects
# - 0 duplicates
# - Parse time < 5s
# - All CF values > 0 (positive, not zero)
# - CF values in USD (15-35 range typical, scaled from cents)
# - effective_from = 2025-01-01 (from filename)
# - effective_to = None or 2025-12-31
```

---

## Acceptance Criteria

- [ ] Schema v1.1 created (new file, not mutating v1.0)
- [ ] Natural key corrected to 3-field (mac, locality_code, effective_from)
- [ ] NO state_fips dependency (removed, not in ANES layout)
- [ ] Units & scaling implemented (cents → USD, 1931 → $19.31)
- [ ] Effective dates derived from filename/metadata
- [ ] Parser passes 19/19 tests (100%)
  - [ ] test_anes_units_scaling_from_txt
  - [ ] test_anes_effective_dates_from_metadata
- [ ] Real data: ~110 rows, 0 rejects, 0 duplicates
- [ ] All 4 formats supported (TXT, CSV, XLSX, ZIP)
- [ ] CMS footer filtering works
- [ ] NK uniqueness enforced (QUARANTINE duplicates, strict policy)
- [ ] CF validation (WARN + HARD ranges, after scaling)
- [ ] Row count validation (< 50 FAIL, 50-99 WARN, 100-120 normal)
- [ ] CHANGELOG updated (schema v1.1, units, strict policy)
- [ ] GitHub tasks updated (6/6 complete, 19/19 tests)
- [ ] Parser module 100% complete

---

## Risk Mitigation

**Risk 1:** ANES layout differs from documented  
**Mitigation:** Verified layout against `sample_data/rvu25d_0/ANES2025.txt` (matches registry)

**Risk 2:** CF range assumptions wrong  
**Mitigation:** Sample data shows CF values ~1900-2800 cents (19.00-28.00 USD after scaling), WARN band [15-35] USD conservative

**Risk 4:** Units scaling incorrect  
**Mitigation:** Schema specifies precision (2dp), rounding (HALF_UP), and explicit test `test_anes_units_scaling_from_txt`

**Risk 3:** Test fixtures incomplete  
**Mitigation:** Copy real data for golden tests, manually craft negative edge cases

---

## Dependencies

**Requires:**
- GPCI parser v1.3 (template for structure)
- Layout registry (ANES_2025D_LAYOUT exists)
- Schema contract (v1.0 exists, needs NK fix)
- Test infrastructure (pytest, fixtures)

**Blocks:**
- Parser module completion (5/6 → 6/6)
- Integration testing (E2E with GPCI + PPRRVU)

---

## Files to Create/Modify

**Create (5 files):**
1. `cms_pricing/ingestion/parsers/anes_parser.py` (~600 lines)
2. `tests/ingestion/test_anes_parser_golden.py` (~500 lines)
3. `tests/ingestion/test_anes_parser_negatives.py` (~200 lines)
4. `tests/fixtures/anes/golden/` (4 fixtures + README)
5. `tests/fixtures/anes/negatives/` (6 fixtures)

**Create (1 schema file):**
1. `cms_pricing/ingestion/contracts/cms_anescf_v1.1.json` (NEW - clean version history)

**Modify (3 files):**
1. `cms_pricing/ingestion/parsers/__init__.py` (import)
2. `CHANGELOG.md` (entry with v1.1, scaling, strict policy)
3. `github_tasks_plan.md` (progress, 19/19 tests)

---

## Success Metrics

- 19/19 tests passing (100%)
  - Includes units scaling test
  - Includes effective date derivation test
- Parser module 6/6 complete (100%)
- Real data: 0.00% reject rate
- Parse time: < 0.05s (similar to GPCI)
- CF values correctly scaled (cents → USD)
- Effective dates correctly derived from filename
- Code coverage: > 90% (validation, parsing, formats, scaling)

