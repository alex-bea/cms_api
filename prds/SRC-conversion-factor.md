# Conversion Factor (National) Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** MPFS Pricer, Payment Calculator, Pricing API, QA, Compliance  
**Change control:** PR review  
**Review cadence:** Annual + mid-year ARs (Administrative Revisions)

**Disambiguation:** This covers **NATIONAL** physician & anesthesia BASE conversion factors (1-2 rows/year).  
For **LOCALITY-SPECIFIC** anesthesia CFs (100+ rows), see `SRC-anes.md`.

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog
- **PRD-mpfs-prd-v1.0.md:** MPFS payment calculations use these CFs
- **SRC-anes.md:** Locality-specific anesthesia CFs (different dataset)
- **STD-parser-contracts-prd-v2.0.md:** Parser core contracts
- **REF-parser-quality-guardrails-v1.0.md:** Validation and guardrails
- **STD-qa-testing-prd-v1.0.md:** QA testing standards

**Last Updated:** 2026-06-11
**Verified Against CMS Release:** 2025 Physician Fee Schedule Final Rule

---

## 1. Overview

**Official CMS Name:** Physician Fee Schedule & Anesthesia Conversion Factors  
**Dataset Code:** CF (Conversion Factor)  
**Source URL:** [CMS Physician Fee Schedule](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)  
**Authoritative Source:** Federal Register (CY YYYY Physician Fee Schedule Final Rule)  
**Release Cadence:** Annual (typically November for following calendar year) + potential mid-year ARs  
**Business Purpose:** National multipliers that convert RVUs to dollar payments. Applied uniformly across all localities (before geographic adjustment).

**Typical Characteristics:**
- **File Size:** < 10KB (minimal data)
- **Row Count:** 1-3 rows per year (physician CF + anesthesia base CF + potential AR rows)
- **Update Frequency:** Annual + mid-year Administrative Revisions (rare but critical)
- **Effective Dates:** Calendar year start (Jan 1) or AR effective date (mid-year)

**Payment Formula Context:**
```
MPFS Payment = [Sum of (RVU × GPCI for each component)] × Conversion Factor
Anesthesia Payment = Base Units × Anesthesia CF (national) × Locality Anesthesia CF
```

**Runtime RVU-backed pricing note:**
For the live RVU snapshot pricing path, the physician conversion factor is read
from `rvu_items.conversion_factor` on the RVU row selected by valuation date.
Pricing provenance must expose `CF:release:<rvu_release_id>` and
`CF:source:rvu_items.conversion_factor` so the derived CF source is auditable
without adding a separate public API field.

**Key Distinction:**
- **This dataset:** National baseline ($32-35 for physician, $20 for anesthesia)
- **ANES dataset (SRC-anes.md):** Locality-specific anesthesia adjustments ($19-28 varying by locality)

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Parser Support | Notes |
|--------|-----------|--------------|----------------|-------|
| **CSV** | `.csv` | ✅ Primary | ✅ Implemented | Official CMS distribution |
| **XLSX** | `.xlsx` | ✅ Common | ✅ Implemented | Excel workbook, easier for manual review |
| **ZIP** | `.zip` | ✅ Sometimes | ✅ Implemented | Archives one of above formats |
| **TXT** | `.txt` | ❌ Rare | ❌ Not needed | CFs not distributed in fixed-width |

**Note:** Unlike PPRRVU/GPCI/ANES, Conversion Factors are NOT distributed in fixed-width TXT format.

### 2.2 Format-Specific Details

**CSV:**
- **Delimiter:** Comma
- **Header Structure:** Row 1 = column names
- **Skip Rows:** `skiprows=0` (header is first line)
- **Quoting:** QUOTE_MINIMAL
- **Column Names:** English phrases (e.g., "Conversion Factor", "Type", "Effective Date")
- **Currency Format:** May include $ symbols and commas ($32.3465 or 32.3465)

**Example Structure:**
```csv
CF Type,Conversion Factor,Effective Date,End Date
Physician,$32.3465,2025-01-01,2025-12-31
Anesthesia,$20.3178,2025-01-01,2025-12-31
```

**XLSX:**
- **Sheet Name:** First sheet (Sheet1 or "Conversion Factors")
- **Header Row:** Row 1
- **Data Type:** Complete dataset (all CFs for year)
- **Known Issues:** 
  - Excel may coerce dates to datetime (parser handles)
  - Precision: 4 decimal places required (32.3465 not 32.35)

**ZIP:**
- **Inner Files:** Typically `conversion-factor-2025.csv` or `cf_2025.xlsx`
- **Format Detection:** Extension-based
- **Single Member:** Usually one file per ZIP

---

## 3. Schema & Column Mapping

### 3.1 Natural Keys

**Schema Version:** cms_conversion_factor_v2.0 (current)

```python
NATURAL_KEYS = ['cf_type', 'effective_from']
```

**Uniqueness:** 
- **Expected:** 2 rows per year (physician + anesthesia)
- **Mid-year AR:** May have 3-4 rows (original + AR for each type)
- **Test Fixtures:** Must be unique (QTS §5.1.1)

**Example Rows:**
```
('physician', '2025-01-01')   → $32.3465
('anesthesia', '2025-01-01')  → $20.3178
('physician', '2024-03-09')   → $32.7442  (mid-year AR)
```

### 3.2 Column Specifications

| Column | Type | Precision | Nullable | Range | Notes |
|--------|------|-----------|----------|-------|-------|
| **cf_type** | string | - | No | `['physician', 'anesthesia']` | Categorical, 2 allowed values |
| **cf_value** | float64 | 4dp | No | $0-200 | Currency, strip $, commas |
| **effective_from** | datetime | - | No | N/A | Start date for this CF |
| **effective_to** | datetime | - | Yes | N/A | End date (or null for current) |
| **cf_description** | string | - | Yes | N/A | Optional notes (AR details, etc.) |

**Precision Requirements:**
- **Schema:** 4 decimal places (32.3465 not 32.35)
- **Rounding:** HALF_UP per schema contract
- **Hash Stability:** Canonicalized to 4dp before hashing

### 3.3 Column Mapping (Alias Map)

```python
ALIAS_MAP = {
    # CF value variations
    'conversion factor': 'cf_value',
    'conversion_factor': 'cf_value',
    'cf': 'cf_value',
    'factor': 'cf_value',
    'value': 'cf_value',
    
    # CF type variations
    'type': 'cf_type',
    'cf type': 'cf_type',
    
    # Description variations
    'source': 'cf_description',
    'description': 'cf_description',
    'notes': 'cf_description',
    
    # Date variations
    'effective date': 'effective_from',
    'effective': 'effective_from',
    'effective_date': 'effective_from',
    'start date': 'effective_from',
    'end date': 'effective_to',
    'expiration': 'effective_to',
}
```

---

## 4. Validation Rules

### 4.1 Range Validation

**CF Value Range:**
- **HARD Range:** $0.01 - $200.00
- **Typical Range (2020-2025):** 
  - Physician: $30-35
  - Anesthesia: $18-22

**Validation Logic:**
```python
# After currency symbol stripping and canonicalization
cf_numeric = pd.to_numeric(df['cf_value'], errors='coerce')

# HARD range
invalid_range = (cf_numeric <= 0) | (cf_numeric > 200)
if invalid_range.any():
    # Reject with validation_severity='BLOCK'
```

### 4.2 Categorical Validation

**cf_type Domain:**
- **Allowed Values:** `['physician', 'anesthesia']`
- **Action on Unknown:** Reject with `validation_severity='BLOCK'`
- **Validation Rule:** `R-CF-001: cf_type_domain`

```python
allowed_cf_types = ['physician', 'anesthesia']
unknown_types = ~df['cf_type'].isin(allowed_cf_types)
if unknown_types.any():
    # Move to rejects with validation_rule='cf_type_domain'
```

### 4.3 CMS Authoritative Values Guardrails ⭐

**Unique Pattern:** CF parser validates against known CMS-published values from Federal Register.

**Source of Truth:**
- CMS Federal Register (annual Physician Fee Schedule Final Rule)
- Example: [CMS-1807-F](https://www.cms.gov/newsroom/press-releases/) for CY 2025

**Known Values (2024-2025):**
```python
CMS_KNOWN_VALUES = {
    '2025': {
        'physician': 32.3465,  # From Federal Register CY-2025 PFS Final Rule
        'anesthesia': 20.3178,  # From CMS Anesthesia CF table
    },
    '2024': {
        'physician': 33.0607,  # CY-2024 original
        # Mid-year AR on 2024-03-09: 32.7442
    }
}
```

**Guardrail Validation:**
```python
# Compare parsed value against known CMS value
if product_year in CMS_KNOWN_VALUES:
    expected = CMS_KNOWN_VALUES[product_year].get(cf_type)
    if expected:
        deviation = abs(actual - expected)
        if deviation > 0.0001:  # Tolerance: 0.01 cents
            logger.warning(
                "CF value deviation from CMS authoritative source",
                cf_type=cf_type,
                expected=expected,
                actual=actual,
                deviation=deviation
            )
            # Add to metrics, don't reject (may be AR or correction)
            metrics['guardrail_warnings'][f'{cf_type}_value_deviation'] = {
                'expected': expected,
                'actual': actual,
                'deviation': deviation
            }
```

**Benefits:**
- Catches data entry errors
- Alerts to unexpected AR values
- Provides audit trail for compliance
- Logged, not rejected (may be intentional update)

**Applies To:** Any dataset with externally-verified authoritative values

---

## 5. Known Data Quirks

### 5.1 Mid-Year Administrative Revisions (AR)

**Issue:** CMS may publish mid-year updates to conversion factors.

**Example:**
- **2024-01-01:** Physician CF = $33.0607 (original)
- **2024-03-09:** Physician CF = $32.7442 (AR, court-ordered reduction)

**Handling:**
- Both rows stored with different `effective_from` dates
- Natural key `(cf_type, effective_from)` allows multiple rows per year
- Queries use `effective_from <= query_date` for time-travel

**Test Coverage:** `test_cf_mid_year_ar_handling` validates multiple effective dates

### 5.2 Currency Symbol Stripping

**Issue:** CMS files may include $ symbols and comma separators.

**Examples:**
- `$32.3465` → parse as `32.3465`
- `$32,345.67` → parse as `32345.67` (rare, but handle)

**Parser Logic:**
```python
# Strip currency formatting before numeric conversion
df['cf_value'] = df['cf_value'].str.replace('$', '').str.replace(',', '').str.strip()
cf_numeric = pd.to_numeric(df['cf_value'], errors='coerce')
```

### 5.3 Tiny Dataset Characteristics

**Issue:** Only 1-3 rows per year (unlike typical CMS datasets with thousands of rows)

**Implications:**
- **Row Count Validation:** Different tiers than GPCI/ANES
  - 0 rows: ERROR (empty file)
  - 1-5 rows: OK (expected for CF)
  - >10 rows: WARN (verify not duplicate data)
  
- **Duplicate Detection:** Still critical even for small datasets
  - Same `(cf_type, effective_from)` = duplicate
  - Use strict quarantine (both copies rejected)

- **Performance:** Parse time <0.01s (minimal optimization needed)

### 5.4 Inferred CF Type

**Issue:** Some files don't have explicit `cf_type` column.

**Detection Logic:**
```python
# Infer from filename or value range
if 'anesthesia' in filename.lower() or 'anes' in filename.lower():
    cf_type = 'anesthesia'
elif cf_value < 25:  # Anesthesia CFs typically $18-22
    cf_type = 'anesthesia'
else:  # Physician CFs typically $30-35
    cf_type = 'physician'
```

**Preference:** Explicit column preferred over inference

---

## 6. Schema Evolution

### Version 2.0 (Current)

**Natural Keys:** `['cf_type', 'effective_from']`  
**Precision:** 4 decimal places  
**Hash Spec:** v1 (schema-driven)

**Key Features:**
- Supports mid-year ARs via `effective_from`
- Two CF types: physician, anesthesia (national base)
- Guardrail metrics for CMS value validation

### Version 1.0 (Historical)

**Issues:**
- Less precise hashing
- No guardrail validation

**Migration:** None needed (v2.0 is first production schema)

---

## 7. Parser Implementation

### 7.1 Parser Details

**File:** `cms_pricing/ingestion/parsers/conversion_factor_parser.py`  
**Version:** v1.0.0  
**Schema:** cms_conversion_factor_v2.0  
**Tests:** Passing (golden + 11 negative)

**Key Features:**
1. **CMS Authoritative Values Guardrails** ⭐ (unique pattern)
2. **Mid-Year AR Support** (multiple effective dates)
3. **Currency Symbol Stripping** ($ and commas)
4. **CF Type Inference** (from filename or value range)
5. **Tiny Dataset Validation** (1-3 rows expected)
6. **Categorical Validation** (only physician/anesthesia allowed)

### 7.2 Performance Metrics

**Real Data (2 rows):**
- **Parse Time:** < 0.01s
- **Reject Rate:** 0%
- **Duplicate Rate:** 0%
- **CF Values:** Physician $32.3465, Anesthesia $20.3178 (2025)
- **Memory:** Minimal (<1KB DataFrame)

### 7.3 Validation Tiers (Unique for Tiny Datasets)

| Rows | Severity | Action | Use Case |
|------|----------|--------|----------|
| 0 | CRITICAL | Raise ParseError | Empty file |
| 1-5 | OK | Normal | Expected (1-2 CFs + potential AR) |
| 6-10 | WARN | Log warning | Verify not duplicate years |
| >10 | CRITICAL | Raise ParseError | Likely wrong file or duplicates |

**Different from GPCI/ANES:** Tiny dataset, 1-5 rows is NORMAL (not test fixture)

---

## 8. CMS Authoritative Values (Guardrail Pattern) ⭐

**Principle:** Validate parsed values against externally-verified authoritative sources.

**Source of Truth:**

| Year | Physician CF | Anesthesia Base CF | Authority |
|------|-------------|--------------------|-----------|
| **2025** | $32.3465 | $20.3178 | CMS-1807-F (Federal Register) |
| **2024** | $33.0607 | $19.9887 | CMS-1784-F |
| **2024 AR** | $32.7442 | - | Court-ordered reduction (Mar 2024) |

**Guardrail Validation Logic:**
```python
# Load known values from config or hardcoded
CMS_KNOWN_VALUES = {
    '2025': {'physician': 32.3465, 'anesthesia': 20.3178},
    '2024': {'physician': 33.0607},
}

# Compare parsed against expected
def _apply_guardrails(df: pd.DataFrame, product_year: str) -> Dict:
    warnings = {}
    
    if product_year not in CMS_KNOWN_VALUES:
        logger.info(f"No authoritative CF for {product_year}, skipping guardrail")
        return warnings
    
    for cf_type in ['physician', 'anesthesia']:
        expected = CMS_KNOWN_VALUES[product_year].get(cf_type)
        if not expected:
            continue
        
        actual_rows = df[df['cf_type'] == cf_type]
        if len(actual_rows) == 0:
            warnings[f'{cf_type}_missing'] = True
            continue
        
        actual = float(actual_rows['cf_value'].iloc[0])
        deviation = actual - expected
        
        if abs(deviation) > 0.0001:  # Tolerance: 0.01 cents
            warnings[f'{cf_type}_value_deviation'] = {
                'expected': expected,
                'actual': actual,
                'deviation': deviation,
                'authority': 'Federal Register',
            }
            logger.warning(
                "CF value deviation from authoritative source",
                cf_type=cf_type,
                expected=expected,
                actual=actual,
                deviation=deviation,
                note="May be AR or data entry error"
            )
    
    return warnings

# In main parser
metrics['guardrail_warnings'] = _apply_guardrails(final_df, metadata['product_year'])
```

**Metrics Structure:**
```json
{
  "guardrail_warnings": {
    "physician_value_deviation": {
      "expected": 32.3465,
      "actual": 32.3400,
      "deviation": -0.0065,
      "authority": "Federal Register"
    }
  }
}
```

**Decision Tree:**
- **Deviation < 0.0001:** OK (rounding differences)
- **Deviation 0.0001-0.10:** WARN (log, may be AR)
- **Deviation > 0.10:** WARN + alert (likely data error)

**Benefits:**
- ✅ Catches data entry errors before production
- ✅ Provides audit trail for compliance
- ✅ Alerts to unexpected ARs
- ✅ Self-documenting (logs authority source)

**Reusable Pattern:** Apply to any dataset with externally-verified values (e.g., statutory floors, published indices)

---

## 9. Integration Points

### 9.1 Payment Calculation

**MPFS Payment:**
```sql
-- Get current physician CF
SELECT cf_value 
FROM conversion_factors
WHERE cf_type = 'physician'
  AND effective_from <= :payment_date
  AND (effective_to IS NULL OR effective_to >= :payment_date)
ORDER BY effective_from DESC
LIMIT 1;

-- Apply to RVU calculation
Payment = (Work_RVU × Work_GPCI + PE_RVU × PE_GPCI + MP_RVU × MP_GPCI) × CF_value
```

**Anesthesia Payment:**
```sql
-- Get national anesthesia base CF
SELECT cf_value
FROM conversion_factors
WHERE cf_type = 'anesthesia'
  AND effective_from <= :payment_date
  AND (effective_to IS NULL OR effective_to >= :payment_date);

-- Then apply locality-specific adjustment from anes_cfs table
Payment = Base_Units × National_CF × Locality_CF
```

### 9.2 Relationship with ANES Dataset

**Two-Tier System:**
1. **National Base (this dataset):** $20.3178 (same for all localities)
2. **Locality Adjustment (ANES):** Varies by locality ($19-28)

**Conceptual Formula:**
```
Final Anesthesia CF = National Base (from this dataset) × Locality Factor (from ANES)
```

**However:** In practice, ANES dataset stores **final** locality CF, not the adjustment factor. Check schema for actual implementation.

---

## 10. Testing Strategy

### 10.1 Test Coverage

**Golden Tests:**
- `test_cf_golden_csv` - CSV parsing
- `test_cf_golden_xlsx` - XLSX parsing
- `test_cf_golden_zip` - ZIP extraction
- `test_cf_metadata_injection` - Metadata fields
- `test_cf_row_hashing` - Deterministic hashing
- `test_cf_schema_compliance` - v2.0 columns

**Negative Tests (11 total):**
- Invalid cf_value (negative, zero, >$200)
- Invalid cf_type (not physician/anesthesia)
- Duplicate natural keys
- Missing required columns
- Unparseable dates
- Malformed currency ($, commas edge cases)

### 10.2 Test Fixtures

**Golden:**
- `cf_2025_minimal.csv` - 2 rows (physician + anesthesia)
- `cf_2025_minimal.xlsx` - Same 2 rows
- `cf_2025_minimal.zip` - Compressed CSV

**Negative:**
- 11 fixtures covering validation edge cases

**Unique Fixture Pattern:** Only 2 rows (minimal valid dataset)

### 10.3 Guardrail Testing

**Test:** `test_cf_guardrail_validation`

**Pattern:**
```python
# Test that known 2025 values trigger no warnings
csv_content = "CF Type,Conversion Factor\nPhysician,32.3465\nAnesthesia,20.3178\n"
result = parse_conversion_factor(BytesIO(csv_content.encode()), 'cf_2025.csv', metadata)

# Should have no guardrail warnings for exact match
assert 'guardrail_warnings' in result.metrics
assert len(result.metrics['guardrail_warnings']) == 0

# Test deviation detection
csv_bad = "CF Type,Conversion Factor\nPhysician,99.0000\n"  # Wrong value
result = parse_conversion_factor(BytesIO(csv_bad.encode()), 'cf_bad.csv', metadata)

# Should warn about deviation
assert 'physician_value_deviation' in result.metrics['guardrail_warnings']
assert result.metrics['guardrail_warnings']['physician_value_deviation']['deviation'] > 0.1
```

---

## 11. Operational Notes

### 11.1 Annual Release Workflow

**Step 1: Monitor CMS Announcements**
```bash
# Watch for Federal Register publication (November typically)
# https://www.cms.gov/medicare/payment/fee-schedules/physician
```

**Step 2: Extract CF Values**
```bash
# From Federal Register PDF or CMS website
# Physician CF: Look for "Conversion Factor" in final rule
# Anesthesia CF: Look for "Anesthesia Conversion Factor" table
```

**Step 3: Update Guardrails**
```python
# Update CMS_KNOWN_VALUES in conversion_factor_parser.py
CMS_KNOWN_VALUES['2026'] = {
    'physician': XX.XXXX,  # From Federal Register
    'anesthesia': YY.YYYY,  # From CMS table
}
```

**Step 4: Parse & Validate**
```bash
# Run parser
python -m cms_pricing.ingestion.parsers.conversion_factor_parser \
    --file conversion-factor-2026.csv \
    --year 2026

# Check guardrail metrics
# Should show 0 deviation if values match Federal Register
```

### 11.2 Mid-Year AR Handling

**Trigger:** Court orders, legislative changes, CMS corrections

**Process:**
1. CMS publishes AR with new effective_from date
2. Parse AR file (will have different effective_from)
3. Both original + AR rows stored (natural key allows)
4. Queries use latest value for date range

**Example Query (handles AR):**
```sql
-- Get CF for specific date (handles mid-year AR)
SELECT cf_value
FROM conversion_factors
WHERE cf_type = 'physician'
  AND effective_from <= '2024-04-01'
  AND (effective_to IS NULL OR effective_to >= '2024-04-01')
ORDER BY effective_from DESC  -- Latest effective_from wins
LIMIT 1;
```

### 11.3 Common Issues & Fixes

**Issue 1: "Unknown cf_type: Anesthesia" (capitalization)**  
**Cause:** Case-sensitive validation  
**Fix:** Normalize to lowercase before validation

**Issue 2: "CF value deviation: expected 32.3465, got 32.35"**  
**Cause:** Excel truncated precision  
**Fix:** Verify 4dp in source file, update CMS_KNOWN_VALUES if authoritative

**Issue 3: "Duplicate natural key" for same year**  
**Cause:** File has duplicate rows  
**Fix:** Deduplicate with `keep='first'` or investigate source

---

## 12. Quality Metrics

### 12.1 Expected Metrics (Production)

```json
{
  "total_rows": 2,
  "valid_rows": 2,
  "reject_rows": 0,
  "parse_duration_sec": 0.005,
  "cf_types": ["physician", "anesthesia"],
  "guardrail_warnings": {},
  "encoding_detected": "utf-8"
}
```

### 12.2 SLO Thresholds

- **Reject Rate:** 0% (should never reject clean CMS data)
- **Parse Time:** <0.05s for 5 rows
- **Guardrail Deviation:** <0.0001 (1 cent tolerance)
- **Duplicate Rate:** 0% (unless AR exists)

### 12.3 Monitoring Alerts

**CRITICAL:**
- Guardrail deviation > $0.10 (likely data error)
- Row count = 0 or > 10
- Missing physician or anesthesia CF

**WARN:**
- Guardrail deviation 0.0001-0.10 (may be AR)
- Row count 6-10 (verify intentional)
- Mid-year AR detected (expected but notable)

---

## 13. Implementation References

**Parser:** `cms_pricing/ingestion/parsers/conversion_factor_parser.py` (617 lines)  
**Schema:** `cms_pricing/ingestion/contracts/cms_conversion_factor_v2.0.json`  
**Tests:** `tests/ingestion/test_conversion_factor_parser_golden.py` (golden tests)  
**Tests:** `tests/ingestion/test_conversion_factor_parser_negatives.py` (11 negative tests)  
**Fixtures:** `tests/fixtures/conversion_factor/golden/` (CSV, XLSX, ZIP)  
**Fixtures:** `tests/fixtures/conversion_factor/negatives/` (11 edge cases)

**Test Results:** Passing (golden + 11 negative)

**QTS Compliance:**
- §G.1: Rich error messages with examples
- §G.2: Metrics structure (guardrail_warnings)
- §G.3: Rejects structure validation
- §G.4: String/numeric validation pattern

**Related PRDs:**
- `PRD-mpfs-prd-v1.0.md` - MPFS payment uses these CFs
- `PRD-rvu-gpci-prd-v0.1.md` - RVU bundle ingestion
- `STD-parser-contracts-prd-v2.0.md` - Parser standards

---

## Appendix A: Sample Data

### A.1 CSV Format

```csv
CF Type,Conversion Factor,Effective Date,End Date
Physician,32.3465,2025-01-01,2025-12-31
Anesthesia,20.3178,2025-01-01,2025-12-31
```

### A.2 Mid-Year AR Example

```csv
CF Type,Conversion Factor,Effective Date,End Date,Notes
Physician,33.0607,2024-01-01,2024-03-08,Original 2024 CF
Physician,32.7442,2024-03-09,2024-12-31,Administrative Revision (court order)
Anesthesia,19.9887,2024-01-01,2024-12-31,
```

**Natural Key Handling:**
- `('physician', '2024-01-01')` = $33.0607
- `('physician', '2024-03-09')` = $32.7442
- Both rows valid (different effective_from)

---

## Appendix B: Guardrail Values Maintenance

**When to Update CMS_KNOWN_VALUES:**

1. **Annual PFS Final Rule Published** (November)
   - Extract physician CF from Federal Register
   - Extract anesthesia CF from CMS table
   - Add to `CMS_KNOWN_VALUES['{year}']`

2. **Mid-Year AR Published**
   - Extract revised CF
   - Note effective date
   - Add separate entry (don't replace original)

3. **Historical Backfill**
   - Research past Federal Registers
   - Add known historical values
   - Enables drift detection for old data

**Update Locations:**
1. Parser: `cms_pricing/ingestion/parsers/conversion_factor_parser.py` (lines 47-60)
2. Tests: Update guardrail test assertions
3. This doc: Update Appendix B table

---

## Appendix C: Federal Register Citation Pattern

**How to Find Authoritative CF Values:**

1. **Navigate to:** https://www.federalregister.gov/
2. **Search:** "CMS-{RULE} Physician Fee Schedule {YEAR}"
   - Example: "CMS-1807-F Physician Fee Schedule 2025"
3. **Locate:** "Conversion Factor" section (typically in Summary or Part VIII)
4. **Extract:** Exact value with 4 decimal places
5. **Verify:** Cross-check with CMS newsroom press release

**Example Citations:**
- **2025:** CMS-1807-F, 89 FR 83264 (November 2024)
- **2024:** CMS-1784-F, 88 FR 78818 (November 2023)

**Record in Code:**
```python
CMS_KNOWN_VALUES = {
    '2025': {
        'physician': 32.3465,  # CMS-1807-F, 89 FR 83264
        'anesthesia': 20.3178,  # RVU25D Anesthesia CF table
    },
}
```

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| **1.0** | 2025-10-21 | Initial Conversion Factor source reference. Documented CMS authoritative values guardrails (Federal Register validation), tiny dataset validation strategy (1-3 rows expected), mid-year AR handling (multiple effective dates), national vs locality-specific distinction, CF type inference, currency symbol stripping. Captured guardrail pattern for reuse in other datasets. Based on Conversion Factor Parser v1.0 implementation. |

---

**End of SRC-conversion-factor.md v1.0**
