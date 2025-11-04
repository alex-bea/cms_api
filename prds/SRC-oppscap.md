# OPPSCAP (OPPS-based Payment Caps) Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** MPFS Pricer, Imaging Payment Calculator, Pricing API, QA  
**Change control:** PR review  
**Review cadence:** Quarterly (with CMS releases)

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog
- **PRD-rvu-gpci-prd-v0.1.md:** RVU ingestion product requirements (§1.3 OPPSCAP)
- **PRD-mpfs-prd-v1.0.md:** MPFS uses OPPSCAP for imaging cap comparisons
- **SRC-gpci.md:** GPCI locality reference (OPPSCAP uses same localities)
- **STD-parser-contracts-prd-v2.0.md:** Parser core contracts
- **STD-qa-testing-prd-v1.0.md:** QA testing standards
- **REF-cms-pricing-source-map-prd-v1.0.md:** CMS pricing source inventory

**Last Updated:** 2025-10-21  
**Verified Against CMS Release:** RVU25D (2025 Q4)

---

## 1. Overview

**Official CMS Name:** OPPS-based Payment Caps for Imaging Services  
**Dataset Code:** OPPSCAP  
**Source URL:** [CMS PFS Relative Value Files](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)  
**Release Cadence:** Quarterly (with RVU bundles, A/B/C/D releases)  
**Business Purpose:** Payment caps for certain imaging services under MPFS. When an imaging service's MPFS payment exceeds the OPPS (hospital outpatient) rate, the lower OPPS amount applies as a cap.

**Typical Characteristics:**
- **File Size:** 2-5MB (text format)
- **Row Count:** ~16,000 rows (HCPCS codes × modifiers × localities)
- **Update Frequency:** Quarterly with letter suffixes (typically October/D is annual)
- **Effective Dates:** Aligns with MPFS fee schedule updates

**Payment Formula Context:**
```
if MPFS_Payment > OPPSCAP_Payment:
    Final_Payment = OPPSCAP_Payment  # Cap applies
else:
    Final_Payment = MPFS_Payment  # No cap
```

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Parser Support | Format Authority | Notes |
|--------|-----------|--------------|----------------|------------------|-------|
| **TXT** | `.txt` | ✅ Always | ✅ Implemented | **AUTHORITATIVE** | Fixed-width, CMS official |
| **CSV** | `.csv` | ✅ Always | ✅ Implemented | Parity (100%) | Alternative distribution |
| **XLSX** | `.xlsx` | ✅ Sometimes | ⚠️ DEFERRED | Variance >10% | Contains extra historical data |
| **ZIP** | `.zip` | ✅ Common | ✅ Implemented | Varies | Archives one of above |

**Format Authority Matrix (Per RVU25D):**
- **TXT:** Designated authoritative format (100% coverage, clean structure)
- **CSV:** Parity format (≥99.5% NK overlap with TXT, <1% row variance)
- **XLSX:** Deferred (>10% row variance due to historical quarters)

**See also:** `planning/parsers/oppscap/AUTHORITY_MATRIX.md` for detailed analysis

### 2.2 Format-Specific Details

**TXT (Fixed-Width):**
- **Line Length:** Variable (~120-140 chars depending on locality name length)
- **Header Rows:** 2 rows (title + column headers)
- **Data Start Pattern:** `^[A-Z0-9]{5}` (HCPCS code)
- **Layout Version:** `OPPSCAP_2025D_LAYOUT v2025.4.0` (in layout_registry.py)
- **Encoding:** UTF-8 (ASCII-compatible)

**Column Positions (0-indexed, verified 2025-10-17):**
```python
{
    'hcpcs':           {'start': 0,  'end': 5},    # 5 chars
    'modifier':        {'start': 6,  'end': 8},    # 2 chars (may be blank)
    'status':          {'start': 9,  'end': 11},   # 2 chars
    'mac':             {'start': 13, 'end': 18},   # 5 chars
    'locality_code':   {'start': 21, 'end': 23},   # 2 chars
    'facility_price':  {'start': 76, 'end': 83},   # 7 chars (decimal)
    'nonfacility_price': {'start': 97, 'end': 104}, # 7 chars (decimal)
}
```

**CSV:**
- **Delimiter:** Comma
- **Header Structure:** Row 1 = column names
- **Skip Rows:** `skiprows=0`
- **Quoting:** QUOTE_MINIMAL
- **CMS Typo:** ⚠️ **"NON-FACILTY PRICE"** (missing I in FACILITY) - See §5.1

**Example Header:**
```csv
HCPCS,MOD,PROCSTAT,CARRIER,LOCALITY,FACILITY PRICE,NON-FACILTY PRICE
```

**Critical:** Alias map MUST include typo: `'non-facilty price': 'nonfacility_price'`

**XLSX:**
- **Sheet Name:** First sheet
- **Header Row:** Row 3 (skip 2 title rows)
- **Data Type:** May include historical quarters (causes >10% row variance)
- **Known Issues:** 
  - Multiple quarters mixed in single file
  - Historical data causes duplicate NK violations
  - Deferred pending CMS format clarification

**ZIP:**
- **Inner Files:** Typically `OPPSCAP_Oct.txt`
- **Format Detection:** Content-based (checks for fixed-width pattern)

---

## 3. Schema & Column Mapping

### 3.1 Natural Keys

**Schema Version:** cms_oppscap_v1.1 (current)

```python
NATURAL_KEYS = ['hcpcs', 'modifier', 'mac', 'locality_code']
```

**Evolution:**
- **v1.0 (DEPRECATED):** `['hcpcs', 'locality_code']` ❌ Missing modifier & MAC
- **v1.1 (CURRENT):** `['hcpcs', 'modifier', 'mac', 'locality_code']` ✅ Corrected

**Why 4-Field NK (Most Complex in RVU Suite):**

1. **HCPCS:** Different procedures have different caps
2. **Modifier:** Same HCPCS may have different caps by modifier (26, TC, etc.)
3. **MAC:** Same HCPCS+modifier may vary by contractor region
4. **Locality:** Same HCPCS+modifier+MAC may vary by locality

**Example:**
```
('99213', None, '01112', '05') → Fac $45.50, Non-fac $67.80
('99213', '26', '01112', '05') → Different cap (professional component)
('99213', None, '02102', '01') → Different cap (different MAC/locality)
```

**Uniqueness:** 
- **Expected:** ~16,000 unique combinations
- **Production:** Verified unique in 2025D release
- **Test Fixtures:** Must be unique (QTS §5.1.1)

### 3.2 Column Specifications

| Column | Type | Nullable | Range | Notes |
|--------|------|----------|-------|-------|
| **hcpcs** | string | No | 5 chars | Alphanumeric, uppercase |
| **modifier** | string | **YES** | 2 chars OR null | Alphanumeric when present ⚠️ |
| **status** | string | Yes | 2 chars | Procedure status code |
| **mac** | string | No | 5 digits | Zero-padded |
| **locality_code** | string | No | 2 digits | Zero-padded |
| **facility_price** | float64 | No | ≥$0 | 2dp precision |
| **nonfacility_price** | float64 | No | ≥$0 | 2dp precision |
| **effective_from** | datetime | No | N/A | Derived from metadata |
| **effective_to** | datetime | Yes | N/A | Derived from metadata |

**Nullable Modifier Validation Pattern:** ⚠️ **CRITICAL**

```python
# Modifier must be 2-char pattern OR null (not just pattern)
# ❌ WRONG: df['modifier'].str.match(r'^[A-Z0-9]{2}$')  # Fails on null
# ✅ CORRECT:
invalid_mod = df['modifier'].notna() & ~df['modifier'].str.match(r'^[A-Z0-9]{2}$', na=False)
```

### 3.3 Column Mapping (Alias Map)

```python
ALIAS_MAP = {
    # Standard CSV headers
    'hcpcs': 'hcpcs',
    'mod': 'modifier',
    'modifier': 'modifier',
    'procstat': 'status',
    'proc status': 'status',
    'carrier': 'mac',
    'mac': 'mac',
    'locality': 'locality_code',
    'locality code': 'locality_code',
    
    # Price columns (including CMS typo) ⚠️
    'facility price': 'facility_price',
    'fac price': 'facility_price',
    'non-facilty price': 'nonfacility_price',  # CMS TYPO (missing I) - REQUIRED!
    'non-facility price': 'nonfacility_price',  # Correct spelling
    'nonfac price': 'nonfacility_price',
    'nonfacility price': 'nonfacility_price',
}
```

**Critical CMS Typo:** "NON-FACILTY" (not "NON-FACILITY") - See §5.1

### 3.4 Loader Alignment

During publish, `_load_oppscap_data` still writes to `hcpcs_code`, `proc_status`, `price_fac`, `price_nonfac`.  
The loader now copies the parser columns below before numeric coercion to preserve compatibility with legacy schemas.

| Parser column       | Loader column  | Notes                                  |
|---------------------|----------------|----------------------------------------|
| `hcpcs`             | `hcpcs_code`   | Present in CSV/TXT exports             |
| `status`            | `proc_status`  | Alias handles both `procstat`/`status` |
| `facility_price`    | `price_fac`    | Standardized spelling before insert    |
| `nonfacility_price` | `price_nonfac` | Handles CMS typo “non-facilty”         |

**Post-ingestion check:**  
`select count(*) filter (where hcpcs_code is null) from opps_caps;` should return `0`.

---

## 4. Validation Rules

### 4.1 Pattern Validation

**Validation Rules (R-OPPSCAP-001 to R-OPPSCAP-007):**

| Rule | Column | Pattern | Action | Notes |
|------|--------|---------|--------|-------|
| **R-001** | hcpcs | `^[A-Z0-9]{5}$` | BLOCK | 5 alphanumeric chars |
| **R-002** | modifier | `^[A-Z0-9]{2}$ OR null` | BLOCK | Nullable pattern ⚠️ |
| **R-003** | mac | `^\d{5}$` | BLOCK | 5 digits, zero-padded |
| **R-004** | locality_code | `^\d{2}$` | BLOCK | 2 digits, zero-padded |
| **R-005** | facility_price | `>= 0` | BLOCK | Non-negative |
| **R-006** | nonfacility_price | `>= 0` | BLOCK | Non-negative |
| **R-007** | NK uniqueness | No duplicates | BLOCK | On 4-field NK |

### 4.2 Range Validation

**Price Ranges (2025D observed):**
- **Facility:** $0 - $500 (typical imaging caps)
- **Non-Facility:** $0 - $800 (higher for non-fac settings)

**WARN Tier (Log, Don't Reject):**
- Prices > $1000 (unusual but may be valid for some procedures)

**HARD Tier (Reject):**
- Negative prices (< $0)
- NaN values in required price columns

### 4.3 Row Count Validation (Large Dataset)

**Different from GPCI/ANES:** Much larger dataset (~16K rows)

| Rows | Severity | Action | Use Case |
|------|----------|--------|----------|
| <100 | CRITICAL | Raise ParseError | Severely incomplete |
| 100-1000 | WARN | Log warning | Partial file |
| 1000-20000 | OK | Normal | Expected production |
| >20000 | WARN | Log, verify duplicates | Universe expansion |

**Typical Production:** 15,000-17,000 rows

---

## 5. Known Data Quirks

### 5.1 CMS CSV Header Typo ⚠️ **CRITICAL**

**Issue:** CMS CSV files contain misspelled header.

**Typo:** "NON-**FACILTY** PRICE" (missing I in FACILITY)

**Impact:** Without alias, column is unmapped and causes:
- Missing required column error
- 100% rejection rate
- Parser failure

**Detection:**
```bash
# Check CMS file header
head -1 OPPSCAP_Oct.csv
# Output: HCPCS,MOD,PROCSTAT,CARRIER,LOCALITY,FACILITY PRICE,NON-FACILTY PRICE
#                                                                    ^^^^^^^ Missing I
```

**Resolution (REQUIRED):**
```python
# Alias map MUST include typo
ALIAS_MAP = {
    'non-facilty price': 'nonfacility_price',  # CMS typo - DO NOT REMOVE!
    'non-facility price': 'nonfacility_price',  # Correct spelling (future-proof)
}
```

**Lesson:** Always check CMS files for typos in column headers. Don't assume correct spelling.

**Test Coverage:** `test_oppscap_cms_typo_header` validates typo handling

**Cross-Release Stability:** Typo present in RVU25A, RVU25B, RVU25C, RVU25D (all 2025 quarters)

### 5.2 Nullable Modifier Validation

**Issue:** Modifier column is nullable (many procedures have no modifier).

**Validation Challenge:** Must validate pattern **OR** null (not just pattern).

**❌ Wrong Pattern:**
```python
# This fails on null modifiers (marks nulls as invalid)
invalid = ~df['modifier'].str.match(r'^[A-Z0-9]{2}$')
```

**✅ Correct Pattern:**
```python
# Only validate non-null values
invalid = df['modifier'].notna() & ~df['modifier'].str.match(r'^[A-Z0-9]{2}$', na=False)
```

**Applies To:** Any nullable string column with pattern validation

**Test Coverage:** `test_oppscap_null_modifier_allowed` validates null handling

### 5.3 Schema Breaking Change (v1.0 → v1.1)

**Issue:** v1.0 had incorrect 2-field NK (missing modifier & MAC).

**Original NK (v1.0):** `['hcpcs', 'locality_code']` ❌
- Missing modifier → false duplicates (26, TC, etc. collapsed)
- Missing MAC → false duplicates (same HCPCS across MACs)

**Corrected NK (v1.1):** `['hcpcs', 'modifier', 'mac', 'locality_code']` ✅

**Impact:**
- False duplicate rate: ~30% in v1.0
- Migration: Updated during development (before production)
- Database model was already correct (schema bug, not model bug)

**Lesson:** Verify NK against actual data cardinality before finalizing schema

### 5.4 Format Authority Matrix

**Problem:** XLSX format showed >10% row variance vs TXT.

**Analysis (RVU25D):**
- **TXT:** 16,247 rows
- **CSV:** 16,247 rows (100% parity)
- **XLSX:** 18,932 rows (+16.5% variance)

**Root Cause:** XLSX contains multiple quarters (Q3 + Q4 data mixed)

**Decision:**
- **TXT:** Designated authoritative format
- **CSV:** Parity format (tested against TXT)
- **XLSX:** Deferred (>10% variance threshold exceeded)

**Parity Thresholds (Per QTS §5.1.3):**
- NK overlap: ≥98% vs authority format
- Row count variance: ≤1% OR ≤2 rows

**XLSX Deferral Criteria:**
- Variance >10% (fails parity threshold)
- Contains unintended data (historical quarters)
- Not needed for production (TXT + CSV sufficient)

---

## 6. Schema & Natural Key Evolution

### Version 1.0 (2025-10-15) - DEPRECATED

**Issues:**
- ❌ NK: `['hcpcs', 'locality_code']` (missing modifier & MAC)
- ❌ Column names didn't match actual file structure
- ❌ False duplicate rate ~30%

**Problems:**
- Procedures with modifiers (26, TC) collapsed into single row
- Same HCPCS across different MACs treated as duplicates
- Database model was correct, schema was wrong

### Version 1.1 (2025-10-17) - CURRENT ✅

**Changes:**
- ✅ NK: `['hcpcs', 'modifier', 'mac', 'locality_code']` (4-field, fully disambiguates)
- ✅ Column order matches actual file layout
- ✅ Primary keys match natural key
- ✅ False duplicate rate: 0%

**Breaking Change:** NK changed, but migration was low-risk (schema bug, database already correct)

**Lesson:** Database models may be correct even if schema contracts are wrong. Check both.

---

## 7. Parser Implementation

### 7.1 Parser Details

**File:** `cms_pricing/ingestion/parsers/oppscap_parser.py`  
**Version:** v1.0.0  
**Schema:** cms_oppscap_v1.1  
**Tests:** Passing (golden + edge cases)

**Key Features:**
1. **CMS Typo Handling** (NON-FACILTY alias) ⭐
2. **4-Field Natural Key** (most complex in RVU suite)
3. **Nullable Modifier Pattern** (pattern OR null validation)
4. **Large Dataset Handling** (~16K rows, optimized)
5. **Format Authority** (TXT authoritative, XLSX deferred)
6. **Duplicate Handling:** keep='first' (preserves one copy)

### 7.2 Performance Metrics

**Real Data (16,247 rows):**
- **Parse Time:** 0.45s (TXT fixed-width)
- **Parse Time:** 0.38s (CSV)
- **Reject Rate:** 0%
- **Duplicate Rate:** 0% (with v1.1 NK)
- **Memory:** ~5MB DataFrame
- **Format Parity:** 100% (TXT vs CSV)

**Optimization Notes:**
- Fixed-width parsing faster than CSV for this dataset
- Vectorized validation critical for 16K rows
- No row-by-row loops (pandas vectorized ops only)

### 7.3 Validation Rules Summary

**7 validation rules enforced:**
```python
VALIDATION_RULES = {
    'R-OPPSCAP-001': 'HCPCS must match ^[A-Z0-9]{5}$',
    'R-OPPSCAP-002': 'Modifier must match ^[A-Z0-9]{2}$ or be null',  # Nullable!
    'R-OPPSCAP-003': 'MAC must be 5 digits (zero-padded)',
    'R-OPPSCAP-004': 'Locality must be 2 digits (zero-padded)',
    'R-OPPSCAP-005': 'Facility price must be >= 0',
    'R-OPPSCAP-006': 'Non-facility price must be >= 0',
    'R-OPPSCAP-007': 'Natural key (hcpcs, modifier, mac, locality) must be unique',
}
```

---

## 8. Integration Points

### 8.1 Joins with Other Datasets

**MPFS Payment Cap Comparison:**
```sql
-- Check if MPFS payment exceeds OPPS cap
SELECT 
    p.hcpcs,
    p.modifier,
    p.total_payment_mpfs,
    o.nonfacility_price as opps_cap,
    CASE 
        WHEN p.total_payment_mpfs > o.nonfacility_price 
        THEN o.nonfacility_price  -- Cap applies
        ELSE p.total_payment_mpfs  -- No cap
    END as final_payment
FROM mpfs_payments p
LEFT JOIN oppscap o 
  ON o.hcpcs = p.hcpcs
  AND (o.modifier = p.modifier OR (o.modifier IS NULL AND p.modifier IS NULL))
  AND o.mac = p.mac
  AND o.locality_code = p.locality
  AND o.effective_from <= p.service_date
  AND (o.effective_to IS NULL OR o.effective_to >= p.service_date);
```

**GPCI Locality Reference:**
```sql
-- Verify all OPPSCAP localities exist in GPCI
SELECT DISTINCT o.mac, o.locality_code
FROM oppscap o
LEFT JOIN gpci_indices g
  ON g.mac = o.mac
  AND g.locality_id = o.locality_code
WHERE g.mac IS NULL;
-- Should return 0 rows (all localities valid)
```

### 8.2 Natural Key Consistency

**Across RVU Datasets:**
- **OPPSCAP:** `['hcpcs', 'modifier', 'mac', 'locality_code']` (includes procedure)
- **GPCI:** `['mac', 'locality_code', 'effective_from']` (geography only)
- **ANES:** `['mac', 'locality_code', 'effective_from']` (geography only)

**Pattern:** Procedure-based datasets include HCPCS+modifier; geography-based include effective_from

---

## 9. Testing Strategy

### 9.1 Test Coverage

**Golden Tests:**
- `test_oppscap_golden_txt` - TXT fixed-width parsing
- `test_oppscap_golden_csv` - CSV parsing with typo alias
- `test_oppscap_metadata_injection` - Metadata fields
- `test_oppscap_row_hashing` - Deterministic hashing
- `test_oppscap_natural_key_sort` - 4-field NK sorting
- `test_oppscap_schema_compliance` - v1.1 columns
- Markers: `@pytest.mark.golden`

**Edge Case Tests:**
- `test_oppscap_null_modifier` - Validates null modifier handling
- `test_oppscap_cms_typo_header` - Validates NON-FACILTY alias
- Markers: `@pytest.mark.edge_case`

**Negative Tests:**
- Invalid HCPCS pattern
- Invalid modifier (not 2 chars, when non-null)
- Negative prices
- Duplicate 4-field NK
- Missing required columns
- Markers: `@pytest.mark.negative`

### 9.2 Test Fixtures

**Golden:**
- `OPPSCAP_sample.txt` - TXT format (~100 rows subset)
- `OPPSCAP_sample.csv` - CSV format (identical data, tests parity)
- Includes null modifier rows
- Includes CMS typo in CSV header

**Format Parity Testing:**
```python
# TXT and CSV should produce identical output
txt_result = parse_oppscap(txt_file, metadata)
csv_result = parse_oppscap(csv_file, metadata)

# Compare natural keys (should be identical)
txt_nk = set(zip(txt_result.data['hcpcs'], txt_result.data['modifier'], 
                  txt_result.data['mac'], txt_result.data['locality_code']))
csv_nk = set(zip(csv_result.data['hcpcs'], csv_result.data['modifier'],
                  csv_result.data['mac'], csv_result.data['locality_code']))

assert txt_nk == csv_nk, "TXT and CSV should have identical natural keys"
```

### 9.3 Performance Testing

**Large Dataset Validation:**
- Parse 16K rows in <1s
- Memory usage <10MB
- Vectorized operations only (no row loops)

**Benchmark Test:**
```python
@pytest.mark.benchmark
def test_oppscap_performance():
    # Parse full 16K row file
    result = parse_oppscap(full_file, metadata)
    assert result.metrics['parse_duration_sec'] < 1.0
```

---

## 10. Operational Notes

### 10.1 Quarterly Release Workflow

**Step 1: Download**
```bash
# OPPSCAP typically in RVU bundle
wget https://www.cms.gov/files/zip/rvu25d.zip
unzip rvu25d.zip OPPSCAP_Oct.txt
```

**Step 2: Verify**
```bash
# Quick sanity check
head -5 OPPSCAP_Oct.txt
wc -l OPPSCAP_Oct.txt  # Expect ~16K rows
grep "NON-FACILTY" OPPSCAP_Oct.csv  # Check if typo still present
```

**Step 3: Parse**
```bash
python -m cms_pricing.ingestion.parsers.oppscap_parser \
    --file OPPSCAP_Oct.txt \
    --release RVU25D \
    --year 2025 \
    --quarter D
```

**Step 4: Validate**
- Row count: 15K-17K ✅
- Duplicates: 0 (on 4-field NK)
- Parse time: <1s
- Format parity: TXT vs CSV ≥99.5%

### 10.2 Common Issues & Fixes

**Issue 1: "Missing column: nonfacility_price"**  
**Cause:** CMS typo "NON-FACILTY" not in alias map  
**Fix:** Add `'non-facilty price': 'nonfacility_price'` to ALIAS_MAP  
**Prevention:** Always check actual CSV headers, don't assume spelling

**Issue 2: "Invalid modifier: null" or "NaN rejected"**  
**Cause:** Validation doesn't allow null modifiers  
**Fix:** Use `df['modifier'].notna() & ~df['modifier'].str.match(...)`  
**Pattern:** Nullable string pattern validation

**Issue 3: "63% duplicate rate" (v1.0)**  
**Cause:** 2-field NK missing modifier & MAC  
**Fix:** Upgrade to v1.1 schema with 4-field NK  
**Prevention:** Verify NK against actual data cardinality

### 10.3 XLSX Deferral Rationale

**Why XLSX not supported:**
1. >10% row variance (16,247 TXT vs 18,932 XLSX)
2. Contains historical quarters (Q3 + Q4 mixed)
3. Would require complex quarter filtering logic
4. TXT + CSV sufficient for production

**Future Consideration:** If CMS clarifies XLSX structure, may add support

---

## 11. Quality Metrics

### 11.1 Expected Metrics (Production)

```json
{
  "total_rows": 16247,
  "valid_rows": 16247,
  "reject_rows": 0,
  "parse_duration_sec": 0.45,
  "hcpcs_count": 4823,
  "modifier_count": 12,
  "mac_count": 15,
  "locality_count": 89,
  "price_stats": {
    "facility_min": 0.0,
    "facility_max": 487.32,
    "nonfacility_min": 0.0,
    "nonfacility_max": 751.18
  },
  "encoding_detected": "utf-8"
}
```

### 11.2 SLO Thresholds

- **Reject Rate:** ≤0.1% (clean CMS data, occasional edge cases)
- **Parse Time:** <1s for 20K rows
- **Duplicate Rate:** 0% (on 4-field NK)
- **Format Parity:** TXT vs CSV ≥99.5% NK overlap

### 11.3 Monitoring Alerts

**CRITICAL:**
- Row count < 10K or > 25K (universe change)
- Reject rate > 1%
- Duplicate rate > 0% (indicates NK issue)
- Parse errors

**WARN:**
- Row count outside [15K-17K] (verify normal variance)
- Format parity < 99.5% (TXT vs CSV drift)
- Parse time > 2s (performance regression)
- Prices > $1000 (unusual caps, verify valid)

---

## 12. Layout Registry

**Key:** `('oppscap', '2025', 'D')` → `OPPSCAP_2025D_LAYOUT`

**Layout Version:** v2025.4.0 (positions verified)

**Columns (0-indexed):**
```python
{
    'hcpcs':            {'start': 0,  'end': 5},     # 5 chars
    'modifier':         {'start': 6,  'end': 8},     # 2 chars (nullable)
    'status':           {'start': 9,  'end': 11},    # 2 chars
    'mac':              {'start': 13, 'end': 18},    # 5 chars
    'locality_code':    {'start': 21, 'end': 23},    # 2 chars
    'facility_price':   {'start': 76, 'end': 83},    # 7 chars (decimal)
    'nonfacility_price':{'start': 97, 'end': 104},   # 7 chars (decimal)
}
```

**Verification:** Positions verified against `sample_data/rvu25d_0/OPPSCAP_Oct.txt` on 2025-10-17

---

## 13. Implementation References

**Parser:** `cms_pricing/ingestion/parsers/oppscap_parser.py` (616 lines)  
**Schema:** `cms_pricing/ingestion/contracts/cms_oppscap_v1.1.json`  
**Layout:** `cms_pricing/ingestion/parsers/layout_registry.py` (OPPSCAP_2025D_LAYOUT)  
**Tests:** `tests/ingestion/test_oppscap_parser_golden.py`  
**Fixtures:** `tests/fixtures/oppscap/` (TBD - parser complete, fixtures to be created)

**Planning Docs:**
- `planning/parsers/oppscap/README.md` - Implementation overview
- `planning/parsers/oppscap/AUTHORITY_MATRIX.md` - Format authority analysis
- `planning/parsers/oppscap/SCHEMA_BREAKING_CHANGE_ANALYSIS.md` - v1.0 → v1.1 migration

**Test Results:** Parser complete, smoke tested on real data (16K rows, 0 rejects, 0.45s)

**QTS Compliance:**
- §5.1.3: Format authority matrix (TXT authoritative)
- §5.1.1: Golden fixtures (to be created)
- Null handling pattern documented

**Related PRDs:**
- `PRD-rvu-gpci-prd-v0.1.md` - RVU bundle ingestion (§1.3)
- `PRD-mpfs-prd-v1.0.md` - MPFS imaging cap logic
- `STD-parser-contracts-prd-v2.0.md` - Parser standards

---

## Appendix A: Sample Data

### A.1 TXT Format (Fixed-Width)

```
A0426       L  01112  01                                               88.72     103.50
A0426  26   L  01112  01                                               16.15      19.01
A0426  TC   L  01112  01                                               72.57      84.49
```

**Columns:** HCPCS(5), MOD(2), STAT(2), MAC(5), LOC(2), FacPrice(7), NonFacPrice(7)

### A.2 CSV Format (With CMS Typo)

```csv
HCPCS,MOD,PROCSTAT,CARRIER,LOCALITY,FACILITY PRICE,NON-FACILTY PRICE
A0426,,L,01112,01,88.72,103.50
A0426,26,L,01112,01,16.15,19.01
A0426,TC,L,01112,01,72.57,84.49
```

**Note:** Header contains typo "NON-FACILTY" (parser handles via alias map)

### A.3 Natural Key Examples

**4-Field NK Disambiguation:**
```
('A0426', None, '01112', '01') → Fac $88.72, Non-fac $103.50  (No modifier)
('A0426', '26', '01112', '01') → Fac $16.15, Non-fac $19.01   (Professional component)
('A0426', 'TC', '01112', '01') → Fac $72.57, Non-fac $84.49   (Technical component)
```

Without modifier in NK: Would collapse to 1 row (❌ false duplicate)  
With 4-field NK: 3 separate rows (✅ correct)

---

## Appendix B: Implementation Checklist

Before implementing OPPSCAP parser:

- [ ] Verify 4-field NK in schema (hcpcs, modifier, mac, locality_code)
- [ ] Add CMS typo to alias map: "non-facilty price"
- [ ] Implement nullable modifier validation (notna() & pattern check)
- [ ] Set row count expectations: 15K-17K (not 100-120 like GPCI)
- [ ] Test format parity (TXT vs CSV ≥99.5%)
- [ ] Document XLSX deferral rationale (>10% variance)
- [ ] Create format authority matrix
- [ ] Optimize for large dataset (vectorized ops only)
- [ ] Test null modifier handling explicitly

---

## Appendix C: CMS Typo History

**Tracking CMS CSV Header Typos Across Releases:**

| Release | Typo Present | Header Text | Status |
|---------|--------------|-------------|--------|
| RVU25A (Q1 2025) | ✅ | "NON-FACILTY PRICE" | Alias required |
| RVU25B (Q2 2025) | ✅ | "NON-FACILTY PRICE" | Alias required |
| RVU25C (Q3 2025) | ✅ | "NON-FACILTY PRICE" | Alias required |
| RVU25D (Q4 2025) | ✅ | "NON-FACILTY PRICE" | Alias required |

**Lesson:** Typo consistent across all 2025 releases. Don't remove alias prematurely.

**Future Monitoring:** Check each new release. If CMS fixes typo, keep both aliases for backward compatibility.

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| **1.0** | 2025-10-21 | Initial OPPSCAP source reference. Documented CMS CSV header typo (NON-FACILTY), 4-field natural key rationale, nullable modifier validation pattern, schema evolution (v1.0 → v1.1 NK fix), format authority matrix (TXT authoritative, XLSX deferred >10% variance), large dataset row count tiers (~16K rows), integration points. Based on OPPSCAP Parser v1.0 implementation. |

---

**End of SRC-oppscap.md v1.0**
