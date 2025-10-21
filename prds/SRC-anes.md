# ANES (Anesthesia Conversion Factor) Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** MPFS Ingester, RVU Services, Pricing API, QA  
**Change control:** PR review  
**Review cadence:** Quarterly (with CMS releases)

**Disambiguation:** ANES = Anesthesia Conversion Factor  
(NOT American National Election Studies)

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog
- **PRD-rvu-gpci-prd-v0.1.md:** RVU ingestion product requirements (§1.4 ANES)
- **PRD-mpfs-prd-v1.0.md:** MPFS uses ANES CF for anesthesia pricing
- **STD-parser-contracts-prd-v2.0.md:** Parser core contracts
- **REF-parser-reference-appendix-v1.0.md:** §A.8 Format-aware unit scaling
- **STD-qa-testing-prd-v1.0.md:** QA testing standards (§5.1.1 golden fixture hygiene)
- **REF-cms-pricing-source-map-prd-v1.0.md:** CMS pricing source inventory

**Last Updated:** 2025-10-21  
**Verified Against CMS Release:** RVU25D (2025 Q4)

---

## 1. Overview

**Official CMS Name:** Anesthesia Conversion Factors by Locality  
**Dataset Code:** ANES  
**Source URL:** [CMS PFS Relative Value Files](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)  
**Release Cadence:** Annual (typically with October/Q4 RVU bundle)  
**Business Purpose:** Locality-specific conversion factors for anesthesia services. Used to calculate anesthesia payments based on base units and time units.

**Typical Characteristics:**
- **File Size:** < 100KB (text format)
- **Row Count:** 100-120 rows (matches GPCI locality universe)
- **Update Frequency:** Annual (with YYYY suffix, e.g., ANES2025)
- **Effective Dates:** Calendar year (Jan 1 - Dec 31)

**Payment Formula Context:**
```
Anesthesia Payment = (Base Units + Time Units) × Anesthesia CF × Geographic Adjustment
```

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Parser Support | Units | Notes |
|--------|-----------|--------------|----------------|-------|-------|
| **TXT** | `.txt` | ✅ Always | ✅ Implemented | Integer cents | Fixed-width, CMS official format |
| **CSV** | `.csv` | ✅ Always | ✅ Implemented | USD decimals | Alternative distribution |
| **XLSX** | `.xlsx` | ⚠️ Rare | ⚠️ Not tested | USD decimals | May include historical years |
| **ZIP** | `.zip` | ✅ Common | ✅ Implemented | Varies by inner file | Archives one of the above formats |

### 2.2 Critical Format Difference: Units Variance ⭐

**Problem:** CMS publishes ANES in different units depending on format.

| Format | Units | Example Value | Meaning |
|--------|-------|---------------|---------|
| **TXT** | Integer cents | 1931 | $19.31 USD |
| **CSV** | USD decimals | 19.31 | $19.31 USD |

**Impact:** Parser MUST detect format and scale appropriately.

**Detection Pattern:**
```python
# Smart detection via median value
median = raw_values.median()

if median > 200:
    # TXT format: cents (typical range 1900-2800)
    cf_usd = raw_values / 100.0
else:
    # CSV format: already USD (typical range 19-28)
    cf_usd = raw_values
```

**Threshold Rationale:**
- Typical ANES CF in USD: $15-35
- Typical ANES CF in cents: 1500-3500
- Threshold 200 safely separates formats
- Allows edge cases (up to $150 USD) without misclassification

**See also:** REF-parser-reference-appendix §A.8 "Format-Aware Unit Scaling"

### 2.3 Format-Specific Details

**TXT (Fixed-Width):**
- **Line Length:** 77-78 chars
- **Header Rows:** None (starts directly with data)
- **Data Start Pattern:** `^\d{5}` (5-digit MAC code)
- **Layout Version:** `ANES_2025D_LAYOUT v2025.4.1` (in layout_registry.py)
- **Encoding:** UTF-8 (ASCII-compatible)
- **Footer:** 1 row disclaimer ("*Alaska has 1.5 floor")

**Column Positions (0-indexed, verified 2025-10-21):**
```python
{
    'mac':            {'start': 0,  'end': 5},   # Cols 1-5   (5 chars)
    'locality_id':    {'start': 12, 'end': 14},  # Cols 13-14 (2 chars)
    'locality_name':  {'start': 17, 'end': 57},  # Cols 18-57 (40 chars)
    'anesthesia_cf':  {'start': 73, 'end': 77},  # Cols 74-77 (4 chars) ⚠️ NOTE: NOT 70-74
}
```

**Layout Precision Warning:**
- Initial spec had CF at positions 70-74 ❌
- Actual data is at positions 73-77 ✅
- Off-by-3 error extracted "   1" instead of "1931"
- **Always verify manually** against real file before finalizing layout

**CSV:**
- **Delimiter:** Comma
- **Header Structure:** Row 1 = column names with verbose labels
- **Skip Rows:** `skiprows=0` (header is first line)
- **Quoting:** QUOTE_MINIMAL
- **Column Names:** Full English phrases (e.g., "National Anes CF of 20.3178")
- **Units:** USD decimals (pre-scaled)

**Example Header:**
```csv
Contractor,Locality,Locality Name,National Anes CF of 20.3178
10112,00,ALABAMA,19.31
```

**ZIP:**
- **Inner Files:** Typically `ANES2025.txt`
- **Format Detection:** Content-based (checks for fixed-width pattern)
- **Note:** Inner file determines units (cents or USD)

---

## 3. Schema & Column Mapping

### 3.1 Natural Keys

**Schema Version:** cms_anescf_v1.1 (current)

```python
NATURAL_KEYS = ['mac', 'locality_code', 'effective_from']
```

**Evolution:**
- **v1.0 (DEPRECATED):** `['locality_code', 'effective_from']` ❌ Missing MAC
- **v1.1 (CURRENT):** `['mac', 'locality_code', 'effective_from']` ✅ Corrected

**Why MAC is Required:**
- Locality code `'00'` appears in 10+ states (AL, AZ, AR, CO, CT, ID, IN, IA, KS, KY, MN, MS, etc.)
- Without MAC: 10+ false duplicates (56% of 109 rows!)
- Same issue found in GPCI v1.2 → v1.3 migration

**Example:**
```
MAC 10112 + Locality 00 = Alabama ($19.31)    ✅ Unique
MAC 03102 + Locality 00 = Arizona ($20.04)    ✅ Unique
```

**Uniqueness:** 
- **Expected:** 100-120 unique (MAC, locality_code) pairs per release
- **Production:** 109 localities in 2025D release
- **Test Fixtures:** Must be unique (clean golden data per QTS §5.1.1)

### 3.2 Column Specifications

| Column | Source Name | Type | Precision | Range | Notes |
|--------|-------------|------|-----------|-------|-------|
| **mac** | Contractor / Medicare Administrative Contractor | string | - | 5 digits | Zero-padded |
| **locality_code** | Locality / Locality Number | string | - | 2 digits | Zero-padded |
| **locality_name** | Locality Name | string | - | N/A | Descriptive, optional enrichment |
| **anesthesia_cf_usd** | Anesthesia CF / National Anes CF | float64 | 2dp | $0.01-100 | **TXT: scale from cents!** |
| **effective_from** | - | datetime | - | N/A | Derived from filename (ANES2025 → 2025-01-01) |
| **effective_to** | - | datetime | - | N/A | Dec 31 of year (or null for current) |

**Units & Scaling (Critical):**
- **Schema stores:** USD with 2 decimal places
- **TXT files contain:** Integer cents (e.g., 1931)
- **CSV files contain:** USD decimals (e.g., 19.31)
- **Parser must:** Auto-detect format and scale appropriately
- **Validation:** After scaling to USD

### 3.3 Column Mapping (Alias Map)

**TXT (Layout-Based):**
- Fixed positions extract: `mac`, `locality_id`, `locality_name`, `anesthesia_cf`
- `locality_id` → `locality_code` (normalization)
- `anesthesia_cf` → `anesthesia_cf_raw` → scale → `anesthesia_cf_usd`

**CSV (Header-Based):**
```python
ALIAS_MAP = {
    'contractor': 'mac',
    'medicare administrative contractor (mac)': 'mac',
    'locality': 'locality_code',
    'locality number': 'locality_code',
    'national anes cf of 20.3178': 'anesthesia_cf_raw',  # Verbose CMS header
    'national anes cf': 'anesthesia_cf_raw',
    'anesthesia cf': 'anesthesia_cf_raw',
}
```

---

## 4. Validation Rules

### 4.1 Range Validation (AFTER scaling to USD)

**WARN Tier (Log, Don't Reject):**
- **Range:** $15.00 - $35.00 USD
- **Rationale:** Typical CF values in 2025
- **Action:** Log to metrics, alert if >5% outside range

**HARD Tier (Reject):**
- **Range:** $0.01 - $100.00 USD
- **Rationale:** Must be positive, <$100 ceiling
- **Action:** Quarantine to rejects DataFrame

**Explicit Checks (Run First):**
1. **Zero/Negative:** CF ≤ 0 → reject with rule `NEGATIVE_OR_ZERO`
2. **HARD Range:** CF < $0.01 or > $100 → reject with rule `HARD_RANGE`

**Validation Ordering:**
- Run zero/negative BEFORE hard range (prevents duplicate rejects)
- Test assertions expect specific `validation_rule` values

### 4.2 Pattern Validation

- **MAC:** Must match `^\d{5}$` (5 digits, zero-padded)
- **Locality Code:** Must match `^\d{2}$` (2 digits, zero-padded)
- **CF Value:** Must be numeric (after scaling, not string)

### 4.3 Row Count Validation (Tiered)

| Rows | Severity | Action | Use Case |
|------|----------|--------|----------|
| ≤5 | EXEMPT | Skip validation | Negative test fixtures |
| 6-9 | CRITICAL | Raise ParseError | Malformed file |
| 10-49 | INFO | Log, allow | Small golden fixtures |
| 50-99 | WARN | Log, may be incomplete | Below production |
| 100-120 | OK | Normal | Expected production |
| >120 | WARN | Log, verify growth | Universe expansion |

**Rationale:**
- Production files: 100-120 rows
- Test fixtures: 10-30 rows (allowed)
- Negative fixtures: 1-3 rows (exempt)

---

## 5. Known Data Quirks

### 5.1 Duplicate Locality Codes (Resolved in v1.1)

**Issue:** Locality code `'00'` appears in multiple states.

**Examples:**
```
MAC 10112 (Alabama)    Locality 00  CF $19.31
MAC 03102 (Arizona)    Locality 00  CF $20.04
MAC 13102 (Connecticut) Locality 00  CF $21.24
... (10+ states total)
```

**Resolution:** Include MAC in natural key (schema v1.1)

**Impact:** Without MAC, 10+ states would be marked as duplicates (false positive rate ~10%)

### 5.2 Format Unit Variance (Critical)

**Issue:** TXT uses cents (1931), CSV uses USD (19.31)

**Resolution:** Smart detection via median value:
- Median > 200 → cents (scale by /100)
- Median ≤ 200 → USD (no scaling)

**Threshold Choice:** 200 is between max USD ($100) and min cents (1900)

**Testing:** 
- `test_anes_units_scaling_from_txt` verifies 1931 → $19.31
- Both formats produce identical output after scaling

### 5.3 Layout Position Precision (Implementation Bug)

**Issue:** Initial layout spec had CF at positions 70-74  
**Reality:** Actual CF data is at positions 73-77

**Detection:**
```python
line = "10112       00    ALABAMA                                                1931"
print(f"CF (70-74): {repr(line[70:74])}")  # '   1' ❌
print(f"CF (73-77): {repr(line[73:77])}")  # '1931' ✅
```

**Resolution:** Manual verification against real file required

**Lesson:** Always extract and print sample fields before finalizing layout

### 5.4 Footer Rows

**Issue:** TXT files may have 1 footer row with disclaimers

**Example:**
```
*Work GPCI reflects a 1.5 floor in Alaska established by the MIPPA.
```

**Detection:** MAC field doesn't match `^\d{5}$` pattern  
**Action:** Filter out rows with invalid MAC

---

## 6. Schema Evolution

### Version 1.0 (2025-09-30) - DEPRECATED

**Issues:**
- ❌ Missing MAC in natural key
- ❌ Included non-existent `state_fips` column
- ❌ Wrong precision (4dp instead of 2dp)

**Problems:**
- 10+ false duplicates (locality_code='00' across states)
- Schema mismatch with actual ANES layout

### Version 1.1 (2025-10-21) - CURRENT ✅

**Changes:**
- ✅ Natural Key: `['mac', 'locality_code', 'effective_from']`
- ✅ Removed `state_fips` (not in ANES layout)
- ✅ Precision: 2dp (matches CMS cents: 1931 → $19.31)
- ✅ Min value: $0.01 (CF must be positive)
- ✅ Units & scaling: Raw cents → USD documented
- ✅ Primary keys match natural key

**Breaking Change:** NK changed, requires migration for existing data

**Prevention:** Fixed before first release (no production migration needed)

---

## 7. Parser Implementation

### 7.1 Parser Details

**File:** `cms_pricing/ingestion/parsers/anes_parser.py`  
**Version:** v1.0.0  
**Schema:** cms_anescf_v1.1  
**Tests:** 23/23 passing (100%)

**Key Features:**
1. **Smart Scaling:** Auto-detects TXT (cents) vs CSV (USD)
2. **Date Derivation:** Extracts year from filename (ANES2025 → 2025-01-01)
3. **Strict Duplicates:** Quarantines ALL copies (not keep='first')
4. **Footer Filtering:** Removes CMS disclaimer rows
5. **Tiered Validation:** Row count, range, pattern checks

### 7.2 Performance Metrics

**Real Data (109 localities):**
- **Parse Time:** 0.012s
- **Reject Rate:** 0%
- **Duplicate Rate:** 0%
- **CF Range:** $19.12 - $27.86 USD
- **Memory:** Minimal (<1MB DataFrame)

### 7.3 Layout Registry

**Key:** `('anes', '2025', 'D')` → `ANES_2025D_LAYOUT`

**Layout Version:** v2025.4.1 (positions verified)

**Columns:**
```python
{
    'mac': {'start': 0, 'end': 5},           # 5 chars
    'locality_id': {'start': 12, 'end': 14}, # 2 chars
    'locality_name': {'start': 17, 'end': 57}, # 40 chars
    'anesthesia_cf': {'start': 73, 'end': 77}, # 4 chars (VERIFIED)
}
```

**Verification Process (2025-10-21):**
1. Printed sample line with positions
2. Extracted each field manually
3. Discovered CF at 73-77 (not 70-74 as initially assumed)
4. Updated layout to v2025.4.1

---

## 8. Integration Points

### 8.1 Joins with Other Datasets

**GPCI (Geographic Adjustment):**
```sql
-- Join ANES with GPCI for full locality profile
SELECT 
    a.mac,
    a.locality_code,
    a.anesthesia_cf_usd,
    g.work_gpci,
    g.pe_gpci,
    g.mp_gpci
FROM anes_cfs a
JOIN gpci_indices g 
  ON g.mac = a.mac 
  AND g.locality_id = a.locality_code
  AND g.effective_start = a.effective_start;
```

**Locality Crosswalk:**
```sql
-- Map ANES to counties for geographic analysis
SELECT 
    a.mac,
    a.locality_code,
    a.anesthesia_cf_usd,
    l.county_fips,
    l.county_name
FROM anes_cfs a
JOIN locality_county l
  ON l.mac = a.mac
  AND l.locality_code = a.locality_code;
```

### 8.2 Natural Key Consistency

**Across RVU Datasets:**
- **GPCI:** `['mac', 'locality_code', 'effective_from']` ✅ Matches ANES
- **OPPSCAP:** `['hcpcs', 'modifier', 'mac', 'locality_code']` (includes procedure)
- **Locality:** `['mac', 'locality_code', 'county_fips']` (includes geography)

**Pattern:** All locality-based datasets include `(mac, locality_code)` for joins

---

## 9. Testing Strategy

### 9.1 Test Coverage

**Golden Tests (14):**
- Per QTS §5.1.1: Clean fixtures with 0 rejects
- Formats: TXT, CSV, ZIP
- Markers: `@pytest.mark.golden`
- **NEW Tests:**
  - `test_anes_units_scaling_from_txt` (verifies 1931 → $19.31)
  - `test_anes_effective_dates_from_metadata` (verifies date derivation)

**Negative Tests (6):**
- Out of range ($150 > $100)
- Negative values ($-10)
- Zero values ($0)
- Duplicate natural keys
- Too few rows (<10)
- Missing required columns (MAC)
- Markers: `@pytest.mark.negative`

**Edge Case Tests (3):**
- Parser version tracking
- Encoding detection
- Date patterns (ANES2025, ANES25D)

### 9.2 Test Fixtures

**Golden:**
- `ANES2025_sample.txt` - 20 rows (TXT, integer cents)
- `ANES2025_sample.csv` - 20 rows (CSV, USD decimals)
- **Format Parity:** Different units but produce identical parsed output

**Negative:**
- 6 fixtures in CSV format (USD values for consistent testing)
- All have proper headers and MAC columns

---

## 10. Operational Notes

### 10.1 Quarterly Release Workflow

**Step 1: Download**
```bash
# ANES typically in RVU bundle
wget https://www.cms.gov/files/zip/rvu25d.zip
unzip rvu25d.zip ANES2025.txt
```

**Step 2: Verify**
```bash
# Quick sanity check
head -5 ANES2025.txt
wc -l ANES2025.txt  # Expect ~110 rows
```

**Step 3: Parse**
```bash
# Run parser with proper metadata
python -m cms_pricing.ingestion.parsers.anes_parser \
    --file ANES2025.txt \
    --release RVU25D \
    --year 2025 \
    --quarter D
```

**Step 4: Validate**
- Row count: 100-120 ✅
- CF range: $15-35 (typical)
- Duplicates: 0
- Parse time: <1s

### 10.2 Common Issues & Fixes

**Issue 1: CF values are $0.19 instead of $19.31**  
**Cause:** Double-scaling (CSV values scaled twice)  
**Fix:** Use smart detection (median threshold)

**Issue 2: "Missing column: anesthesia_cf_raw"**  
**Cause:** CSV header not in alias map  
**Fix:** Add all header variations to ALIAS_MAP

**Issue 3: "Duplicate natural key" for different states**  
**Cause:** MAC missing from natural key  
**Fix:** Upgrade to schema v1.1

---

## 11. Quality Metrics

### 11.1 Expected Metrics (Production)

```json
{
  "total_rows": 109,
  "valid_rows": 109,
  "reject_rows": 0,
  "parse_duration_sec": 0.012,
  "cf_value_stats": {
    "min": 19.12,
    "max": 27.86,
    "mean": 21.45,
    "median": 20.71
  },
  "locality_count": 109,
  "encoding_detected": "utf-8"
}
```

### 11.2 SLO Thresholds

- **Reject Rate:** ≤0% (clean CMS data)
- **Parse Time:** <1s for 120 rows
- **CF Range:** 95% within $15-35
- **Duplicate Rate:** 0%

### 11.3 Monitoring Alerts

**CRITICAL:**
- Row count < 90 or > 130
- Reject rate > 1%
- Parse errors

**WARN:**
- Row count outside [100-120]
- >10% CF values outside [$15-35]
- Parse time > 0.5s

---

## 12. Migration Guide (v1.0 → v1.1)

**Status:** N/A (v1.1 shipped before production use)

**For Reference (if backfill needed):**

1. **Database:** Add unique index on `(mac, locality_id, effective_start)`
2. **Backfill:** Re-parse historical ANES files with v1.1 parser
3. **Verification:** Check for 0 duplicates on new NK
4. **Cleanup:** Delete old rows with incorrect hashes

**See also:** `.cursor/plans/gpci_v13_migration_plan.md` for similar migration pattern

---

## 13. Future Considerations

### 13.1 Historical Data

- **Retention:** Keep all annual releases (ANES2024, ANES2025, etc.)
- **Storage:** Partition by `effective_from` year
- **Queries:** Use `effective_from` for time-travel queries

### 13.2 Locality Universe Changes

- **Scenario:** CMS adds/removes localities
- **Detection:** Row count drift alerts
- **Handling:** Automatic (parser handles variable row counts)

### 13.3 Format Changes

**Watch For:**
- New column positions (verify quarterly)
- New formats (PDF tables, JSON APIs)
- Unit changes (all USD, no cents?)

**Mitigation:** Layout version tracking + automated diff detection

---

## 14. Reference Implementation

**Files:**
- Schema: `cms_pricing/ingestion/contracts/cms_anescf_v1.1.json`
- Parser: `cms_pricing/ingestion/parsers/anes_parser.py` (758 lines)
- Layout: `cms_pricing/ingestion/parsers/layout_registry.py` (ANES_2025D_LAYOUT)
- Tests: `tests/ingestion/test_anes_parser_golden.py` (14 tests)
- Tests: `tests/ingestion/test_anes_parser_negatives.py` (6 tests)
- Fixtures: `tests/fixtures/anes/golden/` (TXT, CSV, README)
- Fixtures: `tests/fixtures/anes/negatives/` (6 edge cases)

**Sample Data:**
- Real CMS: `sample_data/rvu25d_0/ANES2025.txt` (109 rows)
- Test Fixture: `tests/fixtures/anes/golden/ANES2025_sample.txt` (20 rows)

**Test Results:** 23/23 passing (100%)

**QTS Compliance:**
- §5.1.1: Golden fixtures clean (0 rejects)
- §2.2.1: Proper pytest markers
- §G.1-G.4: Parser testing patterns
- §5.1.2: Multi-format support

---

## Appendix A: Sample Data

### A.1 TXT Format (Integer Cents)

```
10112       00    ALABAMA                                                1931
02102       01    ALASKA                                                 2786
03102       00    ARIZONA                                                2004
```

**Parsing:** Fixed-width at positions (0-5, 12-14, 17-57, 73-77)  
**Scaling:** 1931 ÷ 100 = $19.31 USD

### A.2 CSV Format (USD Decimals)

```csv
Contractor,Locality,Locality Name,National Anes CF of 20.3178
10112,00,ALABAMA,19.31
02102,01,ALASKA,27.86
03102,00,ARIZONA,20.04
```

**Parsing:** CSV with header normalization  
**Scaling:** Already USD, no scaling needed

---

## Appendix B: Implementation Checklist

Before implementing ANES parser:

- [ ] Verify layout positions manually (print sample extractions)
- [ ] Check units in TXT vs CSV (cents vs USD)
- [ ] Confirm MAC in natural key (prevents false duplicates)
- [ ] Test smart scaling logic with both formats
- [ ] Verify row count expectations (100-120 production)
- [ ] Add tiered row count validation (≤5 exempt, 6-9 fail, 10+ OK)
- [ ] Test effective date derivation from filename
- [ ] Create 14 golden + 6 negative tests (QTS compliant)
- [ ] Smoke test on real CMS data (0 rejects, 0 duplicates)

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| **1.0** | 2025-10-21 | Initial ANES source reference. Documented format unit variance (TXT cents vs CSV USD), layout positions (73-77 verified), schema evolution (v1.0 → v1.1 NK fix), smart scaling pattern, validation tiers, integration points. Based on ANES Parser v1.0 implementation. |

---

**End of SRC-anes.md v1.0**

