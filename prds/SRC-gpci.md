# GPCI (Geographic Practice Cost Index) Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** MPFS Ingester, RVU Services, Pricing API, QA  
**Change control:** PR review  
**Review cadence:** Quarterly (with CMS releases)

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog
- **PRD-rvu-gpci-prd-v0.1.md:** GPCI ingestion product requirements
- **PRD-mpfs-prd-v1.0.md:** MPFS uses GPCI for geographic adjustments
- **STD-parser-contracts-prd-v2.0.md:** Parser core contracts
- **RUN-parser-qa-runbook-prd-v1.0.md:** Pre-implementation verification (§1 format verification)
- **STD-qa-testing-prd-v1.0.md:** QA testing standards (§5.1.1 golden fixture hygiene)
- **REF-cms-pricing-source-map-prd-v1.0.md:** CMS pricing source inventory

**Last Updated:** 2025-10-17  
**Verified Against CMS Release:** RVU25D (2025 Q4)

---

## 1. Overview

**Official CMS Name:** Geographic Practice Cost Indices (GPCI)  
**Dataset Code:** GPCI  
**Source URL:** [CMS PFS Relative Value Files](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)  
**Release Cadence:** Quarterly (RVU25A/B/C/D) + Annual updates  
**Business Purpose:** Geographic cost adjustment factors applied to physician payment calculations. GPCI values adjust for regional variations in work, practice expense, and malpractice costs.

**Typical Characteristics:**
- **File Size:** < 1MB (text format)
- **Row Count:** 100-120 rows (varies slightly by CMS policy changes)
- **Update Frequency:** Quarterly with letter suffixes (A/B/C/D where D is typically the annual)
- **Effective Dates:** Corresponds to Medicare physician fee schedule updates

**Payment Formula Context:**
```
Payment = [(Work RVU × Work GPCI) + (PE RVU × PE GPCI) + (MP RVU × MP GPCI)] × Conversion Factor
```

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Parser Support | Notes |
|--------|-----------|--------------|----------------|-------|
| **TXT** | `.txt` | ✅ Always | ✅ Implemented | Fixed-width, CMS official format |
| **CSV** | `.csv` | ✅ Always | ✅ Implemented | Alternative distribution |
| **XLSX** | `.xlsx` | ✅ Always | ✅ Implemented | Excel workbook, may include historical quarters |
| **ZIP** | `.zip` | ✅ Common | ✅ Implemented | Archives one of the above formats |

### 2.2 Format-Specific Details

**TXT (Fixed-Width):**
- **Line Length:** 173 chars (measured, not 200 as initially assumed)
- **Header Rows:** 3 rows (title + blank + column headers)
- **Data Start Pattern:** `^\d{2}` (2-digit locality code)
- **Layout Version:** `gpci_2025_q4` (in layout_registry.py)
- **Encoding:** UTF-8 (CP1252 fallback for older files)
- **Detection:** No header row with "locality", starts directly with data

**CSV:**
- **Delimiter:** Comma
- **Header Structure:** Row 1 = title ("2025 GPCI..."), Row 2 = column names
- **Skip Rows:** `skiprows=2` in pandas
- **Quoting:** QUOTE_MINIMAL
- **Column Names:** Underscore-separated with year prefix (e.g., `2025_pw_gpci_(with_1.0_floor)`)

**XLSX:**
- **Sheet Name:** First sheet (Sheet1 or similar)
- **Header Row:** Row 3
- **Data Type:** Full dataset (may include 100+ rows with quarterly snapshots)
- **Known Issues:** 
  - Excel may coerce GPCI values to float (precision loss)
  - Multiple quarters may be present → duplicates expected
  - Contains more rows than quarterly TXT/CSV (historical data)

**ZIP:**
- **Inner Files:** Typically `GPCI2025.txt` or similar
- **Format Detection:** Content-based (checks for fixed-width pattern vs delimiters)
- **Note:** Inner file format varies, parser performs content sniffing

---

## 3. Schema & Column Mapping

### 3.1 Natural Keys

```python
NATURAL_KEYS = ['mac', 'locality_code', 'effective_from']
```

**Why MAC + locality + effective_from?**
- MAC disambiguates locality code reuse (e.g., locality `00` exists in AL, AZ, AR, etc.).
- `effective_from` differentiates quarterly vintages when CMS bundles multiple quarters in XLSX.
- This NK matches `cms_gpci_v1.3.json` and the live parser; older `['locality_code']` guidance (v1.2) is deprecated.

**Uniqueness expectations:** 
- **Production releases:** 100-120 unique (`mac`, `locality_code`, `effective_from`) tuples.
- **Historical XLSX:** May include multiple quarters; uniqueness enforced per quarter.
- **Golden fixtures:** Smaller row counts acceptable, but NK must remain unique.

### 3.2 Schema Contract

**Location:** `cms_pricing/ingestion/contracts/cms_gpci_v1.3.json`

**Core Columns (hash order):**
| Column | Type | Nullable | Validation | Notes |
|--------|------|----------|------------|-------|
| `mac` | String(5) | N | `^\d{5}$` | Medicare Administrative Contractor (zero-padded) |
| `locality_code` | String(2) | N | `^\d{2}$` | 2-digit locality identifier |
| `gpci_work` | Decimal(5,3) | N | Warn: [0.30, 2.00], Fail: [0.20, 2.50] | Work index (statutory 1.0 floor applied by CMS) |
| `gpci_pe` | Decimal(5,3) | N | Warn: [0.30, 2.00], Fail: [0.20, 2.50] | Practice expense index |
| `gpci_mp` | Decimal(5,3) | N | Warn: [0.30, 2.00], Fail: [0.20, 2.50] | Malpractice index |
| `effective_from` | Date | N | yyyy-mm-dd | Derived from quarter vintage (A/B/C/D → Jan/Apr/Jul/Oct 1) |

**Additional columns:** 
- **Required metadata:** `source_release`, `source_inner_file`, `row_content_hash`, provenance fields.  
- **Optional enrichment:** `effective_to`, `locality_name`, `state` (populated when present in source or via locality crosswalk).

### 3.3 Column Header Aliases

```python
# Map CMS header variations to schema canonical names
ALIAS_MAP = {
    # TXT format (from layout or CMS documentation)
    'locality code': 'locality_code',
    'locality name': 'locality_name',
    '2025 PW GPCI (with 1.0 floor)': 'gpci_work',
    '2025 PE GPCI': 'gpci_pe',
    '2025 MP GPCI': 'gpci_mp',
    
    # CSV format (underscore-separated after normalization)
    'locality_code': 'locality_code',
    'locality_name': 'locality_name',
    '2025_pw_gpci_(with_1.0_floor)': 'gpci_work',
    '2025_pe_gpci': 'gpci_pe',
    '2025_mp_gpci': 'gpci_mp',
    
    # XLSX format (space-separated)
    '2025 PW GPCI': 'gpci_work',
    '2025 PE GPCI': 'gpci_pe',
    '2025 MP GPCI': 'gpci_mp',
    
    # Historical variations (older releases)
    'pw_gpci_2024': 'gpci_work',
    'pe_gpci_2024': 'gpci_pe',
    'mp_gpci_2024': 'gpci_mp',
}
```

**Normalization Notes:**
- Parser applies `_normalize_column_names()` which converts spaces to underscores
- Parentheses in headers (e.g., "(with 1.0 floor)") are preserved then mapped via aliases
- Year prefix varies by release (2025, 2024, etc.)

---

## 4. Business Rules & Validations

### 4.1 Value Ranges

| Column | Warn Low | Warn High | Fail Low | Fail High | Typical | Notes |
|--------|----------|-----------|----------|-----------|---------|-------|
| `gpci_work` | 0.30 | 2.00 | 0.20 | 2.50 | 0.85-1.10 | Alaska often triggers WARN (>1.5) |
| `gpci_pe` | 0.30 | 2.00 | 0.20 | 2.50 | 0.80-1.20 | Urban centers trend higher |
| `gpci_mp` | 0.30 | 2.00 | 0.20 | 2.50 | 0.70-1.30 | Volatile quarter to quarter |

**Validation tiers (STD-parser-contracts-impl-v2.0 §2.3):**
- **BLOCK:** < 0.20 or > 2.50 (hard fail, quarantined)
- **WARN:** Outside [0.30, 2.00] but within BLOCK bounds (logged, not rejected)
- **OK:** Within [0.30, 2.00]

### 4.2 Derived Fields

```python
# Effective date from metadata (GPCI has no inherent date)
df['effective_from'] = metadata['vintage_date']

# Product year and quarter for time-series tracking
df['product_year'] = metadata['product_year']
df['quarter_vintage'] = metadata['quarter_vintage']
```

### 4.3 Floor Values & Exceptions

**GPCI Work Floor:**
- **Rule:** Work GPCI has statutory floor of 1.0 (Social Security Act §1848(e)(1)(E))
- **Application:** Floor is already applied in CMS source data
- **Column Naming:** `gpci_work` reflects "with 1.0 floor" as indicated in header
- **Validation:** Parser logs WARN for values outside [0.30, 2.00] and blocks < 0.20 or > 2.50 per quality thresholds

**Geographic Exceptions:**
- **Alaska (locality 01):** Has highest work GPCI (~1.500+, well above floor)
- **Rural areas:** May have lower PE/MP values but within expected range

---

## 5. Known Data Quality Issues

### 5.1 Duplicate Locality Code 00 (resolved in schema v1.3)

**Historical issue:** Alabama and Arizona both shipped locality code `00`, which collided under the legacy NK (`['locality_code']`).  
**Current status:** Schema v1.3 requires `mac`, so rows like `01112-00` (AL) and `02102-00` (AZ) load as distinct records.  
**Parser handling:** 
- **v1.2 and earlier:** WARN-level quarantine of the second+ duplicate; legacy fixtures retained for regression coverage.  
- **v1.3+:** NK uniqueness enforced on (`mac`, `locality_code`, `effective_from`). Only true duplicates on that trio are quarantined; otherwise all locality `00` rows are accepted.

**Test coverage:** 
- Legacy fixture `tests/fixtures/gpci/edge_cases/GPCI2025_duplicate_locality_00.txt` ensures the fix remains in place.  
- Golden tests verify `mac` is required across TXT/CSV/XLSX/ZIP inputs.

### 5.2 Missing Values

| Column | Expected Nulls | Reason | Handling |
|--------|----------------|--------|----------|
| `locality_code` | Never | Natural key | BLOCK if missing |
| `locality_name` | Never | Required by CMS | BLOCK if missing |
| `gpci_work` | Never | Required for payment | BLOCK if missing |
| `gpci_pe` | Never | Required for payment | BLOCK if missing |
| `gpci_mp` | Never | Required for payment | BLOCK if missing |

**Note:** GPCI is a complete, required dataset. No columns should have nulls.

### 5.3 Outliers & Anomalies

**Valid Outliers (Do NOT Quarantine):**
- **Alaska (locality 01):** GPCI Work 1.500-1.700 (high cost area, valid)
- **Hawaii (locality 02):** GPCI Work/PE 1.100-1.300 (high cost area, valid)
- **Rural areas:** GPCI PE/MP 0.700-0.900 (low cost areas, valid)

**Action:** Log INFO, include in metrics, do not quarantine

---

## 6. CMS-Specific Context

### 6.1 MAC vs Locality Codes

**For GPCI:**
- **Primary Code:** Locality code (2-digit string: "00"-"99")
- **MAC Relationship:** Localities span MACs; not directly used in GPCI data
- **Crosswalk:** Not needed for GPCI parsing, but needed for:
  - County→Locality mapping (via ZIP locality crosswalk)
  - State→Locality assignments
  - Provider location→Locality resolution

**Note:** Locality code "00" is atypical; typically starts at "01"

### 6.2 Quarter-to-Date Mapping

**CMS Release Naming:**
- **RVU25A** → Q1 (Effective: January 1, 2025)
- **RVU25B** → Q2 (Effective: April 1, 2025)
- **RVU25C** → Q3 (Effective: July 1, 2025)
- **RVU25D** → Q4 / Annual (Effective: October 1, 2025)

**GPCI Updates:**
- Annual updates typically in Q4 (D release)
- Quarterly releases may have minor adjustments
- Most significant changes occur with annual cycle

---

## 7. Testing Strategy

### 7.1 Golden Fixtures (Clean Happy Path)

| Format | Fixture File | Rows | Rejects | Purpose |
|--------|--------------|------|---------|---------|
| TXT | `GPCI2025_sample.txt` | 18 | 0 | Clean sample, no duplicates |
| CSV | `GPCI2025_sample.csv` | 18 | 0 | Identical to TXT (format parity) |
| ZIP | `GPCI2025_sample.zip` | 18 | 0 | Contains clean TXT (format parity) |
| XLSX | `GPCI2025.xlsx` | 113 | 2+ | Full dataset with known duplicates |

**Location:** `tests/fixtures/gpci/golden/`

**Parity Requirement (QTS §5.1.2):**
- TXT, CSV, and ZIP must contain identical 18 rows
- XLSX is exception (full dataset for load testing)
- SHA-256 hashes documented in `tests/fixtures/gpci/golden/README.md`

**Sample Localities in Golden Fixtures:**
- 01: Alaska (high GPCI values)
- 06: Napa, CA
- 26: Los Angeles, CA
- ... 15 more diverse localities

### 7.2 Edge Case Fixtures (Real CMS Quirks)

| Fixture | Issue Tested | Expected Outcome |
|---------|--------------|------------------|
| `GPCI2025_duplicate_locality_00.txt` | AL and AZ both use locality 00 | 2 rejects, 1 valid (Alaska) |

**Location:** `tests/fixtures/gpci/edge_cases/`

**Marker:** `@pytest.mark.edge_case`

**Purpose:** Validate parser handling of authentic CMS data issues

### 7.3 Negative Fixtures

{Planned - not yet implemented}

**Location:** `tests/fixtures/gpci/negative/`

**Marker:** `@pytest.mark.negative`

---

## 8. Historical Format Changes

| Release | Change | Impact | Migration |
|---------|--------|--------|-----------|
| 2025D | Added "(with 1.0 floor)" to Work GPCI header | Alias map update | Added alias to ALIAS_MAP |
| 2024C | Changed line length from 180 to 173 chars | Layout update | Updated min_line_length in layout registry |
| 2023A | Introduced XLSX format | Parser extension | Added _parse_xlsx() support |

---

## 9. Implementation References

**Parser:** `cms_pricing/ingestion/parsers/gpci_parser.py` (547 lines)  
**Schema:** `cms_pricing/ingestion/contracts/cms_gpci_v1.0.json`  
**Layout:** `cms_pricing/ingestion/parsers/layout_registry.py` (fixed-width TXT)  
**Tests:** `tests/ingestion/test_gpci_parser_golden.py` (11 tests, 100% passing)  
**Fixtures:** `tests/fixtures/gpci/` (golden + edge_cases)

**Test Execution:**
```bash
# Run all GPCI tests
pytest -m gpci -v

# Run only golden tests
pytest -m "gpci and golden" -v

# Run edge case tests
pytest -m "gpci and edge_case" -v
```

**Related PRDs:**
- `PRD-rvu-gpci-prd-v0.1.md` - GPCI ingestion product requirements
- `STD-parser-contracts-prd-v2.0.md` - Parser core contracts
- `STD-parser-contracts-impl-v2.0.md` - Implementation templates (§2.1-2.3)
- `RUN-parser-qa-runbook-prd-v1.0.md` - QA procedures (§1-2)
- `STD-qa-testing-prd-v1.0.md` - Testing standards (v1.3 §5.1.1-5.1.2)

**Planning Documents:**
- `planning/parsers/gpci/LESSONS_LEARNED.md` - 15 implementation lessons
- `planning/parsers/gpci/QTS_COMPLIANCE_ACHIEVEMENT.md` - QTS compliance journey

---

## 10. Maintenance

**Next CMS Release Expected:** RVU26A (January 2026)  
**Last Verified:** 2025-10-17  
**Verification Checklist:**
- [x] File formats unchanged (TXT/CSV/XLSX/ZIP all supported)
- [x] Column headers stable (aliases cover all variations)
- [x] Value ranges within expected bounds (0.5-2.0)
- [x] Duplicate locality 00 quirk documented and tested
- [x] Layout accurate for TXT (173 chars, min_line_length=165)

**Known Issues:**
- Duplicate locality code 00 (AL/AZ) persists in some releases - edge case test validates handling
- XLSX may contain quarterly snapshots → duplicates expected and properly quarantined

**Future Enhancements:**
- Add negative test fixtures (malformed data)
- Add performance test for large-scale GPCI updates
- Document state-to-locality mapping for downstream services

---

*Based on: GPCI parser implementation (October 2025)*  
*QTS Compliance: 100% (11/11 tests passing)*  
*Reference Implementation: First QTS-compliant parser in cms-api*
