# Source Descriptor: CMS Locality-County Crosswalk (SRC-locality)

**Version:** 1.0  
**Status:** Active  
**Type:** Source Descriptor (SRC-)  
**Date:** 2025-10-17  
**Owners:** Data Platform Engineering  

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md:** GPCI uses locality codes (geographic payment areas)
- **STD-parser-contracts-prd-v1.0.md:** Parser implementation standards
- **STD-data-architecture-impl-v1.0.md:** Two-stage transformation pattern (§1.3)
- **STD-qa-testing-prd-v1.0.md:** Real-source variance testing (§5.1.3)
- **planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md:** Detailed implementation guide
- **planning/parsers/locality/AUTHORITY_MATRIX.md:** Format authority per vintage

---

## 1. Overview

The **CMS Locality-County Crosswalk** (file `25LOCCO.txt`) maps Medicare Administrative Contractor (MAC) locality codes to US counties and states. This crosswalk is essential for joining GPCI, RVU, and other Medicare payment data that use locality codes as geographic keys.

**Purpose:**
- Resolve locality codes to specific counties and states
- Enable geographic payment calculations (GPCI × RVU)
- Support Medicare payment area analysis
- Join MPFS payment data across geographic dimensions

**Key Characteristics:**
- **Granularity:** One row per MAC-locality-county combination (after Stage 2 explosion)
- **Update Frequency:** Quarterly (A/B/C/D) with MPFS releases
- **Stability:** Low volatility (localities change ~1-2x per year)
- **Formats:** TXT (fixed-width), CSV, XLSX

---

## 2. Data Source Details

### 2.1 Source & Access

**Primary Source:**
- **URL:** https://www.cms.gov/medicare/payment/fee-schedules/physician
- **File Pattern:** `25LOCCO.txt`, `25LOCCO.csv`, `25LOCCO.xlsx`
- **Bundle:** Included in RVU quarterly release ZIP (e.g., `rvu25d.zip`)
- **Direct Download:** Available standalone from CMS MPFS page

**Authority Format:** TXT (fixed-width) is canonical
- CSV and XLSX are derived/export formats
- Format priority: `TXT > CSV > XLSX` per vintage
- See `planning/parsers/locality/AUTHORITY_MATRIX.md` for details

**Cadence:**
- **Frequency:** Quarterly (Jan/Apr/Jul/Oct)
- **Vintage Labels:** A (Jan), B (Apr), C (Jul), D (Oct)
- **Example:** 2025D = October 2025 release

### 2.2 File Formats

**TXT (Fixed-Width):**
- **Layout:** `LOCCO_2025D_LAYOUT` in `layout_registry.py`
- **Columns:**
  - MAC (cols 1-12): Medicare Admin Contractor code
  - Locality Code (cols 13-18): 2-digit locality identifier
  - State Name (cols 19-50): State name (may be blank on continuation rows)
  - Fee Schedule Area (cols 51-120): Locality description
  - County Names (cols 121+): Comma/slash-delimited county list
- **Quirks:**
  - Header rows start with "Medicare Admi" or "MAC"
  - Continuation rows have blank MAC/locality/state (forward-filled)
  - Some localities have wrapped county lists

**CSV:**
- **Header Row:** Row 0 is title, Row 2 is headers (dynamic detection)
- **Columns:** Similar to TXT but with different names
  - "Medicare Adminstrative Contractor" (CMS has typo!)
  - "Locality Number"
  - "State"
  - "Fee Schedule Area"
  - "Counties"
- **Quirks:**
  - Trailing spaces in headers
  - Typo: "Adminstrative" (not "Administrative")
  - Blank rows interspersed

**XLSX:**
- **Sheet:** Usually first sheet, auto-detected
- **Header Row:** Similar to CSV (Row 0 title, Row 2 headers)
- **Quirks:**
  - Excel may coerce numeric codes (0 → 0.0)
  - Known variance: 2025D XLSX has 15% fewer rows than TXT/CSV
  - See `AUTHORITY_MATRIX.md` for documented variance

### 2.3 Schema & Natural Keys

**Raw Schema:** `cms_locality_raw_v1.0`
- **Columns:** `mac`, `locality_code`, `state_name`, `fee_area`, `county_names`
- **Data Types:** All strings (names, not FIPS codes)
- **Natural Keys:** `['mac', 'locality_code']`
- **Row Count:** ~109-111 unique localities (includes duplicates in raw layer)

**Canonical Schema (after Stage 2):** `std_localitycounty_v1.0`
- **Columns:** Includes FIPS codes (`state_fips`, `county_fips`)
- **Natural Keys:** `['locality_code', 'state_fips', 'county_fips']`
- **Row Count:** ~3,100+ (exploded to one-row-per-county)

### 2.4 Known Data Quality Issues

**Duplicates:**
- Real CMS files contain exact duplicate rows
- Example: MAC=05302, locality_code=99 appears twice
- **Raw parser preserves** duplicates (QTS §5.1.3 philosophy)
- Dedup happens in Stage 2 (FIPS normalizer)

**Format Variance (2025D):**
- **TXT:** 109 unique localities (after dedup)
- **CSV:** 109 unique localities (100% match with TXT)
- **XLSX:** 93 unique localities (78% overlap with TXT)
  - Missing: 24 localities from TXT
  - Extra: 8 localities not in TXT
  - See `tests/artifacts/variance/` for diff reports

**Encoding:**
- Typically UTF-8 or CP1252
- CSV may have UTF-8 BOM (EF BB BF)
- Parser auto-detects and handles both

---

## 3. Parser Implementation

### 3.1 Two-Stage Architecture

**Stage 1: Raw Parser** (`locality_parser.py`)
- **Purpose:** Parse CMS file layout-faithfully (names, not FIPS)
- **Output:** `raw_cms_localitycounty_<vintage>`
- **Schema:** `cms_locality_raw_v1.0` (state/county NAMES)
- **Natural Keys:** `['mac', 'locality_code']`
- **Duplicate Policy:** Preserve duplicates in raw layer
- **Formats:** TXT, CSV, XLSX
- **Version:** v1.1.0
- **Time:** ~60 min implementation (vs 8h GPCI baseline)

**Stage 2: FIPS Normalizer** (`normalize_locality_fips.py` - planned)
- **Purpose:** Derive FIPS codes, deduplicate, explode to per-county
- **Input:** Raw layer (`mac`, `locality_code`, state/county NAMES)
- **Output:** `std_localitycounty_v1.0` (with FIPS codes)
- **Transformations:**
  - County name → FIPS lookup (with alias map)
  - Explode multi-county localities to separate rows
  - Deduplicate on canonical natural keys
  - Add `match_method`, `mapping_confidence`
- **Status:** Not yet implemented
- **Estimated Time:** 90-120 min

### 3.2 Parser Features

**Format Detection:**
- Dynamic header detection (no hardcoded skiprows)
- Content sniffing for header rows ("Locality" + "Contractor" + "Counties")
- Auto-sheet selection for XLSX (searches all sheets)

**Format Normalization:**
- Zero-padding: MAC to 5 digits, locality_code to 2 digits
- Canonical alias map (handles CMS typo: "Adminstrative")
- Robust blank row filtering (before padding to avoid "nan" strings)
- BOM detection and stripping

**Data Quality:**
- Encoding auto-detection (UTF-8, CP1252, BOM)
- Forward-fill logic for TXT continuation rows (state_name)
- Preserves special characters (/, , - ) in county names
- Deterministic column order

### 3.3 Test Coverage

**Test Suite:** 18 tests (17 passing, 1 skipped)

**By Category:**
- **Golden** (`@pytest.mark.golden`): 3 tests (TXT, CSV, XLSX)
- **Real-Source** (`@pytest.mark.real_source`): 2 tests (CSV parity, XLSX variance)
- **Edge Case** (`@pytest.mark.edge_case`): 5 tests (duplicates, BOM, continuation, padding, delimiters)
- **Negative** (`@pytest.mark.negative`): 5 tests (metadata, format, empty, columns, header)
- **Other**: 3 tests (encoding, normalization, etc.)

**Coverage:** TBD (planned for Phase 3 final validation)

**QTS Compliance:**
- ✅ Golden fixture hygiene (§5.1.1)
- ✅ Real-source variance testing (§5.1.3)
- ✅ Test categorization with markers (§2.2.1)
- ✅ Error message quality (Appendix G.1)

---

## 4. Data Quality Notes

### 4.1 Known Quirks

**CMS File Issues:**
- **CSV Typo:** "Medicare Adminstrative Contractor" (misspelled)
- **Duplicate Rows:** Some MAC-locality combinations appear twice
- **XLSX Variance:** 2025D XLSX missing 15% of localities vs TXT
- **Continuation Rows:** TXT uses blank MAC/locality/state for multi-county entries

**Parser Mitigations:**
- Comprehensive alias map (handles typos)
- Duplicate preservation in raw layer (two-stage architecture)
- Dynamic header detection (robust to title rows)
- Forward-fill logic for continuation rows

### 4.2 Validation & Guardrails

**Raw Layer (Stage 1):**
- No range validation (preserves source exactly)
- No FIPS derivation (deferred to Stage 2)
- Duplicate preservation (log only, no dedup)
- Natural key uniqueness check (log duplicates, don't fail)

**Normalized Layer (Stage 2 - planned):**
- FIPS lookup with alias map (St/Saint, Parish, Borough)
- Fuzzy matching with deterministic tie-breakers
- Explode to one-row-per-county
- Deduplicate on canonical natural keys
- Quarantine non-matches with trace

### 4.3 Real-Source Variance Testing

**Per QTS §5.1.3:**
- **Authority Format:** TXT for 2025D
- **Thresholds:**
  - NK overlap ≥ 98% vs TXT
  - Row variance ≤ 1% OR ≤ 2 rows
- **CSV Status:** ✅ 100% parity (0 missing, 0 extra)
- **XLSX Status:** ⚠️ 78% overlap (24 missing, 8 extra - documented)

**Artifacts Generated:**
- `tests/artifacts/variance/locality_parity_summary_<format>.json`
- `tests/artifacts/variance/locality_parity_missing_in_<format>.csv`
- `tests/artifacts/variance/locality_parity_extra_in_<format>.csv`

---

## 5. Usage & Integration

### 5.1 Parser API

```python
from cms_pricing.ingestion.parsers.locality_parser import parse_locality_raw

# Metadata from ingestor
metadata = {
    'release_id': 'mpfs_2025_q4_rvu25d',
    'schema_id': 'cms_locality_raw_v1.0',
    'product_year': 2025,
    'quarter_vintage': 'D',
    'vintage_date': '2025-10-01',
    'file_sha256': '<sha256>',
    'source_uri': 'https://www.cms.gov/...'
}

# Parse
with open('25LOCCO.txt', 'rb') as f:
    result = parse_locality_raw(f, '25LOCCO.txt', metadata)

# Result
result.data        # DataFrame: ~110 rows (includes duplicates)
result.rejects     # DataFrame: [] (raw layer has no rejects)
result.metrics     # Dict: encoding, row counts, parse duration
```

### 5.2 Downstream Usage

**Stage 2: FIPS Normalization (planned)**
```python
# Input: Raw locality data (names)
raw_df = result.data

# Transform
normalized_df = normalize_locality_fips(raw_df, fips_crosswalk, alias_map)

# Output: std_localitycounty_v1.0 (with FIPS codes, exploded to per-county)
```

**Joining with GPCI:**
```sql
-- After Stage 2 normalization
SELECT 
    g.locality_code,
    l.state_fips,
    l.county_fips,
    l.county_name_canonical,
    g.gpci_work,
    g.gpci_pe,
    g.gpci_mp
FROM std_gpci g
JOIN std_localitycounty l 
  ON g.locality_code = l.locality_code
WHERE g.product_year = 2025
  AND l.product_year = 2025;
```

---

## 6. Operational Notes

### 6.1 Refresh Cadence

**Ingest Schedule:**
- Quarterly with MPFS/RVU releases
- Typically within 1-2 days of CMS publication
- Monitor CMS MPFS page for new releases

**Validation:**
- Compare row counts vs previous quarter
- Alert if >10% change (likely new MAC or locality reconfig)
- Verify TXT/CSV parity (should be 100%)
- Document XLSX variance (if applicable)

### 6.2 Troubleshooting

**Common Issues:**

| Issue | Symptom | Resolution |
|-------|---------|------------|
| XLSX row count differs | 78% NK overlap | Expected per 2025D - see AUTHORITY_MATRIX.md |
| Duplicate localities | 1-2 duplicate rows | Expected - preserved in raw layer |
| Header not found | ParseError on CSV | Check for title rows, CMS header changes |
| Forward-fill errors | Missing state names | Verify continuation row logic |

**Escalation:**
- Data Platform Engineering team
- See `planning/parsers/locality/` for implementation details

---

## 7. Performance & SLOs

**Parse Performance:**
- **Target SLO:** < 100ms per file (TXT, CSV, XLSX)
- **File Size:** ~18KB (25LOCCO.txt)
- **Row Count:** ~110 rows (raw)
- **Memory:** < 50MB peak RSS

**Benchmarks:**
- Measured via `pytest-benchmark`
- Gated by `ENFORCE_BENCH_SLO=1` for CI stability
- See `tests/parsers/test_locality_parser.py::test_locality_parse_performance`

---

## 8. Change Log

| Version | Date | Changes |
|---------|------|---------|
| **1.0** | **2025-10-17** | **Initial SRC document for Locality-County Crosswalk.** Covers source details, file formats (TXT/CSV/XLSX), two-stage architecture (raw → FIPS), parser implementation (v1.1.0), test coverage (18 tests, 17 passing), real-source variance testing (QTS §5.1.3), known quirks (CSV typo, XLSX variance, duplicates), format authority (TXT for 2025D), and operational notes. **Cross-References:** PRD-rvu-gpci, STD-parser-contracts §21.4, STD-qa-testing §5.1.3, TWO_STAGE_ARCHITECTURE.md, AUTHORITY_MATRIX.md. | TBD |

---

## Appendix A: Format Comparison

| Aspect | TXT | CSV | XLSX |
|--------|-----|-----|------|
| **Authority** | ✅ Primary | Secondary | Secondary |
| **Row Count (2025D)** | 109 unique | 109 unique | 93 unique |
| **Parity vs TXT** | N/A | 100% | 78% |
| **Header Detection** | Fixed-width layout | Dynamic (Row 2) | Dynamic (Row 2, Sheet 1) |
| **Encoding** | UTF-8/CP1252 | UTF-8/CP1252/BOM | Binary (Excel) |
| **Known Issues** | Continuation rows | Typo in header | 15% fewer rows, numeric coercion |
| **Parser Support** | ✅ v1.1.0 | ✅ v1.1.0 | ✅ v1.1.0 |
| **Test Coverage** | Golden + edge + negative | Golden + edge + negative + BOM | Golden + real-source variance |

---

## Appendix B: Example Data

**TXT Format (Fixed-Width):**
```
     Medicare Admi  Locality                           Fee Schedule Area                                             Counties
     10112          0           ALABAMA                STATEWIDE                                                      ALL COUNTIES
     10112          1           ALABAMA                BIRMINGHAM                                                     JEFFERSON
     02102          0           ALASKA                 STATEWIDE                                                      ALL COUNTIES
```

**CSV Format:**
```
Medicare Adminstrative Contractor,Locality Number,State ,Fee Schedule Area ,Counties
10112,0,ALABAMA ,STATEWIDE ,ALL COUNTIES 
10112,1,ALABAMA ,BIRMINGHAM ,JEFFERSON
02102,0,ALASKA ,STATEWIDE ,ALL COUNTIES
```

**XLSX Format:**
- Similar to CSV but in Excel workbook
- May have multiple sheets (title, data, notes)
- Parser auto-selects sheet with proper headers

---

**End of SRC-locality.md**

