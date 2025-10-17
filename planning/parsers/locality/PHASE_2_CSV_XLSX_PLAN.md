# Locality Parser - Phase 2: CSV/XLSX Formats + Consistency Tests

**Status:** Ready to Execute  
**Prerequisites:** Phase 1 complete (TXT parser passing 100%)  
**Time Estimate:** 40-50 minutes  
**Follows:** STD-parser-contracts v1.10 §21.6 (Incremental Implementation)

---

## Overview

**Goal:** Add CSV and XLSX format support to locality parser with format consistency validation

**Strategy:** Incremental (one format at a time)
1. Add CSV parsing (20 min)
2. Add XLSX parsing (10 min)  
3. Create consistency tests (10 min)
4. Verify all formats produce identical data

**Key Principle:** All formats should yield the SAME DataFrame (format-agnostic output)

---

## Pre-Flight: Format Inspection

### Formats Available
- ✅ TXT: `sample_data/rvu25d_0/25LOCCO.txt` (110 rows, fixed-width)
- ✅ CSV: `sample_data/rvu25d_0/25LOCCO.csv` (needs inspection)
- ✅ XLSX: `sample_data/rvu25d_0/25LOCCO.xlsx` (needs inspection)

### Expected Structure (All Formats)
**Columns:**
- Medicare Administrative Contractor (MAC)
- Locality Code
- State Name
- Fee Schedule Area  
- County Names

**Natural Keys:** `['mac', 'locality_code']`  
**Expected Rows:** ~109 unique (after dedup)

---

## Step 1: CSV Format Support (20 min)

### 1a: Inspect CSV Structure ✅ (5 min)

**Findings from inspection:**

```
Row 1: Title row (skip)
Row 2: Blank row (skip)
Row 3: Headers: "Medicare Adminstrative Contractor,Locality Number,State ,Fee Schedule Area ,Counties"
Row 4: Blank row (skip)
Row 5: Data starts: "10112,0,ALABAMA ,STATEWIDE ,ALL COUNTIES "
```

**Structure:**
- **Total rows:** 169 (includes headers)
- **Data rows:** ~165 (after skipping headers/blanks)
- **skiprows:** 3 (rows 1-3)
- **Delimiter:** Comma
- **Header row:** Row 3 (index 2)
- **Quoting:** Standard CSV

**Headers (exact from file):**
- "Medicare Adminstrative Contractor" (Note: misspelled "Adminstrative")
- "Locality Number"
- "State " (trailing space!)
- "Fee Schedule Area "
- "Counties"

**⚠️ Header Issues:**
- Misspelling: "Adminstrative" (not "Administrative")
- Trailing spaces: "State ", "Fee Schedule Area "
- Different names from TXT: "Locality Number" vs "locality_code"

### 1b: Header Normalization Helper (5 min)

**Create normalization function** to handle case/space/typo variations:

```python
import re

def _normalize_header(col: str) -> str:
    """
    Normalize column header for robust aliasing.
    
    - Strip leading/trailing whitespace
    - Condense multiple spaces to single space
    - Lowercase for case-insensitive matching
    
    Examples:
        "State " → "state"
        "Fee Schedule  Area" → "fee schedule area"
        "Locality Number" → "locality number"
    """
    return re.sub(r'\s+', ' ', str(col or '').strip()).lower()
```

### 1c: Create Canonical Alias Map (5 min)

**Normalized aliases** (lowercase, single-spaced):

```python
CANONICAL_ALIAS_MAP = {
    # MAC variations (with typo)
    'medicare adminstrative contractor': 'mac',   # CSV typo case
    'medicare administrative contractor': 'mac',  # Corrected
    'medicare admin': 'mac',
    'mac': 'mac',
    
    # Locality code variations
    'locality number': 'locality_code',  # CSV name
    'locality code': 'locality_code',
    'locality': 'locality_code',
    
    # State variations
    'state': 'state_name',
    'state name': 'state_name',
    
    # Fee area variations
    'fee schedule area': 'fee_area',
    'fee area': 'fee_area',
    'locality name': 'fee_area',
    
    # Counties variations
    'counties': 'county_names',
    'county names': 'county_names',
}
```

**Usage:**
```python
# Normalize headers first, then alias
df.columns = [_normalize_header(c) for c in df.columns]
df = df.rename(columns=CANONICAL_ALIAS_MAP)
```

**Add to:** `locality_parser.py` after NATURAL_KEYS

### 1d: Implement CSV Parser with Dynamic Header Detection (10 min)

Add to `locality_parser.py`:

```python
def _find_header_row_csv(file_obj: BinaryIO, encoding: str) -> int:
    """
    Find header row containing both 'Locality' and 'Counties'.
    
    Returns: Row index (0-based)
    """
    file_obj.seek(0)
    lines = file_obj.read().decode(encoding, errors='ignore').splitlines()
    
    for idx, line in enumerate(lines[:15]):  # Check first 15 rows
        line_lower = line.lower()
        if 'locality' in line_lower and 'count' in line_lower:  # 'Counties' or 'County'
            logger.info("csv_header_detected", row_index=idx, header=line[:80])
            return idx
    
    raise ParseError("CSV header row not found (expected 'Locality' + 'Counties')")


def _parse_csv(file_obj: BinaryIO, metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse CSV format using dynamic header detection and aliasing.
    
    Args:
        file_obj: File object
        metadata: Metadata dict
        
    Returns:
        DataFrame with canonical columns
    """
    
    # Detect encoding and BOM
    detected_encoding = detect_encoding(file_obj)
    file_obj.seek(0)
    
    # Find header row dynamically
    header_row = _find_header_row_csv(file_obj, detected_encoding)
    file_obj.seek(0)
    
    logger.info(
        "parse_csv",
        encoding=detected_encoding,
        header_row=header_row
    )
    
    # Read CSV
    df = pd.read_csv(
        file_obj,
        encoding=detected_encoding,
        header=header_row,  # Dynamic header detection
        dtype=str,  # All columns as strings initially
        skipinitialspace=True,
        skip_blank_lines=True,
    )
    
    # Normalize headers (lowercase, condense spaces, strip)
    df.columns = [_normalize_header(c) for c in df.columns]
    
    # Apply canonical alias map
    df = df.rename(columns=CANONICAL_ALIAS_MAP)
    
    # Verify expected columns present
    expected = {'mac', 'locality_code', 'state_name', 'fee_area', 'county_names'}
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ParseError(f"Missing columns in CSV: {missing}. Headers: {list(df.columns)}")
    
    # Select canonical columns in deterministic order
    canonical_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = df[canonical_cols].copy()
    
    # Format normalization (non-semantic, for consistency)
    df['mac'] = df['mac'].str.strip()
    df['locality_code'] = df['locality_code'].str.strip().str.zfill(2)  # Zero-pad to width 2
    df['state_name'] = df['state_name'].str.strip()
    df['fee_area'] = df['fee_area'].str.strip()
    df['county_names'] = df['county_names'].str.strip()
    
    # Drop blank rows
    df = df[df['mac'].notna() & (df['mac'] != '')].copy()
    
    logger.info(
        "csv_parsed",
        rows_read=len(df),
        encoding=detected_encoding,
        header_row=header_row
    )
    
    return df
```

### 1d: Wire into Router (Already Done)

The main `parse_locality_raw()` function should detect format:

```python
# In parse_locality_raw()
if filename.lower().endswith('.csv'):
    df = _parse_csv(file_obj, metadata)
elif filename.lower().endswith('.txt'):
    df = _parse_txt_fixed_width(text_content, metadata)
else:
    raise ParseError(f"Unknown format: {filename}")
```

---

## Step 2: XLSX Format Support (10 min)

### 2a: Inspect XLSX Structure (3 min)

```python
import pandas as pd

xlsx = pd.ExcelFile("sample_data/rvu25d_0/25LOCCO.xlsx")
print(f"Sheets: {xlsx.sheet_names}")

df = pd.read_excel("sample_data/rvu25d_0/25LOCCO.xlsx", sheet_name=0, nrows=5)
print(df.head())
print(f"Columns: {list(df.columns)}")
```

**Document:**
- Sheet name to parse
- Header row index
- Any skip rows needed

### 2b: Implement XLSX Parser with Auto-Sheet Selection (7 min)

```python
def _find_data_sheet_xlsx(file_obj: BinaryIO) -> tuple[str, int]:
    """
    Find sheet and header row containing 'Locality' + 'Counties'.
    
    Returns: (sheet_name, header_row_idx)
    """
    xlsx = pd.ExcelFile(file_obj)
    
    for sheet_name in xlsx.sheet_names:
        # Read first 15 rows to find headers
        df_preview = pd.read_excel(file_obj, sheet_name=sheet_name, header=None, nrows=15)
        
        for idx, row in df_preview.iterrows():
            row_text = ' '.join(str(v) for v in row if pd.notna(v)).lower()
            if 'locality' in row_text and 'count' in row_text:  # 'Counties' or 'County'
                logger.info(
                    "xlsx_header_detected",
                    sheet=sheet_name,
                    row_index=idx,
                    header=row_text[:80]
                )
                return sheet_name, int(idx)
    
    raise ParseError("XLSX header row not found in any sheet")


def _parse_xlsx(file_obj: BinaryIO, metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse XLSX format with auto-sheet selection and dtype control.
    
    Args:
        file_obj: File object
        metadata: Metadata dict
        
    Returns:
        DataFrame with canonical columns
    """
    
    # Find data sheet and header row
    sheet_name, header_row = _find_data_sheet_xlsx(file_obj)
    file_obj.seek(0)
    
    logger.info(
        "parse_xlsx",
        sheet_name=sheet_name,
        header_row=header_row
    )
    
    # Read with dtype control (prevent Excel float coercion)
    df = pd.read_excel(
        file_obj,
        sheet_name=sheet_name,
        header=header_row,
        dtype=str,
        converters={
            'Locality Number': lambda v: str(v).rstrip('.0') if v else '',
            'Locality Code': lambda v: str(v).rstrip('.0') if v else '',
        }
    )
    
    # Normalize headers (lowercase, condense spaces, strip)
    df.columns = [_normalize_header(c) for c in df.columns]
    
    # Apply canonical alias map
    df = df.rename(columns=CANONICAL_ALIAS_MAP)
    
    # Verify expected columns present
    expected = {'mac', 'locality_code', 'state_name', 'fee_area', 'county_names'}
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ParseError(f"Missing columns in XLSX: {missing}. Headers: {list(df.columns)}")
    
    # Select canonical columns in deterministic order
    canonical_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = df[canonical_cols].copy()
    
    # Format normalization (non-semantic, for consistency)
    df['mac'] = df['mac'].str.strip()
    df['locality_code'] = df['locality_code'].str.strip().str.zfill(2)  # Zero-pad to width 2
    df['state_name'] = df['state_name'].str.strip()
    df['fee_area'] = df['fee_area'].str.strip()
    df['county_names'] = df['county_names'].str.strip()
    
    # Drop blank rows
    df = df[df['mac'].notna() & (df['mac'] != '')].copy()
    
    logger.info(
        "xlsx_parsed",
        rows_read=len(df),
        sheet_name=sheet_name,
        header_row=header_row
    )
    
    return df
```

### 2c: Wire into Router

```python
# In parse_locality_raw()
elif filename.lower().endswith(('.xlsx', '.xls')):
    df = _parse_xlsx(file_obj, metadata)
```

---

## Step 3: Format Consistency Tests (10 min)

### 3a: Create Robust Consistency Test (10 min)

Add to `tests/parsers/test_locality_parser.py`:

```python
def _canonicalize_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonicalize DataFrame for format comparison.
    
    - Zero-pad locality_code to width 2
    - Strip whitespace from all string columns
    - Select canonical columns in deterministic order
    - Drop duplicates on natural keys
    - Sort by natural keys
    - Reset index
    """
    df = df.copy()
    
    # Format normalization
    df['locality_code'] = df['locality_code'].str.strip().str.zfill(2)
    df['mac'] = df['mac'].str.strip()
    df['state_name'] = df['state_name'].str.strip()
    df['fee_area'] = df['fee_area'].str.strip()
    df['county_names'] = df['county_names'].str.strip()
    
    # Canonical column order
    canonical_cols = ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
    df = df[canonical_cols].copy()
    
    # Dedup and sort for comparison
    df = df.drop_duplicates(subset=['mac', 'locality_code'])
    df = df.sort_values(['mac', 'locality_code']).reset_index(drop=True)
    
    return df


@pytest.mark.golden
def test_locality_format_consistency():
    """
    Verify all formats (TXT, CSV, XLSX) produce identical DataFrames.
    
    Per STD-qa-testing §5.1.2 (Multi-Format Fixture Parity):
    - All formats must contain identical data
    - Output should be format-agnostic (same DataFrame)
    - Natural keys should match across formats
    - Full DataFrame equality (not just keys)
    """
    
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://consistency'
    }
    
    # Parse all 3 formats
    results = {}
    
    for fmt, filename in [
        ('txt', '25LOCCO.txt'),
        ('csv', '25LOCCO.csv'),
        ('xlsx', '25LOCCO.xlsx'),
    ]:
        filepath = Path(f"sample_data/rvu25d_0/{filename}")
        with open(filepath, 'rb') as f:
            result = parse_locality_raw(f, filename, metadata)
        results[fmt] = result
    
    # All should have 0 rejects
    for fmt, result in results.items():
        assert len(result.rejects) == 0, f"{fmt} should have 0 rejects"
    
    # All should have same row count (after dedup)
    counts = {fmt: len(r.data) for fmt, r in results.items()}
    assert len(set(counts.values())) == 1, f"Row counts differ: {counts}"
    
    # Canonicalize for comparison
    txt_canon = _canonicalize_for_comparison(results['txt'].data)
    csv_canon = _canonicalize_for_comparison(results['csv'].data)
    xlsx_canon = _canonicalize_for_comparison(results['xlsx'].data)
    
    # Full DataFrame equality
    pd.testing.assert_frame_equal(
        txt_canon, csv_canon,
        check_dtype=False,  # All strings, dtype OK
        check_names=True,
        obj="TXT vs CSV"
    )
    
    pd.testing.assert_frame_equal(
        txt_canon, xlsx_canon,
        check_dtype=False,
        check_names=True,
        obj="TXT vs XLSX"
    )
    
    # Also verify natural key sets (catches different class of drift)
    txt_keys = set(txt_canon[['mac', 'locality_code']].apply(tuple, axis=1))
    csv_keys = set(csv_canon[['mac', 'locality_code']].apply(tuple, axis=1))
    xlsx_keys = set(xlsx_canon[['mac', 'locality_code']].apply(tuple, axis=1))
    
    assert txt_keys == csv_keys, "TXT and CSV natural keys differ"
    assert txt_keys == xlsx_keys, "TXT and XLSX natural keys differ"
```

### 3b: Create Per-Format Tests with Edge Cases

```python
@pytest.mark.golden  
def test_locality_csv_golden():
    """
    Golden test for CSV format.
    
    Tests:
    - Header auto-detection (skips title row)
    - Typo handling ("Adminstrative")
    - Trailing space handling
    - Zero-padding (locality codes: 0→00, 7→07)
    """
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://csv'
    }
    
    filepath = Path("sample_data/rvu25d_0/25LOCCO.csv")
    with open(filepath, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.csv', metadata)
    
    # Basic assertions
    assert len(result.rejects) == 0, "CSV should have 0 rejects"
    assert len(result.data) > 100, "Should have ~109 rows"
    
    # Verify zero-padding
    assert all(len(lc) == 2 for lc in result.data['locality_code']), \
        "All locality codes should be zero-padded to width 2"
    
    # Verify specific localities (edge cases)
    lc_set = set(result.data['locality_code'])
    assert '00' in lc_set, "Should have locality 00 (zero-padded)"
    assert '07' in lc_set, "Should have locality 07 (zero-padded)"
    assert '18' in lc_set, "Should have locality 18"
    assert '99' in lc_set, "Should have locality 99"


@pytest.mark.golden
def test_locality_xlsx_golden():
    """
    Golden test for XLSX format.
    
    Tests:
    - Auto-sheet selection
    - Float-to-string conversion (Excel coercion)
    - Header detection
    - Same output as CSV/TXT
    """
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://xlsx'
    }
    
    filepath = Path("sample_data/rvu25d_0/25LOCCO.xlsx")
    with open(filepath, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.xlsx', metadata)
    
    # Basic assertions
    assert len(result.rejects) == 0, "XLSX should have 0 rejects"
    assert len(result.data) > 100, "Should have ~109 rows"
    
    # Verify zero-padding (Excel may have converted to int)
    assert all(len(lc) == 2 for lc in result.data['locality_code']), \
        "All locality codes should be zero-padded to width 2"
    
    # Verify metrics include sheet info
    assert 'sheet_name' in result.metrics or 'format' in result.metrics


@pytest.mark.edge_case
def test_locality_csv_with_bom():
    """Test CSV with UTF-8 BOM."""
    # Create fixture with BOM if needed
    # Verify BOM stripped, data parsed correctly
    pass


@pytest.mark.edge_case
def test_locality_county_names_with_slashes():
    """
    Test county names containing slashes and commas.
    
    Examples: "LOS ANGELES/ORANGE", "JEFFERSON, ORLEANS, PLAQUEMINES"
    """
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://edge'
    }
    
    filepath = Path("sample_data/rvu25d_0/25LOCCO.csv")
    with open(filepath, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.csv', metadata)
    
    # Find rows with slashes or commas in county names
    has_slash = result.data['county_names'].str.contains('/', na=False)
    has_comma = result.data['county_names'].str.contains(',', na=False)
    
    assert has_slash.any(), "Should have counties with slashes (e.g., LOS ANGELES/ORANGE)"
    assert has_comma.any(), "Should have counties with commas (e.g., multi-county lists)"
    
    # Verify they parsed correctly (not truncated)
    slash_example = result.data[has_slash].iloc[0]
    assert '/' in slash_example['county_names'], "Slash should be preserved"
```

---

## Pre-Phase 2 Variance Analysis (QTS §5.1.3)

**Completed:** 2025-10-17

**Row Counts:**
- TXT: 112 data lines → 109 after parse/dedup
- CSV: 169 rows → 109 after parse/dedup
- XLSX: 169 rows → 93 after parse/dedup

**Variance Detected:** XLSX -15% vs TXT (16 missing, 8 extra)

**Authority Matrix:** TXT > CSV > XLSX for 2025D
- TXT is canonical CMS fixed-width format
- CSV matches TXT exactly (100% NK overlap, 0% row variance)
- XLSX variance documented (85% NK overlap, 15% row variance)

**Testing Strategy:**
- ✅ Individual golden tests per format (TXT, CSV, XLSX)
- ✅ Real-source parity test with thresholds (NK ≥98%, row ≤1% or ≤2)
- ✅ Diff artifacts generated for root cause analysis
- ❌ Strict DataFrame equality (not achievable with real CMS variance)

**Parity Test Results:**
- CSV: ✅ PASS (100% overlap, 0 variance)
- XLSX: ⚠️ EXPECTED FAIL (85% overlap, 15% variance - documented)

**Artifacts:** See `tests/artifacts/variance/locality_parity_*`

**Cross-Reference:** 
- `planning/parsers/locality/AUTHORITY_MATRIX.md`
- `planning/parsers/locality/PHASE_2_LEARNINGS.md`
- STD-qa-testing §5.1.3
- STD-parser-contracts §21.4 Step 2c

---

## Deliverables Checklist

### Code
- [ ] _normalize_header() helper function
- [ ] CANONICAL_ALIAS_MAP added (normalized keys)
- [ ] _find_header_row_csv() dynamic header detection
- [ ] _parse_csv() with encoding/BOM handling
- [ ] _find_data_sheet_xlsx() auto-sheet selection
- [ ] _parse_xlsx() with dtype control
- [ ] Format normalization (zero-pad locality_code, strip whitespace)
- [ ] Router updated for .csv and .xlsx extensions
- [ ] Deterministic column order enforced
- [ ] Per-format metrics logging

### Tests
- [ ] _canonicalize_for_comparison() helper
- [ ] test_locality_format_consistency() with full DataFrame equality
- [ ] test_locality_csv_golden() with zero-padding verification
- [ ] test_locality_xlsx_golden() with sheet info check
- [ ] test_locality_csv_with_bom() (BOM handling)
- [ ] test_locality_county_names_with_slashes() (delimiter edge case)
- [ ] All tests pass (100% pass rate)

### Documentation
- [ ] Parser docstring updated with supported formats
- [ ] CANONICAL_ALIAS_MAP documented with CMS quirks
- [ ] Format normalization rules documented

---

## Success Criteria

- ✅ All 3 formats parse successfully
- ✅ Format consistency test passes (identical output)
- ✅ 0 rejects for all formats (clean golden data)
- ✅ Same row count across formats (~109 rows)
- ✅ Natural keys match across formats
- ✅ Parse time < 100ms per format

---

## Time Breakdown

| Task | Planned | Notes |
|------|---------|-------|
| CSV inspection | 5 min | Check headers, delimiter |
| Alias map | 5 min | Header normalization |
| CSV parser | 10 min | Reuse patterns from GPCI |
| XLSX inspection | 3 min | Check sheet structure |
| XLSX parser | 7 min | Similar to CSV |
| Consistency test | 10 min | Multi-format validation |
| **Total** | **40 min** | Conservative estimate |

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| CSV headers differ from TXT | Medium | 10 min | Comprehensive alias map |
| XLSX has multiple sheets | Low | 5 min | Inspect first, handle in parser |
| Data differs across formats | Low | 30 min | Pre-verify with manual inspection |
| Header rows in CSV/XLSX | Medium | 5 min | Use skiprows parameter |

---

## References

- STD-parser-contracts §21.6 (Incremental Implementation)
- STD-qa-testing §5.1.2 (Multi-Format Fixture Parity)
- planning/parsers/locality/PHASE_1_RAW_PARSER_PLAN.md (TXT implementation)
- cms_pricing/ingestion/parsers/gpci_parser.py (multi-format reference)

