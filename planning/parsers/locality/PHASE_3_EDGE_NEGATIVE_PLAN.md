# Phase 3: Edge Cases & Negative Tests - Locality Parser

**Date:** 2025-10-17  
**Parser:** Locality (Raw)  
**Dependencies:** Phase 1 (TXT) ✅, Phase 2 (CSV/XLSX) ✅, QTS §5.1.3 (v1.5) ✅, Authority Matrix (2025D: TXT > CSV > XLSX) ✅  
**Status:** Planning  

---

## Overview

Phase 3 completes the Locality raw parser test suite by adding:
1. **Edge case tests** (`@pytest.mark.edge_case`) - Real CMS quirks
2. **Negative tests** (`@pytest.mark.negative`) - Error handling
3. **Performance benchmarks** - Latency SLO validation
4. **Coverage validation** - Ensure ≥90% line coverage

**Goal:** Achieve QTS compliance with comprehensive test coverage for production readiness.

**Time Estimate:** 30-40 minutes

---

## Phase 3 Test Plan

### 1. Edge Case Tests (`@pytest.mark.edge_case`)

**Real CMS quirks identified:**

#### 1.1 Duplicate Natural Keys (Real CMS Data)
Raw layer must preserve duplicates; add explicit edge case test:

```python
@pytest.mark.edge_case
def test_locality_duplicate_natural_keys_real_data_preserved():
    """
    Raw layer must **preserve duplicates** exactly as in source.
    Known: Real 25LOCCO.txt contains duplicate NKs (e.g., MAC=05302, locality_code=99).
    Expectation:
    - Raw TXT/CSV/XLSX parsers do NOT deduplicate.
    - Canonical comparison helpers (for parity tests) may drop duplicates for NK-based comparison only.
    """
    # Load real TXT and assert duplicate presence by NK count > unique NK count
    # Assert no dedup performed in raw parse path
```

#### 1.2 CSV BOM Handling

```python
@pytest.mark.edge_case
def test_locality_csv_with_utf8_bom():
    """
    Test CSV with UTF-8 BOM (EF BB BF) header.
    
    Real CMS files sometimes have BOM from Excel exports.
    Parser should detect and strip BOM correctly.
    """
    csv_with_bom = b'\xef\xbb\xbf' + open('sample_data/rvu25d_0/25LOCCO.csv', 'rb').read()
    
    # Parse and verify:
    # - Encoding detected as UTF-8 (with BOM)
    # - Headers parsed correctly (no \ufeff in column names)
    # - Data integrity maintained
```

#### 1.3 County Names with Special Characters

Already partially covered, but expand:

```python
@pytest.mark.edge_case
def test_locality_county_names_special_chars():
    """
    Test county names with special characters and delimiters.
    
    Real examples from CMS data:
    - Slashes: "LOS ANGELES/ORANGE"
    - Commas: "JEFFERSON, ORLEANS, PLAQUEMINES"
    - Hyphens: "MIAMI-DADE"
    - Parentheses: "DOÑA ANA (NEW MEXICO)"
    - Multiple spaces: "SAN  FRANCISCO"
    """
    # Verify all special characters preserved
    # No truncation on parsing
    # Correct string stripping (no over-normalization)
```

#### 1.4 Continuation Rows (State Name Forward-Fill)

```python
@pytest.mark.edge_case
def test_locality_continuation_rows_forward_fill_state_only():
    """
    TXT continuation rows may omit the State field for multi-county localities.
    Raw parser forward-fills **state_name only**; MAC and locality_code are parsed from fixed-width spans.
    Example:
    Row 1: 01112  00  CALIFORNIA  STATEWIDE  ALL COUNTIES
    Row 2:                       (blank)     LOS ANGELES  LOS ANGELES/ORANGE
    Expectation:
    - state_name forward-filled on row 2 from row 1
    - mac and locality_code unchanged from their fixed-width fields
    """
    # Minimal fixture with continuation rows
    # Verify state forward-fill; verify MAC/locality_code unchanged
```

#### 1.5 XLSX Multi-Sheet Handling

```python
@pytest.mark.edge_case  
def test_locality_xlsx_multi_sheet_auto_select():
    """
    Test XLSX with multiple sheets - parser selects correct one.
    
    Real CMS XLSX files may have:
    - Cover/title sheet
    - Data sheet
    - Notes/metadata sheet
    
    Parser should auto-select sheet with "Locality" + "Contractor" headers.
    """
    # Create XLSX with 3 sheets: Title, Data, Notes
    # Verify parser selects "Data" sheet
    # Verify correct header row detected
```

#### 1.6 Zero-Padding Edge Cases

```python
@pytest.mark.edge_case
def test_locality_zero_padding_edge_cases():
    """
    Test zero-padding for edge case locality codes and MACs.
    
    Edge cases:
    - locality_code: 0, 1, 7, 99 (single and double digits)
    - MAC: 1112, 11402, 5302 (4-5 digits)
    
    All should be zero-padded consistently:
    - 0 → 00, 1 → 01, 7 → 07, 99 → 99
    - 1112 → 01112, 11402 → 11402, 5302 → 05302
    """
    # Test various padding widths
    # Verify consistent output format
```

#### 1.7 Wrapped County Lists (TXT line wrap)

```python
@pytest.mark.edge_case
def test_locality_txt_wrapped_county_list_treated_as_separate_rows():
    """
    Some vintages wrap long county lists onto the next line.
    Phase 1/2 policy: treat wrapped lines as separate rows in raw layer; joining/exploding occurs in Stage 2.
    Verify parser does not attempt to merge lines in raw phase.
    """
    # Construct a tiny TXT with a wrapped county list and assert two rows emitted
```

#### 1.8 CP1252 Encoding & Diacritics

```python
@pytest.mark.edge_case
def test_locality_cp1252_with_diacritics():
    """
    CSV in Windows-1252 with diacritics (e.g., 'Doña Ana').
    Parser should detect encoding, decode correctly, and preserve characters.
    """
    # Create a small CP1252-encoded CSV fixture and parse
```

---

### 2. Negative Tests (`@pytest.mark.negative`)

**Error handling scenarios:**

#### 2.1 Missing Required Metadata

```python
@pytest.mark.negative
def test_locality_missing_required_metadata():
    """
    Test parser raises ParseError for missing metadata fields.
    
    Required fields:
    - release_id, schema_id, product_year, quarter_vintage
    - vintage_date, file_sha256, source_uri
    """
    metadata_incomplete = {
        'release_id': 'test_2025d',
        # Missing schema_id and others
    }
    
    with pytest.raises(ParseError) as exc_info:
        parse_locality_raw(file_obj, '25LOCCO.txt', metadata_incomplete)
    
    # Verify error message lists missing fields
    assert 'schema_id' in str(exc_info.value)
```

#### 2.2 Unsupported File Format

```python
@pytest.mark.negative
def test_locality_unsupported_format():
    """
    Test parser rejects unsupported file formats.
    
    Supported: .txt, .csv, .xlsx
    Unsupported: .json, .xml, .pdf, etc.
    """
    with pytest.raises(ParseError) as exc_info:
        parse_locality_raw(file_obj, '25LOCCO.json', metadata)
    
    assert 'Unsupported format' in str(exc_info.value)
    assert '.txt, .csv, .xlsx' in str(exc_info.value)
```

#### 2.3 Empty File

```python
@pytest.mark.negative
def test_locality_empty_file():
    """
    Test parser handles empty files gracefully.
    """
    empty_bytes = BytesIO(b'')
    
    with pytest.raises(ParseError) as exc_info:
        parse_locality_raw(empty_bytes, '25LOCCO.txt', metadata)
    
    assert 'No data rows found' in str(exc_info.value)
```

#### 2.4 Malformed CSV (Missing Columns)

```python
@pytest.mark.negative
def test_locality_csv_missing_columns():
    """
    Test CSV with missing required columns raises ParseError.
    
    Required: mac, locality_code, state_name, fee_area, county_names
    """
    csv_bad = "MAC,State\n10112,ALABAMA\n"  # Missing locality_code, etc.
    
    with pytest.raises(ParseError) as exc_info:
        parse_locality_raw(BytesIO(csv_bad.encode()), '25LOCCO.csv', metadata)
    
    assert 'Missing columns' in str(exc_info.value)
    assert 'locality_code' in str(exc_info.value)
```

#### 2.5 CSV Header Not Found

```python
@pytest.mark.negative
def test_locality_csv_no_header_row():
    """
    Test CSV without proper header row raises ParseError.
    
    Parser looks for row with "Locality" + "Contractor" + "Counties"
    File without these tokens should fail gracefully.
    """
    csv_no_header = "Data,Values,Here\n10112,00,ALABAMA\n"
    
    with pytest.raises(ParseError) as exc_info:
        parse_locality_raw(BytesIO(csv_no_header.encode()), '25LOCCO.csv', metadata)
    
    assert 'header row not found' in str(exc_info.value).lower()
```

#### 2.6 XLSX Sheet Not Found

```python
@pytest.mark.negative
def test_locality_xlsx_no_data_sheet():
    """
    Test XLSX without proper data sheet raises ParseError.
    
    Parser looks for sheet with "Locality" + "Contractor" + "Counties"
    Workbook without these tokens should fail gracefully.
    """
    # Create XLSX with only "Cover" sheet (no data)
    # Should raise ParseError
    # Error message should mention sheet search failed
```

#### 2.7 Encoding Detection Failure

```python
@pytest.mark.negative
def test_locality_binary_garbage():
    """
    Test parser handles binary garbage gracefully.
    """
    garbage = BytesIO(b'\x00\x01\x02\x03\xff\xfe\xfd')
    
    with pytest.raises(ParseError) as exc_info:
        parse_locality_raw(garbage, '25LOCCO.txt', metadata)
    
    # Should fail during encoding detection or parsing
    # Error message should be actionable
```

#### 2.8 XLSX Numeric Coercion of Codes

```python
@pytest.mark.negative
def test_locality_xlsx_numeric_coercion_of_codes():
    """
    Excel often coerces 'Locality Number' to numeric (e.g., 0, 1.0).
    Parser must use converters/dtype=str and then zfill(2) to avoid 'nan' or '1.0' artifacts.
    """
    # Build minimal XLSX where 'Locality Number' is numeric; expect clean '00','01' strings
```

---

### 3. Performance Benchmarks

**Per QTS §3.3 (Performance Testing)**

#### 3.1 Parse Time SLO

```python
@pytest.mark.benchmark(group="locality_parse")
def test_locality_parse_performance(benchmark, monkeypatch):
    """
    Benchmark parse time per QTS performance SLO.
    SLO: < 100ms per format (TXT) — enforcement gated by ENFORCE_BENCH_SLO=1 to reduce CI flakiness.
    """
    def parse_txt():
        with open('sample_data/rvu25d_0/25LOCCO.txt', 'rb') as f:
            return parse_locality_raw(f, '25LOCCO.txt', metadata)
    result = benchmark(parse_txt)
    # Only assert in controlled environments
    import os
    if os.getenv("ENFORCE_BENCH_SLO") == "1":
        # Use elapsed stats provided by plugin (approximate, environment-dependent)
        assert benchmark.stats.stats.mean < 0.1, "Parse time > 100ms SLO"
```

#### 3.2 Memory Usage

```python
def test_locality_memory_usage():
    """
    Verify parser doesn't leak memory or create excessive copies.
    
    Expectation: Peak RSS < 50MB for standard file (~18KB input)
    """
    import tracemalloc
    
    tracemalloc.start()
    with open('sample_data/rvu25d_0/25LOCCO.txt', 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # Peak should be reasonable
    assert peak < 50 * 1024 * 1024, f"Peak memory {peak} > 50MB"
```

---

### 4. Coverage Validation

**Per QTS §7 (Quality Gates): ≥90% line coverage**

```bash
# Run with coverage
pytest tests/parsers/test_locality_parser.py --cov=cms_pricing/ingestion/parsers/locality_parser --cov-report=term-missing

# Verify ≥90%
pytest tests/parsers/test_locality_parser.py --cov=cms_pricing/ingestion/parsers/locality_parser --cov-fail-under=90
```

**Expected Coverage:**
- Main parser function: 100%
- Format-specific parsers (_parse_txt, _parse_csv, _parse_xlsx): 95-100%
- Helper functions (_normalize_header, _find_header_row_csv, etc.): 100%
- Error paths: 100% (via negative tests)

---

## Deliverables Checklist

### Edge Case Tests
- [ ] test_locality_duplicate_natural_keys_real_data()
- [ ] test_locality_csv_with_utf8_bom()
- [ ] test_locality_county_names_special_chars()
- [ ] test_locality_continuation_rows_forward_fill()
- [ ] test_locality_xlsx_multi_sheet_auto_select()
- [ ] test_locality_zero_padding_edge_cases()

### Negative Tests
- [ ] test_locality_missing_required_metadata()
- [ ] test_locality_unsupported_format()
- [ ] test_locality_empty_file()
- [ ] test_locality_csv_missing_columns()
- [ ] test_locality_csv_no_header_row()
- [ ] test_locality_xlsx_no_data_sheet()
- [ ] test_locality_binary_garbage()

### Performance Tests
- [ ] test_locality_parse_performance() (with pytest-benchmark)
- [ ] test_locality_memory_usage()

### Coverage
- [ ] Run coverage report
- [ ] Verify ≥90% line coverage
- [ ] Document any uncovered lines with rationale

### Documentation
- [ ] Update test file docstring with Phase 3 additions
- [ ] Document known edge cases in README or docstrings
- [ ] Update PHASE_3 plan with results

---

## Test Fixtures Needed

### Edge Case Fixtures

**tests/fixtures/locality/edge_cases/**
- `duplicate_keys.txt` - Contains exact duplicate rows
- `utf8_bom.csv` - CSV with UTF-8 BOM header
- `special_chars.csv` - County names with slashes, commas, hyphens
- `continuation_rows.txt` - Minimal TXT with forward-fill scenarios
- `multi_sheet.xlsx` - XLSX with 3 sheets (Title, Data, Notes)
- `wrapped_counties.txt` - TXT where county list wraps onto next line
- `cp1252_diacritics.csv` - Windows-1252 CSV containing diacritics (e.g., Doña Ana)

### Negative Test Fixtures

**tests/fixtures/locality/negative/**
- `empty.txt` - Empty file
- `missing_columns.csv` - CSV missing required columns
- `no_header.csv` - CSV without proper header row
- `no_data_sheet.xlsx` - XLSX without proper data sheet
- `binary_garbage.txt` - Non-text binary data

---

## Implementation Strategy

**Sequential Approach** (isolate failures):

1. **Edge Cases First** (20 min)
   - Leverage existing real CMS files where possible
   - Create minimal synthetic fixtures only when needed
   - Focus on parser robustness (not just passing tests)

2. **Negative Tests Next** (15 min)
   - Test error messages (must include examples per QTS Appendix G.1)
   - Verify ParseError raised (not generic exceptions)
   - Ensure actionable error messages

3. **Performance Last** (5 min)
   - Benchmark against real files
   - Document baseline metrics
   - Set SLO alerts

4. **Coverage Check** (5 min)
   - Run coverage report
   - Identify gaps
   - Add tests for uncovered lines (if critical)

---

## Success Criteria

**QTS Compliance:**
- ✅ Test categorization: golden, edge_case, negative, real_source markers used
- ✅ Golden fixture hygiene: 0 rejects, exact counts (Phase 1-2)
- ✅ Multi-format parity: Real-source tests with thresholds (Phase 2)
- ✅ Raw layer preserves source duplicates; NK dedup only in comparison helpers
- ✅ Deterministic column order enforced across formats
- ✅ Edge case coverage: 6+ edge case tests
- ✅ Negative coverage: 7+ negative tests
- ✅ Performance: Parse time < 100ms, memory < 50MB
- ✅ Line coverage: ≥90% on locality_parser.py

**Test Suite Totals:**
- Phase 1: 4 tests (TXT golden)
- Phase 2: 5 tests (CSV/XLSX + real_source)
- **Phase 3: +13 tests** (6 edge + 7 negative)
- **Total: 22 tests** (comprehensive coverage)

**Artifact Quality:**
- All error messages include examples (QTS Appendix G.1)
- Performance baselines documented
- Coverage report generated and reviewed

---

## Time Breakdown

| Task | Estimate | Notes |
|------|----------|-------|
| Edge case tests (6) | 20 min | Leverage real files, minimal fixtures |
| Negative tests (7) | 15 min | Straightforward error cases |
| Performance benchmarks (2) | 5 min | Quick baseline establishment |
| Coverage validation | 5 min | Review and gap analysis |
| **Total** | **45 min** | Conservative (allows for surprises) |

---

## Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Creating edge fixtures takes longer or grows in scope | Medium | Prefer minimal synthetic fixtures; reuse real files where possible; keep each case to <= 10 lines |
| Uncovered code paths discovered | Low | Add targeted tests quickly |
| Performance benchmarks flaky | Low | Use Docker for consistency |
| Binary garbage test fails oddly | Low | Catch broad exception, verify message |
| Benchmark flakiness in CI | Medium | Gate SLO assertion behind ENFORCE_BENCH_SLO; run performance jobs on pinned Docker image |

---

## Cross-References

- **STD-qa-testing-prd §2.2.1:** Test categorization with markers
- **STD-qa-testing-prd §5.1.1:** Golden fixture hygiene (already met)
- **STD-qa-testing-prd §5.1.3:** Authentic source variance testing (Phase 2)
- **STD-qa-testing-prd Appendix G.1:** Error message testing (rich messages)
- **STD-qa-testing-prd §3.3:** Performance testing (SLO < 100ms)
- **STD-parser-contracts §21.1:** Standard parser structure
- **planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md:** Overall design
- **planning/parsers/locality/PHASE_1_RAW_PARSER_PLAN.md:** TXT implementation
- **planning/parsers/locality/PHASE_2_CSV_XLSX_PLAN.md:** CSV/XLSX implementation
- **planning/parsers/locality/AUTHORITY_MATRIX.md:** Authority & thresholds per vintage
- **tests/helpers/variance_testing.py:** Canonicalization + real-source parity helpers

---

## Definition of Done

- ✅ 6+ edge case tests implemented and passing
- ✅ 7+ negative tests implemented and passing  
- ✅ Performance benchmarks documented (< 100ms SLO)
- ✅ Line coverage ≥ 90% verified
- ✅ All tests use proper markers (@pytest.mark.*)
- ✅ Error messages include examples (QTS compliance)
- ✅ No new linter errors
- ✅ All tests passing in Docker
- ✅ Phase 3 complete, ready for Phase 4 (Stage 2: FIPS Normalization)

---

## Phase 4 Preview (Out of Scope for Phase 3)

**Phase 4 = Stage 2: FIPS Normalization/Enrichment**
- Implement `normalize_locality_fips.py`
- County name → FIPS derivation
- Alias map (St/Saint, Parish, Borough)
- Fuzzy matching with deterministic tie-breakers
- Explode to one-row-per-county
- Estimated time: 90-120 minutes

**Deferred to future session.**

---

## Next Actions

1. Review this plan
2. Create edge case fixtures (if needed)
3. Implement edge case tests (20 min)
4. Implement negative tests (15 min)
5. Add performance benchmarks (5 min)
6. Validate coverage (5 min)
7. Commit Phase 3
8. Measure total time vs GPCI baseline (8h → 3-4h expected)

---

**Ready to proceed with Phase 3 implementation?**

