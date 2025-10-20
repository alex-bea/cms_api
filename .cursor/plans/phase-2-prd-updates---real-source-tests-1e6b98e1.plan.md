<!-- 1e6b98e1-2f30-4e1b-b2c7-2c23f66b2b94 0ad64303-1391-49b8-a60c-0f93dc3a6fcb -->
# OPPSCAP Parser Implementation with Locality Test Closure

## Overview

Implement next parser (OPPSCAP) to validate 70% time savings from PRD improvements, with quick preflight ensuring Locality suite is self-policing.

## Phase 0: Preflight - Locality Test Closure (30 min)

### Current Status Check

- `test_locality_e2e_gpci_join_smoke`: Already PASSING (fixed in commit 6dfacaa)
- `test_locality_quarantine_slo_real_source`: Already XFAIL (strict=True, expires 2025-12-31, commit f011681)

**Action:** Verify both tests properly marked, no open-ended skips remaining

**Files:**

- `tests/integration/test_locality_e2e.py`

**Success:** All Locality tests accounted for (26 passed + 1 skipped + 1 xfail = 28/28)

---

## Phase 1: OPPSCAP Pre-Implementation Verification (1-1.5h)

Following STD-parser-contracts-impl §21.4 (Pre-Implementation Checklist)

### 1.1 Review Dataset PRD

- Check if `PRD-opps-prd-v1.0.md` or `SRC-opps.md` covers OPPSCAP
- Identify formats: TXT, CSV, XLSX, ZIP?
- Natural keys, schema requirements

### 1.2 Inspect Real Files

- Locate sample files in `sample_data/` or `data/raw/`
- Check each format:
  - Header detection patterns
  - Row count consistency
  - Encoding (UTF-8, CP1252, BOM?)
  - Fixed-width vs delimited

### 1.3 Layout Verification

- If fixed-width: Use `tools/verify_layout_positions.py`
- Create layout spec in `layout_registry.py`
- Verify column boundaries on 3-5 sample rows

### 1.4 Format Authority Matrix

- Declare authoritative format (TXT > CSV > XLSX?)
- Document known variance
- Set parity thresholds (NK ≥98%, row ≤1%)

**Deliverables:**

- `planning/parsers/oppscap/PRE_IMPLEMENTATION_VERIFICATION.md`
- `planning/parsers/oppscap/AUTHORITY_MATRIX.md`
- Layout entry in `layout_registry.py` (if fixed-width)

---

## Phase 2: OPPSCAP Parser - Single Format (2-2.5h)

Following STD-parser-contracts-impl §21.6 (Incremental Strategy)

### 2.1 Parser Skeleton

**File:** `cms_pricing/ingestion/parsers/oppscap_parser.py`

**Structure (11-step template per §21.1):**

```python
PARSER_VERSION = "v1.0.0"
SCHEMA_ID = "cms_oppscap_v1_0"  # Check schema contracts
NATURAL_KEYS = ['...']  # From PRD

def parse_oppscap(file_obj, filename, metadata) -> ParseResult:
    # 1. Validate metadata
    # 2. Detect encoding
    # 3. Router: TXT/CSV/XLSX/ZIP
    # 4-6. Format-specific parsing
    # 7. Column normalization + aliasing
    # 8. Type casting (Decimal for amounts)
    # 9. Validation (range, categorical, uniqueness)
    # 10. Metrics + row hash
    # 11. Return ParseResult
```

### 2.2 Golden Test (Clean Fixture)

**File:** `tests/parsers/test_oppscap_parser.py`

```python
@pytest.mark.golden
def test_oppscap_golden_txt():
    # Clean 10-row fixture
    # Assert: len(result.rejects) == 0
    # Assert: exact row count, key values
    # Assert: deterministic hash
```

### 2.3 Schema Contract

- Check `cms_pricing/ingestion/contracts/cms_oppscap_v1_0.json`
- Create if missing (copy from GPCI template)

**Deliverables:**

- Parser (TXT format only, ~200-300 lines)
- 1 golden test passing
- Schema contract

---

## Phase 3: Multi-Format Support (1-1.5h)

### 3.1 Add CSV/XLSX Parsers

- Header detection (`_find_header_row_csv`, `_find_data_sheet_xlsx`)
- Zero-padding for codes
- Canonical alias map

### 3.2 Format Parity Test

```python
@pytest.mark.golden
def test_oppscap_format_consistency():
    # Load all 3 formats
    # Assert: identical natural key sets
    # Use canon_oppscap() helper
```

### 3.3 Real-Source Variance Test

```python
@pytest.mark.real_source
def test_oppscap_parity_real_source():
    # QTS §5.1.3 pattern
    # NK overlap ≥98%, row variance ≤1%
    # Emit diff artifacts
```

**Deliverables:**

- CSV/XLSX support
- 2 more golden tests
- 1 real-source parity test

---

## Phase 4: Edge Cases + Negatives (30-45 min)

### 4.1 Edge Cases

- BOM handling
- Duplicate natural keys (preserve in raw)
- Header variations
- Continuation rows (if applicable)

### 4.2 Negative Tests

- Missing required metadata
- Unsupported format
- Missing columns
- Empty file
- Out-of-range values

**Deliverables:**

- 3-5 edge case tests (@pytest.mark.edge_case)
- 3-5 negative tests (@pytest.mark.negative)

---

## Phase 5: Documentation (30 min)

### 5.1 Source Descriptor

**File:** `prds/SRC-oppscap.md` (or update existing)

Sections:

- Overview (dataset purpose)
- Data source details (URL, cadence, formats)
- Parser implementation (version, features)
- Data quality notes (known quirks)
- Cross-references

### 5.2 Update CHANGELOG

- Add OPPSCAP parser entry
- Note time spent per phase
- Compare to GPCI baseline

### 5.3 Update GitHub Tasks

- Mark OPPSCAP task as complete
- Document actual vs estimated time

---

## Success Criteria

**Time:**

- **Target:** 2-3 hours total (70% faster than GPCI)
- **Conservative:** 4-5 hours (50% faster)
- **Baseline:** 8 hours (GPCI actual)

**Tests:**

- Golden tests: 3/3 passing (TXT, CSV, XLSX)
- Real-source parity: passing with diff artifacts
- Edge cases: 3-5 passing
- Negatives: 3-5 passing
- **Total:** 10-15 tests, 100% pass rate

**QTS Compliance:**

- §5.1.1: Golden fixture hygiene
- §5.1.2: Multi-format parity
- §5.1.3: Authentic source variance testing
- Appendix H: Applicable patterns used

**Deliverables:**

1. `cms_pricing/ingestion/parsers/oppscap_parser.py` (~300-400 lines)
2. `tests/parsers/test_oppscap_parser.py` (10-15 tests)
3. `prds/SRC-oppscap.md` (source descriptor)
4. Updated CHANGELOG
5. Time measurement report

---

## Risks & Mitigations

**Risk 1:** OPPSCAP files not available in `sample_data/`

- **Mitigation:** Check first, switch to ANES if unavailable

**Risk 2:** Format variance exceeds thresholds

- **Mitigation:** Authority Matrix + diff artifacts per QTS §5.1.3

**Risk 3:** Time exceeds target

- **Mitigation:** Still validates improvements (any savings = win)

---

## Alternative Parser Candidates

If OPPSCAP unavailable or too complex:

1. **ANES** (Anesthesia CF) - Similar to CF parser
2. **NCCI/MUE** (Edits) - Has PRD, moderate complexity
3. **Locality completion** - Finish quarantine SLO (Option 1)

### To-dos

- [ ] Verify Locality tests properly marked (PASSING/XFAIL), no open skips
- [ ] Locate OPPSCAP sample files, verify formats available
- [ ] Complete pre-implementation verification (inspect files, create layouts, Authority Matrix)
- [ ] Implement OPPSCAP parser - single format (TXT) with golden test
- [ ] Add CSV/XLSX support + format parity tests
- [ ] Add edge case + negative tests
- [ ] Create SRC doc, update CHANGELOG, measure time
- [ ] Compare actual time vs GPCI baseline, document ROI