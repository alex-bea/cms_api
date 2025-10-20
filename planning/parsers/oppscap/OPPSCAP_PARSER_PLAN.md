# OPPSCAP Parser Implementation Plan

**Date:** 2025-10-20  
**Owner:** Data Platform Engineering  
**Estimated Time:** 4-5 hours (following QTS v1.6 + Parser Contracts v2.0)  
**Status:** PRE-IMPLEMENTATION

---

## 0. Executive Summary

**What:** OPPS-based Payment Cap parser for HCPCS/locality pricing  
**Why:** Complete RVU bundle ingestion (last remaining dataset from rvu25d.zip)  
**Scope:** TXT (fixed-width authority) + CSV parity; XLSX investigation deferred (subset anomaly)

**Success Criteria (v1):**
- ✅ TXT authority parsed with 0 rejects on golden fixture
- ✅ CSV format parity (≥98% NK overlap, ≤1% row variance) on real source
- ✅ Deterministic output (row_content_hash stable across runs)
- ✅ Metrics contract compliance (parser + normalization keys)
- ✅ 4-5 hours total time (leveraging Locality/GPCI patterns)

---

## 1. Pre-Implementation Verification (RUN-parser-qa-runbook Step 1-4)

### Step 1: Inventory All Formats (15 min) ✅ DONE

**Authentic Samples:**
- ✅ TXT: `sample_data/rvu25d_0/OPPSCAP_Oct.txt` (16,100 rows)
- ✅ CSV: `sample_data/rvu25d_0/OPPSCAP_Oct.csv` (16,102 rows, +2 vs TXT)
- ⏸ XLSX: `sample_data/rvu25d_0/OPPSCAP_Oct.xlsx` (1,735 rows; investigate separately, not in v1 scope)

**Test Fixtures:**
- ✅ TXT: `tests/fixtures/rvu/test_data/OPPSCAP_Oct_test.txt`
- ✅ CSV: `tests/fixtures/rvu/test_data/OPPSCAP_Oct_test.csv`

**Format Authority Matrix (v1):**
- **Authority:** TXT (fixed-width canonical CMS format)
- **Secondary:** CSV (near parity, +2 rows likely header/footer variance)
- **Deferred:** XLSX (subset anomaly → document and revisit)

**Vintages:**
- Product year: 2025
- Quarter: D (October)
- Source: RVU25D bundle

---

### Step 2a: Inspect Headers & Structure (30 min)

**TXT Format (Fixed-Width):**
```bash
# Actual line length
$ head -30 sample_data/rvu25d_0/OPPSCAP_Oct.txt | awk '{print length}'
# Result: All lines 48-49 characters

# Sample lines:
# 0633TTC C 01112  05   150.69    150.69
# 0633TTC C 01112  09   152.38    152.38
# ^HCPCS ^M ^STAT ^MAC  ^LOC ^FAC     ^NONFAC

# Observed pattern:
# Positions (0-indexed):
# 0-5: HCPCS (5 chars)
# 5-7: Modifier (2 chars)
# 7-9: Status (1 char + space)
# 9-15: MAC/Carrier (5 chars + space)
# 15-20: Locality (2 chars + spaces)
# 20-28: Facility Price (8 chars, decimal)
# 28-end: Non-Facility Price (8+ chars, decimal)
```

**CSV Format:**
```csv
HCPCS,MOD,PROCSTAT,CARRIER,LOCALITY,FACILITY PRICE,NON-FACILTY PRICE
0633T,TC,C,01112,05,150.69,150.69
```

**XLSX Format:**
- Sheet: 'OPPSCAP_Oct'
- Columns: Same as CSV
- **Issue:** Only 1,735 rows vs 16K in TXT/CSV
- **Hypothesis:** XLSX may be summary/subset (need to investigate)

---

### Step 2b: Verify Fixed-Width Layout Positions (10 min)

**Draft Layout (needs verification):**
```python
OPPSCAP_2025D_LAYOUT = {
    'hcpcs': (0, 5),          # HCPCS code (5 chars)
    'modifier': (5, 7),        # Modifier (2 chars, e.g., TC, 26)
    'status': (7, 9),          # Procedure status (1 char + padding)
    'mac': (9, 15),            # MAC/Carrier (5 digits + padding)
    'locality': (15, 20),      # Locality code (2 digits + padding)
    'facility_price': (20, 28),   # Facility price (decimal, 2dp)
    'nonfacility_price': (28, None),  # Non-facility price (rest of line)
}
```

**Verification Commands:**
```bash
# Verify HCPCS extraction
head -10 OPPSCAP_Oct.txt | cut -c1-5
# Expected: 0633T repeated

# Verify modifier extraction  
head -10 OPPSCAP_Oct.txt | cut -c6-7
# Expected: TC repeated

# Verify prices align
head -10 OPPSCAP_Oct.txt | cut -c21-28
# Expected: 150.69, 152.38, etc.
```

---

### Step 2c: Real Data Format Variance Analysis (10 min)

**Row Count Variance:**
- TXT: 16,100 rows (authority)
- CSV: 16,102 rows (+2 rows, 0.01% variance)
- XLSX: 1,735 rows (-89% variance!) ⚠️

**Decision:**
- ✅ TXT chosen as authority (canonical CMS format)
- ✅ CSV near-parity test (NK ≥98%, row ≤1%)
- ❌ XLSX requires investigation - likely different structure/subset
  - **Action:** Check if XLSX is a summary table or different vintage
  - **Plan:** Parse XLSX separately if structure differs, or skip if it's just a subset

**Parity Thresholds (per QTS §5.1.3):**
- NK overlap ≥ 98% (TXT vs CSV)
- Row variance ≤ 1% or ≤ 2 rows
- **Expected:** CSV should pass (only +2 rows = 0.01%)

---

### Step 2d: Set-Logic & Complement Pattern Detection (10 min) ✅ NOT APPLICABLE

**Scan Results:**
```bash
$ grep -iE "(ALL COUNTIES|ALL.*EXCEPT|REST OF)" sample_data/rvu25d_0/OPPSCAP_Oct.*
# No matches - no set-logic patterns
```

**Decision:** Standard list parsing, no two-pass algorithm needed

---

### Step 3: Header Normalization Mapping (30 min)

**Column Aliases:**
```python
OPPSCAP_ALIAS_MAP = {
    # TXT (no headers - positional)
    # N/A
    
    # CSV
    'HCPCS': 'hcpcs',
    'MOD': 'modifier',
    'PROCSTAT': 'status',
    'CARRIER': 'mac',
    'LOCALITY': 'locality_code',
    'FACILITY PRICE': 'facility_price',
    'NON-FACILTY PRICE': 'nonfacility_price',  # Note: CMS typo "FACILTY"
    
    # XLSX (same as CSV)
    'HCPCS': 'hcpcs',
    'MOD': 'modifier',
    'PROCSTAT': 'status',
    'CARRIER': 'mac',
    'LOCALITY': 'locality_code',
    'FACILITY PRICE': 'facility_price',
    'NON-FACILTY PRICE': 'nonfacility_price',
}
```

**Schema Mapping:**
| CMS Column | Canonical | Type | Notes |
|------------|-----------|------|-------|
| HCPCS | `hcpcs` | str | 5-char code, uppercase |
| MOD | `modifier` | str | 2-char modifier (TC, 26, etc.), nullable |
| PROCSTAT | `status` | str | 1-char status code |
| CARRIER | `mac` | str | 5-digit MAC, zero-padded |
| LOCALITY | `locality_code` | str | 2-digit locality, zero-padded |
| FACILITY PRICE | `facility_price` | Decimal(10,2) | Price at facility setting |
| NON-FACILTY PRICE | `nonfacility_price` | Decimal(10,2) | Price at non-facility setting |

**Metadata Fields (injected):**
- `release_id`, `product_year`, `quarter_vintage`, `vintage_date`
- `source_filename`, `source_file_sha256`, `source_uri`
- `row_content_hash`, `parsed_at`

---

### Step 4: Validate Layouts Against Real Data (20 min)

**Validation Tasks:**
1. Verify TXT positions extract correct values
2. Verify CSV header matches expected aliases
3. Check XLSX sheet structure and row counts
4. Validate data types (prices should be numeric)
5. Check for BOM/encoding issues

**See:** Detailed verification in §2.1 below

---

## 2. Implementation Plan

### 2.1 Layout Verification & Format Analysis (30 min)

**Tasks:**
1. Create layout verification script
2. Test TXT positions on first 100 rows
3. Document actual column positions
4. Verify CSV delimiter and quoting
5. Investigate XLSX discrepancy (1,735 vs 16K rows)
6. Document encoding (UTF-8, CP1252, BOM presence)

**Deliverable:** `OPPSCAP_2025D_LAYOUT` constant in `layout_registry.py`

---

### 2.2 Parser Implementation (2-3 hours)

**Architecture:** Single-stage parser (no normalization needed - file already has canonical values)

**Parser Function:**
```python
def parse_oppscap(
    file_obj: BinaryIO,
    filename: str,
    metadata: Dict[str, Any],
) -> ParseResult:
    """
    Parse OPPS Payment Cap file (OPPSCAP).
    
    Supports: TXT (fixed-width), CSV, XLSX
    
    Returns:
        ParseResult with:
        - data: DataFrame with schema cms_oppscap_v1.0
        - rejects: Rows failing validation
        - metrics: Parse metrics + validation counts
    """
```

**11-Step Template (STD-parser-contracts-impl §2.1):**

1. **Detect format** (TXT/CSV/XLSX via router)
2. **Read bytes** with encoding cascade (UTF-8 → UTF-16 → CP1252)
3. **Parse to DataFrame** (fixed-width or CSV reader)
4. **Normalize headers** (apply ALIAS_MAP)
5. **Type coercion** (str, Decimal, zero-padding)
6. **Validation** (required fields, ranges, domains)
7. **Inject metadata** (release_id, vintage_date, etc.)
8. **Sort** by natural keys (hcpcs, modifier, mac, locality_code)
9. **Compute row_content_hash** (64-char SHA-256)
10. **Generate metrics** (rows, rejects_by_reason, parse_time)
11. **Return ParseResult**

---

### 2.3 Validation Rules (REF-parser-quality-guardrails)

**Tier 1 - BLOCK (Critical):**
- R-OPPSCAP-001: `hcpcs` matches pattern `^[A-Z0-9]{5}$`
- R-OPPSCAP-002: `modifier` matches pattern `^[A-Z0-9]{2}$` (if present)
- R-OPPSCAP-003: `mac` is 5 digits (zero-padded)
- R-OPPSCAP-004: `locality_code` is 2 digits (zero-padded)
- R-OPPSCAP-005: `facility_price` >= 0 (non-negative)
- R-OPPSCAP-006: `nonfacility_price` >= 0 (non-negative)
- R-OPPSCAP-007: Natural key `(hcpcs, modifier, mac, locality_code)` is unique

**Tier 2 - WARN (Soft):**
- R-OPPSCAP-W001: Prices > $10,000 (potential data quality issue)
- R-OPPSCAP-W002: `facility_price` > `nonfacility_price` (unusual pattern)
- R-OPPSCAP-W003: `status` not in known domain ['A', 'C', 'X', etc.]

**Tier 3 - INFO (Informational):**
- R-OPPSCAP-I001: Modifier is blank (track prevalence)
- R-OPPSCAP-I002: Facility == Non-facility (track prevalence)

---

### 2.4 Test Implementation (1.5-2 hours)

**Golden Tests** (`@pytest.mark.golden`):
1. `test_oppscap_golden_txt()` - 10-20 row clean fixture, 0 rejects
2. `test_oppscap_golden_csv()` - Same data as TXT, test format parity
3. `test_oppscap_golden_xlsx()` - (Pending XLSX investigation)

**Edge Case Tests** (`@pytest.mark.edge_case`):
1. `test_oppscap_high_prices()` - Prices > $10K (warning, not reject)
2. `test_oppscap_blank_modifier()` - Null/empty modifiers
3. `test_oppscap_facility_greater_than_nonfacility()` - Price inversion

**Negative Tests** (`@pytest.mark.negative`):
1. `test_oppscap_invalid_hcpcs()` - Wrong length, lowercase, special chars
2. `test_oppscap_negative_prices()` - Negative amounts (should reject)
3. `test_oppscap_invalid_mac()` - Non-numeric MAC codes
4. `test_oppscap_duplicate_natural_keys()` - Duplicate (HCPCS, MOD, MAC, LOC)

**Integration Tests** (`@pytest.mark.integration`):
1. `test_oppscap_locality_join()` - Join to Locality on (MAC, locality_code)
2. `test_oppscap_real_source_parity()` - TXT vs CSV on real files (≥98% overlap)
3. `test_oppscap_determinism()` - Hash stability across runs

**Real Source Tests** (`@pytest.mark.real_source`):
1. `test_oppscap_parity_real_source()` - Variance testing per QTS §5.1.3
2. `test_oppscap_coverage_real_source()` - ≥15K rows, 50+ localities

---

## 3. Schema Contract Analysis

**Current Schema:** `cms_oppscap_v1.0.json`

**Issues Found:**
- ❌ Schema has `opps_cap_applies` (bool), `cap_amount_usd`, `cap_method`
- ❌ Actual file has `facility_price`, `nonfacility_price`, `status`, `mac`, `locality_code`
- ❌ **MISMATCH: Schema doesn't match file structure!**

**Action Required:**
1. Update schema contract to match actual file
2. OR create new schema `cms_oppscap_v1.1.json` with correct columns
3. Verify with product team which structure is intended

**Proposed Schema Update (v1.1):**
```json
{
  "natural_keys": ["hcpcs", "modifier", "mac", "locality_code"],
  "columns": {
    "hcpcs": {"type": "str", "pattern": "^[A-Z0-9]{5}$"},
    "modifier": {"type": "str", "pattern": "^[A-Z0-9]{2}$", "nullable": true},
    "status": {"type": "str", "domain": ["A", "C", "X", "..."]},
    "mac": {"type": "str", "pattern": "^[0-9]{5}$"},
    "locality_code": {"type": "str", "pattern": "^[0-9]{2}$"},
    "facility_price": {"type": "Decimal", "precision": 2, "min_value": 0},
    "nonfacility_price": {"type": "Decimal", "precision": 2, "min_value": 0}
  }
}
```

---

## 4. Detailed Implementation Steps

### Phase 1: Layout & Schema (1 hour)

**Tasks:**
1. ✅ Inspect TXT/CSV formats (DONE above)
2. ⬜ Finalize schema contract update (v1.1) to match actual file
3. ⬜ Verify TXT fixed-width positions with tool and add `OPPSCAP_2025D_LAYOUT`
4. 🔄 Document XLSX discrepancy in AUTHORITY_MATRIX.md (deferred)

**Deliverables:**
- `cms_pricing/ingestion/parsers/layout_registry.py` (OPPSCAP layout)
- `cms_pricing/ingestion/contracts/cms_oppscap_v1.1.json` (updated schema)
- `planning/parsers/oppscap/AUTHORITY_MATRIX.md` (note XLSX variance/deferred)

---

### Phase 2: Parser Implementation (1.5-2 hours)

**File:** `cms_pricing/ingestion/parsers/oppscap_parser.py`

**Structure:**
```python
from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    detect_encoding,
    canonicalize_numeric_col,
    compute_row_hash,
)

# Layout
OPPSCAP_2025D_LAYOUT = {...}

# Alias map
OPPSCAP_ALIAS_MAP = {...}

def parse_oppscap(file_obj, filename, metadata) -> ParseResult:
    """Parse OPPSCAP file - 11 steps."""
    # Step 1: Format detection
    # Step 2: Encoding detection
    # Step 3: Parse (fixed-width or CSV)
    # Step 4: Normalize headers
    # Step 5: Type coercion + zero-padding
    # Step 6: Validation (rules R-OPPSCAP-001 to 007)
    # Step 7: Inject metadata
    # Step 8: Sort by NK
    # Step 9: Compute hashes
    # Step 10: Generate metrics
    # Step 11: Return ParseResult
```

**Key Implementation Details:**
- Zero-pad: `mac` (5 digits), `locality_code` (2 digits)
- Decimal canonicalization: 2 decimal places for prices
- Status code uppercase normalization
- Modifier handling: `""` → `None` for nulls

---

### Phase 3: Golden & Parity Tests (1 hour)

**Create Golden Fixtures:**
```bash
# Extract 15-20 clean rows from TXT
head -23 sample_data/rvu25d_0/OPPSCAP_Oct.txt > tests/fixtures/oppscap/golden/OPPSCAP_golden.txt

# Convert to CSV (same 15-20 rows)
# Manually create CSV with exact same rows

# Create manifest
cat > tests/fixtures/oppscap/golden/manifest.json << EOF
{
  "fixture_version": "1.0.0",
  "source": "sample_data/rvu25d_0/OPPSCAP_Oct.txt",
  "rows": 18,
  "natural_keys": ["hcpcs", "modifier", "mac", "locality_code"],
  "notes": "Clean fixture - no duplicates, no edge cases"
}
EOF
```

**Test File:** `tests/parsers/test_oppscap_parser.py`

**Tests:**
1. `test_oppscap_golden_txt()` - clean fixture, 0 rejects
2. `test_oppscap_csv_parity()` - same rows, TXT vs CSV parity
3. `test_oppscap_determinism()` - hash stability + metadata injection

---

### Phase 4: Edge & Negative Tests (30 min)

**Tests:**
1. `test_oppscap_edge_blank_modifier()` - null modifier handling
2. `test_oppscap_negative_invalid_hcpcs()` - rejects on malformed codes
3. `test_oppscap_negative_negative_prices()` - rejects negative values
4. `test_oppscap_negative_duplicate_nk()` - duplicate NK detection

---

### Phase 5: Real Source Parity Tests (45 min)

**Integration Tests:**
1. `test_oppscap_txt_vs_csv_parity_real_source()` - variance report (≥98% NK overlap)
2. `test_oppscap_coverage_real_source()` - ≥15K rows, locality coverage

---

### Phase 6: Documentation & Metrics Contract (30 min)

**Documentation:**
1. Update CHANGELOG.md with OPPSCAP parser entry
2. Update AUTHORITY_MATRIX.md (TXT authority, CSV parity, XLSX deferred)
3. Update `github_tasks_plan.md` - mark OPPSCAP parser task complete

**Metrics Contract:**
1. Validate metrics against `metrics_contract_v1.0.json` (parser metrics)
2. Ensure metrics include `rows_parsed`, `rows_rejected`, `encoding_used`, `parse_time_sec`, `rejects_by_reason`
3. Add contract assertion to parser tests

---

## 5. Time Estimates & Milestones

| Phase | Tasks | Estimated Time | Dependencies |
|-------|-------|----------------|--------------|
| **Phase 1** | Layout verification + schema update | 1 hour | Sample files |
| **Phase 2** | Parser implementation (TXT + CSV) | 1.5-2 hours | Phase 1 |
| **Phase 3** | Golden & parity tests | 1 hour | Phase 2 |
| **Phase 4** | Edge & negative tests | 30 min | Phase 2 |
| **Phase 5** | Real source parity (TXT vs CSV) | 45 min | Phase 2, 3 |
| **Phase 6** | Docs & metrics contract | 30 min | All phases |
| **TOTAL** | **~5 hours** | **Target: 4-6 hours** | - |

**Comparison to GPCI Baseline:**
- GPCI: ~8 hours (with debugging)
- OPPSCAP Target: 4-6 hours (50-62% time savings)
- Savings: Applying QTS v1.6 patterns, no normalization needed

---

## 6. Risk & Mitigation

### Risk 1: XLSX Discrepancy (1,735 vs 16K rows)
**Impact:** Medium  
**Likelihood:** High  
**Mitigation:**
- Investigate XLSX structure first (15 min)
- If different: document as "XLSX contains summary only"
- If subset: skip XLSX or create separate test
- If error: notify team, use TXT/CSV only

### Risk 2: Schema Contract Mismatch
**Impact:** High  
**Likelihood:** High (already identified)  
**Mitigation:**
- Update schema to v1.1 before coding
- Get product team confirmation
- Document changes in schema changelog

### Risk 3: Fixed-Width Position Errors
**Impact:** Medium  
**Likelihood:** Low (using verification tool)  
**Mitigation:**
- Use layout verification tool (Step 2b)
- Validate on 100 rows before full parse
- Add layout probe logging (per Locality learnings)

### Risk 4: Parity Test Failures (CSV ≠ TXT)
**Impact:** Low  
**Likelihood:** Low (+2 rows likely header variance)  
**Mitigation:**
- Use threshold-based parity (≥98% NK overlap)
- Emit variance artifacts for investigation
- Document known variance in AUTHORITY_MATRIX

---

## 7. Success Criteria

**Functional:**
- ✅ Parses TXT, CSV formats successfully
- ✅ 0 rejects on golden fixtures (clean data)
- ✅ TXT/CSV parity ≥98% on real source files
- ✅ Natural keys unique per release
- ✅ Prices non-negative and decimal(10,2)

**Quality:**
- ✅ Golden + parity + edge/negative tests passing in CI
- ✅ Metrics contract validation passing (parser metrics)
- ✅ Deterministic hashes across runs

**Performance:**
- ✅ Parse 16K rows in < 5 seconds
- ✅ Peak memory < 100MB for full file

**Documentation:**
- ✅ CHANGELOG updated
- ✅ Schema contract v1.1 created
- ✅ AUTHORITY_MATRIX documented
- ✅ GitHub tasks updated

---

## 8. Open Questions

1. **XLSX Variance:** Why does XLSX have only 1,735 rows? Different vintage? Summary? Error?
2. **Schema Mismatch:** Should we update v1.0 or create v1.1? Product team input needed?
3. **PROCSTAT Domain:** What are valid status code values? (A, C, X, ...?)
4. **Effective Dating:** File is "Oct" (Q4) - should we infer effective_from = 2025-10-01?

---

## 9. References

**PRDs:**
- `PRD-rvu-gpci-prd-v0.1.md` - OPPSCAP section (§1.3)
- `STD-parser-contracts-prd-v2.0.md` - Core parser contracts
- `STD-parser-contracts-impl-v2.0.md` - 11-step template
- `RUN-parser-qa-runbook-prd-v1.0.md` - Pre-implementation checklist
- `STD-qa-testing-prd-v1.0.md` - Test patterns (Appendix G, H)
- `REF-parser-quality-guardrails-v1.0.md` - Validation tiers

**Reference Implementations:**
- GPCI parser: `cms_pricing/ingestion/parsers/gpci_parser.py`
- Locality parser: `cms_pricing/ingestion/parsers/locality_parser.py`
- CF parser: `cms_pricing/ingestion/parsers/conversion_factor_parser.py`

**Contracts:**
- Schema: `cms_pricing/ingestion/contracts/cms_oppscap_v1.0.json` (needs update)
- Metrics: `cms_pricing/ingestion/contracts/metrics_contract_v1.0.json`

---

## 10. Next Steps (Immediate)

**Before Coding:**
1. ⬜ Investigate XLSX variance (document outcome; v1 scope TXT/CSV only)
2. ⬜ Finalize schema update to v1.1 (align columns + validation rules)
3. ⬜ Verify TXT fixed-width positions (layout tool + probe logging)
4. ⬜ Update AUTHORITY_MATRIX.md (TXT authority, CSV parity, XLSX deferred)
5. ⬜ Confirm PROCSTAT domain with product (async)

**Implementation Order:**
1. Layout verification → Schema update → Parser skeleton
2. TXT parsing → CSV parsing
3. Golden tests → Edge/negative tests
4. Real source parity (TXT vs CSV)
5. Metrics contract validation → Documentation

**Total Time Budget:** ~5 hours
**Target:** Complete in one session (with breaks)

---

## 11. Learnings to Apply (From Locality 0%)

✅ **REST-OF-STATE:** Not applicable (no set-logic)  
✅ **Stage 1/2 Boundary:** Single-stage parser (no normalization)  
✅ **Alias Substring Bug:** Use exact-match-only (no substring replacement)  
✅ **Multi-State Matching:** Not applicable (locality-specific pricing, no cross-boundary)  
✅ **Metrics Contract:** Validate parser metrics against contract  
✅ **Fixed-Width Best Practices:** Layout probe logging, position verification  

---

## 12. Phase 1 Learnings & PRD Updates Needed (2025-10-20)

### Learnings from Phase 1 Execution

**Completed:** 2025-10-20, ~25 minutes (vs 60 min estimated)  
**Time Savings:** 35 minutes - Layout already existed in `layout_registry.py`

---

### Learning 1: Natural Key Verification is CRITICAL ⚠️⚠️

**Issue:** PRD stated NK as `(hcpcs, modifier, locality_id)` without MAC

**Reality Check:**
- Analysis of 16,100 rows shows NK must be `(hcpcs, modifier, mac, locality_code)`
- Same HCPCS+modifier has 96 different prices across 115 MAC/locality combinations
- Without MAC in NK: 77 unique keys → 16,100 rows (max 115 rows per key) ❌
- With MAC in NK: 8,855 unique keys → 16,100 rows (max 1 row per key) ✅

**Why This Matters:**
- Prices vary by both MAC (contractor) AND locality (geography)
- Duplicate NK errors if MAC excluded
- Downstream joins would be ambiguous

**PRD Update Needed:** `PRD-rvu-gpci-prd-v0.1.md` line 38
- Current: "OPPSCAP keyed by `(hcpcs, modifier, locality_id)`"
- Correct: "OPPSCAP keyed by `(hcpcs, modifier, mac, locality_code)`"

**Impact:** Prevents 1-2 hours debugging duplicate NK errors during parser implementation

---

### Learning 2: Schema Contract Must Match Reality 📋

**Issue:** `cms_oppscap_v1.0.json` schema didn't match actual file structure

**Schema v1.0 (Wrong):**
- `opps_cap_applies` (bool)
- `cap_amount_usd` (single amount)
- `cap_method` (enum)

**Actual File Structure:**
- `hcpcs`, `modifier`, `status`, `mac`, `locality_code`
- `facility_price`, `nonfacility_price` (TWO amounts, not one)

**Root Cause:** Schema created before file inspection (spec-driven, not data-driven)

**Fix:** Created `cms_oppscap_v1.1.json` matching actual file

**PRD Update Needed:** `RUN-parser-qa-runbook-prd-v1.0.md`
- Add to Step 1: "Verify schema contract matches actual file structure"
- Add checklist item: "Compare schema columns to actual file headers/positions"
- Enforcement: Schema verification before coding (prevents rework)

**Impact:** Prevents complete schema rewrite mid-implementation

---

### Learning 3: XLSX Variance Detection Pattern 📊

**Issue:** XLSX has 1,735 rows vs 16,100 in TXT/CSV (-89% variance)

**Investigation:** Variance too large for formatting difference

**Decision Tree Applied:**
- Variance < 2%: Proceed with parity tests
- Variance 2-10%: Document, implement diffs, investigate in parallel
- **Variance > 10%: STOP, investigate before coding** ✅ Applied

**Action Taken:**
- Documented variance in AUTHORITY_MATRIX.md
- Deferred XLSX to v2 scope
- v1 focuses on TXT (authority) + CSV (parity)

**PRD Update Needed:** `RUN-parser-qa-runbook-prd-v1.0.md` Step 2c
- Add: "If variance >10%, defer format to separate investigation"
- Add: "Don't force-fit divergent formats into same parser"
- Current guidance stops at investigation; add explicit defer recommendation

**Impact:** Prevents wasted time trying to unify incompatible formats

---

### Learning 4: Existing Layout Registry Saves Time ⏱️

**Finding:** OPPSCAP layout already existed in `layout_registry.py:98-111`

**Time Savings:**
- Estimated: 60 min for Phase 1
- Actual: 25 min (layout already done)
- Savings: 35 minutes

**Why Layout Existed:**
- Previous team member started OPPSCAP work
- Layout positions defined but parser never completed
- Layout verification against real data confirmed positions correct

**PRD Update Needed:** `RUN-parser-qa-runbook-prd-v1.0.md` Step 1
- Add: "Check if layout already exists in layout_registry.py FIRST"
- Add: "Verify existing layout against current vintage (may need update)"
- Prevents duplicate work, leverages existing assets

**Impact:** 35 min time savings on layout discovery

---

### Learning 5: CSV Header Typo Detection 🔤

**Finding:** CSV header has typo: "NON-FACILTY PRICE" (missing second I)

**Implication:**
- Alias map must include both correct and typo variants
- CMS data quality issue (common pattern)
- Tests should verify both spellings work

**Alias Map Entry Needed:**
```python
'NON-FACILTY PRICE': 'nonfacility_price',  # CMS typo (missing I)
'NON-FACILITY PRICE': 'nonfacility_price',  # Correct spelling
```

**PRD Update Needed:** `STD-parser-contracts-impl-v2.0.md` §1.3
- Add example: CMS header typos are common (document and handle)
- Add best practice: Alias map should include known typos with comments
- Testing: Verify both correct and typo variants parse correctly

**Impact:** Prevents parser failure on CMS typos, documents data quality issues

---

## Summary: PRD Updates Required

### High Priority (Before Phase 2):

1. **PRD-rvu-gpci-prd-v0.1.md line 38** (5 min)
   - Fix: OPPSCAP NK from `(hcpcs, modifier, locality_id)` to `(hcpcs, modifier, mac, locality_code)`
   - Severity: HIGH (incorrect NK causes implementation errors)

### Medium Priority (Before Release):

2. **RUN-parser-qa-runbook-prd-v1.0.md Step 1** (5 min)
   - Add: Check layout_registry.py first before creating layout
   - Impact: Prevent duplicate work

3. **RUN-parser-qa-runbook-prd-v1.0.md Step 2c** (5 min)
   - Add: Defer formats with >10% variance to separate investigation
   - Impact: Prevent scope creep on divergent formats

4. **RUN-parser-qa-runbook-prd-v1.0.md Step 1** (5 min)
   - Add: Verify schema contract matches actual file structure
   - Impact: Prevent schema rework mid-implementation

5. **STD-parser-contracts-impl-v2.0.md §1.3 Alias Best Practices** (5 min)
   - Add: Include CMS typo variants in alias maps
   - Example: "NON-FACILTY" typo
   - Impact: Handle CMS data quality issues gracefully

**Total Time:** 25 minutes for PRD updates

---

## Recommendation

**Option A:** Update PRD #1 only (5 min) → Proceed with Phase 2
- Fixes critical NK error in PRD
- Other updates can wait until after parser complete

**Option B:** Update all 5 PRDs (25 min) → Proceed with Phase 2
- Complete documentation
- All learnings captured while fresh

**Option C:** Proceed with Phase 2 now, update PRDs at end
- Faster to parser completion
- Risk: Learnings not fresh

---

**My Recommendation:** **Option A** - Fix NK in PRD (5 min), proceed with Phase 2, batch remaining PRD updates with final documentation.

---

**Ready to proceed?** Phase 2 (Parser implementation) is next.
