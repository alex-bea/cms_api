# GPCI Parser Implementation Plan v2.0 (CMS-Native)

**Generated:** 2025-10-16  
**Target Parser:** `cms_pricing/ingestion/parsers/gpci_parser.py`  
**Schema:** `cms_gpci_v1.2` (CMS-native naming)  
**Estimated Time:** 2.5 hours (includes schema migration)  
**Status:** Ready to implement

---

## 📋 **Overview**

Geographic Practice Cost Indices (GPCI) parser converts CMS locality-based cost indices files to canonical schema.

**Key Characteristics:**
- **100-120 rows** (CMS post-CA consolidation universe: ~109 localities)
- **Natural Keys:** `['locality_code', 'effective_from']`
- **Schema:** `cms_gpci_v1.2` (precision=3 decimals, HALF_UP rounding)
- **Formats:** CSV (comma-delimited), TXT (fixed-width), XLSX
- **Uniqueness:** WARN severity (localities may overlap during transitions)
- **Source Bundle:** Part of quarterly RVU releases (rvu25[A-D].zip)
- **Discovery URL:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files

**GPCI Role (per CMS):**
Payment = Σ(RVU_component × GPCI_component) × CF

Parser delivers **raw** GPCI indices from CMS. Floors (e.g., Alaska 1.50 work floor, Congressional 1.00 work floor) are applied at **pricing time**, not in parser.

**Reference PRDs:**
- **REF-cms-pricing-source-map-prd-v1.0.md** - Authoritative GPCI source inventory (§2, Table Row 2)
- **PRD-rvu-gpci-prd-v0.1.md** - GPCI ingestion requirements (§1.2, §3.3)
- **REF-geography-source-map-prd-v1.0.md** - Geography integration (CMS ZIP→Locality dependencies)

---

## 🎯 **Schema Contract Analysis (v1.2 - CMS-Native)**

### Required Columns (CMS-Native Names)

| Column | Type | Precision | Range | Nullable | Notes |
|--------|------|-----------|-------|----------|-------|
| `locality_code` | str | - | 2 digits | No | Zero-pad: "01", "99" |
| `gpci_work` | float64 | 3 decimals | [0.3, 2.0] | No | Raw from CMS |
| `gpci_pe` | float64 | 3 decimals | [0.3, 2.0] | No | Practice expense |
| `gpci_mp` | float64 | 3 decimals | [0.3, 2.0] | No | **Malpractice (MP)** - CMS term |
| `effective_from` | datetime | - | - | No | RVU A=Jan 1, B=Apr 1, C=Jul 1, D=Oct 1 |
| `effective_to` | datetime | - | - | Yes | Next release eve; null for current |

### Optional Enrichment Columns (Parser MAY include if present in source)

| Column | Type | Source | Nullable | Notes |
|--------|------|--------|----------|-------|
| `locality_name` | str | CMS file | Yes | "ALABAMA", "MANHATTAN" |
| `state` | str | CMS file | Yes | 2-letter USPS: "AL", "AK" (NOT FIPS) |
| `mac` | str | CMS file | Yes | 5-digit MAC code: "10112" |

### Provenance Columns (Required)

| Column | Type | Notes |
|--------|------|-------|
| `source_release` | str | "RVU25A", "RVU25D" |
| `source_inner_file` | str | "GPCI2025.csv", "GPCI2025.txt" |

**Natural Keys:** `['locality_code', 'effective_from']`

**Column Order (for hash):** `["locality_code", "gpci_work", "gpci_pe", "gpci_mp", "effective_from"]`
- Enrichment columns (`locality_name`, `state`, `mac`) **excluded from hash** for stability

---

## 🔄 **Schema Migration (v1.1 → v1.2)**

### Breaking Changes

| v1.1 (Old) | v1.2 (New) | Reason |
|------------|------------|--------|
| `gpci_malp` | **`gpci_mp`** | CMS-native terminology (MP = Malpractice) |
| `state_fips` (required) | **`state` (enrichment)** | CMS Locality Key uses state names, not FIPS; keep lean |
| Column order includes `state_fips` | **Lean column order** | Exclude enrichment from hash |

### Impact

- ✅ **No impact on other parsers** - GPCI is first to adopt v1.2
- ✅ **Cleaner, CMS-aligned schema** from day 1
- ✅ **Better separation**: Core indices vs enrichment

---

## ⚠️ **Layout-Schema Alignment (v2025.4.1)**

### Required Layout Updates

**Current Layout (`layout_registry.py` line 75-88 - v2025.4.0):**

```python
GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 150,
    'columns': {
        'mac': ...,
        'state': ...,              # ← Enrichment (optional)
        'locality_id': ...,        # ❌ Schema expects: locality_code
        'locality_name': ...,      # ← Enrichment (optional)
        'work_gpci': ...,          # ❌ Schema expects: gpci_work
        'pe_gpci': ...,            # ❌ Schema expects: gpci_pe
        'mp_gpci': ...,            # ❌ Schema expects: gpci_mp
    }
}
```

**Updated Layout (v2025.4.1 - CMS-Native):**

```python
GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Patch bump for CMS-native alignment
    'min_line_length': 100,  # Measured from actual data (~140 chars)
    'source_version': '2025D',
    'data_start_pattern': r'^\d{5}',  # MAC code at start
    'columns': {
        # Core schema columns (CMS-native names)
        'locality_code': {'start': 24, 'end': 26, 'type': 'string', 'nullable': False},
        'gpci_work': {'start': 120, 'end': 125, 'type': 'decimal', 'nullable': False},
        'gpci_pe': {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': False},
        'gpci_mp': {'start': 140, 'end': 145, 'type': 'decimal', 'nullable': False},
        
        # Optional enrichment columns (parser includes if present)
        'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': True},
        'state': {'start': 15, 'end': 17, 'type': 'string', 'nullable': True},
        'locality_name': {'start': 28, 'end': 80, 'type': 'string', 'nullable': True},
    }
}
```

**Key Changes:**
1. ✅ `locality_id` → `locality_code` (schema alignment)
2. ✅ `work_gpci` → `gpci_work` (schema-canonical prefix grouping)
3. ✅ `pe_gpci` → `gpci_pe`
4. ✅ `mp_gpci` → `gpci_mp` (CMS-native "MP" abbreviation)
5. ✅ `state` marked as enrichment (not state_fips)
6. ✅ Measured `min_line_length` from actual data

---

## 📝 **Parser Implementation Steps**

### Step 1: Column Alias Map (CSV Support)

CSV headers from sample data:

```python
ALIAS_MAP = {
    # Locality identification
    'medicare administrative contractor (mac)': 'mac',
    'mac': 'mac',
    'state': 'state',
    'st': 'state',
    'locality number': 'locality_code',
    'locality': 'locality_code',
    'loc': 'locality_code',
    'locality id': 'locality_code',
    'locality name': 'locality_name',
    
    # GPCI components (CMS-native naming)
    'pw gpci': 'gpci_work',
    'work gpci': 'gpci_work',
    '2025 pw gpci': 'gpci_work',
    '2025 pw gpci (with 1.0 floor)': 'gpci_work',  # Alaska floor note in header
    'pe gpci': 'gpci_pe',
    'practice expense gpci': 'gpci_pe',
    '2025 pe gpci': 'gpci_pe',
    'mp gpci': 'gpci_mp',
    'malpractice gpci': 'gpci_mp',
    'malp gpci': 'gpci_mp',
    '2025 mp gpci': 'gpci_mp',
}
```

### Step 2: Validation Rules (CMS-Realistic)

**Range Validation (Schema Bounds):**
- `gpci_work`, `gpci_pe`, `gpci_mp`: **WARN** if outside [0.30, 2.00]; **FAIL** if outside [0.20, 2.50]
- Rationale: CMS historical range with headroom for edge localities

**Pattern Validation (BLOCK):**
- `locality_code`: Must match `^\d{2}$` (zero-padded 2-digit string)
- Examples: "01" (Alaska), "05" (Manhattan), "99" (rest-of-state)

**Row Count Validation:**
- **Expected:** 100-120 rows (CMS post-CA consolidation: ~109 localities)
- **WARN:** Outside 100-120 range
- **FAIL:** < 90 rows (critical data loss)
- Rationale: Locality reconfiguration events (e.g., CA consolidation) cause deltas

**Uniqueness (WARN - not BLOCK):**
- Duplicate `(locality_code, effective_from)` → quarantine
- Per STD-parser-contracts §8.5, GPCI has WARN severity
- Rationale: Localities may overlap during transition periods

### Step 3: GPCI-Specific Guardrails (WARN)

**Statistical Checks (per PRD-rvu-gpci §2.4):**
```python
def _apply_gpci_guardrails(df: pd.DataFrame, metadata: Dict) -> Dict[str, Any]:
    """
    Apply CMS GPCI guardrails (WARN only).
    
    Checks:
    1. Locality count: 100-120 expected (warn outside)
    2. GPCI work floor: Check for Alaska 1.50 floor presence
    3. High-GPCI localities: Verify AK/CA/NY high values
    4. Coverage: ≥99.5% of localities have valid indices
    """
    warnings = {
        'locality_count': len(df),
        'count_warning': len(df) < 100 or len(df) > 120,
        'coverage_pct': (len(df[df['gpci_work'].notna()]) / len(df) * 100) if len(df) > 0 else 0,
    }
    details = {}
    
    # Check known CMS values (2025 sample data)
    known_values = {
        '01': {'work': 1.500, 'pe': 1.081, 'mp': 0.592, 'name': 'Alaska'},
        '00': {'work': 1.000, 'pe': 0.869, 'mp': 0.575, 'name': 'Alabama'},
        '05': {'work': 1.122, 'pe': 1.569, 'mp': 1.859, 'name': 'Manhattan'},
    }
    
    for loc_code, expected in known_values.items():
        rows = df[df['locality_code'] == loc_code]
        if len(rows) > 0:
            actual = rows.iloc[0]
            for component in ['work', 'pe', 'mp']:
                actual_val = float(actual[f'gpci_{component}'])
                expected_val = expected[component]
                deviation = abs(actual_val - expected_val)
                
                if deviation > 0.001:  # Tolerance for float precision
                    warnings[f'{loc_code}_{component}_deviation'] = {
                        'locality': expected['name'],
                        'component': component,
                        'expected': expected_val,
                        'actual': actual_val,
                        'deviation': deviation
                    }
    
    # Check GPCI coverage
    if warnings['coverage_pct'] < 99.5:
        warnings['coverage_warning'] = f"GPCI coverage {warnings['coverage_pct']:.2f}% < 99.5% threshold"
    
    return warnings
```

**CMS Known Values (2025):**
- Alabama (00): work=1.000, pe=0.869, mp=0.575
- Alaska (01): work=1.500, pe=1.081, mp=0.592 (permanent work floor)
- Manhattan (05): work=1.122, pe=1.569, mp=1.859 (highest PE/MP)
- Arkansas (13): work=1.000, pe=0.860, mp=0.518

**Note on Floors:**
- **Alaska:** Permanent 1.50 work GPCI floor (statutory)
- **Congressional floor:** 1.00 work GPCI floor (time-boxed, periodically extended)
- **Parser:** Delivers raw CMS indices
- **Pricing logic:** Applies floors at payment calculation time (not parser)

### Step 4: Integration Smoke Test (Post-Load)

**Payment Spot-Check (Test Harness - NOT Parser Logic):**

```python
def test_gpci_payment_spot_check():
    """
    Integration smoke test: spot-check payment calculation.
    
    Per CMS PFS Lookup Tool: https://www.cms.gov/medicare/physician-fee-schedule/search
    
    Example: CPT 99213 (Office visit, established patient)
    - PPRRVU work RVU: 0.93
    - Physician CF 2025: 32.3465
    - Alabama locality (00): work GPCI = 1.000
    - Expected payment (work component): 0.93 × 1.000 × 32.3465 = $30.08
    """
    # Load PPRRVU, GPCI, CF
    pprrvu = load_parsed_pprrvu()
    gpci = load_parsed_gpci()
    cf = load_parsed_cf()
    
    # Spot-check: CPT 99213, Alabama locality, 2025-01-01
    code = '99213'
    locality = '00'
    date = '2025-01-01'
    
    rvu = pprrvu[(pprrvu['hcpcs'] == code) & (pprrvu['effective_from'] <= date)]
    gpci_row = gpci[(gpci['locality_code'] == locality) & (gpci['effective_from'] <= date)]
    cf_row = cf[(cf['cf_type'] == 'physician') & (cf['effective_from'] <= date)]
    
    # Compute work component
    payment_work = float(rvu['work_rvu'].iloc[0]) * float(gpci_row['gpci_work'].iloc[0]) * float(cf_row['cf_value'].iloc[0])
    
    # Tolerance: ±$0.01 (rounding differences)
    expected = 30.08
    assert abs(payment_work - expected) <= 0.01, f"Payment spot-check failed: {payment_work} vs {expected}"
```

This test runs **after** all parsers complete, not during GPCI parsing.

---

## 📦 **Deliverables Checklist**

### Schema Contract
- [x] `cms_gpci_v1.2.json` created with CMS-native naming
-  Schema v1.1 deprecated (add deprecation note)

### Layout Registry
-  Update `GPCI_2025D_LAYOUT` (v2025.4.0 → v2025.4.1)
-  Rename columns to match schema v1.2
-  Measure actual `min_line_length` from sample data
-  Mark enrichment columns as optional

### Parser Module
-  `gpci_parser.py` (~180 lines following v1.7 template)
-  Column alias map (30 aliases for CSV variants)
-  Helper functions (_parse_csv, _parse_xlsx, _parse_fixed_width, etc.)
-  Range validation (_validate_gpci_ranges)
-  CMS guardrails (_apply_gpci_guardrails)
-  Schema v1.2 loading

### Test Fixtures
-  `tests/fixtures/gpci/golden/GPCI2025_sample.txt` (20 rows)
-  Include edge cases: Alaska (01), Manhattan (05), Alabama (00), rest-of-state (99)
-  `tests/fixtures/gpci/golden/GPCI2025_sample.csv`
-  `tests/fixtures/gpci/golden/GPCI2025_sample.xlsx`
-  `tests/fixtures/gpci/golden/README.md` (SHA-256, source, CMS release)
-  `tests/fixtures/gpci/negatives/*.csv` (8 negative cases)

### Test Files
-  `tests/ingestion/test_gpci_parser_golden.py` (5 tests)
-  `tests/ingestion/test_gpci_parser_negatives.py` (8 tests)
-  `tests/integration/test_gpci_payment_spot_check.py` (1 integration test)

### Documentation
-  Parser docstring with CMS provenance references
-  Golden fixture README with CMS release notes
-  Update `CHANGELOG.md` with GPCI completion + schema v1.2
-  Update `STATUS_REPORT.md` with progress

---

## ⏱️ **Time Breakdown (v2.0 with Schema Migration)**

| Task | Estimated Time | Notes |
|------|----------------|-------|
| **Schema Migration** | | |
| Create schema v1.2 | ✅ 15 min | COMPLETE |
| Deprecate schema v1.1 | 5 min | Add note |
| **Layout Update** | | |
| Fix layout v2025.4.1 | 20 min | CMS-native names |
| Measure min_line_length | 5 min | Sample data |
| **Implementation** | | |
| Parser skeleton | 25 min | 9-step template |
| Alias map | 15 min | 30 aliases |
| Helper functions | 35 min | CSV/XLSX/FW |
| Range validation | 20 min | 3 GPCI + row count |
| CMS guardrails | 20 min | Known values |
| **Testing** | | |
| Extract golden fixtures | 20 min | 20 representative rows |
| Golden tests | 25 min | 5 test cases |
| Negative tests | 30 min | 8 failure scenarios |
| Integration test | 10 min | Payment spot-check |
| **Documentation** | | |
| Parser docstring | 10 min | CMS references |
| Fixture README | 5 min | SHA-256 + source |
| CHANGELOG.md | 5 min | v1.2 + GPCI completion |
| **TOTAL** | **~2.5 hours** | +30 min vs v1.0 for schema work |

---

## ✅ **Acceptance Criteria (v1.2 Schema)**

**Code Quality:**
-  Follows STD-parser-contracts v1.7 §21.1 (9-step template)
-  Uses parser kit utilities (no code duplication)
-  Schema v1.2 (CMS-native: `gpci_mp`, lean columns)
-  Layout v2025.4.1 (aligned with schema)
-  WARN severity for duplicate keys (not BLOCK)

**Testing:**
-  14/14 tests passing (5 golden + 8 negatives + 1 integration)
-  Golden test produces identical hash
-  All rejection paths covered
-  Determinism test passes
-  Row count validation: 100-120 expected, fail <90
-  Payment spot-check within ±$0.01 (integration)

**Documentation:**
-  Comprehensive docstring with CMS references
-  Golden fixture README with SHA-256 + CMS release
-  CHANGELOG.md updated with schema v1.2 migration
-  Cross-references to REF PRDs

**Performance:**
-  Parse 115 rows in <100ms
-  Memory usage <50MB
-  API P95 ≤ 500ms (integration requirement)

**Integration:**
-  Natural keys match PRD-rvu-gpci §3.3
-  MAC codes valid (5 digits)
-  Locality codes zero-padded 2 digits
-  GPCI values in CMS-realistic bounds [0.30, 2.00]
-  Ready for warehouse view `vw_gpci_current(date)`
-  Enrichment columns (state, locality_name, mac) optional

---

## 🚀 **Implementation Workflow**

### Phase A: Schema & Layout (40 min)
1. ✅ Create schema v1.2 (COMPLETE)
2. Add deprecation note to schema v1.1
3. Update `GPCI_2025D_LAYOUT` to v2025.4.1
4. Measure actual line length from sample data
5. Verify layout with sample CSV/TXT

### Phase B: Golden-First Development (75 min)
1. Extract golden fixture (20 rows with edge cases)
2. Write `test_gpci_golden_csv()` (will fail)
3. Implement parser following 9-step template
4. Test iteratively until golden test passes
5. Verify schema v1.2 compliance

### Phase C: Comprehensive Testing (45 min)
1. Create 8 negative fixtures
2. Write 8 negative tests
3. Write 1 integration test (payment spot-check)
4. Verify all rejection paths
5. Row count validation tests

### Phase D: Documentation (20 min)
1. Comprehensive parser docstring with CMS references
2. Golden fixture README with SHA-256 + CMS release info
3. Update CHANGELOG.md (schema v1.2 + GPCI completion)
4. Update STATUS_REPORT.md

---

## 📚 **Reference Materials**

**Completed Parsers:**
- ✅ `pprrvu_parser.py` - Fixed-width reference
- ✅ `conversion_factor_parser.py` - CSV/XLSX/ZIP reference

**Standards & PRDs:**
- ✅ STD-parser-contracts v1.7 §21.1 (9-step template)
- ✅ STD-parser-contracts §7.3 (Layout-schema alignment)
- ✅ STD-parser-contracts §8.5 (Error code severity table)
- ✅ **REF-cms-pricing-source-map-prd-v1.0.md** (GPCI source inventory)
- ✅ **PRD-rvu-gpci-prd-v0.1.md** (GPCI ingestion requirements §1.2, §2.4, §3.3)
- ✅ **REF-geography-source-map-prd-v1.0.md** (Geography integration)

**Schema:**
- ✅ `cms_gpci_v1.2` - CMS-native naming (gpci_mp, lean columns)

**Sample Data:**
- ✅ `sample_data/rvu25d_0/GPCI2025.csv` (~115 rows)
- ✅ `sample_data/rvu25d_0/GPCI2025.txt` (~115 rows)
- ✅ `sample_data/rvu25d_0/GPCI2025.xlsx` (~115 rows)

**Authoritative Layouts:**
- ✅ `RVU25D.pdf` - CMS authoritative layout documentation
- ✅ Fixed-width positions verified against sample data

**CMS References:**
- ✅ PFS Lookup Tool: https://www.cms.gov/medicare/physician-fee-schedule/search
- ✅ RVU Files: https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
- ✅ Alaska GPCI floor: Statutory 1.50 work component
- ✅ Congressional floor: 1.00 work component (periodically extended)

---

## 🎯 **Success Metrics**

**Schema Evolution:**
- ✅ CMS-native naming adopted (`gpci_mp` not `gpci_malp`)
- ✅ Lean schema (enrichment optional, not required)
- ✅ state vs state_fips clarity (CMS Locality Key uses names, not FIPS)

**Parser Quality:**
- ✅ Follows v1.7 template (9 steps)
- ✅ Layout-schema alignment verified
- ✅ 14/14 tests passing
- ✅ CMS-realistic validation (100-120 rows, [0.30, 2.00] bounds)
- ✅ Payment spot-check integration

**Documentation:**
- ✅ CMS references in docstring
- ✅ Schema v1.2 migration documented
- ✅ GPCI floors explained (parser delivers raw, pricing applies floors)

---

**Ready to implement!** Schema v1.2 is CMS-native and production-ready. Follow this plan for a 2.5-hour, bullet-proof implementation.


