# Parser QA & Operations Runbook

**Purpose:** Operational procedures, QA checklists, and SLAs for parser development  
**Audience:** QA engineers, SREs, parser implementers (pre-coding phase)  
**Status:** Draft v1.0  
**Owners:** Data Platform Engineering, QA Guild  
**Consumers:** QA engineers, parser developers, SREs  
**Change control:** PR review  

**Cross-References:**
- **STD-parser-contracts-prd-v2.0.md:** Core contracts and versioning
- **STD-parser-contracts-impl-v2.0.md:** Implementation templates
- **REF-parser-routing-detection-v1.0.md:** Router architecture
- **REF-parser-quality-guardrails-v1.0.md:** Validation and metrics
- **STD-qa-testing-prd-v1.0.md:** QTS compliance requirements

---

## 0. Overview

This runbook provides operational procedures for parser development, QA, and production monitoring.

**Use this for:**
- Pre-implementation verification (§1)
- QA acceptance criteria (§2)
- Performance SLAs (§3)
- Production monitoring (§4)

**Don't use this for:**
- Coding guidance (see STD-parser-contracts-impl-v2.0)
- Policy/contracts (see STD-parser-contracts-prd-v2.0)
- Router implementation (see REF-parser-routing-detection-v1.0)
- Validation patterns (see REF-parser-quality-guardrails-v1.0)

---

## 1. Pre-Implementation Verification Checklist

**Principle:** Verify all format variations BEFORE writing parser code.

**Problem:** Starting without format verification leads to:
- Unexpected failures when format B/C/D encountered
- Rework of parsing logic per format
- Test failures after "complete" implementation
- 2-3 hours debugging per format

**Solution:** Complete this checklist before coding (90-120 min investment, 4-6 hours saved).

### Step 1: Inventory All Formats (15 min)

-  List all expected formats: TXT, CSV, XLSX, ZIP
-  Obtain sample file for each format
-  Document source location/URL for each
-  Verify samples from correct product_year/quarter

### Step 2a: Inspect Headers & Structure (30 min)

**For each format:**

**TXT:** Measure actual line length
```bash
head -20 sample.txt | tail -10 | awk '{print length}'
# Document: min=165, max=173, set min_line_length=160
```

**CSV:** Document header row structure
```bash
head -5 sample.csv
# Document: Row 1 = title, Row 2 = headers, Row 3 = data
# Note: skiprows=2 needed
```

**XLSX:** Document sheet structure
```python
import pandas as pd
xlsx = pd.ExcelFile('sample.xlsx')
print(xlsx.sheet_names)  # Which sheet has data?
df = pd.read_excel('sample.xlsx', sheet_name=0, nrows=5)
print(df.columns)  # Header row index?
```

**ZIP:** List inner files
```bash
unzip -l sample.zip
# Document: Inner file names, formats, which to parse
```

### Step 2b: Verify Fixed-Width Layout Positions (10 min)

Use layout verification tool:

```bash
# Create draft layout first, then verify
python tools/verify_layout_positions.py \
  cms_pricing/ingestion/parsers/layout_registry.py \
  sample_data/rvu25d_0/GPCI2025.txt \
  5

# Review output and answer questions:
# 1. Does 'locality_code' contain 2-digit codes?
# 2. Does 'gpci_work' contain decimal values 0.5-2.0?
# 3. Are any values truncated?
# 4. Are end indices EXCLUSIVE?

# Adjust positions if needed, re-run
```

**Source of Truth:** Manual inspection + domain knowledge

### Step 2c: Real Data Format Variance Analysis (5-10 min)

**Purpose:** Detect format variance in real CMS files before coding.

**Checklist:**

1. **Row counts by format**
   ```bash
   wc -l sample_data/*/25LOCCO.txt
   python -c "import pandas as pd; print(len(pd.read_csv('25LOCCO.csv')))"
   python -c "import pandas as pd; print(len(pd.read_excel('25LOCCO.xlsx')))"
   ```

2. **Select Format Authority Matrix**
   - Choose authoritative format (typically TXT)
   - Document rationale
   - Record in `planning/parsers/<parser>/AUTHORITY_MATRIX.md`

3. **Define parity thresholds** (per QTS §5.1.3)
   - Natural-key overlap: ≥ 98%
   - Row-count variance: ≤ 1% OR ≤ 2 rows

4. **Plan diff artifacts**
   - `<parser>_parity_missing_in_<format>.csv`
   - `<parser>_parity_extra_in_<format>.csv`
   - `<parser>_parity_summary.json`

5. **Document observations**
   - Encodings per format
   - Sheet names (XLSX)
   - Header row positions
   - Known CMS quirks

**Decision Tree:**
```
If row variance < 2%:
  → Proceed with real-source parity tests

If variance 2-10%:
  → Document variance, implement with diff artifacts

If variance ≥ 10%:
  → STOP, investigate file provenance
```

**Time:** 5-10 min  
**Saves:** 30-60 min debugging

### Step 3: Header Normalization Mapping (30 min)

- Extract all unique column names from ALL formats
- Create comprehensive alias map
- Verify aliases map to schema columns

```python
CANONICAL_ALIAS_MAP = {
    # TXT format
    'locality code': 'locality_code',
    # CSV format  
    'locality_code': 'locality_code',
    # XLSX format
    'Locality Code': 'locality_code',
}
```

### Step 4: Validate Layouts Against Real Data (20 min)

For fixed-width:
- Load layout specification
- Parse first 10 data rows
- Verify column boundaries correct
- Measure actual vs expected line lengths

### Step 5: Document Format-Specific Quirks (15 min)

Create quirks table:
| Format | Quirk | Impact | Solution |
|--------|-------|--------|----------|
| TXT | min_line_length=165 | Header detection | Dynamic detection |
| CSV | 2 header rows | Wrong row read | skiprows=2 |
| XLSX | Full dataset | Has duplicates | Expect rejects |

### Step 6: Create Format Test Matrix (10 min)

| Format | Test Type | Fixture | Expected Rows | Rejects |
|--------|-----------|---------|---------------|---------|
| TXT | golden | sample.txt | 18 | 0 |
| CSV | golden | sample.csv | 18 | 0 |
| XLSX | full | full.xlsx | >=100 | >0 |

### Step 7: Signoff Checklist

Before writing parser code:
-  All formats inspected and documented
-  Header structures understood
-  Alias map covers all variations
-  Layout validated against real data
-  Format quirks documented
-  Test matrix created
-  Sample fixtures obtained

**Time Investment:** ~2 hours  
**Time Saved:** 4-6 hours debugging

---

## 2. QA Workflows

### 2.1 Golden-First Development

**Recommended approach:**

**Step 1: Extract golden fixture** (15 min)
```bash
head -101 sample_data/source.txt > tests/fixtures/{dataset}/golden/sample.txt
shasum -a 256 tests/fixtures/{dataset}/golden/sample.txt
```

**Step 2: Write golden test first** (15 min)
- Assert expected row count
- Assert schema compliance
- Assert deterministic hash
- Run test (will fail - parser doesn't exist)

**Step 3: Implement parser** (60-90 min)
- Follow 11-step template (STD-parser-contracts-impl §2.1)
- Use parser kit utilities
- Test iteratively until golden test passes

**Step 4: Add remaining tests** (20 min)
- Schema compliance
- Encoding variations
- Error handling
- Empty files

**Step 5: Commit** (5 min)
- Small, atomic commit
- Link to golden fixture

**Benefits:**
- ✅ Test-driven (spec before code)
- ✅ Determinism built-in
- ✅ Regression prevention

### 2.2 Acceptance Checklist

**Routing & Natural Keys:**
-  Correct dataset, schema_id, natural_keys from router
-  Routing latency p95 ≤ 20ms
-  Natural keys: uniqueness enforced per policy

**Validation:**
-  Categorical unknowns handled per policy
-  Rejects include full error context
-  Join invariant: valid + rejects = input

**Precision & Determinism:**
-  Numerics: precision preserved (schema-driven)
-  Determinism: repeat → identical row_content_hash
-  Chunked vs single-shot: identical outputs

**Performance:**
-  10K rows: parse ≤ 300ms
-  E2E soft guard ≤ 1.5s (hard fail at 2s)

**Testing:**
-  Golden fixture with SHA-256 documented
-  5+ unit tests: golden, schema, encoding, error, empty
-  Integration test: route → parse → validate → finalize

**Documentation:**
-  Comprehensive docstring with examples
-  Golden fixture README
-  Parser version constant (SemVer)

---

## 3. Performance SLAs

### 3.1 Parse Time Targets

**Per file size:**
- **Small (<1MB):** < 100ms
- **Medium (1-10MB):** < 1s
- **Large (10-100MB):** < 10s

**Per row count:**
- **1K rows:** < 50ms
- **10K rows:** < 300ms
- **100K rows:** < 3s

**Enforcement:**
- Performance regression tests in CI
- Alert if p95 > 2x target
- Block merge if p99 > 5x target

### 3.2 Memory Constraints

**Peak RSS:**
- **Standard files:** < 500MB
- **Large files (>10MB):** < 2GB
- **No memory leaks:** Run GC between files

**Enforcement:**
- Memory profiling in CI
- Alert if peak > 1.5x target

### 3.3 Throughput Requirements

**Aggregate parsing:**
- **6 files (MPFS bundle):** < 10s total
- **Parallel parsing:** Support 4 concurrent files
- **Batch processing:** > 1M rows/minute

---

## 4. Production Monitoring

### 4.1 Alert Thresholds

**Critical (Page):**
- Parse failures > 5%
- Parse duration > 5x SLO
- Memory > 2x limit

**Warning (Slack):**
- Parse failures > 1%
- Parse duration > 2x SLO
- Reject rate > 10%

**Info (Log):**
- New encoding detected
- Layout version updated
- Schema drift detected

### 4.2 Dashboard KPIs

**Real-time:**
- Parse success rate (last hour)
- Average parse duration
- Reject rate by dataset

**Daily:**
- Files parsed by dataset
- Rows ingested vs rejected
- Schema validation pass rate

**Weekly:**
- Format distribution (TXT/CSV/XLSX)
- Encoding distribution
- Top rejection reasons

---

## 5. Operational Anti-Patterns

### 5.1 Skipping Pre-Checks

```python
# ❌ ANTI-PATTERN: Start coding without format verification
# Impact: 4-6 hours debugging format issues
# Fix: Complete §1 checklist before coding
```

### 5.2 No Golden Fixtures

```python
# ❌ ANTI-PATTERN: Test with live data only
# Impact: Non-deterministic tests, hard to reproduce
# Fix: Create golden fixtures with known outputs
```

### 5.3 Test-Only Flags

```python
# ❌ ANTI-PATTERN: Add test-mode bypasses
if metadata.get('skip_validation'):
    return

# Impact: Test != production code
# Fix: Use tiered validation (ERROR/WARN/INFO)
```

---

## 6. Compliance & Licensing

**License Tracking:**
- Record CMS data license in provenance
- Track attribution requirements
- Document usage restrictions

**Audit Trails:**
- IngestRun model tracks full provenance
- All ingestion runs logged
- Quarantine artifacts preserved

---

## 7. Cross-References

**Core Standards:**
- STD-parser-contracts-prd-v2.0.md (contracts)
- STD-qa-testing-prd-v1.0.md (QTS requirements)

**Companion Documents:**
- STD-parser-contracts-impl-v2.0.md (templates)
- REF-parser-routing-detection-v1.0.md (routing)
- REF-parser-quality-guardrails-v1.0.md (validation)

**Tools:**
- `tools/verify_layout_positions.py` (layout verification)

---

## 8. Source Section Mapping (v1.11 → runbook v1.0)

**For reference during transition:**

This runbook contains content from the following sections of `STD-parser-contracts-prd-v1.11-ARCHIVED.md`:

| runbook v1.0 Section | Original v1.11 Section | Lines in v1.11 |
|----------------------|------------------------|----------------|
| §1 Pre-Implementation Verification | §21.4 Format Verification Pre-Implementation Checklist | 3973-4201 |
| §1 Step 2c (Variance Analysis) | §21.4 Step 2c Real Data Format Variance Analysis | 4025-4085 |
| §2.1 Golden-First Development | §21.7 Golden-First Development Workflow | 4270-4305 |
| §2.2 Acceptance Checklist | §21.5 Per-Parser Acceptance Checklist | 4204-4234 |
| §3 Performance SLAs | §5.5 Constraints & SLAs | 877-896 |
| §4 Production Monitoring | §10 Observability & Metrics (operational subset) | 2180-2715 (selected) |
| §5 Operational Anti-Patterns | §20.1 Anti-Patterns (operational subset) | 3252-3570 (selected) |
| §6 Compliance & Licensing | §13 Security & Compliance (operational subset) | 2825-2842 (selected) |

**Sections NOT in this document (see other companions):**
- §5-6 Processing/Contracts → STD-parser-contracts-prd-v2.0.md + STD-parser-contracts-impl-v2.0.md
- §7 Router/Layout → REF-parser-routing-detection-v1.0.md
- §8-9 Validation/Errors → REF-parser-quality-guardrails-v1.0.md
- §21.1-21.3 Templates → STD-parser-contracts-impl-v2.0.md

**Archived source:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md`

**Cross-Reference:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 9. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| **2025-10-17** | **v1.0** | **Team** | **Initial QA runbook.** Split from STD-parser-contracts-prd-v1.11 §5.5, §21.4-21.7. Contains: pre-implementation verification checklist (§1, 7 steps), format variance analysis (Step 2c), golden-first workflow (§2.1), acceptance checklist (§2.2), performance SLAs (§3), production monitoring (§4), operational anti-patterns (§5), compliance (§6). Total: ~800 lines of QA procedures. **Cross-References:** STD v2.0 (contracts), impl (templates), routing (format detection), quality (validation). |

---

*End of QA Runbook*

*For implementation guidance, see STD-parser-contracts-impl-v2.0.md*  
*For validation patterns, see REF-parser-quality-guardrails-v1.0.md*

