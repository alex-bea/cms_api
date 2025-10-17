# Parser Routing & Format Detection Reference

**Purpose:** Router architecture, format detection patterns, and layout registry implementation  
**Audience:** Engineers implementing routers and format detection logic  
**Status:** Draft v1.0  
**Owners:** Data Platform Engineering  
**Consumers:** Parser developers, Platform engineers  
**Change control:** PR review  

**Cross-References:**
- **STD-parser-contracts-prd-v2.0.md:** Core contracts (§6.2 Router Contract)
- **STD-parser-contracts-impl-v2.0.md:** Implementation patterns
- **REF-parser-quality-guardrails-v1.0.md:** Validation after routing
- **RUN-parser-qa-runbook-prd-v1.0.md:** Pre-implementation format verification

---

## 1. Overview

This reference document provides detailed architecture and implementation patterns for:
- **Parser routing** - Selecting the correct parser for a file
- **Format detection** - Identifying file format (TXT/CSV/XLSX/ZIP)
- **Layout registry** - Managing fixed-width column specifications
- **Layout-schema alignment** - Ensuring layouts match schema contracts

**Use this for:**
- Implementing router logic (`route_to_parser()`)
- Adding format detection for new file types
- Defining fixed-width layouts
- Troubleshooting format detection issues

**Don't use this for:**
- Parser implementation (see STD-parser-contracts-impl-v2.0)
- Validation/metrics (see REF-parser-quality-guardrails-v1.0)
- QA procedures (see RUN-parser-qa-runbook-prd-v1.0)

---

## 2. Router & Format Detection

### 2.1 Format Detection Strategy

**Principle:** Robust format detection using extension + content sniffing to handle misnamed files.

**Two-Phase Approach:**

**Phase 1: Extension-Based (Fast Path - 90% of cases)**
```python
# Check file extension first
if filename.endswith('.zip'):
    return _parse_zip(...)
elif filename.endswith('.xlsx') or filename.endswith('.xls'):
    return _parse_xlsx(...)
elif filename.endswith('.csv'):
    return _parse_csv(...)
elif filename.endswith('.txt'):
    # Could be fixed-width OR CSV → Phase 2 needed
    return _detect_and_parse_txt(...)
```

**Phase 2: Content Sniffing (Fallback)**
```python
def _detect_and_parse_txt(content: bytes, encoding: str, metadata: Dict) -> pd.DataFrame:
    """
    Content-based detection for ambiguous .txt files.
    
    Checks:
    1. Layout existence (is fixed-width expected?)
    2. Delimiter detection (commas, tabs, pipes)
    3. Fixed-width pattern (consistent column positions)
    """
    # Check if layout exists
    layout = get_layout(
        product_year=metadata['product_year'],
        quarter_vintage=metadata['quarter_vintage'],
        dataset=metadata['dataset_id']
    )
    
    if layout:
        # Try fixed-width first
        try:
            return _parse_fixed_width(content, encoding, layout)
        except LayoutMismatchError:
            logger.warning("Layout mismatch, falling back to CSV")
    
    # No layout or layout failed → Check for delimiters
    sample = content[:1000].decode(encoding, errors='replace')
    
    if ',' in sample and sample.count(',') > 10:
        logger.info("Detected CSV format (comma delimiter)")
        return _parse_csv(content, encoding)
    elif '\t' in sample and sample.count('\t') > 5:
        logger.info("Detected TSV format (tab delimiter)")
        return _parse_csv(content, encoding, delimiter='\t')
    else:
        raise ParseError(f"Could not detect format for {filename}")
```

**Benefits:**
- Fast path for correctly named files
- Handles misnamed files (`.txt` containing CSV)
- Explicit fallback order
- Logged format detection for debugging

### 2.2 ZIP File Handling

**Challenge:** Inner files may have ambiguous formats.

**Solution: Recursive Format Detection**

```python
def _parse_zip(content: bytes, encoding: str, metadata: Dict) -> Tuple[pd.DataFrame, str]:
    """
    Parse ZIP with content-based format detection for inner files.
    
    Returns:
        (DataFrame, inner_filename)
    """
    import zipfile
    from io import BytesIO
    
    with zipfile.ZipFile(BytesIO(content)) as zf:
        # Find first parseable file (skip PDFs, docs)
        for inner in zf.namelist():
            if inner.endswith(('.pdf', '.doc', '.docx')):
                continue
                
            with zf.open(inner) as f:
                raw = f.read()
                
            # Detect inner file format
            if inner.lower().endswith(('.xlsx', '.xls')):
                return _parse_xlsx(BytesIO(raw)), inner
            elif inner.lower().endswith('.csv'):
                return _parse_csv(raw, encoding), inner
            else:
                # Content sniffing for .txt or unknown
                sample = raw[:500].decode(encoding, errors='replace')
                
                # Check for fixed-width pattern
                if re.search(r'^\d{5}', sample, re.MULTILINE):
                    logger.debug(f"Detected fixed-width in {inner}")
                    layout = get_layout(...)
                    if layout:
                        return _parse_fixed_width(raw, encoding, layout), inner
                
                # Default to CSV
                logger.debug(f"Parsing {inner} as CSV (fallback)")
                return _parse_csv(raw, encoding), inner
    
    raise ParseError(f"No parseable files found in ZIP")
```

**Key Patterns:**
- Skip non-data files (PDFs, docs)
- Prioritize extension hint
- Fall back to content sniffing
- Pass metadata through for layout lookup

### 2.3 Format Detection Flowchart

```
File Input
    |
    v
Check Extension
    |
    +-- .zip -----> Extract inner --> Recursive detection
    |
    +-- .xlsx ----> Parse as Excel
    |
    +-- .csv -----> Parse as CSV
    |
    +-- .txt -----> Content Sniffing
                        |
                        +-- Layout exists? --> Try fixed-width
                        |                          |
                        |                          +-- Success --> Return
                        |                          |
                        |                          +-- Fail ---+
                        |                                      |
                        +-- Check delimiters <----------------+
                                |
                                +-- Commas? --> CSV
                                |
                                +-- Tabs? --> TSV
                                |
                                +-- None --> ERROR
```

### 2.4 Router Pattern Matching

**Router inspects:**
- Magic bytes (ZIP: `PK`, Excel: `PK` + XML, PDF: `%PDF`)
- BOM markers
- First N lines for headers
- Header tokens for dataset identification
- Filename as fallback hint

**Implementation:** `cms_pricing/ingestion/parsers/__init__.py`

**Pattern Matching:**
```python
PARSER_ROUTING = {
    r"PPRRVU.*\.(txt|csv|xlsx)$": ("pprrvu", "cms_pprrvu_v1.0", parse_pprrvu),
    r"GPCI.*\.(txt|csv|xlsx)$": ("gpci", "cms_gpci_v1.0", parse_gpci),
    r"LOCCO.*\.(txt|csv|xlsx)$": ("locality", "cms_locality_raw_v1.0", parse_locality_raw),
    # ...
}
```

- Regex patterns for each dataset
- Case-insensitive matching
- Prioritized matching (most specific first)

### 2.5 Common Detection Pitfalls

**Pitfall 1: Extension Trust**
```python
# ❌ WRONG: Trust extension blindly
if filename.endswith('.txt'):
    return _parse_fixed_width(...)  # Fails if actually CSV

# ✅ CORRECT: Verify with content
if filename.endswith('.txt'):
    return _detect_and_parse_txt(...)  # Checks content
```

**Pitfall 2: No Fallback**
```python
# ❌ WRONG: Fail immediately
layout = get_layout(...)
if not layout:
    raise ParseError("No layout found")

# ✅ CORRECT: Try CSV as fallback
layout = get_layout(...)
if layout:
    try:
        return _parse_fixed_width(...)
    except LayoutMismatchError:
        pass  # Fall through
return _parse_csv(...)  # Fallback
```

**Pitfall 3: Insufficient Sample Size**
```python
# ❌ WRONG: Only first line
sample = content[:100]  # May miss patterns

# ✅ CORRECT: First 500-1000 bytes
sample = content[:1000]  # Covers multiple rows
```

### 2.6 Implementation Checklist

For each parser:
-  Extension-based fast path implemented
-  Content sniffing for ambiguous cases
-  ZIP inner file detection
-  Format detection logged
-  Graceful fallback order
-  Clear error messages when detection fails

---

## 3. Layout Registry

### 3.1 Purpose

Externalize fixed-width column specifications with SemVer by year/quarter.

**Implementation:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Current Format (Python dicts):**
```python
LAYOUT_REGISTRY = {
    ('pprrvu', '2025', 'Q4'): {
        'version': 'v2025.4.1',  # SemVer
        'min_line_length': 165,
        'source_version': '2025D',
        'data_start_pattern': r'^[A-Z0-9]{5}',  # HCPCS code pattern
        'columns': {
            # Schema-canonical names (NOT API names)
            'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
            'modifier': {'start': 5, 'end': 7, 'type': 'string', 'nullable': False},
            'rvu_work': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},
            'rvu_malp': {'start': 85, 'end': 89, 'type': 'decimal', 'nullable': True},
            # ...
        }
    },
    ('gpci', '2025', 'Q4'): {
        'version': 'v2025.4.2',
        'min_line_length': 160,
        'data_start_pattern': r'^\d{2}',  # 2-digit locality code
        'columns': {
            'locality_code': {'start': 0, 'end': 2, 'type': 'string'},
            'locality_name': {'start': 3, 'end': 50, 'type': 'string'},
            'gpci_work': {'start': 51, 'end': 57, 'type': 'decimal'},
            'gpci_pe': {'start': 58, 'end': 64, 'type': 'decimal'},
            'gpci_mp': {'start': 65, 'end': 71, 'type': 'decimal'},
        }
    },
    # For locality (LOCCO) fixed-width
    ('locality', '2025', 'Q4'): {
        'version': 'v2025.4.2',
        'min_line_length': 120,
        'data_start_pattern': r'^\s+\d{5}',  # MAC code (5 digits, may have leading spaces)
        'columns': {
            'mac': {'start': 0, 'end': 12},
            'locality_code': {'start': 12, 'end': 18},
            'state_name': {'start': 18, 'end': 50},
            'fee_area': {'start': 50, 'end': 120},
            'county_names': {'start': 120, 'end': None},  # Variable width
        },
        'notes': [
            'State name may be blank on continuation rows (forward-fill required)',
            'County names are comma/slash-delimited',
            'Header rows start with "Medicare" or "MAC"'
        ]
    },
}
```

### 3.2 Function Signature

```python
def get_layout(
    product_year: str,      # "2025"
    quarter_vintage: str,   # "2025Q4" or "2025D"
    dataset: str            # "pprrvu", "gpci", etc.
) -> Optional[Dict[str, Any]]:
    """
    Get layout specification for fixed-width parsing.
    
    Returns layout dict or None if not found.
    """
```

**Lookup Logic:**
1. Extract quarter from `quarter_vintage`: `"2025Q4"` → `"Q4"`
2. Try specific quarter: `(dataset, product_year, quarter)` tuple
3. Fallback to annual: `(dataset, product_year, None)`
4. Return `None` if not found

**Critical Semantics:**
- **`end` is EXCLUSIVE** - For `read_fwf(colspecs=[(start, end)])` and `line[start:end]` slicing
- **Dict order ≠ positional order** - MUST sort columns by `start` before building colspecs
- **Column names MUST match schema** - No API names (`work_rvu` ❌, use `rvu_work` ✅)
- **`min_line_length`** = minimum observed data-line length (heuristic, not hard requirement)

### 3.3 Layout Versioning

**Version format:** `v{YEAR}.{QUARTER}.{PATCH}`

**Bump rules:**
- **YEAR.QUARTER**: New CMS release year/quarter
- **PATCH**: Corrections to existing layout

**Examples:**
- `v2025.4.0`: 2025 Q4 (revision D) layout
- `v2025.4.1`: Corrected column 15 width
- `v2026.1.0`: 2026 Q1 (revision A) layout

### 3.4 Common Layout Anti-Patterns

**Anti-Pattern 1: Positional Arguments**
```python
# ❌ WRONG: Positional (easy to swap dataset/quarter)
layout = get_layout("2025", "pprrvu", "Q4")

# ✅ CORRECT: Keyword arguments
layout = get_layout(
    product_year="2025",
    quarter_vintage="2025Q4",
    dataset="pprrvu"
)
```

**Anti-Pattern 2: Inclusive End**
```python
# ❌ WRONG: Treating end as inclusive
'hcpcs': {'start': 0, 'end': 5}  # Gets 6 chars (0-5 inclusive)

# ✅ CORRECT: End is exclusive
'hcpcs': {'start': 0, 'end': 5}  # Gets 5 chars (0-4)
```

**Anti-Pattern 3: Hardcoded Skiprows**
```python
# ❌ WRONG: Hardcode header rows
df = pd.read_fwf(file, skiprows=3)  # Breaks when CMS changes headers

# ✅ CORRECT: Dynamic detection
data_start = _find_data_start_row(lines, layout['data_start_pattern'])
df = pd.read_fwf(file, skiprows=data_start)
```

**Anti-Pattern 4: API Names in Layout**
```python
# ❌ WRONG: Use API presentation names
'columns': {
    'work_rvu': {'start': 61, 'end': 65},  # API name!
}

# ✅ CORRECT: Use schema canonical names
'columns': {
    'rvu_work': {'start': 61, 'end': 65},  # Schema name
}
```

---

## 4. Layout-Schema Alignment

### 4.1 Normative Requirements

**MUST Rules (CI-enforceable):**

1. **MUST** sort fixed-width columns by `start` position before building colspecs
2. **MUST** treat layout `end` as EXCLUSIVE for `read_fwf()` and slicing
3. **MUST** detect data start dynamically (no hardcoded skiprows)
4. **MUST** call `get_layout()` using keyword arguments
5. **MUST** use schema canonical column names (not API names)

### 4.2 Alignment Checklist

**Before implementing parser:**

1. ✅ Load schema contract: `cms_{dataset}_v1.0.json`
2. ✅ List natural keys: `schema['natural_keys']`
3. ✅ List all required columns: `schema['columns'].keys()`
4. ✅ Verify layout has ALL required columns (exact names, case-sensitive)
5. ✅ Verify natural key columns present in layout
6. ✅ Measure actual data line length (don't guess `min_line_length`)

### 4.3 Validation Guard (Post-Parse)

```python
# After normalization, before validation
required_cols = set(schema['columns'].keys())
actual_cols = set(df.columns)
missing = required_cols - actual_cols

if missing:
    sample = df.head(1).to_dict('records')[0] if len(df) > 0 else {}
    raise LayoutMismatchError(
        f"DataFrame missing required schema columns: {missing}. "
        f"Layout may be out of sync. First row: {sample}"
    )
```

### 4.4 Common Misalignments

| Schema Has | Layout Had | Fix | Version Bump |
|------------|------------|-----|--------------|
| `rvu_work` | `work_rvu` | Rename layout column | Patch |
| `modifier` | ❌ MISSING | Add at position 5:7 | Minor |
| `effective_from` | ❌ MISSING | Inject from metadata in parser | None |
| `rvu_malp` | `mp_rvu` | Rename layout column | Patch |

### 4.5 Real Example: PPRRVU Alignment Fix

**Before (v2025.4.0 - BROKE):**
```python
PPRRVU_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 200,  # ❌ Too strict (actual=173)
    'columns': {
        'hcpcs': {'start': 0, 'end': 5},
        # 'modifier' MISSING!  # ❌ Natural key absent
        'work_rvu': {'start': 61, 'end': 65},  # ❌ API name
    }
}
# Result: 0/7 tests passing, KeyError: 'rvu_work'
```

**After (v2025.4.1 - FIXED):**
```python
PPRRVU_2025D_LAYOUT = {
    'version': 'v2025.4.1',
    'min_line_length': 165,  # ✅ Measured actual (173) with margin
    'columns': {
        'hcpcs': {'start': 0, 'end': 5},
        'modifier': {'start': 5, 'end': 7},  # ✅ Added
        'rvu_work': {'start': 61, 'end': 65},  # ✅ Schema name
    }
}
# Result: 7/7 tests passing
```

**Time Saved:** 2+ hours debugging

---

## 5. CI Test Snippets

### 5.1 Colspecs Sorted by Start

```python
def test_layout_colspecs_sorted(layout):
    """Verify columns are sorted by start position."""
    cols = list(layout['columns'].items())
    for i in range(1, len(cols)):
        prev_start = cols[i-1][1]['start']
        curr_start = cols[i][1]['start']
        assert prev_start <= curr_start
```

### 5.2 End Exclusive Sanity

```python
def test_layout_end_exclusive(layout):
    """Verify end is exclusive by synthetic line test."""
    from io import StringIO
    
    # Build synthetic line with sentinel at end-1
    max_end = max(spec['end'] for spec in layout['columns'].values())
    line = [' '] * max_end
    for name, spec in layout['columns'].items():
        if spec['end'] > 0:
            line[spec['end'] - 1] = 'X'  # Sentinel
    synthetic = ''.join(line)
    
    # Parse with read_fwf
    colspecs = [(s['start'], s['end']) for s in layout['columns'].values()]
    names = list(layout['columns'].keys())
    df = pd.read_fwf(StringIO(synthetic), colspecs=colspecs, names=names, header=None)
    
    # Verify sentinel captured
    for col in df.columns:
        val = str(df[col].iloc[0]).strip()
        assert val.endswith('X') or val == ''
```

### 5.3 Layout Column Names Match Schema

```python
def test_layout_schema_alignment(layout, schema):
    """Verify layout column names match schema exactly."""
    schema_cols = set(schema['columns'].keys())
    layout_cols = set(layout['columns'].keys())
    missing = schema_cols - layout_cols
    assert not missing, f"Layout missing schema columns: {sorted(missing)}"
```

### 5.4 Natural Keys Present After Parse

```python
def test_parser_natural_keys_present(df, schema):
    """Verify natural key columns exist in parsed DataFrame."""
    required = set(schema['natural_keys'])
    actual = set(df.columns)
    missing = required - actual
    assert not missing, f"Missing natural keys: {sorted(missing)}"
```

### 5.5 Dynamic Data Start Enforced

```python
def test_parser_reports_dynamic_skiprows(metrics):
    """Verify parser reports dynamic header detection."""
    assert 'skiprows_dynamic' in metrics
    assert metrics['skiprows_dynamic'] >= 0
    assert 'data_start_pattern' in metrics
```

**Usage in CI:**
```python
# tests/ingestion/test_layout_compliance.py
@pytest.mark.parametrize("dataset,year,quarter", [
    ("pprrvu", "2025", "Q4"),
    ("gpci", "2025", "Q1"),
    ("locality", "2025", "Q4"),
])
def test_layout_ci_guards(dataset, year, quarter):
    layout = get_layout(
        dataset=dataset, 
        product_year=year, 
        quarter_vintage=f"{year}{quarter}"
    )
    schema = load_schema(f"cms_{dataset}_v1.0")
    
    test_layout_colspecs_sorted(layout)
    test_layout_end_exclusive(layout)
    test_layout_schema_alignment(layout, schema)
```

---

## 6. Routing Anti-Patterns from Production

**These are routing-specific anti-patterns extracted from real debugging sessions:**

### 6.1 Hardcoded Skiprows

```python
# ❌ ANTI-PATTERN: Hardcoded header rows
df = pd.read_fwf(file, colspecs=colspecs, header=None, skiprows=3)

# Why wrong: CMS changes header structure (2 rows → 3 rows)
# Impact: First data row treated as header
# Fix time: 1 hour debugging

# ✅ CORRECT: Dynamic detection
def _find_data_start(lines: List[str], pattern: str) -> int:
    """Find first line matching data pattern."""
    for i, line in enumerate(lines):
        if re.match(pattern, line.strip()):
            return i
    return 0

data_start = _find_data_start(lines, layout['data_start_pattern'])
df = pd.read_fwf(file, skiprows=data_start)
```

### 6.2 Missing Content Sniffing

```python
# ❌ ANTI-PATTERN: Extension-only routing
if filename.endswith('.txt'):
    return _parse_fixed_width(...)  # Assumes TXT = fixed-width

# Why wrong: Some .txt files are actually CSV
# Impact: LayoutMismatchError on CSV files named .txt
# Fix time: 30 min debugging

# ✅ CORRECT: Content sniffing fallback
if filename.endswith('.txt'):
    return _detect_and_parse_txt(...)  # Checks content first
```

### 6.3 Layout-Schema Name Mismatch

```python
# ❌ ANTI-PATTERN: API names in layout
'columns': {
    'work_rvu': {'start': 61, 'end': 65},  # API name!
}

# Why wrong: Schema expects 'rvu_work' (canonical)
# Impact: KeyError when finalizing output
# Fix time: 2 hours (all tests failing)

# ✅ CORRECT: Schema canonical names
'columns': {
    'rvu_work': {'start': 61, 'end': 65},  # Schema name
}
```

### 6.4 Insufficient Sample for Detection

```python
# ❌ ANTI-PATTERN: Small sample
sample = content[:50]  # Only first line

# Why wrong: May not contain delimiter in first line
# Impact: Incorrect format detection
# Fix time: 45 min debugging edge cases

# ✅ CORRECT: Adequate sample
sample = content[:1000]  # First 1KB, multiple rows
```

---

## 7. Layout Verification Tool

**Tool:** `tools/verify_layout_positions.py`

**Purpose:** Guided manual verification of fixed-width column positions.

**Usage:**
```bash
# Create draft layout first, then verify
python tools/verify_layout_positions.py \
  cms_pricing/ingestion/parsers/layout_registry.py \
  sample_data/rvu25d_0/PPRRVU25D.txt \
  5

# Review output:
# Sample line 1 (len=173)
# hcpcs          [  0,  5) → "99213"
# modifier       [  5,  7) → "  "
# rvu_work       [ 61, 65) → "0.97"

# Answer verification questions:
# 1. Does 'hcpcs' contain only 5-char HCPCS codes? YES
# 2. Does 'modifier' contain 2-char modifiers or blanks? YES
# 3. Does 'rvu_work' contain decimal values? YES
# ...

# If any NO, adjust layout positions and re-run
```

**Verification Checklist:**
- Does each column contain expected content type?
- Are any values truncated or spanning wrong columns?
- Are end indices EXCLUSIVE (not inclusive)?
- Do blank fields extract correctly?

**Reference:** STD-parser-contracts-prd-v1.0.md §21.4 Step 2b

---

## 8. Cross-References

**Core Standards:**
- STD-parser-contracts-prd-v2.0.md (§6.2 Router Contract)
- STD-parser-contracts-impl-v2.0.md (Implementation patterns)

**Related References:**
- REF-parser-quality-guardrails-v1.0.md (Validation after routing)
- RUN-parser-qa-runbook-prd-v1.0.md (§1 Pre-implementation format verification)

**Tools:**
- `cms_pricing/ingestion/parsers/__init__.py` (Router implementation)
- `cms_pricing/ingestion/parsers/layout_registry.py` (Layout definitions)
- `tools/verify_layout_positions.py` (Layout verification)

---

## 9. Source Section Mapping (v1.11 → routing v1.0)

**For reference during transition:**

This reference contains content from the following sections of `STD-parser-contracts-prd-v1.11-ARCHIVED.md`:

| routing v1.0 Section | Original v1.11 Section | Lines in v1.11 |
|----------------------|------------------------|----------------|
| §2.1 Format Detection Strategy | §7.1 Router & Format Detection (subsections 7.1.1-7.1.6) | 1314-1565 |
| §2.2 ZIP File Handling | §7.1.2 ZIP File Handling | 1382-1441 |
| §2.3 Format Detection Flowchart | §7.1.3 Format Detection Flowchart | 1443-1472 |
| §2.4 Router Pattern Matching | §7.1.4 Router Pattern Matching | 1477-1499 |
| §2.5 Common Detection Pitfalls | §7.1.5 Common Detection Pitfalls | 1503-1541 |
| §2.6 Implementation Checklist | §7.1.6 Implementation Checklist | 1545-1565 |
| §3 Layout Registry | §7.2 Layout Registry | 1566-1684 |
| §4 Layout-Schema Alignment | §7.3 Layout-Schema Alignment | 1685-1788 |
| §5 CI Test Snippets | §7.4 CI Test Snippets | 1791-1883 |
| §6 Routing Anti-Patterns | §20.1 Anti-Patterns (routing subset) | 3252-3570 (selected) |
| §7 Layout Verification Tool | (Referenced in §21.4 Step 2b) | 4003-4024 |

**Sections NOT in this document (see other companions):**
- §5 Processing → STD-parser-contracts-impl-v2.0.md
- §8-10 Validation/Errors/Metrics → REF-parser-quality-guardrails-v1.0.md
- §21 Templates/QA → STD-parser-contracts-impl-v2.0.md + RUN-parser-qa-runbook-prd-v1.0.md

**Archived source:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md`

**Cross-Reference:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 10. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| **2025-10-17** | **v1.0** | **Team** | **Initial routing reference document.** Split from STD-parser-contracts-prd-v1.11 §7. Contains: format detection strategy (§2), two-phase detection (extension + content sniffing), ZIP handling (§2.2), detection flowchart, router pattern matching, common pitfalls, layout registry (§3), layout versioning, layout-schema alignment (§4), CI test snippets (§5), routing anti-patterns (§6), layout verification tool (§7). Total: ~800 lines of routing architecture. **Cross-References:** STD v2.0 (contracts), impl (patterns), quality (validation), runbook (QA). |

---

*End of Routing Reference*

*For core contracts, see STD-parser-contracts-prd-v2.0.md*  
*For implementation guidance, see STD-parser-contracts-impl-v2.0.md*

