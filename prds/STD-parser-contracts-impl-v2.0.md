# Parser Contracts Implementation Guide (v2.0)

**Companion to:** STD-parser-contracts-prd-v2.0.md  
**Purpose:** Detailed implementation guidance and code patterns for parser development  
**Audience:** Engineers actively coding parsers  
**Status:** Draft v2.0  
**Owners:** Data Platform Engineering  
**Consumers:** Parser developers, QA engineers  
**Change control:** PR review  

**Cross-References:**
- **STD-parser-contracts-prd-v2.0.md:** Core policy, contracts, versioning rules
- **REF-parser-routing-detection-v1.0.md:** Router architecture, format detection
- **REF-parser-quality-guardrails-v1.0.md:** Validation, errors, metrics
- **RUN-parser-qa-runbook-prd-v1.0.md:** Pre-implementation checklists, QA workflows
- **STD-qa-testing-prd-v1.0.md:** QTS compliance requirements

---

## 0. Overview

This guide provides code-level implementation details for parsers complying with **STD-parser-contracts-prd-v2.0.md**.

**Read this for:**
- Step-by-step parser templates (§2)
- Input processing patterns (§1)
- Type handling and anti-patterns (§1.2-1.3)
- Validation phases and patterns (§2.2-2.3)
- Incremental implementation workflow (§2.4)

**Don't read this for:**
- High-level policy and contracts (see STD-parser-contracts-prd-v2.0)
- Router architecture details (see REF-parser-routing-detection-v1.0)
- Validation/metrics implementation (see REF-parser-quality-guardrails-v1.0)
- Pre-implementation checklists (see RUN-parser-qa-runbook-prd-v1.0)

---

## 1. Input Processing & Requirements

### 1.1 Inputs

**Primary inputs:**
- `file_obj` (binary stream/IO[bytes]) and filename
- Optional sidecar files (codebooks, lookup tables, layout specifications)

**Required metadata (injected by ingestor):**
- `dataset_id`: str - Dataset identifier (e.g., "pprrvu", "gpci")
- `release_id`: str - Release identifier (e.g., "mpfs_2025_q4_20251015")
- `vintage_date`: datetime - When data was published (timestamp)
- `product_year`: str - Valuation year (e.g., "2025")
- `quarter_vintage`: str - Quarter identifier (e.g., "2025Q4", "2025_annual")
- `source_uri`: str - Source URL
- `file_sha256`: str - File checksum
- `parser_version`: str - Parser SemVer version
- `schema_id`: str - Schema contract identifier with version
- `layout_version`: str - Layout specification version (for fixed-width)

**Supported formats (CMS-specific):**
- **CSV/TSV** - With header variations (case-insensitive matching)
- **Fixed-width TXT** - Using layout registry specifications  
- **XLSX** - Excel workbooks (single or multiple sheets)
- **ZIP** - Archives containing CSV/TXT/XLSX files
- **XML** - Structured XML (via pre-parsing to tabular)
- **JSONL** - Line-delimited JSON

**Not supported:**
- PDF (must be pre-extracted to structured format)
- Binary formats without documented structure

### 1.2 Encoding & Dialect Detection

**Encoding Priority Cascade:**
1. **BOM detection** (highest priority):
   - UTF-8-sig (0xEF 0xBB 0xBF)
   - UTF-16 LE (0xFF 0xFE)
   - UTF-16 BE (0xFE 0xFF)
2. **UTF-8 strict decode** (no errors)
3. **CP1252** (Windows default, common for CMS files)
4. **Latin-1** (ISO-8859-1, always succeeds as final fallback)

**Metrics Recording:**
- `encoding_detected`: str (actual encoding used)
- `encoding_fallback`: bool (true if not UTF-8)

**CSV Dialect Detection:**
- Delimiter: comma, tab, pipe
- Quote char: double quote, single quote
- Escape char: backslash
- Log dialect detection results for debugging

**Column Header Normalization:**
- Trim whitespace from headers
- Collapse multiple spaces to single space
- Convert to canonical snake_case
- Case-insensitive matching for aliases
- Map variations to canonical names (e.g., "HCPCS_CODE" → "hcpcs")

### 1.3 Alias Map Best Practices

**Purpose:** Comprehensive column header mapping to handle CMS format variations.

**Problem:**
CMS uses different header names across formats:
- TXT: "locality code" (spaces)
- CSV: "locality_code" (underscores)
- XLSX: "Locality Code" (title case)
- Yearly: "2025 PW GPCI" → "2026 PW GPCI"

**Solution:** Comprehensive alias map with testing.

**Alias Map Structure:**

```python
# Location: In parser module (e.g., gpci_parser.py)

CANONICAL_ALIAS_MAP = {
    # TXT Format (Fixed-Width)
    'locality code': 'locality_code',
    'locality name': 'locality_name',
    '2025 PW GPCI (with 1.0 floor)': 'gpci_work',
    '2025 PE GPCI': 'gpci_pe',
    '2025 MP GPCI': 'gpci_mp',
    
    # CSV Format (After normalization: spaces→underscores)
    'locality_code': 'locality_code',  # Identity mapping
    '2025_pw_gpci_(with_1.0_floor)': 'gpci_work',
    '2025_pe_gpci': 'gpci_pe',
    
    # XLSX Format
    '2025 PW GPCI': 'gpci_work',  # Without qualifier
    'Locality Code': 'locality_code',  # Title case
    
    # Historical Variations
    '2024 PW GPCI': 'gpci_work',
    'pw_gpci_2024': 'gpci_work',
}

# Usage in parser
def apply_aliases(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """Apply column name aliases with case-insensitive matching."""
    lower_map = {k.lower(): v for k, v in alias_map.items()}
    
    new_cols = {}
    for col in df.columns:
        canonical = lower_map.get(col.lower(), col)
        new_cols[col] = canonical
    
    return df.rename(columns=new_cols)
```

**Best Practices:**
- Group by format (TXT, CSV, XLSX, Historical)
- Add comments explaining each section
- Include identity mappings
- Case-insensitive matching
- Document year-specific patterns
- Log unmapped columns

**Testing:**
```python
def test_alias_map_covers_all_formats():
    """Verify alias map handles all format variations."""
    from gpci_parser import CANONICAL_ALIAS_MAP
    
    txt_headers = ['locality code', '2025 PW GPCI']
    csv_headers = ['locality_code', '2025_pw_gpci']
    xlsx_headers = ['Locality Code', '2025 PW GPCI']
    
    for header in txt_headers + csv_headers + xlsx_headers:
        assert header.lower() in {k.lower() for k in CANONICAL_ALIAS_MAP.keys()}
```

### 1.4 Defensive Type Handling Patterns

**Purpose:** Safe type conversion handling CMS data variations (integer strings, empty values, scientific notation).

**Problem:**
CMS data contains mixed numeric representations:
- Integer strings: `"1"` (not `"1.000"`)
- Empty values: `""`, `None`, `"nan"`
- Scientific notation: `"1.23E+05"`
- Invalid values: `"N/A"`, `"--"`, `"*"`

**Solution:** Use `_parser_kit.py` utilities with defensive patterns.

**Standard Implementation:**

```python
from cms_pricing.ingestion.parsers._parser_kit import canonicalize_numeric_col

# Recommended: Use parser kit (handles all cases)
df['gpci_work'] = canonicalize_numeric_col(
    df['gpci_work'],
    precision=3,
    rounding_mode='HALF_UP'
)
```

**Parser Kit Handles:**
- ✅ Integer strings: `"1"` → `"1.000"`
- ✅ Empty values: `""` → `""`
- ✅ Scientific notation: `"1.5E+02"` → `"150.000"`
- ✅ Invalid values: `"N/A"` → `""` (logged)

**Common Type Patterns:**

**Decimal (Money, RVUs, Rates):**
```python
# ✅ CORRECT
df['rvu_work'] = canonicalize_numeric_col(df['rvu_work'], precision=2)

# ❌ WRONG: Fails on empty
df['rvu_work'] = df['rvu_work'].apply(lambda x: Decimal(x))
```

**Boolean (Flags, Indicators):**
```python
# ✅ CORRECT
bool_map = {'Y': True, 'N': False, '1': True, '0': False, '': False}
df['flag'] = df['flag'].map(bool_map).fillna(False)

# ❌ WRONG: Unexpected results
df['flag'] = df['flag'].astype(bool)  # "" → False, "N" → True!
```

**Date (Effective Dates):**
```python
# ✅ CORRECT
df['effective_from'] = pd.to_datetime(df['effective_from'], format='%Y-%m-%d', errors='coerce')

# ❌ WRONG: Ambiguous dates
df['effective_from'] = pd.to_datetime(df['effective_from'])
```

### 1.5 Output Artifacts

**IO Boundary:** Parsers do NOT write files. Ingestor persists artifacts from ParseResult.

**Parser Responsibilities:**
Return `ParseResult` NamedTuple with:
- `data`: pandas DataFrame (canonical rows, metadata injected, sorted)
- `rejects`: pandas DataFrame (validation failures)
- `metrics`: Dict (parse metrics)

**Ingestor Writes (from ParseResult):**
- `parsed.parquet` - From `ParseResult.data`
- `rejects.parquet` - From `ParseResult.rejects`
- `metrics.json` - From `ParseResult.metrics`
- `provenance.json` - Full metadata

### 1.6 Row Hash Specification

**Canonical algorithm for `row_content_hash`:**

1. **Columns**: Use schema columns in declared order; exclude metadata columns
2. **Normalize each value** to canonical string:
   - `None` → `""` (empty string)
   - strings → trimmed, case preserved
   - decimals/floats → quantize to schema-defined precision, no scientific notation
   - dates → ISO-8601 `YYYY-MM-DD`
   - categorical → `.astype(str)` before hashing
   - booleans → `True` or `False`
3. **Join** with `\x1f` (ASCII unit separator)
4. **Encode** as UTF-8 bytes
5. **Hash** with SHA-256, return full 64-char hex digest

**Implementation:**
```python
def compute_row_hash(
    row: pd.Series, 
    schema_columns: List[str],
    column_precision: Dict[str, int] = None
) -> str:
    """Compute deterministic row content hash."""
    parts = []
    for col in schema_columns:
        val = row[col]
        if pd.isna(val):
            parts.append("")
        elif isinstance(val, (float, Decimal)):
            precision = column_precision.get(col, 6) if column_precision else 6
            quantizer = Decimal(10) ** -precision
            parts.append(str(Decimal(str(val)).quantize(quantizer, rounding=ROUND_HALF_UP)))
        elif isinstance(val, datetime):
            parts.append(val.strftime('%Y-%m-%dT%H:%M:%SZ'))
        elif isinstance(val, date):
            parts.append(val.isoformat())
        else:
            parts.append(str(val).strip())
    
    content = '\x1f'.join(parts)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()
```

**Production Note:** Use `_parser_kit.py::finalize_parser_output()` - don't reimplement.

---

## 2. Parser Implementation Templates

### 2.1 Standard 11-Step Parser Structure

Every parser MUST follow this structure:

```python
def parse_{dataset}(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse {dataset} file to canonical schema.
    
    Per STD-parser-contracts-prd-v2.0.md §6.1.
    """
    import time
    start_time = time.perf_counter()
    
    # Step 1: Detect encoding
    head = file_obj.read(8192)
    encoding, _ = detect_encoding(head)
    file_obj.seek(0)
    logger.info("Encoding detected", encoding=encoding)
    
    content_clean = file_obj.read()
    
    # Step 2: Parse format (router-based)
    if _is_fixed_width(content_clean):
        layout = get_layout(dataset, metadata['product_year'], metadata['quarter'])
        df = _parse_fixed_width(content_clean, encoding, layout)
    elif filename.endswith('.xlsx'):
        df = pd.read_excel(file_obj, dtype=str)
    elif filename.endswith('.zip'):
        df = _parse_zip(file_obj, filename, metadata)
    else:
        df = _parse_csv(content_clean, encoding)
    
    # Step 3: Normalize column names
    df = _normalize_column_names(df)
    
    # Step 4: Normalize string values
    df = normalize_string_columns(df)
    
    # Step 5: Cast dtypes
    df = _cast_dtypes(df)
    
    # Step 6: Load schema contract
    schema = load_schema(metadata['schema_id'])
    
    # Step 7: Categorical validation
    cat_result = enforce_categorical_dtypes(
        df, schema,
        natural_keys=schema['natural_keys'],
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id'],
        severity=ValidationSeverity.WARN
    )
    
    # Step 8: Inject metadata columns
    for col in ['release_id', 'vintage_date', 'product_year', 'quarter_vintage']:
        cat_result.valid_df[col] = metadata[col]
    cat_result.valid_df['source_filename'] = filename
    cat_result.valid_df['parsed_at'] = pd.Timestamp.utcnow()
    
    # Step 9: Finalize (hash + sort)
    final_df = finalize_parser_output(
        cat_result.valid_df,
        schema['natural_keys'],
        schema
    )
    
    # Step 10: Build metrics
    parse_duration = time.perf_counter() - start_time
    metrics = {
        **cat_result.metrics,
        'parser_version': PARSER_VERSION,
        'encoding_detected': encoding,
        'parse_duration_sec': parse_duration,
        'schema_id': metadata['schema_id']
    }
    
    # Step 11: Join invariant check
    assert metrics['total_rows'] == len(final_df) + len(cat_result.rejects_df)
    
    logger.info("Parse completed", rows=len(final_df), rejects=len(cat_result.rejects_df))
    
    return ParseResult(
        data=final_df,
        rejects=cat_result.rejects_df,
        metrics=metrics
    )
```

### 2.2 Validation Phases & Rejects Handling

**Phase Order (MUST):**

1. **Type Coercion** - Convert strings to canonical types
2. **Post-Cast Validation** - Range checks, business rules (AFTER canonicalization)
3. **Categorical Validation** - Enum/domain checks
4. **Natural Key Uniqueness** - Duplicate detection

**String vs Numeric Handling:**

**Problem:** `canonicalize_numeric_col()` returns **strings** for hash stability, but range validation needs **numbers**.

**Solution:**
```python
# ✅ CORRECT: Validate after canonicalization with numeric conversion
df['cf_value'] = canonicalize_numeric_col(df['cf_value'], precision=4)
cf_numeric = pd.to_numeric(df['cf_value'], errors='coerce')
invalid_range = (cf_numeric <= 0) | (cf_numeric > 200)
if invalid_range.any():
    # Move invalid rows to rejects
```

**Rejects Aggregation Pattern:**
```python
# Multiple validation phases produce separate rejects
range_rejects = pd.DataFrame()  # From range validation
cat_result = enforce_categorical_dtypes(...)  # Returns rejects_df

# Aggregate all rejects
all_rejects = pd.concat([
    range_rejects,
    cat_result.rejects_df
], ignore_index=True)
```

**Error Message Enrichment:**

```python
# ❌ POOR
raise ParseError("cf_value out of range")

# ✅ GOOD: Rich message with examples
raise ParseError(
    f"cf_value out of range (0, 200]: {len(bad_rows)} rows. "
    f"Examples: {bad_rows[['cf_type', 'cf_value']].head().to_dict('records')}"
)
```

### 2.3 Tiered Validation Thresholds

**Principle:** Use severity tiers (INFO/WARN/ERROR) instead of binary pass/fail.

**Standard Tier Definitions:**

| Tier | Meaning | Action | Use Case |
|------|---------|--------|----------|
| **ERROR** | Critical failure | Raise ParseError | Empty files, corrupt data |
| **WARN** | Production boundary | Log warning | Unusual but valid counts |
| **INFO** | Expected variation | Log info | Test fixtures, edge cases |
| **OK** | Normal range | No message | Production typical values |

**Implementation Example:**

```python
def validate_row_count(df: pd.DataFrame) -> Optional[str]:
    """
    Tiered validation supports test fixtures AND production.
    
    Returns None if valid, or INFO/WARN message (logged, doesn't raise)
    Raises ParseError only for ERROR-tier failures
    """
    count = len(df)
    
    # ERROR tier
    if count == 0:
        raise ParseError("CRITICAL: Row count is 0 (empty file)")
    
    # INFO tier: Test data
    if 1 <= count < 10:
        return f"INFO: Row count {count} suggests edge case fixture"
    if 10 <= count < 100:
        return f"INFO: Row count {count} suggests test fixture"
    
    # WARN tier: Production boundaries
    if count > 120:
        return f"WARN: Row count {count} > 120. Check for duplicates"
    
    # OK tier: Production normal
    return None  # 100-120 rows
```

**Usage:**
```python
# In parser
rowcount_msg = _validate_row_count(df)
if rowcount_msg:
    logger.warning(rowcount_msg)  # Log INFO/WARN messages
# Continues execution (only ERROR raises)
```

**Benefits:**
- ✅ No test-only flags needed
- ✅ Tests exercise exact production code path
- ✅ Supports edge cases, test fixtures, production

**Prohibited Patterns:**
```python
# ❌ Test-only bypass
if metadata.get('skip_validation'):
    return

# ❌ Environment check
if os.getenv('ENV') == 'test':
    return

# ❌ Binary threshold (forces bypasses)
if count < 100:
    raise ParseError()
```

### 2.4 Incremental Implementation Strategy

**Principle:** Implement one format at a time to isolate errors.

**Three-Phase Approach:**

**Phase 1: Single Format + Core Logic (3-4h)**
- Choose simplest format (TXT or CSV)
- Implement core parser structure
- Create single-format golden test
- Iterate until passing
- ✅ Checkpoint: One format working, 0 rejects

**Phase 2: Additional Formats (2-3h)**
- Pre-inspect each format (see RUN-parser-qa-runbook §1)
- Add format-specific parsing
- Create golden test per format
- Add consistency tests
- ✅ Checkpoint per format: Parses correctly, matches Phase 1

**Phase 3: Edge Cases & Negative Tests (1-2h)**
- Document known quirks
- Create edge case fixtures
- Write edge case tests (`@pytest.mark.edge_case`)
- Add negative tests (`@pytest.mark.negative`)
- ✅ Checkpoint: All error paths validated

**Benefits:** 40% faster time to first test, isolated debugging

---

## 3. Type Handling Patterns

### 3.1 Type Casting Per Schema

**Type Assignment:**
- Codes: **categorical** (HCPCS, modifier, status, locality)
- Money/RVUs: **decimal** or **float64** (explicit, not object)
- Dates: **datetime64[ns]** or **date**
- Booleans: **bool** (not 0/1 or Y/N strings)
- No silent coercion - fail on type cast errors

### 3.2 Three-Stage Casting

```python
# Stage 1: Clean strings
df = normalize_string_columns(df)  # From _parser_kit

# Stage 2: Cast to intermediate types
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

# Stage 3: Canonicalize to schema (deterministic strings)
df['amount'] = canonicalize_numeric_col(df['amount'], precision=2)
```

### 3.3 Empty Value Handling

```python
def safe_decimal_cast(series: pd.Series, precision: int) -> pd.Series:
    """Handle empty strings explicitly."""
    # Replace empty with NaN
    clean = series.replace('', None)
    
    # Cast to numeric
    numeric = pd.to_numeric(clean, errors='coerce')
    
    # Canonicalize
    return canonicalize_numeric_col(numeric, precision, 'HALF_UP')
```

### 3.4 Error Reporting

```python
def cast_with_reporting(series: pd.Series, column: str) -> pd.Series:
    """Cast with error reporting for debugging."""
    original = series.copy()
    result = pd.to_numeric(series, errors='coerce')
    
    failures = series[result.isna() & original.notna()]
    if len(failures) > 0:
        examples = failures.head(5).tolist()
        logger.warning(
            f"Type cast failures in {column}",
            count=len(failures),
            examples=examples
        )
    
    return result
```

---

## 4. Canonical Transforms

**Allowed in parsers:**
- Unit normalization (cents → dollars if schema requires)
- Enum mapping (Y/N → True/False)
- Zero-padding codes (ZIP5, FIPS, ZCTA)
- Denormalization joins ONLY if required for parse validity

**Deterministic Output:**
- Sort by composite natural key (defined in schema)
- Use stable sort algorithm
- Reset index after sorting
- Compute `row_content_hash` for each row
- Output format: Parquet with compression='snappy'

---

## 5. Anti-Patterns & Fixes

### 5.1 Type Handling Anti-Patterns

**1. No Error Handling**
```python
# ❌ WRONG: Crashes on invalid
df['amount'] = df['amount'].apply(lambda x: Decimal(x))

# ✅ CORRECT
df['amount'] = canonicalize_numeric_col(df['amount'], 2, 'HALF_UP')
```

**2. Silent NaN Propagation**
```python
# ❌ WRONG: No logging
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

# ✅ CORRECT: Log failures
original = df['amount'].copy()
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
failures = df[df['amount'].isna() & original.notna()]
if len(failures) > 0:
    logger.warning(f"Coerced {len(failures)} to NaN", examples=failures.head(3).tolist())
```

**3. Missing Empty Check**
```python
# ❌ WRONG: Empty fails
df['amount'] = df['amount'].apply(lambda x: Decimal(str(float(x))))

# ✅ CORRECT: Filter empties
df['amount'] = df['amount'].apply(lambda x: Decimal(str(float(x))) if x != '' else None)
# Better: Use canonicalize_numeric_col
```

### 5.2 Alias Map Anti-Patterns

**1. Incomplete Coverage**
```python
# ❌ WRONG: Only one format
ALIAS_MAP = {
    'locality code': 'locality_code',  # Missing CSV, XLSX!
}

# ✅ CORRECT: All formats
ALIAS_MAP = {
    'locality code': 'locality_code',      # TXT
    'locality_code': 'locality_code',      # CSV
    'Locality Code': 'locality_code',      # XLSX
}
```

**2. Case-Sensitive Matching**
```python
# ❌ WRONG
if col in ALIAS_MAP:
    col = ALIAS_MAP[col]

# ✅ CORRECT
lower_map = {k.lower(): v for k, v in ALIAS_MAP.items()}
if col.lower() in lower_map:
    col = lower_map[col.lower()]
```

**3. Silent Failures**
```python
# ❌ WRONG: Skip unmapped
canonical_cols = [ALIAS_MAP.get(col, col) for col in df.columns]

# ✅ CORRECT: Log unmapped
unmapped = [col for col in df.columns if col.lower() not in lower_map]
if unmapped:
    logger.warning(f"Unmapped columns: {unmapped}")
```

### 5.3 Validation Anti-Patterns

**1. Validate Before Canonicalization**
```python
# ❌ WRONG: Compare strings
df['cf_value'] = canonicalize_numeric_col(df['cf_value'], precision=4)
if (df['cf_value'] <= 0).any():  # FAILS: comparing strings!

# ✅ CORRECT: Convert back for validation
df['cf_value'] = canonicalize_numeric_col(df['cf_value'], precision=4)
cf_numeric = pd.to_numeric(df['cf_value'], errors='coerce')
if (cf_numeric <= 0).any():
    # Validation logic
```

**2. Test-Only Flags**
```python
# ❌ PROHIBITED
if metadata.get('skip_row_count_validation'):
    return

# ✅ REQUIRED: Tiered validation
msg = _validate_row_count(df)  # Returns INFO/WARN/None
if msg:
    logger.warning(msg)
```

---

## 6. Implementation Checklist

**Parser Function:**
-  Follows 11-step structure (§2.1)
-  Returns ParseResult(data, rejects, metrics)
-  Accepts metadata dict
-  Pure function (no filesystem writes, no global state)

**Type Handling:**
-  Uses `canonicalize_numeric_col()` for all numerics
-  Handles empty strings explicitly
-  Logs type cast failures with examples
-  Documents expected input types in docstrings

**Alias Maps:**
-  Organized by format (TXT/CSV/XLSX/Historical)
-  Includes identity mappings
-  Case-insensitive matching
-  Logs unmapped columns

**Validation:**
-  Tiered thresholds (ERROR/WARN/INFO/OK)
-  No test-only bypasses
-  Rich error messages with examples
-  Join invariant enforced

**Metadata:**
-  Injects all required metadata columns
-  Computes row_content_hash per spec
-  Sorts by natural key
-  Deterministic output

---

## 7. Quick Start Workflow

**Follow this sequence:**

1. **Read STD-parser-contracts-prd-v2.0** (understand contracts) - 15 min
2. **Run RUN-parser-qa-runbook-prd-v1.0 §1** (pre-implementation checks) - 90 min
3. **Use §2.1 above** (11-step template) to implement - 60-90 min
4. **Apply §3-5** (type handling, validation) as needed - 30 min
5. **Avoid §5 anti-patterns** - ongoing
6. **Run RUN-parser-qa-runbook-prd-v1.0 §4** (acceptance) - 20 min

**Total Time:** 3.5-4.5 hours for first working parser

---

## 8. Cross-References

**Core Standards:**
- STD-parser-contracts-prd-v2.0.md (policy)
- STD-qa-testing-prd-v1.0.md (QTS requirements)
- STD-data-architecture-prd-v1.0.md (DIS pipeline)

**Companion Documents:**
- REF-parser-routing-detection-v1.0.md (router details)
- REF-parser-quality-guardrails-v1.0.md (validation/metrics)
- RUN-parser-qa-runbook-prd-v1.0.md (QA procedures)
- REF-parser-reference-appendix-v1.0.md (reference tables)

**Utilities:**
- `cms_pricing/ingestion/parsers/_parser_kit.py` (shared utilities)

---

## 9. Source Section Mapping (v1.11 → impl v2.0)

**For reference during transition:**

This companion guide contains content from the following sections of `STD-parser-contracts-prd-v1.11-ARCHIVED.md`:

| impl v2.0 Section | Original v1.11 Section | Lines in v1.11 |
|-------------------|------------------------|----------------|
| §1.1 Inputs | §5.1 Inputs | 123-152 |
| §1.2 Encoding & Dialect | §5.2 Processing Requirements (partial) | 153-182 |
| §1.3 Alias Map Best Practices | §5.2.3 Alias Map Best Practices | 183-486 |
| §1.4 Defensive Type Handling | §5.2.4 Defensive Type Handling Patterns | 494-726 |
| §1.5 Output Artifacts | §5.3 Output Artifacts | 813-851 |
| §1.6 Row Hash Specification | §5.2 Row Hash Spec v1.1 | 727-812 |
| §2.1 Standard Parser Structure | §21.1 Standard Parser Structure | 3591-3704 |
| §2.2 Validation Phases | §21.2 Validation Phases & Rejects | 3705-3797 |
| §2.3 Tiered Validation | §21.3 Tiered Validation Thresholds | 3840-3970 |
| §2.4 Incremental Strategy | §21.6 Incremental Implementation | 4235-4267 |
| §3-4 Type Handling | §5.2 Processing Requirements (partial) | 487-726 |
| §5 Anti-Patterns | §20.1 Anti-Patterns (type/validation subset) | 3252-3570 (selected) |

**Sections NOT in this document (see other companions):**
- §7 Router & Layout → REF-parser-routing-detection-v1.0.md
- §8-10 Validation/Errors/Metrics → REF-parser-quality-guardrails-v1.0.md
- §21.4-21.5, §21.7 QA Workflows → RUN-parser-qa-runbook-prd-v1.0.md
- Appendices → REF-parser-reference-appendix-v1.0.md

**Archived source:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md`

**Cross-Reference:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 10. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| **2025-10-17** | **v2.0** | **Team** | **Initial implementation companion guide.** Split from STD-parser-contracts-prd-v1.11. Contains: input processing (§1), 11-step parser template (§2.1), validation phases (§2.2), tiered validation (§2.3), incremental strategy (§2.4), type handling patterns (§3), canonical transforms (§4), anti-patterns (§5). Total: ~1,200 lines of implementation guidance. Cross-references: STD v2.0 (policy), REF routing (format detection), REF quality (validation), RUN runbook (QA). |

---

*End of Implementation Companion*

*For policy and contracts, see STD-parser-contracts-prd-v2.0.md*  
*For operational procedures, see RUN-parser-qa-runbook-prd-v1.0.md*

