# Parser Quality Guardrails Reference

**Purpose:** Validation patterns, error taxonomy, and metrics/observability implementation  
**Audience:** Engineers implementing validation logic and quality checks  
**Status:** Draft v1.0  
**Owners:** Data Platform Engineering, QA Guild  
**Consumers:** Parser developers, QA engineers  
**Change control:** PR review  

**Cross-References:**
- **STD-parser-contracts-prd-v2.0.md:** Core contracts (§6)
- **STD-parser-contracts-impl-v2.0.md:** Implementation patterns (§2.2-2.3)
- **REF-parser-routing-detection-v1.0.md:** Router and layout architecture
- **RUN-parser-qa-runbook-prd-v1.0.md:** QA procedures and SLAs
- **STD-qa-testing-prd-v1.0.md:** QTS requirements

---

## 1. Overview

This reference provides detailed patterns for:
- **Validation requirements** - Schema, reference, categorical validation
- **Error taxonomy** - Exception hierarchy and error codes
- **Observability & metrics** - Per-file and aggregate metrics
- **Safe calculation patterns** - Handling empty values and edge cases

**Use this for:**
- Implementing validation logic in parsers
- Defining error handling patterns
- Calculating metrics safely
- Understanding quarantine artifact formats

**Don't use this for:**
- Core contracts (see STD-parser-contracts-prd-v2.0)
- Implementation templates (see STD-parser-contracts-impl-v2.0)
- Router logic (see REF-parser-routing-detection-v1.0)
- QA procedures (see RUN-parser-qa-runbook-prd-v1.0)

---

## 2. Validation Requirements

### 2.1 Schema Validation

**Timing:** After parsing, before enrichment

**Rules:**
- Required fields present
- Field types match schema
- Domain values valid (if specified)
- Range constraints met

**Action:** BLOCK on schema validation failures

**Implementation:**
```python
# After parsing
schema = load_schema(metadata['schema_id'])

# Verify columns
required_cols = set(schema['columns'].keys())
actual_cols = set(df.columns)
missing = required_cols - actual_cols

if missing:
    raise SchemaRegressionError(
        f"Missing required columns: {missing}"
    )

# Verify types
for col, spec in schema['columns'].items():
    if spec['type'] == 'decimal' and df[col].dtype != 'object':
        logger.warning(f"Column {col} not decimal type")
```

### 2.2 Reference Validation Hooks

**CMS-Specific References:**
- **HCPCS/CPT**: Validate against CMS code reference set
- **Locality codes**: Validate against MAC/locality crosswalk
- **FIPS codes**: Validate 5-digit format and existence
- **State codes**: Validate 2-letter format

**Phase 1 (Minimal):**
- Format validation only (regex patterns)
- Log unknown codes, create quarantine artifact
- Don't block parsing

**Phase 2 (Comprehensive):**
- Full reference table lookups
- Effective date range validation
- Cross-dataset consistency checks

**Implementation:**
```python
# Phase 1: Format validation
hcpcs_invalid = ~df['hcpcs'].str.match(r'^[A-Z0-9]{5}$')
if hcpcs_invalid.any():
    invalid_rows = df[hcpcs_invalid].copy()
    invalid_rows['validation_error'] = 'Invalid HCPCS format'
    invalid_rows['validation_severity'] = 'WARN'
    rejects = pd.concat([rejects, invalid_rows])
    df = df[~hcpcs_invalid]

# Phase 2: Reference lookup (future)
from reference import load_hcpcs_codes
valid_codes = load_hcpcs_codes(vintage=metadata['vintage_date'])
unknown = ~df['hcpcs'].isin(valid_codes)
if unknown.any():
    # Quarantine unknown codes
```

### 2.3 Categorical Domain Validation

**Problem:** `CategoricalDtype(categories=[...])` silently converts unknown values to NaN.

**Solution:** Pre-check domain before categorical conversion.

**Implementation:**
```python
from cms_pricing.ingestion.parsers._parser_kit import (
    enforce_categorical_dtypes,
    ValidationSeverity,
    ValidationResult
)

# Usage
result: ValidationResult = enforce_categorical_dtypes(
    df=df,
    schema_contract=schema,
    natural_keys=['hcpcs', 'modifier'],
    schema_id='cms_pprrvu_v1.0',
    release_id='mpfs_2025_q1',
    severity=ValidationSeverity.WARN
)

# Returns:
# - valid_df: Valid rows with categorical dtypes
# - rejects_df: Invalid rows with error details
# - metrics: Validation metrics
```

**No Silent NaN Coercion:**
- Invalid values MUST be rejected explicitly
- Never silently convert to NaN
- Severity: WARN (quarantine + continue) or BLOCK (fail) per business rule
- Audit trail in rejects DataFrame

### 2.4 Validation Tiers

**BLOCK (Critical Errors):**
- Invalid file format
- Missing required fields
- Type cast failures on required fields
- Schema contract violations
- Corrupt/unreadable file

**Action:** Raise exception, stop processing

**WARN (Soft Failures):**
- Unknown reference codes
- Out-of-range values
- Missing optional fields
- Data quality issues

**Action:** Create quarantine artifact, log warning, continue

**INFO (Statistical Anomalies):**
- Row count drift from prior version
- Distribution changes
- New code appearances

**Action:** Log only, include in metrics

### 2.5 Quarantine Artifact Format

**File:** `data/quarantine/{release_id}/{dataset}_{reason}.parquet`

**Schema:**
```python
{
    # Original columns (all)
    ...
    
    # Quarantine metadata
    'quarantine_reason': str,
    'quarantine_rule_id': str,
    'quarantined_at': datetime,
    'source_line_num': int,
    'error_details': str,
}
```

**Created for:**
- Invalid reference codes
- Format violations (WARN level)
- Schema mismatches (if not blocking)

### 2.6 Error Code Severity Table

**Canonical Error Codes:**

| Error Code | Default Severity | Parser Action | Artifact | Notes |
|------------|------------------|---------------|----------|-------|
| `ENCODING_ERROR` | **BLOCK** | Raise `UnicodeDecodeError` | — | Cannot decode file |
| `DIALECT_UNDETECTED` | **BLOCK** | Raise `ParseError` | — | CSV delimiter ambiguity |
| `LAYOUT_MISMATCH` | **BLOCK** | Raise `LayoutMismatchError` | — | Cannot continue |
| `FIELD_MISSING` | **BLOCK** | Raise `SchemaRegressionError` | — | Required column absent |
| `TYPE_CAST_ERROR` | **BLOCK** | Raise `ValueError` | — | Cannot cast type |
| `KEY_VIOLATION` | **BLOCK** | Raise `DuplicateKeyError` | — | Primary key constraint |
| `ROW_DUPLICATE` | **Varies** | Raise or quarantine | `rejects.parquet` | Dataset-specific |
| `CATEGORY_UNKNOWN` | **WARN** | Quarantine | `rejects.parquet` | Unknown categorical |
| `REFERENCE_MISS` | **WARN** | Quarantine | `rejects.parquet` | Code not in ref set |
| `OUT_OF_RANGE` | **WARN** | Quarantine | `rejects.parquet` | Value outside bounds |
| `BOM_DETECTED` | **INFO** | Log only | — | BOM stripped |
| `ENCODING_FALLBACK` | **INFO** | Log only | — | Used CP1252/Latin-1 |

**Per-Dataset Natural Key Policies:**

| Dataset | Uniqueness Severity | Rationale |
|---------|---------------------|-----------|
| PPRRVU | **BLOCK** | Critical reference; duplicates = CMS error |
| Conversion Factor | **BLOCK** | Single value per (type, date) |
| ANES CF | **BLOCK** | Single value per (locality, date) |
| GPCI | **WARN** | May overlap during transitions |
| Locality | **WARN** | County-locality maps may overlap |
| OPPSCAP | **WARN** | May have multiple modifiers |

---

## 3. Error Taxonomy

### 3.1 Exception Hierarchy

**Base Exception:**
```python
class ParseError(Exception):
    """Base exception for all parser errors."""
    pass
```

**Specific Exceptions:**

**1. DuplicateKeyError**
```python
class DuplicateKeyError(ParseError):
    def __init__(self, message: str, duplicates: Optional[List[Dict]] = None):
        super().__init__(message)
        self.duplicates = duplicates  # Duplicate key combinations
```
- **Usage:** Natural key violations
- **When:** `check_natural_key_uniqueness()` with BLOCK severity
- **Contains:** List of duplicate key combinations

**2. CategoryValidationError**
```python
class CategoryValidationError(ParseError):
    def __init__(self, field: str, invalid_values: List[Any]):
        self.field = field
        self.invalid_values = invalid_values
```
- **Usage:** Invalid categorical values
- **When:** Unknown values before domain casting
- **Contains:** Field name and invalid values

**3. LayoutMismatchError**
```python
class LayoutMismatchError(ParseError):
    pass
```
- **Usage:** Fixed-width parsing failures
- **When:** Wrong column widths, missing layout, truncated lines

**4. SchemaRegressionError**
```python
class SchemaRegressionError(ParseError):
    def __init__(self, message: str, unexpected_fields: Optional[List[str]] = None):
        self.unexpected_fields = unexpected_fields
```
- **Usage:** Unexpected schema fields
- **When:** DataFrame has fields not in schema contract

**When to Raise vs Return in Rejects:**
- `DuplicateKeyError`: Raise if BLOCK, return in rejects if WARN
- `CategoryValidationError`: Return in rejects (soft failure)
- `LayoutMismatchError`: Always raise (critical)
- `SchemaRegressionError`: Always raise (contract violation)

**Location:** `cms_pricing/ingestion/parsers/_parser_kit.py`

### 3.2 Error Codes

- `ENCODING_ERROR` - Cannot decode file
- `DIALECT_UNDETECTED` - Cannot detect CSV dialect
- `HEADER_MISSING` - No header row found
- `FIELD_MISSING` - Required field not in data
- `TYPE_CAST_ERROR` - Cannot cast to required type
- `OUT_OF_RANGE` - Value exceeds constraints
- `REFERENCE_MISS` - Code not in reference set
- `ROW_DUPLICATE` - Duplicate natural key
- `KEY_VIOLATION` - Primary key constraint
- `LAYOUT_MISMATCH` - Fixed-width layout mismatch
- `BOM_DETECTED` - BOM found and stripped (info)
- `PARSER_INTERNAL` - Internal parser error

**Reject Record Schema:**
```json
{
  "line_no": 42,
  "raw_row": "original line content",
  "error_code": "REFERENCE_MISS",
  "error_message": "HCPCS code 99999 not in reference set",
  "context": {
    "hcpcs": "99999",
    "reference_version": "2025Q4"
  },
  "sha256": "hash of raw_row"
}
```

---

## 4. Observability & Metrics

### 4.1 Per-File Metrics Structure

**Metrics emitted for each file:**

```json
{
  "filename": "PPRRVU2025_Oct.txt",
  "dataset": "pprrvu",
  "schema_id": "cms_pprrvu_v1.0",
  "parser_version": "v1.0.0",
  "layout_version": "v2025.4.0",
  "encoding_detected": "utf-8",
  "bom_detected": false,
  "dialect_detected": "fixed-width",
  "rows_in": 19453,
  "rows_out": 19450,
  "rejects_count": 3,
  "reject_rate": 0.0001,
  "parse_seconds": 4.23,
  "checksum_match": true,
  "schema_validation_passed": true
}
```

### 4.2 Safe Metrics Calculation Patterns

**Principle:** Metrics calculations must handle empty values, nulls, edge cases gracefully.

#### Safe Min/Max Calculation

```python
def safe_min_max(df: pd.DataFrame, column: str, expected_range: tuple = None) -> Dict[str, Optional[float]]:
    """
    Calculate min/max with safe handling.
    
    Returns {'min': float or None, 'max': float or None}
    """
    # Filter empty strings and nulls
    valid = df[df[column] != ''][column]
    valid = valid[valid.notna()]
    
    if len(valid) == 0:
        return {'min': None, 'max': None}
    
    try:
        numeric = pd.to_numeric(valid, errors='coerce')
        numeric = numeric.dropna()
        
        if len(numeric) == 0:
            return {'min': None, 'max': None}
        
        min_val = float(numeric.min())
        max_val = float(numeric.max())
        
        # Validate against expected range
        if expected_range:
            expected_min, expected_max = expected_range
            if min_val < expected_min or max_val > expected_max:
                logger.warning(
                    f"Metric out of range for {column}",
                    min=min_val, max=max_val,
                    expected=expected_range
                )
        
        return {'min': min_val, 'max': max_val}
        
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to calculate min/max for {column}: {e}")
        return {'min': None, 'max': None}
```

#### Safe Count Calculation

```python
def safe_count(df: pd.DataFrame, column: str, value: Any) -> int:
    """Count occurrences safely."""
    if value == '' or value is None:
        return len(df[df[column].isna() | (df[column] == '')])
    else:
        return len(df[df[column] == value])
```

#### Safe Percentage Calculation

```python
def safe_percentage(numerator: int, denominator: int, decimals: int = 2) -> Optional[float]:
    """Calculate percentage with division-by-zero protection."""
    if denominator == 0:
        return None
    
    percentage = (numerator / denominator) * 100
    return round(percentage, decimals)
```

### 4.3 Comprehensive Metrics Pattern

**Standard metrics structure:**

```python
# Core counts
metrics = {
    'total_rows': len(df),
    'valid_rows': len(final_df),
    'reject_rows': len(rejects_df),
    'parse_duration_sec': round(elapsed, 3),
}

# Encoding info
metrics.update({
    'encoding_detected': encoding,
    'encoding_fallback': encoding != 'utf-8',
})

# Column-specific stats (safe aggregation)
for col in numeric_columns:
    stats = safe_min_max(final_df, col, EXPECTED_RANGES.get(col))
    metrics[f'{col}_min'] = stats['min']
    metrics[f'{col}_max'] = stats['max']

# Null rates
for col in required_columns:
    null_count = safe_count(df, col, None)
    metrics[f'{col}_null_count'] = null_count
    metrics[f'{col}_null_rate_pct'] = safe_percentage(null_count, len(df))

# Categorical distributions
for col in categorical_columns:
    metrics[f'{col}_top5'] = df[col].value_counts().head(5).to_dict()
```

### 4.4 Aggregate Metrics (Per Run)

Tracked in IngestRun model:
- `files_discovered`, `files_parsed`, `files_failed`
- `rows_discovered`, `rows_ingested`, `rows_rejected`, `rows_quarantined`
- `bytes_processed`
- `schema_drift_detected`
- `validation_errors`, `validation_warnings`
- `parse_duration_total_sec`

### 4.5 Logging Requirements

**Structured logging with structlog:**

```python
import structlog

logger = structlog.get_logger(__name__)

# Log with context
logger.info(
    "Parsing started",
    filename=filename,
    dataset=metadata['dataset_id'],
    release_id=metadata['release_id'],
    schema_id=metadata['schema_id']
)

# Log metrics
logger.info(
    "Parse completed",
    rows=len(final_df),
    rejects=len(rejects_df),
    duration_sec=parse_duration,
    encoding=encoding
)
```

**Required log fields:**
- `release_id`, `schema_id`, `parser_version`
- `filename`, `encoding_detected`
- `rows`, `rejects`, `duration_sec`

---

## 5. Quality Anti-Patterns

### 5.1 Missing Validation Tiers

```python
# ❌ ANTI-PATTERN: Binary validation
if len(df) < 100:
    raise ParseError("Too few rows")  # Fails all test fixtures!

# ✅ CORRECT: Tiered validation
msg = _validate_row_count(df)  # Returns INFO/WARN/None
if msg:
    logger.warning(msg)  # Log but don't raise for INFO/WARN
```

### 5.2 Hardcoded Thresholds

```python
# ❌ ANTI-PATTERN: Magic numbers
if gpci_value > 2.0:
    raise ParseError("Value too high")

# ✅ CORRECT: Named constants with tiers
GPCI_WORK_RANGE = (0.5, 2.0)
ERROR_THRESHOLD = 10.0  # Impossible value

if gpci_value > ERROR_THRESHOLD:
    raise ParseError(f"Value {gpci_value} > {ERROR_THRESHOLD}")
elif gpci_value > GPCI_WORK_RANGE[1]:
    logger.warning(f"Value {gpci_value} above typical range")
```

### 5.3 Unsafe Metrics Calculation

```python
# ❌ ANTI-PATTERN: No empty handling
metrics['min_value'] = df['amount'].min()  # Fails on empty strings

# ✅ CORRECT: Safe aggregation
metrics.update(safe_min_max(df, 'amount', expected_range=(0, 1000)))
```

### 5.4 Silent NaN Coercion

```python
# ❌ ANTI-PATTERN: Silent conversion
df['status'] = df['status'].astype('category')  # Unknown → NaN silently

# ✅ CORRECT: Pre-check domain
result = enforce_categorical_dtypes(df, schema, severity=ValidationSeverity.WARN)
df = result.valid_df  # Only valid categorical values
rejects = result.rejects_df  # Unknown values with error details
```

### 5.5 No Error Context

```python
# ❌ ANTI-PATTERN: Generic error
raise ParseError("Validation failed")

# ✅ CORRECT: Rich error with examples
raise ParseError(
    f"Natural key duplicates: {len(dupes)} rows. "
    f"Example: {dupes.iloc[0].to_dict()}"
)
```

---

## 6. Validation Implementation Checklist

-  Schema validation after parsing
-  Reference validation hooks for CMS codes
-  Categorical domain pre-check (no silent NaN)
-  Tiered validation (ERROR/WARN/INFO)
-  Safe metrics calculation (handle empties)
-  Rich error messages with examples
-  Quarantine artifacts for WARN-level failures
-  Structured logging with context
-  Join invariant: total_rows = valid + rejects

---

## 7. Cross-References

**Core Standards:**
- STD-parser-contracts-prd-v2.0.md (contracts)
- STD-qa-testing-prd-v1.0.md (QTS requirements)

**Companion Documents:**
- STD-parser-contracts-impl-v2.0.md (validation phases §2.2)
- REF-parser-routing-detection-v1.0.md (routing)
- RUN-parser-qa-runbook-prd-v1.0.md (QA procedures)

**Tools:**
- `cms_pricing/ingestion/parsers/_parser_kit.py` (shared utilities)

---

## 8. Source Section Mapping (v1.11 → quality v1.0)

**For reference during transition:**

This reference contains content from the following sections of `STD-parser-contracts-prd-v1.11-ARCHIVED.md`:

| quality v1.0 Section | Original v1.11 Section | Lines in v1.11 |
|----------------------|------------------------|----------------|
| §2.1 Schema Validation | §8.1 Schema Validation | 1888-1899 |
| §2.2 Reference Validation Hooks | §8.2 Reference Validation Hooks | 1900-1917 |
| §2.3 Categorical Domain Validation | §8.2.1 Categorical Domain Validation | 1918-1955 |
| §2.4 Validation Tiers | §8.3 Validation Tiers | 1956-1981 |
| §2.5 Quarantine Artifact Format | §8.4 Quarantine Artifact Format | 1982-2005 |
| §2.6 Error Code Severity Table | §8.5 Error Code Severity Table | 2006-2079 |
| §3.1 Exception Hierarchy | §9.1 Exception Hierarchy | 2083-2144 |
| §3.2 Error Codes | §9.2 Error Codes | 2145-2178 |
| §4.1 Per-File Metrics | §10.1 Per-File Metrics | 2182-2188 |
| §4.2 Safe Metrics Calculation | §10.1.1 + §10.3 Safe Metrics Calculation | 2189-2686 |
| §4.3 Comprehensive Metrics Pattern | §10.1 Per-File Metrics (detailed) | 2350-2375 |
| §4.4 Aggregate Metrics | §10.2 Aggregate Metrics | 2376-2386 |
| §4.5 Logging Requirements | §10.4 Logging Requirements | 2687-2715 |
| §5 Quality Anti-Patterns | §20.1 Anti-Patterns (validation/metrics subset) | 3252-3570 (selected) |

**Sections NOT in this document (see other companions):**
- §6 Contracts → STD-parser-contracts-prd-v2.0.md
- §7 Router/Layout → REF-parser-routing-detection-v1.0.md
- §21 Templates/QA → STD-parser-contracts-impl-v2.0.md + RUN-parser-qa-runbook-prd-v1.0.md

**Archived source:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md`

**Cross-Reference:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 9. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| **2025-10-17** | **v1.0** | **Team** | **Initial quality guardrails reference.** Split from STD-parser-contracts-prd-v1.11 §8-10. Contains: validation requirements (§2), schema validation, reference hooks, categorical validation, validation tiers, quarantine format, error code severity table, exception hierarchy (§3), error codes, observability & metrics (§4), safe metrics calculation patterns (safe_min_max, safe_count, safe_percentage), aggregate metrics, logging requirements, quality anti-patterns (§5). Total: ~900 lines of quality patterns. **Cross-References:** STD v2.0 (contracts), impl (validation phases), routing (architecture), runbook (QA). |

---

*End of Quality Guardrails Reference*

*For core contracts, see STD-parser-contracts-prd-v2.0.md*  
*For implementation templates, see STD-parser-contracts-impl-v2.0.md*

