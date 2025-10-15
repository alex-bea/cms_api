# Parser Contracts Standard

**Status:** Draft v1.1  
**Owners:** Data Platform Engineering  
**Consumers:** Data Engineering, MPFS Ingestor, RVU Ingestor, OPPS Ingestor, API Teams, QA Guild  
**Change control:** ADR + PR review  
**Review cadence:** Quarterly (first business Monday)

**Cross-References:**
- **STD-data-architecture-prd-v1.0.md:** DIS pipeline and normalize stage requirements
- **STD-data-architecture-impl-v1.0.md:** BaseDISIngestor implementation patterns
- **PRD-mpfs-prd-v1.0.md:** MPFS ingestor uses parser contracts for RVU bundle
- **PRD-rvu-gpci-prd-v0.1.md:** RVU parsing requirements and fixed-width layout specifications
- **PRD-opps-prd-v1.0.md:** OPPS ingestor uses parser contracts
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 1. Summary

Parsers return canonical pandas DataFrames; the ingestor writes Arrow/Parquet artifacts with deterministic outputs and full provenance. The system converts heterogeneous CMS source files (CSV/TSV/TXT/XLSX/ZIP) into schema-validated tabular datasets. Parsers are selected by a router using filename patterns and optional content sniffing. The normalize stage writes DataFrames to Arrow/Parquet format.

**Why this matters**: Parsing quality is the single largest driver of downstream reliability and cost. Standardizing parser behavior, observability, and contracts prevents silent data drift, enables reproducible releases, and accelerates onboarding of new ingestors.

**v1.1 Implementation:**
- **ParseResult return type** - Structured output (data, rejects, metrics) for cleaner separation
- **Content sniffing** - Router accepts filename + file_head (first 8KB) for magic byte detection
- **64-char SHA-256 hash** - Full digest for collision avoidance and schema compliance
- **Schema-driven precision** - Per-column decimal places for numeric hash stability
- **CP1252 encoding support** - Windows encoding in detection cascade
- **Explicit categorical validation** - Domain pre-check before conversion (no silent NaN)
- **Pinned natural keys** - Exact sort columns per dataset including effective_from
- Metadata injected via dict parameter
- Python dict layouts (YAML migration planned for v2.0)

**Key principles:**
- Parsers are **public contracts** (importable across ingestors), not private methods
- **Metadata injection** by ingestor, not hardcoded in parsers
- **Explicit dtypes** - no silent coercion of codes to floats
- **Deterministic output** - sorted by natural key + formal content hash spec
- **Tiered validation** - block on critical, warn on soft failures
- **Comprehensive provenance** - full audit trail via IngestRun model

---

## 2. Goals & Non-Goals

### Goals

- Deterministic, idempotent parsing across environments for each release
- Strong typing via Arrow/pandas with explicit dtype mapping; no implicit coercion
- Pluggable selection via content sniffing and layout registry (SemVer by year/quarter)
- Strict schema contracts with backward/forward compatibility rules
- Comprehensive error taxonomy, quarantine artifacts, and actionable metrics
- Built-in reference checks for domain codes (HCPCS/CPT/locality/FIPS) when applicable
- Provenance capture: `release_id`, `dataset_id`, `schema_id`, `parser_version`, `source_uri`, `sha256`, timestamps
- **CMS-specific**: Support for fixed-width TXT, quarterly releases, three vintage fields

### Non-Goals

- Fetching/scraping (handled by scrapers per `STD-scraper-prd-v1.0.md`)
- API serving (handled by API layer per `STD-api-architecture-prd-v1.0.md`)
- Complex business transforms beyond normalization
- Price calculations (handled downstream)

---

## 3. Users & Scenarios

### Primary Users

- **Ingestion Engineers**: Implement dataset-specific parsers (MPFS, RVU, OPPS) with minimal boilerplate
- **Data QA**: Review rejects/quarantine artifacts, metrics, Great Expectations results
- **Downstream Consumers**: Rely on stable canonical schemas and deterministic partitions
- **Platform Engineers**: Maintain parser infrastructure, layout registry, schema contracts

### Scenarios

**Scenario 1: MPFS Quarterly Release**
- Engineer implements MPFS ingestor
- Reuses shared PPRRVU/GPCI/ANES parsers
- Parsers inject three vintage fields
- Schema validation blocks on drift
- Output is deterministic across runs

**Scenario 2: Layout Change Detection**
- CMS changes PPRRVU column widths in 2026Q1
- Layout registry test detects breaking change
- CI blocks merge until new layout version added
- Prevents silent parsing failures

**Scenario 3: Invalid HCPCS Codes**
- Parser detects 50 invalid HCPCS codes
- Creates quarantine artifact with samples
- Logs warning but continues parsing
- QA reviews quarantine, determines if acceptable

---

## 4. Key Decisions & Rationale

1. **ParseResult return type** → Parsers return pandas DataFrames; ingestor writes Arrow/Parquet artifacts on disk for performance, zero-copy interchange, and strict dtype preservation
2. **Deterministic outputs** → Sort by stable composite key + 64-char `row_content_hash` for idempotency and collision avoidance
3. **Content sniffing router** → filename + file_head (magic bytes, BOM) for robust format detection
4. **External layout registry** (SemVer by year/quarter) → Decouple code from layout drift
5. **Router returns parser_func** → Direct callable for clarity; no secondary lookup
6. **Metadata injection by ingestor** → Pure function parsers, testable, no global state
7. **Quarantine-first error policy** → Never drop bad rows silently; structured error codes
8. **Reference validation hooks** → Catch domain drift early (HCPCS/CPT/locality)
9. **Explicit encoding/BOM handling** → Reduce silent corruption; log fallbacks
10. **Three vintage fields** → `vintage_date`, `product_year`, `quarter_vintage` per DIS standards
11. **Tiered validation** → BLOCK on critical, WARN on soft, INFO on statistical
12. **Public contracts** → Parsers as shared utilities, SemVer'd, tested independently

---

## 5. Scope & Requirements

### 5.1 Inputs

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

### 5.2 Processing Requirements

**Dialect & Encoding Detection:**

**Encoding Priority Cascade:**
1. **BOM detection** (highest priority):
   - UTF-8-sig (0xEF 0xBB 0xBF)
   - UTF-16 LE (0xFF 0xFE)
   - UTF-16 BE (0xFE 0xFF)
2. **UTF-8 strict decode** (no errors)
3. **CP1252** (Windows default, common for CMS files with smart quotes, em-dashes)
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

**Type Casting Per Schema:**
- Codes as **categorical** (HCPCS, modifier, status, locality)
- Money/RVUs as **decimal** or **float64** (explicit, not object)
- Dates as **datetime64[ns]** or **date**
- Booleans as **bool** (not 0/1 or Y/N strings)
- No silent coercion - fail on type cast errors

**Canonical Transforms (allowed in parsers):**
- Unit normalization (cents → dollars if schema requires)
- Enum mapping (Y/N → True/False)
- Zero-padding codes (ZIP5, FIPS, ZCTA)
- Denormalization joins ONLY if required for parse validity

**Deterministic Output:**
- Sort by composite natural key (defined in schema contract)
- Use stable sort algorithm
- Reset index after sorting
- Compute `row_content_hash` for each row (excludes metadata columns)
- Output format: Parquet with compression='snappy'

**Row Hash Specification (v1.1):**

Canonical algorithm for `row_content_hash` to ensure reproducibility:

1. **Columns**: Use schema columns in declared order; exclude metadata columns (those starting with `source_`, `release_`, `vintage_`, `product_`, `quarter_`, `parsed_`, `row_`)
2. **Normalize each value** to canonical string:
   - `None` → `""` (empty string)
   - strings → trimmed, exact case preserved
   - decimals/floats → quantize to **schema-defined precision** (read from schema contract `precision` field per column; defaults to 6 decimals if not specified), no scientific notation
   - dates → ISO-8601 `YYYY-MM-DD`
   - datetimes → ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ`
   - categorical → `.astype(str)` before hashing (avoid category code drift)
   - booleans → `True` or `False`
3. **Join** with `\x1f` (ASCII unit separator character)
4. **Encode** as UTF-8 bytes
5. **Hash** with SHA-256, return **full 64-character hex digest** (collision avoidance, schema compliance)

**Version stability:** Column order, delimiter, and precision are part of the spec. Any change requires **MAJOR** parser version bump.

**Implementation example:**
```python
def compute_row_hash(
    row: pd.Series, 
    schema_columns: List[str],
    column_precision: Dict[str, int] = None
) -> str:
    """
    Compute deterministic row content hash per spec v1.1
    
    Args:
        row: DataFrame row
        schema_columns: Columns in schema-declared order
        column_precision: Per-column decimal places (e.g. {'work_rvu': 2, 'cf_value': 4})
                         Read from schema contract; defaults to 6 if not specified
    
    Returns:
        64-character SHA-256 hex digest
    """
    parts = []
    for col in schema_columns:  # Declared order from schema
        val = row[col]
        if pd.isna(val):
            parts.append("")
        elif isinstance(val, (float, Decimal)):
            # Schema-driven precision for hash stability
            precision = column_precision.get(col, 6) if column_precision else 6
            parts.append(f"{float(val):.{precision}f}")
        elif isinstance(val, datetime):
            parts.append(val.strftime('%Y-%m-%dT%H:%M:%SZ'))
        elif isinstance(val, date):
            parts.append(val.isoformat())
        elif hasattr(val, 'categories'):  # Categorical
            parts.append(str(val).strip())
        else:
            parts.append(str(val).strip())
    
    content = '\x1f'.join(parts)
    # Return FULL 64-char digest (not truncated)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()
```

**Categorical Type Handling:**
- Use categorical dtype for memory efficiency and domain enforcement
- Always hash on `.astype(str)` to avoid category code drift between pandas versions
- Fix dictionary encoding at Parquet write time: `df.to_parquet(..., use_dictionary=True)`
- Never rely on category internal codes for equality comparisons

### 5.3 Output Artifacts

**IO Boundary:** The parser does not write files. The ingestor persists `parsed.parquet`, `rejects.parquet`, `metrics.json`, and `provenance.json` based on the parser's `ParseResult`.

**Parser Responsibilities (v1.1):**

Parsers **prepare data in-memory** and return `ParseResult` NamedTuple with:
- `data`: pandas DataFrame with canonical data columns, metadata columns injected, row content hash computed, sorted by natural key
- `rejects`: pandas DataFrame with rows that failed validation (empty if all valid)
- `metrics`: Dict with parse metrics (total_rows, valid_rows, reject_rows, parse_duration_sec, encoding_detected, etc.)

**Parsers do NOT write files** - all artifact writes handled by normalize stage/ingestor.

**Ingestor/Normalize Stage Writes (from ParseResult):**

**Primary Output:**
- `parsed.parquet` - Canonical data from `ParseResult.data` DataFrame
- Partitioned by: `dataset_id/release_id/schema_id/`
- Format: Arrow/Parquet with explicit schema, compression='snappy'

**Quarantine Output:**
- `rejects.parquet` - Rows from `ParseResult.rejects` DataFrame
- Columns: All original columns + `validation_error`, `validation_severity`, `validation_rule_id`, `source_line_num`
- Created for any rejected rows (domain violations, schema mismatches, etc.)
- Location: `data/quarantine/{release_id}/{dataset}_{reason}.parquet`

**Metrics Output:**
- `metrics.json` - Aggregated from `ParseResult.metrics` across all files
- Location: `data/stage/{dataset}/{release_id}/metrics.json`
- Includes: total_rows, valid_rows, reject_rows, parse_duration_sec, encoding_detected, encoding_fallback, validation_summary

**Provenance Output:**
- `provenance.json` - Full metadata, library versions, git SHA
- Location: `data/stage/{dataset}/{release_id}/provenance.json`

**Optional:**
- `sample_100.parquet` - First 100 rows from `ParseResult.data` for quick inspection

### 5.4 CMS File Type Support

**Fixed-Width TXT:**
- Uses layout registry keyed by (dataset, year, quarter)
- Column positions defined in layout specification
- Layout SemVer: breaking change = major version bump
- Examples: PPRRVU, GPCI, ANES, LOCCO, OPPSCAP

**CSV with Header Variations:**
- Case-insensitive header matching
- Handles multiple column name aliases
- Tab-delimited support
- Quoted fields support

**Excel Workbooks:**
- Single sheet (default: first sheet)
- Multiple sheets (specified by name)
- Header row detection
- Skip rows support

**ZIP Archives:**
- Auto-extracts all parseable files
- Routes each file to appropriate parser
- Preserves directory structure in metadata

### 5.5 Constraints & SLAs

**Performance:**
- Target: ≥ 5M rows/hour on 4 vCPU
- Memory: ≤ 2GB per 5M rows
- p95 parse time: ≤ 5 min for 2M rows
- Cold start: ≤ 10 seconds

**Correctness:**
- **Correctness over speed** - Never sacrifice accuracy for performance
- Zero silent drops - All failures logged and quarantined
- Deterministic - Same input → same output (hash-verified)

**Alerts:**
- Reject rate threshold: Configurable per dataset (default: >5% triggers alert)
- Parse failure: Immediate alert
- Schema drift: Warning alert

---

## 6. Contracts

### 6.1 Function Contract (Python)

**Parser Function Signature (v1.1):**

```python
from typing import IO, Dict, Any, NamedTuple, Callable
import pandas as pd

class ParseResult(NamedTuple):
    """Parser output per STD-parser-contracts v1.1 §5.3"""
    data: pd.DataFrame           # canonical rows (pandas)
    rejects: pd.DataFrame        # quarantine rows with error_code/context
    metrics: Dict[str, Any]      # per-file metrics (rows_in/out, encoding, etc.)

# Pure function requirement
ParseFn = Callable[[IO[bytes], str, Dict[str, Any]], ParseResult]

# Example signature for dataset-specific parser
def parse_{dataset}(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse {dataset} file to canonical schema.
    
    Pure function: no filesystem writes, no global state.
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required metadata from ingestor (see §6.4)
            - release_id, product_year, quarter_vintage, schema_id, 
            - layout_version (fixed-width only), file_sha256, etc.
        
    Returns:
        ParseResult with:
            - data: pandas DataFrame (canonical rows with metadata)
            - rejects: pandas DataFrame (validation failures)
            - metrics: Dict (total_rows, valid_rows, reject_rows, parse_duration_sec, encoding_detected)
        
    Raises:
        ParseError: If parsing fails critically
        ValueError: If required metadata missing
    
    Contract guarantees:
        - Returns ParseResult (not bare DataFrame)
        - Explicit dtypes (no object for codes)
        - Sorted by natural key
        - 64-char row_content_hash computed per spec v1.1 (§5.2)
        - Metadata columns injected
        - Encoding/BOM handled with CP1252 support
        - Deterministic output (same input → same hash)
        - No filesystem writes (ingestor persists artifacts)
    """
```

**All parsers MUST return ParseResult.**

**ParseResult Return Type (v1.1):**

```python
from typing import NamedTuple, Dict, Any
import pandas as pd

class ParseResult(NamedTuple):
    """
    Structured parser output per STD-parser-contracts §5.3
    
    Attributes:
        data: Canonical DataFrame (valid rows with metadata + row_content_hash)
        rejects: Rejected rows DataFrame (validation failures, empty if all valid)
        metrics: Parse metrics dict (total_rows, valid_rows, reject_rows, parse_duration_sec, encoding_detected, etc.)
    """
    data: pd.DataFrame
    rejects: pd.DataFrame
    metrics: Dict[str, Any]

def parse_{dataset}(
    file_obj: BinaryIO,
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Returns structured result for cleaner separation.
    Ingestor handles all file writes from ParseResult components.
    """
    # ... parse logic ...
    
    # Separate valid rows from rejects
    rejects = df[df['_validation_error'].notna()].copy()
    data = df[df['_validation_error'].isna()].drop(columns=['_validation_error'])
    
    # Build metrics
    metrics = {
        'total_rows': len(df),
        'valid_rows': len(data),
        'reject_rows': len(rejects),
        'parse_duration_sec': elapsed,
        'encoding_detected': encoding,
        'encoding_fallback': encoding != 'utf-8'
    }
    
    return ParseResult(data=data, rejects=rejects, metrics=metrics)
```

This eliminates helper-based quarantine writes and returns metrics in-band for cleaner ingestor integration.

### 6.2 Router Contract

**Router Function Signature (v1.1):**

```python
def route_to_parser(
    filename: str, 
    file_head: Optional[bytes] = None
) -> Tuple[str, str, Callable]:
    """
    Route file to appropriate parser using filename + content sniffing.
    
    Args:
        filename: Source filename (used for pattern matching)
        file_head: First ~8KB of file for magic byte/BOM detection (optional but recommended)
        
    Returns:
        (dataset_name, schema_id, parser_func)
        
    Raises:
        ValueError: If no parser found for filename pattern
    
    Content Sniffing (when file_head provided):
        - Magic bytes: ZIP (PK), Excel (PK + XML markers), PDF (%PDF)
        - BOM detection: UTF-8-sig, UTF-16 LE/BE
        - Format detection: Fixed-width vs CSV (column alignment, delimiters)
        - Can override parser choice if filename extension misleading
    
    Note: Presence of parser_func IS the status.
          No separate "status" string needed.
    
    Example:
        # Caller passes first 8KB for content sniffing
        with open(filepath, 'rb') as f:
            file_head = f.read(8192)
            f.seek(0)  # Reset for parser
            
            dataset, schema_id, parser_func = route_to_parser(filename, file_head)
            result = parser_func(f, filename, metadata)
    """
```

**Implementation:** `cms_pricing/ingestion/parsers/__init__.py`

This enables robust format detection without filename-only misroutes (e.g., `.csv` files that are actually fixed-width).

### 6.3 Schema Contract Format

**Existing Format (JSON):**

Schema contracts use JSON format defined in `cms_pricing/ingestion/contracts/`:

```json
{
  "dataset_name": "cms_pprrvu",
  "version": "1.0",
  "columns": {
    "hcpcs": {
      "name": "hcpcs",
      "type": "str",
      "nullable": false,
      "pattern": "^[A-Z0-9]{5}$",
      "description": "HCPCS code"
    },
    "work_rvu": {
      "type": "float64",
      "nullable": true,
      "min_value": 0.0,
      "max_value": 100.0
    }
  },
  "primary_keys": ["hcpcs", "modifier", "effective_from"],
  "business_rules": [...],
  "quality_thresholds": {...}
}
```

**Schema Registry:** `cms_pricing/ingestion/contracts/schema_registry.py`

**Validation:** Schema validation runs AFTER parsing, BEFORE enrichment

### 6.4 Metadata Injection Contract

**Ingestor Responsibilities:**

Ingestors MUST provide metadata dict to parsers:

```python
metadata = {
    # Core identity
    'dataset_id': 'pprrvu',
    'release_id': 'mpfs_2025_q4_20251015',
    
    # Three vintage fields (REQUIRED per DIS standards)
    'vintage_date': datetime(2025, 10, 15, 14, 30, 22),
    'product_year': '2025',
    'quarter_vintage': '2025Q4',
    
    # Provenance
    'source_uri': 'https://www.cms.gov/...',
    'file_sha256': 'abc123...',
    
    # Versioning
    'parser_version': 'v1.0.0',
    'schema_id': 'cms_pprrvu_v1.0',
    'layout_version': 'v2025.4.0',  # For fixed-width only
}
```

**Parser Responsibilities:**

Parsers MUST inject metadata as DataFrame columns:

```python
# Required metadata columns
df['release_id'] = metadata['release_id']
df['vintage_date'] = metadata['vintage_date']
df['product_year'] = metadata['product_year']
df['quarter_vintage'] = metadata['quarter_vintage']
df['source_filename'] = filename
df['source_file_sha256'] = metadata['file_sha256']
df['parsed_at'] = datetime.utcnow()

# Quality column
df['row_content_hash'] = df.apply(compute_row_hash, axis=1)
```

**Separation of Concerns:**
- Ingestor: Extracts vintage from manifest, builds release_id
- Parser: Accepts metadata, injects as columns
- Benefit: Pure functions, easy to test, no global state

**⚠️ Important - Avoid Vintage Field Duplication:**
- `vintage_date`, `product_year`, `quarter_vintage` are **METADATA columns** (injected by parser)
- Do NOT duplicate as data columns in schemas (e.g., don't add `vintage_year` as a data field)
- If business logic requires a different vintage concept (e.g., "valuation year" for conversion factors), use distinct naming
- Rationale: Prevents confusion between metadata vintage (when data was released) vs business vintage (what year data describes)

### 6.5 Integration with DIS Pipeline

Parsers integrate with DIS normalize stage per `STD-data-architecture-prd-v1.0.md` §3.4:

```python
class MPFSIngestor(BaseDISIngestor):
    async def normalize_stage(self, raw_batch: RawBatch) -> AdaptedBatch:
        """Normalize stage calls parsers"""
        
        # Prepare metadata (once per run)
        metadata = {
            'release_id': self.current_release_id,
            'vintage_date': self.vintage_date,
            'product_year': self.product_year,
            'quarter_vintage': self.quarter_vintage,
            # ...
        }
        
        adapted_data = {}
        
        for file_data in raw_batch.files:
            # Route to parser
            dataset, schema_id, parser_func = route_to_parser(file_data['filename'])
            
            # Call parser with metadata injection
            df = parser_func(
                file_obj=file_data['file_obj'],
                filename=file_data['filename'],
                metadata=metadata
            )
            
            adapted_data[dataset] = df
        
        return AdaptedBatch(adapted_data=adapted_data, metadata=metadata)
```

**IngestRun Tracking:**

Parsers update IngestRun model fields during normalize stage:
- `files_parsed` - Count of successfully parsed files
- `rows_discovered` - Total rows across all files
- `schema_version` - Schema contract version used
- `stage_timings['normalize_duration_sec']` - Parse duration

---

## 7. Router & Layout Registry

### 7.1 Router (Content Sniffing)

**Router inspects:**
- Magic bytes (ZIP: `PK`, Excel: `PK` + XML, PDF: `%PDF`)
- BOM markers
- First N lines for headers
- Header tokens for dataset identification
- Filename as fallback hint

**Implementation:** `cms_pricing/ingestion/parsers/__init__.py`

**Pattern Matching:**
- Regex patterns for each dataset
- Case-insensitive matching
- Prioritized matching (most specific first)

**Example:**
```python
PARSER_ROUTING = {
    r"PPRRVU.*\.(txt|csv|xlsx)$": ("pprrvu", "cms_pprrvu_v1.0", parse_pprrvu),
    r"GPCI.*\.(txt|csv|xlsx)$": ("gpci", "cms_gpci_v1.0", parse_gpci),
    # ...
}
```

### 7.2 Layout Registry

**Purpose:** Externalize fixed-width column specifications, SemVer by year/quarter

**Implementation (v1.0):** `cms_pricing/ingestion/parsers/layout_registry.py`

**Current Format (Python dicts in code):**
```python
LAYOUT_REGISTRY = {
    ('pprrvu', '2025', 'Q4'): {
        'version': 'v2025.4.0',  # SemVer
        'min_line_length': 200,
        'source_version': '2025D',
        'columns': {
            'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
            'work_rvu': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},
            # ... all 20 columns for PPRRVU
        }
    }
}
```

**v1.0 Note:** Layouts are Python dicts in-code for simplicity and speed. Lookups via `get_layout(year, quarter, dataset)`.

**v1.1 Enhancement (Planned - YAML Source of Truth):**

```yaml
# File: layout_registry/pprrvu/v2025.4.0.yaml
dataset: pprrvu
version: v2025.4.0
year: "2025"
quarter: "Q4"
min_line_length: 200
columns:
  - name: hcpcs
    start: 0
    end: 5
    type: string
    nullable: false
  - name: work_rvu
    start: 61
    end: 65
    type: decimal(6,2)
    nullable: true
  # ... all columns
```

`layout_registry.py` will load and validate YAMLs for better governance and diff reviews.

**Versioning:**
- **Major version bump**: Column position/width changes, required field changes
- **Minor version bump**: Optional field additions  
- **Patch version bump**: Documentation updates, clarifications

**Testing:**
- Layout tests detect column width changes
- CI blocks if layout changes without version bump

---

## 8. Validation Requirements

### 8.1 Schema Validation

**Timing:** After parsing, before enrichment

**Rules:**
- Required fields present
- Field types match schema
- Domain values valid (if specified)
- Range constraints met

**Action:** BLOCK on schema validation failures

### 8.2 Reference Validation Hooks

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

### 8.2.1 Categorical Domain Validation

**Problem:** `CategoricalDtype(categories=[...])` silently converts unknown values to NaN, violating fail-loud principle.

**Solution:** Pre-check domain before categorical conversion.

**Implementation Pattern:**
```python
def enforce_categorical_dtypes(
    df: pd.DataFrame,
    categorical_domains: Dict[str, List[str]]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Enforce categorical dtypes with explicit domain validation.
    
    Returns:
        (valid_df, rejects_df) - Separated valid and out-of-domain rows
    """
    rejects_list = []
    
    for col, domain in categorical_domains.items():
        if col not in df.columns:
            continue
        
        # Check for out-of-domain values
        invalid_mask = ~df[col].isin(domain + [None, pd.NA, ''])
        
        if invalid_mask.any():
            # Move invalid rows to rejects
            rejects = df[invalid_mask].copy()
            rejects['validation_error'] = f"Invalid {col}: not in domain {domain}"
            rejects['validation_severity'] = 'WARN'  # or BLOCK per business rule
            rejects['validation_rule_id'] = f'DOMAIN_CHECK_{col.upper()}'
            rejects_list.append(rejects)
            
            # Remove from main DF
            df = df[~invalid_mask].copy()
        
        # Now safe to convert to categorical
        df[col] = df[col].astype(pd.CategoricalDtype(categories=domain))
    
    rejects_df = pd.concat(rejects_list) if rejects_list else pd.DataFrame()
    return df, rejects_df
```

**No Silent NaN Coercion:**
- Invalid values MUST be rejected with explicit error
- Never silently convert to NaN
- Severity: WARN (quarantine but continue) or BLOCK (fail parse) per business rule
- Audit trail: All domain violations tracked in rejects DataFrame

### 8.3 Validation Tiers

**BLOCK (Critical Errors):**
- Invalid file format
- Missing required fields
- Type cast failures on required fields
- Schema contract violations
- Corrupt/unreadable file

**Action:** Raise exception, stop processing

**WARN (Soft Failures):**
- Unknown reference codes (HCPCS not in ref set)
- Out-of-range values (RVU > 100)
- Missing optional fields
- Data quality issues (high null rates)

**Action:** Create quarantine artifact, log warning, continue

**INFO (Statistical Anomalies):**
- Row count drift from prior version
- Distribution changes
- New code appearances

**Action:** Log only, include in metrics

### 8.4 Quarantine Artifact Format

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

---

## 9. Error Taxonomy

**Error Codes:**
- `ENCODING_ERROR` - Cannot decode file with known encodings
- `DIALECT_UNDETECTED` - Cannot detect CSV dialect
- `HEADER_MISSING` - No header row found
- `FIELD_MISSING` - Required field not in data
- `TYPE_CAST_ERROR` - Cannot cast field to required type
- `OUT_OF_RANGE` - Value exceeds min/max constraints
- `REFERENCE_MISS` - Code not in reference set
- `ROW_DUPLICATE` - Duplicate natural key
- `KEY_VIOLATION` - Primary key constraint violation
- `LAYOUT_MISMATCH` - Fixed-width layout doesn't match data
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
    "reference_version": "2025Q4",
    "valid_codes_count": 19000
  },
  "sha256": "hash of raw_row"
}
```

---

## 10. Observability & Metrics

### 10.1 Per-File Metrics

**v1.0 Implementation:**
Parsers log metrics during execution. Ingestor aggregates into `metrics.json` and IngestRun model.

**v1.1 Enhancement:** Parsers will return metrics in ParseResult for cleaner separation.

**Metrics emitted for each file parsed:**

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
  "p50_row_latency_ms": 0.21,
  "p95_row_latency_ms": 0.45,
  "p99_row_latency_ms": 0.89,
  "checksum_match": true,
  "schema_validation_passed": true,
  "reference_checks_passed": true,
  "reference_misses": 3,
  "null_rate_max": 0.0023,
  "memory_peak_mb": 145
}
```

### 10.2 Aggregate Metrics (Per Run)

Tracked in IngestRun model:

- `files_discovered`, `files_parsed`, `files_failed`
- `rows_discovered`, `rows_ingested`, `rows_rejected`, `rows_quarantined`
- `bytes_processed`
- `schema_drift_detected`
- `validation_errors`, `validation_warnings`
- `parse_duration_total_sec`

### 10.3 Logging Requirements

**Every parse operation MUST log:**

```python
logger.info(
    "Parsing file",
    filename=filename,
    release_id=metadata['release_id'],
    file_sha256=metadata['file_sha256'],
    schema_id=schema_id,
    parser_version=metadata['parser_version']
)
```

**On completion:**
```python
logger.info(
    "Parse completed",
    rows=len(df),
    columns=len(df.columns),
    null_rate_max=...,
    duration_sec=...
)
```

**Benefit:** Enables incident investigation, performance debugging

---

## 11. Provenance Requirements

### 11.1 Provenance Manifest

**File:** `data/stage/{dataset}/{release_id}/provenance.json`

**Contents:**
```json
{
  "dataset_id": "pprrvu",
  "release_id": "mpfs_2025_q4_20251015",
  "parsed_at": "2025-10-15T14:30:22Z",
  
  "source": {
    "uri": "https://www.cms.gov/...",
    "sha256": "abc123...",
    "downloaded_at": "2025-10-15T12:00:00Z",
    "cms_publication_date": "2025-10-01"
  },
  
  "parser": {
    "version": "v1.0.0",
    "function": "parse_pprrvu",
    "schema_id": "cms_pprrvu_v1.0",
    "layout_version": "v2025.4.0"
  },
  
  "environment": {
    "hostname": "ingest-worker-01",
    "os": "Linux 5.15.0",
    "python": "3.11.5",
    "pandas": "2.1.0",
    "pyarrow": "13.0.0",
    "git_sha": "cd62ea9"
  },
  
  "output": {
    "rows": 19450,
    "columns": 35,
    "parquet_path": "data/stage/pprrvu/mpfs_2025_q4_20251015/parsed.parquet",
    "parquet_size_bytes": 2458932,
    "parquet_sha256": "def456..."
  },
  
  "quality": {
    "rejects_count": 3,
    "quarantine_path": "data/quarantine/mpfs_2025_q4_20251015/pprrvu_invalid_hcpcs.parquet"
  }
}
```

---

## 12. Compatibility & Versioning

### 12.1 Parser Versioning (SemVer)

**Version format:** `v{MAJOR}.{MINOR}.{PATCH}`

**Bump rules:**
- **MAJOR**: Breaking changes to output schema, sort order, or function signature
- **MINOR**: Additive features (new optional columns, improved error handling)
- **PATCH**: Bug fixes, performance improvements, documentation

**Examples:**
- `v1.0.0 → v1.0.1`: Fixed encoding bug
- `v1.0.0 → v1.1.0`: Added optional metadata column
- `v1.0.0 → v2.0.0`: Changed primary key sort order
- `v1.0.0 → v2.0.0`: Changed row hash algorithm or column order

**Special case:** Any change to row-hash spec (column order in hash, delimiter, normalization rules per §5.2) requires **MAJOR** parser version bump to ensure cross-version reproducibility.

### 12.2 Schema Versioning (SemVer)

**Version format:** `v{MAJOR}.{MINOR}`

**Bump rules:**
- **MAJOR**: Breaking changes (type changes, column removals, meaning changes)
- **MINOR**: Additive changes (new optional columns)

**Examples:**
- `v1.0 → v1.1`: Added optional `description` column
- `v1.0 → v2.0`: Changed `hcpcs` from string to categorical

### 12.3 Layout Versioning (SemVer)

**Version format:** `v{YEAR}.{QUARTER}.{PATCH}`

**Bump rules:**
- **YEAR.QUARTER**: New CMS release year/quarter
- **PATCH**: Corrections to existing layout

**Examples:**
- `v2025.4.0`: 2025 Q4 (revision D) layout
- `v2025.4.1`: Corrected column 15 width
- `v2026.1.0`: 2026 Q1 (revision A) layout

### 12.4 Backfill Requirements

Backfills MUST pin all three versions:
- `parser_version`: Ensures same parsing logic
- `schema_id`: Ensures same validation
- `layout_version`: Ensures same column positions

**Reproducibility guarantee:** Same versions → same output

---

## 13. Security & Compliance

**Data Classification:**
- No PII expected in CMS pricing files
- If PII detected: Redact and quarantine entire file
- Alert security team

**Integrity:**
- Verify `sha256` of source against metadata
- Sign artifacts optionally (future enhancement)
- Supply-chain: Record Docker image digest + git SHA in provenance

**Access Control:**
- Parser code is **public within monorepo** (stable, SemVer'd, importable across ingestors)
- Parsers are NOT external HTTP APIs or customer-facing
- Schema contracts can be published externally (based on public CMS data)
- Quarantine artifacts are internal only (contain rejected data)

---

## 14. Testing Strategy

### 14.1 Unit Tests

**Location:** `tests/ingestion/test_parsers.py`

**Coverage:**
- Each parser function tested independently
- Synthetic fixtures with known outputs
- Edge cases: empty files, malformed data, encoding issues

**Example:**
```python
def test_parse_pprrvu_csv():
    """Parser unit test - assert on ParseResult only, NO file writes"""
    metadata = {'release_id': 'test', ...}
    
    # Parser returns ParseResult
    result = parse_pprrvu(sample_csv, "test.csv", metadata)
    
    # ✅ GOOD: Assert on ParseResult structure
    assert isinstance(result, ParseResult)
    assert isinstance(result.data, pd.DataFrame)
    assert isinstance(result.rejects, pd.DataFrame)
    assert isinstance(result.metrics, dict)
    
    # ✅ GOOD: Assert on data content
    assert 'hcpcs' in result.data.columns
    assert result.data['hcpcs'].dtype.name == 'category'
    assert len(result.data) > 0
    
    # ✅ GOOD: Assert on metrics
    assert result.metrics['total_rows'] == len(result.data) + len(result.rejects)
    assert result.metrics['valid_rows'] == len(result.data)
    
    # ❌ BAD: Parser tests should NOT check file system
    # assert Path("output/pprrvu.parquet").exists()  # DON'T DO THIS!
```

**⚠️ IO Boundary Rule:**
- **Parser tests**: Assert on `ParseResult` only (data, rejects, metrics)
- **Ingestor E2E tests**: Assert on file artifacts (`.parquet`, `.json`, etc.)
- Parsers do NOT write files - ingestor handles all IO

### 14.2 Golden-File Tests

**Location:** `tests/fixtures/{dataset}/golden/`

**Validation:**
- Known input → byte-for-byte identical output
- Verified via `row_content_hash`
- Excludes timestamp columns from comparison

**Purpose:** Detect regressions, ensure determinism

### 14.3 Schema Drift Tests

**Location:** `tests/ingestion/test_schema_contracts.py`

**Tests:**
- Parser output matches schema contract
- Required columns present
- Dtypes correct
- Constraints satisfied

### 14.4 Layout Version Tests

**Location:** `tests/ingestion/test_layout_registry.py`

**Tests:**
- Detect column width changes without version bump
- Validate layout completeness
- Test all year/quarter combinations

### 14.5 Performance Tests

**Location:** `tests/ingestion/test_parser_performance.py`

**Benchmarks:**
- Parse 1M rows in < 60 seconds
- Memory usage < 500MB for 1M rows
- CPU usage reasonable

**CI Integration:** Performance regression detection

---

## 15. Rollout & Operations

### 15.1 Deployment Strategy

**Feature Flags:**
- Per-dataset parser version selection
- Enable blue/green deployments
- Gradual rollout (1% → 10% → 100%)

**Canary Testing:**
- Run new parser on 1% of files
- Compare metrics with baseline
- Sample diff analysis
- Auto-rollback on failure

### 15.2 Monitoring

**Alerts:**
- Parse failure rate > 1%
- Reject rate > 5%
- Performance regression > 20%
- Schema drift detected

**Dashboards:**
- Parse success rate by dataset
- Performance trends
- Reject rate trends
- Schema version adoption

---

## 16. Directory Layout

**Current Implementation:**

```
cms_pricing/ingestion/
├── parsers/
│   ├── __init__.py              # Router with PARSER_ROUTING
│   ├── pprrvu_parser.py         # PPRRVU parser contract
│   ├── gpci_parser.py           # GPCI parser contract
│   ├── locality_parser.py       # Locality parser contract
│   ├── anes_parser.py           # Anesthesia parser contract
│   ├── oppscap_parser.py        # OPPS cap parser contract
│   ├── conversion_factor_parser.py  # Conversion factor parser (NEW)
│   └── layout_registry.py       # Fixed-width layouts (SemVer)
├── contracts/
│   ├── schema_registry.py       # Schema validation engine
│   ├── cms_pprrvu_v1.0.json    # PPRRVU schema contract
│   ├── cms_gpci_v1.0.json      # GPCI schema contract
│   ├── cms_localitycounty_v1.0.json
│   ├── cms_anescf_v1.0.json
│   ├── cms_oppscap_v1.0.json
│   └── cms_conversion_factor_v1.0.json  # NEW
├── metadata/
│   ├── discovery_manifest.py    # Scraper manifest handling
│   └── vintage_extractor.py     # Shared metadata extraction (NEW)
└── ingestors/
    ├── mpfs_ingestor.py         # Uses shared parsers
    ├── rvu_ingestor.py          # Uses shared parsers (wrappers deprecated)
    └── base_dis_ingestor.py     # DIS base class

tests/ingestion/
├── test_parsers.py              # Parser unit tests
├── test_schema_contracts.py     # Schema validation tests
├── test_layout_registry.py      # Layout version tests
└── test_parser_performance.py   # Performance benchmarks

tests/fixtures/
├── pprrvu/
│   ├── golden/
│   │   ├── PPRRVU2025D_sample.txt
│   │   └── expected_output.parquet
│   └── synthetic/
│       ├── invalid_hcpcs.csv
│       └── malformed.txt
└── (other datasets)
```

---

## 17. Current Implementation

### 17.1 Existing Components

**Schema Registry:**
- File: `cms_pricing/ingestion/contracts/schema_registry.py`
- Status: ✅ Implemented
- Features: Schema loading, validation, contract enforcement

**Schema Contracts:**
- Location: `cms_pricing/ingestion/contracts/*.json`
- Count: 8 contracts (PPRRVU, GPCI, Locality, ANES, OPPSCAP, CF, OPPS, ZIP)
- Format: JSON with columns, types, constraints

**Layout Registry:**
- File: `cms_pricing/ingestion/parsers/layout_registry.py`
- Status: ✅ Implemented
- Versions: 2025 layouts for Q1-Q4
- SemVer: v2025.{quarter}.{patch}

**Parser Routing:**
- File: `cms_pricing/ingestion/parsers/__init__.py`
- Status: ✅ Implemented
- Datasets: 8 CMS file types routed

**Metadata Extraction:**
- File: `cms_pricing/ingestion/metadata/vintage_extractor.py`
- Status: ✅ Implemented
- Features: Extracts three vintage fields from manifests/filenames

**IngestRun Tracking:**
- File: `cms_pricing/models/nearest_zip.py`
- Status: ✅ Enhanced with 65 fields
- Features: Five pillar metrics, stage timing, provenance

### 17.2 Implementation Status

| Parser | Status | File | Lines |
|--------|--------|------|-------|
| PPRRVU | ⏳ In Progress | `pprrvu_parser.py` | ~200 |
| GPCI | ⏸️ Pending | `gpci_parser.py` | ~120 |
| Locality | ⏸️ Pending | `locality_parser.py` | ~100 |
| ANES | ⏸️ Pending | `anes_parser.py` | ~100 |
| OPPSCAP | ⏸️ Pending | `oppscap_parser.py` | ~120 |
| Conversion Factor | ⏸️ Pending | `conversion_factor_parser.py` | ~180 |

**Next:** Complete Phase 1 parser implementation following this standard

---

## 18. Acceptance Criteria

**Parser Implementation:**
- [ ] Parser follows function signature contract (§6.1)
- [ ] Returns `ParseResult(data, rejects, metrics)` per v1.1 spec (§5.3)
- [ ] Accepts metadata dict, injects metadata columns (§6.4)
- [ ] Returns DataFrame with explicit dtypes (no object for codes)
- [ ] Output sorted by natural key (pinned columns per dataset)
- [ ] Includes 64-char row_content_hash column (schema-driven precision)
- [ ] Handles encoding with CP1252 cascade (§5.2)
- [ ] Content sniffing via file_head parameter (§6.2)
- [ ] Logs with release_id, file_sha256, schema_id, encoding_detected
- [ ] Pre-checks categorical domains before conversion (§8.2.1)
- [ ] Separates valid/reject rows explicitly (no silent NaN coercion)
- [ ] Validates against schema contract
- [ ] Performance meets SLAs (§5.5)

**Testing:**
- [ ] Unit tests pass for parser function
- [ ] Golden-file test produces identical output (hash-verified)
- [ ] Schema validation test catches contract violations
- [ ] Layout version test detects breaking changes
- [ ] Performance test meets benchmarks
- [ ] Integration test with ingestor passes

**Documentation:**
- [ ] Parser function has comprehensive docstring
- [ ] Schema contract registered in `contracts/`
- [ ] Layout registered in `layout_registry.py` (if fixed-width)
- [ ] Cross-referenced from dataset PRD

**Compliance:**
- [ ] No private method calls to other ingestors
- [ ] Backwards compatible (if replacing existing parser)
- [ ] Deprecation notices added (if applicable)
- [ ] CI passes all quality gates

**Versioning:**
- [ ] Changing hash spec (§5.2) or schema column order requires **MAJOR** parser version bump
- [ ] Hash algorithm, delimiter (`\x1f`), or normalization rules are part of formal contract
- [ ] Breaking changes properly documented with migration path

---

## 19. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Layout drift undetected | High | Layout registry with SemVer, golden tests, CI gates |
| Silent dtype coercion | High | Arrow dtypes + explicit casting + Great Expectations |
| Throughput regressions | Medium | Benchmarks in CI, performance budgets, profiling hooks |
| Reference table mismatch | Medium | Pin references by release_id, version validation |
| Encoding corruption | Medium | Explicit encoding handling, BOM detection, fallback strategy |
| RVU ingestor breaks | Low | Backwards-compatible wrappers, deprecation period |
| Parser version conflicts | Low | SemVer enforcement, feature flags for selection |

---

## 20. Implementation Roadmap

### v1.1: Enhanced Contracts (Current)

**Goal:** Implement production-grade parser contracts with ParseResult, content sniffing, and robust validation

**v1.1 Features:**
- **ParseResult return type** - Structured output (data, rejects, metrics)
- **Content sniffing** - Router accepts file_head for magic byte detection
- **64-char SHA-256 hash** - Full digest with schema-driven precision
- **CP1252 encoding support** - Windows encoding in detection cascade  
- **Categorical domain validation** - Pre-check before conversion (no silent NaN)
- **Pinned natural keys** - Exact sort columns per dataset
- Metadata injection via dict parameter
- Python dict-based layout registry
- Filename-only routing
- Formal row hash spec
- IngestRun tracking integrated

**Deliverables:**
- 6 parser modules following v1.0 contract
- Schema contracts for all file types
- Layout registry with 2025 layouts
- Metadata extractor utility
- Unit and integration tests

**Timeline:** 2-3 hours (in progress)

### v1.1: Enhanced Contracts (Future - ~6 months)

**Goal:** Refine parser interface for cleaner separation

**v1.1 Enhancements:**
- ParseResult return type (df, rejects, metrics)
- Router accepts file_head for magic byte detection
- YAML-based layout registry
- Eliminate helper-based quarantine writes
- Return metrics in-band (not via logging)

**Timeline:** 1-2 hours refactoring

### Phase 2: Enhanced Validation (Next - After v1.0)

**Goal:** Comprehensive reference validation, tiered constraints

**Deliverables:**
- Reference validators module
- Tiered validation engine (BLOCK/WARN/INFO)
- Enhanced quarantine with error details

**Timeline:** 2-3 hours

### Phase 3: Performance Optimization (Future)

**Goal:** Achieve SLA targets, parallel parsing

**Deliverables:**
- Thread-safe parsers
- Parallel file processing
- Streaming for large files

**Timeline:** 3-4 hours

### Phase 4: Advanced Features (Future)

**Goal:** XML support, PDF extraction, custom formats

**Deliverables:**
- XML parser
- PDF text extraction
- Custom format adapters

**Timeline:** 4-6 hours

---

## 21. Change Log

| Version | Date | Summary | PR |
|---------|------|---------|-----|
| 1.1 | 2025-10-15 | **Production-grade enhancements for MPFS implementation.** Upgraded parser return type to `ParseResult(data, rejects, metrics)` for cleaner separation (§5.3). Added content sniffing via `file_head` parameter for magic byte/BOM detection (§6.2). Changed row hash to full 64-char SHA-256 digest with schema-driven precision per column (§5.2). Added CP1252 to encoding cascade for Windows file support (§5.2). Implemented explicit categorical domain validation - pre-check before conversion to prevent silent NaN coercion (§8.2.1). Pinned exact natural keys per dataset including `effective_from` for time-variant data. Added vintage field duplication guidance (§6.4). Enhanced test strategy with explicit IO boundary rules - parsers return data, ingestors write files (§14.1). Updated acceptance criteria with all v1.1 requirements (§18). These changes prevent technical debt and enable immediate production deployment. | #TBD |
| 1.0 | 2025-10-15 | Initial adoption of parser contracts standard. Defines public contract requirements (pandas DataFrame return type), metadata injection pattern with three vintage fields, tiered validation (BLOCK/WARN/INFO), formal row hash specification for reproducibility, SemVer for parsers/schemas/layouts, integration with DIS normalize stage, and comprehensive testing strategy. Documents v1.0 implementation (pandas, helper-based quarantine, Python dict layouts, filename routing) and v1.1 enhancements (ParseResult, YAML layouts, magic byte routing). Establishes governance for shared parser infrastructure used by MPFS, RVU, OPPS, and future ingestors. | #TBD |

---

## Appendix A: CMS File Format Reference

### A.1 Fixed-Width TXT Files

**Datasets:** PPRRVU, GPCI, ANES, LOCCO, OPPSCAP

**Characteristics:**
- No delimiters (comma, tab, pipe)
- Column positions defined in layout specification
- Usually include header row (skip line 1)
- Line length varies by dataset (200-300 chars typical)

**Authoritative Source:** CMS RVU cycle PDFs (e.g., RVU25D.pdf)

### A.2 CSV Files

**Datasets:** All datasets have CSV variants

**Characteristics:**
- Header row (case varies)
- Comma-delimited (sometimes tab)
- Quoted fields for descriptions
- UTF-8 or Latin-1 encoding

### A.3 Excel Workbooks

**Datasets:** Conversion factors, some quarterly files

**Characteristics:**
- .xlsx format (Office Open XML)
- Multiple sheets possible
- First row is header
- Numeric formatting can vary

### A.4 ZIP Archives

**Datasets:** All CMS releases distributed as ZIP

**Contents:**
- Multiple files (TXT, CSV, XLSX, PDF)
- Directory structure varies
- Readme/documentation files included

---

## Appendix B: Column Normalization Examples

### B.1 HCPCS Code Variations

**Aliases found in CMS files:**
- `HCPCS`, `HCPCS_CODE`, `HCPCS_CD`, `CPT`, `CODE`

**Canonical:** `hcpcs`

### B.2 RVU Component Variations

**Work RVU aliases:**
- `WORK_RVU`, `RVU_WORK`, `WORK`, `WORK_RVUS`

**PE Non-Facility aliases:**
- `PE_NONFAC_RVU`, `PE_RVU_NONFAC`, `NON_FAC_PE_RVU`, `PE_NON_FAC`

**Canonical:**
- `work_rvu`, `pe_rvu_nonfac`, `pe_rvu_fac`, `mp_rvu`

---

## Appendix C: Reference Validation Details

### C.1 HCPCS/CPT Validation

**Phase 1 (Minimal):**
- Format: 5 alphanumeric characters `^[A-Z0-9]{5}$`
- Action: WARN on format violation, quarantine

**Phase 2 (Comprehensive):**
- Lookup in CMS HCPCS reference file
- Check effective date range
- Validate status (active, deleted, etc.)
- Action: BLOCK on unknown codes

### C.2 Locality Code Validation

**Phase 1:**
- Format: 2-digit string
- Action: WARN on format violation

**Phase 2:**
- Lookup in locality crosswalk
- Validate MAC + locality combination
- Check effective date
- Action: BLOCK on unknown locality

---

## Appendix D: Backward Compatibility

### D.1 Deprecation Policy

**When replacing private methods with public parsers:**

1. Keep original method as wrapper (1-2 releases)
2. Add `DeprecationWarning`
3. Update docstring with migration path
4. Remove in next major version

**Example:**
```python
def _parse_pprrvu_file(self, file_obj, filename):
    """
    DEPRECATED: Use cms_pricing.ingestion.parsers.pprrvu_parser.parse_pprrvu()
    
    This wrapper maintained for backwards compatibility.
    Will be removed in v2.0.
    """
    warnings.warn(
        "RVUIngestor._parse_pprrvu_file is deprecated. "
        "Use pprrvu_parser.parse_pprrvu() instead.",
        DeprecationWarning
    )
    
    from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu
    
    # Build metadata for new parser
    metadata = self._build_parser_metadata()
    
    return parse_pprrvu(file_obj, filename, metadata)
```

### D.2 Migration Timeline

- **v1.0**: Introduce shared parsers, keep wrappers
- **v1.5**: Deprecation warnings active, migration guide published
- **v2.0**: Remove wrappers, shared parsers only

---

## QA Summary

**Primary Goal:** Establish shared parser contracts for CMS data ingestion

**Key Requirements:**
1. Public parser contracts (not private methods)
2. Metadata injection by ingestor
3. Explicit dtypes and deterministic output
4. Tiered validation (BLOCK/WARN/INFO)
5. Comprehensive provenance tracking

**Success Criteria:**
- All parsers follow contract signature
- Schema validation enforced
- IngestRun tracking complete
- Tests demonstrate idempotency
- No breaking changes to existing code

**Cross-References:**
- Implements STD-data-architecture normalize stage requirements
- Referenced by PRD-mpfs, PRD-rvu-gpci, PRD-opps
- Registered in DOC-master-catalog

**Implementation Status:**
- Phase 0: Infrastructure complete ✅
- Phase 1: Parser extraction in progress ⏳
- Phase 2-4: Pending

---

*End of Document*

