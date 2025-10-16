# Parser Contracts Standard

**Status:** Draft v1.6  
**Owners:** Data Platform Engineering  
**Consumers:** Data Engineering, MPFS Ingestor, RVU Ingestor, OPPS Ingestor, API Teams, QA Guild  
**Change control:** ADR + PR review  
**Review cadence:** Quarterly (first business Monday)  
**Companion Docs:** _(Planned for v2.0)_

> **For Implementers:** Parser implementation patterns are documented inline (§21). 
> A dedicated companion implementation guide is planned for v2.0.

**Cross-References:**
- **STD-data-architecture-prd-v1.0.md:** DIS pipeline and normalize stage requirements
- **STD-data-architecture-impl-v1.0.md:** BaseDISIngestor implementation patterns
- **PRD-mpfs-prd-v1.0.md:** MPFS ingestor uses parser contracts for RVU bundle
- **PRD-rvu-gpci-prd-v0.1.md:** RVU parsing requirements and fixed-width layout specifications
- **PRD-opps-prd-v1.0.md:** OPPS ingestor uses parser contracts
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 1. Summary

Parsers return `ParseResult(data, rejects, metrics)`; the ingestor writes Arrow/Parquet artifacts with deterministic outputs and full provenance. The system converts heterogeneous CMS source files (CSV/TSV/TXT/XLSX/ZIP) into schema-validated tabular datasets. Parsers are selected by a router using filename patterns and optional content sniffing. The normalize stage writes DataFrames to Arrow/Parquet format.

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
            # Use Decimal for exact rounding (avoid binary float quirks)
            from decimal import Decimal as D, ROUND_HALF_UP
            precision = column_precision.get(col, 6) if column_precision else 6
            quantizer = D(10) ** -precision
            parts.append(str(D(str(val)).quantize(quantizer, rounding=ROUND_HALF_UP)))
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

**Production Note:** Use `cms_pricing/ingestion/parsers/_parser_kit.py::finalize_parser_output()` which implements this spec. Don't reimplement row hashing in individual parsers.

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
- **Note:** Ingestor may enrich/rename fields (e.g., `validation_error` → `quarantine_reason`) for consistency with §8.4 quarantine schema. Parser provides raw reject data; ingestor normalizes for storage.

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
    
    # Join invariant: input == valid + rejects
    assert metrics['total_rows'] == (len(data) + len(rejects)), \
        "Join invariant violated: valid + rejects must equal input rows"
    
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
        rejects_data = {}
        metrics_data = {}
        
        for file_data in raw_batch.files:
            # Route to parser with content sniffing
            file_head = file_data['file_obj'].read(8192)
            file_data['file_obj'].seek(0)
            
            dataset, schema_id, parser_func = route_to_parser(
                file_data['filename'], 
                file_head
            )
            
            # Call parser - returns ParseResult (v1.1+)
            result = parser_func(
                file_obj=file_data['file_obj'],
                filename=file_data['filename'],
                metadata=metadata
            )
            
            # Ingestor writes artifacts from ParseResult components
            adapted_data[dataset] = result.data
            rejects_data[dataset] = result.rejects
            metrics_data[dataset] = result.metrics
            
            # Write parsed.parquet (from result.data)
            self._write_parquet(result.data, f"{dataset}/parsed.parquet")
            
            # Write rejects.parquet (from result.rejects)
            if len(result.rejects) > 0:
                self._write_parquet(result.rejects, f"{dataset}/rejects.parquet")
        
        return AdaptedBatch(
            adapted_data=adapted_data, 
            metadata=metadata,
            rejects=rejects_data,
            metrics=metrics_data
        )
```

**IngestRun Tracking:**

Parsers update IngestRun model fields during normalize stage:
- `files_parsed` - Count of successfully parsed files
- `rows_discovered` - Total rows across all files
- `schema_version` - Schema contract version used
- `stage_timings['normalize_duration_sec']` - Parse duration

### 6.6 Schema vs API Naming Convention (v1.3)

**Problem:** CMS datasets appear in both internal storage (database) and external API responses, requiring different naming conventions for different audiences.

**Solution:** Parsers output **schema format** (DB canonical). API layer transforms to **presentation format** at serialization boundary.

**Schema Format (DB Canonical):**
- **Pattern:** `{component}_{type}` with prefix grouping
- **Example:** `rvu_work`, `rvu_pe_nonfac`, `rvu_pe_fac`, `rvu_malp`
- **Used by:** Parsers, database tables, schema contracts, ingestors
- **Rationale:** Logical grouping by prefix, clear data ownership (all RVU fields start with `rvu_*`)

**API Format (Presentation):**
- **Pattern:** `{type}_{component}` with suffix grouping
- **Example:** `work_rvu`, `pe_rvu_nonfac`, `pe_rvu_fac`, `mp_rvu`
- **Used by:** API responses, Pydantic schemas, external documentation
- **Rationale:** More intuitive for API consumers, industry-standard naming

**Transformation Boundary:**

Transform at **API serialization**, NOT in parser or ingestor:

```python
# Parser outputs schema format (DB canonical):
result = parse_pprrvu(file_obj, filename, metadata)
# result.data has columns: rvu_work, rvu_pe_nonfac, rvu_pe_fac, rvu_malp

# Database stores schema format (no transformation):
db_table.insert(result.data)  # Columns match table schema

# API router transforms for response (presentation format):
from cms_pricing.mappers import schema_to_api

@router.get("/rvu/{hcpcs}")
async def get_rvu(hcpcs: str):
    df = await db.query_rvu(hcpcs)  # Has rvu_work
    api_df = schema_to_api(df)       # Now has work_rvu
    return RVUResponse.from_dataframe(api_df)
```

**Column Mapper Location:** `cms_pricing/mappers/__init__.py`

**Per-Dataset Mappings:**

| Dataset | Schema → API Mappings |
|---------|----------------------|
| PPRRVU | `rvu_work` → `work_rvu`, `rvu_malp` → `mp_rvu` |
| GPCI | (add as implemented) |
| Others | (add as implemented) |

**Benefits:**
- **Single source of truth:** Schema contract is canonical
- **Clean layer separation:** Parser→DB (no transform), DB→API (transform)
- **Reversible:** Both `schema_to_api()` and `api_to_schema()` available
- **No silent drift:** Explicit mapping prevents misalignment
- **API evolution:** Can change presentation without affecting storage

**Example Mapping (PPRRVU):**

| Schema (DB Canonical) | API (Presentation) | Why |
|-----------------------|--------------------|-----|
| `rvu_work` | `work_rvu` | API aligns to legacy/intuitive naming |
| `rvu_malp` | `mp_rvu` | API uses common abbreviation (mp vs malp) |

**Code - PPRRVU:**
```python
# Transformation layer (NOT in parser, NOT in schema)
# Location: cms_pricing/mappers/__init__.py (API adapter only)

PPRRVU_SCHEMA_TO_API = {
    'rvu_work': 'work_rvu',
    'rvu_pe_nonfac': 'pe_rvu_nonfac',
    'rvu_pe_fac': 'pe_rvu_fac',
    'rvu_malp': 'mp_rvu',
}

def schema_to_api(df):
    """Transform schema columns to API presentation format."""
    return df.rename(columns=PPRRVU_SCHEMA_TO_API)

# Usage in API router:
@router.get("/rvu/{hcpcs}")
async def get_rvu(hcpcs: str):
    df_schema = await db.query_rvu(hcpcs)  # Has rvu_work
    df_api = schema_to_api(df_schema)       # Now has work_rvu
    return RVUResponse.from_dataframe(df_api)
```

**Parser Requirement:**
- ✅ **MUST** output schema format (rvu_work, etc.)
- ❌ **MUST NOT** output API format (work_rvu, etc.)
- ✅ **MUST** align layout column names with schema
- ✅ **SHOULD** document mapping in column mapper if API differs

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
        'version': 'v2025.4.1',  # SemVer
        'min_line_length': 165,
        'source_version': '2025D',
        'columns': {
            # Layout column names MUST match schema exactly (see §7.3)
            'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
            'modifier': {'start': 5, 'end': 7, 'type': 'string', 'nullable': False},
            'rvu_work': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},  # NOT work_rvu
            'rvu_malp': {'start': 85, 'end': 89, 'type': 'decimal', 'nullable': True},  # NOT mp_rvu
            # ... all 20 columns for PPRRVU (schema-canonical names, not API names)
        }
    }
}
```

**v1.0 Note (Current Source of Truth):** Layouts are Python dicts in-code for simplicity and speed. YAML migration is planned but NOT required for v1.0-v1.4 compliance.

**Function Signature & Semantics (v1.3):**

```python
def get_layout(
    product_year: str,      # "2025"
    quarter_vintage: str,   # "2025Q4" or "2025D" (annual)
    dataset: str            # "pprrvu", "gpci", etc.
) -> Optional[Dict[str, Any]]:
    """
    Get layout specification for fixed-width parsing.
    
    Returns:
        {
            'version': str,           # SemVer: vYYYY.Q.P
            'min_line_length': int,   # Observed minimum data line length
            'source_version': str,    # CMS release ID (e.g., "2025D")
            'columns': {
                name: {
                    'start': int,     # 0-indexed, INCLUSIVE
                    'end': int,       # EXCLUSIVE (for read_fwf, slicing)
                    'type': str,      # 'string', 'decimal', 'int'
                    'nullable': bool
                }
            }
        }
        OR None if layout not found
    """
```

**Lookup Logic:**
1. Extract quarter from `quarter_vintage`: `"2025Q4"` → `"Q4"`
2. Try specific quarter: `(dataset, product_year, quarter)` tuple lookup
3. Fallback to annual: `(dataset, product_year, None)`
4. Return `None` if not found

**Example:**
```python
layout = get_layout(product_year="2025", quarter_vintage="2025Q4", dataset="pprrvu")
# Looks up: ("pprrvu", "2025", "Q4"), then ("pprrvu", "2025", None)
# Returns: {'version': 'v2025.4.1', 'columns': {...}, 'min_line_length': 165}
```

**Critical Semantics:**
- **`end` is EXCLUSIVE** - For `read_fwf(colspecs=[(start, end)])` and `line[start:end]` slicing
- **Dict order ≠ positional order** - MUST sort columns by `start` before building colspecs
- **Column names MUST match schema** - See §7.3 for alignment requirements
- **`min_line_length`** = minimum observed data-line length (excluding `\n`); parsers MUST treat as heuristic for header detection only, NOT a hard requirement

**Common Pitfalls:**
- ❌ Positional args: `get_layout("2025", "pprrvu", "Q4")` swaps dataset/quarter → returns None
- ❌ Inclusive end: `{'start': 0, 'end': 5}` expecting 6 chars (only gets 5: indices 0-4)
- ❌ Hardcoded skiprows: CMS headers vary by release; detect data start dynamically

**CI Enforcement:**
- CI MUST verify `end` is exclusive by reconstructing a synthetic line and ensuring `read_fwf` reproduces exact boundaries (see §7.4 for test snippets)

**v2.0 Future Enhancement (Informative - YAML Source of Truth):**

**Note:** This section describes a FUTURE enhancement. Current implementations (v1.0-v1.4) use Python dict layouts and are fully compliant. YAML migration is optional and NOT required.

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

### 7.3 Layout-Schema Alignment (v1.3 - CRITICAL)

**NORMATIVE REQUIREMENT:** Layout column names MUST exactly match schema contract column names.

**Hard Rules (CI-enforceable):**

1. **MUST** sort fixed-width columns by `start` position before building colspecs
2. **MUST** treat layout `end` as EXCLUSIVE for `read_fwf()` and `line[start:end]` slicing
3. **MUST** detect data start dynamically (no hardcoded skiprows; use pattern matching + min_line_length)
4. **MUST** call `get_layout(product_year=..., quarter_vintage=..., dataset=...)` using keyword arguments
5. **MUST** use explicit data-line detection pattern - default: `^[A-Z0-9]{5}$` at first natural key colspec (e.g., HCPCS for PPRRVU/GPCI); dataset-specific patterns SHOULD be documented in layout metadata `data_start_pattern` field

**Alignment Checklist (Pre-Implementation):**

Before implementing parser:
1. ✅ Load schema contract: `cms_{dataset}_v1.0.json`
2. ✅ List natural keys: `schema['natural_keys']`
3. ✅ List all required columns: `schema['columns'].keys()`
4. ✅ Verify layout has ALL required columns (exact names, case-sensitive)
5. ✅ Verify natural key columns present in layout
6. ✅ Measure actual data line length (do NOT guess `min_line_length`)

**Validation Guard (Post-Parse):**

Add after normalization, before categorical validation:

```python
# In parser, after _normalize_column_names(df):
required_cols = set(schema['columns'].keys())
actual_cols = set(df.columns)
missing = required_cols - actual_cols

if missing:
    # Include first row sample for debugging
    sample = df.head(1).to_dict('records')[0] if len(df) > 0 else {}
    raise LayoutMismatchError(
        f"DataFrame missing required schema columns: {missing}. "
        f"Layout may be out of sync with schema. First row sample: {sample}"
    )
```

**Common Misalignments:**

| Schema Contract Has | Layout Had | Fix | Version Bump |
|---------------------|------------|-----|--------------|
| `rvu_work` | `work_rvu` | Rename layout column | Patch (v2025.4.0→v2025.4.1) |
| `modifier` | ❌ MISSING | Add at position 5:7 | Minor (v2025.4→v2025.5) |
| `effective_from` | ❌ MISSING | Inject from metadata in parser | No bump (parser handles) |
| `rvu_malp` | `mp_rvu` | Rename layout column | Patch (v2025.4.0→v2025.4.1) |

**Example: PPRRVU Alignment (Real Fix from commit 7ea293e):**

**Before (v2025.4.0 - BROKE PARSER):**
```python
PPRRVU_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 200,  # ❌ Too strict (actual data = 173 chars)
    'columns': {
        'hcpcs': {'start': 0, 'end': 5},  # ✓ Matches schema
        # 'modifier' MISSING!  # ❌ Natural key absent → uniqueness check fails
        'work_rvu': {'start': 61, 'end': 65},  # ❌ Schema expects rvu_work
        'mp_rvu': {'start': 85, 'end': 89},  # ❌ Schema expects rvu_malp
    }
}
# Result: 0/7 tests passing, KeyError: 'rvu_work'
```

**After (v2025.4.1 - FIXED):**
```python
PPRRVU_2025D_LAYOUT = {
    'version': 'v2025.4.1',
    'min_line_length': 165,  # ✅ Measured actual data (173 chars) with margin
    'columns': {
        'hcpcs': {'start': 0, 'end': 5},  # ✓ Matches schema
        'modifier': {'start': 5, 'end': 7},  # ✅ Added natural key
        'rvu_work': {'start': 61, 'end': 65},  # ✅ Renamed to match schema
        'rvu_malp': {'start': 85, 'end': 89},  # ✅ Renamed to match schema
    }
}
# Result: 7/7 tests passing, production-ready
```

**Debugging Time Saved:** 2+ hours (from KeyError to working parser)

**CI Enforcement (Future):**

Add to schema contract validator:

```python
def validate_layout_schema_alignment(layout: Dict, schema: Dict) -> None:
    """Enforce layout-schema column name alignment."""
    schema_cols = set(schema['columns'].keys())
    layout_cols = set(layout['columns'].keys())
    
    missing = schema_cols - layout_cols
    if missing:
        raise ValidationError(f"Layout missing schema columns: {missing}")
    
    # Check for common anti-patterns (API names in layout)
    if 'work_rvu' in layout_cols and 'rvu_work' in schema_cols:
        raise ValidationError(
            "Layout uses 'work_rvu' but schema expects 'rvu_work'. "
            "Layout must use schema column names, not API names."
        )
```

### 7.4 CI Test Snippets (v1.3 - Copy/Paste Guards)

**Purpose:** Enforce Hard Rules from §7.3 with automated tests.

**1. Colspecs Sorted by Start:**

```python
def test_layout_colspecs_sorted(layout):
    """Verify columns are sorted by start position."""
    cols = list(layout['columns'].items())
    for i in range(1, len(cols)):
        prev_start = cols[i-1][1]['start']
        curr_start = cols[i][1]['start']
        assert prev_start <= curr_start, \
            f"Columns not sorted: {cols[i-1][0]} @ {prev_start} vs {cols[i][0]} @ {curr_start}"
```

**2. End Exclusive Sanity (Synthetic Probe):**

```python
def test_layout_end_exclusive(layout):
    """Verify end is exclusive by synthetic line test."""
    from io import StringIO
    import pandas as pd
    
    # Build synthetic line with sentinel at each end-1 position
    max_end = max(spec['end'] for spec in layout['columns'].values())
    line = [' '] * max_end
    for name, spec in layout['columns'].items():
        if spec['end'] > 0:
            line[spec['end'] - 1] = 'X'  # Sentinel at last included char
    synthetic = ''.join(line)
    
    # Parse with read_fwf
    colspecs = [(s['start'], s['end']) for s in layout['columns'].values()]
    names = list(layout['columns'].keys())
    df = pd.read_fwf(StringIO(synthetic), colspecs=colspecs, names=names, header=None)
    
    # Verify each column ends with sentinel
    for col in df.columns:
        val = str(df[col].iloc[0]).strip()
        assert val.endswith('X') or val == '', \
            f"Column {col} did not capture sentinel (end may not be exclusive)"
```

**3. Key Columns Present After FWF:**

```python
def test_parser_natural_keys_present(df, schema):
    """Verify natural key columns exist in parsed DataFrame."""
    required = set(schema['natural_keys'])
    actual = set(df.columns)
    missing = required - actual
    assert not missing, f"Missing natural key columns: {sorted(missing)}"
```

**4. Dynamic Data Start Enforced:**

```python
def test_parser_reports_dynamic_skiprows(metrics):
    """Verify parser reports dynamic header detection."""
    assert 'skiprows_dynamic' in metrics, \
        "Parser must report skiprows_dynamic in metrics"
    assert metrics['skiprows_dynamic'] >= 0, \
        f"skiprows_dynamic must be non-negative, got {metrics['skiprows_dynamic']}"
    assert 'data_start_pattern' in metrics, \
        "Parser must report data_start_pattern used for detection"
```

**5. Layout Column Names Match Schema:**

```python
def test_layout_schema_column_name_alignment(layout, schema):
    """Verify fixed-width layout column names match schema contract exactly."""
    schema_cols = set(schema['columns'].keys())
    layout_cols = set(layout['columns'].keys())
    missing = schema_cols - layout_cols
    assert not missing, f"Layout missing schema columns: {sorted(missing)}"
```

**Usage:**
```python
# In tests/ingestion/test_layout_compliance.py
@pytest.mark.parametrize("dataset,year,quarter", [
    ("pprrvu", "2025", "Q4"),
    ("gpci", "2025", "Q1"),
])
def test_layout_ci_guards(dataset, year, quarter):
    layout = get_layout(dataset=dataset, product_year=year, quarter_vintage=f"{year}{quarter}")
    test_layout_colspecs_sorted(layout)
    test_layout_end_exclusive(layout)
```

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

**Implementation Pattern (v1.1 - Phase 0 Commit 4):**
```python
# See cms_pricing/ingestion/parsers/_parser_kit.py for full implementation

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

# Returns ValidationResult with:
# - valid_df: Valid rows with categorical dtypes applied
# - rejects_df: Rejected rows with row_id, schema_id, release_id, reason codes
# - metrics: Validation metrics (reject_rate, columns_validated, reject_rate_by_column)
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

### 8.5 Error Code Severity Table & Uniqueness Policies (v1.3)

**Canonical Error Code Table:**

All parsers use these default severities unless explicitly overridden:

| Error Code | Default Severity | Parser Action | Artifact | Notes |
|------------|------------------|---------------|----------|-------|
| `ENCODING_ERROR` | **BLOCK** | Raise `UnicodeDecodeError` | — | Cannot decode file with any codec |
| `DIALECT_UNDETECTED` | **BLOCK** | Raise `ParseError` | — | CSV/TSV/delimiter ambiguity unresolved |
| `LAYOUT_MISMATCH` | **BLOCK** | Raise `LayoutMismatchError` | — | Cannot continue parsing |
| `FIELD_MISSING` | **BLOCK** | Raise `SchemaRegressionError` | — | Required column absent |
| `TYPE_CAST_ERROR` | **BLOCK** | Raise `ValueError` | — | Cannot cast to required type |
| `KEY_VIOLATION` | **BLOCK** | Raise `DuplicateKeyError` | — | Primary key constraint |
| `ROW_DUPLICATE` | **Varies by dataset** | Raise (BLOCK) or quarantine (WARN) | `rejects.parquet` (if WARN) | See table below |
| `CATEGORY_UNKNOWN` | **WARN** | Quarantine | `rejects.parquet` | Unknown categorical value |
| `REFERENCE_MISS` | **WARN** | Quarantine | `rejects.parquet` | Code not in ref set |
| `OUT_OF_RANGE` | **WARN** | Quarantine | `rejects.parquet` | Value outside bounds |
| `BOM_DETECTED` | **INFO** | Log + metric only | — | BOM stripped successfully |
| `ENCODING_FALLBACK` | **INFO** | Log + metric only | — | Fell back to CP1252/Latin-1 |

**Severity Precedence:** Dataset override > Global default > Function default parameter

**Per-Dataset Natural Key Uniqueness Policies:**

For `ROW_DUPLICATE` errors, severity varies by dataset business rules:

| Dataset | Severity | Rationale |
|---------|----------|-----------|
| **PPRRVU** | **BLOCK** | Critical reference data; duplicates indicate CMS data quality issue |
| **Conversion Factor** | **BLOCK** | Single value per (cf_type, effective_from); duplicates are errors |
| **ANES CF** | **BLOCK** | Single value per (locality, effective_from) |
| **GPCI** | **WARN** | May have valid overlapping effective dates during transitions |
| **Locality** | **WARN** | County-locality mappings may overlap temporarily |
| **OPPSCAP** | **WARN** | May have multiple modifiers for same HCPCS |

**Configuration Pattern:**

```python
# In parser module (e.g., pprrvu_parser.py):
UNIQUENESS_SEVERITY = ValidationSeverity.BLOCK  # Or WARN per table above

# In parser normalize stage:
unique_df, dupes_df = check_natural_key_uniqueness(
    df,
    natural_keys=NATURAL_KEYS,
    severity=UNIQUENESS_SEVERITY,  # Dataset-specific
    schema_id=metadata['schema_id'],
    release_id=metadata['release_id']
)
```

**Behavior:**

**When BLOCK:**
- Raises `DuplicateKeyError` immediately
- Stops processing (no partial ingestion)
- Error contains `duplicates` list for debugging

**When WARN:**
- Returns duplicates in `dupes_df`
- Continues processing with unique rows in `unique_df`
- Duplicates written to `rejects.parquet` with full provenance

**Future - Dataset Config:**
```python
# Could externalize to config file:
SEVERITY_OVERRIDES = {
    'pprrvu': {'ROW_DUPLICATE': 'BLOCK'},
    'gpci': {'ROW_DUPLICATE': 'WARN'},
}
```

---

## 9. Error Taxonomy

### 9.1 Exception Hierarchy (v1.3)

All parsers use a common exception hierarchy for consistent error handling:

**Base Exception:**
```python
class ParseError(Exception):
    """Base exception for all parser errors."""
    pass
```

**Specific Exceptions:**

**1. DuplicateKeyError** - Natural key violations
```python
class DuplicateKeyError(ParseError):
    def __init__(self, message: str, duplicates: Optional[List[Dict]] = None):
        super().__init__(message)
        self.duplicates = duplicates  # List of duplicate key combinations
```
- **Usage:** Raised when `severity=BLOCK` and duplicate natural keys detected
- **When:** `check_natural_key_uniqueness()` with BLOCK severity
- **Contains:** List of duplicate key combinations for debugging

**2. CategoryValidationError** - Invalid categorical values
```python
class CategoryValidationError(ParseError):
    def __init__(self, field: str, invalid_values: List[Any]):
        self.field = field
        self.invalid_values = invalid_values
```
- **Usage:** Raised when unknown categorical values found before domain casting
- **When:** Pre-validation before CategoricalDtype conversion
- **Contains:** Field name and list of invalid values

**3. LayoutMismatchError** - Fixed-width parsing failures
```python
class LayoutMismatchError(ParseError):
    pass
```
- **Usage:** Raised when layout doesn't match file structure
- **When:** Wrong column widths, missing layout, truncated lines
- **Contains:** String error message

**4. SchemaRegressionError** - Unexpected schema fields
```python
class SchemaRegressionError(ParseError):
    def __init__(self, message: str, unexpected_fields: Optional[List[str]] = None):
        self.unexpected_fields = unexpected_fields
```
- **Usage:** Raised when DataFrame has fields not in schema contract
- **When:** Banned columns appear (e.g., vintage_year in CF v2.0+)
- **Contains:** List of unexpected field names

**When to Raise vs Return in Rejects:**
- `DuplicateKeyError`: Raise if `severity=BLOCK`, return in rejects if `severity=WARN`
- `CategoryValidationError`: Return in rejects (soft failure, quarantine)
- `LayoutMismatchError`: Always raise (critical parsing failure, cannot continue)
- `SchemaRegressionError`: Always raise (contract violation, cannot continue)

**Location:** `cms_pricing/ingestion/parsers/_parser_kit.py`

### 9.2 Error Codes (v1.0)

**Error Codes:**
- `ENCODING_ERROR` - Cannot decode file with known encodings
- `DIALECT_UNDETECTED` - Cannot detect CSV dialect
- `HEADER_MISSING` - No header row found
- `FIELD_MISSING` - Required field not in data
- `TYPE_CAST_ERROR` - Cannot cast field to required type
- `OUT_OF_RANGE` - Value exceeds min/max constraints
- `REFERENCE_MISS` - Code not in reference set
- `ROW_DUPLICATE` - Duplicate natural key (maps to DuplicateKeyError in v1.3)
- `KEY_VIOLATION` - Primary key constraint violation
- `LAYOUT_MISMATCH` - Fixed-width layout doesn't match data (maps to LayoutMismatchError in v1.3)
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

### 14.6 Schema File Naming & Loading (v1.3)

**Filename Convention:**
- **Format:** `cms_{dataset}_v{major}.0.json`
- **Example:** `cms_pprrvu_v1.0.json`

**Internal Version:**
- Schema JSON contains: `"version": "{major}.{minor}"`
- **Example:** `"version": "1.1"` (inside `cms_pprrvu_v1.0.json`)

**Version Mismatch Pattern:**
- **Metadata schema_id:** `"cms_pprrvu_v1.1"` (authoritative, includes minor)
- **Filename:** `cms_pprrvu_v1.0.json` (stable, major only)
- **Parser MUST strip minor version to locate file**

**Rationale:**
- **Major version** = breaking changes (new file required)
- **Minor version** = additive changes (update existing file)
- **Filename stability** = predictable imports, no cascading renames

**Registry Load Pattern (package-safe):** Use `importlib.resources` to load contracts whether running from source or an installed package. A dev-only Path fallback is shown in comments.

```python
from importlib.resources import files
import json
from typing import Dict

def load_schema(schema_id: str) -> Dict:
    """Load schema contract, stripping minor version from ID (package-safe)."""
    # Strip minor: cms_pprrvu_v1.1 → cms_pprrvu_v1.0
    major_id = schema_id.rsplit('.', 1)[0] + '.0'

    # Package-safe load from installed resources
    schema_path = files('cms_pricing.ingestion.contracts').joinpath(f'{major_id}.json')
    with schema_path.open('r', encoding='utf-8') as f:
        return json.load(f)

    # --- Dev-only fallback (example) ---
    # from pathlib import Path
    # with open(Path(__file__).parent.parent / 'contracts' / f'{major_id}.json', encoding='utf-8') as f:
    #     return json.load(f)
```

**Example:**
- `schema_id = "cms_pprrvu_v1.1"` → loads `cms_pprrvu_v1.0.json`
- `schema_id = "cms_conversion_factor_v2.0"` → loads `cms_conversion_factor_v2.0.json`

**CI Warning (SHOULD):**
- Warn if schema ID and filename diverge unexpectedly
- Example: `cms_pprrvu_v2.1` but file still named `v1.0.json`

**Version Examples:**
- **v1.0 → v1.1:** Add `precision` field → update `cms_pprrvu_v1.0.json` to version `"1.1"`
- **v1.1 → v2.0:** Remove `vintage_year` → create new file `cms_conversion_factor_v2.0.json`

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

## 20.1 Common Pitfalls & Anti-Patterns (v1.3)

**Top 5 Issues from PPRRVU Implementation (Real Debugging - commit 7ea293e)**

### Anti-Pattern 1: Positional get_layout() Arguments

**Problem:**
```python
layout = get_layout("2025", "pprrvu", "Q4")  # ❌ Wrong parameter order!
```
Swaps `dataset` and `quarter` → returns `None` → `LayoutMismatchError`.

**Fix:**
```python
# Use keyword arguments (self-documenting):
layout = get_layout(
    product_year="2025",
    quarter_vintage="2025Q4",
    dataset="pprrvu"
)
```

**Why #1:** Easiest to regress; happens during copy/paste from examples.

**See:** §7.2 Layout Registry API for signature.

---

### Anti-Pattern 2: Layout-Schema Column Name Mismatch

**Problem:**
```python
# Layout:          Schema:
'work_rvu'    ≠   'rvu_work'
'mp_rvu'      ≠   'rvu_malp'
```
Result: `KeyError: 'rvu_work'` in categorical validation (2 hours debugging).

**Fix:**
```python
# BEFORE coding parser, verify alignment:
schema = json.load(open('cms_pprrvu_v1.0.json'))
layout = get_layout("2025", "2025Q4", "pprrvu")

schema_cols = set(schema['columns'].keys())
layout_cols = set(layout['columns'].keys())

assert schema_cols.issubset(layout_cols), f"Missing: {schema_cols - layout_cols}"
# Fails fast with: Missing: {'rvu_work', 'modifier', 'rvu_malp'}
```

**Solution:** Rename layout columns to match schema exactly, bump layout version.

---

### Anti-Pattern 3: min_line_length Too High

**Problem:**
```python
'min_line_length': 200  # Assumption, not measurement!
```
Actual data is 173 chars → All 94 rows skipped → Empty DataFrame → `KeyError: 'hcpcs'`

**Fix:**
```bash
# Measure actual data first:
head -20 data.txt | tail -10 | awk '{print length}'
# Output: 173, 173, 173, ...

# Set conservatively (with margin):
'min_line_length': 165  # Allows 173-char lines + 8-char margin
```

**Debugging time:** 30 minutes

---

### Anti-Pattern 4: Category Validation After Cast

**Problem:**
```python
df['status'] = df['status'].astype(CategoricalDtype(['A', 'B']))
# Unknown value 'Z' silently becomes NaN! ❌
```

**Fix:**
```python
# Validate BEFORE casting:
allowed = {'A', 'B'}
invalid = set(df['status'].dropna().unique()) - allowed
if invalid:
    raise CategoryValidationError('status', list(invalid))

# Only then cast:
df['status'] = df['status'].astype(CategoricalDtype(['A', 'B']))
```

**See:** §8.2.1 Categorical Domain Validation

---

### Anti-Pattern 5: Hash Includes Metadata

**Problem:**
```python
row_hash = hashlib.sha256(df.to_dict('records')).hexdigest()
# Includes release_id, parsed_at! ❌
```
Result: Same row, different release → different hash (not deterministic).

**Fix:**
```python
# Exclude metadata columns:
data_cols = [c for c in df.columns 
             if c not in ['release_id', 'parsed_at', 'source_file_sha256', ...]]
row_hash = hashlib.sha256(df[data_cols].to_dict('records')).hexdigest()
```

**Or use kit:**
```python
from cms_pricing.ingestion.parsers._parser_kit import finalize_parser_output
final_df = finalize_parser_output(df, natural_keys, schema)  # Handles exclusions
```

---

---

### Anti-Pattern 6: BOM in Header Names

**Problem:**
```python
df = pd.read_csv(file_obj)
# Column becomes: '\ufeffhcpcs' (U+FEFF BOM prefix)
assert 'hcpcs' in df.columns  # ❌ Fails!
```
Result: "Missing required column: hcpcs" even though it's there.

**Fix:**
```python
# Detect and strip BOM BEFORE parsing:
from cms_pricing.ingestion.parsers._parser_kit import detect_encoding

content = file_obj.read()
encoding, content_clean = detect_encoding(content)  # Strips BOM
df = pd.read_csv(StringIO(content_clean.decode(encoding)))

# Verify after normalization:
assert 'hcpcs' in df.columns, f"Missing hcpcs, got: {list(df.columns)}"
```

---

### Anti-Pattern 7: Duplicate Headers Mangled by Pandas

**Problem:**
```python
# CSV has: description,amount,description
df = pd.read_csv(file_obj)
# Pandas auto-renames: description, amount, description.1
```
Result: Schema validation fails (expects `description` column, not `description.1`).

**Fix:**
```python
# Detect and reject duplicate headers (add to §21.1 template):
df = pd.read_csv(file_obj)
dupes = [c for c in df.columns if '.' in c]
if dupes:
    raise ParseError(f"Duplicate column headers detected (pandas mangled): {dupes}")

# OR normalize intentionally (if duplicates are expected):
df.columns = [normalize_column_name(col.split('.')[0]) for col in df.columns]
```

---

### Anti-Pattern 8: Excel Date/Float Coercion

**Problem:**
```python
df = pd.read_excel(file_obj)
# Excel date 01/02/2025 → locale-dependent interpretation
# Float 19.31 → 19.30999999999... (binary precision loss)
```

**Fix:**
```python
# Read Excel as strings, then cast:
df = pd.read_excel(file_obj, dtype=str)

# Cast with schema-driven precision:
from cms_pricing.ingestion.parsers._parser_kit import canonicalize_numeric_col

for col, spec in schema['columns'].items():
    if spec['type'] in ['decimal', 'float']:
        df[col] = canonicalize_numeric_col(
            df[col],
            precision=spec.get('precision', 6),
            rounding_mode=spec.get('rounding_mode', 'HALF_UP')
        )
```

---

### Anti-Pattern 9: Whitespace & NBSP in Codes

**Problem:**
```python
df['hcpcs'] = '99213 '  # Trailing space
df['status'] = 'A\u00a0'  # Non-breaking space (NBSP)

# Category validation fails:
allowed = {'A', 'B'}
actual = set(df['status'].unique())  # {'A\u00a0'}
assert actual.issubset(allowed)  # ❌ Fails
```

**Fix:**
```python
# Strip whitespace + replace NBSP before validation:
for col in categorical_columns:
    df[col] = df[col].str.strip()  # Regular spaces
    df[col] = df[col].str.replace('\u00a0', ' ')  # NBSP → space
    df[col] = df[col].str.strip()  # Trim again

# Then validate:
allowed = get_categorical_columns(schema)[col]['enum']
invalid = set(df[col].dropna().unique()) - set(allowed)
if invalid:
    raise CategoryValidationError(col, list(invalid))
```

---

### Anti-Pattern 10: CRLF Leftovers in Fixed-Width

**Problem:**
```python
# Windows file with CRLF endings:
line = 'HCPCS00123  19.31\r\n'
token = line[0:5]  # 'HCPCS'
length = len(line)  # 22 (includes \r\n)

# min_line_length check fails or last char misaligned
```

**Fix:**
```python
# Strip CRLF when measuring and detecting data start:
def detect_data_start(file_obj, layout):
    for i, raw_line in enumerate(file_obj):
        line = raw_line.rstrip(b'\r\n').decode('utf-8', errors='ignore')
        if len(line) >= layout['min_line_length']:
            # Check pattern at first key position
            ...
```

---

**More Issues?** See PPRRVU parser commit history (7ea293e) for full debugging trail.

---

## 21. Change Log

| Version | Date | Summary | PR |
|---------|------|---------|-----|
| **1.6** | **2025-10-16** | **Package safety + CI guards.** Type: Non-breaking (implementation guidance). **Fixed:** §14.6 schema loader (importlib.resources for package-safe loading); §6.1 ParseResult example (join invariant assert). **Added:** §7.4 CI guard #5 (layout-schema column name alignment test). **Motivation:** Package installation support + prevent layout-schema drift. **Impact:** Prevents import errors in installed packages + 1-2h debugging per parser. | TBD |
| **1.5** | **2025-10-16** | **Production hardening: 10 surgical fixes.** Type: Non-breaking (implementation guidance). **Fixed:** §6.5 normalize example (ParseResult consumption + file writes); §21.1 Step 1 (head-sniff + seek pattern); row-hash impl (Decimal quantization not float); §5.3 rejects/quarantine naming consistency; §21.1 join invariant (assert total = valid + rejects); §20.1 duplicate header guard; §21.1 Excel/ZIP guidance; §14.6 schema loader path (relative to module); §7.2 layout names = schema names (cross-ref §7.3). **Motivation:** User feedback pre-CF parser - eliminate last gotchas. **Impact:** 10 fixes prevent 2-3 hours debugging per parser × 4 = 8-12 hours saved. | TBD |
| **1.4** | **2025-10-16** | **Template hardening + CMS-specific pitfalls.** Type: Non-breaking (guidance). **Added:** §20.1 Anti-Patterns 6-10 (BOM in headers, duplicate headers, Excel coercion, whitespace/NBSP, CRLF leftovers) - real issues from CMS file parsing. **Fixed:** §1 summary consistency (ParseResult not DataFrame); §7.2 example layout (rvu_work not work_rvu per §7.3 requirement); YAML section tagged as "Future (informative)" with current=dicts note. **Motivation:** Pre-document common CMS file issues before GPCI/ANES/OPPSCAP parsers to prevent 1-2 hours debugging each. **Impact:** 5 new pitfalls × 4 parsers = 4-8 hours saved. | ffae4e9 |
| **1.3** | **2025-10-16** | **Normative clarifications from PPRRVU implementation.** Type: Non-breaking (guidance, enforcement rules). **Added (Normative):** §7.3 Layout-Schema Alignment with 5 MUST rules (colspec sorting, end exclusivity, dynamic header detection, keyword args, explicit data-line pattern) and validation guard; §8.5 Error Code Severity Table (12 codes) with per-dataset policies; §20.1 Common Pitfalls (top 5 anti-patterns with fixes, reordered by frequency). **Added (Guidance):** §6.6 Schema vs API Naming Convention (DB canonical vs presentation, mapper location); §14.6 Schema File Naming & Loading (version stripping pattern); §7.4 CI Test Snippets (colspec order, end exclusivity, key columns, dynamic skiprows). **Enhanced:** §7.2 Layout Registry API (signature, semantics, end=EXCLUSIVE, min_line_length as heuristic, CI enforcement); §9.1 Exception Hierarchy (custom error types); §21.1 Implementation Template (validation guard). **CI Evolution:** New validations for layout exclusivity, colspec sorting, data-start detection (pattern-based), key-column guard. **Motivation:** PPRRVU parser (commit 7ea293e) hit 3 major issues: layout-schema column mismatch (2h debug), missing natural keys (1h debug), min_line_length too strict (30min debug). **Impact:** Prevents 3-4 hours debugging per parser × 5 remaining = 15-20 hours saved. | 5fd7fd4 |
| 1.2 | 2025-10-16 | **Phase 1 readiness: Parser implementation template and acceptance criteria.** Added §21 Parser Implementation Template with standardized 9-step structure (detect encoding → parse format → normalize → cast → validate → inject metadata → finalize → metrics → return ParseResult). Includes per-parser acceptance checklist with routing, validation, precision, performance, and testing requirements. Added golden-first development workflow (extract fixture → write test → implement → verify → commit). Provides copy/paste checklist for parser PRs. Supports Phase 1 parser implementations (PPRRVU, CF, GPCI, ANES, OPPSCAP, Locality). | #TBD |
| 1.1 | 2025-10-15 | **Production-grade enhancements for MPFS implementation.** Upgraded parser return type to `ParseResult(data, rejects, metrics)` for cleaner separation (§5.3). Added content sniffing via `file_head` parameter for magic byte/BOM detection (§6.2). Changed row hash to full 64-char SHA-256 digest with schema-driven precision per column (§5.2). Added CP1252 to encoding cascade for Windows file support (§5.2). Implemented explicit categorical domain validation - pre-check before conversion to prevent silent NaN coercion (§8.2.1). Pinned exact natural keys per dataset including `effective_from` for time-variant data. Added vintage field duplication guidance (§6.4). Enhanced test strategy with explicit IO boundary rules - parsers return data, ingestors write files (§14.1). Updated acceptance criteria with all v1.1 requirements (§18). These changes prevent technical debt and enable immediate production deployment. | #TBD |
| 1.0 | 2025-10-15 | Initial adoption of parser contracts standard. Defines public contract requirements (pandas DataFrame return type), metadata injection pattern with three vintage fields, tiered validation (BLOCK/WARN/INFO), formal row hash specification for reproducibility, SemVer for parsers/schemas/layouts, integration with DIS normalize stage, and comprehensive testing strategy. Documents v1.0 implementation (pandas, helper-based quarantine, Python dict layouts, filename routing) and v1.1 enhancements (ParseResult, YAML layouts, magic byte routing). Establishes governance for shared parser infrastructure used by MPFS, RVU, OPPS, and future ingestors. | #TBD |

---

## 21. Parser Implementation Template (Phase 1 Guide)

### 21.1 Standard Parser Structure

Every parser MUST follow this 9-step structure for consistency and maintainability:

```python
def parse_{dataset}(
    file_obj: IO[bytes],
    filename: str,
    metadata: Dict[str, Any]
) -> ParseResult:
    """
    Parse {dataset} file to canonical schema.
    
    Per STD-parser-contracts v1.1 §6.1.
    
    Args:
        file_obj: Binary file stream
        filename: Filename for format detection
        metadata: Required metadata from ingestor
        
    Returns:
        ParseResult(data, rejects, metrics)
    """
    import time
    start_time = time.perf_counter()
    
    # Step 1: Detect encoding (head-sniff to keep memory bounded)
    head = file_obj.read(8192)  # First 8KB for BOM/encoding detection
    encoding, _ = detect_encoding(head)
    file_obj.seek(0)  # Reset for full parse
    logger.info("Encoding detected", encoding=encoding)
    
    # Read full content after encoding known
    content_clean = file_obj.read()
    
    # Step 2: Parse format (fixed-width via layout OR CSV/Excel)
    if _is_fixed_width(content_clean):
        layout = get_layout(dataset, metadata['product_year'], metadata['quarter'])
        df = _parse_fixed_width(content_clean, encoding, layout)
    elif filename.endswith('.xlsx'):
        # Excel: Read as dtype=str, then cast with Decimal (never trust Excel's inferred dates/floats)
        df = pd.read_excel(file_obj, dtype=str)
    elif filename.endswith('.zip'):
        # ZIP: Iterate members, route each through same contract, concat results
        import zipfile
        with zipfile.ZipFile(file_obj) as zf:
            dfs = []
            for member in zf.namelist():
                with zf.open(member) as f:
                    # Recurse through router for each member
                    member_result = parse_{dataset}(f, member, metadata)
                    member_result.data['source_zip_member'] = member
                    dfs.append(member_result.data)
            df = pd.concat(dfs, ignore_index=True)
    else:
        df = _parse_csv(content_clean, encoding)
    
    # Step 3: Normalize column names (canonical snake_case)
    df = _normalize_column_names(df)
    
    # Step 4: Cast dtypes (explicit, no coercion)
    df = _cast_dtypes(df)
    
    # Step 5: Load schema contract
    schema = load_schema(metadata['schema_id'])
    
    # Step 6: Categorical validation (use parser kit)
    cat_result = enforce_categorical_dtypes(
        df, schema,
        natural_keys=schema['natural_keys'],
        schema_id=metadata['schema_id'],
        release_id=metadata['release_id'],
        severity=ValidationSeverity.WARN
    )
    
    # Step 7: Inject metadata columns
    for col in ['release_id', 'vintage_date', 'product_year', 'quarter_vintage']:
        cat_result.valid_df[col] = metadata[col]
    cat_result.valid_df['source_filename'] = filename
    cat_result.valid_df['parsed_at'] = pd.Timestamp.utcnow()
    
    # Step 8: Finalize (hash + sort via parser kit)
    final_df = finalize_parser_output(
        cat_result.valid_df,
        schema['natural_keys'],
        schema
    )
    
    # Step 9: Build comprehensive metrics
    parse_duration = time.perf_counter() - start_time
    metrics = {
        **cat_result.metrics,
        'parser_version': PARSER_VERSION,
        'encoding_detected': encoding,
        'parse_duration_sec': parse_duration,
        'schema_id': metadata['schema_id']
    }
    
    # Join invariant: total_rows = valid + rejects (catches bugs)
    assert metrics['total_rows'] == len(final_df) + len(cat_result.rejects_df), \
        f"Row count mismatch: {metrics['total_rows']} != {len(final_df)} + {len(cat_result.rejects_df)}"
    
    logger.info("Parse completed", rows=len(final_df), rejects=len(cat_result.rejects_df))
    
    return ParseResult(
        data=final_df,
        rejects=cat_result.rejects_df,
        metrics=metrics
    )
```

### 21.2 Per-Parser Acceptance Checklist

Copy/paste this checklist for each parser PR/commit:

**Routing & Natural Keys:**
- [ ] Correct dataset, schema_id, natural_keys from route_to_parser()
- [ ] Routing latency p95 ≤ 20ms
- [ ] Natural keys: uniqueness enforced, duplicates → NATURAL_KEY_DUPLICATE

**Validation:**
- [ ] Categoricals: unknowns/nulls handled per policy
- [ ] Rejects include: row_id, schema_id, release_id, validation_rule_id, validation_column, validation_context
- [ ] Join invariant: len(valid) + len(rejects) == len(input)

**Precision & Determinism:**
- [ ] Numerics: precision preserved (schema-driven rounding)
- [ ] Determinism: repeat → identical row_content_hash
- [ ] Chunked vs single-shot: identical outputs

**Performance:**
- [ ] 10K rows: validate ≤ 300ms
- [ ] E2E soft guard ≤ 1.5s (hard fail at 2s)

**Testing:**
- [ ] Golden fixture created with SHA-256 documented
- [ ] 5 unit tests: golden, schema, encoding, error, empty
- [ ] Integration test: route → parse → validate → finalize

**Documentation:**
- [ ] Comprehensive docstring with examples
- [ ] Golden fixture README with source + hash
- [ ] Parser version constant (SemVer)

### 21.3 Golden-First Development Workflow

**Recommended approach for new parsers:**

1. **Extract golden fixture** (15 min)
   ```bash
   head -101 sample_data/source.txt > tests/fixtures/{dataset}/golden/sample.txt
   shasum -a 256 tests/fixtures/{dataset}/golden/sample.txt
   ```

2. **Write golden test first** (15 min)
   - Assert expected row count
   - Assert schema compliance
   - Assert deterministic hash
   - Run test (will fail - parser doesn't exist)

3. **Implement parser** (60-90 min)
   - Follow 9-step template
   - Use parser kit utilities (no duplication)
   - Test iteratively until golden test passes

4. **Add remaining tests** (20 min)
   - Schema compliance
   - Encoding variations
   - Error handling
   - Empty files

5. **Commit** (5 min)
   - Small, atomic commit
   - Link to golden fixture

**Benefits:**
- ✅ Test-driven (spec before code)
- ✅ Determinism built-in (hash verification)
- ✅ Regression prevention (any change breaks hash)

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

