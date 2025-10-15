# Parser Contracts Standard

**Status:** Draft v1.0  
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

The Parser Core converts heterogeneous CMS source files (CSV/TSV/TXT/XLSX/ZIP/XML) into canonical, Arrow-backed, schema-validated tabular datasets with deterministic outputs and full provenance. Parsers are selected by a content-sniffing router using an external layout registry. Artifacts include the canonical table, rejects/quarantine set, metrics, and provenance metadata.

**Why this matters**: Parsing quality is the single largest driver of downstream reliability and cost. Standardizing parser behavior, observability, and contracts prevents silent data drift, enables reproducible releases, and accelerates onboarding of new ingestors.

**Key principles:**
- Parsers are **public contracts**, not private methods
- **Metadata injection** by ingestor, not hardcoded in parsers
- **Explicit dtypes** - no silent coercion of codes to floats
- **Deterministic output** - sorted by natural key + content hash
- **Tiered validation** - block on critical, warn on soft failures
- **Comprehensive provenance** - full audit trail

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

1. **Arrow-backed canonical tables** → Performance, zero-copy interchange, strict dtypes
2. **Deterministic outputs** → Sort by stable composite key + `row_content_hash` for idempotency
3. **Content sniffing router** → Don't rely on filenames; detect dialect, headers, magic bytes
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
- `file_bytes` or stream (file object)
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
- Detect encoding: UTF-8 (default) → Latin-1 (fallback) → CP1252
- Strip BOM markers: UTF-8 BOM (0xEF 0xBB 0xBF), UTF-16 LE/BE
- Detect CSV dialect: delimiter, quote char, escape char
- Log encoding detection results for debugging

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

### 5.3 Output Artifacts

**Primary Output:**
- `parsed.parquet` - Canonical data with metadata columns
- Partitioned by: `dataset_id/release_id/schema_id/`
- Format: Arrow/Parquet with explicit schema

**Quarantine Output:**
- `rejects.parquet` - Rows that failed validation
- Columns: All original columns + `error_code`, `error_message`, `raw_row`, `line_no`, `quarantined_at`
- Created even in Phase 1 (minimal validation)

**Metrics Output:**
- `metrics.json` - Per-file parse metrics (see §10)

**Provenance Output:**
- `provenance.json` - Full metadata, library versions, git SHA

**Optional:**
- `sample_100.parquet` - First 100 rows for quick inspection

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

**Parser Function Signature:**

```python
from typing import IO, Dict, Any
import pandas as pd

def parse_{dataset}(
    file_obj: IO,
    filename: str,
    metadata: Dict[str, Any],
    schema_version: str = "v1.0"
) -> pd.DataFrame:
    """
    Parse {dataset} file to canonical schema.
    
    Args:
        file_obj: File object (bytes or text stream)
        filename: Filename for format detection
        metadata: Required metadata from ingestor (see §6.4)
        schema_version: Schema contract version
        
    Returns:
        DataFrame with canonical schema + metadata columns
        
    Raises:
        ParseError: If parsing fails
        SchemaValidationError: If schema validation fails
    
    Contract guarantees:
        - Explicit dtypes (no object for codes)
        - Sorted by natural key
        - row_content_hash for idempotency
        - Metadata columns injected
        - Encoding/BOM handled
    """
```

**All parsers MUST follow this signature.**

### 6.2 Router Contract

**Router Function Signature:**

```python
def route_to_parser(filename: str) -> Tuple[str, str, Callable]:
    """
    Route filename to parser configuration.
    
    Args:
        filename: Source filename
        
    Returns:
        (dataset_name, schema_id, parser_func)
        
    Raises:
        ValueError: If no parser found
    
    Note: Presence of parser_func IS the status.
          No separate "status" string needed.
    """
```

**Implementation:** `cms_pricing/ingestion/parsers/__init__.py`

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

**Implementation:** `cms_pricing/ingestion/parsers/layout_registry.py`

**Structure:**
```python
LAYOUT_REGISTRY = {
    ('pprrvu', '2025', 'Q4'): {
        'version': 'v2025.4.0',  # SemVer
        'min_line_length': 200,
        'columns': {
            'hcpcs': {'start': 0, 'end': 5, 'type': 'string'},
            'work_rvu': {'start': 61, 'end': 65, 'type': 'decimal'},
            # ... all columns
        }
    }
}
```

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

Emit for each file parsed:

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
- `v1.0.0 → v1.1.0`: Added row_content_hash column
- `v1.0.0 → v2.0.0`: Changed primary key sort order

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
- Parser code is internal (not public API)
- Schema contracts can be published (public CMS data)
- Quarantine artifacts are internal only

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
    metadata = {'release_id': 'test', ...}
    df = parse_pprrvu(sample_csv, "test.csv", metadata)
    
    assert 'hcpcs' in df.columns
    assert df['hcpcs'].dtype.name == 'category'
    assert len(df) > 0
```

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
- [ ] Accepts metadata dict, injects metadata columns (§6.4)
- [ ] Returns DataFrame with explicit dtypes (no object for codes)
- [ ] Output sorted by natural key
- [ ] Includes row_content_hash column
- [ ] Handles encoding/BOM explicitly
- [ ] Logs with release_id, file_sha256, schema_id
- [ ] Creates quarantine artifact for WARN-level failures
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

### Phase 1: Core Parsers (Current)

**Goal:** Extract and standardize PPRRVU, GPCI, Locality, ANES, OPPSCAP parsers

**Deliverables:**
- 5 parser modules following contract
- Metadata injection implemented
- Quarantine artifacts created
- IngestRun tracking integrated

**Timeline:** 2-3 hours (in progress)

### Phase 2: Enhanced Validation (Next)

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
| 1.0 | 2025-10-15 | Initial adoption of parser contracts standard. Defines public contract requirements, metadata injection pattern, tiered validation, three vintage fields requirement, SemVer for parsers/schemas/layouts, integration with DIS pipeline, and comprehensive testing strategy. Establishes governance for shared parser infrastructure used by MPFS, RVU, OPPS, and future ingestors. | #TBD |

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

