# Parser Contracts Standard

**Status:** Draft v2.0  
**Owners:** Data Platform Engineering  
**Consumers:** Data Engineering, MPFS Ingestor, RVU Ingestor, OPPS Ingestor, API Teams, QA Guild  
**Change control:** ADR + PR review  
**Review cadence:** Quarterly (first business Monday)  

**Companion Documents:**
- **STD-parser-contracts-impl-v2.0.md:** Implementation guidance, code patterns, templates
- **REF-parser-routing-detection-v1.0.md:** Router architecture and format detection
- **REF-parser-quality-guardrails-v1.0.md:** Validation, errors, metrics patterns
- **RUN-parser-qa-runbook-prd-v1.0.md:** QA procedures, checklists, SLAs
- **REF-parser-reference-appendix-v1.0.md:** Reference tables and examples

**Cross-References:**
- **STD-data-architecture-prd-v1.0.md:** DIS pipeline and normalize stage requirements
- **STD-data-architecture-impl-v1.0.md:** BaseDISIngestor implementation patterns
- **STD-qa-testing-prd-v1.0.md:** QTS compliance requirements
- **PRD-mpfs-prd-v1.0.md:** MPFS ingestor uses parser contracts for RVU bundle
- **PRD-rvu-gpci-prd-v0.1.md:** RVU parsing requirements and fixed-width layout specifications
- **PRD-opps-prd-v1.0.md:** OPPS ingestor uses parser contracts
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## 1. Summary

Parsers return `ParseResult(data, rejects, metrics)`; the ingestor writes Arrow/Parquet artifacts with deterministic outputs and full provenance. The system converts heterogeneous CMS source files (CSV/TSV/TXT/XLSX/ZIP) into schema-validated tabular datasets. Parsers are selected by a router using filename patterns and optional content sniffing. The normalize stage writes DataFrames to Arrow/Parquet format.

**Why this matters**: Parsing quality is the single largest driver of downstream reliability and cost. Standardizing parser behavior, observability, and contracts prevents silent data drift, enables reproducible releases, and accelerates onboarding of new ingestors.

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

## 5. Scope & High-Level Requirements

**For detailed implementation guidance, see:**
- **STD-parser-contracts-impl-v2.0.md:** Input processing, encoding, normalization, type handling, output format
- **REF-parser-routing-detection-v1.0.md:** Router architecture, format detection, layout registry
- **REF-parser-quality-guardrails-v1.0.md:** Validation requirements, error taxonomy, metrics
- **RUN-parser-qa-runbook-prd-v1.0.md:** Performance SLAs, pre-implementation checklists

**High-Level Scope:**

**Supported Formats:** CSV/TSV, Fixed-width TXT, XLSX, ZIP, XML, JSONL  
**Not Supported:** PDF (must be pre-extracted to structured format)

**Output Format:** Arrow/Parquet with explicit dtypes, sorted by natural key, with deterministic row hashes

**Metadata Requirements:** All parsers inject `release_id`, `vintage_date`, `product_year`, `quarter_vintage`, `source_uri`, `file_sha256`, `parsed_at`, `row_content_hash`

---

## 6. Contracts

### 6.1 Function Contract (Python)

**Parser Function Signature:**

```python
from typing import IO, Dict, Any, NamedTuple, Callable
import pandas as pd

class ParseResult(NamedTuple):
    """Parser output per STD-parser-contracts v2.0"""
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
            - metrics: Dict (total_rows, valid_rows, reject_rows, 
                            parse_duration_sec, encoding_detected)
        
    Raises:
        ParseError: If parsing fails critically
        ValueError: If required metadata missing
    
    Contract guarantees:
        - Returns ParseResult (not bare DataFrame)
        - Explicit dtypes (no object for codes)
        - Sorted by natural key
        - 64-char row_content_hash computed per spec
        - Metadata columns injected
        - Encoding/BOM handled with CP1252 support
        - Deterministic output (same input → same hash)
        - No filesystem writes (ingestor persists artifacts)
    """
```

**All parsers MUST return ParseResult.**

### 6.2 Router Contract

**Router Function Signature:**

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
    """
```

**Implementation:** See REF-parser-routing-detection-v1.0.md for detailed routing architecture

### 6.3 Schema Contract Format

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
- Do NOT duplicate as data columns in schemas
- If business logic requires a different vintage concept (e.g., "valuation year"), use distinct naming

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
            
            # Call parser - returns ParseResult
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

### 6.6 Schema vs API Naming Convention

**Problem:** CMS datasets appear in both internal storage (database) and external API responses, requiring different naming conventions.

**Solution:** Parsers output **schema format** (DB canonical). API layer transforms to **presentation format** at serialization boundary.

**Schema Format (DB Canonical):**
- **Pattern:** `{component}_{type}` with prefix grouping
- **Example:** `rvu_work`, `rvu_pe_nonfac`, `rvu_pe_fac`, `rvu_malp`
- **Used by:** Parsers, database tables, schema contracts, ingestors

**API Format (Presentation):**
- **Pattern:** `{type}_{component}` with suffix grouping
- **Example:** `work_rvu`, `pe_rvu_nonfac`, `pe_rvu_fac`, `mp_rvu`
- **Used by:** API responses, Pydantic schemas, external documentation

**Transformation Boundary:**

Transform at **API serialization**, NOT in parser or ingestor:

```python
# Parser outputs schema format (DB canonical):
result = parse_pprrvu(file_obj, filename, metadata)
# result.data has columns: rvu_work, rvu_pe_nonfac, rvu_pe_fac, rvu_malp

# Database stores schema format (no transformation):
db_table.insert(result.data)

# API router transforms for response (presentation format):
from cms_pricing.mappers import schema_to_api

@router.get("/rvu/{hcpcs}")
async def get_rvu(hcpcs: str):
    df = await db.query_rvu(hcpcs)  # Has rvu_work
    api_df = schema_to_api(df)       # Now has work_rvu
    return RVUResponse.from_dataframe(api_df)
```

**Benefits:**
- **Single source of truth:** Schema contract is canonical
- **Clean layer separation:** Parser→DB (no transform), DB→API (transform)
- **Reversible:** Both `schema_to_api()` and `api_to_schema()` available
- **API evolution:** Can change presentation without affecting storage

---

## 7. Router & Layout Registry

**See:** REF-parser-routing-detection-v1.0.md for comprehensive router architecture, format detection patterns, layout registry implementation, and layout-schema alignment rules.

**Key Concepts:**
- Two-phase detection (extension + content sniffing)
- ZIP handling with inner file routing
- Layout registry API and SemVer versioning
- Format detection flowcharts

---

## 8. Validation Requirements

**See:** REF-parser-quality-guardrails-v1.0.md for detailed validation patterns, tiered thresholds, reference validation hooks, and quarantine artifact formats.

**Key Concepts:**
- Schema validation
- Categorical domain validation
- Tiered validation (BLOCK/WARN/INFO)
- Quarantine artifact structure

---

## 9. Error Taxonomy

**See:** REF-parser-quality-guardrails-v1.0.md §2 for complete exception hierarchy, error codes, and error handling patterns.

**Key Exception Types:**
- `ParseError`: Critical parsing failures
- `DuplicateKeyError`: Natural key violations
- `CategoryValidationError`: Domain validation failures
- `LayoutMismatchError`: Fixed-width layout issues
- `SchemaRegressionError`: Schema contract violations

---

## 10. Observability & Metrics

**See:** REF-parser-quality-guardrails-v1.0.md §3 for per-file metrics structure, aggregate metrics, safe calculation patterns, and logging requirements.

**Key Metrics:**
- Per-file: total_rows, valid_rows, reject_rows, parse_duration_sec, encoding_detected
- Aggregate: files_parsed, rows_discovered, reject_rate, parse_throughput
- Logging: Structured logging with release_id, schema_id, parser_version

---

## 11. Provenance Requirements

Parsers track full provenance via IngestRun model and metadata injection:

**Provenance Fields:**
- `release_id`: Release identifier
- `dataset_id`: Dataset name
- `schema_id`: Schema contract version
- `parser_version`: Parser SemVer
- `source_uri`: Source URL
- `file_sha256`: File checksum
- `vintage_date`: Publication date
- `product_year`: Valuation year
- `quarter_vintage`: Quarter identifier
- `parsed_at`: Parse timestamp

**See:** STD-data-architecture-prd-v1.0.md §4 for comprehensive provenance requirements

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

**Special case:** Any change to row-hash spec (column order in hash, delimiter, normalization rules) requires **MAJOR** parser version bump to ensure cross-version reproducibility.

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

**See:** RUN-parser-qa-runbook-prd-v1.0.md for comprehensive testing workflows, golden-first development, and acceptance checklists.

**Key Test Types:**
- Unit tests (parser function independently)
- Golden-file tests (deterministic output verification)
- Schema drift tests (contract compliance)
- Layout version tests (change detection)
- Performance tests (SLA validation)

---

## 15. Implementation

**See Companion Documents:**
- **STD-parser-contracts-impl-v2.0.md:** 11-step parser template, implementation patterns, code examples
- **RUN-parser-qa-runbook-prd-v1.0.md:** Pre-implementation checklists, golden-first workflow, acceptance criteria

**Implementation Status:** Tracked in §17 of archived v1.11 document

---

## 16. Acceptance Criteria

**Parser Implementation:**
- ✅ Parser follows function signature contract (§6.1)
- ✅ Returns `ParseResult(data, rejects, metrics)`
- ✅ Accepts metadata dict, injects metadata columns (§6.4)
- ✅ Returns DataFrame with explicit dtypes (no object for codes)
- ✅ Output sorted by natural key
- ✅ Includes 64-char row_content_hash column
- ✅ Handles encoding with CP1252 cascade
- ✅ Content sniffing via file_head parameter (§6.2)
- ✅ Logs with release_id, file_sha256, schema_id, encoding_detected
- ✅ Pre-checks categorical domains before conversion
- ✅ Separates valid/reject rows explicitly
- ✅ Validates against schema contract
- ✅ Performance meets SLAs

**Testing:**
- ✅ Unit tests pass for parser function
- ✅ Golden-file test produces identical output (hash-verified)
- ✅ Schema validation test catches contract violations
- ✅ Layout version test detects breaking changes
- ✅ Performance test meets benchmarks
- ✅ Integration test with ingestor passes

**Documentation:**
- ✅ Parser function has comprehensive docstring
- ✅ Schema contract registered in `contracts/`
- ✅ Layout registered in `layout_registry.py` (if fixed-width)
- ✅ Cross-referenced from dataset PRD

**Compliance:**
- ✅ No private method calls to other ingestors
- ✅ Backwards compatible (if replacing existing parser)
- ✅ Deprecation notices added (if applicable)
- ✅ CI passes all quality gates

**Versioning:**
- ✅ Changing hash spec or schema column order requires **MAJOR** parser version bump
- ✅ Breaking changes properly documented with migration path

---

## 17. Risks & Mitigations

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

## 18. Out of Scope

**Deferred to Other Standards/Systems:**
- Fetching/scraping: See STD-scraper-prd-v1.0.md
- API serving: See STD-api-architecture-prd-v1.0.md
- Complex business transforms: See STD-data-architecture-prd-v1.0.md §3.5 (Enrich)
- Price calculations: Downstream services
- PDF extraction: Pre-processing required
- Real-time streaming: Batch-oriented only

---

## 19. Glossary

**DIS**: Data Ingestion & Serving pipeline (Land → Normalize → Enrich → Publish)

**Natural Key**: Business identifier for uniqueness (e.g., HCPCS + modifier + effective_from)

**ParseResult**: NamedTuple output (data, rejects, metrics) from parser functions

**Quarantine**: Artifact containing rejected rows with validation error details

**Row Content Hash**: 64-char SHA-256 hash of row data (deterministic, excludes metadata)

**Schema Contract**: JSON specification defining column types, constraints, and business rules

**Layout Registry**: Python dict (planned: YAML) mapping fixed-width column positions by year/quarter

**Vintage Fields**: Three temporal identifiers (vintage_date, product_year, quarter_vintage)

**Content Sniffing**: Format detection using magic bytes and BOM (not filename alone)

**Tiered Validation**: BLOCK (critical), WARN (soft), INFO (statistical) severity levels

---

## 20. Cross-References

**Companion Documents (v2.0 Modularization):**
- STD-parser-contracts-impl-v2.0.md
- REF-parser-routing-detection-v1.0.md
- REF-parser-quality-guardrails-v1.0.md
- RUN-parser-qa-runbook-prd-v1.0.md
- REF-parser-reference-appendix-v1.0.md

**Related Standards:**
- STD-data-architecture-prd-v1.0.md
- STD-data-architecture-impl-v1.0.md
- STD-qa-testing-prd-v1.0.md
- STD-scraper-prd-v1.0.md
- STD-api-architecture-prd-v1.0.md

**Dataset PRDs:**
- PRD-mpfs-prd-v1.0.md
- PRD-rvu-gpci-prd-v0.1.md
- PRD-opps-prd-v1.0.md

**Master Catalog:**
- DOC-master-catalog-prd-v1.0.md

---

## 21. Source Section Mapping (v1.11 → v2.0)

**For reference during transition:**

This document contains content from the following sections of the archived `STD-parser-contracts-prd-v1.11-ARCHIVED.md`:

| v2.0 Section | Original v1.11 Section | Lines in v1.11 |
|--------------|------------------------|----------------|
| §1 Summary | §1 Summary | 23-48 |
| §2 Goals & Non-Goals | §2 Goals & Non-Goals | 50-70 |
| §3 Users & Scenarios | §3 Users & Scenarios | 72-102 |
| §4 Key Decisions | §4 Key Decisions & Rationale | 104-119 |
| §6 Contracts | §6 Contracts (all subsections) | 897-1296 |
| §12 Compatibility & Versioning | §12 Compatibility & Versioning | 2770-2822 |
| §13 Security & Compliance | §13 Security & Compliance | 2825-2842 |
| §16 Acceptance Criteria | §18 Acceptance Criteria | 3118-3159 |
| §17 Risks | §19 Risks & Mitigations | 3162-3173 |
| §18 Out of Scope | §16 Out of Scope | (referenced in §2) |
| §19 Glossary | (New - extracted from inline definitions) | Various |

**Sections moved to companion documents:**
- §5 Scope & Requirements → STD-parser-contracts-impl-v2.0.md §1
- §7 Router & Layout → REF-parser-routing-detection-v1.0.md §2-4
- §8-10 Validation/Errors/Metrics → REF-parser-quality-guardrails-v1.0.md §2-4
- §21 Implementation Template → STD-parser-contracts-impl-v2.0.md §2 + RUN-parser-qa-runbook-prd-v1.0.md §1-2
- Appendices A-D → REF-parser-reference-appendix-v1.0.md

**Archived source:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md` (kept for 2-week transition)

---

## 22. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| **2025-10-17** | **v2.0** | **Team** | **MAJOR: Modularized parser contracts into 6 focused documents.** Split v1.11 (4,477 lines) into: (1) STD-parser-contracts-prd-v2.0.md (core policy, ~500 lines), (2) STD-parser-contracts-impl-v2.0.md (implementation guide, ~1,200 lines), (3) REF-parser-routing-detection-v1.0.md (router/layout architecture, ~800 lines), (4) REF-parser-quality-guardrails-v1.0.md (validation/errors/metrics, ~900 lines), (5) RUN-parser-qa-runbook-prd-v1.0.md (QA procedures/checklists, ~800 lines), (6) REF-parser-reference-appendix-v1.0.md (reference tables, ~300 lines). **Benefits:** 3-4x faster AI context loading, improved governance compliance, independent versioning, clearer separation of concerns (policy vs implementation vs operations). **Breaking change:** Document structure reorganized; all content preserved with cross-references. See companion documents for full details. |
| 2025-10-17 | v1.11 | Team | Added §21.6 Incremental Implementation Strategy. Documents three-phase approach (single format → additional formats → edge cases) for 40% faster time to first test. Added §21.4 Step 2c Real Data Format Variance Analysis. Enhanced pre-implementation checklist with 5-step variance detection process. Updated §21.4 Step 2b with verify_layout_positions.py tool guidance. |
| 2025-10-17 | v1.10 | Team | Added §5.2.3 Alias Map Best Practices & Testing. Comprehensive guidance on structuring alias maps, handling year/case/parenthetical variations, and testing strategies. Added §5.2.4 Defensive Type Handling Patterns. Documents safe type conversion using _parser_kit.py utilities to handle integer strings, empty values, and scientific notation. Expanded §7.1 Router & Format Detection with 6 subsections detailing two-phase detection, ZIP handling, flowchart, pitfalls, and checklist. Added §10.1.1 and §10.3 Safe Metrics Calculation Patterns. Provides patterns for safe_min_max, safe_count, safe_percentage to handle empty values, nulls, and mixed types gracefully. |
| 2025-10-16 | v1.9 | Team | Added §21.3 Tiered Validation Thresholds. Replaces binary pass/fail with INFO/WARN/ERROR severity levels to support both test fixtures and production data without test-only flags. |
| 2025-10-16 | v1.8 | Team | Added §21.4 Format Verification Pre-Implementation Checklist. 7-step checklist to verify all format variations (TXT, CSV, XLSX, ZIP) before coding, preventing 4-6 hours of debugging. |
| 2025-10-15 | v1.7 | Team | Added §21 Parser Implementation Template. Documents 11-step standard structure for parser development, validation phases, and row hash computation. |
| 2025-10-02 | v1.6 | Team | Enhanced §6 with ParseResult return type, content sniffing, and improved function signatures. |
| 2025-09-30 | v1.5 | Team | Added §7.3 Layout-Schema Alignment rules and §7.4 CI Test Snippets. |
| 2025-09-28 | v1.4 | Team | Expanded §12 with detailed SemVer rules for parsers, schemas, and layouts. |
| 2025-09-25 | v1.3 | Team | Added §6.6 Schema vs API Naming Convention, §9.1 Exception Hierarchy, §8.5 Error Code Severity Table, §20.1 Common Pitfalls & Anti-Patterns. |
| 2025-09-20 | v1.2 | Team | Added §14.6 Schema File Naming & Loading with importlib.resources pattern. |
| 2025-09-15 | v1.1 | Team | Enhanced validation requirements, added reference hooks. |
| 2025-09-10 | v1.0 | Team | Initial parser contracts standard. |

---

*End of Core Standard Document*

*For implementation details, see companion documents listed in §20.*

