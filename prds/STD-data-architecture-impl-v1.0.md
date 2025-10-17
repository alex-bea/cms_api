# Data Architecture Implementation Guide (v1.0)

**Status:** Draft v1.0.2  
**Owners:** Platform/Data Engineering  
**Consumers:** Ingestor Implementers, Data Engineers  
**Change control:** PR review (no ADR required for code examples)  
**Companion Of:** [STD-data-architecture-prd-v1.0.md](STD-data-architecture-prd-v1.0.md)  
**Document Type:** Implementation Guide

> **Companion to:** [STD-data-architecture-prd-v1.0.md](STD-data-architecture-prd-v1.0.md)
>
> This **implementation guide** shows **how to implement** the DIS (Discovery → Ingestion → Serving) 
> architecture. It provides **integration patterns**, **code examples**, practical **mapper guardrails**, 
> and end-to-end workflows for building DIS-compliant data pipelines.

**Cross-References:**
- **STD-data-architecture-prd-v1.0.md:** Main standard (requirements, architecture, policies)
- **STD-parser-contracts-prd-v1.0.md:** Parser contracts v1.1 for normalize stage implementation (ParseResult return type, 64-char hashing, schema-driven precision)
- **STD-scraper-prd-v1.0.md:** Scraper patterns and discovery manifests
- **STD-observability-monitoring-prd-v1.0.md:** Observability requirements
- **STD-qa-testing-prd-v1.0.md:** Testing requirements
- **REF-cms-pricing-source-map-prd-v1.0.md:** CMS dataset source mappings

---

## 0. Overview & Quick Start

### 0.1 Purpose

This guide provides practical implementation patterns for the DIS (Discovery → Ingestion → Serving) pipeline defined in `STD-data-architecture-prd-v1.0.md`. Use this guide to:

- Understand the `BaseDISIngestor` interface
- Implement each pipeline stage correctly
- Use centralized components (factories, validators, observability)
- Follow operational best practices
- Bootstrap new ingestors from templates
- Reference working examples

### 0.2 Quick Navigation

| I want to... | Go to Section |
|--------------|---------------|
| Understand the DIS interface | §1. DIS Pipeline Implementation Reference |
| Use shared components | §2. Centralized Components & Factories |
| Declare schemas and validation | §3. Schema Contracts & Validation |
| Configure and operate ingestors | §4. Operational Patterns |
| See code examples | §5. Implementation Reference Table |
| Build a new ingestor | §6. Step-by-Step: Building a New Ingestor |
| Study working examples | §7. Working Examples |
| Use code templates | §8. Code Templates |
| Certify compliance | §9. Compliance & Certification |
| Troubleshoot issues | §10. Troubleshooting & Common Issues |

### 0.3 Prerequisites

Before implementing an ingestor:
- ✅ Read `STD-data-architecture-prd-v1.0.md` (main standard)
- ✅ Understand the 5-stage DIS pipeline (Land → Validate → Normalize → Enrich → Publish)
- ✅ Have a dataset PRD or create one following the template in main PRD §13
- ✅ Confirm source mapping in `REF-cms-pricing-source-map-prd-v1.0.md`
- ✅ Understand your data source (scraper available or manual discovery)

---

## 1. DIS Pipeline Implementation Reference

### 1.1 Canonical Interface

The DIS pipeline is implemented via `BaseDISIngestor` abstract class:

**Location:** `cms_pricing/ingestion/contracts/ingestor_spec.py:197`  
**Orchestrator:** `cms_pricing/ingestion/run/dis_pipeline.py:41`

All ingestors MUST extend `BaseDISIngestor` and implement the required methods and properties.

### 1.2 Stage Method Mapping

| PRD Stage | BaseDISIngestor Method | Input | Output | Reference |
|-----------|------------------------|-------|--------|-----------|
| **Discovery** | `discover_source_files() -> List[SourceFile]` | Scraper manifest or manual list | Source file metadata | `ingestor_spec.py:220` |
| **Land** | `land_stage(source_files) -> RawBatch` | Source file list | Raw batch with downloaded files | `ingestor_spec.py:230` |
| **Validate** | `validate_stage(raw_batch) -> Tuple[RawBatch, List[ValidationResult]]` | Raw batch | Validated batch + results | `ingestor_spec.py:240` |
| **Normalize** | `normalize_stage(raw_batch) -> AdaptedBatch` | Validated batch | Adapted/normalized batch | `ingestor_spec.py:250` |
| **Enrich** | `enrich_stage(adapted_batch) -> StageFrame` | Adapted batch | Enriched stage frame | `ingestor_spec.py:260` |
| **Publish** | `publish_stage(stage_frame) -> Dict[str, Any]` | Stage frame | Publish result metadata | `ingestor_spec.py:270` |

---

### 1.3 Transformation Boundaries: Parser vs Normalize vs Enrich (Added 2025-10-17)

**Intent:** Make pipelines predictable and auditable by drawing a hard line between layout-faithful parsing and business/semantic transforms.

**Non-goals:** No business joins, no derived keys, no imputations in parsers.

---

#### A) Responsibilities (Who Does What)

| Stage | Do | Don't |
|-------|-----|-------|
| **Parse (Raw→Stage)** | Read bytes layout-faithfully; map headers to canonical names; set explicit dtypes; inject metadata (`release_id`, `vintage_date`, `quarter_vintage`, `file_sha256`); produce deterministic sort + `row_content_hash`; write rejects on structural/schema errors. | No reference joins; no derivations (FIPS from name, etc.); no imputation; no filtering except hard rejects. |
| **Normalize (light)** | Zero-pad codes, trim/whitespace, unit/coercion to canonical (e.g., decimals); rename columns to standard; enforce contract (Schema Registry). | No cross-dataset enrichment or lookups. |
| **Enrich** | Join to `/ref` (FIPS, ZIP↔ZCTA, Gazetteer, CPT/HCPCS/POS); compute `mapping_confidence` and apply tie-breakers; create "latest-effective" views. | Change raw semantics; silently drop conflicts; override upstream values. |

---

#### B) Decision Tree (Where Does a Change Belong?)

1. **Needs external lookup** (any ref table)? → **Enrich**
2. **Only formatting/type/units?** → **Normalize**
3. **Fixing a parser layout/width/header?** → **Parse** (update `layout_registry` + bump SemVer)
4. **Deriving keys** (FIPS, locality from ZIP)? → **Enrich** with precedence & thresholds from Appendix J
5. **Removing records?** Only if hard rule violation (quarantine with `violation_rule_id`)—otherwise keep and flag.

---

#### C) Contracts & I/O Shape

**Parser outputs** must conform to `schema_id` (SemVer) with explicit dtypes (Arrow decimals for RVUs/CFs).

**Normalize** may only perform contract-preserving changes (no column add/remove except metadata).

**Enrich** writes new columns (`*_fips`, `locality_code`, `mapping_confidence`) and must log which `/ref` vintage was used.

**Required metadata columns (all stages):**  
`release_id`, `vintage_date`, `product_year`, `quarter_vintage`, `source_filename`, `source_file_sha256`, `row_content_hash`

---

#### D) Example: MPFS (PPRRVU + GPCI + Locality)

**Parse:**
- Read PPRRVU fixed-width/CSV → canonical names
- Decimals for `rvu_*`
- Inject metadata
- No GPCI application

**Normalize:**
- Zero-pad `locality_code`, `state_fips`
- Coerce `status_code`, `global_days` to domains

**Enrich:**
- ZIP→locality via precedence (PIP > crosswalk > nearest ≤1.0 mi)
- Join GPCI
- Emit `mapping_confidence`
- Block on unknown HCPCS/CPT/POS

---

#### E) Quality Gates & Alerts

**Block (critical):**
- Schema contract fail
- Unknown HCPCS/CPT/POS
- Invalid FIPS
- Missing locality/GPCI key

**Warn + quarantine:**
- ZIP↔ZCTA disagreements
- NBER vs haversine deltas (median > 1.0 mi or p95 > 3.0 mi)
- Nearest fallback > 1.0 mi → mark ambiguous

**Emit per-file metrics:**
- Rows, nulls on criticals, encoding used, parse time

---

#### F) Tests (Must Pass)

**Parsers:**
- Golden fixed-width/CSV → exact columns/dtypes
- BOM/encoding matrix
- Property-based fuzz on widths
- Deterministic `row_content_hash`

**Boundary tests:**
- Assert no `/ref` joins appear before Enrich
- Assert Normalize never adds/removes business columns

**Enrich:**
- Precedence/tie-breaker tests
- Thresholds (share sum ±0.01, distance deltas)

**Idempotency:**
- Re-run same inputs → identical checksums
- Older release after newer → newer remains current

---

#### G) Change Control

- **Parser layout changes** require `layout_registry` bump + ADR if breaking
- **Precedence/tie-breaker updates** require ADR (see Appendix J)

---

#### H) Real-World Example: Locality-County Crosswalk

**Problem:** CMS file has state/county NAMES, canonical schema needs FIPS codes

**Wrong Approach (One-Stage):**
```python
# ❌ WRONG: Parser derives FIPS (violates separation)
def parse_locality(file_obj, filename, metadata):
    df = parse_fixed_width(file_obj, LAYOUT)
    
    # BAD: Reference lookup in parser!
    df['state_fips'] = df['state_name'].map(STATE_NAME_TO_FIPS)
    df['county_fips'] = df.apply(
        lambda row: county_lookup(row['state_fips'], row['county_name']),
        axis=1
    )
    return ParseResult(data=df, rejects=rejects, metrics=metrics)
```

**Correct Approach (Two-Stage):**
```python
# ✅ CORRECT: Parse as-is, derive in enrich

# Stage 1: Parser (layout-faithful, no transforms)
def parse_locality_raw(file_obj, filename, metadata):
    """Parse LOCCO file exactly as CMS ships it."""
    df = parse_fixed_width(file_obj, LOCCO_LAYOUT)
    
    # Columns from file: mac, locality_id, state (NAME), county_name (NAMES)
    # No FIPS derivation - that's enrich stage!
    
    return ParseResult(data=df, rejects=rejects, metrics=metrics)

# Stage 2: Enrich (derive FIPS from names via reference tables)
def enrich_locality_fips(raw_df, ref_states, ref_counties, aliases):
    """Derive FIPS codes from state/county names."""
    
    # Load reference tables
    state_fips_map = load_state_crosswalk(ref_states)  # name → FIPS
    county_fips_map = load_county_crosswalk(ref_counties)  # (state_fips, name) → FIPS
    
    # Derive state FIPS
    df['state_fips'] = df['state'].map(state_fips_map)
    
    # Derive county FIPS (tiered matching: exact → alias → fuzzy)
    df['county_fips'] = df.apply(
        lambda row: match_county_to_fips(
            row['state_fips'],
            row['county_name'],
            county_fips_map,
            aliases
        ),
        axis=1
    )
    
    # Explode multi-county rows (e.g., "LOS ANGELES/ORANGE" → 2 rows)
    exploded = explode_counties(df)
    
    # Quarantine unmatched
    unmatched = exploded[exploded['county_fips'].isna()]
    valid = exploded[exploded['county_fips'].notna()]
    
    return EnrichResult(data=valid, quarantine=unmatched, metrics=...)
```

**Benefits of Two-Stage:**
- ✅ Parser stays simple (layout-faithful)
- ✅ Reference logic isolated (testable, reusable)
- ✅ Reference data versioned separately
- ✅ Audit trail clear (raw vs enriched)

---

#### I) Boundary Tests (Required)

**Test: Parser Doesn't Enrich**
```python
def test_locality_parser_no_fips_derivation():
    """Verify parser outputs raw columns, no FIPS derivation."""
    result = parse_locality_raw(fixture, 'LOCCO.txt', metadata)
    
    # Raw columns present
    assert 'mac' in result.data.columns
    assert 'state' in result.data.columns  # NAME, not state_fips
    assert 'county_name' in result.data.columns  # NAMES, not county_fips
    
    # Canonical columns NOT present (added in enrich)
    assert 'state_fips' not in result.data.columns
    assert 'county_fips' not in result.data.columns
```

**Test: Enrich Produces Canonical**
```python
def test_locality_enrich_derives_fips():
    """Verify enrich stage produces canonical schema."""
    raw_df = pd.DataFrame([
        {'mac': '10112', 'state': 'ALABAMA', 'county_name': 'ALL COUNTIES'}
    ])
    
    enriched = enrich_locality_fips(raw_df, ref_states, ref_counties, aliases)
    
    # Canonical columns present
    assert 'state_fips' in enriched.data.columns
    assert 'county_fips' in enriched.data.columns
    
    # Explosion occurred (ALL COUNTIES → 67 rows for Alabama)
    assert len(enriched.data) == 67
    assert enriched.data['state_fips'].iloc[0] == '01'
```

---

#### J) When to Use This Pattern

**Use two-stage (parse-as-is + enrich) when:**
- ✅ CMS file has names, canonical needs codes
- ✅ Requires external reference table
- ✅ Complex matching logic (aliases, fuzzy matching)
- ✅ One-to-many explosion (e.g., "ALL COUNTIES" → N rows)

**Use single-stage (parse directly) when:**
- ✅ File already has canonical values (FIPS codes present)
- ✅ Simple column renaming only
- ✅ No external lookups needed

**Reference Implementations:**
- **Two-stage:** Locality parser (§H above)
- **Single-stage:** GPCI parser (file has locality codes as-is)

---

**Cross-References:**
- **STD-data-architecture-prd §3.4** (Normalize stage requirements)
- **STD-data-architecture-prd §3.5** (Enrich stage requirements)
- **STD-parser-contracts-prd §6** (Parser contract boundaries)
- **planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md** (Detailed example)

---

### 1.4 Required Properties

All ingestors must implement these properties:

```python
@property
def dataset_name(self) -> str:
    """Dataset identifier (e.g., 'MPFS', 'RVU', 'OPPS')"""
    return "DATASET_NAME"

@property
def release_cadence(self) -> ReleaseCadence:
    """Release frequency: ANNUAL, QUARTERLY, MONTHLY, WEEKLY"""
    return ReleaseCadence.QUARTERLY

@property
def data_classification(self) -> DataClass:
    """Data classification: PUBLIC, INTERNAL, CONFIDENTIAL"""
    return DataClass.PUBLIC

@property
def contract_schema_ref(self) -> str:
    """Schema contract reference (e.g., 'cms.mpfs:v1.0')"""
    return "cms.dataset:v1.0"

@property
def validators(self) -> List[ValidationRule]:
    """List of validation rules for this dataset"""
    return self.validation_rules

@property
def slas(self) -> SlaSpec:
    """SLA specifications"""
    return self.sla_spec

@property
def outputs(self) -> OutputSpec:
    """Output specifications"""
    return self.output_spec
```

### 1.4 Pipeline Orchestrator

The pipeline orchestrator (`dis_pipeline.py:41`) executes stages in sequence:

```python
# Simplified orchestrator flow
async def run_dis_pipeline(ingestor: BaseDISIngestor, year: int, quarter: Optional[str] = None):
    # 1. Discovery
    source_files = await ingestor.discover_source_files()
    
    # 2. Land
    raw_batch = await ingestor.land_stage(source_files)
    
    # 3. Validate
    validated_batch, validation_results = await ingestor.validate_stage(raw_batch)
    
    # 4. Normalize
    adapted_batch = await ingestor.normalize_stage(validated_batch)
    
    # 5. Enrich
    stage_frame = await ingestor.enrich_stage(adapted_batch)
    
    # 6. Publish
    result = await ingestor.publish_stage(stage_frame)
    
    return result
```

### 1.5 Reference Implementations

Study these working examples:

| Dataset | File | Pattern | Key Features |
|---------|------|---------|--------------|
| **MPFS** | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py:55` | Composition | Reuses RVU scraper, creates curated views |
| **RVU** | `cms_pricing/ingestion/ingestors/rvu_ingestor.py:97` | Direct Links | Quarterly releases, fixed-width parsing |
| **OPPS** | `cms_pricing/ingestion/ingestors/opps_ingestor.py:52` | Quarterly Navigation | AMA license handling, addenda |

---

## 2. Centralized Components & Factories

### 2.1 AdapterFactory

**Location:** `cms_pricing/ingestion/adapters/data_adapters.py`

Provides dataset-specific adapters for data transformation:

```python
from cms_pricing.ingestion.adapters.data_adapters import AdapterFactory, AdapterConfig

# Create adapter for your dataset
adapter = AdapterFactory.create_adapter("mpfs", AdapterConfig())

# Use adapter to transform raw data
adapted_data = adapter.adapt(raw_data)
```

**Supported adapters:** `mpfs`, `rvu`, `opps`, `geography`

**When to use:** In `normalize_stage()` to transform raw data into canonical format.

### 2.2 ValidationEngine

**Location:** `cms_pricing/ingestion/validators/validation_engine.py`

Centralized validation execution:

```python
from cms_pricing.ingestion.validators.validation_engine import ValidationEngine

# Initialize in __init__
self.validation_engine = ValidationEngine()

# Use in validate_stage()
results = self.validation_engine.run_validations(data, self.validation_rules)
```

**Features:**
- Executes validation rules in parallel
- Collects and aggregates results
- Supports severity levels (CRITICAL, ERROR, WARNING, INFO)
- Generates validation reports

### 2.3 QuarantineManager

**Location:** `cms_pricing/ingestion/quarantine/dis_quarantine.py`

Manages failed records and quarantine workflows:

```python
from cms_pricing.ingestion.quarantine.dis_quarantine import QuarantineManager, QuarantineSeverity

# Initialize in __init__
self.quarantine_manager = QuarantineManager(str(Path(self.output_dir) / "quarantine"))

# Use when validation fails
self.quarantine_manager.quarantine_batch(
    batch_id=batch_id,
    records=failed_records,
    reason="Validation failed: missing required columns",
    severity=QuarantineSeverity.CRITICAL
)
```

**Quarantine severity levels:**
- `CRITICAL`: Pipeline must stop
- `HIGH`: Significant data quality issues
- `MEDIUM`: Moderate issues, may proceed with warnings
- `LOW`: Minor issues, informational

### 2.4 DISObservabilityCollector

**Location:** `cms_pricing/ingestion/observability/dis_observability.py`

Implements 5-pillar observability framework:

```python
from cms_pricing.ingestion.observability.dis_observability import DISObservabilityCollector

# Initialize in __init__
self.observability_collector = DISObservabilityCollector()

# Record metrics throughout pipeline
self.observability_collector.record_freshness(last_run, expected_cadence)
self.observability_collector.record_volume(rows_processed, rows_rejected)
self.observability_collector.record_schema(schema_version, drift_detected)
self.observability_collector.record_quality(validation_score, completeness_score)
self.observability_collector.record_lineage(source_files, transformations)

# Generate report at end
report = self.observability_collector.generate_report()
```

**5 Pillars:**
1. **Freshness**: Last run time, expected cadence, freshness score
2. **Volume**: Rows processed, rows rejected, volume score
3. **Schema**: Version, drift detection, schema score
4. **Quality**: Validation score, completeness, quality score
5. **Lineage**: Source files, transformations, lineage score

### 2.5 ReferenceDataManager

**Location:** `cms_pricing/ingestion/enrichers/dis_reference_data_integration.py`

Manages reference data joins and lookups:

```python
from cms_pricing.ingestion.enrichers.dis_reference_data_integration import (
    ReferenceDataManager, DISReferenceDataEnricher
)

# Initialize in __init__
self.reference_data_manager = ReferenceDataManager()

# Use in enrich_stage()
enricher = DISReferenceDataEnricher(self.reference_data_manager)
enriched_data = enricher.enrich(data, reference_sources)
```

**Common reference data:**
- Geography: ZIP→Locality crosswalks
- HCPCS codes: Code descriptions and metadata
- Locality: Carrier→Locality mappings
- FIPS codes: State/county codes

### 2.6 Component Initialization Pattern

**Canonical pattern from `mpfs_ingestor.py:58-68`:**

```python
def __init__(self, output_dir: str = "./data/ingestion/mpfs", db_session: Any = None):
    super().__init__(output_dir, db_session)
    
    # Initialize scraper
    self.scraper = CMSMPFSScraper(str(Path(self.output_dir) / "scraped"))
    
    # Initialize managers
    self.historical_manager = HistoricalDataManager(str(Path(self.output_dir) / "historical"))
    self.schema_registry = schema_registry
    self.validation_engine = ValidationEngine()
    self.quarantine_manager = QuarantineManager(str(Path(self.output_dir) / "quarantine"))
    self.observability_collector = DISObservabilityCollector()
    self.reference_data_manager = ReferenceDataManager()
    
    # Current run metadata
    self.current_release_id: Optional[str] = None
    self.current_batch_id: Optional[str] = None
    
    # Configuration
    self._dataset_name = "MPFS"
    self._release_cadence = ReleaseCadence.ANNUAL
    self._data_classification = DataClass.PUBLIC
    self._contract_schema_ref = "cms.mpfs:v1.0"
    
    # SLA and output specifications
    self.sla_spec = SlaSpec(...)
    self.output_spec = OutputSpec(...)
    
    # Validation rules
    self.validation_rules = self._create_validation_rules()
    
    # Schema contracts
    self.schema_contracts = self._load_schema_contracts()
```

---

## 3. Schema Contracts & Validation

### 3.1 Schema Contract Storage

**Location:** `cms_pricing/ingestion/contracts/`

Schema contracts are JSON files defining dataset structure:

```
cms_pricing/ingestion/contracts/
├── cms_pprrvu_v1.0.json          # PPRRVU schema
├── cms_gpci_v1.0.json             # GPCI schema
├── cms_oppscap_v1.0.json          # OPPS Cap schema
├── cms_anescf_v1.0.json           # Anesthesia CF schema
├── cms_localitycounty_v1.0.json   # Locality schema
└── schema_registry.py             # Schema registry
```

**Schema contract format:**

```json
{
  "dataset_name": "cms_pprrvu",
  "version": "1.0",
  "generated_at": "2025-09-30T20:15:34.438211",
  "columns": {
    "hcpcs": {
      "name": "hcpcs",
      "type": "string",
      "required": true,
      "description": "HCPCS code",
      "pattern": "^[A-Z0-9]{5}$"
    },
    "rvu_work": {
      "name": "rvu_work",
      "type": "decimal",
      "required": true,
      "description": "Work RVU",
      "min_value": 0.0,
      "max_value": 100.0
    }
  },
  "primary_keys": ["hcpcs", "modifier", "effective_from"],
  "partition_columns": ["effective_from"],
  "business_rules": ["HCPCS codes must be 5 characters"],
  "quality_thresholds": {
    "completeness": 0.99,
    "validity": 0.99
  }
}
```

### 3.2 Schema Registry Usage

**Location:** `cms_pricing/ingestion/contracts/schema_registry.py`

```python
from cms_pricing.ingestion.contracts.schema_registry import schema_registry

# Get schema contract
contract = schema_registry.get_contract("cms.mpfs", "1.0")

# Validate data against contract
is_valid = schema_registry.validate_data(dataframe, "cms.mpfs:v1.0")

# Register new contract
schema_registry.register_contract("cms.newdataset", "1.0", contract_json)
```

### 3.3 Validation Rule Declaration

**Pattern from `mpfs_ingestor.py:152-185`:**

```python
def _create_validation_rules(self) -> List[ValidationRule]:
    """Create validation rules for dataset"""
    return [
        ValidationRule(
            name="Required columns present",
            description="All required columns must be present",
            validator_func=self._validate_required_columns,
            severity=ValidationSeverity.CRITICAL
        ),
        ValidationRule(
            name="HCPCS code format",
            description="HCPCS codes must be 5 characters",
            validator_func=self._validate_hcpcs_format,
            severity=ValidationSeverity.ERROR
        ),
        ValidationRule(
            name="Status code valid",
            description="Status codes must be valid CMS codes",
            validator_func=self._validate_status_codes,
            severity=ValidationSeverity.ERROR
        ),
        ValidationRule(
            name="Row count drift",
            description="Row count within ±15% of previous vintage",
            validator_func=self._validate_row_count_drift,
            severity=ValidationSeverity.WARNING
        ),
        ValidationRule(
            name="RVU sum validation",
            description="RVU components sum correctly for payable items",
            validator_func=self._validate_rvu_sums,
            severity=ValidationSeverity.ERROR
        )
    ]

def _validate_required_columns(self, df: pd.DataFrame) -> List[ValidationResult]:
    """Validate required columns are present"""
    required = ["hcpcs", "rvu_work", "rvu_pe_nonfac", "rvu_pe_fac", "rvu_malp"]
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        return [ValidationResult(
            rule_id="required_columns",
            severity=ValidationSeverity.CRITICAL,
            message=f"Missing required columns: {', '.join(missing)}",
            failed_count=len(missing)
        )]
    return []
```

### 3.4 Reference Data Dependencies

Declare reference data sources:

```python
@property
def reference_data_sources(self) -> List[ReferenceDataSource]:
    return [
        ReferenceDataSource(
            name="locality_crosswalk",
            path="/ref/geography/locality_county.parquet",
            join_keys=["locality_id"],
            required=True
        ),
        ReferenceDataSource(
            name="hcpcs_codes",
            path="/ref/codes/hcpcs_master.parquet",
            join_keys=["hcpcs"],
            required=False
        )
    ]
```

---

## 4. Operational Patterns

### 4.1 Configuration Management

**Environment Variables:**

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/cms_pricing
TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/cms_pricing_test

# Cache
REDIS_URL=redis://localhost:6379/0

# Storage (if using S3)
AWS_S3_BUCKET=org-pricing-data
AWS_REGION=us-west-2

# Logging
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR

# API
API_KEYS=dev-key-123,prod-key-456
```

**Configuration in code:**

```python
from cms_pricing.config import settings

# Access configuration
db_url = settings.database_url
log_level = settings.log_level
```

**Secrets Management:**
- ❌ Never commit secrets to code
- ✅ Use environment variables for local development
- ✅ Use AWS Secrets Manager / Parameter Store for production
- ✅ Reference: `STD-api-security-and-auth-prd-v1.0.md`

### 4.2 Release & Batch ID Generation

**Release ID Format:** `{source}_{year}_{period}_{timestamp}`

```python
# Example from mpfs_ingestor.py:623
release_id = f"mpfs_{year}_{quarter or 'annual'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
# Result: "mpfs_2025_annual_20251015_143022"
```

**Batch ID Format:** UUID v4

```python
import uuid

batch_id = str(uuid.uuid4())
# Result: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

### 4.3 Logging Conventions

Use structured logging via `structlog`:

```python
import structlog

logger = structlog.get_logger()

# Info logging with context
logger.info("Starting MPFS ingestion", 
           year=year, 
           quarter=quarter, 
           release_id=release_id,
           batch_id=batch_id)

# Error logging with exception
logger.error("Validation failed", 
            filename=filename, 
            error=str(e),
            rule_id=rule.rule_id,
            exc_info=True)

# Warning with metrics
logger.warning("Row count drift detected",
              current_count=len(df),
              previous_count=historical_count,
              drift_pct=drift_percentage)
```

**Logging levels:**
- `DEBUG`: Detailed diagnostic information
- `INFO`: General informational messages
- `WARNING`: Warning messages for non-critical issues
- `ERROR`: Error messages for failures
- `CRITICAL`: Critical failures requiring immediate attention

### 4.4 Observability Events

Emit events for all major pipeline stages:

```python
# Pipeline started
logger.info("ingestion.started",
           dataset=self.dataset_name,
           release_id=release_id)

# Stage completed
logger.info("stage.land.completed",
           files_downloaded=len(source_files),
           total_bytes=total_bytes)

# Validation completed
logger.info("stage.validate.completed",
           validation_score=score,
           warnings=warning_count,
           errors=error_count)

# Pipeline completed
logger.info("ingestion.completed",
           duration_seconds=duration,
           rows_processed=row_count)

# Pipeline failed
logger.error("ingestion.failed",
            stage="validate",
            error=str(e),
            exc_info=True)
```

Reference: `STD-observability-monitoring-prd-v1.0.md` §3.2

### 4.5 SLA Enforcement

Define SLAs in ingestor properties:

```python
from cms_pricing.ingestion.contracts.ingestor_spec import SlaSpec

self.sla_spec = SlaSpec(
    max_processing_time_hours=24,      # Pipeline must complete in 24 hours
    freshness_alert_hours=120,         # Alert if data >5 days old
    quality_threshold=0.99,            # 99% data quality required
    availability_target=0.999          # 99.9% uptime target
)
```

Pipeline automatically monitors and alerts on SLA breaches.

---

## 5. Implementation Reference Table

### 5.1 Pattern → File → Key Methods

| Pattern | Dataset | File | Key Methods | Notes |
|---------|---------|------|-------------|-------|
| **Composition** | MPFS | `mpfs_ingestor.py:55` | `scrape_mpfs_files()` composes with RVU scraper | Reuses RVU discovery, adds CF/abstracts |
| **Direct Links** | RVU | `rvu_ingestor.py:97` | `discover_source_files()`, `land()` | Quarterly releases A/B/C/D |
| **Quarterly Navigation** | OPPS | `opps_ingestor.py:52` | `discover_files()`, `_land_stage()` | Handles AMA license interstitial |
| **Reference Data Join** | Geography | `cms_zip_locality_ingestor.py` | `_enrich_data()` | Census crosswalk joins |

### 5.2 Stage Implementation Examples

| Stage | Reference Implementation | Line | Key Pattern |
|-------|-------------------------|------|-------------|
| **Discovery** | `mpfs_ingestor.py` | 237-262 | Use scraper, return `List[SourceFile]` |
| **Land** | `mpfs_ingestor.py` | 264-319 | Download files, calculate checksums, create `RawBatch` |
| **Validate** | `mpfs_ingestor.py` | 321-365 | Structural + domain + statistical validation |
| **Normalize** | `mpfs_ingestor.py` | 429-468 | Parse ZIP/CSV/Excel to DataFrames |
| **Enrich** | `mpfs_ingestor.py` | 510-527 | Join reference data, compute derived fields |
| **Publish** | `mpfs_ingestor.py` | 529-552 | Create curated views, store in database |

### 5.3 Shared Infrastructure

| Component | File | Usage Example |
|-----------|------|---------------|
| **BaseDISIngestor** | `contracts/ingestor_spec.py:197` | Extend this class |
| **ValidationEngine** | `validators/validation_engine.py` | `run_validations(data, rules)` |
| **QuarantineManager** | `quarantine/dis_quarantine.py` | `quarantine_batch(batch_id, ...)` |
| **ObservabilityCollector** | `observability/dis_observability.py` | `record_freshness(...)` |
| **AdapterFactory** | `adapters/data_adapters.py` | `create_adapter("mpfs", config)` |
| **SchemaRegistry** | `contracts/schema_registry.py` | `get_contract("cms.mpfs", "1.0")` |
| **ReferenceDataManager** | `enrichers/dis_reference_data_integration.py` | `ReferenceDataManager()` |

### 5.4 Testing Patterns

| Test Type | Reference | File |
|-----------|-----------|------|
| **Unit Tests** | Stage method tests | `tests/ingestors/test_mpfs_ingestor_e2e.py` |
| **Integration** | Full pipeline test | `tests/ingestors/test_rvu_ingestor_e2e.py` |
| **Validation** | Rule execution tests | `tests/ingestors/test_rvu_validations.py` |
| **Schema Drift** | Contract tests | `tests/ingestors/test_rvu_parsers.py` |
| **Performance** | Load tests | `tests/ingestors/test_opps_ingestor_e2e.py` |

---

## 6. Step-by-Step: Building a New Ingestor

### 6.1 Prerequisites Checklist

Before starting:
-  Dataset PRD created (following `STD-data-architecture-prd-v1.0.md` §13)
-  Source confirmed in `REF-cms-pricing-source-map-prd-v1.0.md`
-  Scraper available or discovery method defined
-  Schema contract drafted (JSON format)
-  Sample data files available for testing
-  Database schema designed (if new tables needed)

### 6.2 Step 1: Create Ingestor Class

Create file: `cms_pricing/ingestion/ingestors/{dataset}_ingestor.py`

```python
#!/usr/bin/env python3
"""
{DATASET} DIS-Compliant Ingestor
Following STD-data-architecture-prd-v1.0
"""

import asyncio
import hashlib
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import structlog

from ..contracts.ingestor_spec import (
    BaseDISIngestor, SourceFile, RawBatch, AdaptedBatch, 
    StageFrame, ValidationRule, OutputSpec, SlaSpec,
    ReleaseCadence, DataClass, ValidationSeverity
)
from ..scrapers.cms_{dataset}_scraper import CMS{Dataset}Scraper
from ..validators.validation_engine import ValidationEngine
from ..quarantine.dis_quarantine import QuarantineManager
from ..observability.dis_observability import DISObservabilityCollector

logger = structlog.get_logger()


class {Dataset}Ingestor(BaseDISIngestor):
    """DIS-compliant ingestor for {DATASET}"""
    
    def __init__(self, output_dir: str = "./data/ingestion/{dataset}", db_session: Any = None):
        super().__init__(output_dir, db_session)
        
        # Initialize components
        self.scraper = CMS{Dataset}Scraper(str(Path(self.output_dir) / "scraped"))
        self.validation_engine = ValidationEngine()
        self.quarantine_manager = QuarantineManager(str(Path(self.output_dir) / "quarantine"))
        self.observability_collector = DISObservabilityCollector()
        
        # Configuration
        self._dataset_name = "{DATASET}"
        self._release_cadence = ReleaseCadence.QUARTERLY  # or ANNUAL
        self._data_classification = DataClass.PUBLIC
        self._contract_schema_ref = "cms.{dataset}:v1.0"
        
        # SLA and output specifications
        self.sla_spec = SlaSpec(
            max_processing_time_hours=24,
            freshness_alert_hours=120,
            quality_threshold=0.99,
            availability_target=0.999
        )
        
        self.output_spec = OutputSpec(
            table_name="{dataset}_curated",
            partition_columns=["vintage_date", "effective_from"],
            output_format="parquet",
            compression="snappy",
            schema_evolution=True
        )
        
        # Validation rules
        self.validation_rules = self._create_validation_rules()
    
    @property
    def dataset_name(self) -> str:
        return self._dataset_name
    
    @property
    def release_cadence(self) -> ReleaseCadence:
        return self._release_cadence
    
    @property
    def data_classification(self) -> DataClass:
        return self._data_classification
    
    @property
    def contract_schema_ref(self) -> str:
        return self._contract_schema_ref
    
    @property
    def validators(self) -> List[ValidationRule]:
        return self.validation_rules
    
    @property
    def slas(self) -> SlaSpec:
        return self.sla_spec
    
    @property
    def outputs(self) -> OutputSpec:
        return self.output_spec
    
    def _create_validation_rules(self) -> List[ValidationRule]:
        """Create validation rules for {DATASET}"""
        return [
            ValidationRule(
                name="Required columns present",
                description="All required columns must be present",
                validator_func=self._validate_required_columns,
                severity=ValidationSeverity.CRITICAL
            ),
            # Add more validation rules
        ]
    
    # Implement stage methods in steps 3-8
```

### 6.3 Step 2: Implement Discovery

```python
async def discover_source_files(self) -> List[SourceFile]:
    """Discover source files using scraper"""
    logger.info("Starting {DATASET} file discovery")
    
    try:
        # Use scraper to discover files
        current_year = datetime.now().year
        scraped_files = await self.scraper.scrape_{dataset}_files(
            current_year, 
            current_year, 
            latest_only=True
        )
        
        # Convert to SourceFile format
        source_files = []
        for file_info in scraped_files:
            source_files.append(SourceFile(
                url=file_info.url,
                filename=file_info.filename,
                content_type=file_info.content_type,
                expected_size_bytes=file_info.size_bytes,
                last_modified=file_info.last_modified,
                checksum=file_info.checksum
            ))
        
        logger.info("{DATASET} file discovery completed", files_found=len(source_files))
        return source_files
        
    except Exception as e:
        logger.error("{DATASET} file discovery failed", error=str(e))
        raise
```

### 6.4 Step 3: Implement Land Stage

```python
async def land_stage(self, source_files: List[SourceFile]) -> RawBatch:
    """Land stage: Download and store raw files"""
    logger.info("Starting {DATASET} land stage", file_count=len(source_files))
    
    raw_batch = RawBatch(
        batch_id=str(uuid.uuid4()),
        source_files=source_files,
        raw_data={},
        metadata={
            "ingestion_timestamp": datetime.now().isoformat(),
            "source": "CMS {DATASET}",
            "license": "CMS Public Domain"
        }
    )
    
    # Download and store each file
    for source_file in source_files:
        try:
            logger.info("Downloading file", filename=source_file.filename)
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(source_file.url)
                response.raise_for_status()
                
                # Store raw file
                raw_path = Path(self.output_dir) / "raw" / source_file.filename
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(raw_path, 'wb') as f:
                    f.write(response.content)
                
                # Calculate checksum
                checksum = hashlib.sha256(response.content).hexdigest()
                
                # Store file metadata
                raw_batch.raw_data[source_file.filename] = {
                    "path": str(raw_path),
                    "size_bytes": len(response.content),
                    "checksum": checksum,
                    "downloaded_at": datetime.now().isoformat()
                }
                
                logger.info("File downloaded successfully", 
                           filename=source_file.filename,
                           size_bytes=len(response.content))
                
        except Exception as e:
            logger.error("Failed to download file", 
                       filename=source_file.filename, 
                       error=str(e))
            raise
    
    logger.info("{DATASET} land stage completed", files_processed=len(source_files))
    return raw_batch
```

### 6.5 Step 4: Implement Validation

```python
async def validate_stage(self, raw_batch: RawBatch) -> Tuple[RawBatch, List[Dict[str, Any]]]:
    """Validate stage: Structural, domain, and statistical validation"""
    logger.info("Starting {DATASET} validate stage")
    
    validation_results = []
    validated_data = {}
    
    for filename, file_data in raw_batch.raw_data.items():
        try:
            logger.info("Validating file", filename=filename)
            
            # Structural validation
            struct_result = await self._validate_structural(filename, file_data)
            validation_results.extend(struct_result)
            
            # Domain validation
            domain_result = await self._validate_domain(filename, file_data)
            validation_results.extend(domain_result)
            
            # Statistical validation
            stat_result = await self._validate_statistical(filename, file_data)
            validation_results.extend(stat_result)
            
            # If validation passed, add to validated data
            if not any(r["severity"] == "CRITICAL" for r in validation_results):
                validated_data[filename] = file_data
            else:
                # Quarantine failed files
                self.quarantine_manager.quarantine_file(
                    filename=filename,
                    reason="Validation failed",
                    validation_results=validation_results
                )
            
            logger.info("File validation completed", filename=filename)
            
        except Exception as e:
            logger.error("File validation failed", filename=filename, error=str(e))
            validation_results.append({
                "rule_id": "{dataset}_validation_error",
                "severity": "CRITICAL",
                "message": f"Validation failed: {str(e)}",
                "filename": filename
            })
    
    # Update raw batch with validated data
    raw_batch.raw_data = validated_data
    
    logger.info("{DATASET} validate stage completed", 
               files_validated=len(validated_data),
               validation_results=len(validation_results))
    
    return raw_batch, validation_results
```

### 6.6 Step 5: Implement Normalization

```python
async def normalize_stage(self, raw_batch: RawBatch) -> AdaptedBatch:
    """
    Normalize stage: Parse and canonicalize data
    
    Per STD-parser-contracts v1.1, parsers return ParseResult(data, rejects, metrics).
    Ingestor handles all file writes (parsed.parquet, rejects.parquet, metrics.json).
    """
    logger.info("Starting {DATASET} normalize stage")
    
    adapted_data = {}
    all_rejects = []
    all_metrics = []
    
    for filename, file_data in raw_batch.raw_data.items():
        try:
            logger.info("Normalizing file", filename=filename)
            
            # Route to appropriate parser (v1.1: uses file_head for content sniffing)
            from cms_pricing.ingestion.parsers import route_to_parser
            
            file_head = file_data[:8192] if isinstance(file_data, bytes) else None
            dataset, schema_id, parser_func = route_to_parser(filename, file_head)
            
            # Prepare metadata for parser
            metadata = {
                'release_id': self.current_release_id,
                'vintage_date': self.vintage_date,
                'product_year': self.product_year,
                'quarter_vintage': self.quarter_vintage,
                'source_uri': raw_batch.metadata.get('source_uri'),
                'file_sha256': self._compute_file_hash(file_data),
                'parser_version': 'v1.1.0',
                'schema_id': schema_id
            }
            
            # Parse file (returns ParseResult per v1.1)
            result = parser_func(file_data, filename, metadata)
            
            # Ingestor writes artifacts
            adapted_data[filename] = result.data  # Valid rows
            all_rejects.append(result.rejects)    # Rejected rows
            all_metrics.append(result.metrics)    # Parse metrics
            
            logger.info("File normalization completed", 
                       filename=filename,
                       valid_rows=len(result.data),
                       rejected_rows=len(result.rejects))
            
        except Exception as e:
            logger.error("File normalization failed", filename=filename, error=str(e))
            raise
    
    adapted_batch = AdaptedBatch(
        batch_id=raw_batch.batch_id,
        source_files=raw_batch.source_files,
        adapted_data=adapted_data,
        metadata={
            **raw_batch.metadata,
            "normalized_at": datetime.now().isoformat()
        }
    )
    
    logger.info("{DATASET} normalize stage completed", files_processed=len(adapted_data))
    return adapted_batch
```

### 6.7 Step 6: Implement Enrichment

```python
async def enrich_stage(self, adapted_batch: AdaptedBatch) -> StageFrame:
    """Enrich stage: Join with reference data"""
    logger.info("Starting {DATASET} enrich stage")
    
    enriched_data = {}
    
    for filename, data in adapted_batch.adapted_data.items():
        try:
            # Join with reference data
            enriched = await self._join_reference_data(data)
            
            # Compute derived fields
            enriched = self._compute_derived_fields(enriched)
            
            enriched_data[filename] = enriched
            
        except Exception as e:
            logger.error("Enrichment failed", filename=filename, error=str(e))
            raise
    
    stage_frame = StageFrame(
        batch_id=adapted_batch.batch_id,
        source_files=adapted_batch.source_files,
        stage_data=enriched_data,
        metadata={
            **adapted_batch.metadata,
            "enriched_at": datetime.now().isoformat()
        }
    )
    
    logger.info("{DATASET} enrich stage completed")
    return stage_frame
```

### 6.8 Step 7: Implement Publishing

```python
async def publish_stage(self, stage_frame: StageFrame) -> Dict[str, Any]:
    """Publish stage: Create curated views and store in database"""
    logger.info("Starting {DATASET} publish stage")
    
    # Create curated views
    curated_views = await self._create_curated_views(stage_frame)
    
    # Store in database
    await self._store_curated_data(curated_views)
    
    # Generate observability report
    observability_report = await self._generate_observability_report(stage_frame)
    
    result = {
        "batch_id": stage_frame.batch_id,
        "dataset_name": self.dataset_name,
        "release_id": self.current_release_id,
        "curated_views": list(curated_views.keys()),
        "observability_report": observability_report,
        "metadata": stage_frame.metadata
    }
    
    logger.info("{DATASET} publish stage completed")
    return result
```

### 6.9 Step 8: Add Tests

Create file: `tests/ingestors/test_{dataset}_ingestor_e2e.py`

```python
import pytest
from cms_pricing.ingestion.ingestors.{dataset}_ingestor import {Dataset}Ingestor


@pytest.mark.asyncio
async def test_{dataset}_discovery():
    """Test file discovery"""
    ingestor = {Dataset}Ingestor()
    source_files = await ingestor.discover_source_files()
    
    assert len(source_files) > 0
    assert all(f.url for f in source_files)
    assert all(f.filename for f in source_files)


@pytest.mark.asyncio
async def test_{dataset}_full_pipeline():
    """Test full DIS pipeline"""
    ingestor = {Dataset}Ingestor()
    
    # Run full pipeline
    result = await ingestor.ingest(2025)
    
    assert result["batch_id"]
    assert result["dataset_name"] == "{DATASET}"
    assert "curated_views" in result
    assert len(result["curated_views"]) > 0
```

### 6.10 Step 9: Create Dataset PRD

Create file: `prds/PRD-{dataset}-prd-v1.0.md`

Follow the template in `STD-data-architecture-prd-v1.0.md` §13.

### 6.11 Step 10: Certification

Complete the DIS Compliance Checklist (§9.1) and submit for review.

---

## 7. Working Examples

### 7.1 MPFS Ingestor (Composition Pattern)

**File:** `cms_pricing/ingestion/ingestors/mpfs_ingestor.py:55`

**Key Features:**
- Composes with RVU scraper for shared files
- Discovers MPFS-specific files (conversion factors, abstracts)
- Creates 6 curated views referencing RVU data
- Annual release cadence

**Discovery Pattern:**
```python
async def discover_source_files(self) -> List[SourceFile]:
    # 1. Get shared RVU files via composition
    rvu_files = await self.rvu_scraper.scrape_rvu_files(start_year, end_year)
    all_files.extend(rvu_files)
    
    # 2. Discover MPFS-specific files
    for year in range(start_year, end_year + 1):
        year_files = await self._discover_mpfs_year_files(year)
        all_files.extend(year_files)
    
    return all_files
```

**Curated Views:**
- `mpfs_rvu`: References PPRRVU data
- `mpfs_indicators_all`: Exploded policy flags
- `mpfs_locality`: References LocalityCounty data
- `mpfs_gpci`: References GPCI data
- `mpfs_cf_vintage`: Conversion factor data (MPFS-specific)
- `mpfs_link_keys`: Minimal key set for joins

### 7.2 RVU Ingestor (Direct Links Pattern)

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py:97`

**Key Features:**
- Direct file links from CMS website
- Quarterly releases (A/B/C/D)
- Fixed-width file parsing
- Multiple file formats (ZIP, TXT, CSV, XLSX)

**Discovery Pattern:**
```python
async def discover_source_files(self) -> List[SourceFile]:
    # Navigate to RVU files page
    page_html = await self._fetch_page(rvu_files_url)
    
    # Extract RVU file links (RVU24A, RVU24B, etc.)
    for link in self._extract_rvu_links(page_html):
        file_info = RVUFileInfo(
            url=link.url,
            filename=link.filename,
            file_type=self._detect_file_type(link),
            year=self._extract_year(link),
            quarter=self._extract_quarter(link)
        )
        source_files.append(file_info)
    
    return source_files
```

**Parsing Pattern:**
- Fixed-width text files with layout specifications
- Multiple formats per release (TXT, CSV, XLSX)
- Layout files define column positions and widths

### 7.3 OPPS Ingestor (Quarterly Navigation)

**File:** `cms_pricing/ingestion/ingestors/opps_ingestor.py:52`

**Key Features:**
- Quarterly navigation pattern
- AMA license interstitial handling
- Addendum A (APC payments) and B (HCPCS crosswalk)
- Quarterly release cadence

**Discovery Pattern:**
```python
async def discover_files(self, max_quarters=8) -> List[ScrapedFileInfo]:
    # 1. Get quarterly addenda links from main page
    addenda_links = await self._get_quarterly_addenda_links()
    
    # 2. For each quarter, navigate to quarterly page
    for quarter_link in addenda_links:
        quarter_page = await self._fetch_page(quarter_link.url)
        
        # 3. Extract Addendum A and B file links
        addendum_files = self._extract_addendum_links(
            quarter_page, 
            year=quarter_link.year,
            quarter=quarter_link.quarter
        )
        all_files.extend(addendum_files)
    
    return all_files
```

**Special Handling:**
- AMA license interstitial detection
- Automatic disclaimer acceptance
- Redirect URL tracking

---

## 8. Code Templates

### 8.1 Minimal Ingestor Template

See §6.2 for complete minimal ingestor template.

### 8.2 Validation Rule Template

```python
def _validate_{rule_name}(self, df: pd.DataFrame) -> List[ValidationResult]:
    """Validate {description}"""
    results = []
    
    # Perform validation check
    failed_rows = df[df["{column}"].{condition}]
    
    if len(failed_rows) > 0:
        results.append(ValidationResult(
            rule_id="{dataset}_{rule_name}",
            severity=ValidationSeverity.ERROR,  # or WARNING, CRITICAL
            message=f"{len(failed_rows)} rows failed {rule_name} validation",
            failed_count=len(failed_rows),
            sample_failures=failed_rows.head(5).to_dict('records')
        ))
    
    return results
```

### 8.3 Schema Contract Template

```json
{
  "dataset_name": "cms_{dataset}",
  "version": "1.0",
  "generated_at": "2025-10-15T00:00:00.000000",
  "columns": {
    "{column_name}": {
      "name": "{column_name}",
      "type": "string",
      "required": true,
      "description": "{description}",
      "pattern": "^{regex}$",
      "min_value": null,
      "max_value": null
    }
  },
  "primary_keys": ["{key1}", "{key2}"],
  "partition_columns": ["{partition_col}"],
  "business_rules": [
    "{rule_description}"
  ],
  "quality_thresholds": {
    "completeness": 0.99,
    "validity": 0.99
  }
}
```

### 8.4 Test Suite Template

```python
import pytest
from cms_pricing.ingestion.ingestors.{dataset}_ingestor import {Dataset}Ingestor


class Test{Dataset}Ingestor:
    """Test suite for {DATASET} ingestor"""
    
    @pytest.fixture
    def ingestor(self):
        return {Dataset}Ingestor(output_dir="./test_data")
    
    @pytest.mark.asyncio
    async def test_discovery(self, ingestor):
        """Test file discovery"""
        source_files = await ingestor.discover_source_files()
        assert len(source_files) > 0
    
    @pytest.mark.asyncio
    async def test_land_stage(self, ingestor, sample_source_files):
        """Test land stage"""
        raw_batch = await ingestor.land_stage(sample_source_files)
        assert raw_batch.batch_id
        assert len(raw_batch.raw_data) > 0
    
    @pytest.mark.asyncio
    async def test_validate_stage(self, ingestor, sample_raw_batch):
        """Test validate stage"""
        validated_batch, results = await ingestor.validate_stage(sample_raw_batch)
        assert len(results) >= 0
    
    @pytest.mark.asyncio
    async def test_full_pipeline(self, ingestor):
        """Test full DIS pipeline"""
        result = await ingestor.ingest(2025)
        assert result["batch_id"]
        assert result["dataset_name"] == "{DATASET}"
```

---

## 9. Compliance & Certification

### 9.1 DIS Compliance Checklist

Before marking an ingestor as production-ready:

**Interface Compliance:**
-  Extends `BaseDISIngestor` from `ingestor_spec.py`
-  Implements all required stage methods
-  Declares all required properties
-  Returns correct data types from each stage

**Schema & Validation:**
-  Schema contract exists in `cms_pricing/ingestion/contracts/`
-  Registered in schema registry
-  Validation rules defined for all quality gates (§7 of main PRD)
-  Quarantine policy implemented
-  Validation severity levels used correctly

**Observability:**
-  Emits all required observability events
-  Integrates with `DISObservabilityCollector`
-  SLA specifications defined
-  Monitoring dashboards created
-  Structured logging throughout

**Testing (per STD-qa-testing-prd-v1.0):**
-  Unit tests for each stage method (≥80% coverage)
-  Integration tests with real data samples
-  Schema drift detection tests
-  Validation rule tests
-  End-to-end pipeline test
-  Performance tests (if applicable)

**Documentation:**
-  Dataset PRD created (follows `PRD-{dataset}-prd-v1.0` naming)
-  Ingestion Summary section completed (§13 template from main PRD)
-  Schema contracts documented
-  Reference data dependencies listed
-  Deviations documented (if any)
-  Registered in `DOC-master-catalog-prd-v1.0.md`

**Operational:**
-  Configuration documented
-  Secrets managed properly
-  Release/Batch ID generation follows conventions
-  Logging follows structured logging patterns
-  Error handling comprehensive

### 9.2 Review & Approval Process

1. **Code Review:** Standard PR review process
2. **Schema Review:** Data Engineering lead approval
3. **Security Review:** If handling sensitive data
4. **QA Approval:** QA Guild sign-off on test coverage
5. **Documentation Audit:** Run `tools/audit_doc_catalog.py`
6. **Production Deployment:** Ops approval + runbook

### 9.3 Automated Verification

**Run these commands before submitting for review:**

```bash
# Schema contract validation
python -m cms_pricing.ingestion.contracts.validate_contracts

# Interface compliance check
python -m cms_pricing.ingestion.contracts.verify_ingestor_compliance

# Documentation audit
python tools/audit_doc_catalog.py
python tools/audit_doc_links.py
python tools/audit_cross_references.py

# Run tests
pytest tests/ingestors/test_{dataset}_ingestor_e2e.py -v

# Check coverage
pytest tests/ingestors/test_{dataset}_ingestor_e2e.py --cov=cms_pricing.ingestion.ingestors.{dataset}_ingestor --cov-report=html
```

---

## 10. Troubleshooting & Common Issues

### 10.1 Discovery Issues

**Problem:** Scraper not finding files

**Solutions:**
- Check scraper URL is correct and accessible
- Verify file patterns match actual file names
- Check for website structure changes
- Review scraper logs for HTTP errors
- Test scraper independently before running full pipeline

**Problem:** Discovery manifest not generated

**Solutions:**
- Ensure `DiscoveryManifestStore` is initialized correctly
- Check output directory permissions
- Verify manifest format matches schema

### 10.2 Validation Failures

**Problem:** All files failing validation

**Solutions:**
- Check schema contract matches actual data structure
- Verify column names match (case-sensitive)
- Review validation rule logic
- Check for data type mismatches
- Examine quarantine logs for specific errors

**Problem:** Intermittent validation failures

**Solutions:**
- Check for data quality issues in source
- Review statistical validation thresholds
- Examine historical data for drift
- Check for null/missing value handling

### 10.3 Schema Drift

**Problem:** Schema contract validation failing

**Solutions:**
- Compare current data structure with contract
- Check for new columns in source data
- Review CMS release notes for changes
- Update schema contract if legitimate change
- Add schema evolution handling

**Problem:** Column type mismatches

**Solutions:**
- Review data type casting logic
- Check for locale-specific parsing issues
- Verify decimal precision handling
- Update schema contract if needed

### 10.4 Performance Problems

**Problem:** Pipeline taking too long

**Solutions:**
- Profile each stage to identify bottleneck
- Check for unnecessary data loading
- Optimize validation rules (parallel execution)
- Use chunking for large files
- Consider async operations for I/O

**Problem:** Memory issues with large files

**Solutions:**
- Use chunked reading for large CSVs
- Stream ZIP file extraction
- Clear intermediate data structures
- Use generators instead of lists
- Monitor memory usage

### 10.5 Observability Gaps

**Problem:** Missing metrics or logs

**Solutions:**
- Ensure `DISObservabilityCollector` is initialized
- Check logging configuration
- Verify structured logging format
- Review observability event emissions
- Check monitoring dashboard configuration

**Problem:** SLA breaches not alerting

**Solutions:**
- Verify SLA specifications are defined
- Check alert routing configuration
- Review observability collector integration
- Test alert system independently

---

## 11. Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| 2025-10-15 | v1.0.1 | Data Engineering | Updated normalize stage example to show ParseResult return type per STD-parser-contracts v1.1. Parsers now return ParseResult(data, rejects, metrics); ingestor handles all file writes. Updated cross-reference to parser contracts v1.1 (64-char hashing, schema-driven precision, content sniffing). |
| 2025-10-15 | v1.0 | Data Engineering | Initial implementation guide for DIS pipeline: interface reference, centralized components, schema contracts, operational patterns, implementation reference table, step-by-step guide, working examples, code templates, compliance checklist, and troubleshooting. |

