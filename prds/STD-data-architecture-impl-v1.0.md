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
- **STD-parser-contracts-prd-v2.0.md:** Parser core contracts (ParseResult, versioning)
- **STD-parser-contracts-impl-v2.0.md:** Parser implementation (§1.6 row hashing, §2.1 structure)
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

### 1.3 Modular Stage Helpers (Phase 2)

**Shared Executors:** Each stage has a reusable helper (`cms_pricing/ingestion/stages/{land,validate,normalize,enrich,publish}.py`) exposing `execute_*` functions. Ingestors call these helpers instead of duplicating stage logic.

**Service Factory Integration:** Use `cms_pricing/ingestion/services/__init__.py:ServiceFactory` to lazily initialise observability, quarantine, reference data, schema, and validation services for the dataset.

**Stage Module Pattern:**

Stage modules are the authoritative source of truth for stage logic. Ingestors delegate to these modules rather than implementing inline logic:

```python
# In ingestor:
from ..stages import execute_normalize, NormalizeConfig

async def normalize(self, validated_batch, raw_batch):
    config = NormalizeConfig(
        output_dir=self.output_dir,
        dataset_name=self.dataset_name,
        enable_schema_validation=True
    )
    
    result = await execute_normalize(
        validated_batch=validated_batch,
        raw_batch=raw_batch,
        config=config,
        adapter_func=None,  # Defaults to rvu_adapter when None
        schema_registry=self.services.schema_registry,
        validation_engine=self.services.validation_service.engine,
        cached_schemas=self._cached_schemas,  # Performance optimization (5-10% speedup)
        dataset_schema_map=self._dataset_schema_map
    )
    return result
```

**Performance Optimizations:**

Stage modules accept cached schemas and dataset mappings to improve validation performance:

```python
# Pre-cache schemas in ingestor __init__:
self._cached_schemas = SchemaService.cache_schemas(
    self.services.schema_registry,
    self._dataset_schema_map
)

# Pass to stage modules for optimal performance:
result = await execute_normalize(
    # ... other params ...
    cached_schemas=self._cached_schemas,  # Eliminates repeated registry lookups
    dataset_schema_map=self._dataset_schema_map  # Maps dataset names to schema IDs
)
```

**Benefits:**
- **Stage modules are authoritative:** All stage logic lives in reusable modules, not in ingestors
- **Reusable across ingestors:** MPFS, OPPS, and other ingestors can use the same stage modules
- **Consistent stage behavior:** Uniform stage execution across all datasets
- **Performance optimizations:** Schema caching and vectorized operations improve speed by 5-10%
- **Testable in isolation:** Stage modules can be unit tested independently of ingestors

**Feature Flags:** Honour pipeline toggles (e.g., `ENABLE_ENRICHMENT`) when wiring stages; shared helpers accept config flags to support dry runs.

**Schema Drift Callbacks:** Provide a drift-detector callable (see the publish workflow in `cms_pricing/ingestion/ingestors/rvu_ingestor.py`) so the stage can emit drift metrics without inlining drift logic.

**Testing Guidance:** When adding a new dataset, unit-test stage modules directly and wire them through the ingestor to ensure compatibility with base contracts.

**Migration Path:**
- Replace inline stage logic with `execute_*` calls from stage modules
- Pass adapter/loader functions as parameters (defaults provided for backward compatibility)
- Remove duplicate stage helpers from ingestors
- Use module-level functions from stage modules instead of instance methods

### 1.4 DatasetSpec Registry (Phase 2 update)
- **Definition:** `cms_pricing/ingestion/datasets/spec.py` defines `DatasetSpec` and `EnrichmentRule` dataclasses capturing parser reference, schema ID, natural keys, loader, validation/business rules, enrichment rules, and filename patterns.
- **RVU example:** `cms_pricing/ingestion/datasets/rvu_spec.py` registers specs for PPRRVU, GPCI, OPPS Cap, ANES CF, and Locality. `route_file_to_rvu_spec(filename)` returns the matching spec for adapter/loader orchestration.
- **Usage pattern:** Ingestors build stage configs by iterating `RVU_DATASETS.values()` (see `RVUIngestor` discovery/normalization setup) instead of hardcoding per-dataset switches.
- **Onboarding checklist:** New datasets MUST add a DatasetSpec entry (parser, schema SemVer, natural keys, loader, validation rules, enrichment rules, routing patterns) and register schemas via `SchemaService.bootstrap_*`.
- **Documentation impact:** Dataset PRDs should reference their DatasetSpec modules; implementation guides should illustrate spec extension rather than direct parser lookups.

### 1.2.3 Quarter Vintage Encoding (Added 2025-10-20)

**Standard:** CMS Letter Format (A, B, C, D)

Quarter releases are encoded using CMS letter notation to match official file naming:

```python
# Preferred encoding
quarter_vintage = 'D'  # October/Q4 release

# Mapping table
QUARTER_MAPPING = {
    'A': {'q_notation': 'Q1', 'month': 'January', 'fiscal_quarter': 1},
    'B': {'q_notation': 'Q2', 'month': 'April', 'fiscal_quarter': 2},
    'C': {'q_notation': 'Q3', 'month': 'July', 'fiscal_quarter': 3},
    'D': {'q_notation': 'Q4', 'month': 'October', 'fiscal_quarter': 4},
}
```

**Database Storage:**

Store `quarter_vintage` as CMS letter (1 char) in all metadata tables:

```sql
CREATE TABLE cms_release (
    release_id VARCHAR(50) PRIMARY KEY,
    product_year INTEGER NOT NULL,
    quarter_vintage CHAR(1) CHECK (quarter_vintage IN ('A', 'B', 'C', 'D')),
    vintage_date DATE NOT NULL,
    source_uri TEXT,
    CONSTRAINT uk_release UNIQUE (product_year, quarter_vintage)
);

-- Index for common queries
CREATE INDEX idx_release_quarter ON cms_release(product_year, quarter_vintage);
```

**API Responses:**

Return CMS letter in API responses for consistency:

```json
{
  "release_id": "OPPSCAP_2025D",
  "product_year": 2025,
  "quarter_vintage": "D",
  "quarter_label": "Q4 (October)",
  "vintage_date": "2025-10-01",
  "source_uri": "https://www.cms.gov/medicare/...",
  "metadata": {
    "quarter_numeric": 4,
    "quarter_month": "October",
    "fiscal_year": 2025
  }
}
```

**Ingestor Metadata:**

All ingestors MUST populate `quarter_vintage` with CMS letter format:

```python
class MyIngestor(BaseDISIngestor):
    async def land_stage(self, release_id: str) -> Dict[str, Any]:
        # Extract quarter from filename
        match = re.match(r'^([A-Z]+)(\d{2})([A-D])\.zip$', filename)
        quarter = match.group(3)  # 'D' (CMS letter)
        
        # Build metadata
        metadata = {
            'release_id': release_id,
            'product_year': 2025,
            'quarter_vintage': quarter,  # ✅ Use CMS letter directly
            'vintage_date': '2025-10-01',
            ...
        }
```

**Rule:** `R-DATA-007` - All database schemas MUST store `quarter_vintage` as CMS letter (CHAR(1))  
**Rule:** `R-DATA-008` - APIs SHOULD return both CMS letter and human-readable quarter label

**See Also:** `REF-parser-routing-detection-v1.0.md` §3.4, `planning/standards/QUARTER_NOTATION_STANDARD.md`

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
    # ⚠️ PERFORMANCE: For large datasets (10k+ rows), prefer vectorized operations over .apply()
    # If matching logic is complex, batch process by state_fips groups instead of row-by-row
    # Example optimization: df.groupby('state_fips').apply(batch_county_match) instead of axis=1
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

**H.1) Stage 1 Continuation Row Handling & Stage 2 State Inference** (Added 2025-10-20)

**Context:** Fixed-width TXT files often have continuation rows where identifying columns (MAC, locality) are present but contextual columns (state) bleed/truncate/are blank.

**Problem:** Missing or bleeding state headers cause Stage 1 forward-fill to propagate wrong state across MAC boundaries (e.g., Arkansas rows bleeding into California block).

**Design Decision:**

**Stage 1 (Raw Parser) - Trust MAC Spans, Emit Empty State:**
```python
# Stage 1: Layout-faithful parsing, trust MAC/locality boundaries
def parse_locality_raw(file_obj, filename, metadata):
    """Parse fixed-width file, trust MAC spans even when state bleeds."""
    
    last_valid_state = None
    rows = []
    
    for line in file_obj:
        mac_value = line[0:10].strip()
        locality_value = line[10:16].strip()
        state_value = line[16:50].strip()
        county_names = line[50:150].strip()
        
        # Trust MAC/locality spans (these define row boundaries)
        has_codes = mac_value != "" and locality_value != ""
        
        # Is state column valid?
        is_valid_state = state_value in VALID_US_STATES
        
        if is_valid_state:
            # Valid state header - update tracking
            last_valid_state = state_value
            rows.append({
                'mac': mac_value,
                'locality_code': locality_value,
                'state_name': state_value,  # Use header state
                'county_names': county_names,
            })
        elif has_codes:
            # Continuation row: MAC/locality present, state blank/bleeding
            rows.append({
                'mac': mac_value,
                'locality_code': locality_value,
                'state_name': '',  # ⭐ Emit EMPTY - Stage 2 will infer!
                'county_names': county_names,
            })
        else:
            # Skip non-data lines (headers, footers, blanks)
            continue
    
    return ParseResult(data=pd.DataFrame(rows), rejects=[], metrics={})
```

**Key Principles (Stage 1):**
- ✅ Trust MAC/locality code spans (these define record boundaries)
- ✅ Emit empty `state_name` when state column is blank/invalid
- ✅ Do NOT guess or forward-fill across MAC boundaries
- ✅ Preserve layout fidelity (keep what CMS ships)

**Stage 2 (Normalizer) - Infer State from County Names:**
```python
# Stage 2: FIPS normalization with state inference
def normalize_locality_fips(raw_df, counties_df, states_df, aliases):
    """Derive FIPS codes, infer missing states from county names."""
    
    normalized_rows = []
    
    for _, row in raw_df.iterrows():
        state_fips = None
        state_name = row['state_name']
        
        # Lookup state FIPS from name
        if state_name:
            state_fips = states_df[states_df['state_name'] == state_name.upper()]['state_fips'].iloc[0]
        
        # ⭐ If no state_fips, infer from county names
        if not state_fips:
            state_fips, state_name = infer_state_from_counties(
                row['county_names'], counties_df, states_df
            )
            if not state_fips:
                # Cannot infer - quarantine
                quarantine_rows.append({'reason': 'unknown_state', ...})
                continue
        
        # Detect set-logic (ALL, EXCEPT, REST OF)
        expansion_method = detect_set_logic(row['county_names'])
        
        # ⭐ CRITICAL: Re-expand set-logic with inferred state
        if expansion_method == 'all_counties':
            county_list = expand_all_counties(state_fips, counties_df)
        elif expansion_method == 'rest_of_state':
            county_list = expand_rest_of_state(state_fips, locality_code, ...)
        else:
            county_list = explode_county_list(row['county_names'])
        
        # Match counties to FIPS...
        for county_name in county_list:
            county_key = normalize_key(county_name, state_fips)
            match = match_exact(county_key, state_fips, counties_df) or \
                    match_alias(county_key, state_fips, counties_df, aliases)
            
            if match:
                normalized_rows.append({
                    'mac': row['mac'],
                    'locality_code': row['locality_code'],
                    'state_fips': state_fips,  # Use inferred state
                    'county_fips': match['county_fips'],
                    'county_name_canonical': match['county_name_canonical'],
                    'match_method': 'exact' if exact else 'alias',
                })
```

**Key Principles (Stage 2):**
- ✅ Infer state from county names when Stage 1 emits empty state
- ✅ Re-expand set-logic (ALL, EXCEPT, REST OF) with inferred state
- ✅ Only quarantine if inference fails or is ambiguous
- ✅ Log state inference events for observability

**Why This Works:**
- Stage 1 stays simple: trust layout, no inference/guessing
- Stage 2 has reference data to infer correctly
- Prevents cross-MAC bleeding (AR → CA)
- Handles missing CA headers in sample files gracefully

**Practical Example:** Locality Parser (2025-10-18)
- Sample file missing California state header
- Stage 1 emitted empty `state_name` for CA continuation rows
- Stage 2 inferred state=06 from county names (e.g., "LOS ANGELES")
- 28 localities successfully inferred, 0 quarantined for missing CA header
- See: `cms_pricing/ingestion/parsers/locality_parser.py:630-670`, `cms_pricing/ingestion/normalize/normalize_locality_fips.py:920-975`

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

#### G) Set-Logic & Complement Patterns (REST-OF-STATE) (Added 2025-10-20)

**Context:** Geographic and set-based expansions (e.g., "ALL COUNTIES", "ALL EXCEPT X/Y", "REST OF STATE") require special handling when the complement set depends on other rows' assignments.

**Problem:** "REST OF STATE" = all entities NOT assigned to other localities/groups. Cannot compute until all explicit assignments are collected.

**Solution: Two-Pass Algorithm**

**Pass 1 - Collect Explicit Assignments:**
```python
explicit_entities_by_group = defaultdict(set)  # e.g., {state_fips: {county_geoid, ...}}
deferred_rest_rows = []

# ⚠️ PERFORMANCE NOTE: For very large datasets, consider vectorized groupby operations
# instead of iterating row-by-row. Current approach is acceptable for typical sizes (<100k rows).
for row in raw_df:
    expansion_method = detect_set_logic(row['entity_names'])
    
    if expansion_method == 'rest_of_state':
        # Defer REST OF rows until Pass 2
        deferred_rest_rows.append({
            'row': row,
            'group_key': row['state_fips'],  # or other grouping dimension
            'locality_code': row['locality_code'],
        })
        continue
    
    # Process explicit rows (list, all_counties, all_except)
    if expansion_method == 'all_counties':
        entities = expand_all_entities(row['group_key'], reference_df)
    elif expansion_method == 'all_except':
        entities = expand_all_except(row['group_key'], row['exceptions'], reference_df)
    else:  # 'list'
        entities = explode_entity_list(row['entity_names'])
    
    # Track which entities are assigned
    for entity in entities:
        explicit_entities_by_group[row['group_key']].add(entity)
    
    # Emit normalized rows...
```

**Pass 2 - Process REST OF with Complement:**
```python
for rest_ctx in deferred_rest_rows:
    group_key = rest_ctx['group_key']
    
    # Get ALL entities in this group
    all_group_entities = reference_df[reference_df['group_key'] == group_key]
    
    # Compute complement: ALL - explicitly_assigned (vectorized operation)
    assigned_set = explicit_entities_by_group.get(group_key, set())
    rest_entities = all_group_entities[~all_group_entities['entity_id'].isin(assigned_set)]
    
    # ✅ PERFORMANCE: Use vectorized operations instead of iterrows() for large datasets
    # Vectorized approach (10-50x faster for 10k+ rows):
    if len(rest_entities) > 0:
        rest_rows = rest_entities.copy()
        rest_rows['locality_code'] = rest_ctx['locality_code']
        rest_rows['group_key'] = group_key
        rest_rows['expansion_method'] = 'rest_of_state'
        # Append all rows at once instead of row-by-row
        normalized_rows.extend(rest_rows.to_dict('records'))
    
    # ❌ ANTI-PATTERN (slow for large datasets):
    # for _, entity_row in rest_entities.iterrows():
    #     normalized_rows.append({...})
    
    logger.info(
        "rest_of_state_expanded",
        group_key=group_key,
        locality_code=rest_ctx['locality_code'],
        assigned=len(rest_entities),
        previously_assigned=len(assigned_set),
    )
```

**Observability Requirements:**
- Track `expansion_methods` in metrics: `{'list': N, 'all_counties': M, 'rest_of_state': K}`
- Log REST OF expansion counts per group
- Emit warning if REST OF complement is empty (all entities already assigned)

**When to Use:**
- Geographic complements (REST OF STATE → remaining counties)
- Provider group complements (ALL OTHER PROVIDERS)
- Time-based complements (REMAINING QUARTERS)
- Any domain where "the rest" is meaningful

**Practical Example:** Locality Parser (2025-10-18)
- 16 REST OF STATE localities across 10 states
- 1,042 counties correctly expanded
- Prevented double-assignment (counties can't be in multiple localities)
- See: `cms_pricing/ingestion/normalize/normalize_locality_fips.py:900-1025`

**Cross-References:**
- **STD-data-architecture-prd §3.4** (Normalize stage requirements)
- **STD-data-architecture-prd §3.5** (Enrich stage requirements)
- **STD-parser-contracts-prd-v2.0 §6** (Parser contract boundaries)
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
| **MPFS** | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | Snapshot Reuse | Reuses RVU/GPCI snapshots via `DatasetSnapshotService`, fetches CF via `ConversionFactorFetcher`, creates curated views |
| **RVU** | `cms_pricing/ingestion/ingestors/rvu_ingestor.py` | Direct Links | Quarterly releases, fixed-width parsing |
| **OPPS** | `cms_pricing/ingestion/ingestors/opps_ingestor.py` | Quarterly Navigation | AMA license handling, addenda |

---

### 1.8 Advanced Ingestor Patterns

This section documents advanced patterns proven in the RVU ingestor implementation that are reusable across all ingestors. For the RVU reference implementation, see **PRD-rvu-gpci-prd-v0.1.md §6.3**.

**Reference Implementation:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - RVU Implementation Reference
- **REF-scraper-ingestor-integration-v1.0.md** - Discovery Manifest Contract
- **STD-scraper-prd-v1.0.md** - Scraper Pattern Implementations
- **STD-data-architecture-impl-v1.0.md §1.3** - Modular Stage Helpers
- **STD-data-architecture-impl-v1.0.md §1.4** - DatasetSpec Registry
- **STD-data-architecture-impl-v1.0.md §2.6** - Component Initialization Pattern
- **STD-data-architecture-impl-v1.0.md §4.2.1** - Feature Flags
- **STD-data-architecture-impl-v1.0.md §4.5** - Observability Events

#### 1.8.1 Discovery Callable Wrapper Pattern

**Purpose:** Provide dual sync/async access to discovery results, automatically detecting event loop state.

**Problem:** Legacy code expects sync `discovery()` calls, but modern ingestors use async scrapers. Calling `asyncio.run()` when an event loop is already running causes errors.

**Solution:** Wrap the async discovery coroutine in a callable that detects event loop state and handles both cases.

**Implementation:**

```python
class _DiscoveryCallable:
    """Wrapper providing both sync and async access to discovery results."""
    
    def __init__(self, coro_factory: Callable[[], Awaitable[List[SourceFile]]]):
        self._coro_factory = coro_factory
    
    def __call__(self) -> List[SourceFile]:
        """Synchronous entrypoint (used by legacy callers).
        
        Avoid calling asyncio.run() if an event loop is already running.
        In that case, return the coroutine so callers can `await` it.
        """
        try:
            asyncio.get_running_loop()
            # A loop is running in this thread; return coroutine for awaiting.
            return self._coro_factory()
        except RuntimeError:
            # No running loop → safe to run
            return asyncio.run(self._coro_factory())
    
    def __await__(self):
        """Allow `await ingestor.discovery()` in async contexts."""
        return self._coro_factory().__await__()

# Usage in ingestor:
@property
def discovery(self):
    return _DiscoveryCallable(self._discover_source_files_async)
```

**Reference:** Discovery helpers on `RVUIngestor` in `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**When to Use:**
- Implementing discovery methods that may be called from both sync and async contexts
- Supporting legacy test code that expects sync discovery
- Integrating with scrapers that return async coroutines

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Discovery Implementation
- **REF-scraper-ingestor-integration-v1.0.md §2** - Discovery Manifest Contract

#### 1.8.2 Schema Pre-caching Optimization

**Purpose:** Improve validation performance by pre-caching schema contracts at initialization time.

**Problem:** Repeated schema lookups during validation stage add 5-10% overhead.

**Solution:** Cache schemas during ingestor initialization and pass cached schemas to stage modules.

**Implementation:**

```python
def __init__(self, output_dir: str, db_session: Any = None):
    super().__init__(output_dir, db_session)
    
    # Initialize services
    self.services = ServiceFactory(ServiceConfig(...))
    
    # Register schemas first (before lazy access to schema_registry)
    registry = self.services.schema_registry
    self.services.schema_service.bootstrap_rvu_schemas(registry)
    
    # Pre-cache schema contracts for validation performance (Optimization #1)
    # This eliminates repeated schema lookups during validation - saves 5-10% on validation time
    self._dataset_schema_map = {
        "pprrvu": "cms_pprrvu",
        "gpci": "cms_gpci",
        "oppscap": "cms_oppscap",
        "anescf": "cms_anescf",
        "localitycounty": "cms_localitycounty"
    }
    self._cached_schemas = self.services.schema_service.cache_schemas(
        registry, self._dataset_schema_map
    )

# In normalize stage:
result = await execute_normalize(
    validated_batch=validated_batch,
    raw_batch=raw_batch,
    config=config,
    adapter_func=adapter_func,
    schema_registry=self.services.schema_registry,
    validation_engine=self.services.validation_service,
    dataset_schema_map=self._dataset_schema_map,
    cached_schemas=self._cached_schemas,  # Pass cached schemas
)
```

**Reference:** Schema caching and normalize configuration inside `RVUIngestor.__init__` and `_normalize_stage`

**Performance Impact:** 5-10% reduction in validation time for large datasets.

**When to Use:**
- Ingestors processing multiple datasets
- High-volume validation workloads
- Performance-critical ingestion pipelines

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Initialization Implementation
- **STD-data-architecture-impl-v1.0.md §2.6** - Component Initialization Pattern

#### 1.8.3 Compatibility Helpers (RawBatch Coercion)

**Purpose:** Maintain backward compatibility with legacy tests and code that pass dict-like objects instead of typed objects.

**Problem:** Legacy tests pass dicts instead of `RawBatch` objects, causing type errors in stage methods.

**Solution:** Implement coercion helpers that detect object type and convert dicts to appropriate typed objects.

**Implementation:**

```python
def _coerce_raw_batch_like(self, candidate: Any) -> Optional[RawBatch]:
    """Accept dict-like raw batch and coerce to an object with .metadata, .raw_content.
    
    This maintains compatibility with legacy tests that pass dicts.
    """
    if candidate is None:
        return None
    if hasattr(candidate, "metadata"):
        return candidate  # Already RawBatch-like
    if hasattr(candidate, "get") and callable(candidate.get):
        meta = candidate.get("metadata", {}) or {}
        raw_content = candidate.get("raw_content")
        raw_directory = candidate.get("raw_directory") or candidate.get("raw_data_path")
        source_files = candidate.get("source_files")
        raw_data_path = candidate.get("raw_data_path")
        # Create a minimal shim object
        class _Shim:
            pass
        shim = _Shim()
        shim.metadata = meta
        shim.raw_content = raw_content
        shim.raw_directory = raw_directory
        shim.source_files = source_files
        shim.raw_data_path = raw_data_path
        return shim  # type: ignore
    return None

# In normalize stage, handle both signatures:
async def _normalize_stage(self, validated_batch: Dict[str, Any], raw_batch: Optional[Dict[str, Any]] = None):
    # Handle backward-compatible signature where raw_batch might be passed first
    if isinstance(validated_batch, RawBatch):
        actual_raw_batch = validated_batch
        actual_validated_batch = raw_batch if raw_batch else validated_batch
    else:
        # Coerce dict to RawBatch if needed
        actual_raw_batch = self._coerce_raw_batch_like(raw_batch)
        actual_validated_batch = validated_batch
    # ... rest of implementation
```

**Reference:** `_coerce_raw_batch_like` and `_normalize_stage` in `RVUIngestor`

**When to Use:**
- Migrating existing ingestors with legacy test suites
- Supporting gradual migration from dict-based to typed APIs
- Maintaining compatibility during refactoring

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Compatibility Patterns Implementation

#### 1.8.4 Enrichment Orchestration (Multi-Dataset)

**Purpose:** Process multiple datasets in a single enrichment stage, aggregating metrics and reference data usage.

**Problem:** Ingestors handling multiple datasets need to process each dataset separately while maintaining overall metrics.

**Solution:** Iterate over datasets, process each with `execute_enrich()`, then aggregate metrics and reference data sources.

**Implementation:**

```python
async def enrich(self, adapted_batch: Any) -> Dict[str, Any]:
    # Extract dataframes from adapted_batch
    dataframes = adapted_batch.get("dataframes", {})
    
    enriched_dataframes: Dict[str, "pd.DataFrame"] = {}
    enrichment_metrics: Dict[str, Dict[str, Any]] = {}
    reference_data_sources: Dict[str, List[str]] = {}
    total_records = 0
    confidence_scores: List[float] = []
    
    # Process each dataset using the extracted stage module
    for dataset_key, df in dataframes.items():
        if df is None or (hasattr(df, "empty") and df.empty):
            enriched_dataframes[dataset_key] = df
            enrichment_metrics[dataset_key] = {"enrichment_skipped": True}
            continue
        
        dataset_schema = schema_bundle.get(dataset_key, {})
        stage_metadata = {
            "batch_id": batch_id,
            "release_id": release_id,
            "dataset": dataset_key,
            **metadata_block
        }
        stage_frame = StageFrame(
            data=df,
            schema=dataset_schema,
            metadata=stage_metadata,
            quality_metrics={}
        )
        
        ref_data = RefData(tables={}, metadata={})
        
        # Use extracted enrichment stage module
        enriched_stage_frame = await execute_enrich(
            stage_frame=stage_frame,
            ref_data=ref_data,
            config=config,
            reference_enricher=self.services.reference_enricher,
            reference_data_manager=self.services.reference_data_manager,
            observability_collector=self.services.observability_collector,
            release_id=release_id
        )
        
        enriched_df = enriched_stage_frame.data
        enriched_dataframes[dataset_key] = enriched_df
        enrichment_metrics[dataset_key] = enriched_stage_frame.quality_metrics or {}
        
        sources = enrichment_metrics[dataset_key].get("reference_data_sources", [])
        if sources:
            reference_data_sources[dataset_key] = sources
        
        total_records += len(enriched_df)
        if "enrichment_rate" in enrichment_metrics[dataset_key]:
            confidence_scores.append(enrichment_metrics[dataset_key]["enrichment_rate"])
    
    # Aggregate metrics
    average_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
    flattened_sources: List[str] = sorted({
        source for sources in reference_data_sources.values() for source in sources
    })
    
    return {
        "status": "success",
        "batch_id": batch_id,
        "release_id": release_id,
        "enriched_data": enriched_dataframes,
        "reference_data_used": flattened_sources,
        "mapping_confidence": average_confidence,
        "record_count": total_records,
        "enrichment_metrics": enrichment_metrics
    }
```

**Reference:** Enrichment orchestration within `RVUIngestor.enrich`

**When to Use:**
- Ingestors processing multiple related datasets
- Need to aggregate enrichment metrics across datasets
- Tracking reference data usage across multiple sources

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Enrichment Orchestration Implementation
- **STD-data-architecture-impl-v1.0.md §4.2.1** - Feature Flags (ENABLE_ENRICHMENT)

#### 1.8.5 Publish Stage Callbacks (Drift Detector, Loader Function)

**Purpose:** Separate dataset-specific logic (database loading, schema drift detection) from shared publish stage logic.

**Problem:** `execute_publish()` needs dataset-specific database loading and drift detection, but should remain reusable.

**Solution:** Use callback functions (`drift_detector`, `loader_func`) passed to `execute_publish()` to encapsulate dataset-specific logic.

**Implementation:**

```python
async def publish(self, enriched_batch: Any) -> Dict[str, Any]:
    # Create drift detector wrapper
    def drift_detector(schema_dict: Dict[str, Any], dataset_name: str) -> Dict[str, Any]:
        return self._detect_schema_drift(schema_dict, dataset_name)
    
    # Create loader function wrapper for dataset-specific database loading
    def rvu_loader_func(enriched_data_dict: Dict[str, Any], release_id: str, batch_id: str, vintage_date: str) -> Dict[str, Any]:
        # Create DB session if not provided
        from cms_pricing.database import SessionLocal
        db_session = self.db_session
        if db_session is None:
            db_session = SessionLocal()
            self.db_session = db_session  # Cache for reuse
        
        try:
            return load_rvu_dataframes(
                enriched_data_dict,
                release_id,
                batch_id,
                vintage_date,
                db_session,
            )
        except Exception as e:
            logger.error("Database loading failed", error=str(e), batch_id=batch_id)
            return {"error": str(e)}
    
    config = PublishConfig(
        output_dir=self.output_dir,
        dataset_name=self.dataset_name,
        enable_database_load=True,
        enable_schema_drift_detection=True,
        enable_latest_effective_view=True
    )
    
    # Use extracted publish stage module
    result = await execute_publish(
        enriched_batch=enriched_batch_dict,
        config=config,
        db_session=self.db_session,
        loader_func=rvu_loader_func,  # Dataset-specific loader
        drift_detector=drift_detector  # Dataset-specific drift detection
    )
    
    return result
```

**Reference:** Publish orchestration within `RVUIngestor.publish` and `_publish_stage`

**When to Use:**
- Ingestors with dataset-specific database schemas
- Need custom schema drift detection logic
- Want to reuse `execute_publish()` while keeping dataset-specific logic separated

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Publish Stage Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.8** - Database Loader Function Pattern

#### 1.8.6 Empty Input Detection

**Purpose:** Distinguish between truly empty input (no files discovered) and parsing failures (files discovered but invalid).

**Problem:** Pipeline should return "partial" status only for truly empty input, not for parsing failures.

**Solution:** Check for empty input only if no source files discovered AND no files downloaded AND no records processed.

**Implementation:**

```python
async def ingest(self, release_id: str, batch_id: str) -> Dict[str, Any]:
    # Execute pipeline
    pipeline_result = await pipeline.execute(release_id, batch_id)
    
    # Adjust status for empty input to satisfy test expectations
    # Mark as partial only if discovery found absolutely nothing (truly empty)
    files_downloaded = pipeline_result.get("files_downloaded", 0)
    total_records = pipeline_result.get("total_records", 0)
    source_files = pipeline_result.get("source_files", [])
    
    # Only mark partial if: no source files discovered AND no files downloaded AND no records
    # (If files were discovered but parsing failed, that's not "empty input" - it's a parsing issue)
    truly_empty = len(source_files) == 0 and files_downloaded == 0 and total_records == 0
    if truly_empty:
        # Only override to partial if current status is success (preserve explicit failures)
        if pipeline_result.get("status") == "success":
            pipeline_result["status"] = "partial"
        pipeline_result["_empty_input"] = True
    
    return pipeline_result
```

**Reference:** Empty-input guard within `RVUIngestor.ingest`

**When to Use:**
- Need to distinguish empty input from parsing failures
- Want to provide accurate status reporting
- Supporting test suites that expect "partial" status for empty runs

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Error Handling Implementation

#### 1.8.7 Error Handling Patterns

**Purpose:** Provide structured error responses with proper status preservation and error type tracking.

**Problem:** Pipeline failures should return structured responses with error context, not just raise exceptions.

**Solution:** Catch exceptions in pipeline execution, log context, and return structured error responses.

**Implementation:**

```python
async def ingest(self, release_id: str, batch_id: str) -> Dict[str, Any]:
    # Create and execute DIS pipeline
    pipeline = DISPipeline(
        ingestor=self,
        output_dir=self.output_dir,
        db_session=self.db_session
    )
    
    # Execute pipeline and collect results
    try:
        pipeline_result = await pipeline.execute(release_id, batch_id)
    except Exception as e:
        # Handle pipeline execution failures gracefully
        logger.error("Pipeline execution failed in ingest()", error=str(e), release_id=release_id, batch_id=batch_id)
        pipeline_result = {
            "status": "failed",
            "release_id": release_id,
            "batch_id": batch_id,
            "error": str(e),
            "error_type": type(e).__name__,  # Preserve error type
            "files_downloaded": 0,
            "total_records": 0,
            "source_files": []
        }
    
    return pipeline_result

# In publish stage:
try:
    result = await execute_publish(...)
except Exception as e:
    error_batch_id = batch_id if 'batch_id' in locals() else "unknown"
    logger.error("Publish stage failed", error=str(e), batch_id=error_batch_id)
    return {
        "status": "failed",
        "batch_id": error_batch_id,
        "error": str(e)
    }
```

**Reference:** Error-handling utilities on `RVUIngestor`

**When to Use:**
- Need structured error responses for API consumers
- Want to preserve error context for debugging
- Supporting graceful degradation

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Error Handling Implementation

#### 1.8.8 Database Loader Function Pattern

**Purpose:** Standardize database loading interface while allowing dataset-specific implementation.

**Problem:** `execute_publish()` needs to load data to database, but each dataset has different table schemas and loading logic.

**Solution:** Wrap dataset-specific loader functions in a standardized signature that `execute_publish()` can call.

**Implementation:**

```python
# In ingestor publish method:
def rvu_loader_func(enriched_data_dict: Dict[str, Any], release_id: str, batch_id: str, vintage_date: str) -> Dict[str, Any]:
    """Dataset-specific loader function with standardized signature.
    
    Args:
        enriched_data_dict: Dictionary of dataset_name -> DataFrame
        release_id: Release identifier
        batch_id: Batch identifier
        vintage_date: Vintage date string (YYYY-MM-DD)
    
    Returns:
        Dictionary with loading results (record_counts, table_names, etc.)
    """
    # Create DB session if not provided
    from cms_pricing.database import SessionLocal
    db_session = self.db_session
    if db_session is None:
        db_session = SessionLocal()
        self.db_session = db_session  # Cache for reuse
    
    try:
        return load_rvu_dataframes(
            enriched_data_dict,
            release_id,
            batch_id,
            vintage_date,
            db_session,
        )
    except Exception as e:
        logger.error("Database loading failed", error=str(e), batch_id=batch_id)
        return {"error": str(e)}

# Pass to execute_publish:
result = await execute_publish(
    enriched_batch=enriched_batch_dict,
    config=config,
    db_session=self.db_session,
    loader_func=rvu_loader_func,  # Dataset-specific loader
    drift_detector=drift_detector
)
```

**Reference:** Loader callback wiring in `RVUIngestor._publish_stage`

**Loader Function Signature:**
```python
def loader_func(
    enriched_data_dict: Dict[str, Any],
    release_id: str,
    batch_id: str,
    vintage_date: str
) -> Dict[str, Any]:
    """Standard loader function signature.
    
    Returns dict with keys:
    - record_counts: Dict[str, int] - Record counts per dataset
    - table_names: Dict[str, str] - Table names per dataset
    - errors: List[str] - Any errors encountered
    """
    pass
```

**When to Use:**
- Need to load multiple datasets to different database tables
- Want to reuse `execute_publish()` while keeping loading logic dataset-specific
- Supporting multiple database schemas per ingestor

**Cross-References:**
- **PRD-rvu-gpci-prd-v0.1.md §6.3** - Publish Stage Implementation
- **STD-data-architecture-impl-v1.0.md §1.8.5** - Publish Stage Callbacks
- **STD-database-platform-prd-v1.0.md §6.1** - Loader Pattern Documentation

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

Manages reference data joins and lookups (honors `REF_MODE` per §4.2):

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

**See also:** §4.2 Dual-Mode Reference Data Access for inline vs curated modes

### 2.6 Component Initialization Pattern

**Canonical pattern from `mpfs_ingestor.py:58-68`:**

```python
def __init__(self, output_dir: str = "./data/ingestion/mpfs", db_session: Any = None):
    super().__init__(output_dir, db_session)
    
    # Initialize services
    self.snapshot_service = DatasetSnapshotService()
    self.cf_fetcher = ConversionFactorFetcher(str(Path(self.output_dir) / "raw"))
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
    
    # Schema contracts (pre-cache for performance)
    self.schema_contracts = self._load_schema_contracts()
    # Cache schema contracts at initialization to avoid repeated lookups during validation
    self._cached_schemas = {}
    for dataset_name, schema_name in self._get_dataset_schema_mapping().items():
        schema = self.schema_registry.get_contract(schema_name)
        if schema:
            self._cached_schemas[dataset_name] = schema
```

---

## 3. Schema Contracts & Validation

> **Phase 2 at a glance**

| Pattern | Primary Modules | Doc Reference |
|---------|-----------------|---------------|
| SchemaService bootstrap & caching | `services/schema_service.py`, `ingestors/rvu_ingestor.py` | §3.2.1 |
| DatasetSpec loaders & dispatcher | `datasets/rvu_loaders.py`, `datasets/rvu_spec.py`, `stages/publish.py` | §3.2.2 |
| Adapter extraction via DatasetSpec | `datasets/rvu_adapter.py`, `datasets/rvu_spec.py`, `stages/normalize.py` | §3.2.3 |
| ValidationService + business rules | `datasets/rvu_spec.py`, `services/validation_service.py` | §3.3 |
| Stage executors & thin orchestrator | `stages/*.py`, `ingestors/rvu_ingestor.py` | §1.3, §1.6 |

For the full change log see `docs/release_notes/phase2_refactor.md`.

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

**Performance Best Practices:**
- **Pre-cache schemas at ingestor initialization** to avoid repeated lookups during validation
- **Use vectorized pandas operations** for domain validation (`.isin()` instead of set operations)
- For datasets with 10k+ rows, vectorized validation is 10-50x faster than row-by-row or set-based approaches

### 3.2.1 SchemaService Pattern (Phase 2)

**Location:** `cms_pricing/ingestion/services/schema_service.py`

SchemaService centralises schema registration and caching. Typical RVU usage:

```python
service_config = ServiceConfig(dataset_name=self.dataset_name, output_dir=output_dir, enable_schema_registry=True, lazy_init=True)
self.services = ServiceFactory(service_config)
self.services.schema_service.bootstrap_rvu_schemas(self.services.schema_registry)
self._cached_schemas = SchemaService.cache_schemas(
    self.services.schema_registry,
    {"pprrvu": "cms_pprrvu", "gpci": "cms_gpci", "oppscap": "cms_oppscap", "anescf": "cms_anescf", "localitycounty": "cms_localitycounty"}
)
```

**Benefits**
- Idempotent registration (safe re-entry, no double-register errors)
- One place to maintain schema contracts, reusable across ingestors
- Cached schemas remove repeat registry lookups (≈5‑10 % validation gain)

**Migration path**
1. Delete legacy `_register_schema_contracts()` helpers.
2. Instantiate `ServiceFactory` and call `schema_service.bootstrap_*()` in `__init__`.
3. Cache schema IDs and pass `cached_schemas` into normalize/validate stages.

**References:** `RVUIngestor.__init__`, `cms_pricing/ingestion/services/schema_service.py`.

### 3.2.2 DatasetSpec.loader Pattern (Phase 2)

**Location:** `cms_pricing/ingestion/datasets/{dataset}_loaders.py`

Database loaders now live in `{dataset}_loaders.py` modules. Each DatasetSpec points to its loader, and publish stages call a dispatcher (e.g., `load_rvu_dataframes`) that creates the `Release` record and executes the dataset loaders in chunks.

**Benefits**
- Shared bulk-insert / natural-key dedupe logic across datasets.
- Loader functions are easy to unit test in isolation.
- Ingestors focus on orchestration only; persistence lives in one place.

**Migration path**
1. Move `_load_*_data()` helpers into `{dataset}_loaders.py` and expose a dispatcher.
2. Set `DatasetSpec.loader` to the extracted functions.
3. Update publish stage to call the dispatcher rather than inline SQL.

**References:** `datasets/rvu_loaders.py`, `datasets/rvu_spec.py`, `stages/publish.py`.

### 3.2.3 Adapter Extraction Pattern (Phase 2)

**Location:** `cms_pricing/ingestion/datasets/{dataset}_adapter.py`

Adapters handle file routing + parsing in `{dataset}_adapter.py`. DatasetSpec decides which parser to run, and normalize stages default to the shared adapter when none is passed.

**Benefits**
- Eliminates hand-written `_classify_inner_file()` logic; routing is declarative via DatasetSpec.
- Normalise stage simply calls `adapter_func(raw_batch)`; ingestors keep a 10‑line delegate.
- Easy to write unit tests for adapters without touching ingestors.

**Migration path**
1. Move `_adapt_raw_data_sync()` into `{dataset}_adapter.py`.
2. Use `DatasetSpec.route_file()` + `DatasetSpec.parser` instead of hardcoded mappings.
3. Update normalize stage to accept an `adapter_func` (defaulting to the shared adapter).

**References:** `datasets/rvu_adapter.py`, `datasets/rvu_spec.py`, `stages/normalize.py`, `rvu_ingestor.py` thin delegate.

### 3.3 Validation Rules & Business Rules (Phase 2)

**Location:** `cms_pricing/ingestion/datasets/{dataset}_spec.py` + `cms_pricing/ingestion/services/validation_service.py`

DatasetSpec now owns all validation logic:

- `validation_rules`: simple boolean validators for format/structure checks (e.g., `validate_hcpcs_format`).
- `business_rules`: functions returning `ValidationResult` for rich reporting (e.g., natural-key uniqueness).
- `ValidationService.register_dataset_business_rules(spec)` wires business rules automatically during ingestor init.

**Benefits:**
- Single source of truth for dataset validation.
- Declarative registration via ValidationService (no manual wiring).
- Works across ingestors; detailed `ValidationResult` output when needed.

**Migration Path:**
1. Move existing `_register_validation_rules()` helpers into DatasetSpec (`validation_rules` / `business_rules`).
2. Remove inline ValidationEngine wiring; use `self.services.validation_service` instead.
3. Trim obsolete ingestor methods once specs carry the logic (~100 lines saved in RVU).

**Reference Implementation:** `cms_pricing/ingestion/datasets/rvu_spec.py`, `cms_pricing/ingestion/services/validation_service.py`, `RVUIngestor` initialization hooks.

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

**See also:** §4.2 for dual-mode reference access and publish gates

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

# Reference Data Mode (see §4.2)
REF_MODE=curated  # curated (default, prod) | inline (dev/test only)
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

---

### 4.2 Dual-Mode Reference Data Access

**Purpose:** Enable fast dev/CI while protecting production quality by supporting two controlled modes of reference data access.

**Added:** 2025-10-17 (v1.0.2)  
**Reference:** `cms_pricing/infra/reference_mode.py`

#### 4.2.1 Modes & Feature Flag

- **`REF_MODE=curated`** (default, required for publish): Load refs from `/ref/<domain>/<dataset>/<vintage>/` via `ReferenceDataManager`.
- **`REF_MODE=inline`** (dev/test only): Load minimal, in-repo shims that mirror curated schemas for rapid iteration.

**Fail-closed:** When `REF_MODE=inline`, normalize/enrich may run, but publish **MUST** block with a clear message.

```bash
# Dev/CI: Fast iteration
export REF_MODE=inline  # Blocks publish, allows inspect

# Staging/Prod: Full pipeline
export REF_MODE=curated  # Allows publish (default)
```

#### 4.2.2 Contract Parity (Schema & Dtypes)

- Inline refs **must** validate against the same Schema Registry contracts as curated refs (SemVer).
- Use Arrow decimals for numeric precision (e.g., `rvu_*: decimal(8,3)`, `cf_value: decimal(9,6)`).
- **Determinism:** Sorted outputs + `row_content_hash`; null/numeric normalization for hashing.

**Example validation:**

```python
# cms_pricing/ingestion/normalize/locality_fips_lookup.py
STATE_SCHEMA = {
    'state_fips': 'str',      # 2-digit zero-padded
    'state_name': 'str',      # ALL CAPS
    'state_abbr': 'str',      # 2-letter
    'alt_names': 'str',       # Pipe-delimited or empty
}

def get_states_dataframe() -> pd.DataFrame:
    """Returns DataFrame matching curated schema"""
    df = pd.DataFrame(rows)
    # Validate schema
    for col in STATE_SCHEMA:
        assert col in df.columns, f"Missing column: {col}"
    return df.sort_values('state_fips').reset_index(drop=True)
```

#### 4.2.3 Provider Interface

Both modes expose the same interface:

```python
class RefProvider:
    def states(self) -> DataFrame: ...
    def counties(self) -> DataFrame: ...
    def zip_zcta(self) -> DataFrame: ...
    # Returns frames with columns per schema_id and metadata:
    # ref_version, ref_vintage
```

**Selection:**

```python
from cms_pricing.ingestion.normalize.reference_mode import get_config, ReferenceMode

config = get_config()
if config.mode == ReferenceMode.INLINE:
    provider = InlineProvider()
else:
    provider = ReferenceDataManager()
```

#### 4.2.4 Guardrails

1. **Publish gate:** Block curated publish if `REF_MODE!=curated`.
   ```python
   from cms_pricing.ingestion.normalize.reference_mode import validate_publish_allowed
   
   validate_publish_allowed(config, stage="publish")
   # Raises RuntimeError if REF_MODE=inline
   ```

2. **No Restricted leakage:** Inline providers must not include Restricted content (e.g., CPT descriptions). Keys only.

3. **Zip safety & size limits:** Enforce max compressed/uncompressed sizes; reject path traversal (`..`) in zips.

#### 4.2.5 Observability & Metadata

Emit on each run:

```python
{
  "ref_source": "curated" | "inline",
  "ref_vintage_used": "2025-01-01" | "dev-inline",
  "ref_version": "1.0",
  "conflict_rate": 0.002,
  "fallback_usage_rate": 0.01
}
```

Per-file parse summary: `(release_id, schema_id, file_sha256, encoding, rows, parse_ms)`.

#### 4.2.6 Bootstrap (Recommended)

- **Seed one small real curated ref** (e.g., `/ref/census/fips_states/2025/{data.parquet, manifest.json}`) to exercise manifests, lineage, and contracts.
- **Use inline only for heavier sets** (ZIP↔ZCTA, counties) during early development.

**Recommended structure:**

```
/ref/
  census/
    fips_states/2025/
      us_states.parquet        # 51 rows - ship in repo ✅
      manifest.json
  cms/
    hcpcs_codes/2025/
      [use inline during dev]  # 10K+ rows - inline dict ok
```

#### 4.2.7 Tests (Must Pass)

```python
# Test 1: Inline frames validate against curated contracts
def test_inline_schema_validation():
    states = get_states_dataframe()
    assert 'state_fips' in states.columns
    assert 'mapping_confidence' in states.columns

# Test 2: Publish gate with inline mode
def test_publish_blocked_with_inline():
    os.environ['REF_MODE'] = 'inline'
    config = get_reference_config()
    with pytest.raises(RuntimeError, match="Cannot.*publish"):
        validate_publish_allowed(config)

# Test 3: Determinism
def test_inline_deterministic():
    df1 = get_states_dataframe()
    df2 = get_states_dataframe()
    assert df1.equals(df2)
    assert (df1['mapping_confidence'] == 1.0).all()

# Test 4: Precedence/tie-breaker unchanged across modes
def test_tie_breaker_parity():
    # See Appendix J for tie-breaker rules
    pass
```

#### 4.2.8 Runbook

**Dev/CI:**
```bash
export REF_MODE=inline
make test-parsers  # Run normalize/enrich
make inspect       # Inspect artifacts
# Publish blocked by design ✅
```

**Staging/Prod:**
```bash
export REF_MODE=curated  # Default
# Require green freshness for refs per SLAs
make publish
```

---

### 4.4 Release & Batch ID Generation

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

### 4.4 Logging Conventions

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

### 4.5 Observability Events

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

### 4.6 SLA Enforcement

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
| **Snapshot Reuse** | MPFS | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `discover_source_files()` uses `DatasetSnapshotService` + `ConversionFactorFetcher` | Reuses RVU/GPCI snapshots, fetches CF artifacts |

> **Release namespace guardrail:** Multi-dataset ingestors (RVU → MPFS, RVU → OPPS) MUST derive dataset-specific release IDs (e.g., `gpci_2025_B`). Do not reuse the base release ID across datasets—tests must assert the derived prefixes.

> **Manifest resolver requirement:** Until the snapshot schema stores an explicit parquet path, every ingestor that relies on `DatasetSnapshotService` MUST keep the manifest JSON accessible on disk or ship a resolver that can map manifest entries to parquet files. Deleting manifests without re-registering snapshots is a break-glass operation.
| **Direct Links** | RVU | `cms_pricing/ingestion/ingestors/rvu_ingestor.py` | `discover_source_files()`, `land()` | Quarterly releases, fixed-width parsing |
| **Quarterly Navigation** | OPPS | `cms_pricing/ingestion/ingestors/opps_ingestor.py` | `discover_files()`, `_land_stage()` | Handles AMA license interstitial |
| **Reference Data Join** | Geography | `cms_zip_locality_ingestor.py` | `_enrich_data()` | Census crosswalk joins |

### 5.2 Stage Implementation Examples

| Stage | Reference Implementation | Reference | Key Pattern |
|-------|-------------------------|-----------|-------------|
| **Discovery** | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `discover_source_files()` | Use `DatasetSnapshotService.get_latest_snapshot()` + `ConversionFactorFetcher.ensure_conversion_factor()`, return `List[SourceFile]` |
| **Land** | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `_land_stage()` | Download files, calculate checksums, create `RawBatch` |
| **Validate** | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `_validate_stage()` | Structural + domain + statistical validation |
| **Normalize** | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `_normalize_stage()` | Parse ZIP/CSV/Excel to DataFrames |
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

### 6.2 Step 1: Create Ingestor Class (Thin Orchestrator Pattern - Phase 2)

**Target:** Ingestors should be <1,500 lines (RVU currently 1,383 lines, 67.4% reduction from 4,247)

Create file: `cms_pricing/ingestion/ingestors/{dataset}_ingestor.py`

```python
#!/usr/bin/env python3
"""
{DATASET} DIS-Compliant Ingestor - Thin Orchestrator Pattern
Following STD-data-architecture-prd-v1.0

Phase 2 Pattern: Ingestors delegate all logic to stage modules and shared services.
Target: <1,000 lines (pure orchestration, no inline business logic).
"""

import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional

import structlog

from ..contracts.ingestor_spec import (
    BaseDISIngestor, SourceFile, RawBatch, AdaptedBatch, 
    StageFrame, OutputSpec, SlaSpec,
    ReleaseCadence, DataClass
)
from ..scrapers.cms_{dataset}_scraper import CMS{Dataset}Scraper
from ..services import ServiceFactory, ServiceConfig
from ..stages import (
    execute_land, LandConfig,
    execute_validate, ValidateConfig,
    execute_normalize, NormalizeConfig,
    execute_enrich, EnrichConfig,
    execute_publish, PublishConfig,
)
from ..datasets.{dataset}_spec import {DATASET}_DATASETS  # If using DatasetSpec pattern

logger = structlog.get_logger()


class {Dataset}Ingestor(BaseDISIngestor):
    """DIS-compliant ingestor for {DATASET} - Thin orchestrator pattern"""
    
    def __init__(self, output_dir: str = "./data/ingestion/{dataset}", db_session: Any = None):
        super().__init__(output_dir, db_session)
        
        # Initialize shared services via factory (lazy initialization)
        service_config = ServiceConfig(
            output_dir=output_dir,
            dataset_name=self.dataset_name,
            enable_observability=True,
            enable_quarantine=True,
            enable_reference_data=True,
            enable_validation=True,
            enable_schema_registry=True,
            lazy_init=True,
            db_session=db_session
        )
        self.services = ServiceFactory(service_config)
        
        # Bootstrap schemas (if using SchemaService pattern)
        # self.services.schema_service.bootstrap_{dataset}_schemas(self.services.schema_registry)
        
        # Cache schemas for performance (5-10% validation speedup)
        # self._dataset_schema_map = {"dataset_name": "schema_id", ...}
        # self._cached_schemas = self.services.schema_service.cache_schemas(
        #     self.services.schema_registry, self._dataset_schema_map
        # )
        
        # Register business rules from DatasetSpecs (if using DatasetSpec pattern)
        # validation_service = self.services.validation_service
        # for dataset_spec in {DATASET}_DATASETS.values():
        #     validation_service.register_dataset_business_rules(dataset_spec)
        
        # Initialize scraper
        self.scraper = CMS{Dataset}Scraper(str(Path(self.output_dir) / "scraped"))
        
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
        
        # Metadata tracking
        self.current_release_id: Optional[str] = None
        self.current_batch_id: Optional[str] = None
    
    # Stage methods delegate to stage modules (Phase 2 pattern)
    async def land(self, release_id: str, source_files: Optional[List[SourceFile]] = None):
        """Land stage - delegate to stage module"""
        from ..stages import execute_land, LandConfig
        
        config = LandConfig(
            output_dir=self.output_dir,
            dataset_name=self.dataset_name,
            enable_guidance_extraction=True
        )
        
        result = await execute_land(
            release_id=release_id,
            source_files=source_files or [],
            config=config,
            scraper=self.scraper,
            observability_collector=self.services.observability_collector,
            quarantine_manager=self.services.quarantine_manager
        )
        return result
    
    async def normalize(self, validated_batch, raw_batch):
        """Normalize stage - delegate to stage module"""
        from ..stages import execute_normalize, NormalizeConfig
        
        config = NormalizeConfig(
            output_dir=self.output_dir,
            dataset_name=self.dataset_name,
            enable_schema_validation=True
        )
        
        result = await execute_normalize(
            validated_batch=validated_batch,
            raw_batch=raw_batch,
            config=config,
            adapter_func=None,  # Defaults to shared adapter if None
            schema_registry=self.services.schema_registry,
            validation_engine=self.services.validation_service.engine,
            cached_schemas=getattr(self, '_cached_schemas', None),
            dataset_schema_map=getattr(self, '_dataset_schema_map', None)
        )
        return result
    
    # ... other stage methods follow same pattern
    
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
    
    Per STD-parser-contracts-prd-v2.0 §6.1, parsers return ParseResult(data, rejects, metrics).
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
            
            # Schema validation: Validate parsed dataframes against registered schema contracts
            # Per STD-parser-contracts-prd-v2.0 §285: "Schema validation runs AFTER parsing, BEFORE enrichment"
            # Performance: Use cached schemas and vectorized validation for large datasets
            from cms_pricing.ingestion.contracts.schema_registry import schema_registry
            schema_validation_result = schema_registry.validate_dataframe(result.data, schema_id)
            
            if not schema_validation_result.get("valid", False):
                # Schema validation failures: add to rejects and log warnings
                errors = schema_validation_result.get("errors", [])
                warnings = schema_validation_result.get("warnings", [])
                logger.warning("Schema validation failed for parsed data",
                             filename=filename,
                             errors=len(errors),
                             warnings=len(warnings))
                # Optionally quarantine schema validation failures or add to rejects
            
            # Ingestor writes artifacts
            adapted_data[filename] = result.data  # Valid rows
            all_rejects.append(result.rejects)    # Rejected rows
            all_metrics.append(result.metrics)    # Parse metrics
            
            logger.info("File normalization completed", 
                       filename=filename,
                       valid_rows=len(result.data),
                       rejected_rows=len(result.rejects),
                       schema_valid=schema_validation_result.get("valid", True))
            
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
            # ⚠️ PERFORMANCE: Use pandas .merge() with proper indexing for reference joins
            # Pre-cache reference DataFrames at ingestor initialization to avoid repeated loads
            enriched = await self._join_reference_data(data)
            
            # Compute derived fields
            # ⚠️ PERFORMANCE: Prefer vectorized operations over .apply(axis=1) for large datasets
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

**File:** `cms_pricing/ingestion/ingestors/mpfs_ingestor.py`

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

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

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

# Task completion audit (optional, for tracking implementation status)
python tools/audit_task_completion.py --dry-run

# Run tests
pytest tests/ingestors/test_{dataset}_ingestor_e2e.py -v

# Check coverage
pytest tests/ingestors/test_{dataset}_ingestor_e2e.py --cov=cms_pricing.ingestion.ingestors.{dataset}_ingestor --cov-report=html
```

---

## 10. Migration Guide for Existing Ingestors (Phase 2)

**Purpose:** Guide for migrating monolithic ingestors (MPFS, OPPS) to thin orchestrator pattern.

**Reference Implementation:** RVU ingester refactoring (Phase 2, Steps 1-7)
- **Before:** 4,247 lines
- **After:** 1,383 lines (67.4% reduction)
- **Extracted:** ~1,585 lines to reusable modules

### 10.1 Migration Steps (In Order)

**Step 1: Create DatasetSpecs**
- [ ] Define DatasetSpec for each dataset
- [ ] Add parser references
- [ ] Add schema IDs
- [ ] Add natural keys
- [ ] Add filename patterns
- [ ] Add loader function references (will be extracted in Step 3)

**Step 2: Extract Schema Registration**
- [ ] Create `SchemaService` with `bootstrap_{dataset}_schemas()` method
- [ ] Move schema contract definitions from ingestor to SchemaService
- [ ] Update ingestor to call `self.services.schema_service.bootstrap_{dataset}_schemas()`
- [ ] Remove `_register_schema_contracts()` method from ingestor (~300-400 lines)
- [ ] Cache schemas for performance: `self._cached_schemas = SchemaService.cache_schemas(...)`

**Step 3: Extract Database Loaders**
- [ ] Move `_load_*_data()` methods to `{dataset}_loaders.py`
- [ ] Create dispatcher function `load_{dataset}_dataframes()`
- [ ] Update DatasetSpec.loader to reference extracted functions
- [ ] Update publish stage to use `load_{dataset}_dataframes()` (or DatasetSpec.loader pattern)
- [ ] Remove loader methods from ingestor (~400+ lines)

**Step 4: Extract Adapter Logic**
- [ ] Move `_adapt_raw_data_sync()` to `{dataset}_adapter.py`
- [ ] Replace hardcoded routing (`_classify_inner_file()`) with `DatasetSpec.route_file()`
- [ ] Replace parser dict lookup with `DatasetSpec.parser()`
- [ ] Update normalize stage to use adapter module (default parameter for backward compatibility)
- [ ] Remove adapter method from ingestor (~400-500 lines)

**Step 5: Extract Business Rules**
- [ ] Move business rule functions to DatasetSpec (create `_create_{dataset}_business_rules()` functions)
- [ ] Distinguish between `validation_rules` (boolean) and `business_rules` (ValidationResult)
- [ ] Use ValidationService for registration
- [ ] Remove `_register_validation_rules()` method from ingestor (~100 lines)

**Step 6: Integrate Stage Modules**
- [ ] Replace inline stage logic with `execute_*` calls from stage modules
- [ ] Remove duplicate stage helpers (e.g., `_land_with_provided_files()`, `_validate_parsed_dataframes()`)
- [ ] Use module-level functions from stage modules instead of instance methods
- [ ] Pass adapter/loader functions as parameters (defaults provided for backward compatibility)
- [ ] Pass cached schemas to stage modules for performance (~290 lines removed)

**Step 7: Final Cleanup**
- [ ] Remove unused imports
- [ ] Remove unused instance variables
- [ ] Remove unused helper methods
- [ ] Verify line count <1,000
- [ ] Update docstrings with Phase 2 references

### 10.2 Migration Checklist

- [ ] DatasetSpecs created and tested
- [ ] Schema registration extracted to SchemaService
- [ ] Database loaders extracted to `{dataset}_loaders.py`
- [ ] Adapter logic extracted to `{dataset}_adapter.py`
- [ ] Business rules extracted to DatasetSpec
- [ ] Stage modules integrated (all stages delegate to `execute_*` functions)
- [ ] Line count <1,000 (target achieved)
- [ ] All tests passing
- [ ] PRD updated with new architecture
- [ ] Performance verified (no regression, schema caching provides 5-10% speedup)

### 10.3 Estimated Time

- **MPFS:** 6-8 hours (similar complexity to RVU, multiple datasets)
- **OPPS:** 6-8 hours (similar complexity to RVU, quarterly navigation)
- **Other ingestors:** 4-6 hours (simpler, fewer datasets or single dataset)

**Time Breakdown (per ingestor):**
- Step 1 (DatasetSpecs): 1-1.5 hours
- Step 2 (Schema Registration): 45 minutes
- Step 3 (Database Loaders): 1.5-2 hours
- Step 4 (Adapter Logic): 2 hours
- Step 5 (Business Rules): 30 minutes
- Step 6 (Stage Integration): 1 hour
- Step 7 (Cleanup): 30 minutes
- Testing & Verification: 1-2 hours

### 10.4 Benefits of Migration

**Maintainability:**
- **Easier to understand:** Ingestors are pure orchestration (<1,000 lines)
- **Easier to modify:** Logic lives in focused modules (single responsibility)
- **Clearer separation:** Ingestor vs. stage modules vs. dataset-specific modules

**Reusability:**
- **Components shared:** Stage modules, adapters, loaders reusable across ingestors
- **Consistent patterns:** Uniform architecture across all ingestors
- **Faster development:** New ingestors can reuse existing components

**Testability:**
- **Isolated components:** Modules can be unit tested independently
- **Easier mocking:** Thin dependencies (services, configs) easier to mock
- **Faster tests:** Isolated tests run faster than full integration tests

**Performance:**
- **Schema caching:** 5-10% validation speedup (eliminates repeated registry lookups)
- **Vectorized operations:** Stage modules use pandas vectorized operations
- **Bulk operations:** Database loaders use bulk insert with chunking

**Consistency:**
- **Uniform patterns:** All ingestors follow same architecture
- **Predictable structure:** Easy to find where logic lives
- **Better onboarding:** New developers can understand any ingestor quickly

### 10.5 Common Pitfalls & Solutions

**Pitfall 1: Breaking Tests During Migration**
- **Solution:** Keep thin delegate methods for backward compatibility
- **Solution:** Test after each extraction step
- **Solution:** Use feature flags if needed to toggle old/new behavior

**Pitfall 2: Import Cycles**
- **Solution:** ServiceFactory lazy initialization prevents circular imports
- **Solution:** Use module-level functions instead of instance methods where possible
- **Solution:** Import services at method level, not module level if needed

**Pitfall 3: Performance Regression**
- **Solution:** Pre-cache schemas in `__init__` (eliminates repeated lookups)
- **Solution:** Use vectorized pandas operations instead of row iteration
- **Solution:** Profile before/after to verify improvements

**Pitfall 4: Missing Dependencies**
- **Solution:** Pass all dependencies as parameters to extracted functions
- **Solution:** Use ServiceFactory for shared services (consistent initialization)
- **Solution:** Document all function signatures clearly

**Pitfall 5: Incomplete Extraction**
- **Solution:** Verify line count <1,000 after all steps
- **Solution:** Remove all unused imports and methods
- **Solution:** Check that all stage logic lives in stage modules

### 10.6 Reference Resources

- **Phase 2 Completion Plan:** `artifacts/phase2_completion_plan.md` (detailed step-by-step guide)
- **RVU Reference Implementation:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (1,383 lines, completed migration)
- **Step-by-Step Plans:**
  - Step 1: `artifacts/phase2_completion_plan.md` §Step 1
  - Step 2: `artifacts/phase2_completion_plan.md` §Step 2
  - Step 3: `artifacts/phase2_step3_detailed_plan.md`
  - Step 4: `artifacts/phase2_step4_detailed_plan.md`
  - Step 5: `artifacts/phase2_step5_detailed_plan.md`
  - Step 6: `artifacts/phase2_step6_detailed_plan.md`
- **Verification Reports:**
  - Step 4: `artifacts/phase2_step4_verification_report.md`
  - Step 5: `artifacts/phase2_step5_verification_report.md`

---

## 11. Troubleshooting & Common Issues

### 11.1 Discovery Issues

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

### 11.2 Validation Failures

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
