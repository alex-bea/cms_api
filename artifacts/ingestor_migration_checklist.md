# Ingestor Migration Checklist: Monolithic to Modular Architecture

**Purpose:** Step-by-step checklist for migrating monolithic ingestors (MPFS, OPPS, etc.) to the modular architecture pattern established in Phase 2 RVU refactoring.

**Reference Implementation:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (990 lines, down from 4,247)

**Estimated Time:** 2-3 days per ingestor

**Last Updated:** 2025-11-04

---

## Pre-Migration Assessment

### Step 0: Assess Current State

- [ ] **Measure current ingestor line count**
  - Command: `wc -l cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`
  - Target: Reduce to <1,000 lines

- [ ] **Identify large methods (>100 lines)**
  - Command: `grep -n "^    def " {ingestor}_ingestor.py | while read line; do echo "$line"; done`
  - Look for: schema registration, database loaders, adapter logic, validation rules

- [ ] **Identify dataset-specific logic**
  - File classification/routing logic
  - Parser lookup dictionaries
  - Hardcoded dataset names
  - Switch statements on dataset types

- [ ] **Document current dependencies**
  - Direct service instantiations (ValidationEngine, SchemaRegistry, etc.)
  - Inline helper methods
  - Stage logic embedded in ingestor

- [ ] **Check test coverage**
  - Ensure tests exist before refactoring
  - Note which tests might need updates

**Time:** 30 minutes  
**Output:** Assessment document with current state metrics

---

## Phase 1: Create DatasetSpecs

### Step 1.1: Define DatasetSpec Instances

**File to Create/Update:** `cms_pricing/ingestion/datasets/{dataset}_spec.py`

- [ ] **Create DatasetSpec for each dataset**
  ```python
  from .spec import DatasetSpec, EnrichmentRule
  
  {DATASET}_DATASETS: Dict[str, DatasetSpec] = {
      "dataset1": DatasetSpec(
          dataset_id="dataset1",
          parser=parse_dataset1,
          schema_id="cms_dataset1",
          natural_keys=["key1", "key2"],
          loader=load_dataset1_data,  # Will be wired in Step 2
          validation_rules=[...],
          enrichment_rules=[...],
          business_rules=[...],  # Will be added in Step 4
          filename_patterns=[r".*dataset1.*\.(txt|csv|xlsx)$"]
      ),
      # ... more datasets
  }
  ```

- [ ] **Wire existing parsers**
  - Import parser functions from `parsers/` modules
  - Reference them in `DatasetSpec.parser` field

- [ ] **Define natural keys**
  - Document unique identifier columns for each dataset
  - These will be used for deduplication in loaders

- [ ] **Define filename patterns**
  - Regex patterns for file routing
  - Used by `DatasetSpec.route_file()` method

**Time:** 1-2 hours  
**Dependencies:** None  
**Reference:** `cms_pricing/ingestion/datasets/rvu_spec.py`

---

## Phase 2: Extract Database Loaders

### Step 2.1: Create Loader Module

**File to Create:** `cms_pricing/ingestion/datasets/{dataset}_loaders.py`

- [ ] **Create dispatcher function**
  ```python
  def load_{dataset}_dataframes(
      dataframes: Dict[str, pd.DataFrame],
      release_id: str,
      batch_id: str,
      vintage_date: str,
      db_session: Session,
  ) -> Dict[str, Any]:
      """Persist all {dataset} DataFrames to database using dataset-specific loaders."""
      # Create Release record
      # Iterate dataframes, call DatasetSpec.loader for each
      # Return results
  ```

- [ ] **Extract individual loader functions**
  - One function per dataset: `load_dataset1_data()`, `load_dataset2_data()`, etc.
  - Signature: `(df: pd.DataFrame, release_uuid: Any, batch_id: str, db_session: Session) -> int`
  - Function should:
    - Handle natural key deduplication
    - Map DataFrame columns to SQLAlchemy model fields
    - Use bulk insert (`bulk_insert_mappings()`)
    - Return number of rows loaded

- [ ] **Add module docstring with Phase 2 refactoring context**
  ```python
  """
  {Dataset} dataset loader utilities.
  
  Phase 2 Refactoring Context:
      - Step 2: Database loader extraction
        • Plan: artifacts/phase2_completion_plan.md (§Step 2)
        • Verification: docs/ingestion_verification.md (loader checks)
  
  This module centralizes the database loading logic that was previously embedded
  in {Dataset}Ingestor. Functions operate on pandas DataFrames produced by the
  normalize stage and persist them into the {dataset} schema tables.
  """
  ```

- [ ] **Add function-level code comments**
  ```python
  # Phase 2 Step 2: Database loader extraction
  # See: artifacts/phase2_completion_plan.md (§Step 2)
  def load_dataset1_data(...):
      ...
  ```

**Time:** 1.5-2 hours  
**Dependencies:** Step 1.1 (DatasetSpecs must exist)  
**Reference:** `cms_pricing/ingestion/datasets/rvu_loaders.py`

### Step 2.2: Wire Loaders to DatasetSpecs

**File to Update:** `cms_pricing/ingestion/datasets/{dataset}_spec.py`

- [ ] **Import loader functions**
  ```python
  from .{dataset}_loaders import (
      load_dataset1_data,
      load_dataset2_data,
      ...
  )
  ```

- [ ] **Update DatasetSpec.loader fields**
  ```python
  DatasetSpec(
      dataset_id="dataset1",
      loader=load_dataset1_data,  # Wire loader function
      ...
  )
  ```

**Time:** 15 minutes  
**Dependencies:** Step 2.1

### Step 2.3: Update Publish Stage

**File to Update:** `cms_pricing/ingestion/stages/publish.py`

- [ ] **Verify default loader wiring**
  - Check that `execute_publish()` defaults to `load_{dataset}_dataframes` when available
  - If not, add default loader logic (similar to RVU pattern)

**Time:** 15 minutes  
**Dependencies:** Step 2.1

### Step 2.4: Remove Loader Methods from Ingestor

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Remove `_load_dataframes_to_database()` method**
- [ ] **Remove individual `_load_*_data()` methods**
- [ ] **Update `publish()` method to call loader dispatcher**
  ```python
  from ..datasets.{dataset}_loaders import load_{dataset}_dataframes
  
  # In publish() method:
  load_results = load_{dataset}_dataframes(
      dataframes=enriched_data,
      release_id=release_id,
      batch_id=batch_id,
      vintage_date=vintage_date,
      db_session=self.db_session
  )
  ```

**Time:** 30 minutes  
**Dependencies:** Step 2.1-2.3

### Step 2.5: Test Loader Extraction

- [ ] **Run compilation check**
  ```bash
  python -m compileall cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py
  ```

- [ ] **Test individual loader functions**
  - Create test fixtures with sample DataFrames
  - Verify loaders work correctly
  - Test transaction rollback on failure

- [ ] **Test full pipeline with database**
  - Run land → validate → normalize → enrich → publish
  - Verify data appears in database tables
  - Verify row counts match expectations

**Time:** 30 minutes  
**Dependencies:** Step 2.4

---

## Phase 3: Extract Adapter Logic

### Step 3.1: Create Adapter Module

**File to Create:** `cms_pricing/ingestion/datasets/{dataset}_adapter.py`

- [ ] **Create adapter function**
  ```python
  def adapt_{dataset}_raw_data(
      raw_batch: RawBatch,
      *,
      dataset_specs: Optional[Dict[str, DatasetSpec]] = None,
      schema_registry: Optional[Any] = None,
      ...
  ) -> AdaptedBatch:
      """Parse raw {dataset} archives into canonical DataFrames using DatasetSpec routing."""
      # Use DatasetSpec.route_file() for file routing
      # Use DatasetSpec.parser() for parser invocation
      # Use DatasetSpec.schema_id for schema contracts
  ```

- [ ] **Replace hardcoded file classification**
  - Old: `_classify_inner_file()` or switch statements
  - New: `spec.route_file(filename)` for each DatasetSpec

- [ ] **Replace hardcoded parser lookup**
  - Old: `self._dataset_parsers[dataset_key]` or if/elif chains
  - New: `spec.parser()` from DatasetSpec

- [ ] **Use DatasetSpec.schema_id for schema contracts**
  - Old: Hardcoded schema names
  - New: `spec.schema_id` from DatasetSpec

- [ ] **Add module docstring with Phase 2 refactoring context**
  ```python
  """
  {Dataset} Adapter Module
  ------------------
  
  Phase 2 Refactoring Context:
      - Step 3: Adapter extraction
        • Plan: artifacts/phase2_step3_detailed_plan.md
        • Verification: artifacts/phase2_regression_test_results.md
  
  Parses raw {dataset} archives into AdaptedBatch objects using DatasetSpec routing.
  This is an extraction of the `_adapt_raw_data_sync` logic from {Dataset}Ingestor
  so stage modules can reuse the adapter outside the ingestor class.
  """
  ```

- [ ] **Add function-level code comments**
  ```python
  # Phase 2 Step 3: Adapter extraction
  # See: artifacts/phase2_step3_detailed_plan.md
  def adapt_{dataset}_raw_data(...):
      ...
  ```

**Time:** 2-3 hours  
**Dependencies:** Step 1.1 (DatasetSpecs must exist)  
**Reference:** `cms_pricing/ingestion/datasets/rvu_adapter.py`

### Step 3.2: Update Normalize Stage

**File to Update:** `cms_pricing/ingestion/stages/normalize.py`

- [ ] **Add default adapter wiring** (if not already present)
  ```python
  from ..datasets.{dataset}_adapter import adapt_{dataset}_raw_data
  
  # In execute_normalize():
  if adapter_func is None:
      adapter_func = adapt_{dataset}_raw_data  # Default adapter
  ```

**Time:** 15 minutes  
**Dependencies:** Step 3.1

### Step 3.3: Create Thin Delegate in Ingestor

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Replace `_adapt_raw_data_sync()` with thin delegate**
  ```python
  from ..datasets.{dataset}_adapter import adapt_{dataset}_raw_data
  
  def _adapt_raw_data_sync(self, raw_batch: RawBatch) -> AdaptedBatch:
      """Delegate to the shared {dataset} adapter module (sync path)."""
      return adapt_{dataset}_raw_data(
          raw_batch,
          dataset_specs={DATASET}_DATASETS,
          schema_registry=self.services.schema_registry,
          release_id_override=self.current_release_id,
          ...
      )
  ```

**Time:** 15 minutes  
**Dependencies:** Step 3.1-3.2

### Step 3.4: Test Adapter Extraction

- [ ] **Test adapter with real files**
  - Use actual ZIP/CSV files from CMS
  - Verify routing works correctly
  - Verify all datasets parse correctly

- [ ] **Test error handling**
  - Unclassified files
  - Parse failures
  - Missing schema contracts

- [ ] **Run full pipeline test**
  - Verify end-to-end pipeline works
  - Compare outputs before/after extraction

**Time:** 1 hour  
**Dependencies:** Step 3.3

---

## Phase 4: Extract Business Rules

### Step 4.1: Identify Business Rules

**File to Analyze:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Find complex validation rules**
  - Look for methods returning `ValidationResult` (not just `bool`)
  - Natural key uniqueness checks
  - Range/domain validations
  - Cross-field validations

- [ ] **Distinguish from simple validation rules**
  - `validation_rules`: Simple boolean validators (`df -> bool`)
  - `business_rules`: Complex validators (`df -> ValidationResult`)

**Time:** 30 minutes  
**Dependencies:** None

### Step 4.2: Create Business Rule Functions

**File to Update:** `cms_pricing/ingestion/datasets/{dataset}_spec.py`

- [ ] **Create business rule factory functions**
  ```python
  def _create_dataset1_business_rules() -> List[Callable[[pd.DataFrame], ValidationResult]]:
      """Create business rule validators for dataset1."""
      def validate_dataset1_uniqueness(df: pd.DataFrame) -> ValidationResult:
          """Validate dataset1 natural key uniqueness."""
          # Implementation
          return ValidationResult(...)
      return [validate_dataset1_uniqueness]
  ```

- [ ] **Wire to DatasetSpecs**
  ```python
  DatasetSpec(
      dataset_id="dataset1",
      business_rules=_create_dataset1_business_rules(),
      ...
  )
  ```

**Time:** 1 hour  
**Dependencies:** Step 4.1  
**Reference:** `cms_pricing/ingestion/datasets/rvu_spec.py` (see `_create_pprrvu_business_rules()`)

### Step 4.3: Create ValidationService (if not exists)

**File to Check:** `cms_pricing/ingestion/services/validation_service.py`

- [ ] **Verify ValidationService exists**
  - If exists: Proceed to Step 4.4
  - If not: Create using RVU pattern as template

**Time:** 15 minutes (if creation needed)  
**Dependencies:** None

### Step 4.4: Auto-Register Business Rules

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Add auto-registration in `__init__`**
  ```python
  # Register dataset-specific business rules with validation engine (Step 4)
  validation_service = self.services.validation_service
  for dataset_spec in {DATASET}_DATASETS.values():
      validation_service.register_dataset_business_rules(dataset_spec)
  ```

- [ ] **Remove old `_register_validation_rules()` method**
  - If it exists, remove it (should be ~100 lines)

**Time:** 15 minutes  
**Dependencies:** Step 4.2-4.3

### Step 4.5: Test Business Rules

- [ ] **Verify rules are registered**
  - Check that rules are available during validation
  - Test validation still works end-to-end

- [ ] **Test rule execution**
  - Verify rules are applied correctly
  - Test with invalid data to trigger rules

**Time:** 30 minutes  
**Dependencies:** Step 4.4

---

## Phase 5: Extract Schema Registration

### Step 5.1: Create SchemaService (if not exists)

**File to Check:** `cms_pricing/ingestion/services/schema_service.py`

- [ ] **Verify SchemaService exists**
  - If exists: Proceed to Step 5.2
  - If not: Create using RVU pattern as template

**Time:** 30 minutes (if creation needed)  
**Dependencies:** None

### Step 5.2: Add Schema Bootstrap Method

**File to Update:** `cms_pricing/ingestion/services/schema_service.py`

- [ ] **Create `bootstrap_{dataset}_schemas()` method**
  ```python
  def bootstrap_{dataset}_schemas(self, registry: Any) -> None:
      """Register all {dataset} schema contracts with the provided registry."""
      if self.dataset_name not in {"{dataset}", "{dataset_alias}"}:
          return
      
      if self._{dataset}_bootstrapped:
          return
      
      schemas = self._build_{dataset}_schema_contracts()
      # Register schemas
      self._{dataset}_bootstrapped = True
  ```

- [ ] **Move schema contract definitions**
  - Extract from `_register_schema_contracts()` in ingestor
  - Add to `SchemaService._build_{dataset}_schema_contracts()`

**Time:** 1-2 hours  
**Dependencies:** Step 5.1  
**Reference:** `cms_pricing/ingestion/services/schema_service.py` (see `bootstrap_rvu_schemas()`)

### Step 5.3: Update Ingestor to Use SchemaService

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Replace schema registration call**
  ```python
  # OLD:
  self._register_schema_contracts()
  
  # NEW:
  self.services.schema_service.bootstrap_{dataset}_schemas(self.services.schema_registry)
  ```

- [ ] **Remove `_register_schema_contracts()` method**
  - Should be ~300-400 lines removed

- [ ] **Add schema caching (optional optimization)**
  ```python
  # Pre-cache schema contracts for validation performance
  self._cached_schemas = {}
  for dataset_name, schema_name in dataset_to_schema.items():
      schema = self.services.schema_registry.get_contract(schema_name)
      if schema:
          self._cached_schemas[dataset_name] = schema
  ```

**Time:** 30 minutes  
**Dependencies:** Step 5.2

### Step 5.4: Test Schema Registration

- [ ] **Verify schemas are registered**
  - Check schema registry after initialization
  - Verify schema contracts are accessible

- [ ] **Test schema validation**
  - Verify validation still works with registered schemas
  - Test with invalid data

**Time:** 15 minutes  
**Dependencies:** Step 5.3

---

## Phase 6: Integrate Stage Helpers

### Step 6.1: Move Land Helpers to Stage Module

**File to Update:** `cms_pricing/ingestion/stages/land.py`

- [ ] **Verify helpers don't exist in stage module**
  - Check if `_land_with_provided_files()` equivalent exists
  - If not, move from ingestor to stage module

- [ ] **Update ingestor to delegate**
  ```python
  # In {ingestor}_ingestor.py:
  async def _land_stage(self, ...):
      return await stages.execute_land(...)
  ```

- [ ] **Remove `_land_with_provided_files()` from ingestor**
  - Should be ~100-200 lines removed

**Time:** 1 hour  
**Dependencies:** None  
**Reference:** Phase 2 Step 5

### Step 6.2: Move Validation Helpers to Stage Module

**File to Update:** `cms_pricing/ingestion/stages/normalize.py`

- [ ] **Move `_validate_parsed_dataframes()` to stage module**
  - Extract from ingestor
  - Replace stub in `stages/normalize.py`
  - Preserve return shape and cached schema usage

- [ ] **Update normalize flow to use stage helper**
  - Remove ingestor method
  - Use stage module version

**Time:** 1 hour  
**Dependencies:** None  
**Reference:** Phase 2 Step 5

### Step 6.3: Remove Unused Helpers

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Remove unused file type inference helpers**
  - `_infer_file_type_from_name()` (should be in `stages/land.py`)
  - `_is_guidance_file()` (should be in `stages/land.py`)

- [ ] **Remove unused discovery helpers**
  - Check for any discovery helpers that are no longer needed

**Time:** 30 minutes  
**Dependencies:** Step 6.1-6.2

---

## Phase 7: Final Cleanup

### Step 7.1: Remove Unused Imports

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Remove unused service imports**
  - `ValidationEngine` (use `self.services.validation_engine`)
  - `SchemaRegistry` (use `self.services.schema_registry`)
  - Other direct service instantiations

- [ ] **Move type-only imports to TYPE_CHECKING guard**
  ```python
  from typing import TYPE_CHECKING
  if TYPE_CHECKING:
      import pandas as pd
  ```

**Time:** 15 minutes

### Step 7.2: Remove Unused Instance Variables

**File to Update:** `cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py`

- [ ] **Remove unused initialization flags**
  - `_initialize_reference_data`
  - `_initialize_schema_drift_detection`
  - Other unused flags

**Time:** 15 minutes

### Step 7.3: Verify Line Count

- [ ] **Check final line count**
  ```bash
  wc -l cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py
  ```
  - Target: <1,000 lines

- [ ] **If still over 1,000 lines:**
  - Identify remaining large methods
  - Consider additional extraction
  - Check for duplicate code

**Time:** 15 minutes

### Step 7.4: Update Docstrings

- [ ] **Add Phase 2 refactoring breadcrumbs**
  - Add comments linking to plan documents
  - Update module docstrings with refactoring context

- [ ] **Update class docstrings**
  - Document thin orchestrator pattern
  - Reference extracted modules

**Time:** 30 minutes

### Step 7.5: Run Full Test Suite

- [ ] **Compilation check**
  ```bash
  python -m compileall cms_pricing/ingestion/ingestors/{ingestor}_ingestor.py
  ```

- [ ] **Run unit tests**
  ```bash
  pytest tests/ingestors/test_{ingestor}_*.py -v
  ```

- [ ] **Run integration tests**
  ```bash
  pytest tests/integration/test_{ingestor}_*.py -v
  ```

- [ ] **Run full pipeline test**
  ```bash
  pytest tests/ingestors/test_{ingestor}_e2e.py::test_full_dis_pipeline -v
  ```

**Time:** 30 minutes

---

## Post-Migration Verification

### Verification Checklist

- [ ] **Line count target met** (<1,000 lines)
- [ ] **All tests passing** (or 90%+ pass rate)
- [ ] **No performance regression** (or <10% regression acceptable)
- [ ] **Schema registration working** (SchemaService)
- [ ] **Database loading working** (loader functions)
- [ ] **Adapter parsing working** (adapter module)
- [ ] **Validation working** (ValidationService)
- [ ] **Stage modules working** (all stages delegated)

### Documentation Updates

- [ ] **Update ingestor PRD** with new architecture notes
- [ ] **Update cross-references** in related PRDs
- [ ] **Update "Last Reviewed" dates** in PRD metadata
- [ ] **Add migration completion note** to PRD

### Code Quality

- [ ] **No linter errors**
- [ ] **Type hints updated**
- [ ] **Docstrings complete**
- [ ] **Traceability comments added** (Phase 2 Step X references)

---

## Migration Timeline

| Phase | Steps | Estimated Time | Cumulative |
|-------|-------|----------------|------------|
| Assessment | Step 0 | 30 min | 30 min |
| DatasetSpecs | Step 1.1 | 1-2 hrs | 1.5-2.5 hrs |
| Loaders | Steps 2.1-2.5 | 2.5-3.5 hrs | 4-6 hrs |
| Adapter | Steps 3.1-3.4 | 3-4.5 hrs | 7-10.5 hrs |
| Business Rules | Steps 4.1-4.5 | 2.5-3 hrs | 9.5-13.5 hrs |
| Schema Service | Steps 5.1-5.4 | 2-3 hrs | 11.5-16.5 hrs |
| Stage Helpers | Steps 6.1-6.3 | 2.5 hrs | 14-19 hrs |
| Cleanup | Steps 7.1-7.5 | 1.5 hrs | 15.5-20.5 hrs |

**Total Estimated Time:** 2-3 days (16-24 hours)

---

## Common Pitfalls & Solutions

### Pitfall 1: Import Cycles
**Symptom:** `ImportError: cannot import name`  
**Solution:** Use ServiceFactory lazy initialization, import inside functions if needed

### Pitfall 2: Test Failures After Extraction
**Symptom:** Tests fail because they reference old method names  
**Solution:** Keep thin delegate methods for backward compatibility during migration

### Pitfall 3: Missing Dependencies
**Symptom:** Extracted functions can't access ingestor instance variables  
**Solution:** Pass dependencies as function parameters, not instance variables

### Pitfall 4: Schema Registration Double-Call
**Symptom:** Schemas registered twice, causing errors  
**Solution:** Use SchemaService idempotent registration (checks `_bootstrapped` flag)

### Pitfall 5: Loader Function Signature Mismatch
**Symptom:** Loader functions called with wrong parameters  
**Solution:** Follow exact signature: `(df, release_uuid, batch_id, db_session) -> int`

---

## Reference Documents

- **Implementation Guide:** `prds/STD-data-architecture-impl-v1.0.md` §1.7
- **Migration Template:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (990 lines)
- **Pattern Catalog:** `prds/DOC-master-catalog-prd-v1.0.md` §10
- **Loader Pattern:** `prds/STD-database-platform-prd-v1.0.md` §6.1
- **Validation Pattern:** `prds/STD-qa-testing-prd-v1.0.md` §7.5

---

## Success Metrics

- ✅ Ingestor <1,000 lines
- ✅ All dataset logic in DatasetSpec or dedicated modules
- ✅ Schema registration centralized in SchemaService
- ✅ Database loaders in DatasetSpec.loader functions
- ✅ Adapter logic extracted to reusable module
- ✅ Business rules auto-registered from DatasetSpecs
- ✅ All tests passing (90%+ pass rate)
- ✅ No performance regression

---

**Migration Checklist Version:** 1.0  
**Based on:** Phase 2 RVU Refactoring (2025-11-04)  
**Template Status:** Ready for use

