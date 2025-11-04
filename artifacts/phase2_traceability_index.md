# Phase 2 Refactoring: Traceability Index

**Purpose:** Central index for AI and human developers to trace plan → implementation → verification

**Last Updated:** 2025-11-03

---

## Quick Reference

| Step | Status | Plan Doc | Implementation | Verification | Lines Removed |
|------|--------|----------|-----------------|--------------|---------------|
| Step 1: Schema Registration | ✅ Complete | `phase2_completion_plan.md` §Step 1 | `services/schema_service.py` | Schema bootstrap tests | 368 lines |
| Step 2: Database Loaders | ✅ Complete | `phase2_completion_plan.md` §Step 2 | `datasets/rvu_loaders.py` | Loader tests | 400+ lines |
| Step 3: Adapter Logic | ✅ Complete | `phase2_step3_detailed_plan.md` | `datasets/rvu_adapter.py` | `phase2_regression_test_results.md` | 429 lines |
| Step 4: Validation Rules | ✅ Complete | `phase2_step4_detailed_plan.md` | `datasets/rvu_spec.py` + `services/validation_service.py` | `phase2_step4_verification_report.md` | 98 lines |
| Step 5: Stage Integration | ✅ Complete | `phase2_step5_detailed_plan.md` | `stages/normalize.py`, `stages/land.py` | `phase2_step5_verification_report.md` | ~290 lines |
| Step 6: Cleanup | ✅ Complete | `phase2_completion_plan.md` §Step 6 | Code breadcrumbs + docs (`docs/ingestion_verification.md`, module annotations) | `tests/ingestors/test_rvu_loader_aliases.py` | — |
| Step 7: Final Verification | ⏳ Pending | `phase2_completion_plan.md` §Step 7 | TBD | TBD | TBD |

---

## Document Map

### Planning Documents
- **`phase2_completion_plan.md`** - Master plan with all 7 steps, PRD update notes, migration path
- **`phase2_step3_detailed_plan.md`** - Detailed plan for adapter extraction (Step 3)
- **`phase2_step4_detailed_plan.md`** - Detailed plan for validation rules extraction (Step 4)
- **`phase2_step5_detailed_plan.md`** - Detailed plan for stage integration (Step 5)

### Verification Documents
- **`phase2_regression_test_results.md`** - Regression test results after Steps 1-3
- **`phase2_step4_verification_report.md`** - Step 4 verification and code quality assessment
- **`docs/ingestion_verification.md`** - Step 6 ops checklist (staged parquet + SQL validation)

### Implementation Files

#### Step 1: Schema Registration
- **Created:** `cms_pricing/ingestion/services/schema_service.py`
  - Method: `SchemaService.bootstrap_rvu_schemas(registry)`
  - Extracted from: `RVUIngestor._register_schema_contracts()` (368 lines)
- **Modified:** 
  - `services/service_factory.py` - Added `schema_service` property
  - `services/__init__.py` - Exported `SchemaService`
  - `ingestors/rvu_ingestor.py` - Removed `_register_schema_contracts()`, calls `schema_service.bootstrap_rvu_schemas()`

#### Step 2: Database Loaders
- **Created:** `cms_pricing/ingestion/datasets/rvu_loaders.py`
  - Functions: `load_rvu_dataframes()`, `load_pprrvu_data()`, `load_gpci_data()`, etc.
  - Extracted from: `RVUIngestor._load_dataframes_to_database()` + individual `_load_*_data()` methods (400+ lines)
- **Modified:**
  - `datasets/rvu_spec.py` - DatasetSpec.loader wired to loader functions
  - `stages/publish.py` - Uses `load_rvu_dataframes()` as default loader
  - `ingestors/rvu_ingestor.py` - Removed loader methods, calls `load_rvu_dataframes()`

#### Step 3: Adapter Logic
- **Created:** `cms_pricing/ingestion/datasets/rvu_adapter.py`
  - Function: `adapt_rvu_raw_data()`
  - Extracted from: `RVUIngestor._adapt_raw_data_sync()` (429 lines → 510 lines in module, 13-line delegate remains)
- **Modified:**
  - `stages/normalize.py` - Defaults to `adapt_rvu_raw_data()` when no adapter_func provided
  - `ingestors/rvu_ingestor.py` - `_adapt_raw_data_sync()` now 13-line delegate

#### Step 4: Validation Rules
- **Modified:** `cms_pricing/ingestion/datasets/spec.py`
  - Added field: `business_rules: List[Callable[[DataFrame], ValidationResult]]`
- **Created:** `cms_pricing/ingestion/services/validation_service.py`
  - Class: `ValidationService`
  - Method: `register_dataset_business_rules(dataset_spec)`
- **Modified:**
  - `datasets/rvu_spec.py` - Added `_create_pprrvu_business_rules()`, `_create_gpci_business_rules()`, wired to DatasetSpecs
  - `services/service_factory.py` - Added `validation_service` property
  - `services/__init__.py` - Exported `ValidationService`
  - `ingestors/rvu_ingestor.py` - Removed `_register_validation_rules()` (~98 lines), auto-registers in `__init__`

#### Step 5: Stage Integration (Complete)
- **Modified:**
  - `stages/normalize.py` - `_validate_parsed_dataframes()` implemented (replaced stub), `execute_normalize()` enhanced with schema cache parameters
  - `ingestors/rvu_ingestor.py` - Removed `_land_with_provided_files()` (~164 lines), removed `_validate_parsed_dataframes()` (~127 lines), `_land_stage()` now always calls `stages.execute_land`
  - Removed unused helpers: `_infer_file_type_from_name()`, `_is_guidance_file()`
- **Verification:** `phase2_step5_verification_report.md`

#### Step 6: Cleanup & Traceability (Complete)
- **Documentation:**
  - `docs/ingestion_verification.md` - Single-source verification + release reset instructions
  - `prds/SRC-gpci.md`, `prds/SRC-oppscap.md`, `prds/SRC-locality.md`, `prds/SRC-cms-rvu.md` – Parser → loader alias tables
  - `prds/RUN-render-deployment-prd-v1.0.md` – Release reset snippet
- **Code Annotations:**
  - Added phase breadcrumbs to `schema_service.py`, `rvu_loaders.py`, `rvu_adapter.py`, `validation_service.py`, `stages/normalize.py`, `stages/land.py`
- **Tests:** `tests/ingestors/test_rvu_loader_aliases.py` enforces loader alias behaviour

---

## Code-to-Plan Traceability

### Code Breadcrumbs (Added 2025-11-03)

| Module / Test | Phase Step | Notes |
|---------------|------------|-------|
| `services/schema_service.py` | Step 1 | Module docstring + inline comment linking to plan (§Step 1) |
| `datasets/rvu_loaders.py` | Step 2 | Module docstring + loader entry comment |
| `datasets/rvu_adapter.py` | Step 3 | Module docstring + `adapt_rvu_raw_data` breadcrumb |
| `services/validation_service.py` | Step 4 | Module docstring + `register_dataset_business_rules` breadcrumb |
| `stages/land.py`, `stages/normalize.py` | Step 5 | Module docstrings + entrypoint comments |
| `docs/ingestion_verification.md` | Step 6 | Operations checklist (parquet + SQL validation) |
| `tests/ingestors/test_rvu_loader_aliases.py` | Step 6 | Regression guard ensuring alias columns reach DB loaders |

### Recommended Code Comment Pattern

For AI traceability, add comments like this in code:

```python
# Phase 2 Step 3: Adapter extraction
# See: artifacts/phase2_step3_detailed_plan.md
# Extracted from: RVUIngestor._adapt_raw_data_sync() (429 lines)
# Date: 2025-11-03
def adapt_rvu_raw_data(...):
    """Parse raw RVU ZIP files using DatasetSpec routing."""
    ...

# Phase 2 Step 4: Business rules extraction
# See: artifacts/phase2_step4_detailed_plan.md
# Extracted from: RVUIngestor._register_validation_rules() (98 lines)
# Date: 2025-11-03
def _create_pprrvu_business_rules() -> List[Callable[[pd.DataFrame], ValidationResult]]:
    """Create business rule validators for PPRRVU dataset."""
    ...
```

### Git Commit Message Pattern

For traceability in git history:

```
Phase 2 Step X: [Brief description]

- Extracted [method/function] from [source] to [target]
- See: artifacts/phase2_stepX_detailed_plan.md
- Lines removed: ~N
- Files: [list of files]
```

Example:
```
Phase 2 Step 4: Extract business rules to DatasetSpec

- Added business_rules field to DatasetSpec
- Created ValidationService for consistent registration
- Extracted business rule functions to rvu_spec.py
- Auto-registration in RVUIngestor.__init__
- See: artifacts/phase2_step4_detailed_plan.md
- Lines removed: ~98
- Files: datasets/spec.py, datasets/rvu_spec.py, services/validation_service.py, ingestors/rvu_ingestor.py
```

---

## Metrics Tracker

### Line Count Evolution
- **Initial:** 4,247 lines (RVUIngestor)
- **After Step 1:** ~4,247 - 368 = ~3,879 lines
- **After Step 2:** ~3,879 - 400 = ~3,479 lines
- **After Step 3:** ~3,479 - 429 = ~3,050 lines (but adapter extracted, delegate added)
- **After Step 4:** ~3,050 - 98 = ~2,952 lines (but business rules added to specs)
- **After Step 5:** ~1,079 lines ✅ (~1,365 total reduction, 75% reduction from original)
- **Target:** <1,000 lines (only 79 lines away!)

### Code Extracted
- **Total extracted:** ~1,585+ lines
- **Reusable modules created:**
  - `schema_service.py`: 469 lines (reusable)
  - `rvu_loaders.py`: 604 lines (reusable)
  - `rvu_adapter.py`: 510 lines (reusable)
  - `validation_service.py`: 60 lines (reusable)
  - Business rules in `rvu_spec.py`: ~140 lines (reusable)

---

## PRD Traceability

### PRDs to Update (from phase2_completion_plan.md §PRD Update Notes)

1. **STD-data-architecture-impl-v1.0.md**
   - Step 1: SchemaService pattern
   - Step 2: DatasetSpec.loader pattern
   - Step 3: Adapter extraction pattern
   - Step 4: ValidationService pattern, business_rules pattern

2. **STD-parser-contracts-prd-v2.0.md**
   - Step 1: SchemaService usage
   - Step 3: DatasetSpec routing

3. **STD-database-platform-prd-v1.0.md**
   - Step 2: DatasetSpec.loader pattern

4. **Validation Rules Documentation** (new)
   - Step 4: business_rules vs validation_rules distinction

5. **DOC-master-catalog-prd-v1.0.md**
   - All steps: Add new patterns to architectural standards table

---

## AI-Friendly Metadata

### Searchable Keywords
- `Phase 2 refactoring`
- `RVU ingestor modularization`
- `DatasetSpec pattern`
- `ServiceFactory pattern`
- `SchemaService`
- `ValidationService`
- `business_rules`
- `rvu_adapter`
- `rvu_loaders`

### File Naming Convention
- Plans: `phase2_stepX_detailed_plan.md`
- Verification: `phase2_stepX_verification_report.md`
- Master plan: `phase2_completion_plan.md`
- Index: `phase2_traceability_index.md` (this file)

### Status Tracking
- ✅ Complete
- ⏳ Pending
- 🔄 In Progress
- ❌ Blocked
- ⚠️ Needs Review

---

## Recommendations for Better Traceability

### 1. Add Code Comments
```python
# Phase 2 Step X: [Description]
# Plan: artifacts/phase2_stepX_detailed_plan.md
# Extracted: YYYY-MM-DD
```

### 2. Standardize Commit Messages
Always include: `Phase 2 Step X: [summary]` + reference to plan doc

### 3. Update This Index
After each step completion:
- Mark status as ✅
- Update line counts
- Add verification doc link
- Update metrics

### 4. Cross-Reference in Code
In docstrings, reference plan documents:
```python
def adapt_rvu_raw_data(...):
    """
    Parse raw RVU ZIP files using DatasetSpec routing.
    
    Extracted from RVUIngestor._adapt_raw_data_sync() as part of
    Phase 2 Step 3 refactoring. See artifacts/phase2_step3_detailed_plan.md
    for full implementation details.
    """
```

### 5. Create Verification Checklist Template
For each step, create a checklist that references:
- Plan document
- Implementation files
- Test files
- Verification report

---

## Next Steps for Traceability

1. **Add code comments** to all extracted modules (Steps 1-4)
2. **Update git commit messages** to reference plan documents
3. **Create PRD update tracker** linking plan steps to PRD sections
4. **Add traceability to tests** - test docstrings should reference plan steps
5. **Create migration guide** showing before/after for each step

---

## Quick Links

- [Master Plan](./phase2_completion_plan.md)
- [Step 3 Detailed Plan](./phase2_step3_detailed_plan.md)
- [Step 4 Detailed Plan](./phase2_step4_detailed_plan.md)
- [Step 5 Detailed Plan](./phase2_step5_detailed_plan.md)
- [Regression Test Results](./phase2_regression_test_results.md)
- [Step 4 Verification Report](./phase2_step4_verification_report.md)
