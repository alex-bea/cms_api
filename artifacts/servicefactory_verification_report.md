# ServiceFactory Integration Verification Report

**Date:** 2025-11-06  
**Purpose:** Verify ServiceFactory migration status per Phase 3 Step 4

## Verification Method

Used ripgrep (`rg`) to search for:
- `ServiceFactory` / `ServiceConfig` imports
- `self.services.` usage patterns
- Manual instantiation patterns (`ValidationEngine(`, `QuarantineManager(`, `DISObservabilityCollector(`)
- Compile check: `python -m compileall cms_pricing/ingestion/ingestors/`

## Results

### RVU Ingestor (`rvu_ingestor.py`)
**Status:** ✅ **MIGRATED**

- ✅ Imports `ServiceFactory` and `ServiceConfig`
- ✅ Creates `ServiceFactory` in `__init__` (line 167)
- ✅ Uses `self.services.*` pattern extensively (21 instances)
- ✅ No manual instantiation found

**Services accessed via factory:**
- `self.services.schema_registry`
- `self.services.schema_service`
- `self.services.validation_service`
- `self.services.observability_collector`
- `self.services.reference_enricher`
- `self.services.reference_data_manager`

### MPFS Ingestor (`mpfs_ingestor.py`)
**Status:** ❌ **NOT MIGRATED**

- ❌ No `ServiceFactory` or `ServiceConfig` imports found
- ❌ Manual instantiation found:
  - `self.validation_engine = ValidationEngine()` (line 109)
  - `self.quarantine_manager = QuarantineManager(...)` (line 110)
  - `self.observability_collector = DISObservabilityCollector()` (line 111)
  - `self.reference_data_manager = ReferenceDataManager()` (line 112)
- ❌ No `self.services.` usage found

**Note:** Phase 3 plan marks this as complete, but code inspection shows migration incomplete.

### OPPS Ingestor (`opps_ingestor.py`)
**Status:** ❌ **NOT MIGRATED**

- ❌ No `ServiceFactory` or `ServiceConfig` imports found
- ❌ Manual instantiation found:
  - `self.validation_engine = ValidationEngine()` (line 84)
  - `self.quarantine_manager = QuarantineManager()` (line 87)
  - `self.observability = DISObservabilityCollector()` (line 88)
- ❌ No `self.services.` usage found

**Note:** Phase 3 plan marks this as complete, but code inspection shows migration incomplete.

### ZIP9 Ingester (`cms_zip9_ingester.py`)
**Status:** ❌ **NOT MIGRATED**

- ❌ No `ServiceFactory` or `ServiceConfig` imports found
- ❌ Uses custom validators (`ZIP9OverridesValidator`, `IngestionRunsManager`)
- ❌ No `self.services.` usage found

**Note:** ZIP9 may have dataset-specific requirements (e.g., disable reference data per Phase 3 plan).

## Compile Check

**Command:** `python -m compileall cms_pricing/ingestion/ingestors/ -q`

**Result:** ✅ **PASSED** (no errors)

All ingestor modules compile successfully.

## Summary

**Migration Status:**
- ✅ RVU: Complete (reference implementation)
- ❌ MPFS: Incomplete (marked complete in plan but not in code)
- ❌ OPPS: Incomplete (marked complete in plan but not in code)
- ❌ ZIP9: Incomplete (marked complete in plan but not in code)

**Recommendation:**
- Proceed with test coverage for ServiceFactory (RVU is known-good reference)
- Add smoke-style tests for MPFS/OPPS/ZIP9 if/when they migrate
- Document discrepancy between plan status and actual code state

## Schema Bootstrap Dataset-Awareness (Task 1 - Completed)

**Improvement:** Schema bootstrap is now dataset-aware via keyed bootstrap map in `SchemaService`.

**Changes:**
- Added `_dataset_bootstrap_map: Dict[str, Callable]` to `SchemaService`
- Added `get_bootstrapper(dataset_name: str)` class method
- Registered RVU bootstrap: `_dataset_bootstrap_map["cms_rvu"] = _bootstrap_rvu_wrapper`
- Updated `ServiceFactory.initialize_all()` to use `schema_service.get_bootstrapper(dataset_name)` instead of hard-coded `bootstrap_rvu_schemas()`
- Non-RVU datasets no longer trigger RVU bootstrap (fixes correctness bug)

**Benefits:**
- MPFS/OPPS/ZIP9 can register their own bootstrappers when they migrate
- No hard-coded dataset-specific calls in ServiceFactory
- Single point of registration (SchemaService)

## Test Strategy

**Smoke Tests with xfail Markers:**
- MPFS/OPPS/ZIP9 smoke tests use `@pytest.mark.xfail(strict=False)` to document expected behavior
- Tests show as "XFAIL" (expected failure) until migrations complete
- Clear TODO markers and verification report links for when migrations start
- Easy to flip to strict mode once migrations begin

**Test Coverage:**
- 4 xfail tests across MPFS/OPPS/ZIP9 (ServiceFactory presence, manual instantiation absence)
- ZIP9 includes additional test for `enable_reference_data=False` flag
- Tests will pass once migrations complete (remove xfail markers)

## Next Steps

1. ✅ Schema bootstrap dataset-awareness (Task 1) - **COMPLETED**
2. ✅ Reconcile documentation with actual migration status (Task 2) - **COMPLETED**
3. ✅ Strengthen ingestor smoke tests with xfail markers (Task 3) - **COMPLETED**
4. Clean up hygiene issues (Task 4)

