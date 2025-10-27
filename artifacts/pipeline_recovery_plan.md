# Pipeline Recovery Implementation Plan

**Goal:** Complete the scraper → API data pipeline with real parsing, enrichment, and database loading

**Status:** In Progress  
**Last Updated:** 2025-10-27  
**Related Docs:** `prds/STD-data-architecture-prd-v1.0.md`, `prds/REF-scraper-ingestor-integration-v1.0.md`

---

## Executive Summary

The current pipeline has real parsers integrated (PPRRVU, GPCI, OPPSCap, Anes, LocalityCounty) but test failures indicate:
- Directory structure mismatches
- Method signature incompatibilities  
- Missing database publisher integration
- Incomplete ZIP9 ingester

This plan addresses all four areas to complete the pipeline.

---

## Phase 1: Fix RVU Ingestor Test Failures (Priority: HIGH)

**Goal:** Make RVU ingestor tests pass with real parser integration already in place

### Task 1.1: Fix Directory Structure
**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`  
**Location:** Line 1976 in `_land_with_provided_files`

**Issue:** Creates `raw/cms_rvu/<release_id>/files/` but test expects `raw/cms_rvu/<release_id>/files/files/`

**Fix:**
```python
# Current (line 1976):
raw_dir = Path(self.output_dir) / "raw" / "cms_rvu" / release_id / "files"

# Should be:
raw_dir = Path(self.output_dir) / "raw" / "cms_rvu" / release_id / "files"
# Or check test expectation - may need to adjust test instead
```

**Action:** Verify which path structure is correct per DIS standard

---

### Task 1.2: Update Tests to Pass raw_content
**File:** `tests/ingestors/test_rvu_ingestor_e2e.py`  
**Location:** Lines 188-197

**Issue:** Tests create `RawBatch` without `raw_content` field

**Fix:**
```python
# Current:
raw_batch = RawBatch(
    source_files=sample_source_files,
    raw_data_path=land_result["raw_directory"],
    metadata={...}
)

# Should be:
raw_batch = RawBatch(
    source_files=sample_source_files,
    raw_content=land_result.get("raw_content", {}),  # ADD THIS
    raw_data_path=land_result["raw_directory"],
    metadata={...}
)
```

---

### Task 1.3: Fix Method Signature Mismatches
**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`  
**Locations:** Lines 263-273

**Issue:** Tests call `_normalize_stage(raw_batch, validate_result)` but method signature is `_normalize_stage(validated_batch: Dict[str, Any])`

**Analysis:**
- Lines 263-265 show legacy helpers that call newer methods
- Tests expect old signature but ingestor uses new signature
- Need to either update tests OR add wrapper methods

**Fix Options:**
1. Update tests to use new signatures
2. Keep legacy wrappers and update implementations
3. Make methods more flexible with **kwargs

**Recommended:** Update tests to match new signatures

---

### Task 1.4: Fix SchemaRegistry API
**File:** `cms_pricing/ingestion/contracts/schema_registry.py`  
**Location:** Line 2085 (caller)

**Issue:** Code calls `self.schema_registry.get_contract("cms_rvu_v1")` but method doesn't exist

**Fix:** Either add `get_contract()` method or update callers to use existing method:
```python
# Option 1: Add method to SchemaRegistry
def get_contract(self, schema_id: str) -> Optional[SchemaContract]:
    return self.get_schema(schema_id)

# Option 2: Update callers
schema_contract = self.schema_registry.get_schema("cms_rvu_v1")  # Use existing
```

**Recommended:** Add `get_contract()` as alias for `get_schema()`

---

## Phase 2: Complete ZIP9/Locality Ingestion (Priority: MEDIUM)

**Goal:** Complete the ZIP9 ingester with real adapters and enrichment

### Task 2.1: Implement _adapt_raw_data_sync
**File:** `cms_pricing/ingestion/ingestors/cms_zip9_ingester.py`  
**Location:** Lines 685-693

**Current State:** Returns empty DataFrame
```python
def _adapt_raw_data_sync(self, raw_batch: RawBatch) -> AdaptedBatch:
    return AdaptedBatch(
        data=pd.DataFrame(),  # EMPTY!
        metadata=raw_batch.metadata,
        schema_version="1.0"
    )
```

**Implementation Plan:**
1. Extract ZIP file from `raw_batch.raw_content`
2. Parse ZIP9 override data (CSV format)
3. Validate against schema `cms_zip9_overrides_v1.1`
4. Return AdaptedBatch with parsed DataFrame

**Schema Reference:** `cms_pricing/ingestion/contracts/cms_zip9_overrides_v1.1.json`

---

### Task 2.2: Implement _enrich_data_sync  
**File:** `cms_pricing/ingestion/ingestors/cms_zip9_ingester.py`  
**Location:** Lines 695-699

**Current State:** Returns stage frame as-is
```python
def _enrich_data_sync(self, stage_frame: StageFrame, ref_data: RefData) -> StageFrame:
    return stage_frame  # NO ENRICHMENT
```

**Implementation Plan:**
1. Load ZIP5 → locality crosswalk reference data
2. Join ZIP9 overrides with locality mappings
3. Add geography enrichment (state, county, locality names)
4. Calculate derived fields (effective date ranges, etc.)
5. Return enriched StageFrame

---

### Task 2.3: Add ZIP9 Parser
**Create:** `cms_pricing/ingestion/parsers/zip9_parser.py`

**Purpose:** Dedicated parser for ZIP9 override format

**Functionality:**
- Parse CSV with columns: zip9_start, zip9_end, carrier, locality, state
- Validate ZIP9 format (9 digits)
- Normalize column names
- Generate ParseResult

---

## Phase 3: Schema-Governed Publishing to Postgres (Priority: HIGH)

**Goal:** Automate loading Parquet files into Postgres tables

### Task 3.1: Create ParquetToDBLoader Utility
**Create:** `cms_pricing/ingestion/loaders/parquet_to_db.py`

**Purpose:** Generic utility to load Parquet → Postgres

**Interface:**
```python
class ParquetToDBLoader:
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        model: Base,
        upsert_strategy: str = "replace"
    ) -> LoadResult:
        # Use SQLAlchemy bulk operations or PostgreSQL COPY
        pass
```

**Options:**
- Use SQLAlchemy ORM bulk_insert_mappings (slower, existing pattern)
- Use PostgreSQL COPY FROM with psycopg2 (faster)
- Hybrid: COPY for initial load, upserts for updates

**Recommendation:** Start with SQLAlchemy for compatibility, add COPY optimization later

---

### Task 3.2: Wire Publish Stage to Database
**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`  
**Location:** `publish()` method (lines 2234-2339)

**Current State:** Only saves Parquet files, doesn't load to database

**Implementation Plan:**
1. After saving Parquet files
2. Load each DataFrame to corresponding table:
   - `pprrvu` → `RVUItem` model
   - `gpci` → `GPCIIndex` model  
   - `oppscap` → `OPPSCap` model
   - `anescf` → `AnesCF` model
   - `localitycounty` → `LocalityCounty` model
3. Handle upserts (replace or merge based on natural keys)
4. Log load statistics

**Database Models:** `cms_pricing/models/rvu.py` (lines 10-150)

---

### Task 3.3: Add Loader to DIS Pipeline
**File:** `cms_pricing/ingestion/run/dis_pipeline.py`  
**Location:** `_publish_data()` method

**Implementation:**
1. After ParquetPublisher writes files
2. Call ParquetToDBLoader for each dataset
3. Track load success/failure
4. Return combined results

---

## Phase 4: Add Integration Tests (Priority: MEDIUM)

**Goal:** Add end-to-end tests proving real data flows

### Task 4.1: Create E2E Test Suite
**Create:** `tests/integration/test_rvu_pipeline_e2e.py`

**Test Cases:**
1. **Full Pipeline Test**
   - Start with real CMS ZIP file fixture
   - Run entire ingest() pipeline
   - Verify parquet files contain real data (non-synthetic)
   - Verify database tables populated with correct row counts

2. **Parser Integration Test**
   - Verify ParseResult objects have correct structure
   - Check metrics (rows_parsed, rows_valid, rows_quarantined)
   - Validate no mock/synthetic data

3. **Database Load Test**
   - Verify data loaded to correct tables
   - Check natural key uniqueness
   - Validate referential integrity

**Fixtures:** Use existing `tests/fixtures/rvu/` with real CMS sample data

---

### Task 4.2: Add Row Count Assertions
**Location:** Various test files

**Add assertions:**
```python
# After ingestion completes
assert result["record_count"] > 1000, "Should have real data"
assert result["record_count"] < expected_max, "Check for duplicates"

# After database load
db_count = db.query(RVUItem).filter_by(release_id=release_id).count()
assert db_count == result["record_count"], "All records should be in DB"
```

---

## Phase 5: Documentation and Operations (Priority: LOW)

**Goal:** Update docs and define ownership

### Task 5.1: Update INGESTION_GUIDE.md
**File:** `INGESTION_GUIDE.md`

**Changes:**
- Remove manual "load parquet into DB" step
- Add automated pipeline description
- Update CLI commands to reflect automated flow

---

### Task 5.2: Update PRDs
**Files:** `prds/RUN-global-operations-prd-v1.0.md`, `prds/STD-data-architecture-prd-v1.0.md`

**Changes:**
- Document automated publish → database loading
- Define ingestion failure ownership
- Add monitoring/alerting requirements

---

### Task 5.3: Add Failure Handlers
**File:** `cms_pricing/ingestion/run/dis_pipeline.py`

**Add:**
- Retry logic for transient failures
- Quarantine handling for invalid data
- Alerting on SLO violations
- Recovery procedures

---

## Implementation Order

### Sprint 1 (Critical Path)
1. Fix test failures in `rvu_ingestor.py` (Tasks 1.1-1.4)
2. Verify RVU tests pass
3. Create ParquetToDBLoader (Task 3.1)
4. Wire publish → database (Task 3.2)

**Goal:** RVU ingestion works end-to-end with real data in database

### Sprint 2 (ZIP9 Support)
5. Complete ZIP9 adapter (Task 2.1)
6. Add ZIP9 parser (Task 2.3)
7. Complete ZIP9 enrichment (Task 2.2)
8. Test ZIP9 ingestion

**Goal:** ZIP9 ingestion works with real data

### Sprint 3 (Validation & Docs)
9. Add E2E tests (Task 4.1-4.2)
10. Update documentation (Task 5.1-5.3)
11. Add monitoring/alerting

**Goal:** Production-ready with full observability

---

## Success Criteria

**Phase 1 Complete When:**
- ✅ All RVU ingestor tests pass
- ✅ Real parsers are confirmed running (no mocks)
- ✅ Parquet files contain real CMS data

**Phase 2 Complete When:**
- ✅ ZIP9 data flows through complete DIS pipeline
- ✅ Enrichment joins reference tables correctly
- ✅ Data validated and quarantined properly

**Phase 3 Complete When:**
- ✅ Parquet files automatically load to Postgres
- ✅ Database tables contain real data
- ✅ Row counts match parsed data counts

**Phase 4 Complete When:**
- ✅ E2E tests validate scraper → API flow
- ✅ No synthetic/mock data in final tables
- ✅ Tests run in CI pipeline

**Phase 5 Complete When:**
- ✅ Documentation reflects automated flow
- ✅ Failure ownership clearly defined
- ✅ Monitoring dashboards show pipeline health

---

## Risk Mitigation

**Risk 1: Test/Implementation Mismatch**
- **Mitigation:** Update tests to match actual signatures, not vice versa
- **Rationale:** Implementation is correct, tests need updating

**Risk 2: Database Load Performance**
- **Mitigation:** Start with SQLAlchemy bulk inserts, profile and optimize
- **Fallback:** Use PostgreSQL COPY for large datasets

**Risk 3: Parser Failures on Real Data**
- **Mitigation:** Quarantine invalid rows, track metrics, alert on failures
- **Testing:** Use real CMS fixtures in tests from day 1

---

## Estimation

**Phase 1:** 4-6 hours (test fixes, basic DB loading)  
**Phase 2:** 6-8 hours (ZIP9 parser + enrichment)  
**Phase 3:** 2-4 hours (integrate with pipeline)  
**Phase 4:** 4-6 hours (E2E tests + validation)  
**Phase 5:** 2-3 hours (docs + monitoring)

**Total:** 18-27 hours (2-3 days of focused work)

---

## Next Steps

1. User confirms approach (fix tests vs. fix implementation)
2. Start with Phase 1, Task 1.1 (directory structure fix)
3. Verify with pytest to see one test pass
4. Continue iteratively through remaining tasks

