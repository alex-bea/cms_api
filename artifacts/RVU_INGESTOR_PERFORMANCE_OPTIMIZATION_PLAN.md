# RVU Ingestor Performance Optimization Plan

**Date:** 2025-01-15  
**Priority:** High  
**Estimated Impact:** Reduce ingestion time from minutes to seconds on large datasets  
**Estimated Effort:** 1-2 days  

## Problem Statement

The current RVU ingestion pipeline uses row-by-row processing with `iterrows()`, triggering:
1. **Thousands of database round-trips** (one `DELETE` + `INSERT` per row)
2. **Repeated Python overhead** (UUID generation, datetime parsing, string slicing per row)
3. **Exponential slowdown** as dataset size grows (O(n) database calls for n rows)

**Current Pattern:**
```python
for idx, row in df.iterrows():
    # Parse dates
    effective_start = pd.to_datetime(row.get('effective_start')).date()
    # Generate UUID
    record = {"id": uuid.uuid4(), ...}
    # Delete existing
    self._delete_existing_record(model, keys, record)
    self.db_session.add(ORMObject(**record))
```

**Estimated Current Performance:**
- 10K rows: ~30-60 seconds
- 100K rows: ~5-10 minutes
- 500K rows: ~30-60 minutes

## Solution Approach

### 1. Vectorized DataFrame Preprocessing
Convert DataFrame columns **once** before the loop:
```python
df = df.copy()  # Avoid modifying input
df['effective_start_parsed'] = pd.to_datetime(df['effective_start']).dt.date
df['effective_end_parsed'] = pd.to_datetime(df['effective_end']).dt.date
df['locality_id_clean'] = df['locality_code'].str.strip().str[:10]
# Generate UUIDs in batch via numpy
df['id'] = [uuid.uuid4() for _ in range(len(df))]
```

### 2. Single Bulk INSERT
Use SQLAlchemy's `bulk_insert_mappings()` to insert all rows in one transaction, optionally chunked (e.g. 5k rows) to keep memory usage predictable:
```python
records = df.to_dict('records')  # Fast conversion without a Python loop
for chunk in batched(records, size=5000):
    self.db_session.bulk_insert_mappings(model, chunk)
```

### 3. Batch DELETE (Optional)
Since we always create fresh `release_id`, delete all existing records for that release **once** upfront:
```python
self.db_session.execute(delete(model).where(model.release_id == release_uuid))
```

**Note:** If deduplication is already handled in `processed_dataframes`, we can skip the delete entirely.

### 4. UUID Generation
Stick with `uuid.uuid4()` in a list comprehension so the RFC4122 variant/version bits remain correct while still being fast after vectorization:
```python
df['id'] = [uuid.uuid4() for _ in range(len(df))]
```

## Implementation Plan

### Phase 1: Vectorized Preprocessing (Easy Wins)

**Tasks:**
1. ✅ Extract date parsing to DataFrame-level operations
2. ✅ Extract string normalization to vectorized `.str` operations
3. ✅ Pre-generate UUIDs in batch
4. ✅ Remove per-row type casts

**Files:**
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py`:
  - `_load_pprrvu_data()` (~100 LOC)
  - `_load_gpci_data()` (~60 LOC)
  - `_load_oppscap_data()` (~60 LOC)
  - `_load_anes_data()` (~50 LOC)
  - `_load_locality_data()` (~50 LOC)

**Expected Impact:** 5-10x speedup

### Phase 2: Bulk Database Operations

**Tasks:**
1. ✅ Replace `iterrows()` + `db_session.add()` with `bulk_insert_mappings()`
2. ✅ Replace per-row deletes with `DELETE WHERE release_id = :release_uuid`
3. ✅ Ensure delete + insert happen inside a single transaction scope
4. ✅ Maintain row-level error tracking (count failures)
5. ✅ Chunk bulk insert payloads to avoid memory spikes on very large datasets

**Expected Impact:** Additional 10-50x speedup (depending on network latency)

### Phase 3: Script Optimizations

**Tasks:**
1. ✅ Add release-exists short-circuit to `scripts/load_rvu_to_production.py`
2. ✅ Replace `db_session.query(Release).count()` with `exists()/limit(1)` O(1) check
3. ✅ Add CLI flags: `--release-id`, `--output-dir`, `--skip-download`
4. ✅ Trim verbose logging (log status/counts, not full JSON)

**Expected Impact:** Faster development/testing iterations

### Phase 4: Validation & Testing

**Tasks:**
1. ✅ Run E2E tests to verify data integrity
2. ✅ Performance benchmarking (before/after metrics)
3. ✅ Verify natural key uniqueness still enforced
4. ✅ Verify effective date filtering still works
5. ✅ Update PRD documentation

**Metrics to Track:**
- Ingestion time (seconds)
- Database query count
- Memory usage (peak)
- Row count processed
- Error rate (%)

## Detailed Changes

### Change 1: `_load_pprrvu_data` - Bulk Insert Pattern

**Before:**
```python
def _load_pprrvu_data(self, df: pd.DataFrame, release_uuid: Any, batch_id: str) -> int:
    if df is None or df.empty:
        return 0
    records_inserted = 0
    for idx, row in df.iterrows():
        try:
            effective_start = pd.to_datetime(row.get('effective_start')).date()
            record = {"id": uuid.uuid4(), ...}
            self._delete_existing_record(RVUItem, keys, record)
            self.db_session.add(RVUItem(**record))
            records_inserted += 1
        except Exception as e:
            logger.warning(...)
            continue
    return records_inserted
```

**After:**
```python
def _load_pprrvu_data(self, df: pd.DataFrame, release_uuid: Any, batch_id: str) -> int:
    if df is None or df.empty:
        return 0

    df = df.copy().reset_index(drop=True)
    df['id'] = [uuid.uuid4() for _ in range(len(df))]
    df['hcpcs_code'] = self._string_column(df, 'hcpcs_code', max_len=5)
    df['modifier_key'] = self._string_column(df, 'modifier', max_len=10)
    df['modifiers'] = df['modifier_key'].map(lambda v: [v] if v else None)
    df['effective_start'] = self._date_column(df, 'effective_start', fallback='vintage_date')
    df['effective_end'] = self._date_column(df, 'effective_end')
    df['source_file'] = self._string_column(df, 'source_filename', max_len=100, default=batch_id)
    df['row_num'] = df.index.astype(int)
    df['release_id'] = release_uuid

    insert_columns = [
        "id",
        "release_id",
        "hcpcs_code",
        "modifiers",
        "modifier_key",
        # ...
        "effective_start",
        "effective_end",
        "source_file",
        "row_num",
    ]

    records = self._replace_null_like(df[insert_columns]).to_dict('records')

    with self.db_session.begin_nested():
        self.db_session.execute(delete(RVUItem).where(RVUItem.release_id == release_uuid))
        self._bulk_insert_chunked(RVUItem, records)

    return len(records)
```

**Optimization Note:** All expensive operations (date parsing, UUID generation, string normalization) run once on whole columns. `df.to_dict('records')` keeps the conversion in C, and chunked `bulk_insert_mappings` preserves throughput while keeping memory usage predictable.

### Change 2: Script Optimizations

**Before:**
```python
def main():
    output_dir = "data/ingestion/production"
    # ... setup ...
    result = asyncio.run(ingestor.ingest(release_id=release_id))
    logger.info(f"Ingestion completed: {result}")  # Huge JSON dump
    release_count = db_session.query(Release).count()  # O(n) scan
```

**After:**
```python
def main(release_id: str = None, output_dir: str = None, skip_download: bool = False):
    # Check if release already exists (short-circuit)
    if release_id and db_session.query(Release).filter_by(
        type='RVU_FULL', source_version=release_id
    ).first():
        logger.info("Release already exists, skipping ingestion")
        return 0
    
    # ... run ingestion ...
    logger.info(f"Status: {result.get('status')}, Records: {result.get('total_records')}")
    
    # O(1) verification
    exists = db_session.query(Release).filter_by(id=release_uuid).first() is not None
```

## Backward Compatibility

✅ **All changes are internal to the ingestor** — external APIs remain unchanged  
✅ **Error handling preserved** — bulk failures still logged  
✅ **Data integrity maintained** — same validation rules apply  
✅ **Natural key uniqueness** — enforced via schema constraints  

## Testing Strategy

### Unit Tests
- Test bulk insert with 1K, 10K, 100K row DataFrames
- Verify data integrity (compare row-by-row vs bulk output)
- Test error handling (partial failures in bulk)
- Test empty/null value handling

### Integration Tests
- Run E2E test suite (RVU E2E harness)
- Verify Release/ORM relationships still work
- Verify queries against inserted data return correct results

### Performance Tests
- Benchmark before/after with representative dataset sizes
- Measure database query count reduction
- Measure memory usage (should be similar or lower)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Data integrity bugs | Low | High | Comprehensive E2E tests, row-by-row comparison |
| Memory overflow | Low | Medium | Chunk large datasets (>500K rows) |
| Partial bulk failures | Medium | Medium | Wrap in try/except, log details, rollback |
| Schema mismatch | Low | High | Validate column mappings against models |
| Transaction deadlocks | Very Low | Medium | Use short-lived transactions |

## Success Criteria

✅ **Performance:** 50-100x speedup on 10K+ row datasets  
✅ **Correctness:** E2E tests pass, no data integrity issues  
✅ **Reliability:** Error handling robust, partial failures handled gracefully  
✅ **Maintainability:** Code remains readable, PRD docs updated  

## Rollout Plan

1. **Development:** Implement in feature branch
2. **Testing:** Run full E2E suite, performance benchmarks
3. **Code Review:** Verify changes, check error handling
4. **Staging:** Deploy to staging, run production-scale test
5. **Production:** Deploy with monitoring enabled

## Related Documentation

- `prds/STD-database-platform-prd-v1.0.md` (bulk operations guidance)
- `prds/STD-api-performance-scalability-prd-v1.0.md` (performance best practices)
- `artifacts/RVU_PIPELINE_COMPLETION_PLAN_REVIEW.md` (upstream context)

---

**Next Steps:**
1. Implement Phase 1 (vectorized preprocessing)
2. Implement Phase 2 (bulk inserts)
3. Run E2E tests
4. Benchmark performance improvements
