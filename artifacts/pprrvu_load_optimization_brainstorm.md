# PPRRVU Load Optimization Brainstorm

**Date:** 2025-01-15  
**Context:** Current PPRRVU loading takes ~20 minutes for 227k rows  
**Goal:** Identify and prioritize optimization strategies to reduce load time to <3 minutes  

---

## Executive Summary

**Current State:**
- ✅ Progress logging implemented (chunk-level visibility)
- ✅ Streaming DataFrame slices implemented (memory optimization)
- ⏳ Bulk insert optimization pending (execute_values, COPY FROM STDIN)
- ⏳ Database-level optimizations pending (indexes, autovacuum)

**Optimization Pipeline:**
1. **Completed:** Low-risk improvements (logging + memory streaming)
2. **Next:** Medium-risk SQLAlchemy bypass (execute_values) - **2-3x speedup**
3. **Future:** High-performance COPY FROM STDIN - **Additional 25-30% speedup**

**Estimated Total Potential:** 20 min → 2-3 min (6-10x improvement)

---

## Current Architecture Analysis

### Load Flow (227k rows)

```
Parser Output (DataFrame)
    ↓
Column Mapping & Type Conversion
    ↓
_prepare_base_dataframe() - UUID generation, metadata injection
    ↓
_replace_null_like() - Null handling
    ↓
_bulk_replace_records()
    ├─ DELETE WHERE release_id = :uuid (single query)
    └─ _bulk_insert_chunked()
        ├─ Chunk 1: 5,000 rows → bulk_insert_mappings()
        ├─ Chunk 2: 5,000 rows → bulk_insert_mappings()
        ├─ ...
        └─ Chunk 46: 2,000 rows → bulk_insert_mappings()
```

### Current Bottlenecks

1. **SQLAlchemy ORM Overhead** (Primary)
   - `bulk_insert_mappings()` still goes through SQLAlchemy layer
   - Dictionary → SQLAlchemy type conversion per field
   - ~40-50ms per chunk = ~2 seconds total for 46 chunks (not the main issue)

2. **Network Round-Trips** (Secondary)
   - 46 separate INSERT statements sent to database
   - Connection pooling helps, but batch size could be larger

3. **PostgreSQL Index Maintenance** (Background)
   - Multiple indexes on `rvu_items` table (hcpcs_code, modifier_key, status_code, release_id)
   - Index updates happen during insert (can be deferred)

4. **Dictionary Materialization** (Minimized)
   - ✅ Already optimized: Streaming chunks instead of full DataFrame → dicts
   - Current: Only one chunk's dicts in memory at a time

---

## Optimization Strategies

### ✅ Tier 1: Completed (Low Risk)

#### 1.1 Progress Logging
**Status:** ✅ Implemented  
**Impact:** Observability only (no speedup)  
**Implementation:** `BULK_INSERT_LOG_FREQUENCY = 5` reports every 5th chunk

**Code Location:**
- `cms_pricing/ingestion/datasets/rvu_loaders.py:39` (constant)
- `cms_pricing/ingestion/datasets/rvu_loaders.py:717-746` (logging logic)

**Value:**
- Eliminates "is it hung?" confusion
- Provides timing visibility per chunk
- Helps diagnose slow chunks

#### 1.2 Streaming DataFrame Slices
**Status:** ✅ Implemented  
**Impact:** Memory stability (prevents swapping on small dynos)  
**Implementation:** Process chunks sequentially, discard after insert

**Code Location:**
- `cms_pricing/ingestion/datasets/rvu_loaders.py:729-731` (chunk slicing)
- Previous: `records = prepared.to_dict("records")` (227k dicts in memory)
- Current: `chunk_df.to_dict("records")` (5k dicts max in memory)

**Value:**
- Prevents memory spikes that cause swapping
- Enables stable runs on small Render dynos
- No wall-clock speedup, but prevents hangs

---

### 🚀 Tier 2: Recommended Next (Medium Risk, High Reward)

#### 2.1 psycopg2.extras.execute_values() 
**Status:** ⏳ Planned  
**Impact:** 2-3x speedup (20 min → 6-7 min)  
**Effort:** 3-4 days  
**Risk:** Medium (bypasses SQLAlchemy, needs careful testing)

**How It Works:**
```python
# Current (SQLAlchemy):
db_session.bulk_insert_mappings(RVUItem, records)

# Optimized (psycopg2):
from psycopg2.extras import execute_values
conn = db_session.connection().connection  # Get raw psycopg2 connection
cursor = conn.cursor()

# Convert records to tuples (leaner than dicts)
columns = ['id', 'release_id', 'hcpcs_code', ...]
rows = [(r['id'], r['release_id'], r['hcpcs_code'], ...) for r in records]

execute_values(
    cursor,
    f"INSERT INTO rvu_items ({', '.join(columns)}) VALUES %s",
    rows,
    template=None,
    page_size=10000  # Larger batches than bulk_insert_mappings
)
```

**Advantages:**
- Direct PostgreSQL protocol (no SQLAlchemy overhead)
- Tuple-based (no dict key lookups)
- Larger page sizes supported (10k+ vs 5k)
- ~2-3x faster than `bulk_insert_mappings()`

**Disadvantages:**
- Bypasses SQLAlchemy unit-of-work (no automatic session tracking)
- Manual column mapping required
- Error handling more complex (raw SQL exceptions)
- Type coercion must be handled manually

**Implementation Plan:**
1. **Feature Flag:** Add `USE_EXECUTE_VALUES = True` config
2. **Helper Function:** `_bulk_insert_execute_values()` parallel to `_bulk_insert_chunked()`
3. **Column Mapping:** Generate INSERT column list from model
4. **Type Conversion:** Ensure Python types match PostgreSQL (UUID, dates, arrays)
5. **Error Handling:** Wrap in try/except, fallback to SQLAlchemy on failure
6. **Testing:** Verify data integrity, compare row counts, test error paths

**Code Changes:**
- New function: `_bulk_insert_execute_values()` in `rvu_loaders.py`
- Modify: `_bulk_replace_records()` to choose method based on flag
- Add: Type conversion helpers for UUID, dates, arrays
- Test: Extend existing e2e tests with flag toggle

**Estimated Time Savings:**
- Current: ~20 minutes for 227k rows
- With execute_values: ~6-7 minutes (2-3x improvement)

---

#### 2.2 Parquet → COPY FROM STDIN
**Status:** 🔮 Future Option  
**Impact:** Additional 25-30% speedup (6-7 min → 4-5 min)  
**Effort:** 1-2 days (after execute_values)  
**Risk:** Medium-High (most complex, requires parquet_fdw or CSV conversion)

**How It Works:**
```python
# Option A: CSV COPY (simpler)
import io
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False, header=False)
csv_buffer.seek(0)

conn = db_session.connection().connection
cursor = conn.cursor()
cursor.copy_from(
    csv_buffer,
    'rvu_items',
    columns=['id', 'release_id', 'hcpcs_code', ...],
    sep=','
)

# Option B: Parquet via COPY (requires parquet_fdw extension)
# More complex, but preserves data types better
```

**Advantages:**
- Fastest bulk load method (PostgreSQL native)
- Handles large datasets efficiently
- Lower memory footprint (streaming)

**Disadvantages:**
- Requires CSV conversion or parquet_fdw extension
- Type coercion happens at DB level (less control)
- No per-row error handling (all-or-nothing)
- Requires careful NULL/quoting handling

**Feasibility Assessment:**
- ✅ CSV COPY: Feasible, medium effort
- ⚠️ Parquet COPY: Requires extension installation (may not be available on Render)

**Recommendation:**
- Implement execute_values first
- Only pursue COPY if execute_values doesn't meet <3 min target
- CSV COPY is simpler and likely sufficient

---

### 🔧 Tier 3: Database-Level Optimizations (Low Risk, Moderate Impact)

#### 3.1 Index Maintenance Strategy

**Current Indexes:**
```sql
CREATE INDEX idx_rvu_items_release_id ON rvu_items(release_id);
CREATE INDEX idx_rvu_items_hcpcs_code ON rvu_items(hcpcs_code);
CREATE INDEX idx_rvu_items_modifier_key ON rvu_items(modifier_key);
CREATE INDEX idx_rvu_items_status_code ON rvu_items(status_code);
```

**Optimization 1: Defer Index Updates**
```sql
-- Create indexes as NOT VALID, validate after load
CREATE INDEX CONCURRENTLY idx_rvu_items_hcpcs_code_new 
ON rvu_items(hcpcs_code) WHERE release_id = :new_release_id;

-- After load completes:
ALTER INDEX idx_rvu_items_hcpcs_code_new VALIDATE;
```

**Optimization 2: Partial Indexes**
- Only index active releases (WHERE release_id IN (active_releases))
- Reduces index size and update overhead

**Optimization 3: Drop and Recreate**
- Drop indexes before bulk load
- Recreate after load completes
- Faster than incremental updates

**Estimated Impact:**
- Deferred indexes: 10-20% speedup on inserts
- Partial indexes: 5-10% speedup (smaller index)
- Drop/recreate: 20-30% speedup (if acceptable downtime)

**Recommendation:**
- Start with deferred index updates (safest)
- Consider drop/recreate if load window allows

---

#### 3.2 Autovacuum Tuning

**Issue:**
- Autovacuum runs during inserts (contention)
- Dead tuples accumulate from DELETE operations
- Index bloat slows inserts

**Solutions:**

**Option A: Manual VACUUM After Load**
```python
# After bulk insert completes:
db_session.execute(text("VACUUM ANALYZE rvu_items"))
```

**Option B: Tune Autovacuum Parameters**
```sql
-- Per-table autovacuum settings
ALTER TABLE rvu_items SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_analyze_scale_factor = 0.05
);
```

**Option C: Disable Autovacuum During Load**
```sql
-- Before load:
ALTER TABLE rvu_items SET (autovacuum_enabled = false);

-- After load:
ALTER TABLE rvu_items SET (autovacuum_enabled = true);
VACUUM ANALYZE rvu_items;
```

**Estimated Impact:**
- Manual VACUUM: 5-10% speedup (cleaner tables)
- Tuned autovacuum: 2-5% speedup (less contention)
- Disabled during load: 10-15% speedup (no contention)

**Recommendation:**
- Manual VACUUM after load (safest, immediate benefit)
- Consider disabling autovacuum if load window is predictable

---

#### 3.3 Batch Size Optimization

**Current:** `BULK_INSERT_CHUNK_SIZE = 5000`

**Analysis:**
- Smaller batches = more round-trips = slower
- Larger batches = more memory = risk of timeouts
- Optimal depends on row size and network latency

**Testing Strategy:**
1. Benchmark with 5k, 10k, 20k, 50k batch sizes
2. Measure: insert time, memory usage, error rate
3. Choose largest batch that doesn't timeout

**Expected Optimal:** 10k-20k rows per batch

**Code Change:**
```python
# Tune based on testing
BULK_INSERT_CHUNK_SIZE = 10000  # or 20000

# For execute_values, can go even larger:
EXECUTE_VALUES_PAGE_SIZE = 20000
```

**Estimated Impact:**
- 5k → 10k: 5-10% speedup (fewer round-trips)
- 10k → 20k: 3-5% additional speedup
- Diminishing returns after 20k

---

### 🎯 Tier 4: Advanced Optimizations (Higher Risk, Specialized Use Cases)

#### 4.1 Parallel Inserts (Multi-threaded)

**Concept:**
- Split DataFrame into N chunks
- Insert chunks in parallel using multiple DB connections
- Requires connection pooling and transaction coordination

**Challenges:**
- Database connection limits
- Transaction isolation (need separate transactions per chunk)
- Error handling complexity
- Potential deadlocks

**Feasibility:**
- ⚠️ Limited by Render Postgres connection limits
- ⚠️ Complexity may not justify gains
- ✅ Could help on dedicated databases with many connections

**Estimated Impact:**
- 2-4x speedup on multi-core systems with sufficient connections
- Risk: Deadlocks, connection exhaustion

**Recommendation:**
- Only consider if execute_values + COPY still too slow
- Requires careful connection pool management

---

#### 4.2 Unlogged Tables (Temporary)

**Concept:**
- Insert into UNLOGGED table (no WAL)
- After load, convert to logged table
- Fastest inserts, but data loss risk on crash

**Trade-offs:**
- ✅ Fastest possible inserts (no WAL overhead)
- ❌ Data loss risk if database crashes during load
- ❌ No replication to read replicas
- ❌ Requires table conversion after load

**Feasibility:**
- ⚠️ High risk (data loss potential)
- ⚠️ Complex migration (table conversion)
- ✅ Could work for non-critical staging loads

**Recommendation:**
- Only for staging/test environments
- Not recommended for production

---

#### 4.3 Foreign Data Wrappers (FDW)

**Concept:**
- Use `file_fdw` or `parquet_fdw` to load directly from file
- PostgreSQL reads file directly (no Python round-trip)

**Challenges:**
- Requires FDW extension installation
- File must be accessible to PostgreSQL server
- Less flexible than programmatic loading

**Feasibility:**
- ⚠️ May not be available on Render Postgres
- ⚠️ File access logistics (S3, local filesystem)
- ✅ Could be fastest if infrastructure supports it

**Recommendation:**
- Investigate Render Postgres FDW support
- Consider for future if other optimizations insufficient

---

## Implementation Roadmap

### Phase 1: Baseline & Measurement (0.5 days)
**Goal:** Establish current performance metrics

**Tasks:**
1. Add timing instrumentation to `_bulk_insert_chunked()`
2. Log: total time, per-chunk time, database query counts
3. Run test load with 227k rows, capture metrics
4. Document baseline in `artifacts/pprrvu_performance_baseline.md`

**Deliverables:**
- Baseline metrics (time, memory, query counts)
- Performance test script

---

### Phase 2: execute_values Implementation (2-3 days)
**Goal:** Implement psycopg2.execute_values() path

**Tasks:**
1. **Day 1: Core Implementation**
   - Add feature flag: `USE_EXECUTE_VALUES = True`
   - Create `_bulk_insert_execute_values()` function
   - Implement column mapping helper
   - Add type conversion helpers (UUID, dates, arrays)

2. **Day 2: Integration & Testing**
   - Integrate into `_bulk_replace_records()`
   - Add error handling and fallback to SQLAlchemy
   - Unit tests for type conversion
   - Integration test with small dataset

3. **Day 3: Validation & Benchmarking**
   - Run full 227k row load with execute_values
   - Compare results with SQLAlchemy path
   - Verify data integrity (row counts, sample queries)
   - Document performance improvement

**Deliverables:**
- Feature-flagged execute_values implementation
- Performance comparison (before/after)
- Updated tests

---

### Phase 3: Database Optimizations (1 day)
**Goal:** Apply database-level optimizations

**Tasks:**
1. **Index Optimization**
   - Test deferred index creation
   - Measure impact on insert time
   - Document index strategy

2. **Autovacuum Tuning**
   - Add manual VACUUM after load
   - Test autovacuum disable during load (optional)
   - Measure impact

3. **Batch Size Tuning**
   - Benchmark different batch sizes (5k, 10k, 20k)
   - Choose optimal size
   - Update constants

**Deliverables:**
- Optimized index strategy
- VACUUM automation
- Tuned batch sizes

---

### Phase 4: COPY FROM STDIN (Optional, 1-2 days)
**Goal:** Implement COPY FROM STDIN if execute_values insufficient

**Prerequisites:**
- execute_values implemented and tested
- Still not meeting <3 min target

**Tasks:**
1. **CSV COPY Implementation**
   - Create CSV conversion helper
   - Implement `_bulk_insert_copy_from_csv()`
   - Add error handling
   - Test with 227k rows

2. **Validation**
   - Compare performance with execute_values
   - Verify data integrity
   - Document trade-offs

**Deliverables:**
- COPY FROM STDIN implementation (if needed)
- Performance comparison

---

## Risk Assessment

### Risk Matrix

| Optimization | Risk Level | Impact | Mitigation |
|--------------|------------|--------|------------|
| execute_values | Medium | High | Feature flag, fallback to SQLAlchemy, comprehensive testing |
| COPY FROM STDIN | Medium-High | High | CSV conversion validation, error handling, type coercion testing |
| Index Optimization | Low | Medium | Test in staging, measure impact, rollback plan |
| Autovacuum Tuning | Low | Low-Medium | Manual VACUUM is safe, disable during load is optional |
| Batch Size Tuning | Low | Low | Easy to revert, benchmark first |
| Parallel Inserts | High | High | Only if other optimizations insufficient, complex error handling |

### Risk Mitigation Strategies

1. **Feature Flags:** All optimizations behind flags for easy rollback
2. **Fallback Paths:** Always maintain SQLAlchemy path as fallback
3. **Comprehensive Testing:** Unit tests, integration tests, data integrity checks
4. **Staged Rollout:** Test in staging, then production with monitoring
5. **Performance Monitoring:** Track metrics before/after, alert on regressions

---

## Success Metrics

### Performance Targets

| Metric | Current | Target | Stretch Goal |
|--------|---------|--------|--------------|
| **Load Time (227k rows)** | ~20 min | <3 min | <2 min |
| **Throughput** | ~190 rows/sec | >1,200 rows/sec | >1,800 rows/sec |
| **Memory Peak** | ~500 MB | <1 GB | <500 MB |
| **Database Queries** | 46 INSERTs | <10 batches | <5 batches |

### Quality Metrics

- ✅ **Data Integrity:** 100% row count match (source vs loaded)
- ✅ **Error Rate:** <0.1% (same as current)
- ✅ **Test Coverage:** All optimizations covered by tests
- ✅ **Observability:** Progress logging, timing metrics, error tracking

---

## Signal 11 Issue Status

### Current Status
- **Issue:** Python segmentation fault (Signal 11) in sandbox test environment
- **Impact:** Blocks some test execution (not production loads)
- **Root Cause:** Environment-specific (pandas/pyarrow native library conflicts)

### Resolution Status
- ✅ **Docker Environment:** Tests pass in Docker (environment isolation)
- ✅ **Production:** No issues reported in production (Render environment stable)
- ⚠️ **Local macOS:** May still experience segfaults (environment-specific)

### Relationship to Optimization
- **Not Blocking:** Optimization work doesn't require fixing Signal 11
- **Testing Strategy:** Use Docker for optimization testing
- **Production Impact:** None (production uses Render environment)

### Recommendation
- Continue optimization work using Docker for testing
- Signal 11 is environment issue, not code issue
- Track separately from optimization work

---

## Environment Stabilization Plan Status

### Completed Items
- ✅ Native dependencies documented
- ✅ Docker environment working (tests pass)
- ✅ Requirements lockfile maintained

### Missing Items
- ⏳ Bootstrap script (`scripts/bootstrap_env.sh`)
- ⏳ CI workflow updates (GitHub Actions)
- ⏳ Developer setup documentation updates

### Relationship to Optimization
- **Not Blocking:** Optimization can proceed with Docker environment
- **Future Work:** Complete env stabilization for local development
- **Priority:** Low (Docker works, optimization is priority)

---

## Alternatives to Python Dicts

### Question: "Can we use something smaller than Python dicts?"

### Analysis

**Current Approach:**
- DataFrame → `to_dict("records")` → List of dicts
- Each dict has string keys (memory overhead)
- Dict lookup overhead (hash table)

**Alternative 1: Tuples (execute_values)**
- ✅ **Leaner:** No string keys, direct positional access
- ✅ **Faster:** No hash lookups, better CPU cache
- ✅ **Implemented:** Part of execute_values optimization
- Example: `(uuid, release_id, hcpcs_code, ...)` vs `{"id": uuid, "release_id": ..., ...}`

**Alternative 2: Named Tuples**
- ✅ **Type-safe:** `from collections import namedtuple`
- ✅ **Memory efficient:** Similar to tuples
- ⚠️ **Limited:** Still requires conversion to tuples for execute_values
- Example: `Record(id=uuid, release_id=..., ...)` vs dict

**Alternative 3: Parquet Files**
- ❓ **Feasibility:** Can't use directly with SQLAlchemy `bulk_insert_mappings()`
- ✅ **Could work with COPY:** PostgreSQL COPY can read Parquet via `parquet_fdw`
- ⚠️ **Complexity:** Requires extension, file access, type mapping
- **Verdict:** Not practical for current architecture

**Alternative 4: NumPy Arrays**
- ✅ **Memory efficient:** Dense arrays, no dict overhead
- ❌ **Type limitations:** Difficult to handle mixed types (strings, UUIDs, dates)
- ❌ **Conversion overhead:** Still need to convert to tuples/dicts for database
- **Verdict:** Not worth the complexity

**Alternative 5: Streaming Generators**
- ✅ **Memory efficient:** Process one row at a time
- ⚠️ **Performance:** Still need to batch for database efficiency
- ✅ **Already implemented:** Current chunking is similar
- **Verdict:** Current approach is optimal

### Recommendation

**Best Approach: Tuples (via execute_values)**
- ✅ Memory efficient (no string keys)
- ✅ CPU efficient (no hash lookups)
- ✅ Compatible with PostgreSQL bulk insert
- ✅ Part of recommended optimization (Tier 2)

**Implementation:**
```python
# Instead of:
records = df.to_dict("records")  # List of dicts

# Use:
columns = ['id', 'release_id', 'hcpcs_code', ...]
records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
# Or more efficiently:
records = [tuple(row) for row in df[columns].itertuples(index=False)]
```

**Memory Savings:**
- Dict: ~200-300 bytes per row (227k rows = ~50-70 MB)
- Tuple: ~100-150 bytes per row (227k rows = ~25-35 MB)
- **Savings: ~50% memory reduction**

---

## Next Steps

### Immediate (This Week)
1. ✅ **Baseline Measurement:** Run test load, capture current metrics
2. ✅ **execute_values Implementation:** Start Phase 2 (2-3 days)
3. ✅ **Testing:** Verify data integrity, performance improvement

### Short-term (Next Week)
1. ✅ **Database Optimizations:** Apply index and autovacuum tuning
2. ✅ **Batch Size Tuning:** Find optimal batch size
3. ✅ **Documentation:** Update performance benchmarks

### Medium-term (If Needed)
1. ⏳ **COPY FROM STDIN:** Implement if execute_values insufficient
2. ⏳ **Advanced Optimizations:** Parallel inserts, FDW (only if needed)

---

## References

### Code Locations
- **Current Implementation:** `cms_pricing/ingestion/datasets/rvu_loaders.py`
- **Load Function:** `load_pprrvu_data()` (line 192)
- **Bulk Insert:** `_bulk_insert_chunked()` (line 717)
- **Constants:** `BULK_INSERT_CHUNK_SIZE = 5000` (line 38)

### Related Plans
- `artifacts/pprrvu_bulk_insert_optimization_plan.md` (detailed execute_values plan)
- `artifacts/env_stabilization_plan.md` (environment setup)
- `artifacts/RVU_INGESTOR_PERFORMANCE_OPTIMIZATION_PLAN.md` (general RVU optimization)

### Documentation
- `prds/REF-rvu-database-schema-v1.0.md` (database schema)
- `artifacts/RVU_DATABASE_LOADING_COMPLETE.md` (current implementation status)

---

## Summary

**Current State:** ~20 minutes for 227k rows (bulk_insert_mappings with streaming)

**Optimization Path:**
1. ✅ **Completed:** Progress logging + memory streaming (observability + stability)
2. 🚀 **Next:** execute_values implementation (2-3x speedup → ~6-7 min)
3. 🔧 **Then:** Database optimizations (additional 20-30% → ~4-5 min)
4. 🔮 **If Needed:** COPY FROM STDIN (additional 25-30% → ~3-4 min)

**Expected Outcome:** 20 min → 3-4 min (5-6x improvement)

**Risk Level:** Low-Medium (feature flags, fallbacks, comprehensive testing)

**Effort:** 3-4 days for execute_values + database optimizations

**Recommendation:** Proceed with execute_values implementation as highest priority.

