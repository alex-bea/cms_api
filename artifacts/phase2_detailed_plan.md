# Phase 2: Detailed Provenance Implementation Plan

**Date:** 2025-01-XX  
**Status:** Planning  
**Purpose:** Add `release_id` and `batch_id` columns to simplified fee schedule tables for deterministic provenance tracking

---

## Overview

Phase 2 implements provenance tracking across all pricing engines by adding `release_id` and `batch_id` columns to the simplified `Fee*` tables. This enables the ClearBill app to track exactly which version of CMS data was used for each pricing calculation.

**Timeline Estimate:** 5-7 days  
**Risk Level:** Medium (requires database migration, ingestion updates, and engine changes)  
**Dependencies:** None (can proceed after Sprint 1 completion)

---

## Phase 2.1: Database Migration (Days 1-2)

### Step 1.1: Create Alembic Migration

**File:** `alembic/versions/8d80f393d0ee_add_provenance_to_fee_tables.py` ✅ **CREATED**

**Migration Details:**
```python
"""Add release_id and batch_id to fee schedule tables

Revision ID: 8d80f393d0ee
Revises: 6d0f0408be80
Create Date: 2025-10-31 11:45:05.679288

Adds provenance columns (release_id, batch_id) to all simplified fee schedule
tables to enable deterministic tracking of CMS data versions used in pricing.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = '8d80f393d0ee'
down_revision = '6d0f0408be80'  # Latest revision: add_plans_and_plan_components_tables
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Set safety timeouts per PRD requirements
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    
    # Tables to modify (in dependency order)
    tables_to_modify = [
        'fee_mpfs',
        'fee_opps',
        'fee_asc',
        'fee_ipps',
        'fee_clfs',
        'fee_dmepos',
        'gpci',
        'conversion_factors',
        'wage_index',
        'ipps_base_rates'
    ]
    
    for table_name in tables_to_modify:
        # Add nullable columns (will be populated by future ingestion)
        op.add_column(
            table_name,
            sa.Column('release_id', sa.String(50), nullable=True)
        )
        op.add_column(
            table_name,
            sa.Column('batch_id', sa.String(50), nullable=True)
        )
        
        # Create indexes for efficient provenance queries
        op.create_index(
            f'idx_{table_name}_release',
            table_name,
            ['release_id'],
            unique=False
        )
        op.create_index(
            f'idx_{table_name}_batch',
            table_name,
            ['batch_id'],
            unique=False
        )

def downgrade() -> None:
    # Remove indexes first
    tables_to_modify = [
        'fee_mpfs', 'fee_opps', 'fee_asc', 'fee_ipps',
        'fee_clfs', 'fee_dmepos', 'gpci', 'conversion_factors',
        'wage_index', 'ipps_base_rates'
    ]
    
    for table_name in tables_to_modify:
        op.drop_index(f'idx_{table_name}_batch', table_name=table_name)
        op.drop_index(f'idx_{table_name}_release', table_name=table_name)
        op.drop_column(table_name, 'batch_id')
        op.drop_column(table_name, 'release_id')
```

**Acceptance Criteria:**
- [ ] Migration runs successfully on empty database
- [ ] Migration runs successfully on database with existing data
- [ ] All columns are nullable (no NOT NULL constraint)
- [ ] Indexes created without CONCURRENTLY (acceptable for new columns)
- [ ] Downgrade path tested (rollback works)
- [ ] Migration execution time < 30 seconds per table

**Testing:**
```bash
# Test on dev database
alembic upgrade head

# Verify columns exist
psql -d cms_pricing -c "\d fee_mpfs"
psql -d cms_pricing -c "\d fee_opps"

# Test downgrade
alembic downgrade -1
alembic upgrade head
```

**Staging Migration Testing (Required Before Production):**
```bash
# 1. Restore production snapshot to staging database
# 2. Run migration with timing
time alembic upgrade head

# 3. Verify migration completes within timeout limits
#    - Lock timeout: 5s per table
#    - Statement timeout: 30s total
#    - Expected runtime: < 2 minutes for all 10 tables

# 4. Verify columns on canary tables (largest tables)
psql -d cms_pricing_staging -c "
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name IN ('fee_mpfs', 'fee_opps', 'fee_asc')
  AND column_name IN ('release_id', 'batch_id')
ORDER BY table_name, column_name;
"

# 5. Verify indexes created
psql -d cms_pricing_staging -c "
SELECT 
    indexname,
    tablename
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname LIKE 'idx_%_release'
   OR indexname LIKE 'idx_%_batch'
ORDER BY tablename, indexname;
"

# 6. Test queries still work (no performance regression)
EXPLAIN ANALYZE SELECT * FROM fee_mpfs WHERE year = 2025 LIMIT 10;
EXPLAIN ANALYZE SELECT * FROM fee_opps WHERE year = 2025 AND quarter = '1' LIMIT 10;
```

**Migration Runtime Validation:**
- Capture start/end timestamps
- Verify each `ALTER TABLE` completes within 5s
- Total migration time should be < 2 minutes
- If any table exceeds timeout, investigate and adjust timeouts

---

### Step 1.2: Update SQLAlchemy Models

**File:** `cms_pricing/models/fee_schedules.py`

**Changes Required:**

1. **Add columns to each model:**
```python
class FeeMPFS(Base):
    # ... existing columns ...
    
    # Provenance columns
    release_id = Column(String(50), nullable=True, index=True)
    batch_id = Column(String(50), nullable=True, index=True)
    
    # Update indexes
    __table_args__ = (
        Index("idx_mpfs_year_hcpcs", "year", "hcpcs"),
        Index("idx_mpfs_effective", "effective_from", "effective_to"),
        Index("idx_fee_mpfs_release", "release_id"),  # New
        Index("idx_fee_mpfs_batch", "batch_id"),      # New
    )
```

**Models to Update:**
- `FeeMPFS`
- `FeeOPPS`
- `FeeASC`
- `FeeIPPS`
- `FeeCLFS`
- `FeeDMEPOS`
- `GPCI`
- `ConversionFactor`
- `WageIndex`
- `IPPSBaseRate`

**Acceptance Criteria:**
- [ ] All models have `release_id` and `batch_id` columns
- [ ] Columns are nullable=True (existing data compatibility)
- [ ] Indexes added to `__table_args__`
- [ ] No linting errors
- [ ] Model definitions match migration

**Testing:**
```python
# Test model instantiation
from cms_pricing.models.fee_schedules import FeeMPFS
record = FeeMPFS(
    year=2025,
    hcpcs="99213",
    # ... other required fields ...
    release_id="mpfs_2025_annual_20250115_120000",
    batch_id="batch-uuid-123"
)
assert record.release_id is not None
```

---

## Phase 2.2: Ingestion Pipeline Updates (Days 2-3)

### Step 2.1: Update Database Loaders

**File:** `scripts/load_data.py`

**Current Problem:**
```python
# Line 128-141: Missing release_id/batch_id
fee_record = FeeMPFS(
    hcpcs=row['hcpcs'],
    work_rvu=row['work_rvu'],
    # ... missing release_id, batch_id ...
)
```

**Solution:**
```python
def load_mpfs_data(self, build_id: str = None) -> int:
    # ... existing code ...
    
    # Extract release_id and batch_id from parquet metadata or DataFrame
    # Option 1: From DataFrame columns (if preserved from ingestion)
    release_id = df.get('release_id', [None])[0] if 'release_id' in df.columns else None
    batch_id = df.get('batch_id', [None])[0] if 'batch_id' in df.columns else None
    
    # Option 2: From build_id or file path metadata
    if not release_id and build_id:
        # Parse build_id format or extract from path
        # e.g., "MPFS/2025-01-15_123456_MPFS" -> extract timestamp
        release_id = f"mpfs_{year}_legacy_{build_id}"
        batch_id = build_id
    
    for _, row in df.iterrows():
        fee_record = FeeMPFS(
            hcpcs=row['hcpcs'],
            work_rvu=row['work_rvu'],
            pe_nf_rvu=row['pe_nf_rvu'],
            pe_fac_rvu=row['pe_fac_rvu'],
            mp_rvu=row['mp_rvu'],
            global_days=row.get('global_days', 0),
            status_indicator=row.get('status_indicator', ''),
            year=row['year'],
            revision=row.get('revision', 'A'),
            effective_from=datetime.strptime(row['effective_from'], '%Y-%m-%d').date(),
            effective_to=None,
            # NEW: Provenance fields
            release_id=row.get('release_id', release_id),  # Prefer row-level, fallback to batch
            batch_id=row.get('batch_id', batch_id)
        )
        db.add(fee_record)
```

**Files to Update:**
- `scripts/load_data.py`:
  - `load_mpfs_data()` method
  - `load_opps_data()` method (if exists)
  - Any other dataset loaders

**Pattern to Apply:**
1. Check DataFrame for `release_id`/`batch_id` columns (from normalized parquet)
2. If missing, infer from build_id or file metadata
3. Include in record creation
4. Log provenance when loading

**Acceptance Criteria:**
- [ ] All loaders include release_id/batch_id in record creation
- [ ] Graceful fallback when metadata unavailable (sets to None)
- [ ] Logs include provenance information
- [ ] Existing data loads still work (backward compatible)

---

### Step 2.2: Ensure Ingestion Preserves Metadata

**Files to Verify:**
- `cms_pricing/ingestion/ingestors/opps_ingestor.py` - Already adds to DataFrames (line 592-593) ✅
- `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` - Need to verify
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py` - Need to verify
- Other ingestors

**Verification Checklist:**
- [ ] `normalize_stage()` adds `release_id` and `batch_id` to DataFrames
- [ ] `publish_stage()` preserves these columns in parquet output
- [ ] Parquet files include provenance columns when written

**Example Enhancement (if needed):**
```python
# In normalize_stage:
for table_name, df in normalized_data.items():
    df['year'] = batch_info.year
    df['quarter'] = batch_info.quarter
    df['effective_from'] = batch_info.effective_from
    df['effective_to'] = batch_info.effective_to
    df['release_id'] = batch_info.release_id or batch_info.batch_id  # Ensure populated
    df['batch_id'] = batch_info.batch_id  # Ensure populated
```

---

### Step 2.3: Create Data Backfill Script (Optional)

**File:** `scripts/backfill_provenance.py`

**Purpose:** Populate `release_id`/`batch_id` for existing data if needed (or leave NULL for legacy data).

**Strategy Options:**
1. **Leave NULL** (Recommended): Mark legacy data explicitly
2. **Set placeholder**: `"legacy_unknown"` for all existing rows
3. **Infer from dates**: Attempt to match effective dates to known releases

**If backfilling needed:**
```python
def backfill_provenance_placeholders(db_session):
    """Set placeholder provenance for legacy data"""
    tables = [FeeMPFS, FeeOPPS, FeeASC, FeeIPPS, FeeCLFS, FeeDMEPOS, GPCI, ConversionFactor, WageIndex]
    
    for model in tables:
        count = db_session.query(model).filter(
            model.release_id.is_(None)
        ).update({
            model.release_id: "legacy_unknown",
            model.batch_id: "legacy_unknown"
        })
        logger.info(f"Backfilled {count} records in {model.__tablename__}")
    
    db_session.commit()
```

**Decision Point:** Choose strategy before migration. Recommendation: Leave NULL (Option 1) to distinguish legacy from new data.

---

## Phase 2.3: Engine Updates (Days 3-4)

### Step 3.1: Update Engines to Return Provenance

**Files to Update:**
- `cms_pricing/engines/mpfs.py`
- `cms_pricing/engines/opps.py`
- `cms_pricing/engines/asc.py`
- `cms_pricing/engines/ipps.py`
- `cms_pricing/engines/clfs.py`
- `cms_pricing/engines/dmepos.py`

**Pattern to Apply:**

**Before:**
```python
# In engine.price_code():
return {
    "allowed_cents": ...,
    "trace_refs": [f"mpfs_{year}_{quarter}_{code}"]
}
```

**After:**
```python
# In engine.price_code():
return {
    "allowed_cents": ...,
    "trace_refs": [
        f"mpfs_{year}_{quarter}_{code}",  # Existing format
        # NEW: Provenance trace refs with standardized format
        f"{dataset_id}:release:{mpfs_data.release_id}" if mpfs_data.release_id else None,
        f"{dataset_id}:batch:{mpfs_data.batch_id}" if mpfs_data.batch_id else None
    ],
    # Direct provenance fields
    "release_id": mpfs_data.release_id,
    "batch_id": mpfs_data.batch_id,
    "dataset_id": "MPFS"  # For aggregation
}
```

**Trace Refs Format Specification:**

When engines append provenance to `trace_refs`, use this standardized format:
- **Release ID:** `{dataset_id}:release:{release_id}`
- **Batch ID:** `{dataset_id}:batch:{batch_id}`

**Examples:**
- `MPFS:release:mpfs_2025_annual_20250115_120000`
- `MPFS:batch:uuid-1234-5678`
- `OPPS:release:opps_2025q1_r1`
- `OPPS:batch:opps_2025q1_batch_001`

**Parsing Pattern (for downstream tooling):**
```python
# Parse trace refs for provenance extraction
release_id = None
batch_id = None

for ref in trace_refs:
    if ref and ':' in ref:
        parts = ref.split(':')
        if len(parts) == 3:
            dataset, provenance_type, value = parts
            if provenance_type == 'release':
                release_id = value
            elif provenance_type == 'batch':
                batch_id = value
```

This format ensures:
- Consistent parsing across all datasets
- Clear provenance type (release vs batch)
- Dataset identification for multi-dataset queries

**Specific Changes by Engine:**

#### MPFS Engine (`cms_pricing/engines/mpfs.py`):
```python
# After querying mpfs_data, gpci_data, cf_data
dataset_id = "MPFS"
trace_refs = [
    f"mpfs_{year}_{quarter}_{code}",  # Existing format
]

# Add MPFS provenance (standardized format)
if mpfs_data.release_id:
    trace_refs.append(f"{dataset_id}:release:{mpfs_data.release_id}")
if mpfs_data.batch_id:
    trace_refs.append(f"{dataset_id}:batch:{mpfs_data.batch_id}")

# Add supporting data provenance if available
if hasattr(gpci_data, 'release_id') and gpci_data.release_id:
    trace_refs.append(f"GPCI:release:{gpci_data.release_id}")
if hasattr(cf_data, 'release_id') and cf_data.release_id:
    trace_refs.append(f"CF:release:{cf_data.release_id}")

# Filter out None values
trace_refs = [ref for ref in trace_refs if ref is not None]

return {
    # ... existing fields ...
    "trace_refs": trace_refs,
    "release_id": mpfs_data.release_id,
    "batch_id": mpfs_data.batch_id,
    "dataset_id": dataset_id
}
```

#### OPPS Engine (`cms_pricing/engines/opps.py`):
```python
# After querying opps_data, wage_index_data
dataset_id = "OPPS"
trace_refs = [
    f"opps_{year}_{quarter_value}_{code}",  # Existing format
    f"wage_index_{year}_{quarter_value}_{cbsa}",  # Existing format
]

# Add OPPS provenance (standardized format)
if opps_data.release_id:
    trace_refs.append(f"{dataset_id}:release:{opps_data.release_id}")
if opps_data.batch_id:
    trace_refs.append(f"{dataset_id}:batch:{opps_data.batch_id}")

# Add wage index provenance if available
if hasattr(wage_index_data, 'release_id') and wage_index_data.release_id:
    trace_refs.append(f"WageIndex:release:{wage_index_data.release_id}")
if hasattr(wage_index_data, 'batch_id') and wage_index_data.batch_id:
    trace_refs.append(f"WageIndex:batch:{wage_index_data.batch_id}")

# Filter out None values
trace_refs = [ref for ref in trace_refs if ref is not None]

return {
    # ... existing fields ...
    "trace_refs": trace_refs,
    "release_id": opps_data.release_id,
    "batch_id": opps_data.batch_id,
    "dataset_id": dataset_id
}
```

**Acceptance Criteria:**
- [ ] All engines return `release_id` and `batch_id` in results
- [ ] Provenance included in `trace_refs` array
- [ ] Handles NULL gracefully (doesn't break if provenance missing)
- [ ] All engines tested with and without provenance data

**Testing:**
```python
# Test engine with provenance
result = await engine.price_code(...)
assert 'release_id' in result
assert 'batch_id' in result
assert any('release_' in ref for ref in result['trace_refs'] if ref)

# Test engine without provenance (legacy data)
# Should still work, return None for provenance fields
```

---

## Phase 2.4: Service Layer Updates (Day 4-5)

### Step 4.1: Update `_collect_datasets_used()` Method

**File:** `cms_pricing/services/pricing.py`

**Current Implementation:**
- Queries `snapshots` table by `dataset_id` and valuation date
- Returns dataset metadata without release context

**Enhanced Implementation:**
```python
def _collect_datasets_used(
    self,
    datasets_seen: set,
    valuation_year: int,
    valuation_quarter: Optional[str] = None,
    line_items: Optional[List[LineItemResponse]] = None  # NEW: Pass line items for release extraction
) -> List[Dict[str, Any]]:
    """
    Collect active dataset snapshot information for provenance.
    
    Enhanced to include release_id/batch_id from engine results when available.
    """
    if not self.db:
        return [
            {"dataset_id": ds, "digest": None, "effective_from": None, "effective_to": None}
            for ds in sorted(datasets_seen)
        ]
    
    datasets_used = []
    valuation_date = date(valuation_year, 12, 31)
    
    # Extract release_id/batch_id from line items if available
    release_map = {}  # dataset_id -> {release_id, batch_id}
    if line_items:
        for item in line_items:
            setting = item.setting
            if setting and setting not in release_map:
                # Extract from trace_refs using standardized format
                # Format: {dataset_id}:release:{release_id} or {dataset_id}:batch:{batch_id}
                release_id = None
                batch_id = None
                for ref in item.trace_refs:
                    if ref and ':' in ref:
                        parts = ref.split(':')
                        if len(parts) == 3:
                            dataset, provenance_type, value = parts
                            if provenance_type == 'release' and dataset == setting:
                                release_id = value
                            elif provenance_type == 'batch' and dataset == setting:
                                batch_id = value
                
                if release_id or batch_id:
                    release_map[setting] = {
                        "release_id": release_id,
                        "batch_id": batch_id
                    }
    
    for dataset_id in sorted(datasets_seen):
        try:
            # Query snapshot (existing logic)
            snapshot = self.db.query(Snapshot).filter(
                # ... existing filter ...
            ).first()
            
            # Get release info if available
            release_info = release_map.get(dataset_id, {})
            
            if snapshot:
                datasets_used.append({
                    "dataset_id": snapshot.dataset_id,
                    "digest": snapshot.digest,
                    "effective_from": snapshot.effective_from.isoformat() if snapshot.effective_from else None,
                    "effective_to": snapshot.effective_to.isoformat() if snapshot.effective_to else None,
                    "source_url": snapshot.source_url,
                    # NEW: Include release provenance
                    "release_id": release_info.get("release_id"),
                    "batch_id": release_info.get("batch_id")
                })
            else:
                datasets_used.append({
                    "dataset_id": dataset_id,
                    "digest": None,
                    "effective_from": None,
                    "effective_to": None,
                    "source_url": None,
                    "release_id": release_info.get("release_id"),
                    "batch_id": release_info.get("batch_id")
                })
        except Exception as e:
            # ... error handling ...
    
    return datasets_used
```

**Alternative Approach (Simpler):**
If engines return `release_id`/`batch_id` directly in results, extract from engine results before creating `LineItemResponse`:

```python
# In price_plan() method, before creating line items:
for i, component in enumerate(components):
    result = await engine.price_code(...)
    
    # Track provenance per dataset
    if component['setting'] not in dataset_releases:
        dataset_releases[component['setting']] = {
            "release_id": result.get('release_id'),
            "batch_id": result.get('batch_id')
        }
    
    # ... create line_item ...

# Pass to _collect_datasets_used
datasets_used = self._collect_datasets_used(
    datasets_seen=datasets_seen,
    valuation_year=request.year,
    valuation_quarter=request.quarter,
    dataset_releases=dataset_releases  # NEW parameter
)
```

**Acceptance Criteria:**
- [ ] `datasets_used` includes `release_id` and `batch_id` when available
- [ ] Gracefully handles missing provenance (legacy data)
- [ ] Provenance aggregated correctly per dataset

---

## Phase 2.5: Testing & Validation (Days 5-6)

### Step 5.1: Unit Tests

**New Test Files:**
- `tests/models/test_fee_schedules_provenance.py`
- `tests/engines/test_provenance_return.py`
- `tests/services/test_datasets_used_provenance.py`

**Test Cases:**

1. **Model Tests:**
```python
def test_fee_mpfs_with_provenance():
    """Test FeeMPFS model with release_id/batch_id"""
    record = FeeMPFS(
        year=2025,
        hcpcs="99213",
        # ... required fields ...
        release_id="mpfs_2025_annual_20250115",
        batch_id="batch-uuid-123"
    )
    assert record.release_id == "mpfs_2025_annual_20250115"
    assert record.batch_id == "batch-uuid-123"

def test_fee_mpfs_without_provenance():
    """Test FeeMPFS model without provenance (legacy data)"""
    record = FeeMPFS(
        year=2025,
        hcpcs="99213",
        # ... required fields ...
        release_id=None,
        batch_id=None
    )
    assert record.release_id is None
```

2. **Engine Tests:**
```python
async def test_mpfs_engine_returns_provenance(db_session):
    """Test MPFS engine includes provenance in results"""
    # Insert test data with provenance
    test_data = FeeMPFS(
        year=2025,
        hcpcs="99213",
        release_id="test_release",
        batch_id="test_batch",
        # ... other fields ...
    )
    db_session.add(test_data)
    db_session.commit()
    
    engine = MPSFEngine()
    result = await engine.price_code(
        code="99213",
        zip="94102",
        year=2025,
        geography=test_geography
    )
    
    assert result['release_id'] == "test_release"
    assert result['batch_id'] == "test_batch"
    assert any('release_test_release' in ref for ref in result['trace_refs'])

async def test_mpfs_engine_handles_null_provenance(db_session):
    """Test MPFS engine handles NULL provenance gracefully"""
    # Insert test data without provenance (legacy)
    test_data = FeeMPFS(
        year=2025,
        hcpcs="99213",
        release_id=None,
        batch_id=None,
        # ... other fields ...
    )
    db_session.add(test_data)
    db_session.commit()
    
    engine = MPSFEngine()
    result = await engine.price_code(...)
    
    assert result['release_id'] is None
    assert result['batch_id'] is None
    # Should still return valid pricing
    assert 'allowed_cents' in result
```

3. **Service Tests:**
```python
def test_collect_datasets_used_with_provenance(db_session):
    """Test _collect_datasets_used includes release_id/batch_id"""
    service = PricingService(db_session)
    
    # Create test snapshot
    snapshot = Snapshot(
        dataset_id="MPFS",
        effective_from=date(2025, 1, 1),
        digest="test_digest",
        release_id="test_release"  # If snapshots table has this
    )
    db_session.add(snapshot)
    
    datasets_used = service._collect_datasets_used(
        datasets_seen={"MPFS"},
        valuation_year=2025,
        dataset_releases={"MPFS": {"release_id": "test_release", "batch_id": "test_batch"}}
    )
    
    assert len(datasets_used) == 1
    assert datasets_used[0]["dataset_id"] == "MPFS"
    assert datasets_used[0]["release_id"] == "test_release"
    assert datasets_used[0]["batch_id"] == "test_batch"
```

---

### Step 5.2: Integration Tests

**Test End-to-End Provenance Flow:**

```python
async def test_pricing_response_includes_provenance(client, db_session):
    """Test that pricing responses include full provenance"""
    # Setup: Insert data with provenance
    # Execute: POST /pricing/price
    # Verify: Response.datasets_used includes release_id/batch_id
```

---

### Step 5.3: Migration Testing

**Checklist:**
- [ ] Migration runs on empty database
- [ ] Migration runs on database with existing data (no errors)
- [ ] Columns added correctly (verify with `\d table_name`)
- [ ] Indexes created (verify with `\di idx_*_release`)
- [ ] Existing queries still work (no breaking changes)
- [ ] Downgrade works (can rollback)

**Commands:**
```bash
# Test migration
alembic upgrade head

# Verify
psql -d cms_pricing -c "SELECT column_name, data_type, is_nullable 
                       FROM information_schema.columns 
                       WHERE table_name = 'fee_mpfs' 
                       AND column_name IN ('release_id', 'batch_id');"

# Test rollback
alembic downgrade -1
alembic upgrade head
```

---

## Phase 2.6: Documentation & Rollout (Day 6-7)

### Step 6.1: Update API Documentation

**Files:**
- `api-contracts/openapi.yaml` - Add provenance fields to response schemas
- `README.md` - Document provenance tracking

**Schema Updates:**
```yaml
components:
  schemas:
    DatasetUsed:
      type: object
      properties:
        dataset_id:
          type: string
        digest:
          type: string
          nullable: true
        effective_from:
          type: string
          format: date
          nullable: true
        effective_to:
          type: string
          format: date
          nullable: true
        source_url:
          type: string
          nullable: true
        release_id:
          type: string
          nullable: true
          description: "Release identifier from CMS data source. None for legacy data."
        batch_id:
          type: string
          nullable: true
          description: "Batch identifier from ingestion run. None for legacy data."
```

---

### Step 6.2: Deployment Checklist

**Pre-Deployment:**
- [ ] All tests pass (unit + integration)
- [ ] Migration tested on staging database
- [ ] Code review completed
- [ ] Documentation updated

**Deployment Steps:**
1. Deploy code changes (models, engines, services)
2. Run migration: `alembic upgrade head`
3. Verify migration: Check columns exist
4. Deploy ingestion updates
5. Re-ingest sample dataset to verify provenance population
6. Monitor for errors

**Post-Deployment:**
- [ ] Verify new ingestion populates provenance
- [ ] Test pricing endpoints return provenance
- [ ] Monitor error logs for NULL-related issues
- [ ] Verify indexes are being used (EXPLAIN queries)

---

## Phase 2.7: Rollout Strategy

### Option A: Big Bang (Recommended for Phase 2)

**Approach:** Deploy all changes together in single release

**Pros:**
- Simpler deployment
- No intermediate state confusion
- Faster completion

**Cons:**
- All-or-nothing risk
- Requires careful testing

**Steps:**
1. Deploy migration + code changes
2. Run migration
3. Deploy ingestion updates
4. Re-run ingestion for one dataset (MPFS) to verify
5. Monitor for 24 hours
6. Continue with other datasets

---

### Option B: Phased Rollout

**Phase 2A:** Migration + Models (no code changes)
- Add columns, update models
- Existing code continues to work
- No breaking changes

**Phase 2B:** Engine Updates
- Update engines to return provenance
- Services can use new fields

**Phase 2C:** Ingestion Updates
- Start populating provenance for new data

**Pros:**
- Lower risk per phase
- Can verify each step independently

**Cons:**
- Longer timeline
- More deployments

**Recommendation:** Use Option A (Big Bang) since changes are additive and backward-compatible.

---

## Risk Mitigation

### Risk 1: Migration Performance

**Mitigation:**
- Add columns with NULL (fast, metadata-only on PG 11+)
- Use `SET LOCAL` timeouts
- Create indexes after columns (can be done concurrently later if needed)

### Risk 2: Existing Data Compatibility

**Mitigation:**
- All columns nullable
- Engines handle None gracefully
- Clear documentation that NULL = legacy data

### Risk 3: Ingestion Pipeline Breaks

**Mitigation:**
- Verify ingestors preserve metadata before Phase 2
- Test ingestion on dev with sample data
- Have rollback plan for ingestion scripts

### Risk 4: Query Performance Impact

**Mitigation:**
- Indexes added for provenance queries
- NULL columns don't affect existing queries
- Monitor query plans post-deployment

---

## Success Metrics

**Phase 2 Complete When:**
- [ ] All Fee* tables have release_id/batch_id columns
- [ ] All models updated with new columns
- [ ] All engines return provenance in results
- [ ] `datasets_used` includes release_id/batch_id
- [ ] New ingestion populates provenance
- [ ] Unit tests pass (100% coverage for new code)
- [ ] Integration tests pass
- [ ] Documentation updated
- [ ] Staging deployment successful
- [ ] Production deployment successful

**Post-Deployment Validation:**
- Run test pricing request
- Verify `datasets_used` contains provenance
- Check logs for any NULL-related warnings
- Verify new ingestion run populates fields

---

## Timeline Summary

| Day | Tasks | Deliverables |
|-----|-------|--------------|
| 1 | Create migration, test on dev | Migration file, tested migration |
| 2 | Update models, update loaders | Models updated, loaders populate provenance |
| 3 | Update all engines | Engines return provenance |
| 4 | Update pricing service | `datasets_used` includes release info |
| 5 | Write unit tests | Test suite for provenance |
| 6 | Integration testing | End-to-end provenance flow verified |
| 7 | Documentation, deployment | Docs updated, production deployed |

**Total Estimated Time:** 5-7 days (depending on test coverage requirements)

---

## Dependencies & Prerequisites

- [x] Sprint 1 complete (validation fixes)
- [x] Investigation document complete
- [ ] Access to staging database for migration testing
- [ ] Sample data available for testing
- [ ] Code review process ready

---

## Next Steps After Phase 2

1. **Phase 2.1:** Seed snapshot records (D1 from original plan)
2. **Phase 2.2:** Enhance snapshots with release_id mapping
3. **Phase 3:** Add regression tests for validation paths

---

## Questions & Decision Points

1. **Backfill Strategy:** Leave NULL or set placeholder? → **Decision: Leave NULL** (recommended)
2. **Index Creation:** Use CONCURRENTLY? → **Decision: Not needed for new columns (acceptable for initial deploy)**
3. **Release ID Format:** Standardize format across datasets? → **Decision: Use existing ingestor formats (MPFS: `mpfs_YYYY_Q_batch`, OPPS: `opps_YYYYqN_rNN`)**
4. **Testing Scope:** Unit + integration or full E2E? → **Decision: Unit + integration minimum**

---

Ready to begin Phase 2 implementation when approved.

