# D2: Relational Model Investigation - Release ID / Batch ID Addition Plan

**Date:** 2025-01-XX  
**Status:** Complete  
**Purpose:** Map current relational model to plan release_id/batch_id addition to simplified fee schedule tables

## Current State Analysis

### Tables Used by Pricing Engines

| Engine | Primary Tables Queried | Supporting Tables | Has Provenance? |
|--------|----------------------|-------------------|-----------------|
| **MPFS** | `FeeMPFS` | `GPCI`, `ConversionFactor` | ❌ No (FeeMPFS missing) |
| **OPPS** | `FeeOPPS` | `WageIndex` | ❌ No (FeeOPPS missing) |
| **ASC** | `FeeASC` | - | ❌ No |
| **IPPS** | `FeeIPPS` | `IPPSBaseRate`, `WageIndex` | ❌ No |
| **CLFS** | `FeeCLFS` | - | ❌ No |
| **DMEPOS** | `FeeDMEPOS` | - | ❌ No |

### Richer Models with Provenance (Not Used by Engines)

| Model | Table | Has release_id? | Has batch_id? | Used By |
|-------|-------|-----------------|---------------|---------|
| `MPFSRVU` | `mpfs_rvu` | ✅ Yes | ✅ Yes | MPFS API router (not engine) |
| `OPPSAPCPayment` | `opps_apc_payment` | ✅ Yes | ✅ Yes | OPPS API router |
| `OPPSHCPCSCrosswalk` | `opps_hcpcs_crosswalk` | ✅ Yes | ✅ Yes | OPPS API router |
| `OPPSRatesEnriched` | `opps_rates_enriched` | ✅ Yes | ✅ Yes | OPPS API router |

### Key Finding: Data Flow Disconnect

**Ingestion Pipeline:**
- `OPPSIngestor._normalize_stage()` adds `release_id` and `batch_id` to DataFrames (line 592-593)
- Data is saved to parquet files with these fields
- **BUT:** When loading to `FeeOPPS` table (see `scripts/load_data.py`), these fields are not preserved

**Current Gap:**
```python
# In ingestion: DataFrames have release_id/batch_id
df['release_id'] = batch_info.batch_id
df['batch_id'] = batch_info.batch_id

# In load_data.py: FeeMPFS/FeeOPPS creation doesn't include these
fee_record = FeeMPFS(
    hcpcs=row['hcpcs'],
    work_rvu=row['work_rvu'],
    # ... missing: release_id, batch_id
)
```

## Recommended Solution

### Step 1: Add Columns to Simplified Tables (Alembic Migration)

**Tables to Modify:**
- `fee_mpfs` 
- `fee_opps`
- `fee_asc`
- `fee_ipps`
- `fee_clfs`
- `fee_dmepos`
- `gpci` (for MPFS locality adjustments)
- `conversion_factors` (for MPFS/ASC calculations)
- `wage_index` (for OPPS/IPPS calculations)

**Migration Details:**
```sql
ALTER TABLE fee_mpfs 
  ADD COLUMN release_id VARCHAR(50),
  ADD COLUMN batch_id VARCHAR(50);

CREATE INDEX idx_fee_mpfs_release ON fee_mpfs(release_id);
CREATE INDEX idx_fee_mpfs_batch ON fee_mpfs(batch_id);

-- Repeat for other tables...
```

**Default Strategy:**
- Set `release_id` and `batch_id` to `NULL` for existing rows (or use placeholder like `"legacy_unknown"`)
- New ingestion will populate these fields going forward

### Step 2: Update Model Definitions

**Files to Modify:**
- `cms_pricing/models/fee_schedules.py` - Add columns to FeeMPFS, FeeOPPS, FeeASC, etc.
- Add indexes for efficient provenance queries

### Step 3: Update Ingestion Loaders

**Files to Modify:**
- `scripts/load_data.py` - Include release_id/batch_id when creating FeeMPFS records
- `cms_pricing/ingestion/ingestors/opps_ingestor.py` - Ensure publish stage preserves metadata
- Any other loaders that populate Fee* tables

### Step 4: Update Engines

**Files to Modify:**
- `cms_pricing/engines/mpfs.py` - Return release_id/batch_id in trace_refs
- `cms_pricing/engines/opps.py` - Return release_id/batch_id in trace_refs
- `cms_pricing/engines/asc.py` - Return release_id/batch_id in trace_refs
- Similar for other engines

**Example Pattern:**
```python
# In engine.price_code():
result = {
    "allowed_cents": ...,
    "trace_refs": [
        f"mpfs_{year}_{quarter}_{code}",
        f"release_{mpfs_data.release_id}",
        f"batch_{mpfs_data.batch_id}"
    ],
    "release_id": mpfs_data.release_id,
    "batch_id": mpfs_data.batch_id
}
```

### Step 5: Update Pricing Service

**Files to Modify:**
- `cms_pricing/services/pricing.py` - Update `_collect_datasets_used()` to include release_id/batch_id from trace_refs or engine results

## Implementation Checklist

- [ ] Create Alembic migration for all Fee* tables
- [ ] Update SQLAlchemy models with new columns
- [ ] Update load_data.py to populate release_id/batch_id
- [ ] Verify ingestion pipeline preserves metadata through all stages
- [ ] Update all pricing engines to return provenance
- [ ] Update _collect_datasets_used() to aggregate release metadata
- [ ] Add database indexes for provenance queries
- [ ] Test with sample data to verify provenance flow

## Dependencies & Considerations

1. **Backfill Strategy:** Decide how to handle existing rows without provenance
   - Option A: Leave NULL (indicates legacy data)
   - Option B: Set to "legacy_unknown" placeholder
   - Option C: Attempt to infer from effective_from dates and dataset

2. **WageIndex Provenance:** `WageIndex` is shared between OPPS and IPPS - ensure release tracking works for both

3. **GPCI/ConversionFactor Provenance:** These are annual datasets - may use different release_id format than quarterly

4. **Testing:** Need to verify:
   - New ingestion populates fields correctly
   - Engines can query and return provenance
   - datasets_used array includes full provenance chain

## Next Steps

After completing this investigation:
1. Proceed with Sprint 1 quick wins (quarter validation, wage-index improvements)
2. Create Alembic migration as first task of provenance implementation
3. Test migration on development database
4. Update models and ingestion in parallel

