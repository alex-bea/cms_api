# Issue 1: Fix and Reload GPCI Data

## Problem Found

The RVU ingestor has a column name mismatch:
- **Parser outputs**: `gpci_work`, `gpci_pe`, `gpci_mp`
- **Loader expects**: `work_gpci`, `pe_gpci`, `mp_gpci`
- **Result**: All GPCI values loaded as NULL

## Fix Applied

Updated `rvu_ingestor.py` `_load_gpci_data()` to handle both column naming conventions.

## Solution: Re-run Ingestion

Since the data in `gpci_indices` has NULL values, we need to re-run the ingestion:

```bash
# In Render One-Off Job:
python scripts/load_rvu_to_production.py
```

This will:
1. Re-parse GPCI files with correct column mapping
2. Load to `gpci_indices` with actual GPCI values (not NULL)
3. Then we can run the script to load to `gpci` table

## Alternative: Check Parquet Files

If parquet files were created during ingestion, they might have the correct data:

```bash
# Check if parquet files exist
find data/ingestion/production/curated -name "*gpci*.parquet" -type f

# If found, we could load directly from parquet to gpci table
```

## Next Steps

1. **Re-run ingestion** with the fix
2. **Verify** `gpci_indices` has non-NULL GPCI values
3. **Run** the script to load from `gpci_indices` → `gpci` table

