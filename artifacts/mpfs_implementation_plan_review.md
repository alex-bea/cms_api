# MPFS Implementation Plan Review

**Date:** 2025-11-04  
**Reviewer:** AI Code Review  
**Status:** Critical Issues Found - Requires Fixes Before Implementation

---

## Executive Summary

Found **5 critical schema mismatches** and **2 vectorization opportunities** that will cause runtime failures. The plan needs updates before implementation.

---

## Critical Issues

### 1. ❌ **CRITICAL: RVU-GPCI Join Missing Key**

**Location:** Phase 5.1, Line 559  
**Issue:** `rvu_df.merge(gpci_df, on="locality_id", how="inner")` will fail because **RVU items don't have `locality_id`**.

**Evidence:**
- `cms_pricing/models/rvu.py` shows `RVUItem` has no `locality_id` column
- RVU items are HCPCS-level data (national), not locality-specific
- GPCI is locality-specific

**Impact:** This join will produce zero rows or fail with KeyError.

**Solution:**
```python
# Option 1: Cartesian product (all RVU × all GPCI) - then filter by effective dates
# This is the correct MPFS calculation approach
joined = rvu_df.assign(key=1).merge(gpci_df.assign(key=1), on='key').drop('key', axis=1)

# Option 2: If locality is specified in query context, filter GPCI first
# But for full payment table, need all combinations

# Option 3: Use explicit cross join (pandas >= 1.2.0)
joined = rvu_df.merge(gpci_df, how='cross')
```

**Recommendation:** Use cartesian product approach (all HCPCS × all localities) as this matches CMS MPFS calculation model where every HCPCS code has a payment for every locality.

---

### 2. ❌ **Schema Mismatch: RVU Column Names**

**Location:** Phase 4.1, Line 425; Phase 5.1, Lines 563-573, 577

**Issue:** Plan uses incorrect column names:
- Plan: `hcpcs` → **Actual:** `hcpcs_code` (in DB model)
- Plan: `pe_nf_rvu` → **Actual:** `pe_rvu_nonfac` (in DB model)
- Plan: `pe_fac_rvu` → **Actual:** `pe_rvu_fac` (in DB model)

**Evidence:**
- `cms_pricing/models/rvu.py` line 43: `hcpcs_code`
- `cms_pricing/models/rvu.py` line 49: `pe_rvu_nonfac`
- `cms_pricing/models/rvu.py` line 50: `pe_rvu_fac`
- `cms_pricing/ingestion/datasets/rvu_loaders.py` line 151: alias from `hcpcs` → `hcpcs_code`

**Impact:** Column selection in Phase 4.1 will fail or return empty columns. Payment calculations will fail.

**Solution:**
```python
# Phase 4.1 - Fix column selection
required_cols = ["hcpcs_code", "work_rvu", "pe_rvu_nonfac", "pe_rvu_fac", "mp_rvu", "status_code", "global_days"]
# Note: Parquet might have "hcpcs" (from parser), loader aliases to "hcpcs_code"
# Check both and normalize:
if "hcpcs" in df.columns and "hcpcs_code" not in df.columns:
    df["hcpcs_code"] = df["hcpcs"]
    df = df.drop(columns=["hcpcs"])

# Phase 5.1 - Fix payment calculation
joined["facility_amount"] = (
    joined["work_rvu"] * joined["gpci_work"] +
    joined["pe_rvu_fac"] * joined["gpci_pe"] +  # Not pe_fac_rvu
    joined["mp_rvu"] * joined["gpci_mp"]
) * joined["cf_value"]

joined["non_facility_amount"] = (
    joined["work_rvu"] * joined["gpci_work"] +
    joined["pe_rvu_nonfac"] * joined["gpci_pe"] +  # Not pe_nf_rvu
    joined["mp_rvu"] * joined["gpci_mp"]
) * joined["cf_value"]
```

---

### 3. ❌ **Schema Mismatch: GPCI Column Names**

**Location:** Phase 4.1, Line 440; Phase 5.1, Lines 563-573

**Issue:** Plan uses `gpci_work`, `gpci_pe`, `gpci_mp` but loader expects these and maps to `work_gpci`, `pe_gpci`, `mp_gpci`.

**Evidence:**
- `cms_pricing/models/rvu.py` line 93-95: `work_gpci`, `pe_gpci`, `mp_gpci` (DB model)
- `cms_pricing/ingestion/datasets/rvu_loaders.py` lines 235-242: Aliases `gpci_work` → `work_gpci`
- Parquet files may have either naming convention

**Impact:** Column selection may fail if parquet uses DB naming. Payment calculations will fail if wrong names used.

**Solution:**
```python
# Phase 4.1 - Handle both naming conventions
def _normalize_gpci_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize GPCI column names to canonical form."""
    column_mapping = {
        "gpci_work": "gpci_work",  # Keep canonical form
        "work_gpci": "gpci_work",
        "gpci_pe": "gpci_pe",
        "pe_gpci": "gpci_pe",
        "gpci_mp": "gpci_mp",
        "gpci_malp": "gpci_mp",  # Some parsers use gpci_malp
        "mp_gpci": "gpci_mp",
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    return df

# In _load_gpci_slice:
df = _normalize_gpci_columns(df)
required_cols = ["locality_id", "gpci_work", "gpci_pe", "gpci_mp"]  # Use canonical form
```

---

### 4. ❌ **Missing Column: `year` for CF Join**

**Location:** Phase 5.1, Line 560

**Issue:** `joined.merge(cf_df, on="year", how="inner")` will fail because RVU/GPCI don't have `year` column.

**Impact:** Join will fail with KeyError.

**Solution:**
```python
# Extract year from effective dates before joining
# Option 1: Use effective_from year
rvu_df["year"] = pd.to_datetime(rvu_df["effective_from"]).dt.year
gpci_df["year"] = pd.to_datetime(gpci_df["effective_start"]).dt.year

# Option 2: Use CF year from metadata (if CF is single value per year)
# Most CFs are annual, so might be simpler:
cf_year = cf_df["year"].iloc[0]  # Assuming single CF per build
joined = rvu_df.assign(year=cf_year).merge(gpci_df.assign(year=cf_year), on=["locality_id", "year"], how="inner")
joined = joined.merge(cf_df, on="year", how="inner")
```

**Better Approach:**
```python
# Since CF is typically a single value per year, merge it differently
cf_value = cf_df["cf_value"].iloc[0]  # Single CF value
cf_year = cf_df["year"].iloc[0]

# Join RVU + GPCI first (cartesian product)
joined = rvu_df.merge(gpci_df, how='cross')

# Add CF as scalar (all rows get same CF)
joined["cf_value"] = cf_value
joined["cf_year"] = cf_year
```

---

### 5. ❌ **Missing Column: `site_of_service`**

**Location:** Phase 5.1, Line 596

**Issue:** `joined[["hcpcs", "locality_id", "site_of_service"]]` references `site_of_service` which doesn't exist in RVU or GPCI.

**Impact:** Column selection will fail.

**Solution:**
```python
# Option 1: Remove site_of_service (not in RVU/GPCI)
curated["mpfs_link_keys"] = joined[[
    "hcpcs_code", "locality_id"  # Remove site_of_service
]].drop_duplicates().copy()

# Option 2: If site_of_service is needed, derive from facility vs non-facility
# But this is payment-level, not link-level
# Recommendation: Remove it from link_keys
```

---

### 6. ❌ **Missing Columns in Payment Table Selection**

**Location:** Phase 5.1, Lines 576-580

**Issue:** References `effective_from`, `effective_to` but these may not be in joined DataFrame after column selection.

**Impact:** Column selection will fail.

**Solution:**
```python
# Ensure effective dates are preserved
# RVU has: effective_start, effective_end
# GPCI has: effective_start, effective_end
# Need to align names

# Before merge, normalize date columns:
rvu_df = rvu_df.rename(columns={"effective_start": "effective_from", "effective_end": "effective_to"})
gpci_df = gpci_df.rename(columns={"effective_start": "gpci_effective_from", "effective_end": "gpci_effective_to"})

# After merge, use RVU dates for payment table:
curated["mpfs_payment_curated"] = joined[[
    "hcpcs_code", "locality_id", "facility_amount", "non_facility_amount",
    "status_code", "global_days", "effective_from", "effective_to",
    "release_id"
]].copy()
```

---

## Vectorization Opportunities

### 7. ⚠️ **CF Join Can Be Optimized**

**Location:** Phase 5.1, Line 560

**Issue:** Joining CF DataFrame when it's typically a single row per year.

**Current Approach:**
```python
joined = joined.merge(cf_df, on="year", how="inner")
```

**Optimized Approach:**
```python
# Since CF is typically single value per year, use scalar assignment
# This avoids merge overhead for single-row DataFrame
cf_value = cf_df["cf_value"].iloc[0]
cf_year = cf_df["year"].iloc[0]
joined["cf_value"] = cf_value  # Vectorized assignment (broadcasting)
joined["cf_year"] = cf_year
```

**Performance Gain:** Eliminates merge operation for single-row DataFrame, reduces memory overhead.

---

### 8. ⚠️ **Column Selection Safety**

**Location:** Phase 4.1, Lines 424-426, 440-441

**Issue:** Column selection doesn't check if columns exist before selecting.

**Impact:** Will fail with KeyError if parquet schema differs.

**Solution:**
```python
def _load_rvu_slice(self, release_id: str) -> pd.DataFrame:
    # ... existing code ...
    
    # Select required columns with safety check
    required_cols = ["hcpcs_code", "work_rvu", "pe_rvu_nonfac", "pe_rvu_fac", "mp_rvu", "status_code", "global_days"]
    
    # Handle column name variations
    if "hcpcs" in df.columns and "hcpcs_code" not in df.columns:
        df["hcpcs_code"] = df["hcpcs"]
    
    # Check for missing columns
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in RVU parquet: {missing_cols}. Available: {list(df.columns)}")
    
    # Select only available required columns
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    # ... rest of code ...
```

---

## Additional Issues

### 9. ⚠️ **Missing Effective Date Handling**

**Location:** Phase 5.1, Line 559-560

**Issue:** Join doesn't account for effective date ranges. RVU and GPCI may have different effective periods.

**Impact:** May join incompatible RVU/GPCI combinations (e.g., 2025 RVU with 2024 GPCI).

**Solution:**
```python
# Filter GPCI to match RVU effective dates
# Or filter both to valuation date range
valuation_date = pd.Timestamp(self.current_release_id.split('_')[-1]) if '_' in self.current_release_id else pd.Timestamp.now()

# Filter by effective dates before join
rvu_filtered = rvu_df[
    (rvu_df["effective_start"] <= valuation_date) &
    ((rvu_df["effective_end"].isna()) | (rvu_df["effective_end"] >= valuation_date))
]

gpci_filtered = gpci_df[
    (gpci_df["effective_start"] <= valuation_date) &
    ((gpci_df["effective_end"].isna()) | (gpci_df["effective_end"] >= valuation_date))
]

# Then join filtered DataFrames
```

---

### 10. ⚠️ **Missing MAC Handling**

**Location:** Phase 5.1, Line 559

**Issue:** GPCI has `mac` column (unique constraint on `mac, locality_id, effective_start`), but plan doesn't account for it.

**Impact:** If multiple MACs exist for same locality, join will produce duplicates.

**Solution:**
```python
# Option 1: Filter to specific MAC (if known)
# Option 2: Use most recent MAC per locality
# Option 3: Aggregate across MACs (if applicable)

# Most common: Filter to single MAC or use latest
gpci_filtered = gpci_df.sort_values("effective_start").drop_duplicates(subset=["locality_id"], keep="last")
```

---

## Recommendations

### Immediate Fixes Required:

1. **Fix RVU-GPCI join** - Use cartesian product, not locality_id join
2. **Fix column names** - Use `hcpcs_code`, `pe_rvu_nonfac`, `pe_rvu_fac`
3. **Fix GPCI column normalization** - Handle both naming conventions
4. **Fix CF join** - Use scalar assignment instead of merge
5. **Remove `site_of_service`** - Not in source data
6. **Add column existence checks** - Before selection
7. **Add effective date filtering** - Before joins

### Code Pattern to Follow:

```python
# Recommended pattern for Phase 5.1:
def build_curated_datasets(
    self,
    rvu_df: pd.DataFrame,
    gpci_df: pd.DataFrame,
    cf_df: pd.DataFrame,
    release_id: str,
    valuation_date: Optional[pd.Timestamp] = None
) -> Dict[str, pd.DataFrame]:
    """Build all curated MPFS datasets."""
    
    # 1. Normalize column names
    rvu_df = self._normalize_rvu_columns(rvu_df)
    gpci_df = self._normalize_gpci_columns(gpci_df)
    
    # 2. Filter by effective dates
    if valuation_date:
        rvu_df = self._filter_by_effective_dates(rvu_df, valuation_date)
        gpci_df = self._filter_by_effective_dates(gpci_df, valuation_date)
    
    # 3. Extract CF value (assuming single row per year)
    cf_value = cf_df["cf_value"].iloc[0]
    cf_year = cf_df["year"].iloc[0]
    
    # 4. Cartesian product join (all RVU × all GPCI)
    joined = rvu_df.merge(gpci_df, how='cross')
    
    # 5. Add CF as scalar
    joined["cf_value"] = cf_value
    joined["cf_year"] = cf_year
    
    # 6. Compute payments (vectorized)
    joined["facility_amount"] = (
        joined["work_rvu"] * joined["gpci_work"] +
        joined["pe_rvu_fac"] * joined["gpci_pe"] +
        joined["mp_rvu"] * joined["gpci_mp"]
    ) * joined["cf_value"]
    
    joined["non_facility_amount"] = (
        joined["work_rvu"] * joined["gpci_work"] +
        joined["pe_rvu_nonfac"] * joined["gpci_pe"] +
        joined["mp_rvu"] * joined["gpci_mp"]
    ) * joined["cf_value"]
    
    # 7. Build curated tables
    # ... rest of implementation
```

---

## Testing Recommendations

1. **Add schema validation tests** - Verify parquet columns match expectations
2. **Add join validation tests** - Verify cartesian product produces expected row count
3. **Add payment calculation tests** - Golden comparison with known CMS PFREV values
4. **Add edge case tests** - Missing columns, empty DataFrames, date mismatches

---

## References

- **RVU Model:** `cms_pricing/models/rvu.py`
- **GPCI Model:** `cms_pricing/models/rvu.py` (GPCIIndex class)
- **RVU Loader:** `cms_pricing/ingestion/datasets/rvu_loaders.py`
- **Database Schema:** `prds/REF-rvu-database-schema-v1.0.md`
- **MPFS PRD:** `prds/PRD-mpfs-prd-v1.0.md`

---

**Status:** ⚠️ **BLOCKING** - These issues must be fixed before implementation can proceed.

