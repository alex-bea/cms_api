# Specific Fixes for RVU Ingestor Test Failures

**Goal:** Make all DIS stage tests pass by fixing 5 specific issues

---

## Fix 1: Land Output Layout (test_dis_land_stage failure)

**Problem:** Tests (and legacy callers) expect `raw_directory` to point at the release root
while the shared land stage now returns the concrete files directory.

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Current Code (Phase 2 refactor baseline):**
```python
result = await execute_land(...)
return result
```

**Fix:**
```python
raw_dir = Path(result.get("raw_directory", ""))
if raw_dir.name == "files":
    raw_files_dir = str(raw_dir)
    result.setdefault("raw_files_directory", raw_files_dir)
    result["raw_directory"] = str(raw_dir.parent)
    raw_batch_obj = result.get("raw_batch")
    if raw_batch_obj:
        raw_batch_obj.raw_data_path = raw_files_dir
```

**Change:** Preserve the new executor output while adding a `raw_files_directory`
field and restoring `raw_directory` to the release root for backward compatibility.

---

## Fix 2: Wrap Validation Payload (test_dis_validate_stage failure)

**Problem:** validate() returns only internal dict, missing outer `status`, `quality_score`, etc.

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`  
**Location:** Lines 2049-2136 in `validate()` method

**Current Code:**
```python
return {
    "batch_id": raw_batch.metadata.get("batch_id", "unknown"),
    "release_id": raw_batch.metadata.get("release_id", "unknown"),
    "validation_rules": [],
    "quality_score": 1.0,
    "rejects": [],
    # ... missing status key
}
```

**Fix:**
```python
return {
    "status": "success",  # ADD THIS
    "batch_id": raw_batch.metadata.get("batch_id", "unknown"),
    "release_id": raw_batch.metadata.get("release_id", "unknown"),
    "validation_results": validation_results,  # Wrap internal results
    "validation_rules": [],
    "quality_score": validation_results.get("quality_score", 1.0) * 100,  # Scale 0-1 to 0-100
    "rejected_records": validation_results.get("rejected_records", 0),
    "total_records": validation_results.get("total_records", 0),
    "valid_records": validation_results.get("valid_records", 0),
    "quarantine_summary": validation_results.get("quarantine_summary", ""),
}
```

**Also need to update validation_results structure:**
```python
validation_results = {
    "batch_id": ...,
    "release_id": ..., 
    "quality_score": quality_score,  # Keep 0-1 internally
    "total_records": ...,
    "valid_records": ...,
    "rejected_records": ...,
    "quarantine_batch_id": ...,
    "quarantine_summary": ...
}
```

---

## Fix 3: Adjust Normalize Shim (test_dis_normalize_stage, etc. failures)

**Problem:** Tests call `_normalize_stage(raw_batch, validate_result)` but method only accepts 1 arg

**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`  
**Location:** Lines 263-265

**Current Code:**
```python
async def _normalize_stage(self, validated_batch: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy helper retained for compatibility with DIS tests."""
    return await self.normalize(validated_batch)
```

**Fix:**
```python
async def _normalize_stage(self, *args) -> Dict[str, Any]:
    """Legacy helper retained for compatibility with DIS tests."""
    # Accept either (validated_batch) or (raw_batch, validated_batch) for backward compat
    if len(args) == 2:
        # Tests pass (raw_batch, validated_batch) - ignore raw_batch
        validated_batch = args[1]
    else:
        validated_batch = args[0]
    
    result = await self.normalize(validated_batch)
    
    # Wrap result to match test expectations
    return {
        "status": "success",
        "batch_id": validated_batch.get("batch_id"),
        "release_id": validated_batch.get("release_id"),
        "normalized_records": result.get("normalized_records", 0),
        "schema_contract_path": result.get("schema_contract_path"),
        "column_dictionary_path": result.get("column_dictionary_path"),
    }
```

---

## Fix 4: Ensure Pipeline Result Shape (test_full_dis_pipeline failure)

**Problem:** validate() no longer returns outer payload with batch_id, quality_score, etc.

**File:** `cms_pricing/ingestion/run/dis_pipeline.py`  
**Location:** `_generate_final_results()` method

**Current:** Echoes inputs but missing fields from validation stage

**Fix:** Extract and propagate validation fields:
```python
def _generate_final_results(
    self, 
    release_id: str, 
    batch_id: str,
    raw_batch: Any,
    validation_results: Any,
    adapted_batch: Any,
    enriched_data: Any,
    publish_results: Any
) -> Dict[str, Any]:
    
    # Extract validation fields if present
    quality_score = 1.0
    record_count = 0
    
    if isinstance(validation_results, dict):
        quality_score = validation_results.get("quality_score", 1.0)
    
    if adapted_batch and hasattr(adapted_batch, 'dataframes'):
        record_count = sum(len(df) for df in adapted_batch.dataframes.values() if hasattr(df, '__len__'))
    
    return {
        "status": "success",
        "release_id": release_id,
        "batch_id": batch_id,
        "record_count": record_count,
        "quality_score": quality_score,
        "pipeline_stages": {
            "land": "completed",
            "validate": "completed", 
            "normalize": "completed",
            "enrich": "completed",
            "publish": "completed"
        },
        "raw_batch": raw_batch,
        "validation_results": validation_results,
        "adapted_batch": adapted_batch,
        "enriched_data": enriched_data,
        "publish_results": publish_results
    }
```

---

## Fix 5: Add get_contract() to SchemaRegistry

**Problem:** Code calls `get_contract()` but method doesn't exist

**File:** `cms_pricing/ingestion/contracts/schema_registry.py`

**Fix:** Add alias method:
```python
def get_contract(self, schema_id: str) -> Optional[SchemaContract]:
    """Get schema contract by ID (alias for get_schema)"""
    return self.get_schema(schema_id)
```

**Or update callers to use get_schema()**

---

## Implementation Checklist

- [ ] Fix 1: Update _land_with_provided_files to return release root directory
- [ ] Fix 2: Wrap validate() return with status, quality_score (scaled), etc.
- [ ] Fix 3: Update _normalize_stage to accept both signature patterns
- [ ] Fix 4: Update _generate_final_results to extract and propagate all fields
- [ ] Fix 5: Add get_contract() method to SchemaRegistry
- [ ] Run pytest to verify all tests pass

---

## Estimated Time

- Fix 1: 5 minutes (simple return value change)
- Fix 2: 15 minutes (wrap return dict)
- Fix 3: 10 minutes (update signature with *args)
- Fix 4: 15 minutes (update _generate_final_results)
- Fix 5: 5 minutes (add method or update callers)

**Total:** ~50 minutes
