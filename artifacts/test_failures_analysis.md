# RVU Ingestor Test Failures Analysis

## Summary of Failures

**Command:** `pytest tests/ingestors/test_rvu_ingestor_e2e.py -k dis`

**Results:** 6 failed, 1 passed

### Issues Identified:

1. **Directory Structure Issue**
   - **Error:** `AssertionError: assert (raw_dir / "files").exists()`
   - **Line:** `tests/ingestors/test_rvu_ingestor_e2e.py:164`
   - **Problem:** The land stage creates files in `raw/cms_rvu/<release_id>/files/` but the test expects them in `raw/cms_rvu/<release_id>/files/files/` (double nested)
   - **Root Cause:** Line 1976 in `_land_with_provided_files` creates directory with `/files` suffix, but test expects separate `files/` subdirectory

2. **RawBatch Constructor Issue**
   - **Error:** `TypeError: RawBatch.__init__() missing 1 required positional argument`
   - **Lines:** Multiple test failures
   - **Problem:** Tests create RawBatch with wrong signature
   - **Actual signature from ingestor_spec.py:54-59:**
     ```python
     @dataclass
     class RawBatch:
         source_files: List[SourceFile]  # Required
         raw_content: Optional[Dict[str, bytes]] = None
         metadata: Optional[Dict[str, Any]] = None
         raw_data_path: Optional[str] = None
     ```
   - **Test usage (line 189-197):** Creates RawBatch correctly, but `raw_content` should be passed

3. **_normalize_stage Signature Mismatch**
   - **Error:** `TypeError: RVUIngestor._normalize_stage() takes 2 positional arguments but 3 were given`
   - **Problem:** Tests call `_normalize_stage(raw_batch, validate_result)` but method signature is `_normalize_stage(validated_batch: Dict[str, Any])`
   - **Root Cause:** Line 263-265 shows method expects Dict, not RawBatch

4. **SchemaRegistry Method Missing**
   - **Error:** `'SchemaRegistry' object has no attribute 'get_contract'`
   - **Problem:** Line 2085 calls `self.schema_registry.get_contract("cms_rvu_v1")` but method doesn't exist
   - **Actual method:** Should be `get_schema()` or similar

5. **Publish Stage Metadata Missing**
   - **Error:** `KeyError: 'vintage_date'` and `KeyError: 'batch_id'`
   - **Problem:** `publish()` method expects `enriched_batch` to be a Dict with keys like `vintage_date`, `batch_id` but received different structure

6. **RawBatch.get() Error**
   - **Error:** `AttributeError: 'RawBatch' object has no attribute 'get'`
   - **Problem:** Code treats RawBatch as a dict, but it's a dataclass

## Current Implementation Status

### What's Working:
1. **Parser Integration** ✅
   - `_dataset_parsers` dict (lines 99-125) correctly maps dataset types to parsers
   - `_invoke_parser` (lines 1049-1060) properly calls parser functions
   - `_adapt_raw_data_sync` (lines 1294-1426) extracts ZIP files and invokes parsers

2. **Real Parser Usage** ✅
   - Parsers are imported at top of file (lines 51-55)
   - Real parsing happens in `_adapt_raw_data_sync` when ZIP files are processed
   - ParseResult objects are correctly used

3. **ZIP Handling** ✅
   - `_land_with_provided_files` (lines 1969-2047) properly reads files from `file://` URLs
   - Files are correctly saved to disk

### What Needs Fixing:

1. **Directory Structure**
   - `_land_with_provided_files` line 1976 creates wrong path
   - Should be: `raw/cms_rvu/<release_id>/files/`  
   - Currently creates: path already includes `/files` at the end

2. **Test Compatibility**
   - Tests call methods that expect different signatures
   - Need to update tests OR ingestor methods to match

3. **Missing raw_content in RawBatch**
   - Tests need to pass `raw_content` when creating RawBatch
   - Currently missing this field

4. **Schema Registry API**
   - Missing `get_contract()` method on SchemaRegistry
   - Need to add or use existing method

## Next Steps

To fix the pipeline recovery plan needs:

1. **Fix directory structure** in `_land_with_provided_files` (line 1976)
2. **Update tests** to pass correct RawBatch data (include raw_content)
3. **Fix SchemaRegistry** method calls
4. **Align method signatures** between tests and implementation
5. **Fix publish stage** to handle correct metadata structure

