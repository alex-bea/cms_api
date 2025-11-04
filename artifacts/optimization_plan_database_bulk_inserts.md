# Database Bulk Insert Optimization Plan

**Goal:** Replace row-by-row `iterrows()` inserts with `bulk_insert_mappings()` in three ingestors, achieving 3-6x performance improvement on database loading operations.

**Current State:**
- **ZIP Locality Ingester**: Uses `df.iterrows()` + `db.add()` for ~43K records (~30 seconds)
- **ZIP9 Ingester**: Uses `df.iterrows()` + `db.add()` for ~1K records (~1 second)  
- **Locality FIPS Normalizer**: Uses `df.iterrows()` for fuzzy matching and row processing (~10 seconds)

**Target State:**
- All three use SQLAlchemy `bulk_insert_mappings()` pattern (same as RVU ingestor)
- ZIP Locality: ~5 seconds (6x faster)
- ZIP9: ~0.3 seconds (3x faster)
- Locality FIPS: ~3 seconds (3x faster)

**Total Expected Time Savings:** ~35 seconds per full ingestion run

---

## Detailed Task Breakdown

### Task 1: ZIP Locality Ingester Optimization (HIGHEST PRIORITY)

**File:** `cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`  
**Lines:** 315-344  
**Records:** ~43,000  
**Expected Improvement:** 30s → 5s (6x faster)

#### Step 1.1: Review Current Implementation

**Current Code Pattern:**
```python
# Lines 315-344
records_inserted = 0
for _, row in df.iterrows():
    try:
        cms_zip = CMSZipLocality(
            zip5=row['zip5'],
            state=row['state'],
            locality=row['locality'],
            carrier_mac=row.get('carrier_mac'),
            rural_flag=row.get('rural_flag'),
            effective_from=row['effective_from'],
            effective_to=row.get('effective_to'),
            vintage=row['vintage'],
            source_filename=row.get('source_filename', 'zip_code_carrier_locality.zip'),
            ingest_run_id=uuid.UUID(self.current_batch_id),
            data_quality_score=quality_score,
            validation_results=validation_results,
            processing_timestamp=processing_timestamp,
            file_checksum=file_checksum,
            record_count=record_count,
            schema_version=self.schema_version,
            business_rules_applied=business_rules_applied
        )
        db.add(cms_zip)
        records_inserted += 1
    except Exception as e:
        logger.warning("Failed to insert record", error=str(e), zip5=row.get('zip5'))
        continue

db.commit()
```

**Issues:**
- Row-by-row iteration is slow for 43K records
- Each `db.add()` creates ORM overhead
- Single commit at end (could batch commit for progress)
- Error handling skips individual records (good, but needs preservation)

**Estimated Time:** 5 minutes

#### Step 1.2: Reference RVU Ingester Pattern

**File to Reference:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Key Pattern (lines 1414-1421):**
```python
def _bulk_insert_chunked(self, model, records: List[Dict[str, Any]]) -> None:
    """Insert records using SQLAlchemy bulk mappings in predictable chunks."""
    if not records:
        return
    chunk_size = self.BULK_INSERT_CHUNK_SIZE  # 5000
    for start in range(0, len(records), chunk_size):
        chunk = records[start:start + chunk_size]
        self.db_session.bulk_insert_mappings(model, chunk)
```

**Also Note:**
- RVU uses `_bulk_replace_records()` which does DELETE + bulk insert (lines 1489-1512)
- ZIP Locality likely needs UPSERT or simple INSERT (check if duplicates possible)

**Estimated Time:** 10 minutes

#### Step 1.3: Prepare DataFrame for Bulk Mapping

**Required Transformations:**
1. Convert DataFrame to dict records: `df.to_dict('records')`
2. Map DataFrame column names to SQLAlchemy model field names (if different)
3. Add computed fields (UUID conversion, defaults)
4. Handle None/NaN values appropriately

**Example Mapping Logic:**
```python
def _prepare_bulk_records(df: pd.DataFrame, batch_id: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert DataFrame to list of dicts suitable for bulk_insert_mappings.
    
    Args:
        df: Input DataFrame with columns matching CMSZipLocality fields
        batch_id: Ingest batch UUID string
        metadata: Dict with quality_score, validation_results, etc.
    
    Returns:
        List of dicts, one per row, with all required CMSZipLocality fields
    """
    records = []
    
    # Convert DataFrame to dict records
    df_dict = df.to_dict('records')
    
    # Prepare batch UUID once
    batch_uuid = uuid.UUID(batch_id)
    
    for row in df_dict:
        # Map DataFrame columns to model fields (same names here, but convert types)
        record = {
            'zip5': str(row['zip5']).strip()[:5],  # Ensure 5 chars, strip whitespace
            'state': str(row['state']).strip().upper()[:2],
            'locality': str(row['locality']).strip()[:10],
            'carrier_mac': str(row.get('carrier_mac', '')).strip()[:10] if pd.notna(row.get('carrier_mac')) else None,
            'rural_flag': _normalize_rural_flag(row.get('rural_flag')),
            'effective_from': row['effective_from'] if pd.notna(row.get('effective_from')) else date.today(),
            'effective_to': row.get('effective_to') if pd.notna(row.get('effective_to')) else None,
            'vintage': str(row['vintage']).strip()[:10],
            'source_filename': row.get('source_filename', 'zip_code_carrier_locality.zip'),
            'ingest_run_id': batch_uuid,
            # Metadata fields
            'data_quality_score': metadata.get('quality_score'),
            'validation_results': metadata.get('validation_results'),
            'processing_timestamp': metadata.get('processing_timestamp'),
            'file_checksum': metadata.get('file_checksum'),
            'record_count': metadata.get('record_count', len(df)),
            'schema_version': metadata.get('schema_version', '1.0'),
            'business_rules_applied': metadata.get('business_rules_applied'),
        }
        records.append(record)
    
    return records

def _normalize_rural_flag(value) -> Optional[bool]:
    """Normalize rural_flag from various formats to boolean."""
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() in ['R', 'Y', 'TRUE', '1']
    return bool(value)
```

**Estimated Time:** 30 minutes

#### Step 1.4: Implement Bulk Insert with Error Handling

**New Implementation:**
```python
async def _publish_data(self, enriched_data: Dict[str, pd.DataFrame], db) -> Dict[str, Any]:
    """Stage 5: Publish - Store in curated format with bulk inserts"""
    
    publish_results = {}
    total_records = 0
    
    for table_name, df in enriched_data.items():
        if df is None or df.empty:
            continue
        
        # Prepare metadata (same as before)
        quality_score = 0.95  # Calculate from validation_results
        validation_results = {}  # From validate stage
        processing_timestamp = datetime.utcnow()
        file_checksum = self.current_file_checksum or ""
        record_count = len(df)
        business_rules_applied = [
            "zip5_format_validation",
            "state_code_validation",
            "locality_code_validation",
            "data_completeness_check"
        ]
        
        # Prepare records for bulk insert
        try:
            records = _prepare_bulk_records(
                df, 
                self.current_batch_id,
                {
                    'quality_score': quality_score,
                    'validation_results': validation_results,
                    'processing_timestamp': processing_timestamp,
                    'file_checksum': file_checksum,
                    'record_count': record_count,
                    'schema_version': self.schema_version,
                    'business_rules_applied': business_rules_applied
                }
            )
            
            # Bulk insert in chunks (like RVU ingestor)
            chunk_size = 5000
            records_inserted = 0
            errors = []
            
            with db.begin_nested():
                for start in range(0, len(records), chunk_size):
                    chunk = records[start:start + chunk_size]
                    try:
                        db.bulk_insert_mappings(CMSZipLocality, chunk)
                        records_inserted += len(chunk)
                    except Exception as e:
                        logger.warning(
                            "Bulk insert chunk failed, falling back to individual inserts",
                            chunk_start=start,
                            chunk_size=len(chunk),
                            error=str(e)
                        )
                        db.rollback()
                        for record in chunk:
                            try:
                                db.bulk_insert_mappings(CMSZipLocality, [record])
                                records_inserted += 1
                            except Exception as individual_error:
                                errors.append({
                                    'zip5': record.get('zip5'),
                                    'error': str(individual_error)
                                })
                                logger.warning(
                                    "Failed to insert record",
                                    error=str(individual_error),
                                    zip5=record.get('zip5')
                                )
                        db.rollback()
                db.commit()
            
            publish_results[table_name] = {
                "records_inserted": records_inserted,
                "records_skipped": len(errors),
                "errors": errors,
                "quality_score": quality_score,
                "processing_timestamp": processing_timestamp.isoformat()
            }
            
            total_records += records_inserted
            
        except Exception as e:
            db.rollback()
            logger.error("Failed to publish ZIP locality data", error=str(e), table=table_name)
            raise
    
    # Update run completion
    self.runs_manager.complete_run(
        self.current_batch_id,
        RunStatus.SUCCESS,
        output_record_count=total_records,
        processing_cost_usd=0.01
    )
    
    return {
        "record_count": total_records,
        "tables_processed": len(publish_results),
        "publish_results": publish_results
    }
```

**Key Changes:**
- Remove `for _, row in df.iterrows()` loop
- Use `df.to_dict('records')` + mapping function
- Use `db.bulk_insert_mappings()` in chunks
- Preserve error handling (fallback to individual inserts on chunk failure)
- Single commit after all chunks

**Estimated Time:** 45 minutes

#### Step 1.5: Add Helper Function as Class Method

**Add to CMSZipLocalityProductionIngester class:**
```python
def _prepare_bulk_records(self, df: pd.DataFrame, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Prepare DataFrame records for bulk insert mapping."""
    # Implementation from Step 1.3
    pass

def _bulk_insert_chunked(self, db, model, records: List[Dict[str, Any]], chunk_size: int = 5000) -> Tuple[int, List[Dict]]:
    """
    Insert records using bulk mappings in chunks.
    
    Returns:
        Tuple of (records_inserted, errors)
    """
    # Implementation from Step 1.4
    pass
```

**Estimated Time:** 15 minutes

#### Step 1.6: Testing

**Test Cases (run via Docker):**
1. **Happy Path:** Insert 43K valid records → verify all inserted, time < 10s  
   Command: `docker compose run --rm api pytest tests/ingestors/test_cms_zip_locality_production_ingester.py -k bulk_insert`
2. **Error Handling:** Mix of valid/invalid rows → ensure valid rows insert, errors captured
3. **Chunking:** 15K records → confirm chunk loop executed (>=3 iterations)
4. **Data Integrity:** Verify DB column values (UUID, dates, booleans) via SQL assertions
5. **Idempotency (if constraints exist):** Re-run same batch and confirm uniqueness behavior

**Estimated Time:** 30 minutes

#### Task 1 Summary

- **Total Estimated Time:** 2 hours 15 minutes
- **Files Modified:** 1 file (`cms_zip_locality_production_ingester.py`)
- **Test Files:** 1 file (add test case)
- **Risk Level:** Low (pattern already proven in RVU ingestor)
- **Expected Improvement:** 30s → 5s (6x faster)

---

### Task 2: ZIP9 Ingester Optimization

**File:** `cms_pricing/ingestion/ingestors/cms_zip9_ingester.py`  
**Lines:** 568-589  
**Records:** ~1,000  
**Expected Improvement:** 1s → 0.3s (3x faster)

#### Step 2.1: Review Current Implementation

**Current Code (lines 568-589):**
```python
records_inserted = 0
for _, row in zip9_data.iterrows():
    zip9_override = ZIP9Overrides(
        zip9_low=row['zip9_low'],
        zip9_high=row['zip9_high'],
        state=row['state'],
        locality=row['locality'],
        rural_flag=row['rural_flag'],
        effective_from=row['effective_from'],
        effective_to=row['effective_to'],
        vintage=row['vintage'],
        source_filename=row['source_filename'],
        ingest_run_id=self.current_batch_id,
        data_quality_score=quality_score,
        validation_results=validation_results,
        processing_timestamp=processing_timestamp,
        file_checksum=file_checksum,
        record_count=len(zip9_data),
        schema_version=schema_version,
        business_rules_applied=business_rules_applied
    )
    db.add(zip9_override)
    records_inserted += 1

db.commit()
```

**Estimated Time:** 5 minutes

#### Step 2.2: Apply Same Pattern as Task 1

**Implementation:**
- Copy `_prepare_bulk_records()` pattern from ZIP Locality (simplified)
- Map ZIP9 DataFrame columns to `ZIP9Overrides` model fields
- Use `bulk_insert_mappings()` in single chunk (1K records, no chunking needed)

**Simplified Helper:**
```python
def _prepare_zip9_bulk_records(
    df: pd.DataFrame, 
    batch_id: str, 
    metadata: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Prepare ZIP9 DataFrame for bulk insert."""
    records = []
    for row in df.to_dict('records'):
        record = {
            'zip9_low': str(row['zip9_low']).strip()[:9],
            'zip9_high': str(row['zip9_high']).strip()[:9],
            'state': str(row['state']).strip().upper()[:2],
            'locality': str(row['locality']).strip()[:10],
            'rural_flag': _normalize_rural_flag(row.get('rural_flag')),
            'effective_from': row['effective_from'] if pd.notna(row.get('effective_from')) else date.today(),
            'effective_to': row.get('effective_to') if pd.notna(row.get('effective_to')) else None,
            'vintage': str(row['vintage']).strip()[:10],
            'source_filename': row.get('source_filename', ''),
            'ingest_run_id': batch_id,  # Already UUID type?
            # Metadata fields
            'data_quality_score': metadata.get('quality_score'),
            'validation_results': metadata.get('validation_results'),
            'processing_timestamp': metadata.get('processing_timestamp'),
            'file_checksum': metadata.get('file_checksum'),
            'record_count': metadata.get('record_count', len(df)),
            'schema_version': metadata.get('schema_version'),
            'business_rules_applied': metadata.get('business_rules_applied'),
        }
        records.append(record)
    return records
```

**New Publish Method:**
```python
# Replace lines 568-589 with:
records = self._prepare_zip9_bulk_records(
    zip9_data,
    self.current_batch_id,
    {
        'quality_score': quality_score,
        'validation_results': validation_results,
        'processing_timestamp': processing_timestamp,
        'file_checksum': file_checksum,
        'record_count': len(zip9_data),
        'schema_version': schema_version,
        'business_rules_applied': business_rules_applied
    }
)

try:
    db.bulk_insert_mappings(ZIP9Overrides, records)
    db.commit()
    records_inserted = len(records)
except Exception as e:
    db.rollback()
    logger.error("Failed to publish ZIP9 data", error=str(e))
    raise
```

**Estimated Time:** 30 minutes

#### Step 2.3: Testing

**Test Cases (run via Docker):**
1. Insert 1K valid ZIP9 records → verify all inserted
2. Verify ZIP9 range validation (zip9_low <= zip9_high)
3. Performance test: < 1 second end-to-end

**Estimated Time:** 20 minutes

#### Task 2 Summary

- **Total Estimated Time:** 55 minutes
- **Files Modified:** 1 file (`cms_zip9_ingester.py`)
- **Test Files:** 1 file (verify existing tests still pass)
- **Risk Level:** Very Low (small dataset, simple structure)
- **Expected Improvement:** 1s → 0.3s (3x faster)

---

### Task 3: Locality FIPS Normalizer Optimization

**File:** `cms_pricing/ingestion/normalize/normalize_locality_fips.py`  
**Lines:** 844 (main loop), 713 (fuzzy matching loop)  
**Records:** ~1,000-2,000 rows processed  
**Expected Improvement:** 10s → 3s (3x faster)

#### Step 3.1: Analyze Current Iterrows Usage

**Two Main Loops:**

1. **Main Processing Loop (line 844):**
```python
for _, raw_row in raw_df.iterrows():
    # Complex logic: state inference, county matching, explosion
    # ~1,000 rows input, explodes to ~3,000-4,000 output rows
```

2. **Fuzzy Matching Loop (line 713):**
```python
for _, row in state_counties.iterrows():
    score = fuzz.ratio(county_key, row['county_name_key']) / 100.0
    # Filters to matches >= threshold
```

**Challenge:** These loops do complex business logic, not just data mapping. Can't use simple `bulk_insert_mappings()` here.

**Strategy:** Vectorize where possible, optimize DataFrame operations. If RapidFuzz vector APIs are unavailable in the pinned version, fallback is `itertuples()` + precomputed Series operations to stay faster than `iterrows()`.

**Estimated Time:** 20 minutes

#### Step 3.2: Optimize Fuzzy Matching Loop

**Current (line 713):**
```python
scores = []
for _, row in state_counties.iterrows():
    score = fuzz.ratio(county_key, row['county_name_key']) / 100.0
    if score >= threshold:
        scores.append({...})
```

**Optimized Using RapidFuzz Vectorization:**
```python
# Vectorized fuzzy matching (if RapidFuzz supports it)
from rapidfuzz import process

def match_fuzzy_optimized(
    county_key: str, 
    state_fips: str, 
    counties_df: pd.DataFrame, 
    threshold: float = 0.95
) -> Optional[Tuple[str, str, str, str, float]]:
    """Optimized fuzzy match using vectorized operations."""
    if not FUZZY_AVAILABLE:
        return None
    
    # Filter to state counties once
    state_counties = counties_df[counties_df['state_fips'] == state_fips]
    
    if len(state_counties) == 0:
        return None
    
    # Use RapidFuzz process.extract for vectorized matching
    candidates = state_counties['county_name_key'].tolist()
    matches = process.extract(
        county_key,
        candidates,
        limit=2,  # Only need top 2 for ambiguity check
        scorer=fuzz.ratio
    )
    
    if not matches or matches[0][1] < (threshold * 100):
        return None
    
    # Check for ambiguity (top 2 within 2 points)
    if len(matches) >= 2 and (matches[0][1] - matches[1][1]) < 2:
        logger.warning("Ambiguous fuzzy match (quarantined)", ...)
        return None
    
    # Get best match row
    best_key = matches[0][0]
    best_score = matches[0][1] / 100.0
    best_row = state_counties[state_counties['county_name_key'] == best_key].iloc[0]
    
    return (
        best_row['county_fips'],
        best_row['county_geoid'],
        best_row['county_name_canonical'],
        best_row['county_type'],
        best_score
    )
```

**Expected Improvement:** 5-7 seconds saved on fuzzy matching

**Estimated Time:** 45 minutes

#### Step 3.3: Optimize Main Processing Loop

**Challenge:** Main loop (line 844) does complex state inference and county explosion. Hard to vectorize completely.

**Strategy:** Optimize DataFrame operations within loop, reduce redundant lookups.

**Current Issues:**
- `raw_df.iterrows()` is slow
- Multiple DataFrame filters inside loop
- String operations on each row

**Optimizations:**
1. Use `df.itertuples()` instead of `iterrows()` (2-3x faster)
2. Pre-compute lookups (state_map, counties_by_state) outside loop
3. Batch string operations where possible

**Example Optimization:**
```python
# Pre-compute lookups outside loop
state_map = dict(zip(states_df['state_name'].str.upper(), states_df['state_fips']))
counties_by_state = {
    state: counties_df[counties_df['state_fips'] == state]
    for state in counties_df['state_fips'].unique()
}

# Use itertuples (faster than iterrows)
normalized_rows = []
for raw_row in raw_df.itertuples(index=False):
    # Access fields: raw_row.mac, raw_row.locality_code, etc.
    # Same logic, but faster iteration
    ...
```

**Estimated Time:** 1 hour

#### Step 3.4: Testing

**Test Cases (run via Docker):**
1. Fuzzy matching produces same results as before
2. Main normalization produces same output DataFrame
3. Performance test: < 5 seconds for full normalization

**Estimated Time:** 30 minutes

#### Task 3 Summary

- **Total Estimated Time:** 2 hours 35 minutes
- **Files Modified:** 1 file (`normalize_locality_fips.py`)
- **Test Files:** Verify existing tests pass
- **Risk Level:** Medium (complex business logic, needs careful testing)
- **Expected Improvement:** 10s → 3s (3x faster)

---

## Implementation Order & Schedule

### Recommended Sequence

1. **Task 1: ZIP Locality** (2h 15m) - Highest impact, proven pattern
2. **Task 2: ZIP9** (55m) - Quick win, low risk
3. **Task 3: Locality FIPS** (2h 35m) - Most complex, do last

**Total Estimated Time:** ~5 hours 45 minutes

### Daily Schedule Suggestion

**Day 1 (Morning - 3 hours):**
- Task 1: ZIP Locality optimization
- Test and verify

**Day 1 (Afternoon - 3 hours):**
- Task 2: ZIP9 optimization (55m)
- Task 3: Locality FIPS optimization (start)

**Day 2 (Morning - 2 hours):**
- Task 3: Complete and test
- End-to-end verification

---

## Pre-Implementation Checklist

- [ ] Review RVU ingestor `_bulk_insert_chunked()` implementation
- [ ] Understand SQLAlchemy `bulk_insert_mappings()` API
- [ ] Check if ZIP Locality/ZIP9 tables have unique constraints (affects UPSERT vs INSERT)
- [ ] Review existing tests for all three files
- [ ] Set up performance measurement tools (time.time() or pytest-benchmark)

---

## Success Criteria

### Performance Targets

- **ZIP Locality:** Insert 43K records in < 10 seconds (currently ~30s)
- **ZIP9:** Insert 1K records in < 1 second (currently ~1s)
- **Locality FIPS:** Normalize 1K-2K rows in < 5 seconds (currently ~10s)

### Functional Requirements

- All existing tests pass
- Data integrity maintained (all fields mapped correctly)
- Error handling preserved (graceful degradation on bad rows)
- Logging preserved (same log messages, possibly different counts)

### Code Quality

- Follow existing patterns (match RVU ingestor style)
- Add docstrings to new helper functions
- No new linter errors
- Code review ready

---

## Risk Mitigation

### Risk 1: Data Type Mismatches
**Mitigation:** Add type conversion in `_prepare_bulk_records()` helpers, test with real data

### Risk 2: UUID/String Conversion Issues
**Mitigation:** Verify `ingest_run_id` type (UUID vs string), handle conversion explicitly

### Risk 3: Error Handling Regressions
**Mitigation:** Preserve try/except patterns, add fallback to individual inserts on chunk failure

### Risk 4: Performance Regression (Task 3)
**Mitigation:** Benchmark before/after, keep original code commented for rollback

---

## Post-Implementation Tasks

1. **Performance Benchmarking:** Measure actual time improvements, document results
2. **Documentation:** Update CHANGELOG.md with optimization notes
3. **Code Review:** Submit PR with before/after metrics
4. **Monitoring:** Watch production logs for any error rate changes

---

## Related Files & References

**Reference Implementation:**
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (lines 1414-1421, 1489-1512)

**Models:**
- `cms_pricing/models/nearest_zip.py` (CMSZipLocality, ZIP9Overrides)

**Test Files:**
- `tests/ingestors/test_cms_zip_locality_production_ingester.py`
- `tests/ingestors/test_cms_zip9_ingester.py`
- `tests/normalize/test_locality_fips_normalization.py`

**Documentation:**
- `artifacts/existing_load_patterns.md` (database load patterns reference)
#### Step 1.3: Prepare DataFrame for Bulk Mapping (Vectorized)

**Required Transformations:**
1. Copy and reset index (consistent with RVU vector helpers)
2. Use column-wise helpers (`_string_column`, `_date_column`, `_boolean_column`) to normalize types/lengths
3. Explicitly convert `ingest_run_id` to a UUID object (ZIP models expect UUID columns)
4. Replace pandas null markers (`pd.NA`, `np.nan`) with Python `None`

**Vectorized Mapping Pattern:**
```python
def _prepare_bulk_records(self, df: pd.DataFrame, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    df = df.copy().reset_index(drop=True)

    df['zip5'] = self._string_column(df, 'zip5', max_len=5)
    df['state'] = self._string_column(df, 'state', max_len=2, uppercase=True)
    df['locality'] = self._string_column(df, 'locality', max_len=10)
    df['carrier_mac'] = self._string_column(df, 'carrier_mac', max_len=10)
    df['rural_flag'] = self._boolean_column(df, 'rural_flag')
    df['effective_from'] = self._date_column(df, 'effective_from', fallback='vintage')
    df['effective_to'] = self._date_column(df, 'effective_to')
    df['vintage'] = self._string_column(df, 'vintage', max_len=10)
    df['source_filename'] = self._string_column(
        df,
        'source_filename',
        max_len=120,
        default='zip_code_carrier_locality.zip'
    )

    batch_uuid = uuid.UUID(str(self.current_batch_id))
    df['ingest_run_id'] = batch_uuid
    df['data_quality_score'] = metadata.get('quality_score')
    df['validation_results'] = metadata.get('validation_results')
    df['processing_timestamp'] = metadata.get('processing_timestamp')
    df['file_checksum'] = metadata.get('file_checksum')
    df['record_count'] = metadata.get('record_count', len(df))
    df['schema_version'] = metadata.get('schema_version', self.schema_version)
    df['business_rules_applied'] = metadata.get('business_rules_applied')

    columns = [c.name for c in CMSZipLocality.__table__.columns]
    return self._replace_null_like(df[columns]).to_dict('records')
```
