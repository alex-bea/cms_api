# Database Loader Implementation Plan

## Summary
Create a utility class to load DataFrames from the RVU pipeline into Postgres tables.

## Current State
- ✅ RVU pipeline parses real CMS data
- ✅ 7/7 tests passing
- ✅ QTS-compliant logging implemented
- ❌ DataFrames are not yet loaded into Postgres
- ❌ Publish stage returns mock paths

## Database Models Available
From `cms_pricing/models/rvu.py`:
1. **Release** - Release metadata
2. **RVUItem** - PPRRVU data (main RVU table)
3. **GPCIIndex** - GPCI indices
4. **OPPSCap** - OPPS caps
5. **AnesCF** - Anesthesia conversion factors
6. **LocalityCounty** - Locality to county mapping

## Implementation Plan

### Step 1: Create `ParquetToDBLoader` Utility (Priority: HIGH)
**File:** `cms_pricing/ingestion/utils/db_loader.py`

**Responsibilities:**
- Map DataFrame columns to SQLAlchemy model fields
- Handle data type conversions (e.g., string dates → Date objects)
- Use bulk insert for performance
- Track load metadata (record counts, timestamps)
- Return load summary (records inserted, errors)

**API:**
```python
class ParquetToDBLoader:
    def __init__(self, db_session: Session):
        self.db_session = db_session
    
    def load_dataframe_to_model(
        self,
        df: pd.DataFrame,
        model_class: Type[Base],
        mapping: Dict[str, str],  # DataFrame column -> Model field
        release_id: str,
        batch_id: str
    ) -> Dict[str, Any]:
        """Load DataFrame rows into specified model table"""
        
    def load_batch(
        self,
        dataframes: Dict[str, pd.DataFrame],
        models_mapping: Dict[str, Tuple[Type[Base], Dict[str, str]]],
        release_id: str,
        batch_id: str
    ) -> Dict[str, Any]:
        """Load multiple tables at once"""
```

### Step 2: Create Column Mapping Configuration (Priority: HIGH)
**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (or separate config)

**Schema Mappings:**
1. **PPRRVU → RVUItem:**
   ```python
   PPRRVU_MAPPING = {
       'hcpcs_code': 'hcpcs_code',
       'modifier': 'modifier_key',
       'description': 'description',
       'status_code': 'status_code',
       'work_rvu': 'work_rvu',
       'pe_rvu_nonfac': 'pe_rvu_nonfac',
       'pe_rvu_fac': 'pe_rvu_fac',
       'mp_rvu': 'mp_rvu',
       # ... etc
   }
   ```

2. **GPCI → GPCIIndex:**
   ```python
   GPCI_MAPPING = {
       'mac': 'mac',
       'locality_id': 'locality_id',
       'locality_name': 'locality_name',
       'work_gpci': 'work_gpci',
       'pe_gpci': 'pe_gpci',
       'mp_gpci': 'mp_gpci',
   }
   ```

3. **OPPSCap → OPPSCap:**
   ```python
   OPPSCAP_MAPPING = {
       'hcpcs_code': 'hcpcs_code',
       'modifier': 'modifier',
       'mac': 'mac',
       'locality_id': 'locality_id',
       'price_fac': 'price_fac',
       'price_nonfac': 'price_nonfac',
   }
   ```

4. **ANES → AnesCF:**
   ```python
   ANES_MAPPING = {
       'mac': 'mac',
       'locality_id': 'locality_id',
       'locality_name': 'locality_name',
       'anesthesia_cf': 'anesthesia_cf',
   }
   ```

5. **Locality → LocalityCounty:**
   ```python
   LOCALITY_MAPPING = {
       'mac': 'mac',
       'locality_id': 'locality_id',
       'state': 'state',
       'county_name': 'county_name',
       'fee_schedule_area': 'fee_schedule_area',
   }
   ```

### Step 3: Integrate into RVU Ingestor Publish Stage (Priority: HIGH)
**File:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`

**Changes needed:**
1. Import `ParquetToDBLoader`
2. Add `models_mapping` to `RVUIngestor` class
3. Update `publish()` to call loader with parsed DataFrames
4. Return actual load results instead of mock paths

**Pseudo-code:**
```python
def publish(self, enriched_batch: Union[StageFrame, Dict[str, Any]], ...) -> Dict[str, Any]:
    # Extract dataframes
    dataframes = enriched_batch.dataframes if isinstance(enriched_batch, StageFrame) else enriched_batch['enriched_data']
    
    # Create Release record first
    release = Release(
        type='RVU_FULL',
        source_version=self._derive_release_context(filename, release_id)['source_release'],
        imported_at=datetime.now().date(),
        notes=f'RVU batch {batch_id}'
    )
    self.db_session.add(release)
    self.db_session.flush()
    release_id = release.id
    
    # Load each dataset
    loader = ParquetToDBLoader(self.db_session)
    results = {}
    
    if 'pprrvu' in dataframes:
        results['rvu_items'] = loader.load_dataframe_to_model(
            df=dataframes['pprrvu'],
            model_class=RVUItem,
            mapping=PPRRVU_MAPPING,
            release_id=release_id,
            batch_id=batch_id
        )
    
    if 'gpci' in dataframes:
        results['gpci_indices'] = loader.load_dataframe_to_model(...)
    
    # ... etc for oppscap, anes, locality
    
    self.db_session.commit()
    
    return {
        'curated_tables': list(results.keys()),
        'records_inserted': sum(r['records_inserted'] for r in results.values()),
        'release_id': str(release_id)
    }
```

### Step 4: Add Integration Test (Priority: MEDIUM)
**File:** `tests/ingestors/test_rvu_publish_db.py`

**Test cases:**
1. Load sample PPRRVU DataFrame into `rvu_items` table
2. Load GPCI DataFrame into `gpci_indices` table
3. Verify records in database match DataFrame
4. Test bulk insert performance (>1000 rows)
5. Test error handling (missing columns, wrong types)

## Dependencies
- SQLAlchemy session management
- DataFrame column type conversions
- UUID handling for release_id

## Estimated Effort
- Step 1: 2-3 hours
- Step 2: 1 hour
- Step 3: 2-3 hours
- Step 4: 2 hours
**Total: ~8 hours**

## Success Criteria
✅ DataFrames loaded into correct Postgres tables  
✅ All records inserted with proper metadata  
✅ Integration test passes  
✅ Pipeline completes without errors  
✅ Database queries return real data

