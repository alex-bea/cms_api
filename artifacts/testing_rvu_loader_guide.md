# Testing the RVU Loader After Phase 2 Refactor

**Status:** ✅ Complete  
**Last Updated:** 2025-11-03  
**Phase 2 Context:** Loader extracted from ingestor to `rvu_loaders.py`

---

## 📋 Overview

The RVU loader was refactored in Phase 2 to be a separate module (`cms_pricing/ingestion/datasets/rvu_loaders.py`) that's passed as a callback to the shared `execute_publish` stage. This guide shows how to test it.

---

## 🧪 Test Types

### 1. Unit Tests (Loader Functions)

**File:** `tests/ingestors/test_rvu_loader_aliases.py`

**What it tests:**
- Individual loader functions (`load_pprrvu_data`, `load_gpci_data`, etc.)
- Column mapping/aliasing
- Data transformations
- Uses mocked database sessions (fast)

**Run:**
```bash
# In Docker (recommended - avoids Signal 11)
docker compose exec api pytest tests/ingestors/test_rvu_loader_aliases.py -xvs

# Or locally (if pandas/pyarrow work)
pytest tests/ingestors/test_rvu_loader_aliases.py -xvs
```

**Example test:**
```python
def test_gpci_loader_aliases():
    session = _make_session()  # Mock DB session
    df = pd.DataFrame([{
        "mac": "01112",
        "state": "ca",
        "locality_code": "05",
        "gpci_work": 1.088,
        "gpci_pe": 1.419,
        "gpci_mp": 0.445,
    }])
    
    inserted = load_gpci_data(df, uuid.uuid4(), batch_id="test", db_session=session)
    assert inserted == 1
    records = _extract_records(session)
    assert records[0]["work_gpci"] == 1.088  # Column alias mapping
```

**Tests:**
- ✅ `test_gpci_loader_aliases()` - GPCI column mapping
- ✅ `test_oppscap_loader_aliases()` - OPPS cap column mapping
- ✅ `test_locality_loader_aliases()` - Locality column mapping
- ✅ `test_pprrvu_loader_aliases()` - PPRRVU column mapping (if exists)

---

### 2. Integration Tests (Publish Stage)

**File:** `tests/ingestors/test_rvu_ingestor_e2e.py`

**What it tests:**
- Loader integration with `execute_publish` stage
- Full publish stage workflow
- Database loading via `loader_func` callback
- Uses real test database

**Run:**
```bash
# Test publish stage (includes loader)
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_dis_publish_stage \
  -xvs

# Test with database session
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_dis_publish_stage \
  --database-url="postgresql://..." \
  -xvs
```

**What it verifies:**
- `execute_publish` calls `loader_func` when provided
- Loader receives correct parameters (dataframes, release_id, etc.)
- Database tables get populated
- Publish result includes `database_load_results`

---

### 3. End-to-End Tests (Full Pipeline)

**File:** `tests/ingestors/test_rvu_ingestor_e2e.py`

**What it tests:**
- Complete ingestion pipeline
- All stages including publish → database loading
- Real data flow from files → DB

**Run:**
```bash
# Full pipeline test (includes database loading)
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_full_dis_pipeline \
  -xvs

# With specific test DB
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_full_dis_pipeline \
  --database-url="postgresql://cms_user:cms_password@db:5432/cms_pricing_test" \
  -xvs
```

**What it verifies:**
- Land → Validate → Normalize → Enrich → Publish
- Publish stage loads data to database
- All 5 datasets loaded (PPRRVU, GPCI, OPPSCap, ANES, Locality)
- Database tables contain correct data

---

## 🔍 Manual Testing

### Quick Manual Test Script

Create a test script to verify the loader:

```python
#!/usr/bin/env python3
"""Quick manual test for RVU loader"""
import asyncio
import sys
from pathlib import Path
import pandas as pd
import uuid
from datetime import datetime

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from cms_pricing.database import SessionLocal
from cms_pricing.ingestion.datasets.rvu_loaders import load_rvu_dataframes
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex

async def test_loader_manually():
    """Manually test the loader with sample data"""
    db = SessionLocal()
    
    try:
        # Create test dataframes
        pprrvu_df = pd.DataFrame([{
            "hcpcs": "99213",
            "modifier": None,
            "status_code": "A",
            "work_rvu": 1.50,
            "pe_rvu_nonfac": 0.75,
            "pe_rvu_fac": 0.50,
            "mp_rvu": 0.10,
            "effective_from": "2025-01-01",
        }])
        
        gpci_df = pd.DataFrame([{
            "mac": "01112",
            "state": "CA",
            "locality_code": "05",
            "gpci_work": 1.088,
            "gpci_pe": 1.419,
            "gpci_mp": 0.445,
        }])
        
        dataframes = {
            "pprrvu": pprrvu_df,
            "gpci": gpci_df,
        }
        
        # Test loader
        release_id = str(uuid.uuid4())
        batch_id = str(uuid.uuid4())
        vintage_date = "2025-01-01"
        
        print(f"🚀 Testing loader with release_id={release_id}")
        
        result = load_rvu_dataframes(
            dataframes=dataframes,
            release_id=release_id,
            batch_id=batch_id,
            vintage_date=vintage_date,
            db_session=db,
        )
        
        print(f"✅ Loader completed:")
        print(f"   Total records: {result.get('total_records', 0)}")
        print(f"   Tables loaded: {result.get('tables_loaded', [])}")
        
        # Verify database
        rvu_count = db.query(RVUItem).filter(RVUItem.release_id == release_id).count()
        gpci_count = db.query(GPCIIndex).filter(GPCIIndex.release_id == release_id).count()
        
        print(f"   RVU items in DB: {rvu_count}")
        print(f"   GPCI indices in DB: {gpci_count}")
        
        assert rvu_count > 0, "No RVU items loaded"
        assert gpci_count > 0, "No GPCI indices loaded"
        
        print("✅ All assertions passed!")
        
    finally:
        # Cleanup
        if 'release_id' in locals():
            db.query(RVUItem).filter(RVUItem.release_id == release_id).delete()
            db.query(GPCIIndex).filter(GPCIIndex.release_id == release_id).delete()
            db.query(Release).filter(Release.id == release_id).delete()
            db.commit()
        db.close()

if __name__ == "__main__":
    asyncio.run(test_loader_manually())
```

**Run:**
```bash
# In Docker
docker compose exec api python tests/ingestors/scripts/test_loader_manual.py

# Or with PYTHONPATH
PYTHONPATH=/app docker compose exec api python tests/ingestors/scripts/test_loader_manual.py
```

---

## 🎯 Test Database Setup

### Option 1: Use Test Database (Recommended)

```bash
# Start test database
docker compose up -d db

# Bootstrap schema
docker compose exec api python tests/scripts/bootstrap_test_db.py \
  --database-url="postgresql://cms_user:cms_password@db:5432/cms_pricing_test"

# Run tests
docker compose exec api pytest tests/ingestors/test_rvu_loader_aliases.py \
  --database-url="postgresql://cms_user:cms_password@db:5432/cms_pricing_test"
```

### Option 2: Use In-Memory SQLite (Fast, Limited)

For unit tests that don't need Postgres-specific features:

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("sqlite:///:memory:")
Session = sessionmaker(bind=engine)
db = Session()
```

**Note:** Some features (JSONB, ARRAY types) won't work with SQLite.

---

## ✅ Verification Checklist

After running tests, verify:

- [ ] **Unit tests pass** - Loader functions work correctly
- [ ] **Integration tests pass** - Loader integrates with publish stage
- [ ] **E2E tests pass** - Full pipeline loads data to database
- [ ] **Database records created** - Check tables have data
- [ ] **Column mappings correct** - Verify DataFrame columns → DB columns
- [ ] **Error handling works** - Invalid data handled gracefully
- [ ] **Batching works** - Large datasets load in batches
- [ ] **Transactions work** - Rollback on failure

---

## 🐛 Debugging Tips

### Check if loader is called:

Add logging to `rvu_loaders.py`:
```python
logger.info("load_rvu_dataframes called",
            release_id=release_id,
            batch_id=batch_id,
            dataframe_keys=list(dataframes.keys()))
```

### Verify database connection:

```python
from cms_pricing.database import SessionLocal
db = SessionLocal()
result = db.execute("SELECT 1").scalar()
assert result == 1
print("✅ Database connection works")
```

### Check what's in database:

```python
from cms_pricing.models.rvu import RVUItem, Release
from cms_pricing.database import SessionLocal

db = SessionLocal()
releases = db.query(Release).all()
print(f"Releases: {[r.id for r in releases]}")

rvu_items = db.query(RVUItem).limit(5).all()
print(f"Sample RVU items: {len(rvu_items)}")
```

---

## 📚 Related Documentation

- **Loader Implementation:** `cms_pricing/ingestion/datasets/rvu_loaders.py`
- **Publish Stage:** `cms_pricing/ingestion/stages/publish.py`
- **Ingestor Integration:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (lines 888-944)
- **Database Models:** `cms_pricing/models/rvu.py`
- **Phase 2 Plan:** `artifacts/phase2_completion_plan.md`

---

## 🚀 Quick Commands Reference

```bash
# Run all loader tests
docker compose exec api pytest tests/ingestors/test_rvu_loader_aliases.py -xvs

# Run publish stage test (includes loader)
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_dis_publish_stage \
  -xvs

# Run full pipeline (end-to-end)
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_full_dis_pipeline \
  -xvs

# Check database records
docker compose exec api python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import RVUItem
db = SessionLocal()
print(f'RVU items: {db.query(RVUItem).count()}')
"
```

---

**Last Updated:** 2025-11-03  
**Phase 2 Status:** ✅ Loader extracted and tested
