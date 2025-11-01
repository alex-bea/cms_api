# Quick Check: What's in the Database?

Since the verification script isn't deployed yet, run these SQL queries directly in Render:

## Option 1: Render Database Dashboard

Go to **Render Dashboard → Database → Connect** and run these queries:

```sql
-- Check gpci_indices table (RVU model)
SELECT COUNT(*) as total_rows FROM gpci_indices;
SELECT mac, locality_id, effective_start, COUNT(*) 
FROM gpci_indices 
GROUP BY mac, locality_id, effective_start 
HAVING COUNT(*) > 1;
-- Expected: Should return 0 rows (v1.3 NK uniqueness)

-- Check gpci table (simplified fee schedule)
SELECT COUNT(*) as total_rows FROM gpci;
SELECT 
  COUNT(*) as total,
  COUNT(release_id) as has_release_id,
  COUNT(batch_id) as has_batch_id
FROM gpci;

-- Sample data
SELECT * FROM gpci LIMIT 5;
```

## Option 2: Render Shell (psql)

```bash
# In Render shell, connect to database:
psql $DATABASE_URL

# Then run the queries above
```

## Option 3: Quick Python One-Liner

```bash
# In Render One-Off Job or shell:
python -c "
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex
from cms_pricing.models.fee_schedules import GPCI
db = SessionLocal()
gpci_indices_count = db.query(GPCIIndex).count()
gpci_count = db.query(GPCI).count()
print(f'gpci_indices: {gpci_indices_count} rows')
print(f'gpci (fee schedule): {gpci_count} rows')
db.close()
"
```

---

## What to Look For

### Success Scenario:
- `gpci_indices` has data (100+ rows) ✅
- `gpci` table might be empty initially (that's OK if parquet files exist)

### If `gpci_indices` has data but `gpci` is empty:
- Need to run: `python scripts/load_data.py`
- This loads from parquet → simplified fee schedule tables

### If both are empty:
- Check ingestion logs for errors
- Verify source files were processed

---

Run one of these options and share the results!

