# Issue 1: Verification SQL Queries

After running `python scripts/load_rvu_to_production.py`, verify GPCI data with these queries:

## Check gpci_indices for NULL values

```sql
-- Count rows with NULL GPCI values
SELECT 
    COUNT(*) as total_rows,
    COUNT(work_gpci) as has_work_gpci,
    COUNT(pe_gpci) as has_pe_gpci,
    COUNT(mp_gpci) as has_mp_gpci,
    COUNT(*) - COUNT(work_gpci) as null_work_gpci,
    COUNT(*) - COUNT(pe_gpci) as null_pe_gpci,
    COUNT(*) - COUNT(mp_gpci) as null_mp_gpci
FROM gpci_indices;
```

**Expected:** All NULL counts should be 0

## Sample data check

```sql
-- View sample rows to confirm values are populated
SELECT 
    mac,
    locality_id,
    effective_start,
    work_gpci,
    pe_gpci,
    mp_gpci
FROM gpci_indices
ORDER BY locality_id
LIMIT 10;
```

**Expected:** All GPCI columns should have numeric values (not NULL)

## Python verification

```python
python << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
with db.connection() as conn:
    result = conn.execute(text("""
        SELECT 
            COUNT(*) as total,
            COUNT(work_gpci) as has_work,
            COUNT(pe_gpci) as has_pe,
            COUNT(mp_gpci) as has_mp,
            COUNT(*) - COUNT(work_gpci) as null_work,
            COUNT(*) - COUNT(pe_gpci) as null_pe,
            COUNT(*) - COUNT(mp_gpci) as null_mp
        FROM gpci_indices
    """))
    row = result.fetchone()
    print(f"Total rows: {row[0]}")
    print(f"Has work_gpci: {row[1]}")
    print(f"Has pe_gpci: {row[2]}")
    print(f"Has mp_gpci: {row[3]}")
    print(f"NULL work_gpci: {row[4]}")
    print(f"NULL pe_gpci: {row[5]}")
    print(f"NULL mp_gpci: {row[6]}")
    
    if row[4] == 0 and row[5] == 0 and row[6] == 0:
        print("\n✅ SUCCESS: No NULL values in gpci_indices!")
    else:
        print("\n❌ FAIL: Some NULL values still exist")
db.close()
EOF
```

