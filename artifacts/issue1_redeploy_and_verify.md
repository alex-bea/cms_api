# Issue 1: Redeploy and Verify GPCI Loading

## ✅ Completed
- **Commit**: `537f1b2` - Added column alias normalization for GPCI data
- **Pushed**: Changes are now on `origin/main`
- **Render**: Will auto-deploy via GitHub Actions (check Render dashboard)

## Next Steps

### 1. Wait for Render Deployment (2-3 minutes)
Check Render dashboard or wait for GitHub Actions to complete:
- Visit: https://dashboard.render.com
- Check deployment status
- Or wait for GitHub Actions workflow to finish

### 2. Run Ingestion in Render Shell

Once deployment completes, open Render shell and run:

```bash
python scripts/load_rvu_to_production.py
```

**Expected output:**
- Ingestion logs showing GPCI parsing
- Success message with record counts

### 3. Verify NULL Values (Required Check)

Run this SQL query in Render shell:

```python
python << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
with db.connection() as conn:
    result = conn.execute(text("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(work_gpci) as has_work_gpci,
            COUNT(pe_gpci) as has_pe_gpci,
            COUNT(mp_gpci) as has_mp_gpci,
            COUNT(*) - COUNT(work_gpci) as null_work_gpci,
            COUNT(*) - COUNT(pe_gpci) as null_pe_gpci,
            COUNT(*) - COUNT(mp_gpci) as null_mp_gpci
        FROM gpci_indices
    """))
    row = result.fetchone()
    print("=" * 60)
    print("GPCI Indices NULL Check")
    print("=" * 60)
    print(f"Total rows: {row[0]}")
    print(f"Has work_gpci: {row[1]}")
    print(f"Has pe_gpci: {row[2]}")
    print(f"Has mp_gpci: {row[3]}")
    print(f"\nNULL work_gpci: {row[4]}")
    print(f"NULL pe_gpci: {row[5]}")
    print(f"NULL mp_gpci: {row[6]}")
    print("=" * 60)
    
    if row[4] == 0 and row[5] == 0 and row[6] == 0:
        print("\n✅ SUCCESS: No NULL values in gpci_indices!")
        print("   All GPCI columns are populated.")
    else:
        print(f"\n❌ FAIL: Found NULL values")
        print(f"   - work_gpci: {row[4]} NULLs")
        print(f"   - pe_gpci: {row[5]} NULLs")
        print(f"   - mp_gpci: {row[6]} NULLs")
db.close()
EOF
```

**Expected Result:**
```
NULL work_gpci: 0
NULL pe_gpci: 0
NULL mp_gpci: 0

✅ SUCCESS: No NULL values in gpci_indices!
```

### 4. Alternative: Direct SQL Query

If you prefer SQL directly:

```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(work_gpci) as null_work_gpci,
    COUNT(*) - COUNT(pe_gpci) as null_pe_gpci,
    COUNT(*) - COUNT(mp_gpci) as null_mp_gpci
FROM gpci_indices;
```

**Expected:** All NULL counts should be 0

## What Changed

The fix adds column alias normalization in `_load_gpci_data()`:
- Maps `gpci_work` → `work_gpci`
- Maps `gpci_pe` → `pe_gpci`
- Maps `gpci_mp` → `mp_gpci`

This ensures that regardless of which column names the parser outputs, the downstream logic always finds the expected columns.

## Troubleshooting

If NULL values still exist:
1. Check ingestion logs for GPCI parsing activity:
   ```bash
   python scripts/load_rvu_to_production.py 2>&1 | grep -i gpci
   ```

2. Verify GPCI files exist in downloaded ZIPs:
   ```bash
   find data/ingestion -name "*.zip" -exec unzip -l {} \; | grep -i gpci
   ```

3. Check if GPCI parser was invoked:
   ```bash
   python scripts/load_rvu_to_production.py 2>&1 | grep -E "invoking_parser|GPCI"
   ```

