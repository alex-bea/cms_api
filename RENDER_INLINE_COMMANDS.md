# Render Shell Commands - Copy & Paste

Copy and paste these commands directly into your Render shell.

## Quick Check (All-in-One)

```python
python3 << 'ENDOFSCRIPT'
from cms_pricing.database import SessionLocal
from sqlalchemy import text
from datetime import datetime, timedelta

print("=" * 60)
print("CMS API - Render Deployment Data Check")
print("=" * 60)
print()

# 1. Test connection
print("1. Testing database connection...")
try:
    from cms_pricing.database import engine
    with engine.connect() as conn:
        result = conn.execute(text('SELECT version();'))
        version = result.fetchone()[0]
        print(f"   ✅ Database connected")
        print(f"   PostgreSQL: {version[:60]}...")
except Exception as e:
    print(f"   ❌ Database connection failed: {e}")
    exit(1)

# 2. Check table counts
print("\n2. Checking table record counts...")
db = SessionLocal()
try:
    tables = {
        'releases': 'SELECT COUNT(*) FROM releases',
        'rvu_items': 'SELECT COUNT(*) FROM rvu_items',
        'gpci_indices': 'SELECT COUNT(*) FROM gpci_indices',
        'opps_caps': 'SELECT COUNT(*) FROM opps_caps',
        'anes_cfs': 'SELECT COUNT(*) FROM anes_cfs',
        'locality_counties': 'SELECT COUNT(*) FROM locality_counties',
        'ingest_runs': 'SELECT COUNT(*) FROM ingest_runs',
    }
    
    print("\n📊 Table Record Counts:")
    print("-" * 50)
    total_records = 0
    for table_name, query in tables.items():
        try:
            result = db.execute(text(query))
            count = result.fetchone()[0] or 0
            total_records += count
            if count > 0:
                print(f"   ✅ {table_name:25} {count:>12,} records")
            else:
                print(f"   ⚠️  {table_name:25} {count:>12,} records (empty)")
        except Exception as e:
            print(f"   ❌ {table_name:25} Error: {str(e)[:50]}")
    
    print("-" * 50)
    print(f"   Total records: {total_records:,}")
    
    if total_records > 0:
        print(f"\n   ✅ Data is present!")
    else:
        print(f"\n   ❌ No data found")
        
except Exception as e:
    print(f"   ❌ Error: {e}")
finally:
    db.close()

# 3. Check recent releases
print("\n3. Checking recent releases...")
db = SessionLocal()
try:
    result = db.execute(text("""
        SELECT release_id, created_at 
        FROM releases 
        ORDER BY created_at DESC 
        LIMIT 5
    """))
    
    releases = result.fetchall()
    if releases:
        print(f"\n   📦 Recent Releases (last 5):")
        print("-" * 70)
        for release in releases:
            print(f"   {release[0] or 'N/A':40} | Created: {release[1]}")
        print("-" * 70)
    else:
        print("\n   ⚠️  No releases found")
except Exception as e:
    print(f"   ❌ Error: {e}")
finally:
    db.close()

# 4. Check ingestion runs
print("\n4. Checking ingestion runs...")
db = SessionLocal()
try:
    result = db.execute(text("""
        SELECT 
            release_id, 
            status,
            input_record_count,
            output_record_count,
            quality_score,
            created_at
        FROM ingest_runs 
        ORDER BY created_at DESC 
        LIMIT 5
    """))
    
    runs = result.fetchall()
    if runs:
        print(f"\n   🔄 Recent Ingestion Runs (last 5):")
        print("-" * 100)
        for run in runs:
            release_id = (run[0] or "N/A")[:30]
            status = run[1] or "unknown"
            input_count = run[2] or 0
            output_count = run[3] or 0
            quality = run[4] or 0.0
            created = run[5]
            
            status_icon = "✅" if status == "success" else "⚠️" if status == "partial" else "❌"
            print(f"   {status_icon} {release_id:30} | Status: {status:10} | In: {input_count:>8,} | Out: {output_count:>8,} | Quality: {quality:.2f}")
            print(f"      Created: {created}")
        print("-" * 100)
    else:
        print("\n   ⚠️  No ingestion runs found")
except Exception as e:
    print(f"   ❌ Error: {e}")
finally:
    db.close()

# 5. Sample data
print("\n5. Checking sample data...")
db = SessionLocal()
try:
    print("\n   📄 Sample Data:")
    print("-" * 70)
    
    # RVU Items
    try:
        result = db.execute(text("SELECT hcpcs_code, modifier, work_rvu FROM rvu_items LIMIT 3"))
        rvu_samples = result.fetchall()
        if rvu_samples:
            print(f"\n   RVU Items:")
            for r in rvu_samples:
                print(f"      HCPCS: {r[0]}, Modifier: {r[1] or 'N/A'}, Work RVU: {r[2]}")
        else:
            print(f"\n   ⚠️  No RVU items found")
    except Exception as e:
        print(f"\n   ⚠️  Error querying RVU: {e}")
    
    # GPCI Indices
    try:
        result = db.execute(text("SELECT mac, locality_id, work_gpci FROM gpci_indices LIMIT 3"))
        gpci_samples = result.fetchall()
        if gpci_samples:
            print(f"\n   GPCI Indices:")
            for r in gpci_samples:
                print(f"      MAC: {r[0]}, Locality: {r[1]}, Work GPCI: {r[2]}")
        else:
            print(f"\n   ⚠️  No GPCI indices found")
    except Exception as e:
        print(f"\n   ⚠️  Error querying GPCI: {e}")
    
    # OPPS Caps
    try:
        result = db.execute(text("SELECT hcpcs_code, modifier, price_fac FROM opps_caps LIMIT 3"))
        opps_samples = result.fetchall()
        if opps_samples:
            print(f"\n   OPPS Caps:")
            for r in opps_samples:
                print(f"      HCPCS: {r[0]}, Modifier: {r[1] or 'N/A'}, Price: {r[2]}")
        else:
            print(f"\n   ⚠️  No OPPS caps found")
    except Exception as e:
        print(f"\n   ⚠️  Error querying OPPS: {e}")
        
    print("-" * 70)
        
except Exception as e:
    print(f"   ❌ Error: {e}")
finally:
    db.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
ENDOFSCRIPT
```

## Quick One-Liner Checks

### Check if any data exists:
```python
python3 -c "from cms_pricing.database import SessionLocal; from sqlalchemy import text; db = SessionLocal(); print('RVU Items:', db.execute(text('SELECT COUNT(*) FROM rvu_items')).fetchone()[0]); print('GPCI Indices:', db.execute(text('SELECT COUNT(*) FROM gpci_indices')).fetchone()[0]); print('OPPS Caps:', db.execute(text('SELECT COUNT(*) FROM opps_caps')).fetchone()[0]); db.close()"
```

### Check recent releases:
```python
python3 -c "from cms_pricing.database import SessionLocal; from sqlalchemy import text; db = SessionLocal(); result = db.execute(text('SELECT release_id, created_at FROM releases ORDER BY created_at DESC LIMIT 3')); [print(f'{r[0]} | {r[1]}') for r in result.fetchall()]; db.close()"
```

### Check ingestion runs:
```python
python3 -c "from cms_pricing.database import SessionLocal; from sqlalchemy import text; db = SessionLocal(); result = db.execute(text('SELECT release_id, status, output_record_count, created_at FROM ingest_runs ORDER BY created_at DESC LIMIT 3')); [print(f'{r[0]} | {r[1]} | {r[2]:,} records | {r[3]}') for r in result.fetchall()]; db.close()"
```

