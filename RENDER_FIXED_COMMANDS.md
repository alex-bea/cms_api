# Fixed Render Shell Commands

Copy and paste this corrected version into your Render shell:

```python
python3 << 'ENDOFSCRIPT'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

print("=" * 60)
print("Checking Render Deployment Data")
print("=" * 60)

# Check table counts
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
    for table_name, query in tables.items():
        try:
            result = db.execute(text(query))
            count = result.fetchone()[0] or 0
            if count > 0:
                print(f"✅ {table_name:25} {count:>12,} records")
            else:
                print(f"⚠️  {table_name:25} {count:>12,} records (empty)")
        except Exception as e:
            print(f"❌ {table_name:25} Error: {str(e)[:50]}")
    
    # Check recent releases (using correct column names)
    print("\n📦 Recent Releases:")
    print("-" * 70)
    try:
        result = db.execute(text("""
            SELECT id, type, source_version, imported_at 
            FROM releases 
            ORDER BY imported_at DESC 
            LIMIT 5
        """))
        releases = result.fetchall()
        if releases:
            for r in releases:
                print(f"   {str(r[0])[:36]:36} | Type: {r[1]:15} | Version: {r[2]:10} | Imported: {r[3]}")
        else:
            print("   ⚠️  No releases found")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    print("-" * 70)
    
    # Check ingestion runs (using correct column names)
    print("\n🔄 Recent Ingestion Runs:")
    print("-" * 100)
    try:
        result = db.execute(text("""
            SELECT 
                release_id, 
                status,
                input_record_count,
                output_record_count,
                quality_score,
                started_at
            FROM ingest_runs 
            ORDER BY started_at DESC 
            LIMIT 5
        """))
        runs = result.fetchall()
        if runs:
            for r in runs:
                release_id = (r[0] or "N/A")[:30]
                status = r[1] or "unknown"
                input_count = r[2] or 0
                output_count = r[3] or 0
                quality = float(r[4]) if r[4] else 0.0
                started = r[5]
                
                icon = "✅" if status == "completed" else "⚠️" if status == "started" else "❌"
                print(f"   {icon} {release_id:30} | Status: {status:15} | In: {input_count:>8,} | Out: {output_count:>8,} | Quality: {quality:.2f}")
                print(f"      Started: {started}")
        else:
            print("   ⚠️  No ingestion runs found")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    print("-" * 100)
    
    # Sample data
    print("\n📄 Sample Data:")
    print("-" * 70)
    
    # RVU Items
    try:
        result = db.execute(text("SELECT hcpcs_code, modifier_key, work_rvu FROM rvu_items LIMIT 3"))
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
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    rvu_count = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0] or 0
    gpci_count = db.execute(text("SELECT COUNT(*) FROM gpci_indices")).fetchone()[0] or 0
    opps_count = db.execute(text("SELECT COUNT(*) FROM opps_caps")).fetchone()[0] or 0
    
    if rvu_count > 0 or gpci_count > 0 or opps_count > 0:
        print(f"\n✅ Deployment has real data!")
        print(f"   - RVU Items: {rvu_count:,}")
        print(f"   - GPCI Indices: {gpci_count:,}")
        print(f"   - OPPS Caps: {opps_count:,}")
    else:
        print("\n❌ No data found in main tables")
        
finally:
    db.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
ENDOFSCRIPT
```

