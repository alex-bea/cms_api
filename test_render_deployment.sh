#!/bin/bash
# Render Shell Commands to Test Data Upload
# Run these commands in Render shell to verify deployment has real data

echo "=========================================="
echo "CMS API - Render Deployment Data Check"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Check database connection
echo "1. Testing database connection..."
python3 -c "
from cms_pricing.database import engine
from sqlalchemy import text
try:
    with engine.connect() as conn:
        result = conn.execute(text('SELECT version();'))
        version = result.fetchone()[0]
        print(f'${GREEN}✅ Database connected${NC}')
        print(f'   PostgreSQL version: {version[:50]}...')
except Exception as e:
    print(f'${RED}❌ Database connection failed: {e}${NC}')
    exit(1)
"
echo ""

# 2. Check table record counts
echo "2. Checking table record counts..."
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty

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
            count = result.fetchone()[0]
            if count > 0:
                print(f"✅ {table_name:25} {count:>12,} records")
            else:
                print(f"⚠️  {table_name:25} {count:>12,} records (empty)")
        except Exception as e:
            print(f"❌ {table_name:25} Error: {str(e)[:50]}")
    
    print("-" * 50)
    
    # Check total data
    total_rvu = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
    total_gpci = db.execute(text("SELECT COUNT(*) FROM gpci_indices")).fetchone()[0]
    total_opps = db.execute(text("SELECT COUNT(*) FROM opps_caps")).fetchone()[0]
    
    if total_rvu > 0 or total_gpci > 0 or total_opps > 0:
        print(f"\n✅ Data is present! Total records: {total_rvu + total_gpci + total_opps:,}")
    else:
        print(f"\n❌ No data found in main tables")
        
finally:
    db.close()
EOF
echo ""

# 3. Check recent releases
echo "3. Checking recent releases..."
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text
from datetime import datetime, timedelta

db = SessionLocal()
try:
    result = db.execute(text("""
        SELECT release_id, created_at, metadata 
        FROM releases 
        ORDER BY created_at DESC 
        LIMIT 5
    """))
    
    releases = result.fetchall()
    
    if releases:
        print(f"\n📦 Recent Releases (last 5):")
        print("-" * 70)
        for release in releases:
            release_id = release[0]
            created_at = release[1]
            metadata = release[2] if release[2] else {}
            print(f"   {release_id:30} | Created: {created_at}")
            if metadata:
                print(f"   {'Metadata: ' + str(metadata)[:100]}")
        print("-" * 70)
    else:
        print("\n⚠️  No releases found in database")
        
finally:
    db.close()
EOF
echo ""

# 4. Check ingestion runs
echo "4. Checking ingestion runs..."
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text
from datetime import datetime, timedelta

db = SessionLocal()
try:
    result = db.execute(text("""
        SELECT 
            release_id, 
            batch_id,
            status,
            input_record_count,
            output_record_count,
            rejected_record_count,
            quality_score,
            created_at
        FROM ingest_runs 
        ORDER BY created_at DESC 
        LIMIT 5
    """))
    
    runs = result.fetchall()
    
    if runs:
        print(f"\n🔄 Recent Ingestion Runs (last 5):")
        print("-" * 100)
        for run in runs:
            release_id = run[0] or "N/A"
            batch_id = run[1] or "N/A"
            status = run[2] or "unknown"
            input_count = run[3] or 0
            output_count = run[4] or 0
            rejected_count = run[5] or 0
            quality = run[6] or 0.0
            created = run[7]
            
            status_icon = "✅" if status == "success" else "⚠️" if status == "partial" else "❌"
            print(f"   {status_icon} {release_id[:30]:30} | Status: {status:10} | In: {input_count:>8,} | Out: {output_count:>8,} | Quality: {quality:.2f}")
            print(f"      Batch: {batch_id[:40]} | Created: {created}")
        print("-" * 100)
        
        # Summary stats
        stats_result = db.execute(text("""
            SELECT 
                COUNT(*) as total_runs,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                SUM(output_record_count) as total_output_records
            FROM ingest_runs
            WHERE created_at >= NOW() - INTERVAL '7 days'
        """))
        
        stats = stats_result.fetchone()
        if stats and stats[0] > 0:
            print(f"\n📈 Last 7 days: {stats[0]} runs ({stats[1]} successful, {stats[2]} failed)")
            print(f"   Total records ingested: {stats[3] or 0:,}")
    else:
        print("\n⚠️  No ingestion runs found in database")
        
finally:
    db.close()
EOF
echo ""

# 5. Sample data from each table
echo "5. Checking sample data..."
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    print("\n📄 Sample Data from Main Tables:")
    print("-" * 70)
    
    # RVU Items
    result = db.execute(text("SELECT hcpcs_code, modifier, work_rvu, facility_rvu FROM rvu_items LIMIT 3"))
    rvu_samples = result.fetchall()
    if rvu_samples:
        print(f"\n   RVU Items (sample):")
        for r in rvu_samples:
            print(f"      HCPCS: {r[0]}, Modifier: {r[1]}, Work RVU: {r[2]}, Facility RVU: {r[3]}")
    else:
        print(f"\n   ⚠️  No RVU items found")
    
    # GPCI Indices
    result = db.execute(text("SELECT mac, locality_id, locality_name, work_gpci FROM gpci_indices LIMIT 3"))
    gpci_samples = result.fetchall()
    if gpci_samples:
        print(f"\n   GPCI Indices (sample):")
        for r in gpci_samples:
            print(f"      MAC: {r[0]}, Locality: {r[1]}, Name: {r[2][:30] if r[2] else 'N/A'}, Work GPCI: {r[3]}")
    else:
        print(f"\n   ⚠️  No GPCI indices found")
    
    # OPPS Caps
    result = db.execute(text("SELECT hcpcs_code, modifier, mac, locality_id, price_fac FROM opps_caps LIMIT 3"))
    opps_samples = result.fetchall()
    if opps_samples:
        print(f"\n   OPPS Caps (sample):")
        for r in opps_samples:
            print(f"      HCPCS: {r[0]}, Modifier: {r[1]}, MAC: {r[2]}, Locality: {r[3]}, Price: {r[4]}")
    else:
        print(f"\n   ⚠️  No OPPS caps found")
        
    print("-" * 70)
        
finally:
    db.close()
EOF
echo ""

# 6. Test API health endpoint (if API is running)
echo "6. Testing API health endpoint..."
API_URL="${RENDER_SERVICE_URL:-http://localhost:8000}"
echo "   Testing: $API_URL/health"

curl -s -f "$API_URL/health" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "   ✅ API is responding"
    curl -s "$API_URL/health" | python3 -m json.tool 2>/dev/null || echo "   (Response received but not JSON)"
else
    echo "   ⚠️  API not responding (may not be running or URL incorrect)"
    echo "   Set RENDER_SERVICE_URL environment variable if needed"
fi
echo ""

# 7. Check for data freshness
echo "7. Checking data freshness..."
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text
from datetime import datetime

db = SessionLocal()
try:
    # Check latest release date
    result = db.execute(text("""
        SELECT MAX(created_at) as latest_release
        FROM releases
    """))
    
    latest_release = result.fetchone()[0]
    
    if latest_release:
        print(f"\n📅 Latest Release Date: {latest_release}")
        
        # Check if data is recent (within last 7 days)
        from datetime import timedelta
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        
        if latest_release and latest_release.replace(tzinfo=None) >= seven_days_ago:
            print("   ✅ Data is recent (within last 7 days)")
        else:
            print("   ⚠️  Data is older than 7 days")
    else:
        print("\n   ⚠️  No release dates found")
        
    # Check latest ingestion run
    result = db.execute(text("""
        SELECT MAX(created_at) as latest_ingestion
        FROM ingest_runs
    """))
    
    latest_ingestion = result.fetchone()[0]
    if latest_ingestion:
        print(f"📅 Latest Ingestion Run: {latest_ingestion}")
        
finally:
    db.close()
EOF
echo ""

# Summary
echo "=========================================="
echo "Summary"
echo "=========================================="
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    # Overall health check
    checks = {
        'Database Connection': False,
        'Releases Table': False,
        'RVU Data': False,
        'GPCI Data': False,
        'OPPS Data': False,
        'Recent Ingestion': False,
    }
    
    # Test connection
    try:
        db.execute(text("SELECT 1"))
        checks['Database Connection'] = True
    except:
        pass
    
    # Check tables
    try:
        release_count = db.execute(text("SELECT COUNT(*) FROM releases")).fetchone()[0]
        checks['Releases Table'] = release_count > 0
    except:
        pass
    
    try:
        rvu_count = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
        checks['RVU Data'] = rvu_count > 0
    except:
        pass
    
    try:
        gpci_count = db.execute(text("SELECT COUNT(*) FROM gpci_indices")).fetchone()[0]
        checks['GPCI Data'] = gpci_count > 0
    except:
        pass
    
    try:
        opps_count = db.execute(text("SELECT COUNT(*) FROM opps_caps")).fetchone()[0]
        checks['OPPS Data'] = opps_count > 0
    except:
        pass
    
    try:
        from datetime import datetime, timedelta
        recent = db.execute(text("""
            SELECT COUNT(*) FROM ingest_runs 
            WHERE created_at >= NOW() - INTERVAL '7 days'
        """)).fetchone()[0]
        checks['Recent Ingestion'] = recent > 0
    except:
        pass
    
    print("\n")
    for check, status in checks.items():
        icon = "✅" if status else "❌"
        print(f"   {icon} {check}")
    
    all_good = all(checks.values())
    if all_good:
        print("\n🎉 All checks passed! Deployment appears healthy.")
    else:
        print("\n⚠️  Some checks failed. Review the output above.")
        
finally:
    db.close()
EOF

echo ""
echo "=========================================="
echo "Done!"
echo "=========================================="

