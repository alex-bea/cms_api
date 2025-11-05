#!/usr/bin/env python3
"""
Quick script to check if Render deployment has real data uploaded.
Run this in Render shell: python3 check_render_data.py
"""

import sys
from cms_pricing.database import SessionLocal, engine
from sqlalchemy import text
from datetime import datetime, timedelta

def check_database_connection():
    """Test database connection"""
    print("1. Testing database connection...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SELECT version();'))
            version = result.fetchone()[0]
            print(f"   ✅ Database connected")
            print(f"   PostgreSQL: {version[:60]}...")
            return True
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        return False

def check_table_counts():
    """Check record counts in all tables"""
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
        print(f"   Total records across all tables: {total_records:,}")
        
        if total_records > 0:
            print(f"\n   ✅ Data is present!")
        else:
            print(f"\n   ❌ No data found in any tables")
            
        return total_records > 0
        
    finally:
        db.close()

def check_recent_releases():
    """Check recent releases"""
    print("\n3. Checking recent releases...")
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
            print(f"\n   📦 Recent Releases (last 5):")
            print("-" * 70)
            for release in releases:
                release_id = release[0] or "N/A"
                created_at = release[1]
                print(f"   {release_id:40} | Created: {created_at}")
            print("-" * 70)
            return True
        else:
            print("\n   ⚠️  No releases found in database")
            return False
            
    finally:
        db.close()

def check_ingestion_runs():
    """Check recent ingestion runs"""
    print("\n4. Checking ingestion runs...")
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
            print(f"\n   🔄 Recent Ingestion Runs (last 5):")
            print("-" * 100)
            for run in runs:
                release_id = (run[0] or "N/A")[:30]
                status = run[2] or "unknown"
                input_count = run[3] or 0
                output_count = run[4] or 0
                quality = run[6] or 0.0
                created = run[7]
                
                status_icon = "✅" if status == "success" else "⚠️" if status == "partial" else "❌"
                print(f"   {status_icon} {release_id:30} | Status: {status:10} | In: {input_count:>8,} | Out: {output_count:>8,} | Quality: {quality:.2f}")
                print(f"      Created: {created}")
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
                print(f"\n   📈 Last 7 days: {stats[0]} runs ({stats[1]} successful, {stats[2]} failed)")
                print(f"      Total records ingested: {stats[3] or 0:,}")
            
            return True
        else:
            print("\n   ⚠️  No ingestion runs found in database")
            return False
            
    finally:
        db.close()

def check_sample_data():
    """Check sample data from each table"""
    print("\n5. Checking sample data...")
    db = SessionLocal()
    try:
        print("\n   📄 Sample Data from Main Tables:")
        print("-" * 70)
        
        # RVU Items
        try:
            result = db.execute(text("SELECT hcpcs_code, modifier, work_rvu, facility_rvu FROM rvu_items LIMIT 3"))
            rvu_samples = result.fetchall()
            if rvu_samples:
                print(f"\n   RVU Items (sample):")
                for r in rvu_samples:
                    print(f"      HCPCS: {r[0]}, Modifier: {r[1] or 'N/A'}, Work RVU: {r[2]}, Facility RVU: {r[3]}")
            else:
                print(f"\n   ⚠️  No RVU items found")
        except Exception as e:
            print(f"\n   ⚠️  Error querying RVU items: {e}")
        
        # GPCI Indices
        try:
            result = db.execute(text("SELECT mac, locality_id, locality_name, work_gpci FROM gpci_indices LIMIT 3"))
            gpci_samples = result.fetchall()
            if gpci_samples:
                print(f"\n   GPCI Indices (sample):")
                for r in gpci_samples:
                    name = (r[2][:30] + "...") if r[2] and len(r[2]) > 30 else (r[2] or 'N/A')
                    print(f"      MAC: {r[0]}, Locality: {r[1]}, Name: {name}, Work GPCI: {r[3]}")
            else:
                print(f"\n   ⚠️  No GPCI indices found")
        except Exception as e:
            print(f"\n   ⚠️  Error querying GPCI indices: {e}")
        
        # OPPS Caps
        try:
            result = db.execute(text("SELECT hcpcs_code, modifier, mac, locality_id, price_fac FROM opps_caps LIMIT 3"))
            opps_samples = result.fetchall()
            if opps_samples:
                print(f"\n   OPPS Caps (sample):")
                for r in opps_samples:
                    print(f"      HCPCS: {r[0]}, Modifier: {r[1] or 'N/A'}, MAC: {r[2]}, Locality: {r[3]}, Price: {r[4]}")
            else:
                print(f"\n   ⚠️  No OPPS caps found")
        except Exception as e:
            print(f"\n   ⚠️  Error querying OPPS caps: {e}")
            
        print("-" * 70)
            
    finally:
        db.close()

def check_data_freshness():
    """Check how fresh the data is"""
    print("\n6. Checking data freshness...")
    db = SessionLocal()
    try:
        # Check latest release date
        result = db.execute(text("""
            SELECT MAX(created_at) as latest_release
            FROM releases
        """))
        
        latest_release = result.fetchone()[0]
        
        if latest_release:
            print(f"\n   📅 Latest Release Date: {latest_release}")
            
            # Check if data is recent (within last 7 days)
            if isinstance(latest_release, datetime):
                seven_days_ago = datetime.utcnow() - timedelta(days=7)
                # Remove timezone if present for comparison
                release_dt = latest_release.replace(tzinfo=None) if latest_release.tzinfo else latest_release
                
                if release_dt >= seven_days_ago:
                    print("      ✅ Data is recent (within last 7 days)")
                else:
                    days_old = (datetime.utcnow() - release_dt).days
                    print(f"      ⚠️  Data is {days_old} days old")
            else:
                print("      (Could not parse date)")
        else:
            print("\n   ⚠️  No release dates found")
        
        # Check latest ingestion run
        result = db.execute(text("""
            SELECT MAX(created_at) as latest_ingestion
            FROM ingest_runs
        """))
        
        latest_ingestion = result.fetchone()[0]
        if latest_ingestion:
            print(f"   📅 Latest Ingestion Run: {latest_ingestion}")
            
    finally:
        db.close()

def main():
    """Main function"""
    print("=" * 60)
    print("CMS API - Render Deployment Data Check")
    print("=" * 60)
    print()
    
    # Run all checks
    checks = {
        'Database Connection': check_database_connection(),
        'Table Counts': check_table_counts(),
        'Recent Releases': check_recent_releases(),
        'Ingestion Runs': check_ingestion_runs(),
        'Sample Data': check_sample_data(),
        'Data Freshness': check_data_freshness(),
    }
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print()
    
    all_passed = all(checks.values())
    for check_name, status in checks.items():
        icon = "✅" if status else "❌"
        print(f"   {icon} {check_name}")
    
    print()
    if all_passed:
        print("🎉 All checks passed! Deployment appears healthy.")
    else:
        print("⚠️  Some checks failed. Review the output above.")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())

