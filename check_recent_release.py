#!/usr/bin/env python3
"""Check what data was actually loaded for the most recent release"""
import sys
sys.path.insert(0, '/app')
from sqlalchemy import text
from cms_pricing.database import SessionLocal

db = SessionLocal()

# Check most recent release and its data
result = db.execute(text("""
    SELECT 
        r.id,
        r.type,
        r.source_version,
        r.imported_at,
        COUNT(DISTINCT lc.id) as locality_count,
        COUNT(DISTINCT ac.id) as anescf_count,
        COUNT(DISTINCT oc.id) as oppscap_count,
        COUNT(DISTINCT pr.id) as pprrvu_count
    FROM releases r
    LEFT JOIN locality_counties lc ON lc.release_id = r.id
    LEFT JOIN anes_cfs ac ON ac.release_id = r.id
    LEFT JOIN opps_caps oc ON oc.release_id = r.id
    LEFT JOIN rvu_items pr ON pr.release_id = r.id
    WHERE r.imported_at > CURRENT_DATE - INTERVAL '1 day'
    GROUP BY r.id, r.type, r.source_version, r.imported_at
    ORDER BY r.imported_at DESC
    LIMIT 1
"""))

latest = result.fetchone()

if latest:
    release_id, release_type, source_version, imported_at, loc_count, anes_count, opps_count, pprrvu_count = latest
    print(f"✅ Latest Release:")
    print(f"   ID: {release_id}")
    print(f"   Type: {release_type}")
    print(f"   Source Version: {source_version}")
    print(f"   Imported: {imported_at}")
    print(f"   Locality: {loc_count} records")
    print(f"   AnesCF: {anes_count} records")
    print(f"   OPPSCap: {opps_count} records")
    print(f"   PPRRVU: {pprrvu_count} records")
    
    # Check long descriptions for latest release
    if loc_count > 0:
        check = db.execute(text("""
            SELECT 
                MAX(LENGTH(fee_schedule_area)) as max_fee,
                MAX(LENGTH(county_name)) as max_county,
                COUNT(*) as total
            FROM locality_counties
            WHERE release_id = :rid
        """), {"rid": release_id})
        
        row = check.fetchone()
        print(f"\n📊 Locality Data Details:")
        print(f"   Total rows: {row[2]}")
        print(f"   Max fee_schedule_area length: {row[0] or 'N/A'} chars")
        print(f"   Max county_name length: {row[1] or 'N/A'} chars")
        
        if row[0] and row[0] > 10:
            print(f"   ✅ SUCCESS! Long fee_schedule_area preserved (up to {row[0]} chars)")
        if row[1] and row[1] > 100:
            print(f"   ✅ SUCCESS! Long county_name preserved (up to {row[1]} chars)")
        
        # Show a sample of long descriptions
        samples = db.execute(text("""
            SELECT fee_schedule_area, county_name, LENGTH(fee_schedule_area) as fee_len, LENGTH(county_name) as county_len
            FROM locality_counties
            WHERE release_id = :rid
            AND (LENGTH(fee_schedule_area) > 10 OR LENGTH(county_name) > 100)
            ORDER BY fee_len DESC, county_len DESC
            LIMIT 3
        """), {"rid": release_id})
        
        sample_rows = samples.fetchall()
        if sample_rows:
            print(f"\n📝 Sample Long Descriptions:")
            for s in sample_rows:
                print(f"   fee_schedule_area ({s[2]} chars): {s[0][:60]}{'...' if len(s[0] or '') > 60 else ''}")
                print(f"   county_name ({s[3]} chars): {s[1][:60]}{'...' if len(s[1] or '') > 60 else ''}")
                print()
else:
    print("No recent releases found in the last 24 hours")

db.close()
