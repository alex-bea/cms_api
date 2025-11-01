#!/usr/bin/env python3
"""
Direct diagnostic to check what's happening with GPCI data.
Run this in Render after ingestion to see what's in the database.
"""

from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex, Release
from sqlalchemy import text

db = SessionLocal()

try:
    print("=" * 70)
    print("GPCI Data Diagnostic")
    print("=" * 70)
    
    # Check recent releases
    print("\n1. Recent Releases:")
    releases = db.query(Release).order_by(Release.imported_at.desc()).limit(3).all()
    for r in releases:
        print(f"   {r.id} - {r.source_version} - {r.imported_at}")
    
    # Check gpci_indices with raw SQL to see actual values
    print("\n2. Raw Database Values (first 5 rows):")
    with db.connection() as conn:
        result = conn.execute(text("""
            SELECT 
                mac, 
                locality_id, 
                effective_start,
                work_gpci,
                pe_gpci,
                mp_gpci,
                release_id
            FROM gpci_indices 
            ORDER BY release_id DESC, locality_id
            LIMIT 5
        """))
        rows = result.fetchall()
        for row in rows:
            print(f"   MAC={row[0]}, Loc={row[1]}, Date={row[2]}")
            print(f"      work_gpci={row[3]}, pe_gpci={row[4]}, mp_gpci={row[5]}")
            print(f"      release_id={row[6]}")
    
    # Check column data types
    print("\n3. Column Data Types:")
    with db.connection() as conn:
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'gpci_indices'
            AND column_name IN ('work_gpci', 'pe_gpci', 'mp_gpci', 'gpci_work', 'gpci_pe', 'gpci_mp')
            ORDER BY column_name
        """))
        for row in result:
            print(f"   {row[0]}: {row[1]} (nullable: {row[2]})")
    
    # Check if any non-null GPCI values exist
    print("\n4. Value Statistics:")
    with db.connection() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(work_gpci) as has_work,
                COUNT(pe_gpci) as has_pe,
                COUNT(mp_gpci) as has_mp,
                MIN(work_gpci) as min_work,
                MAX(work_gpci) as max_work
            FROM gpci_indices
        """))
        stats = result.fetchone()
        print(f"   Total rows: {stats[0]}")
        print(f"   Rows with work_gpci: {stats[1]}")
        print(f"   Rows with pe_gpci: {stats[2]}")
        print(f"   Rows with mp_gpci: {stats[3]}")
        if stats[4] is not None:
            print(f"   work_gpci range: {stats[4]} to {stats[5]}")
    
    print("\n" + "=" * 70)
    print("Diagnostic Complete")
    print("=" * 70)
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    db.close()

