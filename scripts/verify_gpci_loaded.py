#!/usr/bin/env python3
"""
Verify GPCI data was loaded to Render production database.

This script checks:
1. If GPCI data exists in either gpci_indices (RVU model) or gpci (fee schedule table)
2. If provenance columns are populated
3. Row counts and data quality
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from cms_pricing.database import SessionLocal, engine
from cms_pricing.models.rvu import GPCIIndex
from cms_pricing.models.fee_schedules import GPCI
from sqlalchemy import text
import structlog

logger = structlog.get_logger()


def verify_gpci_data():
    """Verify GPCI data in production database."""
    
    db = SessionLocal()
    
    try:
        print("=" * 70)
        print("GPCI v1.3 Data Verification")
        print("=" * 70)
        print()
        
        # Check gpci_indices table (RVU model)
        print("1. Checking gpci_indices table (RVU model)...")
        gpci_indices_count = db.query(GPCIIndex).count()
        print(f"   Total rows: {gpci_indices_count}")
        
        if gpci_indices_count > 0:
            sample = db.query(GPCIIndex).first()
            print(f"   Sample row: MAC={sample.mac}, locality_id={sample.locality_id}")
            
            # Check for v1.3 natural key (MAC included)
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM gpci_indices 
                    WHERE mac IS NOT NULL 
                    AND locality_id IS NOT NULL 
                    AND effective_start IS NOT NULL
                """))
                valid_nk_count = result.scalar()
                print(f"   Valid v1.3 NK rows (MAC + locality + date): {valid_nk_count}")
        
        print()
        
        # Check gpci table (fee schedule - simplified)
        print("2. Checking gpci table (fee schedule - simplified)...")
        gpci_count = db.query(GPCI).count()
        print(f"   Total rows: {gpci_count}")
        
        if gpci_count > 0:
            sample = db.query(GPCI).first()
            print(f"   Sample row: year={sample.year}, locality_id={sample.locality_id}")
            print(f"                release_id={sample.release_id}, batch_id={sample.batch_id}")
            
            # Check provenance
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(release_id) as has_release_id,
                        COUNT(batch_id) as has_batch_id
                    FROM gpci
                """))
                row = result.fetchone()
                print(f"   Provenance: {row[1]}/{row[0]} have release_id, {row[2]}/{row[0]} have batch_id")
                
                # Check for duplicates on v1.3 NK
                result = conn.execute(text("""
                    SELECT locality_id, effective_from, COUNT(*) as cnt
                    FROM gpci
                    GROUP BY locality_id, effective_from
                    HAVING COUNT(*) > 1
                    LIMIT 5
                """))
                dupes = result.fetchall()
                if dupes:
                    print(f"   ⚠️  Found {len(dupes)} potential duplicate groups (MAC not in NK)")
                else:
                    print(f"   ✅ No duplicates found (but note: gpci table doesn't have MAC column)")
        
        print()
        
        # Summary
        print("=" * 70)
        print("Summary")
        print("=" * 70)
        
        if gpci_indices_count > 0:
            print(f"✅ GPCI data found in gpci_indices table: {gpci_indices_count} rows")
            print("   Note: This is the RVU model table (not simplified fee schedule)")
        
        if gpci_count > 0:
            print(f"✅ GPCI data found in gpci table: {gpci_count} rows")
            print("   This is the simplified fee schedule table used by pricing engines")
        else:
            print("⚠️  No data in gpci table (simplified fee schedule)")
            print("   Action: May need to run load_data.py to populate from parquet files")
        
        if gpci_indices_count > 0 and gpci_count == 0:
            print()
            print("RECOMMENDATION:")
            print("  Data exists in gpci_indices but not in gpci (simplified table).")
            print("  The pricing engines use the 'gpci' table, so you may need to:")
            print("  1. Check if parquet files were generated")
            print("  2. Run: python scripts/load_data.py")
            print("     (This loads from parquet to simplified fee schedule tables)")
        
        return gpci_count > 0
        
    except Exception as e:
        logger.error("Verification failed", error=str(e))
        print(f"❌ Error: {e}")
        return False
        
    finally:
        db.close()


if __name__ == "__main__":
    success = verify_gpci_data()
    sys.exit(0 if success else 1)

