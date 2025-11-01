#!/usr/bin/env python3
"""
Debug script to see what's in gpci_indices table.
"""

from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex

db = SessionLocal()

try:
    print("=" * 70)
    print("GPCI Indices Data Inspection")
    print("=" * 70)
    
    indices = db.query(GPCIIndex).all()
    print(f"\nTotal rows: {len(indices)}")
    
    if indices:
        print("\nSample rows (first 5):")
        for i, idx in enumerate(indices[:5]):
            print(f"\n  Row {i+1}:")
            print(f"    mac: {idx.mac}")
            print(f"    locality_id: {idx.locality_id}")
            print(f"    locality_name: {idx.locality_name}")
            print(f"    effective_start: {idx.effective_start}")
            print(f"    effective_end: {idx.effective_end}")
            print(f"    work_gpci: {idx.work_gpci}")
            print(f"    pe_gpci: {idx.pe_gpci}")
            print(f"    mp_gpci: {idx.mp_gpci}")
        
        print("\n" + "=" * 70)
        print("Data Quality Checks")
        print("=" * 70)
        
        # Check for missing effective_start
        missing_start = sum(1 for idx in indices if not idx.effective_start)
        print(f"\nRows without effective_start: {missing_start}")
        
        # Check for missing GPCI values
        missing_work = sum(1 for idx in indices if not idx.work_gpci)
        missing_pe = sum(1 for idx in indices if not idx.pe_gpci)
        missing_mp = sum(1 for idx in indices if not idx.mp_gpci)
        print(f"Rows missing work_gpci: {missing_work}")
        print(f"Rows missing pe_gpci: {missing_pe}")
        print(f"Rows missing mp_gpci: {missing_mp}")
        
        # Check for duplicates on (locality_id, effective_from)
        from collections import Counter
        keys = [(str(idx.locality_id), idx.effective_start) for idx in indices if idx.effective_start]
        key_counts = Counter(keys)
        duplicates = {k: v for k, v in key_counts.items() if v > 1}
        print(f"\nDuplicate (locality_id, effective_from) keys: {len(duplicates)}")
        if duplicates:
            print("  Example duplicates:")
            for key, count in list(duplicates.items())[:3]:
                print(f"    {key}: {count} rows")
        
        # Check year distribution
        years = [idx.effective_start.year for idx in indices if idx.effective_start]
        if years:
            from collections import Counter
            year_counts = Counter(years)
            print(f"\nYear distribution:")
            for year, count in sorted(year_counts.items()):
                print(f"  {year}: {count} rows")
        
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    db.close()

