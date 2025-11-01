#!/usr/bin/env python3
"""
Inline script to load GPCI from gpci_indices to gpci table.
Paste this into Render shell.
"""

from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import GPCIIndex
from cms_pricing.models.fee_schedules import GPCI
from datetime import datetime

db = SessionLocal()

try:
    # Get all GPCI indices
    print("Fetching data from gpci_indices...")
    gpci_indices = db.query(GPCIIndex).all()
    print(f"Found {len(gpci_indices)} rows")
    
    if not gpci_indices:
        print("No data found")
        exit(1)
    
    # Get year from data
    years = [idx.effective_start.year if idx.effective_start else 2025 for idx in gpci_indices]
    year = max(set(years), key=years.count) if years else 2025
    print(f"Using year: {year}")
    
    # Clear existing data
    deleted = db.query(GPCI).filter(GPCI.year == year).delete(synchronize_session=False)
    print(f"Cleared {deleted} existing rows")
    
    # Deduplicate by (locality_id, effective_from)
    seen_keys = set()
    records_loaded = 0
    release_id = "rvu_gpci_2025"
    batch_id = f"batch_{int(datetime.now().timestamp())}"
    
    for idx in gpci_indices:
        try:
            # Extract year and effective date
            record_year = idx.effective_start.year if idx.effective_start else year
            effective_from = idx.effective_start
            
            if not effective_from:
                continue
            
            # Dedupe key
            dedupe_key = (str(idx.locality_id), effective_from)
            
            if dedupe_key in seen_keys:
                continue
            
            # Check actual column names - may be gpci_work/gpci_pe/gpci_mp instead of work_gpci/pe_gpci/mp_gpci
            # Try both naming conventions
            work_val = getattr(idx, 'work_gpci', None) or getattr(idx, 'gpci_work', None)
            pe_val = getattr(idx, 'pe_gpci', None) or getattr(idx, 'gpci_pe', None)
            mp_val = getattr(idx, 'mp_gpci', None) or getattr(idx, 'gpci_mp', None) or getattr(idx, 'gpci_malp', None)
            
            # Validate required fields
            if not work_val or not pe_val or not mp_val:
                continue
            
            # Create record
            gpci_record = GPCI(
                year=record_year,
                locality_id=str(idx.locality_id),
                locality_name=idx.locality_name or "",
                gpci_work=float(work_val),
                gpci_pe=float(pe_val),
                gpci_mp=float(mp_val),
                effective_from=effective_from,
                effective_to=idx.effective_end,
                release_id=release_id,
                batch_id=batch_id
            )
            
            db.add(gpci_record)
            seen_keys.add(dedupe_key)
            records_loaded += 1
            
            if records_loaded % 50 == 0:
                db.commit()
                print(f"  Committed {records_loaded} records...")
                
        except Exception as e:
            print(f"  Warning: Failed to load record for locality {idx.locality_id}: {e}")
            continue
    
    db.commit()
    print(f"\n✅ Successfully loaded {records_loaded} rows to gpci table")
    print(f"   Release ID: {release_id}")
    print(f"   Batch ID: {batch_id}")
    
except Exception as e:
    db.rollback()
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
    
finally:
    db.close()

