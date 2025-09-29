#!/usr/bin/env python3
"""Load test data for nearest ZIP resolver"""

import asyncio
from datetime import date, datetime
from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, 
    ZipMetadata, IngestRun, ZIP9Overrides
)
import uuid

async def load_test_data():
    """Load test data for nearest ZIP resolver"""
    db = SessionLocal()
    
    try:
        print("Loading test data for nearest ZIP resolver...")
        
        # Create test ingest run
        ingest_run = IngestRun(
            run_id=str(uuid.uuid4()),
            source_url="test_data",
            filename="test_data.py",
            started_at=datetime.now(),
            finished_at=datetime.now(),
            row_count=0,
            tool_version="1.0.0",
            status="success",
            notes="Test data for nearest ZIP resolver"
        )
        db.add(ingest_run)
        db.flush()
        
        # Load ZCTA coordinates (San Francisco Bay Area)
        zcta_coords = [
            ZCTACoords(zcta5="94107", lat=37.76, lon=-122.39, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94110", lat=37.75, lon=-122.42, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94115", lat=37.78, lon=-122.45, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94117", lat=37.77, lon=-122.43, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94118", lat=37.79, lon=-122.46, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94121", lat=37.74, lon=-122.48, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94122", lat=37.73, lon=-122.47, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94123", lat=37.80, lon=-122.44, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94124", lat=37.72, lon=-122.46, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZCTACoords(zcta5="94125", lat=37.75, lon=-122.49, vintage="2025", ingest_run_id=ingest_run.run_id),
        ]
        
        # Load ZIP to ZCTA mappings
        zip_to_zcta = [
            ZipToZCTA(zip5="94107", zcta5="94107", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94110", zcta5="94110", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94115", zcta5="94115", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94117", zcta5="94117", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94118", zcta5="94118", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94121", zcta5="94121", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94122", zcta5="94122", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94123", zcta5="94123", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94124", zcta5="94124", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
            ZipToZCTA(zip5="94125", zcta5="94125", relationship="Zip matches ZCTA", weight=1.0, vintage="2023", ingest_run_id=ingest_run.run_id),
        ]
        
        # Load CMS ZIP locality mappings (all in CA, locality 01)
        cms_zip_locality = [
            CMSZipLocality(zip5="94107", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94110", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94115", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94117", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94118", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94121", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94122", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94123", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94124", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
            CMSZipLocality(zip5="94125", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=ingest_run.run_id),
        ]
        
        # Load ZIP metadata (all non-PO Box)
        zip_metadata = [
            ZipMetadata(zip5="94107", zcta_bool=True, population=50000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94110", zcta_bool=True, population=45000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94115", zcta_bool=True, population=60000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94117", zcta_bool=True, population=55000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94118", zcta_bool=True, population=48000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94121", zcta_bool=True, population=52000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94122", zcta_bool=True, population=47000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94123", zcta_bool=True, population=58000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94124", zcta_bool=True, population=43000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZipMetadata(zip5="94125", zcta_bool=True, population=51000, is_pobox=False, vintage="2025", ingest_run_id=ingest_run.run_id),
        ]
        
        # Load ZIP9 overrides (test data)
        zip9_overrides = [
            ZIP9Overrides(zip9_low="941070000", zip9_high="941079999", state="CA", locality="02", rural_flag=False, vintage="2025", ingest_run_id=ingest_run.run_id),
            ZIP9Overrides(zip9_low="941150000", zip9_high="941159999", state="CA", locality="03", rural_flag=False, vintage="2025", ingest_run_id=ingest_run.run_id),
        ]
        
        # Add all records to database
        for record in zcta_coords + zip_to_zcta + cms_zip_locality + zip_metadata + zip9_overrides:
            db.add(record)
        
        # Update row count
        ingest_run.row_count = len(zcta_coords) + len(zip_to_zcta) + len(cms_zip_locality) + len(zip_metadata) + len(zip9_overrides)
        
        db.commit()
        
        print(f"✅ Loaded {len(zcta_coords)} ZCTA coordinates")
        print(f"✅ Loaded {len(zip_to_zcta)} ZIP to ZCTA mappings")
        print(f"✅ Loaded {len(cms_zip_locality)} CMS ZIP locality mappings")
        print(f"✅ Loaded {len(zip_metadata)} ZIP metadata records")
        print(f"✅ Loaded {len(zip9_overrides)} ZIP9 overrides")
        print(f"✅ Total records: {ingest_run.row_count}")
        
        print("\n🎯 Test data loaded successfully!")
        print("You can now test the nearest ZIP resolver with ZIP codes like:")
        print("  - 94107 (should find 94110 as nearest)")
        print("  - 94115 (should find 94117 as nearest)")
        print("  - 94121 (should find 94122 as nearest)")
        
    except Exception as e:
        print(f"❌ Error loading test data: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(load_test_data())
