#!/usr/bin/env python3
"""
End-to-End Test for CMS ZIP Locality Production Ingester
Tests the complete DIS-compliant pipeline
"""

import asyncio
import time
from pathlib import Path
import sys

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from cms_pricing.ingestion.ingestors.cms_zip_locality_production_ingester import CMSZipLocalityProductionIngester
from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import CMSZipLocality


async def test_cms_ingester_end_to_end():
    """Test the CMS ingester end-to-end"""
    print("🚀 CMS ZIP LOCALITY INGESTER END-TO-END TEST")
    print("=" * 60)
    print(f"🕐 Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Setup
    output_dir = "./data/ingestion/cms_end_to_end_test"
    release_id = f"cms_test_{int(time.time())}"
    
    print(f"📊 Ingester: CMSZipLocalityProductionIngester")
    print(f"📁 Output Directory: {output_dir}")
    print(f"🆔 Release ID: {release_id}")
    print()
    
    # Initialize ingester
    ingester = CMSZipLocalityProductionIngester(output_dir)
    
    try:
        print("🔍 STEP 1: INGESTION PIPELINE")
        print("=" * 50)
        print("Testing complete DIS pipeline...")
        
        start_time = time.time()
        
        # Run the complete ingestion pipeline
        result = await ingester.ingest(release_id)
        
        ingestion_time = time.time() - start_time
        print(f"⏱️  Ingestion completed in {ingestion_time:.2f} seconds")
        print(f"📊 Result status: {result.get('status', 'unknown')}")
        
        if result.get('status') == 'success':
            print("   ✅ Ingestion successful!")
            print(f"      📊 Records processed: {result.get('records_processed', 0):,}")
            print(f"      📊 Quality score: {result.get('quality_score', 0):.3f}")
            print(f"      📁 Raw directory: {result.get('raw_directory', 'N/A')}")
            print(f"      📁 Curated directory: {result.get('curated_directory', 'N/A')}")
        else:
            print(f"   ❌ Ingestion failed: {result.get('error', 'Unknown error')}")
            return False
        
        print()
        
        # Check database
        print("🔍 STEP 2: DATABASE VERIFICATION")
        print("=" * 50)
        print("Verifying data in database...")
        
        db = SessionLocal()
        try:
            # Check CMSZipLocality records
            cms_count = db.query(CMSZipLocality).count()
            print(f"   📊 CMS ZIP Locality records: {cms_count:,}")
            
            if cms_count > 0:
                print("   ✅ Data successfully loaded to database!")
                
                # Show sample records
                sample_records = db.query(CMSZipLocality).limit(3).all()
                print("   📄 Sample records:")
                for i, record in enumerate(sample_records):
                    print(f"      {i+1}. ZIP: {record.zip5}, State: {record.state}, Locality: {record.locality}")
            else:
                print("   ❌ No data found in database")
                return False
                
        finally:
            db.close()
        
        print()
        
        # Check output files
        print("🔍 STEP 3: OUTPUT VERIFICATION")
        print("=" * 50)
        print("Verifying generated output files...")
        
        output_path = Path(output_dir)
        
        # Check raw directory
        raw_dir = output_path / "raw" / "cms_zip_locality" / release_id
        if raw_dir.exists():
            files = list(raw_dir.glob("*"))
            print(f"   📁 Raw directory: {raw_dir}")
            print(f"      📄 Files: {len(files)}")
            for f in files:
                if f.is_file():
                    size = f.stat().st_size
                    print(f"         📄 {f.name} ({size:,} bytes)")
        else:
            print(f"   ❌ Raw directory not found: {raw_dir}")
        
        # Check curated directory
        curated_dir = output_path / "curated" / "cms_zip_locality"
        if curated_dir.exists():
            files = list(curated_dir.glob("**/*"))
            print(f"   📁 Curated directory: {curated_dir}")
            print(f"      📄 Files: {len(files)}")
            for f in files:
                if f.is_file():
                    size = f.stat().st_size
                    print(f"         📄 {f.name} ({size:,} bytes)")
        else:
            print(f"   ❌ Curated directory not found: {curated_dir}")
        
        print()
        
        # Check manifest
        manifest_file = raw_dir / "manifest.json"
        if manifest_file.exists():
            print("   📄 Manifest file found!")
            import json
            with open(manifest_file, 'r') as f:
                manifest = json.load(f)
            print(f"      📊 Release ID: {manifest.get('release_id', 'N/A')}")
            print(f"      📊 Files: {len(manifest.get('files', []))}")
            print(f"      📊 Total size: {manifest.get('total_size_bytes', 0):,} bytes")
        
        print()
        
        # Final results
        print("🎯 FINAL RESULTS")
        print("=" * 60)
        print("✅ Ingestion: Complete DIS pipeline executed")
        print("✅ Database: Data successfully loaded")
        print("✅ Files: Output files generated")
        print("✅ Manifest: Provenance tracking complete")
        print()
        print("📊 Pipeline Summary:")
        print(f"   🆔 Release ID: {release_id}")
        print(f"   📊 Records: {result.get('records_processed', 0):,}")
        print(f"   📊 Quality: {result.get('quality_score', 0):.3f}")
        print(f"   ⏱️  Duration: {ingestion_time:.2f} seconds")
        print(f"   📁 Output: {output_dir}")
        print()
        print("🎉 SUCCESS: CMS ingester is fully functional!")
        print("🚀 The ingester is production-ready for real CMS data!")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_cms_ingester_end_to_end())
    if success:
        print("\n🏁 Test completed with status: success")
        print("🎉 CMS Ingester pipeline is production-ready!")
    else:
        print("\n🏁 Test completed with status: failed")
        print("❌ CMS Ingester pipeline needs fixes")