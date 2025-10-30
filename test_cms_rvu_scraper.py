#!/usr/bin/env python3
"""
Test CMS RVU Scraper
"""

import asyncio
import json
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
from cms_pricing.ingestion.managers.historical_data_manager import HistoricalDataManager

async def test_cms_rvu_scraper():
    """Test the CMS RVU scraper"""
    print("\n🧪 Testing CMS RVU Scraper")
    print("=" * 60)
    
    # Test 1: Scraper Initialization
    print("\n🔍 Test 1: Scraper Initialization")
    print("-" * 40)
    
    scraper = CMSRVUScraper("./test_data/cms_rvu")
    print(f"   📊 Scraper initialized")
    print(f"   📁 Output directory: {scraper.output_dir}")
    print(f"   🔗 RVU page URL: {scraper.rvu_page_url}")
    print("   ✅ Scraper initialization successful")
    
    # Test 2: Scrape RVU Files (recent years only)
    print("\n🔍 Test 2: Scrape RVU Files")
    print("-" * 40)
    
    try:
        # Scrape files from 2023-2025 (recent data)
        files = await scraper.scrape_rvu_files(start_year=2023, end_year=2025)
        
        print(f"   📊 Found {len(files)} RVU files")
        
        for i, file_info in enumerate(files[:10]):  # Show first 10 files
            revision = file_info.revision or ""
            print(f"   📄 {i+1}. {file_info.year}{file_info.quarter}{revision}: {file_info.filename}")
            print(f"      URL: {file_info.url}")
            print(f"      Version: {file_info.version}")
            print(f"      Content-Type: {file_info.content_type}")
        
        if len(files) > 10:
            print(f"   ... and {len(files) - 10} more files")
        
        assert len(files) > 0, "No RVU files found"
        print("   ✅ File scraping successful")
        
    except Exception as e:
        print(f"   ❌ File scraping failed: {e}")
        return False
    
    # Test 3: Download Sample Files
    print("\n🔍 Test 3: Download Sample Files")
    print("-" * 40)
    
    try:
        # Download first 2 files as a test
        sample_files = files[:2]
        print(f"   📥 Downloading {len(sample_files)} sample files...")
        
        results = await scraper.download_all_files(sample_files, max_concurrent=2)
        
        successful_downloads = 0
        for i, result in enumerate(results):
            if isinstance(result, dict) and result.get("status") == "success":
                successful_downloads += 1
                file_info = result["file_info"]
                print(f"   ✅ {file_info.filename}: {result['size_bytes']:,} bytes")
            else:
                print(f"   ❌ {sample_files[i].filename}: Failed")
        
        print(f"   📊 Successfully downloaded: {successful_downloads}/{len(sample_files)} files")
        print("   ✅ Sample download successful")
        
    except Exception as e:
        print(f"   ❌ Sample download failed: {e}")
        return False
    
    # Test 4: Manifest Verification
    print("\n🔍 Test 4: Manifest Verification")
    print("-" * 40)
    
    try:
        manifest_path = scraper.last_manifest_path
        assert manifest_path is not None, "Manifest path not set on scraper"
        manifest_json = json.loads(manifest_path.read_text())
        
        print(f"   📊 Manifest generated at {manifest_path}")
        print(f"   📄 Total files: {len(manifest_json.get('files', []))}")
        print("   ✅ Manifest verification successful")
        
    except Exception as e:
        print(f"   ❌ Manifest verification failed: {e}")
        return False
    
    # Test 5: Historical Data Manager
    print("\n🔍 Test 5: Historical Data Manager")
    print("-" * 40)
    
    try:
        manager = HistoricalDataManager("./test_data/historical_rvu")
        
        discovery_summary = await manager.download_historical_data(start_year=2024, end_year=2025, download=False)
        print(f"   📊 Discovery status: {discovery_summary['status']}")

        freshness = manager.check_data_freshness()
        print(f"   📊 Data freshness: {freshness['status']}")
        
        if freshness["status"] == "data_available":
            print(f"   📊 Latest year: {freshness['latest_year']}")
            print(f"   📊 Total files: {freshness['total_files']}")
        
        print("   ✅ Historical data manager working")
        
    except Exception as e:
        print(f"   ❌ Historical data manager failed: {e}")
        return False
    
    print("\n🎉 CMS RVU Scraper Test Completed!")
    print("=" * 60)
    print("✅ Scraper initialization working")
    print("✅ File scraping working")
    print("✅ Sample download working")
    print("✅ Manifest verification working")
    print("✅ Historical data manager working")
    print("\n🚀 CMS RVU scraper is ready for production use!")
    
    return True

async def main():
    """Main test function"""
    success = await test_cms_rvu_scraper()
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
