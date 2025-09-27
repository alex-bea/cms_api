#!/usr/bin/env python3
"""Ingest real CMS data for testing"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cms_pricing.ingestion.geography import GeographyIngester
from cms_pricing.ingestion.cms_downloader import CMSDownloader
from cms_pricing.ingestion.zip_handler import ZIPHandler
import structlog

logger = structlog.get_logger()


async def test_cms_download():
    """Test downloading real CMS data"""
    
    print("🔄 Testing CMS data download...")
    
    downloader = CMSDownloader()
    
    # Test downloading geography data for 2024 (2025 may not be available yet)
    result = await downloader.download_dataset("geography", 2024)
    
    # Check if at least one file downloaded successfully
    successful_files = [f for f in result["files"].values() if f["success"]]
    
    if successful_files:
        print(f"✅ CMS download partially successful! ({len(successful_files)}/{len(result['files'])} files)")
        for file_type, file_info in result["files"].items():
            if file_info["success"]:
                print(f"   📁 {file_type}: {file_info['filename']} ({file_info['size_bytes']} bytes)")
            else:
                print(f"   ❌ {file_type}: Failed - {file_info.get('error', 'Unknown error')}")
        
        # Mark as success if we got the main ZIP locality file
        if any(f["success"] for f in result["files"].values() if "zip_locality" in f["filename"]):
            result["success"] = True
    else:
        print("❌ CMS download failed")
        print(f"   Error details: {result}")
    
    return result


async def test_zip_processing():
    """Test processing downloaded ZIP files"""
    
    print("\n🔄 Testing ZIP file processing...")
    
    zip_handler = ZIPHandler()
    
    # Process all ZIP files in the cms_raw directory
    result = zip_handler.batch_process_zips()
    
    if result["success"]:
        print(f"✅ ZIP processing successful! Processed {result['successful']}/{result['total_files']} files")
        
        for filename, file_result in result["results"].items():
            if file_result["success"]:
                csv_files = file_result.get("csv_files", [])
                print(f"   📁 {filename}: {len(csv_files)} CSV files extracted")
                
                for csv in csv_files[:3]:  # Show first 3 CSV files
                    print(f"      📄 {csv['filename']}: {csv['header_count']} columns, {csv['size_bytes']} bytes")
            else:
                print(f"   ❌ {filename}: Failed - {file_result.get('error', 'Unknown error')}")
    else:
        print("❌ ZIP processing failed")
        print(f"   Error details: {result}")
    
    return result


async def test_geography_ingestion():
    """Test full geography data ingestion"""
    
    print("\n🔄 Testing geography data ingestion...")
    
    ingester = GeographyIngester("./data")
    
    try:
        # Ingest geography data for 2024
        result = await ingester.ingest(2024)
        
        if result.get("build_id") and result.get("digest"):
            print("✅ Geography ingestion successful!")
            print(f"   📊 Build ID: {result['build_id']}")
            print(f"   📝 Digest: {result['digest']}")
            
            # Check the normalized data
            normalized_dir = Path("./data/GEOGRAPHY") / result['build_id'] / "normalized"
            if normalized_dir.exists():
                parquet_files = list(normalized_dir.glob("*.parquet"))
                print(f"   📁 Normalized files: {len(parquet_files)}")
                
                for parquet_file in parquet_files:
                    print(f"      📄 {parquet_file.name}")
            
            # Show warnings if any
            if result.get("warnings"):
                print(f"   ⚠️  Warnings: {len(result['warnings'])}")
                for warning in result["warnings"]:
                    print(f"      - {warning}")
        else:
            print("❌ Geography ingestion failed")
            print(f"   Error details: {result}")
        
        return result
        
    except Exception as e:
        print(f"❌ Geography ingestion error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


async def main():
    """Main test function"""
    
    print("🚀 Starting real CMS data ingestion test")
    print("=" * 50)
    
    # Test 1: Download CMS data
    download_result = await test_cms_download()
    
    # Test 2: Process ZIP files (only if download was successful)
    if download_result["success"]:
        zip_result = await test_zip_processing()
        
        # Test 3: Full ingestion (only if ZIP processing was successful)
        if zip_result["success"]:
            ingestion_result = await test_geography_ingestion()
        else:
            print("\n⏭️ Skipping ingestion test due to ZIP processing failure")
    else:
        print("\n⏭️ Skipping ZIP processing and ingestion tests due to download failure")
    
    print("\n" + "=" * 50)
    print("🎉 Real CMS data ingestion test completed!")
    
    # Summary
    print("\n📊 Test Summary:")
    print(f"   Download: {'✅ Success' if download_result['success'] else '❌ Failed'}")
    
    if download_result["success"]:
        print(f"   ZIP Processing: {'✅ Success' if zip_result['success'] else '❌ Failed'}")
        
        if zip_result["success"]:
            print(f"   Ingestion: {'✅ Success' if ingestion_result.get('build_id') else '❌ Failed'}")


if __name__ == "__main__":
    asyncio.run(main())
