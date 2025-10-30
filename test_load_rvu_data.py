#!/usr/bin/env python3
"""
Test script to load RVU data locally
"""
import asyncio
import sys
from pathlib import Path
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor

async def main():
    print("🧪 Testing Local RVU Data Loading")
    print("=" * 50)
    
    # Initialize ingestor with sample data directory
    test_data_dir = Path("sample_data/rvu25a")
    ingestor = RVUIngestor(output_dir="./test_data_loaded")
    
    print(f"📁 Test data directory: {test_data_dir}")
    print(f"📁 Output directory: ./test_data_loaded")
    print("")
    
    # Check if test data exists
    if not test_data_dir.exists():
        print("❌ Test data directory not found!")
        return 1
    
    # List available files
    files = list(test_data_dir.glob("*.txt")) + list(test_data_dir.glob("*.csv"))
    print(f"✅ Found {len(files)} data files")
    print("")
    
    # Try to ingest
    release_id = "test-local-rvu-2025"
    batch_id = "batch-001"
    
    print("Starting ingestion...")
    try:
        result = await ingestor.ingest(release_id, batch_id)
        print("")
        print("✅ Ingestion completed!")
        print(f"Status: {result.get('status')}")
        print(f"Records processed: {result.get('record_count', 'N/A')}")
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
