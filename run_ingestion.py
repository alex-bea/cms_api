#!/usr/bin/env python3
"""
Simple script to run data ingestion for nearest ZIP resolver
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from cms_pricing.ingestion.nearest_zip_ingestion import NearestZipIngestionPipeline


async def main():
    """Run the ingestion pipeline"""
    print("🚀 Starting Nearest ZIP Resolver Data Ingestion")
    print("=" * 60)
    
    try:
        # Create pipeline
        pipeline = NearestZipIngestionPipeline("./data/ingestion")
        
        # Run full pipeline
        results = await pipeline.run_full_pipeline()
        
        # Print results
        print("\n📊 INGESTION RESULTS")
        print("=" * 60)
        
        overall_status = results.get('overall_status', 'unknown')
        print(f"Overall Status: {overall_status}")
        
        for source, result in results.get('results', {}).items():
            status = result.get('status', 'unknown')
            record_counts = result.get('record_counts', {})
            
            print(f"\n{source.upper()}:")
            print(f"  Status: {status}")
            
            if record_counts:
                print("  Records loaded:")
                for table, count in record_counts.items():
                    print(f"    {table}: {count:,}")
            
            if 'warnings' in result and result['warnings']:
                print("  Warnings:")
                for warning in result['warnings']:
                    print(f"    - {warning}")
            
            if 'error' in result:
                print(f"  Error: {result['error']}")
        
        # Check overall status
        if overall_status == 'success':
            print(f"\n✅ All data sources ingested successfully!")
            return 0
        elif overall_status == 'partial_failure':
            print(f"\n⚠️  Some data sources failed to ingest")
            return 1
        else:
            print(f"\n❌ Ingestion failed")
            return 1
            
    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
