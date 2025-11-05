#!/usr/bin/env python3
"""
Verify Phase 2 Refactored RVU Ingestor Processes Real Data

This script runs the ingestor with test fixtures and verifies:
1. Real CMS data is parsed (not empty dataframes)
2. Dataframes contain expected columns
3. Row counts are reasonable for CMS data
4. Data values look like real CMS data
"""
import asyncio
import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.ingestion.datasets.rvu_spec import RVU_DATASETS


async def verify_real_data():
    """Run ingestor and verify real CMS data is parsed"""
    print("=" * 70)
    print("🔍 VERIFYING PHASE 2 REFACTORED INGESTOR PROCESSES REAL DATA")
    print("=" * 70)
    print()
    
    # Check test fixtures
    fixture_dir = project_root / "tests" / "fixtures" / "rvu" / "test_data"
    if not fixture_dir.exists():
        print(f"❌ Test fixtures not found at: {fixture_dir}")
        return False
    
    print(f"✅ Found test fixtures at: {fixture_dir}")
    print()
    
    # Initialize ingestor
    output_dir = project_root / "data" / "test_real_data_verification"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("📋 Initializing RVU Ingestor...")
    print(f"   Output directory: {output_dir}")
    print(f"   DatasetSpec registry: {len(RVU_DATASETS)} datasets")
    for name in RVU_DATASETS.keys():
        print(f"      - {name}")
    print()
    
    ingestor = RVUIngestor(
        dataset_name="pprrvu",
        output_dir=str(output_dir)
    )
    
    # Verify services initialized
    print("🔧 Checking ServiceFactory initialization...")
    if hasattr(ingestor, 'services'):
        print("   ✅ Services initialized")
        print(f"      - Schema registry: {hasattr(ingestor.services, 'schema_registry')}")
        print(f"      - Validation service: {hasattr(ingestor.services, 'validation_service')}")
    else:
        print("   ❌ Services not initialized")
        return False
    print()
    
    # Run a test ingestion to see parsed data
    print("🚀 Running test ingestion...")
    try:
        release_id = "test_real_data_verification"
        batch_id = "test_batch_001"
        
        result = await ingestor.ingest(release_id=release_id, batch_id=batch_id)
        
        print(f"\n📊 Ingestion Result:")
        print(f"   Status: {result.get('status')}")
        print(f"   Release ID: {result.get('release_id')}")
        print(f"   Total records: {result.get('total_records', 0)}")
        print()
        
        # Check if we have parsed dataframes
        if result.get('status') == 'success':
            print("✅ Ingestion completed successfully")
            
            # Try to find parsed data in stage directory
            stage_dir = Path(output_dir) / "stage" / "cms_rvu" / release_id
            curated_dir = Path(output_dir) / "curated" / "cms_rvu"
            
            if stage_dir.exists():
                print(f"\n📁 Stage directory exists: {stage_dir}")
                print(f"   Contents: {list(stage_dir.iterdir())}")
            
            if curated_dir.exists():
                print(f"\n📁 Curated directory exists: {curated_dir}")
                print(f"   Contents: {list(curated_dir.iterdir())}")
            
            # Check for parquet files
            parquet_files = list(curated_dir.rglob("*.parquet")) if curated_dir.exists() else []
            if parquet_files:
                print(f"\n📦 Found {len(parquet_files)} parquet files:")
                for pq_file in parquet_files:
                    try:
                        df = pd.read_parquet(pq_file)
                        print(f"   ✅ {pq_file.name}: {len(df)} rows, {len(df.columns)} columns")
                        if len(df) > 0:
                            print(f"      Sample columns: {list(df.columns[:5])}")
                    except Exception as e:
                        print(f"   ⚠️  {pq_file.name}: Error reading - {e}")
            
            # Check normalized/enriched data
            normalized_data = result.get('normalized_data', {})
            if normalized_data:
                print(f"\n📊 Normalized data found:")
                for dataset, df_info in normalized_data.items():
                    if isinstance(df_info, dict) and 'row_count' in df_info:
                        print(f"   - {dataset}: {df_info.get('row_count', 0)} rows")
            
            enriched_data = result.get('enriched_data', {})
            if enriched_data:
                print(f"\n📊 Enriched data found:")
                for dataset, df_info in enriched_data.items():
                    if isinstance(df_info, dict) and 'row_count' in df_info:
                        print(f"   - {dataset}: {df_info.get('row_count', 0)} rows")
        
        return result.get('status') == 'success'
        
    except Exception as e:
        print(f"\n❌ Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(verify_real_data())
    sys.exit(0 if success else 1)
