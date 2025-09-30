#!/usr/bin/env python3
"""
RVU Ingester File Cleanup Script
Identifies and removes unused files related to RVU ingester development
"""

import os
import shutil
from pathlib import Path

def cleanup_rvu_files():
    """Clean up unused RVU ingester files"""
    
    print("🧹 RVU INGESTER FILE CLEANUP")
    print("=" * 50)
    
    # Files to keep (core functionality)
    keep_files = {
        # Core RVU ingester
        "cms_pricing/ingestion/ingestors/rvu_ingestor.py",
        
        # Core DIS infrastructure (used by RVU ingester)
        "cms_pricing/ingestion/contracts/ingestor_spec.py",
        "cms_pricing/ingestion/contracts/schema_registry.py",
        "cms_pricing/ingestion/adapters/data_adapters.py",
        "cms_pricing/ingestion/validators/validation_engine.py",
        "cms_pricing/ingestion/enrichers/data_enrichers.py",
        "cms_pricing/ingestion/publishers/data_publishers.py",
        "cms_pricing/ingestion/quarantine/quarantine_manager.py",
        "cms_pricing/ingestion/observability/metrics_collector.py",
        "cms_pricing/ingestion/run/dis_pipeline.py",
        "cms_pricing/ingestion/metadata/ingestion_runs_manager.py",
        "cms_pricing/ingestion/observability/cms_observability_collector.py",
        
        # Core models
        "cms_pricing/models/rvu.py",
        
        # Core routers
        "cms_pricing/routers/rvu.py",
        
        # Core schemas
        "cms_pricing/schemas/rvu.py",
        
        # Core services
        "cms_pricing/services/rvu.py",
        
        # Essential test files
        "test_rvu_pipeline_step_by_step.py",  # Most comprehensive test
        "tests/test_rvu_basic.py",  # Unit tests
        "tests/test_rvu_api_contracts.py",  # API tests
    }
    
    # Files to remove (temporary, duplicate, or unused)
    remove_files = [
        # Temporary test files
        "test_rvu_components_individual.py",
        "test_rvu_end_to_end_real.py", 
        "test_rvu_ingester_end_to_end.py",
        "test_rvu_ingester_simple.py",
        
        # Old/unused ingestion files
        "cms_pricing/ingestion/rvu.py",  # Old RVU ingestion
        "cms_pricing/ingestion/rvu_scraper.py",  # Old scraper
        "cms_pricing/ingestion/cms_scraper.py",  # Old scraper
        "cms_pricing/ingestion/cms_zip9_ingestion.py",  # Old ZIP9 ingestion
        
        # Old CSV mapping files
        "cms_pricing/ingestion/authoritative_csv_mappings.py",
        "cms_pricing/ingestion/corrected_csv_mappings.py", 
        "cms_pricing/ingestion/final_csv_mappings.py",
        "cms_pricing/ingestion/updated_csv_mappings.py",
        
        # Old parser files
        "cms_pricing/ingestion/csv_parser.py",
        "cms_pricing/ingestion/hybrid_csv_parser.py",
        "cms_pricing/ingestion/position_csv_parser.py",
        "cms_pricing/ingestion/robust_csv_parser.py",
        
        # Old/unused files
        "cms_pricing/ingestion/geography_notifications.py",
        "cms_pricing/ingestion/geography.py",
        "cms_pricing/ingestion/state_crosswalk.py",
        "cms_pricing/ingestion/zip_handler.py",
        "cms_pricing/ingestion/scheduler.py",
        "cms_pricing/ingestion/mpfs.py",
        "cms_pricing/ingestion/opps.py",
        
        # Old ingestor files
        "cms_pricing/ingestion/ingestors/cms_zip_locality_ingestor_fixed.py",
        "cms_pricing/ingestion/ingestors/cms_zip_locality_ingestor.py",
        "cms_pricing/ingestion/ingestors/test_minimal_ingestor.py",
        
        # Old scripts
        "scrape_rvu_data.py",
        "create_nber_sample.py",
        "test_resolver_with_current_data.py",
        
        # Old analysis files
        "detailed_layout_analysis.py",
        "pprrvu_duplicate_analysis.md",
        "rvu_data_quality_report.json",
        "ZIP9lyout.txt",
    ]
    
    # Files to check if they exist before removing
    files_to_check = [
        "cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py",
    ]
    
    print("📋 CLEANUP PLAN")
    print("-" * 30)
    print(f"✅ Files to keep: {len(keep_files)}")
    print(f"🗑️  Files to remove: {len(remove_files)}")
    print(f"❓ Files to check: {len(files_to_check)}")
    print()
    
    # Check which files actually exist
    existing_files = []
    missing_files = []
    
    for file_path in remove_files:
        if os.path.exists(file_path):
            existing_files.append(file_path)
        else:
            missing_files.append(file_path)
    
    print("📊 FILE STATUS")
    print("-" * 30)
    print(f"✅ Files that exist and will be removed: {len(existing_files)}")
    print(f"❌ Files that don't exist: {len(missing_files)}")
    print()
    
    if existing_files:
        print("🗑️  FILES TO REMOVE:")
        for file_path in existing_files:
            print(f"   📄 {file_path}")
        print()
        
        # Auto-proceed with cleanup
        response = 'y'  # Auto-proceed
        if response.lower() == 'y':
            print("\n🚀 REMOVING FILES...")
            removed_count = 0
            for file_path in existing_files:
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"   ✅ Removed file: {file_path}")
                        removed_count += 1
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        print(f"   ✅ Removed directory: {file_path}")
                        removed_count += 1
                except Exception as e:
                    print(f"   ❌ Failed to remove {file_path}: {e}")
            
            print(f"\n🎉 CLEANUP COMPLETE!")
            print(f"   📊 Files removed: {removed_count}")
        else:
            print("❌ Cleanup cancelled by user")
    else:
        print("✅ No files to remove - cleanup not needed")
    
    # Check files that might be in use
    if files_to_check:
        print("\n❓ FILES TO REVIEW:")
        for file_path in files_to_check:
            if os.path.exists(file_path):
                print(f"   📄 {file_path} - EXISTS (review manually)")
            else:
                print(f"   📄 {file_path} - NOT FOUND")
    
    print("\n📋 KEEP FILES (Core RVU Ingester):")
    for file_path in sorted(keep_files):
        if os.path.exists(file_path):
            print(f"   ✅ {file_path}")
        else:
            print(f"   ❌ {file_path} - MISSING")

if __name__ == "__main__":
    cleanup_rvu_files()
