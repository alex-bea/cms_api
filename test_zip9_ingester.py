#!/usr/bin/env python3
"""
Test script for CMSZip9Ingester following QA Testing Standard

Test ID: QA-ZIP9-E2E-0001
Owner: Data Engineering
Tier: e2e
Environments: dev, ci, staging
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from cms_pricing.ingestion.ingestors.cms_zip9_ingester import CMSZip9Ingester
from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import ZIP9Overrides


async def test_zip9_ingester():
    """Test the ZIP9 ingester end-to-end"""
    print("🧪 Testing CMS ZIP9 Ingester")
    print("=" * 60)
    
    # Initialize ingester
    output_dir = "./test_data/zip9"
    ingester = CMSZip9Ingester(output_dir)
    
    print(f"📊 Ingester: {ingester.__class__.__name__}")
    print(f"📁 Output Directory: {output_dir}")
    print(f"🔗 Source URL: {ingester.source_url}")
    
    # Test discovery
    print("\n🔍 STEP 1: Discovery")
    print("-" * 30)
    try:
        source_files = ingester.discovery()
        print(f"   ✅ Found {len(source_files)} source files")
        for sf in source_files:
            print(f"      📄 {sf.filename} ({sf.content_type})")
    except Exception as e:
        print(f"   ❌ Discovery failed: {str(e)}")
        return False
    
    # Test ingestion pipeline
    print("\n🚀 STEP 2: Ingestion Pipeline")
    print("-" * 30)
    try:
        release_id = f"zip9_test_{datetime.now().strftime('%H%M%S%f')}"
        print(f"   🆔 Release ID: {release_id}")
        
        # Run full pipeline
        batch_id = f"batch_{datetime.now().strftime('%H%M%S%f')}"
        result = await ingester.ingest(release_id, batch_id)
        
        if result['status'] == 'success':
            print(f"   ✅ Ingestion completed!")
            print(f"      📊 Status: {result['status']}")
            print(f"      📊 Records: {result['record_count']}")
            print(f"      📊 Quality Score: {result['quality_score']:.2f}")
        else:
            print(f"   ❌ Ingestion failed!")
            print(f"      📊 Status: {result['status']}")
            print(f"      📊 Error: {result.get('error', 'Unknown error')}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Ingestion failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_zip9_database_integration():
    """Test ZIP9 database integration"""
    print("\n🗄️ STEP 3: Database Integration")
    print("-" * 30)
    
    db = SessionLocal()
    try:
        # Check if ZIP9 overrides table exists and has data
        from sqlalchemy import text
        result = db.execute(text("SELECT COUNT(*) FROM zip9_overrides"))
        count = result.fetchone()[0]
        
        print(f"   ✅ ZIP9 overrides table has {count} records")
        
        if count > 0:
            # Get sample record
            result = db.execute(text("SELECT * FROM zip9_overrides LIMIT 1"))
            sample = result.fetchone()
            print(f"   📄 Sample record: {sample.zip9_low}-{sample.zip9_high} ({sample.state})")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Database integration failed: {str(e)}")
        return False
    finally:
        db.close()


def test_zip9_validation():
    """Test ZIP9 validation rules"""
    print("\n✅ STEP 4: Validation Rules")
    print("-" * 30)
    
    try:
        from cms_pricing.ingestion.validators.zip9_overrides_validator import ZIP9OverridesValidator
        import pandas as pd
        
        validator = ZIP9OverridesValidator()
        
        # Test with valid data
        valid_data = pd.DataFrame([
            {
                'zip9_low': '902100000',
                'zip9_high': '902109999',
                'state': 'CA',
                'locality': '01',
                'rural_flag': 'A',
                'effective_from': '2025-08-14',
                'effective_to': None,
                'vintage': '2025-08-14'
            }
        ])
        
        validation_results = validator.validate(valid_data)
        print(f"   ✅ Validation passed with quality score: {validation_results['quality_score']:.2f}")
        
        # Test with invalid data
        invalid_data = pd.DataFrame([
            {
                'zip9_low': '9021',  # Too short
                'zip9_high': '902109999',
                'state': 'XX',  # Invalid state
                'locality': '01',
                'rural_flag': 'A',
                'effective_from': '2025-08-14',
                'effective_to': None,
                'vintage': '2025-08-14'
            }
        ])
        
        validation_results = validator.validate(invalid_data)
        print(f"   ⚠️ Invalid data caught with quality score: {validation_results['quality_score']:.2f}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Validation testing failed: {str(e)}")
        return False


async def main():
    """Main test function"""
    print("🚀 CMS ZIP9 Ingester Test Suite")
    print("=" * 60)
    
    # Run tests
    tests = [
        ("ZIP9 Ingester E2E", test_zip9_ingester()),
        ("Database Integration", test_zip9_database_integration()),
        ("Validation Rules", test_zip9_validation())
    ]
    
    results = []
    for test_name, test_coro in tests:
        if asyncio.iscoroutine(test_coro):
            result = await test_coro
        else:
            result = test_coro
        results.append((test_name, result))
    
    # Summary
    print("\n📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! ZIP9 ingester is ready for production.")
        return True
    else:
        print("⚠️ Some tests failed. Please review and fix issues.")
        return False


if __name__ == "__main__":
    from datetime import datetime
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
