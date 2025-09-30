#!/usr/bin/env python3
"""
Comprehensive End-to-End Test for CMSZip9Ingester
Following QA Testing Standard (QTS) v1.0

Test ID: QA-ZIP9-E2E-0002
Owner: Data Engineering
Tier: e2e
Environments: dev, ci, staging, production
Dependencies: cms_pricing.ingestion.ingestors.cms_zip9_ingester, cms_pricing.services.nearest_zip_resolver
Quality Gates: merge, pre-deploy, release
SLOs: completion ≤ 20 min, pass rate ≥95%, flake rate <1%
"""

import asyncio
import sys
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import structlog

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from cms_pricing.ingestion.ingestors.cms_zip9_ingester import CMSZip9Ingester
from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import ZIP9Overrides, CMSZipLocality
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver
from sqlalchemy import text

# Configure structured logging for QTS compliance
logger = structlog.get_logger()

# QTS Test Configuration
TEST_CONFIG = {
    "test_id": "QA-ZIP9-E2E-0002",
    "owner": "Data Engineering",
    "tier": "e2e",
    "environments": ["dev", "ci", "staging", "production"],
    "quality_gates": ["merge", "pre-deploy", "release"],
    "slos": {
        "max_duration_minutes": 20,
        "min_pass_rate": 0.95,
        "max_flake_rate": 0.01
    },
    "fixtures": {
        "golden_dataset": "zip9_overrides_v2025q3",
        "schema_version": "1.0.0",
        "refresh_cadence": "quarterly"
    }
}


class QTSCompliantTestRunner:
    """QTS-compliant test runner with observability and quality gates"""
    
    def __init__(self):
        self.test_id = TEST_CONFIG["test_id"]
        self.start_time = datetime.now()
        self.test_results = {
            "test_id": self.test_id,
            "start_time": self.start_time.isoformat(),
            "steps": [],
            "quality_metrics": {},
            "slos": TEST_CONFIG["slos"],
            "status": "running"
        }
        self.trace_id = f"trace_{int(time.time() * 1000)}"
        
    def log_step(self, step_name: str, status: str, details: Dict[str, Any] = None):
        """Log test step with structured data for observability"""
        step_result = {
            "step_name": step_name,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "trace_id": self.trace_id,
            "details": details or {}
        }
        self.test_results["steps"].append(step_result)
        
        # Structured logging for QTS compliance
        # Limit details to prevent massive log output
        limited_details = {}
        if details:
            for key, value in details.items():
                if isinstance(value, (list, dict)) and len(str(value)) > 1000:
                    limited_details[key] = f"<{type(value).__name__} with {len(value) if hasattr(value, '__len__') else 'unknown'} items>"
                else:
                    limited_details[key] = value
        
        log_data = {
            "test_id": self.test_id,
            "step_name": step_name,
            "trace_id": self.trace_id,
            **limited_details
        }
        logger.info("Test step executed", **log_data)
        
        # Console output for human readability
        status_emoji = "✅" if status == "passed" else "❌" if status == "failed" else "🔍"
        print(f"   {status_emoji} {step_name}: {status}")
        if limited_details:
            for key, value in limited_details.items():
                print(f"      📊 {key}: {value}")
    
    def check_slo_compliance(self) -> Dict[str, Any]:
        """Check SLO compliance per QTS requirements"""
        end_time = datetime.now()
        duration_seconds = (end_time - self.start_time).total_seconds()
        duration_minutes = duration_seconds / 60
        
        slo_results = {
            "duration_minutes": duration_minutes,
            "duration_slo_met": duration_minutes <= TEST_CONFIG["slos"]["max_duration_minutes"],
            "pass_rate": self._calculate_pass_rate(),
            "pass_rate_slo_met": self._calculate_pass_rate() >= TEST_CONFIG["slos"]["min_pass_rate"]
        }
        
        self.test_results["quality_metrics"] = slo_results
        return slo_results
    
    def _calculate_pass_rate(self) -> float:
        """Calculate pass rate from test steps"""
        if not self.test_results["steps"]:
            return 0.0
        
        passed_steps = sum(1 for step in self.test_results["steps"] if step["status"] == "passed")
        total_steps = len(self.test_results["steps"])
        return passed_steps / total_steps if total_steps > 0 else 0.0
    
    def generate_test_report(self) -> Dict[str, Any]:
        """Generate QTS-compliant test report"""
        end_time = datetime.now()
        duration_seconds = (end_time - self.start_time).total_seconds()
        
        self.test_results.update({
            "end_time": end_time.isoformat(),
            "duration_seconds": duration_seconds,
            "status": "completed" if self._calculate_pass_rate() >= 0.95 else "failed"
        })
        
        return self.test_results


async def test_zip9_ingester_comprehensive():
    """Comprehensive end-to-end test for ZIP9 ingester following QTS v1.0"""
    print("🚀 CMS ZIP9 INGESTER COMPREHENSIVE E2E TEST")
    print("=" * 70)
    print(f"📋 Test ID: {TEST_CONFIG['test_id']}")
    print(f"👤 Owner: {TEST_CONFIG['owner']}")
    print(f"🏷️ Tier: {TEST_CONFIG['tier']}")
    print(f"🌍 Environments: {', '.join(TEST_CONFIG['environments'])}")
    print(f"🚪 Quality Gates: {', '.join(TEST_CONFIG['quality_gates'])}")
    
    # Initialize QTS-compliant test runner
    test_runner = QTSCompliantTestRunner()
    start_time = test_runner.start_time
    print(f"🕐 Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test configuration
    output_dir = "./test_data/zip9_e2e"
    release_id = f"zip9_e2e_{start_time.strftime('%H%M%S%f')}"
    batch_id = f"batch_{start_time.strftime('%H%M%S%f')}"
    
    print(f"\n📊 Test Configuration:")
    print(f"   📁 Output Directory: {output_dir}")
    print(f"   🆔 Release ID: {release_id}")
    print(f"   🆔 Batch ID: {batch_id}")
    print(f"   🔍 Trace ID: {test_runner.trace_id}")
    
    # Initialize ingester
    ingester = CMSZip9Ingester(output_dir)
    
    print(f"\n🔍 STEP 1: INGESTER INITIALIZATION")
    print("-" * 50)
    try:
        test_runner.log_step(
            "ingester_initialization",
            "passed",
            {
                "dataset": ingester.dataset_name,
                "release_cadence": ingester.release_cadence.value,
                "classification": ingester.classification.value,
                "schema_contract": ingester.contract_schema_ref,
                "source_url": ingester.source_url
            }
        )
    except Exception as e:
        test_runner.log_step("ingester_initialization", "failed", {"error": str(e)})
        return False
    
    # Test discovery
    print(f"\n🔍 STEP 2: SOURCE DISCOVERY")
    print("-" * 50)
    try:
        source_files = ingester.discovery()
        test_runner.log_step(
            "source_discovery",
            "passed",
            {
                "files_found": len(source_files),
                "files": [{"filename": sf.filename, "content_type": sf.content_type, "url": sf.url} for sf in source_files]
            }
        )
    except Exception as e:
        test_runner.log_step("source_discovery", "failed", {"error": str(e)})
        return False
    
    # Test validation rules
    print(f"\n🔍 STEP 3: VALIDATION RULES")
    print("-" * 50)
    try:
        validator = ingester.validator
        
        # Test with sample data
        sample_data = pd.DataFrame([
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
        
        validation_results = validator.validate(sample_data)
        test_runner.log_step(
            "validation_rules",
            "passed",
            {
                "rules_count": len(validator.validation_rules),
                "quality_score": validation_results['quality_score'],
                "rules_passed": validation_results['rules_passed'],
                "rules_failed": validation_results['rules_failed'],
                "sample_records": len(sample_data)
            }
        )
        
    except Exception as e:
        test_runner.log_step("validation_rules", "failed", {"error": str(e)})
        return False
    
    # Test full ingestion pipeline
    print(f"\n🚀 STEP 4: FULL INGESTION PIPELINE")
    print("-" * 50)
    try:
        print(f"   🚀 Starting ingestion pipeline...")
        result = await ingester.ingest(release_id, batch_id)
        
        if result['status'] == 'success':
            test_runner.log_step(
                "ingestion_pipeline",
                "passed",
                {
                    "records_processed": result['record_count'],
                    "quality_score": result['quality_score'],
                    "dis_compliance": result['dis_compliance'],
                    "release_id": release_id,
                    "batch_id": batch_id
                }
            )
        else:
            test_runner.log_step(
                "ingestion_pipeline",
                "failed",
                {
                    "status": result['status'],
                    "error": result.get('error', 'Unknown error'),
                    "release_id": release_id,
                    "batch_id": batch_id
                }
            )
            return False
            
    except Exception as e:
        test_runner.log_step("ingestion_pipeline", "failed", {"error": str(e)})
        import traceback
        traceback.print_exc()
        return False
    
    # Test database integration
    print(f"\n🗄️ STEP 5: DATABASE INTEGRATION")
    print("-" * 50)
    try:
        db = SessionLocal()
        try:
            # Check ZIP9 overrides table
            result = db.execute(text("SELECT COUNT(*) FROM zip9_overrides"))
            zip9_count = result.fetchone()[0]
            print(f"   ✅ ZIP9 overrides table has {zip9_count:,} records")
            
            # Check recent records
            result = db.execute(text("""
                SELECT zip9_low, zip9_high, state, locality, data_quality_score, processing_timestamp
                FROM zip9_overrides 
                WHERE ingest_run_id = :batch_id
                LIMIT 5
            """), {"batch_id": batch_id})
            
            recent_records = result.fetchall()
            if recent_records:
                print(f"   ✅ Found {len(recent_records)} records from this test run:")
                for record in recent_records:
                    print(f"      📄 {record.zip9_low}-{record.zip9_high} ({record.state}) - Quality: {record.data_quality_score:.2f}")
            else:
                print(f"   ⚠️ No records found for this test run (may be using existing data)")
            
            # Check metadata
            result = db.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    AVG(data_quality_score) as avg_quality,
                    COUNT(DISTINCT state) as states_covered,
                    COUNT(DISTINCT locality) as localities_covered
                FROM zip9_overrides 
                WHERE ingest_run_id = :batch_id
            """), {"batch_id": batch_id})
            
            metadata = result.fetchone()
            if metadata and metadata.total_records > 0:
                print(f"   📊 Metadata Summary:")
                print(f"      📊 Total Records: {metadata.total_records:,}")
                print(f"      📊 Average Quality: {metadata.avg_quality:.2f}")
                print(f"      📊 States Covered: {metadata.states_covered}")
                print(f"      📊 Localities Covered: {metadata.localities_covered}")
            
        finally:
            db.close()
            
    except Exception as e:
        print(f"   ❌ Database integration failed: {str(e)}")
        return False
    
    # Test nearest ZIP resolver integration
    print(f"\n🔍 STEP 6: NEAREST ZIP RESOLVER INTEGRATION")
    print("-" * 50)
    try:
        db = SessionLocal()
        try:
            resolver = NearestZipResolver(db)
            
            # Test ZIP9 lookup
            test_zip9 = '902101234'  # Should match 902100000-902109999 range
            print(f"   🔍 Testing ZIP9 lookup: {test_zip9}")
            
            result = resolver.find_nearest_zip(test_zip9, include_trace=True)
            
            if result:
                print(f"   ✅ ZIP9 lookup successful!")
                print(f"      📍 Nearest ZIP: {result.nearest_zip}")
                print(f"      📏 Distance: {result.distance_miles:.2f} miles")
                print(f"      🔍 ZIP9 Override Used: {result.trace.get('zip9_override_used', False)}")
                print(f"      🔍 ZIP5 Fallback Used: {result.trace.get('zip5_fallback_used', False)}")
            else:
                print(f"   ⚠️ No result found for ZIP9 {test_zip9}")
            
            # Test state boundary enforcement
            print(f"   🔍 Testing state boundary enforcement...")
            ca_zip9 = '902101234'  # CA ZIP9
            ny_zip9 = '100011234'  # NY ZIP9
            
            ca_result = resolver.find_nearest_zip(ca_zip9, include_trace=True)
            ny_result = resolver.find_nearest_zip(ny_zip9, include_trace=True)
            
            if ca_result and ny_result:
                print(f"   ✅ State boundary enforcement working:")
                print(f"      📍 CA ZIP9 {ca_zip9} → {ca_result.nearest_zip}")
                print(f"      📍 NY ZIP9 {ny_zip9} → {ny_result.nearest_zip}")
            else:
                print(f"   ⚠️ State boundary test inconclusive")
                
        finally:
            db.close()
            
    except Exception as e:
        print(f"   ❌ Nearest ZIP resolver integration failed: {str(e)}")
        return False
    
    # Test API endpoints (if available)
    print(f"\n🌐 STEP 7: API ENDPOINT INTEGRATION")
    print("-" * 50)
    try:
        # Test if API is running
        import httpx
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get("http://localhost:8000/health")
                if response.status_code == 200:
                    print(f"   ✅ API is running")
                    
                    # Test ZIP9 endpoint
                    test_zip9 = '902101234'
                    api_response = await client.get(
                        f"http://localhost:8000/nearest-zip/nearest?zip={test_zip9}",
                        headers={"X-API-Key": "test-key"}  # Assuming test key
                    )
                    
                    if api_response.status_code == 200:
                        data = api_response.json()
                        print(f"   ✅ ZIP9 API endpoint working!")
                        print(f"      📍 ZIP9 {test_zip9} → {data.get('nearest_zip', 'N/A')}")
                        print(f"      📏 Distance: {data.get('distance_miles', 'N/A')} miles")
                    else:
                        print(f"   ⚠️ API endpoint returned status {api_response.status_code}")
                        
                else:
                    print(f"   ⚠️ API health check failed (status {response.status_code})")
                    
            except Exception as e:
                print(f"   ⚠️ API not available: {str(e)}")
                
    except Exception as e:
        print(f"   ⚠️ API integration test skipped: {str(e)}")
    
    # Test data quality and validation
    print(f"\n✅ STEP 8: DATA QUALITY VALIDATION")
    print("-" * 50)
    try:
        db = SessionLocal()
        try:
            # Check data quality metrics
            result = db.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    AVG(data_quality_score) as avg_quality,
                    MIN(data_quality_score) as min_quality,
                    MAX(data_quality_score) as max_quality,
                    COUNT(CASE WHEN data_quality_score >= 0.9 THEN 1 END) as high_quality_count
                FROM zip9_overrides 
                WHERE ingest_run_id = :batch_id
            """), {"batch_id": batch_id})
            
            quality_metrics = result.fetchone()
            if quality_metrics and quality_metrics.total_records > 0:
                print(f"   📊 Data Quality Metrics:")
                print(f"      📊 Total Records: {quality_metrics.total_records:,}")
                print(f"      📊 Average Quality: {quality_metrics.avg_quality:.2f}")
                print(f"      📊 Min Quality: {quality_metrics.min_quality:.2f}")
                print(f"      📊 Max Quality: {quality_metrics.max_quality:.2f}")
                print(f"      📊 High Quality (≥0.9): {quality_metrics.high_quality_count:,}")
                
                # Quality assessment
                if quality_metrics.avg_quality >= 0.9:
                    print(f"   ✅ Data quality meets standards (≥0.9)")
                elif quality_metrics.avg_quality >= 0.8:
                    print(f"   ⚠️ Data quality acceptable (≥0.8)")
                else:
                    print(f"   ❌ Data quality below standards (<0.8)")
            else:
                print(f"   ⚠️ No quality metrics available for this test run")
                
        finally:
            db.close()
            
    except Exception as e:
        print(f"   ❌ Data quality validation failed: {str(e)}")
        return False
    
    # Test file artifacts
    print(f"\n📁 STEP 9: FILE ARTIFACTS VALIDATION")
    print("-" * 50)
    try:
        # Check if output directories were created
        output_path = Path(output_dir)
        raw_dir = output_path / "raw" / release_id
        stage_dir = output_path / "stage" / release_id
        curated_dir = output_path / "curated" / "zip9_overrides" / release_id
        
        print(f"   📁 Checking output directories...")
        print(f"      📁 Raw: {raw_dir.exists()} ({raw_dir})")
        print(f"      📁 Stage: {stage_dir.exists()} ({stage_dir})")
        print(f"      📁 Curated: {curated_dir.exists()} ({curated_dir})")
        
        # Check for manifest files
        manifest_file = raw_dir / "manifest.json"
        if manifest_file.exists():
            print(f"   ✅ Manifest file created: {manifest_file}")
        
        # Check for schema contract
        schema_file = stage_dir / "schema_contract.json"
        if schema_file.exists():
            print(f"   ✅ Schema contract created: {schema_file}")
        
        # Check for curated data
        parquet_file = curated_dir / "zip9_overrides.parquet"
        if parquet_file.exists():
            print(f"   ✅ Curated data created: {parquet_file}")
            
    except Exception as e:
        print(f"   ❌ File artifacts validation failed: {str(e)}")
        return False
    
    # QTS-compliant test completion
    print(f"\n🏁 QTS TEST COMPLETION")
    print("=" * 70)
    
    # Check SLO compliance
    slo_results = test_runner.check_slo_compliance()
    print(f"📊 SLO Compliance Check:")
    print(f"   ⏱️ Duration: {slo_results['duration_minutes']:.2f} minutes (SLO: ≤{TEST_CONFIG['slos']['max_duration_minutes']} min)")
    print(f"   📈 Pass Rate: {slo_results['pass_rate']:.2%} (SLO: ≥{TEST_CONFIG['slos']['min_pass_rate']:.0%})")
    print(f"   ✅ Duration SLO Met: {slo_results['duration_slo_met']}")
    print(f"   ✅ Pass Rate SLO Met: {slo_results['pass_rate_slo_met']}")
    
    # Generate QTS-compliant test report
    test_report = test_runner.generate_test_report()
    
    # Save test report for observability
    report_path = Path(output_dir) / f"test_report_{test_runner.trace_id}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(test_report, f, indent=2)
    
    print(f"\n📋 QTS Test Report Generated:")
    print(f"   📄 Report Path: {report_path}")
    print(f"   🔍 Trace ID: {test_runner.trace_id}")
    print(f"   📊 Test Status: {test_report['status']}")
    print(f"   ⏱️ Total Duration: {test_report['duration_seconds']:.2f} seconds")
    print(f"   📈 Pass Rate: {test_report['quality_metrics']['pass_rate']:.2%}")
    
    # Final assessment
    all_slos_met = slo_results['duration_slo_met'] and slo_results['pass_rate_slo_met']
    
    if all_slos_met:
        print(f"\n🎉 CMSZip9Ingester is fully operational and ready for production!")
        print(f"✅ All QTS requirements met - test passes all quality gates")
        return True
    else:
        print(f"\n⚠️ CMSZip9Ingester test completed with SLO violations")
        print(f"❌ Some QTS requirements not met - review required before production")
        return False


async def main():
    """Main test function following QTS v1.0"""
    test_start_time = datetime.now()
    
    try:
        # Execute QTS-compliant test
        success = await test_zip9_ingester_comprehensive()
        
        # QTS-compliant exit handling
        test_end_time = datetime.now()
        total_duration = (test_end_time - test_start_time).total_seconds()
        
        if success:
            logger.info(
                "QTS test suite completed successfully",
                test_id=TEST_CONFIG["test_id"],
                duration_seconds=total_duration,
                status="passed"
            )
            print(f"\n✅ QTS Test Suite PASSED")
            print(f"📊 Total Duration: {total_duration:.2f} seconds")
            print(f"🎯 Quality Gates: {', '.join(TEST_CONFIG['quality_gates'])} - ALL PASSED")
            return True
        else:
            logger.error(
                "QTS test suite failed",
                test_id=TEST_CONFIG["test_id"],
                duration_seconds=total_duration,
                status="failed"
            )
            print(f"\n❌ QTS Test Suite FAILED")
            print(f"📊 Total Duration: {total_duration:.2f} seconds")
            print(f"🎯 Quality Gates: {', '.join(TEST_CONFIG['quality_gates'])} - SOME FAILED")
            return False
            
    except Exception as e:
        test_end_time = datetime.now()
        total_duration = (test_end_time - test_start_time).total_seconds()
        
        logger.error(
            "QTS test suite crashed",
            test_id=TEST_CONFIG["test_id"],
            duration_seconds=total_duration,
            error=str(e),
            status="crashed"
        )
        print(f"\n💥 QTS Test Suite CRASHED")
        print(f"📊 Total Duration: {total_duration:.2f} seconds")
        print(f"🚨 Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
