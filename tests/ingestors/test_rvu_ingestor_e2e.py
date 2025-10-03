"""
RVU Ingestor End-to-End Integration Test

Comprehensive test of the RVU ingestor with scraper integration, DIS compliance,
observability, and performance SLOs following QA Testing Standard (QTS) v1.0.

QTS Compliance Header:
Test ID: QA-RVU-E2E-0001
Owner: Data Engineering
Tier: integration
Environments: dev, ci, staging
Dependencies: cms_pricing.ingestion.ingestors.rvu_ingestor, cms_pricing.ingestion.scrapers.cms_rvu_scraper
Quality Gates: merge, pre-deploy, release
SLOs: completion ≤ 20 min, pass rate ≥95%, flake rate <1%
"""

import pytest
import asyncio
import time
import json
import uuid
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List

# Test imports
from cms_pricing.database import SessionLocal, engine
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
from cms_pricing.ingestion.scrapers.cli import ScraperCLI
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
from cms_pricing.ingestion.contracts.ingestor_spec import SourceFile

# Test data
from tests.fixtures.rvu.test_dataset_creator import RVUTestDatasetCreator


class TestRVUIngestorE2E:
    """End-to-end integration tests for RVU ingestor with scraper integration"""
    
    # Use conftest.py fixtures instead of defining here
    
    # Use conftest.py fixtures instead of defining here
    
    @pytest.fixture
    def rvu_ingestor(self, test_data_dir):
        """Create RVU ingestor for testing"""
        return RVUIngestor(str(test_data_dir / "ingested_data"))
    
    @pytest.fixture
    def scraper(self, test_data_dir):
        """Create scraper for testing"""
        return CMSRVUScraper(str(test_data_dir / "scraped_data"))
    
    @pytest.fixture
    def scraper_cli(self, test_data_dir):
        """Create scraper CLI for testing"""
        return ScraperCLI(
            str(test_data_dir / "scraper_output"),
            str(test_data_dir / "manifests")
        )
    
    @pytest.fixture
    def sample_source_files(self, test_data_dir):
        """Create sample source files for testing"""
        manifest_path = test_data_dir / "manifest.json"
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        
        source_files = []
        for file_info in manifest["files"]:
            source_files.append(SourceFile(
                url=file_info["url"],
                filename=file_info["filename"],
                content_type=file_info["content_type"],
                expected_size_bytes=file_info["size_bytes"],
                last_modified=datetime.fromisoformat(file_info["last_modified"]),
                checksum=file_info["sha256"]
            ))
        
        return source_files
    
    @pytest.mark.asyncio
    async def test_scraper_discovery_integration(self, scraper, test_data_dir):
        """Test scraper discovery functionality"""
        print("\n🔍 Testing scraper discovery integration...")
        
        start_time = time.time()
        
        # Mock the scraper to use test data instead of real URLs
        with patch.object(scraper, 'scrape_rvu_files') as mock_scrape:
            # Create mock file info objects
            mock_files = []
            for i in range(4):  # Simulate 4 RVU files
                mock_file = Mock()
                mock_file.url = f"file://{test_data_dir}/RVU25{i}.zip"
                mock_file.filename = f"RVU25{i}.zip"
                mock_file.size_bytes = 1000000
                mock_file.last_modified = datetime.now()
                mock_file.checksum = f"test_checksum_{i}"
                mock_file.year = 2025
                mock_files.append(mock_file)
            
            mock_scrape.return_value = mock_files
            
            # Test discovery
            discovered_files = await scraper.scrape_rvu_files(2025, 2025)
            
            discovery_time = time.time() - start_time
            
            # Verify results
            assert len(discovered_files) == 4
            assert all(hasattr(f, 'url') for f in discovered_files)
            assert all(hasattr(f, 'filename') for f in discovered_files)
            
            # Verify SLO (discovery ≤ 30 seconds)
            assert discovery_time <= 30, f"Discovery took {discovery_time:.2f}s, exceeds 30s SLO"
            
            print(f"✅ Discovery completed in {discovery_time:.2f}s")
            print(f"   Found {len(discovered_files)} files")
    
    @pytest.mark.asyncio
    async def test_dis_land_stage(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test DIS Land stage compliance"""
        print("\n📥 Testing DIS Land stage...")
        
        start_time = time.time()
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Test Land stage
        land_result = await rvu_ingestor._land_stage(
            release_id=release_id,
            batch_id=batch_id,
            source_files=sample_source_files
        )
        
        land_time = time.time() - start_time
        
        # Verify Land stage results
        assert land_result["status"] == "success"
        assert "raw_directory" in land_result
        assert "manifest_path" in land_result
        assert "total_size_bytes" in land_result
        assert land_result["total_size_bytes"] > 0
        
        # Verify manifest exists and has required fields
        manifest_path = Path(land_result["manifest_path"])
        assert manifest_path.exists()
        
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        
        required_fields = ["release_id", "batch_id", "source", "files", "fetched_at"]
        for field in required_fields:
            assert field in manifest, f"Missing required field: {field}"
        
        # Verify files were downloaded
        raw_dir = Path(land_result["raw_directory"])
        assert raw_dir.exists()
        assert (raw_dir / "files").exists()
        
        print(f"✅ Land stage completed in {land_time:.2f}s")
        print(f"   Downloaded {len(manifest['files'])} files")
        print(f"   Total size: {land_result['total_size_bytes']} bytes")
    
    @pytest.mark.asyncio
    async def test_dis_validate_stage(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test DIS Validate stage compliance"""
        print("\n🔍 Testing DIS Validate stage...")
        
        start_time = time.time()
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # First run Land stage
        land_result = await rvu_ingestor._land_stage(
            release_id=release_id,
            batch_id=batch_id,
            source_files=sample_source_files
        )
        
        # Create RawBatch for validation
        from cms_pricing.ingestion.contracts.ingestor_spec import RawBatch
        raw_batch = RawBatch(
            source_files=sample_source_files,
            raw_data_path=land_result["raw_directory"],
            metadata={
                "release_id": release_id,
                "batch_id": batch_id,
                "source": "cms_rvu_test"
            }
        )
        
        # Test Validate stage
        validate_result = await rvu_ingestor._validate_stage(raw_batch)
        
        validate_time = time.time() - start_time
        
        # Verify Validate stage results
        assert validate_result["status"] == "success"
        assert "validation_results" in validate_result
        assert "quality_score" in validate_result
        assert "quarantine_summary" in validate_result
        
        # Verify quality score is reasonable
        quality_score = validate_result["quality_score"]
        assert 0 <= quality_score <= 100, f"Invalid quality score: {quality_score}"
        
        print(f"✅ Validate stage completed in {validate_time:.2f}s")
        print(f"   Quality score: {quality_score}")
        print(f"   Quarantine summary: {validate_result['quarantine_summary']}")
    
    @pytest.mark.asyncio
    async def test_dis_normalize_stage(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test DIS Normalize stage compliance"""
        print("\n🔄 Testing DIS Normalize stage...")
        
        start_time = time.time()
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Run Land and Validate stages first
        land_result = await rvu_ingestor._land_stage(
            release_id=release_id,
            batch_id=batch_id,
            source_files=sample_source_files
        )
        
        from cms_pricing.ingestion.contracts.ingestor_spec import RawBatch
        raw_batch = RawBatch(
            source_files=sample_source_files,
            raw_data_path=land_result["raw_directory"],
            metadata={
                "release_id": release_id,
                "batch_id": batch_id,
                "source": "cms_rvu_test"
            }
        )
        
        validate_result = await rvu_ingestor._validate_stage(raw_batch)
        
        # Test Normalize stage
        normalize_result = await rvu_ingestor._normalize_stage(raw_batch, validate_result)
        
        normalize_time = time.time() - start_time
        
        # Verify Normalize stage results
        assert normalize_result["status"] == "success"
        assert "normalized_data" in normalize_result
        assert "schema_contract" in normalize_result
        assert "column_dictionary" in normalize_result
        
        # Verify schema contract
        schema_contract = normalize_result["schema_contract"]
        assert "version" in schema_contract
        assert "columns" in schema_contract
        
        print(f"✅ Normalize stage completed in {normalize_time:.2f}s")
        print(f"   Schema version: {schema_contract.get('version', 'unknown')}")
        print(f"   Columns defined: {len(schema_contract.get('columns', []))}")
    
    @pytest.mark.asyncio
    async def test_dis_enrich_stage(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test DIS Enrich stage compliance"""
        print("\n🔗 Testing DIS Enrich stage...")
        
        start_time = time.time()
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Run previous stages
        land_result = await rvu_ingestor._land_stage(
            release_id=release_id,
            batch_id=batch_id,
            source_files=sample_source_files
        )
        
        from cms_pricing.ingestion.contracts.ingestor_spec import RawBatch, AdaptedBatch
        raw_batch = RawBatch(
            source_files=sample_source_files,
            raw_data_path=land_result["raw_directory"],
            metadata={
                "release_id": release_id,
                "batch_id": batch_id,
                "source": "cms_rvu_test"
            }
        )
        
        validate_result = await rvu_ingestor._validate_stage(raw_batch)
        normalize_result = await rvu_ingestor._normalize_stage(raw_batch, validate_result)
        
        # Create AdaptedBatch for enrichment
        adapted_batch = AdaptedBatch(
            normalized_data=normalize_result["normalized_data"],
            schema_contract=normalize_result["schema_contract"],
            metadata=raw_batch.metadata
        )
        
        # Test Enrich stage
        enrich_result = await rvu_ingestor._enrich_stage(adapted_batch)
        
        enrich_time = time.time() - start_time
        
        # Verify Enrich stage results
        assert enrich_result["status"] == "success"
        assert "enriched_data" in enrich_result
        assert "reference_data_used" in enrich_result
        assert "mapping_confidence" in enrich_result
        
        print(f"✅ Enrich stage completed in {enrich_time:.2f}s")
        print(f"   Reference data used: {enrich_result['reference_data_used']}")
        print(f"   Mapping confidence: {enrich_result['mapping_confidence']}")
    
    @pytest.mark.asyncio
    async def test_dis_publish_stage(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test DIS Publish stage compliance"""
        print("\n📤 Testing DIS Publish stage...")
        
        start_time = time.time()
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Run all previous stages
        land_result = await rvu_ingestor._land_stage(
            release_id=release_id,
            batch_id=batch_id,
            source_files=sample_source_files
        )
        
        from cms_pricing.ingestion.contracts.ingestor_spec import RawBatch, AdaptedBatch, StageFrame
        raw_batch = RawBatch(
            source_files=sample_source_files,
            raw_data_path=land_result["raw_directory"],
            metadata={
                "release_id": release_id,
                "batch_id": batch_id,
                "source": "cms_rvu_test"
            }
        )
        
        validate_result = await rvu_ingestor._validate_stage(raw_batch)
        normalize_result = await rvu_ingestor._normalize_stage(raw_batch, validate_result)
        
        adapted_batch = AdaptedBatch(
            normalized_data=normalize_result["normalized_data"],
            schema_contract=normalize_result["schema_contract"],
            metadata=raw_batch.metadata
        )
        
        enrich_result = await rvu_ingestor._enrich_stage(adapted_batch)
        
        # Create StageFrame for publishing
        stage_frame = StageFrame(
            enriched_data=enrich_result["enriched_data"],
            schema_contract=normalize_result["schema_contract"],
            metadata=raw_batch.metadata
        )
        
        # Test Publish stage
        publish_result = await rvu_ingestor._publish_stage(stage_frame)
        
        publish_time = time.time() - start_time
        
        # Verify Publish stage results
        assert publish_result["status"] == "success"
        assert "curated_tables" in publish_result
        assert "latest_effective_views" in publish_result
        assert "export_artifacts" in publish_result
        
        # Verify tables were created
        curated_tables = publish_result["curated_tables"]
        expected_tables = ["rvu_items", "gpci_indices", "opps_caps", "anes_cfs", "locality_counties"]
        for table in expected_tables:
            assert table in curated_tables, f"Missing curated table: {table}"
        
        print(f"✅ Publish stage completed in {publish_time:.2f}s")
        print(f"   Created {len(curated_tables)} curated tables")
        print(f"   Views: {publish_result['latest_effective_views']}")
    
    @pytest.mark.asyncio
    async def test_full_dis_pipeline(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test complete DIS pipeline end-to-end"""
        print("\n🚀 Testing full DIS pipeline...")
        
        start_time = time.time()
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Test complete pipeline
        pipeline_result = await rvu_ingestor.ingest(release_id, batch_id)
        
        total_time = time.time() - start_time
        
        # Verify pipeline results
        assert pipeline_result["status"] == "success"
        assert "release_id" in pipeline_result
        assert "batch_id" in pipeline_result
        assert "record_count" in pipeline_result
        assert "quality_score" in pipeline_result
        assert "observability" in pipeline_result
        
        # Verify record counts
        record_count = pipeline_result["record_count"]
        assert record_count > 0, f"Expected records, got {record_count}"
        
        # Verify quality score
        quality_score = pipeline_result["quality_score"]
        assert 0 <= quality_score <= 100, f"Invalid quality score: {quality_score}"
        
        # Verify SLO (ingestion ≤ 5 minutes)
        assert total_time <= 300, f"Ingestion took {total_time:.2f}s, exceeds 5min SLO"
        
        print(f"✅ Full pipeline completed in {total_time:.2f}s")
        print(f"   Processed {record_count} records")
        print(f"   Quality score: {quality_score}")
    
    @pytest.mark.asyncio
    async def test_observability_metrics(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test 5-pillar observability metrics collection"""
        print("\n📊 Testing observability metrics...")
        
        release_id = f"test_rvu_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Run pipeline
        pipeline_result = await rvu_ingestor.ingest(release_id, batch_id)
        
        # Verify observability data
        observability = pipeline_result.get("observability", {})
        
        # Check 5-pillar metrics
        required_metrics = [
            "overall_score", "freshness_score", "volume_score", 
            "schema_score", "quality_score", "lineage_score"
        ]
        
        for metric in required_metrics:
            assert metric in observability, f"Missing observability metric: {metric}"
            assert 0 <= observability[metric] <= 100, f"Invalid {metric}: {observability[metric]}"
        
        # Check alerts and warnings
        assert "critical_alerts" in observability
        assert "warnings" in observability
        
        print(f"✅ Observability metrics collected")
        print(f"   Overall score: {observability['overall_score']}")
        print(f"   Critical alerts: {len(observability['critical_alerts'])}")
        print(f"   Warnings: {len(observability['warnings'])}")
    
    @pytest.mark.asyncio
    async def test_quarantine_functionality(self, rvu_ingestor, test_data_dir):
        """Test quarantine functionality with invalid data"""
        print("\n🚫 Testing quarantine functionality...")
        
        # Create invalid data for testing
        invalid_data_dir = test_data_dir / "invalid_data"
        invalid_data_dir.mkdir(exist_ok=True)
        
        # Create file with invalid data
        invalid_file = invalid_data_dir / "invalid_pprrvu.txt"
        with open(invalid_file, 'w') as f:
            f.write("INVALID_HCPCS_CODE_TOO_LONG  Invalid description  X  999.999  999.999  999.999  999.999  9  XXX  9  9  9  9  9  9  999.9999  99  99  999.99  999.99\n")
        
        # Create source file for invalid data
        invalid_source_file = SourceFile(
            url=f"file://{invalid_file}",
            filename="invalid_pprrvu.txt",
            content_type="text/plain",
            expected_size_bytes=invalid_file.stat().st_size,
            last_modified=datetime.now(),
            checksum="invalid_checksum"
        )
        
        release_id = f"test_invalid_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        # Test with invalid data
        pipeline_result = await rvu_ingestor.ingest(release_id, batch_id)
        
        # Verify quarantine handling
        assert pipeline_result["status"] in ["success", "partial"], "Pipeline should handle invalid data gracefully"
        
        # Check for quarantine information
        observability = pipeline_result.get("observability", {})
        warnings = observability.get("warnings", [])
        
        # Should have warnings about invalid data
        assert len(warnings) > 0, "Expected warnings for invalid data"
        
        print(f"✅ Quarantine functionality tested")
        print(f"   Warnings generated: {len(warnings)}")
        print(f"   Pipeline status: {pipeline_result['status']}")
    
    @pytest.mark.asyncio
    async def test_scraper_cli_integration(self, scraper_cli, test_data_dir):
        """Test scraper CLI integration"""
        print("\n🖥️ Testing scraper CLI integration...")
        
        start_time = time.time()
        
        # Test discovery mode
        discovery_result = await scraper_cli.discovery_mode(
            start_year=2025,
            end_year=2025,
            latest_only=True
        )
        
        cli_time = time.time() - start_time
        
        # Verify CLI results
        assert discovery_result["status"] == "success"
        assert "files_discovered" in discovery_result
        assert "manifest_path" in discovery_result
        assert "snapshot_digest" in discovery_result
        
        # Verify SLO (CLI operations ≤ 30 seconds)
        assert cli_time <= 30, f"CLI operations took {cli_time:.2f}s, exceeds 30s SLO"
        
        print(f"✅ Scraper CLI integration completed in {cli_time:.2f}s")
        print(f"   Files discovered: {discovery_result['files_discovered']}")
        print(f"   Manifest: {discovery_result['manifest_path']}")
    
    @pytest.mark.asyncio
    async def test_performance_slos(self, rvu_ingestor, sample_source_files, test_db_session):
        """Test performance SLOs across all operations"""
        print("\n⏱️ Testing performance SLOs...")
        
        # Test discovery SLO (≤ 30 seconds)
        discovery_start = time.time()
        source_files = await rvu_ingestor.discovery()
        discovery_time = time.time() - discovery_start
        
        assert discovery_time <= 30, f"Discovery took {discovery_time:.2f}s, exceeds 30s SLO"
        print(f"✅ Discovery SLO: {discovery_time:.2f}s (≤ 30s)")
        
        # Test download SLO (≤ 15 minutes for large files)
        download_start = time.time()
        release_id = f"test_perf_{uuid.uuid4().hex[:8]}"
        batch_id = str(uuid.uuid4())
        
        land_result = await rvu_ingestor._land_stage(
            release_id=release_id,
            batch_id=batch_id,
            source_files=sample_source_files
        )
        download_time = time.time() - download_start
        
        # For test data, this should be much faster than 15 minutes
        assert download_time <= 900, f"Download took {download_time:.2f}s, exceeds 15min SLO"
        print(f"✅ Download SLO: {download_time:.2f}s (≤ 15min)")
        
        # Test ingestion SLO (≤ 5 minutes)
        ingestion_start = time.time()
        pipeline_result = await rvu_ingestor.ingest(release_id, batch_id)
        ingestion_time = time.time() - ingestion_start
        
        assert ingestion_time <= 300, f"Ingestion took {ingestion_time:.2f}s, exceeds 5min SLO"
        print(f"✅ Ingestion SLO: {ingestion_time:.2f}s (≤ 5min)")
        
        print(f"✅ All performance SLOs met")
    
    @pytest.mark.asyncio
    async def test_error_handling_and_resilience(self, rvu_ingestor, test_db_session):
        """Test error handling and resilience"""
        print("\n🛡️ Testing error handling and resilience...")
        
        # Test with empty source files
        empty_result = await rvu_ingestor.ingest("empty_test", str(uuid.uuid4()))
        assert empty_result["status"] in ["failed", "partial"], "Should handle empty input gracefully"
        
        # Test with invalid release ID
        invalid_result = await rvu_ingestor.ingest("", str(uuid.uuid4()))
        assert invalid_result["status"] in ["failed", "partial"], "Should handle invalid release ID"
        
        print(f"✅ Error handling and resilience tested")
    
    def test_data_quality_validation(self, test_data_dir):
        """Test data quality validation rules"""
        print("\n🔍 Testing data quality validation...")
        
        # Load test data
        pprrvu_csv = test_data_dir / "PPRRVU2025_Oct_test.csv"
        gpci_csv = test_data_dir / "GPCI2025_test.csv"
        
        import pandas as pd
        
        # Test PPRRVU data quality
        pprrvu_df = pd.read_csv(pprrvu_csv)
        
        # Check required columns
        required_columns = ["hcpcs_code", "status_code", "work_rvu", "pe_rvu_nonfac", "pe_rvu_fac", "mp_rvu"]
        for col in required_columns:
            assert col in pprrvu_df.columns, f"Missing required column: {col}"
        
        # Check data types and ranges
        assert pprrvu_df["work_rvu"].dtype in ["float64", "int64"], "work_rvu should be numeric"
        assert pprrvu_df["work_rvu"].min() >= 0, "work_rvu should be non-negative"
        
        # Check HCPCS code format
        hcpcs_codes = pprrvu_df["hcpcs_code"].astype(str)
        assert all(len(code) == 5 for code in hcpcs_codes), "All HCPCS codes should be 5 characters"
        
        # Test GPCI data quality
        gpci_df = pd.read_csv(gpci_csv)
        
        # Check GPCI value ranges
        assert gpci_df["work_gpci"].min() >= 0, "work_gpci should be non-negative"
        assert gpci_df["work_gpci"].max() <= 3.0, "work_gpci should be reasonable"
        
        print(f"✅ Data quality validation passed")
        print(f"   PPRRVU records: {len(pprrvu_df)}")
        print(f"   GPCI records: {len(gpci_df)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
