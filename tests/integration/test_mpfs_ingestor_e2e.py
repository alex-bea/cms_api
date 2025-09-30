"""
MPFS Ingestor End-to-End Integration Tests

Following QTS (QA & Testing Standard) PRD v1.0 and DIS compliance
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any
import structlog

from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor
from cms_pricing.ingestion.scrapers.cms_mpfs_scraper import CMSMPFSScraper
from tests.fixtures.mpfs.test_dataset_creator import MPFSTestDatasetCreator

logger = structlog.get_logger()


class TestMPFSIngestorE2E:
    """End-to-end tests for MPFS ingestor"""
    
    @pytest.fixture
    async def temp_dir(self):
        """Create temporary directory for test data"""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    async def test_data(self, temp_dir):
        """Create test data"""
        creator = MPFSTestDatasetCreator(str(temp_dir / "test_data"))
        return creator.create_all()
    
    @pytest.fixture
    async def mpfs_ingestor(self, temp_dir):
        """Create MPFS ingestor instance"""
        return MPFSIngestor(str(temp_dir / "ingestion"))
    
    @pytest.fixture
    async def mpfs_scraper(self, temp_dir):
        """Create MPFS scraper instance"""
        return CMSMPFSScraper(str(temp_dir / "scraped"))
    
    @pytest.mark.asyncio
    async def test_mpfs_scraper_discovery(self, mpfs_scraper):
        """Test MPFS scraper file discovery"""
        logger.info("Testing MPFS scraper discovery")
        
        # Test discovery for 2025
        files = await mpfs_scraper.scrape_mpfs_files(2025, 2025, latest_only=True)
        
        # Verify discovery results
        assert isinstance(files, list)
        logger.info("MPFS scraper discovery test completed", files_found=len(files))
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_full_pipeline(self, mpfs_ingestor, test_data):
        """Test full MPFS ingestor pipeline"""
        logger.info("Testing MPFS ingestor full pipeline")
        
        try:
            # Run ingestion for 2025
            result = await mpfs_ingestor.ingest(2025)
            
            # Verify result structure
            assert "batch_id" in result
            assert "dataset_name" in result
            assert "release_id" in result
            assert "curated_views" in result
            assert "observability_report" in result
            assert "metadata" in result
            
            # Verify dataset name
            assert result["dataset_name"] == "MPFS"
            
            # Verify curated views
            curated_views = result["curated_views"]
            expected_views = [
                "mpfs_rvu", "mpfs_indicators_all", "mpfs_locality", 
                "mpfs_gpci", "mpfs_cf_vintage", "mpfs_link_keys"
            ]
            for view_name in expected_views:
                assert view_name in curated_views
            
            # Verify observability report
            obs_report = result["observability_report"]
            assert "batch_id" in obs_report
            assert "freshness_metrics" in obs_report
            assert "volume_metrics" in obs_report
            assert "schema_metrics" in obs_report
            assert "quality_metrics" in obs_report
            assert "lineage_metrics" in obs_report
            
            logger.info("MPFS ingestor full pipeline test completed successfully")
            
        except Exception as e:
            logger.error("MPFS ingestor full pipeline test failed", error=str(e))
            raise
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_dis_stages(self, mpfs_ingestor, test_data):
        """Test individual DIS pipeline stages"""
        logger.info("Testing MPFS ingestor DIS stages")
        
        try:
            # Test discovery stage
            source_files = await mpfs_ingestor.discover_source_files()
            assert isinstance(source_files, list)
            logger.info("Discovery stage test completed", files_found=len(source_files))
            
            # Test land stage
            raw_batch = await mpfs_ingestor.land_stage(source_files)
            assert raw_batch.batch_id is not None
            assert len(raw_batch.source_files) > 0
            logger.info("Land stage test completed", files_processed=len(raw_batch.raw_data))
            
            # Test validate stage
            validated_batch, validation_results = await mpfs_ingestor.validate_stage(raw_batch)
            assert validated_batch.batch_id == raw_batch.batch_id
            assert isinstance(validation_results, list)
            logger.info("Validate stage test completed", validation_results=len(validation_results))
            
            # Test normalize stage
            adapted_batch = await mpfs_ingestor.normalize_stage(validated_batch)
            assert adapted_batch.batch_id == validated_batch.batch_id
            assert len(adapted_batch.adapted_data) > 0
            logger.info("Normalize stage test completed", files_processed=len(adapted_batch.adapted_data))
            
            # Test enrich stage
            stage_frame = await mpfs_ingestor.enrich_stage(adapted_batch)
            assert stage_frame.batch_id == adapted_batch.batch_id
            logger.info("Enrich stage test completed")
            
            # Test publish stage
            result = await mpfs_ingestor.publish_stage(stage_frame)
            assert "batch_id" in result
            assert "curated_views" in result
            logger.info("Publish stage test completed")
            
            logger.info("All DIS stages test completed successfully")
            
        except Exception as e:
            logger.error("DIS stages test failed", error=str(e))
            raise
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_validation_rules(self, mpfs_ingestor):
        """Test MPFS ingestor validation rules"""
        logger.info("Testing MPFS ingestor validation rules")
        
        # Get validation rules
        validation_rules = mpfs_ingestor.validation_rules
        
        # Verify rules exist
        assert len(validation_rules) > 0
        
        # Verify rule structure
        for rule in validation_rules:
            assert hasattr(rule, 'rule_id')
            assert hasattr(rule, 'name')
            assert hasattr(rule, 'description')
            assert hasattr(rule, 'severity')
            assert hasattr(rule, 'validation_type')
        
        # Verify specific rules exist
        rule_ids = [rule.rule_id for rule in validation_rules]
        expected_rules = [
            "mpfs_structural_001",
            "mpfs_domain_001", 
            "mpfs_domain_002",
            "mpfs_statistical_001",
            "mpfs_business_001"
        ]
        
        for expected_rule in expected_rules:
            assert expected_rule in rule_ids
        
        logger.info("Validation rules test completed", rules_count=len(validation_rules))
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_schema_contracts(self, mpfs_ingestor):
        """Test MPFS ingestor schema contracts"""
        logger.info("Testing MPFS ingestor schema contracts")
        
        # Get schema contracts
        schema_contracts = mpfs_ingestor.schema_contracts
        
        # Verify contracts exist
        assert len(schema_contracts) > 0
        
        # Verify contract structure
        for contract_name, contract in schema_contracts.items():
            assert hasattr(contract, 'schema_id')
            assert hasattr(contract, 'schema_name')
            assert hasattr(contract, 'version')
            assert hasattr(contract, 'fields')
        
        # Verify specific contracts exist
        expected_contracts = ["mpfs_rvu", "mpfs_cf"]
        for expected_contract in expected_contracts:
            assert expected_contract in schema_contracts
        
        logger.info("Schema contracts test completed", contracts_count=len(schema_contracts))
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_sla_compliance(self, mpfs_ingestor):
        """Test MPFS ingestor SLA compliance"""
        logger.info("Testing MPFS ingestor SLA compliance")
        
        # Get SLA spec
        sla_spec = mpfs_ingestor.sla_spec
        
        # Verify SLA spec structure
        assert hasattr(sla_spec, 'max_processing_time_hours')
        assert hasattr(sla_spec, 'freshness_alert_days')
        assert hasattr(sla_spec, 'quality_threshold')
        assert hasattr(sla_spec, 'availability_target')
        
        # Verify SLA values are reasonable
        assert sla_spec.max_processing_time_hours > 0
        assert sla_spec.freshness_alert_days > 0
        assert 0 < sla_spec.quality_threshold <= 1
        assert 0 < sla_spec.availability_target <= 1
        
        logger.info("SLA compliance test completed")
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_error_handling(self, mpfs_ingestor):
        """Test MPFS ingestor error handling"""
        logger.info("Testing MPFS ingestor error handling")
        
        try:
            # Test with invalid year (should handle gracefully)
            result = await mpfs_ingestor.ingest(1999)  # Very old year
            
            # Should still return a result structure
            assert "batch_id" in result
            assert "dataset_name" in result
            
            logger.info("Error handling test completed")
            
        except Exception as e:
            # Should handle errors gracefully
            logger.info("Error handling test completed with expected error", error=str(e))
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_metadata_preservation(self, mpfs_ingestor, test_data):
        """Test MPFS ingestor metadata preservation"""
        logger.info("Testing MPFS ingestor metadata preservation")
        
        try:
            # Run ingestion
            result = await mpfs_ingestor.ingest(2025)
            
            # Verify metadata is preserved
            metadata = result["metadata"]
            assert "ingestion_timestamp" in metadata
            assert "source" in metadata
            assert "license" in metadata
            assert "attribution_required" in metadata
            
            # Verify source information
            assert metadata["source"] == "CMS Medicare Physician Fee Schedule"
            assert metadata["license"] == "CMS Public Domain"
            assert metadata["attribution_required"] == False
            
            logger.info("Metadata preservation test completed")
            
        except Exception as e:
            logger.error("Metadata preservation test failed", error=str(e))
            raise
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_observability_metrics(self, mpfs_ingestor, test_data):
        """Test MPFS ingestor observability metrics"""
        logger.info("Testing MPFS ingestor observability metrics")
        
        try:
            # Run ingestion
            result = await mpfs_ingestor.ingest(2025)
            
            # Verify observability report
            obs_report = result["observability_report"]
            
            # Verify 5-pillar observability
            assert "freshness_metrics" in obs_report
            assert "volume_metrics" in obs_report
            assert "schema_metrics" in obs_report
            assert "quality_metrics" in obs_report
            assert "lineage_metrics" in obs_report
            
            # Verify metrics structure
            for pillar in ["freshness_metrics", "volume_metrics", "schema_metrics", "quality_metrics", "lineage_metrics"]:
                pillar_data = obs_report[pillar]
                assert "score" in pillar_data or "last_successful_run" in pillar_data
            
            logger.info("Observability metrics test completed")
            
        except Exception as e:
            logger.error("Observability metrics test failed", error=str(e))
            raise


# Test configuration
@pytest.mark.integration
@pytest.mark.mpfs
@pytest.mark.e2e
class TestMPFSIngestorIntegration:
    """Integration test class for MPFS ingestor"""
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_with_real_data(self):
        """Test MPFS ingestor with real data (if available)"""
        logger.info("Testing MPFS ingestor with real data")
        
        # This test would use real CMS data if available
        # For now, just verify the ingestor can be instantiated
        ingestor = MPFSIngestor()
        assert ingestor is not None
        assert ingestor.dataset_name == "MPFS"
        
        logger.info("Real data test completed")


# Performance tests
@pytest.mark.performance
@pytest.mark.mpfs
class TestMPFSIngestorPerformance:
    """Performance tests for MPFS ingestor"""
    
    @pytest.mark.asyncio
    async def test_mpfs_ingestor_performance(self, temp_dir):
        """Test MPFS ingestor performance"""
        logger.info("Testing MPFS ingestor performance")
        
        ingestor = MPFSIngestor(str(temp_dir / "ingestion"))
        
        # Measure ingestion time
        start_time = datetime.now()
        
        try:
            result = await ingestor.ingest(2025)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Verify performance is within SLA
            assert duration < ingestor.sla_spec.max_processing_time_hours * 3600
            
            logger.info("Performance test completed", duration_seconds=duration)
            
        except Exception as e:
            logger.error("Performance test failed", error=str(e))
            raise


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
