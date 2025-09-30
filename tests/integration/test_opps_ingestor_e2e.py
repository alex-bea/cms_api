#!/usr/bin/env python3
"""
OPPS Ingester End-to-End Test
=============================

Comprehensive end-to-end testing of the OPPS ingester following QTS v1.0 standards.
Tests the complete DIS pipeline: Land → Validate → Normalize → Enrich → Publish.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.0
DIS Compliance: v1.0
"""

import asyncio
import json
import logging
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
import pytest
import structlog

from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor
from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper
from tests.fixtures.opps.test_dataset_creator import OPPSTestDatasetCreator


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


class TestOPPSIngestorE2E:
    """End-to-end test suite for OPPS ingester."""
    
    @pytest.fixture
    async def test_environment(self):
        """Set up test environment with temporary directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test directories
            output_dir = temp_path / "output"
            fixtures_dir = temp_path / "fixtures"
            
            # Create test dataset
            creator = OPPSTestDatasetCreator(fixtures_dir)
            creator.save_golden_datasets()
            
            yield {
                "output_dir": output_dir,
                "fixtures_dir": fixtures_dir,
                "temp_dir": temp_path
            }
    
    @pytest.fixture
    def opps_ingester(self, test_environment):
        """Create OPPS ingester instance."""
        return OPPSIngestor(
            output_dir=test_environment["output_dir"],
            cpt_masking_enabled=True
        )
    
    @pytest.fixture
    def opps_scraper(self, test_environment):
        """Create OPPS scraper instance."""
        return CMSOPPSScraper(output_dir=test_environment["output_dir"])
    
    @pytest.mark.asyncio
    async def test_opps_ingester_full_pipeline(self, opps_ingester, test_environment):
        """Test complete OPPS ingester pipeline."""
        logger.info("Starting OPPS ingester full pipeline test")
        
        # Test batch ID
        batch_id = "opps_2025q1_r01"
        
        try:
            # Run the complete ingestion pipeline
            result = await opps_ingester.ingest_batch(batch_id)
            
            # Verify result structure
            assert "status" in result
            assert "batch_id" in result
            assert result["batch_id"] == batch_id
            
            # Verify success
            if result["status"] == "success":
                logger.info("OPPS ingester pipeline completed successfully")
                
                # Verify stages completed
                assert "stages_completed" in result
                expected_stages = ["land", "validate", "normalize", "enrich", "publish"]
                for stage in expected_stages:
                    assert stage in result["stages_completed"]
                
                # Verify validation results
                assert "validation_results" in result
                validation_results = result["validation_results"]
                assert validation_results["passed"] is True
                
                # Verify publish results
                assert "publish_results" in result
                publish_results = result["publish_results"]
                assert "tables_published" in publish_results
                assert "records_published" in publish_results
                assert publish_results["records_published"] > 0
                
                logger.info("All pipeline stages completed successfully")
                
            else:
                logger.error("OPPS ingester pipeline failed", result=result)
                pytest.fail(f"Pipeline failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error("OPPS ingester test failed", error=str(e))
            pytest.fail(f"Test failed with exception: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_scraper_discovery(self, opps_scraper, test_environment):
        """Test OPPS scraper discovery functionality."""
        logger.info("Testing OPPS scraper discovery")
        
        try:
            # Test discovery of latest quarters
            files = await opps_scraper.discover_latest(quarters=2)
            
            # Verify files were discovered
            assert isinstance(files, list)
            logger.info(f"Discovered {len(files)} files")
            
            # Verify file structure
            for file_info in files:
                assert hasattr(file_info, 'url')
                assert hasattr(file_info, 'filename')
                assert hasattr(file_info, 'file_type')
                assert hasattr(file_info, 'batch_id')
                assert hasattr(file_info, 'metadata')
                
                # Verify metadata structure
                metadata = file_info.metadata
                assert 'year' in metadata
                assert 'quarter' in metadata
                assert isinstance(metadata['year'], int)
                assert isinstance(metadata['quarter'], int)
                assert 1 <= metadata['quarter'] <= 4
            
            logger.info("OPPS scraper discovery test passed")
            
        except Exception as e:
            logger.error("OPPS scraper discovery test failed", error=str(e))
            pytest.fail(f"Discovery test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_validation_rules(self, opps_ingester, test_environment):
        """Test OPPS ingester validation rules."""
        logger.info("Testing OPPS ingester validation rules")
        
        try:
            # Test validation rules
            validation_rules = opps_ingester.validators
            
            # Verify validation rules exist
            assert len(validation_rules) > 0
            logger.info(f"Found {len(validation_rules)} validation rules")
            
            # Verify rule structure
            for rule in validation_rules:
                assert hasattr(rule, 'rule_name')
                assert hasattr(rule, 'description')
                assert hasattr(rule, 'severity')
                assert hasattr(rule, 'validator')
                
                # Verify rule names are meaningful
                assert len(rule.rule_name) > 0
                assert len(rule.description) > 0
            
            # Test specific validation rules
            required_rules = [
                "required_files_present",
                "file_format_valid",
                "required_columns_present",
                "data_types_valid",
                "hcpcs_code_format",
                "apc_code_format",
                "status_indicator_valid",
                "payment_rates_positive"
            ]
            
            rule_names = [rule.rule_name for rule in validation_rules]
            for required_rule in required_rules:
                assert required_rule in rule_names, f"Missing required validation rule: {required_rule}"
            
            logger.info("OPPS ingester validation rules test passed")
            
        except Exception as e:
            logger.error("OPPS ingester validation rules test failed", error=str(e))
            pytest.fail(f"Validation rules test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_schema_contracts(self, opps_ingester, test_environment):
        """Test OPPS ingester schema contracts."""
        logger.info("Testing OPPS ingester schema contracts")
        
        try:
            # Test schema contracts
            assert hasattr(opps_ingester, 'opps_schema')
            assert hasattr(opps_ingester, 'si_schema')
            
            # Verify main OPPS schema
            opps_schema = opps_ingester.opps_schema
            assert 'schema_id' in opps_schema
            assert 'dataset_name' in opps_schema
            assert 'version' in opps_schema
            assert 'tables' in opps_schema
            
            # Verify required tables
            required_tables = [
                "opps_apc_payment",
                "opps_hcpcs_crosswalk", 
                "opps_rates_enriched"
            ]
            
            for table_name in required_tables:
                assert table_name in opps_schema['tables'], f"Missing required table: {table_name}"
            
            # Verify SI schema
            si_schema = opps_ingester.si_schema
            assert 'schema_id' in si_schema
            assert 'dataset_name' in si_schema
            assert 'tables' in si_schema
            assert 'ref_si_lookup' in si_schema['tables']
            
            logger.info("OPPS ingester schema contracts test passed")
            
        except Exception as e:
            logger.error("OPPS ingester schema contracts test failed", error=str(e))
            pytest.fail(f"Schema contracts test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_cpt_masking(self, opps_ingester, test_environment):
        """Test OPPS ingester CPT masking functionality."""
        logger.info("Testing OPPS ingester CPT masking")
        
        try:
            # Test CPT masking configuration
            assert hasattr(opps_ingester, 'cpt_masking_enabled')
            assert isinstance(opps_ingester.cpt_masking_enabled, bool)
            
            # Test masking method exists
            assert hasattr(opps_ingester, '_apply_cpt_masking')
            assert callable(opps_ingester._apply_cpt_masking)
            
            # Test masking with sample data
            sample_df = pd.DataFrame({
                'hcpcs_code': ['99213', '99214', '99215'],
                'description': ['Office visit', 'Office visit', 'Office visit'],
                'payment_rate_usd': [100.00, 150.00, 200.00]
            })
            
            masked_df = opps_ingester._apply_cpt_masking(sample_df)
            
            # Verify masking preserves structure
            assert len(masked_df) == len(sample_df)
            assert list(masked_df.columns) == list(sample_df.columns)
            
            logger.info("OPPS ingester CPT masking test passed")
            
        except Exception as e:
            logger.error("OPPS ingester CPT masking test failed", error=str(e))
            pytest.fail(f"CPT masking test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_dis_compliance(self, opps_ingester, test_environment):
        """Test OPPS ingester DIS compliance."""
        logger.info("Testing OPPS ingester DIS compliance")
        
        try:
            # Test DIS compliance properties
            assert hasattr(opps_ingester, 'dataset_name')
            assert hasattr(opps_ingester, 'release_cadence')
            assert hasattr(opps_ingester, 'data_classification')
            assert hasattr(opps_ingester, 'contract_schema_ref')
            assert hasattr(opps_ingester, 'validators')
            assert hasattr(opps_ingester, 'slas')
            assert hasattr(opps_ingester, 'outputs')
            assert hasattr(opps_ingester, 'classification')
            assert hasattr(opps_ingester, 'adapter')
            assert hasattr(opps_ingester, 'enricher')
            
            # Verify property values
            assert opps_ingester.dataset_name == "cms_opps"
            assert opps_ingester.release_cadence == "quarterly"
            assert opps_ingester.contract_schema_ref == "cms_opps:1.0.0"
            
            # Verify SLA specifications
            sla_spec = opps_ingester.slas
            assert hasattr(sla_spec, 'max_processing_time_hours')
            assert hasattr(sla_spec, 'freshness_alert_hours')
            assert hasattr(sla_spec, 'quality_threshold')
            assert hasattr(sla_spec, 'availability_target')
            
            # Verify output specifications
            output_spec = opps_ingester.outputs
            assert hasattr(output_spec, 'table_name')
            assert hasattr(output_spec, 'partition_columns')
            assert hasattr(output_spec, 'output_format')
            assert hasattr(output_spec, 'compression')
            assert hasattr(output_spec, 'schema_evolution')
            
            logger.info("OPPS ingester DIS compliance test passed")
            
        except Exception as e:
            logger.error("OPPS ingester DIS compliance test failed", error=str(e))
            pytest.fail(f"DIS compliance test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_error_handling(self, opps_ingester, test_environment):
        """Test OPPS ingester error handling."""
        logger.info("Testing OPPS ingester error handling")
        
        try:
            # Test with invalid batch ID
            invalid_batch_id = "invalid_batch_id"
            
            result = await opps_ingester.ingest_batch(invalid_batch_id)
            
            # Verify error handling
            assert result["status"] == "failed"
            assert "error" in result
            
            logger.info("OPPS ingester error handling test passed")
            
        except Exception as e:
            logger.error("OPPS ingester error handling test failed", error=str(e))
            pytest.fail(f"Error handling test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_quarantine_system(self, opps_ingester, test_environment):
        """Test OPPS ingester quarantine system."""
        logger.info("Testing OPPS ingester quarantine system")
        
        try:
            # Test quarantine directory exists
            assert hasattr(opps_ingester, 'quarantine_dir')
            assert opps_ingester.quarantine_dir.exists()
            
            # Test quarantine method exists
            assert hasattr(opps_ingester, '_quarantine_batch')
            assert callable(opps_ingester._quarantine_batch)
            
            # Test quarantine with sample data
            test_batch_id = "test_quarantine_batch"
            test_error = "Test quarantine error"
            
            await opps_ingester._quarantine_batch(test_batch_id, test_error)
            
            # Verify quarantine file was created
            quarantine_path = opps_ingester.quarantine_dir / test_batch_id
            assert quarantine_path.exists()
            
            quarantine_info_path = quarantine_path / "quarantine_info.json"
            assert quarantine_info_path.exists()
            
            # Verify quarantine info content
            with open(quarantine_info_path, 'r') as f:
                quarantine_info = json.load(f)
            
            assert quarantine_info["batch_id"] == test_batch_id
            assert quarantine_info["error_message"] == test_error
            assert "quarantined_at" in quarantine_info
            
            logger.info("OPPS ingester quarantine system test passed")
            
        except Exception as e:
            logger.error("OPPS ingester quarantine system test failed", error=str(e))
            pytest.fail(f"Quarantine system test failed: {str(e)}")
    
    @pytest.mark.asyncio
    async def test_opps_ingester_observability(self, opps_ingester, test_environment):
        """Test OPPS ingester observability metrics."""
        logger.info("Testing OPPS ingester observability")
        
        try:
            # Test observability components
            assert hasattr(opps_ingester, 'observability')
            assert hasattr(opps_ingester, '_update_observability_metrics')
            assert callable(opps_ingester._update_observability_metrics)
            
            # Test observability update method
            from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSBatchInfo
            from cms_pricing.ingestion.scrapers.cms_opps_scraper import ScrapedFileInfo
            
            # Create mock batch info
            mock_batch_info = OPPSBatchInfo(
                batch_id="test_observability",
                year=2025,
                quarter=1,
                release_number=1,
                effective_from=date(2025, 1, 1),
                effective_to=None,
                files=[],
                discovered_at=datetime.utcnow()
            )
            
            mock_validation_results = {"passed": True, "rules": {}}
            mock_publish_results = {"tables_published": [], "records_published": 0}
            
            # Test observability update
            await opps_ingester._update_observability_metrics(
                mock_batch_info, 
                mock_validation_results, 
                mock_publish_results
            )
            
            logger.info("OPPS ingester observability test passed")
            
        except Exception as e:
            logger.error("OPPS ingester observability test failed", error=str(e))
            pytest.fail(f"Observability test failed: {str(e)}")
    
    def test_opps_ingester_property_methods(self, opps_ingester, test_environment):
        """Test OPPS ingester property methods."""
        logger.info("Testing OPPS ingester property methods")
        
        try:
            # Test all property methods
            properties = [
                'dataset_name', 'release_cadence', 'data_classification',
                'contract_schema_ref', 'validators', 'slas', 'outputs',
                'classification', 'adapter', 'enricher'
            ]
            
            for prop in properties:
                assert hasattr(opps_ingester, prop)
                value = getattr(opps_ingester, prop)
                assert value is not None, f"Property {prop} returned None"
            
            logger.info("OPPS ingester property methods test passed")
            
        except Exception as e:
            logger.error("OPPS ingester property methods test failed", error=str(e))
            pytest.fail(f"Property methods test failed: {str(e)}")


# Test configuration
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Test markers
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
    pytest.mark.opps,
    pytest.mark.e2e
]


# CLI interface
def main():
    """CLI entry point for running tests."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run OPPS ingester E2E tests')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--test', help='Run specific test')
    
    args = parser.parse_args()
    
    # Configure pytest arguments
    pytest_args = ['-v' if args.verbose else '-q']
    
    if args.test:
        pytest_args.append(f'tests/integration/test_opps_ingestor_e2e.py::{args.test}')
    else:
        pytest_args.append('tests/integration/test_opps_ingestor_e2e.py')
    
    # Run tests
    exit_code = pytest.main(pytest_args)
    return exit_code


if __name__ == "__main__":
    exit(main())
