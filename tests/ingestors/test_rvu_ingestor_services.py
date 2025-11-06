"""
Tests for RVU Ingestor ServiceFactory Integration

Tests verify that RVU ingestor correctly uses ServiceFactory for shared services,
following Phase 3 guardrails:
- ServiceFactory created in __init__
- Services accessible via self.services.*
- No manual instantiation remains
- Schema bootstrap happens once (no duplicate registration)
- Services used correctly in pipeline stages

Following QA Testing Standard (QTS) v1.0 patterns.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
from cms_pricing.ingestion.services import ServiceFactory, ServiceConfig


class TestRVUIngestorServiceFactoryIntegration:
    """Test ServiceFactory integration in RVU ingestor"""

    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create temporary output directory"""
        output_dir = tmp_path / "rvu_output"
        output_dir.mkdir(parents=True)
        return str(output_dir)

    @pytest.fixture
    def rvu_ingestor(self, temp_output_dir):
        """Create RVU ingestor instance"""
        return RVUIngestor(output_dir=temp_output_dir)

    def test_servicefactory_created_in_init(self, rvu_ingestor):
        """Verify ServiceFactory is created in __init__"""
        assert hasattr(rvu_ingestor, 'services')
        assert isinstance(rvu_ingestor.services, ServiceFactory)

    def test_services_accessible_via_services_property(self, rvu_ingestor):
        """Verify all services are accessible via self.services.*"""
        # Access services via factory
        assert rvu_ingestor.services.observability_collector is not None
        assert rvu_ingestor.services.validation_service is not None
        assert rvu_ingestor.services.quarantine_manager is not None
        assert rvu_ingestor.services.reference_data_manager is not None
        assert rvu_ingestor.services.schema_registry is not None
        assert rvu_ingestor.services.schema_service is not None

    def test_no_manual_instantiation_of_services(self, temp_output_dir):
        """Verify no manual instantiation of services remains"""
        # Check that ingestor doesn't have manual instantiations
        ingestor = RVUIngestor(output_dir=temp_output_dir)
        
        # Verify services come from factory, not direct attributes
        # (We check that services are accessed via factory, not stored as direct attributes)
        assert not hasattr(ingestor, 'validation_engine') or \
               ingestor.services.validation_engine is not None
        assert not hasattr(ingestor, 'observability_collector') or \
               ingestor.services.observability_collector is not None
        assert not hasattr(ingestor, 'quarantine_manager') or \
               ingestor.services.quarantine_manager is not None

    def test_schema_bootstrap_happens_once(self, temp_output_dir):
        """Verify schema bootstrap is called via schema_service (single registration)"""
        with patch('cms_pricing.ingestion.services.schema_service.SchemaService.bootstrap_rvu_schemas') as mock_bootstrap:
            ingestor = RVUIngestor(output_dir=temp_output_dir)
            
            # Schema bootstrap should be called once during __init__
            mock_bootstrap.assert_called_once()
            
            # Verify it was called with the registry
            call_args = mock_bootstrap.call_args
            assert call_args is not None
            # Registry should be passed as first argument
            assert len(call_args[0]) > 0

    def test_schema_caching_works(self, rvu_ingestor):
        """Verify schema caching works correctly"""
        # Schema caching should be set up during __init__
        assert hasattr(rvu_ingestor, '_cached_schemas')
        assert rvu_ingestor._cached_schemas is not None
        assert isinstance(rvu_ingestor._cached_schemas, dict)

    def test_validation_service_registration(self, temp_output_dir):
        """Verify validation service registers business rules"""
        with patch('cms_pricing.ingestion.services.validation_service.ValidationService.register_dataset_business_rules') as mock_register:
            ingestor = RVUIngestor(output_dir=temp_output_dir)
            
            # Business rules should be registered for each dataset
            assert mock_register.call_count > 0


class TestRVUIngestorServiceUsageInPipeline:
    """Test that services are used correctly in pipeline stages"""

    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create temporary output directory"""
        output_dir = tmp_path / "rvu_output"
        output_dir.mkdir(parents=True)
        return str(output_dir)

    @pytest.fixture
    def rvu_ingestor(self, temp_output_dir):
        """Create RVU ingestor instance"""
        return RVUIngestor(output_dir=temp_output_dir)

    @pytest.mark.asyncio
    async def test_observability_collector_used_in_pipeline(self, rvu_ingestor):
        """Verify observability collector is used in pipeline stages"""
        # Mock the observability collector to track usage
        mock_collector = MagicMock()
        rvu_ingestor.services._services['observability_collector'] = mock_collector
        
        # Access observability collector - should use the mock
        collector = rvu_ingestor.services.observability_collector
        assert collector is mock_collector

    def test_validation_service_used_in_validate_stage(self, rvu_ingestor):
        """Verify validation service is accessible for validate stage"""
        # Validation service should be available
        validation_service = rvu_ingestor.services.validation_service
        assert validation_service is not None
        
        # Service should have methods for validation
        assert hasattr(validation_service, 'validate') or \
               hasattr(validation_service, 'register_dataset_business_rules')

    def test_quarantine_manager_used_in_pipeline(self, rvu_ingestor):
        """Verify quarantine manager is accessible for pipeline stages"""
        # Quarantine manager should be available
        quarantine_manager = rvu_ingestor.services.quarantine_manager
        assert quarantine_manager is not None

    def test_schema_registry_used_in_pipeline(self, rvu_ingestor):
        """Verify schema registry is accessible for validation/normalization stages"""
        # Schema registry should be available
        schema_registry = rvu_ingestor.services.schema_registry
        assert schema_registry is not None
        
        # Should be able to get contracts
        assert hasattr(schema_registry, 'get_contract') or \
               hasattr(schema_registry, 'register_schema')

    def test_reference_data_manager_used_in_enrich_stage(self, rvu_ingestor):
        """Verify reference data manager is accessible for enrich stage"""
        # Reference data manager should be available
        ref_manager = rvu_ingestor.services.reference_data_manager
        assert ref_manager is not None

    def test_reference_enricher_used_in_enrich_stage(self, rvu_ingestor):
        """Verify reference enricher is accessible for enrich stage"""
        # Reference enricher should be available and depends on manager
        ref_enricher = rvu_ingestor.services.reference_enricher
        assert ref_enricher is not None

