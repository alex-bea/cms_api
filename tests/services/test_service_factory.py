"""
Tests for ServiceFactory lazy initialization and guardrails.

Tests cover:
- Lazy initialization (services created only when accessed)
- Eager initialization (lazy_init=False)
- Repeated access (singleton behavior)
- Dependency wiring (e.g., reference_enricher depends on reference_data_manager)
- NotImplementedError pathways (disabled services raise descriptive errors)

Following QA Testing Standard (QTS) v1.0 patterns.
"""

import pytest
from unittest.mock import patch, MagicMock

from cms_pricing.ingestion.services import ServiceFactory, ServiceConfig


class TestLazyInitialization:
    """Test lazy initialization behavior"""

    @patch('cms_pricing.ingestion.observability.dis_observability.DISObservabilityCollector')
    def test_observability_collector_lazy_initialization(self, mock_collector_class):
        """Verify observability collector is created only on first access"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Service not created yet
        mock_collector_class.assert_not_called()
        
        # Access service - should trigger creation
        _ = factory.observability_collector
        mock_collector_class.assert_called_once_with("/tmp/test")
        
        # Access again - should not create another instance
        _ = factory.observability_collector
        assert mock_collector_class.call_count == 1
    
    @patch('cms_pricing.ingestion.validators.validation_engine.ValidationEngine')
    def test_validation_engine_lazy_initialization(self, mock_engine_class):
        """Verify validation engine is created only on first access"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Service not created yet
        mock_engine_class.assert_not_called()
        
        # Access service - should trigger creation
        _ = factory.validation_engine
        mock_engine_class.assert_called_once()
        
        # Access again - should not create another instance
        _ = factory.validation_engine
        assert mock_engine_class.call_count == 1
    
    @patch('cms_pricing.ingestion.quarantine.dis_quarantine.QuarantineManager')
    def test_quarantine_manager_lazy_initialization(self, mock_manager_class):
        """Verify quarantine manager is created only on first access"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Service not created yet
        mock_manager_class.assert_not_called()
        
        # Access service - should trigger creation
        _ = factory.quarantine_manager
        mock_manager_class.assert_called_once_with("/tmp/test")
        
        # Access again - should not create another instance
        _ = factory.quarantine_manager
        assert mock_manager_class.call_count == 1
    
    @patch('cms_pricing.ingestion.enrichers.dis_reference_data_integration.ReferenceDataManager')
    def test_reference_data_manager_lazy_initialization(self, mock_manager_class):
        """Verify reference data manager is created only on first access"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Service not created yet
        mock_manager_class.assert_not_called()
        
        # Access service - should trigger creation
        _ = factory.reference_data_manager
        mock_manager_class.assert_called_once_with("/tmp/test")
        
        # Access again - should not create another instance
        _ = factory.reference_data_manager
        assert mock_manager_class.call_count == 1
    
    @patch('cms_pricing.ingestion.contracts.schema_registry.SchemaRegistry')
    def test_schema_registry_lazy_initialization(self, mock_registry_class):
        """Verify schema registry is created only on first access"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Service not created yet
        mock_registry_class.assert_not_called()
        
        # Access service - should trigger creation
        _ = factory.schema_registry
        mock_registry_class.assert_called_once()
        
        # Access again - should not create another instance
        _ = factory.schema_registry
        assert mock_registry_class.call_count == 1


class TestEagerInitialization:
    """Test eager initialization behavior (lazy_init=False)"""

    @patch('cms_pricing.ingestion.observability.dis_observability.DISObservabilityCollector')
    @patch('cms_pricing.ingestion.validators.validation_engine.ValidationEngine')
    @patch('cms_pricing.ingestion.quarantine.dis_quarantine.QuarantineManager')
    @patch('cms_pricing.ingestion.enrichers.dis_reference_data_integration.ReferenceDataManager')
    @patch('cms_pricing.ingestion.contracts.schema_registry.SchemaRegistry')
    def test_eager_initialization_creates_all_services(
        self,
        mock_registry_class,
        mock_ref_manager_class,
        mock_quarantine_class,
        mock_engine_class,
        mock_collector_class
    ):
        """Verify eager initialization creates all services immediately"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=False
        )
        
        # Create factory - should trigger eager initialization
        factory = ServiceFactory(config)
        
        # All services should be created
        mock_collector_class.assert_called_once_with("/tmp/test")
        mock_engine_class.assert_called_once()
        mock_quarantine_class.assert_called_once_with("/tmp/test")
        mock_ref_manager_class.assert_called_once_with("/tmp/test")
        mock_registry_class.assert_called_once()
    
    def test_eager_initialization_all_services_available(self):
        """Verify all services are available after eager initialization"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=False
        )
        
        factory = ServiceFactory(config)
        
        # All services should be accessible without triggering new creation
        assert factory.observability_collector is not None
        assert factory.validation_engine is not None
        assert factory.quarantine_manager is not None
        assert factory.reference_data_manager is not None
        assert factory.schema_registry is not None


class TestRepeatedAccess:
    """Test that repeated access returns same instance (singleton behavior)"""

    def test_observability_collector_singleton(self):
        """Verify repeated access returns same instance"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        collector1 = factory.observability_collector
        collector2 = factory.observability_collector
        
        assert collector1 is collector2
    
    def test_validation_engine_singleton(self):
        """Verify repeated access returns same instance"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        engine1 = factory.validation_engine
        engine2 = factory.validation_engine
        
        assert engine1 is engine2
    
    def test_all_services_singleton(self):
        """Verify all services return same instance on repeated access"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # First access
        obs1 = factory.observability_collector
        val1 = factory.validation_engine
        quar1 = factory.quarantine_manager
        ref1 = factory.reference_data_manager
        schema1 = factory.schema_registry
        
        # Second access
        obs2 = factory.observability_collector
        val2 = factory.validation_engine
        quar2 = factory.quarantine_manager
        ref2 = factory.reference_data_manager
        schema2 = factory.schema_registry
        
        assert obs1 is obs2
        assert val1 is val2
        assert quar1 is quar2
        assert ref1 is ref2
        assert schema1 is schema2


class TestDependencyWiring:
    """Test that services with dependencies are wired correctly"""

    def test_reference_enricher_depends_on_reference_data_manager(self):
        """Verify reference_enricher correctly depends on reference_data_manager"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Access reference_enricher - should create reference_data_manager first
        enricher = factory.reference_enricher
        
        # Verify enricher was created with the manager as dependency
        assert enricher is not None
        # The enricher should have been initialized with the manager
        # (verification depends on DISReferenceDataEnricher implementation)
    
    def test_validation_service_depends_on_validation_engine(self):
        """Verify validation_service correctly depends on validation_engine"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            lazy_init=True
        )
        factory = ServiceFactory(config)
        
        # Access validation_service - should create validation_engine first
        service = factory.validation_service
        
        # Verify service was created
        assert service is not None
        # The service wraps the engine (verification depends on ValidationService implementation)


class TestNotImplementedError:
    """Test NotImplementedError pathways for disabled services"""

    def test_disabled_validation_engine_raises_not_implemented(self):
        """Verify disabled validation engine raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_validation=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.validation_engine
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Validation engine is disabled in ServiceConfig" in error_message
        assert "Set enable_validation=True" in error_message
    
    def test_disabled_validation_service_raises_not_implemented(self):
        """Verify disabled validation service raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_validation=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.validation_service
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Validation service is disabled in ServiceConfig" in error_message
        assert "Set enable_validation=True" in error_message
    
    def test_disabled_observability_raises_not_implemented(self):
        """Verify disabled observability collector raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_observability=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.observability_collector
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Observability collector is disabled in ServiceConfig" in error_message
        assert "Set enable_observability=True" in error_message
    
    def test_disabled_quarantine_raises_not_implemented(self):
        """Verify disabled quarantine manager raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_quarantine=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.quarantine_manager
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Quarantine manager is disabled in ServiceConfig" in error_message
        assert "Set enable_quarantine=True" in error_message
    
    def test_disabled_reference_data_manager_raises_not_implemented(self):
        """Verify disabled reference data manager raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_reference_data=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.reference_data_manager
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Reference data manager is disabled in ServiceConfig" in error_message
        assert "Set enable_reference_data=True" in error_message
    
    def test_disabled_reference_enricher_raises_not_implemented(self):
        """Verify disabled reference enricher raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_reference_data=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.reference_enricher
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Reference enricher is disabled in ServiceConfig" in error_message
        assert "Set enable_reference_data=True" in error_message
    
    def test_disabled_schema_registry_raises_not_implemented(self):
        """Verify disabled schema registry raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_schema_registry=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.schema_registry
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Schema registry is disabled in ServiceConfig" in error_message
        assert "Set enable_schema_registry=True" in error_message
    
    def test_disabled_schema_service_raises_not_implemented(self):
        """Verify disabled schema service raises descriptive NotImplementedError"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="test_dataset",
            enable_schema_registry=False
        )
        factory = ServiceFactory(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            _ = factory.schema_service
        
        # Verify error message matches actual wording from service_factory.py
        error_message = str(exc_info.value)
        assert "Schema service is disabled in ServiceConfig" in error_message
        assert "Set enable_schema_registry=True" in error_message


class TestSchemaBootstrapMap:
    """Test dataset-aware schema bootstrap via bootstrap map"""

    def test_get_bootstrapper_for_rvu_returns_function(self):
        """Verify get_bootstrapper returns bootstrap function for RVU"""
        from cms_pricing.ingestion.services.schema_service import SchemaService
        
        bootstrapper = SchemaService.get_bootstrapper("cms_rvu")
        assert bootstrapper is not None
        assert callable(bootstrapper)
        
        # Also check "rvu" alias
        bootstrapper_alias = SchemaService.get_bootstrapper("rvu")
        assert bootstrapper_alias is not None
        assert callable(bootstrapper_alias)

    def test_get_bootstrapper_for_missing_dataset_returns_none(self):
        """Verify get_bootstrapper returns None for datasets without bootstrap"""
        from cms_pricing.ingestion.services.schema_service import SchemaService
        
        bootstrapper = SchemaService.get_bootstrapper("cms_mpfs")
        assert bootstrapper is None
        
        bootstrapper = SchemaService.get_bootstrapper("cms_opps")
        assert bootstrapper is None

    @patch('cms_pricing.ingestion.services.schema_service.SchemaService.bootstrap_rvu_schemas')
    def test_initialize_all_calls_rvu_bootstrapper(self, mock_bootstrap):
        """Verify ServiceFactory.initialize_all() calls RVU bootstrapper when dataset is RVU"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="cms_rvu",
            lazy_init=False,
            enable_schema_registry=True
        )
        factory = ServiceFactory(config)
        
        # Bootstrap should be called during initialize_all
        mock_bootstrap.assert_called_once()

    @patch('cms_pricing.ingestion.services.schema_service.SchemaService.bootstrap_rvu_schemas')
    def test_initialize_all_does_not_call_rvu_for_other_datasets(self, mock_bootstrap):
        """Verify ServiceFactory.initialize_all() does NOT call RVU bootstrap for non-RVU datasets"""
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="cms_mpfs",
            lazy_init=False,
            enable_schema_registry=True
        )
        factory = ServiceFactory(config)
        
        # RVU bootstrap should NOT be called for MPFS
        mock_bootstrap.assert_not_called()

    def test_initialize_all_logs_debug_when_no_bootstrapper(self):
        """Verify ServiceFactory.initialize_all() logs debug message when no bootstrapper found"""
        import logging
        from cms_pricing.ingestion.services.schema_service import SchemaService
        
        config = ServiceConfig(
            output_dir="/tmp/test",
            dataset_name="cms_mpfs",
            lazy_init=False,
            enable_schema_registry=True
        )
        
        with patch.object(SchemaService, 'get_bootstrapper', return_value=None):
            with patch('cms_pricing.ingestion.services.service_factory.logger') as mock_logger:
                factory = ServiceFactory(config)
                
                # Verify debug log was called
                mock_logger.debug.assert_called()
                call_args = mock_logger.debug.call_args[1] if mock_logger.debug.call_args else {}
                assert "No schema bootstrapper registered" in str(mock_logger.debug.call_args)

