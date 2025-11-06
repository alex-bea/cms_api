"""
Service Factory for DIS Ingestors

Provides lazy-loaded access to shared ingestion services (validation, observability,
quarantine, reference data, schema registry) to eliminate duplication across ingestors.

Services are instantiated on first access (lazy initialization) unless lazy_init=False.
"""

from typing import Any, Dict, Optional
import structlog

from .service_config import ServiceConfig

logger = structlog.get_logger()


class ServiceFactory:
    """
    Factory for creating and managing shared ingestion services.
    
    Provides lazy-loaded access to services via properties. Services are created
    on first access unless lazy_init=False in the config.
    
    Per guardrails:
    - Clear module naming: Services are accessed via consistent property names
    - Consistent factory surface area: All services exposed as properties
    - Schema bootstrap coordination: Schema registration happens once
    - Lazy initialization coverage: Services created only when accessed
    """
    
    def __init__(self, config: ServiceConfig):
        """
        Initialize service factory with configuration.
        
        Args:
            config: Service configuration
        """
        self.config = config
        self._services: Dict[str, Any] = {}
        self._initialized = False
        
        # If lazy_init=False, eagerly initialize all services
        if not config.lazy_init:
            self.initialize_all()
    
    @property
    def validation_engine(self) -> Any:
        """
        Get or create validation engine (lazy).
        
        Returns:
            ValidationEngine instance
            
        Raises:
            NotImplementedError: If validation is disabled in config
        """
        if not self.config.enable_validation:
            raise NotImplementedError(
                "Validation engine is disabled in ServiceConfig. "
                "Set enable_validation=True to use this service."
            )
        
        if "validation_engine" not in self._services:
            from ..validators.validation_engine import ValidationEngine
            self._services["validation_engine"] = ValidationEngine()
            logger.debug("Validation engine initialized", dataset=self.config.dataset_name)
        
        return self._services["validation_engine"]
    
    @property
    def validation_service(self) -> Any:
        """
        Get or create validation service (lazy).
        
        Returns:
            ValidationService instance wrapping the validation engine
        """
        if not self.config.enable_validation:
            raise NotImplementedError(
                "Validation service is disabled in ServiceConfig. "
                "Set enable_validation=True to use this service."
            )
        
        if "validation_service" not in self._services:
            from .validation_service import ValidationService
            self._services["validation_service"] = ValidationService(self.validation_engine)
            logger.debug("Validation service initialized", dataset=self.config.dataset_name)
        
        return self._services["validation_service"]
    
    @property
    def observability_collector(self) -> Any:
        """
        Get or create observability collector (lazy).
        
        Returns:
            DISObservabilityCollector instance
            
        Raises:
            NotImplementedError: If observability is disabled in config
        """
        if not self.config.enable_observability:
            raise NotImplementedError(
                "Observability collector is disabled in ServiceConfig. "
                "Set enable_observability=True to use this service."
            )
        
        if "observability_collector" not in self._services:
            from ..observability.dis_observability import DISObservabilityCollector
            self._services["observability_collector"] = DISObservabilityCollector(self.config.output_dir)
            logger.debug("Observability collector initialized", dataset=self.config.dataset_name)
        
        return self._services["observability_collector"]
    
    @property
    def quarantine_manager(self) -> Any:
        """
        Get or create quarantine manager (lazy).
        
        Returns:
            QuarantineManager instance
            
        Raises:
            NotImplementedError: If quarantine is disabled in config
        """
        if not self.config.enable_quarantine:
            raise NotImplementedError(
                "Quarantine manager is disabled in ServiceConfig. "
                "Set enable_quarantine=True to use this service."
            )
        
        if "quarantine_manager" not in self._services:
            from ..quarantine.dis_quarantine import QuarantineManager
            self._services["quarantine_manager"] = QuarantineManager(self.config.output_dir)
            logger.debug("Quarantine manager initialized", dataset=self.config.dataset_name)
        
        return self._services["quarantine_manager"]
    
    @property
    def reference_data_manager(self) -> Any:
        """
        Get or create reference data manager (lazy).
        
        Returns:
            ReferenceDataManager instance
            
        Raises:
            NotImplementedError: If reference data is disabled in config
        """
        if not self.config.enable_reference_data:
            raise NotImplementedError(
                "Reference data manager is disabled in ServiceConfig. "
                "Set enable_reference_data=True to use this service."
            )
        
        if "reference_data_manager" not in self._services:
            from ..enrichers.dis_reference_data_integration import ReferenceDataManager
            self._services["reference_data_manager"] = ReferenceDataManager(self.config.output_dir)
            logger.debug("Reference data manager initialized", dataset=self.config.dataset_name)
        
        return self._services["reference_data_manager"]
    
    @property
    def reference_enricher(self) -> Any:
        """
        Get or create reference enricher (lazy, depends on reference_data_manager).
        
        Returns:
            DISReferenceDataEnricher instance
            
        Raises:
            NotImplementedError: If reference data is disabled in config
        """
        if not self.config.enable_reference_data:
            raise NotImplementedError(
                "Reference enricher is disabled in ServiceConfig. "
                "Set enable_reference_data=True to use this service."
            )
        
        if "reference_enricher" not in self._services:
            # Ensure reference_data_manager is initialized first (dependency)
            ref_manager = self.reference_data_manager
            
            from ..enrichers.dis_reference_data_integration import DISReferenceDataEnricher
            self._services["reference_enricher"] = DISReferenceDataEnricher(ref_manager)
            logger.debug("Reference enricher initialized", dataset=self.config.dataset_name)
        
        return self._services["reference_enricher"]
    
    @property
    def schema_registry(self) -> Any:
        """
        Get or create schema registry (lazy).
        
        Returns:
            SchemaRegistry instance
            
        Raises:
            NotImplementedError: If schema registry is disabled in config
        """
        if not self.config.enable_schema_registry:
            raise NotImplementedError(
                "Schema registry is disabled in ServiceConfig. "
                "Set enable_schema_registry=True to use this service."
            )
        
        if "schema_registry" not in self._services:
            from ..contracts.schema_registry import SchemaRegistry
            self._services["schema_registry"] = SchemaRegistry()
            logger.debug("Schema registry initialized", dataset=self.config.dataset_name)
        
        return self._services["schema_registry"]
    
    def initialize_all(self):
        """
        Eagerly initialize all enabled services.
        
        This is useful when lazy_init=False or when you want to ensure all services
        are ready before starting ingestion.
        """
        if self._initialized:
            return
        
        logger.info("Eagerly initializing all services", dataset=self.config.dataset_name)
        
        # Initialize all enabled services
        if self.config.enable_validation:
            _ = self.validation_engine
        if self.config.enable_observability:
            _ = self.observability_collector
        if self.config.enable_quarantine:
            _ = self.quarantine_manager
        if self.config.enable_reference_data:
            _ = self.reference_data_manager
            _ = self.reference_enricher
        if self.config.enable_schema_registry:
            registry = self.schema_registry
            service = self.schema_service
            # Look up dataset-specific bootstrap function
            bootstrapper = service.get_bootstrapper(self.config.dataset_name)
            if bootstrapper:
                bootstrapper(service, registry)
            else:
                logger.debug(
                    "No schema bootstrapper registered for dataset",
                    dataset=self.config.dataset_name
                )

        self._initialized = True
        logger.info("All services initialized", dataset=self.config.dataset_name)

    @property
    def schema_service(self) -> Any:
        """
        Get or create schema service (lazy).

        Returns:
            SchemaService instance

        Raises:
            NotImplementedError: If schema registry is disabled in config
        """
        if not self.config.enable_schema_registry:
            raise NotImplementedError(
                "Schema service is disabled in ServiceConfig. "
                "Set enable_schema_registry=True to use this service."
            )

        if "schema_service" not in self._services:
            from ..services.schema_service import SchemaService
            self._services["schema_service"] = SchemaService(self.config.dataset_name)
            logger.debug("Schema service initialized", dataset=self.config.dataset_name)

        return self._services["schema_service"]
