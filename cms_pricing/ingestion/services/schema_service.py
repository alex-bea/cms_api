"""
Schema Service Adapter

Thin wrapper around SchemaRegistry for consistent initialization, bootstrap,
and caching patterns across ingestors.

Per guardrail #3: Ensures schema registration happens exactly once and maintains
existing schema-contract caching semantics.

NOTE: These adapters are currently placeholders for future standardization.
They provide helper methods for common schema operations, but the
ServiceFactory directly exposes SchemaRegistry instances for now.
These adapters can be used when we standardize service interfaces across
multiple ingestors (MPFS, OPPS, ZIP9) in Phase 3 Step 4.
"""

from typing import Any, Dict, Optional
import structlog

logger = structlog.get_logger()


class SchemaService:
    """
    Thin adapter for schema registry with bootstrap support.
    
    Provides consistent initialization, schema registration, and caching
    for schema contracts across all DIS ingestors.
    
    Per guardrail #3: Coordinates schema bootstrap to avoid double-registration
    and maintains caching semantics for validation performance.
    """
    
    @staticmethod
    def create() -> Any:
        """
        Create schema registry with consistent configuration.
        
        Returns:
            SchemaRegistry instance
        """
        from ..contracts.schema_registry import SchemaRegistry
        return SchemaRegistry()
    
    @staticmethod
    def get_contract(registry: Any, schema_name: str) -> Optional[Any]:
        """
        Get schema contract from registry.
        
        Args:
            registry: SchemaRegistry instance
            schema_name: Name of the schema contract
            
        Returns:
            SchemaContract instance or None if not found
        """
        try:
            return registry.get_contract(schema_name)
        except Exception as e:
            logger.debug("Schema contract not found", schema=schema_name, error=str(e))
            return None
    
    @staticmethod
    def bootstrap_rvu_schemas(registry: Any):
        """
        Bootstrap RVU-related schema contracts.
        
        This ensures schema registration happens exactly once (per guardrail #3).
        Should be called during ingestor initialization.
        
        Args:
            registry: SchemaRegistry instance
        """
        # Import schema registration logic
        # This will be populated from RVUIngestor._register_schema_contracts()
        # For now, this is a placeholder - actual implementation will be added
        # when migrating RVUIngestor to use the service factory
        
        logger.debug("RVU schemas bootstrap called", 
                    registry_type=type(registry).__name__)
        
        # TODO: Extract schema registration from RVUIngestor._register_schema_contracts()
        #       and implement here to avoid double-registration
    
    @staticmethod
    def get_cached_schemas(registry: Any, dataset_to_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Pre-cache schema contracts for validation performance.
        
        This maintains existing schema-contract caching semantics (per guardrail #3).
        
        Args:
            registry: SchemaRegistry instance
            dataset_to_schema: Dictionary mapping dataset names to schema names
            
        Returns:
            Dictionary mapping dataset names to cached SchemaContract instances
        """
        cached = {}
        for dataset_name, schema_name in dataset_to_schema.items():
            schema = SchemaService.get_contract(registry, schema_name)
            if schema:
                cached[dataset_name] = schema
                logger.debug("Schema cached", dataset=dataset_name, schema=schema_name)
        return cached

