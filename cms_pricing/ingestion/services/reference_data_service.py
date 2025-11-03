"""
Reference Data Service Adapter

Thin wrapper around ReferenceDataManager and DISReferenceDataEnricher
for consistent initialization and usage patterns across ingestors.
"""

from typing import Any, Dict, Optional
import structlog

logger = structlog.get_logger()


class ReferenceDataService:
    """
    Thin adapter for reference data management and enrichment.
    
    Provides consistent initialization and helper methods for reference data
    operations across all DIS ingestors.
    """
    
    @staticmethod
    def create_manager(output_dir: str) -> Any:
        """
        Create reference data manager with consistent configuration.
        
        Args:
            output_dir: Base output directory for reference data artifacts
            
        Returns:
            ReferenceDataManager instance
        """
        from ..enrichers.dis_reference_data_integration import ReferenceDataManager
        return ReferenceDataManager(output_dir)
    
    @staticmethod
    def create_enricher(manager: Any) -> Any:
        """
        Create reference data enricher with consistent configuration.
        
        Args:
            manager: ReferenceDataManager instance
            
        Returns:
            DISReferenceDataEnricher instance
        """
        from ..enrichers.dis_reference_data_integration import DISReferenceDataEnricher
        return DISReferenceDataEnricher(manager)
    
    @staticmethod
    def register_reference_sources(
        manager: Any,
        sources: Dict[str, Dict[str, Any]]
    ):
        """
        Register reference data sources with the manager.
        
        Args:
            manager: ReferenceDataManager instance
            sources: Dictionary mapping source names to source metadata
        """
        try:
            for source_name, source_metadata in sources.items():
                manager.register_reference_source(
                    source_name,
                    source_metadata.get("url"),
                    source_metadata.get("metadata", {})
                )
            logger.debug("Reference sources registered", count=len(sources))
        except Exception as e:
            logger.error("Failed to register reference sources", error=str(e))
            raise

