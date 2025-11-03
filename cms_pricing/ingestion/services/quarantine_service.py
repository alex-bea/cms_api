"""
Quarantine Service Adapter

Thin wrapper around QuarantineManager for consistent initialization
and usage patterns across ingestors.
"""

from typing import Any, Dict, List, Optional
import structlog

logger = structlog.get_logger()


class QuarantineService:
    """
    Thin adapter for quarantine manager.
    
    Provides consistent initialization and helper methods for quarantine
    operations across all DIS ingestors.
    """
    
    @staticmethod
    def create(output_dir: str) -> Any:
        """
        Create quarantine manager with consistent configuration.
        
        Args:
            output_dir: Base output directory for quarantine artifacts
            
        Returns:
            QuarantineManager instance
        """
        from ..quarantine.dis_quarantine import QuarantineManager
        return QuarantineManager(output_dir)
    
    @staticmethod
    def quarantine_validation_failures(
        manager: Any,
        dataset_name: str,
        batch_id: str,
        release_id: str,
        validation_results: Dict[str, Any],
        raw_data: List[Dict[str, Any]]
    ) -> Any:
        """
        Quarantine records that failed validation.
        
        Args:
            manager: QuarantineManager instance
            dataset_name: Name of the dataset
            batch_id: Batch identifier
            release_id: Release identifier
            validation_results: Validation results dictionary
            raw_data: Raw data records that failed validation
            
        Returns:
            QuarantineBatch with quarantined records
        """
        try:
            return manager.quarantine_records(
                dataset_name=dataset_name,
                batch_id=batch_id,
                release_id=release_id,
                validation_results=validation_results,
                raw_data=raw_data
            )
        except Exception as e:
            logger.error("Failed to quarantine records", error=str(e), dataset=dataset_name)
            raise

