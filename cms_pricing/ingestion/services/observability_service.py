"""
Observability Service Adapter

Thin wrapper around DISObservabilityCollector for consistent initialization
and usage patterns across ingestors.
"""

from typing import Any
import structlog

logger = structlog.get_logger()


class ObservabilityService:
    """
    Thin adapter for observability collector.
    
    Provides consistent initialization and helper methods for observability
    operations across all DIS ingestors.
    """
    
    @staticmethod
    def create(output_dir: str) -> Any:
        """
        Create observability collector with consistent configuration.
        
        Args:
            output_dir: Base output directory for observability artifacts
            
        Returns:
            DISObservabilityCollector instance
        """
        from ..observability.dis_observability import DISObservabilityCollector
        return DISObservabilityCollector(output_dir)
    
    @staticmethod
    def record_stage_start(collector: Any, stage_name: str, batch_id: str, release_id: str):
        """
        Record the start of a pipeline stage.
        
        Args:
            collector: DISObservabilityCollector instance
            stage_name: Name of the stage (land, validate, normalize, enrich, publish)
            batch_id: Batch identifier
            release_id: Release identifier
        """
        try:
            collector.record_metric(
                f"stage_{stage_name}_start",
                1,
                tags={"batch_id": batch_id, "release_id": release_id}
            )
        except Exception as e:
            logger.debug("Failed to record stage start", stage=stage_name, error=str(e))
    
    @staticmethod
    def record_stage_complete(collector: Any, stage_name: str, batch_id: str, release_id: str, metrics: dict = None):
        """
        Record the completion of a pipeline stage.
        
        Args:
            collector: DISObservabilityCollector instance
            stage_name: Name of the stage
            batch_id: Batch identifier
            release_id: Release identifier
            metrics: Optional additional metrics to record
        """
        try:
            collector.record_metric(
                f"stage_{stage_name}_complete",
                1,
                tags={"batch_id": batch_id, "release_id": release_id}
            )
            if metrics:
                for key, value in metrics.items():
                    collector.record_metric(
                        f"stage_{stage_name}_{key}",
                        value,
                        tags={"batch_id": batch_id, "release_id": release_id}
                    )
        except Exception as e:
            logger.debug("Failed to record stage completion", stage=stage_name, error=str(e))

