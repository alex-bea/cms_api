"""
DIS-Compliant Pipeline Orchestrator
Following Data Ingestion Standard PRD v1.0

This module provides the main pipeline orchestration following the
DIS Land → Validate → Normalize → Enrich → Publish architecture.
"""

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Type
import structlog

from ..contracts.ingestor_spec import IngestorSpec, SourceFile, RawBatch, AdaptedBatch, StageFrame, RefData
from ..contracts.schema_registry import schema_registry
from ..validators.validation_engine import ValidationEngine
from ..adapters.data_adapters import AdapterFactory, AdapterConfig
from ..enrichers.data_enrichers import EnricherFactory
from ..publishers.data_publishers import PublisherFactory, PublishSpec
from ..quarantine.quarantine_manager import QuarantineManager
from ..observability.metrics_collector import DISObservabilityCollector

logger = structlog.get_logger()


@dataclass
class PipelineConfig:
    """Configuration for DIS pipeline execution"""
    output_dir: str
    quarantine_dir: str = "cms_pricing/ingestion/quarantine"
    enable_quarantine: bool = True
    enable_observability: bool = True
    quality_threshold: float = 0.95
    max_retries: int = 3


class DISPipeline:
    """
    Main DIS-compliant pipeline orchestrator.
    
    Implements the complete Land → Validate → Normalize → Enrich → Publish
    architecture with full observability, quarantine, and error handling.
    """
    
    def __init__(
        self, 
        ingestor: IngestorSpec, 
        output_dir: str, 
        db_session: Any = None,
        config: PipelineConfig = None
    ):
        self.ingestor = ingestor
        self.output_dir = Path(output_dir)
        self.db_session = db_session
        self.config = config or PipelineConfig(output_dir=output_dir)
        
        # Initialize components
        self.validation_engine = ValidationEngine()
        self.quarantine_manager = QuarantineManager(self.config.quarantine_dir)
        self.observability_collector = DISObservabilityCollector(ingestor.dataset_name)
        
        # Create output directories
        self.raw_dir = self.output_dir / "raw" / ingestor.dataset_name
        self.stage_dir = self.output_dir / "stage" / ingestor.dataset_name
        self.curated_dir = self.output_dir / "curated" / "payments" / ingestor.dataset_name
        
        for dir_path in [self.raw_dir, self.stage_dir, self.curated_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    async def execute(self, release_id: str, batch_id: str) -> Dict[str, Any]:
        """
        Execute the complete DIS pipeline.
        
        Args:
            release_id: Unique identifier for this release
            batch_id: Unique identifier for this batch execution
            
        Returns:
            Complete pipeline execution results
        """
        
        logger.info(
            "Starting DIS pipeline execution",
            dataset=self.ingestor.dataset_name,
            release_id=release_id,
            batch_id=batch_id
        )
        
        try:
            # Start observability collection
            if self.config.enable_observability:
                self.observability_collector.record_ingestion_start()
            
            # Stage 1: Land - Download and store raw files
            raw_batch = await self._land_data(release_id, batch_id)
            
            # Stage 2: Validate - Structural and domain validation
            validation_results = await self._validate_data(raw_batch, release_id)
            
            # Stage 3: Normalize - Adapt and canonicalize data
            adapted_batch = await self._normalize_data(validation_results, raw_batch, release_id)
            
            # Stage 4: Enrich - Join with reference data
            enriched_data = await self._enrich_data(adapted_batch, release_id)
            
            # Stage 5: Publish - Store in curated format
            publish_results = await self._publish_data(enriched_data, release_id, batch_id)
            
            # Generate final results
            result = await self._generate_final_results(
                release_id, batch_id, raw_batch, validation_results, 
                adapted_batch, enriched_data, publish_results
            )
            
            # Record completion
            if self.config.enable_observability:
                self.observability_collector.record_ingestion_end()
                result["observability"] = self.observability_collector.generate_observability_report(batch_id)
            
            logger.info(
                "DIS pipeline execution completed successfully",
                dataset=self.ingestor.dataset_name,
                release_id=release_id,
                batch_id=batch_id,
                record_count=publish_results.get("record_count", 0)
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "DIS pipeline execution failed",
                dataset=self.ingestor.dataset_name,
                release_id=release_id,
                batch_id=batch_id,
                error=str(e)
            )
            
            if self.config.enable_observability:
                self.observability_collector.record_error("pipeline_failure", str(e))
            
            raise
    
    async def _land_data(self, release_id: str, batch_id: str) -> RawBatch:
        """Stage 1: Land raw data from source"""
        
        logger.info("Landing raw data", dataset=self.ingestor.dataset_name)
        
        # Use ingestor's land method
        land_result = await self.ingestor.land(release_id)
        
        # Extract source files and raw content from land result
        source_files = land_result.get("source_files", [])
        raw_content = land_result.get("raw_content", {})
        
        # Record source files in observability
        if self.config.enable_observability:
            self.observability_collector.record_source_files([
                {
                    "filename": sf.filename,
                    "url": sf.url,
                    "size_bytes": sf.expected_size_bytes,
                    "checksum": sf.checksum,
                    "file_type": getattr(sf, "file_type", None)
                }
                for sf in source_files
            ])
        
        return RawBatch(
            source_files=source_files,
            raw_content=raw_content,
            metadata={
                "release_id": release_id,
                "batch_id": batch_id,
                "manifest": land_result.get("manifest"),
                "docs_directory": land_result.get("docs_directory"),
                "docs_manifest_path": land_result.get("docs_manifest_path"),
                "guidance_documents": land_result.get("guidance_documents", [])
            }
        )
    
    async def _validate_data(self, raw_batch: RawBatch, release_id: str) -> Dict[str, Any]:
        """Stage 2: Validate raw data"""
        
        logger.info("Validating data", dataset=self.ingestor.dataset_name)
        
        # Use ingestor's validate method
        validation_results = await self.ingestor.validate(raw_batch)

        if isinstance(validation_results, dict):
            validation_results.setdefault("raw_batch", raw_batch)
        
        # Record validation results in observability
        if self.config.enable_observability:
            overall_quality = validation_results.get("overall_quality_score", 0)
            self.observability_collector.record_validation_results({
                "quality_score": overall_quality,
                "total_checks": validation_results.get("total_checks", 0),
                "passed_checks": validation_results.get("passed_checks", 0),
                "failed_checks": validation_results.get("failed_checks", 0),
                "warning_checks": validation_results.get("warning_checks", 0)
            })
        
        return validation_results
    
    async def _normalize_data(self, validated_batch: Dict[str, Any], raw_batch: RawBatch, release_id: str) -> AdaptedBatch:
        """Stage 3: Normalize and adapt data"""
        
        logger.info("Normalizing data", dataset=self.ingestor.dataset_name)
        
        # Use ingestor's normalize method
        normalize_result = await self.ingestor.normalize(validated_batch, raw_batch)
        
        # Create AdaptedBatch from normalize result
        adapted_batch = AdaptedBatch(
            dataframes=normalize_result.get("dataframes", {}),
            schema_contract=normalize_result.get("schema_contract", {}),
            metadata=normalize_result.get("metadata", {})
        )
        
        # Save normalized data to stage
        stage_dir = self.stage_dir / release_id
        stage_dir.mkdir(parents=True, exist_ok=True)
        
        for table_name, df in adapted_batch.dataframes.items():
            stage_file = stage_dir / f"{table_name}.parquet"
            df.to_parquet(stage_file, index=False)
        
        return adapted_batch
    
    async def _enrich_data(self, adapted_batch: AdaptedBatch, release_id: str) -> Dict[str, Any]:
        """Stage 4: Enrich data with reference tables"""
        
        logger.info("Enriching data", dataset=self.ingestor.dataset_name)
        
        # Load reference data (this would be implemented based on dataset needs)
        ref_data = await self._load_reference_data()
        
        enriched_data = {}
        
        for table_name, df in adapted_batch.dataframes.items():
            # Create stage frame
            stage_frame = StageFrame(
                data=df,
                schema=adapted_batch.schema_contract,
                metadata=adapted_batch.metadata,
                quality_metrics={}
            )
            
            # Apply enrichment rules
            enricher_func = self.ingestor.enricher
            enriched_df = enricher_func(stage_frame, ref_data)
            
            enriched_data[table_name] = enriched_df
        
        # Record enrichment results
        if self.config.enable_observability:
            total_records = sum(len(df) for df in enriched_data.values())
            self.observability_collector.record_enrichment_results({
                "enrichment_count": total_records
            })
        
        return enriched_data
    
    async def _publish_data(self, enriched_data: Dict[str, Any], release_id: str, batch_id: str) -> Dict[str, Any]:
        """Stage 5: Publish data to curated storage"""
        
        logger.info("Publishing data", dataset=self.ingestor.dataset_name)
        
        # Use ingestor's publish method
        publish_result = await self.ingestor.publish(enriched_data)
        
        # Record publish results
        if self.config.enable_observability:
            self.observability_collector.record_publish_results({
                "record_count": publish_result.get("record_count", 0),
                "file_paths": publish_result.get("file_paths", [])
            })
        
        return publish_result
    
    async def _generate_final_results(
        self, 
        release_id: str, 
        batch_id: str,
        raw_batch: RawBatch,
        validation_results: Dict[str, Any],
        adapted_batch: AdaptedBatch,
        enriched_data: Dict[str, Any],
        publish_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate final pipeline results"""
        
        # Extract quality metrics from validation results
        quality_score = validation_results.get("quality_score", 100.0)
        record_count = validation_results.get("total_records", 0)
        valid_records = validation_results.get("valid_records", 0)

        guidance_summary = None
        if hasattr(self.ingestor, "generate_guidance_summary"):
            try:
                summary_result = self.ingestor.generate_guidance_summary(raw_batch, adapted_batch)
                if summary_result:
                    guidance_summary = summary_result
            except Exception as exc:
                logger.warning(
                    "Guidance summary generation failed",
                    dataset=self.ingestor.dataset_name,
                    release_id=release_id,
                    error=str(exc)
                )
                if self.config.enable_observability:
                    self.observability_collector.record_error("guidance_summary_failed", str(exc))
        
        result = {
            "status": "success",
            "dataset_name": self.ingestor.dataset_name,
            "release_id": release_id,
            "batch_id": batch_id,
            "pipeline_version": "1.0",
            "dis_version": "1.0",
            "execution_timestamp": datetime.utcnow().isoformat(),
            "record_count": record_count,
            "valid_records": valid_records,
            "quality_score": quality_score,
            "record_counts": {
                table: len(df) for table, df in enriched_data.items()
            },
            "validation_results": validation_results,
            "publish_results": publish_results,
            "source_files": [
                {
                    "filename": sf.filename,
                    "url": sf.url,
                    "size_bytes": sf.expected_size_bytes,
                    "checksum": sf.checksum,
                    "file_type": getattr(sf, "file_type", None)
                }
                for sf in raw_batch.source_files
            ]
        }
        if guidance_summary:
            result["guidance_summary"] = guidance_summary
        return result
    
    async def _download_file(self, source_file: SourceFile) -> bytes:
        """Download file from source URL"""
        # This would be implemented by the specific ingestor
        # For now, we'll raise an error to indicate it needs implementation
        raise NotImplementedError("File download must be implemented by the ingestor")
    
    async def _parse_file_for_validation(self, filename: str, content: bytes) -> Any:
        """Parse file for validation purposes"""
        # This would use the appropriate adapter based on file type
        # For now, we'll raise an error to indicate it needs implementation
        raise NotImplementedError("File parsing must be implemented by the ingestor")
    
    async def _load_reference_data(self) -> RefData:
        """Load reference data for enrichment"""
        # This would load reference tables from the database
        # For now, we'll return empty reference data
        return RefData(tables={}, metadata={})
