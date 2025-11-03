"""
Enrichment stage module for DIS pipeline.

Per DIS §3.5: Join with reference data, compute mapping confidence, and apply derived fields.
This module extracts enrichment logic from ingestors for reuse across datasets.
"""

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any
import structlog

from ..contracts.ingestor_spec import StageFrame, RefData
from ..enrichers.dis_reference_data_integration import (
    DISReferenceDataEnricher,
    get_rvu_geography_enrichment_rules,
    get_rvu_code_enrichment_rules
)

logger = structlog.get_logger()


@dataclass
class EnrichConfig:
    """Configuration for enrichment stage"""
    enable_enrichment: bool = True  # Feature flag (default: True per feedback)
    geography_rules_enabled: bool = True
    code_rules_enabled: bool = True
    log_feature_flag_state: bool = True


async def execute_enrich(
    stage_frame: StageFrame,
    ref_data: RefData,
    config: EnrichConfig,
    reference_enricher: DISReferenceDataEnricher,
    reference_data_manager: Any,
    observability_collector: Optional[Any] = None,
    release_id: Optional[str] = None
) -> StageFrame:
    """
    Execute enrichment stage with shared services per DIS §3.5.
    
    This function extracts the enrichment logic from _enrich_data for reuse.
    It applies reference data joins, computes mapping confidence, and updates quality metrics.
    
    Args:
        stage_frame: Staged data frame with schema and metadata
        ref_data: Reference data tables for enrichment
        config: Enrichment configuration
        reference_enricher: DIS-compliant enricher instance
        reference_data_manager: Reference data manager for loading refs
        observability_collector: Optional observability collector for metrics
        release_id: Optional release ID for logging
        
    Returns:
        Enriched StageFrame with updated quality metrics
    """
    # Feature flag check (following REF_MODE pattern per STD-data-architecture-impl-v1.0.md §4.2.1)
    if not config.enable_enrichment:
        logger.warning("Enrichment disabled via config, returning original data")
        return stage_frame
    
    # Log feature flag state in observability (per feedback)
    if config.log_feature_flag_state and observability_collector:
        try:
            observability_collector.record_metric(
                "enrichment_feature_flag",
                1 if config.enable_enrichment else 0,
                tags={"release_id": release_id or "unknown"}
            )
        except Exception as metric_err:
            logger.debug("enrichment_flag_metric_failed", error=str(metric_err))
    
    try:
        # Load reference data into the reference data manager and capture summary
        ref_load_summary = _load_reference_data_for_enrichment(
            ref_data,
            reference_data_manager,
            observability_collector,
            release_id
        )
        
        # Get enrichment rules (can be customized per dataset)
        all_rules = []
        if config.geography_rules_enabled:
            all_rules.extend(get_rvu_geography_enrichment_rules())
        if config.code_rules_enabled:
            all_rules.extend(get_rvu_code_enrichment_rules())
        
        # Apply enrichment using DIS-compliant enricher
        enriched_df, enrichment_results = reference_enricher.enrich_data(
            source_df=stage_frame.data,
            enrichment_rules=all_rules,
            effective_date=stage_frame.metadata.get("effective_date")
        )
        
        # Update quality metrics with enrichment results
        enrichment_quality_score = (
            sum(r.quality_score for r in enrichment_results) / len(enrichment_results)
            if enrichment_results else 1.0
        )
        enrichment_rate = (
            sum(r.enrichment_rate for r in enrichment_results) / len(enrichment_results)
            if enrichment_results else 1.0
        )
        
        updated_quality_metrics = stage_frame.quality_metrics.copy()
        updated_quality_metrics.update({
            "enrichment_quality_score": enrichment_quality_score,
            "enrichment_rate": enrichment_rate,
            "enrichment_rules_applied": len(enrichment_results),
            "enrichment_successful": sum(1 for r in enrichment_results if r.success)  # Vectorized count, avoids intermediate list
        })
        
        # Optional guardrail: flag if all reference datasets were missing/empty
        try:
            nonempty_ref = (ref_load_summary or {}).get("nonempty_count", 0)
            if nonempty_ref == 0:
                updated_quality_metrics["reference_data_missing"] = True
                logger.warning(
                    "reference_data_all_missing",
                    release_id=release_id or stage_frame.metadata.get("release_id", "unknown")
                )
                if observability_collector:
                    try:
                        observability_collector.record_metric(
                            "reference_data_all_missing", 1,
                            tags={"release_id": release_id or stage_frame.metadata.get("release_id", "unknown")}
                        )
                    except Exception as metric_err:
                        logger.debug("reference_missing_metric_emit_failed", error=str(metric_err))
        except Exception as guard_err:
            logger.debug("reference_missing_guard_failed", error=str(guard_err))
        
        # Log enrichment results
        logger.info("Data enrichment completed",
                   rules_applied=len(enrichment_results),
                   enrichment_rate=enrichment_rate,
                   quality_score=enrichment_quality_score)
        
        return StageFrame(
            data=enriched_df,
            schema=stage_frame.schema,
            metadata=stage_frame.metadata,
            quality_metrics=updated_quality_metrics
        )
        
    except Exception as e:
        logger.error(f"Data enrichment failed: {e}", exc_info=True)
        # Return original data if enrichment fails (fail-safe)
        return stage_frame


def _load_reference_data_for_enrichment(
    ref_data: RefData,
    reference_data_manager: Any,
    observability_collector: Optional[Any] = None,
    release_id: Optional[str] = None
) -> Dict[str, int]:
    """
    Load and verify reference data for enrichment with structured logging and metrics.
    
    Returns a summary dict with nonempty_count, empty_count, failure_count.
    """
    load_targets = [
        ("cms_zip_locality", ref_data.tables.get("cms_zip_locality")),
        ("cms_gpci", ref_data.tables.get("cms_gpci")),
        ("cms_hcpcs_codes", ref_data.tables.get("cms_hcpcs_codes")),
    ]
    success_count = 0
    failure_count = 0
    empty_count = 0
    
    for name, table in load_targets:
        if table is None:
            continue
        try:
            reference_data_manager.load_reference_data(name, table)
            row_count = len(table) if hasattr(table, "__len__") else None
            if row_count is None or row_count == 0:
                logger.warning(
                    "reference_data_loaded_empty",
                    dataset=name,
                    rows=row_count
                )
                empty_count += 1
            else:
                success_count += 1
                logger.info(
                    "reference_data_loaded",
                    dataset=name,
                    rows=row_count
                )
        except Exception as load_err:
            failure_count += 1
            logger.error(
                "reference_data_load_failed",
                dataset=name,
                error=str(load_err)
            )
    
    # Emit observability metrics (best-effort)
    if observability_collector:
        try:
            observability_collector.record_metric(
                "reference_data_load_success_count", success_count,
                tags={"release_id": release_id or "unknown"}
            )
            observability_collector.record_metric(
                "reference_data_load_failure_count", failure_count,
                tags={"release_id": release_id or "unknown"}
            )
            observability_collector.record_metric(
                "reference_data_load_empty_count", empty_count,
                tags={"release_id": release_id or "unknown"}
            )
        except Exception as metric_err:
            logger.debug("reference_data_metrics_emit_failed", error=str(metric_err))
    
    return {
        "nonempty_count": success_count,
        "empty_count": empty_count,
        "failure_count": failure_count
    }

