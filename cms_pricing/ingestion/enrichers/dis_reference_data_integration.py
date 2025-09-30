"""
DIS-Compliant Reference Data Integration System
Following Data Ingestion Standard PRD v1.0

This module implements comprehensive reference data integration with:
- Multi-source reference data management
- Data lineage tracking
- Quality scoring and validation
- Automatic fallback and enrichment strategies
- Reference data versioning and temporal resolution
"""

import json
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd
import structlog

logger = structlog.get_logger()


class ReferenceDataSource(Enum):
    """Reference data source types"""
    CMS_OFFICIAL = "cms_official"
    CENSUS = "census"
    HUD = "hud"
    NBER = "nber"
    INTERNAL = "internal"
    EXTERNAL_API = "external_api"


class EnrichmentStrategy(Enum):
    """Enrichment strategy types"""
    EXACT_MATCH = "exact_match"
    FUZZY_MATCH = "fuzzy_match"
    NEAREST_NEIGHBOR = "nearest_neighbor"
    INTERPOLATION = "interpolation"
    FALLBACK = "fallback"
    DEFAULT_VALUE = "default_value"


@dataclass
class ReferenceDataMetadata:
    """Metadata for reference data sources"""
    source_name: str
    source_type: ReferenceDataSource
    version: str
    effective_from: date
    effective_to: Optional[date]
    last_updated: datetime
    record_count: int
    quality_score: float
    schema_version: str
    data_license: str
    attribution_required: bool
    refresh_cadence: str
    confidence_level: str  # "high", "medium", "low"
    coverage_scope: str  # "national", "state", "local", "regional"


@dataclass
class EnrichmentRule:
    """Enhanced enrichment rule with DIS compliance"""
    rule_id: str
    name: str
    description: str
    source_columns: List[str]
    target_columns: List[str]
    join_keys: List[Tuple[str, str]]  # (source_key, reference_key)
    strategy: EnrichmentStrategy
    reference_source: str
    confidence_threshold: float
    fallback_strategy: Optional[EnrichmentStrategy] = None
    default_values: Dict[str, Any] = None
    validation_rules: List[str] = None
    business_rules: List[str] = None
    quality_gates: Dict[str, float] = None
    
    def __post_init__(self):
        if self.default_values is None:
            self.default_values = {}
        if self.validation_rules is None:
            self.validation_rules = []
        if self.business_rules is None:
            self.business_rules = []
        if self.quality_gates is None:
            self.quality_gates = {}


@dataclass
class EnrichmentResult:
    """Result of enrichment operation"""
    rule_id: str
    success: bool
    records_processed: int
    records_enriched: int
    records_failed: int
    enrichment_rate: float
    quality_score: float
    processing_time_seconds: float
    errors: List[str] = None
    warnings: List[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.metadata is None:
            self.metadata = {}


class ReferenceDataManager:
    """Manages reference data sources and metadata"""
    
    def __init__(self, output_dir: str = "data/reference"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.reference_sources: Dict[str, ReferenceDataMetadata] = {}
        self.reference_data: Dict[str, pd.DataFrame] = {}
        self._load_reference_metadata()
    
    def _load_reference_metadata(self):
        """Load reference data metadata from disk"""
        metadata_file = self.output_dir / "reference_metadata.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as f:
                    data = json.load(f)
                
                for source_name, metadata_dict in data.items():
                    metadata = ReferenceDataMetadata(**metadata_dict)
                    self.reference_sources[source_name] = metadata
                    
                logger.info(f"Loaded {len(self.reference_sources)} reference data sources")
            except Exception as e:
                logger.error(f"Failed to load reference metadata: {e}")
    
    def register_reference_source(
        self,
        source_name: str,
        source_type: ReferenceDataSource,
        version: str,
        effective_from: date,
        effective_to: Optional[date],
        record_count: int,
        quality_score: float,
        schema_version: str = "1.0",
        data_license: str = "Unknown",
        attribution_required: bool = False,
        refresh_cadence: str = "quarterly",
        confidence_level: str = "medium",
        coverage_scope: str = "national"
    ) -> ReferenceDataMetadata:
        """Register a new reference data source"""
        
        metadata = ReferenceDataMetadata(
            source_name=source_name,
            source_type=source_type,
            version=version,
            effective_from=effective_from,
            effective_to=effective_to,
            last_updated=datetime.utcnow(),
            record_count=record_count,
            quality_score=quality_score,
            schema_version=schema_version,
            data_license=data_license,
            attribution_required=attribution_required,
            refresh_cadence=refresh_cadence,
            confidence_level=confidence_level,
            coverage_scope=coverage_scope
        )
        
        self.reference_sources[source_name] = metadata
        self._save_reference_metadata()
        
        logger.info(f"Registered reference source: {source_name}")
        return metadata
    
    def load_reference_data(self, source_name: str, data: pd.DataFrame) -> bool:
        """Load reference data for a source"""
        try:
            self.reference_data[source_name] = data.copy()
            
            # Update metadata
            if source_name in self.reference_sources:
                self.reference_sources[source_name].record_count = len(data)
                self.reference_sources[source_name].last_updated = datetime.utcnow()
                self._save_reference_metadata()
            
            logger.info(f"Loaded reference data for {source_name}: {len(data)} records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load reference data for {source_name}: {e}")
            return False
    
    def get_reference_data(self, source_name: str, effective_date: Optional[date] = None) -> Optional[pd.DataFrame]:
        """Get reference data for a source, optionally filtered by effective date"""
        if source_name not in self.reference_data:
            return None
        
        data = self.reference_data[source_name].copy()
        
        if effective_date and source_name in self.reference_sources:
            metadata = self.reference_sources[source_name]
            
            # Filter by effective date if applicable
            if 'effective_from' in data.columns and 'effective_to' in data.columns:
                mask = (data['effective_from'] <= effective_date) & (
                    (data['effective_to'].isna()) | (data['effective_to'] >= effective_date)
                )
                data = data[mask]
        
        return data
    
    def get_available_sources(self, source_type: Optional[ReferenceDataSource] = None) -> List[str]:
        """Get available reference sources, optionally filtered by type"""
        if source_type is None:
            return list(self.reference_sources.keys())
        
        return [
            name for name, metadata in self.reference_sources.items()
            if metadata.source_type == source_type
        ]
    
    def _save_reference_metadata(self):
        """Save reference metadata to disk"""
        metadata_file = self.output_dir / "reference_metadata.json"
        
        # Convert to dict for JSON serialization
        metadata_dict = {}
        for name, metadata in self.reference_sources.items():
            metadata_dict[name] = asdict(metadata)
            # Convert datetime objects to ISO strings
            metadata_dict[name]['last_updated'] = metadata.last_updated.isoformat()
            if metadata.effective_from:
                metadata_dict[name]['effective_from'] = metadata.effective_from.isoformat()
            if metadata.effective_to:
                metadata_dict[name]['effective_to'] = metadata.effective_to.isoformat()
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata_dict, f, indent=2)


class DISReferenceDataEnricher:
    """DIS-compliant reference data enricher with advanced capabilities"""
    
    def __init__(self, reference_manager: ReferenceDataManager):
        self.reference_manager = reference_manager
        self.enrichment_history: List[EnrichmentResult] = []
    
    def enrich_data(
        self,
        source_df: pd.DataFrame,
        enrichment_rules: List[EnrichmentRule],
        effective_date: Optional[date] = None
    ) -> Tuple[pd.DataFrame, List[EnrichmentResult]]:
        """Enrich source data with reference data following DIS standards"""
        
        enriched_df = source_df.copy()
        enrichment_results = []
        
        for rule in enrichment_rules:
            start_time = datetime.utcnow()
            
            try:
                # Get reference data for this rule
                ref_data = self.reference_manager.get_reference_data(
                    rule.reference_source, 
                    effective_date
                )
                
                if ref_data is None:
                    result = EnrichmentResult(
                        rule_id=rule.rule_id,
                        success=False,
                        records_processed=len(enriched_df),
                        records_enriched=0,
                        records_failed=len(enriched_df),
                        enrichment_rate=0.0,
                        quality_score=0.0,
                        processing_time_seconds=0.0,
                        errors=[f"Reference data not found: {rule.reference_source}"]
                    )
                    enrichment_results.append(result)
                    continue
                
                # Apply enrichment based on strategy
                if rule.strategy == EnrichmentStrategy.EXACT_MATCH:
                    enriched_df, result = self._apply_exact_match_enrichment(
                        enriched_df, ref_data, rule, start_time
                    )
                elif rule.strategy == EnrichmentStrategy.FUZZY_MATCH:
                    enriched_df, result = self._apply_fuzzy_match_enrichment(
                        enriched_df, ref_data, rule, start_time
                    )
                elif rule.strategy == EnrichmentStrategy.NEAREST_NEIGHBOR:
                    enriched_df, result = self._apply_nearest_neighbor_enrichment(
                        enriched_df, ref_data, rule, start_time
                    )
                elif rule.strategy == EnrichmentStrategy.INTERPOLATION:
                    enriched_df, result = self._apply_interpolation_enrichment(
                        enriched_df, ref_data, rule, start_time
                    )
                elif rule.strategy == EnrichmentStrategy.FALLBACK:
                    enriched_df, result = self._apply_fallback_enrichment(
                        enriched_df, ref_data, rule, start_time
                    )
                else:
                    enriched_df, result = self._apply_default_value_enrichment(
                        enriched_df, rule, start_time
                    )
                
                enrichment_results.append(result)
                
            except Exception as e:
                processing_time = (datetime.utcnow() - start_time).total_seconds()
                result = EnrichmentResult(
                    rule_id=rule.rule_id,
                    success=False,
                    records_processed=len(enriched_df),
                    records_enriched=0,
                    records_failed=len(enriched_df),
                    enrichment_rate=0.0,
                    quality_score=0.0,
                    processing_time_seconds=processing_time,
                    errors=[str(e)]
                )
                enrichment_results.append(result)
                logger.error(f"Enrichment rule {rule.rule_id} failed: {e}")
        
        # Store enrichment history
        self.enrichment_history.extend(enrichment_results)
        
        # Add enrichment metadata to result
        enriched_df = self._add_enrichment_metadata(enriched_df, enrichment_results)
        
        return enriched_df, enrichment_results
    
    def _apply_exact_match_enrichment(
        self, 
        source_df: pd.DataFrame, 
        ref_data: pd.DataFrame, 
        rule: EnrichmentRule, 
        start_time: datetime
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """Apply exact match enrichment"""
        
        enriched_df = source_df.copy()
        records_processed = len(enriched_df)
        records_enriched = 0
        errors = []
        warnings = []
        
        try:
            # Apply joins for each join key pair
            for source_key, ref_key in rule.join_keys:
                if source_key in enriched_df.columns and ref_key in ref_data.columns:
                    # Perform left join
                    before_count = len(enriched_df)
                    enriched_df = enriched_df.merge(
                        ref_data,
                        left_on=source_key,
                        right_on=ref_key,
                        how='left',
                        suffixes=('', f'_{rule.reference_source}')
                    )
                    
                    # Count successful enrichments
                    new_enriched = len(enriched_df) - before_count
                    records_enriched += new_enriched
                    
                    # Check for duplicates
                    if len(enriched_df) > before_count:
                        warnings.append(f"Duplicate records found for key {source_key}")
                else:
                    errors.append(f"Join key not found: {source_key} or {ref_key}")
            
            # Apply default values for missing enrichments
            for col, default_val in rule.default_values.items():
                if col not in enriched_df.columns:
                    enriched_df[col] = default_val
                else:
                    enriched_df[col] = enriched_df[col].fillna(default_val)
            
            # Calculate quality metrics
            enrichment_rate = records_enriched / records_processed if records_processed > 0 else 0.0
            quality_score = self._calculate_quality_score(enriched_df, rule)
            
        except Exception as e:
            errors.append(str(e))
            enrichment_rate = 0.0
            quality_score = 0.0
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        result = EnrichmentResult(
            rule_id=rule.rule_id,
            success=len(errors) == 0,
            records_processed=records_processed,
            records_enriched=records_enriched,
            records_failed=records_processed - records_enriched,
            enrichment_rate=enrichment_rate,
            quality_score=quality_score,
            processing_time_seconds=processing_time,
            errors=errors,
            warnings=warnings
        )
        
        return enriched_df, result
    
    def _apply_fuzzy_match_enrichment(
        self, 
        source_df: pd.DataFrame, 
        ref_data: pd.DataFrame, 
        rule: EnrichmentRule, 
        start_time: datetime
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """Apply fuzzy match enrichment (simplified implementation)"""
        # This would implement fuzzy matching logic
        # For now, fall back to exact match
        return self._apply_exact_match_enrichment(source_df, ref_data, rule, start_time)
    
    def _apply_nearest_neighbor_enrichment(
        self, 
        source_df: pd.DataFrame, 
        ref_data: pd.DataFrame, 
        rule: EnrichmentRule, 
        start_time: datetime
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """Apply nearest neighbor enrichment (simplified implementation)"""
        # This would implement nearest neighbor logic
        # For now, fall back to exact match
        return self._apply_exact_match_enrichment(source_df, ref_data, rule, start_time)
    
    def _apply_interpolation_enrichment(
        self, 
        source_df: pd.DataFrame, 
        ref_data: pd.DataFrame, 
        rule: EnrichmentRule, 
        start_time: datetime
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """Apply interpolation enrichment (simplified implementation)"""
        # This would implement interpolation logic
        # For now, fall back to exact match
        return self._apply_exact_match_enrichment(source_df, ref_data, rule, start_time)
    
    def _apply_fallback_enrichment(
        self, 
        source_df: pd.DataFrame, 
        ref_data: pd.DataFrame, 
        rule: EnrichmentRule, 
        start_time: datetime
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """Apply fallback enrichment strategy"""
        # Try primary strategy first
        enriched_df, result = self._apply_exact_match_enrichment(source_df, ref_data, rule, start_time)
        
        # If enrichment rate is below threshold, try fallback
        if result.enrichment_rate < rule.confidence_threshold and rule.fallback_strategy:
            if rule.fallback_strategy == EnrichmentStrategy.DEFAULT_VALUE:
                enriched_df, fallback_result = self._apply_default_value_enrichment(enriched_df, rule, start_time)
                # Merge results
                result.records_enriched += fallback_result.records_enriched
                result.enrichment_rate = result.records_enriched / result.records_processed
                result.warnings.append("Fallback strategy applied")
        
        return enriched_df, result
    
    def _apply_default_value_enrichment(
        self, 
        source_df: pd.DataFrame, 
        rule: EnrichmentRule, 
        start_time: datetime
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """Apply default value enrichment"""
        
        enriched_df = source_df.copy()
        records_processed = len(enriched_df)
        records_enriched = 0
        
        # Apply default values
        for col, default_val in rule.default_values.items():
            if col not in enriched_df.columns:
                enriched_df[col] = default_val
                records_enriched = records_processed
            else:
                # Count how many null values were filled
                null_count = enriched_df[col].isnull().sum()
                enriched_df[col] = enriched_df[col].fillna(default_val)
                records_enriched += null_count
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        result = EnrichmentResult(
            rule_id=rule.rule_id,
            success=True,
            records_processed=records_processed,
            records_enriched=records_enriched,
            records_failed=records_processed - records_enriched,
            enrichment_rate=records_enriched / records_processed if records_processed > 0 else 0.0,
            quality_score=0.5,  # Lower quality score for default values
            processing_time_seconds=processing_time
        )
        
        return enriched_df, result
    
    def _calculate_quality_score(self, df: pd.DataFrame, rule: EnrichmentRule) -> float:
        """Calculate quality score for enriched data"""
        if not rule.quality_gates:
            return 1.0
        
        quality_score = 1.0
        
        for metric, threshold in rule.quality_gates.items():
            if metric == "completeness":
                # Calculate completeness for target columns
                target_cols = [col for col in rule.target_columns if col in df.columns]
                if target_cols:
                    completeness = df[target_cols].notnull().all(axis=1).mean()
                    if completeness < threshold:
                        quality_score *= 0.8
            elif metric == "accuracy":
                # This would implement accuracy checks
                pass
            elif metric == "consistency":
                # This would implement consistency checks
                pass
        
        return quality_score
    
    def _add_enrichment_metadata(self, df: pd.DataFrame, results: List[EnrichmentResult]) -> pd.DataFrame:
        """Add enrichment metadata to the dataframe"""
        enriched_df = df.copy()
        
        # Add enrichment summary
        total_enriched = sum(r.records_enriched for r in results)
        total_processed = sum(r.records_processed for r in results)
        avg_quality = sum(r.quality_score for r in results) / len(results) if results else 0.0
        
        enriched_df['_enrichment_timestamp'] = datetime.utcnow()
        enriched_df['_enrichment_version'] = "1.0"
        enriched_df['_enrichment_quality_score'] = avg_quality
        enriched_df['_enrichment_rate'] = total_enriched / total_processed if total_processed > 0 else 0.0
        
        return enriched_df
    
    def get_enrichment_summary(self) -> Dict[str, Any]:
        """Get summary of enrichment operations"""
        if not self.enrichment_history:
            return {"message": "No enrichment history available"}
        
        total_processed = sum(r.records_processed for r in self.enrichment_history)
        total_enriched = sum(r.records_enriched for r in self.enrichment_history)
        avg_quality = sum(r.quality_score for r in self.enrichment_history) / len(self.enrichment_history)
        avg_processing_time = sum(r.processing_time_seconds for r in self.enrichment_history) / len(self.enrichment_history)
        
        return {
            "total_operations": len(self.enrichment_history),
            "total_records_processed": total_processed,
            "total_records_enriched": total_enriched,
            "overall_enrichment_rate": total_enriched / total_processed if total_processed > 0 else 0.0,
            "average_quality_score": avg_quality,
            "average_processing_time_seconds": avg_processing_time,
            "successful_operations": len([r for r in self.enrichment_history if r.success]),
            "failed_operations": len([r for r in self.enrichment_history if not r.success])
        }


# Predefined enrichment rules for RVU data
def get_rvu_geography_enrichment_rules() -> List[EnrichmentRule]:
    """Get enrichment rules for RVU geography data"""
    return [
        EnrichmentRule(
            rule_id="rvu_geography_001",
            name="zip_to_locality",
            description="Map ZIP codes to locality information",
            source_columns=["zip5"],
            target_columns=["locality_code", "state_fips", "rural_flag"],
            join_keys=[("zip5", "zip5")],
            strategy=EnrichmentStrategy.EXACT_MATCH,
            reference_source="cms_zip_locality",
            confidence_threshold=0.95,
            fallback_strategy=EnrichmentStrategy.DEFAULT_VALUE,
            default_values={"locality_code": "99", "state_fips": "00", "rural_flag": "U"},
            quality_gates={"completeness": 0.95}
        ),
        EnrichmentRule(
            rule_id="rvu_geography_002",
            name="locality_to_gpci",
            description="Add GPCI indices for localities",
            source_columns=["locality_code", "state_fips"],
            target_columns=["gpci_work", "gpci_pe", "gpci_malp"],
            join_keys=[("locality_code", "locality_code"), ("state_fips", "state_fips")],
            strategy=EnrichmentStrategy.EXACT_MATCH,
            reference_source="cms_gpci",
            confidence_threshold=0.90,
            fallback_strategy=EnrichmentStrategy.DEFAULT_VALUE,
            default_values={"gpci_work": 1.0, "gpci_pe": 1.0, "gpci_malp": 1.0},
            quality_gates={"completeness": 0.90, "accuracy": 0.95}
        )
    ]


def get_rvu_code_enrichment_rules() -> List[EnrichmentRule]:
    """Get enrichment rules for RVU code data"""
    return [
        EnrichmentRule(
            rule_id="rvu_code_001",
            name="hcpcs_descriptions",
            description="Add HCPCS code descriptions",
            source_columns=["hcpcs"],
            target_columns=["description", "category"],
            join_keys=[("hcpcs", "hcpcs")],
            strategy=EnrichmentStrategy.EXACT_MATCH,
            reference_source="cms_hcpcs_codes",
            confidence_threshold=0.98,
            fallback_strategy=EnrichmentStrategy.DEFAULT_VALUE,
            default_values={"description": "Unknown", "category": "Unknown"},
            quality_gates={"completeness": 0.98}
        ),
        EnrichmentRule(
            rule_id="rvu_code_002",
            name="modifier_descriptions",
            description="Add modifier descriptions",
            source_columns=["modifier"],
            target_columns=["modifier_description"],
            join_keys=[("modifier", "modifier")],
            strategy=EnrichmentStrategy.EXACT_MATCH,
            reference_source="cms_modifiers",
            confidence_threshold=0.95,
            fallback_strategy=EnrichmentStrategy.DEFAULT_VALUE,
            default_values={"modifier_description": "Unknown"},
            quality_gates={"completeness": 0.95}
        )
    ]


# Global reference data manager and enricher instances
reference_data_manager = ReferenceDataManager()
dis_reference_enricher = DISReferenceDataEnricher(reference_data_manager)
