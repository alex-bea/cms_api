"""
DIS-Compliant Data Enrichers
Following Data Ingestion Standard PRD v1.0

This module provides generic join utilities and data enrichment capabilities
for combining source data with reference datasets.
"""

import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import structlog

logger = structlog.get_logger()


@dataclass
class JoinSpec:
    """Specification for data joins"""
    left_key: str
    right_key: str
    join_type: str  # "left", "right", "inner", "outer"
    mapping_confidence: str = "high"  # "high", "medium", "low"
    tie_breaker: Optional[str] = None


@dataclass
class EnrichmentRule:
    """Rule for data enrichment"""
    name: str
    description: str
    join_specs: List[JoinSpec]
    enrichment_columns: List[str]
    default_values: Dict[str, Any] = None


class DataEnricher(ABC):
    """Base class for data enrichers"""
    
    def __init__(self, reference_data: Dict[str, pd.DataFrame]):
        self.reference_data = reference_data
    
    @abstractmethod
    def enrich(self, source_df: pd.DataFrame, enrichment_rules: List[EnrichmentRule]) -> pd.DataFrame:
        """Enrich source data with reference data"""
        pass
    
    def _apply_join(self, left_df: pd.DataFrame, right_df: pd.DataFrame, join_spec: JoinSpec) -> pd.DataFrame:
        """Apply a single join operation"""
        try:
            # Ensure join keys exist
            if join_spec.left_key not in left_df.columns:
                logger.warning(f"Left join key '{join_spec.left_key}' not found in source data")
                return left_df
            
            if join_spec.right_key not in right_df.columns:
                logger.warning(f"Right join key '{join_spec.right_key}' not found in reference data")
                return left_df
            
            # Perform the join
            if join_spec.join_type == "left":
                result = left_df.merge(
                    right_df, 
                    left_on=join_spec.left_key, 
                    right_on=join_spec.right_key, 
                    how="left"
                )
            elif join_spec.join_type == "right":
                result = left_df.merge(
                    right_df, 
                    left_on=join_spec.left_key, 
                    right_on=join_spec.right_key, 
                    how="right"
                )
            elif join_spec.join_type == "inner":
                result = left_df.merge(
                    right_df, 
                    left_on=join_spec.left_key, 
                    right_on=join_spec.right_key, 
                    how="inner"
                )
            elif join_spec.join_type == "outer":
                result = left_df.merge(
                    right_df, 
                    left_on=join_spec.left_key, 
                    right_on=join_spec.right_key, 
                    how="outer"
                )
            else:
                logger.error(f"Unsupported join type: {join_spec.join_type}")
                return left_df
            
            # Add mapping confidence metadata
            result[f"{join_spec.left_key}_mapping_confidence"] = join_spec.mapping_confidence
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to apply join {join_spec.left_key} -> {join_spec.right_key}: {e}")
            return left_df
    
    def _apply_tie_breaker(self, df: pd.DataFrame, tie_breaker: str) -> pd.DataFrame:
        """Apply tie-breaking logic for one-to-many mappings"""
        if not tie_breaker:
            return df
        
        # Sort by tie-breaker column and keep first occurrence
        df = df.sort_values(tie_breaker).drop_duplicates(
            subset=[col for col in df.columns if col != tie_breaker],
            keep='first'
        )
        
        return df


class GeographyEnricher(DataEnricher):
    """Enricher for geographic data"""
    
    def enrich(self, source_df: pd.DataFrame, enrichment_rules: List[EnrichmentRule]) -> pd.DataFrame:
        """Enrich geographic data with reference information"""
        enriched_df = source_df.copy()
        
        for rule in enrichment_rules:
            try:
                # Apply each join specification
                for join_spec in rule.join_specs:
                    # Find the appropriate reference table
                    ref_table = None
                    for ref_name, ref_df in self.reference_data.items():
                        if join_spec.right_key in ref_df.columns:
                            ref_table = ref_df
                            break
                    
                    if ref_table is None:
                        logger.warning(f"No reference table found for key '{join_spec.right_key}'")
                        continue
                    
                    # Apply the join
                    enriched_df = self._apply_join(enriched_df, ref_table, join_spec)
                    
                    # Apply tie-breaker if specified
                    if join_spec.tie_breaker:
                        enriched_df = self._apply_tie_breaker(enriched_df, join_spec.tie_breaker)
                
                # Add enrichment metadata
                enriched_df[f"{rule.name}_enriched_at"] = pd.Timestamp.now()
                enriched_df[f"{rule.name}_enrichment_version"] = "1.0"
                
            except Exception as e:
                logger.error(f"Failed to apply enrichment rule '{rule.name}': {e}")
                continue
        
        return enriched_df


class CodeEnricher(DataEnricher):
    """Enricher for code lookups and mappings"""
    
    def enrich(self, source_df: pd.DataFrame, enrichment_rules: List[EnrichmentRule]) -> pd.DataFrame:
        """Enrich data with code lookups"""
        enriched_df = source_df.copy()
        
        for rule in enrichment_rules:
            try:
                # Apply each join specification
                for join_spec in rule.join_specs:
                    # Find the appropriate reference table
                    ref_table = None
                    for ref_name, ref_df in self.reference_data.items():
                        if join_spec.right_key in ref_df.columns:
                            ref_table = ref_df
                            break
                    
                    if ref_table is None:
                        logger.warning(f"No reference table found for key '{join_spec.right_key}'")
                        continue
                    
                    # Apply the join
                    enriched_df = self._apply_join(enriched_df, ref_table, join_spec)
                    
                    # Apply tie-breaker if specified
                    if join_spec.tie_breaker:
                        enriched_df = self._apply_tie_breaker(enriched_df, join_spec.tie_breaker)
                
                # Add enrichment metadata
                enriched_df[f"{rule.name}_enriched_at"] = pd.Timestamp.now()
                enriched_df[f"{rule.name}_enrichment_version"] = "1.0"
                
            except Exception as e:
                logger.error(f"Failed to apply enrichment rule '{rule.name}': {e}")
                continue
        
        return enriched_df


class EnricherFactory:
    """Factory for creating appropriate enrichers"""
    
    @staticmethod
    def create_enricher(enricher_type: str, reference_data: Dict[str, pd.DataFrame]) -> DataEnricher:
        """Create appropriate enricher based on type"""
        
        if enricher_type == "geography":
            return GeographyEnricher(reference_data)
        elif enricher_type == "codes":
            return CodeEnricher(reference_data)
        else:
            raise ValueError(f"Unsupported enricher type: {enricher_type}")


# Predefined enrichment rules for common use cases

def get_zip_to_zcta_enrichment_rule() -> EnrichmentRule:
    """Get enrichment rule for ZIP to ZCTA mapping"""
    return EnrichmentRule(
        name="zip_to_zcta",
        description="Map ZIP codes to ZCTA codes",
        join_specs=[
            JoinSpec(
                left_key="zip5",
                right_key="zip5",
                join_type="left",
                mapping_confidence="high"
            )
        ],
        enrichment_columns=["zcta5", "relationship"],
        default_values={"zcta5": None, "relationship": "unknown"}
    )


def get_zcta_centroid_enrichment_rule() -> EnrichmentRule:
    """Get enrichment rule for ZCTA centroid coordinates"""
    return EnrichmentRule(
        name="zcta_centroids",
        description="Add ZCTA centroid coordinates",
        join_specs=[
            JoinSpec(
                left_key="zcta5",
                right_key="zcta5",
                join_type="left",
                mapping_confidence="high"
            )
        ],
        enrichment_columns=["lat", "lon", "population"],
        default_values={"lat": None, "lon": None, "population": 0}
    )


def get_state_crosswalk_enrichment_rule() -> EnrichmentRule:
    """Get enrichment rule for state crosswalk data"""
    return EnrichmentRule(
        name="state_crosswalk",
        description="Add state crosswalk information",
        join_specs=[
            JoinSpec(
                left_key="state",
                right_key="state_code",
                join_type="left",
                mapping_confidence="high"
            )
        ],
        enrichment_columns=["state_name", "state_fips", "region"],
        default_values={"state_name": None, "state_fips": None, "region": None}
    )


def get_locality_enrichment_rule() -> EnrichmentRule:
    """Get enrichment rule for locality information"""
    return EnrichmentRule(
        name="locality_info",
        description="Add locality information",
        join_specs=[
            JoinSpec(
                left_key="locality",
                right_key="locality_code",
                join_type="left",
                mapping_confidence="high"
            )
        ],
        enrichment_columns=["locality_name", "carrier", "rural_flag"],
        default_values={"locality_name": None, "carrier": None, "rural_flag": None}
    )
