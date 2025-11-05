"""
RVU Dataset Specifications.

This module defines DatasetSpec instances for all RVU-related datasets:
- PPRRVU: Physician Fee Schedule RVU Items
- GPCI: Geographic Practice Cost Index
- OPPSCap: OPPS-based Payment Caps
- AnesCF: Anesthesia Conversion Factors
- LocalityCounty: Locality to County mapping

Per DIS standards and PRD alignment:
- STD-parser-contracts-prd-v2.0.md: Parser contracts and schema versioning
- STD-data-architecture-prd-v1.0.md: Natural keys and idempotency
"""

from typing import Dict

from .spec import DatasetSpec, EnrichmentRule
from .rvu_loaders import (
    load_pprrvu_data,
    load_gpci_data,
    load_oppscap_data,
    load_anes_data,
    load_locality_data,
)
from ..contracts.ingestor_spec import ValidationRule, ValidationSeverity
from ..parsers.pprrvu_parser import parse_pprrvu, SCHEMA_ID as PPRRVU_SCHEMA_ID, NATURAL_KEYS as PPRRVU_NK
from ..parsers.gpci_parser import parse_gpci, SCHEMA_ID as GPCI_SCHEMA_ID, NATURAL_KEYS as GPCI_NK
from ..parsers.oppscap_parser import parse_oppscap, SCHEMA_ID as OPPSCAP_SCHEMA_ID, NATURAL_KEYS as OPPSCAP_NK
from ..parsers.anes_parser import parse_anes, SCHEMA_ID as ANES_SCHEMA_ID, NATURAL_KEYS as ANES_NK
from ..parsers.locality_parser import parse_locality_raw, SCHEMA_ID as LOCALITY_SCHEMA_ID, NATURAL_KEYS as LOCALITY_NK
from ..enrichers.dis_reference_data_integration import (
    get_rvu_geography_enrichment_rules,
    get_rvu_code_enrichment_rules
)


def _create_pprrvu_validation_rules() -> list[ValidationRule]:
    """Create validation rules for PPRRVU dataset"""
    import pandas as pd
    
    def validate_hcpcs_format(df: pd.DataFrame) -> bool:
        """Validate HCPCS format"""
        if 'hcpcs' not in df.columns:
            return False
        return df['hcpcs'].str.match(r'^[A-Z0-9]{5}$').all()
    
    def validate_status_codes(df: pd.DataFrame) -> bool:
        """Validate status codes"""
        if 'status_code' not in df.columns:
            return False
        valid_statuses = {'A', 'R', 'T', 'I', 'N'}
        return df['status_code'].isin(valid_statuses).all()
    
    def validate_rvu_ranges(df: pd.DataFrame) -> bool:
        """Validate RVU ranges"""
        rvu_columns = ['rvu_work', 'rvu_pe_nonfac', 'rvu_pe_fac', 'rvu_malp']
        for col in rvu_columns:
            if col in df.columns:
                if df[col].min() < 0 or df[col].max() > 100:
                    return False
        return True
    
    return [
        ValidationRule("hcpcs_format", "HCPCS codes must be 5 characters", ValidationSeverity.CRITICAL, validate_hcpcs_format),
        ValidationRule("status_codes", "Status codes must be valid", ValidationSeverity.CRITICAL, validate_status_codes),
        ValidationRule("rvu_ranges", "RVU values must be within valid ranges", ValidationSeverity.CRITICAL, validate_rvu_ranges),
    ]


def _create_gpci_validation_rules() -> list[ValidationRule]:
    """Create validation rules for GPCI dataset"""
    import pandas as pd
    
    def validate_gpci_ranges(df: pd.DataFrame) -> bool:
        """Validate GPCI ranges"""
        gpci_columns = ['gpci_work', 'gpci_pe', 'gpci_malp']
        for col in gpci_columns:
            if col in df.columns:
                if df[col].min() < 0.3 or df[col].max() > 2.0:
                    return False
        return True
    
    return [
        ValidationRule("gpci_ranges", "GPCI values must be between 0.3 and 2.0", ValidationSeverity.CRITICAL, validate_gpci_ranges),
    ]


def _create_locality_validation_rules() -> list[ValidationRule]:
    """Create validation rules for LocalityCounty dataset"""
    import pandas as pd
    
    def validate_locality_codes(df: pd.DataFrame) -> bool:
        """Validate locality codes"""
        if 'locality_code' not in df.columns:
            return False
        return df['locality_code'].str.match(r'^\d{2}$').all()
    
    return [
        ValidationRule("locality_codes", "Locality codes must be 2 digits", ValidationSeverity.CRITICAL, validate_locality_codes),
    ]


def _create_enrichment_rules() -> list[EnrichmentRule]:
    """Create enrichment rules for RVU datasets"""
    geography_rules = get_rvu_geography_enrichment_rules()
    code_rules = get_rvu_code_enrichment_rules()
    
    # Convert to EnrichmentRule format (simplified - actual rules are more complex)
    # This is a placeholder - actual implementation would map from DIS enrichment rules
    return [
        EnrichmentRule(
            name="geography_enrichment",
            description="Join with geography reference data",
            source_column="locality_id",
            target_table="cms_zip_locality",
            join_keys=["locality_id"],
            confidence_threshold=0.8
        ),
        EnrichmentRule(
            name="code_enrichment",
            description="Join with HCPCS code reference data",
            source_column="hcpcs",
            target_table="cms_hcpcs_codes",
            join_keys=["hcpcs"],
            confidence_threshold=0.9
        ),
    ]


# RVU Dataset Specifications
RVU_DATASETS: Dict[str, DatasetSpec] = {
    "pprrvu": DatasetSpec(
        dataset_id="pprrvu",
        parser=parse_pprrvu,
        schema_id=PPRRVU_SCHEMA_ID,  # "cms_pprrvu_v1.0"
        natural_keys=PPRRVU_NK,  # ["hcpcs", "modifier"]
        loader=load_pprrvu_data,
        validation_rules=_create_pprrvu_validation_rules(),
        enrichment_rules=_create_enrichment_rules(),
        filename_patterns=[
            r".*pprrvu.*\.(txt|csv|xlsx|xls)$",
            r"^rvu\d+[a-z]\.(txt|csv|xlsx|xls)$"
        ]
    ),
    "gpci": DatasetSpec(
        dataset_id="gpci",
        parser=parse_gpci,
        schema_id=GPCI_SCHEMA_ID,  # "cms_gpci_v1.3"
        natural_keys=GPCI_NK,  # ["mac", "locality_id", "effective_start"]
        loader=load_gpci_data,
        validation_rules=_create_gpci_validation_rules(),
        enrichment_rules=_create_enrichment_rules(),
        filename_patterns=[
            r"(?i).*gpci.*\.(txt|csv|xlsx|xls)$"
        ]
    ),
    "oppscap": DatasetSpec(
        dataset_id="oppscap",
        parser=parse_oppscap,
        schema_id=OPPSCAP_SCHEMA_ID,  # "cms_oppscap_v1.1"
        natural_keys=OPPSCAP_NK,
        loader=load_oppscap_data,
        validation_rules=[],  # Add if needed
        enrichment_rules=_create_enrichment_rules(),
        filename_patterns=[
            r".*oppscap.*\.(txt|csv|xlsx|xls)$",
            r".*opps.*cap.*\.(txt|csv|xlsx|xls)$"
        ]
    ),
    "anescf": DatasetSpec(
        dataset_id="anescf",
        parser=parse_anes,
        schema_id=ANES_SCHEMA_ID,  # "cms_anescf_v1.1"
        natural_keys=ANES_NK,
        loader=load_anes_data,
        validation_rules=[],  # Add if needed
        enrichment_rules=_create_enrichment_rules(),
        filename_patterns=[
            r".*anescf.*\.(txt|csv|xlsx|xls)$",
            r".*anes.*\.(txt|csv|xlsx|xls)$"
        ]
    ),
    "localitycounty": DatasetSpec(
        dataset_id="localitycounty",
        parser=parse_locality_raw,
        schema_id=LOCALITY_SCHEMA_ID,  # "cms_localitycounty_v1.0"
        natural_keys=LOCALITY_NK,
        loader=load_locality_data,
        validation_rules=_create_locality_validation_rules(),
        enrichment_rules=[],
        filename_patterns=[
            r".*locco.*\.(txt|csv|xlsx|xls)$",
            r".*locality.*\.(txt|csv|xlsx|xls)$"
        ]
    ),
}


def get_rvu_dataset_spec(dataset_id: str) -> DatasetSpec:
    """
    Get RVU dataset spec by ID.
    
    Args:
        dataset_id: Dataset identifier (pprrvu, gpci, oppscap, anescf, localitycounty)
        
    Returns:
        DatasetSpec instance
        
    Raises:
        KeyError: If dataset_id not found
    """
    if dataset_id not in RVU_DATASETS:
        raise KeyError(f"Unknown RVU dataset: {dataset_id}. Available: {list(RVU_DATASETS.keys())}")
    return RVU_DATASETS[dataset_id]


def route_file_to_rvu_spec(filename: str, file_head: bytes = None) -> DatasetSpec:
    """
    Route a file to the appropriate RVU dataset spec using route_file() method.
    
    This demonstrates the DatasetSpec.route_file() pattern for multi-dataset routing.
    
    Args:
        filename: Name of the file to route
        file_head: Optional first bytes for content sniffing
        
    Returns:
        DatasetSpec that matches the file, or None if no match
    """
    for dataset_id, spec in RVU_DATASETS.items():
        if spec.route_file(filename, file_head):
            return spec
    return None
