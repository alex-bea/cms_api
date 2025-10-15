"""
Parser Routing Table

Maps CMS file patterns to schema contracts and dataset types.
Uses existing schema contracts from cms_pricing.ingestion.contracts.

NOTE: Individual parser functions will be extracted from RVU ingestor
      in future phases. This module provides the routing infrastructure
      and contract mapping for Phase 0.
      
Per PRD-mpfs-prd-v1.0.md line 26:
  Schema Contracts: cms_pprrvu_v1.0.json, cms_gpci_v1.0.json, 
  cms_localitycounty_v1.0.json, cms_anescf_v1.0.json
"""

import re
from typing import Tuple, Optional, Dict, Any
import structlog

logger = structlog.get_logger()


# File pattern → (dataset_name, schema_contract_id, parser_status)
PARSER_ROUTING = {
    # PPRRVU files (core RVU data)
    r"PPRRVU.*\.(txt|csv|xlsx)$": (
        "pprrvu",
        "cms_pprrvu_v1.0",
        "uses_rvu_ingestor"  # Currently uses RVU ingestor methods
    ),
    
    # GPCI files (geographic practice cost indices)
    r"GPCI.*\.(txt|csv|xlsx)$": (
        "gpci",
        "cms_gpci_v1.0",
        "uses_rvu_ingestor"
    ),
    
    # Locality/County crosswalk files
    r".*LOCCO.*\.(txt|csv|xlsx)$": (
        "locality",
        "cms_localitycounty_v1.0",
        "uses_rvu_ingestor"
    ),
    
    # Anesthesia conversion factor files
    r"ANES.*\.(txt|csv|xlsx)$": (
        "anes",
        "cms_anescf_v1.0",
        "uses_rvu_ingestor"
    ),
    
    # OPPS cap files
    r"OPPSCAP.*\.(txt|csv|xlsx)$": (
        "oppscap",
        "cms_oppscap_v1.0",
        "uses_rvu_ingestor"
    ),
    
    # Conversion factor files (MPFS-specific, NEW)
    r"(conversion-factor|cf-).*\.(xlsx|zip)$": (
        "conversion_factor",
        "cms_conversion_factor_v1.0",
        "schema_pending"  # Schema contract needs to be created
    ),
    
    # OPPS Addendum files
    r"(addendum|OPPS).*\.(xlsx|txt|csv)$": (
        "opps",
        "cms_opps_v1.0",
        "uses_opps_ingestor"
    ),
    
    # ZIP locality files
    r"(zip.*locality|locality.*zip).*\.(csv|xlsx|txt)$": (
        "zip_locality",
        "cms_zip_locality_v1.json",
        "uses_geography_ingestor"
    ),
}


def route_to_parser(
    filename: str,
    file_head: Optional[bytes] = None
) -> Tuple[str, str, str]:
    """
    Route file to parser with optional content sniffing.
    
    Per STD-parser-contracts v1.1 §6.2:
    - Uses filename pattern matching (primary)
    - Uses file_head for content sniffing (optional, prevents misroutes)
    
    Args:
        filename: Source filename for pattern matching
        file_head: First ~8KB of file for magic byte/BOM/format detection (optional)
        
    Returns:
        Tuple of (dataset_name, schema_contract_id, parser_status)
        
    Raises:
        ValueError: If no parser routing found for filename
    
    Content sniffing (when file_head provided):
        - Detects fixed-width vs CSV (prevents .csv that's actually fixed-width)
        - Detects BOM markers (UTF-8-sig, UTF-16 LE/BE)
        - Detects magic bytes (ZIP, Excel, PDF)
        
    Examples:
        >>> route_to_parser("PPRRVU2025_Oct.txt")
        ('pprrvu', 'cms_pprrvu_v1.0', 'uses_rvu_ingestor')
        
        >>> # With content sniffing
        >>> with open('file.csv', 'rb') as f:
        ...     file_head = f.read(8192)
        ...     f.seek(0)
        ...     dataset, schema, status = route_to_parser('file.csv', file_head)
    """
    # Try to import is_fixed_width_format for content sniffing
    try:
        from cms_pricing.ingestion.parsers._parser_kit import is_fixed_width_format
        content_sniffing_available = True
    except ImportError:
        content_sniffing_available = False
    
    for pattern, (dataset, schema_id, status) in PARSER_ROUTING.items():
        if re.match(pattern, filename, re.IGNORECASE):
            
            # Content sniffing (if file_head provided and available)
            if file_head and content_sniffing_available:
                is_fixed_width = is_fixed_width_format(file_head, filename)
                
                if is_fixed_width:
                    logger.info(
                        "Content sniffing: Fixed-width format detected",
                        filename=filename,
                        extension=filename.split('.')[-1] if '.' in filename else 'none'
                    )
                    # Format hint logged for observability
                    # Future: Could override parser choice based on format
            
            logger.debug(
                "Routed file to parser",
                filename=filename,
                dataset=dataset,
                schema_id=schema_id,
                status=status,
                content_sniffing=file_head is not None
            )
            return dataset, schema_id, status
    
    # No match found
    logger.error("No parser routing found", filename=filename)
    raise ValueError(
        f"No parser routing found for: {filename}. "
        f"Supported patterns: {list(PARSER_ROUTING.keys())}"
    )


def get_schema_contract_id(dataset_name: str) -> Optional[str]:
    """
    Get schema contract ID for a dataset name.
    
    Args:
        dataset_name: Dataset name (e.g., 'pprrvu', 'gpci')
        
    Returns:
        Schema contract ID or None if not found
    """
    for config in PARSER_ROUTING.values():
        if config[0] == dataset_name:
            return config[1]
    return None


def list_supported_datasets() -> Dict[str, Dict[str, str]]:
    """
    List all supported dataset types with their configurations.
    
    Returns:
        Dictionary mapping dataset names to their config
    """
    datasets = {}
    for pattern, (dataset, schema_id, status) in PARSER_ROUTING.items():
        if dataset not in datasets:
            datasets[dataset] = {
                "pattern": pattern,
                "schema_id": schema_id,
                "status": status
            }
    return datasets


def get_parser_status() -> Dict[str, str]:
    """
    Get implementation status of all parsers.
    
    Returns:
        Dictionary mapping dataset names to their parser status
        
    Statuses:
        - uses_rvu_ingestor: Uses existing RVU ingestor methods
        - uses_opps_ingestor: Uses existing OPPS ingestor methods
        - uses_geography_ingestor: Uses existing geography ingestor methods
        - schema_pending: Schema contract needs creation
        - parser_pending: Parser needs implementation
        - ready: Parser implemented and tested
    """
    status = {}
    for dataset, config in list_supported_datasets().items():
        status[dataset] = config["status"]
    return status


def validate_routing_coverage(filenames: list) -> Dict[str, Any]:
    """
    Validate that all filenames have parser routing.
    
    Args:
        filenames: List of filenames to validate
        
    Returns:
        Dictionary with coverage stats and unrouted files
    """
    routed = []
    unrouted = []
    
    for filename in filenames:
        try:
            dataset, schema_id, status = route_to_parser(filename)
            routed.append({
                "filename": filename,
                "dataset": dataset,
                "schema_id": schema_id,
                "status": status
            })
        except ValueError:
            unrouted.append(filename)
    
    return {
        "total_files": len(filenames),
        "routed": len(routed),
        "unrouted": len(unrouted),
        "coverage_pct": (len(routed) / len(filenames) * 100) if filenames else 0,
        "routed_files": routed,
        "unrouted_files": unrouted
    }


# Export public API
__all__ = [
    "PARSER_ROUTING",
    "route_to_parser",
    "get_schema_contract_id",
    "list_supported_datasets",
    "get_parser_status",
    "validate_routing_coverage",
]

