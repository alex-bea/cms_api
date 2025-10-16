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
import json
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, List, NamedTuple, Literal
import structlog

logger = structlog.get_logger()


class RouteDecision(NamedTuple):
    """
    Router decision output per STD-parser-contracts v1.1 §6.2.
    
    Production-grade routing result with schema-driven natural keys.
    
    Attributes:
        dataset: Dataset identifier (e.g., 'pprrvu', 'gpci')
        schema_id: Schema contract identifier (e.g., 'cms_pprrvu_v1.0')
        status: Routing status ('ok', 'quarantine', 'reject')
        natural_keys: Sort/primary key columns from schema contract (single source of truth)
    
    Examples:
        >>> decision = route_to_parser("PPRRVU2025.csv")
        >>> decision.dataset
        'pprrvu'
        >>> decision.natural_keys
        ['hcpcs', 'modifier', 'effective_from']
    """
    dataset: str
    schema_id: str
    status: Literal["ok", "quarantine", "reject"]
    natural_keys: List[str]


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
) -> RouteDecision:
    """
    Route file to parser with schema-driven natural keys.
    
    Production-grade routing per STD-parser-contracts v1.1 §6.2:
    - Pattern matching (filename regex)
    - Content sniffing (optional file_head for format detection)
    - Natural keys from schema contract (single source of truth)
    
    Args:
        filename: Source filename for pattern matching
        file_head: First ~8KB of file for magic byte/BOM/format detection (optional)
        
    Returns:
        RouteDecision NamedTuple with dataset, schema_id, status, natural_keys
        
    Raises:
        ValueError: If no parser routing found for filename
    
    Content sniffing (when file_head provided):
        - Detects fixed-width vs CSV (prevents .csv that's actually fixed-width)
        - Detects BOM markers (UTF-8-sig, UTF-16 LE/BE)
        - Detects magic bytes (ZIP, Excel, PDF)
        
    Examples:
        >>> decision = route_to_parser("PPRRVU2025_Oct.txt")
        >>> decision.dataset
        'pprrvu'
        >>> decision.natural_keys
        ['hcpcs', 'modifier', 'effective_from']
    """
    # Strip compression suffixes for pattern matching
    clean_filename = filename
    compression_suffixes = ['.gz', '.gzip', '.bz2', '.zip']
    for suffix in compression_suffixes:
        if clean_filename.lower().endswith(suffix):
            clean_filename = clean_filename[:-len(suffix)]
            logger.debug(
                "Stripped compression suffix",
                original=filename,
                cleaned=clean_filename,
                suffix=suffix
            )
            break
    
    # Try to import is_fixed_width_format for content sniffing
    try:
        from cms_pricing.ingestion.parsers._parser_kit import is_fixed_width_format
        content_sniffing_available = True
    except ImportError:
        content_sniffing_available = False
    
    for pattern, (dataset, schema_id, parser_status) in PARSER_ROUTING.items():
        if re.match(pattern, clean_filename, re.IGNORECASE):
            
            # Fetch natural_keys from schema contract (single source of truth)
            try:
                contracts_dir = Path(__file__).parent.parent / "contracts"
                schema_path = contracts_dir / f"{schema_id}.json"
                
                with open(schema_path, 'r') as f:
                    schema_contract = json.load(f)
                    natural_keys = schema_contract.get("natural_keys", [])
                    
                    # Handle nested schemas (OPPS has multiple tables)
                    if isinstance(natural_keys, dict):
                        # For nested schemas, use first table's keys as default
                        # Parsers will handle table-specific routing
                        natural_keys = list(natural_keys.values())[0] if natural_keys else []
                    
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                logger.error(
                    "Failed to load natural_keys from schema contract",
                    schema_id=schema_id,
                    error=str(e)
                )
                # Quarantine if schema contract missing or invalid
                return RouteDecision(
                    dataset=dataset,
                    schema_id=schema_id,
                    status="quarantine",
                    natural_keys=[]
                )
            
            # Content sniffing (if file_head provided and available)
            if file_head and content_sniffing_available:
                try:
                    is_fixed_width = is_fixed_width_format(file_head, filename)
                    
                    if is_fixed_width:
                        logger.info(
                            "Content sniffing: Fixed-width format detected",
                            filename=filename,
                            extension=filename.split('.')[-1] if '.' in filename else 'none',
                            dataset=dataset
                        )
                        # Format hint logged for observability
                        # Future Phase 2: Could override parser choice based on format
                except Exception as e:
                    logger.warning(
                        "Content sniffing failed, using filename only",
                        filename=filename,
                        error=str(e)
                    )
            
            logger.debug(
                "Routed file to parser",
                filename=filename,
                dataset=dataset,
                schema_id=schema_id,
                status="ok",
                natural_keys=natural_keys,
                content_sniffing_used=file_head is not None
            )
            
            return RouteDecision(
                dataset=dataset,
                schema_id=schema_id,
                status="ok",
                natural_keys=natural_keys
            )
    
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
            decision = route_to_parser(filename)
            routed.append({
                "filename": filename,
                "dataset": decision.dataset,
                "schema_id": decision.schema_id,
                "status": decision.status,
                "natural_keys": decision.natural_keys
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

