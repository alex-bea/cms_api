"""
Service configuration for shared ingestion services.

Defines the configuration dataclass used by ServiceFactory to initialize services
with consistent settings across all DIS ingestors.
"""

from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class ServiceConfig:
    """
    Configuration for shared ingestion services.
    
    This configuration is used by ServiceFactory to initialize services with
    consistent settings. All services are lazy-loaded by default unless explicitly
    configured otherwise.
    
    Args:
        output_dir: Base output directory for ingestion artifacts
        dataset_name: Name of the dataset (e.g., "cms_rvu", "cms_mpfs")
        enable_observability: Whether to enable observability collection (default: True)
        enable_quarantine: Whether to enable quarantine management (default: True)
        enable_reference_data: Whether to enable reference data management (default: True)
        enable_validation: Whether to enable validation engine (default: True)
        enable_schema_registry: Whether to enable schema registry (default: True)
        lazy_init: Whether services should be lazy-loaded (default: True)
        db_session: Optional database session for services that need DB access
    """
    output_dir: str
    dataset_name: str
    enable_observability: bool = True
    enable_quarantine: bool = True
    enable_reference_data: bool = True
    enable_validation: bool = True
    enable_schema_registry: bool = True
    lazy_init: bool = True
    db_session: Optional[Any] = None

