"""
Reference Data Mode Controller

Manages dual-mode reference data access (inline dict vs curated infrastructure)
with fail-closed policy to prevent inline data from reaching production.

Reference: User feedback 2025-10-17 on Phase 0 implementation
"""

import os
from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import structlog

logger = structlog.get_logger()


class ReferenceMode(Enum):
    """Reference data access modes"""
    INLINE = "inline"      # Dev/test only - uses inline dicts
    CURATED = "curated"    # Production - uses /ref/ infrastructure


@dataclass
class ReferenceConfig:
    """Configuration for reference data access"""
    mode: ReferenceMode
    allow_publish: bool
    vintage_date: Optional[str]
    source_label: str
    
    def __post_init__(self):
        # Fail-closed: inline mode cannot publish curated outputs
        if self.mode == ReferenceMode.INLINE and self.allow_publish:
            raise ValueError(
                "INLINE mode cannot publish curated outputs. "
                "Set REF_MODE=curated for production publishing."
            )


def get_reference_mode() -> ReferenceMode:
    """
    Get reference mode from environment.
    
    Defaults to CURATED (production-safe).
    Set REF_MODE=inline for dev/CI testing only.
    """
    mode_str = os.getenv("REF_MODE", "curated").lower()
    
    try:
        mode = ReferenceMode(mode_str)
        logger.info(
            "reference_mode_configured",
            mode=mode.value,
            allow_publish=(mode == ReferenceMode.CURATED)
        )
        return mode
    except ValueError:
        logger.warning(
            "invalid_ref_mode",
            provided=mode_str,
            defaulting_to="curated"
        )
        return ReferenceMode.CURATED


def get_reference_config() -> ReferenceConfig:
    """
    Get reference configuration with fail-closed policy.
    
    Returns:
        ReferenceConfig with mode, publish permission, and metadata
    """
    mode = get_reference_mode()
    
    if mode == ReferenceMode.INLINE:
        return ReferenceConfig(
            mode=mode,
            allow_publish=False,  # Fail-closed
            vintage_date=datetime.utcnow().isoformat(),
            source_label="dev-inline"
        )
    else:  # CURATED
        return ReferenceConfig(
            mode=mode,
            allow_publish=True,
            vintage_date=None,  # Use actual ref vintage
            source_label="curated"
        )


def validate_publish_allowed(config: ReferenceConfig, stage: str = "publish"):
    """
    Validate that publishing is allowed for current reference mode.
    
    Args:
        config: Reference configuration
        stage: Pipeline stage attempting publish (for error message)
        
    Raises:
        RuntimeError: If publish attempted with inline mode
    """
    if not config.allow_publish:
        raise RuntimeError(
            f"Cannot {stage} with REF_MODE={config.mode.value}. "
            f"Inline reference data is for dev/test only. "
            f"Set REF_MODE=curated to publish to production."
        )


def get_reference_metadata() -> Dict[str, Any]:
    """
    Get reference metadata for run summaries and observability.
    
    Returns:
        Dict with ref_source, ref_mode, vintage_used, etc.
    """
    config = get_reference_config()
    
    return {
        "ref_source": config.source_label,
        "ref_mode": config.mode.value,
        "ref_vintage_used": config.vintage_date or "from_curated_manifest",
        "ref_allow_publish": config.allow_publish,
        "ref_disclaimer": (
            "DEV ONLY - DO NOT PUBLISH" 
            if config.mode == ReferenceMode.INLINE 
            else "production-ready"
        )
    }


# Module-level config (lazy init)
_config: Optional[ReferenceConfig] = None


def get_config() -> ReferenceConfig:
    """Get or initialize module-level config"""
    global _config
    if _config is None:
        _config = get_reference_config()
    return _config

