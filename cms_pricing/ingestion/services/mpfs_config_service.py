"""
MPFS Config Service
-------------------

Service for loading YAML-based conversion factor overrides for MPFS ingestion.
Supports per-release configuration files that override CLI flags.

CLI flags remain the primary/fallback mechanism until YAML service is production-ready.
"""

from __future__ import annotations

import yaml
from pathlib import Path
from typing import Optional, Dict, Any

import structlog

logger = structlog.get_logger()


class MPFSConfigService:
    """
    Service for loading MPFS configuration from YAML files.
    
    Configuration is cached in-memory for the process lifetime. Restart required
    for config updates.
    
    Configuration files are located at: cf_overrides/{release_id}.yaml
    """

    def __init__(self, config_dir: str = "./cf_overrides"):
        """
        Initialize config service.
        
        Args:
            config_dir: Directory containing YAML config files (default: ./cf_overrides)
        """
        self.config_dir = Path(config_dir)
        self._cache: Dict[str, Optional[Dict[str, str]]] = {}

    def get_cf_overrides(self, release_id: str) -> Optional[Dict[str, str]]:
        """
        Get conversion factor overrides for a release.
        
        Args:
            release_id: Release identifier (e.g., "mpfs_2025_D")
            
        Returns:
            Dict with keys 'manual_override_path' and 'expected_checksum' if config exists,
            None if config file missing (triggers CLI fallback)
            
        Raises:
            ValueError: If YAML file exists but is malformed
            FileNotFoundError: If override path in config doesn't exist (validation error)
        """
        # Check cache first
        if release_id in self._cache:
            return self._cache[release_id]

        config_path = self.config_dir / f"{release_id}.yaml"
        
        if not config_path.exists():
            logger.debug("MPFS config file not found, falling back to CLI flags", 
                        release_id=release_id, config_path=str(config_path))
            self._cache[release_id] = None
            return None

        try:
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            error_msg = (
                f"Malformed YAML in MPFS config file: {config_path} "
                f"(line {getattr(e, 'problem_mark', {}).line if hasattr(e, 'problem_mark') else 'unknown'})"
            )
            logger.error(error_msg, error=str(e))
            raise ValueError(error_msg) from e
        except Exception as e:
            error_msg = f"Error reading MPFS config file: {config_path}"
            logger.error(error_msg, error=str(e))
            raise ValueError(error_msg) from e

        if not isinstance(config_data, dict):
            raise ValueError(
                f"MPFS config file must contain a YAML dictionary: {config_path}"
            )

        # Extract overrides
        manual_override_path = config_data.get("manual_override_path")
        expected_checksum = config_data.get("expected_checksum")

        # Validate override path if provided
        if manual_override_path:
            override_path = Path(manual_override_path)
            if not override_path.exists():
                raise FileNotFoundError(
                    f"Conversion factor override path does not exist: {manual_override_path} "
                    f"(configured in {config_path})"
                )
            if not override_path.is_file():
                raise ValueError(
                    f"Conversion factor override path is not a file: {manual_override_path} "
                    f"(configured in {config_path})"
                )

        result = {
            "manual_override_path": manual_override_path,
            "expected_checksum": expected_checksum,
        }

        # Cache result (including None for missing files to avoid repeated file checks)
        self._cache[release_id] = result
        
        logger.info("Loaded MPFS config overrides", 
                   release_id=release_id, 
                   has_override=bool(manual_override_path),
                   has_checksum=bool(expected_checksum))
        
        return result

    def clear_cache(self) -> None:
        """Clear the in-memory cache (useful for testing)."""
        self._cache.clear()
