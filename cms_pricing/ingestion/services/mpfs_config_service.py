"""
MPFS Config Service
-------------------

Service for loading YAML-based conversion factor overrides for MPFS ingestion.
Supports per-release configuration files that override CLI flags.

CLI flags remain the primary/fallback mechanism until YAML service is production-ready.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import yaml
import structlog

logger = structlog.get_logger()

SUFFIX_TO_QUARTER = {"A": "Q1", "B": "Q2", "C": "Q3", "D": "Q4"}


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

        release_match = re.match(r"^(?P<prefix>[a-zA-Z]+)_(?P<year>\d{4})_(?P<suffix>[A-Z]{1,2})$", release_id)
        suffix = release_match.group("suffix") if release_match else None
        year = release_match.group("year") if release_match else None
        prefix = release_match.group("prefix") if release_match else None

        candidate_paths = [self.config_dir / f"{release_id}.yaml"]
        if release_match:
            candidate_paths.append(self.config_dir / f"{prefix}_{year}.yaml")
            candidate_paths.append(self.config_dir / f"{year}.yaml")

        config_path = None
        config_data: Optional[Dict[str, Any]] = None

        for path in candidate_paths:
            if not path.exists():
                continue
            try:
                with open(path, "r") as f:
                    config_data = yaml.safe_load(f)
                config_path = path
                break
            except yaml.YAMLError as e:
                error_msg = (
                    f"Malformed YAML in MPFS config file: {path} "
                    f"(line {getattr(e, 'problem_mark', {}).line if hasattr(e, 'problem_mark') else 'unknown'})"
                )
                logger.error(error_msg, error=str(e))
                raise ValueError(error_msg) from e
            except Exception as e:
                error_msg = f"Error reading MPFS config file: {path}"
                logger.error(error_msg, error=str(e))
                raise ValueError(error_msg) from e

        if config_data is None or config_path is None:
            # No config available for this release/year
            logger.debug(
                "MPFS config file not found, falling back to CLI flags",
                release_id=release_id,
                searched_paths=[str(p) for p in candidate_paths],
            )
            self._cache[release_id] = None
            return None

        if not isinstance(config_data, dict):
            raise ValueError(f"MPFS config file must contain a YAML dictionary: {config_path}")

        allowed_keys = {"manual_override_path", "expected_checksum"}

        def _extract_allowed(source: Optional[Dict[str, Any]]) -> Dict[str, Any]:
            if not isinstance(source, dict):
                return {}
            return {k: v for k, v in source.items() if k in allowed_keys and v is not None}

        base_override = _extract_allowed(config_data.get("default"))
        # Top-level values override defaults
        base_override.update(_extract_allowed(config_data))

        release_override: Dict[str, Any] = {}
        matched_release_keys: Optional[List[str]] = None
        releases_section = config_data.get("releases")
        if isinstance(releases_section, dict):
            release_override, matched_release_keys = self._resolve_release_specific_override(
                releases_section, release_id, prefix, year, suffix
            )

        combined_override = {}
        combined_override.update(base_override)
        combined_override.update(_extract_allowed(release_override))

        manual_override_path = combined_override.get("manual_override_path") if combined_override else None
        expected_checksum = combined_override.get("expected_checksum") if combined_override else None

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

        if manual_override_path is None and expected_checksum is None:
            self._cache[release_id] = None
            return None

        result_dict = {
            "manual_override_path": manual_override_path,
            "expected_checksum": expected_checksum,
        }

        self._cache[release_id] = result_dict

        logger.info(
            "Loaded MPFS config overrides",
            release_id=release_id,
            config_path=str(config_path),
            has_override=bool(manual_override_path),
            has_checksum=bool(expected_checksum),
            release_key=", ".join(matched_release_keys) if matched_release_keys else None,
        )

        return result_dict

    def _resolve_release_specific_override(
        self,
        releases_section: Dict[Any, Any],
        release_id: str,
        prefix: Optional[str],
        year: Optional[str],
        suffix: Optional[str],
    ) -> Tuple[Dict[str, Any], Optional[List[str]]]:
        """Select release-specific override entry from config."""
        normalized: Dict[str, Any] = {}
        original_key_lookup: Dict[str, Any] = {}
        for raw_key, value in releases_section.items():
            key_str = str(raw_key).lower()
            normalized[key_str] = value
            original_key_lookup[key_str] = raw_key

        candidate_keys: List[str] = []

        def _enqueue(value: Optional[str]) -> None:
            if not value:
                return
            value_str = str(value).lower()
            if value_str not in candidate_keys:
                candidate_keys.append(value_str)

        if prefix and year and suffix:
            _enqueue(f"{prefix}_{year}_{suffix}")
        _enqueue(release_id)
        if year and suffix:
            _enqueue(f"{year}_{suffix}")
            _enqueue(f"{year}{suffix}")
        if suffix:
            _enqueue(suffix)
            quarter_token = SUFFIX_TO_QUARTER.get(suffix.upper())
            if quarter_token:
                _enqueue(quarter_token)
                if year:
                    _enqueue(f"{year}_{quarter_token}")
                    _enqueue(f"{year}{quarter_token}")

        matched_original_keys: List[str] = []
        aggregated: Dict[str, Any] = {}

        for candidate in candidate_keys:
            if candidate not in normalized:
                continue
            entry = normalized[candidate]
            if not isinstance(entry, dict):
                raise ValueError(
                    f"Config override for key '{original_key_lookup[candidate]}' must be a YAML mapping"
                )
            aggregated.update(entry)
            matched_original_keys.append(str(original_key_lookup[candidate]))

        return aggregated, matched_original_keys or None

    def clear_cache(self) -> None:
        """Clear the in-memory cache (useful for testing)."""
        self._cache.clear()
