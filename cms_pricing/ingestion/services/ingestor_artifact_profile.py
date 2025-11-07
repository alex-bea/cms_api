"""Reusable service for loading ingestor artifact profiles.

Each dataset (e.g., OPPS, MPFS, RVU) can declare required/optional artifact
sets per release profile (baseline, quarterly, correction, etc.) via YAML
configuration. Ingutators can resolve the correct profile for a batch and
validate discovered file types uniformly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml
import structlog

logger = structlog.get_logger()

CONFIG_PATH_DEFAULT = Path(__file__).resolve().parent.parent / "config" / "ingestor_artifacts.yml"


@dataclass
class ArtifactProfileResult:
    """Validation result returned to ingestors."""

    passed: bool
    errors: List[str]
    warnings: List[str]
    required: List[str]
    optional: List[str]
    profile_name: str


@dataclass
class ArtifactProfile:
    """Resolved artifact profile for a dataset/release."""

    name: str
    required: List[str]
    optional: List[str]
    allow_missing_required: bool = False

    def validate(self, discovered_file_types: Sequence[str]) -> ArtifactProfileResult:
        found = list(discovered_file_types)
        missing_required = [ft for ft in self.required if ft not in found]
        missing_optional = [ft for ft in self.optional if ft not in found]

        errors: List[str] = []
        warnings: List[str] = []

        if missing_required:
            message = f"Missing required artifact(s): {', '.join(missing_required)}"
            if self.allow_missing_required:
                warnings.append(message + " [sandbox leniency]")
            else:
                errors.append(message)

        if missing_optional:
            warnings.append(f"Optional artifact(s) missing: {', '.join(missing_optional)}")

        return ArtifactProfileResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            required=self.required,
            optional=self.optional,
            profile_name=self.name,
        )


class IngestorArtifactProfileService:
    """Loads and resolves artifact profile configuration for ingestors."""

    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = Path(config_path or CONFIG_PATH_DEFAULT)
        self._config = self._load_config(self.config_path)

    def resolve(
        self,
        dataset: str,
        *,
        release_id: Optional[str] = None,
        profile_override: Optional[str] = None,
        sandbox_mode: bool = False,
    ) -> ArtifactProfile:
        dataset_cfg = self._config.get("datasets", {}).get(dataset.lower())
        if not dataset_cfg:
            logger.warning("Artifact profile: dataset missing from config, using empty defaults", dataset=dataset)
            return ArtifactProfile(name="default", required=[], optional=[], allow_missing_required=sandbox_mode)

        profiles = dataset_cfg.get("profiles") or {}

        profile_name = self._determine_profile_name(dataset_cfg, release_id, profile_override)
        profile_cfg = profiles.get(profile_name)
        if not profile_cfg:
            logger.warning(
                "Artifact profile: profile missing, falling back to default",
                dataset=dataset,
                profile=profile_name,
            )
            profile_name = dataset_cfg.get("default_profile", "default")
            profile_cfg = profiles.get(profile_name, {"required": [], "optional": []})

        allow_missing = False
        if sandbox_mode:
            sandbox_cfg = dataset_cfg.get("sandbox", {})
            allow_missing = bool(
                sandbox_cfg.get("allow_missing_required") or profile_cfg.get("allow_missing_required")
            )

        return ArtifactProfile(
            name=profile_name,
            required=list(profile_cfg.get("required", [])),
            optional=list(profile_cfg.get("optional", [])),
            allow_missing_required=allow_missing,
        )

    def _determine_profile_name(
        self,
        dataset_cfg: Dict[str, Any],
        release_id: Optional[str],
        profile_override: Optional[str],
    ) -> str:
        profiles = dataset_cfg.get("profiles", {})

        def valid(name: Optional[str]) -> Optional[str]:
            if name and name in profiles:
                return name
            return None

        if profile_override:
            override = valid(profile_override)
            if override:
                return override

        release_profiles = dataset_cfg.get("release_profiles") or {}
        if release_id:
            for pattern, profile in release_profiles.items():
                if valid(profile) and re.search(pattern, release_id):
                    return profile

        baseline_regex = dataset_cfg.get("baseline_regex")
        baseline_profile = valid(dataset_cfg.get("baseline_profile"))
        if release_id and baseline_regex and baseline_profile and re.search(baseline_regex, release_id):
            return baseline_profile

        default_profile = valid(dataset_cfg.get("default_profile"))
        if default_profile:
            return default_profile

        # Fallback to first defined profile or "default"
        return next(iter(profiles.keys()), "default")

    def _load_config(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            logger.warning("Artifact profile config missing; using minimal defaults", path=str(path))
            return {"datasets": {}}

        try:
            with open(path, "r") as handle:
                data = yaml.safe_load(handle) or {}
                if not isinstance(data, dict):
                    raise ValueError("Artifact config root must be a mapping")
                return data
        except Exception as exc:
            logger.error("Failed to load artifact profile config; using minimal defaults", path=str(path), error=str(exc))
            return {"datasets": {}}
