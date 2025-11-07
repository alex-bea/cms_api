from pathlib import Path

import pytest

from cms_pricing.ingestion.services.ingestor_artifact_profile import (
    IngestorArtifactProfileService,
    ArtifactProfile,
)


def test_service_resolves_quarterly_profile(tmp_path):
    config = tmp_path / "config.yml"
    config.write_text(
        """
datasets:
  opps:
    default_profile: quarterly
    profiles:
      quarterly:
        required: ["addendum_a"]
        optional: ["addendum_b"]
"""
    )
    service = IngestorArtifactProfileService(config_path=config)
    profile = service.resolve("opps")
    assert profile.name == "quarterly"
    assert profile.required == ["addendum_a"]
    assert profile.optional == ["addendum_b"]


def test_service_detects_baseline_by_regex(tmp_path):
    config = tmp_path / "config.yml"
    config.write_text(
        """
datasets:
  opps:
    default_profile: quarterly
    baseline_profile: baseline
    baseline_regex: "_r00$"
    profiles:
      baseline:
        required: ["baseline_file"]
        optional: []
      quarterly:
        required: ["quarterly_file"]
        optional: []
"""
    )
    service = IngestorArtifactProfileService(config_path=config)
    profile = service.resolve("opps", release_id="opps_2025q1_r00")
    assert profile.name == "baseline"
    assert profile.required == ["baseline_file"]


def test_validation_warnings_for_optional():
    profile = ArtifactProfile(name="test", required=[], optional=["opt"])
    result = profile.validate(["foo"])
    assert result.passed is True
    assert "opt" in result.warnings[0]


def test_validation_errors_when_required_missing():
    profile = ArtifactProfile(name="test", required=["req"], optional=[])
    result = profile.validate([])
    assert result.passed is False
    assert "req" in result.errors[0]
