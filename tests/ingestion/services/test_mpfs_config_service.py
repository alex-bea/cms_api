"""
Tests for MPFS Config Service
"""

import pytest
import tempfile
import yaml
from pathlib import Path

from cms_pricing.ingestion.services.mpfs_config_service import MPFSConfigService


class TestMPFSConfigService:
    """Test MPFS config service YAML loading and caching."""

    def test_missing_file_returns_none(self):
        """Test that missing config file returns None (triggers CLI fallback)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = MPFSConfigService(config_dir=tmpdir)
            result = service.get_cf_overrides("nonexistent_release")
            
            assert result is None

    def test_malformed_yaml_raises_error(self):
        """Test that malformed YAML raises ValueError with clear message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            config_file.write_text("invalid: yaml: [unclosed")
            
            service = MPFSConfigService(config_dir=str(config_dir))
            
            with pytest.raises(ValueError) as exc_info:
                service.get_cf_overrides("test_release")
            
            assert "Malformed YAML" in str(exc_info.value)
            assert "test_release.yaml" in str(exc_info.value)

    def test_valid_yaml_returns_correct_dict(self):
        """Test that valid YAML returns correct override dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            
            # Create a dummy file for override path validation
            override_file = Path(tmpdir) / "cf_2025.xlsx"
            override_file.write_text("dummy")
            
            config_data = {
                "manual_override_path": str(override_file),
                "expected_checksum": "abc123def456"
            }
            config_file.write_text(yaml.dump(config_data))
            
            service = MPFSConfigService(config_dir=str(config_dir))
            result = service.get_cf_overrides("test_release")
            
            assert result is not None
            assert result["manual_override_path"] == str(override_file)
            assert result["expected_checksum"] == "abc123def456"

    def test_valid_yaml_without_override_path(self):
        """Test that YAML with only checksum works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            
            config_data = {
                "expected_checksum": "abc123def456"
            }
            config_file.write_text(yaml.dump(config_data))
            
            service = MPFSConfigService(config_dir=str(config_dir))
            result = service.get_cf_overrides("test_release")
            
            assert result is not None
            assert result["manual_override_path"] is None
            assert result["expected_checksum"] == "abc123def456"

    def test_invalid_override_path_raises_error(self):
        """Test that invalid override path raises FileNotFoundError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            
            config_data = {
                "manual_override_path": "/nonexistent/path/to/cf.xlsx",
                "expected_checksum": "abc123"
            }
            config_file.write_text(yaml.dump(config_data))
            
            service = MPFSConfigService(config_dir=str(config_dir))
            
            with pytest.raises(FileNotFoundError) as exc_info:
                service.get_cf_overrides("test_release")
            
            assert "does not exist" in str(exc_info.value)
            assert "/nonexistent/path/to/cf.xlsx" in str(exc_info.value)

    def test_override_path_not_file_raises_error(self):
        """Test that override path pointing to directory raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            
            config_data = {
                "manual_override_path": tmpdir,  # Directory, not file
                "expected_checksum": "abc123"
            }
            config_file.write_text(yaml.dump(config_data))
            
            service = MPFSConfigService(config_dir=str(config_dir))
            
            with pytest.raises(ValueError) as exc_info:
                service.get_cf_overrides("test_release")
            
            assert "is not a file" in str(exc_info.value)

    def test_per_release_lookup(self):
        """Test that different releases resolve to different config files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            
            # Create override files
            override1 = Path(tmpdir) / "cf1.xlsx"
            override1.write_text("dummy1")
            override2 = Path(tmpdir) / "cf2.xlsx"
            override2.write_text("dummy2")
            
            # Create config files for different releases
            config1_file = config_dir / "release_1.yaml"
            config1_file.write_text(yaml.dump({
                "manual_override_path": str(override1),
                "expected_checksum": "checksum1"
            }))
            
            config2_file = config_dir / "release_2.yaml"
            config2_file.write_text(yaml.dump({
                "manual_override_path": str(override2),
                "expected_checksum": "checksum2"
            }))
            
            service = MPFSConfigService(config_dir=str(config_dir))
            
            result1 = service.get_cf_overrides("release_1")
            result2 = service.get_cf_overrides("release_2")
            
            assert result1 is not None
            assert result2 is not None
            assert result1["manual_override_path"] == str(override1)
            assert result2["manual_override_path"] == str(override2)
            assert result1["expected_checksum"] == "checksum1"
            assert result2["expected_checksum"] == "checksum2"

    def test_cache_behavior(self):
        """Test that config is cached for process lifetime."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            
            override_file = Path(tmpdir) / "cf_2025.xlsx"
            override_file.write_text("dummy")
            
            config_data = {
                "manual_override_path": str(override_file),
                "expected_checksum": "abc123"
            }
            config_file.write_text(yaml.dump(config_data))
            
            service = MPFSConfigService(config_dir=str(config_dir))
            
            # First call - should load from file
            result1 = service.get_cf_overrides("test_release")
            assert result1 is not None
            
            # Modify file (should not affect cached result)
            config_file.write_text(yaml.dump({
                "manual_override_path": str(override_file),
                "expected_checksum": "modified_checksum"
            }))
            
            # Second call - should return cached result
            result2 = service.get_cf_overrides("test_release")
            assert result2 is not None
            assert result2["expected_checksum"] == "abc123"  # Original cached value
            
            # Clear cache and reload
            service.clear_cache()
            result3 = service.get_cf_overrides("test_release")
            assert result3 is not None
            assert result3["expected_checksum"] == "modified_checksum"  # New value

    def test_non_dict_yaml_raises_error(self):
        """Test that YAML with non-dict root raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            config_file = config_dir / "test_release.yaml"
            config_file.write_text("- item1\n- item2")  # List, not dict
            
            service = MPFSConfigService(config_dir=str(config_dir))
            
            with pytest.raises(ValueError) as exc_info:
                service.get_cf_overrides("test_release")
            
            assert "YAML dictionary" in str(exc_info.value)

