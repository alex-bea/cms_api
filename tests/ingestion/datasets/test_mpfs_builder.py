"""
Tests for MPFS Builder
"""

import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch

from cms_pricing.ingestion.datasets.mpfs_builder import normalize_conversion_factor


class TestNormalizeConversionFactor:
    """Test conversion factor normalization with WARN logging for extra columns."""

    def test_warn_logged_when_extra_numeric_columns_present(self):
        """Test that WARN is logged when extra numeric columns (e.g., anesthesia_cf) are present."""
        df = pd.DataFrame({
            "cf_type": ["physician", "anesthesia"],
            "cf_value": [32.3465, 20.3178],
            "anesthesia_cf": [20.3178, 20.3178],  # Extra numeric column
            "year": [2025, 2025],
            "effective_from": ["2025-01-01", "2025-01-01"],
            "effective_to": ["2025-12-31", "2025-12-31"],
        })
        
        with patch("cms_pricing.ingestion.datasets.mpfs_builder.logger") as mock_logger:
            result = normalize_conversion_factor(df, year=2025, release_id="test_release")
            
            # Verify WARN was logged
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            assert "Additional CF columns present but unused" in str(call_args)
            
            # Verify extra columns are in the log
            kwargs = call_args.kwargs
            assert "extra_columns" in kwargs
            assert "anesthesia_cf" in kwargs["extra_columns"]
            assert "message" in kwargs
            assert "MVP scope is physician-factor only" in kwargs["message"]

    def test_warn_logged_when_midyear_columns_present(self):
        """Test that WARN is logged when midyear adjustment columns are present."""
        df = pd.DataFrame({
            "cf_type": ["physician"],
            "cf_value": [32.3465],
            "midyear_cf": [32.7442],  # Extra numeric column
            "year": [2025],
            "effective_from": ["2025-01-01"],
            "effective_to": ["2025-12-31"],
        })
        
        with patch("cms_pricing.ingestion.datasets.mpfs_builder.logger") as mock_logger:
            result = normalize_conversion_factor(df, year=2025, release_id="test_release")
            
            # Verify WARN was logged
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            kwargs = call_args.kwargs
            assert "midyear_cf" in kwargs.get("extra_columns", [])

    def test_warn_not_logged_when_only_expected_columns(self):
        """Test that WARN is NOT logged when only expected columns are present."""
        df = pd.DataFrame({
            "cf_type": ["physician"],
            "cf_value": [32.3465],
            "year": [2025],
            "effective_start": ["2025-01-01"],
            "effective_end": ["2025-12-31"],
        })
        
        with patch("cms_pricing.ingestion.datasets.mpfs_builder.logger") as mock_logger:
            result = normalize_conversion_factor(df, year=2025, release_id="test_release")
            
            # Verify WARN was NOT logged
            mock_logger.warning.assert_not_called()

    def test_output_contains_only_physician_factor(self):
        """Test that output DataFrame only contains physician factor rows."""
        df = pd.DataFrame({
            "cf_type": ["physician", "anesthesia"],
            "cf_value": [32.3465, 20.3178],
            "year": [2025, 2025],
            "effective_start": ["2025-01-01", "2025-01-01"],
            "effective_end": ["2025-12-31", "2025-12-31"],
        })
        
        result = normalize_conversion_factor(df, year=2025, release_id="test_release")
        
        # Verify only physician factor in output
        assert len(result) == 1
        assert result.iloc[0]["cf_type"] == "physician"
        assert result.iloc[0]["cf_value"] == 32.3465

    def test_output_columns_are_correct(self):
        """Test that output DataFrame has correct columns."""
        df = pd.DataFrame({
            "cf_type": ["physician"],
            "cf_value": [32.3465],
            "year": [2025],
            "effective_start": ["2025-01-01"],
            "effective_end": ["2025-12-31"],
            "anesthesia_cf": [20.3178],  # Extra column (should be filtered out)
        })
        
        result = normalize_conversion_factor(df, year=2025, release_id="test_release")
        
        # Verify correct columns
        expected_columns = ["year", "cf_type", "cf_value", "effective_start", "effective_end", "release_id"]
        assert list(result.columns) == expected_columns
        assert "anesthesia_cf" not in result.columns

    def test_mpfs_cf_vintage_remains_physician_factor_only(self):
        """Test that mpfs_cf_vintage curated table only persists physician factor."""
        df = pd.DataFrame({
            "cf_type": ["physician", "anesthesia", "physician"],
            "cf_value": [32.3465, 20.3178, 32.7442],
            "year": [2025, 2025, 2025],
            "effective_start": ["2025-01-01", "2025-01-01", "2025-03-09"],
            "effective_end": ["2025-03-08", "2025-12-31", "2025-12-31"],
        })
        
        result = normalize_conversion_factor(df, year=2025, release_id="test_release")
        
        # Verify only physician factor rows
        assert len(result) == 2  # Two physician factor rows
        assert all(result["cf_type"] == "physician")
        assert 20.3178 not in result["cf_value"].values  # Anesthesia CF should be filtered out

