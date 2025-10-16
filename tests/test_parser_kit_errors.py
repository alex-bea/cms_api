"""
Parser Kit Error Types Tests

Tests Phase 1 custom exceptions:
- ParseError (base)
- DuplicateKeyError
- CategoryValidationError  
- LayoutMismatchError
- SchemaRegressionError

Per Phase 1 enhancement from user feedback.
"""

import pytest
import pandas as pd
from cms_pricing.ingestion.parsers._parser_kit import (
    ParseError,
    DuplicateKeyError,
    CategoryValidationError,
    LayoutMismatchError,
    SchemaRegressionError,
    ValidationSeverity,
    check_natural_key_uniqueness
)


def test_duplicate_key_error_structure():
    """DuplicateKeyError stores duplicate records for debugging."""
    dupes = [
        {"hcpcs": "99213", "modifier": ""},
        {"hcpcs": "99214", "modifier": "26"}
    ]
    
    error = DuplicateKeyError("Test duplicate error", duplicates=dupes)
    
    assert isinstance(error, ParseError)
    assert "Test duplicate error" in str(error)
    assert error.duplicates == dupes
    assert len(error.duplicates) == 2


def test_category_validation_error_structure():
    """CategoryValidationError stores field name and invalid values."""
    error = CategoryValidationError("status_code", ["X", "Y", "Z"])
    
    assert isinstance(error, ParseError)
    assert error.field == "status_code"
    assert error.invalid_values == ["X", "Y", "Z"]
    assert "status_code" in str(error)
    assert "invalid values" in str(error).lower()


def test_layout_mismatch_error_basic():
    """LayoutMismatchError is simple string-based exception."""
    error = LayoutMismatchError("Width mismatch at column 5")
    
    assert isinstance(error, ParseError)
    assert "Width mismatch" in str(error)


def test_schema_regression_error_structure():
    """SchemaRegressionError stores unexpected field names."""
    unexpected = ["vintage_year", "unknown_col"]
    error = SchemaRegressionError("Schema regression detected", unexpected_fields=unexpected)
    
    assert isinstance(error, ParseError)
    assert error.unexpected_fields == unexpected
    assert "Schema regression" in str(error)


def test_check_natural_key_uniqueness_block_severity_raises():
    """
    check_natural_key_uniqueness with severity=BLOCK raises DuplicateKeyError.
    
    Critical for PPRRVU parser which must hard-fail on duplicates.
    """
    # Create DataFrame with duplicate natural keys
    df = pd.DataFrame([
        {"hcpcs": "99213", "modifier": "", "work_rvu": 1.5},
        {"hcpcs": "99213", "modifier": "", "work_rvu": 1.6},  # Duplicate!
        {"hcpcs": "99214", "modifier": "26", "work_rvu": 2.0}
    ])
    
    # BLOCK severity should raise
    with pytest.raises(DuplicateKeyError) as exc_info:
        check_natural_key_uniqueness(
            df,
            natural_keys=["hcpcs", "modifier"],
            severity=ValidationSeverity.BLOCK,
            schema_id="cms_pprrvu_v1.1",
            release_id="mpfs_2025_q4"
        )
    
    # Verify error details
    error = exc_info.value
    assert isinstance(error, DuplicateKeyError)
    assert "2 duplicates" in str(error)
    assert len(error.duplicates) == 1  # 1 unique duplicate key
    assert error.duplicates[0]["hcpcs"] == "99213"
    assert error.duplicates[0]["modifier"] == ""


def test_check_natural_key_uniqueness_warn_severity_returns_rejects():
    """
    check_natural_key_uniqueness with severity=WARN returns rejects (no raise).
    
    Default behavior for backwards compatibility.
    """
    # Create DataFrame with duplicate natural keys
    df = pd.DataFrame([
        {"hcpcs": "99213", "modifier": "", "work_rvu": 1.5},
        {"hcpcs": "99213", "modifier": "", "work_rvu": 1.6},  # Duplicate!
        {"hcpcs": "99214", "modifier": "26", "work_rvu": 2.0}
    ])
    
    # WARN severity should return rejects
    unique_df, dupes_df = check_natural_key_uniqueness(
        df,
        natural_keys=["hcpcs", "modifier"],
        severity=ValidationSeverity.WARN,
        schema_id="cms_pprrvu_v1.1",
        release_id="mpfs_2025_q4"
    )
    
    # Verify split
    assert len(unique_df) == 1  # Only 99214
    assert len(dupes_df) == 2  # Both 99213 rows
    
    # Verify provenance in rejects
    assert all(dupes_df["validation_rule_id"] == "NATURAL_KEY_DUPLICATE")
    assert all(dupes_df["validation_severity"] == "WARN")
    assert all(dupes_df["schema_id"] == "cms_pprrvu_v1.1")
    assert all(dupes_df["release_id"] == "mpfs_2025_q4")
    assert "row_id" in dupes_df.columns


def test_check_natural_key_uniqueness_no_duplicates():
    """No duplicates → all rows valid, empty rejects."""
    df = pd.DataFrame([
        {"hcpcs": "99213", "modifier": "", "work_rvu": 1.5},
        {"hcpcs": "99214", "modifier": "26", "work_rvu": 2.0},
        {"hcpcs": "99215", "modifier": "TC", "work_rvu": 2.5}
    ])
    
    unique_df, dupes_df = check_natural_key_uniqueness(
        df,
        natural_keys=["hcpcs", "modifier"],
        severity=ValidationSeverity.BLOCK  # Even BLOCK is OK if no dupes
    )
    
    assert len(unique_df) == 3
    assert len(dupes_df) == 0
    assert "row_id" in unique_df.columns  # row_id added to all rows


def test_error_inheritance_chain():
    """All custom errors inherit from ParseError for consistent catching."""
    assert issubclass(DuplicateKeyError, ParseError)
    assert issubclass(CategoryValidationError, ParseError)
    assert issubclass(LayoutMismatchError, ParseError)
    assert issubclass(SchemaRegressionError, ParseError)
    
    # Can catch all parser errors with single except
    try:
        raise DuplicateKeyError("test")
    except ParseError:
        pass  # Successfully caught


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

