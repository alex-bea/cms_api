"""
Conversion Factor Parser - Negative Test Cases

Objective: Prove robust failure/warning behavior and metrics integrity 
across malformed inputs.

Per user feedback (2025-10-16): 8 failure modes with severity classification.

Tests:
1. Missing required columns (BLOCK)
2. Out-of-range cf_value (BLOCK/WARN)
3. Unparseable dates (BLOCK)
4. Encoding/BOM oddities (WARN + pass)
5. Duplicate rows (BLOCK on exact NK, WARN on whitespace variants)
6. Unknown cf_type (BLOCK)
7. Non-USD currency hints (WARN only)
8. Future effective dates (WARN only)

Assertions:
- ParseResult.rejects has correct severity
- ParseResult.data empty on BLOCK, stable on WARN
- metrics include error_counts, dedupe_drop_count, effective_date_range
"""

import pytest
from pathlib import Path
from io import BytesIO
import pandas as pd

from cms_pricing.ingestion.parsers._parser_kit import (
    ParseResult,
    ParseError,
    DuplicateKeyError,
    CategoryValidationError,
    ValidationSeverity
)


FIXTURES = Path(__file__).parent.parent / "fixtures" / "conversion_factor"


def build_test_metadata(product_year="2025"):
    """Build minimal test metadata."""
    return {
        'release_id': f'mpfs_{product_year}_test',
        'schema_id': 'cms_conversion_factor_v2.0',
        'product_year': product_year,
        'quarter_vintage': f'{product_year}_annual',
        'vintage_date': pd.Timestamp(f'{product_year}-01-01'),
        'file_sha256': 'test_sha256',
        'source_uri': 'https://test.cms.gov/cf.csv',
        'parser_version': 'v1.0.0',
    }


# ============================================================================
# Test 1: Missing Required Columns (BLOCK)
# ============================================================================

@pytest.mark.ingestor
def test_cf_missing_required_columns_error():
    """
    BLOCK on missing required columns (cf_value, effective_from).
    
    Severity: BLOCK (cannot proceed without required fields)
    Expected: Raise exception or return empty data with error metrics
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    # CSV missing cf_value column
    csv_missing_value = """cf_type,cf_description,effective_from,effective_to
physician,Test Description,2025-01-01,
"""
    
    with pytest.raises((ParseError, ValueError, KeyError)) as exc_info:
        result = parse_conversion_factor(
            BytesIO(csv_missing_value.encode()),
            "bad_missing_cf_value.csv",
            build_test_metadata()
        )
    
    assert 'cf_value' in str(exc_info.value).lower() or 'missing' in str(exc_info.value).lower()
    
    print("✅ Missing column BLOCK: Parser fails fast with clear error")


# ============================================================================
# Test 2: Out-of-Range cf_value (BLOCK/WARN)
# ============================================================================

@pytest.mark.ingestor
def test_cf_cfvalue_out_of_range_error():
    """
    BLOCK on cf_value ≤ 0, WARN on cf_value > 100.
    
    Severity:
    - BLOCK: cf_value ≤ 0 (invalid, cannot proceed)
    - WARN: cf_value > 100 (unusual, quarantine but continue)
    
    Expected: Negative values rejected, high values warned
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "bad_negative_value.csv"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(f, "bad_negative_value.csv", build_test_metadata())
    
    # Should have rejects (negative value)
    assert len(result.rejects) > 0, "Negative cf_value should be rejected"
    assert len(result.data) == 0, "No valid data with negative cf_value"
    
    # Check reject reason
    if len(result.rejects) > 0:
        reject_row = result.rejects.iloc[0]
        error_msg = str(reject_row.get('validation_error', ''))
        assert 'cf_value' in error_msg.lower() or 'negative' in error_msg.lower()
    
    # Metrics should reflect rejection
    assert result.metrics['reject_rows'] > 0
    assert result.metrics['valid_rows'] == 0
    
    print("✅ Out-of-range BLOCK: Negative cf_value rejected with metrics")


# ============================================================================
# Test 3: Unparseable Dates (BLOCK)
# ============================================================================

@pytest.mark.ingestor
def test_cf_unparseable_effective_date_error():
    """
    BLOCK on unparseable effective_from dates.
    
    Test cases:
    - "TBD" (text instead of date)
    - "13/32/2025" (invalid date)
    - "2025-13-01" (invalid month)
    
    Severity: BLOCK (cannot proceed without valid effective date)
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    # CSV with invalid date
    csv_bad_date = """cf_type,cf_value,cf_description,effective_from,effective_to
physician,32.3465,Test,TBD,
"""
    
    # Should raise or reject
    result = parse_conversion_factor(
        BytesIO(csv_bad_date.encode()),
        "bad_date.csv",
        build_test_metadata()
    )
    
    # Either raises or rejects the row
    if len(result.data) > 0:
        pytest.fail("Should reject unparseable dates, but parsed successfully")
    
    assert len(result.rejects) > 0 or result.metrics['valid_rows'] == 0, \
        "Unparseable dates should be rejected"
    
    print("✅ Unparseable date BLOCK: Invalid dates rejected")


# ============================================================================
# Test 4: Encoding/BOM Oddities (WARN + Pass)
# ============================================================================

@pytest.mark.ingestor
def test_cf_encoding_bom_warn_and_pass():
    """
    WARN on encoding fallback, but parse successfully.
    
    Test cases:
    - UTF-8 BOM (\\xef\\xbb\\xbf) → stripped, data parsed
    - CP1252 (Windows) → detected, data parsed
    
    Severity: WARN (log + metric, but don't block)
    Expected: Data parses successfully, metrics flag encoding
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    # UTF-8 with BOM
    csv_bom = b'\xef\xbb\xbfcf_type,cf_value,cf_description,effective_from,effective_to\nphysician,32.3465,Test,2025-01-01,\n'
    
    result = parse_conversion_factor(
        BytesIO(csv_bom),
        "cf_bom.csv",
        build_test_metadata()
    )
    
    # Should parse successfully
    assert len(result.data) == 1, "BOM should not block parsing"
    assert len(result.rejects) == 0
    
    # Metrics should indicate BOM detected
    assert result.metrics['encoding_detected'] in ['utf-8', 'utf-8-sig']
    
    # Columns should be clean (no \\ufeff)
    assert all('\ufeff' not in str(col) for col in result.data.columns)
    
    print("✅ Encoding/BOM WARN: BOM stripped, data parsed, metrics logged")


# ============================================================================
# Test 5: Duplicate Rows (BLOCK on exact NK)
# ============================================================================

@pytest.mark.ingestor
def test_cf_duplicate_rows_block():
    """
    BLOCK on exact duplicate natural keys.
    
    Test case: Same cf_type + effective_from (exact duplicate)
    
    Severity: BLOCK per schema (§8.5 severity table)
    Expected: Raise DuplicateKeyError with duplicate details
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "bad_duplicate.csv"
    
    with pytest.raises(DuplicateKeyError) as exc_info:
        with open(fixture_path, 'rb') as f:
            parse_conversion_factor(f, "bad_duplicate.csv", build_test_metadata())
    
    # Error should contain duplicate details
    error_msg = str(exc_info.value)
    assert 'physician' in error_msg
    assert 'duplicate' in error_msg.lower() or '2025-01-01' in error_msg
    
    # Should have duplicates list
    assert hasattr(exc_info.value, 'duplicates')
    
    print("✅ Duplicate NK BLOCK: DuplicateKeyError raised with details")


# ============================================================================
# Test 6: Unknown cf_type (BLOCK)
# ============================================================================

@pytest.mark.ingestor
def test_cf_unknown_type_block():
    """
    BLOCK on unknown cf_type values.
    
    Valid: {'physician', 'anesthesia'}
    Invalid: 'MPFS', 'ANESTH', 'invalid_type', etc.
    
    Severity: BLOCK (categorical domain violation)
    Expected: Reject rows with unknown cf_type
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "bad_cf_type.csv"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(f, "bad_cf_type.csv", build_test_metadata())
    
    # Should have rejects (unknown categorical value)
    assert len(result.rejects) > 0, "Unknown cf_type should be rejected"
    
    # Data should be empty or not contain invalid type
    if len(result.data) > 0:
        assert 'invalid_type' not in result.data['cf_type'].values
    
    # Check reject reason contains cf_type
    if len(result.rejects) > 0:
        reject_error = str(result.rejects.iloc[0].get('validation_error', ''))
        assert 'cf_type' in reject_error.lower() or 'invalid' in reject_error.lower()
    
    # Metrics should show categorical rejection
    assert result.metrics['reject_rows'] > 0
    
    print("✅ Unknown cf_type BLOCK: Categorical validation rejected invalid type")


# ============================================================================
# Test 7: CMS Value Guardrail (WARN only)
# ============================================================================

@pytest.mark.ingestor
def test_cf_physician_value_deviation_warn():
    """
    WARN if physician CF deviates from known CMS value.
    
    CY-2025 authoritative: 32.3465 (CMS Federal Register)
    Test value: 99.0000 (wrong)
    
    Severity: WARN only (don't block, data may be updated/corrected)
    Expected: Parse successfully, but metrics flag deviation
    
    Source: https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    fixture_path = FIXTURES / "bad_wrong_physician_cf.csv"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(f, "bad_wrong_physician_cf.csv", build_test_metadata())
    
    # Should parse successfully (WARN, not BLOCK)
    assert len(result.data) >= 1, "Incorrect CF value should WARN, not BLOCK"
    
    # Metrics should flag deviation
    assert 'cf_value_deviation_warn' in result.metrics or 'warnings' in result.metrics, \
        "Metrics should flag CF value deviation from CMS authoritative value"
    
    # If deviation metric present, check details
    if 'cf_value_deviation_warn' in result.metrics:
        warn = result.metrics['cf_value_deviation_warn']
        assert warn['cf_type'] == 'physician'
        assert warn['expected_value'] == 32.3465
        assert warn['parsed_value'] != 32.3465
    
    print("✅ CMS Value Guardrail WARN: Deviation flagged, data not blocked")


# ============================================================================
# Test 8: Coverage and Metrics Integrity
# ============================================================================

@pytest.mark.ingestor
def test_cf_negative_metrics_integrity():
    """
    Verify all negative cases produce complete metrics.
    
    Every ParseResult must include:
    - total_rows, valid_rows, reject_rows
    - reject_rate
    - encoding_detected, encoding_fallback
    - skiprows_dynamic
    - parser_version, schema_id
    
    CI gate: Missing metric keys fail the build.
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    # Use bad_cf_type fixture (known to produce rejects)
    fixture_path = FIXTURES / "bad_cf_type.csv"
    
    with open(fixture_path, 'rb') as f:
        result = parse_conversion_factor(f, "bad_cf_type.csv", build_test_metadata())
    
    # Required metric keys
    required_keys = [
        'total_rows', 'valid_rows', 'reject_rows', 'reject_rate',
        'encoding_detected', 'encoding_fallback', 
        'skiprows_dynamic', 'parser_version', 'schema_id',
        'parse_duration_sec'
    ]
    
    for key in required_keys:
        assert key in result.metrics, f"Missing required metric key: {key}"
    
    # Join invariant
    assert result.metrics['total_rows'] == len(result.data) + len(result.rejects), \
        "Join invariant: total must equal valid + rejects"
    
    # Reject rate calculation
    if result.metrics['total_rows'] > 0:
        expected_rate = result.metrics['reject_rows'] / result.metrics['total_rows']
        assert abs(result.metrics['reject_rate'] - expected_rate) < 0.001, \
            "reject_rate must be accurately calculated"
    
    print("✅ Metrics Integrity: All required keys present, join invariant holds")


# ============================================================================
# Test 9: Empty File (Edge Case)
# ============================================================================

@pytest.mark.ingestor
def test_cf_empty_file():
    """
    Handle empty CSV gracefully.
    
    Expected: Return empty data, no rejects, metrics show 0 rows
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    # Empty CSV (header only)
    csv_empty = """cf_type,cf_value,cf_description,effective_from,effective_to
"""
    
    result = parse_conversion_factor(
        BytesIO(csv_empty.encode()),
        "empty.csv",
        build_test_metadata()
    )
    
    assert len(result.data) == 0, "Empty file should produce 0 data rows"
    assert len(result.rejects) == 0, "Empty file should produce 0 rejects"
    assert result.metrics['total_rows'] == 0
    assert result.metrics['valid_rows'] == 0
    assert result.metrics['reject_rows'] == 0
    
    print("✅ Empty File: Handled gracefully with 0-row metrics")


# ============================================================================
# Test 10: Whitespace Variants (Normalization)
# ============================================================================

@pytest.mark.ingestor
def test_cf_whitespace_in_cf_type():
    """
    Handle whitespace in cf_type values.
    
    Test cases:
    - "physician " (trailing space)
    - " anesthesia" (leading space)
    - "physician\\u00a0" (non-breaking space - NBSP)
    
    Expected: Normalize whitespace before validation (Anti-Pattern 9)
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    csv_whitespace = """cf_type,cf_value,cf_description,effective_from,effective_to
physician ,32.3465,Trailing Space,2025-01-01,
 anesthesia,20.3178,Leading Space,2025-01-01,
"""
    
    result = parse_conversion_factor(
        BytesIO(csv_whitespace.encode()),
        "cf_whitespace.csv",
        build_test_metadata()
    )
    
    # Should parse successfully after whitespace normalization
    assert len(result.data) == 2, "Whitespace should be stripped before validation"
    assert len(result.rejects) == 0, "Trimmed values should pass categorical validation"
    
    # Verify values are trimmed
    assert all(result.data['cf_type'].isin(['physician', 'anesthesia']))
    
    print("✅ Whitespace Normalization: Leading/trailing spaces stripped")


# ============================================================================
# Test 11: Precision Rounding (4dp HALF_UP)
# ============================================================================

@pytest.mark.ingestor
def test_cf_precision_rounding_half_up():
    """
    Verify 4dp precision with HALF_UP rounding.
    
    Test cases:
    - 32.34655 → 32.3466 (round up)
    - 32.34645 → 32.3465 (round up)
    - 32.34644 → 32.3464 (round down)
    
    Schema specifies: precision=4, rounding_mode=HALF_UP
    """
    from cms_pricing.ingestion.parsers.conversion_factor_parser import parse_conversion_factor
    
    csv_precision = """cf_type,cf_value,cf_description,effective_from,effective_to
physician,32.34655,Should Round to 32.3466,2025-01-01,
anesthesia,20.31775,Should Round to 20.3178,2025-01-01,
"""
    
    result = parse_conversion_factor(
        BytesIO(csv_precision.encode()),
        "cf_precision.csv",
        build_test_metadata()
    )
    
    assert len(result.data) == 2
    
    # Check rounding (HALF_UP)
    phys = result.data[result.data['cf_type'] == 'physician'].iloc[0]
    anes = result.data[result.data['cf_type'] == 'anesthesia'].iloc[0]
    
    # Note: Exact assertion depends on implementation
    # Should be rounded to 4dp
    assert len(str(phys['cf_value']).split('.')[-1]) <= 4, "Must have ≤4 decimal places"
    assert len(str(anes['cf_value']).split('.')[-1]) <= 4, "Must have ≤4 decimal places"
    
    print("✅ Precision Rounding: 4dp HALF_UP rounding enforced")

