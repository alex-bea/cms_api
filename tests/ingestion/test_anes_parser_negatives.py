"""
ANES Parser - Negative/Error Tests

Tests error handling, validation rejection, and edge cases.

Per STD-qa-testing-prd-v1.0 §G.1 (Rich Error Messages).
Per QTS §2.2.1 (Test Categorization with pytest markers).
Per STD-parser-contracts v2.0 §21.3 (Negative Tests).
"""

import pytest
from pathlib import Path
from datetime import datetime
from io import BytesIO

from cms_pricing.ingestion.parsers.anes_parser import parse_anes
from cms_pricing.ingestion.parsers._parser_kit import ParseError


# Test metadata (shared across all negative tests)
TEST_METADATA = {
    'release_id': 'test_anes_negative',
    'schema_id': 'cms_anescf_v1.1',
    'product_year': '2025',
    'quarter_vintage': 'D',
    'vintage_date': datetime(2025, 10, 1),
    'file_sha256': 'test_negative',
    'source_uri': 'test://anes_negative',
    'source_release': 'RVU25D_NEGATIVE',
}


# =============================================================================
# Range Validation Tests
# =============================================================================

@pytest.mark.negative
def test_anes_cf_out_of_range_rejects():
    """
    Test CF > $100 rejected (HARD range).
    
    QTS §G.3: Rejects structure with validation details
    """
    fixture_path = Path('tests/fixtures/anes/negatives/out_of_range.csv')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'out_of_range.csv', TEST_METADATA)
    
    # Should have rejects (CF = 15000 cents = $150 > $100 max)
    assert len(result.rejects) > 0, "Should reject CF values > $100"
    
    # QTS §G.3: Verify rejects structure
    assert 'validation_error' in result.rejects.columns
    assert 'validation_severity' in result.rejects.columns
    assert 'validation_rule' in result.rejects.columns
    
    # Verify rejection details
    reject = result.rejects.iloc[0]
    assert reject['validation_severity'] == 'HARD'
    assert reject['validation_rule'] == 'HARD_RANGE'
    assert 'range' in reject['validation_error'].lower()


@pytest.mark.negative
def test_anes_cf_negative_rejected():
    """
    Test negative CF values rejected.
    
    QTS §G.1: Rich error messages with context
    """
    fixture_path = Path('tests/fixtures/anes/negatives/negative_cf.csv')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'negative_cf.csv', TEST_METADATA)
    
    # Should reject negative values
    assert len(result.rejects) > 0, "Should reject negative CF values"
    
    # Verify explicit negative check caught it
    reject = result.rejects.iloc[0]
    assert reject['validation_severity'] == 'HARD'
    assert reject['validation_rule'] in ['NEGATIVE_OR_ZERO', 'HARD_RANGE']


@pytest.mark.negative
def test_anes_cf_zero_rejected():
    """
    Test CF = 0 rejected (must be positive).
    
    QTS: Per schema min_value = 0.01 (CF must be > 0)
    """
    fixture_path = Path('tests/fixtures/anes/negatives/zero_cf.csv')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'zero_cf.csv', TEST_METADATA)
    
    # Should reject zero values
    assert len(result.rejects) > 0, "Should reject CF = 0 (must be positive)"
    
    # Verify validation details
    reject = result.rejects.iloc[0]
    assert reject['validation_severity'] == 'HARD'
    assert reject['validation_rule'] == 'NEGATIVE_OR_ZERO'
    assert 'positive' in reject['validation_error'].lower() or '> 0' in reject['validation_error']


# =============================================================================
# Uniqueness Tests (QTS: Strict quarantine policy)
# =============================================================================

@pytest.mark.negative
def test_anes_duplicate_natural_keys():
    """
    Test duplicate natural keys are QUARANTINED (strict policy).
    
    QTS: ANES uses strict quarantine (not keep='first' like GPCI)
    """
    fixture_path = Path('tests/fixtures/anes/negatives/duplicate_keys.csv')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'duplicate_keys.csv', TEST_METADATA)
    
    # Should quarantine BOTH copies (strict policy)
    # Fixture has 2 rows with same (mac, locality_code) → same effective_from
    assert len(result.rejects) == 2, \
           "Strict policy: ALL duplicate copies should be quarantined (not keep='first')"
    
    # Valid data should be empty (both copies rejected)
    assert len(result.data) == 0, "No valid rows when all are duplicates"
    
    # Verify quarantine details
    assert (result.rejects['validation_rule'] == 'NATURAL_KEY_DUPLICATE').all()
    assert 'Duplicate natural key' in result.rejects['validation_error'].iloc[0]


# =============================================================================
# Row Count Validation Tests
# =============================================================================

@pytest.mark.negative
def test_anes_row_count_below_minimum_fails():
    """
    Test < 10 rows fails (CRITICAL threshold).
    
    QTS: Files with 6-9 rows trigger CRITICAL (too small for production)
    """
    fixture_path = Path('tests/fixtures/anes/negatives/too_few_rows.csv')
    
    # Fixture has 7 rows (< 10 threshold, triggers CRITICAL)
    with pytest.raises(ParseError) as exc_info:
        with open(fixture_path, 'rb') as f:
            parse_anes(f, 'too_few_rows.csv', TEST_METADATA)
    
    # QTS §G.1: Verify rich error message
    error_msg = str(exc_info.value)
    assert 'row count' in error_msg.lower()
    assert '10' in error_msg or 'too few' in error_msg.lower() or 'empty' in error_msg.lower()


# =============================================================================
# Schema Validation Tests
# =============================================================================

@pytest.mark.negative
def test_anes_missing_required_column_fails():
    """
    Test missing MAC column raises ParseError.
    
    QTS §G.1: Rich error messages with context
    """
    fixture_path = Path('tests/fixtures/anes/negatives/missing_mac.csv')
    
    # Fixture is missing 'mac' column
    with pytest.raises(ParseError) as exc_info:
        with open(fixture_path, 'rb') as f:
            parse_anes(f, 'missing_mac.csv', TEST_METADATA)
    
    # QTS §G.1: Error should mention missing column
    error_msg = str(exc_info.value)
    assert 'mac' in error_msg.lower() or 'missing' in error_msg.lower()

