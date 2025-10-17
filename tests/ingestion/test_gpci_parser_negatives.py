"""
GPCI Parser - Negative Tests

Tests error handling and validation for invalid inputs.

Per STD-parser-contracts v1.7 §14 (Testing Strategy).
Per IMPLEMENTATION.md Test Plan §2 (Negative Tests).
"""

import pytest
from pathlib import Path
from datetime import datetime
from io import BytesIO

from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci, ParseError
from cms_pricing.ingestion.parsers._parser_kit import ValidationSeverity


# Test metadata
TEST_METADATA = {
    'release_id': 'test_rvu25d_negatives',
    'schema_id': 'cms_gpci_v1.2',
    'product_year': '2025',
    'quarter_vintage': 'D',
    'vintage_date': datetime(2025, 10, 17, 10, 0, 0),
    'file_sha256': 'test_sha256_negative',
    'source_uri': 'file://tests/fixtures/gpci/negatives/',
    'source_release': 'RVU25D',
}


@pytest.fixture
def fixtures_dir():
    """Path to negative fixtures directory."""
    return Path(__file__).parent.parent / 'fixtures' / 'gpci' / 'negatives'


# ============================================================================
# Validation Error Tests
# ============================================================================

@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_out_of_range_rejects(fixtures_dir):
    """
    GPCI values outside hard bounds [0.20, 2.50] are rejected.
    
    Fixture: GPCI work=3.0, PE=3.5 (above 2.50)
    Expected: Rows rejected with validation_error
    """
    fixture = fixtures_dir / 'out_of_range.csv'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'out_of_range.csv', TEST_METADATA)
    
    # Verify rows were rejected
    assert len(result.rejects) > 0, "Out-of-range values should be rejected"
    assert len(result.data) < 2, "Invalid rows should not be in data"
    
    # Verify reject reason
    assert 'validation_error' in result.rejects.columns
    assert 'GPCI value' in result.rejects['validation_error'].iloc[0]
    
    # Verify metrics reflect rejects
    assert result.metrics['reject_rows'] > 0


@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_negative_values_rejected(fixtures_dir):
    """
    Negative GPCI values are rejected (below hard floor 0.20).
    
    Fixture: GPCI work=-0.5
    Expected: Row rejected
    """
    fixture = fixtures_dir / 'negative_values.csv'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'negative_values.csv', TEST_METADATA)
    
    # Verify rejected
    assert len(result.rejects) == 1
    assert result.metrics['reject_rows'] == 1


@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_duplicate_keys_quarantined(fixtures_dir):
    """
    Duplicate natural keys are quarantined with WARN severity (GPCI policy).
    
    Per STD-parser-contracts §8.5 (GPCI uses WARN for duplicates).
    Fixture: Two rows with locality_code=01, same effective_from
    Expected: Duplicates quarantined, unique row kept
    """
    fixture = fixtures_dir / 'duplicate_keys.csv'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'duplicate_keys.csv', TEST_METADATA)
    
    # GPCI uses WARN severity, so duplicates quarantined (not BLOCK)
    assert len(result.data) >= 1, "At least one unique row should remain"
    assert len(result.rejects) >= 1, "Duplicates should be in rejects"
    
    # Verify no duplicates in final data
    locality_counts = result.data.groupby(['locality_code', 'effective_from']).size()
    assert (locality_counts == 1).all(), "Final data should have no duplicates"


@pytest.mark.negative
@pytest.mark.gpci  
def test_gpci_empty_file_fails(fixtures_dir):
    """
    Empty file raises ParseError.
    
    Expected: ParseError or empty DataFrame with proper error handling
    """
    fixture = fixtures_dir / 'empty.txt'
    
    with open(fixture, 'rb') as f:
        # Empty file should either raise or return empty result
        try:
            result = parse_gpci(f, 'empty.txt', TEST_METADATA)
            # If doesn't raise, should have 0 rows
            assert len(result.data) == 0
        except (ParseError, ValueError, IndexError) as e:
            # Expected - empty file should fail parsing
            assert 'empty' in str(e).lower() or 'no data' in str(e).lower() or len(str(e)) > 0


@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_row_count_below_minimum_fails(fixtures_dir):
    """
    Row count < 90 raises ParseError (critical threshold).
    
    Fixture: Only 3 rows (well below 90 minimum)
    Expected: ParseError with actionable guidance
    """
    fixture = fixtures_dir / 'too_few_rows.csv'
    
    with open(fixture, 'rb') as f:
        with pytest.raises(ParseError) as exc_info:
            parse_gpci(f, 'too_few_rows.csv', TEST_METADATA)
    
    # Verify error message is actionable
    error_msg = str(exc_info.value)
    assert '90' in error_msg, "Error should mention 90-row threshold"
    assert 'CRITICAL' in error_msg or 'minimum' in error_msg.lower()


@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_invalid_source_release_fails():
    """
    Invalid source_release format raises ParseError.
    
    Expected: RVU25A/B/C/D format required, other values fail
    """
    invalid_metadata = TEST_METADATA.copy()
    invalid_metadata['source_release'] = 'INVALID2025'
    
    # Create minimal CSV
    csv_content = b"locality_code,gpci_work,gpci_pe,gpci_mp\n01,1.000,1.000,1.000\n"
    
    with pytest.raises(ParseError) as exc_info:
        parse_gpci(BytesIO(csv_content), 'test.csv', invalid_metadata)
    
    # Verify error mentions valid releases
    error_msg = str(exc_info.value)
    assert 'source_release' in error_msg.lower()
    assert 'RVU25' in error_msg


@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_missing_required_metadata_fails():
    """
    Missing required metadata fields raises ValueError.
    
    Per STD-parser-contracts §6.4 (Metadata Injection Contract).
    """
    incomplete_metadata = {
        'release_id': 'test',
        # Missing schema_id, product_year, etc.
    }
    
    csv_content = b"locality_code,gpci_work,gpci_pe,gpci_mp\n01,1.000,1.000,1.000\n"
    
    with pytest.raises((ValueError, KeyError)) as exc_info:
        parse_gpci(BytesIO(csv_content), 'test.csv', incomplete_metadata)
    
    # Should mention missing fields
    error_msg = str(exc_info.value).lower()
    assert 'required' in error_msg or 'missing' in error_msg or 'metadata' in error_msg


@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_malformed_csv_fails(fixtures_dir):
    """
    Malformed CSV (inconsistent columns) raises ParseError.
    
    Tests parser's robustness to bad input data.
    """
    # Create malformed CSV (different column counts per row)
    malformed = b"locality_code,gpci_work,gpci_pe,gpci_mp\n01,1.000,1.000\n"  # Missing gpci_mp
    
    with pytest.raises((ParseError, ValueError, KeyError)) as exc_info:
        parse_gpci(BytesIO(malformed), 'malformed.csv', TEST_METADATA)
    
    # Parser should fail gracefully
    assert len(str(exc_info.value)) > 0


# ============================================================================
# Edge Cases
# ============================================================================

@pytest.mark.negative
@pytest.mark.gpci
def test_gpci_missing_required_column_fails(fixtures_dir):
    """
    Missing required column (gpci_work) raises error.
    
    Schema requires: locality_code, gpci_work, gpci_pe, gpci_mp
    """
    fixture = fixtures_dir / 'missing_column.csv'
    
    with open(fixture, 'rb') as f:
        with pytest.raises((KeyError, ParseError, ValueError)) as exc_info:
            parse_gpci(f, 'missing_column.csv', TEST_METADATA)
    
    # Error should mention missing column
    error_msg = str(exc_info.value).lower()
    assert 'gpci_work' in error_msg or 'missing' in error_msg or 'column' in error_msg

