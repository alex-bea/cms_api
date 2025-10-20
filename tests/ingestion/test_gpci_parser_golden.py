"""
GPCI Parser - Golden File Tests

Tests deterministic parsing across all supported formats (TXT, CSV, XLSX, ZIP).

Per STD-parser-contracts v1.7 §14.2 (Golden-File Tests).
Per IMPLEMENTATION.md Test Plan §1 (Golden Tests).
"""

import pytest
from pathlib import Path
from datetime import datetime
from io import BytesIO

import pandas as pd

from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci, PARSER_VERSION


# Test metadata (shared across all golden tests)
TEST_METADATA = {
    'release_id': 'test_rvu25d_20251017',
    'schema_id': 'cms_gpci_v1.3',
    'product_year': '2025',
    'quarter_vintage': 'D',
    'vintage_date': datetime(2025, 10, 17, 10, 0, 0),
    'file_sha256': 'test_sha256_golden_fixture',
    'source_uri': 'file://tests/fixtures/gpci/golden/',
    'source_release': 'RVU25D',
}


@pytest.fixture
def fixtures_dir():
    """Path to golden fixtures directory."""
    return Path(__file__).parent.parent / 'fixtures' / 'gpci' / 'golden'


# ============================================================================
# Golden Tests - Format Support
# ============================================================================

@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_golden_txt(fixtures_dir):
    """
    TXT format (fixed-width) produces valid, deterministic output.
    
    Fixture: 20 data rows from GPCI2025.txt
    Layout: GPCI_2025D_LAYOUT v2025.4.1
    Expected: 20 localities parsed
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Verify ParseResult structure
    assert hasattr(result, 'data')
    assert hasattr(result, 'rejects')
    assert hasattr(result, 'metrics')
    
    # Verify row count (clean golden fixture: no duplicates, no rejects)
    # Per STD-qa-testing-prd §5.1: "Validate goldens against schema contracts"
    assert len(result.data) == 18, f"Expected exactly 18 unique rows, got {len(result.data)}"
    assert len(result.rejects) == 0, f"No rejects expected for clean golden fixture, got {len(result.rejects)}"
    
    # Verify metrics (deterministic)
    assert result.metrics['total_rows'] == 18
    assert result.metrics['valid_rows'] == 18
    assert result.metrics['reject_rows'] == 0
    
    # Verify schema compliance (core columns)
    required_cols = ['mac', 'locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 
                     'effective_from', 'effective_to']
    for col in required_cols:
        assert col in result.data.columns, f"Missing required column: {col}"
    
    # Verify provenance columns
    provenance_cols = ['source_release', 'source_inner_file', 'source_file_sha256',
                      'release_id', 'parsed_at', 'row_content_hash']
    for col in provenance_cols:
        assert col in result.data.columns, f"Missing provenance column: {col}"
    
    # Verify row_content_hash is 64 characters (SHA-256)
    assert result.data['row_content_hash'].str.len().eq(64).all(), \
        "All row_content_hash values must be 64 characters"
    
    # Verify specific localities (Alaska - locality 01)
    # Note: Locality 00 quarantined as duplicate, so using Alaska instead
    alaska = result.data[result.data['locality_code'] == '01']
    assert len(alaska) > 0, "Alaska (locality 01) should be in valid data"
    alaska = alaska.iloc[0]
    assert alaska['gpci_work'] == '1.500', f"Alaska work GPCI should be 1.500 (floor), got {alaska['gpci_work']}"
    assert alaska['gpci_pe'] == '1.081', f"Alaska PE GPCI should be 1.081, got {alaska['gpci_pe']}"
    assert alaska['gpci_mp'] == '0.592', f"Alaska MP GPCI should be 0.592, got {alaska['gpci_mp']}"
    
    # Verify metrics (clean fixture: no rejects)
    assert result.metrics['total_rows'] == 18
    assert result.metrics['valid_rows'] == 18
    assert result.metrics['reject_rows'] == 0
    assert result.metrics['parser_version'] == PARSER_VERSION
    assert result.metrics['schema_id'] == 'cms_gpci_v1.3'
    assert 'gpci_value_stats' in result.metrics
    
    # Verify sorted by natural keys: ['mac', 'locality_code', 'effective_from']
    sorted_df = result.data.sort_values(['mac', 'locality_code', 'effective_from']).reset_index(drop=True)
    result_reset = result.data.reset_index(drop=True)
    assert result_reset.equals(sorted_df), \
           "Data should be sorted by natural keys ['mac', 'locality_code', 'effective_from']"


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_golden_csv(fixtures_dir):
    """
    CSV format produces same output as TXT (determinism across formats).
    
    Fixture: 20 data rows from GPCI2025.csv
    Expected: Same localities as TXT fixture
    """
    fixture = fixtures_dir / 'GPCI2025_sample.csv'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.csv', TEST_METADATA)
    
    # Verify row count (clean golden fixture: identical to TXT)
    assert len(result.data) == 18, f"Expected exactly 18 rows (same as TXT), got {len(result.data)}"
    assert len(result.rejects) == 0, f"No rejects expected for clean golden fixture, got {len(result.rejects)}"
    
    # Verify schema compliance (all required columns per cms_gpci_v1.3)
    required_cols = ['mac', 'locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 
                     'effective_from', 'effective_to', 'row_content_hash']
    for col in required_cols:
        assert col in result.data.columns, f"Missing required column: {col}"
    
    # Verify Alaska values match TXT fixture (locality 00 quarantined)
    alaska = result.data[result.data['locality_code'] == '01']
    if len(alaska) > 0:
        alaska = alaska.iloc[0]
        assert alaska['gpci_work'] == '1.500'
        assert alaska['gpci_pe'] == '1.081'
        assert alaska['gpci_mp'] == '0.592'
    
    # Verify metrics (clean fixture: no rejects)
    assert result.metrics['total_rows'] == 18
    assert result.metrics['valid_rows'] == 18
    assert result.metrics['reject_rows'] == 0
    assert result.metrics['encoding_detected'] in ['utf-8', 'utf-8-sig', 'latin-1']


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_golden_xlsx(fixtures_dir):
    """
    XLSX format produces valid output.
    
    Fixture: Full GPCI2025.xlsx (~115 rows)
    Expected: All localities parsed correctly
    """
    fixture = fixtures_dir / 'GPCI2025_sample.xlsx'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.xlsx', TEST_METADATA)
    
    # Verify row count (fixture has ~113 rows)
    # Note: XLSX has full dataset, not just a sample like CSV/TXT
    # With corrected NK (mac, locality, effective_from), each MAC+locality+date is unique
    # No false duplicates from different quarters (they have different effective_from)
    assert len(result.data) >= 100, \
        f"Expected at least 100 unique rows, got {len(result.data)}"
    assert result.metrics['total_rows'] >= 100, "XLSX should have full dataset"
    
    # With corrected NK, should have minimal rejects (only true duplicates)
    # Allow some rejects for data quality issues, but not the 60+ from old NK
    assert len(result.rejects) < 10, \
        f"Expected <10 rejects with corrected NK, got {len(result.rejects)}"
    
    # Verify schema compliance (all required columns per cms_gpci_v1.3)
    required_cols = ['mac', 'locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 
                     'effective_from', 'effective_to', 'row_content_hash']
    for col in required_cols:
        assert col in result.data.columns, f"Missing required column: {col}"
    
    # Verify all GPCI values are valid
    # Use pd.to_numeric (canonicalize_numeric_col returns strings)
    gpci_work = pd.to_numeric(result.data['gpci_work'], errors='coerce')
    gpci_pe = pd.to_numeric(result.data['gpci_pe'], errors='coerce')
    gpci_mp = pd.to_numeric(result.data['gpci_mp'], errors='coerce')
    
    # After footer filtering, should have no NaN in required columns
    assert gpci_work.notna().all(), f"gpci_work should have no NaN after footer filtering, found {gpci_work.isna().sum()}"
    assert gpci_pe.notna().all(), f"gpci_pe should have no NaN after footer filtering, found {gpci_pe.isna().sum()}"
    assert gpci_mp.notna().all(), f"gpci_mp should have no NaN after footer filtering, found {gpci_mp.isna().sum()}"
    
    # Split range checks for clarity (per user guidance)
    assert (gpci_work >= 0.20).all(), "All gpci_work values should be >= 0.20"
    assert (gpci_work <= 2.50).all(), "All gpci_work values should be <= 2.50"
    assert (gpci_pe >= 0.20).all(), "All gpci_pe values should be >= 0.20"
    assert (gpci_pe <= 2.50).all(), "All gpci_pe values should be <= 2.50"
    assert (gpci_mp >= 0.20).all(), "All gpci_mp values should be >= 0.20"
    assert (gpci_mp <= 2.50).all(), "All gpci_mp values should be <= 2.50"
    
    # Verify metrics
    assert result.metrics['locality_count'] == len(result.data)
    assert 'gpci_value_stats' in result.metrics


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_golden_zip(fixtures_dir):
    """
    ZIP format extracts and parses inner TXT file.
    
    Fixture: ZIP containing GPCI2025_sample.txt
    Expected: Same output as TXT fixture
    """
    fixture = fixtures_dir / 'GPCI2025_sample.zip'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.zip', TEST_METADATA)
    
    # Verify row count (ZIP contains clean TXT: 18 rows, no rejects)
    assert len(result.data) == 18, f"Expected exactly 18 rows from ZIP (same as TXT), got {len(result.data)}"
    assert len(result.rejects) == 0, f"No rejects expected for clean golden fixture, got {len(result.rejects)}"
    
    # Verify source_inner_file tracked
    assert result.data['source_inner_file'].iloc[0] == 'GPCI2025_sample.txt', \
        "ZIP member name should be tracked in source_inner_file"
    
    # Verify same Alaska values as TXT (locality 00 quarantined)
    alaska = result.data[result.data['locality_code'] == '01']
    assert len(alaska) > 0, "Alaska (locality 01) should be in valid data"
    alaska = alaska.iloc[0]
    assert alaska['gpci_work'] == '1.500'
    assert alaska['gpci_pe'] == '1.081'
    assert alaska['gpci_mp'] == '0.592'


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_determinism(fixtures_dir):
    """
    Determinism test: Same input produces identical row_content_hash.
    
    Per STD-parser-contracts v1.7 §5.2 (Row Hash Specification).
    Verifies reproducibility across multiple parse runs.
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    # Parse twice
    with open(fixture, 'rb') as f:
        result1 = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    with open(fixture, 'rb') as f:
        result2 = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Verify row counts match
    assert len(result1.data) == len(result2.data)
    
    # Verify row_content_hash is identical (deterministic)
    hash_match = result1.data['row_content_hash'].equals(result2.data['row_content_hash'])
    assert hash_match, "row_content_hash must be deterministic across runs"
    
    # Verify locality_code order is identical (stable sort)
    locality_match = result1.data['locality_code'].equals(result2.data['locality_code'])
    assert locality_match, "Locality order must be deterministic (sorted by natural keys)"
    
    # Verify all data columns are identical (not just hash)
    # Include all NK fields per schema v1.3
    core_cols = ['mac', 'locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 'effective_from']
    for col in core_cols:
        assert result1.data[col].equals(result2.data[col]), \
            f"Column {col} must be identical across runs"


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_schema_v1_2_compliance(fixtures_dir):
    """
    Verify parser output complies with cms_gpci_v1.3 schema contract.
    
    Checks:
    - CMS-native column names (locality_code, gpci_mp not gpci_malp)
    - Core vs Enrichment vs Provenance column separation
    - Hash excludes enrichment and provenance columns
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Verify CMS-native naming (v1.2 change)
    assert 'locality_code' in result.data.columns, "Should use locality_code (not locality_id)"
    assert 'gpci_mp' in result.data.columns, "Should use gpci_mp (not gpci_malp)"
    assert 'gpci_malp' not in result.data.columns, "Should NOT have old gpci_malp name"
    
    # Verify enrichment columns present (optional in schema)
    if 'mac' in result.data.columns:
        assert result.data['mac'].str.len().eq(5).all(), "MAC should be 5 digits"
    
    if 'state' in result.data.columns:
        assert result.data['state'].str.len().eq(2).all(), "State should be 2 letters"
    
    # Verify provenance columns
    assert result.data['source_release'].iloc[0] == 'RVU25D'
    assert result.data['source_inner_file'].iloc[0] == 'GPCI2025_sample.txt'
    
    # Verify 3 decimal precision for GPCI values
    for col in ['gpci_work', 'gpci_pe', 'gpci_mp']:
        # Values should be strings with exactly 3 decimal places
        sample_val = result.data[col].iloc[0]
        assert isinstance(sample_val, str), f"{col} should be string (for hash stability)"
        if '.' in sample_val:
            decimals = len(sample_val.split('.')[1])
            assert decimals == 3, f"{col} should have 3 decimal places, got {decimals}"


@pytest.mark.golden
@pytest.mark.gpci  
def test_gpci_metadata_injection(fixtures_dir):
    """
    Verify all required metadata columns are injected correctly.
    
    Per STD-parser-contracts §6.4 (Metadata Injection Contract).
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Verify DIS metadata columns (3 vintage fields)
    assert 'vintage_date' in result.data.columns
    assert 'product_year' in result.data.columns
    assert 'quarter_vintage' in result.data.columns
    
    # Verify provenance columns
    assert 'release_id' in result.data.columns
    assert 'source_filename' in result.data.columns
    assert 'source_file_sha256' in result.data.columns
    assert 'source_uri' in result.data.columns
    assert 'parsed_at' in result.data.columns
    
    # Verify GPCI-specific provenance
    assert 'source_release' in result.data.columns
    assert 'source_inner_file' in result.data.columns
    
    # Verify values match metadata
    assert result.data['release_id'].iloc[0] == TEST_METADATA['release_id']
    assert result.data['product_year'].iloc[0] == '2025'
    assert result.data['quarter_vintage'].iloc[0] == 'D'
    assert result.data['source_release'].iloc[0] == 'RVU25D'


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_natural_key_sort(fixtures_dir):
    """
    Verify output is sorted by natural keys: ['mac', 'locality_code', 'effective_from'].
    
    Per STD-parser-contracts §6.3 (Deterministic Output) and schema v1.3.
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Verify sorted by natural keys (mac, locality_code, effective_from)
    nk_tuples = list(zip(result.data['mac'], 
                         result.data['locality_code'],
                         result.data['effective_from']))
    sorted_nk_tuples = sorted(nk_tuples)
    assert nk_tuples == sorted_nk_tuples, \
        "Data must be sorted by natural keys ['mac', 'locality_code', 'effective_from']"
    
    # Verify index is 0, 1, 2, ... (reset after sort)
    assert list(result.data.index) == list(range(len(result.data))), \
        "Index should be reset to 0, 1, 2, ... after sort"


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_metrics_structure(fixtures_dir):
    """
    Verify metrics dictionary has all required fields.
    
    Per STD-parser-contracts §10.1 (Per-File Metrics).
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Required metrics
    required_metrics = [
        'total_rows', 'valid_rows', 'reject_rows',
        'encoding_detected', 'parse_duration_sec',
        'parser_version', 'schema_id', 'locality_count'
    ]
    for metric in required_metrics:
        assert metric in result.metrics, f"Missing required metric: {metric}"
    
    # Verify join invariant
    assert result.metrics['total_rows'] == result.metrics['valid_rows'] + result.metrics['reject_rows']
    
    # Verify GPCI-specific metrics
    assert 'gpci_value_stats' in result.metrics
    stats = result.metrics['gpci_value_stats']
    assert 'work_min' in stats
    assert 'work_max' in stats
    assert 'pe_min' in stats
    assert 'pe_max' in stats
    assert 'mp_min' in stats
    assert 'mp_max' in stats
    
    # Verify reasonable values
    assert 0.20 <= stats['work_min'] <= 2.50
    assert 0.20 <= stats['work_max'] <= 2.50


# ============================================================================
# Cross-Format Consistency
# ============================================================================

@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_txt_csv_consistency(fixtures_dir):
    """
    TXT and CSV fixtures should produce identical core data (same localities).
    
    Verifies format independence - same CMS data, different serialization.
    """
    # Parse TXT
    with open(fixtures_dir / 'GPCI2025_sample.txt', 'rb') as f:
        txt_result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Parse CSV
    with open(fixtures_dir / 'GPCI2025_sample.csv', 'rb') as f:
        csv_result = parse_gpci(f, 'GPCI2025_sample.csv', TEST_METADATA)
    
    # Per STD-qa-testing-prd §5.1: Fixtures should be identical across formats
    # Both TXT and CSV have exactly 18 unique localities (no duplicates)
    assert len(txt_result.data) == 18, "TXT should have exactly 18 rows"
    assert len(csv_result.data) == 18, "CSV should have exactly 18 rows"
    assert len(txt_result.rejects) == 0, "TXT should have no rejects"
    assert len(csv_result.rejects) == 0, "CSV should have no rejects"
    
    # Should have identical localities
    txt_localities = set(txt_result.data['locality_code'])
    csv_localities = set(csv_result.data['locality_code'])
    assert txt_localities == csv_localities, \
        f"TXT and CSV should have identical localities. Diff: {txt_localities ^ csv_localities}"
    
    # Verify GPCI values match exactly for Alaska (MAC 02102, locality 01)
    # Filter by full NK (mac + locality) to avoid ambiguity
    txt_ak = txt_result.data[(txt_result.data['mac'] == '02102') & 
                             (txt_result.data['locality_code'] == '01')].iloc[0]
    csv_ak = csv_result.data[(csv_result.data['mac'] == '02102') & 
                             (csv_result.data['locality_code'] == '01')].iloc[0]
    
    assert txt_ak['gpci_work'] == csv_ak['gpci_work'] == '1.500'
    assert txt_ak['gpci_pe'] == csv_ak['gpci_pe'] == '1.081'
    assert txt_ak['gpci_mp'] == csv_ak['gpci_mp'] == '0.592'


@pytest.mark.edge_case
@pytest.mark.gpci
def test_gpci_real_cms_duplicate_locality_00(fixtures_dir):
    """
    Edge case: Real CMS quirk where AL and AZ both use locality 00 (different MACs).
    
    With corrected NK ['mac', 'locality_code', 'effective_from']:
    - Different MACs with same locality → UNIQUE (not duplicates)
    - Only same MAC+locality+date → TRUE duplicate
    
    Tests that locality 00 in different states (different MACs) are treated as unique.
    Per STD-qa-testing-prd §2.2 (edge case testing requirements).
    
    Fixture: GPCI2025_duplicate_locality_00.txt
    Expected: All 3 rows valid (no false duplicates based on locality alone)
    """
    from pathlib import Path
    
    fixture = Path(__file__).parent.parent / 'fixtures/gpci/edge_cases/GPCI2025_duplicate_locality_00.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_duplicate_locality_00.txt', TEST_METADATA)
    
    # Should have 3 input rows
    assert result.metrics['total_rows'] == 3, f"Expected 3 input rows, got {result.metrics['total_rows']}"
    
    # With corrected NK (mac, locality, effective_from):
    # Different MACs with locality='00' are UNIQUE (not duplicates)
    # All 3 rows should be valid (no false duplicates)
    assert len(result.data) == 3, \
        f"Expected 3 valid rows (different MACs = different localities), got {len(result.data)}"
    assert len(result.rejects) == 0, \
        f"Expected 0 rejects (no true duplicates), got {len(result.rejects)}"
    
    # Verify locality 00 rows are present (not quarantined as duplicates)
    locality_00_rows = result.data[result.data['locality_code'] == '00']
    assert len(locality_00_rows) == 2, \
        "Both locality 00 rows should be valid (different MACs)"
    
    # Verify they have different MACs (this makes them unique)
    locality_00_macs = set(locality_00_rows['mac'])
    assert len(locality_00_macs) == 2, \
        "Locality 00 rows should have different MACs (not true duplicates)"
