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

from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci, PARSER_VERSION


# Test metadata (shared across all golden tests)
TEST_METADATA = {
    'release_id': 'test_rvu25d_20251017',
    'schema_id': 'cms_gpci_v1.2',
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
    required_cols = ['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 
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
    assert result.metrics['schema_id'] == 'cms_gpci_v1.2'
    assert 'gpci_value_stats' in result.metrics
    
    # Verify sorted by natural keys
    assert result.data['locality_code'].is_monotonic_increasing or \
           (result.data['locality_code'] == result.data['locality_code'].shift()).any(), \
           "Data should be sorted by natural keys"


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
    
    # Verify schema compliance
    assert 'locality_code' in result.data.columns
    assert 'gpci_work' in result.data.columns
    assert 'row_content_hash' in result.data.columns
    
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
    
    # Verify row count (fixture has ~113 rows, many duplicates from multiple quarters)
    # Note: XLSX has full dataset, not just a sample like CSV/TXT
    # Natural key is (locality_code, effective_from), so multiple quarters → duplicates
    assert len(result.data) >= 20, \
        f"Expected at least 20 unique localities, got {len(result.data)}"
    assert result.metrics['total_rows'] >= 100, "XLSX should have full dataset"
    
    # Large number of duplicates expected (multiple quarters in file)
    assert len(result.rejects) > 0, "XLSX has duplicate quarters, should have rejects"
    
    # Verify schema compliance
    assert 'locality_code' in result.data.columns
    assert 'gpci_work' in result.data.columns
    assert 'row_content_hash' in result.data.columns
    
    # Verify all GPCI values are valid
    gpci_work = result.data['gpci_work'].astype(float)
    gpci_pe = result.data['gpci_pe'].astype(float)
    gpci_mp = result.data['gpci_mp'].astype(float)
    
    assert (gpci_work >= 0.20).all() and (gpci_work <= 2.50).all()
    assert (gpci_pe >= 0.20).all() and (gpci_pe <= 2.50).all()
    assert (gpci_mp >= 0.20).all() and (gpci_mp <= 2.50).all()
    
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
    core_cols = ['locality_code', 'gpci_work', 'gpci_pe', 'gpci_mp', 'effective_from']
    for col in core_cols:
        assert result1.data[col].equals(result2.data[col]), \
            f"Column {col} must be identical across runs"


@pytest.mark.golden
@pytest.mark.gpci
def test_gpci_schema_v1_2_compliance(fixtures_dir):
    """
    Verify parser output complies with cms_gpci_v1.2 schema contract.
    
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
    Verify output is sorted by natural keys: ['locality_code', 'effective_from'].
    
    Per STD-parser-contracts §5.2 (Deterministic Output).
    """
    fixture = fixtures_dir / 'GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', TEST_METADATA)
    
    # Verify sorted by locality_code
    locality_codes = result.data['locality_code'].tolist()
    sorted_codes = sorted(locality_codes)
    assert locality_codes == sorted_codes, \
        "Data must be sorted by locality_code (natural key)"
    
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
    
    # Verify GPCI values match exactly for Alaska (locality 01)
    txt_ak = txt_result.data[txt_result.data['locality_code'] == '01'].iloc[0]
    csv_ak = csv_result.data[csv_result.data['locality_code'] == '01'].iloc[0]
    
    assert txt_ak['gpci_work'] == csv_ak['gpci_work'] == '1.500'
    assert txt_ak['gpci_pe'] == csv_ak['gpci_pe'] == '1.081'
    assert txt_ak['gpci_mp'] == csv_ak['gpci_mp'] == '0.592'


@pytest.mark.edge_case
@pytest.mark.gpci
def test_gpci_real_cms_duplicate_locality_00(fixtures_dir):
    """
    Edge case: Real CMS quirk where AL and AZ both use locality 00.
    
    Tests duplicate natural key handling with authentic CMS data.
    Per STD-qa-testing-prd §2.2 (negative testing requirements).
    
    Fixture: GPCI2025_duplicate_locality_00.txt
    Expected: Duplicate detection and quarantine (WARN severity)
    """
    from pathlib import Path
    
    fixture = Path(__file__).parent.parent / 'fixtures/gpci/edge_cases/GPCI2025_duplicate_locality_00.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_duplicate_locality_00.txt', TEST_METADATA)
    
    # Should have 3 input rows
    assert result.metrics['total_rows'] == 3, f"Expected 3 input rows, got {result.metrics['total_rows']}"
    
    # Should detect BOTH locality 00 rows as duplicates (Alabama AND Arizona)
    # Natural key is (locality_code, effective_from), so both 00s have same key
    assert len(result.rejects) == 2, f"Should quarantine both locality 00 rows, got {len(result.rejects)}"
    assert len(result.data) == 1, f"Should keep only Alaska (unique key), got {len(result.data)}"
    
    # Verify both locality 00s were quarantined (none in valid data)
    locality_00_rows = result.data[result.data['locality_code'] == '00']
    assert len(locality_00_rows) == 0, "Both locality 00 rows should be quarantined"
    
    # Alaska (unique key) should be the only valid row
    assert result.data.iloc[0]['locality_code'] == '01', "Alaska should be only valid row"
    assert result.data.iloc[0]['gpci_work'] == '1.500'

