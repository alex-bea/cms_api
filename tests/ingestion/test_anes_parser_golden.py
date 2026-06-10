"""
ANES Parser - Golden File Tests

Tests deterministic parsing across all supported formats (TXT, CSV, ZIP).

Per STD-qa-testing-prd-v1.0 §5.1.1 (Golden Fixture Hygiene).
Per QTS §G.1-G.5 (Parser Testing Patterns).
Per STD-parser-contracts v2.0 §21.2 (Golden Tests).
"""

import pytest
from pathlib import Path
from datetime import datetime
from io import BytesIO

import pandas as pd
import zipfile
from textwrap import dedent

from cms_pricing.ingestion.parsers.anes_parser import parse_anes, PARSER_VERSION


# Test metadata (shared across all golden tests)
TEST_METADATA = {
    'release_id': 'test_anes25d_20251020',
    'schema_id': 'cms_anescf_v1.1',
    'product_year': '2025',
    'quarter_vintage': 'D',
    'vintage_date': datetime(2025, 10, 1),
    'file_sha256': 'test_sha256',
    'source_uri': 'test://anes_golden',
    'source_release': 'RVU25D',
}


# =============================================================================
# Metadata & Structure Tests
# =============================================================================

@pytest.mark.golden
def test_anes_metadata_injection():
    """
    Test metadata fields injected into every row.
    
    QTS: Per §G.2 Metrics Structure Testing
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Verify metadata columns exist
    metadata_cols = [
        'release_id', 'vintage_date', 'product_year', 'quarter_vintage',
        'source_filename', 'source_file_sha256', 'source_uri', 
        'source_release', 'source_inner_file', 'parsed_at'
    ]
    for col in metadata_cols:
        assert col in result.data.columns, f"Missing metadata column: {col}"
    
    # Verify metadata values match input
    assert (result.data['release_id'] == TEST_METADATA['release_id']).all()
    assert (result.data['product_year'] == '2025').all()
    assert (result.data['quarter_vintage'] == 'D').all()


@pytest.mark.golden
def test_anes_row_hashing():
    """
    Test SHA-256 row content hashing (deterministic, excludes metadata).
    
    QTS: Per §G.3 Rejects Structure, STD-parser-contracts §5.2
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Verify hash column exists
    assert 'row_content_hash' in result.data.columns
    
    # Verify all hashes are 64-char hex (SHA-256)
    assert result.data['row_content_hash'].str.match(r'^[a-f0-9]{64}$').all()
    
    # Verify hashes are unique (different rows = different hashes)
    assert result.data['row_content_hash'].nunique() == len(result.data)
    
    # Verify determinism: parse same file twice → same hashes
    with open(fixture_path, 'rb') as f:
        result2 = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Sort both by NK before comparing
    df1 = result.data.sort_values(['mac', 'locality_code']).reset_index(drop=True)
    df2 = result2.data.sort_values(['mac', 'locality_code']).reset_index(drop=True)
    
    assert (df1['row_content_hash'] == df2['row_content_hash']).all(), \
           "Hashes should be deterministic across runs"


@pytest.mark.golden
def test_anes_natural_key_sort():
    """
    Test data sorted by natural keys: ['mac', 'locality_code', 'effective_from'].
    
    QTS: Per STD-parser-contracts §6.1
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Verify sorted by natural keys: ['mac', 'locality_code', 'effective_from']
    sorted_df = result.data.sort_values(['mac', 'locality_code', 'effective_from']).reset_index(drop=True)
    result_reset = result.data.reset_index(drop=True)
    assert result_reset.equals(sorted_df), \
           "Data should be sorted by natural keys ['mac', 'locality_code', 'effective_from']"


@pytest.mark.golden
def test_anes_schema_compliance():
    """
    Test output matches cms_anescf_v1.1 schema contract.
    
    QTS: Per STD-parser-contracts §4.1
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Required columns per schema v1.1
    required_cols = [
        'mac', 'locality_code', 'anesthesia_cf_usd',
        'effective_from', 'effective_to', 'row_content_hash'
    ]
    for col in required_cols:
        assert col in result.data.columns, f"Missing required column: {col}"
    
    # Schema v1.1 does NOT have state_fips (removed from v1.0)
    assert 'state_fips' not in result.data.columns, \
           "state_fips should not be present (not in ANES layout)"


# =============================================================================
# Format-Specific Golden Tests (QTS §5.1.1 - Clean Fixtures)
# =============================================================================

@pytest.mark.golden
def test_anes_golden_txt():
    """
    Test TXT fixed-width parsing (ANES_2025D_LAYOUT).
    
    QTS §5.1.1: Golden fixtures have 0 rejects (clean data)
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # QTS §5.1.1 REQUIRED: Golden fixtures have 0 rejects
    assert len(result.rejects) == 0, "Golden fixture should have no rejects"
    
    # Verify exact count (determinism)
    assert len(result.data) == 20, f"Expected 20 rows, got {len(result.data)}"
    
    # Verify first row (spot check)
    row0 = result.data.iloc[0]
    assert row0['mac'] == '01112'  # First after sort
    assert row0['locality_code'] in ['05', '09', '51', '52']  # CA localities
    
    # Verify parser version in metrics
    assert result.metrics['parser_version'] == PARSER_VERSION


@pytest.mark.golden
def test_anes_golden_csv():
    """
    Test CSV parsing with header normalization.
    
    QTS §5.1.1: Golden fixtures have 0 rejects
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.csv')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.csv', TEST_METADATA)
    
    # QTS §5.1.1 REQUIRED: Golden fixtures have 0 rejects
    assert len(result.rejects) == 0, "Golden fixture should have no rejects"
    
    # Verify exact count
    assert len(result.data) == 20, f"Expected 20 rows, got {len(result.data)}"
    
    # CSV has already-scaled values (19.31 not 1931)
    # Parser should detect and handle
    assert result.data['anesthesia_cf_usd'].notna().all()


@pytest.mark.golden
def test_anes_reject_handling():
    """
    Test rejects DataFrame structure and validation context.
    
    QTS §G.3: Rejects Structure Testing
    """
    # Use negative fixture to generate rejects
    fixture_path = Path('tests/fixtures/anes/negatives/out_of_range.csv')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'out_of_range.csv', TEST_METADATA)
    
    # Should have rejects (CF = 15000 cents = $150 > $100 max)
    assert len(result.rejects) > 0, "Should have rejects for out-of-range CF"
    
    # QTS §G.3 REQUIRED: Rejects structure
    assert 'validation_error' in result.rejects.columns
    assert 'validation_severity' in result.rejects.columns
    assert 'validation_rule' in result.rejects.columns
    
    # Verify validation details
    reject_row = result.rejects.iloc[0]
    assert reject_row['validation_severity'] in ['HARD', 'WARN']
    assert 'range' in reject_row['validation_error'].lower()


# =============================================================================
# Format Consistency Tests (QTS §5.1.2 - Multi-Format Parity)
# =============================================================================

@pytest.mark.golden
def test_anes_consistency_txt_csv():
    """
    Test TXT and CSV produce consistent results.
    
    QTS §5.1.2: Multi-format fixtures have identical data
    Note: CSV values are pre-scaled (19.31), TXT values are cents (1931)
    """
    txt_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    csv_path = Path('tests/fixtures/anes/golden/ANES2025_sample.csv')
    
    with open(txt_path, 'rb') as f:
        txt_result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    with open(csv_path, 'rb') as f:
        csv_result = parse_anes(f, 'ANES2025_sample.csv', TEST_METADATA)
    
    # Same row count
    assert len(txt_result.data) == len(csv_result.data), "TXT and CSV should have same row count"
    
    # Filter to shared keys for comparison (may have different MACs due to fixture creation)
    txt_keys = set(zip(txt_result.data['mac'], txt_result.data['locality_code']))
    csv_keys = set(zip(csv_result.data['mac'], csv_result.data['locality_code']))
    shared_keys = txt_keys & csv_keys
    
    assert len(shared_keys) >= 10, "Should have at least 10 shared localities for comparison"
    
    # Compare CF values for shared keys (should match after scaling)
    for mac, loc in list(shared_keys)[:5]:  # Spot check first 5
        txt_row = txt_result.data[(txt_result.data['mac'] == mac) & 
                                   (txt_result.data['locality_code'] == loc)]
        csv_row = csv_result.data[(csv_result.data['mac'] == mac) & 
                                   (csv_result.data['locality_code'] == loc)]
        
        if len(txt_row) > 0 and len(csv_row) > 0:
            txt_cf = pd.to_numeric(txt_row['anesthesia_cf_usd'].iloc[0])
            csv_cf = pd.to_numeric(csv_row['anesthesia_cf_usd'].iloc[0])
            
            # Allow small floating point difference
            assert abs(txt_cf - csv_cf) < 0.01, \
                   f"CF mismatch for {mac}-{loc}: TXT={txt_cf}, CSV={csv_cf}"


@pytest.mark.golden
def test_anes_zip_prefers_anes_payload():
    """
    Ensure ZIP archives with multiple CMS files select the ANES member.
    """
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr(
            '25LOCCO.csv',
            dedent(
                """\
                Contractor,Locality,Locality Name
                10112,00,ALABAMA
                """
            )
        )
        zf.writestr(
            'ANES2025.csv',
            dedent(
                """\
                Contractor,Locality,Locality Name,National Anes CF of 20.3178
                10112,00,ALABAMA,19.31
                02102,01,ALASKA,27.86
                """
            )
        )

    zip_buffer.seek(0)
    result = parse_anes(zip_buffer, 'RVU25_bundle.zip', TEST_METADATA)

    assert not result.data.empty, "Expected ANES rows from ZIP bundle"
    assert 'mac' in result.data.columns
    assert 'locality_code' in result.data.columns
    macs = set(result.data['mac'])
    assert '10112' in macs and '02102' in macs, "Expected ANES MAC codes parsed from bundle"


# =============================================================================
# Business Logic Tests (ANES-Specific)
# =============================================================================

@pytest.mark.golden
def test_anes_units_scaling_from_txt():
    """
    Test units scaling: raw cents (1931) → USD ($19.31).
    
    QTS: Per plan requirement - NEW test for scaling logic
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # TXT file has raw cents, parser should scale to USD
    # Example: 1931 → $19.31, 2786 → $27.86
    
    # Verify all CF values in typical USD range (not cents)
    cf_values = pd.to_numeric(result.data['anesthesia_cf_usd'], errors='coerce')
    
    # If scaling worked: values should be $15-35 range (typical)
    # If scaling failed: values would be 1900-2800 range (cents)
    assert cf_values.min() >= 10.0, "Min CF should be > $10 (scaled, not cents)"
    assert cf_values.max() <= 50.0, "Max CF should be < $50 (scaled, not thousands of cents)"
    
    # Verify precision: 2 decimal places
    # All values should be .XX format (cents precision)
    for cf in cf_values.head(5):
        # Check that value has at most 2 decimal places
        cf_str = f"{cf:.10f}".rstrip('0').rstrip('.')
        decimal_part = cf_str.split('.')[-1] if '.' in cf_str else ''
        assert len(decimal_part) <= 2, f"CF {cf} should have ≤2 decimal places"


@pytest.mark.golden
def test_anes_effective_dates_from_metadata():
    """
    Test effective_from/to derived from filename (ANES2025).
    
    QTS: Per plan requirement - NEW test for date derivation logic
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Verify effective_from derived from filename "ANES2025" → 2025-01-01
    assert 'effective_from' in result.data.columns
    first_date = result.data['effective_from'].iloc[0]
    assert pd.to_datetime(first_date) == pd.to_datetime('2025-01-01'), \
           f"effective_from should be 2025-01-01, got {first_date}"
    
    # All rows should have same effective_from (same file)
    assert result.data['effective_from'].nunique() == 1
    
    # effective_to should be 2025-12-31 or NaT (depending on current year)
    assert 'effective_to' in result.data.columns
    # Since we're in 2025, effective_to may be NaT or 2025-12-31


@pytest.mark.golden
def test_anes_duplicate_locality_00():
    """
    Test that different MACs with same locality (00) are unique, not duplicates.
    
    QTS §5.1.1: Tests NK correctness (mac disambiguates localities)
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Check if fixture has multiple MACs with locality 00
    loc_00 = result.data[result.data['locality_code'] == '00']
    
    if len(loc_00) > 1:
        # Different MACs should NOT be duplicates (NK includes MAC)
        unique_macs = loc_00['mac'].nunique()
        assert unique_macs == len(loc_00), \
               f"Each MAC+locality should be unique. Found {len(loc_00)} rows but {unique_macs} unique MACs"
        
        # All should be in valid data, none in rejects
        assert len(result.rejects) == 0, "Different MACs = unique rows, not duplicates"


# =============================================================================
# Validation Tests (QTS §G.4 - String/Numeric Pattern)
# =============================================================================

@pytest.mark.golden
def test_anes_cf_range_validation():
    """
    Test CF range validation (AFTER scaling to USD).
    
    QTS §G.4: Use pd.to_numeric for range checks (canonicalize returns strings)
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Convert canonicalized strings to numeric for validation
    cf_numeric = pd.to_numeric(result.data['anesthesia_cf_usd'], errors='coerce')
    
    # All values should be positive
    assert (cf_numeric > 0).all(), "All CF values should be positive"
    
    # All values should be in reasonable range
    assert (cf_numeric >= 0.01).all(), "CF values should be >= $0.01"
    assert (cf_numeric <= 100.0).all(), "CF values should be <= $100.00"
    
    # Typical range: $15-35 (most should be in WARN band)
    typical_count = ((cf_numeric >= 15.0) & (cf_numeric <= 35.0)).sum()
    assert typical_count >= len(result.data) * 0.8, \
           "At least 80% of golden data should be in typical range [$15-35]"


# =============================================================================
# Metrics Tests (QTS §G.2)
# =============================================================================

@pytest.mark.golden
def test_anes_metrics_structure():
    """
    Test metrics structure and required keys.
    
    QTS §G.2: Metrics Structure Testing
    """
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # Required metrics keys
    assert 'total_rows' in result.metrics
    assert 'valid_rows' in result.metrics
    assert 'reject_rows' in result.metrics
    assert 'parse_duration_sec' in result.metrics
    assert 'parser_version' in result.metrics
    assert 'encoding_detected' in result.metrics
    
    # ANES-specific metrics
    assert 'cf_value_stats' in result.metrics
    assert 'locality_count' in result.metrics
    
    # Verify CF value stats structure
    cf_stats = result.metrics['cf_value_stats']
    assert 'min' in cf_stats
    assert 'max' in cf_stats
    assert 'mean' in cf_stats
    assert 'median' in cf_stats
    
    # Sanity check CF stats (scaled values)
    assert cf_stats['min'] >= 15.0, "Min CF should be in USD (≥$15)"
    assert cf_stats['max'] <= 35.0, "Max CF should be in typical range (≤$35)"


# =============================================================================
# Edge Case Tests
# =============================================================================

@pytest.mark.edge_case
def test_anes_parser_version_tracked():
    """Test parser version tracked in metrics."""
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    assert result.metrics['parser_version'] == PARSER_VERSION
    assert PARSER_VERSION.startswith('v1.')


@pytest.mark.edge_case
def test_anes_encoding_detection():
    """Test encoding detection logs detected encoding."""
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    assert 'encoding_detected' in result.metrics
    assert result.metrics['encoding_detected'] in ['utf-8', 'ascii', 'latin-1', 'windows-1252']


# =============================================================================
# Date Derivation Tests
# =============================================================================

@pytest.mark.golden
def test_anes_date_from_filename_2025():
    """Test date extraction from ANES2025.txt → 2025-01-01."""
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.txt')
    
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES2025_sample.txt', TEST_METADATA)
    
    # All rows should have effective_from = 2025-01-01
    assert (result.data['effective_from'] == pd.to_datetime('2025-01-01')).all()


@pytest.mark.golden
def test_anes_date_from_filename_ANES25D():
    """Test date extraction from ANES25D.csv → 2025-01-01 (2-digit year)."""
    fixture_path = Path('tests/fixtures/anes/golden/ANES2025_sample.csv')
    
    # Rename to ANES25D pattern
    with open(fixture_path, 'rb') as f:
        result = parse_anes(f, 'ANES25D.csv', TEST_METADATA)
    
    # Should still extract 2025 from filename pattern
    assert (result.data['effective_from'] == pd.to_datetime('2025-01-01')).all()


def _anes_2026_fixed_width_line(
    mac: str,
    locality: str,
    name: str,
    qualifying_cents: str,
    non_qualifying_cents: str,
) -> str:
    return (
        f"{mac:<12}"
        f"{locality:<6}"
        f"{name:<49}"
        f"{qualifying_cents:>4}"
        "  "
        f"{non_qualifying_cents:>4}"
    )


@pytest.mark.edge_case
def test_anes_2026_txt_uses_non_qualifying_apm_cf():
    """Current CMS 2026 ANES TXT includes qualifying and non-qualifying CFs."""
    metadata = {
        **TEST_METADATA,
        "release_id": "test_anes26c",
        "product_year": "2026",
        "quarter_vintage": "C",
        "vintage_date": datetime(2026, 7, 1),
        "source_release": "RVU26C",
    }
    content = "\n".join(
        [
            _anes_2026_fixed_width_line("10112", "00", "ALABAMA", "1963", "1953"),
            _anes_2026_fixed_width_line("02102", "01", "ALASKA", "2829", "2815"),
        ]
    )

    result = parse_anes(BytesIO(content.encode("utf-8")), "ANES2026.txt", metadata)

    assert len(result.data) == 2
    alabama = result.data[result.data["mac"] == "10112"].iloc[0]
    alaska = result.data[result.data["mac"] == "02102"].iloc[0]
    assert alabama["anesthesia_cf_usd"] == 19.53
    assert alaska["anesthesia_cf_usd"] == 28.15
    assert pd.to_datetime(alabama["effective_from"]) == pd.Timestamp("2026-01-01")


@pytest.mark.edge_case
def test_anes_2026_csv_uses_non_qualifying_apm_cf_header():
    """Current CMS 2026 ANES CSV has separate qualifying/non-qualifying columns."""
    metadata = {
        **TEST_METADATA,
        "release_id": "test_anes26c_csv",
        "product_year": "2026",
        "quarter_vintage": "C",
        "vintage_date": datetime(2026, 7, 1),
        "source_release": "RVU26C",
    }
    content = (
        "Contractor,Locality,Locality Name,"
        "Qualifying APM National Anes CF (with 2.5% statutory increase) of 20.599835,"
        "Non-Qualifying APM National Anes CF (with 2.5% Statutory increase)  of 20.49754\n"
        "10112 ,00 ,ALABAMA,19.63,19.53\n"
        "02102 ,01 ,ALASKA*,28.29,28.15\n"
    )

    result = parse_anes(BytesIO(content.encode("utf-8")), "ANES2026.csv", metadata)

    assert len(result.data) == 2
    assert set(result.data["anesthesia_cf_usd"]) == {19.53, 28.15}
    assert "anesthesia_cf_qp_raw" in result.data.columns
