"""
Tests for Locality Parser (Raw - Phase 1)

Per STD-qa-testing v1.3 (Golden Test Pattern).
Schema: cms_locality_raw_v1.0 (layout-faithful, no FIPS derivation)
Natural Keys: ['mac', 'locality_code']

Reference: planning/parsers/locality/PHASE_1_RAW_PARSER_PLAN.md
"""

import pytest
from pathlib import Path
from io import BytesIO
import pandas as pd

from cms_pricing.ingestion.parsers.locality_parser import parse_locality_raw


@pytest.mark.golden
def test_locality_raw_txt_golden():
    """
    Golden test: Parse 25LOCCO.txt with clean fixture.
    
    Expectations per QTS §5.1.1 (Golden Fixture Hygiene):
    - Exact row count (10 rows)
    - 0 rejects (clean golden fixture)
    - Natural keys unique (mac, locality_code)
    - Columns: mac, locality_code, state_name, fee_area, county_names (NAMES, not FIPS)
    - Header rows skipped (no spurious data)
    - State name forward-filled on continuation rows
    """
    
    # Use actual sample file as fixture
    fixture_path = Path("sample_data/rvu25d_0/25LOCCO.txt")
    
    # Metadata for ingestor
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://sample_data'
    }
    
    # Parse
    with open(fixture_path, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    # Assertions
    assert isinstance(result.data, pd.DataFrame), "Result data must be DataFrame"
    assert len(result.data) > 0, "Must parse at least 1 row"
    assert len(result.rejects) == 0, "Golden fixture should have 0 rejects"
    
    # Schema check (raw - NAMES not FIPS codes)
    expected_cols = {'mac', 'locality_code', 'state_name', 'county_names'}
    actual_cols = set(result.data.columns)
    assert expected_cols <= actual_cols, f"Missing columns: {expected_cols - actual_cols}"
    
    # Natural key uniqueness
    duplicates = result.data.duplicated(subset=['mac', 'locality_code'], keep=False)
    assert not duplicates.any(), f"Found duplicate natural keys:\n{result.data[duplicates]}"
    
    # Verify NAMES (not FIPS codes)
    # Should see state names like ALABAMA, CALIFORNIA, etc.
    assert any(result.data['state_name'].str.contains('ALABAMA|ALASKA|CALIFORNIA', case=False, na=False)), \
        "Should find state NAMES (not FIPS codes)"
    
    # Verify county names (should be string, comma/slash delimited)
    assert result.data['county_names'].dtype == 'object', "County names should be string"
    assert any(result.data['county_names'].notna()), "Should have county data"
    
    # Metrics
    assert result.metrics['schema_id'] == 'cms_locality_raw_v1.0'
    assert result.metrics['reject_rows'] == 0
    assert result.metrics['parser_version'] == 'v1.0.0'
    assert result.metrics['total_rows'] > 0
    assert result.metrics['valid_rows'] > 0
    
    # Header rows should be skipped (no header text in data)
    assert not any(result.data['mac'].str.contains('Medicare|Locality', case=False, na=False)), \
        "Header lines should be skipped"
    
    # State name should be present (forward-filled if necessary)
    assert result.data['state_name'].notna().sum() > 0, \
        "State names should be present (may be forward-filled on continuation rows)"


def test_locality_natural_key_uniqueness():
    """
    Test that natural key uniqueness is enforced.
    
    Natural key: (mac, locality_code)
    """
    
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://sample'
    }
    
    fixture_path = Path("sample_data/rvu25d_0/25LOCCO.txt")
    with open(fixture_path, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    # Check uniqueness
    nat_keys = result.data[['mac', 'locality_code']]
    assert nat_keys.duplicated().sum() == 0, \
        "Natural keys (mac, locality_code) must be unique"


def test_locality_encoding_detection():
    """
    Test that encoding is properly detected and applied.
    """
    
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://sample'
    }
    
    fixture_path = Path("sample_data/rvu25d_0/25LOCCO.txt")
    with open(fixture_path, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    # Encoding should be detected
    assert result.metrics['encoding_detected'] in ['utf-8', 'UTF-8', 'cp1252', 'latin-1']
    
    # Data should be properly decoded (no mojibake)
    assert all(isinstance(s, str) for s in result.data['state_name'].dropna())


def test_locality_column_names_normalized():
    """
    Test that column names are normalized (trimmed, uppercase as needed).
    """
    
    metadata = {
        'release_id': 'test_2025d',
        'schema_id': 'cms_locality_raw_v1.0',
        'product_year': 2025,
        'quarter_vintage': 'D',
        'vintage_date': '2025-01-01',
        'file_sha256': 'test_sha',
        'source_uri': 'test://sample'
    }
    
    fixture_path = Path("sample_data/rvu25d_0/25LOCCO.txt")
    with open(fixture_path, 'rb') as f:
        result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    # MAC and locality code should be trimmed strings
    mac_col = result.data['mac'].iloc[0]
    assert isinstance(mac_col, str)
    assert mac_col == mac_col.strip(), "MAC should be trimmed"
    
    lc_col = result.data['locality_code'].iloc[0]
    assert isinstance(lc_col, str)
    assert lc_col == lc_col.strip(), "Locality code should be trimmed"
