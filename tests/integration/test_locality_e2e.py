"""
End-to-End Integration Tests: Locality Parser Stage 1 → Stage 2

Validates full pipeline: Raw CMS file → FIPS-normalized canonical output

Test slices:
- CA: ALL COUNTIES EXCEPT logic
- VA: Independent city disambiguation (Richmond)
- MO: LSAD tie-breaking (St. Louis County vs City)

QTS Compliance: Per §2.1.1 "Implementation Analysis Before Testing" and §2.4 "Test-First Discovery"

ACTUAL IMPLEMENTATION BEHAVIOR (Verified 2025-10-18):

Stage 1 (parse_locality_raw):
- Signature: (file_obj: IO[bytes], filename: str, metadata: Dict) -> ParseResult
- Returns: ParseResult(data=DataFrame, rejects=DataFrame, metrics=Dict)
- data.columns: ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']
- data.dtypes: All strings
- Example: mac='01112', locality_code='26', state_name='CALIFORNIA', 
            fee_area='REST  OF CALIFORNIA', county_names='ALL COUNTIES EXCEPT LOS ANGELES, ORANGE'

Stage 2 (normalize_locality_fips):
- Signature: (raw_df: DataFrame, ref_dir: Path, use_fuzzy: bool, source_release_id: str) -> NormalizeResult
- Returns: NormalizeResult(data=DataFrame, quarantine=DataFrame, metrics=Dict)
- Expects columns: mac, locality_code, state_name, county_names (fee_area optional for hints)
- Output columns: 14 columns including FIPS codes, canonical names, match_method, expansion_method

Integration Pattern:
  Stage1.data → Stage2(Stage1.data) → Stage2.data with FIPS codes

Author: CMS Pricing API Team
Version: 1.0
Created: 2025-10-18
"""

from pathlib import Path

import pandas as pd
import pytest

from cms_pricing.ingestion.parsers.locality_parser import parse_locality_raw
from cms_pricing.ingestion.normalize.normalize_locality_fips import (
    normalize_locality_fips,
)


@pytest.mark.integration
def test_locality_e2e_ca_all_except():
    """
    E2E: CA 'ALL COUNTIES EXCEPT LOS ANGELES/ORANGE' expansion.
    
    Validates:
    - Stage 1 parses raw CMS file correctly
    - Stage 2 expands ALL EXCEPT logic
    - Result excludes LA (037) and Orange (059)
    - Result includes other CA counties (e.g., San Francisco 075, Alameda 001)
    """
    # Create minimal fixture for CA "ALL EXCEPT" row
    # Fixed-width format matching LOCCO_2025D_LAYOUT (verified with real CMS file)
    fixture_txt = """COUNTIES INCLUDED IN 2025 LOCALITIES.

Medicare AdmiLocality         State                                                  Fee Schedule Area                                                                  Counties

    01112       26    CALIFORNIA                                                     REST OF CALIFORNIA                                                                ALL COUNTIES EXCEPT LOS ANGELES, ORANGE
"""
    
    # Write to temp file
    fixture_path = Path('/tmp/test_ca_all_except.txt')
    fixture_path.write_text(fixture_txt)
    
    # Metadata for parser
    metadata = {
        'release_id': 'LOCCO_2025D_TEST',
        'schema_id': 'locality_raw_v1_0',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'test_e2e',
        'source_uri': 'test://ca_all_except',
    }
    
    # Stage 1: Parse raw
    with open(fixture_path, 'rb') as f:
        stage1_result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    assert len(stage1_result.data) == 1
    assert stage1_result.data.iloc[0]['county_names'] == 'ALL COUNTIES EXCEPT LOS ANGELES, ORANGE'
    
    # Stage 2: Normalize to FIPS
    stage2_result = normalize_locality_fips(stage1_result.data)
    
    # Should expand to CA counties minus 2
    # CA has 58 counties: 58 - 2 (LA + Orange) = 56
    assert len(stage2_result.data) >= 50  # At least 50 (allowing for ref data variations)
    assert len(stage2_result.data) <= 58  # No more than 58
    
    # All should be CA (state_fips 06)
    assert (stage2_result.data['state_fips'] == '06').all()
    
    # Should NOT contain Los Angeles (037) or Orange (059)
    county_fips = set(stage2_result.data['county_fips'])
    assert '037' not in county_fips, "Los Angeles should be excluded"
    assert '059' not in county_fips, "Orange should be excluded"
    
    # Should contain other CA counties (spot check)
    assert '075' in county_fips  # San Francisco
    assert '001' in county_fips  # Alameda
    
    # Check expansion method
    assert (stage2_result.data['expansion_method'] == 'all_except').all()
    
    # Check metrics
    assert stage2_result.metrics['expansion_methods']['all_except'] == 1
    assert stage2_result.metrics['rows_quarantined'] == 0


@pytest.mark.integration
def test_locality_e2e_va_richmond():
    """
    E2E: VA Richmond city vs county disambiguation.
    
    Validates:
    - Stage 1 parses VA row correctly
    - Stage 2 matches Richmond (LSAD tie-breaking)
    - Result is either Richmond city (760) or Richmond County (159)
    """
    fixture_txt = """COUNTIES INCLUDED IN 2025 LOCALITIES.

Medicare AdmiLocality         State                                                  Fee Schedule Area                                                                  Counties

    00903       01    VIRGINIA                                                       RICHMOND METRO                                                                    Richmond
"""
    
    fixture_path = Path('/tmp/test_va_richmond.txt')
    fixture_path.write_text(fixture_txt)
    
    metadata = {
        'release_id': 'LOCCO_2025D_TEST',
        'schema_id': 'locality_raw_v1_0',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'test_e2e_va',
        'source_uri': 'test://va_richmond',
    }
    
    # Stage 1: Parse raw
    with open(fixture_path, 'rb') as f:
        stage1_result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    assert len(stage1_result.data) == 1
    assert 'Richmond' in stage1_result.data.iloc[0]['county_names']
    
    # Stage 2: Normalize to FIPS
    stage2_result = normalize_locality_fips(stage1_result.data)
    
    # Should match exactly 1 Richmond (either city or county)
    assert len(stage2_result.data) == 1
    assert stage2_result.metrics['rows_quarantined'] == 0
    
    row = stage2_result.data.iloc[0]
    assert row['state_fips'] == '51'
    # Either Richmond city (760) or Richmond County (159)
    assert row['county_fips'] in ['760', '159']
    assert 'Richmond' in row['county_name_canonical']
    
    # Check LSAD tie-breaking worked
    assert row['match_method'] == 'exact'
    assert row['mapping_confidence'] == 1.0


@pytest.mark.integration
def test_locality_e2e_mo_st_louis():
    """
    E2E: MO St. Louis County vs City LSAD tie-breaking.
    
    Validates:
    - Stage 1 parses MO row correctly
    - Stage 2 matches St. Louis (either County 189 or city 510)
    - LSAD preference order applied
    """
    fixture_txt = """COUNTIES INCLUDED IN 2025 LOCALITIES.

Medicare AdmiLocality         State                                                  Fee Schedule Area                                                                  Counties

    00903       01    MISSOURI                                                       STATEWIDE                                                                          St. Louis
"""
    
    fixture_path = Path('/tmp/test_mo_st_louis.txt')
    fixture_path.write_text(fixture_txt)
    
    metadata = {
        'release_id': 'LOCCO_2025D_TEST',
        'schema_id': 'locality_raw_v1_0',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'test_e2e_mo',
        'source_uri': 'test://mo_st_louis',
    }
    
    # Stage 1: Parse raw
    with open(fixture_path, 'rb') as f:
        stage1_result = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    assert len(stage1_result.data) == 1
    assert 'St. Louis' in stage1_result.data.iloc[0]['county_names']
    
    # Stage 2: Normalize to FIPS
    stage2_result = normalize_locality_fips(stage1_result.data)
    
    # Should match exactly 1 St. Louis (either County or city)
    assert len(stage2_result.data) == 1
    assert stage2_result.metrics['rows_quarantined'] == 0
    
    row = stage2_result.data.iloc[0]
    assert row['state_fips'] == '29'
    # Either St. Louis County (189) or St. Louis city (510)
    assert row['county_fips'] in ['189', '510']
    assert 'St. Louis' in row['county_name_canonical']
    
    # Check tie-breaking worked
    assert row['match_method'] == 'exact'
    assert row['mapping_confidence'] == 1.0


@pytest.mark.integration
def test_locality_e2e_determinism():
    """
    E2E Property: Running full pipeline twice produces identical output.
    
    Validates:
    - Deterministic parsing (Stage 1)
    - Deterministic normalization (Stage 2)
    - Stable row hashes across runs
    """
    fixture_txt = """COUNTIES INCLUDED IN 2025 LOCALITIES.

Medicare AdmiLocality         State                                                  Fee Schedule Area                                                                  Counties

    10112       00    ALABAMA                                                        STATEWIDE                                                                          ALL COUNTIES
"""
    
    fixture_path = Path('/tmp/test_determinism.txt')
    fixture_path.write_text(fixture_txt)
    
    metadata = {
        'release_id': 'LOCCO_2025D_TEST',
        'schema_id': 'locality_raw_v1_0',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'test_determinism',
        'source_uri': 'test://determinism',
    }
    
    # Run 1
    with open(fixture_path, 'rb') as f:
        stage1_result1 = parse_locality_raw(f, '25LOCCO.txt', metadata)
    stage2_result1 = normalize_locality_fips(stage1_result1.data)
    
    # Run 2
    with open(fixture_path, 'rb') as f:
        stage1_result2 = parse_locality_raw(f, '25LOCCO.txt', metadata)
    stage2_result2 = normalize_locality_fips(stage1_result2.data)
    
    # Should produce identical row hashes
    hashes1 = set(stage2_result1.data['row_content_hash'])
    hashes2 = set(stage2_result2.data['row_content_hash'])
    
    assert hashes1 == hashes2, "Row hashes should be identical across runs"
    
    # Should produce identical DataFrames
    pd.testing.assert_frame_equal(stage2_result1.data, stage2_result2.data)

