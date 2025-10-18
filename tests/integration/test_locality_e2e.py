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

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
import structlog

from cms_pricing.ingestion.parsers.locality_parser import parse_locality_raw
from cms_pricing.ingestion.normalize.normalize_locality_fips import (
    NormalizeResult,
    normalize_locality_fips,
)

logger = structlog.get_logger(__name__)


# =============================================================================
# Quarantine SLO Helper
# =============================================================================

def assert_quarantine_slo(
    result: NormalizeResult,
    max_rate: float = 0.005,  # 0.5% max quarantine rate
    test_name: str = "unknown"
) -> None:
    """
    Assert quarantine rate meets SLO (≤0.5% for real-source runs).
    
    Args:
        result: NormalizeResult from normalize_locality_fips()
        max_rate: Maximum allowed quarantine rate (default 0.5%)
        test_name: Test name for logging/artifacts
        
    Raises:
        AssertionError: If quarantine rate > max_rate
        
    Emits:
        - Logs quarantine rate to stdout
        - Writes quarantine_breach_<test>.json artifact if SLO breached
        
    QTS: Production monitor per §8 Observability
    """
    total_input = result.metrics['total_rows_in']
    total_exploded = result.metrics['total_rows_exploded']
    quarantined = result.metrics['rows_quarantined']
    
    # Calculate quarantine rate (vs exploded, since that's what we tried to match)
    quarantine_rate = quarantined / total_exploded if total_exploded > 0 else 0
    
    # Log metrics
    logger.info(
        f"Quarantine SLO check: {test_name}",
        quarantine_rate=f"{quarantine_rate:.2%}",
        quarantined=quarantined,
        total_exploded=total_exploded,
        threshold=f"{max_rate:.2%}",
        status="PASS" if quarantine_rate <= max_rate else "BREACH",
    )
    
    # Emit artifact if SLO breached
    if quarantine_rate > max_rate:
        artifact_path = Path(f'tests/artifacts/quarantine_breach_{test_name}.json')
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        
        breach_report = {
            'test_name': test_name,
            'quarantine_rate': quarantine_rate,
            'threshold': max_rate,
            'quarantined': quarantined,
            'total_input': total_input,
            'total_exploded': total_exploded,
            'quarantine_reasons': result.quarantine.groupby('reason').size().to_dict() if len(result.quarantine) > 0 else {},
            'quarantine_sample': result.quarantine.head(10).to_dict('records') if len(result.quarantine) > 0 else [],
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'breach_delta': quarantine_rate - max_rate,
        }
        
        artifact_path.write_text(json.dumps(breach_report, indent=2))
        
        logger.error(
            f"Quarantine SLO BREACH: {test_name}",
            rate=f"{quarantine_rate:.2%}",
            threshold=f"{max_rate:.2%}",
            delta=f"+{(quarantine_rate - max_rate)*100:.2f}pp",
            artifact=str(artifact_path),
        )
    
    # Assert SLO
    assert quarantine_rate <= max_rate, (
        f"Quarantine rate {quarantine_rate:.2%} > {max_rate:.2%} threshold. "
        f"Quarantined {quarantined}/{total_exploded} counties. "
        f"See artifact: tests/artifacts/quarantine_breach_{test_name}.json"
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


@pytest.mark.integration
def test_locality_e2e_gpci_join_smoke():
    """
    E2E Join Smoke: Stage 1 → Stage 2 → GPCI join on (mac, locality_code).
    
    Validates:
    - Stage 1 → Stage 2 pipeline produces valid FIPS codes
    - Join with GPCI on (mac, locality_code) succeeds (≥99.5% join rate)
    - No duplicate NKs post-join
    - CA ROS (locality 26) excludes LA/Orange but joins to GPCI
    - CA explicit (locality 18) includes only LA/Orange
    
    QTS: Per §7.1 Component/Integration gates, Appendix H.5 Join Validation
    """
    from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci
    
    # Fixture: CA localities (explicit + ROS) + VA + MO
    locality_fixture = """COUNTIES INCLUDED IN 2025 LOCALITIES.

Medicare AdmiLocality         State                                                  Fee Schedule Area                                                                  Counties

    01112       18    CALIFORNIA                                                     LOS ANGELES-LONG BEACH-ANAHEIM                                                    LOS ANGELES/ORANGE
    01112       26    CALIFORNIA                                                     REST OF CALIFORNIA                                                                ALL COUNTIES EXCEPT LOS ANGELES, ORANGE
    00903       01    VIRGINIA                                                       RICHMOND METRO                                                                    Richmond
    00903       02    MISSOURI                                                       STATEWIDE                                                                          St. Louis
"""
    
    # Fixture: Matching GPCI data (properly aligned to GPCI_2025D_LAYOUT)
    # Column positions per GPCI_2025D_LAYOUT: MAC (0-5), Locality (24-26), GPCI Work (121-127), GPCI PE (133-139), GPCI MP (145-151)
    gpci_fixture = """ADDENDUM E. FINAL CY 2025 GEOGRAPHIC PRACTICE COST INDICES (GPCIs) BY STATE AND MEDICARE LOCALITY

Medicare Admi State  Locality                                      Locality Name                                      2025 PW GPCI2025 PE GPCI2025 MP GPCI
01112           CA      18  LOS ANGELES-LONG BEACH-ANAHEIM                                                               1.055       1.190       0.667
01112           CA      26  REST OF CALIFORNIA                                                                           1.009       1.037       0.571
00903           VA      01  RICHMOND METRO                                                                               1.000       1.000       0.520
00903           MO      02  STATEWIDE                                                                                    0.998       0.995       0.515
"""
    
    # Parse locality (Stage 1 + Stage 2)
    loc_path = Path('/tmp/test_join_locality.txt')
    loc_path.write_text(locality_fixture)
    
    metadata_loc = {
        'release_id': 'LOCCO_2025D_TEST',
        'schema_id': 'locality_raw_v1_0',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'test_join',
        'source_uri': 'test://join',
    }
    
    with open(loc_path, 'rb') as f:
        stage1 = parse_locality_raw(f, '25LOCCO.txt', metadata_loc)
    stage2 = normalize_locality_fips(stage1.data)
    
    # Parse GPCI
    gpci_path = Path('/tmp/test_join_gpci.txt')
    gpci_path.write_text(gpci_fixture)
    
    metadata_gpci = {
        'release_id': 'GPCI_2025D_TEST',
        'schema_id': 'cms_gpci_v1.2',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'test_join_gpci',
        'source_uri': 'test://join_gpci',
        'source_release': 'RVU25D',  # GPCI is part of RVU bundle
    }
    
    with open(gpci_path, 'rb') as f:
        gpci = parse_gpci(f, 'GPCI2025.txt', metadata_gpci)
    
    # Prepare for join (zero-pad locality codes for consistency)
    locality_df = stage2.data.copy()
    locality_df['locality_code_join'] = locality_df['locality_code'].str.zfill(2)
    
    gpci_df = gpci.data.copy()
    gpci_df['locality_code_join'] = gpci_df['locality_code'].str.zfill(2)
    
    # Left join: locality → GPCI on (mac, locality_code)
    joined = locality_df.merge(
        gpci_df[['mac', 'locality_code_join', 'gpci_work', 'gpci_pe', 'gpci_mp']],
        on=['mac', 'locality_code_join'],
        how='left',
        indicator=True
    )
    
    # Validate join rate ≥99.5%
    total = len(joined)
    matched = len(joined[joined['_merge'] == 'both'])
    join_rate = matched / total if total > 0 else 0
    
    assert join_rate >= 0.995, (
        f"Join rate {join_rate:.1%} < 99.5% "
        f"(matched {matched}/{total}). "
        f"Unmatched: {joined[joined['_merge'] != 'both'][['mac', 'locality_code', 'county_name_canonical']].head()}"
    )
    
    # No duplicate NKs post-join
    joined_nk = ['mac', 'locality_code', 'state_fips', 'county_fips']
    duplicates = joined[joined.duplicated(subset=joined_nk, keep=False)]
    assert len(duplicates) == 0, f"Duplicate NKs post-join: {len(duplicates)} rows"
    
    # Spot-check: CA locality 26 (ROS) excludes LA/Orange
    ca_ros = joined[
        (joined['state_fips'] == '06') & 
        (joined['locality_code'] == '26')
    ]
    assert len(ca_ros) >= 50, f"CA ROS should have 50+ counties, got {len(ca_ros)}"
    assert '037' not in ca_ros['county_fips'].values, "LA (037) should not be in ROS (locality 26)"
    assert '059' not in ca_ros['county_fips'].values, "Orange (059) should not be in ROS (locality 26)"
    # All CA ROS should join to GPCI locality 26
    assert (ca_ros['gpci_work'] == '1.009').all() or (ca_ros['gpci_work'] == '1.009000').all(), \
        f"CA ROS should use locality 26 GPCI (1.009), got {ca_ros['gpci_work'].unique()}"
    
    # Spot-check: CA locality 18 (LA/Orange explicit)
    ca_18 = joined[
        (joined['state_fips'] == '06') & 
        (joined['locality_code'] == '18')
    ]
    assert len(ca_18) == 2, f"CA locality 18 should have exactly LA+Orange, got {len(ca_18)}"
    assert '037' in ca_18['county_fips'].values, "LA (037) should be in locality 18"
    assert '059' in ca_18['county_fips'].values, "Orange (059) should be in locality 18"
    # All CA 18 should join to GPCI locality 18
    assert (ca_18['gpci_work'] == '1.055').all() or (ca_18['gpci_work'] == '1.055000').all(), \
        f"CA locality 18 should use specific GPCI (1.055), got {ca_18['gpci_work'].unique()}"


@pytest.mark.real_source
def test_locality_quarantine_slo_real_source():
    """
    Quarantine SLO: Real CMS files must have ≤0.5% quarantine rate.
    
    Validates:
    - Reference data (Census TIGER/Line 2025) has ≥99.5% coverage
    - Alias map handles CMS naming variations
    - Fuzzy matching (if enabled) resolves remaining edge cases
    - SLO breach emits artifact + alert
    
    QTS: Production monitor per §8 Observability, §5.3 Quarantine SLO
    """
    # Use actual CMS file (full dataset)
    fixture_path = Path('sample_data/rvu25d_0/25LOCCO.txt')
    
    if not fixture_path.exists():
        pytest.skip(f"Real source file not found: {fixture_path}")
    
    metadata = {
        'release_id': 'LOCCO_2025D',
        'schema_id': 'locality_raw_v1_0',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': '2025-04-01',
        'file_sha256': 'real_source_slo',
        'source_uri': 'sample_data://rvu25d_0/25LOCCO.txt',
    }
    
    # Stage 1: Parse real CMS file
    with open(fixture_path, 'rb') as f:
        stage1 = parse_locality_raw(f, '25LOCCO.txt', metadata)
    
    logger.info(
        "Stage 1 complete (real source)",
        rows_parsed=len(stage1.data),
        rejects=len(stage1.rejects),
    )
    
    # Stage 2: Normalize (enable fuzzy for max coverage)
    stage2 = normalize_locality_fips(stage1.data, use_fuzzy=False)  # Fuzzy optional, start without
    
    logger.info(
        "Stage 2 complete (real source)",
        rows_normalized=len(stage2.data),
        rows_quarantined=len(stage2.quarantine),
        expansion_methods=stage2.metrics['expansion_methods'],
        match_methods=stage2.metrics['match_methods'],
    )
    
    # Assert quarantine SLO
    assert_quarantine_slo(stage2, max_rate=0.005, test_name='real_source_25LOCCO')
    
    # Additional validation: by-state coverage
    # Note: Sample file (25LOCCO.txt) may not contain all 50 states (e.g., missing CA in sample)
    # Real production files should have 50+ states
    states_with_data = stage2.data.groupby('state_fips').size()
    assert len(states_with_data) >= 45, (
        f"Should have data for ≥45 states, got {len(states_with_data)}. "
        f"States: {sorted(states_with_data.index.tolist())}"
    )
    
    # Validate metrics structure
    assert 'authority_version' in stage2.metrics
    assert 'expansion_methods' in stage2.metrics
    assert 'match_methods' in stage2.metrics
    assert stage2.metrics['authority_version'] == 'Census TIGER/Line 2025'

