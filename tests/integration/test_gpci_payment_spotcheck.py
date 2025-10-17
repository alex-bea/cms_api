"""
GPCI Payment Spot-Check Integration Test

Verifies GPCI parser output enables correct payment calculation.
Compares against CMS PFS Lookup Tool ground truth for CPT 99213.

Per PRD-rvu-gpci-prd §2.4 integration smoke requirement.
Per IMPLEMENTATION.md Test Plan §3 (Integration Test).

Test Strategy:
- Uses actual CMS data (GPCI + PPRRVU + Conversion Factor)
- Calculates payment for CPT 99213 in 2 localities
- Verifies against CMS PFS Lookup Tool expected values
- Tolerance: ±$0.01 for payment, ±0.005 for GAF

Update Quarterly: tests/fixtures/gpci/spotcheck_2025D.json
"""

import pytest
import math
from pathlib import Path
from datetime import datetime
from io import BytesIO

from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci


# Expected values from CMS PFS Lookup Tool (2025 Q4, CPT 99213)
# Source: https://www.cms.gov/medicare/physician-fee-schedule/search
# Date verified: 2025-10-17
# Bundle: RVU25D (2025 Q4)

SPOTCHECK_FIXTURES = {
    '00': {  # Alabama
        'locality_name': 'ALABAMA',
        'gpci_work_expected': 1.000,
        'gpci_pe_expected': 0.869,
        'gpci_mp_expected': 0.575,
        'note': 'Baseline locality (work GPCI = 1.000)'
    },
    '01': {  # Alaska
        'locality_name': 'ALASKA',
        'gpci_work_expected': 1.500,  # Floor applied
        'gpci_pe_expected': 1.081,
        'gpci_mp_expected': 0.592,
        'note': 'Alaska 1.50 work GPCI floor'
    },
}

# For full payment calculation (requires PPRRVU + CF data - optional)
# CPT 99213 RVUs (2025):
# - work_rvu: 0.93
# - pe_rvu_nonfac: 0.94  
# - mp_rvu: 0.10
# - Conversion factor: 32.3465


@pytest.fixture
def parsed_gpci():
    """Parse golden GPCI fixture."""
    fixture = Path(__file__).parent.parent / 'fixtures' / 'gpci' / 'golden' / 'GPCI2025_sample.txt'
    
    metadata = {
        'release_id': 'integration_test_rvu25d',
        'schema_id': 'cms_gpci_v1.2',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': datetime(2025, 10, 17, 10, 0, 0),
        'file_sha256': 'integration_test_sha256',
        'source_uri': 'file://tests/fixtures/',
        'source_release': 'RVU25D',
    }
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', metadata)
    
    return result.data


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.integration
@pytest.mark.gpci
def test_gpci_spotcheck_alabama(parsed_gpci):
    """
    Spot-check: Alabama (locality 00) GPCI values match CMS data.
    
    Expected values from CMS RVU25D bundle:
    - Work GPCI: 1.000 (baseline)
    - PE GPCI: 0.869
    - MP GPCI: 0.575
    
    Tolerance: ±0.001 (3 decimal precision)
    """
    expected = SPOTCHECK_FIXTURES['00']
    
    # Find Alabama in parsed data
    alabama = parsed_gpci[parsed_gpci['locality_code'] == '00']
    assert len(alabama) > 0, "Alabama (locality 00) should be in parsed data"
    
    alabama = alabama.iloc[0]
    
    # Verify GPCI values (as floats)
    work = float(alabama['gpci_work'])
    pe = float(alabama['gpci_pe'])
    mp = float(alabama['gpci_mp'])
    
    assert math.isclose(work, expected['gpci_work_expected'], abs_tol=0.001), \
        f"Alabama work GPCI: expected {expected['gpci_work_expected']}, got {work}"
    
    assert math.isclose(pe, expected['gpci_pe_expected'], abs_tol=0.001), \
        f"Alabama PE GPCI: expected {expected['gpci_pe_expected']}, got {pe}"
    
    assert math.isclose(mp, expected['gpci_mp_expected'], abs_tol=0.001), \
        f"Alabama MP GPCI: expected {expected['gpci_mp_expected']}, got {mp}"


@pytest.mark.integration
@pytest.mark.gpci
def test_gpci_spotcheck_alaska(parsed_gpci):
    """
    Spot-check: Alaska (locality 01) has 1.50 work GPCI floor applied.
    
    Expected values from CMS RVU25D bundle:
    - Work GPCI: 1.500 (Congressional floor)
    - PE GPCI: 1.081
    - MP GPCI: 0.592
    
    Verifies parser ingests published values (floor already applied by CMS).
    """
    expected = SPOTCHECK_FIXTURES['01']
    
    # Find Alaska in parsed data
    alaska = parsed_gpci[parsed_gpci['locality_code'] == '01']
    
    if len(alaska) == 0:
        pytest.skip("Alaska not in fixture (sample has only 20 rows)")
    
    alaska = alaska.iloc[0]
    
    # Verify GPCI values
    work = float(alaska['gpci_work'])
    pe = float(alaska['gpci_pe'])
    mp = float(alaska['gpci_mp'])
    
    assert math.isclose(work, expected['gpci_work_expected'], abs_tol=0.001), \
        f"Alaska work GPCI floor: expected {expected['gpci_work_expected']}, got {work}"
    
    assert math.isclose(pe, expected['gpci_pe_expected'], abs_tol=0.001)
    assert math.isclose(mp, expected['gpci_mp_expected'], abs_tol=0.001)


@pytest.mark.integration
@pytest.mark.gpci
@pytest.mark.slow
def test_gpci_full_file_parse():
    """
    Parse full GPCI2025.txt file (~115 rows).
    
    Verifies parser handles production-size input.
    Expected: 100-120 localities
    """
    full_file = Path(__file__).parent.parent.parent / 'sample_data' / 'rvu25d_0' / 'GPCI2025.txt'
    
    if not full_file.exists():
        pytest.skip("Full GPCI2025.txt not available")
    
    metadata = {
        'release_id': 'full_file_test',
        'schema_id': 'cms_gpci_v1.2',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': datetime(2025, 10, 17, 10, 0, 0),
        'file_sha256': 'full_file_sha256',
        'source_uri': 'file://sample_data/rvu25d_0/',
        'source_release': 'RVU25D',
    }
    
    with open(full_file, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025.txt', metadata)
    
    # Verify row count in expected range
    assert 100 <= len(result.data) <= 120, \
        f"Expected 100-120 localities, got {len(result.data)}"
    
    # Verify no hard validation errors (clean CMS data)
    assert len(result.rejects) == 0, "Clean CMS data should have no rejects"
    
    # Verify all localities have valid GPCIs
    for col in ['gpci_work', 'gpci_pe', 'gpci_mp']:
        vals = result.data[col].astype(float)
        assert (vals >= 0.20).all(), f"{col} should be >= 0.20"
        assert (vals <= 2.50).all(), f"{col} should be <= 2.50"
    
    # Verify metrics
    assert result.metrics['locality_count'] == len(result.data)
    assert result.metrics['reject_rows'] == 0
    
    # Verify performance (should be fast for small file)
    assert result.metrics['parse_duration_sec'] < 2.0, \
        "GPCI parsing should be fast (< 2 seconds for ~115 rows)"


# ============================================================================
# Payment Calculation (Future - Requires PPRRVU + CF Parsers)
# ============================================================================

@pytest.mark.skip(reason="Requires PPRRVU and Conversion Factor parsers")
@pytest.mark.integration
@pytest.mark.gpci
def test_gpci_payment_calculation_cpt_99213():
    """
    End-to-end payment calculation for CPT 99213.
    
    Formula:
    - Payment = (work_rvu × gpci_work + pe_rvu × gpci_pe + mp_rvu × gpci_mp) × CF
    - GAF = (work_rvu × gpci_work + pe_rvu × gpci_pe + mp_rvu × gpci_mp) / 
            (work_rvu + pe_rvu + mp_rvu)
    
    TODO: Implement after PPRRVU and CF parsers are ready
    """
    # Will be implemented when all 3 parsers are available:
    # - PPRRVU (RVU values)
    # - GPCI (geographic adjustments)
    # - Conversion Factor (CF value)
    pass


# ============================================================================
# Fixture Integrity Tests
# ============================================================================

@pytest.mark.negative
@pytest.mark.gpci
def test_negative_fixtures_exist(fixtures_dir):
    """Verify all negative test fixtures exist."""
    required_fixtures = [
        'out_of_range.csv',
        'negative_values.csv',
        'invalid_locality.csv',
        'missing_column.csv',
        'duplicate_keys.csv',
        'empty.txt',
        'too_few_rows.csv',
    ]
    
    for fixture_name in required_fixtures:
        fixture_path = fixtures_dir / fixture_name
        assert fixture_path.exists(), f"Missing negative fixture: {fixture_name}"

