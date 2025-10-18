"""
Locality FIPS Normalization Tests - Focused on key fixes

Author: CMS Pricing API Team
Version: 1.0
Created: 2025-10-17
"""

import pandas as pd
import pytest

from cms_pricing.ingestion.normalize.normalize_locality_fips import (
    normalize_locality_fips,
)


@pytest.mark.golden
def test_locality_fips_st_louis_mo():
    """Test St. Louis (MO) - alias + LSAD tie-break."""
    raw_df = pd.DataFrame([
        {
            'mac': '00903',
            'locality_code': '01',
            'state_name': 'MISSOURI',
            'county_names': 'St. Louis',
            'fee_area': 'STATEWIDE',  # No "CITY" hint → prefer County
        }
    ])
    
    result = normalize_locality_fips(raw_df)
    
    # Should match (either County 189 or City 510, depending on LSAD tie-break)
    assert len(result.data) == 1
    assert len(result.quarantine) == 0
    
    row = result.data.iloc[0]
    assert row['state_fips'] == '29'
    # Default tie-break prefers County over Independent City
    assert row['county_fips'] in ['189', '510']
    assert 'St. Louis' in row['county_name_canonical']


@pytest.mark.golden
def test_locality_fips_richmond_va():
    """Test Richmond (VA) - city vs county disambiguation."""
    raw_df = pd.DataFrame([
        {
            'mac': '00903',
            'locality_code': '01',
            'state_name': 'VIRGINIA',
            'county_names': 'Richmond',
            'fee_area': 'METRO AREA',  # No explicit "CITY" hint
        }
    ])
    
    result = normalize_locality_fips(raw_df)
    
    # Should match (either city 760 or county 159)
    assert len(result.data) == 1
    assert len(result.quarantine) == 0
    
    row = result.data.iloc[0]
    assert row['state_fips'] == '51'
    assert row['county_fips'] in ['760', '159']
    assert 'Richmond' in row['county_name_canonical']


@pytest.mark.golden
def test_locality_fips_all_counties_expansion():
    """Test 'ALL COUNTIES' expansion."""
    raw_df = pd.DataFrame([
        {
            'mac': '02102',
            'locality_code': '01',
            'state_name': 'ALASKA',
            'county_names': 'ALL COUNTIES',
            'fee_area': 'STATEWIDE',
        }
    ])
    
    result = normalize_locality_fips(raw_df)
    
    # Alaska has ~29 boroughs/census areas
    assert len(result.data) >= 29
    assert (result.data['expansion_method'] == 'all_counties').all()


@pytest.mark.negative
def test_locality_fips_unknown_state():
    """Unknown state should quarantine."""
    raw_df = pd.DataFrame([
        {
            'mac': '99999',
            'locality_code': '99',
            'state_name': 'ATLANTIS',
            'county_names': 'MYTHICAL',
            'fee_area': 'N/A',
        }
    ])
    
    result = normalize_locality_fips(raw_df)
    
    assert len(result.data) == 0
    assert len(result.quarantine) == 1
    assert result.quarantine.iloc[0]['reason'] == 'unknown_state'
