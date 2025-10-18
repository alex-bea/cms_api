#!/usr/bin/env python3
"""
Build full Census TIGER/Line 2025 county reference data.

Authority: Census 2025 Gazetteer Counties National
Source: https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_counties_national.zip
"""
import csv
import re
from pathlib import Path

def extract_county_type(name: str) -> tuple[str, str]:
    """
    Extract county type from canonical name.
    
    Returns:
        (county_name_without_suffix, county_type)
        
    Examples:
        "Autauga County" -> ("Autauga", "County")
        "Orleans Parish" -> ("Orleans", "Parish")
        "Alexandria city" -> ("Alexandria", "Independent City")
        "Bethel Census Area" -> ("Bethel", "Census Area")
        "Fairbanks North Star Borough" -> ("Fairbanks North Star", "Borough")
    """
    # Patterns ordered by specificity
    patterns = [
        (r'^(.+?)\s+Census\s+Area$', 'Census Area'),
        (r'^(.+?)\s+city$', 'Independent City'),  # VA independent cities
        (r'^(.+?)\s+City\s+and\s+Borough$', 'City and Borough'),  # AK
        (r'^(.+?)\s+Borough$', 'Borough'),  # AK
        (r'^(.+?)\s+Municipality$', 'Municipality'),  # AK (Anchorage)
        (r'^(.+?)\s+Parish$', 'Parish'),  # LA
        (r'^(.+?)\s+County$', 'County'),  # Most common
    ]
    
    for pattern, county_type in patterns:
        match = re.match(pattern, name, re.IGNORECASE)
        if match:
            return match.group(1), county_type
    
    # No match - return as-is (shouldn't happen with Census data)
    return name, 'Unknown'

def main():
    gazetteer_path = Path('/tmp/2025_Gaz_counties_national.txt')
    states_path = Path('/Users/alexanderbea/Cursor/cms-api/data/reference/census/fips_states/2025/us_states.csv')
    output_path = Path('/Users/alexanderbea/Cursor/cms-api/data/reference/census/fips_counties/2025/us_counties.csv')
    
    # Load states mapping
    state_map = {}
    with open(states_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            state_map[row['state_abbr']] = {
                'state_fips': row['state_fips'],
                'state_name': row['state_name']
            }
    
    # Process Gazetteer file
    counties = []
    with open(gazetteer_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            usps = row['USPS']
            geoid = row['GEOID']
            name_canonical = row['NAME']
            
            # Extract FIPS codes from GEOID (format: SSCCC where SS=state, CCC=county)
            state_fips = geoid[:2]
            county_fips = geoid[2:5]
            
            # Extract county type
            county_name, county_type = extract_county_type(name_canonical)
            
            # Get state info
            state_info = state_map.get(usps, {'state_fips': state_fips, 'state_name': 'UNKNOWN'})
            
            counties.append({
                'state_fips': state_fips,
                'state_abbr': usps,
                'state_name': state_info['state_name'],
                'county_fips': county_fips,
                'county_geoid': geoid,
                'county_name': county_name,  # For matching (without suffix)
                'county_name_canonical': name_canonical,  # Official (with suffix)
                'county_type': county_type,
            })
    
    # Sort by state_fips, county_fips for determinism
    counties.sort(key=lambda x: (x['state_fips'], x['county_fips']))
    
    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = [
            'state_fips', 'state_abbr', 'state_name',
            'county_fips', 'county_geoid',
            'county_name', 'county_name_canonical', 'county_type'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(counties)
    
    print(f"✓ Created {output_path}")
    print(f"  Total counties: {len(counties)}")
    print(f"  Authority: Census 2025 Gazetteer")
    print(f"  Source: 2025_Gaz_counties_national.txt")
    
    # Sanity checks
    print("\nSanity checks:")
    types_count = {}
    for c in counties:
        types_count[c['county_type']] = types_count.get(c['county_type'], 0) + 1
    
    for ctype, count in sorted(types_count.items()):
        print(f"  {ctype}: {count}")
    
    # Spot checks
    print("\nSpot checks:")
    # Doña Ana (diacritics)
    dona_ana = [c for c in counties if 'Doña' in c['county_name_canonical']]
    if dona_ana:
        print(f"  ✓ Diacritics preserved: {dona_ana[0]['county_name_canonical']} (FIPS {dona_ana[0]['county_geoid']})")
    
    # VA independent city
    alexandria = [c for c in counties if c['county_geoid'] == '51510']
    if alexandria:
        print(f"  ✓ VA independent city: {alexandria[0]['county_name_canonical']} (FIPS {alexandria[0]['county_geoid']})")
    
    # LA parish
    orleans = [c for c in counties if c['county_geoid'] == '22071']
    if orleans:
        print(f"  ✓ LA parish: {orleans[0]['county_name_canonical']} (FIPS {orleans[0]['county_geoid']})")
    
    # AK census area
    bethel = [c for c in counties if c['county_geoid'] == '02050']
    if bethel:
        print(f"  ✓ AK census area: {bethel[0]['county_name_canonical']} (FIPS {bethel[0]['county_geoid']})")

if __name__ == '__main__':
    main()

