# Locality Parser: Two-Stage Architecture

**Decision:** Keep canonical schema with FIPS, use two-stage pipeline  
**Architecture:** Raw Parser → FIPS Normalizer  
**Rationale:** Separation of concerns, stable downstream joins, testable stages

---

## Architecture Overview

```
CMS File (25LOCCO.txt)                Raw Parser                     FIPS Normalizer              Canonical Table
├─ Medicare Admin: 10112       ───>   ├─ mac: "10112"          ───>  ├─ mac: "10112"        ───>  std_localitycounty_v1.0
├─ Locality: 00                       ├─ locality_id: "00"           ├─ locality_code: "00"       ├─ locality_code: "00"
├─ State: ALABAMA                     ├─ state: "ALABAMA"            ├─ state_fips: "01"          ├─ state_fips: "01"  
├─ Area: STATEWIDE                    ├─ fee_schedule_area: ...      ├─ county_fips: "001"        ├─ county_fips: "001"
└─ Counties: ALL COUNTIES             └─ county_name: "ALL..."       └─ match_method: "exact"     └─ (67 counties exploded)
                                      
                                      Table: raw_cms_localitycounty   Table: std_localitycounty_v1_0
                                      Natural Key: (mac, locality,    Natural Key: (locality, state_fips, county_fips)
                                                    state, county)
```

---

## Stage 1: Raw Parser (Layout-Faithful)

**Purpose:** Parse CMS file exactly as shipped, no transformations

**Table:** `raw_cms_localitycounty_2025d`

**Columns (from LOCCO_2025D_LAYOUT):**
```python
{
    'mac': 'Medicare Admin Contractor code (5-digit)',
    'locality_id': 'Locality code (2-digit)',
    'state': 'State name (e.g., ALABAMA)',
    'fee_schedule_area': 'Locality description',
    'county_name': 'County names (comma/delimited, e.g., "ALL COUNTIES" or "LOS ANGELES/ORANGE")',
    
    # Metadata (injected)
    'release_id': 'mpfs_2025_q4_...',
    'vintage_date': datetime,
    'source_filename': '25LOCCO.txt',
    'source_file_sha256': '...',
    'parsed_at': datetime,
    'row_content_hash': '64-char SHA-256',
}
```

**Parser Responsibilities:**
- ✅ Parse fixed-width layout
- ✅ Trim/normalize strings
- ✅ Inject metadata
- ✅ Compute row hash
- ❌ NO FIPS derivation (that's Stage 2)
- ❌ NO county splitting (that's Stage 2)

**Natural Key (Raw):** `['mac', 'locality_id', 'state']` (as-is from file)

**Complexity:** LOW - Just parse what's there

**Time Estimate:** 45-60 min (simple fixed-width parser)

---

## Stage 2: FIPS Normalizer (Derivation Logic)

**Purpose:** Derive FIPS codes and explode to one-row-per-county

**Input:** `raw_cms_localitycounty_2025d`

**Outputs:**
1. `std_localitycounty_v1_0` (canonical)
2. `quarantine_locco_fips_mismatch` (unmatched counties)

### 2.1 Derivation Steps

**Step 1: Load Crosswalk Tables**
```python
# ref/us_states.csv (state_name → state_fips)
state_fips_map = {
    'ALABAMA': '01',
    'ALASKA': '02',
    ...
}

# ref/us_counties.csv (state_fips + county_name → county_fips)
county_fips_map = {
    ('01', 'AUTAUGA'): '001',
    ('01', 'BALDWIN'): '003',
    ...
}

# ref/county_aliases.yml (name variations)
aliases = {
    'St. Louis': 'Saint Louis',
    'Doña Ana': 'Dona Ana',
    'Miami-Dade': ['Miami', 'Dade'],
    ...
}
```

**Step 2: Parse County Lists**
```python
def parse_county_list(county_str: str) -> List[str]:
    """
    Parse CMS county list format.
    
    Handles:
    - "ALL COUNTIES" → Special case (all counties in state)
    - "LOS ANGELES/ORANGE" → Split on "/"
    - "DADE AND MONROE" → Split on "AND"
    - "JEFFERSON, ORLEANS, ..." → Split on ","
    """
    if county_str.strip() == 'ALL COUNTIES':
        return ['ALL_COUNTIES']  # Marker for state lookup
    
    # Normalize delimiters
    normalized = county_str.replace('/', ',').replace(' AND ', ',')
    counties = [c.strip() for c in normalized.split(',')]
    return [c for c in counties if c]  # Remove empties
```

**Step 3: Match to FIPS (Tiered)**
```python
def match_county_to_fips(
    state_fips: str,
    county_name: str,
    aliases: Dict
) -> Tuple[Optional[str], str]:
    """
    Match county name to FIPS with tiered strategy.
    
    Returns:
        (county_fips, match_method)
        
    Match Methods:
    1. exact: Direct match
    2. alias: Via alias table
    3. fuzzy_high: ≥0.95 similarity
    4. None: No match (quarantine)
    """
    # Tier 1: Exact match
    if (state_fips, county_name) in county_fips_map:
        return county_fips_map[(state_fips, county_name)], 'exact'
    
    # Tier 2: Alias match
    for alias, canonical in aliases.items():
        if county_name == alias:
            if (state_fips, canonical) in county_fips_map:
                return county_fips_map[(state_fips, canonical)], 'alias'
    
    # Tier 3: Fuzzy match (≥0.95 similarity)
    from difflib import SequenceMatcher
    best_match = None
    best_score = 0.95
    for (st, cnty), fips in county_fips_map.items():
        if st == state_fips:
            score = SequenceMatcher(None, county_name, cnty).ratio()
            if score > best_score:
                best_score = score
                best_match = fips
    
    if best_match:
        return best_match, f'fuzzy_{best_score:.2f}'
    
    # No match - quarantine
    return None, 'no_match'
```

**Step 4: Explode to Rows**
```python
def explode_to_counties(raw_row: pd.Series) -> List[Dict]:
    """
    Explode single raw row to multiple canonical rows.
    
    Example:
        Input: mac=10112, locality=00, state=ALABAMA, counties="ALL COUNTIES"
        Output: 67 rows (one per Alabama county)
        
        Input: mac=01182, locality=18, state=CALIFORNIA, counties="LOS ANGELES/ORANGE"
        Output: 2 rows (Los Angeles + Orange)
    """
    state_fips = state_name_to_fips(raw_row['state'])
    county_names = parse_county_list(raw_row['county_name'])
    
    exploded_rows = []
    for county_name in county_names:
        if county_name == 'ALL_COUNTIES':
            # Get all counties for state
            for county_fips in get_counties_for_state(state_fips):
                exploded_rows.append({
                    'locality_code': raw_row['locality_id'],
                    'state_fips': state_fips,
                    'county_fips': county_fips,
                    'match_method': 'all_counties',
                })
        else:
            county_fips, match_method = match_county_to_fips(
                state_fips, county_name, aliases
            )
            if county_fips:
                exploded_rows.append({
                    'locality_code': raw_row['locality_id'],
                    'state_fips': state_fips,
                    'county_fips': county_fips,
                    'match_method': match_method,
                })
            else:
                # Quarantine
                quarantine.append({...})
    
    return exploded_rows
```

---

## File & Asset Plan

### Reference Data (Required)

**1. US States Crosswalk**
```python
# ref/us_states.csv
state_fips,state_abbr,state_name,alt_names
01,AL,ALABAMA,""
02,AK,ALASKA,""
...
```

**2. US Counties Crosswalk**
```python
# ref/us_counties.csv  
state_fips,county_fips,county_name,county_type,alt_names
01,001,AUTAUGA,County,""
01,003,BALDWIN,County,""
02,013,ALEUTIANS EAST,Borough,"Aleutians E."
...
```

**3. County Aliases (Curated)**
```yaml
# ref/county_aliases.yml
aliases:
  - cms_name: "St. Louis"
    canonical: "Saint Louis"
  - cms_name: "Doña Ana"
    canonical: "Dona Ana"
  - cms_name: "Miami-Dade"
    canonical: ["Miami", "Dade"]  # Both valid
special_cases:
  - state: "VA"
    type: "independent_city"
    counties: ["Alexandria City", "Norfolk City", ...]
  - state: "LA"
    suffix: "Parish"
```

### Code Files

**Parser:**
- `cms_pricing/ingestion/parsers/locality_parser.py` (raw parser)

**Normalizer:**
- `cms_pricing/ingestion/normalize/locality_fips_normalizer.py` (new)

**Reference Loaders:**
- `cms_pricing/ingestion/reference/fips_crosswalk.py` (new)

---

## Testing Strategy

### Stage 1 Tests (Raw Parser)

**Golden Test:**
```python
@pytest.mark.golden
def test_locality_raw_parse_txt():
    """Parse 25LOCCO.txt as-is, no FIPS derivation."""
    result = parse_locality_raw(fixture, '25LOCCO.txt', metadata)
    
    # Verify raw parsing only
    assert len(result.data) == 20  # Sample rows
    assert 'mac' in result.data.columns
    assert 'locality_id' in result.data.columns
    assert 'state' in result.data.columns  # NAME, not FIPS
    assert 'county_name' in result.data.columns  # Names, not split
    assert len(result.rejects) == 0  # Clean parsing
```

### Stage 2 Tests (FIPS Normalizer)

**Exact Match Test:**
```python
def test_fips_derivation_exact():
    """Test exact state/county name matching."""
    raw_data = pd.DataFrame([
        {'mac': '10112', 'locality_id': '00', 'state': 'ALABAMA', 'county_name': 'AUTAUGA'}
    ])
    
    normalized = derive_fips(raw_data, state_map, county_map, aliases)
    
    assert normalized.iloc[0]['state_fips'] == '01'
    assert normalized.iloc[0]['county_fips'] == '001'
    assert normalized.iloc[0]['match_method'] == 'exact'
```

**Alias Match Test:**
```python
def test_fips_derivation_alias():
    """Test alias resolution (St. vs Saint)."""
    raw_data = pd.DataFrame([
        {'state': 'MISSOURI', 'county_name': 'St. Louis'}  # CMS uses "St."
    ])
    
    normalized = derive_fips(raw_data, state_map, county_map, aliases)
    
    assert normalized.iloc[0]['match_method'] == 'alias'
    # Should match "Saint Louis County" via alias
```

**Explosion Test:**
```python
def test_county_explosion():
    """Test multi-county explosion."""
    raw_data = pd.DataFrame([
        {'locality_id': '18', 'county_name': 'LOS ANGELES/ORANGE'}
    ])
    
    normalized = derive_fips(raw_data, ...)
    
    assert len(normalized) == 2  # Exploded to 2 rows
    assert set(normalized['county_fips']) == {'037', '059'}  # LA + Orange
```

**ALL COUNTIES Test:**
```python
def test_all_counties_explosion():
    """Test 'ALL COUNTIES' explosion."""
    raw_data = pd.DataFrame([
        {'locality_id': '00', 'state': 'ALABAMA', 'county_name': 'ALL COUNTIES'}
    ])
    
    normalized = derive_fips(raw_data, ...)
    
    assert len(normalized) == 67  # Alabama has 67 counties
    assert normalized['match_method'].iloc[0] == 'all_counties'
```

---

## Implementation Plan (Time-Tracked)

### Phase 0: Reference Data Preparation (60 min)
- Create `ref/us_states.csv` (or find existing)
- Create `ref/us_counties.csv` (Census data)
- Create `ref/county_aliases.yml` (curated list)
- Create `cms_pricing/ingestion/reference/fips_crosswalk.py` loader

### Phase 1: Raw Parser (45-60 min)
- Create `locality_parser.py` (layout-faithful)
- Use existing LOCCO_2025D_LAYOUT
- Create golden test for raw parsing
- Verify: Parses as-is, no transformation

### Phase 2: FIPS Normalizer (90 min)
- Create `normalize/locality_fips_normalizer.py`
- Implement tiered matching (exact → alias → fuzzy)
- Implement county explosion (single row → N rows)
- Handle "ALL COUNTIES" special case
- Create quarantine for unmatched

### Phase 3: Testing (45-60 min)
- Golden tests for raw parser
- Unit tests for normalizer (exact, alias, fuzzy)
- Integration test (raw → normalized)
- Edge case tests (tricky county names)
- Coverage test (≥99.5% counties matched)

### Phase 4: Validation Gates (30 min)
- Coverage assertion (≥99.5%)
- Parity check (SUM(exploded) == expected)
- Referential integrity (FIPS codes valid)

---

## Edge Cases to Handle

### State-Specific Quirks

**Virginia Independent Cities:**
```python
# Not counties, but city=county equivalent
independent_cities = {
    'ALEXANDRIA CITY': '510',
    'NORFOLK CITY': '710',
    ...
}
```

**Louisiana Parishes:**
```python
# "Parish" not "County"
'JEFFERSON': 'JEFFERSON PARISH',
'ORLEANS': 'ORLEANS PARISH',
```

**Alaska Boroughs:**
```python
# Census areas, not counties
'ALEUTIANS EAST': 'ALEUTIANS EAST BOROUGH',
```

### Name Variations

**St. vs Saint:**
- CMS: "St. Louis", "St. Charles"
- Census: "Saint Louis", "Saint Charles"
- Solution: Alias map

**Diacritics:**
- CMS: "Dona Ana" (may strip accents)
- Census: "Doña Ana"
- Solution: Normalize both to ASCII

**Hyphenated Names:**
- CMS: "Miami-Dade"
- Census: Could be "Miami" or "Dade" separately
- Solution: Handle dual mapping

---

## Validation Gates (QTS-Compliant)

### Coverage Gate (≥99.5%)
```python
def validate_coverage(raw_count: int, matched_count: int, quarantine_count: int):
    """Ensure ≥99.5% of counties matched."""
    coverage_rate = matched_count / raw_count
    
    if coverage_rate < 0.995:
        raise ValidationError(
            f"Coverage {coverage_rate:.2%} < 99.5% minimum. "
            f"Matched: {matched_count}, Quarantine: {quarantine_count}"
        )
    
    if coverage_rate < 1.0:
        logger.warning(
            f"Coverage {coverage_rate:.2%} (not 100%). "
            f"Review quarantine for {quarantine_count} unmatched counties."
        )
```

### Parity Gate
```python
def validate_parity(raw_rows: int, exploded_rows: int):
    """Ensure explosion count is reasonable."""
    # Approximate: 169 raw rows × avg 20 counties/row = ~3,000-4,000
    expected_min = raw_rows * 10  # Conservative
    expected_max = raw_rows * 50  # Generous
    
    if not (expected_min <= exploded_rows <= expected_max):
        logger.error(
            f"Exploded row count {exploded_rows} outside expected range "
            f"[{expected_min}, {expected_max}] for {raw_rows} raw rows"
        )
```

---

## Deliverables

**Code:**
- `cms_pricing/ingestion/parsers/locality_parser.py` (raw)
- `cms_pricing/ingestion/normalize/locality_fips_normalizer.py`
- `cms_pricing/ingestion/reference/fips_crosswalk.py`

**Reference Data:**
- `data/reference/us_states.csv`
- `data/reference/us_counties.csv`
- `data/reference/county_aliases.yml`

**Tests:**
- `tests/ingestion/test_locality_parser_raw.py`
- `tests/ingestion/test_locality_fips_normalizer.py`
- `tests/fixtures/locality/golden/` (raw fixtures)
- `tests/fixtures/locality/normalized/` (post-FIPS fixtures)

**Documentation:**
- Update SRC-carrier-localities.md → SRC-locality.md v1.0
- Create `PRD-locality-fips-normalization-v1.0.md` (derivation spec)
- Update `planning/parsers/locality/README.md`

---

## Time Estimate

| Phase | Estimated | Notes |
|-------|-----------|-------|
| **Reference Data** | 60 min | Find/create FIPS tables |
| **Raw Parser** | 60 min | Simple fixed-width, like GPCI Phase 1 |
| **FIPS Normalizer** | 90 min | Matching logic + explosion |
| **Testing** | 60 min | Golden + unit tests |
| **Validation Gates** | 30 min | Coverage + parity checks |
| **Documentation** | 30 min | SRC- update + PRD |
| **TOTAL** | **5.5 hours** | vs 8h GPCI baseline |

**Time Savings:** 2.5 hours (31% reduction)

**Additional Savings from §21.4:**
- Caught schema mismatch in 20 min
- Would have cost 2-3h debugging
- **Net:** §21.4 saved 2-3h, total implementation 5.5h

---

## Benefits of Two-Stage Approach

**Separation of Concerns:**
- ✅ Parser: Simple, layout-faithful, no business logic
- ✅ Normalizer: All complex logic isolated, testable
- ✅ Reference data: Versioned, reviewable, updateable

**Testability:**
- ✅ Raw parser tests: Just parse correctly
- ✅ Normalizer tests: Just match correctly
- ✅ Integration tests: End-to-end flow

**Maintainability:**
- ✅ CMS format change → Update parser only
- ✅ New county → Update reference data only
- ✅ Alias change → Update alias file only

**Downstream Stability:**
- ✅ Canonical schema unchanged (FIPS codes)
- ✅ Downstream joins still work
- ✅ No breaking changes to API/services

---

**Decision:** ✅ Proceed with two-stage architecture

**Next:** Create reference data files, then implement raw parser

---

*Time to decision: 20 minutes*  
*§21.4 value: PROVEN (caught blocker early)*  
*Architecture: Clean separation, testable stages*

