<!-- 1e6b98e1-2f30-4e1b-b2c7-2c23f66b2b94 bce2792f-1e1d-4e94-8127-dbf728bc4233 -->
# Locality Parser - Stage 2: FIPS Normalization

## Overview

Implement the FIPS normalization stage to transform raw county NAMES → FIPS codes and explode to one-row-per-county format for downstream joins with GPCI, RVU, and MPFS data.

**Time Estimate:** 90-120 min

**Dependencies:** Stage 1 complete ✅, Reference data bootstrap complete ✅

**Architecture:** Per `planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md`

## Background

**Stage 1 (Complete):** Raw parser preserves state/county NAMES as-is from CMS

**Stage 2 (This Plan):** Derive FIPS codes for downstream joins

**Why Two Stages:**

- CMS ships county NAMES (not codes)
- Downstream joins require FIPS codes
- Separation keeps parsers simple, normalizers focused
- Matches STD-data-architecture-impl §1.3 pattern

## Implementation Steps

### Step 1: Complete Reference Data (30 min)

**Goal:** Full county coverage (3,100+ counties)

**Current State:**

- ✅ `data/reference/census/fips_states/2025/us_states.csv` (51 states)
- ✅ `data/reference/census/fips_counties/2025/us_counties_mvp.csv` (96 representative counties)
- ❌ Need full county dataset (3,100+ rows)

**Tasks:**

1.1. **Download or create full county dataset** (15 min)

            - Source: Census Bureau county FIPS or curated from CMS files
            - Columns: `state_fips`, `state_name`, `county_fips`, `county_name`
            - File: `data/reference/census/fips_counties/2025/us_counties.csv`
            - Rows: ~3,100 counties

1.2. **Create curated county alias map** (15 min)

            - File: `data/reference/cms/county_aliases/2025/county_aliases.yml`
            - Structure:
     ```yaml
     aliases:
       "St. Louis": "Saint Louis"
       "St. Charles": "Saint Charles"
       "Dona Ana": "Doña Ana"  # Diacritic handling
     
     special_cases:
       VA:  # Independent cities
         - "Alexandria City"
         - "Fairfax City"
       LA:  # Parishes
         - "Orleans Parish" → "Orleans"
       AK:  # Boroughs
         - "Fairbanks North Star Borough" → "Fairbanks North Star"
     ```


### Step 2: Implement FIPS Lookup Logic (40 min)

**File:** `cms_pricing/ingestion/normalize/normalize_locality_fips.py`

**Function Signature:**

```python
from typing import NamedTuple
import pandas as pd

class NormalizeResult(NamedTuple):
    data: pd.DataFrame          # Normalized rows with FIPS
    quarantine: pd.DataFrame    # Non-matches
    metrics: Dict[str, Any]     # Normalization metrics

def normalize_locality_fips(
    raw_df: pd.DataFrame,
    ref_data_manager: ReferenceDataManager
) -> NormalizeResult:
    """
    Transform raw locality data: county NAMES → FIPS codes.
    
    Args:
        raw_df: Raw parser output (mac, locality_code, state_name, county_names)
        ref_data_manager: Reference data provider (states, counties, aliases)
        
    Returns:
        NormalizeResult with FIPS-enriched data, quarantine, metrics
    """
```

**Implementation (5 sub-functions):**

2.1. **Load reference data** (5 min)

```python
def _load_fips_crosswalk(ref_manager):
    states = ref_manager.get_states()  # 51 rows
    counties = ref_manager.get_counties()  # 3,100+ rows
    aliases = ref_manager.get_county_aliases()  # YAML dict
    return states, counties, aliases
```

2.2. **State FIPS lookup** (5 min)

```python
def _derive_state_fips(raw_df, states_df):
    """Exact match: state_name → state_fips"""
    state_map = dict(zip(states_df['state_name'], states_df['state_fips']))
    raw_df['state_fips'] = raw_df['state_name'].map(state_map)
    
    # Quarantine unknown states
    unknown = raw_df[raw_df['state_fips'].isna()]
    return raw_df[raw_df['state_fips'].notna()], unknown
```

2.3. **County name parsing and explosion** (10 min)

```python
def _explode_counties(df):
    """
    Split county_names → array; explode to one-row-per-county.
    
    Input: 1 row with county_names = "JEFFERSON, ORLEANS, PLAQUEMINES"
    Output: 3 rows (one per county)
    """
    # Split on comma or slash
    df['county_list'] = df['county_names'].str.split(r'[,/]')
    df = df.explode('county_list')
    df['county_name_clean'] = df['county_list'].str.strip().str.upper()
    return df
```

2.4. **County FIPS lookup with matching strategy** (15 min)

```python
def _match_county_fips(df, counties_df, aliases):
    """
    Match county names to FIPS with 3-tier strategy:
    1. Exact match
    2. Alias-normalized exact match
    3. Fuzzy match (≥0.95 threshold) with deterministic tie-breaker
    """
    # Build lookup dict
    county_map = {}
    for _, row in counties_df.iterrows():
        key = (row['state_fips'], row['county_name'])
        county_map[key] = row['county_fips']
    
    # Try exact match first
    df['county_fips'] = df.apply(
        lambda r: county_map.get((r['state_fips'], r['county_name_clean'])),
        axis=1
    )
    df['match_method'] = df['county_fips'].apply(lambda x: 'exact' if pd.notna(x) else None)
    
    # Try alias match for unmatched
    unmatched = df[df['county_fips'].isna()]
    # ... alias logic
    
    # Try fuzzy match for still unmatched
    # ... fuzzy logic with rapidfuzz or similar
    
    return df
```

2.5. **Add mapping confidence** (5 min)

```python
def _add_confidence(df):
    """Add mapping_confidence based on match_method"""
    confidence_map = {
        'exact': 1.0,
        'alias': 1.0,
        'fuzzy': 0.95  # Or actual fuzzy score
    }
    df['mapping_confidence'] = df['match_method'].map(confidence_map)
    return df
```

### Step 3: Edge Case Handling (15 min)

**VA Independent Cities:**

- Alexandria City, Fairfax City, etc. (5+ cities)
- Match as city FIPS (not county)

**LA Parishes:**

- "Orleans Parish" → "Orleans" (64 parishes)
- Strip "Parish" suffix for matching

**AK Boroughs:**

- "Fairbanks North Star Borough" → match full name
- Handle "Census Area" vs "Borough"

**Implementation:**

```python
def _apply_special_cases(df, state_fips, county_name):
    """Handle VA/LA/AK edge cases before standard matching."""
    
    # VA: Independent cities
    if state_fips == '51' and 'City' in county_name:
        # Look up city FIPS directly
        return lookup_va_city(county_name)
    
    # LA: Strip "Parish"
    if state_fips == '22':
        county_name = county_name.replace(' Parish', '').replace(' PARISH', '')
    
    # AK: Keep "Borough" or "Census Area"
    # (no transformation needed, match as-is)
    
    return county_name
```

### Step 4: Build Output Schema (10 min)

**Normalized Schema:**

```python
normalized_df = df[[
    'locality_code',      # From raw (2-digit string)
    'mac',                # From raw (5-digit string)
    'state_fips',         # DERIVED (2-digit)
    'state_name',         # From raw
    'county_fips',        # DERIVED (3-digit)
    'county_name_canonical',  # Normalized
    'fee_area',           # From raw
    'match_method',       # 'exact' | 'alias' | 'fuzzy'
    'mapping_confidence', # 0.95-1.0
    'source_release_id',  # From metadata
    'row_content_hash',   # Deterministic
]]

# Natural key: (locality_code, state_fips, county_fips)
# Sorted by natural key
```

**Enforce uniqueness:**

```python
duplicates = normalized_df[normalized_df.duplicated(subset=['locality_code', 'state_fips', 'county_fips'], keep=False)]

if len(duplicates) > 0:
    logger.error(f"Natural key duplicates: {len(duplicates)} rows")
    raise DuplicateKeyError(f"Duplicate (locality, state, county): {duplicates.head().to_dict('records')}")
```

### Step 5: Create Tests (20 min)

**File:** `tests/normalize/test_locality_fips_normalization.py`

**Golden Tests (3):**

5.1. **test_locality_fips_exact_match()**

```python
@pytest.mark.golden
def test_locality_fips_exact_match():
    """Test clean exact matching (no aliases, no fuzzy)."""
    raw_df = pd.DataFrame({
        'mac': ['10112'],
        'locality_code': ['00'],
        'state_name': ['ALABAMA'],
        'county_names': ['AUTAUGA'],
        'fee_area': ['STATEWIDE'],
    })
    
    result = normalize_locality_fips(raw_df, ref_manager)
    
    assert len(result.data) == 1
    assert result.data.iloc[0]['state_fips'] == '01'
    assert result.data.iloc[0]['county_fips'] == '001'
    assert result.data.iloc[0]['match_method'] == 'exact'
    assert result.data.iloc[0]['mapping_confidence'] == 1.0
    assert len(result.quarantine) == 0
```

5.2. **test_locality_fips_multi_county_explosion()**

```python
@pytest.mark.golden
def test_locality_fips_multi_county_explosion():
    """Test exploding multi-county localities."""
    raw_df = pd.DataFrame({
        'mac': ['10112'],
        'locality_code': ['05'],
        'state_name': ['LOUISIANA'],
        'county_names': ['JEFFERSON, ORLEANS, PLAQUEMINES'],
        'fee_area': ['NEW ORLEANS'],
    })
    
    result = normalize_locality_fips(raw_df, ref_manager)
    
    # 1 input row → 3 output rows
    assert len(result.data) == 3
    assert set(result.data['county_name_canonical']) == {'JEFFERSON', 'ORLEANS', 'PLAQUEMINES'}
    # All should have same locality_code, state_fips
    assert (result.data['locality_code'] == '05').all()
    assert (result.data['state_fips'] == '22').all()
```

5.3. **test_locality_fips_alias_match()**

```python
@pytest.mark.golden
def test_locality_fips_alias_match():
    """Test alias matching (St. → Saint)."""
    raw_df = pd.DataFrame({
        'mac': ['05302'],
        'locality_code': ['01'],
        'state_name': ['MISSOURI'],
        'county_names': ['St. Louis'],  # Alias
        'fee_area': ['ST LOUIS'],
    })
    
    result = normalize_locality_fips(raw_df, ref_manager)
    
    assert len(result.data) == 1
    assert result.data.iloc[0]['county_name_canonical'] == 'SAINT LOUIS'
    assert result.data.iloc[0]['match_method'] == 'alias'
    assert result.data.iloc[0]['mapping_confidence'] == 1.0
```

**Edge Case Tests (3):**

5.4. **test_locality_fips_va_independent_city()**

```python
@pytest.mark.edge_case
def test_locality_fips_va_independent_city():
    """Test VA independent city (not a county)."""
    # Alexandria City, Fairfax City, etc.
```

5.5. **test_locality_fips_la_parish()**

```python
@pytest.mark.edge_case
def test_locality_fips_la_parish():
    """Test LA parish name handling."""
    # "Orleans Parish" → "Orleans"
```

5.6. **test_locality_fips_diacritics()**

```python
@pytest.mark.edge_case
def test_locality_fips_diacritics():
    """Test diacritic handling (Doña Ana)."""
```

**Negative Tests (2):**

5.7. **test_locality_fips_unknown_state()**

```python
@pytest.mark.negative
def test_locality_fips_unknown_state():
    """Test unknown state → quarantine."""
```

5.8. **test_locality_fips_unknown_county()**

```python
@pytest.mark.negative
def test_locality_fips_unknown_county():
    """Test unknown county → quarantine."""
```

### Step 6: Acceptance Validation (5 min)

**Success Criteria:**

- ✅ Coverage: ≥99.5% counties derive FIPS successfully
- ✅ Parity: `SUM(exploded_rows) = SUM(county_count(raw))`
- ✅ Uniqueness: Natural key `(locality_code, state_fips, county_fips)` enforced
- ✅ Edge cases: VA cities, LA parishes, AK boroughs handled
- ✅ Tests: 8+ passing (3 golden, 3 edge, 2 negative)
- ✅ Coverage: ≥90% line coverage

## Key Files

**New Files to Create:**

- `cms_pricing/ingestion/normalize/normalize_locality_fips.py` (~300 lines)
- `data/reference/census/fips_counties/2025/us_counties.csv` (3,100+ rows)
- `data/reference/cms/county_aliases/2025/county_aliases.yml` (curated)
- `tests/normalize/test_locality_fips_normalization.py` (~200 lines)

**Files to Reference:**

- `cms_pricing/ingestion/parsers/locality_parser.py` (Stage 1 output)
- `planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md` (design spec)
- `planning/parsers/locality/PHASE_0_REFERENCE_DATA.md` (ref data plan)
- `data/reference/census/fips_states/2025/us_states.csv` (existing)

## Schema

**Input (from Stage 1):**

```python
{
    'mac': '10112',
    'locality_code': '00',
    'state_name': 'ALABAMA',
    'fee_area': 'STATEWIDE',
    'county_names': 'ALL COUNTIES'  # May be comma/slash-delimited
}
```

**Output (Stage 2):**

```python
{
    'locality_code': '01',           # 2-digit (from raw)
    'mac': '10112',                  # 5-digit (from raw)
    'state_fips': '01',              # DERIVED
    'state_name': 'ALABAMA',         # From raw
    'county_fips': '001',            # DERIVED
    'county_name_canonical': 'Autauga',  # Normalized
    'fee_area': 'STATEWIDE',         # From raw
    'match_method': 'exact',         # 'exact' | 'alias' | 'fuzzy'
    'mapping_confidence': 1.0,       # 0.95-1.0
    'source_release_id': '...',      # From metadata
    'row_content_hash': '...',       # Deterministic
}
```

**Natural Key:** `['locality_code', 'state_fips', 'county_fips']`

## Time Breakdown

| Task | Estimate | Notes |

|------|----------|-------|

| Complete reference data (counties + aliases) | 30 min | Download/curate full dataset |

| Implement FIPS lookup (5 functions) | 40 min | Exact, alias, fuzzy matching |

| Edge case handling (VA/LA/AK) | 15 min | Special case logic |

| Build output schema + uniqueness check | 10 min | Enforce natural keys |

| Create tests (8 tests) | 20 min | 3 golden, 3 edge, 2 negative |

| Acceptance validation | 5 min | Run tests, check coverage |

| **Total** | **120 min** | Conservative (2 hours) |

## Risks & Mitigations

| Risk | Mitigation |

|------|------------|

| Fuzzy matching too slow | Use exact/alias first (99% cases), fuzzy for remainder only |

| VA/LA/AK edge cases complex | Use special_cases dict in alias YAML |

| Natural key duplicates | Enforce uniqueness, fail if found (shouldn't happen with FIPS) |

| Coverage < 99.5% | Quarantine with full trace, investigate patterns |

## Success Criteria

- ✅ All 8 tests passing
- ✅ Coverage ≥99.5% (< 5 counties quarantined)
- ✅ Line coverage ≥90%
- ✅ Natural key uniqueness enforced
- ✅ Deterministic output (sorted, hashed)
- ✅ Parity: rows_in (raw) = rows_out (exploded, accounting for multi-county)
- ✅ Integration: Can join with GPCI on locality_code

## Next Steps After Completion

1. Update `prds/SRC-locality.md` (add Stage 2 details)
2. Update `CHANGELOG.md` (add Stage 2 entry)
3. Mark GitHub task as complete
4. Test integration with GPCI data (optional)
5. Consider: Next parser (ANES, CF) or other work

### To-dos

- [x] Step 1: Complete reference data - full counties CSV (3,100+) + alias YAML (30 min)
- [x] Step 2: Implement normalize_locality_fips.py with 5 functions (40 min)
- [x] Step 3: Edge case handling for VA/LA/AK (15 min)
- [x] Step 4: Build output schema + uniqueness enforcement (10 min)
- [x] Step 5: Create 8 tests - golden, edge, negative (20 min)
- [x] Step 6: Run tests, verify acceptance criteria (5 min)