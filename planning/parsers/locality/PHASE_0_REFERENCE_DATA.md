# Phase 0: FIPS Reference Data Creation

**Time Budget:** 60 minutes  
**Start:** TBD  
**Goal:** Create reference tables for state/county FIPS lookup

---

## Plan

### Task 1: US States Reference (15 min)

**Source:** Use standard US state FIPS codes (publicly available)

**File:** `data/reference/us_states.csv`

**Columns:**
```csv
state_fips,state_abbr,state_name,alt_names
01,AL,ALABAMA,""
02,AK,ALASKA,""
...
```

**Strategy:** Create manually or download from Census

**Validation:**
- 50 states + DC + territories = ~56 rows
- All FIPS codes 2-digit zero-padded
- State names match CMS format (ALL CAPS)

---

### Task 2: US Counties Reference (30 min)

**Source:** Census Bureau county FIPS codes

**File:** `data/reference/us_counties.csv`

**Columns:**
```csv
state_fips,county_fips,county_name,county_type,alt_names
01,001,AUTAUGA,County,""
01,003,BALDWIN,County,""
...
```

**Strategy:**
1. Download Census county file OR
2. Create curated list from CMS25LOCCO.txt inspection OR
3. Use existing gazetteer data if available

**Validation:**
- ~3,000+ US counties
- All county_fips 3-digit zero-padded
- County names match CMS format (ALL CAPS)

---

### Task 3: County Aliases (10 min)

**File:** `data/reference/county_aliases.yml`

**Content:**
```yaml
# CMS name variations → canonical county name
aliases:
  # St. vs Saint
  - cms_pattern: "St\\."
    canonical_pattern: "Saint"
    examples: ["St. Louis → Saint Louis", "St. Charles → Saint Charles"]
  
  # Diacritics
  - cms_name: "Dona Ana"
    canonical: "Doña Ana"
  
  # Dual names
  - cms_name: "Miami-Dade"
    canonical: ["Miami", "Dade"]

# State-specific special cases
special_cases:
  VA:
    type: "independent_cities"
    note: "Virginia has independent cities that function as counties"
    examples: ["Alexandria City", "Norfolk City", "Virginia Beach City"]
  
  LA:
    type: "parishes"
    note: "Louisiana has parishes, not counties"
    suffix: "Parish"
  
  AK:
    type: "boroughs_and_census_areas"
    note: "Alaska has boroughs and census areas"
```

**Strategy:** Start minimal, expand as CMS variations discovered

---

### Task 4: FIPS Crosswalk Loader (5 min)

**File:** `cms_pricing/ingestion/reference/fips_crosswalk.py`

**Functions:**
```python
def load_state_fips_map() -> Dict[str, str]:
    """Load state name → FIPS mapping."""
    states = pd.read_csv('data/reference/us_states.csv')
    return dict(zip(states['state_name'], states['state_fips']))

def load_county_fips_map() -> Dict[Tuple[str, str], str]:
    """Load (state_fips, county_name) → county_fips mapping."""
    counties = pd.read_csv('data/reference/us_counties.csv')
    return {
        (row['state_fips'], row['county_name']): row['county_fips']
        for _, row in counties.iterrows()
    }

def load_county_aliases() -> Dict:
    """Load county name aliases."""
    import yaml
    with open('data/reference/county_aliases.yml') as f:
        return yaml.safe_load(f)
```

---

## Implementation Approach

### Option 1: Use Existing Gazetteer Data (FASTEST - 15 min)

**Check if exists:**
```bash
find data -name "*gazetteer*" -o -name "*fips*" | head -10
```

**If Census Gazetteer files exist:**
- Extract state/county FIPS from there
- Already has canonical naming
- High quality, authoritative

**Time:** 15 min (extract + transform)

---

### Option 2: Create from CMS File (MEDIUM - 30 min)

**Extract from 25LOCCO.txt:**
```python
# Parse 25LOCCO.txt
# Get unique states → Create states.csv
# Get counties by state → Create counties.csv
# Manual FIPS assignment (from Census lookup)
```

**Pros:** Guaranteed to match CMS naming  
**Cons:** Need FIPS codes from Census still

**Time:** 30 min

---

### Option 3: Download Census Data (THOROUGH - 45 min)

**Source:**
- Census Bureau: https://www.census.gov/geo/reference/codes/cou.html
- FIPS codes: https://www.census.gov/library/reference/code-lists/ansi.html

**Steps:**
1. Download state FIPS (5 min)
2. Download county FIPS (10 min)
3. Clean/normalize to CMS format (20 min)
4. Create aliases manually (10 min)

**Pros:** Authoritative, complete  
**Cons:** More time

**Time:** 45 min

---

## Recommended: Option 1 (Check Gazetteer) → Option 3 (Download Census)

**Workflow:**
1. Check if Gazetteer data exists (5 min)
2. If yes → Extract FIPS (10 min) → DONE
3. If no → Download Census (45 min)

**Fallback:** Use curated inline dict for MVP (10 min), expand later

---

## Minimal Viable Dataset (If Time-Constrained)

**Just create inline dict for testing:**

```python
# cms_pricing/ingestion/reference/fips_crosswalk.py

STATE_FIPS = {
    'ALABAMA': '01',
    'ALASKA': '02',
    'ARIZONA': '04',
    # ... top 20 states for testing
}

COUNTY_FIPS = {
    ('01', 'AUTAUGA'): '001',
    ('01', 'BALDWIN'): '003',
    # ... representative counties for testing
}
```

**Time:** 10 minutes  
**Coverage:** Enough for golden tests  
**Expand later:** Add full tables in production

---

## Decision Point

**What do you want to prioritize?**

1. **Full Reference Data** (45-60 min) - Complete, production-ready
2. **Check Gazetteer First** (5 min) - May already have data
3. **Minimal Dict** (10 min) - Just enough for testing, expand later

**My Recommendation:** Start with #2 (check Gazetteer), then decide #1 or #3

**Shall I check for existing Gazetteer/FIPS data first?**

