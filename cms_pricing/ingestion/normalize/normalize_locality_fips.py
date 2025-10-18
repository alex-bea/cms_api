"""
Locality-County FIPS Normalization (Stage 2)

Transforms raw CMS locality-county data (state/county NAMES) → canonical FIPS codes.

Architecture: Two-stage pipeline per STD-data-architecture-impl §1.3
  - Stage 1 (Raw Parser): Parse CMS file as-is (names)
  - Stage 2 (This module): Derive FIPS codes & explode to one-row-per-county

Authority: Census TIGER/Line 2025 Gazetteer Counties
Reference: planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md

Key Features:
  - Set-logic expansion (ALL COUNTIES, ALL EXCEPT, REST OF)
  - Tiered matching (exact → alias → fuzzy with guardrails)
  - State-specific rules (VA independent cities, LA parishes, AK boroughs)
  - Deterministic outputs (zero-padding, sorted, row hashes)
  - Per-state coverage metrics + quarantine artifacts

Author: CMS Pricing API Team
Version: 1.0
Created: 2025-10-17
"""

import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import pandas as pd
import structlog
import yaml

# Optional: RapidFuzz for fuzzy matching (install via requirements-dev.txt)
try:
    from rapidfuzz import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False

logger = structlog.get_logger(__name__)


# =============================================================================
# Output Contract
# =============================================================================

class NormalizeResult(NamedTuple):
    """
    Result of FIPS normalization.
    
    Fields:
        data: Successfully normalized DataFrame with FIPS codes
        quarantine: Rows that failed normalization (for review)
        metrics: Dict with coverage stats, match methods, expansion counts
    """
    data: pd.DataFrame
    quarantine: pd.DataFrame
    metrics: Dict[str, Any]


# =============================================================================
# Natural Keys & Schema
# =============================================================================

# Natural Key (Primary): Identifies unique locality-county mapping
NK_PRIMARY = ['locality_code', 'state_fips', 'county_fips']

# Natural Key (Secondary): For mis-wire detection
NK_SECONDARY = ['mac', 'locality_code', 'county_fips']

# Output Schema (Canonical)
OUTPUT_COLUMNS = [
    'locality_code',      # 2-digit, zero-padded
    'mac',                # 5-digit, zero-padded
    'state_fips',         # 2-digit
    'county_fips',        # 3-digit
    'county_geoid',       # 5-digit (state_fips + county_fips)
    'county_name',        # For matching (without suffix)
    'county_name_canonical',  # Official Census name (with suffix, diacritics)
    'county_type',        # County, Parish, Borough, Census Area, Independent City
    'match_method',       # exact, alias, fuzzy, all_counties, all_except, rest_of_state
    'mapping_confidence', # 1.0 for exact/alias, <1.0 for fuzzy
    'expansion_method',   # list, all_counties, all_except, rest_of_state
    'source_release_id',  # From raw data
    'authority_version',  # Census TIGER/Line 2025
    'row_content_hash',   # SHA-256 for determinism
]


# =============================================================================
# Key Normalization (for matching)
# =============================================================================

def normalize_key(name: str, state_fips: Optional[str] = None) -> str:
    """
    Create normalized matching key: ASCII, uppercase, collapsed spaces.
    
    Normalization rules:
    - NFKD decomposition → ASCII (strips diacritics)
    - Uppercase
    - Collapse whitespace to single space
    - Trim leading/trailing space
    - LA-specific: Strip " PARISH" from key (restore in canonical)
    
    Args:
        name: County name to normalize
        state_fips: Optional state FIPS (for state-specific rules)
        
    Returns:
        Normalized key suitable for matching
        
    Examples:
        "Doña Ana" → "DONA ANA"
        "St. Louis" → "ST. LOUIS" (alias map handles St.→Saint)
        "Orleans Parish" (LA) → "ORLEANS" (key), "Orleans Parish" (canonical)
    """
    # NFKD decomposition → ASCII
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ascii', 'ignore').decode('ascii')
    
    # Uppercase, collapse spaces, trim
    clean = ' '.join(ascii_name.upper().split())
    
    # State-specific suffix stripping (for matching only!)
    if state_fips == '22':  # Louisiana
        # Strip " PARISH" from key, but NOT from canonical output
        clean = re.sub(r'\s+PARISH$', '', clean)
    
    return clean


# =============================================================================
# Reference Data Loaders
# =============================================================================

def _compute_authority_fingerprint(counties_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute fingerprint of reference data for drift detection.
    
    Tracks:
    - Total county count
    - By-state distribution
    - By-type distribution
    - GEOID checksum (short hash for quick comparison)
    
    Args:
        counties_df: Full counties reference DataFrame
        
    Returns:
        Dict with fingerprint metrics
        
    Usage:
        Compare fingerprints across runs to detect Census data updates
        Alert if checksum changes (requires re-validation)
    """
    geoid_list = sorted(counties_df['county_geoid'].tolist())
    geoid_checksum = hashlib.sha256(
        '|'.join(geoid_list).encode('utf-8')
    ).hexdigest()[:16]  # First 16 chars for readability
    
    return {
        'total_counties': len(counties_df),
        'by_state_counts': counties_df.groupby('state_fips').size().to_dict(),
        'by_type_counts': counties_df.groupby('county_type').size().to_dict(),
        'geoid_checksum': geoid_checksum,
        'authority_version': 'Census TIGER/Line 2025',
        'authority_date': '2025-09-08',  # Census release date
    }


def load_fips_crosswalk(ref_dir: Optional[Path] = None) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Load FIPS reference data: states, counties, aliases.
    
    Args:
        ref_dir: Root reference data directory
        
    Returns:
        (states_df, counties_df, aliases_dict)
        
    States DataFrame columns:
        - state_fips (str, 2-digit)
        - state_abbr (str, 2-char)
        - state_name (str, uppercase)
        
    Counties DataFrame columns:
        - state_fips (str, 2-digit)
        - county_fips (str, 3-digit)
        - county_geoid (str, 5-digit)
        - county_name (str, for matching - no suffix)
        - county_name_canonical (str, official with suffix/diacritics)
        - county_type (str)
        - county_name_key (str, normalized for matching)
        
    Aliases Dict:
        - 'default': Dict[str, str] - global aliases
        - 'by_state': Dict[str, Dict[str, str]] - state-specific
    """
    # Detect reference data directory
    if ref_dir is None:
        # Try Docker path first, fall back to relative from project root
        docker_ref = Path('/app/data/reference')
        local_ref = Path(__file__).parent.parent.parent.parent / 'data' / 'reference'
        
        if docker_ref.exists():
            ref_dir = docker_ref
        elif local_ref.exists():
            ref_dir = local_ref
        else:
            raise FileNotFoundError(f"Reference data not found at {docker_ref} or {local_ref}")
    
    # Load states
    states_path = ref_dir / 'census/fips_states/2025/us_states.csv'
    states_df = pd.read_csv(states_path, dtype=str)
    states_df['state_name'] = states_df['state_name'].str.upper()
    
    # Load counties
    counties_path = ref_dir / 'census/fips_counties/2025/us_counties.csv'
    counties_df = pd.read_csv(counties_path, dtype=str)
    
    # Add normalized key column for matching
    counties_df['county_name_key'] = counties_df.apply(
        lambda row: normalize_key(row['county_name'], row['state_fips']),
        axis=1
    )
    
    # Load aliases
    aliases_path = ref_dir / 'cms/county_aliases/2025/county_aliases.yml'
    with open(aliases_path, 'r', encoding='utf-8') as f:
        aliases_yaml = yaml.safe_load(f)
    
    aliases = {
        'default': aliases_yaml.get('aliases', {}).get('default', {}),
        'by_state': aliases_yaml.get('aliases', {}).get('by_state', {}),
        'special_cases': aliases_yaml.get('special_cases', []),
    }
    
    logger.info(
        "Loaded FIPS crosswalk",
        states=len(states_df),
        counties=len(counties_df),
        aliases_default=len(aliases['default']),
        aliases_by_state=len(aliases['by_state']),
    )
    
    return states_df, counties_df, aliases


# =============================================================================
# Set-Logic Expansion (ALL COUNTIES, EXCEPT, REST OF)
# =============================================================================

SET_LOGIC_PATTERNS = {
    'all_counties': re.compile(r'^ALL COUNTIES$', re.IGNORECASE),
    'all_except': re.compile(r'^ALL COUNTIES EXCEPT (.+)$', re.IGNORECASE),
    'rest_of': re.compile(r'^REST OF (?:STATE|[A-Z ]+)$', re.IGNORECASE),
}


def detect_set_logic(county_names: str) -> Tuple[str, Optional[str]]:
    """
    Detect set-logic patterns in county names.
    
    Args:
        county_names: Raw county_names field from CMS
        
    Returns:
        (expansion_method, exception_list)
        - expansion_method: 'list' | 'all_counties' | 'all_except' | 'rest_of_state'
        - exception_list: Comma-separated exceptions for 'all_except', else None
        
    Examples:
        "ALL COUNTIES" → ('all_counties', None)
        "ALL COUNTIES EXCEPT LOS ANGELES, ORANGE" → ('all_except', 'LOS ANGELES, ORANGE')
        "REST OF CALIFORNIA" → ('rest_of_state', None)
        "LOS ANGELES/ORANGE" → ('list', None)
    """
    clean = county_names.strip().upper()
    
    # Check ALL COUNTIES
    if SET_LOGIC_PATTERNS['all_counties'].match(clean):
        return 'all_counties', None
    
    # Check ALL EXCEPT
    match = SET_LOGIC_PATTERNS['all_except'].match(clean)
    if match:
        exceptions = match.group(1).strip()
        return 'all_except', exceptions
    
    # Check REST OF
    if SET_LOGIC_PATTERNS['rest_of'].match(clean):
        return 'rest_of_state', None
    
    # Default: standard list
    return 'list', None


def expand_all_counties(state_fips: str, counties_df: pd.DataFrame) -> List[str]:
    """
    Get all county names for a state.
    
    Args:
        state_fips: 2-digit state FIPS
        counties_df: Full counties reference DataFrame
        
    Returns:
        List of county names (for matching, without suffix)
    """
    state_counties = counties_df[counties_df['state_fips'] == state_fips]
    return state_counties['county_name'].tolist()


def expand_all_except(state_fips: str, exception_str: str, counties_df: pd.DataFrame) -> List[str]:
    """
    Get all counties for state EXCEPT those in exception list.
    
    Args:
        state_fips: 2-digit state FIPS
        exception_str: Comma/slash-delimited exceptions (e.g., "LOS ANGELES, ORANGE")
        counties_df: Full counties reference DataFrame
        
    Returns:
        List of county names (all minus exceptions)
    """
    all_counties = expand_all_counties(state_fips, counties_df)
    
    # Parse exceptions (handle both commas and slashes)
    exceptions_raw = re.split(r'[,/]', exception_str)
    exceptions = [normalize_key(e.strip(), state_fips) for e in exceptions_raw if e.strip()]
    
    # Filter out exceptions
    # Match against normalized keys
    counties_with_keys = counties_df[counties_df['state_fips'] == state_fips].copy()
    counties_with_keys['key'] = counties_with_keys['county_name_key']
    
    remaining = counties_with_keys[~counties_with_keys['key'].isin(exceptions)]
    
    return remaining['county_name'].tolist()


def expand_rest_of_state(state_fips: str, locality_code: str, raw_df: pd.DataFrame, counties_df: pd.DataFrame) -> List[str]:
    """
    Get all counties NOT explicitly listed in other localities for this state.
    
    This is complex: requires identifying all counties already assigned to
    other localities in the same state, then returning the remainder.
    
    Args:
        state_fips: 2-digit state FIPS
        locality_code: Current locality code (to exclude self)
        raw_df: Full raw DataFrame (to find explicit localities)
        counties_df: Full counties reference DataFrame
        
    Returns:
        List of county names (REST OF STATE)
        
    Note:
        This is vintage-specific and requires parsing all other localities
        in the same state. For MVP, we'll return all counties and log a warning.
    """
    logger.warning(
        "REST OF STATE expansion is not fully implemented (requires full locality map)",
        state_fips=state_fips,
        locality_code=locality_code,
    )
    
    # For now: return all counties (will be refined in future)
    # TODO(alex, GH-33): Implement proper REST OF logic by parsing all localities for state
    # Requires vintage-specific locality map to compute explicit counties already assigned
    return expand_all_counties(state_fips, counties_df)


# =============================================================================
# County List Explosion
# =============================================================================

def explode_county_list(county_names: str) -> List[str]:
    """
    Split county names on delimiters (comma, slash, AND).
    
    IMPORTANT: Do NOT split on hyphens (e.g., Miami-Dade is ONE county)
    
    Args:
        county_names: Raw county list (e.g., "LOS ANGELES/ORANGE, SAN DIEGO")
        
    Returns:
        List of individual county names
        
    Examples:
        "LOS ANGELES/ORANGE" → ["LOS ANGELES", "ORANGE"]
        "LOS ANGELES, ORANGE" → ["LOS ANGELES", "ORANGE"]
        "DADE AND MONROE" → ["DADE", "MONROE"]
        "MIAMI-DADE" → ["MIAMI-DADE"] (NOT split!)
    """
    # Normalize delimiters: / and " AND " → ,
    normalized = county_names.replace('/', ',')
    normalized = re.sub(r'\s+AND\s+', ',', normalized, flags=re.IGNORECASE)
    
    # Split on comma
    counties = [c.strip() for c in normalized.split(',')]
    
    # Remove empties
    return [c for c in counties if c]


# =============================================================================
# Matching Pipeline (Exact → Alias → Fuzzy)
# =============================================================================

def match_exact(county_key: str, state_fips: str, counties_df: pd.DataFrame, fee_area_hint: Optional[str] = None) -> Optional[Tuple[str, str, str, str]]:
    """
    Exact match on (state_fips, county_name_key) with LSAD tie-breaking.
    
    Args:
        county_key: Normalized county name key
        state_fips: 2-digit state FIPS
        counties_df: Full counties reference DataFrame
        fee_area_hint: Optional fee_area text for disambiguation hints
        
    Returns:
        (county_fips, county_geoid, county_name_canonical, county_type) or None
        
    Tie-breaking:
        If multiple matches (e.g., St. Louis County vs St. Louis city),
        prefer by LSAD type:
        1. If fee_area contains "CITY" → prefer Independent City
        2. Otherwise → prefer County (most common)
        3. Then Parish, Borough, Census Area, etc.
    """
    matches = counties_df[
        (counties_df['state_fips'] == state_fips) &
        (counties_df['county_name_key'] == county_key)
    ]
    
    if len(matches) == 0:
        return None
    
    if len(matches) == 1:
        row = matches.iloc[0]
        return (row['county_fips'], row['county_geoid'], row['county_name_canonical'], row['county_type'])
    
    # Multiple matches - apply LSAD tie-breaking
    logger.info(
        "Multiple exact matches found, applying LSAD tie-break",
        county_key=county_key,
        state_fips=state_fips,
        count=len(matches),
        types=matches['county_type'].tolist(),
    )
    
    # Check fee_area hint for "CITY"
    hint_city = fee_area_hint and "CITY" in fee_area_hint.upper()
    
    # Define preference order
    if hint_city:
        # Prefer Independent City if hint suggests it
        preference = ['Independent City', 'City', 'County', 'Parish', 'Borough', 'Census Area', 'Municipality']
    else:
        # Default: prefer County (most common)
        preference = ['County', 'Parish', 'Borough', 'Census Area', 'Municipality', 'Independent City']
    
    # Apply preference
    for county_type in preference:
        type_matches = matches[matches['county_type'] == county_type]
        if len(type_matches) == 1:
            row = type_matches.iloc[0]
            logger.info(
                "LSAD tie-break resolved",
                county_key=county_key,
                chosen_type=county_type,
                county_fips=row['county_fips'],
            )
            return (row['county_fips'], row['county_geoid'], row['county_name_canonical'], row['county_type'])
        elif len(type_matches) > 1:
            # Still ambiguous within same type - take first
            row = type_matches.iloc[0]
            logger.warning(
                "LSAD tie-break still ambiguous, taking first",
                county_key=county_key,
                county_type=county_type,
                candidates=type_matches[['county_fips', 'county_name_canonical']].to_dict('records'),
            )
            return (row['county_fips'], row['county_geoid'], row['county_name_canonical'], row['county_type'])
    
    # No preference matched - take first
    row = matches.iloc[0]
    logger.warning(
        "LSAD tie-break failed, taking first match",
        county_key=county_key,
        candidates=matches[['county_fips', 'county_type', 'county_name_canonical']].to_dict('records'),
    )
    return (row['county_fips'], row['county_geoid'], row['county_name_canonical'], row['county_type'])


def apply_aliases(county_key: str, state_fips: str, aliases: Dict) -> str:
    """
    Apply alias transformations to county key.
    
    Order:
    1. Global default aliases (normalized)
    2. State-specific aliases (if state_fips in by_state)
    
    Args:
        county_key: Normalized county key (already uppercase)
        state_fips: 2-digit state FIPS
        aliases: Aliases dict from load_fips_crosswalk
        
    Returns:
        Transformed key (or original if no alias)
        
    Examples:
        "ST. LOUIS" → "SAINT LOUIS" (via default alias ST. → SAINT)
        "RICHMOND CITY" (VA) → "RICHMOND" (via state-specific)
    """
    transformed = county_key
    
    # Apply default aliases with normalized keys
    # Normalize both alias keys and values for consistent matching
    for old, new in aliases.get('default', {}).items():
        old_normalized = old.upper().strip()
        new_normalized = new.upper().strip()
        transformed = transformed.replace(old_normalized, new_normalized)
    
    # Apply state-specific aliases
    state_aliases = aliases.get('by_state', {}).get(state_fips, {})
    if isinstance(state_aliases, dict):
        # Look for exact match or partial replacement
        # First try exact match
        for alias_key, alias_value in state_aliases.items():
            if isinstance(alias_key, str) and isinstance(alias_value, str):
                alias_key_norm = alias_key.upper().strip()
                alias_value_norm = alias_value.upper().strip()
                if transformed == alias_key_norm:
                    transformed = alias_value_norm
                    break
                # Try partial replacement (for suffix handling)
                elif alias_key_norm in transformed:
                    transformed = transformed.replace(alias_key_norm, alias_value_norm)
    
    return transformed


def match_alias(county_key: str, state_fips: str, counties_df: pd.DataFrame, aliases: Dict, fee_area_hint: Optional[str] = None) -> Optional[Tuple[str, str, str, str]]:
    """
    Match after applying aliases.
    
    Args:
        county_key: Normalized county name key
        state_fips: 2-digit state FIPS
        counties_df: Full counties reference DataFrame
        aliases: Aliases dict
        fee_area_hint: Optional fee_area text for disambiguation
        
    Returns:
        (county_fips, county_geoid, county_name_canonical, county_type) or None
    """
    # Apply aliases
    transformed_key = apply_aliases(county_key, state_fips, aliases)
    
    # Try exact match on transformed key
    if transformed_key != county_key:
        return match_exact(transformed_key, state_fips, counties_df, fee_area_hint)
    
    return None


def match_fuzzy(county_key: str, state_fips: str, counties_df: pd.DataFrame, threshold: float = 0.95) -> Optional[Tuple[str, str, str, str, float]]:
    """
    Fuzzy match with guardrails.
    
    Guardrails:
    - Threshold: score ≥ 95%
    - Tie-break: Top 2 must differ by ≥2 points
    - Quarantine ambiguous matches
    
    Args:
        county_key: Normalized county name key
        state_fips: 2-digit state FIPS
        counties_df: Full counties reference DataFrame
        threshold: Minimum score (0-1)
        
    Returns:
        (county_fips, county_geoid, county_name_canonical, county_type, score) or None
        
    Note:
        Returns None if no match, or if ambiguous (top 2 within 2 points)
    """
    if not FUZZY_AVAILABLE:
        return None
    
    # Get all counties for this state
    state_counties = counties_df[counties_df['state_fips'] == state_fips]
    
    # Compute scores
    scores = []
    for _, row in state_counties.iterrows():
        score = fuzz.ratio(county_key, row['county_name_key']) / 100.0
        if score >= threshold:
            scores.append({
                'county_fips': row['county_fips'],
                'county_geoid': row['county_geoid'],
                'county_name_canonical': row['county_name_canonical'],
                'county_type': row['county_type'],
                'score': score,
                'key': row['county_name_key'],
            })
    
    if len(scores) == 0:
        return None
    
    # Sort by score desc
    scores.sort(key=lambda x: -x['score'])
    
    # Check for ambiguity: top 2 differ by < 2 points (0.02)
    if len(scores) >= 2 and (scores[0]['score'] - scores[1]['score']) < 0.02:
        logger.warning(
            "Ambiguous fuzzy match (quarantined)",
            county_key=county_key,
            state_fips=state_fips,
            top_2_candidates=[
                {'key': scores[0]['key'], 'score': scores[0]['score']},
                {'key': scores[1]['key'], 'score': scores[1]['score']},
            ]
        )
        return None  # Quarantine
    
    # Return top match
    best = scores[0]
    return (best['county_fips'], best['county_geoid'], best['county_name_canonical'], best['county_type'], best['score'])


# =============================================================================
# Main Normalization Function
# =============================================================================

def normalize_locality_fips(
    raw_df: pd.DataFrame,
    ref_dir: Optional[Path] = None,
    use_fuzzy: bool = False,
    source_release_id: str = 'LOCCO_2025D',
) -> NormalizeResult:
    """
    Normalize raw locality data: county NAMES → FIPS codes.
    
    Two-stage pipeline:
      Stage 1 (Raw Parser): Parse CMS file as-is
      Stage 2 (This function): Derive FIPS & explode to one-row-per-county
    
    Args:
        raw_df: Raw locality DataFrame from Stage 1 parser
                Required columns: mac, locality_code, state_name, county_names
        ref_dir: Reference data root directory
        use_fuzzy: Enable fuzzy matching (requires rapidfuzz)
        source_release_id: Release identifier for provenance
        
    Returns:
        NormalizeResult with data, quarantine, metrics
        
    Output Schema:
        - locality_code (str, 2-digit, zero-padded)
        - mac (str, 5-digit, zero-padded)
        - state_fips (str, 2-digit)
        - county_fips (str, 3-digit)
        - county_geoid (str, 5-digit)
        - county_name (str, for matching)
        - county_name_canonical (str, official Census name)
        - county_type (str)
        - match_method (str)
        - mapping_confidence (float)
        - expansion_method (str)
        - source_release_id (str)
        - authority_version (str)
        - row_content_hash (str)
        
    Natural Key: (locality_code, state_fips, county_fips)
    """
    logger.info("Starting FIPS normalization", raw_rows=len(raw_df))
    
    # Load reference data
    states_df, counties_df, aliases = load_fips_crosswalk(ref_dir)
    authority_version = "Census TIGER/Line 2025"
    
    # Create state name → state_fips mapping
    state_map = dict(zip(states_df['state_name'].str.upper(), states_df['state_fips']))
    
    # Compute authority fingerprint for drift detection
    authority_fingerprint = _compute_authority_fingerprint(counties_df)
    
    # Initialize outputs
    normalized_rows = []
    quarantine_rows = []
    
    # Metrics
    metrics = {
        'total_rows_in': len(raw_df),
        'match_methods': {'exact': 0, 'alias': 0, 'fuzzy': 0, 'unknown_state': 0, 'unknown_county': 0},
        'expansion_methods': {'list': 0, 'all_counties': 0, 'all_except': 0, 'rest_of_state': 0},
        'total_rows_exploded': 0,
        'total_rows_out': 0,
        'rows_quarantined': 0,
        'coverage_by_state': {},
        'authority_version': authority_version,
        'authority_fingerprint': authority_fingerprint,
    }
    
    # Process each raw row
    for _, raw_row in raw_df.iterrows():
        mac = raw_row['mac']
        locality_code = raw_row['locality_code']
        state_name = raw_row['state_name'].strip().upper()
        county_names_raw = raw_row['county_names']
        fee_area = raw_row.get('fee_area', '')  # Optional hint for disambiguation
        
        # Derive state_fips
        state_fips = state_map.get(state_name)
        if not state_fips:
            logger.warning("Unknown state", state_name=state_name)
            quarantine_rows.append({
                'mac': mac,
                'locality_code': locality_code,
                'state_name': state_name,
                'county_names': county_names_raw,
                'reason': 'unknown_state',
            })
            metrics['match_methods']['unknown_state'] += 1
            continue
        
        # Detect set-logic
        expansion_method, exception_str = detect_set_logic(county_names_raw)
        metrics['expansion_methods'][expansion_method] += 1
        
        # Expand set-logic
        if expansion_method == 'all_counties':
            county_list = expand_all_counties(state_fips, counties_df)
        elif expansion_method == 'all_except':
            county_list = expand_all_except(state_fips, exception_str, counties_df)
        elif expansion_method == 'rest_of_state':
            county_list = expand_rest_of_state(state_fips, locality_code, raw_df, counties_df)
        else:  # 'list'
            county_list = explode_county_list(county_names_raw)
        
        metrics['total_rows_exploded'] += len(county_list)
        
        # Match each county
        for county_name_raw in county_list:
            county_key = normalize_key(county_name_raw, state_fips)
            
            # Try exact match (with fee_area hint for disambiguation)
            match_result = match_exact(county_key, state_fips, counties_df, fee_area)
            if match_result:
                county_fips, county_geoid, county_name_canonical, county_type = match_result
                match_method = 'exact'
                mapping_confidence = 1.0
                metrics['match_methods']['exact'] += 1
            else:
                # Try alias match (with fee_area hint)
                match_result = match_alias(county_key, state_fips, counties_df, aliases, fee_area)
                if match_result:
                    county_fips, county_geoid, county_name_canonical, county_type = match_result
                    match_method = 'alias'
                    mapping_confidence = 1.0
                    metrics['match_methods']['alias'] += 1
                else:
                    # Try fuzzy match (if enabled)
                    if use_fuzzy:
                        fuzzy_result = match_fuzzy(county_key, state_fips, counties_df)
                        if fuzzy_result:
                            county_fips, county_geoid, county_name_canonical, county_type, score = fuzzy_result
                            match_method = 'fuzzy'
                            mapping_confidence = score
                            metrics['match_methods']['fuzzy'] += 1
                        else:
                            # No match - quarantine
                            quarantine_rows.append({
                                'mac': mac,
                                'locality_code': locality_code,
                                'state_fips': state_fips,
                                'state_name': state_name,
                                'county_name_raw': county_name_raw,
                                'county_key': county_key,
                                'reason': 'no_fuzzy_match',
                            })
                            metrics['match_methods']['unknown_county'] += 1
                            continue
                    else:
                        # No match and fuzzy disabled - quarantine
                        quarantine_rows.append({
                            'mac': mac,
                            'locality_code': locality_code,
                            'state_fips': state_fips,
                            'state_name': state_name,
                            'county_name_raw': county_name_raw,
                            'county_key': county_key,
                            'reason': 'no_match',
                        })
                        metrics['match_methods']['unknown_county'] += 1
                        continue
            
            # Add normalized row
            normalized_rows.append({
                'locality_code': locality_code,
                'mac': mac,
                'state_fips': state_fips,
                'county_fips': county_fips,
                'county_geoid': county_geoid,
                'county_name': county_name_raw.strip(),
                'county_name_canonical': county_name_canonical,
                'county_type': county_type,
                'match_method': match_method,
                'mapping_confidence': mapping_confidence,
                'expansion_method': expansion_method,
                'source_release_id': source_release_id,
                'authority_version': authority_version,
            })
    
    # Build DataFrames
    if normalized_rows:
        df_norm = pd.DataFrame(normalized_rows)
        
        # Zero-pad codes
        df_norm['mac'] = df_norm['mac'].str.zfill(5)
        df_norm['locality_code'] = df_norm['locality_code'].str.zfill(2)
        df_norm['state_fips'] = df_norm['state_fips'].str.zfill(2)
        df_norm['county_fips'] = df_norm['county_fips'].str.zfill(3)
        
        # Sort by natural key
        df_norm = df_norm.sort_values(NK_PRIMARY).reset_index(drop=True)
        
        # Compute row hashes
        df_norm['row_content_hash'] = df_norm.apply(
            lambda row: hashlib.sha256(
                '|'.join([
                    str(row['locality_code']),
                    str(row['state_fips']),
                    str(row['county_fips']),
                    str(row['county_name_canonical']),
                    str(row['match_method']),
                ]).encode('utf-8')
            ).hexdigest(),
            axis=1
        )
        
        # Check NK uniqueness
        duplicates = df_norm[df_norm.duplicated(subset=NK_PRIMARY, keep=False)]
        if len(duplicates) > 0:
            logger.error(
                "Natural key duplicates detected!",
                count=len(duplicates),
                examples=duplicates.head(3)[NK_PRIMARY].to_dict('records')
            )
        
        metrics['total_rows_out'] = len(df_norm)
    else:
        df_norm = pd.DataFrame(columns=OUTPUT_COLUMNS)
        metrics['total_rows_out'] = 0
    
    if quarantine_rows:
        df_quarantine = pd.DataFrame(quarantine_rows)
        metrics['rows_quarantined'] = len(df_quarantine)
    else:
        df_quarantine = pd.DataFrame()
        metrics['rows_quarantined'] = 0
    
    logger.info(
        "FIPS normalization complete",
        rows_in=metrics['total_rows_in'],
        rows_exploded=metrics['total_rows_exploded'],
        rows_out=metrics['total_rows_out'],
        rows_quarantined=metrics['rows_quarantined'],
        match_methods=metrics['match_methods'],
        expansion_methods=metrics['expansion_methods'],
    )
    
    return NormalizeResult(
        data=df_norm,
        quarantine=df_quarantine,
        metrics=metrics,
    )

