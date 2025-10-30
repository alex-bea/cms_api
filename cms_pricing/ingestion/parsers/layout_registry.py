"""
Fixed-width layout registry (SemVer by year/quarter).

Externalizes column specifications for fixed-width CMS files.
Enables detection of breaking changes via version tracking.

Per PRD refinement #3: Layout registry SemVer'd by year/quarter.
Tests detect when column width changes break parsing.

Layout versions follow SemVer:
  - v{year}.{quarter}.{patch}
  - Example: v2025.4.0 for 2025 Q4 (revision D)
  
Breaking changes (require major version bump):
  - Column width changes
  - Column position changes
  - Required field additions/removals
  
Compatible changes (patch version):
  - Optional field additions
  - Description updates
"""

from typing import Dict, Any, Optional
from decimal import Decimal
import structlog

logger = structlog.get_logger()


# ===================================================================
# PPRRVU LAYOUTS
# ===================================================================

PPRRVU_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Bump: Aligned column names with schema contract (rvu_* prefix)
    'min_line_length': 165,  # Actual data lines are ~173 chars (was 200, too strict)
    'source_version': '2025D',
    'columns': {
        # Core identifiers (schema-aligned)
        'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'modifier': {'start': 5, 'end': 7, 'type': 'string', 'nullable': True},  # Added (natural key!)
        'description': {'start': 7, 'end': 57, 'type': 'string', 'nullable': True},
        'status_code': {'start': 57, 'end': 58, 'type': 'string', 'nullable': False},
        
        # RVU columns (RENAMED to match schema: rvu_* prefix)
        'rvu_work': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},  # was work_rvu
        'rvu_pe_nonfac': {'start': 68, 'end': 72, 'type': 'decimal', 'nullable': True},  # was pe_rvu_nonfac
        'rvu_pe_fac': {'start': 77, 'end': 81, 'type': 'decimal', 'nullable': True},  # was pe_rvu_fac
        'rvu_malp': {'start': 85, 'end': 89, 'type': 'decimal', 'nullable': True},  # was mp_rvu
        
        # Additional fields (kept for completeness, not all in schema)
        'na_indicator': {'start': 92, 'end': 93, 'type': 'string', 'nullable': True},
        'bilateral_ind': {'start': 103, 'end': 104, 'type': 'string', 'nullable': True},
        'multiple_proc_ind': {'start': 104, 'end': 105, 'type': 'string', 'nullable': True},
        'assistant_surg_ind': {'start': 105, 'end': 106, 'type': 'string', 'nullable': True},
        'co_surg_ind': {'start': 106, 'end': 107, 'type': 'string', 'nullable': True},
        'team_surg_ind': {'start': 107, 'end': 108, 'type': 'string', 'nullable': True},
        'endoscopic_base': {'start': 108, 'end': 109, 'type': 'string', 'nullable': True},
        'conversion_factor': {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': True},
        'global_days': {'start': 140, 'end': 143, 'type': 'string', 'nullable': True},
        'physician_supervision': {'start': 144, 'end': 146, 'type': 'string', 'nullable': True},
        'diag_imaging_family': {'start': 147, 'end': 148, 'type': 'string', 'nullable': True},
        'total_nonfac': {'start': 153, 'end': 157, 'type': 'decimal', 'nullable': True},
        'total_fac': {'start': 160, 'end': 164, 'type': 'decimal', 'nullable': True},
        'opps_cap_applicable': {'start': 91, 'end': 92, 'type': 'string', 'nullable': True},  # Added (schema field)
        # Note: effective_from not in fixed-width file - injected from metadata in parser
    }
}

# ===================================================================
# GPCI LAYOUTS
# ===================================================================

GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Patch bump: CMS-native names + corrected positions
    'source_version': '2025D',
    'min_line_length': 100,  # Actual data=150; conservative with margin
    'data_start_pattern': r'^\d{5}',  # MAC code (5 digits) at line start
    'columns': {
        # Core schema columns (CMS-native names, schema v1.2)
        'locality_code': {'start': 24, 'end': 26, 'type': 'string', 'nullable': False},
        'gpci_work':     {'start': 121, 'end': 126, 'type': 'decimal', 'nullable': False},
        'gpci_pe':       {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': False},
        'gpci_mp':       {'start': 145, 'end': 150, 'type': 'decimal', 'nullable': False},
        
        # Optional enrichment columns (excluded from hash)
        'mac':           {'start': 0, 'end': 5, 'type': 'string', 'nullable': True},
        'state':         {'start': 16, 'end': 18, 'type': 'string', 'nullable': True},
        'locality_name': {'start': 28, 'end': 78, 'type': 'string', 'nullable': True},
    }
}

# ===================================================================
# OPPSCAP LAYOUTS
# ===================================================================

OPPSCAP_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 40,
    'source_version': '2025D',
    'columns': {
        'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'modifier': {'start': 5, 'end': 7, 'type': 'string', 'nullable': True},
        'status': {'start': 7, 'end': 9, 'type': 'string', 'nullable': False},
        'mac': {'start': 9, 'end': 15, 'type': 'string', 'nullable': False},
        'locality_code': {'start': 15, 'end': 20, 'type': 'string', 'nullable': False},
        'facility_price': {'start': 20, 'end': 28, 'type': 'decimal', 'nullable': False},
        'nonfacility_price': {'start': 28, 'end': 36, 'type': 'decimal', 'nullable': False},
    }
}

# ===================================================================
# ANES LAYOUTS
# ===================================================================

ANES_2025D_LAYOUT = {
    'version': 'v2025.4.1',  # Fixed CF column positions
    'min_line_length': 75,
    'source_version': '2025D',
    'columns': {
        'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'locality_id': {'start': 12, 'end': 14, 'type': 'string', 'nullable': False},  # Parser maps to locality_code
        'locality_name': {'start': 17, 'end': 57, 'type': 'string', 'nullable': True},
        'anesthesia_cf': {'start': 73, 'end': 77, 'type': 'decimal', 'nullable': False},  # Parser maps to anesthesia_cf_raw then scales to anesthesia_cf_usd
    }
}

# ===================================================================
# LOCALITY-COUNTY LAYOUTS
# ===================================================================

LOCCO_2025D_LAYOUT = {
    'version': 'v2025.4.2',  # FIXED: Correct column positions from actual file analysis
    'min_line_length': 120,  # Min line to reach counties
    'source_version': '2025D',
    'data_start_pattern': r'^\s+\d{5}',  # MAC code (5 digits) after leading spaces
    'columns': {
        # Columns (0-based indices, verified from actual CMS file)
        'mac': {'start': 0, 'end': 12, 'type': 'string', 'nullable': False},             # Cols 1-12 (5-digit MAC)
        'locality_code': {'start': 12, 'end': 18, 'type': 'string', 'nullable': False},  # Cols 13-18 (2-digit locality)
        'state_name': {'start': 18, 'end': 50, 'type': 'string', 'nullable': True},      # Cols 19-50 (may be blank)
        'fee_area': {'start': 50, 'end': 120, 'type': 'string', 'nullable': True},       # Cols 51-120 (locality name, informational)
        'county_names': {'start': 120, 'end': None, 'type': 'string', 'nullable': True}, # Cols 121+ (rest of line)
    },
    'notes': [
        'State name may be blank on continuation rows (forward-fill during parse)',
        'County names are comma- or slash-delimited (not split in raw parser)',
        'Header rows contain "Medicare Admi" or "Locality" - skip these',
    ]
}

# ===================================================================
# LAYOUT REGISTRY (SemVer by year/quarter)
# ===================================================================

LAYOUT_REGISTRY = {
    # Format: (dataset, year, quarter) -> layout
    # Quarter notation: CMS letters (A=Q1, B=Q2, C=Q3, D=Q4)
    # Matches CMS file naming convention (e.g., RVU25D, GPCI25C)
    
    # PPRRVU layouts (D = October/Q4 release)
    ('pprrvu', '2025', 'D'): PPRRVU_2025D_LAYOUT,
    ('pprrvu', '2025', 'C'): PPRRVU_2025D_LAYOUT,  # Same layout for July/Q3
    ('pprrvu', '2025', 'B'): PPRRVU_2025D_LAYOUT,  # Same layout for Apr/Q2
    ('pprrvu', '2025', 'A'): PPRRVU_2025D_LAYOUT,  # Same layout for Jan/Q1
    
    # GPCI layouts (all quarters use same 2025D layout)
    ('gpci', '2025', 'A'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'B'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'C'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', 'D'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', None): GPCI_2025D_LAYOUT,  # Annual fallback
    
    # OPPSCAP layouts (D = October/Q4)
    ('oppscap', '2025', 'D'): OPPSCAP_2025D_LAYOUT,
    ('oppscap', '2025', 'C'): OPPSCAP_2025D_LAYOUT,
    
    # ANES layouts (D = October/Q4)
    ('anes', '2025', 'D'): ANES_2025D_LAYOUT,
    ('anes', '2025', None): ANES_2025D_LAYOUT,  # Annual
    
    # Locality layouts (D = October/Q4)
    ('locco', '2025', 'D'): LOCCO_2025D_LAYOUT,
    ('locco', '2025', None): LOCCO_2025D_LAYOUT,  # Annual
}


def get_layout(
    product_year: str,
    quarter_vintage: str,
    dataset: str
) -> Optional[Dict[str, Any]]:
    """
    Get layout specification for dataset and vintage.
    
    Args:
        product_year: Year (e.g., "2025")
        quarter_vintage: Quarter in CMS letter format (A/B/C/D) or Q-notation (Q1/Q2/Q3/Q4)
        dataset: Dataset type (e.g., "pprrvu", "gpci")
    
    Returns:
        Layout dict with version, columns, min_line_length or None
        
    Raises:
        ValueError: If no layout found and dataset requires fixed-width parsing
        
    Note:
        Accepts multiple quarter formats for backward compatibility:
        - CMS letters: "A", "B", "C", "D" (preferred, matches CMS file naming)
        - Q-notation: "Q1", "Q2", "Q3", "Q4"
        - Composite: "2025Q4", "2025_Q4"
        All formats are normalized to CMS letters for registry lookup.
    """
    # Normalize quarter_vintage to CMS letter format (A/B/C/D)
    quarter = None
    if quarter_vintage:
        # Direct CMS letter notation (A, B, C, D) - preferred
        if quarter_vintage in ['A', 'B', 'C', 'D']:
            quarter = quarter_vintage
        # Q-notation (Q1, Q2, Q3, Q4) - backward compatible
        elif quarter_vintage in ['Q1', 'Q2', 'Q3', 'Q4']:
            quarter_map = {'Q1': 'A', 'Q2': 'B', 'Q3': 'C', 'Q4': 'D'}
            quarter = quarter_map[quarter_vintage]
        # Composite format (2025Q4, 2025_Q4) - extract and map
        elif 'Q' in quarter_vintage:
            q_part = quarter_vintage.split('Q')[-1].strip('_')
            quarter_map = {'1': 'A', '2': 'B', '3': 'C', '4': 'D'}
            quarter = quarter_map.get(q_part, None)
        # Unknown format - leave as None (annual fallback)
        else:
            quarter = None
    
    # Try specific quarter first
    key = (dataset, product_year, quarter)
    layout = LAYOUT_REGISTRY.get(key)
    
    if layout:
        logger.debug(
            "Found layout",
            dataset=dataset,
            year=product_year,
            quarter=quarter,
            version=layout['version']
        )
        return layout
    
    # Fallback to annual layout
    key = (dataset, product_year, None)
    layout = LAYOUT_REGISTRY.get(key)
    
    if layout:
        logger.debug(
            "Using annual layout",
            dataset=dataset,
            year=product_year,
            version=layout['version']
        )
        return layout
    
    # No layout found
    logger.warning(
        "No layout found",
        dataset=dataset,
        year=product_year,
        quarter=quarter
    )
    return None


def parse_fixed_width_record(line: str, layout: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a fixed-width line using layout specification.
    
    Args:
        line: Fixed-width text line
        layout: Layout specification dict
        
    Returns:
        Dictionary of parsed fields
        
    Raises:
        ValueError: If required field is empty or invalid
    """
    result = {}
    columns = layout['columns']
    
    for field_name, field_spec in columns.items():
        start = field_spec['start']
        end = field_spec['end']
        
        # Extract field value
        if end > len(line):
            # Line too short
            if not field_spec['nullable']:
                raise ValueError(
                    f"Line too short for required field {field_name}: "
                    f"need {end} chars, got {len(line)}"
                )
            field_value = ""
        else:
            field_value = line[start:end].strip()
        
        # Handle null values
        if not field_value:
            if field_spec['nullable']:
                result[field_name] = None
            else:
                raise ValueError(f"Required field {field_name} is empty")
            continue
        
        # Type conversion
        if field_spec['type'] == 'decimal':
            try:
                result[field_name] = Decimal(field_value)
            except:
                if field_spec['nullable']:
                    result[field_name] = None
                else:
                    raise ValueError(
                        f"Invalid decimal for {field_name}: {field_value}"
                    )
        elif field_spec['type'] == 'string':
            result[field_name] = field_value
        else:
            result[field_name] = field_value
    
    return result


def get_layout_version(product_year: str, quarter_vintage: str, dataset: str) -> Optional[str]:
    """
    Get SemVer version string for a layout.
    
    Args:
        product_year: Year
        quarter_vintage: Quarter vintage
        dataset: Dataset type
        
    Returns:
        SemVer version string (e.g., "v2025.4.0") or None
    """
    layout = get_layout(product_year, quarter_vintage, dataset)
    return layout['version'] if layout else None


def list_available_layouts() -> Dict[str, list]:
    """
    List all available layouts by dataset.
    
    Returns:
        Dictionary mapping dataset names to list of (year, quarter, version)
    """
    layouts_by_dataset = {}
    
    for (dataset, year, quarter), layout in LAYOUT_REGISTRY.items():
        if dataset not in layouts_by_dataset:
            layouts_by_dataset[dataset] = []
        
        layouts_by_dataset[dataset].append({
            'year': year,
            'quarter': quarter or 'annual',
            'version': layout['version']
        })
    
    return layouts_by_dataset

