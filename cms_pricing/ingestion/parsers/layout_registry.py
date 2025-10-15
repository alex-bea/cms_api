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
    'version': 'v2025.4.0',  # SemVer: year.quarter.patch
    'min_line_length': 200,
    'source_version': '2025D',
    'columns': {
        'hcpcs': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'description': {'start': 6, 'end': 57, 'type': 'string', 'nullable': True},
        'status_code': {'start': 57, 'end': 58, 'type': 'string', 'nullable': False},
        'work_rvu': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},
        'pe_rvu_nonfac': {'start': 68, 'end': 72, 'type': 'decimal', 'nullable': True},
        'pe_rvu_fac': {'start': 77, 'end': 81, 'type': 'decimal', 'nullable': True},
        'mp_rvu': {'start': 85, 'end': 89, 'type': 'decimal', 'nullable': True},
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
    }
}

# ===================================================================
# GPCI LAYOUTS
# ===================================================================

GPCI_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 150,
    'source_version': '2025D',
    'columns': {
        'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'state': {'start': 15, 'end': 17, 'type': 'string', 'nullable': False},
        'locality_id': {'start': 24, 'end': 26, 'type': 'string', 'nullable': False},
        'locality_name': {'start': 25, 'end': 75, 'type': 'string', 'nullable': True},
        'work_gpci': {'start': 120, 'end': 126, 'type': 'decimal', 'nullable': False},
        'pe_gpci': {'start': 133, 'end': 139, 'type': 'decimal', 'nullable': False},
        'mp_gpci': {'start': 140, 'end': 146, 'type': 'decimal', 'nullable': False},
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
        'proc_status': {'start': 8, 'end': 9, 'type': 'string', 'nullable': False},
        'mac': {'start': 10, 'end': 15, 'type': 'string', 'nullable': False},
        'locality_id': {'start': 17, 'end': 19, 'type': 'string', 'nullable': False},
        'price_fac': {'start': 22, 'end': 28, 'type': 'decimal', 'nullable': False},
        'price_nonfac': {'start': 32, 'end': 38, 'type': 'decimal', 'nullable': False},
    }
}

# ===================================================================
# ANES LAYOUTS
# ===================================================================

ANES_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 75,
    'source_version': '2025D',
    'columns': {
        'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'locality_id': {'start': 12, 'end': 14, 'type': 'string', 'nullable': False},
        'locality_name': {'start': 17, 'end': 57, 'type': 'string', 'nullable': True},
        'anesthesia_cf': {'start': 70, 'end': 74, 'type': 'decimal', 'nullable': False},
    }
}

# ===================================================================
# LOCALITY-COUNTY LAYOUTS
# ===================================================================

LOCCO_2025D_LAYOUT = {
    'version': 'v2025.4.0',
    'min_line_length': 120,
    'source_version': '2025D',
    'columns': {
        'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
        'locality_id': {'start': 12, 'end': 14, 'type': 'string', 'nullable': False},
        'state': {'start': 17, 'end': 25, 'type': 'string', 'nullable': False},
        'fee_schedule_area': {'start': 26, 'end': 76, 'type': 'string', 'nullable': True},
        'county_name': {'start': 120, 'end': 151, 'type': 'string', 'nullable': True},
    }
}

# ===================================================================
# LAYOUT REGISTRY (SemVer by year/quarter)
# ===================================================================

LAYOUT_REGISTRY = {
    # Format: (dataset, year, quarter) -> layout
    ('pprrvu', '2025', 'Q4'): PPRRVU_2025D_LAYOUT,
    ('pprrvu', '2025', 'Q3'): PPRRVU_2025D_LAYOUT,  # Same layout for Q3
    ('pprrvu', '2025', 'Q2'): PPRRVU_2025D_LAYOUT,  # Same layout for Q2
    ('pprrvu', '2025', 'Q1'): PPRRVU_2025D_LAYOUT,  # Same layout for Q1
    
    ('gpci', '2025', 'Q4'): GPCI_2025D_LAYOUT,
    ('gpci', '2025', None): GPCI_2025D_LAYOUT,  # Annual
    
    ('oppscap', '2025', 'Q4'): OPPSCAP_2025D_LAYOUT,
    ('oppscap', '2025', 'Q3'): OPPSCAP_2025D_LAYOUT,
    
    ('anes', '2025', 'Q4'): ANES_2025D_LAYOUT,
    ('anes', '2025', None): ANES_2025D_LAYOUT,  # Annual
    
    ('locco', '2025', 'Q4'): LOCCO_2025D_LAYOUT,
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
        quarter_vintage: Quarter vintage (e.g., "2025Q4", "2025_annual")
        dataset: Dataset type (e.g., "pprrvu", "gpci")
    
    Returns:
        Layout dict with version, columns, min_line_length or None
        
    Raises:
        ValueError: If no layout found and dataset requires fixed-width parsing
    """
    # Extract quarter from quarter_vintage
    if 'Q' in quarter_vintage:
        quarter = quarter_vintage.split('Q')[-1]
        quarter = f"Q{quarter}"
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

