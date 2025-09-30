"""Fixed-width layout registry for RVU data parsing - based on RVU25D.pdf"""

# PPRRVU 2025D Fixed-width Layout (based on actual data analysis)
PPRRVU_2025D_LAYOUT = {
    'hcpcs_code': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
    'description': {'start': 6, 'end': 57, 'type': 'string', 'nullable': True},
    'status_code': {'start': 57, 'end': 58, 'type': 'string', 'nullable': False},
    'work_rvu': {'start': 61, 'end': 65, 'type': 'decimal', 'nullable': True},
    'pe_rvu_nonfac': {'start': 68, 'end': 72, 'type': 'decimal', 'nullable': True},
    'pe_rvu_fac': {'start': 77, 'end': 81, 'type': 'decimal', 'nullable': True},
    'mp_rvu': {'start': 85, 'end': 89, 'type': 'decimal', 'nullable': True},
    'na_indicator': {'start': 92, 'end': 93, 'type': 'string', 'nullable': True},
    'global_days': {'start': 140, 'end': 143, 'type': 'string', 'nullable': True},
    'bilateral_ind': {'start': 103, 'end': 104, 'type': 'string', 'nullable': True},
    'multiple_proc_ind': {'start': 104, 'end': 105, 'type': 'string', 'nullable': True},
    'assistant_surg_ind': {'start': 105, 'end': 106, 'type': 'string', 'nullable': True},
    'co_surg_ind': {'start': 106, 'end': 107, 'type': 'string', 'nullable': True},
    'team_surg_ind': {'start': 107, 'end': 108, 'type': 'string', 'nullable': True},
    'endoscopic_base': {'start': 108, 'end': 109, 'type': 'string', 'nullable': True},
    'conversion_factor': {'start': 133, 'end': 138, 'type': 'decimal', 'nullable': True},
    'physician_supervision': {'start': 144, 'end': 146, 'type': 'string', 'nullable': True},
    'diag_imaging_family': {'start': 147, 'end': 148, 'type': 'string', 'nullable': True},
    'total_nonfac': {'start': 153, 'end': 157, 'type': 'decimal', 'nullable': True},
    'total_fac': {'start': 160, 'end': 164, 'type': 'decimal', 'nullable': True},
}

# GPCI 2025D Fixed-width Layout
GPCI_2025D_LAYOUT = {
    'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
    'state': {'start': 15, 'end': 17, 'type': 'string', 'nullable': False},
    'locality_id': {'start': 24, 'end': 26, 'type': 'string', 'nullable': False},
    'locality_name': {'start': 25, 'end': 75, 'type': 'string', 'nullable': True},
    'work_gpci': {'start': 120, 'end': 126, 'type': 'decimal', 'nullable': False},
    'pe_gpci': {'start': 133, 'end': 139, 'type': 'decimal', 'nullable': False},
    'mp_gpci': {'start': 140, 'end': 146, 'type': 'decimal', 'nullable': False},
}

# OPPSCAP 2025D Fixed-width Layout
OPPSCAP_2025D_LAYOUT = {
    'hcpcs_code': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
    'modifier': {'start': 5, 'end': 7, 'type': 'string', 'nullable': True},
    'proc_status': {'start': 8, 'end': 9, 'type': 'string', 'nullable': False},
    'mac': {'start': 10, 'end': 15, 'type': 'string', 'nullable': False},
    'locality_id': {'start': 17, 'end': 19, 'type': 'string', 'nullable': False},
    'price_fac': {'start': 22, 'end': 28, 'type': 'decimal', 'nullable': False},
    'price_nonfac': {'start': 32, 'end': 38, 'type': 'decimal', 'nullable': False},
}

# ANES 2025D Fixed-width Layout
ANES_2025D_LAYOUT = {
    'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
    'locality_id': {'start': 12, 'end': 14, 'type': 'string', 'nullable': False},
    'locality_name': {'start': 17, 'end': 57, 'type': 'string', 'nullable': True},
    'anesthesia_cf': {'start': 70, 'end': 74, 'type': 'decimal', 'nullable': False},
}

# Locality-County 2025D Fixed-width Layout
LOCCO_2025D_LAYOUT = {
    'mac': {'start': 0, 'end': 5, 'type': 'string', 'nullable': False},
    'locality_id': {'start': 12, 'end': 14, 'type': 'string', 'nullable': False},
    'state': {'start': 17, 'end': 25, 'type': 'string', 'nullable': False},
    'fee_schedule_area': {'start': 26, 'end': 76, 'type': 'string', 'nullable': True},
    'county_name': {'start': 120, 'end': 151, 'type': 'string', 'nullable': True},
}

# Layout registry for different source versions
LAYOUT_REGISTRY = {
    '2025D': {
        'pprrvu': PPRRVU_2025D_LAYOUT,
        'gpci': GPCI_2025D_LAYOUT,
        'oppscap': OPPSCAP_2025D_LAYOUT,
        'anes': ANES_2025D_LAYOUT,
        'locco': LOCCO_2025D_LAYOUT,
    }
}

def get_layout(source_version: str, dataset_type: str) -> dict:
    """Get fixed-width layout for a specific source version and dataset type"""
    if source_version not in LAYOUT_REGISTRY:
        raise ValueError(f"Unknown source version: {source_version}")
    
    if dataset_type not in LAYOUT_REGISTRY[source_version]:
        raise ValueError(f"Unknown dataset type: {dataset_type}")
    
    return LAYOUT_REGISTRY[source_version][dataset_type]

def parse_fixed_width_record(record: str, layout: dict) -> dict:
    """Parse a fixed-width record using the provided layout"""
    result = {}
    
    for field_name, field_spec in layout.items():
        start = field_spec['start']
        end = field_spec['end']
        
        # Extract field value
        field_value = record[start:end].strip()
        
        # Handle null values
        if not field_value and field_spec['nullable']:
            result[field_name] = None
        elif not field_value and not field_spec['nullable']:
            raise ValueError(f"Required field {field_name} is empty")
        else:
            # Type conversion
            if field_spec['type'] == 'decimal':
                from decimal import Decimal
                result[field_name] = Decimal(field_value)
            else:
                result[field_name] = field_value
    
    return result
