"""Expected parsed results for RVU data validation"""

from decimal import Decimal

# Expected parsed PPRRVU records
EXPECTED_PPRRVU_PARSED = [
    {
        'hcpcs_code': '00100',
        'modifiers': [],
        'modifier_key': 'null',
        'description': 'Anesth salivary gland',
        'status_code': 'J',
        'work_rvu': Decimal('0.00'),
        'pe_rvu_nonfac': Decimal('0.00'),
        'pe_rvu_fac': Decimal('0.00'),
        'mp_rvu': Decimal('0.00'),
        'na_indicator': '9',
        'global_days': 'XXX',
        'bilateral_ind': '0',
        'multiple_proc_ind': '0',
        'assistant_surg_ind': '0',
        'co_surg_ind': '0',
        'team_surg_ind': '0',
        'endoscopic_base': '0',
        'conversion_factor': Decimal('32.3465'),
        'physician_supervision': '09',
        'diag_imaging_family': '0',
        'total_nonfac': Decimal('0.00'),
        'total_fac': Decimal('0.00'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'PPRRVU2025_Oct.txt',
        'row_num': 1
    },
    {
        'hcpcs_code': '00102',
        'modifiers': [],
        'modifier_key': 'null',
        'description': 'Anesth repair of cleft lip',
        'status_code': 'J',
        'work_rvu': Decimal('0.00'),
        'pe_rvu_nonfac': Decimal('0.00'),
        'pe_rvu_fac': Decimal('0.00'),
        'mp_rvu': Decimal('0.00'),
        'na_indicator': '9',
        'global_days': 'XXX',
        'bilateral_ind': '0',
        'multiple_proc_ind': '0',
        'assistant_surg_ind': '0',
        'co_surg_ind': '0',
        'team_surg_ind': '0',
        'endoscopic_base': '0',
        'conversion_factor': Decimal('32.3465'),
        'physician_supervision': '09',
        'diag_imaging_family': '0',
        'total_nonfac': Decimal('0.00'),
        'total_fac': Decimal('0.00'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'PPRRVU2025_Oct.txt',
        'row_num': 2
    }
]

# Expected parsed GPCI records
EXPECTED_GPCI_PARSED = [
    {
        'mac': '10112',
        'state': 'AL',
        'locality_id': '00',
        'locality_name': 'ALABAMA',
        'work_gpci': Decimal('1.000'),
        'pe_gpci': Decimal('0.869'),
        'mp_gpci': Decimal('0.575'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'GPCI2025.txt',
        'row_num': 1
    },
    {
        'mac': '02102',
        'state': 'AK',
        'locality_id': '01',
        'locality_name': 'ALASKA*',
        'work_gpci': Decimal('1.500'),
        'pe_gpci': Decimal('1.081'),
        'mp_gpci': Decimal('0.592'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'GPCI2025.txt',
        'row_num': 2
    }
]

# Expected parsed OPPSCAP records
EXPECTED_OPPSCAP_PARSED = [
    {
        'hcpcs_code': '0633T',
        'modifier': 'TC',
        'proc_status': 'C',
        'mac': '01112',
        'locality_id': '05',
        'price_fac': Decimal('150.69'),
        'price_nonfac': Decimal('150.69'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'OPPSCAP_Oct.txt',
        'row_num': 1
    },
    {
        'hcpcs_code': '0633T',
        'modifier': 'TC',
        'proc_status': 'C',
        'mac': '01112',
        'locality_id': '09',
        'price_fac': Decimal('152.38'),
        'price_nonfac': Decimal('152.38'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'OPPSCAP_Oct.txt',
        'row_num': 2
    }
]

# Expected parsed ANES records
EXPECTED_ANES_PARSED = [
    {
        'mac': '10112',
        'locality_id': '00',
        'locality_name': 'ALABAMA',
        'anesthesia_cf': Decimal('19.31'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'ANES2025.txt',
        'row_num': 1
    },
    {
        'mac': '02102',
        'locality_id': '01',
        'locality_name': 'ALASKA',
        'anesthesia_cf': Decimal('27.86'),
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': 'ANES2025.txt',
        'row_num': 2
    }
]

# Expected parsed Locality-County records
EXPECTED_LOCCO_PARSED = [
    {
        'mac': '10112',
        'locality_id': '00',
        'state': 'AL',
        'fee_schedule_area': 'STATEWIDE',
        'county_name': 'ALL COUNTIES',
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': '25LOCCO.txt',
        'row_num': 1
    },
    {
        'mac': '02102',
        'locality_id': '01',
        'state': 'AK',
        'fee_schedule_area': 'STATEWIDE',
        'county_name': 'ALL COUNTIES',
        'effective_start': '2025-10-01',
        'effective_end': '2025-12-31',
        'source_file': '25LOCCO.txt',
        'row_num': 2
    }
]

