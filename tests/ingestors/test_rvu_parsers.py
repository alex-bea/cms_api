"""Test suite for RVU data parsers - Test-First Implementation per PRD Section 10"""

import pytest
import pandas as pd
from decimal import Decimal
from typing import Dict, Any, List
from unittest.mock import Mock, patch

# Test fixtures and golden data
from tests.fixtures.rvu import (
    SAMPLE_PPRRVU_TXT_RECORDS,
    SAMPLE_PPRRVU_CSV_RECORDS,
    SAMPLE_GPCI_RECORDS,
    SAMPLE_OPPSCAP_RECORDS,
    SAMPLE_ANES_RECORDS,
    SAMPLE_LOCCO_RECORDS,
    EXPECTED_PPRRVU_PARSED,
    EXPECTED_GPCI_PARSED,
    EXPECTED_OPPSCAP_PARSED,
    EXPECTED_ANES_PARSED,
    EXPECTED_LOCCO_PARSED
)


class TestPPRRVUFixedWidthParser:
    """Unit tests for PPRRVU fixed-width TXT parser"""
    
    def test_parse_pprrvu_fixed_width_basic(self):
        """Test basic fixed-width parsing of PPRRVU records"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        result = parser.parse_fixed_width(SAMPLE_PPRRVU_TXT_RECORDS)
        
        assert len(result) == len(EXPECTED_PPRRVU_PARSED)
        
        for i, record in enumerate(result):
            expected = EXPECTED_PPRRVU_PARSED[i]
            assert record['hcpcs_code'] == expected['hcpcs_code']
            assert record['modifiers'] == expected['modifiers']
            assert record['modifier_key'] == expected['modifier_key']
            assert record['work_rvu'] == expected['work_rvu']
            assert record['pe_rvu_nonfac'] == expected['pe_rvu_nonfac']
            assert record['pe_rvu_fac'] == expected['pe_rvu_fac']
            assert record['mp_rvu'] == expected['mp_rvu']
            assert record['status_code'] == expected['status_code']
            assert record['global_days'] == expected['global_days']
    
    def test_parse_pprrvu_fixed_width_modifiers(self):
        """Test modifier parsing and normalization"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        
        # Test cases for modifier parsing
        test_cases = [
            ("99213", "", [], "null"),
            ("99213", "26", ["26"], "26"),
            ("99213", "26,TC", ["26", "TC"], "26,TC"),
            ("99213", "TC,26", ["TC", "26"], "TC,26"),
        ]
        
        for hcpcs, raw_modifiers, expected_array, expected_key in test_cases:
            result = parser.normalize_modifiers(raw_modifiers)
            assert result['modifiers'] == expected_array
            assert result['modifier_key'] == expected_key
    
    def test_parse_pprrvu_fixed_width_rvu_values(self):
        """Test RVU value parsing and precision"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        
        # Test decimal precision preservation
        test_values = [
            ("1.23", Decimal("1.23")),
            ("0.00", Decimal("0.00")),
            ("123.456", Decimal("123.456")),
            ("", None),
            ("   ", None),
        ]
        
        for raw_value, expected in test_values:
            result = parser.parse_decimal(raw_value)
            assert result == expected
    
    def test_parse_pprrvu_fixed_width_status_codes(self):
        """Test status code validation"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        
        valid_status_codes = ["A", "R", "T", "I", "N", "X", "C", "J"]
        invalid_status_codes = ["Z", "1", "AB", ""]
        
        for status in valid_status_codes:
            assert parser.validate_status_code(status) == True
        
        for status in invalid_status_codes:
            assert parser.validate_status_code(status) == False
    
    def test_parse_pprrvu_fixed_width_policy_indicators(self):
        """Test policy indicator parsing"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        
        # Test bilateral indicator
        assert parser.parse_policy_indicator("1") == True
        assert parser.parse_policy_indicator("0") == False
        assert parser.parse_policy_indicator("") == False
        
        # Test multiple procedure indicator
        assert parser.parse_policy_indicator("2") == True
        assert parser.parse_policy_indicator("0") == False


class TestPPRRVUCSVParser:
    """Unit tests for PPRRVU CSV parser"""
    
    def test_parse_pprrvu_csv_basic(self):
        """Test basic CSV parsing of PPRRVU records"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        df = pd.DataFrame(SAMPLE_PPRRVU_CSV_RECORDS)
        result = parser.parse_csv(df)
        
        assert len(result) == len(EXPECTED_PPRRVU_PARSED)
        
        for i, record in enumerate(result):
            expected = EXPECTED_PPRRVU_PARSED[i]
            assert record['hcpcs_code'] == expected['hcpcs_code']
            assert record['modifiers'] == expected['modifiers']
            assert record['work_rvu'] == expected['work_rvu']
    
    def test_parse_pprrvu_csv_header_aliasing(self):
        """Test CSV header aliasing and normalization"""
        from cms_pricing.ingestion.rvu import PPRRVUParser
        
        parser = PPRRVUParser()
        
        # Test header aliasing
        test_headers = [
            "HCPCS Code",
            "HCPCS_CODE", 
            "hcpcs_code",
            "HcpcsCode",
            "Work RVU",
            "WORK_RVU",
            "work_rvu",
            "WorkRvu"
        ]
        
        expected_canonical = [
            "hcpcs_code",
            "hcpcs_code",
            "hcpcs_code", 
            "hcpcs_code",
            "work_rvu",
            "work_rvu",
            "work_rvu",
            "work_rvu"
        ]
        
        for header, expected in zip(test_headers, expected_canonical):
            result = parser.normalize_header(header)
            assert result == expected


class TestGPCIParser:
    """Unit tests for GPCI parser"""
    
    def test_parse_gpci_basic(self):
        """Test basic GPCI parsing"""
        from cms_pricing.ingestion.rvu import GPCIParser
        
        parser = GPCIParser()
        result = parser.parse(SAMPLE_GPCI_RECORDS)
        
        assert len(result) == len(EXPECTED_GPCI_PARSED)
        
        for i, record in enumerate(result):
            expected = EXPECTED_GPCI_PARSED[i]
            assert record['mac'] == expected['mac']
            assert record['state'] == expected['state']
            assert record['locality_id'] == expected['locality_id']
            assert record['locality_name'] == expected['locality_name']
            assert record['work_gpci'] == expected['work_gpci']
            assert record['pe_gpci'] == expected['pe_gpci']
            assert record['mp_gpci'] == expected['mp_gpci']
    
    def test_parse_gpci_values_bounds(self):
        """Test GPCI value validation and bounds checking"""
        from cms_pricing.ingestion.rvu import GPCIParser
        
        parser = GPCIParser()
        
        # Test valid GPCI values (typically 0.5 - 2.0 range)
        valid_values = ["0.8", "1.0", "1.2", "1.5", "2.0"]
        for value in valid_values:
            assert parser.validate_gpci_value(Decimal(value)) == True
        
        # Test invalid GPCI values
        invalid_values = ["-0.1", "3.0", "0.0"]
        for value in invalid_values:
            assert parser.validate_gpci_value(Decimal(value)) == False


class TestOPPSCAPParser:
    """Unit tests for OPPSCAP parser"""
    
    def test_parse_oppscap_basic(self):
        """Test basic OPPSCAP parsing"""
        from cms_pricing.ingestion.rvu import OPPSCAPParser
        
        parser = OPPSCAPParser()
        result = parser.parse(SAMPLE_OPPSCAP_RECORDS)
        
        assert len(result) == len(EXPECTED_OPPSCAP_PARSED)
        
        for i, record in enumerate(result):
            expected = EXPECTED_OPPSCAP_PARSED[i]
            assert record['hcpcs_code'] == expected['hcpcs_code']
            assert record['modifier'] == expected['modifier']
            assert record['mac'] == expected['mac']
            assert record['locality_id'] == expected['locality_id']
            assert record['price_fac'] == expected['price_fac']
            assert record['price_nonfac'] == expected['price_nonfac']
    
    def test_parse_oppscap_price_validation(self):
        """Test OPPSCAP price validation"""
        from cms_pricing.ingestion.rvu import OPPSCAPParser
        
        parser = OPPSCAPParser()
        
        # Test valid prices (non-negative)
        valid_prices = ["100.00", "0.00", "1500.50"]
        for price in valid_prices:
            assert parser.validate_price(Decimal(price)) == True
        
        # Test invalid prices
        invalid_prices = ["-100.00", "-0.01"]
        for price in invalid_prices:
            assert parser.validate_price(Decimal(price)) == False


class TestANESParser:
    """Unit tests for ANES parser"""
    
    def test_parse_anes_basic(self):
        """Test basic ANES parsing"""
        from cms_pricing.ingestion.rvu import ANESParser
        
        parser = ANESParser()
        result = parser.parse(SAMPLE_ANES_RECORDS)
        
        assert len(result) == len(EXPECTED_ANES_PARSED)
        
        for i, record in enumerate(result):
            expected = EXPECTED_ANES_PARSED[i]
            assert record['mac'] == expected['mac']
            assert record['locality_id'] == expected['locality_id']
            assert record['locality_name'] == expected['locality_name']
            assert record['anesthesia_cf'] == expected['anesthesia_cf']
    
    def test_parse_anes_cf_validation(self):
        """Test anesthesia conversion factor validation"""
        from cms_pricing.ingestion.rvu import ANESParser
        
        parser = ANESParser()
        
        # Test valid CF values (typically 10-50 range)
        valid_cfs = ["15.0", "20.5", "25.0", "30.0"]
        for cf in valid_cfs:
            assert parser.validate_anesthesia_cf(Decimal(cf)) == True
        
        # Test invalid CF values
        invalid_cfs = ["0.0", "-5.0", "100.0"]
        for cf in invalid_cfs:
            assert parser.validate_anesthesia_cf(Decimal(cf)) == False


class TestLocalityCountyParser:
    """Unit tests for Locality-County crosswalk parser"""
    
    def test_parse_locco_basic(self):
        """Test basic locality-county parsing"""
        from cms_pricing.ingestion.rvu import LocalityCountyParser
        
        parser = LocalityCountyParser()
        result = parser.parse(SAMPLE_LOCCO_RECORDS)
        
        assert len(result) == len(EXPECTED_LOCCO_PARSED)
        
        for i, record in enumerate(result):
            expected = EXPECTED_LOCCO_PARSED[i]
            assert record['mac'] == expected['mac']
            assert record['locality_id'] == expected['locality_id']
            assert record['state'] == expected['state']
            assert record['fee_schedule_area'] == expected['fee_schedule_area']
            assert record['county_name'] == expected['county_name']
    
    def test_parse_locco_county_normalization(self):
        """Test county name normalization"""
        from cms_pricing.ingestion.rvu import LocalityCountyParser
        
        parser = LocalityCountyParser()
        
        test_cases = [
            ("Los Angeles County", "Los Angeles County"),
            ("LOS ANGELES COUNTY", "Los Angeles County"),
            ("los angeles county", "Los Angeles County"),
            ("Los Angeles", "Los Angeles"),
            ("", ""),
        ]
        
        for input_name, expected in test_cases:
            result = parser.normalize_county_name(input_name)
            assert result == expected


class TestModifierNormalization:
    """Unit tests for modifier normalization logic"""
    
    def test_modifier_array_creation(self):
        """Test modifier array creation from various formats"""
        from cms_pricing.ingestion.rvu import normalize_modifiers
        
        test_cases = [
            ("", []),
            ("26", ["26"]),
            ("26,TC", ["26", "TC"]),
            ("TC,26", ["TC", "26"]),
            ("26,TC,25", ["26", "TC", "25"]),
            ("26, TC", ["26", "TC"]),  # Space handling
            ("26,  TC,  25", ["26", "TC", "25"]),  # Multiple spaces
        ]
        
        for input_modifiers, expected in test_cases:
            result = normalize_modifiers(input_modifiers)
            assert result['modifiers'] == expected
    
    def test_modifier_key_generation(self):
        """Test modifier key generation for uniqueness"""
        from cms_pricing.ingestion.rvu import normalize_modifiers
        
        test_cases = [
            ("", "null"),
            ("26", "26"),
            ("26,TC", "26,TC"),
            ("TC,26", "TC,26"),
            ("26,TC,25", "26,TC,25"),
        ]
        
        for input_modifiers, expected_key in test_cases:
            result = normalize_modifiers(input_modifiers)
            assert result['modifier_key'] == expected_key


class TestEffectiveDating:
    """Unit tests for effective dating logic"""
    
    def test_effective_date_calculation(self):
        """Test effective date calculation from year/cycle"""
        from cms_pricing.ingestion.rvu import calculate_effective_dates
        
        test_cases = [
            (2025, "D", ("2025-10-01", "2025-12-31")),
            (2025, "A", ("2025-01-01", "2025-03-31")),
            (2025, "B", ("2025-04-01", "2025-06-30")),
            (2025, "C", ("2025-07-01", "2025-09-30")),
        ]
        
        for year, cycle, expected in test_cases:
            start, end = calculate_effective_dates(year, cycle)
            assert start == expected[0]
            assert end == expected[1]
    
    def test_effective_date_override(self):
        """Test effective date override handling"""
        from cms_pricing.ingestion.rvu import calculate_effective_dates
        
        # Test with correction dates
        start, end = calculate_effective_dates(2025, "D", correction_date="2025-11-15")
        assert start == "2025-11-15"
        assert end == "2025-12-31"


class TestValidationEngine:
    """Unit tests for validation engine"""
    
    def test_structural_validation(self):
        """Test structural validation rules"""
        from cms_pricing.ingestion.rvu import ValidationEngine
        
        engine = ValidationEngine()
        
        # Test valid structure
        valid_record = {
            'hcpcs_code': '99213',
            'work_rvu': Decimal('1.0'),
            'status_code': 'A'
        }
        errors = engine.validate_structural(valid_record)
        assert len(errors) == 0
        
        # Test invalid structure
        invalid_record = {
            'hcpcs_code': 'INVALID',  # Wrong format
            'work_rvu': -1.0,  # Negative value
            'status_code': 'Z'  # Invalid status
        }
        errors = engine.validate_structural(invalid_record)
        assert len(errors) > 0
    
    def test_content_validation(self):
        """Test content validation rules"""
        from cms_pricing.ingestion.rvu import ValidationEngine
        
        engine = ValidationEngine()
        
        # Test NA indicator logic
        na_record = {
            'na_indicator': 'NA',
            'pe_rvu_nonfac': Decimal('1.0')  # Should be null when NA
        }
        errors = engine.validate_content(na_record)
        assert len(errors) > 0  # Should have error for NA + non-null PE
        
        # Test valid NA record
        valid_na_record = {
            'na_indicator': 'NA',
            'pe_rvu_nonfac': None  # Correctly null
        }
        errors = engine.validate_content(valid_na_record)
        assert len(errors) == 0
    
    def test_referential_validation(self):
        """Test referential validation rules"""
        from cms_pricing.ingestion.rvu import ValidationEngine
        
        engine = ValidationEngine()
        
        # Mock existing locality data
        engine.existing_localities = {'11302', '11303', '11304'}
        
        # Test valid locality reference
        valid_record = {'locality_id': '11302'}
        errors = engine.validate_referential(valid_record)
        assert len(errors) == 0
        
        # Test invalid locality reference
        invalid_record = {'locality_id': '99999'}
        errors = engine.validate_referential(invalid_record)
        assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__])

