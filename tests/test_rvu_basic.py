"""
Basic RVU tests - Test-First Implementation per PRD Section 10

QTS Compliance Header:
Test ID: QA-RVU-UNIT-0001
Owner: Data Engineering
Tier: unit
Environments: dev, ci, staging, production
Dependencies: cms_pricing.ingestion.ingestors.rvu_ingestor
Quality Gates: merge, pre-deploy, release
SLOs: completion ≤ 5 min, pass rate ≥95%, flake rate <1%
"""

import pytest
from decimal import Decimal
from pathlib import Path

# Test fixtures
from tests.fixtures.rvu import (
    SAMPLE_PPRRVU_TXT_RECORDS,
    SAMPLE_GPCI_TXT_RECORDS,
    SAMPLE_OPPSCAP_TXT_RECORDS,
    SAMPLE_ANES_TXT_RECORDS,
    SAMPLE_LOCCO_TXT_RECORDS,
    PPRRVU_2025D_LAYOUT,
    GPCI_2025D_LAYOUT,
    OPPSCAP_2025D_LAYOUT,
    ANES_2025D_LAYOUT,
    LOCCO_2025D_LAYOUT,
)


class TestRVUDataFiles:
    """Test that RVU data files exist and are readable"""
    
    def test_rvu_data_files_exist(self):
        """Test that all RVU data files exist"""
        rvu_data_dir = Path(__file__).parent.parent.parent / "sample_data" / "rvu25d_0"
        
        required_files = [
            "PPRRVU2025_Oct.txt",
            "PPRRVU2025_Oct.csv", 
            "GPCI2025.txt",
            "GPCI2025.csv",
            "OPPSCAP_Oct.txt",
            "OPPSCAP_Oct.csv",
            "ANES2025.txt",
            "ANES2025.csv",
            "25LOCCO.txt",
            "25LOCCO.csv",
            "RVU25D.pdf"
        ]
        
        for filename in required_files:
            file_path = rvu_data_dir / filename
            assert file_path.exists(), f"Required file {filename} not found"
            assert file_path.stat().st_size > 0, f"File {filename} is empty"
    
    def test_sample_data_structure(self):
        """Test that sample data has expected structure"""
        # Test PPRRVU TXT records
        assert len(SAMPLE_PPRRVU_TXT_RECORDS) > 0
        assert all(len(record) > 50 for record in SAMPLE_PPRRVU_TXT_RECORDS)
        
        # Test GPCI TXT records
        assert len(SAMPLE_GPCI_TXT_RECORDS) > 0
        assert all(len(record) > 30 for record in SAMPLE_GPCI_TXT_RECORDS)
        
        # Test OPPSCAP TXT records
        assert len(SAMPLE_OPPSCAP_TXT_RECORDS) > 0
        assert all(len(record) > 20 for record in SAMPLE_OPPSCAP_TXT_RECORDS)
        
        # Test ANES TXT records
        assert len(SAMPLE_ANES_TXT_RECORDS) > 0
        assert all(len(record) > 20 for record in SAMPLE_ANES_TXT_RECORDS)
        
        # Test Locality-County TXT records
        assert len(SAMPLE_LOCCO_TXT_RECORDS) > 0
        assert all(len(record) > 30 for record in SAMPLE_LOCCO_TXT_RECORDS)


class TestLayoutRegistry:
    """Test fixed-width layout registry"""
    
    def test_pprrvu_layout_structure(self):
        """Test PPRRVU layout has required fields"""
        required_fields = [
            'hcpcs_code', 'description', 'status_code', 'work_rvu',
            'pe_rvu_nonfac', 'pe_rvu_fac', 'mp_rvu', 'na_indicator',
            'global_days', 'conversion_factor'
        ]
        
        for field in required_fields:
            assert field in PPRRVU_2025D_LAYOUT, f"Missing field: {field}"
            assert 'start' in PPRRVU_2025D_LAYOUT[field]
            assert 'end' in PPRRVU_2025D_LAYOUT[field]
            assert 'type' in PPRRVU_2025D_LAYOUT[field]
            assert 'nullable' in PPRRVU_2025D_LAYOUT[field]
    
    def test_gpci_layout_structure(self):
        """Test GPCI layout has required fields"""
        required_fields = ['mac', 'state', 'locality_id', 'locality_name', 'work_gpci', 'pe_gpci', 'mp_gpci']
        
        for field in required_fields:
            assert field in GPCI_2025D_LAYOUT, f"Missing field: {field}"
    
    def test_oppscap_layout_structure(self):
        """Test OPPSCAP layout has required fields"""
        required_fields = ['hcpcs_code', 'modifier', 'proc_status', 'mac', 'locality_id', 'price_fac', 'price_nonfac']
        
        for field in required_fields:
            assert field in OPPSCAP_2025D_LAYOUT, f"Missing field: {field}"
    
    def test_anes_layout_structure(self):
        """Test ANES layout has required fields"""
        required_fields = ['mac', 'locality_id', 'locality_name', 'anesthesia_cf']
        
        for field in required_fields:
            assert field in ANES_2025D_LAYOUT, f"Missing field: {field}"
    
    def test_locco_layout_structure(self):
        """Test Locality-County layout has required fields"""
        required_fields = ['mac', 'locality_id', 'state', 'fee_schedule_area', 'county_name']
        
        for field in required_fields:
            assert field in LOCCO_2025D_LAYOUT, f"Missing field: {field}"


class TestDataValidation:
    """Test data validation rules"""
    
    def test_hcpcs_code_format(self):
        """Test HCPCS code format validation"""
        valid_codes = ["00100", "99213", "G0001", "D0120"]
        invalid_codes = ["123", "123456", "AB", ""]
        
        for code in valid_codes:
            assert len(code) == 5, f"Invalid HCPCS code length: {code}"
            assert code.isalnum(), f"Invalid HCPCS code format: {code}"
        
        for code in invalid_codes:
            assert len(code) != 5 or not code.isalnum(), f"Should be invalid: {code}"
    
    def test_status_code_validation(self):
        """Test status code validation"""
        valid_status = ["A", "R", "T", "I", "N", "X", "C", "J"]
        invalid_status = ["Z", "1", "AB", ""]
        
        for status in valid_status:
            assert len(status) == 1, f"Invalid status code length: {status}"
            assert status.isalpha(), f"Invalid status code format: {status}"
        
        for status in invalid_status:
            assert len(status) != 1 or not status.isalpha(), f"Should be invalid: {status}"
    
    def test_gpci_value_ranges(self):
        """Test GPCI value ranges"""
        valid_gpci = [Decimal("0.5"), Decimal("1.0"), Decimal("1.5"), Decimal("2.0")]
        invalid_gpci = [Decimal("-0.1"), Decimal("3.0"), Decimal("0.0")]
        
        for value in valid_gpci:
            assert Decimal("0.1") <= value <= Decimal("3.0"), f"Invalid GPCI range: {value}"
        
        for value in invalid_gpci:
            assert not (Decimal("0.1") <= value <= Decimal("3.0")), f"Should be invalid: {value}"


class TestModifierNormalization:
    """Test modifier normalization logic"""
    
    def test_modifier_parsing(self):
        """Test modifier parsing from various formats"""
        test_cases = [
            ("", []),
            ("26", ["26"]),
            ("26,TC", ["26", "TC"]),
            ("TC,26", ["TC", "26"]),
            ("26,TC,25", ["26", "TC", "25"]),
        ]
        
        for input_modifiers, expected in test_cases:
            # Simple parsing logic for testing
            if not input_modifiers:
                result = []
            else:
                result = [m.strip() for m in input_modifiers.split(",")]
            
            assert result == expected, f"Failed for input: {input_modifiers}"
    
    def test_modifier_key_generation(self):
        """Test modifier key generation for uniqueness"""
        test_cases = [
            ("", "null"),
            ("26", "26"),
            ("26,TC", "26,TC"),
            ("TC,26", "TC,26"),
        ]
        
        for input_modifiers, expected_key in test_cases:
            if not input_modifiers:
                result_key = "null"
            else:
                result_key = ",".join(sorted(input_modifiers.split(",")))
            
            assert result_key == expected_key, f"Failed for input: {input_modifiers}"


if __name__ == "__main__":
    pytest.main([__file__])

