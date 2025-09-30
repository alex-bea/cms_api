#!/usr/bin/env python3
"""Test suite for RVU validation engine"""

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cms_pricing.validation.rvu_validators import RVUValidator, ValidationLevel, ValidationResult
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem
from datetime import date
import uuid


class TestRVUValidations(unittest.TestCase):
    """Test RVU validation business rules"""
    
    def setUp(self):
        self.validator = RVUValidator()
        self.db = SessionLocal()
        
        # Create a test release
        self.test_release = Release(
            id=uuid.uuid4(),
            type="RVU_FULL",
            source_version="2025D",
            imported_at=date.today(),
            notes="Test release for validation"
        )
        self.db.add(self.test_release)
        self.db.commit()
    
    def tearDown(self):
        # Clean up test data
        self.db.query(RVUItem).filter(RVUItem.release_id == self.test_release.id).delete()
        self.db.query(Release).filter(Release.id == self.test_release.id).delete()
        self.db.commit()
        self.db.close()
    
    def test_payability_rules_valid(self):
        """Test payability rules with valid payable codes"""
        test_cases = [
            {'hcpcs_code': '99213', 'status_code': 'A', 'work_rvu': 1.0, 'pe_rvu_nonfac': 0.5},
            {'hcpcs_code': '99214', 'status_code': 'R', 'work_rvu': 1.5, 'pe_rvu_nonfac': 0.8},
            {'hcpcs_code': '99215', 'status_code': 'T', 'work_rvu': 2.0, 'pe_rvu_nonfac': 1.0},
        ]
        
        for test_case in test_cases:
            with self.subTest(status_code=test_case['status_code']):
                results = self.validator._validate_payability_rules(test_case)
                self.assertEqual(len(results), 0, f"Should pass for status {test_case['status_code']}")
    
    def test_payability_rules_invalid(self):
        """Test payability rules with invalid non-payable codes that have RVUs"""
        test_cases = [
            {'hcpcs_code': '0001F', 'status_code': 'I', 'work_rvu': 1.0, 'pe_rvu_nonfac': 0.5},
            {'hcpcs_code': '0005F', 'status_code': 'N', 'work_rvu': 1.5, 'pe_rvu_nonfac': 0.8},
            {'hcpcs_code': '00100', 'status_code': 'J', 'work_rvu': 2.0, 'pe_rvu_nonfac': 1.0},
        ]
        
        for test_case in test_cases:
            with self.subTest(status_code=test_case['status_code']):
                results = self.validator._validate_payability_rules(test_case)
                self.assertEqual(len(results), 1, f"Should fail for status {test_case['status_code']}")
                self.assertEqual(results[0].level, ValidationLevel.ERROR)
                self.assertEqual(results[0].rule_name, "payability_rules")
    
    def test_na_indicator_logic_valid(self):
        """Test NA indicator logic with valid cases"""
        test_cases = [
            {'hcpcs_code': '99213', 'na_indicator': '0', 'pe_rvu_nonfac': 0.5},  # NA=0, has PE
            {'hcpcs_code': '99214', 'na_indicator': '1', 'pe_rvu_nonfac': None},  # NA=1, no PE
            {'hcpcs_code': '99215', 'na_indicator': '1', 'pe_rvu_nonfac': 0},    # NA=1, PE=0
        ]
        
        for test_case in test_cases:
            with self.subTest(na_indicator=test_case['na_indicator']):
                results = self.validator._validate_na_indicator_logic(test_case)
                self.assertEqual(len(results), 0, f"Should pass for NA={test_case['na_indicator']}")
    
    def test_na_indicator_logic_invalid(self):
        """Test NA indicator logic with invalid cases"""
        test_case = {
            'hcpcs_code': '99213',
            'na_indicator': '1',
            'pe_rvu_nonfac': 0.5  # NA=1 but has PE value
        }
        
        results = self.validator._validate_na_indicator_logic(test_case)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].level, ValidationLevel.ERROR)
        self.assertEqual(results[0].rule_name, "na_indicator_logic")
    
    def test_global_days_semantics_valid(self):
        """Test global days semantics with valid values"""
        valid_days = ['000', '010', '090', 'XXX', 'YYY', 'ZZZ']
        
        for days in valid_days:
            with self.subTest(global_days=days):
                test_case = {'hcpcs_code': '99213', 'global_days': days}
                results = self.validator._validate_global_days_semantics(test_case)
                self.assertEqual(len(results), 0, f"Should pass for {days}")
    
    def test_global_days_semantics_invalid(self):
        """Test global days semantics with invalid values"""
        test_case = {
            'hcpcs_code': '99213',
            'global_days': '999'  # Invalid global days value
        }
        
        results = self.validator._validate_global_days_semantics(test_case)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].level, ValidationLevel.WARN)
        self.assertEqual(results[0].rule_name, "global_days_semantics")
    
    def test_supervision_codes_valid(self):
        """Test supervision codes with valid values"""
        valid_codes = ['01', '02', '03', '04', '05', '06', '21', '22', '66', '6A', '77', '7A', '09']
        
        for code in valid_codes:
            with self.subTest(supervision_code=code):
                test_case = {'hcpcs_code': '99213', 'physician_supervision': code}
                results = self.validator._validate_supervision_codes(test_case)
                self.assertEqual(len(results), 0, f"Should pass for {code}")
    
    def test_supervision_codes_invalid(self):
        """Test supervision codes with invalid values"""
        test_case = {
            'hcpcs_code': '99213',
            'physician_supervision': '99'  # Invalid supervision code
        }
        
        results = self.validator._validate_supervision_codes(test_case)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].level, ValidationLevel.ERROR)
        self.assertEqual(results[0].rule_name, "supervision_codes")
    
    def test_complete_validation(self):
        """Test complete validation of an RVU item"""
        # Valid item
        valid_item = {
            'hcpcs_code': '99213',
            'status_code': 'A',
            'work_rvu': 1.0,
            'pe_rvu_nonfac': 0.5,
            'pe_rvu_fac': 0.3,
            'mp_rvu': 0.1,
            'na_indicator': '0',
            'global_days': '000',
            'physician_supervision': '01',
            'description': 'Office visit'
        }
        
        results = self.validator.validate_rvu_item(valid_item)
        error_results = [r for r in results if r.level == ValidationLevel.ERROR]
        self.assertEqual(len(error_results), 0, "Valid item should have no errors")
        
        # Invalid item with multiple issues
        invalid_item = {
            'hcpcs_code': '0001F',
            'status_code': 'I',  # Non-payable but has RVUs
            'work_rvu': 1.0,
            'pe_rvu_nonfac': 0.5,
            'pe_rvu_fac': 0.3,
            'mp_rvu': 0.1,
            'na_indicator': '1',  # NA=1 but has PE
            'pe_rvu_nonfac': 0.5,
            'global_days': '999',  # Invalid global days
            'physician_supervision': '99',  # Invalid supervision
            'description': 'Invalid procedure'
        }
        
        results = self.validator.validate_rvu_item(invalid_item)
        error_results = [r for r in results if r.level == ValidationLevel.ERROR]
        self.assertGreater(len(error_results), 0, "Invalid item should have errors")


if __name__ == '__main__':
    unittest.main()

