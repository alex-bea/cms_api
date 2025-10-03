"""
Unit tests for CMSZip9Ingester following QA Testing Standard (QTS)

Test ID: QA-ZIP9-UNIT-0001
Owner: Data Engineering
Tier: unit
Environments: dev, ci
Dependencies: cms_pricing.ingestion.ingestors.cms_zip9_ingester
"""

import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path
import json

from cms_pricing.ingestion.ingestors.cms_zip9_ingester import CMSZip9Ingester
from cms_pricing.ingestion.contracts.ingestor_spec import ValidationSeverity
from cms_pricing.models.nearest_zip import ZIP9Overrides


class TestCMSZip9Ingester:
    """Unit tests for CMSZip9Ingester core functionality"""
    
    @pytest.fixture
    def ingester(self):
        """Create ingester instance for testing"""
        return CMSZip9Ingester(output_dir="./test_data/zip9")
    
    @pytest.fixture
    def sample_zip9_data(self):
        """Sample ZIP9 data for testing"""
        return pd.DataFrame([
            {
                'zip9_low': '902100000',
                'zip9_high': '902109999', 
                'state': 'CA',
                'locality': '01',
                'rural_flag': 'A',
                'effective_from': '2025-08-14',
                'effective_to': None
            },
            {
                'zip9_low': '100010000',
                'zip9_high': '100019999',
                'state': 'NY', 
                'locality': '02',
                'rural_flag': 'B',
                'effective_from': '2025-08-14',
                'effective_to': None
            },
            {
                'zip9_low': '606010000',
                'zip9_high': '606019999',
                'state': 'IL',
                'locality': '03', 
                'rural_flag': None,
                'effective_from': '2025-08-14',
                'effective_to': None
            }
        ])
    
    def test_ingester_initialization(self, ingester):
        """Test QA-ZIP9-UNIT-0001: Ingester initializes correctly"""
        assert ingester.dataset_name == "cms_zip9_overrides"
        assert ingester.release_cadence == "quarterly"
        assert ingester.classification == "public"
        assert ingester.contract_schema_ref == "cms_zip9_overrides_v1"
    
    def test_discovery_method(self, ingester):
        """Test QA-ZIP9-UNIT-0002: Discovery returns correct source files"""
        source_files = ingester.discovery()
        
        assert len(source_files) == 1
        source_file = source_files[0]
        assert "ZIP9" in source_file.filename
        assert source_file.content_type == "application/zip"
        assert "cms.gov" in source_file.url
    
    def test_zip9_format_validation(self, ingester, sample_zip9_data):
        """Test QA-ZIP9-UNIT-0003: ZIP9 format validation works correctly"""
        # Test valid ZIP9 codes
        valid_data = sample_zip9_data.copy()
        result = ingester._validate_zip9_format(valid_data)
        assert result['valid_count'] == 3
        assert result['invalid_count'] == 0
        
        # Test invalid ZIP9 codes
        invalid_data = pd.DataFrame([
            {'zip9_low': '9021', 'zip9_high': '902109999'},  # Too short
            {'zip9_low': '902100000', 'zip9_high': '9021'},  # Too short
            {'zip9_low': '90210000a', 'zip9_high': '902109999'},  # Non-numeric
        ])
        result = ingester._validate_zip9_format(invalid_data)
        assert result['valid_count'] == 0
        assert result['invalid_count'] == 3
    
    def test_range_validation(self, ingester, sample_zip9_data):
        """Test QA-ZIP9-UNIT-0004: ZIP9 range validation works correctly"""
        # Test valid ranges
        valid_data = sample_zip9_data.copy()
        result = ingester._validate_zip9_ranges(valid_data)
        assert result['valid_count'] == 3
        assert result['invalid_count'] == 0
        
        # Test invalid ranges (low > high)
        invalid_data = pd.DataFrame([
            {'zip9_low': '902109999', 'zip9_high': '902100000'},  # Reversed
        ])
        result = ingester._validate_zip9_ranges(invalid_data)
        assert result['valid_count'] == 0
        assert result['invalid_count'] == 1
    
    def test_state_consistency_validation(self, ingester, sample_zip9_data):
        """Test QA-ZIP9-UNIT-0005: State consistency validation works correctly"""
        # Test valid state consistency
        valid_data = sample_zip9_data.copy()
        result = ingester._validate_state_consistency(valid_data)
        assert result['valid_count'] == 3
        assert result['invalid_count'] == 0
        
        # Test invalid state consistency
        invalid_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'NY'},  # CA ZIP9 with NY state
        ])
        result = ingester._validate_state_consistency(invalid_data)
        assert result['valid_count'] == 0
        assert result['invalid_count'] == 1
    
    def test_zip5_prefix_extraction(self, ingester):
        """Test QA-ZIP9-UNIT-0006: ZIP5 prefix extraction works correctly"""
        assert ingester._extract_zip5_prefix('902100000') == '90210'
        assert ingester._extract_zip5_prefix('100010000') == '10001'
        assert ingester._extract_zip5_prefix('606010000') == '60601'
    
    def test_metadata_calculation(self, ingester, sample_zip9_data):
        """Test QA-ZIP9-UNIT-0007: Metadata calculation works correctly"""
        metadata = ingester._calculate_metadata(sample_zip9_data)
        
        assert 'data_quality_score' in metadata
        assert 'validation_results' in metadata
        assert 'processing_timestamp' in metadata
        assert 'file_checksum' in metadata
        assert 'record_count' in metadata
        assert 'schema_version' in metadata
        assert 'business_rules_applied' in metadata
        
        assert metadata['record_count'] == 3
        assert metadata['schema_version'] == "1.0"
        assert isinstance(metadata['data_quality_score'], float)
        assert 0.0 <= metadata['data_quality_score'] <= 1.0
    
    def test_schema_contract_generation(self, ingester):
        """Test QA-ZIP9-UNIT-0008: Schema contract generation works correctly"""
        contract = ingester._generate_schema_contract()
        
        assert 'name' in contract
        assert 'version' in contract
        assert 'columns' in contract
        assert contract['name'] == 'cms_zip9_overrides'
        assert contract['version'] == '1.0'
        
        # Check required columns
        required_columns = ['zip9_low', 'zip9_high', 'state', 'locality', 'rural_flag']
        for col in required_columns:
            assert col in contract['columns']
    
    def test_validation_engine_integration(self, ingester, sample_zip9_data):
        """Test QA-ZIP9-UNIT-0009: Validation engine integration works correctly"""
        validation_results = ingester._run_validation_engine(sample_zip9_data)
        
        assert 'status' in validation_results
        assert 'rules_passed' in validation_results
        assert 'rules_failed' in validation_results
        assert 'quality_score' in validation_results
        assert 'details' in validation_results
        
        assert validation_results['status'] == 'validated'
        assert validation_results['rules_passed'] >= 0
        assert validation_results['rules_failed'] >= 0
        assert 0.0 <= validation_results['quality_score'] <= 1.0
    
    def test_error_handling_missing_zip5(self, ingester):
        """Test QA-ZIP9-UNIT-0010: Error handling for missing ZIP5 references"""
        # Create data with ZIP9 that doesn't have corresponding ZIP5
        invalid_data = pd.DataFrame([
            {'zip9_low': '999990000', 'zip9_high': '999999999', 'state': 'XX', 'locality': '99'}
        ])
        
        errors = ingester._check_missing_zip5_references(invalid_data)
        assert len(errors) == 1
        assert '999990000' in errors[0]['zip9_low']
        assert 'missing_zip5' in errors[0]['error_code']
    
    def test_error_handling_conflicting_mappings(self, ingester):
        """Test QA-ZIP9-UNIT-0011: Error handling for conflicting ZIP9/ZIP5 mappings"""
        # Create conflicting data
        conflicting_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': '99'}  # Different locality than ZIP5
        ])
        
        conflicts = ingester._check_conflicting_mappings(conflicting_data)
        assert len(conflicts) == 1
        assert '902100000' in conflicts[0]['zip9_low']
        assert 'conflicting_locality' in conflicts[0]['error_code']


class TestCMSZip9IngesterIntegration:
    """Integration tests for CMSZip9Ingester with nearest ZIP resolver"""
    
    @pytest.fixture
    def ingester(self):
        """Create ingester instance for integration testing"""
        return CMSZip9Ingester(output_dir="./test_data/zip9")
    
    @pytest.fixture
    def mock_zip5_data(self):
        """Mock ZIP5 data for integration testing"""
        return pd.DataFrame([
            {'zip5': '90210', 'state': 'CA', 'locality': '01'},
            {'zip5': '10001', 'state': 'NY', 'locality': '02'},
            {'zip5': '60601', 'state': 'IL', 'locality': '03'},
        ])
    
    def test_zip9_zip5_consistency_check(self, ingester, mock_zip5_data):
        """Test QA-ZIP9-INT-0001: ZIP9/ZIP5 consistency checking"""
        zip9_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': '01'},
            {'zip9_low': '100010000', 'zip9_high': '100019999', 'state': 'NY', 'locality': '02'},
        ])
        
        consistency_result = ingester._check_zip9_zip5_consistency(zip9_data, mock_zip5_data)
        assert consistency_result['consistent_count'] == 2
        assert consistency_result['inconsistent_count'] == 0
    
    def test_zip9_api_integration(self, ingester):
        """Test QA-ZIP9-INT-0002: ZIP9 API integration"""
        # Test that ingester can handle ZIP9 queries
        test_zip9 = '902101234'
        result = ingester._process_zip9_query(test_zip9)
        
        assert 'zip9' in result
        assert 'zip5_prefix' in result
        assert 'state' in result
        assert 'locality' in result
        assert result['zip9'] == test_zip9
        assert result['zip5_prefix'] == '90210'


class TestCMSZip9IngesterDataQuality:
    """Data quality tests for CMSZip9Ingester"""
    
    @pytest.fixture
    def ingester(self):
        """Create ingester instance for data quality testing"""
        return CMSZip9Ingester(output_dir="./test_data/zip9")
    
    def test_data_completeness_validation(self, ingester):
        """Test QA-ZIP9-DQ-0001: Data completeness validation"""
        # Test complete data
        complete_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': '01', 'rural_flag': 'A'}
        ])
        result = ingester._validate_data_completeness(complete_data)
        assert result['completeness_score'] == 1.0
        
        # Test incomplete data
        incomplete_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': None, 'rural_flag': 'A'}
        ])
        result = ingester._validate_data_completeness(incomplete_data)
        assert result['completeness_score'] < 1.0
    
    def test_data_accuracy_validation(self, ingester):
        """Test QA-ZIP9-DQ-0002: Data accuracy validation"""
        # Test accurate data
        accurate_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': '01'}
        ])
        result = ingester._validate_data_accuracy(accurate_data)
        assert result['accuracy_score'] >= 0.9
        
        # Test inaccurate data
        inaccurate_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'XX', 'locality': '99'}
        ])
        result = ingester._validate_data_accuracy(inaccurate_data)
        assert result['accuracy_score'] < 0.9
    
    def test_data_consistency_validation(self, ingester):
        """Test QA-ZIP9-DQ-0003: Data consistency validation"""
        # Test consistent data
        consistent_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': '01'},
            {'zip9_low': '902110000', 'zip9_high': '902119999', 'state': 'CA', 'locality': '01'},
        ])
        result = ingester._validate_data_consistency(consistent_data)
        assert result['consistency_score'] >= 0.9
        
        # Test inconsistent data
        inconsistent_data = pd.DataFrame([
            {'zip9_low': '902100000', 'zip9_high': '902109999', 'state': 'CA', 'locality': '01'},
            {'zip9_low': '902110000', 'zip9_high': '902119999', 'state': 'NY', 'locality': '02'},
        ])
        result = ingester._validate_data_consistency(inconsistent_data)
        assert result['consistency_score'] < 0.9


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
