"""
Test suite for geography data ingestion system

Tests CMS data parsing, normalization, validation, and loading
per PRD requirements.
"""

import pytest
import tempfile
import os
from datetime import date
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

geography_module = pytest.importorskip("cms_pricing.ingestion.geography")

from cms_pricing.models.geography import Geography
from cms_pricing.database import SessionLocal

GeographyIngester = geography_module.GeographyIngester


class TestGeographyIngestion:
    """Test suite for geography data ingestion"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    @pytest.fixture
    def geography_ingester(self, db_session):
        """Create geography ingester with test database"""
        return GeographyIngester(db=db_session)
    
    @pytest.fixture
    def sample_zip5_data(self):
        """Sample ZIP5 fixed-width data"""
        return """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31
90210|CA|18|01182||2025-01-01|2025-12-31
10001|NY|1|31102||2025-01-01|2025-12-31"""
    
    @pytest.fixture
    def sample_zip9_data(self):
        """Sample ZIP9 fixed-width data"""
        return """ZIP5|PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
01434|0001|MA|1|10112|R|2025-01-01|2025-12-31
01434|0002|MA|1|10112|R|2025-01-01|2025-12-31
94110|1234|CA|5|01112||2025-01-01|2025-12-31"""
    
    def test_parse_zip5_fixed_width(self, geography_ingester, sample_zip5_data):
        """Test parsing ZIP5 fixed-width data"""
        records = geography_ingester._parse_zip5_fixed_width(sample_zip5_data)
        
        assert len(records) == 3
        
        # Check first record
        first_record = records[0]
        assert first_record["zip5"] == "94110"
        assert first_record["plus4"] is None
        assert first_record["has_plus4"] == 0
        assert first_record["state"] == "CA"
        assert first_record["locality_id"] == "5"
        assert first_record["carrier"] == "01112"
        assert first_record["rural_flag"] is None
        assert first_record["effective_from"] == date(2025, 1, 1)
        assert first_record["effective_to"] == date(2025, 12, 31)
    
    def test_parse_zip9_fixed_width(self, geography_ingester, sample_zip9_data):
        """Test parsing ZIP9 fixed-width data"""
        records = geography_ingester._parse_zip9_fixed_width(sample_zip9_data)
        
        assert len(records) == 3
        
        # Check first record
        first_record = records[0]
        assert first_record["zip5"] == "01434"
        assert first_record["plus4"] == "0001"
        assert first_record["has_plus4"] == 1
        assert first_record["state"] == "MA"
        assert first_record["locality_id"] == "1"
        assert first_record["carrier"] == "10112"
        assert first_record["rural_flag"] == "R"
    
    def test_parse_zip_plus4_combined_format(self, geography_ingester):
        """Test parsing ZIP+4 in combined format"""
        sample_data = """ZIP_PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
014340001|MA|1|10112|R|2025-01-01|2025-12-31
941101234|CA|5|01112||2025-01-01|2025-12-31"""
        
        records = geography_ingester._process_zip_plus4(sample_data)
        
        assert len(records) == 2
        
        # Check first record
        first_record = records[0]
        assert first_record["zip5"] == "01434"
        assert first_record["plus4"] == "0001"
        assert first_record["has_plus4"] == 1
        
        # Check second record
        second_record = records[1]
        assert second_record["zip5"] == "94110"
        assert second_record["plus4"] == "1234"
        assert second_record["has_plus4"] == 1
    
    def test_parse_zip_plus4_dash_format(self, geography_ingester):
        """Test parsing ZIP+4 in dash format"""
        sample_data = """ZIP_PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
01434-0001|MA|1|10112|R|2025-01-01|2025-12-31
94110-1234|CA|5|01112||2025-01-01|2025-12-31"""
        
        records = geography_ingester._process_zip_plus4(sample_data)
        
        assert len(records) == 2
        
        # Check first record
        first_record = records[0]
        assert first_record["zip5"] == "01434"
        assert first_record["plus4"] == "0001"
        assert first_record["has_plus4"] == 1
    
    def test_leading_zero_preservation(self, geography_ingester):
        """Test preservation of leading zeros in plus4"""
        sample_data = """ZIP5|PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
01434|0001|MA|1|10112|R|2025-01-01|2025-12-31
01434|0002|MA|1|10112|R|2025-01-01|2025-12-31"""
        
        records = geography_ingester._parse_zip9_fixed_width(sample_data)
        
        assert records[0]["plus4"] == "0001"  # Leading zero preserved
        assert records[1]["plus4"] == "0002"  # Leading zero preserved
    
    def test_effective_date_mapping(self, geography_ingester):
        """Test effective date mapping for different periods"""
        # Test annual mapping
        annual_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31"""
        
        records = geography_ingester._parse_zip5_fixed_width(annual_data)
        assert records[0]["effective_from"] == date(2025, 1, 1)
        assert records[0]["effective_to"] == date(2025, 12, 31)
        
        # Test quarterly mapping
        quarterly_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-03-31"""
        
        records = geography_ingester._parse_zip5_fixed_width(quarterly_data)
        assert records[0]["effective_from"] == date(2025, 1, 1)
        assert records[0]["effective_to"] == date(2025, 3, 31)
    
    def test_dataset_digest_generation(self, geography_ingester, sample_zip5_data):
        """Test dataset digest generation"""
        records = geography_ingester._parse_zip5_fixed_width(sample_zip5_data)
        
        # All records should have same dataset_digest
        digests = [record["dataset_digest"] for record in records]
        assert len(set(digests)) == 1  # All same digest
        assert digests[0] is not None
        assert len(digests[0]) == 64  # SHA256 length
    
    def test_data_validation_required_fields(self, geography_ingester):
        """Test validation of required fields"""
        # Missing required field
        invalid_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
|CA|5|01112||2025-01-01|2025-12-31"""  # Missing ZIP5
        
        with pytest.raises(ValueError):
            geography_ingester._parse_zip5_fixed_width(invalid_data)
    
    def test_data_validation_zip_format(self, geography_ingester):
        """Test ZIP format validation"""
        # Invalid ZIP5 length
        invalid_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
123|CA|5|01112||2025-01-01|2025-12-31"""  # 3 digits instead of 5
        
        with pytest.raises(ValueError):
            geography_ingester._parse_zip5_fixed_width(invalid_data)
    
    def test_data_validation_plus4_format(self, geography_ingester):
        """Test ZIP+4 format validation"""
        # Invalid plus4 length
        invalid_data = """ZIP5|PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|123|CA|5|01112||2025-01-01|2025-12-31"""  # 3 digits instead of 4
        
        with pytest.raises(ValueError):
            geography_ingester._parse_zip9_fixed_width(invalid_data)
    
    def test_duplicate_handling(self, geography_ingester):
        """Test handling of duplicate records"""
        duplicate_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31
94110|CA|5|01112||2025-01-01|2025-12-31"""  # Duplicate record
        
        records = geography_ingester._parse_zip5_fixed_width(duplicate_data)
        
        # Should handle duplicates gracefully
        assert len(records) == 2  # Both records parsed
        assert records[0]["zip5"] == records[1]["zip5"]  # Same ZIP5
    
    def test_empty_data_handling(self, geography_ingester):
        """Test handling of empty data"""
        empty_data = ""
        
        records = geography_ingester._parse_zip5_fixed_width(empty_data)
        assert len(records) == 0
    
    def test_header_only_data(self, geography_ingester):
        """Test handling of header-only data"""
        header_only_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO"""
        
        records = geography_ingester._parse_zip5_fixed_width(header_only_data)
        assert len(records) == 0
    
    def test_mixed_case_normalization(self, geography_ingester):
        """Test normalization of mixed case fields"""
        mixed_case_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|ca|5|01112||2025-01-01|2025-12-31"""  # Lowercase state
        
        records = geography_ingester._parse_zip5_fixed_width(mixed_case_data)
        
        # State should be normalized to uppercase
        assert records[0]["state"] == "CA"
    
    def test_rural_flag_preservation(self, geography_ingester):
        """Test preservation of rural flag values"""
        rural_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112|R|2025-01-01|2025-12-31
90210|CA|18|01182|B|2025-01-01|2025-12-31
10001|NY|1|31102||2025-01-01|2025-12-31"""  # Empty rural flag
        
        records = geography_ingester._parse_zip5_fixed_width(rural_data)
        
        assert records[0]["rural_flag"] == "R"
        assert records[1]["rural_flag"] == "B"
        assert records[2]["rural_flag"] is None  # Empty preserved as None


class TestGeographyDataLoading:
    """Test geography data loading into database"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    @pytest.fixture
    def geography_ingester(self, db_session):
        """Create geography ingester with test database"""
        return GeographyIngester(db=db_session)
    
    def test_load_geography_records(self, geography_ingester, db_session):
        """Test loading geography records into database"""
        # Create test records
        test_records = [
            {
                "zip5": "94110",
                "plus4": None,
                "has_plus4": 0,
                "state": "CA",
                "locality_id": "5",
                "locality_name": "San Francisco",
                "carrier": "01112",
                "rural_flag": None,
                "effective_from": date(2025, 1, 1),
                "effective_to": date(2025, 12, 31),
                "dataset_id": "ZIP_LOCALITY",
                "dataset_digest": "test_digest_1",
                "created_at": date.today()
            },
            {
                "zip5": "01434",
                "plus4": "0001",
                "has_plus4": 1,
                "state": "MA",
                "locality_id": "1",
                "locality_name": "Massachusetts",
                "carrier": "10112",
                "rural_flag": "R",
                "effective_from": date(2025, 1, 1),
                "effective_to": date(2025, 12, 31),
                "dataset_id": "ZIP_LOCALITY",
                "dataset_digest": "test_digest_1",
                "created_at": date.today()
            }
        ]
        
        # Load records
        geography_ingester._load_geography_records(test_records)
        
        # Verify records were loaded
        loaded_records = db_session.query(Geography).all()
        assert len(loaded_records) == 2
        
        # Check first record
        first_record = loaded_records[0]
        assert first_record.zip5 == "94110"
        assert first_record.has_plus4 == 0
        assert first_record.state == "CA"
        assert first_record.locality_id == "5"
        
        # Check second record
        second_record = loaded_records[1]
        assert second_record.zip5 == "01434"
        assert second_record.plus4 == "0001"
        assert second_record.has_plus4 == 1
        assert second_record.state == "MA"
        assert second_record.rural_flag == "R"
    
    def test_upsert_behavior(self, geography_ingester, db_session):
        """Test upsert behavior for duplicate records"""
        # Create initial record
        initial_record = {
            "zip5": "94110",
            "plus4": None,
            "has_plus4": 0,
            "state": "CA",
            "locality_id": "5",
            "locality_name": "San Francisco",
            "carrier": "01112",
            "rural_flag": None,
            "effective_from": date(2025, 1, 1),
            "effective_to": date(2025, 12, 31),
            "dataset_id": "ZIP_LOCALITY",
            "dataset_digest": "test_digest_1",
            "created_at": date.today()
        }
        
        geography_ingester._load_geography_records([initial_record])
        
        # Verify initial record
        assert db_session.query(Geography).count() == 1
        
        # Create updated record with same key but different data
        updated_record = initial_record.copy()
        updated_record["locality_name"] = "Updated San Francisco"
        updated_record["dataset_digest"] = "test_digest_2"
        
        geography_ingester._load_geography_records([updated_record])
        
        # Should still have only one record (upserted)
        assert db_session.query(Geography).count() == 1
        
        # Check that record was updated
        loaded_record = db_session.query(Geography).first()
        assert loaded_record.locality_name == "Updated San Francisco"
        assert loaded_record.dataset_digest == "test_digest_2"
    
    def test_database_constraints(self, geography_ingester, db_session):
        """Test database constraint enforcement"""
        # Create record with missing required field
        invalid_record = {
            "zip5": None,  # Required field missing
            "plus4": None,
            "has_plus4": 0,
            "state": "CA",
            "locality_id": "5",
            "locality_name": "San Francisco",
            "carrier": "01112",
            "rural_flag": None,
            "effective_from": date(2025, 1, 1),
            "effective_to": date(2025, 12, 31),
            "dataset_id": "ZIP_LOCALITY",
            "dataset_digest": "test_digest_1",
            "created_at": date.today()
        }
        
        with pytest.raises(Exception):  # Should raise database constraint error
            geography_ingester._load_geography_records([invalid_record])


class TestGeographyValidation:
    """Test geography data validation"""
    
    @pytest.fixture
    def geography_ingester(self):
        """Create geography ingester"""
        return GeographyIngester()
    
    def test_validate_zip5_format(self, geography_ingester):
        """Test ZIP5 format validation"""
        valid_zips = ["94110", "90210", "10001", "01434"]
        invalid_zips = ["123", "123456", "abcde", "9411"]
        
        for valid_zip in valid_zips:
            assert geography_ingester._validate_zip5(valid_zip) is True
        
        for invalid_zip in invalid_zips:
            assert geography_ingester._validate_zip5(invalid_zip) is False
    
    def test_validate_plus4_format(self, geography_ingester):
        """Test ZIP+4 format validation"""
        valid_plus4s = ["0001", "1234", "9999", "0000"]
        invalid_plus4s = ["123", "12345", "abc", "12"]
        
        for valid_plus4 in valid_plus4s:
            assert geography_ingester._validate_plus4(valid_plus4) is True
        
        for invalid_plus4 in invalid_plus4s:
            assert geography_ingester._validate_plus4(invalid_plus4) is False
    
    def test_validate_state_code(self, geography_ingester):
        """Test state code validation"""
        valid_states = ["CA", "NY", "TX", "MA", "FL"]
        invalid_states = ["C", "CAL", "california", "123", ""]
        
        for valid_state in valid_states:
            assert geography_ingester._validate_state_code(valid_state) is True
        
        for invalid_state in invalid_states:
            assert geography_ingester._validate_state_code(invalid_state) is False
    
    def test_validate_locality_id(self, geography_ingester):
        """Test locality ID validation"""
        valid_localities = ["1", "01", "18", "99", "ABC"]
        invalid_localities = ["", None, "12345678901"]  # Too long
        
        for valid_locality in valid_localities:
            assert geography_ingester._validate_locality_id(valid_locality) is True
        
        for invalid_locality in invalid_localities:
            assert geography_ingester._validate_locality_id(invalid_locality) is False
    
    def test_validate_effective_dates(self, geography_ingester):
        """Test effective date validation"""
        # Valid date range
        assert geography_ingester._validate_effective_dates(
            date(2025, 1, 1), date(2025, 12, 31)
        ) is True
        
        # Invalid: end before start
        assert geography_ingester._validate_effective_dates(
            date(2025, 12, 31), date(2025, 1, 1)
        ) is False
        
        # Valid: same start and end
        assert geography_ingester._validate_effective_dates(
            date(2025, 1, 1), date(2025, 1, 1)
        ) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
