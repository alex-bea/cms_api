"""
Comprehensive test suite for geography mapping system

Tests ZIP+4-first resolution, strict mode, effective dating, and edge cases
per PRD requirements.
"""

import pytest
import asyncio
from datetime import date, datetime
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

from cms_pricing.services.geography import GeographyService
from cms_pricing.models.geography import Geography
from cms_pricing.database import SessionLocal
from cms_pricing.schemas.geography import GeographyResolveResponse, GeographyCandidate


class TestGeographyResolver:
    """Test suite for geography resolution service"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    @pytest.fixture
    def geography_service(self, db_session):
        """Create geography service with test database"""
        return GeographyService(db=db_session)
    
    @pytest.fixture
    def sample_geography_data(self, db_session):
        """Create sample geography data for testing"""
        # Create test records
        test_records = [
            # ZIP+4 records
            Geography(
                zip5="01434", plus4="0001", has_plus4=1, state="MA", 
                locality_id="1", locality_name="Massachusetts", 
                carrier="10112", rural_flag="R",
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
            Geography(
                zip5="01434", plus4="0002", has_plus4=1, state="MA", 
                locality_id="1", locality_name="Massachusetts", 
                carrier="10112", rural_flag="R",
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
            # ZIP5-only records
            Geography(
                zip5="94110", plus4=None, has_plus4=0, state="CA", 
                locality_id="5", locality_name="San Francisco", 
                carrier="01112", rural_flag=None,
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
            Geography(
                zip5="90210", plus4=None, has_plus4=0, state="CA", 
                locality_id="18", locality_name="Los Angeles", 
                carrier="01182", rural_flag=None,
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
            # Different effective period
            Geography(
                zip5="10001", plus4=None, has_plus4=0, state="NY", 
                locality_id="1", locality_name="New York", 
                carrier="31102", rural_flag=None,
                effective_from=date(2024, 1, 1), effective_to=date(2024, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_2024",
                created_at=date.today()
            ),
        ]
        
        # Add to database
        for record in test_records:
            db_session.add(record)
        db_session.commit()
        
        yield test_records
        
        # Cleanup
        for record in test_records:
            db_session.delete(record)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_zip4_exact_match(self, geography_service, sample_geography_data):
        """Test ZIP+4 exact matching (highest priority)"""
        result = await geography_service.resolve_zip(
            zip5="01434", plus4="0001", valuation_year=2025
        )
        
        assert result["match_level"] == "zip+4"
        assert result["locality_id"] == "1"
        assert result["state"] == "MA"
        assert result["rural_flag"] == "R"
        assert result["dataset_digest"] == "test_digest_1"
    
    @pytest.mark.asyncio
    async def test_zip5_fallback(self, geography_service, sample_geography_data):
        """Test ZIP5 fallback when ZIP+4 not found"""
        result = await geography_service.resolve_zip(
            zip5="94110", plus4="9999", valuation_year=2025  # ZIP+4 doesn't exist
        )
        
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "5"
        assert result["state"] == "CA"
    
    @pytest.mark.asyncio
    async def test_zip5_only_lookup(self, geography_service, sample_geography_data):
        """Test ZIP5-only lookup (no plus4 provided)"""
        result = await geography_service.resolve_zip(
            zip5="90210", plus4=None, valuation_year=2025
        )
        
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "18"
        assert result["state"] == "CA"
    
    @pytest.mark.asyncio
    async def test_strict_mode_zip4_required(self, geography_service, sample_geography_data):
        """Test strict mode errors when ZIP+4 provided but not found"""
        with pytest.raises(ValueError) as exc_info:
            await geography_service.resolve_zip(
                zip5="94110", plus4="9999", valuation_year=2025, strict=True
            )
        
        assert "ZIP+4" in str(exc_info.value)
        assert "strict mode" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_strict_mode_zip4_found(self, geography_service, sample_geography_data):
        """Test strict mode succeeds when ZIP+4 found"""
        result = await geography_service.resolve_zip(
            zip5="01434", plus4="0001", valuation_year=2025, strict=True
        )
        
        assert result["match_level"] == "zip+4"
        assert result["locality_id"] == "1"
    
    @pytest.mark.asyncio
    async def test_strict_mode_zip5_only(self, geography_service, sample_geography_data):
        """Test strict mode allows ZIP5-only lookups"""
        result = await geography_service.resolve_zip(
            zip5="94110", plus4=None, valuation_year=2025, strict=True
        )
        
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "5"
    
    @pytest.mark.asyncio
    async def test_effective_dating_current_year(self, geography_service, sample_geography_data):
        """Test effective dating selects current year data"""
        result = await geography_service.resolve_zip(
            zip5="94110", valuation_year=2025
        )
        
        assert result["locality_id"] == "5"  # 2025 data
        assert result["dataset_digest"] == "test_digest_1"
    
    @pytest.mark.asyncio
    async def test_effective_dating_historical_year(self, geography_service, sample_geography_data):
        """Test effective dating selects historical year data"""
        result = await geography_service.resolve_zip(
            zip5="10001", valuation_year=2024
        )
        
        assert result["locality_id"] == "1"  # 2024 data
        assert result["dataset_digest"] == "test_digest_2024"
    
    @pytest.mark.asyncio
    async def test_input_normalization_dash_format(self, geography_service, sample_geography_data):
        """Test ZIP+4 normalization with dash format (94110-1234)"""
        result = await geography_service.resolve_zip(
            zip5="01434-0001", valuation_year=2025
        )
        
        assert result["match_level"] == "zip+4"
        assert result["locality_id"] == "1"
    
    @pytest.mark.asyncio
    async def test_input_normalization_combined_format(self, geography_service, sample_geography_data):
        """Test ZIP+4 normalization with combined format (941101234)"""
        result = await geography_service.resolve_zip(
            zip5="014340001", valuation_year=2025
        )
        
        assert result["match_level"] == "zip+4"
        assert result["locality_id"] == "1"
    
    @pytest.mark.asyncio
    async def test_input_normalization_separate_params(self, geography_service, sample_geography_data):
        """Test ZIP+4 normalization with separate zip5 and plus4 parameters"""
        result = await geography_service.resolve_zip(
            zip5="01434", plus4="0001", valuation_year=2025
        )
        
        assert result["match_level"] == "zip+4"
        assert result["locality_id"] == "1"
    
    @pytest.mark.asyncio
    async def test_carrier_exposure_flag(self, geography_service, sample_geography_data):
        """Test carrier/MAC exposure flag"""
        # With carrier exposure
        result_with_carrier = await geography_service.resolve_zip(
            zip5="01434", plus4="0001", valuation_year=2025, expose_carrier=True
        )
        assert result_with_carrier["carrier"] == "10112"
        
        # Without carrier exposure
        result_without_carrier = await geography_service.resolve_zip(
            zip5="01434", plus4="0001", valuation_year=2025, expose_carrier=False
        )
        assert result_without_carrier["carrier"] is None
    
    @pytest.mark.asyncio
    async def test_invalid_zip5_format(self, geography_service):
        """Test error handling for invalid ZIP5 format"""
        with pytest.raises(ValueError) as exc_info:
            await geography_service.resolve_zip(zip5="123", valuation_year=2025)
        
        assert "Invalid ZIP5" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_invalid_zip4_format(self, geography_service):
        """Test error handling for invalid ZIP+4 format"""
        with pytest.raises(ValueError) as exc_info:
            await geography_service.resolve_zip(zip5="94110", plus4="123", valuation_year=2025)
        
        assert "Invalid ZIP+4" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_no_data_found_non_strict(self, geography_service):
        """Test fallback to default when no data found (non-strict mode)"""
        result = await geography_service.resolve_zip(
            zip5="99999", valuation_year=2025, strict=False
        )
        
        assert result["match_level"] == "default"
        assert result["locality_id"] == "01"  # Benchmark locality
        assert result["dataset_digest"] == "benchmark"
    
    @pytest.mark.asyncio
    async def test_no_data_found_strict(self, geography_service):
        """Test error when no data found (strict mode)"""
        with pytest.raises(ValueError) as exc_info:
            await geography_service.resolve_zip(
                zip5="99999", valuation_year=2025, strict=True
            )
        
        assert "strict mode" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_quarter_parameter(self, geography_service, sample_geography_data):
        """Test quarter parameter handling"""
        result = await geography_service.resolve_zip(
            zip5="94110", valuation_year=2025, quarter=1
        )
        
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "5"
    
    @pytest.mark.asyncio
    async def test_default_valuation_year(self, geography_service, sample_geography_data):
        """Test default valuation year (current year)"""
        result = await geography_service.resolve_zip(zip5="94110")
        
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "5"


class TestGeographyInputNormalization:
    """Test ZIP+4 input normalization logic"""
    
    def test_normalize_dash_format(self):
        """Test normalization of 94110-1234 format"""
        service = GeographyService()
        zip5, plus4 = service._normalize_zip_input("94110-1234")
        
        assert zip5 == "94110"
        assert plus4 == "1234"
    
    def test_normalize_combined_format(self):
        """Test normalization of 941101234 format"""
        service = GeographyService()
        zip5, plus4 = service._normalize_zip_input("941101234")
        
        assert zip5 == "94110"
        assert plus4 == "1234"
    
    def test_normalize_separate_params(self):
        """Test normalization with separate zip5 and plus4"""
        service = GeographyService()
        zip5, plus4 = service._normalize_zip_input("94110", "1234")
        
        assert zip5 == "94110"
        assert plus4 == "1234"
    
    def test_normalize_leading_zeros(self):
        """Test preservation of leading zeros in plus4"""
        service = GeographyService()
        zip5, plus4 = service._normalize_zip_input("94110", "0001")
        
        assert zip5 == "94110"
        assert plus4 == "0001"
    
    def test_normalize_zip5_only(self):
        """Test ZIP5-only normalization"""
        service = GeographyService()
        zip5, plus4 = service._normalize_zip_input("94110")
        
        assert zip5 == "94110"
        assert plus4 is None
    
    def test_invalid_zip5_length(self):
        """Test error for invalid ZIP5 length"""
        service = GeographyService()
        
        with pytest.raises(ValueError):
            service._normalize_zip_input("123")
    
    def test_invalid_zip4_length(self):
        """Test error for invalid ZIP+4 length"""
        service = GeographyService()
        
        with pytest.raises(ValueError):
            service._normalize_zip_input("94110", "12345")  # Too long


class TestGeographyAPI:
    """Test geography API endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        from fastapi.testclient import TestClient
        from cms_pricing.main import app
        return TestClient(app)
    
    def test_resolve_endpoint_basic(self, client):
        """Test basic geography resolve endpoint"""
        response = client.get("/geo/resolve?zip=94110")
        
        assert response.status_code == 200
        data = response.json()
        assert "locality_id" in data
        assert "match_level" in data
    
    def test_resolve_endpoint_with_plus4(self, client):
        """Test geography resolve with ZIP+4"""
        response = client.get("/geo/resolve?zip=01434&plus4=0001")
        
        assert response.status_code == 200
        data = response.json()
        assert data["match_level"] == "zip+4"
    
    def test_resolve_endpoint_strict_mode(self, client):
        """Test geography resolve with strict mode"""
        response = client.get("/geo/resolve?zip=94110&plus4=9999&strict=true")
        
        assert response.status_code == 400  # Should error in strict mode
    
    def test_resolve_endpoint_parameters(self, client):
        """Test geography resolve with all parameters"""
        response = client.get(
            "/geo/resolve?"
            "zip=94110&"
            "plus4=1234&"
            "valuation_year=2025&"
            "quarter=1&"
            "strict=false&"
            "max_radius_miles=50&"
            "initial_radius_miles=10&"
            "expand_step_miles=5&"
            "expose_carrier=true"
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "locality_id" in data
    
    def test_resolve_endpoint_invalid_zip(self, client):
        """Test geography resolve with invalid ZIP"""
        response = client.get("/geo/resolve?zip=123")
        
        assert response.status_code == 400


class TestGeographyPerformance:
    """Test geography resolution performance"""
    
    @pytest.mark.asyncio
    async def test_resolution_latency(self, geography_service, sample_geography_data):
        """Test resolution meets latency SLOs"""
        import time
        
        # Test multiple resolutions
        start_time = time.time()
        
        for _ in range(10):
            await geography_service.resolve_zip(zip5="94110", valuation_year=2025)
        
        end_time = time.time()
        avg_latency_ms = (end_time - start_time) * 1000 / 10
        
        # Should be well under 20ms (cold DB SLO)
        assert avg_latency_ms < 20, f"Average latency {avg_latency_ms}ms exceeds SLO"
    
    @pytest.mark.asyncio
    async def test_batch_resolution_performance(self, geography_service, sample_geography_data):
        """Test batch resolution performance"""
        import time
        
        test_zips = ["94110", "90210", "01434", "10001"]
        
        start_time = time.time()
        
        # Resolve multiple ZIPs
        results = []
        for zip_code in test_zips:
            result = await geography_service.resolve_zip(zip5=zip_code, valuation_year=2025)
            results.append(result)
        
        end_time = time.time()
        total_time_ms = (end_time - start_time) * 1000
        
        # Should complete batch within reasonable time
        assert total_time_ms < 100, f"Batch resolution {total_time_ms}ms too slow"
        assert len(results) == len(test_zips)


class TestGeographyEdgeCases:
    """Test edge cases and error conditions"""
    
    @pytest.mark.asyncio
    async def test_empty_database(self, db_session):
        """Test behavior with empty geography database"""
        service = GeographyService(db=db_session)
        
        with pytest.raises(ValueError):
            await service.resolve_zip(zip5="94110", valuation_year=2025, strict=True)
    
    @pytest.mark.asyncio
    async def test_malformed_zip_inputs(self, geography_service):
        """Test various malformed ZIP inputs"""
        malformed_inputs = [
            "94110-",  # Incomplete dash format
            "94110-12",  # Incomplete plus4
            "94110123",  # 8 digits (not 9)
            "9411012345",  # 10 digits (not 9)
            "abcde",  # Non-numeric
            "94110-abc",  # Non-numeric plus4
        ]
        
        for malformed_input in malformed_inputs:
            with pytest.raises(ValueError):
                await geography_service.resolve_zip(zip5=malformed_input, valuation_year=2025)
    
    @pytest.mark.asyncio
    async def test_boundary_effective_dates(self, geography_service, sample_geography_data):
        """Test effective date boundary conditions"""
        # Test exact boundary dates
        result = await geography_service.resolve_zip(
            zip5="94110", valuation_year=2025, quarter=1
        )
        
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "5"
    
    @pytest.mark.asyncio
    async def test_radius_parameters(self, geography_service, sample_geography_data):
        """Test radius parameter validation"""
        # Test with custom radius parameters
        result = await geography_service.resolve_zip(
            zip5="94110", 
            valuation_year=2025,
            max_radius_miles=50,
            initial_radius_miles=10,
            expand_step_miles=5
        )
        
        assert result["match_level"] == "zip5"  # Should find exact match
        assert result["locality_id"] == "5"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
