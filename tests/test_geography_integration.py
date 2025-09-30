"""
Integration tests for geography mapping system

Tests end-to-end functionality including ingestion, resolution, and API
per PRD requirements.
"""

import pytest
import tempfile
import os
from datetime import date
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

from cms_pricing.ingestion.geography import GeographyIngester
from cms_pricing.services.geography import GeographyService
from cms_pricing.models.geography import Geography
from cms_pricing.database import SessionLocal
from fastapi.testclient import TestClient
from cms_pricing.main import app


class TestGeographyIntegration:
    """Integration tests for complete geography pipeline"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @pytest.fixture
    def sample_zip_data_file(self):
        """Create temporary file with sample ZIP data"""
        sample_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31
90210|CA|18|01182||2025-01-01|2025-12-31
10001|NY|1|31102||2025-01-01|2025-12-31
01434|MA|1|10112|R|2025-01-01|2025-12-31"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(sample_data)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        os.unlink(temp_file)
    
    @pytest.fixture
    def sample_zip9_data_file(self):
        """Create temporary file with sample ZIP9 data"""
        sample_data = """ZIP5|PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
01434|0001|MA|1|10112|R|2025-01-01|2025-12-31
01434|0002|MA|1|10112|R|2025-01-01|2025-12-31
94110|1234|CA|5|01112||2025-01-01|2025-12-31
94110|5678|CA|5|01112||2025-01-01|2025-12-31"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(sample_data)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        os.unlink(temp_file)
    
    def test_complete_ingestion_pipeline(self, db_session, sample_zip_data_file, sample_zip9_data_file):
        """Test complete ingestion pipeline from file to database"""
        ingester = GeographyIngester(db=db_session)
        
        # Ingest ZIP5 data
        zip5_records = ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        assert len(zip5_records) == 4
        
        # Ingest ZIP9 data
        zip9_records = ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        assert len(zip9_records) == 4
        
        # Verify records in database
        total_records = db_session.query(Geography).count()
        assert total_records == 8  # 4 ZIP5 + 4 ZIP9
        
        # Verify ZIP+4 records
        zip4_records = db_session.query(Geography).filter(Geography.has_plus4 == 1).count()
        assert zip4_records == 4
        
        # Verify ZIP5-only records
        zip5_only_records = db_session.query(Geography).filter(Geography.has_plus4 == 0).count()
        assert zip5_only_records == 4
    
    def test_resolution_after_ingestion(self, db_session, sample_zip_data_file, sample_zip9_data_file):
        """Test resolution works after ingestion"""
        ingester = GeographyIngester(db=db_session)
        service = GeographyService(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test ZIP+4 resolution
        result = service.resolve_zip("01434", "0001", 2025)
        assert result["match_level"] == "zip+4"
        assert result["locality_id"] == "1"
        assert result["state"] == "MA"
        
        # Test ZIP5 resolution
        result = service.resolve_zip("94110", None, 2025)
        assert result["match_level"] == "zip5"
        assert result["locality_id"] == "5"
        assert result["state"] == "CA"
    
    def test_api_endpoint_after_ingestion(self, db_session, sample_zip_data_file, sample_zip9_data_file, client):
        """Test API endpoints work after ingestion"""
        ingester = GeographyIngester(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test API endpoint
        response = client.get("/geo/resolve?zip=01434&plus4=0001")
        assert response.status_code == 200
        
        data = response.json()
        assert data["match_level"] == "zip+4"
        assert data["locality_id"] == "1"
        assert data["state"] == "MA"
    
    def test_effective_dating_integration(self, db_session):
        """Test effective dating works across ingestion and resolution"""
        ingester = GeographyIngester(db=db_session)
        service = GeographyService(db=db_session)
        
        # Create data for different years
        zip5_2024_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2024-01-01|2024-12-31"""
        
        zip5_2025_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|18|01182||2025-01-01|2025-12-31"""  # Different locality
        
        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(zip5_2024_data)
            file_2024 = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(zip5_2025_data)
            file_2025 = f.name
        
        try:
            # Ingest both years
            ingester.ingest_zip5_file(file_2024, "2024")
            ingester.ingest_zip5_file(file_2025, "2025")
            
            # Test 2024 resolution
            result_2024 = service.resolve_zip("94110", None, 2024)
            assert result_2024["locality_id"] == "5"
            
            # Test 2025 resolution
            result_2025 = service.resolve_zip("94110", None, 2025)
            assert result_2025["locality_id"] == "18"
            
        finally:
            # Cleanup
            os.unlink(file_2024)
            os.unlink(file_2025)
    
    def test_strict_mode_integration(self, db_session, sample_zip_data_file, sample_zip9_data_file, client):
        """Test strict mode works end-to-end"""
        ingester = GeographyIngester(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test strict mode with ZIP+4 that exists
        response = client.get("/geo/resolve?zip=01434&plus4=0001&strict=true")
        assert response.status_code == 200
        
        data = response.json()
        assert data["match_level"] == "zip+4"
        
        # Test strict mode with ZIP+4 that doesn't exist
        response = client.get("/geo/resolve?zip=94110&plus4=9999&strict=true")
        assert response.status_code == 400  # Should error
        
        # Test strict mode with ZIP5-only (should work)
        response = client.get("/geo/resolve?zip=90210&strict=true")
        assert response.status_code == 200
        
        data = response.json()
        assert data["match_level"] == "zip5"
    
    def test_carrier_exposure_integration(self, db_session, sample_zip_data_file, sample_zip9_data_file, client):
        """Test carrier exposure works end-to-end"""
        ingester = GeographyIngester(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test without carrier exposure
        response = client.get("/geo/resolve?zip=01434&plus4=0001")
        assert response.status_code == 200
        
        data = response.json()
        assert data.get("carrier") is None
        
        # Test with carrier exposure
        response = client.get("/geo/resolve?zip=01434&plus4=0001&expose_carrier=true")
        assert response.status_code == 200
        
        data = response.json()
        assert data["carrier"] == "10112"
    
    def test_input_normalization_integration(self, db_session, sample_zip_data_file, sample_zip9_data_file, client):
        """Test input normalization works end-to-end"""
        ingester = GeographyIngester(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test dash format
        response = client.get("/geo/resolve?zip=01434-0001")
        assert response.status_code == 200
        
        data = response.json()
        assert data["match_level"] == "zip+4"
        
        # Test combined format
        response = client.get("/geo/resolve?zip=014340001")
        assert response.status_code == 200
        
        data = response.json()
        assert data["match_level"] == "zip+4"
    
    def test_error_handling_integration(self, db_session, client):
        """Test error handling works end-to-end"""
        # Test invalid ZIP format
        response = client.get("/geo/resolve?zip=123")
        assert response.status_code == 400
        
        # Test invalid ZIP+4 format
        response = client.get("/geo/resolve?zip=94110&plus4=123")
        assert response.status_code == 400
        
        # Test missing ZIP
        response = client.get("/geo/resolve")
        assert response.status_code == 422  # Validation error
    
    def test_performance_integration(self, db_session, sample_zip_data_file, sample_zip9_data_file):
        """Test performance meets SLOs"""
        import time
        
        ingester = GeographyIngester(db=db_session)
        service = GeographyService(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test resolution latency
        start_time = time.time()
        
        for _ in range(10):
            service.resolve_zip("94110", None, 2025)
        
        end_time = time.time()
        avg_latency_ms = (end_time - start_time) * 1000 / 10
        
        # Should be well under 20ms (cold DB SLO)
        assert avg_latency_ms < 20, f"Average latency {avg_latency_ms}ms exceeds SLO"
    
    def test_data_consistency_integration(self, db_session, sample_zip_data_file, sample_zip9_data_file):
        """Test data consistency across ingestion and resolution"""
        ingester = GeographyIngester(db=db_session)
        service = GeographyService(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test that resolution results match ingested data
        result = service.resolve_zip("01434", "0001", 2025)
        
        # Verify all fields match
        assert result["locality_id"] == "1"
        assert result["state"] == "MA"
        assert result["rural_flag"] == "R"
        assert result["match_level"] == "zip+4"
        
        # Test ZIP5 fallback consistency
        result = service.resolve_zip("94110", "9999", 2025)  # ZIP+4 doesn't exist
        
        assert result["locality_id"] == "5"
        assert result["state"] == "CA"
        assert result["match_level"] == "zip5"
    
    def test_determinism_integration(self, db_session, sample_zip_data_file, sample_zip9_data_file):
        """Test determinism - same inputs produce same outputs"""
        ingester = GeographyIngester(db=db_session)
        service = GeographyService(db=db_session)
        
        # Ingest data
        ingester.ingest_zip5_file(sample_zip_data_file, "2025")
        ingester.ingest_zip9_file(sample_zip9_data_file, "2025")
        
        # Test multiple resolutions with same inputs
        results = []
        for _ in range(5):
            result = service.resolve_zip("01434", "0001", 2025)
            results.append(result)
        
        # All results should be identical
        first_result = results[0]
        for result in results[1:]:
            assert result["locality_id"] == first_result["locality_id"]
            assert result["state"] == first_result["state"]
            assert result["match_level"] == first_result["match_level"]
            assert result["dataset_digest"] == first_result["dataset_digest"]


class TestGeographyGoldenFixtures:
    """Test geography system against golden fixtures"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_golden_zip_scenarios(self, db_session, client):
        """Test against golden ZIP resolution scenarios"""
        # Load sample geography data
        ingester = GeographyIngester(db=db_session)
        
        # Create sample data matching golden scenarios
        sample_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31
73301|TX|45|42102||2025-01-01|2025-12-31"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(sample_data)
            temp_file = f.name
        
        try:
            ingester.ingest_zip5_file(temp_file, "2025")
            
            # Test golden scenarios
            golden_scenarios = [
                {"zip": "94110", "expected_locality": "5", "expected_state": "CA"},
                {"zip": "73301", "expected_locality": "45", "expected_state": "TX"},
            ]
            
            for scenario in golden_scenarios:
                response = client.get(f"/geo/resolve?zip={scenario['zip']}")
                assert response.status_code == 200
                
                data = response.json()
                assert data["locality_id"] == scenario["expected_locality"]
                assert data["state"] == scenario["expected_state"]
                
        finally:
            os.unlink(temp_file)
    
    def test_golden_zip4_scenarios(self, db_session, client):
        """Test against golden ZIP+4 resolution scenarios"""
        # Load sample ZIP+4 data
        ingester = GeographyIngester(db=db_session)
        
        sample_data = """ZIP5|PLUS4|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|1234|CA|5|01112||2025-01-01|2025-12-31
73301|5678|TX|45|42102||2025-01-01|2025-12-31"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(sample_data)
            temp_file = f.name
        
        try:
            ingester.ingest_zip9_file(temp_file, "2025")
            
            # Test ZIP+4 scenarios
            zip4_scenarios = [
                {"zip": "94110", "plus4": "1234", "expected_locality": "5"},
                {"zip": "73301", "plus4": "5678", "expected_locality": "45"},
            ]
            
            for scenario in zip4_scenarios:
                response = client.get(f"/geo/resolve?zip={scenario['zip']}&plus4={scenario['plus4']}")
                assert response.status_code == 200
                
                data = response.json()
                assert data["match_level"] == "zip+4"
                assert data["locality_id"] == scenario["expected_locality"]
                
        finally:
            os.unlink(temp_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

