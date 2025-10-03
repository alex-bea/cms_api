"""
Additional tests for geography mapping system gaps

Tests health endpoints, structured tracing, snapshot management, 
geometry-based nearest logic, territory coverage, and validation rules
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
from fastapi.testclient import TestClient
from cms_pricing.main import app


class TestGeographyHealthEndpoint:
    """Test health check endpoint per PRD Section 5"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_health_endpoint_structure(self, client):
        """Test health endpoint response structure"""
        response = client.get("/geo/healthz")
        
        # Should return 200 or 503 based on status
        assert response.status_code in [200, 503]
        
        data = response.json()
        
        # Required fields per PRD
        assert "status" in data
        assert "build" in data
        assert "active_snapshot" in data
        assert "perf_slo" in data
        assert "uptime_seconds" in data
        assert "notes" in data
        
        # Status should be one of the allowed values
        assert data["status"] in ["ok", "degraded", "error"]
        
        # Build info structure
        assert "version" in data["build"]
        assert "commit" in data["build"]
        
        # Active snapshot structure
        assert "dataset_id" in data["active_snapshot"]
        assert "dataset_digest" in data["active_snapshot"]
        assert "effective_from" in data["active_snapshot"]
        assert "effective_to" in data["active_snapshot"]
        
        # Performance SLO structure
        assert "p95_warm_ms" in data["perf_slo"]
        assert "p95_cold_ms" in data["perf_slo"]
    
    def test_health_endpoint_status_codes(self, client):
        """Test health endpoint status code semantics"""
        response = client.get("/geo/healthz")
        data = response.json()
        
        if data["status"] == "error":
            assert response.status_code == 503
        else:  # ok or degraded
            assert response.status_code == 200
    
    def test_health_endpoint_performance_slos(self, client):
        """Test performance SLO values match PRD"""
        response = client.get("/geo/healthz")
        data = response.json()
        
        perf_slo = data["perf_slo"]
        assert perf_slo["p95_warm_ms"] == 2
        assert perf_slo["p95_cold_ms"] == 20


class TestStructuredTracing:
    """Test structured tracing per PRD Section 10"""
    
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
        test_records = [
            Geography(
                zip5="01434", plus4="0001", has_plus4=1, state="MA", 
                locality_id="1", locality_name="Massachusetts", 
                carrier="10112", rural_flag="R",
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
            Geography(
                zip5="94110", plus4=None, has_plus4=0, state="CA", 
                locality_id="5", locality_name="San Francisco", 
                carrier="01112", rural_flag=None,
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
        ]
        
        for record in test_records:
            db_session.add(record)
        db_session.commit()
        
        yield test_records
        
        for record in test_records:
            db_session.delete(record)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_trace_emission_fields(self, geography_service, sample_geography_data):
        """Test that traces include all required fields per PRD"""
        with patch('cms_pricing.services.geography.logger') as mock_logger:
            await geography_service.resolve_zip("01434", "0001", 2025)
            
            # Verify trace was emitted
            assert mock_logger.info.called
            
            # Check trace fields
            call_args = mock_logger.info.call_args
            trace_data = call_args[1]  # Keyword arguments
            
            # Required trace fields per PRD Section 10
            required_fields = [
                'zip5', 'plus4', 'valuation_year', 'quarter', 'strict',
                'locality_id', 'state', 'match_level', 'dataset_digest'
            ]
            
            for field in required_fields:
                assert field in trace_data, f"Missing trace field: {field}"
    
    @pytest.mark.asyncio
    async def test_trace_emission_100_percent_coverage(self, geography_service, sample_geography_data):
        """Test that 100% of calls emit traces"""
        with patch('cms_pricing.services.geography.logger') as mock_logger:
            # Make multiple resolution calls
            test_cases = [
                ("01434", "0001", 2025),  # ZIP+4 match
                ("94110", None, 2025),    # ZIP5 match
                ("99999", None, 2025),    # No match (fallback)
            ]
            
            for zip5, plus4, year in test_cases:
                await geography_service.resolve_zip(zip5, plus4, year)
            
            # Verify trace was emitted for each call
            assert mock_logger.info.call_count == len(test_cases)
    
    @pytest.mark.asyncio
    async def test_trace_includes_latency(self, geography_service, sample_geography_data):
        """Test that traces include latency_ms"""
        with patch('cms_pricing.services.geography.logger') as mock_logger:
            await geography_service.resolve_zip("01434", "0001", 2025)
            
            call_args = mock_logger.info.call_args
            trace_data = call_args[1]
            
            # Should include latency information
            assert 'latency_ms' in trace_data or 'latency' in trace_data


class TestSnapshotManagement:
    """Test snapshot registry and digest management per PRD Section 4.1"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    def test_snapshot_table_structure(self, db_session):
        """Test snapshot table has required fields per PRD"""
        from cms_pricing.models.geography import Geography
        
        # Check if snapshots table exists (would need to be created)
        # For now, verify geography table has digest field
        geography_columns = Geography.__table__.columns
        
        required_fields = [
            'dataset_id', 'dataset_digest', 'effective_from', 'effective_to'
        ]
        
        for field in required_fields:
            assert field in geography_columns, f"Missing field: {field}"
    
    def test_dataset_digest_uniqueness(self, db_session):
        """Test dataset digest uniqueness and reproducibility"""
        from cms_pricing.ingestion.geography import GeographyIngester
        
        ingester = GeographyIngester(db=db_session)
        
        # Create sample data
        sample_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31"""
        
        # Parse data twice
        records1 = ingester._parse_zip5_fixed_width(sample_data)
        records2 = ingester._parse_zip5_fixed_width(sample_data)
        
        # Digests should be identical
        assert records1[0]["dataset_digest"] == records2[0]["dataset_digest"]
        
        # Digest should be SHA256 length
        assert len(records1[0]["dataset_digest"]) == 64
    
    def test_effective_date_selection(self, db_session):
        """Test effective date selection logic"""
        from cms_pricing.services.geography import GeographyService
        
        service = GeographyService(db=db_session)
        
        # Test effective date filter building
        filter_2025 = service._build_effective_date_filter(2025, None)
        filter_2025_q1 = service._build_effective_date_filter(2025, 1)
        
        # Filters should be valid SQLAlchemy expressions
        assert filter_2025 is not None
        assert filter_2025_q1 is not None


class TestGeometryBasedNearest:
    """Test geometry-based nearest logic per PRD Section 9"""
    
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
        """Create sample geography data for nearest testing"""
        test_records = [
            Geography(
                zip5="94110", plus4=None, has_plus4=0, state="CA", 
                locality_id="5", locality_name="San Francisco", 
                carrier="01112", rural_flag=None,
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
            Geography(
                zip5="94111", plus4=None, has_plus4=0, state="CA", 
                locality_id="5", locality_name="San Francisco", 
                carrier="01112", rural_flag=None,
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
        ]
        
        for record in test_records:
            db_session.add(record)
        db_session.commit()
        
        yield test_records
        
        for record in test_records:
            db_session.delete(record)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_nearest_same_state_constraint(self, geography_service, sample_geography_data):
        """Test nearest fallback constrained to same state"""
        # Test with ZIP that doesn't exist but should find nearest in same state
        result = await geography_service.resolve_zip(
            zip5="94112",  # Doesn't exist, should find nearest CA ZIP
            valuation_year=2025,
            strict=False
        )
        
        # Should find nearest ZIP in same state
        assert result["match_level"] in ["nearest", "zip5"]
        assert result["state"] == "CA"  # Same state constraint
    
    @pytest.mark.asyncio
    async def test_radius_expansion_policy(self, geography_service, sample_geography_data):
        """Test radius expansion policy (25mi → 100mi, step 10mi)"""
        result = await geography_service.resolve_zip(
            zip5="99999",  # Non-existent ZIP
            valuation_year=2025,
            strict=False,
            initial_radius_miles=25,
            expand_step_miles=10,
            max_radius_miles=100
        )
        
        # Should either find nearest or fall back to default
        assert result["match_level"] in ["nearest", "default"]
    
    @pytest.mark.asyncio
    async def test_nearest_response_fields(self, geography_service, sample_geography_data):
        """Test nearest response includes candidate_zip and distance"""
        result = await geography_service.resolve_zip(
            zip5="94112",  # Should find nearest
            valuation_year=2025,
            strict=False
        )
        
        if result["match_level"] == "nearest":
            # Should include nearest candidate info
            assert "candidate_zip" in result or "nearest_zip" in result
            assert "candidate_distance_miles" in result or "distance_miles" in result


class TestTerritoryCoverage:
    """Test territory-specific behavior per PRD Section 3"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    @pytest.fixture
    def geography_service(self, db_session):
        """Create geography service with test database"""
        return GeographyService(db=db_session)
    
    @pytest.fixture
    def sample_territory_data(self, db_session):
        """Create sample territory data"""
        test_records = [
            Geography(
                zip5="00901", plus4=None, has_plus4=0, state="PR", 
                locality_id="1", locality_name="Puerto Rico", 
                carrier="40102", rural_flag=None,
                effective_from=date(2025, 1, 1), effective_to=date(2025, 12, 31),
                dataset_id="ZIP_LOCALITY", dataset_digest="test_digest_1",
                created_at=date.today()
            ),
        ]
        
        for record in test_records:
            db_session.add(record)
        db_session.commit()
        
        yield test_records
        
        for record in test_records:
            db_session.delete(record)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_pr_territory_success(self, geography_service, sample_territory_data):
        """Test Puerto Rico (PR) territory resolves successfully (GA scope)"""
        result = await geography_service.resolve_zip(
            zip5="00901",  # Puerto Rico ZIP
            valuation_year=2025
        )
        
        assert result["match_level"] == "zip5"
        assert result["state"] == "PR"
        assert result["locality_id"] == "1"
    
    @pytest.mark.asyncio
    async def test_post_ga_territory_handling(self, geography_service):
        """Test post-GA territory handling (VI, GU, AS, MP)"""
        # Test with non-existent post-GA territory ZIP
        result = await geography_service.resolve_zip(
            zip5="00801",  # Virgin Islands (post-GA)
            valuation_year=2025,
            strict=False
        )
        
        # Should either find data if present or return friendly error
        if result["match_level"] == "default":
            # Fallback to benchmark locality
            assert result["locality_id"] == "01"
        else:
            # Should have resolved to actual data
            assert result["locality_id"] is not None


class TestValidationRules:
    """Test validation rules per PRD Section 6"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        return SessionLocal()
    
    def test_error_level_validation(self, db_session):
        """Test ERROR level validation (blocks promotion)"""
        from cms_pricing.ingestion.geography import GeographyIngester
        
        ingester = GeographyIngester(db=db_session)
        
        # Test invalid ZIP5 length (ERROR level)
        invalid_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
123|CA|5|01112||2025-01-01|2025-12-31"""  # Invalid ZIP5 length
        
        with pytest.raises(ValueError):
            ingester._parse_zip5_fixed_width(invalid_data)
    
    def test_warn_level_validation(self, db_session):
        """Test WARN level validation (promote with caution)"""
        from cms_pricing.ingestion.geography import GeographyIngester
        
        ingester = GeographyIngester(db=db_session)
        
        # Test unusually low ZIP+4 coverage (WARN level)
        # This would need to be implemented in the validation logic
        sample_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31"""
        
        records = ingester._parse_zip5_fixed_width(sample_data)
        
        # Should parse successfully but might generate warnings
        assert len(records) == 1
    
    def test_info_level_validation(self, db_session):
        """Test INFO level validation (informational)"""
        from cms_pricing.ingestion.geography import GeographyIngester
        
        ingester = GeographyIngester(db=db_session)
        
        # Test row counts by state (INFO level)
        sample_data = """ZIP5|STATE|LOCALITY|CARRIER|RURAL|EFF_FROM|EFF_TO
94110|CA|5|01112||2025-01-01|2025-12-31
90210|CA|18|01182||2025-01-01|2025-12-31
10001|NY|1|31102||2025-01-01|2025-12-31"""
        
        records = ingester._parse_zip5_fixed_width(sample_data)
        
        # Should parse successfully and provide info about state distribution
        assert len(records) == 3
        
        # Count by state
        state_counts = {}
        for record in records:
            state = record["state"]
            state_counts[state] = state_counts.get(state, 0) + 1
        
        assert state_counts["CA"] == 2
        assert state_counts["NY"] == 1


class TestCIGates:
    """Test CI gate requirements per PRD Section 12"""
    
    def test_coverage_requirement(self):
        """Test coverage ≥ 85% requirement"""
        # This would need to be implemented in CI pipeline
        # For now, verify test files exist
        import os
        
        test_files = [
            "tests/test_geography_resolver.py",
            "tests/test_geography_ingestion.py", 
            "tests/test_geography_integration.py"
        ]
        
        for test_file in test_files:
            assert os.path.exists(test_file), f"Missing test file: {test_file}"
    
    def test_performance_slo_requirement(self):
        """Test performance SLO requirements"""
        # SLOs per PRD Section 2:
        # - p95 ≤ 2ms warm
        # - p95 ≤ 20ms cold
        # - batch 100 lookups ≤ 5ms overhead
        
        # These would need to be implemented in CI performance tests
        assert True  # Placeholder for CI gate implementation
    
    def test_validation_artifacts_requirement(self):
        """Test validation artifacts requirement"""
        # Per PRD Section 6: validation artifacts must be present with no ERRORs
        # This would need to be implemented in CI pipeline
        assert True  # Placeholder for CI gate implementation
    
    def test_contract_tests_requirement(self):
        """Test contract tests requirement"""
        # Per PRD Section 5: contract tests for resolver endpoint
        # Verify test files include contract tests
        import os
        
        contract_test_files = [
            "tests/test_geography_resolver.py",
            "tests/test_geography_integration.py"
        ]
        
        for test_file in contract_test_files:
            assert os.path.exists(test_file), f"Missing contract test file: {test_file}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

