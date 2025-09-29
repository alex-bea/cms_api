"""Simple test suite for nearest ZIP resolver - focuses on core functionality"""

import pytest
import uuid
from datetime import datetime, date
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZIP9Overrides,
    ZipMetadata, NearestZipTrace, NBERCentroids, ZCTADistances, IngestRun
)
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver
from cms_pricing.services.nearest_zip_distance import DistanceEngine
from cms_pricing.services.nearest_zip_monitoring import NearestZipMonitoring
from cms_pricing.services.nearest_zip_audit import NearestZipAudit
from cms_pricing.main import app


class TestBasicFunctionality:
    """Test basic functionality without complex database setup"""
    
    def test_zip_parsing(self):
        """Test ZIP input parsing"""
        resolver = NearestZipResolver(None)
        
        # Test ZIP5
        zip5, zip9 = resolver._parse_input("94107")
        assert zip5 == "94107"
        assert zip9 is None
        
        # Test ZIP9 with dash
        zip5, zip9 = resolver._parse_input("94107-1234")
        assert zip5 == "94107"
        assert zip9 == "941071234"
        
        # Test ZIP9 without dash
        zip5, zip9 = resolver._parse_input("941071234")
        assert zip5 == "94107"
        assert zip9 == "941071234"
    
    def test_invalid_zip_format(self):
        """Test invalid ZIP format handling"""
        resolver = NearestZipResolver(None)
        
        with pytest.raises(ValueError):
            resolver._parse_input("123")
        
        with pytest.raises(ValueError):
            resolver._parse_input("1234567890")
    
    def test_haversine_distance_calculation(self):
        """Test Haversine distance calculation"""
        from cms_pricing.services.nearest_zip_distance import haversine_distance
        
        # Distance between San Francisco and Oakland (approximately 8 miles)
        distance = haversine_distance(37.7749, -122.4194, 37.8044, -122.2711)
        assert 7.0 < distance < 9.0  # Should be around 8 miles
        
        # Distance between same point (should be 0)
        distance_same = haversine_distance(37.7749, -122.4194, 37.7749, -122.4194)
        assert distance_same == 0.0
    
    def test_calculate_flags(self):
        """Test flag calculation"""
        resolver = NearestZipResolver(None)
        
        # Coincident
        flags = resolver._calculate_flags(0.5)
        assert flags['coincident'] is True
        assert flags['far_neighbor'] is False
        
        # Far neighbor
        flags = resolver._calculate_flags(15.0)
        assert flags['coincident'] is False
        assert flags['far_neighbor'] is True
        
        # Normal
        flags = resolver._calculate_flags(5.0)
        assert flags['coincident'] is False
        assert flags['far_neighbor'] is False


class TestNBERFallbackLogic:
    """Test NBER fallback logic without database"""
    
    def test_nber_fallback_in_distance_engine(self):
        """Test NBER fallback in distance engine"""
        # Mock database session
        mock_db = Mock()
        
        # Mock query results - no Gazetteer data, but NBER data available
        mock_db.query.return_value.filter.return_value.first.side_effect = [
            None,  # No Gazetteer data
            Mock(lat=37.77, lon=-122.41)  # NBER data available
        ]
        
        distance_engine = DistanceEngine(mock_db)
        
        # Test distance calculation with NBER fallback
        result = distance_engine.calculate_distance("94150", "94107", use_nber=True)
        
        assert result['distance_miles'] > 0
        assert result['method_used'] in ['nber', 'haversine']
        assert result['nber_available'] is True
    
    def test_nber_haversine_discrepancy_detection(self):
        """Test NBER vs Haversine discrepancy detection"""
        # Mock database session
        mock_db = Mock()
        
        # Mock NBER distance that differs significantly from Haversine
        mock_nber_distance = 0.5  # Very different from Haversine
        mock_db.query.return_value.filter.return_value.first.return_value = Mock(
            miles=mock_nber_distance
        )
        
        distance_engine = DistanceEngine(mock_db)
        
        # Test distance calculation that should trigger discrepancy
        result = distance_engine.calculate_distance("94107", "94110", use_nber=True)
        
        assert result['distance_miles'] > 0
        assert 'discrepancy_detected' in result
        assert 'discrepancy_miles' in result


class TestCOALESCEPopulationTieBreaking:
    """Test COALESCE population tie-breaking logic"""
    
    def test_tie_breaking_with_none_population(self):
        """Test tie-breaking when population is None"""
        resolver = NearestZipResolver(None)
        
        # Create test data with same distance but different populations including None
        distances = [
            {'zip5': '94110', 'distance_miles': 1.0, 'population': 45000, 'zcta5': '94110'},
            {'zip5': '94160', 'distance_miles': 1.0, 'population': None, 'zcta5': '94160'},
        ]
        
        result = resolver._select_nearest(distances)
        
        # Should select 94160 (None population treated as 0, which is smaller than 45000)
        assert result['nearest_zip'] == '94160'
        assert result['distance_miles'] == 1.0
    
    def test_tie_breaking_with_equal_populations(self):
        """Test tie-breaking when populations are equal"""
        resolver = NearestZipResolver(None)
        
        distances = [
            {'zip5': '94115', 'distance_miles': 1.0, 'population': 50000, 'zcta5': '94115'},
            {'zip5': '94110', 'distance_miles': 1.0, 'population': 50000, 'zcta5': '94110'},  # Lexicographically first
        ]
        
        result = resolver._select_nearest(distances)
        
        # Should select 94110 (lexicographically first)
        assert result['nearest_zip'] == '94110'


class TestAsymmetryDetection:
    """Test asymmetry detection logic"""
    
    def test_asymmetry_check_method(self):
        """Test the asymmetry check method directly"""
        # Mock database session
        mock_db = Mock()
        
        # Mock the resolver to return different nearest ZIPs
        resolver = NearestZipResolver(mock_db)
        
        # Mock the find_nearest_zip method to return different results
        with patch.object(resolver, 'find_nearest_zip') as mock_find:
            mock_find.return_value = {
                'nearest_zip': '94142',
                'distance_miles': 1.5
            }
            
            asymmetry = resolver._check_asymmetry("94107", "94110")
            
            assert 'is_reciprocal' in asymmetry
            assert 'reverse_nearest' in asymmetry
            assert 'asymmetry_detected' in asymmetry
            assert asymmetry['reverse_nearest'] == '94142'
            assert asymmetry['is_reciprocal'] is False
            assert asymmetry['asymmetry_detected'] is True


class TestAPIEndpoints:
    """Test API endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_health_endpoint_no_auth(self, client):
        """Test health endpoint without authentication"""
        response = client.get("/nearest-zip/health")
        assert response.status_code == 200
        
        data = response.json()
        assert 'status' in data
        assert 'message' in data
        assert 'details' in data
    
    def test_stats_endpoint_with_auth(self, client):
        """Test stats endpoint with API key"""
        response = client.get("/nearest-zip/stats", headers={"X-API-Key": "dev-key-123"})
        assert response.status_code == 200
        
        data = response.json()
        assert 'data_sources' in data
        assert 'resolver_stats' in data
        assert 'gazetteer_fallback' in data
    
    def test_stats_endpoint_no_auth(self, client):
        """Test stats endpoint without API key"""
        response = client.get("/nearest-zip/stats")
        assert response.status_code == 401
    
    def test_audit_endpoint(self, client):
        """Test PO Box audit endpoint"""
        response = client.get("/nearest-zip/audit/pobox", headers={"X-API-Key": "dev-key-123"})
        assert response.status_code == 200
        
        data = response.json()
        assert 'pobox_audit' in data
        assert 'validation' in data
        assert 'health_score' in data


class TestMonitoringAndAudit:
    """Test monitoring and audit functionality"""
    
    def test_monitoring_with_empty_database(self):
        """Test monitoring with empty database"""
        # Create a real database session
        db = SessionLocal()
        try:
            monitoring = NearestZipMonitoring(db)
            
            # Test data source counts
            counts = monitoring.get_data_source_counts()
            assert 'zcta_coords' in counts
            assert 'zip_to_zcta' in counts
            assert 'cms_zip_locality' in counts
            assert 'zip_metadata' in counts
            assert 'nber_centroids' in counts
            assert 'zcta_distances' in counts
            
            # Test resolver statistics
            stats = monitoring.get_resolver_statistics()
            assert 'total_resolutions' in stats
            assert 'coincident_resolutions' in stats
            assert 'far_neighbor_resolutions' in stats
            assert 'coincident_percentage' in stats
            assert 'far_neighbor_percentage' in stats
            
            # Test Gazetteer fallback rate
            fallback_stats = monitoring.get_gazetteer_fallback_rate()
            assert 'total_resolutions' in fallback_stats
            assert 'fallback_resolutions' in fallback_stats
            assert 'fallback_rate_percent' in fallback_stats
            assert 'threshold_percent' in fallback_stats
            assert 'alert_triggered' in fallback_stats
            
        finally:
            db.close()
    
    def test_audit_with_empty_database(self):
        """Test audit with empty database"""
        # Create a real database session
        db = SessionLocal()
        try:
            audit = NearestZipAudit(db)
            
            audit_results = audit.run_comprehensive_audit()
            
            assert 'pobox_audit' in audit_results
            assert 'validation' in audit_results
            assert 'exclusion_stats' in audit_results
            assert 'health_score' in audit_results
            assert 'audit_timestamp' in audit_results
            
            # Health score should be calculated
            assert isinstance(audit_results['health_score'], (int, float))
            assert 0 <= audit_results['health_score'] <= 100
            
        finally:
            db.close()


class TestErrorHandling:
    """Test error handling"""
    
    def test_missing_data_handling(self):
        """Test handling of missing data"""
        # Create a real database session
        db = SessionLocal()
        try:
            resolver = NearestZipResolver(db)
            
            # Test with no data in database
            with pytest.raises(ValueError):
                resolver.find_nearest_zip("94107")
                
        finally:
            db.close()
    
    def test_invalid_zip_format_handling(self):
        """Test invalid ZIP format handling"""
        resolver = NearestZipResolver(None)
        
        # Test various invalid formats
        invalid_zips = ["123", "1234567890", "abc", "94107-", "-1234"]
        
        for invalid_zip in invalid_zips:
            with pytest.raises(ValueError):
                resolver._parse_input(invalid_zip)


class TestIntegrationScenarios:
    """Test integration scenarios"""
    
    def test_full_resolution_with_mocked_data(self):
        """Test full resolution with mocked data"""
        # Mock database session
        mock_db = Mock()
        
        # Mock all the database queries
        mock_db.query.return_value.filter.return_value.first.side_effect = [
            # _get_state_and_locality
            Mock(state="CA", locality="01"),
            # _get_zcta_info
            Mock(starting_zcta="94107", zcta_weight=1.0, relationship="Zip matches ZCTA"),
            # _get_starting_centroid
            Mock(lat=37.76, lon=-122.39),
            # _get_candidates
            [{'zip5': '94110', 'zcta5': '94110', 'population': 45000}],
        ]
        
        resolver = NearestZipResolver(mock_db)
        
        # Mock the distance calculation
        with patch.object(resolver, '_calculate_distances') as mock_calc:
            mock_calc.return_value = {
                'distances': [
                    {'zip5': '94110', 'distance_miles': 1.5, 'population': 45000, 'zcta5': '94110'}
                ],
                'summary': {'engine': 'test'}
            }
            
            # Test resolution
            result = resolver.find_nearest_zip("94107", include_trace=True)
            
            assert result['nearest_zip'] == '94110'
            assert result['distance_miles'] == 1.5
            assert 'trace' in result
            
            # Check trace structure
            trace = result['trace']
            assert 'input' in trace
            assert 'normalization' in trace
            assert 'starting_centroid' in trace
            assert 'candidates' in trace
            assert 'dist_calc' in trace
            assert 'result' in trace
            assert 'flags' in trace
            assert 'asymmetry' in trace


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
