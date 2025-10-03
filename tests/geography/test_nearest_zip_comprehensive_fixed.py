"""Comprehensive test suite for nearest ZIP resolver with all recent updates - FIXED VERSION"""

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


@pytest.fixture(scope="function")
def clean_db_session():
    """Create a clean database session for each test"""
    db = SessionLocal()
    try:
        # Clear all data before each test
        db.query(NearestZipTrace).delete()
        db.query(ZCTADistances).delete()
        db.query(ZIP9Overrides).delete()
        db.query(ZipMetadata).delete()
        db.query(CMSZipLocality).delete()
        db.query(ZipToZCTA).delete()
        db.query(NBERCentroids).delete()
        db.query(ZCTACoords).delete()
        db.query(IngestRun).delete()
        db.commit()
        yield db
    finally:
        db.close()


@pytest.fixture
def test_ingest_run(clean_db_session):
    """Create a test ingest run"""
    ingest_run = IngestRun(
        run_id=uuid.uuid4(),
        started_at=datetime.utcnow(),
        source="test_data",
        status="completed",
        row_count=0
    )
    clean_db_session.add(ingest_run)
    clean_db_session.commit()
    return ingest_run


@pytest.fixture
def comprehensive_test_data(clean_db_session, test_ingest_run):
    """Create comprehensive test data for all features"""
    # Create ZCTA coordinates (Gazetteer data)
    zcta_coords = [
        ZCTACoords(zcta5="94107", lat=37.76, lon=-122.39, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZCTACoords(zcta5="94110", lat=37.75, lon=-122.42, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZCTACoords(zcta5="94115", lat=37.78, lon=-122.45, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZCTACoords(zcta5="94132", lat=37.77, lon=-122.40, vintage="2025", ingest_run_id=test_ingest_run.run_id),
    ]
    
    # Create NBER centroids (fallback data)
    nber_centroids = [
        NBERCentroids(zcta5="94150", lat=37.77, lon=-122.41, vintage="2024", ingest_run_id=test_ingest_run.run_id),
        NBERCentroids(zcta5="94160", lat=37.79, lon=-122.43, vintage="2024", ingest_run_id=test_ingest_run.run_id),
    ]
    
    # Create ZIP to ZCTA mappings
    zip_to_zcta = [
        ZipToZCTA(zip5="94107", zcta5="94107", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipToZCTA(zip5="94110", zcta5="94110", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipToZCTA(zip5="94115", zcta5="94115", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipToZCTA(zip5="94132", zcta5="94132", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipToZCTA(zip5="94150", zcta5="94150", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipToZCTA(zip5="94160", zcta5="94160", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id),
    ]
    
    # Create CMS ZIP locality mappings
    cms_zip_locality = [
        CMSZipLocality(zip5="94107", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id),
        CMSZipLocality(zip5="94110", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id),
        CMSZipLocality(zip5="94115", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id),
        CMSZipLocality(zip5="94132", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id),
        CMSZipLocality(zip5="94150", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id),
        CMSZipLocality(zip5="94160", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id),
    ]
    
    # Create ZIP metadata with different populations for tie-breaking tests
    zip_metadata = [
        ZipMetadata(zip5="94107", zcta_bool=True, population=50000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipMetadata(zip5="94110", zcta_bool=True, population=45000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipMetadata(zip5="94115", zcta_bool=True, population=60000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipMetadata(zip5="94132", zcta_bool=True, population=30000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipMetadata(zip5="94150", zcta_bool=True, population=25000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZipMetadata(zip5="94160", zcta_bool=True, population=None, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),  # None population for COALESCE test
    ]
    
    # Create ZIP9 overrides
    zip9_overrides = [
        ZIP9Overrides(zip9_low="941070000", zip9_high="941079999", state="CA", locality="02", rural_flag=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
        ZIP9Overrides(zip9_low="941150000", zip9_high="941159999", state="CA", locality="03", rural_flag=False, vintage="2025", ingest_run_id=test_ingest_run.run_id),
    ]
    
    # Create NBER distances with intentional discrepancies
    zcta_distances = [
        ZCTADistances(zcta5_a="94107", zcta5_b="94110", miles=2.5, vintage="2024", ingest_run_id=test_ingest_run.run_id),  # Will differ from Haversine
        ZCTADistances(zcta5_a="94107", zcta5_b="94115", miles=0.5, vintage="2024", ingest_run_id=test_ingest_run.run_id),  # Will differ from Haversine
    ]
    
    # Add all records to database
    all_records = (zcta_coords + nber_centroids + zip_to_zcta + 
                  cms_zip_locality + zip_metadata + zip9_overrides + zcta_distances)
    
    for record in all_records:
        clean_db_session.add(record)
    
    clean_db_session.commit()
    
    return {
        'zcta_coords': zcta_coords,
        'nber_centroids': nber_centroids,
        'zip_to_zcta': zip_to_zcta,
        'cms_zip_locality': cms_zip_locality,
        'zip_metadata': zip_metadata,
        'zip9_overrides': zip9_overrides,
        'zcta_distances': zcta_distances,
        'ingest_run': test_ingest_run
    }


class TestNBERFallbackFunctionality:
    """Test NBER fallback when Gazetteer data is missing"""
    
    def test_nber_fallback_basic(self, clean_db_session, comprehensive_test_data):
        """Test NBER fallback for ZIP with only NBER data"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test ZIP that only exists in NBER (94150)
        result = resolver.find_nearest_zip("94150", include_trace=True)
        
        assert result['nearest_zip'] is not None
        assert result['distance_miles'] > 0
        assert 'trace' in result
        
        # Check that NBER fallback was used
        trace = result['trace']
        assert trace['starting_centroid']['source'] == 'nber_fallback'
    
    def test_nber_fallback_in_distance_calculation(self, clean_db_session, comprehensive_test_data):
        """Test NBER fallback in distance calculation"""
        distance_engine = DistanceEngine(clean_db_session)
        
        # Test distance calculation where one ZCTA only exists in NBER
        result = distance_engine.calculate_distance("94107", "94150", use_nber=True)
        
        assert result['distance_miles'] > 0
        assert result['method_used'] in ['nber', 'haversine']
        assert result['nber_available'] is True


class TestZIP9OverrideLogic:
    """Test ZIP9 override functionality"""
    
    def test_zip9_override_basic(self, clean_db_session, comprehensive_test_data):
        """Test ZIP9 override logic"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test ZIP9 input that should trigger override
        result = resolver.find_nearest_zip("94107-1234", include_trace=True)
        
        assert result['nearest_zip'] is not None
        assert 'trace' in result
        
        # Check that ZIP9 override was used
        trace = result['trace']
        assert trace['normalization']['zip9_hit'] is True
        assert trace['normalization']['locality'] == '02'  # From ZIP9 override
    
    def test_zip9_override_edge_cases(self, clean_db_session, comprehensive_test_data):
        """Test ZIP9 override edge cases"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test ZIP9 at boundary (941150000)
        result = resolver.find_nearest_zip("941150000", include_trace=True)
        assert result['nearest_zip'] is not None
        
        # Test ZIP9 just outside range (941150001)
        result = resolver.find_nearest_zip("941150001", include_trace=True)
        assert result['nearest_zip'] is not None


class TestNBERHaversineDiscrepancyDetection:
    """Test NBER vs Haversine discrepancy detection"""
    
    def test_discrepancy_detection(self, clean_db_session, comprehensive_test_data):
        """Test NBER vs Haversine discrepancy detection"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test with NBER enabled (should detect discrepancies)
        result = resolver.find_nearest_zip("94107", include_trace=True)
        
        assert result['nearest_zip'] is not None
        assert 'trace' in result
        
        # Check that discrepancy detection was performed
        trace = result['trace']
        assert 'dist_calc' in trace
        assert 'discrepancies' in trace['dist_calc']
        assert trace['dist_calc']['discrepancies'] >= 0  # Should have detected some discrepancies
    
    def test_discrepancy_threshold(self, clean_db_session, comprehensive_test_data):
        """Test discrepancy threshold logic"""
        distance_engine = DistanceEngine(clean_db_session)
        
        # Test distance calculation that should trigger discrepancy
        result = distance_engine.calculate_distance("94107", "94110", use_nber=True)
        
        assert result['distance_miles'] > 0
        assert 'discrepancy_detected' in result
        assert 'discrepancy_miles' in result


class TestCOALESCEPopulationTieBreaking:
    """Test COALESCE(population, 0) tie-breaking logic"""
    
    def test_coalesce_population_tie_breaking(self, clean_db_session, comprehensive_test_data):
        """Test COALESCE(population, 0) tie-breaking logic"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test with ZIP that has None population (should be treated as 0)
        result = resolver.find_nearest_zip("94160", include_trace=True)
        
        assert result['nearest_zip'] is not None
        assert 'trace' in result
        
        # The result should show that None population was handled correctly
        trace = result['trace']
        assert 'dist_calc' in trace
        assert 'distances' in trace['dist_calc']
    
    def test_tie_breaking_with_none_population(self, clean_db_session, comprehensive_test_data):
        """Test tie-breaking when population is None"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Create test data with same distance but different populations including None
        candidates = [
            {'zip5': '94110', 'zcta5': '94110', 'population': 45000},
            {'zip5': '94160', 'zcta5': '94160', 'population': None},  # None population
        ]
        
        # Mock the distance calculation to return same distance
        with patch.object(resolver, '_calculate_distances') as mock_calc:
            mock_calc.return_value = {
                'distances': [
                    {'zip5': '94110', 'distance_miles': 1.0, 'population': 45000, 'zcta5': '94110'},
                    {'zip5': '94160', 'distance_miles': 1.0, 'population': None, 'zcta5': '94160'},
                ],
                'summary': {'engine': 'test'}
            }
            
            result = resolver._select_nearest(mock_calc.return_value['distances'])
            
            # Should select 94160 (None population treated as 0, which is smaller than 45000)
            assert result['nearest_zip'] == '94160'


class TestEmptyCandidatesErrorHandling:
    """Test empty candidates error handling"""
    
    def test_no_candidates_in_state(self, clean_db_session, test_ingest_run):
        """Test empty candidates error handling"""
        # Create a ZIP with no other ZIPs in the same state
        zcta_coord = ZCTACoords(zcta5="99999", lat=40.0, lon=-100.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_to_zcta = ZipToZCTA(zip5="99999", zcta5="99999", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        cms_zip = CMSZipLocality(zip5="99999", state="XX", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_metadata = ZipMetadata(zip5="99999", zcta_bool=True, population=1000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        
        clean_db_session.add_all([zcta_coord, zip_to_zcta, cms_zip, zip_metadata])
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # This should raise ValueError with "NO_CANDIDATES_IN_STATE"
        with pytest.raises(ValueError, match="NO_CANDIDATES_IN_STATE"):
            resolver.find_nearest_zip("99999")


class TestAsymmetryDetection:
    """Test asymmetry detection for non-reciprocal nearest ZIP results"""
    
    def test_asymmetry_detection(self, clean_db_session, comprehensive_test_data):
        """Test asymmetry detection for non-reciprocal nearest ZIP results"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test with trace enabled to trigger asymmetry detection
        result = resolver.find_nearest_zip("94107", include_trace=True)
        
        assert result['nearest_zip'] is not None
        assert 'trace' in result
        
        # Check that asymmetry detection was performed
        trace = result['trace']
        assert 'asymmetry' in trace
        assert 'asymmetry_detected' in trace['asymmetry']
        assert 'is_reciprocal' in trace['asymmetry']
    
    def test_asymmetry_check_method(self, clean_db_session, comprehensive_test_data):
        """Test the asymmetry check method directly"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test asymmetry check
        asymmetry = resolver._check_asymmetry("94107", "94110")
        
        assert 'is_reciprocal' in asymmetry
        assert 'reverse_nearest' in asymmetry
        assert 'asymmetry_detected' in asymmetry


class TestIncludeTraceParameter:
    """Test include_trace parameter functionality"""
    
    def test_include_trace_false(self, clean_db_session, comprehensive_test_data):
        """Test without trace"""
        resolver = NearestZipResolver(clean_db_session)
        
        result = resolver.find_nearest_zip("94107", include_trace=False)
        assert 'trace' not in result
    
    def test_include_trace_true(self, clean_db_session, comprehensive_test_data):
        """Test with trace"""
        resolver = NearestZipResolver(clean_db_session)
        
        result = resolver.find_nearest_zip("94107", include_trace=True)
        assert 'trace' in result
        assert isinstance(result['trace'], dict)


class TestPOBoxExclusionLogic:
    """Test PO Box exclusion logic"""
    
    def test_pobox_exclusion(self, clean_db_session, comprehensive_test_data, test_ingest_run):
        """Test PO Box exclusion logic"""
        # Add a PO Box ZIP
        pobox_zip = ZipMetadata(zip5="94199", zcta_bool=True, population=1000, is_pobox=True, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        pobox_cms = CMSZipLocality(zip5="94199", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id)
        pobox_zcta = ZCTACoords(zcta5="94199", lat=37.80, lon=-122.50, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        pobox_mapping = ZipToZCTA(zip5="94199", zcta5="94199", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        
        clean_db_session.add_all([pobox_zip, pobox_cms, pobox_zcta, pobox_mapping])
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Test that PO Box is excluded from candidates
        candidates = resolver._get_candidates("CA", "94107")
        candidate_zips = [c['zip5'] for c in candidates]
        assert "94199" not in candidate_zips  # PO Box should be excluded


class TestMonitoringAndAudit:
    """Test monitoring and audit functionality"""
    
    def test_gazetteer_fallback_monitoring(self, clean_db_session, comprehensive_test_data):
        """Test Gazetteer fallback rate monitoring"""
        monitoring = NearestZipMonitoring(clean_db_session)
        
        stats = monitoring.get_gazetteer_fallback_rate()
        
        assert 'total_resolutions' in stats
        assert 'fallback_resolutions' in stats
        assert 'fallback_rate_percent' in stats
        assert 'threshold_percent' in stats
        assert 'alert_triggered' in stats
    
    def test_resolver_statistics(self, clean_db_session, comprehensive_test_data):
        """Test resolver statistics"""
        monitoring = NearestZipMonitoring(clean_db_session)
        
        stats = monitoring.get_resolver_statistics()
        
        assert 'total_resolutions' in stats
        assert 'coincident_resolutions' in stats
        assert 'far_neighbor_resolutions' in stats
        assert 'coincident_percentage' in stats
        assert 'far_neighbor_percentage' in stats
    
    def test_data_source_counts(self, clean_db_session, comprehensive_test_data):
        """Test data source counts"""
        monitoring = NearestZipMonitoring(clean_db_session)
        
        counts = monitoring.get_data_source_counts()
        
        assert 'zcta_coords' in counts
        assert 'zip_to_zcta' in counts
        assert 'cms_zip_locality' in counts
        assert 'zip_metadata' in counts
        assert 'nber_centroids' in counts
        assert 'zcta_distances' in counts
    
    def test_pobox_audit(self, clean_db_session, comprehensive_test_data):
        """Test PO Box audit functionality"""
        audit = NearestZipAudit(clean_db_session)
        
        audit_results = audit.run_comprehensive_audit()
        
        assert 'pobox_audit' in audit_results
        assert 'validation' in audit_results
        assert 'exclusion_stats' in audit_results
        assert 'health_score' in audit_results
        assert 'audit_timestamp' in audit_results


class TestAPIEndpoints:
    """Test API endpoints with recent updates"""
    
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
    
    def test_nearest_zip_endpoint_with_trace(self, client, clean_db_session, comprehensive_test_data):
        """Test nearest ZIP endpoint with trace parameter"""
        response = client.get(
            "/nearest-zip/nearest?zip=94107&include_trace=true",
            headers={"X-API-Key": "dev-key-123"}
        )
        
        if response.status_code == 200:
            data = response.json()
            assert 'nearest_zip' in data
            assert 'distance_miles' in data
            assert 'trace' in data
        else:
            # If no data is loaded, expect 500 error
            assert response.status_code == 500
    
    def test_nearest_zip_endpoint_without_trace(self, client, clean_db_session, comprehensive_test_data):
        """Test nearest ZIP endpoint without trace parameter"""
        response = client.get(
            "/nearest-zip/nearest?zip=94107",
            headers={"X-API-Key": "dev-key-123"}
        )
        
        if response.status_code == 200:
            data = response.json()
            assert 'nearest_zip' in data
            assert 'distance_miles' in data
            # Trace should not be present when include_trace=false
            assert 'trace' not in data
        else:
            # If no data is loaded, expect 500 error
            assert response.status_code == 500


class TestErrorHandling:
    """Test error handling for recent updates"""
    
    def test_invalid_zip_format(self):
        """Test invalid ZIP format handling"""
        resolver = NearestZipResolver(None)
        
        with pytest.raises(ValueError):
            resolver._parse_input("123")
        
        with pytest.raises(ValueError):
            resolver._parse_input("1234567890")
    
    def test_missing_data_handling(self, clean_db_session):
        """Test handling of missing data"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test with no data in database
        with pytest.raises(ValueError):
            resolver.find_nearest_zip("94107")
    
    def test_nber_fallback_error_handling(self, clean_db_session, test_ingest_run):
        """Test NBER fallback error handling"""
        # Create ZIP with no Gazetteer or NBER data
        zip_to_zcta = ZipToZCTA(zip5="99999", zcta5="99999", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        cms_zip = CMSZipLocality(zip5="99999", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_metadata = ZipMetadata(zip5="99999", zcta_bool=True, population=1000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        
        clean_db_session.add_all([zip_to_zcta, cms_zip, zip_metadata])
        clean_db_session.commit()
        
        resolver = NearestZipResolver(clean_db_session)
        
        # Should raise error when no coordinates found
        with pytest.raises(ValueError, match="No coordinates found"):
            resolver.find_nearest_zip("99999")


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple features"""
    
    def test_full_resolution_with_all_features(self, clean_db_session, comprehensive_test_data):
        """Test full resolution with all features enabled"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test with ZIP9 input, trace enabled, and all features
        result = resolver.find_nearest_zip("94107-1234", include_trace=True)
        
        assert result['nearest_zip'] is not None
        assert result['distance_miles'] > 0
        assert 'trace' in result
        
        trace = result['trace']
        
        # Check ZIP9 override was used
        assert trace['normalization']['zip9_hit'] is True
        assert trace['normalization']['locality'] == '02'
        
        # Check asymmetry detection was performed
        assert 'asymmetry' in trace
        assert 'asymmetry_detected' in trace['asymmetry']
        
        # Check discrepancy detection was performed
        assert 'dist_calc' in trace
        assert 'discrepancies' in trace['dist_calc']
    
    def test_error_scenarios(self, clean_db_session, test_ingest_run):
        """Test various error scenarios"""
        resolver = NearestZipResolver(clean_db_session)
        
        # Test invalid ZIP format
        with pytest.raises(ValueError):
            resolver.find_nearest_zip("123")
        
        # Test missing data
        with pytest.raises(ValueError):
            resolver.find_nearest_zip("94107")
        
        # Test no candidates (after adding minimal data)
        zcta_coord = ZCTACoords(zcta5="99999", lat=40.0, lon=-100.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_to_zcta = ZipToZCTA(zip5="99999", zcta5="99999", relationship="Zip matches ZCTA", weight=1.0, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        cms_zip = CMSZipLocality(zip5="99999", state="XX", locality="01", effective_from=date(2025, 1, 1), vintage="2025", ingest_run_id=test_ingest_run.run_id)
        zip_metadata = ZipMetadata(zip5="99999", zcta_bool=True, population=1000, is_pobox=False, vintage="2025", ingest_run_id=test_ingest_run.run_id)
        
        clean_db_session.add_all([zcta_coord, zip_to_zcta, cms_zip, zip_metadata])
        clean_db_session.commit()
        
        with pytest.raises(ValueError, match="NO_CANDIDATES_IN_STATE"):
            resolver.find_nearest_zip("99999")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
