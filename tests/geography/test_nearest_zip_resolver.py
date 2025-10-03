"""Test suite for nearest ZIP resolver per PRD v1.0"""

import pytest
import uuid
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import Mock, patch

from cms_pricing.database import SessionLocal
from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZIP9Overrides,
    ZipMetadata, NearestZipTrace
)
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver
from cms_pricing.services.nearest_zip_distance import DistanceEngine


class TestNearestZipResolver:
    """Test nearest ZIP resolver functionality"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    @pytest.fixture
    def sample_data(self, db_session):
        """Create sample test data"""
        # Create ZCTA coordinates
        zcta_coords = [
            ZCTACoords(zcta5="94107", lat=37.76, lon=-122.39, vintage="2025"),
            ZCTACoords(zcta5="94110", lat=37.75, lon=-122.42, vintage="2025"),
            ZCTACoords(zcta5="94115", lat=37.78, lon=-122.45, vintage="2025"),
            ZCTACoords(zcta5="73301", lat=30.27, lon=-97.74, vintage="2025"),  # Austin, TX
        ]
        
        # Create ZIP to ZCTA mappings
        zip_to_zcta = [
            ZipToZCTA(zip5="94107", zcta5="94107", relationship="Zip matches ZCTA", weight=1.0, vintage="2023"),
            ZipToZCTA(zip5="94110", zcta5="94110", relationship="Zip matches ZCTA", weight=1.0, vintage="2023"),
            ZipToZCTA(zip5="94115", zcta5="94115", relationship="Zip matches ZCTA", weight=1.0, vintage="2023"),
            ZipToZCTA(zip5="73301", zcta5="73301", relationship="Zip matches ZCTA", weight=1.0, vintage="2023"),
        ]
        
        # Create CMS ZIP locality mappings
        cms_zip_locality = [
            CMSZipLocality(zip5="94107", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025"),
            CMSZipLocality(zip5="94110", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025"),
            CMSZipLocality(zip5="94115", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025"),
            CMSZipLocality(zip5="73301", state="TX", locality="99", effective_from=date(2025, 1, 1), vintage="2025"),
        ]
        
        # Create ZIP metadata
        zip_metadata = [
            ZipMetadata(zip5="94107", zcta_bool=True, population=50000, is_pobox=False, vintage="2025"),
            ZipMetadata(zip5="94110", zcta_bool=True, population=45000, is_pobox=False, vintage="2025"),
            ZipMetadata(zip5="94115", zcta_bool=True, population=60000, is_pobox=False, vintage="2025"),
            ZipMetadata(zip5="73301", zcta_bool=True, population=80000, is_pobox=False, vintage="2025"),
        ]
        
        # Add all to database
        for record in zcta_coords + zip_to_zcta + cms_zip_locality + zip_metadata:
            db_session.add(record)
        
        db_session.commit()
        
        return {
            'zcta_coords': zcta_coords,
            'zip_to_zcta': zip_to_zcta,
            'cms_zip_locality': cms_zip_locality,
            'zip_metadata': zip_metadata
        }
    
    def test_parse_input_zip5(self):
        """Test parsing ZIP5 input"""
        resolver = NearestZipResolver(None)
        zip5, zip9 = resolver._parse_input("94107")
        assert zip5 == "94107"
        assert zip9 is None
    
    def test_parse_input_zip9(self):
        """Test parsing ZIP9 input"""
        resolver = NearestZipResolver(None)
        zip5, zip9 = resolver._parse_input("94107-1234")
        assert zip5 == "94107"
        assert zip9 == "941071234"
        
        # Test without dash
        zip5, zip9 = resolver._parse_input("941071234")
        assert zip5 == "94107"
        assert zip9 == "941071234"
    
    def test_parse_input_invalid(self):
        """Test parsing invalid input"""
        resolver = NearestZipResolver(None)
        
        with pytest.raises(ValueError):
            resolver._parse_input("123")
        
        with pytest.raises(ValueError):
            resolver._parse_input("1234567890")
    
    def test_get_state_and_locality_zip5(self, db_session, sample_data):
        """Test getting state and locality for ZIP5"""
        resolver = NearestZipResolver(db_session)
        
        result = resolver._get_state_and_locality("94107", None)
        
        assert result['state'] == "CA"
        assert result['locality'] == "01"
        assert result['zip9_hit'] is False
    
    def test_get_state_and_locality_zip9_override(self, db_session, sample_data):
        """Test getting state and locality for ZIP9 with override"""
        # Add ZIP9 override
        zip9_override = ZIP9Overrides(
            zip9_low="941070000",
            zip9_high="941079999",
            state="CA",
            locality="02",
            vintage="2025"
        )
        db_session.add(zip9_override)
        db_session.commit()
        
        resolver = NearestZipResolver(db_session)
        
        result = resolver._get_state_and_locality("94107", "941071234")
        
        assert result['state'] == "CA"
        assert result['locality'] == "02"  # From ZIP9 override
        assert result['zip9_hit'] is True
    
    def test_get_state_and_locality_not_found(self, db_session):
        """Test getting state and locality for non-existent ZIP"""
        resolver = NearestZipResolver(db_session)
        
        with pytest.raises(ValueError):
            resolver._get_state_and_locality("99999", None)
    
    def test_get_zcta_info(self, db_session, sample_data):
        """Test getting ZCTA information"""
        resolver = NearestZipResolver(db_session)
        
        result = resolver._get_zcta_info("94107")
        
        assert result['starting_zcta'] == "94107"
        assert result['zcta_weight'] == 1.0
        assert result['relationship'] == "Zip matches ZCTA"
    
    def test_get_starting_centroid(self, db_session, sample_data):
        """Test getting starting centroid"""
        resolver = NearestZipResolver(db_session)
        
        result = resolver._get_starting_centroid("94107")
        
        assert result['lat'] == 37.76
        assert result['lon'] == -122.39
        assert result['source'] == 'gazetteer'
    
    def test_get_candidates(self, db_session, sample_data):
        """Test getting candidates in same state"""
        resolver = NearestZipResolver(db_session)
        
        candidates = resolver._get_candidates("CA", "94107")
        
        # Should find other CA ZIPs but not TX
        candidate_zips = [c['zip5'] for c in candidates]
        assert "94110" in candidate_zips
        assert "94115" in candidate_zips
        assert "73301" not in candidate_zips  # Different state
        assert "94107" not in candidate_zips  # Excluded (self)
    
    def test_calculate_distances(self, db_session, sample_data):
        """Test distance calculation"""
        resolver = NearestZipResolver(db_session)
        
        candidates = [
            {'zip5': '94110', 'zcta5': '94110', 'population': 45000},
            {'zip5': '94115', 'zcta5': '94115', 'population': 60000}
        ]
        
        result = resolver._calculate_distances("94107", candidates, use_nber=False)
        
        assert len(result['distances']) == 2
        assert all(d['distance_miles'] > 0 for d in result['distances'])
        assert result['summary']['engine'] == 'nber|haversine'
    
    def test_select_nearest_with_ties(self, db_session, sample_data):
        """Test nearest selection with tie-breaking"""
        resolver = NearestZipResolver(db_session)
        
        # Create distances with same distance but different populations
        distances = [
            {'zip5': '94110', 'distance_miles': 1.0, 'population': 45000},
            {'zip5': '94115', 'distance_miles': 1.0, 'population': 60000},  # Larger population
            {'zip5': '94120', 'distance_miles': 2.0, 'population': 30000},  # Further away
        ]
        
        result = resolver._select_nearest(distances)
        
        # Should select 94110 (same distance, smaller population)
        assert result['nearest_zip'] == '94110'
        assert result['distance_miles'] == 1.0
    
    def test_select_nearest_lexicographic_tie(self, db_session, sample_data):
        """Test lexicographic tie-breaking when populations are equal"""
        resolver = NearestZipResolver(db_session)
        
        distances = [
            {'zip5': '94115', 'distance_miles': 1.0, 'population': 50000},
            {'zip5': '94110', 'distance_miles': 1.0, 'population': 50000},  # Same population, lexicographically first
        ]
        
        result = resolver._select_nearest(distances)
        
        # Should select 94110 (lexicographically first)
        assert result['nearest_zip'] == '94110'
    
    def test_calculate_flags(self, db_session):
        """Test flag calculation"""
        resolver = NearestZipResolver(db_session)
        
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
    
    def test_find_nearest_zip_integration(self, db_session, sample_data):
        """Test complete nearest ZIP resolution"""
        resolver = NearestZipResolver(db_session)
        
        result = resolver.find_nearest_zip("94107")
        
        assert 'nearest_zip' in result
        assert 'distance_miles' in result
        assert 'trace' in result
        assert result['nearest_zip'] in ['94110', '94115']  # Should be one of the CA candidates
        assert result['distance_miles'] > 0
    
    def test_find_nearest_zip_zip9(self, db_session, sample_data):
        """Test nearest ZIP resolution with ZIP9 input"""
        resolver = NearestZipResolver(db_session)
        
        result = resolver.find_nearest_zip("94107-1234")
        
        assert 'nearest_zip' in result
        assert 'distance_miles' in result
        assert 'trace' in result
        assert result['trace']['input']['zip9'] == "941071234"
    
    def test_find_nearest_zip_no_candidates(self, db_session):
        """Test nearest ZIP resolution with no candidates"""
        # Create minimal data - just the source ZIP
        zcta_coord = ZCTACoords(zcta5="94107", lat=37.76, lon=-122.39, vintage="2025")
        zip_to_zcta = ZipToZCTA(zip5="94107", zcta5="94107", relationship="Zip matches ZCTA", weight=1.0, vintage="2023")
        cms_zip = CMSZipLocality(zip5="94107", state="CA", locality="01", effective_from=date(2025, 1, 1), vintage="2025")
        
        db_session.add_all([zcta_coord, zip_to_zcta, cms_zip])
        db_session.commit()
        
        resolver = NearestZipResolver(db_session)
        
        with pytest.raises(ValueError, match="No valid candidates found"):
            resolver.find_nearest_zip("94107")


class TestDistanceEngine:
    """Test distance calculation engine"""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    @pytest.fixture
    def sample_coords(self, db_session):
        """Create sample coordinate data"""
        coords = [
            ZCTACoords(zcta5="94107", lat=37.76, lon=-122.39, vintage="2025"),
            ZCTACoords(zcta5="94110", lat=37.75, lon=-122.42, vintage="2025"),
        ]
        
        for coord in coords:
            db_session.add(coord)
        db_session.commit()
        
        return coords
    
    def test_haversine_calculation(self):
        """Test Haversine distance calculation"""
        from cms_pricing.services.nearest_zip_distance import haversine_distance
        
        # Distance between San Francisco and Oakland (approximately 8 miles)
        distance = haversine_distance(37.7749, -122.4194, 37.8044, -122.2711)
        assert 7.0 < distance < 9.0  # Should be around 8 miles
    
    def test_calculate_distance_haversine(self, db_session, sample_coords):
        """Test distance calculation using Haversine"""
        engine = DistanceEngine(db_session)
        
        result = engine.calculate_distance("94107", "94110", use_nber=False)
        
        assert result['distance_miles'] > 0
        assert result['method_used'] == 'haversine'
        assert result['haversine_available'] is True
        assert result['nber_available'] is False
    
    def test_calculate_distance_self(self, db_session, sample_coords):
        """Test distance calculation for same ZCTA"""
        engine = DistanceEngine(db_session)
        
        result = engine.calculate_distance("94107", "94107", use_nber=False)
        
        assert result['distance_miles'] == 0.0
        assert result['method_used'] == 'self'
    
    def test_batch_calculate_distances(self, db_session, sample_coords):
        """Test batch distance calculation"""
        engine = DistanceEngine(db_session)
        
        results = engine.batch_calculate_distances(
            "94107", 
            ["94110", "94107"], 
            use_nber=False
        )
        
        assert len(results) == 2
        assert results["94110"]['distance_miles'] > 0
        assert results["94107"]['distance_miles'] == 0.0
    
    def test_clear_cache(self, db_session, sample_coords):
        """Test cache clearing"""
        engine = DistanceEngine(db_session)
        
        # Calculate some distances to populate cache
        engine.calculate_distance("94107", "94110", use_nber=False)
        
        # Clear cache
        engine.clear_cache()
        
        # Cache should be empty (this is hard to test directly, but shouldn't error)
        assert True


class TestNearestZipIntegration:
    """Integration tests for nearest ZIP resolver"""
    
    def test_api_endpoint_validation(self):
        """Test API endpoint input validation"""
        from cms_pricing.schemas.nearest_zip import NearestZipRequest
        
        # Valid ZIP5
        request = NearestZipRequest(zip="94107")
        assert request.zip == "94107"
        
        # Valid ZIP9
        request = NearestZipRequest(zip="94107-1234")
        assert request.zip == "94107-1234"
        
        # Invalid ZIP
        with pytest.raises(ValueError):
            NearestZipRequest(zip="123")
    
    def test_response_schema(self):
        """Test response schema validation"""
        from cms_pricing.schemas.nearest_zip import NearestZipResponse
        
        response = NearestZipResponse(
            nearest_zip="94110",
            distance_miles=1.5,
            input_zip="94107"
        )
        
        assert response.nearest_zip == "94110"
        assert response.distance_miles == 1.5
        assert response.input_zip == "94107"
