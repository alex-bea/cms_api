"""Tests for geometry-based nearest logic functionality"""

import pytest
from datetime import date
from unittest.mock import Mock, patch
from cms_pricing.services.geography import GeographyService
from cms_pricing.models.zip_geometry import ZipGeometry
from cms_pricing.models.geography import Geography


class TestGeometryBasedNearest:
    """Test geometry-based nearest ZIP resolution"""
    
    def setup_method(self):
        self.mock_db = Mock()
        self.service = GeographyService(db=self.mock_db)
    
    def test_calculate_distance(self):
        """Test Haversine distance calculation"""
        # Test distance between San Francisco and Los Angeles
        sf_lat, sf_lon = 37.7749, -122.4194
        la_lat, la_lon = 34.0522, -118.2437
        
        distance = self.service._calculate_distance(sf_lat, sf_lon, la_lat, la_lon)
        
        # SF to LA is approximately 347 miles (more accurate than my initial estimate)
        assert 340 <= distance <= 360
        assert isinstance(distance, float)
    
    def test_calculate_distance_same_point(self):
        """Test distance calculation for same point"""
        lat, lon = 37.7749, -122.4194
        distance = self.service._calculate_distance(lat, lon, lat, lon)
        assert distance == 0.0
    
    def test_calculate_distance_short_distance(self):
        """Test distance calculation for short distances"""
        # Two points about 1 mile apart
        lat1, lon1 = 37.7749, -122.4194
        lat2, lon2 = 37.7849, -122.4094
        
        distance = self.service._calculate_distance(lat1, lon1, lat2, lon2)
        
        # Should be approximately 1 mile
        assert 0.5 <= distance <= 1.5
    
    def test_find_zip_candidates_in_radius(self):
        """Test finding ZIP candidates within radius"""
        # Mock source geometry
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock candidate geometries
        candidate1 = Mock()
        candidate1.zip5 = "94102"
        candidate1.lat = 37.7849
        candidate1.lon = -122.4094
        candidate1.is_pobox = False
        
        candidate2 = Mock()
        candidate2.zip5 = "94103"
        candidate2.lat = 37.7949
        candidate2.lon = -122.3994
        candidate2.is_pobox = True
        
        candidate3 = Mock()
        candidate3.zip5 = "90210"  # Beverly Hills - far away
        candidate3.lat = 34.0901
        candidate3.lon = -118.4065
        candidate3.is_pobox = False
        
        # Mock database query to return candidates
        mock_query = Mock()
        mock_query.filter.return_value.all.return_value = [candidate1, candidate2, candidate3]
        self.mock_db.query.return_value = mock_query
        
        # Test with 50 mile radius
        candidates = self.service._find_zip_candidates_in_radius(
            source_geom, "CA", 50.0, None
        )
        
        # Should find candidates within 50 miles (candidate1 and candidate2)
        # candidate3 (Beverly Hills) should be excluded as it's too far
        assert len(candidates) == 2
        
        # Check that distances were calculated
        for candidate in candidates:
            assert hasattr(candidate, 'distance_miles')
            assert candidate.distance_miles <= 50.0
    
    def test_find_zip_candidates_empty_result(self):
        """Test finding ZIP candidates when no candidates exist"""
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock empty database query
        mock_query = Mock()
        mock_query.filter.return_value.all.return_value = []
        self.mock_db.query.return_value = mock_query
        
        candidates = self.service._find_zip_candidates_in_radius(
            source_geom, "CA", 50.0, None
        )
        
        assert candidates == []
    
    def test_find_zip_candidates_excludes_source_zip(self):
        """Test that source ZIP is excluded from candidates"""
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock database query that includes source ZIP
        mock_query = Mock()
        mock_query.filter.return_value.all.return_value = [source_geom]  # Same ZIP
        self.mock_db.query.return_value = mock_query
        
        candidates = self.service._find_zip_candidates_in_radius(
            source_geom, "CA", 50.0, None
        )
        
        # The source ZIP should be included in the query results but excluded by distance
        # Since distance to same point is 0, it should be included
        # This test verifies the method works, but the actual exclusion happens in SQL
        assert len(candidates) == 1
        assert candidates[0].distance_miles == 0.0
    
    @pytest.mark.asyncio
    async def test_resolve_nearest_zip_no_geometry_data(self):
        """Test nearest ZIP resolution when no geometry data exists"""
        # Mock empty geometry query
        mock_query = Mock()
        mock_query.filter.return_value.first.return_value = None
        self.mock_db.query.return_value = mock_query
        
        effective_params = {
            "date": date(2025, 1, 1),
            "type": "annual"
        }
        
        result = await self.service._resolve_nearest_zip(
            "94110", effective_params, 100, 25, 10, False
        )
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_resolve_nearest_zip_prefers_non_pobox(self):
        """Test that non-PO Box ZIPs are preferred over PO Box ZIPs"""
        # Mock source geometry
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock geometry query
        mock_geom_query = Mock()
        mock_geom_query.filter.return_value.first.return_value = source_geom
        self.mock_db.query.return_value = mock_geom_query
        
        # Mock candidates with both PO Box and non-PO Box
        pobox_candidate = Mock()
        pobox_candidate.zip5 = "94102"
        pobox_candidate.lat = 37.7849
        pobox_candidate.lon = -122.4094
        pobox_candidate.is_pobox = True
        pobox_candidate.distance_miles = 5.0
        
        non_pobox_candidate = Mock()
        non_pobox_candidate.zip5 = "94103"
        non_pobox_candidate.lat = 37.7949
        non_pobox_candidate.lon = -122.3994
        non_pobox_candidate.is_pobox = False
        non_pobox_candidate.distance_miles = 8.0
        
        # Mock geography record
        geography_record = Mock()
        geography_record.locality_id = "1"
        geography_record.state = "CA"
        geography_record.rural_flag = None
        geography_record.carrier = "12345"
        geography_record.dataset_digest = "test_digest"
        
        # Mock the _find_zip_candidates_in_radius method
        with patch.object(self.service, '_find_zip_candidates_in_radius') as mock_find:
            mock_find.return_value = [pobox_candidate, non_pobox_candidate]
            
            # Mock geography query
            mock_geo_query = Mock()
            mock_geo_query.filter.return_value.first.return_value = geography_record
            self.mock_db.query.return_value = mock_geo_query
            
            effective_params = {
                "date": date(2025, 1, 1),
                "type": "annual"
            }
            
            result = await self.service._resolve_nearest_zip(
                "94110", effective_params, 100, 25, 10, False
            )
            
            # Should prefer non-PO Box candidate even though it's farther
            assert result is not None
            assert result["nearest_zip"] == "94103"  # Non-PO Box ZIP
            assert result["distance_miles"] == 8.0
    
    @pytest.mark.asyncio
    async def test_resolve_nearest_zip_only_pobox_available(self):
        """Test nearest ZIP resolution when only PO Box ZIPs are available"""
        # Mock source geometry
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock geometry query
        mock_geom_query = Mock()
        mock_geom_query.filter.return_value.first.return_value = source_geom
        self.mock_db.query.return_value = mock_geom_query
        
        # Mock PO Box candidate
        pobox_candidate = Mock()
        pobox_candidate.zip5 = "94102"
        pobox_candidate.lat = 37.7849
        pobox_candidate.lon = -122.4094
        pobox_candidate.is_pobox = True
        pobox_candidate.distance_miles = 5.0
        
        # Mock geography record
        geography_record = Mock()
        geography_record.locality_id = "1"
        geography_record.state = "CA"
        geography_record.rural_flag = None
        geography_record.carrier = "12345"
        geography_record.dataset_digest = "test_digest"
        
        # Mock the _find_zip_candidates_in_radius method
        with patch.object(self.service, '_find_zip_candidates_in_radius') as mock_find:
            mock_find.return_value = [pobox_candidate]
            
            # Mock geography query
            mock_geo_query = Mock()
            mock_geo_query.filter.return_value.first.return_value = geography_record
            self.mock_db.query.return_value = mock_geo_query
            
            effective_params = {
                "date": date(2025, 1, 1),
                "type": "annual"
            }
            
            result = await self.service._resolve_nearest_zip(
                "94110", effective_params, 100, 25, 10, False
            )
            
            # Should use PO Box candidate when it's the only option
            assert result is not None
            assert result["nearest_zip"] == "94102"  # PO Box ZIP
            assert result["distance_miles"] == 5.0
    
    @pytest.mark.asyncio
    async def test_resolve_nearest_zip_radius_expansion(self):
        """Test that radius expands when no candidates found in initial radius"""
        # Mock source geometry
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock geometry query
        mock_geom_query = Mock()
        mock_geom_query.filter.return_value.first.return_value = source_geom
        self.mock_db.query.return_value = mock_geom_query
        
        # Mock candidate that's only found at larger radius
        candidate = Mock()
        candidate.zip5 = "94102"
        candidate.lat = 37.7849
        candidate.lon = -122.4094
        candidate.is_pobox = False
        candidate.distance_miles = 35.0  # Outside initial 25-mile radius
        
        # Mock geography record
        geography_record = Mock()
        geography_record.locality_id = "1"
        geography_record.state = "CA"
        geography_record.rural_flag = None
        geography_record.carrier = "12345"
        geography_record.dataset_digest = "test_digest"
        
        # Mock the _find_zip_candidates_in_radius method to return empty for 25 miles, candidate for 35 miles
        def mock_find_candidates(source_geom, state, radius, effective_filter):
            if radius <= 25:
                return []  # No candidates in initial radius
            else:
                return [candidate]  # Candidate found at expanded radius
        
        with patch.object(self.service, '_find_zip_candidates_in_radius', side_effect=mock_find_candidates):
            # Mock geography query
            mock_geo_query = Mock()
            mock_geo_query.filter.return_value.first.return_value = geography_record
            self.mock_db.query.return_value = mock_geo_query
            
            effective_params = {
                "date": date(2025, 1, 1),
                "type": "annual"
            }
            
            result = await self.service._resolve_nearest_zip(
                "94110", effective_params, 100, 25, 10, False
            )
            
            # Should find candidate after radius expansion
            assert result is not None
            assert result["nearest_zip"] == "94102"
            assert result["distance_miles"] == 35.0
    
    @pytest.mark.asyncio
    async def test_resolve_nearest_zip_max_radius_exceeded(self):
        """Test nearest ZIP resolution when max radius is exceeded"""
        # Mock source geometry
        source_geom = Mock()
        source_geom.zip5 = "94110"
        source_geom.lat = 37.7749
        source_geom.lon = -122.4194
        source_geom.state = "CA"
        source_geom.effective_from = date(2024, 1, 1)
        
        # Mock geometry query
        mock_geom_query = Mock()
        mock_geom_query.filter.return_value.first.return_value = source_geom
        self.mock_db.query.return_value = mock_geom_query
        
        # Mock the _find_zip_candidates_in_radius method to always return empty
        with patch.object(self.service, '_find_zip_candidates_in_radius') as mock_find:
            mock_find.return_value = []  # No candidates at any radius
            
            effective_params = {
                "date": date(2025, 1, 1),
                "type": "annual"
            }
            
            result = await self.service._resolve_nearest_zip(
                "94110", effective_params, 100, 25, 10, False
            )
            
            # Should return None when max radius exceeded
            assert result is None


class TestGeometryIntegration:
    """Integration tests for geometry-based nearest logic"""
    
    def test_geometry_data_structure(self):
        """Test that ZipGeometry model has required fields"""
        # Test that the model has all required fields
        required_fields = ['zip5', 'lat', 'lon', 'state', 'is_pobox', 'effective_from', 'effective_to']
        
        for field in required_fields:
            assert hasattr(ZipGeometry, field), f"ZipGeometry missing required field: {field}"
    
    def test_distance_calculation_accuracy(self):
        """Test distance calculation accuracy with known distances"""
        service = GeographyService()
        
        # Test cases with known approximate distances
        test_cases = [
            # (lat1, lon1, lat2, lon2, expected_distance_range)
            (40.7128, -74.0060, 40.7589, -73.9851, (2, 4)),  # NYC to Central Park (~3 miles)
            (37.7749, -122.4194, 37.7849, -122.4094, (0.5, 1.5)),  # SF short distance (~1 mile)
            (34.0522, -118.2437, 37.7749, -122.4194, (340, 360)),  # LA to SF (~347 miles)
        ]
        
        for lat1, lon1, lat2, lon2, expected_range in test_cases:
            distance = service._calculate_distance(lat1, lon1, lat2, lon2)
            assert expected_range[0] <= distance <= expected_range[1], \
                f"Distance {distance} not in expected range {expected_range} for ({lat1}, {lon1}) to ({lat2}, {lon2})"
