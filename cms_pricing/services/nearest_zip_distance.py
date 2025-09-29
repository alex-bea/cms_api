"""Distance calculation engine for nearest ZIP resolver"""

import math
from typing import Optional, Tuple, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.models.nearest_zip import ZCTACoords, ZCTADistances, NBERCentroids
import structlog

logger = structlog.get_logger()


class DistanceEngine:
    """Distance calculation engine with Haversine and NBER fast-path support"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self._coords_cache: Dict[str, Tuple[float, float]] = {}
        self._nber_cache: Dict[Tuple[str, str], float] = {}
    
    def calculate_distance(
        self,
        zcta_a: str,
        zcta_b: str,
        use_nber: bool = True
    ) -> Dict[str, Any]:
        """
        Calculate distance between two ZCTAs using NBER fast-path or Haversine
        
        Returns:
            Dict with distance_miles, method_used, and trace info
        """
        if zcta_a == zcta_b:
            return {
                'distance_miles': 0.0,
                'method_used': 'self',
                'nber_available': False,
                'haversine_available': True
            }
        
        # Try NBER fast-path first
        nber_distance = None
        if use_nber:
            nber_distance = self._get_nber_distance(zcta_a, zcta_b)
        
        # Calculate Haversine distance
        haversine_distance = self._calculate_haversine(zcta_a, zcta_b)
        
        # Check for discrepancy between NBER and Haversine
        discrepancy_detected = False
        if nber_distance is not None and haversine_distance is not None:
            discrepancy = abs(nber_distance - haversine_distance)
            if discrepancy > 1.0:
                discrepancy_detected = True
                logger.warning(f"NBER-Haversine discrepancy detected for {zcta_a}-{zcta_b}: {discrepancy:.3f} mi (NBER: {nber_distance:.3f}, Haversine: {haversine_distance:.3f})")
        
        # Choose final distance (prefer NBER if available, but use Haversine if discrepancy > 1.0 mi)
        if nber_distance is not None and not discrepancy_detected:
            final_distance = nber_distance
            method_used = 'nber'
        else:
            final_distance = haversine_distance
            method_used = 'haversine'
        
        return {
            'distance_miles': final_distance,
            'method_used': method_used,
            'nber_available': nber_distance is not None,
            'haversine_available': haversine_distance is not None,
            'nber_distance': nber_distance,
            'haversine_distance': haversine_distance,
            'discrepancy_detected': discrepancy_detected,
            'discrepancy_miles': abs(nber_distance - haversine_distance) if nber_distance is not None and haversine_distance is not None else None
        }
    
    def _get_nber_distance(self, zcta_a: str, zcta_b: str) -> Optional[float]:
        """Get distance from NBER database (fast-path)"""
        # Check cache first
        cache_key = (zcta_a, zcta_b) if zcta_a < zcta_b else (zcta_b, zcta_a)
        if cache_key in self._nber_cache:
            return self._nber_cache[cache_key]
        
        # Query database (order-insensitive)
        distance_record = self.db.query(ZCTADistances).filter(
            or_(
                and_(ZCTADistances.zcta5_a == zcta_a, ZCTADistances.zcta5_b == zcta_b),
                and_(ZCTADistances.zcta5_a == zcta_b, ZCTADistances.zcta5_b == zcta_a)
            )
        ).first()
        
        if distance_record:
            distance = distance_record.miles
            self._nber_cache[cache_key] = distance
            return distance
        
        return None
    
    def _calculate_haversine(self, zcta_a: str, zcta_b: str) -> Optional[float]:
        """Calculate Haversine distance between two ZCTAs"""
        # Get coordinates for both ZCTAs
        coords_a = self._get_zcta_coords(zcta_a)
        coords_b = self._get_zcta_coords(zcta_b)
        
        if not coords_a or not coords_b:
            return None
        
        lat1, lon1 = coords_a
        lat2, lon2 = coords_b
        
        return self._haversine_formula(lat1, lon1, lat2, lon2)
    
    def _get_zcta_coords(self, zcta: str) -> Optional[Tuple[float, float]]:
        """Get coordinates for a ZCTA from database or cache with NBER fallback"""
        if zcta in self._coords_cache:
            return self._coords_cache[zcta]
        
        # Try Gazetteer first
        coords_record = self.db.query(ZCTACoords).filter(
            ZCTACoords.zcta5 == zcta
        ).first()
        
        if coords_record:
            coords = (coords_record.lat, coords_record.lon)
            self._coords_cache[zcta] = coords
            return coords
        
        # Fallback to NBER centroids
        nber_record = self.db.query(NBERCentroids).filter(
            NBERCentroids.zcta5 == zcta
        ).first()
        
        if nber_record:
            logger.warning(f"Using NBER fallback for ZCTA {zcta} in distance calculation - Gazetteer data missing")
            coords = (nber_record.lat, nber_record.lon)
            self._coords_cache[zcta] = coords
            return coords
        
        return None
    
    def _haversine_formula(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate Haversine distance between two points on Earth
        
        Args:
            lat1, lon1: Latitude and longitude of first point in degrees
            lat2, lon2: Latitude and longitude of second point in degrees
        
        Returns:
            Distance in miles
        """
        # Earth's radius in miles
        R = 3959.0
        
        # Convert degrees to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def batch_calculate_distances(
        self,
        source_zcta: str,
        target_zctas: list[str],
        use_nber: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Calculate distances from one ZCTA to multiple target ZCTAs
        
        Args:
            source_zcta: Source ZCTA
            target_zctas: List of target ZCTAs
            use_nber: Whether to use NBER fast-path
        
        Returns:
            Dict mapping target ZCTA to distance info
        """
        results = {}
        
        for target_zcta in target_zctas:
            results[target_zcta] = self.calculate_distance(
                source_zcta, target_zcta, use_nber
            )
        
        return results
    
    def clear_cache(self):
        """Clear internal caches"""
        self._coords_cache.clear()
        self._nber_cache.clear()
        logger.info("Distance engine caches cleared")


# Utility functions for distance calculations
def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Standalone Haversine distance calculation"""
    engine = DistanceEngine(None)  # No DB needed for standalone calculation
    return engine._haversine_formula(lat1, lon1, lat2, lon2)


def validate_distance_calculation(
    zcta_a: str,
    zcta_b: str,
    expected_distance: float,
    tolerance: float = 0.1
) -> bool:
    """
    Validate distance calculation against expected value
    
    Args:
        zcta_a: First ZCTA
        zcta_b: Second ZCTA
        expected_distance: Expected distance in miles
        tolerance: Acceptable tolerance in miles
    
    Returns:
        True if calculated distance is within tolerance
    """
    # This would be used in tests to validate distance calculations
    # Implementation would depend on having coordinates available
    pass
