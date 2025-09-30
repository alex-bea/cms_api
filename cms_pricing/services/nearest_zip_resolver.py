"""Nearest ZIP resolver implementation per PRD v1.0"""

import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from cms_pricing.models.nearest_zip import (
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZIP9Overrides,
    ZipMetadata, NearestZipTrace, NBERCentroids
)
from cms_pricing.services.nearest_zip_distance import DistanceEngine
import structlog

logger = structlog.get_logger()


class NearestZipResolver:
    """Nearest ZIP resolver with same-state constraint per PRD"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.distance_engine = DistanceEngine(db_session)
    
    def find_nearest_zip(
        self, 
        input_zip: str, 
        use_nber: bool = True,
        max_radius_miles: float = 100.0,
        include_trace: bool = False
    ) -> Dict[str, Any]:
        """
        Find nearest non-PO Box ZIP5 in same CMS state
        
        Args:
            input_zip: ZIP5 or ZIP9 (e.g., "94107" or "94107-1234")
            use_nber: Whether to use NBER fast-path for distances
            max_radius_miles: Maximum search radius in miles
            include_trace: Whether to include detailed trace information
        
        Returns:
            Dict with nearest_zip, distance_miles, and optional trace
        """
        trace = {
            'input': {'zip': input_zip},
            'normalization': {},
            'starting_centroid': {},
            'candidates': {},
            'dist_calc': {},
            'result': {},
            'flags': {}
        }
        
        try:
            # Step 1: Parse & Normalize input
            zip5, zip9 = self._parse_input(input_zip)
            trace['input']['zip5'] = zip5
            trace['input']['zip9'] = zip9
            
            # Step 2: Get state and locality
            state_info = self._get_state_and_locality(zip5, zip9)
            trace['normalization'].update(state_info)
            
            # Step 3: Get starting ZCTA and centroid
            zcta_info = self._get_zcta_info(zip5)
            trace['normalization'].update(zcta_info)
            
            starting_coords = self._get_starting_centroid(zcta_info['starting_zcta'])
            trace['starting_centroid'] = starting_coords
            
            # Step 4: Get candidate ZIPs in same state
            candidates = self._get_candidates(state_info['state'], zip5)
            trace['candidates'] = {
                'state_zip_count': len(candidates),
                'excluded_pobox': 0  # Will be updated
            }
            
            # Step 5: Calculate distances
            distance_results = self._calculate_distances(
                zcta_info['starting_zcta'], 
                candidates, 
                use_nber
            )
            trace['dist_calc'] = distance_results['summary']
            
            # Step 6: Select nearest with tie-breaking
            result = self._select_nearest(distance_results['distances'])
            trace['result'] = result
            trace['flags'] = self._calculate_flags(result['distance_miles'])
            
            # Step 7: Check for asymmetry (optional, can be expensive)
            if include_trace:
                asymmetry = self._check_asymmetry(input_zip, result['nearest_zip'])
                trace['asymmetry'] = asymmetry
                
                if asymmetry.get('asymmetry_detected'):
                    logger.warning(
                        f"Asymmetry detected: {input_zip} -> {result['nearest_zip']}, "
                        f"but {result['nearest_zip']} -> {asymmetry['reverse_nearest']}"
                    )
            
            # Store trace in database
            self._store_trace(input_zip, zip5, zip9, result, trace)
            
            response = {
                'nearest_zip': result['nearest_zip'],
                'distance_miles': result['distance_miles']
            }
            
            if include_trace:
                response['trace'] = trace
                
            return response
            
        except Exception as e:
            logger.error(f"Nearest ZIP resolution failed for {input_zip}: {e}")
            trace['error'] = str(e)
            raise
    
    def _parse_input(self, input_zip: str) -> Tuple[str, Optional[str]]:
        """Parse input ZIP5 or ZIP9"""
        # Strip non-digits
        digits = re.sub(r'[^\d]', '', input_zip)
        
        if len(digits) == 5:
            return digits, None
        elif len(digits) == 9:
            return digits[:5], digits
        else:
            raise ValueError(f"Invalid ZIP format: {input_zip}")
    
    def _get_state_and_locality(self, zip5: str, zip9: Optional[str]) -> Dict[str, Any]:
        """Get state and locality for ZIP, checking ZIP9 overrides first"""
        result = {
            'state': None,
            'locality': None,
            'zip9_hit': False
        }
        
        # Check ZIP9 overrides first
        if zip9:
            zip9_record = self.db.query(ZIP9Overrides).filter(
                and_(
                    ZIP9Overrides.zip9_low <= zip9,
                    ZIP9Overrides.zip9_high >= zip9
                )
            ).first()
            
            if zip9_record:
                result.update({
                    'state': zip9_record.state,
                    'locality': zip9_record.locality,
                    'zip9_hit': True
                })
                return result
        
        # Fall back to ZIP5 lookup
        zip5_record = self.db.query(CMSZipLocality).filter(
            CMSZipLocality.zip5 == zip5
        ).first()
        
        if zip5_record:
            result.update({
                'state': zip5_record.state,
                'locality': zip5_record.locality,
                'zip9_hit': False
            })
        else:
            raise ValueError(f"No state/locality found for ZIP {zip5}")
        
        return result
    
    def _get_zcta_info(self, zip5: str) -> Dict[str, Any]:
        """Get ZCTA mapping for ZIP5 with weight preference"""
        # Prefer exact ZIP=ZCTA matches, otherwise highest weight
        zip_to_zcta = self.db.query(ZipToZCTA).filter(
            ZipToZCTA.zip5 == zip5
        ).order_by(
            # Prefer exact matches first, then by weight descending
            (ZipToZCTA.relationship == 'Zip matches ZCTA').desc(),
            ZipToZCTA.weight.desc().nullslast()
        ).first()
        
        if not zip_to_zcta:
            raise ValueError(f"No ZCTA mapping found for ZIP {zip5}")
        
        return {
            'starting_zcta': zip_to_zcta.zcta5,
            'zcta_weight': float(zip_to_zcta.weight) if zip_to_zcta.weight else None,
            'relationship': zip_to_zcta.relationship
        }
    
    def _get_starting_centroid(self, zcta: str) -> Dict[str, Any]:
        """Get starting centroid coordinates with NBER fallback"""
        # Try Gazetteer first
        coords_record = self.db.query(ZCTACoords).filter(
            ZCTACoords.zcta5 == zcta
        ).first()
        
        if coords_record:
            return {
                'lat': coords_record.lat,
                'lon': coords_record.lon,
                'source': 'gazetteer'
            }
        
        # Fallback to NBER centroids
        nber_record = self.db.query(NBERCentroids).filter(
            NBERCentroids.zcta5 == zcta
        ).first()
        
        if nber_record:
            logger.warning(f"Using NBER fallback for ZCTA {zcta} - Gazetteer data missing")
            return {
                'lat': nber_record.lat,
                'lon': nber_record.lon,
                'source': 'nber_fallback'
            }
        
        raise ValueError(f"No coordinates found for ZCTA {zcta} in Gazetteer or NBER data")
    
    def _get_candidates(self, state: str, exclude_zip: str) -> List[Dict[str, Any]]:
        """Get candidate ZIPs in same state, excluding PO Boxes"""
        # Get all ZIPs that have ZCTA mappings and are in the same state
        # We need to join ZipToZCTA with CMSZipLocality to get state info
        candidates_query = self.db.query(
            ZipToZCTA.zip5,
            ZipToZCTA.zcta5,
            CMSZipLocality.locality,
            ZipMetadata.population,
            ZipMetadata.is_pobox
        ).join(
            CMSZipLocality, ZipToZCTA.zip5 == CMSZipLocality.zip5
        ).outerjoin(
            ZipMetadata, ZipToZCTA.zip5 == ZipMetadata.zip5
        ).filter(
            and_(
                CMSZipLocality.state == state,
                ZipToZCTA.zip5 != exclude_zip
            )
        ).order_by(
            (ZipToZCTA.relationship == 'Zip matches ZCTA').desc(),
            ZipToZCTA.weight.desc().nullslast()
        )
        
        candidates = []
        excluded_pobox = 0
        
        for row in candidates_query.all():
            # Skip PO Boxes
            if row.is_pobox:
                excluded_pobox += 1
                continue
            
            candidates.append({
                'zip5': row.zip5,
                'zcta5': row.zcta5,
                'locality': row.locality,
                'population': row.population
            })
        
        # Check for empty candidates set
        if not candidates:
            raise ValueError("NO_CANDIDATES_IN_STATE")
        
        return candidates
    
    def _calculate_distances(
        self, 
        source_zcta: str, 
        candidates: List[Dict[str, Any]], 
        use_nber: bool
    ) -> Dict[str, Any]:
        """Calculate distances to all candidates"""
        distances = []
        nber_hits = 0
        fallbacks = 0
        discrepancies = 0
        
        for candidate in candidates:
            distance_info = self.distance_engine.calculate_distance(
                source_zcta, 
                candidate['zcta5'], 
                use_nber
            )
            
            if distance_info['nber_available']:
                nber_hits += 1
            else:
                fallbacks += 1
            
            if distance_info.get('discrepancy_detected', False):
                discrepancies += 1
            
            distances.append({
                'zip5': candidate['zip5'],
                'zcta5': candidate['zcta5'],
                'distance_miles': distance_info['distance_miles'],
                'method_used': distance_info['method_used'],
                'population': candidate['population'],
                'discrepancy_detected': distance_info.get('discrepancy_detected', False),
                'discrepancy_miles': distance_info.get('discrepancy_miles')
            })
        
        return {
            'distances': distances,
            'summary': {
                'engine': 'nber|haversine',
                'nber_hits': nber_hits,
                'fallbacks': fallbacks,
                'discrepancies': discrepancies
            }
        }
    
    def _select_nearest(self, distances: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select nearest ZIP with tie-breaking logic"""
        if not distances:
            raise ValueError("No candidates found")
        
        # Filter out self-distance (0.0 miles)
        valid_distances = [d for d in distances if d['distance_miles'] > 0.0]
        
        if not valid_distances:
            raise ValueError("No valid candidates found")
        
        # Sort by distance, then by population (smaller first), then by ZIP (lexicographic)
        # Use COALESCE(population, 0) as specified in PRD
        valid_distances.sort(key=lambda x: (
            x['distance_miles'],
            x['population'] if x['population'] is not None else 0,  # COALESCE(population, 0)
            x['zip5']
        ))
        
        nearest = valid_distances[0]
        
        return {
            'nearest_zip': nearest['zip5'],
            'distance_miles': nearest['distance_miles'],
            'method_used': nearest['method_used'],
            'zcta5': nearest['zcta5']
        }
    
    def _calculate_flags(self, distance_miles: float) -> Dict[str, bool]:
        """Calculate threshold flags"""
        return {
            'coincident': distance_miles < 1.0,
            'far_neighbor': distance_miles > 10.0
        }
    
    def _check_asymmetry(self, input_zip: str, result_zip: str) -> Dict[str, Any]:
        """
        Check for asymmetry in nearest ZIP relationships
        
        Args:
            input_zip: The input ZIP code
            result_zip: The nearest ZIP found for input_zip
            
        Returns:
            Dict with asymmetry detection results
        """
        try:
            # Find the nearest ZIP for the result_zip
            reverse_result = self.find_nearest_zip(result_zip, include_trace=False)
            
            # Check if the reverse lookup returns the original input_zip
            is_reciprocal = reverse_result['nearest_zip'] == input_zip
            
            return {
                'is_reciprocal': is_reciprocal,
                'reverse_nearest': reverse_result['nearest_zip'],
                'reverse_distance': reverse_result['distance_miles'],
                'asymmetry_detected': not is_reciprocal
            }
            
        except Exception as e:
            logger.warning(f"Failed to check asymmetry for {input_zip} -> {result_zip}: {e}")
            return {
                'is_reciprocal': None,
                'reverse_nearest': None,
                'reverse_distance': None,
                'asymmetry_detected': None,
                'error': str(e)
            }
    
    def _store_trace(
        self, 
        input_zip: str, 
        zip5: str, 
        zip9: Optional[str], 
        result: Dict[str, Any], 
        trace: Dict[str, Any]
    ) -> None:
        """Store trace in database for observability"""
        trace_record = NearestZipTrace(
            input_zip=input_zip,
            input_zip5=zip5,
            input_zip9=zip9,
            result_zip=result['nearest_zip'],
            distance_miles=result['distance_miles'],
            trace_json=str(trace),  # Would use proper JSON serialization
            created_at=datetime.now()
        )
        
        self.db.add(trace_record)
        self.db.commit()


# Utility functions
def normalize_zip_input(zip_input: str) -> Tuple[str, Optional[str]]:
    """Utility function to normalize ZIP input"""
    resolver = NearestZipResolver(None)  # No DB needed for parsing
    return resolver._parse_input(zip_input)


def validate_zip_format(zip_input: str) -> bool:
    """Validate ZIP input format"""
    try:
        normalize_zip_input(zip_input)
        return True
    except ValueError:
        return False
