"""Geography resolution service"""

from typing import List, Optional, Dict, Any
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import math
import time

from cms_pricing.database import SessionLocal
from cms_pricing.models.geography import Geography
from cms_pricing.models.zip_geometry import ZipGeometry
from cms_pricing.schemas.geography import (
    GeographyResolveResponse, GeographyCandidate
)
from cms_pricing.services.effective_dates import EffectiveDateSelector, EffectiveDateRecord
from cms_pricing.services.geography_trace import GeographyTraceService
from cms_pricing.config import settings
import structlog

logger = structlog.get_logger()


class GeographyService:
    """Service for resolving ZIP codes to localities and CBSAs"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
        self.effective_date_selector = EffectiveDateSelector()
        self.trace_service = GeographyTraceService(self.db)
    
    async def resolve_zip(
        self, 
        zip5: str, 
        plus4: Optional[str] = None,
        valuation_year: Optional[int] = None,
        quarter: Optional[int] = None,
        valuation_date: Optional[date] = None,
        strict: bool = False,
        max_radius_miles: int = 100,
        initial_radius_miles: int = 25,
        expand_step_miles: int = 10,
        expose_carrier: bool = False
    ) -> Dict[str, Any]:
        """
        Resolve ZIP+4 to locality using ZIP+4-first hierarchy per PRD section 9.
        
        Args:
            zip5: 5-digit ZIP code
            plus4: Optional 4-digit ZIP+4 add-on
            valuation_year: Year for effective date selection
            quarter: Optional quarter for effective date selection
            strict: If True, error on non-ZIP+4 matches instead of falling back
            max_radius_miles: Maximum radius for nearest ZIP fallback
            initial_radius_miles: Initial radius for nearest ZIP search
            expand_step_miles: Step size for radius expansion
            expose_carrier: Whether to include carrier/MAC in response
            
        Returns:
            Dict with locality_id, match_level, and other resolution details
        """
        
        # Start timing for trace
        start_time = time.time()
        
        # Prepare input parameters for tracing
        trace_inputs = {
            "zip5": zip5,
            "plus4": plus4,
            "valuation_year": valuation_year,
            "quarter": quarter,
            "valuation_date": valuation_date.isoformat() if valuation_date else None,
            "strict": strict
        }
        
        try:
            # Normalize input per PRD section 18
            zip5, plus4 = self._normalize_zip_input(zip5, plus4)
            
            # Determine effective date parameters
            effective_params = self.effective_date_selector.determine_effective_date(
                valuation_year, quarter, valuation_date
            )
            
            logger.info(
                "Starting geography resolution",
                zip5=zip5,
                plus4=plus4,
                effective_year=effective_params["year"],
                effective_quarter=effective_params["quarter"],
                effective_date=effective_params["date"],
                effective_type=effective_params["type"],
                strict=strict
            )
            
            # Step 1: ZIP+4 exact match (if plus4 provided)
            if plus4:
                result = await self._resolve_zip_plus4_exact(
                    zip5, plus4, effective_params, expose_carrier
                )
                if result:
                    logger.info("ZIP+4 exact match found", zip5=zip5, plus4=plus4, locality_id=result["locality_id"])
                    # Create trace for successful resolution
                    self.trace_service.create_trace(trace_inputs, result, start_time=start_time)
                    return result
            
                # Strict mode: If ZIP+4 was provided but not found, error immediately
                if strict:
                    error_msg = (
                        f"We require a ZIP+4 for precise locality pricing in this area, but couldn't find one for **{zip5}-{plus4}**. "
                        f"Because strict mode is on, we won't fall back to ZIP5 or nearby ZIPs. "
                        f"You can retry with `strict=false` to let us use the closest match."
                    )
                    # Create trace for error
                    self.trace_service.create_trace(trace_inputs, error=ValueError(error_msg), start_time=start_time)
                    raise ValueError(error_msg)
        
            # Step 2: ZIP5 exact match
            result = await self._resolve_zip5_exact(
                zip5, effective_params, expose_carrier
            )
            if result:
                logger.info("ZIP5 exact match found", zip5=zip5, locality_id=result["locality_id"])
                # Create trace for successful resolution
                self.trace_service.create_trace(trace_inputs, result, start_time=start_time)
                return result
        
            # Strict mode: Only allow exact matches (ZIP+4 or ZIP5)
            if strict:
                error_msg = (
                    f"We require a ZIP+4 for precise locality pricing in this area, but couldn't find one for **{zip5}**. "
                    f"We looked for a 9-digit ZIP (e.g., **{zip5}-1234**) and then a 5-digit match. "
                    f"Because strict mode is on, we won't fall back to nearby ZIPs. "
                    f"You can provide a ZIP+4, or retry with `strict=false` to let us use the closest in-state ZIP."
                )
                # Create trace for error
                self.trace_service.create_trace(trace_inputs, error=ValueError(error_msg), start_time=start_time)
                raise ValueError(error_msg)
        
            # Step 3: Nearest ZIP within same state (non-strict mode only)
            result = await self._resolve_nearest_zip(
                zip5, effective_params, max_radius_miles, 
                initial_radius_miles, expand_step_miles, expose_carrier
            )
            if result:
                logger.info(
                    "Nearest ZIP match found", 
                    zip5=zip5, 
                    nearest_zip=result["nearest_zip"],
                    distance_miles=result["distance_miles"],
                    locality_id=result["locality_id"]
                )
                # Create trace for successful resolution
                self.trace_service.create_trace(trace_inputs, result, start_time=start_time)
                return result
        
            # Step 4: Default/Benchmark locality (should rarely reach here)
            
            # Use benchmark locality (locality 01)
            result = {
                "locality_id": "01",
                "state": None,
                "rural_flag": None,
                "carrier": None if not expose_carrier else "BENCHMARK",
                "match_level": "default",
                "dataset_digest": "benchmark",
                "distance_miles": None,
                "nearest_zip": None
            }
            
            logger.info("Using benchmark locality", zip5=zip5, locality_id="01")
            # Create trace for successful resolution
            self.trace_service.create_trace(trace_inputs, result, start_time=start_time)
            return result
        
        except Exception as e:
            # Create trace for any unexpected error
            self.trace_service.create_trace(trace_inputs, error=e, start_time=start_time)
            raise
    
    def _normalize_zip_input(self, zip5: str, plus4: Optional[str] = None) -> tuple[str, Optional[str]]:
        """Normalize ZIP+4 input per PRD section 18 (ZIP+4 normalization)"""
        
        # Handle combined ZIP+4 format (94110-1234 or 941101234)
        if '-' in zip5:
            # Format: 94110-1234
            parts = zip5.split('-')
            if len(parts) == 2:
                zip5_clean = ''.join(filter(str.isdigit, parts[0]))
                plus4_clean = ''.join(filter(str.isdigit, parts[1]))
                if len(zip5_clean) == 5 and len(plus4_clean) == 4:
                    return zip5_clean, plus4_clean.zfill(4)
        elif len(zip5) == 9 and zip5.isdigit():
            # Format: 941101234 (9 digits)
            zip5_clean = zip5[:5]
            plus4_clean = zip5[5:].zfill(4)
            return zip5_clean, plus4_clean
        
        # Clean ZIP5 (5 digits)
        zip5_clean = ''.join(filter(str.isdigit, zip5))
        if len(zip5_clean) != 5:
            raise ValueError(f"Invalid ZIP5: {zip5}")
        
        # Handle separate plus4 parameter
        if plus4:
            plus4_clean = ''.join(filter(str.isdigit, plus4))
            plus4_clean = plus4_clean.zfill(4)
            
            if len(plus4_clean) != 4:
                raise ValueError(f"Invalid ZIP+4: {plus4}")
                
            return zip5_clean, plus4_clean
        
        return zip5_clean, None
    
    async def _resolve_zip_plus4_exact(
        self, zip5: str, plus4: str, effective_params: Dict[str, Any], expose_carrier: bool
    ) -> Optional[Dict[str, Any]]:
        """Resolve exact ZIP+4 match"""
        
        # Build effective date filter
        effective_filter = self._build_effective_date_filter_from_params(effective_params)
        
        # Query for exact ZIP+4 match
        record = self.db.query(Geography).filter(
            Geography.zip5 == zip5,
            Geography.plus4 == plus4,
            Geography.has_plus4 == 1,
            effective_filter
        ).first()
        
        if record:
            return {
                "locality_id": record.locality_id,
                "state": record.state,
                "rural_flag": record.rural_flag,
                "carrier": record.carrier if expose_carrier else None,
                "match_level": "zip+4",
                "dataset_digest": record.dataset_digest,
                "distance_miles": None,
                "nearest_zip": None
            }
        
        return None
    
    async def _resolve_zip5_exact(
        self, zip5: str, effective_params: Dict[str, Any], expose_carrier: bool
    ) -> Optional[Dict[str, Any]]:
        """Resolve exact ZIP5 match (ZIP5-only records)"""
        
        # Build effective date filter
        effective_filter = self._build_effective_date_filter_from_params(effective_params)
        
        # Query for ZIP5-only records (has_plus4 = 0)
        record = self.db.query(Geography).filter(
            Geography.zip5 == zip5,
            Geography.has_plus4 == 0,
            effective_filter
        ).first()
        
        if record:
            return {
                "locality_id": record.locality_id,
                "state": record.state,
                "rural_flag": record.rural_flag,
                "carrier": record.carrier if expose_carrier else None,
                "match_level": "zip5",
                "dataset_digest": record.dataset_digest,
                "distance_miles": None,
                "nearest_zip": None
            }
        
        return None
    
    async def _resolve_nearest_zip(
        self, zip5: str, effective_params: Dict[str, Any], 
        max_radius_miles: int, initial_radius_miles: int, expand_step_miles: int, expose_carrier: bool
    ) -> Optional[Dict[str, Any]]:
        """Resolve using nearest ZIP within same state using geometry-based distance calculation"""
        
        # Build effective date filter
        effective_filter = self._build_effective_date_filter_from_params(effective_params)
        
        # Get the source ZIP's coordinates
        source_geom = self.db.query(ZipGeometry).filter(
            ZipGeometry.zip5 == zip5,
            ZipGeometry.effective_from <= effective_params["date"],
            or_(
                ZipGeometry.effective_to >= effective_params["date"],
                ZipGeometry.effective_to.is_(None)
            )
        ).first()
        
        if not source_geom:
            logger.warning("No geometry data found for source ZIP", zip5=zip5)
            return None
        
        # Get the state from the source ZIP
        source_state = source_geom.state
        
        # Search for nearest ZIPs within the same state, expanding radius as needed
        current_radius = initial_radius_miles
        
        while current_radius <= max_radius_miles:
            logger.info("Searching for nearest ZIP", zip5=zip5, radius_miles=current_radius)
            
            # Find all ZIPs in the same state within current radius
            candidates = self._find_zip_candidates_in_radius(
                source_geom, source_state, current_radius, effective_filter
            )
            
            if candidates:
                # Prefer non-PO Box ZIPs
                non_pobox_candidates = [c for c in candidates if not c.is_pobox]
                
                if non_pobox_candidates:
                    # Use the closest non-PO Box ZIP
                    closest_candidate = min(non_pobox_candidates, key=lambda c: c.distance_miles)
                    logger.info("Found non-PO Box candidate", 
                               zip5=zip5, 
                               candidate_zip=closest_candidate.zip5,
                               distance_miles=closest_candidate.distance_miles)
                else:
                    # Only PO Box candidates available - use closest one
                    closest_candidate = min(candidates, key=lambda c: c.distance_miles)
                    logger.warning("Only PO Box candidates available", 
                                  zip5=zip5, 
                                  candidate_zip=closest_candidate.zip5,
                                  distance_miles=closest_candidate.distance_miles)
                
                # Get the geography record for the closest candidate
                geography_record = self.db.query(Geography).filter(
                    Geography.zip5 == closest_candidate.zip5,
                    Geography.state == source_state,
                    effective_filter
                ).first()
                
                if geography_record:
                    return {
                        "locality_id": geography_record.locality_id,
                        "state": geography_record.state,
                        "rural_flag": geography_record.rural_flag,
                        "carrier": geography_record.carrier if expose_carrier else None,
                        "match_level": "nearest",
                        "dataset_digest": geography_record.dataset_digest,
                        "distance_miles": closest_candidate.distance_miles,
                        "nearest_zip": closest_candidate.zip5
                    }
            
            # Expand search radius
            current_radius += expand_step_miles
        
        logger.warning("No nearest ZIP found within max radius", 
                      zip5=zip5, max_radius_miles=max_radius_miles)
        return None
    
    def _find_zip_candidates_in_radius(
        self, source_geom: ZipGeometry, source_state: str, 
        radius_miles: float, effective_filter
    ) -> List[Any]:
        """Find ZIP candidates within specified radius in the same state"""
        
        # Get all ZIP geometries in the same state
        state_geometries = self.db.query(ZipGeometry).filter(
            ZipGeometry.state == source_state,
            ZipGeometry.zip5 != source_geom.zip5,  # Exclude source ZIP
            ZipGeometry.effective_from <= source_geom.effective_from,
            or_(
                ZipGeometry.effective_to >= source_geom.effective_from,
                ZipGeometry.effective_to.is_(None)
            )
        ).all()
        
        candidates = []
        for geom in state_geometries:
            distance = self._calculate_distance(
                source_geom.lat, source_geom.lon,
                geom.lat, geom.lon
            )
            
            if distance <= radius_miles:
                # Add distance to the geometry object for sorting
                geom.distance_miles = distance
                candidates.append(geom)
        
        return candidates
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        R = 3959  # Earth's radius in miles
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def _build_effective_date_filter_from_params(self, effective_params: Dict[str, Any]):
        """Build SQLAlchemy filter for effective date selection using effective_params"""
        
        if effective_params["type"] == "specific_date":
            # For specific dates, find records that cover the exact date
            effective_date = effective_params["date"]
            return and_(
                Geography.effective_from <= effective_date,
                or_(
                    Geography.effective_to >= effective_date,
                    Geography.effective_to.is_(None)
                )
            )
        elif effective_params["type"] == "quarterly":
            # For quarterly selection, use the quarter's date range
            effective_from = effective_params["effective_from"]
            effective_to = effective_params["effective_to"]
            return and_(
                Geography.effective_from <= effective_to,
                or_(
                    Geography.effective_to >= effective_from,
                    Geography.effective_to.is_(None)
                )
            )
        else:  # annual
            # For annual selection, use the full year range
            year = effective_params["year"]
            effective_from = date(year, 1, 1)
            effective_to = date(year, 12, 31)
            return and_(
                Geography.effective_from <= effective_to,
                or_(
                    Geography.effective_to >= effective_from,
                    Geography.effective_to.is_(None)
                )
            )
    
    def _build_effective_date_filter(self, valuation_year: int, quarter: Optional[int] = None):
        """Build SQLAlchemy filter for effective date selection"""
        
        if quarter:
            # Calculate quarter date ranges
            quarter_starts = {1: (1, 1), 2: (4, 1), 3: (7, 1), 4: (10, 1)}
            quarter_ends = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
            
            if quarter in quarter_starts:
                start_month, start_day = quarter_starts[quarter]
                end_month, end_day = quarter_ends[quarter]
                effective_from = date(valuation_year, start_month, start_day)
                effective_to = date(valuation_year, end_month, end_day)
            else:
                # Invalid quarter, use annual
                effective_from = date(valuation_year, 1, 1)
                effective_to = date(valuation_year, 12, 31)
        else:
            # Use annual date range
            effective_from = date(valuation_year, 1, 1)
            effective_to = date(valuation_year, 12, 31)
        
        return and_(
            Geography.effective_from <= effective_to,
            or_(
                Geography.effective_to >= effective_from,
                Geography.effective_to.is_(None)
            )
        )
    
    def _check_ambiguity(self, candidates: List[GeographyCandidate]) -> bool:
        """Check if geography mapping is ambiguous"""
        if len(candidates) <= 1:
            return False
        
        # Check if we have multiple different localities for the same ZIP
        unique_localities = set(c.locality_id for c in candidates)
        return len(unique_localities) > 1
    
    def _select_default_candidate(self, candidates: List[GeographyCandidate]) -> Optional[GeographyCandidate]:
        """Select default candidate using deterministic rules"""
        if not candidates:
            return None
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Sort by locality_id (ascending) for deterministic selection
        sorted_candidates = sorted(
            candidates,
            key=lambda c: c.locality_id or ""
        )
        
        return sorted_candidates[0]
    
    # Legacy method for backward compatibility - now redirects to new resolver
    async def resolve_zip_legacy(self, zip5: str, valuation_date: Optional[date] = None) -> GeographyResolveResponse:
        """Legacy ZIP resolution method for backward compatibility"""
        
        try:
            # Use new resolver
            result = await self.resolve_zip(
                zip5=zip5,
                plus4=None,
                valuation_year=valuation_date.year if valuation_date else None,
                quarter=None,
                strict=False,
                expose_carrier=False
            )
            
            # Convert to legacy response format
            candidate = GeographyCandidate(
                zip5=zip5,
                locality_id=result["locality_id"],
                locality_name=None,
                cbsa=None,
                cbsa_name=None,
                county_fips=None,
                state_code=result["state"],
                population_share=None,
                rural_flag=result["rural_flag"],  # Preserve original string value per PRD
                used=True
            )
            
            return GeographyResolveResponse(
                zip5=zip5,
                candidates=[candidate],
                requires_resolution=False,
                ambiguity_threshold=0.2,
                selected_candidate=candidate,
                resolution_method=result["match_level"],
                warnings=[]
            )
            
        except Exception as e:
            logger.error("Legacy resolution failed", zip5=zip5, error=str(e))
        return GeographyResolveResponse(
            zip5=zip5,
            candidates=[],
            requires_resolution=True,
            ambiguity_threshold=0.2,
            selected_candidate=None,
                resolution_method="error",
                warnings=[f"ZIP {zip5} resolution failed: {str(e)}"]
        )
    
    def __del__(self):
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
