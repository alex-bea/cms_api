"""Geography resolution service"""

from typing import List, Optional
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.database import SessionLocal
from cms_pricing.models.geography import Geography
from cms_pricing.schemas.geography import (
    GeographyResolveResponse, GeographyCandidate
)
from cms_pricing.config import settings
import structlog

logger = structlog.get_logger()


class GeographyService:
    """Service for resolving ZIP codes to localities and CBSAs"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
    
    async def resolve_zip(self, zip5: str) -> GeographyResolveResponse:
        """Resolve ZIP code to locality/CBSA with ambiguity handling"""
        
        # Get all geography mappings for this ZIP
        current_date = date.today()
        geography_records = self.db.query(Geography).filter(
            and_(
                Geography.zip5 == zip5,
                Geography.effective_from <= current_date,
                or_(
                    Geography.effective_to.is_(None),
                    Geography.effective_to >= current_date
                )
            )
        ).all()
        
        if not geography_records:
            # Try radius expansion
            return await self._resolve_with_expansion(zip5)
        
        # Convert to candidates
        candidates = []
        for record in geography_records:
            candidate = GeographyCandidate(
                zip5=record.zip5,
                locality_id=record.locality_id,
                locality_name=record.locality_name,
                cbsa=None,  # Not available in our model
                cbsa_name=None,  # Not available in our model
                county_fips=None,  # Not available in our model
                state_code=record.state,  # Use 'state' field
                population_share=None,  # Not available in our model
                is_rural_dmepos=record.rural_flag,  # Use 'rural_flag' field
                used=False
            )
            candidates.append(candidate)
        
        # Check for ambiguity
        requires_resolution = self._check_ambiguity(candidates)
        
        # Select default candidate
        selected_candidate = self._select_default_candidate(candidates)
        if selected_candidate:
            selected_candidate.used = True
        
        # Determine resolution method
        resolution_method = "first_match"  # Since we don't have population_share data
        if len(candidates) == 1:
            resolution_method = "single_candidate"
        elif requires_resolution:
            resolution_method = "ambiguous_requires_resolution"
        
        warnings = []
        if requires_resolution:
            warnings.append(f"ZIP {zip5} has ambiguous geography mapping")
        
        return GeographyResolveResponse(
            zip5=zip5,
            candidates=candidates,
            requires_resolution=requires_resolution,
            ambiguity_threshold=0.2,
            selected_candidate=selected_candidate,
            resolution_method=resolution_method,
            warnings=warnings
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
    
    async def _resolve_with_expansion(self, zip5: str) -> GeographyResolveResponse:
        """Resolve ZIP with radius expansion (placeholder)"""
        # TODO: Implement radius expansion logic
        logger.warning("ZIP not found, radius expansion not implemented", zip5=zip5)
        
        return GeographyResolveResponse(
            zip5=zip5,
            candidates=[],
            requires_resolution=True,
            ambiguity_threshold=0.2,
            selected_candidate=None,
            resolution_method="not_found",
            warnings=[f"ZIP {zip5} not found in geography database"]
        )
    
    def __del__(self):
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
