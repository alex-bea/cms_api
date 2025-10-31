"""Ambulatory Surgical Center pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeASC
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "ASC"


class ASCEngine(BasePricingEngine):
    """Ambulatory Surgical Center pricing engine"""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize ASC engine with optional database session.
        
        Args:
            db: Optional SQLAlchemy session. If None, creates a new session.
                Session will be closed when engine is destroyed.
        """
        self.db = db if db is not None else SessionLocal()
        self._owns_session = db is None
    
    @staticmethod
    def _build_asc_filter(year: int, quarter: str, code: str):
        """
        Build reusable filter expression for ASC queries.
        
        Pre-compiles the common (year, quarter, hcpcs, date range) filter
        to avoid re-compiling on every call.
        """
        return and_(
            FeeASC.year == year,
            FeeASC.quarter == quarter,
            FeeASC.hcpcs == code,
            FeeASC.effective_from <= f"{year}-12-31",
            or_(
                FeeASC.effective_to.is_(None),
                FeeASC.effective_to >= f"{year}-01-01"
            )
        )
    
    async def price_code(
        self,
        code: str,
        zip: str,
        year: int,
        quarter: Optional[str] = None,
        geography: Optional[GeographyResolveResponse] = None,
        ccn: Optional[str] = None,
        payer: Optional[str] = None,
        plan: Optional[str] = None,
        units: float = 1.0,
        utilization_weight: float = 1.0,
        professional_component: bool = True,
        facility_component: bool = True,
        modifiers: Optional[List[str]] = None,
        pos: Optional[str] = None,
        ndc11: Optional[str] = None
    ) -> CodePricingItem:
        """Price a code using ASC fee schedule (Quick Win #2: Returns unified CodePricingItem)"""
        
        try:
            # Coalesce quarter to default to "1" if None
            quarter_value = quarter if quarter is not None else "1"
            
            # Pre-compute trace ref base (optimization: build trace ref before DB work)
            trace_refs = [f"asc_{year}_{quarter_value}_{code}"]
            dataset_id = DATASET_ID
            
            # Query only necessary columns using with_entities (optimization)
            # Returns Row tuple: (asc_rate, release_id, batch_id)
            asc_result = self.db.query(
                FeeASC.asc_rate,
                FeeASC.release_id,
                FeeASC.batch_id
            ).filter(
                self._build_asc_filter(year, quarter_value, code)
            ).first()
            
            if not asc_result:
                raise ValueError(f"No ASC data found for code {code}")
            
            # Extract values from Row tuple (access by index or attribute)
            base_rate = asc_result.asc_rate if asc_result.asc_rate else 0
            release_id = asc_result.release_id
            batch_id = asc_result.batch_id
            
            # Apply modifiers
            if modifiers:
                base_rate = self._apply_modifiers(base_rate, modifiers)
            
            # Apply units and utilization weight
            allowed_amount = base_rate * units * utilization_weight
            
            # Calculate beneficiary cost sharing
            cost_sharing = self._calculate_beneficiary_cost_sharing(allowed_amount)
            
            # Convert to cents
            allowed_cents = int(allowed_amount * 100)
            beneficiary_deductible_cents = int(cost_sharing["beneficiary_deductible"] * 100)
            beneficiary_coinsurance_cents = int(cost_sharing["beneficiary_coinsurance"] * 100)
            beneficiary_total_cents = int(cost_sharing["beneficiary_total"] * 100)
            program_payment_cents = int(cost_sharing["program_payment"] * 100)
            
            # Add ASC provenance (standardized format)
            if release_id:
                trace_refs.append(f"{dataset_id}:release:{release_id}")
            if batch_id:
                trace_refs.append(f"{dataset_id}:batch:{batch_id}")
            
            # Filter out None values and deduplicate while preserving order
            trace_refs = list(dict.fromkeys([ref for ref in trace_refs if ref is not None]))
            
            # Extract primary modifier (if multiple, use first)
            primary_modifier = modifiers[0] if modifiers and len(modifiers) > 0 else None
            
            return CodePricingItem(
                code=code,
                setting="ASC",
                modifier=primary_modifier,
                allowed_cents=allowed_cents,
                beneficiary_deductible_cents=beneficiary_deductible_cents,
                beneficiary_coinsurance_cents=beneficiary_coinsurance_cents,
                beneficiary_total_cents=beneficiary_total_cents,
                program_payment_cents=program_payment_cents,
                professional_allowed_cents=0,  # ASC is facility only
                facility_allowed_cents=allowed_cents if facility_component else 0,
                dataset_id=dataset_id,
                release_id=release_id,
                batch_id=batch_id,
                trace_refs=trace_refs,
                source="benchmark",
                facility_specific=False,
                packaged=False,
                units=units
            )
            
        except Exception as e:
            logger.error(
                "ASC pricing failed",
                code=code,
                zip=zip,
                year=year,
                quarter=quarter,
                error=str(e),
                exc_info=True
            )
            raise
    
    def __del__(self):
        """Clean up database session if we own it"""
        if hasattr(self, '_owns_session') and self._owns_session and hasattr(self, 'db'):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
