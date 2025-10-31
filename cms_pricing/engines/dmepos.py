"""Durable Medical Equipment, Prosthetics, Orthotics, and Supplies pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeDMEPOS
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "DMEPOS"


class DMEPOSEngine(BasePricingEngine):
    """Durable Medical Equipment, Prosthetics, Orthotics, and Supplies pricing engine"""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize DMEPOS engine with optional database session.
        
        Args:
            db: Optional SQLAlchemy session. If None, creates a new session.
                Session will be closed when engine is destroyed.
        """
        self.db = db if db is not None else SessionLocal()
        self._owns_session = db is None
    
    @staticmethod
    def _build_dmepos_filter(year: int, quarter: str, code: str, is_rural: bool):
        """Build reusable filter expression for DMEPOS queries"""
        return and_(
            FeeDMEPOS.year == year,
            FeeDMEPOS.quarter == quarter,
            FeeDMEPOS.code == code,
            FeeDMEPOS.rural_flag == is_rural,
            FeeDMEPOS.effective_from <= f"{year}-12-31",
            or_(
                FeeDMEPOS.effective_to.is_(None),
                FeeDMEPOS.effective_to >= f"{year}-01-01"
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
        """Price a code using DMEPOS fee schedule (Quick Win #2: Returns unified CodePricingItem)"""
        
        try:
            # Coalesce quarter to default to "1" if None
            quarter_value = quarter if quarter is not None else "1"
            
            # Determine rural status
            is_rural = False
            if geography and geography.selected_candidate:
                is_rural = geography.selected_candidate.rural_flag in ['R', 'B'] if geography.selected_candidate and geography.selected_candidate.rural_flag else False
            
            # Pre-compute trace ref base (optimization)
            trace_refs = [f"dmepos_{year}_{quarter_value}_{code}_{is_rural}"]
            dataset_id = DATASET_ID
            
            # Query only necessary columns using with_entities (optimization)
            dmepos_result = self.db.query(
                FeeDMEPOS.fee,
                FeeDMEPOS.release_id,
                FeeDMEPOS.batch_id
            ).filter(
                self._build_dmepos_filter(year, quarter_value, code, is_rural)
            ).first()
            
            if not dmepos_result:
                raise ValueError(f"No DMEPOS data found for code {code} (rural: {is_rural})")
            
            # Extract values from Row result
            base_fee = dmepos_result.fee if dmepos_result.fee else 0
            release_id = dmepos_result.release_id
            batch_id = dmepos_result.batch_id
            
            # Apply modifiers
            if modifiers:
                base_fee = self._apply_modifiers(base_fee, modifiers)
            
            # Apply units and utilization weight
            allowed_amount = base_fee * units * utilization_weight
            
            # Calculate beneficiary cost sharing
            cost_sharing = self._calculate_beneficiary_cost_sharing(allowed_amount)
            
            # Convert to cents
            allowed_cents = int(allowed_amount * 100)
            beneficiary_deductible_cents = int(cost_sharing["beneficiary_deductible"] * 100)
            beneficiary_coinsurance_cents = int(cost_sharing["beneficiary_coinsurance"] * 100)
            beneficiary_total_cents = int(cost_sharing["beneficiary_total"] * 100)
            program_payment_cents = int(cost_sharing["program_payment"] * 100)
            
            # Add DMEPOS provenance (standardized format)
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
                setting="DMEPOS",
                modifier=primary_modifier,
                allowed_cents=allowed_cents,
                beneficiary_deductible_cents=beneficiary_deductible_cents,
                beneficiary_coinsurance_cents=beneficiary_coinsurance_cents,
                beneficiary_total_cents=beneficiary_total_cents,
                program_payment_cents=program_payment_cents,
                professional_allowed_cents=allowed_cents if professional_component else 0,
                facility_allowed_cents=0,  # DMEPOS is professional only
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
                "DMEPOS pricing failed",
                code=code,
                zip=zip,
                year=year,
                quarter=quarter,
                is_rural=is_rural,
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
