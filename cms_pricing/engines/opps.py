"""Outpatient Prospective Payment System pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeOPPS, WageIndex
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "OPPS"


class OPPSEngine(BasePricingEngine):
    """Outpatient Prospective Payment System pricing engine"""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize OPPS engine with optional database session.
        
        Args:
            db: Optional SQLAlchemy session. If None, creates a new session.
                Session will be closed when engine is destroyed.
        """
        self.db = db if db is not None else SessionLocal()
        self._owns_session = db is None
    
    @staticmethod
    def _build_opps_filter(year: int, quarter: str, code: str):
        """Build reusable filter expression for OPPS queries"""
        return and_(
            FeeOPPS.year == year,
            FeeOPPS.quarter == quarter,
            FeeOPPS.hcpcs == code,
            FeeOPPS.effective_from <= f"{year}-12-31",
            or_(
                FeeOPPS.effective_to.is_(None),
                FeeOPPS.effective_to >= f"{year}-01-01"
            )
        )
    
    @staticmethod
    def _build_wage_index_filter(year: int, quarter: str, cbsa: str):
        """Build reusable filter expression for wage index queries"""
        return and_(
            WageIndex.year == year,
            WageIndex.quarter == quarter,
            WageIndex.cbsa == cbsa
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
        """Price a code using OPPS (Quick Win #2: Returns unified CodePricingItem)"""
        
        try:
            # Get CBSA from geography
            cbsa = None
            if geography and geography.selected_candidate:
                cbsa = geography.selected_candidate.cbsa
            
            if not cbsa:
                raise ValueError("No CBSA found for ZIP code")
            
            # Coalesce quarter to default to "1" if None
            quarter_value = quarter if quarter is not None else "1"
            
            # Pre-compute trace ref base (optimization)
            trace_refs = [
                f"opps_{year}_{quarter_value}_{code}",
                f"wage_index_{year}_{quarter_value}_{cbsa}"
            ]
            dataset_id = DATASET_ID
            
            # Query only necessary columns using with_entities (optimization)
            opps_result = self.db.query(
                FeeOPPS.national_unadj_rate,
                FeeOPPS.status_indicator,
                FeeOPPS.release_id,
                FeeOPPS.batch_id
            ).filter(
                self._build_opps_filter(year, quarter_value, code)
            ).first()
            
            if not opps_result:
                raise ValueError(f"No OPPS data found for code {code}")
            
            # Query wage index with column selection
            wage_index_result = self.db.query(
                WageIndex.wage_index,
                WageIndex.release_id,
                WageIndex.batch_id
            ).filter(
                self._build_wage_index_filter(year, quarter_value, cbsa)
            ).first()
            
            if not wage_index_result:
                raise ValueError(f"No wage index found for CBSA {cbsa}")
            
            # Extract values from Row results
            base_rate = opps_result.national_unadj_rate if opps_result.national_unadj_rate else 0
            opps_release_id = opps_result.release_id
            opps_batch_id = opps_result.batch_id
            status_indicator = opps_result.status_indicator
            
            wage_index = wage_index_result.wage_index
            wage_release_id = wage_index_result.release_id
            wage_batch_id = wage_index_result.batch_id
            
            # Calculate base rate
            # Apply wage index
            wage_adjusted_rate = base_rate * wage_index
            
            # Check packaging status
            packaged = self._is_packaged(status_indicator)
            
            if packaged:
                # Packaged items have $0 separate payment
                allowed_amount = 0
            else:
                # Apply modifiers
                if modifiers:
                    wage_adjusted_rate = self._apply_modifiers(wage_adjusted_rate, modifiers)
                
                # Apply units and utilization weight
                allowed_amount = wage_adjusted_rate * units * utilization_weight
            
            # Calculate beneficiary cost sharing
            cost_sharing = self._calculate_beneficiary_cost_sharing(allowed_amount)
            
            # Convert to cents
            allowed_cents = int(allowed_amount * 100)
            beneficiary_deductible_cents = int(cost_sharing["beneficiary_deductible"] * 100)
            beneficiary_coinsurance_cents = int(cost_sharing["beneficiary_coinsurance"] * 100)
            beneficiary_total_cents = int(cost_sharing["beneficiary_total"] * 100)
            program_payment_cents = int(cost_sharing["program_payment"] * 100)
            
            # Add OPPS provenance (standardized format)
            if opps_release_id:
                trace_refs.append(f"{dataset_id}:release:{opps_release_id}")
            if opps_batch_id:
                trace_refs.append(f"{dataset_id}:batch:{opps_batch_id}")
            
            # Add wage index provenance if available
            if wage_release_id:
                trace_refs.append(f"WageIndex:release:{wage_release_id}")
            if wage_batch_id:
                trace_refs.append(f"WageIndex:batch:{wage_batch_id}")
            
            # Filter out None values and deduplicate while preserving order
            trace_refs = list(dict.fromkeys([ref for ref in trace_refs if ref is not None]))
            
            # Extract primary modifier (if multiple, use first)
            primary_modifier = modifiers[0] if modifiers and len(modifiers) > 0 else None
            
            return CodePricingItem(
                code=code,
                setting="OPPS",
                modifier=primary_modifier,
                allowed_cents=allowed_cents,
                beneficiary_deductible_cents=beneficiary_deductible_cents,
                beneficiary_coinsurance_cents=beneficiary_coinsurance_cents,
                beneficiary_total_cents=beneficiary_total_cents,
                program_payment_cents=program_payment_cents,
                professional_allowed_cents=0,  # OPPS is facility only
                facility_allowed_cents=allowed_cents if facility_component else 0,
                dataset_id=dataset_id,
                release_id=opps_release_id,
                batch_id=opps_batch_id,
                trace_refs=trace_refs,
                source="benchmark",
                facility_specific=False,
                packaged=packaged,
                units=units
            )
            
        except Exception as e:
            logger.error(
                "OPPS pricing failed",
                code=code,
                zip=zip,
                year=year,
                quarter=quarter,
                cbsa=cbsa,
                error=str(e),
                exc_info=True
            )
            raise
    
    def _is_packaged(self, status_indicator: Optional[str]) -> bool:
        """Check if item is packaged based on status indicator"""
        if not status_indicator:
            return False
        
        # Packaged indicators
        packaged_indicators = ["N", "J1", "Q1", "Q2", "Q3"]
        return status_indicator in packaged_indicators
    
    def __del__(self):
        """Clean up database session if we own it"""
        if hasattr(self, '_owns_session') and self._owns_session and hasattr(self, 'db'):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
