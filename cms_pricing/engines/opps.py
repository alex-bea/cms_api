"""Outpatient Prospective Payment System pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeOPPS, WageIndex
from cms_pricing.schemas.geography import GeographyResolveResponse
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "OPPS"


class OPPSEngine(BasePricingEngine):
    """Outpatient Prospective Payment System pricing engine"""
    
    def __init__(self):
        self.db = SessionLocal()
    
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
    ) -> Dict[str, Any]:
        """Price a code using OPPS"""
        
        try:
            # Get CBSA from geography
            cbsa = None
            if geography and geography.selected_candidate:
                cbsa = geography.selected_candidate.cbsa
            
            if not cbsa:
                raise ValueError("No CBSA found for ZIP code")
            
            # Coalesce quarter to default to "1" if None
            quarter_value = quarter if quarter is not None else "1"
            
            # Get OPPS data
            opps_data = self.db.query(FeeOPPS).filter(
                and_(
                    FeeOPPS.year == year,
                    FeeOPPS.quarter == quarter_value,
                    FeeOPPS.hcpcs == code,
                    FeeOPPS.effective_from <= f"{year}-12-31",
                    or_(
                        FeeOPPS.effective_to.is_(None),
                        FeeOPPS.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not opps_data:
                raise ValueError(f"No OPPS data found for code {code}")
            
            # Get wage index
            wage_index_data = self.db.query(WageIndex).filter(
                and_(
                    WageIndex.year == year,
                    WageIndex.quarter == quarter_value,
                    WageIndex.cbsa == cbsa
                )
            ).first()
            
            if not wage_index_data:
                raise ValueError(f"No wage index found for CBSA {cbsa}")
            
            # Calculate base rate
            base_rate = opps_data.national_unadj_rate or 0
            
            # Apply wage index
            wage_adjusted_rate = base_rate * wage_index_data.wage_index
            
            # Check packaging status
            packaged = self._is_packaged(opps_data.status_indicator)
            
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
            
            # Build trace refs with provenance (Phase 2.5)
            dataset_id = DATASET_ID
            trace_refs = [
                f"opps_{year}_{quarter_value}_{code}",
                f"wage_index_{year}_{quarter_value}_{cbsa}"
            ]
            
            # Add OPPS provenance (standardized format)
            if opps_data.release_id:
                trace_refs.append(f"{dataset_id}:release:{opps_data.release_id}")
            if opps_data.batch_id:
                trace_refs.append(f"{dataset_id}:batch:{opps_data.batch_id}")
            
            # Add wage index provenance if available
            if wage_index_data.release_id:
                trace_refs.append(f"WageIndex:release:{wage_index_data.release_id}")
            if wage_index_data.batch_id:
                trace_refs.append(f"WageIndex:batch:{wage_index_data.batch_id}")
            
            # Filter out None values and deduplicate while preserving order
            trace_refs = list(dict.fromkeys([ref for ref in trace_refs if ref is not None]))
            
            return {
                "allowed_cents": allowed_cents,
                "beneficiary_deductible_cents": beneficiary_deductible_cents,
                "beneficiary_coinsurance_cents": beneficiary_coinsurance_cents,
                "beneficiary_total_cents": beneficiary_total_cents,
                "program_payment_cents": program_payment_cents,
                "professional_allowed_cents": 0,  # OPPS is facility only
                "facility_allowed_cents": allowed_cents if facility_component else 0,
                "source": "benchmark",
                "facility_specific": False,
                "packaged": packaged,
                "trace_refs": trace_refs,
                # Direct provenance fields (Phase 2.5)
                "release_id": opps_data.release_id,
                "batch_id": opps_data.batch_id,
                "dataset_id": dataset_id
            }
            
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
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
