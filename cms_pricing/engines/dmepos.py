"""Durable Medical Equipment, Prosthetics, Orthotics, and Supplies pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeDMEPOS
from cms_pricing.schemas.geography import GeographyResolveResponse
import structlog

logger = structlog.get_logger()


class DMEPOSEngine(BasePricingEngine):
    """Durable Medical Equipment, Prosthetics, Orthotics, and Supplies pricing engine"""
    
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
        """Price a code using DMEPOS fee schedule"""
        
        try:
            # Determine rural status
            is_rural = False
            if geography and geography.selected_candidate:
                is_rural = geography.selected_candidate.rural_flag in ['R', 'B'] if geography.selected_candidate and geography.selected_candidate.rural_flag else False
            
            # Get DMEPOS data
            dmepos_data = self.db.query(FeeDMEPOS).filter(
                and_(
                    FeeDMEPOS.year == year,
                    FeeDMEPOS.quarter == quarter or "1",  # Default to Q1
                    FeeDMEPOS.code == code,
                    FeeDMEPOS.rural_flag == is_rural,
                    FeeDMEPOS.effective_from <= f"{year}-12-31",
                    or_(
                        FeeDMEPOS.effective_to.is_(None),
                        FeeDMEPOS.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not dmepos_data:
                raise ValueError(f"No DMEPOS data found for code {code} (rural: {is_rural})")
            
            # Get base fee
            base_fee = dmepos_data.fee or 0
            
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
            
            return {
                "allowed_cents": allowed_cents,
                "beneficiary_deductible_cents": beneficiary_deductible_cents,
                "beneficiary_coinsurance_cents": beneficiary_coinsurance_cents,
                "beneficiary_total_cents": beneficiary_total_cents,
                "program_payment_cents": program_payment_cents,
                "professional_allowed_cents": allowed_cents if professional_component else 0,
                "facility_allowed_cents": 0,  # DMEPOS is professional only
                "source": "benchmark",
                "facility_specific": False,
                "packaged": False,
                "trace_refs": [
                    f"dmepos_{year}_{quarter}_{code}_{is_rural}"
                ]
            }
            
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
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
