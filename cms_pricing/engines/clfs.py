"""Clinical Laboratory Fee Schedule pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeCLFS
from cms_pricing.schemas.geography import GeographyResolveResponse
import structlog

logger = structlog.get_logger()


class CLFSEngine(BasePricingEngine):
    """Clinical Laboratory Fee Schedule pricing engine"""
    
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
        """Price a code using CLFS"""
        
        try:
            # Get CLFS data
            clfs_data = self.db.query(FeeCLFS).filter(
                and_(
                    FeeCLFS.year == year,
                    FeeCLFS.quarter == quarter or "1",  # Default to Q1
                    FeeCLFS.hcpcs == code,
                    FeeCLFS.effective_from <= f"{year}-12-31",
                    or_(
                        FeeCLFS.effective_to.is_(None),
                        FeeCLFS.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not clfs_data:
                raise ValueError(f"No CLFS data found for code {code}")
            
            # Get base fee
            base_fee = clfs_data.fee or 0
            
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
                "facility_allowed_cents": 0,  # CLFS is professional only
                "source": "benchmark",
                "facility_specific": False,
                "packaged": False,
                "trace_refs": [
                    f"clfs_{year}_{quarter}_{code}"
                ]
            }
            
        except Exception as e:
            logger.error(
                "CLFS pricing failed",
                code=code,
                zip=zip,
                year=year,
                quarter=quarter,
                error=str(e),
                exc_info=True
            )
            raise
    
    def __del__(self):
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
