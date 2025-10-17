"""Inpatient Prospective Payment System pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeIPPS, IPPSBaseRate, WageIndex
from cms_pricing.schemas.geography import GeographyResolveResponse
import structlog

logger = structlog.get_logger()


class IPPSEngine(BasePricingEngine):
    """Inpatient Prospective Payment System pricing engine"""
    
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
        """Price a code using IPPS"""
        
        try:
            # Get CBSA from geography
            cbsa = None
            if geography and geography.selected_candidate:
                cbsa = geography.selected_candidate.cbsa
            
            if not cbsa:
                raise ValueError("No CBSA found for ZIP code")
            
            # Convert year to fiscal year
            fy = year if year >= 10 else year + 2000
            
            # Get DRG data
            drg_data = self.db.query(FeeIPPS).filter(
                and_(
                    FeeIPPS.fy == fy,
                    FeeIPPS.drg == code,
                    FeeIPPS.effective_from <= f"{year}-12-31",
                    or_(
                        FeeIPPS.effective_to.is_(None),
                        FeeIPPS.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not drg_data:
                raise ValueError(f"No IPPS data found for DRG {code}")
            
            # Get base rates
            base_rate_data = self.db.query(IPPSBaseRate).filter(
                and_(
                    IPPSBaseRate.fy == fy,
                    IPPSBaseRate.effective_from <= f"{year}-12-31",
                    or_(
                        IPPSBaseRate.effective_to.is_(None),
                        IPPSBaseRate.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not base_rate_data:
                raise ValueError(f"No IPPS base rates found for FY {fy}")
            
            # Get wage index
            wage_index_data = self.db.query(WageIndex).filter(
                and_(
                    WageIndex.year == year,
                    WageIndex.cbsa == cbsa,
                    WageIndex.quarter.is_(None)  # IPPS uses annual wage index
                )
            ).first()
            
            if not wage_index_data:
                raise ValueError(f"No wage index found for CBSA {cbsa}")
            
            # Calculate IPPS payment
            # Formula: DRG_weight × ((operating_base × WI) + (capital_base × WI))
            operating_component = base_rate_data.operating_base * wage_index_data.wage_index
            capital_component = base_rate_data.capital_base * wage_index_data.wage_index
            
            base_payment = drg_data.weight * (operating_component + capital_component)
            
            # Apply modifiers
            if modifiers:
                base_payment = self._apply_modifiers(base_payment, modifiers)
            
            # Apply units and utilization weight
            allowed_amount = base_payment * units * utilization_weight
            
            # Calculate beneficiary cost sharing (Part A inpatient deductible)
            # For MVP, allocate entire deductible to DRG line
            part_a_deductible = 1600.0  # TODO(alex, GH-431): Get from benefit params
            beneficiary_deductible = min(part_a_deductible, allowed_amount)
            beneficiary_coinsurance = 0  # No coinsurance for IPPS
            beneficiary_total = beneficiary_deductible
            program_payment = allowed_amount - beneficiary_total
            
            # Convert to cents
            allowed_cents = int(allowed_amount * 100)
            beneficiary_deductible_cents = int(beneficiary_deductible * 100)
            beneficiary_coinsurance_cents = int(beneficiary_coinsurance * 100)
            beneficiary_total_cents = int(beneficiary_total * 100)
            program_payment_cents = int(program_payment * 100)
            
            return {
                "allowed_cents": allowed_cents,
                "beneficiary_deductible_cents": beneficiary_deductible_cents,
                "beneficiary_coinsurance_cents": beneficiary_coinsurance_cents,
                "beneficiary_total_cents": beneficiary_total_cents,
                "program_payment_cents": program_payment_cents,
                "professional_allowed_cents": 0,  # IPPS is facility only
                "facility_allowed_cents": allowed_cents if facility_component else 0,
                "source": "benchmark",
                "facility_specific": False,
                "packaged": False,
                "trace_refs": [
                    f"ipps_{fy}_{code}",
                    f"ipps_base_{fy}",
                    f"wage_index_{year}_{cbsa}"
                ]
            }
            
        except Exception as e:
            logger.error(
                "IPPS pricing failed",
                code=code,
                zip=zip,
                year=year,
                fy=fy,
                cbsa=cbsa,
                error=str(e),
                exc_info=True
            )
            raise
    
    def __del__(self):
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
