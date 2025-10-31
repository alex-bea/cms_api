"""Drug pricing engine for Part B ASP and NADAC"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.drugs import DrugASP, DrugNADAC, NDCHCPCSXwalk
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
import structlog

logger = structlog.get_logger()


class DrugEngine(BasePricingEngine):
    """Drug pricing engine for Part B ASP and NADAC"""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize Drug engine with optional database session.
        
        Args:
            db: Optional SQLAlchemy session. If None, creates a new session.
                Session will be closed when engine is destroyed.
        """
        self.db = db if db is not None else SessionLocal()
        self._owns_session = db is None
    
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
        """Price a drug code using ASP and optionally NADAC (Quick Win #2: Returns unified CodePricingItem)"""
        
        try:
            # Get ASP data for Part B drugs
            asp_data = self.db.query(DrugASP).filter(
                and_(
                    DrugASP.year == year,
                    DrugASP.quarter == quarter or "1",  # Default to Q1
                    DrugASP.hcpcs == code,
                    DrugASP.effective_from <= f"{year}-12-31",
                    or_(
                        DrugASP.effective_to.is_(None),
                        DrugASP.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not asp_data:
                raise ValueError(f"No ASP data found for code {code}")
            
            # Calculate Part B allowed amount
            # Formula: ASP × (1 + 0.06) × units
            asp_per_unit = asp_data.asp_per_unit or 0
            part_b_allowed = asp_per_unit * 1.06 * units * utilization_weight
            
            # Apply modifiers
            if modifiers:
                part_b_allowed = self._apply_modifiers(part_b_allowed, modifiers)
            
            # Calculate beneficiary cost sharing
            cost_sharing = self._calculate_beneficiary_cost_sharing(part_b_allowed)
            
            # Convert to cents
            allowed_cents = int(part_b_allowed * 100)
            beneficiary_deductible_cents = int(cost_sharing["beneficiary_deductible"] * 100)
            beneficiary_coinsurance_cents = int(cost_sharing["beneficiary_coinsurance"] * 100)
            beneficiary_total_cents = int(cost_sharing["beneficiary_total"] * 100)
            program_payment_cents = int(cost_sharing["program_payment"] * 100)
            
            # Build trace refs
            trace_refs = [
                f"asp_{year}_{quarter}_{code}"
            ]
            
            # Drug engine doesn't have Phase 2 provenance yet (no release_id/batch_id in DrugASP)
            # Extract primary modifier (if multiple, use first)
            primary_modifier = modifiers[0] if modifiers and len(modifiers) > 0 else None
            
            # Initialize reference price and unit conversion
            reference_price_cents = None
            unit_conversion = None
            
            # Add NADAC reference if NDC provided
            if ndc11:
                nadac_data = await self._get_nadac_price(ndc11)
                if nadac_data:
                    # Get unit conversion
                    conversion = await self._get_unit_conversion(ndc11, code)
                    if conversion:
                        nadac_price = nadac_data['unit_price'] * conversion * units * utilization_weight
                        reference_price_cents = int(nadac_price * 100)
                        unit_conversion = {
                            "ndc11": ndc11,
                            "hcpcs": code,
                            "units_per_hcpcs": conversion,
                            "nadac_unit_price": nadac_data['unit_price'],
                            "nadac_as_of": nadac_data['as_of']
                        }
                        trace_refs.append(f"nadac_{nadac_data['as_of']}_{ndc11}")
            
            return CodePricingItem(
                code=code,
                setting="DRUGS",
                modifier=primary_modifier,
                allowed_cents=allowed_cents,
                beneficiary_deductible_cents=beneficiary_deductible_cents,
                beneficiary_coinsurance_cents=beneficiary_coinsurance_cents,
                beneficiary_total_cents=beneficiary_total_cents,
                program_payment_cents=program_payment_cents,
                professional_allowed_cents=allowed_cents if professional_component else 0,
                facility_allowed_cents=0,  # Drugs are professional only
                dataset_id="DRUGS",
                release_id=None,  # Drug engine doesn't have Phase 2 provenance yet
                batch_id=None,
                trace_refs=trace_refs,
                source="benchmark",
                facility_specific=False,
                packaged=False,
                reference_price_cents=reference_price_cents,
                unit_conversion=unit_conversion,
                units=units
            )
            
        except Exception as e:
            logger.error(
                "Drug pricing failed",
                code=code,
                zip=zip,
                year=year,
                quarter=quarter,
                ndc11=ndc11,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def _get_nadac_price(self, ndc11: str) -> Optional[Dict[str, Any]]:
        """Get NADAC price for NDC"""
        
        try:
            # Get most recent NADAC data
            nadac_data = self.db.query(DrugNADAC).filter(
                DrugNADAC.ndc11 == ndc11
            ).order_by(DrugNADAC.as_of.desc()).first()
            
            if not nadac_data:
                return None
            
            return {
                "unit_price": nadac_data.unit_price,
                "as_of": nadac_data.as_of.isoformat(),
                "unit_type": nadac_data.unit_type
            }
            
        except Exception as e:
            logger.warning(
                "Failed to get NADAC price",
                ndc11=ndc11,
                error=str(e)
            )
            return None
    
    async def _get_unit_conversion(self, ndc11: str, hcpcs: str) -> Optional[float]:
        """Get unit conversion from NDC to HCPCS"""
        
        try:
            conversion_data = self.db.query(NDCHCPCSXwalk).filter(
                and_(
                    NDCHCPCSXwalk.ndc11 == ndc11,
                    NDCHCPCSXwalk.hcpcs == hcpcs
                )
            ).first()
            
            if not conversion_data:
                return None
            
            return conversion_data.units_per_hcpcs
            
        except Exception as e:
            logger.warning(
                "Failed to get unit conversion",
                ndc11=ndc11,
                hcpcs=hcpcs,
                error=str(e)
            )
            return None
    
    def __del__(self):
        """Clean up database session if we own it"""
        if hasattr(self, '_owns_session') and self._owns_session and hasattr(self, 'db'):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
