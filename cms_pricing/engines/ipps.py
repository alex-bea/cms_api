"""Inpatient Prospective Payment System pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeIPPS, IPPSBaseRate, WageIndex
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "IPPS"


class IPPSEngine(BasePricingEngine):
    """Inpatient Prospective Payment System pricing engine"""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize IPPS engine with optional database session.
        
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
        """Price a code using IPPS (Quick Win #2: Returns unified CodePricingItem)"""
        
        try:
            # Get CBSA from geography
            cbsa = None
            if geography and geography.selected_candidate:
                cbsa = geography.selected_candidate.cbsa
            
            if not cbsa:
                raise ValueError("No CBSA found for ZIP code")
            
            # Convert year to fiscal year
            fy = year if year >= 10 else year + 2000
            
            # Pre-compute trace ref base (optimization)
            trace_refs = [
                f"ipps_{fy}_{code}",
                f"ipps_base_{fy}",
                f"wage_index_{year}_{cbsa}"
            ]
            dataset_id = DATASET_ID
            
            # Query only necessary columns using with_entities (optimization)
            drg_result = self.db.query(
                FeeIPPS.weight,
                FeeIPPS.release_id,
                FeeIPPS.batch_id
            ).filter(
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
            
            if not drg_result:
                raise ValueError(f"No IPPS data found for DRG {code}")
            
            # Query base rates with column selection
            base_rate_result = self.db.query(
                IPPSBaseRate.operating_base,
                IPPSBaseRate.capital_base,
                IPPSBaseRate.release_id,
                IPPSBaseRate.batch_id
            ).filter(
                and_(
                    IPPSBaseRate.fy == fy,
                    IPPSBaseRate.effective_from <= f"{year}-12-31",
                    or_(
                        IPPSBaseRate.effective_to.is_(None),
                        IPPSBaseRate.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not base_rate_result:
                raise ValueError(f"No IPPS base rates found for FY {fy}")
            
            # Query wage index with column selection
            wage_index_result = self.db.query(
                WageIndex.wage_index,
                WageIndex.release_id,
                WageIndex.batch_id
            ).filter(
                and_(
                    WageIndex.year == year,
                    WageIndex.cbsa == cbsa,
                    WageIndex.quarter.is_(None)  # IPPS uses annual wage index
                )
            ).first()
            
            if not wage_index_result:
                raise ValueError(f"No wage index found for CBSA {cbsa}")
            
            # Extract values from Row results
            drg_weight = drg_result.weight
            drg_release_id = drg_result.release_id
            drg_batch_id = drg_result.batch_id
            
            operating_base = base_rate_result.operating_base
            capital_base = base_rate_result.capital_base
            base_release_id = base_rate_result.release_id
            base_batch_id = base_rate_result.batch_id
            
            wage_index = wage_index_result.wage_index
            wage_release_id = wage_index_result.release_id
            wage_batch_id = wage_index_result.batch_id
            
            # Calculate IPPS payment
            # Formula: DRG_weight × ((operating_base × WI) + (capital_base × WI))
            operating_component = operating_base * wage_index
            capital_component = capital_base * wage_index
            
            base_payment = drg_weight * (operating_component + capital_component)
            
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
            
            # Add IPPS DRG provenance (standardized format)
            if drg_release_id:
                trace_refs.append(f"{dataset_id}:release:{drg_release_id}")
            if drg_batch_id:
                trace_refs.append(f"{dataset_id}:batch:{drg_batch_id}")
            
            # Add base rate provenance if available
            if base_release_id:
                trace_refs.append(f"IPPSBaseRate:release:{base_release_id}")
            if base_batch_id:
                trace_refs.append(f"IPPSBaseRate:batch:{base_batch_id}")
            
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
                setting="IPPS",
                modifier=primary_modifier,
                allowed_cents=allowed_cents,
                beneficiary_deductible_cents=beneficiary_deductible_cents,
                beneficiary_coinsurance_cents=beneficiary_coinsurance_cents,
                beneficiary_total_cents=beneficiary_total_cents,
                program_payment_cents=program_payment_cents,
                professional_allowed_cents=0,  # IPPS is facility only
                facility_allowed_cents=allowed_cents if facility_component else 0,
                dataset_id=dataset_id,
                release_id=drg_release_id,
                batch_id=drg_batch_id,
                trace_refs=trace_refs,
                source="benchmark",
                facility_specific=False,
                packaged=False,
                units=units
            )
            
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
        """Clean up database session if we own it"""
        if hasattr(self, '_owns_session') and self._owns_session and hasattr(self, 'db'):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
