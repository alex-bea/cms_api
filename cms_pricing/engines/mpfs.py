"""Medicare Physician Fee Schedule pricing engine"""

from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeMPFS, GPCI, ConversionFactor
from cms_pricing.schemas.geography import GeographyResolveResponse
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "MPFS"


class MPSFEngine(BasePricingEngine):
    """Medicare Physician Fee Schedule pricing engine"""
    
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
        """Price a code using MPFS"""
        
        try:
            # Get locality from geography
            locality_id = None
            if geography and geography.selected_candidate:
                locality_id = geography.selected_candidate.locality_id
            
            if not locality_id:
                raise ValueError("No locality found for ZIP code")
            
            # Get MPFS data (FeeMPFS contains base RVUs - no locality in the table itself)
            # Locality adjustment is applied via GPCI lookup
            mpfs_data = self.db.query(FeeMPFS).filter(
                and_(
                    FeeMPFS.year == year,
                    FeeMPFS.hcpcs == code,
                    FeeMPFS.effective_from <= f"{year}-12-31",
                    or_(
                        FeeMPFS.effective_to.is_(None),
                        FeeMPFS.effective_to >= f"{year}-01-01"
                    )
                )
            ).first()
            
            if not mpfs_data:
                raise ValueError(f"No MPFS data found for code {code} for year {year}")
            
            # Get GPCI
            gpci_data = self.db.query(GPCI).filter(
                and_(
                    GPCI.year == year,
                    GPCI.locality_id == locality_id
                )
            ).first()
            
            if not gpci_data:
                raise ValueError(f"No GPCI data found for locality {locality_id}")
            
            # Get conversion factor
            cf_data = self.db.query(ConversionFactor).filter(
                and_(
                    ConversionFactor.year == year,
                    ConversionFactor.source == "MPFS"
                )
            ).first()
            
            if not cf_data:
                raise ValueError(f"No conversion factor found for year {year}")
            
            # Determine PE RVU based on POS
            pe_rvu = self._get_pe_rvu(mpfs_data, pos)
            
            # Calculate RVUs
            work_rvu = mpfs_data.work_rvu or 0
            pe_rvu = pe_rvu or 0
            mp_rvu = mpfs_data.mp_rvu or 0
            
            # Apply GPCI
            work_rvu_adjusted = work_rvu * gpci_data.gpci_work
            pe_rvu_adjusted = pe_rvu * gpci_data.gpci_pe
            mp_rvu_adjusted = mp_rvu * gpci_data.gpci_mp
            
            # Calculate total RVUs
            total_rvu = work_rvu_adjusted + pe_rvu_adjusted + mp_rvu_adjusted
            
            # Apply conversion factor
            base_allowed = total_rvu * cf_data.cf
            
            # Apply modifiers
            if modifiers:
                base_allowed = self._apply_modifiers(base_allowed, modifiers)
            
            # Apply units and utilization weight
            allowed_amount = base_allowed * units * utilization_weight
            
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
                f"mpfs_{year}_{locality_id}_{code}",
                f"gpci_{year}_{locality_id}",
                f"cf_{year}_MPFS"
            ]
            
            # Add MPFS provenance (standardized format)
            if mpfs_data.release_id:
                trace_refs.append(f"{dataset_id}:release:{mpfs_data.release_id}")
            if mpfs_data.batch_id:
                trace_refs.append(f"{dataset_id}:batch:{mpfs_data.batch_id}")
            
            # Add supporting data provenance if available
            if gpci_data.release_id:
                trace_refs.append(f"GPCI:release:{gpci_data.release_id}")
            if gpci_data.batch_id:
                trace_refs.append(f"GPCI:batch:{gpci_data.batch_id}")
            
            if cf_data.release_id:
                trace_refs.append(f"CF:release:{cf_data.release_id}")
            if cf_data.batch_id:
                trace_refs.append(f"CF:batch:{cf_data.batch_id}")
            
            # Filter out None values and deduplicate while preserving order
            trace_refs = list(dict.fromkeys([ref for ref in trace_refs if ref is not None]))
            
            return {
                "allowed_cents": allowed_cents,
                "beneficiary_deductible_cents": beneficiary_deductible_cents,
                "beneficiary_coinsurance_cents": beneficiary_coinsurance_cents,
                "beneficiary_total_cents": beneficiary_total_cents,
                "program_payment_cents": program_payment_cents,
                "professional_allowed_cents": allowed_cents if professional_component else 0,
                "facility_allowed_cents": 0,  # MPFS is professional only
                "source": "benchmark",
                "facility_specific": False,
                "packaged": False,
                "trace_refs": trace_refs,
                # Direct provenance fields (Phase 2.5)
                "release_id": mpfs_data.release_id,
                "batch_id": mpfs_data.batch_id,
                "dataset_id": dataset_id
            }
            
        except Exception as e:
            logger.error(
                "MPFS pricing failed",
                code=code,
                zip=zip,
                year=year,
                locality_id=locality_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    def _get_pe_rvu(self, mpfs_data: FeeMPFS, pos: Optional[str]) -> Optional[float]:
        """Get appropriate PE RVU based on place of service"""
        
        if not pos:
            # Default to facility PE RVU
            return mpfs_data.pe_fac_rvu
        
        # POS mapping logic
        if pos in ["11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"]:
            # Office/clinic settings - use non-facility PE RVU
            return mpfs_data.pe_nf_rvu
        else:
            # Facility settings - use facility PE RVU
            return mpfs_data.pe_fac_rvu
    
    def __del__(self):
        """Clean up database session"""
        if hasattr(self, 'db'):
            self.db.close()
