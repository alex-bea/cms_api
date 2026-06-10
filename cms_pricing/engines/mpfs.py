"""Medicare Physician Fee Schedule pricing engine"""

from typing import Any, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeMPFS, GPCI, ConversionFactor
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
import structlog

logger = structlog.get_logger()

# Dataset identifier constant (Phase 2.5)
DATASET_ID = "MPFS"


class MPSFEngine(BasePricingEngine):
    """Medicare Physician Fee Schedule pricing engine"""
    
    def __init__(self, db: Optional[Session] = None):
        """
        Initialize MPFS engine with optional database session.
        
        Args:
            db: Optional SQLAlchemy session. If None, creates a new session.
                Session will be closed when engine is destroyed.
        """
        self.db = db if db is not None else SessionLocal()
        self._owns_session = db is None
    
    @staticmethod
    def _build_mpfs_filter(year: int, code: str):
        """Build reusable filter expression for MPFS queries"""
        return and_(
            FeeMPFS.year == year,
            FeeMPFS.hcpcs == code,
            FeeMPFS.effective_from <= f"{year}-12-31",
            or_(
                FeeMPFS.effective_to.is_(None),
                FeeMPFS.effective_to >= f"{year}-01-01"
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
        """Price a code using MPFS (Quick Win #2: Returns unified CodePricingItem)"""

        locality_id = None
        try:
            # Get locality from geography
            if geography and geography.selected_candidate:
                locality_id = geography.selected_candidate.locality_id
            
            if not locality_id:
                raise ValueError("No locality found for ZIP code")
            
            # Pre-compute trace ref base (optimization)
            trace_refs = [
                f"mpfs_{year}_{locality_id}_{code}",
                f"gpci_{year}_{locality_id}",
                f"cf_{year}_MPFS"
            ]
            dataset_id = DATASET_ID
            
            # Query only necessary columns using with_entities (optimization)
            mpfs_result = self.db.query(
                FeeMPFS.work_rvu,
                FeeMPFS.pe_nf_rvu,
                FeeMPFS.pe_fac_rvu,
                FeeMPFS.mp_rvu,
                FeeMPFS.release_id,
                FeeMPFS.batch_id
            ).filter(
                self._build_mpfs_filter(year, code)
            ).first()
            
            if not mpfs_result:
                raise ValueError(f"No MPFS data found for code {code} for year {year}")
            
            # Query GPCI with column selection
            gpci_result = self.db.query(
                GPCI.gpci_work,
                GPCI.gpci_pe,
                GPCI.gpci_mp,
                GPCI.release_id,
                GPCI.batch_id
            ).filter(
                and_(
                    GPCI.year == year,
                    GPCI.locality_id == locality_id
                )
            ).first()
            
            if not gpci_result:
                raise ValueError(f"No GPCI data found for locality {locality_id}")
            
            # Query conversion factor with column selection
            cf_result = self.db.query(
                ConversionFactor.cf,
                ConversionFactor.release_id,
                ConversionFactor.batch_id
            ).filter(
                and_(
                    ConversionFactor.year == year,
                    ConversionFactor.source == "MPFS"
                )
            ).first()
            
            if not cf_result:
                raise ValueError(f"No conversion factor found for year {year}")
            
            required_values = {
                "work_rvu": mpfs_result.work_rvu,
                "mp_rvu": mpfs_result.mp_rvu,
                "gpci_work": gpci_result.gpci_work,
                "gpci_pe": gpci_result.gpci_pe,
                "gpci_mp": gpci_result.gpci_mp,
                "conversion_factor": cf_result.cf,
            }
            missing_values = [name for name, value in required_values.items() if value is None]
            if missing_values:
                raise ValueError(
                    f"Missing MPFS pricing inputs for code {code}, locality {locality_id}: "
                    f"{', '.join(missing_values)}"
                )

            # Extract values from Row results
            work_rvu = mpfs_result.work_rvu
            pe_nf_rvu = mpfs_result.pe_nf_rvu
            pe_fac_rvu = mpfs_result.pe_fac_rvu
            mp_rvu = mpfs_result.mp_rvu
            mpfs_release_id = mpfs_result.release_id
            mpfs_batch_id = mpfs_result.batch_id
            
            gpci_work = gpci_result.gpci_work
            gpci_pe = gpci_result.gpci_pe
            gpci_mp = gpci_result.gpci_mp
            gpci_release_id = gpci_result.release_id
            gpci_batch_id = gpci_result.batch_id
            
            cf = cf_result.cf
            cf_release_id = cf_result.release_id
            cf_batch_id = cf_result.batch_id
            
            # Determine PE RVU based on POS (need to create a minimal object for _get_pe_rvu)
            class MPFSData:
                def __init__(self):
                    self.pe_nf_rvu = pe_nf_rvu
                    self.pe_fac_rvu = pe_fac_rvu
            
            mpfs_data_obj = MPFSData()
            pe_rvu = self._get_pe_rvu(mpfs_data_obj, pos)

            if pe_rvu is None:
                raise ValueError(
                    f"Missing PE RVU for code {code}, locality {locality_id}, POS {pos or 'default'}"
                )
            
            # Apply GPCI
            work_rvu_adjusted = work_rvu * gpci_work
            pe_rvu_adjusted = pe_rvu * gpci_pe
            mp_rvu_adjusted = mp_rvu * gpci_mp
            
            professional_base, technical_base = self._select_component_amounts(
                work_rvu_adjusted=work_rvu_adjusted,
                pe_rvu_adjusted=pe_rvu_adjusted,
                mp_rvu_adjusted=mp_rvu_adjusted,
                conversion_factor=cf,
                professional_component=professional_component,
                facility_component=facility_component,
                modifiers=modifiers,
            )

            quantity_multiplier = units * utilization_weight
            professional_allowed_amount = professional_base * quantity_multiplier
            technical_allowed_amount = technical_base * quantity_multiplier
            allowed_amount = professional_allowed_amount + technical_allowed_amount
            
            # Calculate beneficiary cost sharing
            cost_sharing = self._calculate_beneficiary_cost_sharing(allowed_amount)
            
            # Convert to cents
            allowed_cents = self._amount_to_cents(allowed_amount)
            beneficiary_deductible_cents = self._amount_to_cents(cost_sharing["beneficiary_deductible"])
            beneficiary_coinsurance_cents = self._amount_to_cents(cost_sharing["beneficiary_coinsurance"])
            beneficiary_total_cents = self._amount_to_cents(cost_sharing["beneficiary_total"])
            program_payment_cents = self._amount_to_cents(cost_sharing["program_payment"])
            professional_allowed_cents = self._amount_to_cents(professional_allowed_amount)
            technical_allowed_cents = self._amount_to_cents(technical_allowed_amount)
            
            # Add MPFS provenance (standardized format)
            if mpfs_release_id:
                trace_refs.append(f"{dataset_id}:release:{mpfs_release_id}")
            if mpfs_batch_id:
                trace_refs.append(f"{dataset_id}:batch:{mpfs_batch_id}")
            
            # Add supporting data provenance if available
            if gpci_release_id:
                trace_refs.append(f"GPCI:release:{gpci_release_id}")
            if gpci_batch_id:
                trace_refs.append(f"GPCI:batch:{gpci_batch_id}")
            
            if cf_release_id:
                trace_refs.append(f"CF:release:{cf_release_id}")
            if cf_batch_id:
                trace_refs.append(f"CF:batch:{cf_batch_id}")
            
            # Filter out None values and deduplicate while preserving order
            trace_refs = list(dict.fromkeys([ref for ref in trace_refs if ref is not None]))
            
            # Extract primary modifier (if multiple, use first)
            primary_modifier = modifiers[0] if modifiers and len(modifiers) > 0 else None
            
            return CodePricingItem(
                code=code,
                setting="MPFS",
                modifier=primary_modifier,
                allowed_cents=allowed_cents,
                beneficiary_deductible_cents=beneficiary_deductible_cents,
                beneficiary_coinsurance_cents=beneficiary_coinsurance_cents,
                beneficiary_total_cents=beneficiary_total_cents,
                program_payment_cents=program_payment_cents,
                professional_allowed_cents=professional_allowed_cents,
                facility_allowed_cents=technical_allowed_cents,
                dataset_id=dataset_id,
                release_id=mpfs_release_id,
                batch_id=mpfs_batch_id,
                trace_refs=trace_refs,
                source="benchmark",
                facility_specific=False,
                packaged=False,
                units=units
            )
            
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
    
    def _get_pe_rvu(self, mpfs_data: Any, pos: Optional[str]) -> Optional[float]:
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

    def _select_component_amounts(
        self,
        *,
        work_rvu_adjusted: float,
        pe_rvu_adjusted: float,
        mp_rvu_adjusted: float,
        conversion_factor: float,
        professional_component: bool,
        facility_component: bool,
        modifiers: Optional[List[str]],
    ) -> Tuple[float, float]:
        """Return professional and technical MPFS base allowed amounts."""

        modifier_codes = [
            self._normalize_modifier_code(modifier)
            for modifier in modifiers or []
            if str(modifier).strip()
        ]
        has_professional_modifier = "26" in modifier_codes
        has_technical_modifier = "TC" in modifier_codes

        if has_professional_modifier and has_technical_modifier:
            raise ValueError("MPFS modifiers 26 and TC cannot both be applied")

        if has_professional_modifier:
            include_professional = True
            include_technical = False
        elif has_technical_modifier:
            include_professional = False
            include_technical = True
        else:
            include_professional = professional_component
            include_technical = facility_component

        if not include_professional and not include_technical:
            raise ValueError("At least one MPFS component must be selected")

        generic_modifiers = [
            modifier
            for modifier in modifiers or []
            if self._normalize_modifier_code(modifier) not in {"26", "TC"}
        ]

        professional_amount = (work_rvu_adjusted + mp_rvu_adjusted) * conversion_factor
        technical_amount = pe_rvu_adjusted * conversion_factor

        if include_professional:
            professional_amount = self._apply_modifiers(professional_amount, generic_modifiers)
        else:
            professional_amount = 0.0

        if include_technical:
            technical_amount = self._apply_modifiers(technical_amount, generic_modifiers)
        else:
            technical_amount = 0.0

        return professional_amount, technical_amount
    
    def __del__(self):
        """Clean up database session if we own it"""
        if hasattr(self, '_owns_session') and self._owns_session and hasattr(self, 'db'):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
