"""Outpatient Prospective Payment System pricing engine"""

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeOPPS, WageIndex
from cms_pricing.models.opps import OPPSAPCPayment, OPPSHCPCSCrosswalk
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService
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
        ndc11: Optional[str] = None,
        valuation_date: Optional[date] = None,
    ) -> CodePricingItem:
        """Price a code using OPPS (Quick Win #2: Returns unified CodePricingItem)"""
        
        try:
            source_result = await self._price_code_from_source_tables(
                code=code,
                year=year,
                quarter=quarter,
                valuation_date=valuation_date,
                modifiers=modifiers,
                units=units,
                utilization_weight=utilization_weight,
                facility_component=facility_component,
            )
            if source_result is not None:
                return source_result

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

    async def _price_code_from_source_tables(
        self,
        *,
        code: str,
        year: int,
        quarter: Optional[str],
        valuation_date: Optional[date],
        modifiers: Optional[List[str]],
        units: float,
        utilization_weight: float,
        facility_component: bool,
    ) -> Optional[CodePricingItem]:
        """Resolve OPPS pricing from normalized Addendum A/B source tables."""
        selected_snapshot = None
        if valuation_date is not None:
            selected_snapshot = DatasetSnapshotService(self.db).select_snapshot(
                DATASET_ID,
                valuation_date=valuation_date,
            )

        if selected_snapshot is not None:
            effective_date = valuation_date or selected_snapshot.effective_from
            release_id = selected_snapshot.release_id
            query_year = selected_snapshot.effective_from.year
            query_quarter = self._quarter_for_date(selected_snapshot.effective_from)
        else:
            effective_date = valuation_date or self._date_for_year_quarter(year, quarter)
            release_id = None
            query_year = year
            query_quarter = int(quarter) if quarter is not None else self._quarter_for_date(effective_date)

        modifier_values = [
            self._normalize_modifier_code(modifier)
            for modifier in (modifiers or [])
            if self._normalize_modifier_code(modifier)
        ]

        try:
            query = self.db.query(OPPSHCPCSCrosswalk).filter(
                OPPSHCPCSCrosswalk.year == query_year,
                OPPSHCPCSCrosswalk.quarter == query_quarter,
                OPPSHCPCSCrosswalk.hcpcs_code == code,
                OPPSHCPCSCrosswalk.effective_from <= effective_date,
                or_(
                    OPPSHCPCSCrosswalk.effective_to.is_(None),
                    OPPSHCPCSCrosswalk.effective_to >= effective_date,
                ),
            )
            if release_id:
                query = query.filter(OPPSHCPCSCrosswalk.release_id == release_id)
            candidates = query.all()
        except Exception as exc:
            logger.debug("OPPS source table lookup unavailable; falling back to legacy table", error=str(exc))
            return None

        if not candidates:
            return None

        crosswalk = self._select_crosswalk_candidate(candidates, modifier_values)
        if crosswalk is None:
            raise ValueError(f"No OPPS Addendum B row found for code {code} and modifiers {modifier_values}")

        status_indicator = crosswalk.status_indicator
        treatment = self._payment_treatment(status_indicator)
        if treatment == "unknown":
            raise ValueError(f"Unknown OPPS status indicator {status_indicator} for code {code}")

        trace_refs = [
            f"OPPS:hcpcs:{code}",
            f"OPPS:si:{status_indicator}",
            f"OPPS:release:{crosswalk.release_id}",
            f"OPPS:batch:{crosswalk.batch_id}",
        ]

        allowed_amount = Decimal("0")
        packaged = treatment in {"packaged", "context_required", "not_payable"}
        if treatment == "payable":
            if not crosswalk.apc_code:
                raise ValueError(f"Payable OPPS row for {code} has no APC code")
            apc_query = self.db.query(OPPSAPCPayment).filter(
                OPPSAPCPayment.year == crosswalk.year,
                OPPSAPCPayment.quarter == crosswalk.quarter,
                OPPSAPCPayment.apc_code == crosswalk.apc_code,
                OPPSAPCPayment.effective_from <= effective_date,
                or_(
                    OPPSAPCPayment.effective_to.is_(None),
                    OPPSAPCPayment.effective_to >= effective_date,
                ),
            )
            if release_id:
                apc_query = apc_query.filter(OPPSAPCPayment.release_id == release_id)
            apc_payment = apc_query.first()
            if not apc_payment:
                raise ValueError(f"No OPPS Addendum A APC rate found for APC {crosswalk.apc_code}")
            allowed_amount = Decimal(str(apc_payment.payment_rate_usd))
            allowed_amount *= Decimal(str(units)) * Decimal(str(utilization_weight))
            trace_refs.append(f"OPPS:apc:{crosswalk.apc_code}")
        elif treatment == "context_required":
            trace_refs.append("OPPS:packaging:context_required")

        cost_sharing = self._calculate_decimal_cost_sharing(allowed_amount)
        allowed_cents = self._decimal_to_cents(allowed_amount)
        primary_modifier = modifier_values[0] if modifier_values else None

        return CodePricingItem(
            code=code,
            setting="OPPS",
            modifier=primary_modifier,
            allowed_cents=allowed_cents,
            beneficiary_deductible_cents=self._decimal_to_cents(cost_sharing["beneficiary_deductible"]),
            beneficiary_coinsurance_cents=self._decimal_to_cents(cost_sharing["beneficiary_coinsurance"]),
            beneficiary_total_cents=self._decimal_to_cents(cost_sharing["beneficiary_total"]),
            program_payment_cents=self._decimal_to_cents(cost_sharing["program_payment"]),
            professional_allowed_cents=0,
            facility_allowed_cents=allowed_cents if facility_component else 0,
            dataset_id=DATASET_ID,
            release_id=crosswalk.release_id,
            batch_id=crosswalk.batch_id,
            trace_refs=list(dict.fromkeys([ref for ref in trace_refs if ref])),
            source="benchmark",
            facility_specific=False,
            packaged=packaged,
            units=units,
        )

    def _select_crosswalk_candidate(self, candidates, modifier_values: List[str]):
        if modifier_values:
            for candidate in candidates:
                if candidate.modifier and candidate.modifier.upper() in modifier_values:
                    return candidate
        for candidate in candidates:
            if not candidate.modifier:
                return candidate
        return candidates[0] if candidates else None

    def _payment_treatment(self, status_indicator: Optional[str]) -> str:
        if not status_indicator:
            return "unknown"
        if status_indicator in {"N"}:
            return "packaged"
        if status_indicator in {"Q1", "Q2", "Q3", "Q4", "J1", "J2"}:
            return "context_required"
        if status_indicator in {"E1", "E2", "M", "Y"}:
            return "not_payable"
        if status_indicator in {"A", "G", "K", "P", "R", "S", "S1", "T", "U", "V"}:
            return "payable"
        if status_indicator in {"B", "C", "F", "H", "H1", "K1", "L"}:
            return "packaged"
        return "unknown"

    @staticmethod
    def _date_for_year_quarter(year: int, quarter: Optional[str]) -> date:
        quarter_int = int(quarter) if quarter else 1
        month = {1: 1, 2: 4, 3: 7, 4: 10}[quarter_int]
        return date(year, month, 1)

    @staticmethod
    def _quarter_for_date(value: date) -> int:
        return ((value.month - 1) // 3) + 1

    def _calculate_decimal_cost_sharing(
        self,
        allowed_amount: Decimal,
        deductible_remaining: Decimal = Decimal("0"),
        coinsurance_rate: Decimal = Decimal("0.20"),
    ) -> Dict[str, Decimal]:
        deductible_applied = min(deductible_remaining, allowed_amount)
        remaining = allowed_amount - deductible_applied
        coinsurance = remaining * coinsurance_rate
        beneficiary_total = deductible_applied + coinsurance
        return {
            "beneficiary_deductible": deductible_applied,
            "beneficiary_coinsurance": coinsurance,
            "beneficiary_total": beneficiary_total,
            "program_payment": allowed_amount - beneficiary_total,
        }

    @staticmethod
    def _decimal_to_cents(amount: Decimal) -> int:
        return int((amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    
    def _is_packaged(self, status_indicator: Optional[str]) -> bool:
        """Check if item is packaged based on status indicator"""
        if not status_indicator:
            return False
        
        return self._payment_treatment(status_indicator) in {"packaged", "context_required"}
    
    def __del__(self):
        """Clean up database session if we own it"""
        if hasattr(self, '_owns_session') and self._owns_session and hasattr(self, 'db'):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
