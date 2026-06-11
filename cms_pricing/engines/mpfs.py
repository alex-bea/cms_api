"""Medicare Physician Fee Schedule pricing engine"""

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from cms_pricing.engines.base import BasePricingEngine
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeMPFS, GPCI, ConversionFactor
from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.models.rvu import GPCIIndex, Release, RVUItem
from cms_pricing.schemas.geography import GeographyResolveResponse
from cms_pricing.schemas.pricing import CodePricingItem
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService
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
                FeeMPFS.effective_to.is_(None), FeeMPFS.effective_to >= f"{year}-01-01"
            ),
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
        """Price a code using MPFS (Quick Win #2: Returns unified CodePricingItem)"""

        locality_id = None
        try:
            selected_valuation_date = self._valuation_date_from_inputs(
                year=year,
                quarter=quarter,
                valuation_date=valuation_date,
            )

            # Get locality from geography
            if geography and geography.selected_candidate:
                locality_id = geography.selected_candidate.locality_id

            if not locality_id:
                raise ValueError("No locality found for ZIP code")

            rvu_result = await self._price_code_from_rvu_snapshots(
                code=code,
                locality_id=locality_id,
                year=year,
                valuation_date=selected_valuation_date,
                geography=geography,
                units=units,
                utilization_weight=utilization_weight,
                professional_component=professional_component,
                facility_component=facility_component,
                modifiers=modifiers,
                pos=pos,
            )
            if rvu_result is not None:
                return rvu_result

            # Pre-compute trace ref base (optimization)
            trace_refs = [
                f"mpfs_{year}_{locality_id}_{code}",
                f"gpci_{year}_{locality_id}",
                f"cf_{year}_MPFS",
            ]
            dataset_id = DATASET_ID

            # Query only necessary columns using with_entities (optimization)
            mpfs_result = (
                self.db.query(
                    FeeMPFS.work_rvu,
                    FeeMPFS.pe_nf_rvu,
                    FeeMPFS.pe_fac_rvu,
                    FeeMPFS.mp_rvu,
                    FeeMPFS.release_id,
                    FeeMPFS.batch_id,
                )
                .filter(self._build_mpfs_filter(year, code))
                .first()
            )

            if not mpfs_result:
                raise ValueError(f"No MPFS data found for code {code} for year {year}")

            # Query GPCI with column selection
            gpci_result = (
                self.db.query(
                    GPCI.gpci_work,
                    GPCI.gpci_pe,
                    GPCI.gpci_mp,
                    GPCI.release_id,
                    GPCI.batch_id,
                )
                .filter(and_(GPCI.year == year, GPCI.locality_id == locality_id))
                .first()
            )

            if not gpci_result:
                raise ValueError(f"No GPCI data found for locality {locality_id}")

            # Query conversion factor with column selection
            cf_result = (
                self.db.query(
                    ConversionFactor.cf,
                    ConversionFactor.release_id,
                    ConversionFactor.batch_id,
                )
                .filter(
                    and_(
                        ConversionFactor.year == year, ConversionFactor.source == "MPFS"
                    )
                )
                .first()
            )

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
            missing_values = [
                name for name, value in required_values.items() if value is None
            ]
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
            beneficiary_deductible_cents = self._amount_to_cents(
                cost_sharing["beneficiary_deductible"]
            )
            beneficiary_coinsurance_cents = self._amount_to_cents(
                cost_sharing["beneficiary_coinsurance"]
            )
            beneficiary_total_cents = self._amount_to_cents(
                cost_sharing["beneficiary_total"]
            )
            program_payment_cents = self._amount_to_cents(
                cost_sharing["program_payment"]
            )
            professional_allowed_cents = self._amount_to_cents(
                professional_allowed_amount
            )
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
            trace_refs = list(
                dict.fromkeys([ref for ref in trace_refs if ref is not None])
            )

            # Extract primary modifier (if multiple, use first)
            primary_modifier = (
                modifiers[0] if modifiers and len(modifiers) > 0 else None
            )

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
                units=units,
            )

        except Exception as e:
            logger.error(
                "MPFS pricing failed",
                code=code,
                zip=zip,
                year=year,
                locality_id=locality_id,
                error=str(e),
                exc_info=True,
            )
            raise

    @staticmethod
    def _valuation_date_from_inputs(
        *,
        year: int,
        quarter: Optional[str],
        valuation_date: Optional[date],
    ) -> date:
        if valuation_date is not None:
            return valuation_date

        if quarter:
            quarter_starts = {
                "1": (1, 1),
                "2": (4, 1),
                "3": (7, 1),
                "4": (10, 1),
            }
            month, day = quarter_starts[str(quarter)]
            return date(year, month, day)

        return date(year, 12, 31)

    async def _price_code_from_rvu_snapshots(
        self,
        *,
        code: str,
        locality_id: str,
        year: int,
        valuation_date: date,
        geography: Optional[GeographyResolveResponse],
        units: float,
        utilization_weight: float,
        professional_component: bool,
        facility_component: bool,
        modifiers: Optional[List[str]],
        pos: Optional[str],
    ) -> Optional[CodePricingItem]:
        snapshots = self._select_rvu_pricing_snapshots(valuation_date)
        if snapshots is None:
            return None

        rvu_snapshot, gpci_snapshot = snapshots
        if not self._snapshot_release_keys_match(rvu_snapshot, gpci_snapshot):
            raise ValueError(
                "Selected RVU and GPCI snapshots do not describe the same CMS release: "
                f"{rvu_snapshot.release_id} vs {gpci_snapshot.release_id}"
            )

        release = self._release_for_rvu_snapshot(rvu_snapshot)
        if release is None:
            raise ValueError(
                f"No RVU database release found for selected snapshot {rvu_snapshot.release_id}"
            )

        rvu_row = self._query_rvu_item(release.id, code)
        if rvu_row is None:
            raise ValueError(
                f"No RVU data found for code {code} in selected snapshot {rvu_snapshot.release_id}"
            )

        gpci_row = self._query_gpci_index(
            release_id=release.id,
            locality_id=locality_id,
            geography=geography,
        )
        if gpci_row is None:
            raise ValueError(
                f"No GPCI data found for locality {locality_id} in selected snapshot {gpci_snapshot.release_id}"
            )

        work_rvu = self._required_decimal(
            rvu_row.work_rvu, "work_rvu", code, locality_id
        )
        pe_nf_rvu = self._optional_decimal(rvu_row.pe_rvu_nonfac)
        pe_fac_rvu = self._optional_decimal(rvu_row.pe_rvu_fac)
        mp_rvu = self._required_decimal(rvu_row.mp_rvu, "mp_rvu", code, locality_id)
        conversion_factor = self._required_decimal(
            rvu_row.conversion_factor,
            "conversion_factor",
            code,
            locality_id,
        )

        gpci_work = self._required_decimal(
            gpci_row.work_gpci, "work_gpci", code, locality_id
        )
        gpci_pe = self._required_decimal(gpci_row.pe_gpci, "pe_gpci", code, locality_id)
        gpci_mp = self._required_decimal(gpci_row.mp_gpci, "mp_gpci", code, locality_id)

        pe_rvu = self._select_pe_rvu_decimal(
            pe_nonfac_rvu=pe_nf_rvu,
            pe_fac_rvu=pe_fac_rvu,
            pos=pos,
            code=code,
            locality_id=locality_id,
        )

        work_rvu_adjusted = work_rvu * gpci_work
        pe_rvu_adjusted = pe_rvu * gpci_pe
        mp_rvu_adjusted = mp_rvu * gpci_mp

        professional_base, technical_base = self._select_component_amounts_decimal(
            work_rvu_adjusted=work_rvu_adjusted,
            pe_rvu_adjusted=pe_rvu_adjusted,
            mp_rvu_adjusted=mp_rvu_adjusted,
            conversion_factor=conversion_factor,
            professional_component=professional_component,
            facility_component=facility_component,
            modifiers=modifiers,
        )

        quantity_multiplier = Decimal(str(units)) * Decimal(str(utilization_weight))
        professional_allowed_amount = professional_base * quantity_multiplier
        technical_allowed_amount = technical_base * quantity_multiplier
        allowed_amount = professional_allowed_amount + technical_allowed_amount
        cost_sharing = self._calculate_beneficiary_cost_sharing_decimal(allowed_amount)

        primary_modifier = modifiers[0] if modifiers and len(modifiers) > 0 else None
        trace_refs = list(
            dict.fromkeys(
                [
                    f"mpfs_rvu_{valuation_date.isoformat()}_{locality_id}_{code}",
                    f"rvu_{rvu_snapshot.release_id}_{code}",
                    f"gpci_{gpci_snapshot.release_id}_{locality_id}",
                    f"cf_{rvu_snapshot.release_id}_rvu_items",
                    f"{DATASET_ID}:release:{rvu_snapshot.release_id}",
                    f"RVU:release:{rvu_snapshot.release_id}",
                    f"GPCI:release:{gpci_snapshot.release_id}",
                    f"CF:release:{rvu_snapshot.release_id}",
                    "CF:source:rvu_items.conversion_factor",
                ]
            )
        )

        batch_id = str(release.notes) if release.notes else None
        if batch_id:
            trace_refs.append(f"{DATASET_ID}:batch:{batch_id}")

        return CodePricingItem(
            code=code,
            setting="MPFS",
            modifier=primary_modifier,
            allowed_cents=self._decimal_to_cents(allowed_amount),
            beneficiary_deductible_cents=self._decimal_to_cents(
                cost_sharing["beneficiary_deductible"]
            ),
            beneficiary_coinsurance_cents=self._decimal_to_cents(
                cost_sharing["beneficiary_coinsurance"]
            ),
            beneficiary_total_cents=self._decimal_to_cents(
                cost_sharing["beneficiary_total"]
            ),
            program_payment_cents=self._decimal_to_cents(
                cost_sharing["program_payment"]
            ),
            professional_allowed_cents=self._decimal_to_cents(
                professional_allowed_amount
            ),
            facility_allowed_cents=self._decimal_to_cents(technical_allowed_amount),
            dataset_id=DATASET_ID,
            release_id=rvu_snapshot.release_id,
            batch_id=batch_id,
            trace_refs=trace_refs,
            source="benchmark",
            facility_specific=False,
            packaged=False,
            units=units,
        )

    def _select_rvu_pricing_snapshots(
        self,
        valuation_date: date,
    ) -> Optional[Tuple[DatasetSnapshot, DatasetSnapshot]]:
        if not isinstance(self.db, Session):
            return None

        snapshot_service = DatasetSnapshotService(self.db)
        rvu_snapshot = snapshot_service.select_snapshot(
            "rvu_items",
            valuation_date=valuation_date,
        )
        gpci_snapshot = snapshot_service.select_snapshot(
            "gpci_indices",
            valuation_date=valuation_date,
        )

        if rvu_snapshot is None and gpci_snapshot is None:
            return None
        if rvu_snapshot is None or gpci_snapshot is None:
            raise ValueError(
                "Partial RVU pricing snapshot state: both rvu_items and gpci_indices "
                f"must be registered for {valuation_date.isoformat()}"
            )
        if not isinstance(rvu_snapshot, DatasetSnapshot) or not isinstance(
            gpci_snapshot, DatasetSnapshot
        ):
            return None
        return rvu_snapshot, gpci_snapshot

    @staticmethod
    def _snapshot_release_key(snapshot: DatasetSnapshot) -> Optional[str]:
        parts = str(snapshot.release_id or "").split("_", 1)
        if len(parts) != 2:
            return None
        return parts[1]

    @classmethod
    def _snapshot_release_keys_match(
        cls,
        rvu_snapshot: DatasetSnapshot,
        gpci_snapshot: DatasetSnapshot,
    ) -> bool:
        rvu_key = cls._snapshot_release_key(rvu_snapshot)
        gpci_key = cls._snapshot_release_key(gpci_snapshot)
        return bool(rvu_key and gpci_key and rvu_key == gpci_key)

    def _release_for_rvu_snapshot(self, snapshot: DatasetSnapshot) -> Optional[Release]:
        source_version = str(snapshot.release_id or "")[:10]
        if not source_version:
            return None
        return (
            self.db.query(Release)
            .filter(
                Release.type == "RVU_FULL",
                Release.source_version == source_version,
            )
            .order_by(Release.imported_at.desc())
            .first()
        )

    def _query_rvu_item(self, release_id: Any, code: str) -> Optional[Any]:
        return (
            self.db.query(
                RVUItem.work_rvu,
                RVUItem.pe_rvu_nonfac,
                RVUItem.pe_rvu_fac,
                RVUItem.mp_rvu,
                RVUItem.conversion_factor,
            )
            .filter(
                RVUItem.release_id == release_id,
                RVUItem.hcpcs_code == code,
                or_(RVUItem.modifier_key.is_(None), RVUItem.modifier_key == ""),
            )
            .first()
        )

    def _query_gpci_index(
        self,
        *,
        release_id: Any,
        locality_id: str,
        geography: Optional[GeographyResolveResponse],
    ) -> Optional[Any]:
        locality_candidates = self._locality_candidates(locality_id)
        state_code = None
        if geography and geography.selected_candidate:
            state_code = geography.selected_candidate.state_code

        query = self.db.query(
            GPCIIndex.work_gpci,
            GPCIIndex.pe_gpci,
            GPCIIndex.mp_gpci,
        ).filter(
            GPCIIndex.release_id == release_id,
            GPCIIndex.locality_id.in_(locality_candidates),
        )
        if state_code:
            query = query.filter(GPCIIndex.state == state_code)

        row = query.order_by(GPCIIndex.locality_id.asc()).first()
        if row or not state_code:
            return row

        return (
            self.db.query(
                GPCIIndex.work_gpci,
                GPCIIndex.pe_gpci,
                GPCIIndex.mp_gpci,
            )
            .filter(
                GPCIIndex.release_id == release_id,
                GPCIIndex.locality_id.in_(locality_candidates),
            )
            .order_by(GPCIIndex.locality_id.asc())
            .first()
        )

    @staticmethod
    def _locality_candidates(locality_id: str) -> List[str]:
        raw = str(locality_id or "").strip()
        candidates = [raw]
        if raw.isdigit():
            candidates.append(raw.zfill(2))
        return list(dict.fromkeys(candidate for candidate in candidates if candidate))

    @staticmethod
    def _optional_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        return Decimal(str(value))

    @classmethod
    def _required_decimal(
        cls,
        value: Any,
        field_name: str,
        code: str,
        locality_id: str,
    ) -> Decimal:
        converted = cls._optional_decimal(value)
        if converted is None:
            raise ValueError(
                f"Missing MPFS pricing input {field_name} for code {code}, locality {locality_id}"
            )
        return converted

    @staticmethod
    def _select_pe_rvu_decimal(
        *,
        pe_nonfac_rvu: Optional[Decimal],
        pe_fac_rvu: Optional[Decimal],
        pos: Optional[str],
        code: str,
        locality_id: str,
    ) -> Decimal:
        use_nonfacility = bool(
            pos
            and pos
            in ["11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"]
        )
        selected = pe_nonfac_rvu if use_nonfacility else pe_fac_rvu
        if selected is None:
            setting = "non-facility" if use_nonfacility else "facility"
            raise ValueError(
                f"Missing {setting} PE RVU for code {code}, locality {locality_id}, POS {pos or 'default'}"
            )
        return selected

    def _select_component_amounts_decimal(
        self,
        *,
        work_rvu_adjusted: Decimal,
        pe_rvu_adjusted: Decimal,
        mp_rvu_adjusted: Decimal,
        conversion_factor: Decimal,
        professional_component: bool,
        facility_component: bool,
        modifiers: Optional[List[str]],
    ) -> Tuple[Decimal, Decimal]:
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
            professional_amount = self._apply_modifiers_decimal(
                professional_amount,
                generic_modifiers,
            )
        else:
            professional_amount = Decimal("0")

        if include_technical:
            technical_amount = self._apply_modifiers_decimal(
                technical_amount,
                generic_modifiers,
            )
        else:
            technical_amount = Decimal("0")

        return professional_amount, technical_amount

    def _apply_modifiers_decimal(
        self,
        base_amount: Decimal,
        modifiers: List[str],
    ) -> Decimal:
        amount = base_amount
        for modifier in modifiers:
            modifier_code = self._normalize_modifier_code(modifier)
            if modifier_code == "50":
                amount *= Decimal("1.5")
            elif modifier_code == "51":
                amount *= Decimal("0.5")
        return amount

    @staticmethod
    def _calculate_beneficiary_cost_sharing_decimal(
        allowed_amount: Decimal,
        deductible_remaining: Decimal = Decimal("0"),
        coinsurance_rate: Decimal = Decimal("0.20"),
    ) -> dict[str, Decimal]:
        deductible_applied = min(deductible_remaining, allowed_amount)
        remaining_after_deductible = allowed_amount - deductible_applied
        coinsurance = remaining_after_deductible * coinsurance_rate
        beneficiary_total = deductible_applied + coinsurance
        program_payment = allowed_amount - beneficiary_total
        return {
            "beneficiary_deductible": deductible_applied,
            "beneficiary_coinsurance": coinsurance,
            "beneficiary_total": beneficiary_total,
            "program_payment": program_payment,
        }

    @staticmethod
    def _decimal_to_cents(amount: Decimal) -> int:
        return int(
            (amount * Decimal("100")).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_UP,
            )
        )

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
            professional_amount = self._apply_modifiers(
                professional_amount, generic_modifiers
            )
        else:
            professional_amount = 0.0

        if include_technical:
            technical_amount = self._apply_modifiers(
                technical_amount, generic_modifiers
            )
        else:
            technical_amount = 0.0

        return professional_amount, technical_amount

    def __del__(self):
        """Clean up database session if we own it"""
        if (
            hasattr(self, "_owns_session")
            and self._owns_session
            and hasattr(self, "db")
        ):
            try:
                self.db.close()
            except Exception:
                pass  # Ignore errors during cleanup
