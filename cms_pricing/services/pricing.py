"""Pricing service for calculating Medicare rates"""

import uuid
from typing import Dict, Any, List, Optional
from datetime import date

from cms_pricing.schemas.pricing import (
    PricingRequest, PricingResponse, ComparisonRequest, ComparisonResponse,
    LineItemResponse, GeographyResponse, ComparisonDelta
)
from cms_pricing.schemas.geography import GeographyCandidate
from cms_pricing.services.geography import GeographyService
from cms_pricing.services.trace import TraceService
from sqlalchemy.orm import Session
from cms_pricing.models.plans import Plan, PlanComponent
from cms_pricing.engines.mpfs import MPSFEngine
from cms_pricing.engines.opps import OPPSEngine
from cms_pricing.engines.asc import ASCEngine
from cms_pricing.engines.ipps import IPPSEngine
from cms_pricing.engines.clfs import CLFSEngine
from cms_pricing.engines.dmepos import DMEPOSEngine
from cms_pricing.engines.drugs import DrugEngine
import structlog

logger = structlog.get_logger()


class PricingService:
    """Main pricing service"""
    
    def __init__(self, db: Session = None):
        self.db = db
        self.geography_service = GeographyService(db)
        self.trace_service = TraceService(db)
        self._plan_name_cache: Dict[Any, str] = {}
        self.engines = {
            'MPFS': MPSFEngine(),
            'OPPS': OPPSEngine(),
            'ASC': ASCEngine(),
            'IPPS': IPPSEngine(),
            'CLFS': CLFSEngine(),
            'DMEPOS': DMEPOSEngine(),
            'DRUGS': DrugEngine()
        }
    
    async def price_single_code(
        self,
        zip: str,
        code: str,
        setting: str,
        year: int,
        quarter: Optional[str] = None,
        ccn: Optional[str] = None,
        payer: Optional[str] = None,
        plan: Optional[str] = None
    ) -> Dict[str, Any]:
        """Price a single code/component"""
        
        run_id = str(uuid.uuid4())
        
        try:
            # Resolve geography
            geography_result = await self.geography_service.resolve_zip(zip)
            
            # Get appropriate engine
            engine = self.engines.get(setting)
            if not engine:
                raise ValueError(f"Unknown setting: {setting}")
            
            # Price the code
            result = await engine.price_code(
                code=code,
                zip=zip,
                year=year,
                quarter=quarter,
                geography=geography_result,
                ccn=ccn,
                payer=payer,
                plan=plan
            )
            
            # Add geography info
            result['geography'] = geography_result.dict()
            result['run_id'] = run_id
            
            return result
            
        except Exception as e:
            logger.error(
                "Single code pricing failed",
                run_id=run_id,
                zip=zip,
                code=code,
                setting=setting,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def price_plan(self, request: PricingRequest) -> PricingResponse:
        """Price a complete treatment plan"""
        
        run_id = str(uuid.uuid4())
        
        try:
            # Resolve geography
            geography_result = await self.geography_service.resolve_zip(request.zip)
            
            # Get plan components
            if request.plan_id:
                # Load from database
                components = await self._load_plan_components(request.plan_id)
                plan_name = await self._get_plan_name(request.plan_id)
            else:
                # Use ad-hoc plan
                ad_hoc_plan = request.ad_hoc_plan or {}
                components = self._normalize_ad_hoc_components(
                    ad_hoc_plan.get('components', [])
                )
                plan_name = ad_hoc_plan.get('name', 'Ad-hoc Plan')
            
            # Price each component
            line_items = []
            total_allowed_cents = 0
            total_beneficiary_deductible_cents = 0
            total_beneficiary_coinsurance_cents = 0
            total_beneficiary_cents = 0
            total_program_payment_cents = 0
            
            for i, component in enumerate(components):
                # Get appropriate engine
                engine = self.engines.get(component['setting'])
                if not engine:
                    logger.warning(
                        "Unknown setting for component",
                        run_id=run_id,
                        code=component['code'],
                        setting=component['setting']
                    )
                    continue
                
                # Price the component
                result = await engine.price_code(
                    code=component['code'],
                    zip=request.zip,
                    year=request.year,
                    quarter=request.quarter,
                    geography=geography_result,
                    ccn=request.ccn,
                    payer=request.payer,
                    plan=request.plan,
                    units=component['units'],
                    utilization_weight=component['utilization_weight'],
                    professional_component=component['professional_component'],
                    facility_component=component['facility_component'],
                    modifiers=component['modifiers'],
                    pos=component['pos'],
                    ndc11=component['ndc11']
                )

                # Create line item response
                line_item = LineItemResponse(
                    sequence=i + 1,
                    code=component['code'],
                    setting=component['setting'],
                    units=component['units'],
                    utilization_weight=component['utilization_weight'],
                    allowed_cents=result['allowed_cents'],
                    beneficiary_deductible_cents=result.get('beneficiary_deductible_cents', 0),
                    beneficiary_coinsurance_cents=result.get('beneficiary_coinsurance_cents', 0),
                    beneficiary_total_cents=result.get('beneficiary_total_cents', 0),
                    program_payment_cents=result.get('program_payment_cents', 0),
                    professional_allowed_cents=result.get('professional_allowed_cents'),
                    facility_allowed_cents=result.get('facility_allowed_cents'),
                    source=result.get('source', 'benchmark'),
                    facility_specific=result.get('facility_specific', False),
                    packaged=result.get('packaged', False),
                    reference_price_cents=result.get('reference_price_cents'),
                    unit_conversion=result.get('unit_conversion'),
                    trace_refs=result.get('trace_refs', [])
                )
                
                line_items.append(line_item)
                
                # Accumulate totals
                total_allowed_cents += result['allowed_cents']
                total_beneficiary_deductible_cents += result.get('beneficiary_deductible_cents', 0)
                total_beneficiary_coinsurance_cents += result.get('beneficiary_coinsurance_cents', 0)
                total_beneficiary_cents += result.get('beneficiary_total_cents', 0)
                total_program_payment_cents += result.get('program_payment_cents', 0)
            
            # Create geography response
            geography_response = GeographyResponse(
                zip5=geography_result.zip5,
                locality_id=geography_result.selected_candidate.locality_id if geography_result.selected_candidate else None,
                locality_name=geography_result.selected_candidate.locality_name if geography_result.selected_candidate else None,
                cbsa=geography_result.selected_candidate.cbsa if geography_result.selected_candidate else None,
                cbsa_name=geography_result.selected_candidate.cbsa_name if geography_result.selected_candidate else None,
                county_fips=geography_result.selected_candidate.county_fips if geography_result.selected_candidate else None,
                state_code=geography_result.selected_candidate.state_code if geography_result.selected_candidate else None,
                rural_flag=geography_result.selected_candidate.rural_flag if geography_result.selected_candidate else None,
                resolution_method=geography_result.resolution_method,
                candidates=geography_result.candidates
            )
            
            # Create response
            response = PricingResponse(
                run_id=run_id,
                plan_id=request.plan_id,
                plan_name=plan_name,
                geography=geography_response,
                line_items=line_items,
                total_allowed_cents=total_allowed_cents,
                total_beneficiary_deductible_cents=total_beneficiary_deductible_cents,
                total_beneficiary_coinsurance_cents=total_beneficiary_coinsurance_cents,
                total_beneficiary_cents=total_beneficiary_cents,
                total_program_payment_cents=total_program_payment_cents,
                remaining_part_b_deductible_cents=0,  # TODO: Calculate remaining deductible
                post_acute_included=request.include_home_health or request.include_snf,
                sequestration_applied=request.apply_sequestration,
                facility_specific_used=any(item.facility_specific for item in line_items),
                datasets_used=[],  # TODO: Collect dataset information
                warnings=geography_result.warnings
            )
            
            # Store trace
            await self.trace_service.store_run(
                run_id=run_id,
                endpoint="/pricing/price",
                request_data=request.dict(),
                response_data=response.dict(),
                status="success"
            )
            
            return response
            
        except Exception as e:
            logger.error(
                "Plan pricing failed",
                run_id=run_id,
                request=request.dict(),
                error=str(e),
                exc_info=True
            )
            
            # Store error trace
            await self.trace_service.store_run(
                run_id=run_id,
                endpoint="/pricing/price",
                request_data=request.dict(),
                response_data=None,
                status="error",
                error_message=str(e)
            )
            
            raise
    
    async def compare_locations(self, request: ComparisonRequest) -> ComparisonResponse:
        """Compare pricing between two locations"""
        
        run_id = str(uuid.uuid4())
        
        try:
            # Price location A
            request_a = PricingRequest(
                zip=request.zip_a,
                plan_id=request.plan_id,
                year=request.year,
                quarter=request.quarter,
                ccn=request.ccn_a,
                payer=request.payer,
                plan=request.plan,
                include_home_health=request.include_home_health,
                include_snf=request.include_snf,
                apply_sequestration=request.apply_sequestration,
                sequestration_rate=request.sequestration_rate,
                format=request.format,
                ad_hoc_plan=request.ad_hoc_plan
            )
            
            result_a = await self.price_plan(request_a)
            
            # Price location B
            request_b = PricingRequest(
                zip=request.zip_b,
                plan_id=request.plan_id,
                year=request.year,
                quarter=request.quarter,
                ccn=request.ccn_b,
                payer=request.payer,
                plan=request.plan,
                include_home_health=request.include_home_health,
                include_snf=request.include_snf,
                apply_sequestration=request.apply_sequestration,
                sequestration_rate=request.sequestration_rate,
                format=request.format,
                ad_hoc_plan=request.ad_hoc_plan
            )
            
            result_b = await self.price_plan(request_b)
            
            # Validate parity
            parity_report = self._validate_parity(request_a, request_b, result_a, result_b)
            
            # Calculate deltas
            deltas = self._calculate_deltas(result_a, result_b)
            
            # Create comparison response
            response = ComparisonResponse(
                run_id=run_id,
                plan_id=request.plan_id,
                plan_name=result_a.plan_name,
                location_a=result_a,
                location_b=result_b,
                deltas=deltas,
                parity_report=parity_report,
                total_delta_cents=result_b.total_allowed_cents - result_a.total_allowed_cents,
                total_delta_percent=self._calculate_percentage_delta(
                    result_a.total_allowed_cents,
                    result_b.total_allowed_cents
                )
            )
            
            # Store trace
            await self.trace_service.store_run(
                run_id=run_id,
                endpoint="/pricing/compare",
                request_data=request.dict(),
                response_data=response.dict(),
                status="success"
            )
            
            return response
            
        except Exception as e:
            logger.error(
                "Location comparison failed",
                run_id=run_id,
                request=request.dict(),
                error=str(e),
                exc_info=True
            )
            
            # Store error trace
            await self.trace_service.store_run(
                run_id=run_id,
                endpoint="/pricing/compare",
                request_data=request.dict(),
                response_data=None,
                status="error",
                error_message=str(e)
            )
            
            raise
    
    def _validate_parity(
        self,
        request_a: PricingRequest,
        request_b: PricingRequest,
        result_a: PricingResponse,
        result_b: PricingResponse
    ) -> Dict[str, Any]:
        """Validate parity between two pricing requests"""
        
        parity_report = {
            "valid": True,
            "violations": [],
            "snapshots_match": True,
            "benefits_match": True,
            "toggles_match": True,
            "plan_match": True
        }
        
        # Check snapshot parity
        if result_a.datasets_used != result_b.datasets_used:
            parity_report["valid"] = False
            parity_report["violations"].append("Dataset snapshots differ")
            parity_report["snapshots_match"] = False
        
        # Check benefit parity
        if (request_a.include_home_health != request_b.include_home_health or
            request_a.include_snf != request_b.include_snf):
            parity_report["valid"] = False
            parity_report["violations"].append("Post-acute settings differ")
            parity_report["benefits_match"] = False
        
        # Check toggle parity
        if (request_a.apply_sequestration != request_b.apply_sequestration or
            request_a.sequestration_rate != request_b.sequestration_rate):
            parity_report["valid"] = False
            parity_report["violations"].append("Policy toggles differ")
            parity_report["toggles_match"] = False
        
        # Check plan parity
        if request_a.plan_id != request_b.plan_id:
            parity_report["valid"] = False
            parity_report["violations"].append("Plan IDs differ")
            parity_report["plan_match"] = False
        
        return parity_report
    
    def _calculate_deltas(self, result_a: PricingResponse, result_b: PricingResponse) -> List[ComparisonDelta]:
        """Calculate deltas between two pricing results"""
        
        deltas = []
        
        # Total deltas
        deltas.append(ComparisonDelta(
            field="total_allowed",
            location_a=result_a.total_allowed_cents,
            location_b=result_b.total_allowed_cents,
            delta_cents=result_b.total_allowed_cents - result_a.total_allowed_cents,
            delta_percent=self._calculate_percentage_delta(
                result_a.total_allowed_cents,
                result_b.total_allowed_cents
            )
        ))
        
        deltas.append(ComparisonDelta(
            field="total_beneficiary",
            location_a=result_a.total_beneficiary_cents,
            location_b=result_b.total_beneficiary_cents,
            delta_cents=result_b.total_beneficiary_cents - result_a.total_beneficiary_cents,
            delta_percent=self._calculate_percentage_delta(
                result_a.total_beneficiary_cents,
                result_b.total_beneficiary_cents
            )
        ))
        
        return deltas
    
    def _calculate_percentage_delta(self, value_a: int, value_b: int) -> float:
        """Calculate percentage delta between two values"""
        if value_a == 0:
            return 0.0 if value_b == 0 else float('inf')
        return ((value_b - value_a) / value_a) * 100
    
    async def _load_plan_components(self, plan_id) -> List[Dict[str, Any]]:
        """Load plan components from database"""
        if not self.db:
            raise ValueError("Database session is required to load stored plans")

        plan = self.db.query(Plan).filter(Plan.id == plan_id).first()
        if not plan:
            raise ValueError(f"Plan not found: {plan_id}")

        # Cache plan name for later reuse in the same request
        self._plan_name_cache[plan_id] = plan.name

        components = (
            self.db.query(PlanComponent)
            .filter(PlanComponent.plan_id == plan_id)
            .order_by(PlanComponent.sequence.asc(), PlanComponent.created_at.asc())
            .all()
        )

        if not components:
            logger.warning("Stored plan found without components", plan_id=str(plan_id))
            return []

        normalized_components: List[Dict[str, Any]] = []
        for component in components:
            raw_component = {
                "code": component.code,
                "setting": component.setting,
                "units": component.units,
                "utilization_weight": component.utilization_weight,
                "professional_component": component.professional_component,
                "facility_component": component.facility_component,
                "modifiers": component.modifiers,
                "pos": component.pos,
                "ndc11": component.ndc11,
                "wastage_units": component.wastage_units,
                "sequence": component.sequence,
            }
            normalized_components.append(
                self._apply_component_defaults(raw_component, component.sequence)
            )

        # Already ordered via query; no additional sort required
        return normalized_components
    
    async def _get_plan_name(self, plan_id) -> str:
        """Get plan name from database, using cached value when available"""
        if plan_id in self._plan_name_cache:
            return self._plan_name_cache[plan_id]

        if not self.db:
            raise ValueError("Database session is required to resolve plan name")

        plan = self.db.query(Plan).filter(Plan.id == plan_id).first()
        if not plan:
            raise ValueError(f"Plan not found: {plan_id}")

        self._plan_name_cache[plan_id] = plan.name
        return plan.name

    def _normalize_ad_hoc_components(self, components: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize ad-hoc plan components to match stored plan structure"""
        if not components:
            return []

        normalized: List[Dict[str, Any]] = []
        for idx, raw_component in enumerate(components):
            # Use explicit sequence if provided, otherwise fallback to index ordering
            provided_sequence = raw_component.get('sequence')
            fallback_sequence = provided_sequence if provided_sequence is not None else idx + 1
            normalized.append(
                self._apply_component_defaults(raw_component, fallback_sequence)
            )

        # Preserve explicit sequencing
        normalized.sort(key=lambda component: component['sequence'])
        return normalized

    def _apply_component_defaults(
        self,
        raw_component: Dict[str, Any],
        fallback_sequence: int,
    ) -> Dict[str, Any]:
        """Apply default values and type normalization for plan components"""

        code = raw_component.get('code')
        setting = raw_component.get('setting')
        if not code or not setting:
            raise ValueError("Plan components must include both 'code' and 'setting'")

        # Normalize modifiers
        modifiers_value = raw_component.get('modifiers')
        if modifiers_value is None:
            modifiers: List[str] = []
        elif isinstance(modifiers_value, list):
            modifiers = [str(mod).strip() for mod in modifiers_value if str(mod).strip()]
        elif isinstance(modifiers_value, str):
            modifiers = [part.strip() for part in modifiers_value.split(',') if part.strip()]
        else:
            modifiers = [str(modifiers_value).strip()]

        modifiers = [mod.upper() for mod in modifiers]

        def _to_int(value: Any, default: int) -> int:
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

        fallback_sequence_int = _to_int(fallback_sequence, 1)
        sequence_value = raw_component.get('sequence')
        sequence = _to_int(sequence_value, fallback_sequence_int)

        def _to_bool(value: Any, default: bool) -> bool:
            if value is None:
                return default
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            value_str = str(value).strip().lower()
            if value_str in {"true", "1", "yes", "y"}:
                return True
            if value_str in {"false", "0", "no", "n"}:
                return False
            return default

        def _to_float(value: Any, default: float) -> float:
            if value is None:
                return default
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        normalized_component = {
            "code": str(code).strip(),
            "setting": str(setting).strip().upper(),
            "units": _to_float(raw_component.get('units'), 1.0),
            "utilization_weight": _to_float(raw_component.get('utilization_weight'), 1.0),
            "professional_component": _to_bool(raw_component.get('professional_component'), True),
            "facility_component": _to_bool(raw_component.get('facility_component'), True),
            "modifiers": modifiers,
            "pos": str(raw_component.get('pos')).strip() if raw_component.get('pos') is not None else None,
            "ndc11": str(raw_component.get('ndc11')).strip() if raw_component.get('ndc11') is not None else None,
            "wastage_units": _to_float(raw_component.get('wastage_units'), 0.0),
            "sequence": sequence,
        }

        return normalized_component
