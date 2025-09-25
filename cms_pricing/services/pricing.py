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
    
    def __init__(self):
        self.geography_service = GeographyService()
        self.trace_service = TraceService()
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
                components = request.ad_hoc_plan.get('components', [])
                plan_name = request.ad_hoc_plan.get('name', 'Ad-hoc Plan')
            
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
                    units=component.get('units', 1.0),
                    utilization_weight=component.get('utilization_weight', 1.0),
                    professional_component=component.get('professional_component', True),
                    facility_component=component.get('facility_component', True),
                    modifiers=component.get('modifiers', []),
                    pos=component.get('pos'),
                    ndc11=component.get('ndc11')
                )
                
                # Create line item response
                line_item = LineItemResponse(
                    sequence=i + 1,
                    code=component['code'],
                    setting=component['setting'],
                    units=component.get('units', 1.0),
                    utilization_weight=component.get('utilization_weight', 1.0),
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
                is_rural_dmepos=geography_result.selected_candidate.is_rural_dmepos if geography_result.selected_candidate else False,
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
        """Load plan components from database (placeholder)"""
        # TODO: Implement database loading
        return []
    
    async def _get_plan_name(self, plan_id) -> str:
        """Get plan name from database (placeholder)"""
        # TODO: Implement database loading
        return "Unknown Plan"
