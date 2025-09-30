"""Pricing-related Pydantic schemas"""

from typing import List, Optional, Dict, Any
from datetime import date
from pydantic import BaseModel, Field, field_validator
from uuid import UUID
from .common import MoneyResponse
from .geography import GeographyCandidate


class PricingRequest(BaseModel):
    """Request schema for pricing a plan"""
    zip: str = Field(..., min_length=5, max_length=5, description="5-digit ZIP code")
    plan_id: Optional[UUID] = Field(None, description="Plan ID (if using stored plan)")
    year: int = Field(..., ge=2020, le=2030, description="Valuation year")
    quarter: Optional[str] = Field(None, pattern=r'^[1-4]$', description="Quarter (1-4)")
    ccn: Optional[str] = Field(None, max_length=6, description="CMS Certification Number")
    payer: Optional[str] = Field(None, description="Payer name filter")
    plan: Optional[str] = Field(None, description="Plan name filter")
    include_home_health: bool = Field(default=False, description="Include home health")
    include_snf: bool = Field(default=False, description="Include SNF")
    apply_sequestration: bool = Field(default=False, description="Apply sequestration")
    sequestration_rate: float = Field(default=0.02, ge=0, le=0.1, description="Sequestration rate")
    format: str = Field(default="cents", pattern=r'^(cents|decimal)$', description="Money format")
    
    # Ad-hoc plan (alternative to plan_id)
    ad_hoc_plan: Optional[Dict[str, Any]] = Field(None, description="Ad-hoc plan definition")
    
    @field_validator('zip')
    @classmethod
    def validate_zip(cls, v):
        if not v.isdigit():
            raise ValueError('ZIP must contain only digits')
        return v
    
    @field_validator('ccn')
    @classmethod
    def validate_ccn(cls, v):
        if v is not None and (not v.isdigit() or len(v) != 6):
            raise ValueError('CCN must be exactly 6 digits')
        return v


class LineItemResponse(BaseModel):
    """Response schema for a pricing line item"""
    sequence: int = Field(..., description="Line sequence number")
    code: str = Field(..., description="HCPCS/CPT code")
    setting: str = Field(..., description="Payment setting")
    units: float = Field(..., description="Number of units")
    utilization_weight: float = Field(..., description="Utilization weight")
    
    # Pricing results
    allowed_cents: int = Field(..., description="Medicare allowed amount in cents")
    beneficiary_deductible_cents: int = Field(..., description="Beneficiary deductible in cents")
    beneficiary_coinsurance_cents: int = Field(..., description="Beneficiary coinsurance in cents")
    beneficiary_total_cents: int = Field(..., description="Total beneficiary cost in cents")
    program_payment_cents: int = Field(..., description="Program payment in cents")
    
    # Component breakdown
    professional_allowed_cents: Optional[int] = Field(None, description="Professional component allowed")
    facility_allowed_cents: Optional[int] = Field(None, description="Facility component allowed")
    
    # Source information
    source: str = Field(..., description="Data source (benchmark, mrf, tic)")
    facility_specific: bool = Field(default=False, description="Whether facility-specific rate was used")
    packaged: bool = Field(default=False, description="Whether item is packaged")
    
    # Drug-specific fields
    reference_price_cents: Optional[int] = Field(None, description="NADAC reference price")
    unit_conversion: Optional[Dict[str, Any]] = Field(None, description="Unit conversion details")
    
    # Trace references
    trace_refs: List[str] = Field(default_factory=list, description="Trace reference IDs")


class GeographyResponse(BaseModel):
    """Geography information in pricing response"""
    zip5: str = Field(..., description="5-digit ZIP code")
    locality_id: Optional[str] = Field(None, description="Selected MPFS locality")
    locality_name: Optional[str] = Field(None, description="Locality name")
    cbsa: Optional[str] = Field(None, description="Selected CBSA")
    cbsa_name: Optional[str] = Field(None, description="CBSA name")
    county_fips: Optional[str] = Field(None, description="County FIPS")
    state_code: Optional[str] = Field(None, description="State code")
    rural_flag: Optional[str] = Field(default=None, description="Rural indicator (R, B, or blank) per PRD")
    resolution_method: str = Field(..., description="Resolution method used")
    candidates: List[GeographyCandidate] = Field(..., description="All candidates considered")


class PricingResponse(BaseModel):
    """Response schema for pricing a plan"""
    run_id: str = Field(..., description="Unique run identifier")
    plan_id: Optional[UUID] = Field(None, description="Plan ID")
    plan_name: Optional[str] = Field(None, description="Plan name")
    
    # Geography
    geography: GeographyResponse = Field(..., description="Geography information")
    
    # Line items
    line_items: List[LineItemResponse] = Field(..., description="Pricing line items")
    
    # Totals
    total_allowed_cents: int = Field(..., description="Total Medicare allowed")
    total_beneficiary_deductible_cents: int = Field(..., description="Total beneficiary deductible")
    total_beneficiary_coinsurance_cents: int = Field(..., description="Total beneficiary coinsurance")
    total_beneficiary_cents: int = Field(..., description="Total beneficiary cost")
    total_program_payment_cents: int = Field(..., description="Total program payment")
    remaining_part_b_deductible_cents: int = Field(..., description="Remaining Part B deductible")
    
    # Flags and metadata
    post_acute_included: bool = Field(default=False, description="Post-acute care included")
    sequestration_applied: bool = Field(default=False, description="Sequestration applied")
    facility_specific_used: bool = Field(default=False, description="Facility-specific rates used")
    
    # Dataset information
    datasets_used: List[Dict[str, Any]] = Field(..., description="Datasets and versions used")
    
    # Warnings
    warnings: List[str] = Field(default_factory=list, description="Pricing warnings")


class ComparisonRequest(BaseModel):
    """Request schema for comparing two locations"""
    zip_a: str = Field(..., min_length=5, max_length=5, description="Location A ZIP code")
    zip_b: str = Field(..., min_length=5, max_length=5, description="Location B ZIP code")
    plan_id: Optional[UUID] = Field(None, description="Plan ID")
    year: int = Field(..., ge=2020, le=2030, description="Valuation year")
    quarter: Optional[str] = Field(None, pattern=r'^[1-4]$', description="Quarter")
    ccn_a: Optional[str] = Field(None, max_length=6, description="Location A CCN")
    ccn_b: Optional[str] = Field(None, max_length=6, description="Location B CCN")
    payer: Optional[str] = Field(None, description="Payer name filter")
    plan: Optional[str] = Field(None, description="Plan name filter")
    include_home_health: bool = Field(default=False, description="Include home health")
    include_snf: bool = Field(default=False, description="Include SNF")
    apply_sequestration: bool = Field(default=False, description="Apply sequestration")
    sequestration_rate: float = Field(default=0.02, ge=0, le=0.1, description="Sequestration rate")
    format: str = Field(default="cents", pattern=r'^(cents|decimal)$', description="Money format")
    
    # Ad-hoc plan (alternative to plan_id)
    ad_hoc_plan: Optional[Dict[str, Any]] = Field(None, description="Ad-hoc plan definition")
    
    @field_validator('zip_a', 'zip_b')
    @classmethod
    def validate_zip(cls, v):
        if not v.isdigit():
            raise ValueError('ZIP must contain only digits')
        return v
    
    @field_validator('ccn_a', 'ccn_b')
    @classmethod
    def validate_ccn(cls, v):
        if v is not None and (not v.isdigit() or len(v) != 6):
            raise ValueError('CCN must be exactly 6 digits')
        return v


class ComparisonDelta(BaseModel):
    """Delta between two pricing results"""
    field: str = Field(..., description="Field name")
    location_a: int = Field(..., description="Value for location A (cents)")
    location_b: int = Field(..., description="Value for location B (cents)")
    delta_cents: int = Field(..., description="Difference (B - A) in cents")
    delta_percent: float = Field(..., description="Percentage difference")


class ComparisonResponse(BaseModel):
    """Response schema for comparing two locations"""
    run_id: str = Field(..., description="Unique run identifier")
    plan_id: Optional[UUID] = Field(None, description="Plan ID")
    plan_name: Optional[str] = Field(None, description="Plan name")
    
    # Location A results
    location_a: PricingResponse = Field(..., description="Location A pricing results")
    
    # Location B results
    location_b: PricingResponse = Field(..., description="Location B pricing results")
    
    # Comparison deltas
    deltas: List[ComparisonDelta] = Field(..., description="Field-by-field differences")
    
    # Parity report
    parity_report: Dict[str, Any] = Field(..., description="Parity validation report")
    
    # Summary
    total_delta_cents: int = Field(..., description="Total allowed difference")
    total_delta_percent: float = Field(..., description="Total percentage difference")
