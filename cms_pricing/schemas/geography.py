"""Geography-related Pydantic schemas"""

from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field, field_validator


class GeographyCandidate(BaseModel):
    """Geography resolution candidate"""
    zip5: str = Field(..., description="5-digit ZIP code")
    locality_id: Optional[str] = Field(None, description="MPFS locality ID")
    locality_name: Optional[str] = Field(None, description="Locality name")
    cbsa: Optional[str] = Field(None, description="Core Based Statistical Area")
    cbsa_name: Optional[str] = Field(None, description="CBSA name")
    county_fips: Optional[str] = Field(None, description="County FIPS code")
    state_code: Optional[str] = Field(None, description="State code")
    population_share: Optional[float] = Field(None, description="Population share for this mapping")
    rural_flag: Optional[str] = Field(None, description="Rural indicator (R, B, or blank) per PRD")
    used: bool = Field(default=False, description="Whether this candidate was selected")
    
    @field_validator('zip5')
    @classmethod
    def validate_zip5(cls, v):
        if not v.isdigit() or len(v) != 5:
            raise ValueError('ZIP5 must be exactly 5 digits')
        return v


class GeographyResolveRequest(BaseModel):
    """Request schema for geography resolution"""
    zip: str = Field(..., min_length=5, max_length=5, description="5-digit ZIP code")
    plus4: Optional[str] = Field(None, min_length=4, max_length=4, description="4-digit ZIP+4 add-on")
    valuation_year: Optional[int] = Field(None, ge=2020, le=2030, description="Year for effective date selection")
    quarter: Optional[int] = Field(None, ge=1, le=4, description="Quarter (1-4) for effective date selection")
    valuation_date: Optional[date] = Field(None, description="Specific date for effective date selection (overrides year/quarter)")
    strict: bool = Field(default=False, description="Require exact ZIP+4 match (no fallback)")
    expose_carrier: bool = Field(default=False, description="Include carrier/MAC information in response")
    
    @field_validator('zip')
    @classmethod
    def validate_zip(cls, v):
        if not v.isdigit():
            raise ValueError('ZIP must contain only digits')
        return v
    
    @field_validator('plus4')
    @classmethod
    def validate_plus4(cls, v):
        if v is not None and not v.isdigit():
            raise ValueError('Plus4 must contain only digits')
        return v
    
    @field_validator('quarter')
    @classmethod
    def validate_quarter(cls, v):
        if v is not None and (v < 1 or v > 4):
            raise ValueError('Quarter must be between 1 and 4')
        return v


class GeographyResolveResponse(BaseModel):
    """Response schema for geography resolution"""
    zip5: str = Field(..., description="5-digit ZIP code")
    candidates: List[GeographyCandidate] = Field(..., description="All possible mappings")
    requires_resolution: bool = Field(..., description="Whether manual resolution is required")
    ambiguity_threshold: float = Field(default=0.2, description="Threshold for requiring resolution")
    selected_candidate: Optional[GeographyCandidate] = Field(None, description="Selected candidate")
    resolution_method: str = Field(..., description="Method used for selection")
    warnings: List[str] = Field(default_factory=list, description="Resolution warnings")
