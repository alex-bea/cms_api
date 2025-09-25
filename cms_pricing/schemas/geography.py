"""Geography-related Pydantic schemas"""

from typing import List, Optional
from pydantic import BaseModel, Field, validator


class GeographyCandidate(BaseModel):
    """Geography resolution candidate"""
    zip5: str = Field(..., description="5-digit ZIP code")
    locality_id: Optional[str] = Field(None, description="MPFS locality ID")
    locality_name: Optional[str] = Field(None, description="Locality name")
    cbsa: Optional[str] = Field(None, description="Core Based Statistical Area")
    cbsa_name: Optional[str] = Field(None, description="CBSA name")
    county_fips: Optional[str] = Field(None, description="County FIPS code")
    state_code: Optional[str] = Field(None, description="State code")
    population_share: float = Field(..., description="Population share for this mapping")
    is_rural_dmepos: bool = Field(default=False, description="Rural status for DMEPOS")
    used: bool = Field(default=False, description="Whether this candidate was selected")
    
    @validator('zip5')
    def validate_zip5(cls, v):
        if not v.isdigit() or len(v) != 5:
            raise ValueError('ZIP5 must be exactly 5 digits')
        return v


class GeographyResolveRequest(BaseModel):
    """Request schema for geography resolution"""
    zip: str = Field(..., min_length=5, max_length=5, description="5-digit ZIP code")
    
    @validator('zip')
    def validate_zip(cls, v):
        if not v.isdigit():
            raise ValueError('ZIP must contain only digits')
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
