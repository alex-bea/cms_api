"""Pydantic schemas for geography resolution tracing"""

from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class GeographyTraceInputs(BaseModel):
    """Input parameters for geography resolution"""
    zip5: str = Field(..., description="5-digit ZIP code")
    plus4: Optional[str] = Field(None, description="4-digit ZIP+4 add-on")
    valuation_year: Optional[int] = Field(None, description="Year for effective date selection")
    quarter: Optional[int] = Field(None, description="Quarter (1-4) for effective date selection")
    valuation_date: Optional[str] = Field(None, description="Specific date for effective date selection")
    strict: bool = Field(default=False, description="Require exact ZIP+4 match")


class GeographyTraceNearest(BaseModel):
    """Nearest fallback information"""
    candidate_zip: Optional[str] = Field(None, description="Nearest ZIP code found")
    candidate_distance_miles: Optional[float] = Field(None, description="Distance to nearest ZIP in miles")
    is_pobox: Optional[bool] = Field(None, description="Whether nearest candidate is a PO Box")


class GeographyTraceOutput(BaseModel):
    """Output result from geography resolution"""
    locality_id: Optional[str] = Field(None, description="Resolved locality ID")
    state: Optional[str] = Field(None, description="State code")
    rural_flag: Optional[str] = Field(None, description="Rural indicator")
    match_level: str = Field(..., description="Resolution match level")
    dataset_digest: Optional[str] = Field(None, description="Dataset digest used")


class GeographyResolutionTrace(BaseModel):
    """Complete geography resolution trace per PRD Section 13.3"""
    
    # Required schema fields per PRD
    inputs: GeographyTraceInputs = Field(..., description="Input parameters")
    match_level: str = Field(..., description="Resolution match level")
    output: GeographyTraceOutput = Field(..., description="Resolution output")
    nearest: GeographyTraceNearest = Field(..., description="Nearest fallback details")
    snapshot_digest: Optional[str] = Field(None, description="Active dataset digest")
    latency_ms: float = Field(..., description="Resolution latency in milliseconds")
    service_version: str = Field(..., description="Service version")
    
    # Additional metadata
    resolved_at: datetime = Field(default_factory=datetime.utcnow, description="Resolution timestamp")
    error_message: Optional[str] = Field(None, description="Error message if resolution failed")
    error_code: Optional[str] = Field(None, description="Error code if resolution failed")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class GeographyTraceSummary(BaseModel):
    """Summary statistics for geography resolution traces"""
    total_calls: int = Field(..., description="Total number of resolution calls")
    zip4_matches: int = Field(..., description="Number of ZIP+4 exact matches")
    zip5_matches: int = Field(..., description="Number of ZIP5 exact matches")
    nearest_matches: int = Field(..., description="Number of nearest fallback matches")
    errors: int = Field(..., description="Number of resolution errors")
    avg_latency_ms: float = Field(..., description="Average resolution latency")
    p95_latency_ms: float = Field(..., description="95th percentile latency")
    unique_zips: int = Field(..., description="Number of unique ZIP codes resolved")
    unique_states: int = Field(..., description="Number of unique states")



