"""Pydantic schemas for nearest ZIP resolver API"""

from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator


class NearestZipRequest(BaseModel):
    """Request schema for nearest ZIP resolution"""
    
    zip: str = Field(..., description="ZIP5 or ZIP9 (e.g., '94107' or '94107-1234')")
    use_nber: bool = Field(True, description="Use NBER fast-path for distance calculations")
    max_radius_miles: float = Field(100.0, ge=1.0, le=500.0, description="Maximum search radius in miles")
    include_trace: bool = Field(False, description="Include full trace in response")
    
    @validator('zip')
    def validate_zip(cls, v):
        """Validate ZIP format"""
        import re
        digits = re.sub(r'[^\d]', '', v)
        if len(digits) not in [5, 9]:
            raise ValueError("ZIP must be 5 or 9 digits (e.g., '94107' or '94107-1234')")
        return v


class NearestZipResponse(BaseModel):
    """Response schema for nearest ZIP resolution"""
    
    nearest_zip: str = Field(..., description="Nearest ZIP5 found")
    distance_miles: float = Field(..., description="Distance in miles")
    input_zip: str = Field(..., description="Original input ZIP")
    trace: Optional[Dict[str, Any]] = Field(None, description="Full trace information")


class NearestZipTrace(BaseModel):
    """Schema for trace information"""
    
    id: str = Field(..., description="Trace ID")
    input_zip: str = Field(..., description="Original input ZIP")
    input_zip5: str = Field(..., description="Parsed ZIP5")
    input_zip9: Optional[str] = Field(None, description="Parsed ZIP9")
    result_zip: str = Field(..., description="Result ZIP5")
    distance_miles: float = Field(..., description="Distance in miles")
    trace_json: str = Field(..., description="Full trace as JSON string")
    created_at: datetime = Field(..., description="When the resolution was performed")


class ResolverStats(BaseModel):
    """Schema for resolver statistics"""
    
    data_sources: Dict[str, int] = Field(..., description="Counts for each data source")
    resolver_stats: Dict[str, Any] = Field(..., description="Resolver performance statistics")


class ResolverHealth(BaseModel):
    """Schema for resolver health check"""
    
    status: str = Field(..., description="Health status (healthy/unhealthy)")
    message: str = Field(..., description="Health message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional health details")


# Trace schema components for detailed trace information
class InputTrace(BaseModel):
    """Input trace component"""
    zip: str
    zip5: Optional[str] = None
    zip9: Optional[str] = None


class NormalizationTrace(BaseModel):
    """Normalization trace component"""
    starting_zcta: Optional[str] = None
    zcta_weight: Optional[float] = None
    state: Optional[str] = None
    locality: Optional[str] = None
    zip9_hit: Optional[bool] = None
    relationship: Optional[str] = None


class StartingCentroidTrace(BaseModel):
    """Starting centroid trace component"""
    lat: Optional[float] = None
    lon: Optional[float] = None
    source: Optional[str] = None


class CandidatesTrace(BaseModel):
    """Candidates trace component"""
    state_zip_count: Optional[int] = None
    excluded_pobox: Optional[int] = None


class DistCalcTrace(BaseModel):
    """Distance calculation trace component"""
    engine: Optional[str] = None
    nber_hits: Optional[int] = None
    fallbacks: Optional[int] = None


class ResultTrace(BaseModel):
    """Result trace component"""
    nearest_zip: Optional[str] = None
    distance_miles: Optional[float] = None


class FlagsTrace(BaseModel):
    """Flags trace component"""
    coincident: Optional[bool] = None
    far_neighbor: Optional[bool] = None


class FullTrace(BaseModel):
    """Complete trace schema"""
    input: InputTrace
    normalization: NormalizationTrace
    starting_centroid: StartingCentroidTrace
    candidates: CandidatesTrace
    dist_calc: DistCalcTrace
    result: ResultTrace
    flags: FlagsTrace
    error: Optional[str] = None
