"""
RVU Data API Schemas

Pydantic models for RVU data API requests and responses
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime
from pydantic import BaseModel, Field


class ReleaseResponse(BaseModel):
    """Release response schema"""
    id: str
    type: str
    source_version: str
    source_url: Optional[str] = None
    imported_at: datetime
    published_at: Optional[datetime] = None
    notes: Optional[str] = None


class RVUItemResponse(BaseModel):
    """RVU item response schema"""
    id: str
    hcpcs_code: str
    modifiers: Optional[List[str]] = None
    description: Optional[str] = None
    status_code: str
    work_rvu: Optional[float] = None
    pe_rvu_nonfac: Optional[float] = None
    pe_rvu_fac: Optional[float] = None
    mp_rvu: Optional[float] = None
    na_indicator: Optional[str] = None
    global_days: Optional[str] = None
    bilateral_ind: Optional[str] = None
    multiple_proc_ind: Optional[str] = None
    assistant_surg_ind: Optional[str] = None
    co_surg_ind: Optional[str] = None
    team_surg_ind: Optional[str] = None
    endoscopic_base: Optional[str] = None
    conversion_factor: Optional[float] = None
    physician_supervision: Optional[str] = None
    diag_imaging_family: Optional[str] = None
    total_nonfac: Optional[float] = None
    total_fac: Optional[float] = None
    effective_start: date
    effective_end: date
    source_file: Optional[str] = None
    row_num: Optional[int] = None


class GPCIIndexResponse(BaseModel):
    """GPCI index response schema"""
    id: str
    mac: str
    state: str
    locality_id: str
    locality_name: Optional[str] = None
    work_gpci: float
    pe_gpci: float
    mp_gpci: float
    effective_start: date
    effective_end: date
    source_file: Optional[str] = None
    row_num: Optional[int] = None


class OPPSCapResponse(BaseModel):
    """OPPS cap response schema"""
    id: str
    hcpcs_code: str
    modifier: Optional[str] = None
    proc_status: str
    mac: str
    locality_id: str
    price_fac: float
    price_nonfac: float
    effective_start: date
    effective_end: date
    source_file: Optional[str] = None
    row_num: Optional[int] = None


class AnesCFResponse(BaseModel):
    """Anesthesia CF response schema"""
    id: str
    mac: str
    locality_id: str
    locality_name: Optional[str] = None
    anesthesia_cf: float
    effective_start: date
    effective_end: date
    source_file: Optional[str] = None
    row_num: Optional[int] = None


class LocalityCountyResponse(BaseModel):
    """Locality-County response schema"""
    id: str
    mac: str
    locality_id: str
    state: str
    fee_schedule_area: Optional[str] = None
    county_name: Optional[str] = None
    effective_start: date
    effective_end: date
    source_file: Optional[str] = None
    row_num: Optional[int] = None


class RVUSearchRequest(BaseModel):
    """RVU search request schema"""
    hcpcs_code: Optional[str] = None
    status_code: Optional[str] = None
    effective_date: Optional[date] = None
    release_id: Optional[str] = None
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)


class RVUSearchResponse(BaseModel):
    """RVU search response schema"""
    items: List[RVUItemResponse]
    total_count: int
    limit: int
    offset: int


class APIErrorResponse(BaseModel):
    """API error response schema"""
    error: str
    detail: str
    timestamp: datetime = Field(default_factory=datetime.now)
    request_id: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response schema"""
    status: str
    timestamp: datetime
    service: str
    version: str
    database_connected: Optional[bool] = None
    uptime_seconds: Optional[float] = None

