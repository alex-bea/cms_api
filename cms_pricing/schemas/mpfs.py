"""
MPFS API Schemas

Following Global API Program PRDs v1.0 for standardized response formats
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal


class MPFSRVUItem(BaseModel):
    """MPFS RVU item schema"""
    hcpcs: str = Field(..., description="HCPCS code")
    modifier: Optional[str] = Field(None, description="Modifier code")
    effective_from: Optional[str] = Field(None, description="Effective from date")
    effective_to: Optional[str] = Field(None, description="Effective to date")
    rvu_work: Optional[float] = Field(None, description="Work RVU")
    rvu_pe_nonfac: Optional[float] = Field(None, description="PE RVU non-facility")
    rvu_pe_fac: Optional[float] = Field(None, description="PE RVU facility")
    rvu_malp: Optional[float] = Field(None, description="Malpractice RVU")
    status_code: str = Field(..., description="Status indicator")
    global_days: Optional[str] = Field(None, description="Global period days")
    na_indicator: Optional[str] = Field(None, description="Not applicable indicator")
    opps_cap_applicable: bool = Field(..., description="OPPS cap applies")
    is_payable: bool = Field(..., description="Whether item is payable under MPFS")
    payment_category: Optional[str] = Field(None, description="Payment category")
    bilateral_indicator: bool = Field(..., description="Bilateral surgery indicator")
    multiple_procedure_indicator: bool = Field(..., description="Multiple procedure indicator")
    assistant_surgery_indicator: bool = Field(..., description="Assistant surgery indicator")
    co_surgeon_indicator: bool = Field(..., description="Co-surgeon indicator")
    team_surgery_indicator: bool = Field(..., description="Team surgery indicator")
    total_rvu: Optional[float] = Field(None, description="Total RVU")
    is_surgery: bool = Field(..., description="Whether this is a surgical procedure")
    is_evaluation: bool = Field(..., description="Whether this is an evaluation service")
    is_procedure: bool = Field(..., description="Whether this is a procedure")
    release_id: str = Field(..., description="Release identifier")
    created_at: Optional[str] = Field(None, description="Record creation timestamp")
    updated_at: Optional[str] = Field(None, description="Record update timestamp")


class PaginationInfo(BaseModel):
    """Pagination information schema"""
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
    total_count: int = Field(..., description="Total number of items")
    total_pages: int = Field(..., description="Total number of pages")
    has_next: bool = Field(..., description="Whether there is a next page")
    has_prev: bool = Field(..., description="Whether there is a previous page")


class MPFSRVUListResponse(BaseModel):
    """MPFS RVU list response schema"""
    items: List[MPFSRVUItem] = Field(..., description="List of RVU items")
    pagination: PaginationInfo = Field(..., description="Pagination information")
    metadata: Dict[str, Any] = Field(..., description="Response metadata")


class MPFSRVUResponse(BaseModel):
    """MPFS RVU single item response schema"""
    item: MPFSRVUItem = Field(..., description="RVU item")
    metadata: Dict[str, Any] = Field(..., description="Response metadata")


class MPFSConversionFactorItem(BaseModel):
    """MPFS Conversion Factor item schema"""
    id: str = Field(..., description="Unique identifier")
    cf_type: str = Field(..., description="Conversion factor type")
    cf_value: Optional[float] = Field(None, description="Conversion factor value")
    cf_description: Optional[str] = Field(None, description="Description of the conversion factor")
    effective_from: Optional[str] = Field(None, description="Effective from date")
    effective_to: Optional[str] = Field(None, description="Effective to date")
    release_id: str = Field(..., description="Release identifier")
    vintage_year: str = Field(..., description="Vintage year")
    created_at: Optional[str] = Field(None, description="Record creation timestamp")
    updated_at: Optional[str] = Field(None, description="Record update timestamp")


class MPFSConversionFactorResponse(BaseModel):
    """MPFS Conversion Factor response schema"""
    conversion_factor: MPFSConversionFactorItem = Field(..., description="Conversion factor item")
    metadata: Dict[str, Any] = Field(..., description="Response metadata")


class MPFSAbstractItem(BaseModel):
    """MPFS Abstract item schema"""
    id: str = Field(..., description="Unique identifier")
    abstract_type: str = Field(..., description="Abstract type")
    title: str = Field(..., description="Abstract title")
    content: Optional[str] = Field(None, description="Abstract content")
    national_payment_total: Optional[float] = Field(None, description="Total national payment amount")
    payment_year: str = Field(..., description="Payment year")
    effective_from: Optional[str] = Field(None, description="Effective from date")
    effective_to: Optional[str] = Field(None, description="Effective to date")
    release_id: str = Field(..., description="Release identifier")
    created_at: Optional[str] = Field(None, description="Record creation timestamp")
    updated_at: Optional[str] = Field(None, description="Record update timestamp")


class MPFSAbstractResponse(BaseModel):
    """MPFS Abstract response schema"""
    abstract: MPFSAbstractItem = Field(..., description="Abstract item")
    metadata: Dict[str, Any] = Field(..., description="Response metadata")


class MPFSHealthResponse(BaseModel):
    """MPFS Health check response schema"""
    status: str = Field(..., description="Health status")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")
    timestamp: str = Field(..., description="Health check timestamp")
    checks: Dict[str, str] = Field(..., description="Individual service checks")


class MPFSStatsResponse(BaseModel):
    """MPFS Statistics response schema"""
    rvu_items: Dict[str, Any] = Field(..., description="RVU items statistics")
    conversion_factors: Dict[str, Any] = Field(..., description="Conversion factors statistics")
    metadata: Dict[str, Any] = Field(..., description="Response metadata")


# Error response schemas following Global API Program standards
class MPFSErrorDetail(BaseModel):
    """MPFS Error detail schema"""
    field: Optional[str] = Field(None, description="Field that caused the error")
    issue: str = Field(..., description="Description of the issue")


class MPFSError(BaseModel):
    """MPFS Error schema"""
    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Human-readable error message")
    details: List[MPFSErrorDetail] = Field(default_factory=list, description="Error details")


class MPFSErrorResponse(BaseModel):
    """MPFS Error response schema"""
    error: MPFSError = Field(..., description="Error information")
    trace: Dict[str, str] = Field(..., description="Trace information including correlation ID")
