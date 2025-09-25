"""Trace-related Pydantic schemas"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from uuid import UUID


class TraceData(BaseModel):
    """Individual trace data entry"""
    trace_type: str = Field(..., description="Type of trace (dataset_selection, geo_resolution, formula, etc.)")
    trace_data: Dict[str, Any] = Field(..., description="Structured trace data")
    line_sequence: Optional[int] = Field(None, description="Line sequence for line-specific traces")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Trace timestamp")


class TraceResponse(BaseModel):
    """Response schema for trace information"""
    run_id: str = Field(..., description="Run identifier")
    endpoint: str = Field(..., description="API endpoint")
    status: str = Field(..., description="Run status")
    created_at: datetime = Field(..., description="Run creation time")
    duration_ms: Optional[int] = Field(None, description="Run duration in milliseconds")
    
    # Input parameters
    request_json: Optional[Dict[str, Any]] = Field(None, description="Full request payload")
    
    # Output results
    response_json: Optional[Dict[str, Any]] = Field(None, description="Full response payload")
    
    # Trace data
    traces: List[TraceData] = Field(..., description="Detailed trace information")
    
    # Dataset information
    datasets_used: List[Dict[str, Any]] = Field(..., description="Datasets and versions used")
    
    # Performance metrics
    cache_hits: int = Field(default=0, description="Number of cache hits")
    cache_misses: int = Field(default=0, description="Number of cache misses")
    facility_rates_used: int = Field(default=0, description="Number of facility-specific rates used")
    benchmark_rates_used: int = Field(default=0, description="Number of benchmark rates used")
