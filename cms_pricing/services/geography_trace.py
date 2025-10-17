"""Geography resolution trace service"""

import time
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
import structlog

from cms_pricing.models.geography_trace import GeographyResolutionTrace
from cms_pricing.schemas.geography_trace import (
    GeographyResolutionTrace as TraceSchema,
    GeographyTraceInputs,
    GeographyTraceOutput,
    GeographyTraceNearest
)

logger = structlog.get_logger()


class GeographyTraceService:
    """Service for creating and storing geography resolution traces"""
    
    def __init__(self, db: Session):
        self.db = db
        self.service_version = "1.0.0"  # TODO(alex, GH-427): Get from config
    
    def create_trace(
        self,
        inputs: Dict[str, Any],
        result: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
        latency_ms: float = 0.0,
        start_time: Optional[float] = None
    ) -> GeographyResolutionTrace:
        """
        Create and store a geography resolution trace
        
        Args:
            inputs: Input parameters for resolution
            result: Resolution result (if successful)
            error: Exception (if resolution failed)
            latency_ms: Resolution latency in milliseconds
            start_time: Start time for latency calculation
            
        Returns:
            Created trace record
        """
        
        # Calculate latency if start_time provided
        if start_time:
            latency_ms = (time.time() - start_time) * 1000
        
        # Extract input parameters
        trace_inputs = GeographyTraceInputs(
            zip5=inputs.get("zip5", ""),
            plus4=inputs.get("plus4"),
            valuation_year=inputs.get("valuation_year"),
            quarter=inputs.get("quarter"),
            valuation_date=inputs.get("valuation_date"),
            strict=inputs.get("strict", False)
        )
        
        # Determine match level and create output
        if error:
            match_level = "error"
            trace_output = GeographyTraceOutput(
                locality_id=None,
                state=None,
                rural_flag=None,
                match_level=match_level,
                dataset_digest=None
            )
            error_message = str(error)
            error_code = self._extract_error_code(error)
        elif result:
            match_level = result.get("match_level", "unknown")
            trace_output = GeographyTraceOutput(
                locality_id=result.get("locality_id"),
                state=result.get("state"),
                rural_flag=result.get("rural_flag"),
                match_level=match_level,
                dataset_digest=result.get("dataset_digest")
            )
            error_message = None
            error_code = None
        else:
            match_level = "unknown"
            trace_output = GeographyTraceOutput(
                locality_id=None,
                state=None,
                rural_flag=None,
                match_level=match_level,
                dataset_digest=None
            )
            error_message = "No result or error provided"
            error_code = "UNKNOWN_ERROR"
        
        # Create nearest fallback details
        trace_nearest = GeographyTraceNearest(
            candidate_zip=result.get("nearest_zip") if result else None,
            candidate_distance_miles=result.get("distance_miles") if result else None,
            is_pobox=None  # TODO(alex, GH-427): Extract from result if available
        )
        
        # Create trace record
        trace_record = GeographyResolutionTrace(
            zip5=trace_inputs.zip5,
            plus4=trace_inputs.plus4,
            valuation_year=str(trace_inputs.valuation_year) if trace_inputs.valuation_year else None,
            quarter=str(trace_inputs.quarter) if trace_inputs.quarter else None,
            valuation_date=trace_inputs.valuation_date,
            strict="true" if trace_inputs.strict else "false",
            match_level=match_level,
            locality_id=trace_output.locality_id,
            state=trace_output.state,
            rural_flag=trace_output.rural_flag,
            nearest_zip=trace_nearest.candidate_zip,
            distance_miles=trace_nearest.candidate_distance_miles,
            dataset_digest=trace_output.dataset_digest,
            latency_ms=latency_ms,
            service_version=self.service_version,
            resolved_at=datetime.utcnow(),
            inputs_json=trace_inputs.dict(),
            output_json=trace_output.dict(),
            error_message=error_message,
            error_code=error_code
        )
        
        # Store in database
        try:
            self.db.add(trace_record)
            self.db.commit()
            
            logger.info(
                "Geography resolution trace created",
                trace_id=str(trace_record.id),
                zip5=trace_inputs.zip5,
                plus4=trace_inputs.plus4,
                match_level=match_level,
                latency_ms=latency_ms,
                error=error_message is not None
            )
            
            return trace_record
            
        except Exception as e:
            logger.error(
                "Failed to store geography resolution trace",
                error=str(e),
                zip5=trace_inputs.zip5
            )
            self.db.rollback()
            raise
    
    def _extract_error_code(self, error: Exception) -> str:
        """Extract error code from exception"""
        error_str = str(error)
        
        if "ZIP+4" in error_str and "strict" in error_str.lower():
            return "GEO_NEEDS_PLUS4"
        elif "not found" in error_str.lower():
            return "GEO_NOT_FOUND_IN_STATE"
        elif "coverage" in error_str.lower():
            return "GEO_NO_COVERAGE_FOR_PERIOD"
        else:
            return "GEO_RESOLUTION_ERROR"
    
    def get_trace_summary(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        zip5: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary statistics for traces
        
        Args:
            start_date: Start date for filtering
            end_date: End date for filtering
            zip5: Specific ZIP code to filter by
            
        Returns:
            Summary statistics
        """
        query = self.db.query(GeographyResolutionTrace)
        
        if start_date:
            query = query.filter(GeographyResolutionTrace.resolved_at >= start_date)
        if end_date:
            query = query.filter(GeographyResolutionTrace.resolved_at <= end_date)
        if zip5:
            query = query.filter(GeographyResolutionTrace.zip5 == zip5)
        
        traces = query.all()
        
        if not traces:
            return {
                "total_calls": 0,
                "zip4_matches": 0,
                "zip5_matches": 0,
                "nearest_matches": 0,
                "errors": 0,
                "avg_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "unique_zips": 0,
                "unique_states": 0
            }
        
        # Calculate statistics
        total_calls = len(traces)
        zip4_matches = len([t for t in traces if t.match_level == "zip+4"])
        zip5_matches = len([t for t in traces if t.match_level == "zip5"])
        nearest_matches = len([t for t in traces if t.match_level == "nearest"])
        errors = len([t for t in traces if t.match_level == "error"])
        
        latencies = [t.latency_ms for t in traces if t.latency_ms is not None]
        avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0.0
        p95_latency_ms = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0.0
        
        unique_zips = len(set(t.zip5 for t in traces if t.zip5))
        unique_states = len(set(t.state for t in traces if t.state and t.state != ""))
        
        return {
            "total_calls": total_calls,
            "zip4_matches": zip4_matches,
            "zip5_matches": zip5_matches,
            "nearest_matches": nearest_matches,
            "errors": errors,
            "avg_latency_ms": round(avg_latency_ms, 2),
            "p95_latency_ms": round(p95_latency_ms, 2),
            "unique_zips": unique_zips,
            "unique_states": unique_states
        }


