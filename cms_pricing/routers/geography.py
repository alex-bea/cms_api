"""Geography resolution endpoints"""

from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from slowapi import Limiter
from slowapi.util import get_remote_address

from cms_pricing.schemas.geography import GeographyResolveRequest, GeographyResolveResponse, GeographyCandidate
from cms_pricing.schemas.geography_trace import GeographyTraceSummary
from cms_pricing.auth import verify_api_key
from cms_pricing.services.geography import GeographyService
from cms_pricing.services.geography_trace import GeographyTraceService
from cms_pricing.services.geography_snapshot import GeographySnapshotService

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/resolve")
async def resolve_geography(
    request: Request,
    zip: str,
    plus4: Optional[str] = Query(None, description="4-digit ZIP+4 add-on (optional)"),
    valuation_year: Optional[int] = Query(None, description="Year for effective date selection (defaults to current year)"),
    quarter: Optional[int] = Query(None, description="Quarter for effective date selection (1-4, optional)"),
    valuation_date: Optional[date] = Query(None, description="Specific date for effective date selection (overrides year/quarter)"),
    strict: bool = Query(False, description="If true, error on non-ZIP+4 matches instead of falling back"),
    max_radius_miles: int = Query(100, description="Maximum radius for nearest ZIP fallback"),
    initial_radius_miles: int = Query(25, description="Initial radius for nearest ZIP search"),
    expand_step_miles: int = Query(10, description="Step size for radius expansion"),
    expose_carrier: bool = Query(False, description="Include carrier/MAC information in response"),
    api_key: str = Depends(verify_api_key)
):
    """
    Resolve ZIP+4 to locality using ZIP+4-first hierarchy per PRD.
    
    Implements the full hierarchy:
    1. ZIP+4 exact match (if plus4 provided)
    2. ZIP5 exact match  
    3. Nearest ZIP within same state (25-100mi radius)
    4. Plan default/benchmark locality (or error in strict mode)
    """
    
    # Validate ZIP input
    if not zip.isdigit() or len(zip) != 5:
        raise HTTPException(
            status_code=400,
            detail="ZIP must be exactly 5 digits"
        )
    
    # Validate plus4 input if provided
    if plus4 is not None:
        plus4_clean = ''.join(filter(str.isdigit, plus4))
        if len(plus4_clean) != 4:
            raise HTTPException(
                status_code=400,
                detail="ZIP+4 must be exactly 4 digits"
            )
        plus4 = plus4_clean
    
    # Validate quarter if provided
    if quarter is not None and (quarter < 1 or quarter > 4):
        raise HTTPException(
            status_code=400,
            detail="Quarter must be between 1 and 4"
        )
    
    try:
        geography_service = GeographyService()
        result = await geography_service.resolve_zip(
            zip5=zip,
            plus4=plus4,
            valuation_year=valuation_year,
            quarter=quarter,
            valuation_date=valuation_date,
            strict=strict,
            max_radius_miles=max_radius_miles,
            initial_radius_miles=initial_radius_miles,
            expand_step_miles=expand_step_miles,
            expose_carrier=expose_carrier
        )
        return result
    except ValueError as e:
        # Handle strict mode errors with friendly messages
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Geography resolution failed: {str(e)}"
        )


@router.get("/resolve/legacy", response_model=GeographyResolveResponse)
async def resolve_geography_legacy(
    request: Request,
    zip: str,
    valuation_date: Optional[date] = Query(None, description="Date for which to resolve geography (defaults to today)"),
    api_key: str = Depends(verify_api_key)
):
    """Legacy ZIP resolution endpoint for backward compatibility"""
    
    if not zip.isdigit() or len(zip) != 5:
        raise HTTPException(
            status_code=400,
            detail="ZIP must be exactly 5 digits"
        )
    
    try:
        geography_service = GeographyService()
        result = await geography_service.resolve_zip_legacy(zip, valuation_date)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Geography resolution failed: {str(e)}"
        )


@router.get("/traces", response_model=GeographyTraceSummary)
async def get_geography_traces(
    request: Request,
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)"),
    zip5: Optional[str] = Query(None, description="Filter by specific ZIP code"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of traces to return"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get geography resolution trace summary and statistics.
    
    Returns summary statistics for geography resolution traces including:
    - Total calls and match type breakdowns
    - Performance metrics (avg/p95 latency)
    - Coverage statistics (unique ZIPs/states)
    """
    
    try:
        from cms_pricing.database import SessionLocal
        from datetime import datetime
        
        db = SessionLocal()
        try:
            trace_service = GeographyTraceService(db)
            
            # Parse dates if provided
            start_dt = None
            end_dt = None
            if start_date:
                start_dt = datetime.fromisoformat(start_date)
            if end_date:
                end_dt = datetime.fromisoformat(end_date)
            
            # Get trace summary
            summary = trace_service.get_trace_summary(
                start_date=start_dt,
                end_date=end_dt,
                zip5=zip5
            )
            
            return GeographyTraceSummary(**summary)
            
        finally:
            db.close()
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve traces: {str(e)}"
        )


@router.get("/traces/recent")
async def get_recent_geography_traces(
    request: Request,
    limit: int = Query(10, ge=1, le=100, description="Number of recent traces to return"),
    zip5: Optional[str] = Query(None, description="Filter by specific ZIP code"),
    match_level: Optional[str] = Query(None, description="Filter by match level (zip+4, zip5, nearest, error)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get recent geography resolution traces for debugging and monitoring.
    
    Returns the most recent traces with full details including:
    - Input parameters and resolution results
    - Performance metrics and error information
    - Timestamps and service version
    """
    
    try:
        from cms_pricing.database import SessionLocal
        from cms_pricing.models.geography_trace import GeographyResolutionTrace
        from sqlalchemy import desc
        
        db = SessionLocal()
        try:
            query = db.query(GeographyResolutionTrace).order_by(desc(GeographyResolutionTrace.resolved_at))
            
            if zip5:
                query = query.filter(GeographyResolutionTrace.zip5 == zip5)
            if match_level:
                query = query.filter(GeographyResolutionTrace.match_level == match_level)
            
            traces = query.limit(limit).all()
            
            # Convert to response format
            trace_data = []
            for trace in traces:
                trace_data.append({
                    "id": str(trace.id),
                    "zip5": trace.zip5,
                    "plus4": trace.plus4,
                    "valuation_year": trace.valuation_year,
                    "quarter": trace.quarter,
                    "valuation_date": trace.valuation_date,
                    "strict": trace.strict == "true",
                    "match_level": trace.match_level,
                    "locality_id": trace.locality_id,
                    "state": trace.state,
                    "rural_flag": trace.rural_flag,
                    "nearest_zip": trace.nearest_zip,
                    "distance_miles": trace.distance_miles,
                    "dataset_digest": trace.dataset_digest,
                    "latency_ms": trace.latency_ms,
                    "service_version": trace.service_version,
                    "resolved_at": trace.resolved_at.isoformat(),
                    "error_message": trace.error_message,
                    "error_code": trace.error_code,
                    "inputs_json": trace.inputs_json,
                    "output_json": trace.output_json
                })
            
            return {
                "traces": trace_data,
                "count": len(trace_data),
                "limit": limit
            }
            
        finally:
            db.close()
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve recent traces: {str(e)}"
        )


@router.get("/traces/analytics")
async def get_geography_trace_analytics(
    request: Request,
    days: int = Query(7, ge=1, le=30, description="Number of days to analyze"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get geography resolution trace analytics for monitoring and optimization.
    
    Returns analytics including:
    - Performance trends over time
    - Match level distribution
    - Error patterns and rates
    - Geographic coverage analysis
    """
    
    try:
        from cms_pricing.database import SessionLocal
        from cms_pricing.models.geography_trace import GeographyResolutionTrace
        from sqlalchemy import func, desc
        from datetime import datetime, timedelta
        
        db = SessionLocal()
        try:
            # Calculate date range
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days)
            
            # Get traces in date range
            traces = db.query(GeographyResolutionTrace).filter(
                GeographyResolutionTrace.resolved_at >= start_date,
                GeographyResolutionTrace.resolved_at <= end_date
            ).all()
            
            if not traces:
                return {
                    "period_days": days,
                    "total_calls": 0,
                    "analytics": {
                        "performance": {"avg_latency_ms": 0, "p95_latency_ms": 0},
                        "match_distribution": {"zip+4": 0, "zip5": 0, "nearest": 0, "error": 0},
                        "error_analysis": {"error_rate": 0, "common_errors": []},
                        "geographic_coverage": {"unique_zips": 0, "unique_states": 0, "top_states": []},
                        "usage_patterns": {"strict_mode_rate": 0, "plus4_usage_rate": 0}
                    }
                }
            
            # Calculate analytics
            latencies = [t.latency_ms for t in traces if t.latency_ms is not None]
            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
            
            # Match level distribution
            match_counts = {}
            for trace in traces:
                match_level = trace.match_level or "unknown"
                match_counts[match_level] = match_counts.get(match_level, 0) + 1
            
            # Error analysis
            error_traces = [t for t in traces if t.match_level == "error"]
            error_rate = len(error_traces) / len(traces) if traces else 0
            
            # Common error codes
            error_codes = {}
            for trace in error_traces:
                if trace.error_code:
                    error_codes[trace.error_code] = error_codes.get(trace.error_code, 0) + 1
            
            common_errors = sorted(error_codes.items(), key=lambda x: x[1], reverse=True)[:5]
            
            # Geographic coverage
            unique_zips = len(set(t.zip5 for t in traces if t.zip5))
            unique_states = len(set(t.state for t in traces if t.state and t.state != ""))
            
            # Top states by usage
            state_counts = {}
            for trace in traces:
                if trace.state:
                    state_counts[trace.state] = state_counts.get(trace.state, 0) + 1
            
            top_states = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            # Usage patterns
            strict_mode_count = len([t for t in traces if t.strict == "true"])
            plus4_count = len([t for t in traces if t.plus4])
            
            strict_mode_rate = strict_mode_count / len(traces) if traces else 0
            plus4_usage_rate = plus4_count / len(traces) if traces else 0
            
            return {
                "period_days": days,
                "total_calls": len(traces),
                "analytics": {
                    "performance": {
                        "avg_latency_ms": round(avg_latency, 2),
                        "p95_latency_ms": round(p95_latency, 2)
                    },
                    "match_distribution": {
                        "zip+4": match_counts.get("zip+4", 0),
                        "zip5": match_counts.get("zip5", 0),
                        "nearest": match_counts.get("nearest", 0),
                        "error": match_counts.get("error", 0)
                    },
                    "error_analysis": {
                        "error_rate": round(error_rate * 100, 2),
                        "common_errors": [{"code": code, "count": count} for code, count in common_errors]
                    },
                    "geographic_coverage": {
                        "unique_zips": unique_zips,
                        "unique_states": unique_states,
                        "top_states": [{"state": state, "count": count} for state, count in top_states]
                    },
                    "usage_patterns": {
                        "strict_mode_rate": round(strict_mode_rate * 100, 2),
                        "plus4_usage_rate": round(plus4_usage_rate * 100, 2)
                    }
                }
            }
            
        finally:
            db.close()
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate analytics: {str(e)}"
        )


@router.post("/snapshots")
async def create_geography_snapshot(
    request: Request,
    snapshot_name: str,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    api_key: str = Depends(verify_api_key)
):
    """
    Create a new snapshot of current geography data for reproducibility testing.
    
    Creates a snapshot with:
    - Current dataset digest
    - Row counts and statistics
    - Effective date ranges
    - Metadata for tracking
    """
    
    try:
        from cms_pricing.database import SessionLocal
        
        db = SessionLocal()
        try:
            snapshot_service = GeographySnapshotService(db)
            
            snapshot = snapshot_service.create_snapshot(
                snapshot_name=snapshot_name,
                description=description,
                tags=tags
            )
            
            return {
                "snapshot_name": snapshot.name,
                "description": snapshot.description,
                "tags": snapshot.tags,
                "dataset_digest": snapshot.dataset_digest,
                "row_count": snapshot.row_count,
                "unique_zips": snapshot.unique_zips,
                "unique_states": snapshot.unique_states,
                "effective_date_range": snapshot.effective_date_range,
                "created_at": snapshot.created_at.isoformat()
            }
            
        finally:
            db.close()
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create snapshot: {str(e)}"
        )


@router.post("/snapshots/pin")
async def pin_dataset_digest(
    request: Request,
    digest: str,
    pin_name: Optional[str] = None,
    description: Optional[str] = None,
    api_key: str = Depends(verify_api_key)
):
    """
    Pin a specific dataset digest for reproducibility testing.
    
    Pins a digest to ensure consistent results across time.
    """
    
    try:
        from cms_pricing.database import SessionLocal
        
        db = SessionLocal()
        try:
            snapshot_service = GeographySnapshotService(db)
            
            snapshot = snapshot_service.pin_digest(
                digest=digest,
                pin_name=pin_name,
                description=description
            )
            
            return {
                "pin_name": snapshot.name,
                "description": snapshot.description,
                "tags": snapshot.tags,
                "dataset_digest": snapshot.dataset_digest,
                "row_count": snapshot.row_count,
                "unique_zips": snapshot.unique_zips,
                "unique_states": snapshot.unique_states,
                "effective_date_range": snapshot.effective_date_range,
                "created_at": snapshot.created_at.isoformat()
            }
            
        finally:
            db.close()
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to pin digest: {str(e)}"
        )


@router.get("/snapshots")
async def list_geography_snapshots(
    request: Request,
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    limit: int = Query(50, ge=1, le=100, description="Maximum snapshots to return"),
    api_key: str = Depends(verify_api_key)
):
    """
    List geography data snapshots with optional tag filtering.
    
    Returns snapshots with metadata for reproducibility tracking.
    """
    
    try:
        from cms_pricing.database import SessionLocal
        
        db = SessionLocal()
        try:
            snapshot_service = GeographySnapshotService(db)
            
            snapshots = snapshot_service.list_snapshots(tags=tags, limit=limit)
            
            snapshot_data = []
            for snapshot in snapshots:
                snapshot_data.append({
                    "name": snapshot.name,
                    "description": snapshot.description,
                    "tags": snapshot.tags,
                    "dataset_digest": snapshot.dataset_digest,
                    "row_count": snapshot.row_count,
                    "unique_zips": snapshot.unique_zips,
                    "unique_states": snapshot.unique_states,
                    "effective_date_range": snapshot.effective_date_range,
                    "created_at": snapshot.created_at.isoformat()
                })
            
            return {
                "snapshots": snapshot_data,
                "count": len(snapshot_data),
                "limit": limit
            }
            
        finally:
            db.close()
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list snapshots: {str(e)}"
        )


@router.get("/snapshots/{snapshot_name}")
async def get_geography_snapshot(
    request: Request,
    snapshot_name: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get details of a specific geography snapshot.
    
    Returns snapshot metadata and current reproducibility status.
    """
    
    try:
        from cms_pricing.database import SessionLocal
        
        db = SessionLocal()
        try:
            snapshot_service = GeographySnapshotService(db)
            
            snapshot = snapshot_service.get_snapshot(snapshot_name)
            if not snapshot:
                raise HTTPException(
                    status_code=404,
                    detail=f"Snapshot {snapshot_name} not found"
                )
            
            # Get current digest for comparison
            current_digest = snapshot_service._calculate_geography_digest()
            
            return {
                "name": snapshot.name,
                "description": snapshot.description,
                "tags": snapshot.tags,
                "dataset_digest": snapshot.dataset_digest,
                "current_digest": current_digest,
                "digest_match": snapshot.dataset_digest == current_digest,
                "row_count": snapshot.row_count,
                "unique_zips": snapshot.unique_zips,
                "unique_states": snapshot.unique_states,
                "effective_date_range": snapshot.effective_date_range,
                "created_at": snapshot.created_at.isoformat()
            }
            
        finally:
            db.close()
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get snapshot: {str(e)}"
        )


@router.post("/snapshots/{snapshot_name}/verify")
async def verify_snapshot_reproducibility(
    request: Request,
    snapshot_name: str,
    test_zips: Optional[List[str]] = None,
    api_key: str = Depends(verify_api_key)
):
    """
    Verify reproducibility of a snapshot by testing resolution consistency.
    
    Tests resolution consistency for specified ZIP codes to ensure
    the snapshot produces the same results as when it was created.
    """
    
    try:
        from cms_pricing.database import SessionLocal
        
        db = SessionLocal()
        try:
            snapshot_service = GeographySnapshotService(db)
            
            # Use default test ZIPs if none provided
            if not test_zips:
                test_zips = ["94110", "90210", "10001", "77002", "33101"]
            
            result = snapshot_service.verify_reproducibility(
                snapshot_name=snapshot_name,
                test_zips=test_zips
            )
            
            return result
            
        finally:
            db.close()
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify reproducibility: {str(e)}"
        )
