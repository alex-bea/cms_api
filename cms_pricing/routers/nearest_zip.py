"""API endpoints for nearest ZIP resolver"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session
from sqlalchemy import func

from cms_pricing.database import get_db
from cms_pricing.auth import verify_api_key
from cms_pricing.services.nearest_zip_resolver import NearestZipResolver
from cms_pricing.services.nearest_zip_monitoring import NearestZipMonitoring
from cms_pricing.services.nearest_zip_audit import NearestZipAudit
from cms_pricing.schemas.nearest_zip import (
    NearestZipRequest, NearestZipResponse, NearestZipTrace
)
import structlog

logger = structlog.get_logger()

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/nearest", response_model=NearestZipResponse)
async def find_nearest_zip(
    zip: str = Query(..., description="ZIP5 or ZIP9 (e.g., '94107' or '94107-1234')"),
    use_nber: bool = Query(True, description="Use NBER fast-path for distance calculations"),
    max_radius_miles: float = Query(100.0, ge=1.0, le=500.0, description="Maximum search radius in miles"),
    include_trace: bool = Query(False, description="Include full trace in response"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Find the nearest non-PO Box ZIP5 in the same CMS state.
    
    Implements the ZIP+4-first hierarchy per PRD:
    1. ZIP+4 exact match (if provided)
    2. ZIP5 exact match
    3. Nearest ZIP within same state (with PO Box filtering)
    4. Distance-based selection with tie-breaking
    """
    
    # Validate input
    if not zip or len(zip.strip()) == 0:
        raise HTTPException(status_code=400, detail="ZIP parameter is required")
    
    # Basic format validation
    import re
    digits = re.sub(r'[^\d]', '', zip)
    if len(digits) not in [5, 9]:
        raise HTTPException(
            status_code=400, 
            detail="ZIP must be 5 or 9 digits (e.g., '94107' or '94107-1234')"
        )
    
    try:
        resolver = NearestZipResolver(db)
        result = resolver.find_nearest_zip(
            input_zip=zip,
            use_nber=use_nber,
            max_radius_miles=max_radius_miles,
            include_trace=include_trace
        )
        
        response_data = {
            'nearest_zip': result['nearest_zip'],
            'distance_miles': result['distance_miles'],
            'input_zip': zip
        }
        
        if include_trace:
            response_data['trace'] = result['trace']
        
        return NearestZipResponse(**response_data)
        
    except ValueError as e:
        if str(e) == "NO_CANDIDATES_IN_STATE":
            logger.warning(f"No candidates found in state for ZIP: {zip}")
            raise HTTPException(status_code=422, detail=str(e))
        else:
            logger.warning(f"Invalid ZIP input: {zip}, error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Nearest ZIP resolution failed for {zip}: {e}")
        raise HTTPException(
            status_code=500, 
            detail="Internal server error during ZIP resolution"
        )


@router.post("/nearest", response_model=NearestZipResponse)
async def find_nearest_zip_post(
    request: NearestZipRequest,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Find the nearest non-PO Box ZIP5 in the same CMS state (POST version).
    
    Same functionality as GET endpoint but accepts request body for more complex parameters.
    """
    
    try:
        resolver = NearestZipResolver(db)
        result = resolver.find_nearest_zip(
            input_zip=request.zip,
            use_nber=request.use_nber,
            max_radius_miles=request.max_radius_miles,
            include_trace=request.include_trace
        )
        
        response_data = {
            'nearest_zip': result['nearest_zip'],
            'distance_miles': result['distance_miles'],
            'input_zip': request.zip
        }
        
        if request.include_trace:
            response_data['trace'] = result['trace']
        
        return NearestZipResponse(**response_data)
        
    except ValueError as e:
        if str(e) == "NO_CANDIDATES_IN_STATE":
            logger.warning(f"No candidates found in state for ZIP: {request.zip}")
            raise HTTPException(status_code=422, detail=str(e))
        else:
            logger.warning(f"Invalid ZIP input: {request.zip}, error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Nearest ZIP resolution failed for {request.zip}: {e}")
        raise HTTPException(
            status_code=500, 
            detail="Internal server error during ZIP resolution"
        )


@router.get("/traces/{trace_id}", response_model=NearestZipTrace)
async def get_trace(
    trace_id: str,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get trace information for a specific ZIP resolution"""
    
    from cms_pricing.models.nearest_zip import NearestZipTrace as TraceModel
    from sqlalchemy import text
    
    try:
        # Convert trace_id to UUID if it's a string representation
        import uuid
        trace_uuid = uuid.UUID(trace_id)
        
        trace_record = db.query(TraceModel).filter(
            TraceModel.id == trace_uuid
        ).first()
        
        if not trace_record:
            raise HTTPException(status_code=404, detail="Trace not found")
        
        return NearestZipTrace(
            id=str(trace_record.id),
            input_zip=trace_record.input_zip,
            input_zip5=trace_record.input_zip5,
            input_zip9=trace_record.input_zip9,
            result_zip=trace_record.result_zip,
            distance_miles=trace_record.distance_miles,
            trace_json=trace_record.trace_json,
            created_at=trace_record.created_at
        )
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid trace ID format")
    except Exception as e:
        logger.error(f"Failed to retrieve trace {trace_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats")
async def get_resolver_stats(
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get resolver statistics and health information"""
    
    try:
        monitoring = NearestZipMonitoring(db)
        
        # Get comprehensive statistics
        stats = {
            'data_sources': monitoring.get_data_source_counts(),
            'resolver_stats': monitoring.get_resolver_statistics(),
            'gazetteer_fallback': monitoring.get_gazetteer_fallback_rate()
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to retrieve resolver stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/audit/pobox")
async def audit_pobox_exclusions(
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Audit PO Box exclusions and validation"""
    
    try:
        audit_service = NearestZipAudit(db)
        audit_results = audit_service.run_comprehensive_audit()
        
        return audit_results
        
    except Exception as e:
        logger.error(f"PO Box audit failed: {e}")
        raise HTTPException(status_code=500, detail="PO Box audit failed")


@router.get("/health")
async def health_check(
    db: Session = Depends(get_db)
):
    """Health check for nearest ZIP resolver"""
    
    from cms_pricing.models.nearest_zip import ZCTACoords, ZipToZCTA, CMSZipLocality
    
    try:
        # Check if required data is available
        zcta_count = db.query(func.count(ZCTACoords.zcta5)).scalar() or 0
        zip_to_zcta_count = db.query(func.count(ZipToZCTA.zip5)).scalar() or 0
        cms_zip_count = db.query(func.count(CMSZipLocality.zip5)).scalar() or 0
        
        # Basic health check - need at least some data
        if zcta_count == 0 or zip_to_zcta_count == 0 or cms_zip_count == 0:
            return {
                'status': 'unhealthy',
                'message': 'Required data sources not loaded',
                'details': {
                    'zcta_coords': zcta_count,
                    'zip_to_zcta': zip_to_zcta_count,
                    'cms_zip_locality': cms_zip_count
                }
            }
        
        return {
            'status': 'healthy',
            'message': 'All required data sources available',
            'details': {
                'zcta_coords': zcta_count,
                'zip_to_zcta': zip_to_zcta_count,
                'cms_zip_locality': cms_zip_count
            }
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'message': f'Health check failed: {str(e)}'
        }
