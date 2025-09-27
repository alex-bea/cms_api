"""Trace and audit endpoints"""

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from cms_pricing.schemas.trace import TraceResponse
from cms_pricing.auth import verify_api_key, is_admin_key
from cms_pricing.services.trace import TraceService
from cms_pricing.database import get_db

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/{run_id}", response_model=TraceResponse)
async def get_trace(
    request: Request,
    run_id: str,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get full trace information for a pricing run"""
    
    try:
        trace_service = TraceService(db)
        result = await trace_service.get_trace(run_id)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Trace not found"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Trace retrieval failed: {str(e)}"
        )


@router.get("/{run_id}/replay")
async def replay_run(
    request: Request,
    run_id: str,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Replay a pricing run with identical parameters"""
    
    # Check admin privileges for replay
    if not is_admin_key(api_key):
        raise HTTPException(
            status_code=403,
            detail="Admin privileges required for replay"
        )
    
    try:
        trace_service = TraceService()
        result = await trace_service.replay_run(run_id)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Run not found or cannot be replayed"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Run replay failed: {str(e)}"
        )
