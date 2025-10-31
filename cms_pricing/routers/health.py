"""Health check endpoints"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from sqlalchemy.exc import ProgrammingError, OperationalError
from cms_pricing.database import get_db
from cms_pricing.cache import CacheManager
from cms_pricing.models.dataset_snapshots import DatasetSnapshot

router = APIRouter()


@router.get("/health")
async def health_check():
    """Basic health check (OpenAPI contract endpoint)"""
    return {"status": "healthy", "service": "cms-pricing-api"}


@router.get("/healthz")
async def health_check_legacy():
    """Basic health check (legacy endpoint for Docker)"""
    return {"status": "healthy", "service": "cms-pricing-api"}


@router.get("/snapshots/health")
async def snapshots_health(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Health check for dataset snapshots registry.
    
    Returns summary of available snapshots per dataset.
    Part of Quick Win #1: Dataset Snapshots Table
    """
    try:
        # Single aggregation query for efficiency (computes total in DB)
        result = db.query(
            DatasetSnapshot.dataset_id,
            func.count(DatasetSnapshot.release_id).label('count'),
            func.sum(func.count(DatasetSnapshot.release_id)).over().label('total')
        ).group_by(DatasetSnapshot.dataset_id).first()
        
        if result:
            # Get all dataset counts
            counts = db.query(
                DatasetSnapshot.dataset_id,
                func.count(DatasetSnapshot.release_id).label('count')
            ).group_by(DatasetSnapshot.dataset_id).all()
            
            snapshot_summary = {
                dataset_id: {"count": count}
                for dataset_id, count in counts
            }
            
            # Total already computed or sum from results
            total_snapshots = sum(c.count for c in counts)
        else:
            snapshot_summary = {}
            total_snapshots = 0
        
        return {
            "status": "healthy",
            "total_snapshots": total_snapshots,
            "datasets": snapshot_summary
        }
    except (ProgrammingError, OperationalError) as e:
        # If table doesn't exist yet, return graceful error
        if "does not exist" in str(e) or "relation" in str(e).lower():
            return {
                "status": "not_initialized",
                "message": "dataset_snapshots table not yet created",
                "total_snapshots": 0,
                "datasets": {}
            }
        raise HTTPException(status_code=500, detail=f"Snapshot health check failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snapshot health check failed: {str(e)}")


@router.get("/readyz")
async def readiness_check(
    db: Session = Depends(get_db),
    cache_manager: CacheManager = Depends(lambda: CacheManager())
):
    """Readiness check with dependencies"""
    try:
        # Check database connection
        db.execute(text("SELECT 1"))
        
        # Check cache directory
        cache_manager.disk_cache.cache_dir
        
        return {
            "status": "ready",
            "service": "cms-pricing-api",
            "dependencies": {
                "database": "healthy",
                "cache": "healthy"
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service not ready: {str(e)}"
        )
