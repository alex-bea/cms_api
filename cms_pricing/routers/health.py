"""Health check endpoints"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from cms_pricing.database import get_db
from cms_pricing.cache import CacheManager

router = APIRouter()


@router.get("/healthz")
async def health_check():
    """Basic health check"""
    return {"status": "healthy", "service": "cms-pricing-api"}


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
