"""
Health endpoint and snapshot management implementation

Implements health check endpoint and snapshot registry per PRD requirements.
"""

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import structlog

from cms_pricing.database import SessionLocal, get_db
from cms_pricing.models.geography import Geography

logger = structlog.get_logger()

router = APIRouter()

class GeographyHealthService:
    """Service for geography health check endpoint"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
        self.start_time = datetime.now(timezone.utc)
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status per PRD Section 5"""
        
        # Get active snapshot info
        active_snapshot = self._get_active_snapshot()
        
        # Determine status
        status = self._determine_status(active_snapshot)
        
        # Calculate uptime
        uptime_seconds = int((datetime.now(timezone.utc) - self.start_time).total_seconds())
        
        return {
            "status": status,
            "build": {
                "version": "geo-resolver@1.0.0",
                "commit": "dev-build"
            },
            "active_snapshot": active_snapshot,
            "perf_slo": {
                "p95_warm_ms": 2,
                "p95_cold_ms": 20
            },
            "uptime_seconds": uptime_seconds,
            "notes": ["post-ga etag not enabled"]
        }
    
    def _get_active_snapshot(self) -> Dict[str, Any]:
        """Get active snapshot information"""
        
        # Get latest geography data
        latest_record = self.db.query(Geography).order_by(
            Geography.effective_from.desc()
        ).first()
        
        if latest_record:
            return {
                "dataset_id": latest_record.dataset_id,
                "dataset_digest": latest_record.dataset_digest,
                "effective_from": latest_record.effective_from.isoformat() + "T00:00:00Z",
                "effective_to": latest_record.effective_to.isoformat() + "T23:59:59Z" if latest_record.effective_to else None
            }
        else:
            return {
                "dataset_id": "ZIP_LOCALITY",
                "dataset_digest": "none",
                "effective_from": None,
                "effective_to": None
            }
    
    def _determine_status(self, active_snapshot: Dict[str, Any]) -> str:
        """Determine health status per PRD semantics"""
        
        if active_snapshot["dataset_digest"] == "none":
            return "error"
        
        # Check if snapshot is active for current period
        current_date = datetime.now().date()
        
        if active_snapshot["effective_from"]:
            effective_from = datetime.fromisoformat(active_snapshot["effective_from"].replace("Z", "")).date()
            if current_date < effective_from:
                return "error"
        
        if active_snapshot["effective_to"]:
            effective_to = datetime.fromisoformat(active_snapshot["effective_to"].replace("Z", "")).date()
            if current_date > effective_to:
                return "error"
        
        # TODO: Check performance SLOs
        # For now, assume OK if snapshot exists
        return "ok"


@router.get("/geo/healthz")
async def get_geography_health(db: Session = Depends(get_db)):
    """Health check endpoint per PRD Section 5"""
    
    try:
        health_service = GeographyHealthService(db=db)
        health_data = health_service.get_health_status()
        
        # Return appropriate status code per PRD
        if health_data["status"] == "error":
            raise HTTPException(status_code=503, detail=health_data)
        else:
            return health_data
            
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=503, detail={
            "status": "error",
            "error": str(e)
        })


class SnapshotRegistry:
    """Snapshot registry for dataset management per PRD Section 4.1"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
    
    def register_snapshot(self, 
                        dataset_id: str,
                        effective_from: datetime,
                        effective_to: datetime,
                        dataset_digest: str,
                        source_url: str) -> Dict[str, Any]:
        """Register a new snapshot"""
        
        # TODO: Implement snapshots table
        # For now, return mock data
        return {
            "dataset_id": dataset_id,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "dataset_digest": dataset_digest,
            "source_url": source_url,
            "published_at": datetime.now(timezone.utc)
        }
    
    def get_active_snapshot(self, valuation_date: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """Get active snapshot for valuation date"""
        
        if valuation_date is None:
            valuation_date = datetime.now(timezone.utc)
        
        # TODO: Implement proper snapshot selection logic
        # For now, return latest geography data
        latest_record = self.db.query(Geography).order_by(
            Geography.effective_from.desc()
        ).first()
        
        if latest_record:
            return {
                "dataset_id": latest_record.dataset_id,
                "dataset_digest": latest_record.dataset_digest,
                "effective_from": latest_record.effective_from,
                "effective_to": latest_record.effective_to
            }
        
        return None
    
    def set_active_snapshot(self, dataset_digest: str) -> bool:
        """Set active snapshot by digest"""
        
        # TODO: Implement active snapshot management
        # For now, just log the operation
        logger.info("Setting active snapshot", dataset_digest=dataset_digest)
        return True


class StructuredTracer:
    """Structured tracing per PRD Section 10"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
    
    def trace_resolution(self, 
                        inputs: Dict[str, Any],
                        result: Dict[str, Any],
                        latency_ms: float,
                        service_version: str = "geo-resolver@1.0.0"):
        """Emit structured trace for resolution"""
        
        trace_data = {
            "trace_type": "geo_resolution",
            "inputs": inputs,
            "output": result,
            "match_level": result.get("match_level"),
            "locality_id": result.get("locality_id"),
            "state": result.get("state"),
            "snapshot_digest": result.get("dataset_digest"),
            "latency_ms": latency_ms,
            "service_version": service_version,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Add nearest info if applicable
        if result.get("match_level") == "nearest":
            trace_data["nearest"] = {
                "candidate_zip": result.get("candidate_zip"),
                "candidate_distance_miles": result.get("candidate_distance_miles")
            }
        
        self.logger.info("Geography resolution trace", **trace_data)


# Export services for use in main application
__all__ = ["GeographyHealthService", "SnapshotRegistry", "StructuredTracer", "router"]
