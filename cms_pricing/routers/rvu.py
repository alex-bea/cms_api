"""
RVU Data API Router

Provides read-only endpoints for RVU data with latency SLOs
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Query, Depends, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
from cms_pricing.schemas.rvu import (
    ReleaseResponse, RVUItemResponse, GPCIIndexResponse, 
    OPPSCapResponse, AnesCFResponse, LocalityCountyResponse,
    RVUSearchRequest, RVUSearchResponse
)
from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
import logging
import uuid
import time
import hashlib
import structlog

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/rvu", tags=["RVU Data"])


def get_correlation_id(request: Request) -> str:
    """Get or generate correlation ID per Global API Program standards"""
    correlation_id = request.headers.get("X-Correlation-Id")
    if not correlation_id:
        correlation_id = str(uuid.uuid4())
    return correlation_id


def create_api_response(data: Any, meta: Dict[str, Any] = None, trace: Dict[str, Any] = None) -> Dict[str, Any]:
    """Create standardized API response per Global API Program standards"""
    response = {
        "data": data,
        "meta": meta or {},
        "trace": trace or {}
    }
    return response


def create_error_response(error_code: str, message: str, details: List[Dict[str, str]] = None, 
                        correlation_id: str = None) -> JSONResponse:
    """Create standardized error response per Global API Program standards"""
    error_response = {
        "error": {
            "code": error_code,
            "message": message,
            "details": details or []
        },
        "trace": {
            "correlation_id": correlation_id or str(uuid.uuid4())
        }
    }
    return JSONResponse(status_code=400, content=error_response)


def get_db():
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/healthz")
async def health_check():
    """Health check endpoint per Global API Program standards"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@router.get("/readyz")
async def readiness_check(db: Session = Depends(get_db)):
    """Readiness check endpoint per Global API Program standards"""
    try:
        # Check database connectivity
        from sqlalchemy import text
        db.execute(text("SELECT 1"))
        return {"status": "ready", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/releases")
async def get_releases(
    request: Request,
    limit: int = Query(10, ge=1, le=100, description="Number of releases to return"),
    offset: int = Query(0, ge=0, description="Number of releases to skip"),
    source_version: Optional[str] = Query(None, description="Filter by source version"),
    db: Session = Depends(get_db)
):
    """Get list of RVU releases"""
    
    start_time = time.time()
    correlation_id = get_correlation_id(request)
    
    try:
        query = db.query(Release)
        
        if source_version:
            query = query.filter(Release.source_version == source_version)
        
        releases = query.order_by(Release.imported_at.desc()).offset(offset).limit(limit).all()
        
        # Create response data
        data = [
            ReleaseResponse(
                id=str(release.id),
                type=release.type,
                source_version=release.source_version,
                source_url=release.source_url,
                imported_at=release.imported_at,
                published_at=release.published_at,
                notes=release.notes
            )
            for release in releases
        ]
        
        # Create metadata
        meta = {
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": len(data)
            },
            "filters": {
                "source_version": source_version
            }
        }
        
        # Create trace information
        trace = {
            "correlation_id": correlation_id,
            "vintage": "2025-Q1",  # This would come from active vintage
            "hash": hashlib.sha256(str(data).encode()).hexdigest()[:16],
            "latency_ms": round((time.time() - start_time) * 1000, 2)
        }
        
        return create_api_response(data, meta, trace)
    
    except Exception as e:
        logger.error(f"Failed to get releases: {e}", correlation_id=correlation_id)
        return create_error_response(
            "RELEASES_FETCH_ERROR", 
            "Failed to fetch releases", 
            [{"field": "releases", "issue": str(e)}],
            correlation_id
        )


@router.get("/scraper/files")
async def get_available_files(
    request: Request,
    start_year: int = Query(None, ge=2003, le=2025, description="Starting year for file discovery (defaults to current year if latest_only=True)"),
    end_year: int = Query(None, ge=2003, le=2025, description="Ending year for file discovery (defaults to current year if latest_only=True)"),
    use_scraper: bool = Query(True, description="Whether to use scraper for discovery"),
    latest_only: bool = Query(True, description="Whether to only get the latest available files (default: True)")
):
    """Get available RVU files using the scraper"""
    
    start_time = time.time()
    correlation_id = get_correlation_id(request)
    
    try:
        # Initialize RVU ingestor
        ingestor = RVUIngestor("./data/rvu_scraper")
        
        # Discover files
        source_files = await ingestor.discover_and_download_files(
            start_year=start_year,
            end_year=end_year,
            use_scraper=use_scraper,
            latest_only=latest_only
        )
        
        # Convert to response format
        data = []
        for sf in source_files:
            data.append({
                "filename": sf.filename,
                "url": sf.url,
                "content_type": sf.content_type,
                "expected_size_bytes": sf.expected_size_bytes,
                "last_modified": sf.last_modified.isoformat() if sf.last_modified else None,
                "checksum": sf.checksum
            })
        
        # Implement cursor-based pagination per Global API Program PRD §1.3
        page_size = min(100, len(data))  # Default page size
        next_cursor = None
        
        if len(data) > page_size:
            # Simple cursor implementation - in production would use actual cursor
            next_cursor = f"page_{1}_{page_size}"
        
        # Create metadata
        meta = {
            "discovery_method": "scraper" if use_scraper else "hardcoded",
            "latest_only": latest_only,
            "year_range": {
                "start": start_year,
                "end": end_year
            } if start_year and end_year else "latest",
            "pagination": {
                "page_size": page_size,
                "next_cursor": next_cursor,
                "total_count": len(data)
            }
        }
        
        # Create trace information
        trace = {
            "correlation_id": correlation_id,
            "latency_ms": round((time.time() - start_time) * 1000, 2)
        }
        
        return create_api_response(data, meta, trace)
        
    except Exception as e:
        logger.error("Failed to get available files", error=str(e), correlation_id=correlation_id)
        return create_error_response(
            "FILES_DISCOVERY_ERROR",
            "Failed to discover files",
            [{"field": "files", "issue": str(e)}],
            correlation_id
        )


@router.post("/scraper/download-historical")
async def download_historical_data(
    request: Request,
    start_year: int = Query(2003, ge=2003, le=2025, description="Starting year for historical data"),
    end_year: int = Query(2025, ge=2003, le=2025, description="Ending year for historical data")
):
    """Download historical RVU data using the scraper"""
    
    start_time = time.time()
    correlation_id = get_correlation_id(request)
    
    try:
        # Initialize RVU ingestor
        ingestor = RVUIngestor("./data/rvu_scraper")
        
        # Download historical data
        result = await ingestor.download_historical_data(
            start_year=start_year,
            end_year=end_year
        )
        
        # Create response data
        data = {
            "status": result.get("status"),
            "files_found": result.get("files_found", 0),
            "downloads_completed": result.get("downloads_completed", 0),
            "downloads_failed": result.get("downloads_failed", 0),
            "manifest_path": result.get("manifest_path"),
            "data_directory": result.get("data_directory")
        }
        
        # Create metadata
        meta = {
            "year_range": {
                "start": start_year,
                "end": end_year
            },
            "operation": "historical_download"
        }
        
        # Create trace information
        trace = {
            "correlation_id": correlation_id,
            "latency_ms": round((time.time() - start_time) * 1000, 2)
        }
        
        return create_api_response(data, meta, trace)
        
    except Exception as e:
        logger.error("Failed to download historical data", error=str(e), correlation_id=correlation_id)
        return create_error_response(
            "HISTORICAL_DOWNLOAD_ERROR",
            "Failed to download historical data",
            [{"field": "historical_data", "issue": str(e)}],
            correlation_id
        )


@router.post("/scraper/ingest-from-scraped")
async def ingest_from_scraped_data(
    request: Request,
    release_id: str = Query(..., description="Release identifier"),
    batch_id: str = Query(..., description="Batch identifier"),
    start_year: int = Query(None, ge=2003, le=2025, description="Starting year for file discovery (defaults to current year if latest_only=True)"),
    end_year: int = Query(None, ge=2003, le=2025, description="Ending year for file discovery (defaults to current year if latest_only=True)"),
    latest_only: bool = Query(True, description="Whether to only ingest the latest available files (default: True)")
):
    """Ingest data using files discovered by the scraper"""
    
    start_time = time.time()
    correlation_id = get_correlation_id(request)
    
    try:
        # Initialize RVU ingestor
        ingestor = RVUIngestor("./data/rvu_scraper")
        
        # Ingest from scraped data
        result = await ingestor.ingest_from_scraped_data(
            release_id=release_id,
            batch_id=batch_id,
            start_year=start_year,
            end_year=end_year,
            latest_only=latest_only
        )
        
        # Create response data
        data = {
            "status": result.get("status"),
            "release_id": result.get("release_id"),
            "batch_id": result.get("batch_id"),
            "record_count": result.get("record_count", 0),
            "quality_score": result.get("quality_score", 0),
            "scraper_metadata": result.get("scraper_metadata", {})
        }
        
        # Create metadata
        meta = {
            "latest_only": latest_only,
            "year_range": {
                "start": start_year,
                "end": end_year
            } if start_year and end_year else "latest",
            "operation": "scraped_ingestion"
        }
        
        # Create trace information
        trace = {
            "correlation_id": correlation_id,
            "latency_ms": round((time.time() - start_time) * 1000, 2)
        }
        
        return create_api_response(data, meta, trace)
        
    except Exception as e:
        logger.error("Failed to ingest from scraped data", error=str(e), correlation_id=correlation_id)
        return create_error_response(
            "SCRAPED_INGESTION_ERROR",
            "Failed to ingest from scraped data",
            [{"field": "ingestion", "issue": str(e)}],
            correlation_id
        )


@router.get("/releases/{release_id}", response_model=ReleaseResponse)
async def get_release(
    release_id: str,
    db: Session = Depends(get_db)
):
    """Get specific release by ID"""
    
    try:
        release = db.query(Release).filter(Release.id == release_id).first()
        
        if not release:
            raise HTTPException(status_code=404, detail="Release not found")
        
        return ReleaseResponse(
            id=str(release.id),
            type=release.type,
            source_version=release.source_version,
            source_url=release.source_url,
            imported_at=release.imported_at,
            published_at=release.published_at,
            notes=release.notes
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get release {release_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/rvu-items", response_model=RVUSearchResponse)
async def search_rvu_items(
    hcpcs_code: Optional[str] = Query(None, description="HCPCS code to search for"),
    status_code: Optional[str] = Query(None, description="Status code filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    release_id: Optional[str] = Query(None, description="Filter by release ID"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(get_db)
):
    """Search RVU items with filters"""
    
    try:
        query = db.query(RVUItem)
        
        # Apply filters
        if hcpcs_code:
            query = query.filter(RVUItem.hcpcs_code == hcpcs_code)
        
        if status_code:
            query = query.filter(RVUItem.status_code == status_code)
        
        if effective_date:
            query = query.filter(
                RVUItem.effective_start <= effective_date,
                RVUItem.effective_end >= effective_date
            )
        
        if release_id:
            query = query.filter(RVUItem.release_id == release_id)
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        items = query.order_by(RVUItem.hcpcs_code).offset(offset).limit(limit).all()
        
        return RVUSearchResponse(
            items=[
                RVUItemResponse(
                    id=str(item.id),
                    hcpcs_code=item.hcpcs_code,
                    modifiers=item.modifiers,
                    description=item.description,
                    status_code=item.status_code,
                    work_rvu=item.work_rvu,
                    pe_rvu_nonfac=item.pe_rvu_nonfac,
                    pe_rvu_fac=item.pe_rvu_fac,
                    mp_rvu=item.mp_rvu,
                    na_indicator=item.na_indicator,
                    global_days=item.global_days,
                    bilateral_ind=item.bilateral_ind,
                    multiple_proc_ind=item.multiple_proc_ind,
                    assistant_surg_ind=item.assistant_surg_ind,
                    co_surg_ind=item.co_surg_ind,
                    team_surg_ind=item.team_surg_ind,
                    endoscopic_base=item.endoscopic_base,
                    conversion_factor=item.conversion_factor,
                    physician_supervision=item.physician_supervision,
                    diag_imaging_family=item.diag_imaging_family,
                    total_nonfac=item.total_nonfac,
                    total_fac=item.total_fac,
                    effective_start=item.effective_start,
                    effective_end=item.effective_end,
                    source_file=item.source_file,
                    row_num=item.row_num
                )
                for item in items
            ],
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    
    except Exception as e:
        logger.error(f"Failed to search RVU items: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/rvu-items/{item_id}", response_model=RVUItemResponse)
async def get_rvu_item(
    item_id: str,
    db: Session = Depends(get_db)
):
    """Get specific RVU item by ID"""
    
    try:
        item = db.query(RVUItem).filter(RVUItem.id == item_id).first()
        
        if not item:
            raise HTTPException(status_code=404, detail="RVU item not found")
        
        return RVUItemResponse(
            id=str(item.id),
            hcpcs_code=item.hcpcs_code,
            modifiers=item.modifiers,
            description=item.description,
            status_code=item.status_code,
            work_rvu=item.work_rvu,
            pe_rvu_nonfac=item.pe_rvu_nonfac,
            pe_rvu_fac=item.pe_rvu_fac,
            mp_rvu=item.mp_rvu,
            na_indicator=item.na_indicator,
            global_days=item.global_days,
            bilateral_ind=item.bilateral_ind,
            multiple_proc_ind=item.multiple_proc_ind,
            assistant_surg_ind=item.assistant_surg_ind,
            co_surg_ind=item.co_surg_ind,
            team_surg_ind=item.team_surg_ind,
            endoscopic_base=item.endoscopic_base,
            conversion_factor=item.conversion_factor,
            physician_supervision=item.physician_supervision,
            diag_imaging_family=item.diag_imaging_family,
            total_nonfac=item.total_nonfac,
            total_fac=item.total_fac,
            effective_start=item.effective_start,
            effective_end=item.effective_end,
            source_file=item.source_file,
            row_num=item.row_num
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get RVU item {item_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/gpci", response_model=List[GPCIIndexResponse])
async def get_gpci_indices(
    mac: Optional[str] = Query(None, description="MAC filter"),
    state: Optional[str] = Query(None, description="State filter"),
    locality_id: Optional[str] = Query(None, description="Locality ID filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(get_db)
):
    """Get GPCI indices with filters"""
    
    try:
        query = db.query(GPCIIndex)
        
        # Apply filters
        if mac:
            query = query.filter(GPCIIndex.mac == mac)
        
        if state:
            query = query.filter(GPCIIndex.state == state)
        
        if locality_id:
            query = query.filter(GPCIIndex.locality_id == locality_id)
        
        if effective_date:
            query = query.filter(
                GPCIIndex.effective_start <= effective_date,
                GPCIIndex.effective_end >= effective_date
            )
        
        # Apply pagination
        items = query.order_by(GPCIIndex.mac, GPCIIndex.locality_id).offset(offset).limit(limit).all()
        
        return [
            GPCIIndexResponse(
                id=str(item.id),
                mac=item.mac,
                state=item.state,
                locality_id=item.locality_id,
                locality_name=item.locality_name,
                work_gpci=item.work_gpci,
                pe_gpci=item.pe_gpci,
                mp_gpci=item.mp_gpci,
                effective_start=item.effective_start,
                effective_end=item.effective_end,
                source_file=item.source_file,
                row_num=item.row_num
            )
            for item in items
        ]
    
    except Exception as e:
        logger.error(f"Failed to get GPCI indices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/opps-caps", response_model=List[OPPSCapResponse])
async def get_opps_caps(
    hcpcs_code: Optional[str] = Query(None, description="HCPCS code filter"),
    mac: Optional[str] = Query(None, description="MAC filter"),
    locality_id: Optional[str] = Query(None, description="Locality ID filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(get_db)
):
    """Get OPPS caps with filters"""
    
    try:
        query = db.query(OPPSCap)
        
        # Apply filters
        if hcpcs_code:
            query = query.filter(OPPSCap.hcpcs_code == hcpcs_code)
        
        if mac:
            query = query.filter(OPPSCap.mac == mac)
        
        if locality_id:
            query = query.filter(OPPSCap.locality_id == locality_id)
        
        if effective_date:
            query = query.filter(
                OPPSCap.effective_start <= effective_date,
                OPPSCap.effective_end >= effective_date
            )
        
        # Apply pagination
        items = query.order_by(OPPSCap.hcpcs_code, OPPSCap.mac).offset(offset).limit(limit).all()
        
        return [
            OPPSCapResponse(
                id=str(item.id),
                hcpcs_code=item.hcpcs_code,
                modifier=item.modifier,
                proc_status=item.proc_status,
                mac=item.mac,
                locality_id=item.locality_id,
                price_fac=item.price_fac,
                price_nonfac=item.price_nonfac,
                effective_start=item.effective_start,
                effective_end=item.effective_end,
                source_file=item.source_file,
                row_num=item.row_num
            )
            for item in items
        ]
    
    except Exception as e:
        logger.error(f"Failed to get OPPS caps: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/anes-cfs", response_model=List[AnesCFResponse])
async def get_anes_cfs(
    mac: Optional[str] = Query(None, description="MAC filter"),
    locality_id: Optional[str] = Query(None, description="Locality ID filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(get_db)
):
    """Get anesthesia conversion factors with filters"""
    
    try:
        query = db.query(AnesCF)
        
        # Apply filters
        if mac:
            query = query.filter(AnesCF.mac == mac)
        
        if locality_id:
            query = query.filter(AnesCF.locality_id == locality_id)
        
        if effective_date:
            query = query.filter(
                AnesCF.effective_start <= effective_date,
                AnesCF.effective_end >= effective_date
            )
        
        # Apply pagination
        items = query.order_by(AnesCF.mac, AnesCF.locality_id).offset(offset).limit(limit).all()
        
        return [
            AnesCFResponse(
                id=str(item.id),
                mac=item.mac,
                locality_id=item.locality_id,
                locality_name=item.locality_name,
                anesthesia_cf=item.anesthesia_cf,
                effective_start=item.effective_start,
                effective_end=item.effective_end,
                source_file=item.source_file,
                row_num=item.row_num
            )
            for item in items
        ]
    
    except Exception as e:
        logger.error(f"Failed to get anesthesia CFs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/locality-counties", response_model=List[LocalityCountyResponse])
async def get_locality_counties(
    mac: Optional[str] = Query(None, description="MAC filter"),
    state: Optional[str] = Query(None, description="State filter"),
    locality_id: Optional[str] = Query(None, description="Locality ID filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(get_db)
):
    """Get locality-county mappings with filters"""
    
    try:
        query = db.query(LocalityCounty)
        
        # Apply filters
        if mac:
            query = query.filter(LocalityCounty.mac == mac)
        
        if state:
            query = query.filter(LocalityCounty.state == state)
        
        if locality_id:
            query = query.filter(LocalityCounty.locality_id == locality_id)
        
        if effective_date:
            query = query.filter(
                LocalityCounty.effective_start <= effective_date,
                LocalityCounty.effective_end >= effective_date
            )
        
        # Apply pagination
        items = query.order_by(LocalityCounty.mac, LocalityCounty.locality_id).offset(offset).limit(limit).all()
        
        return [
            LocalityCountyResponse(
                id=str(item.id),
                mac=item.mac,
                locality_id=item.locality_id,
                state=item.state,
                fee_schedule_area=item.fee_schedule_area,
                county_name=item.county_name,
                effective_start=item.effective_start,
                effective_end=item.effective_end,
                source_file=item.source_file,
                row_num=item.row_num
            )
            for item in items
        ]
    
    except Exception as e:
        logger.error(f"Failed to get locality counties: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "RVU Data API",
        "version": "1.0.0"
    }

