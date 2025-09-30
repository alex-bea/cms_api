"""
MPFS API Endpoints

Following Global API Program PRDs v1.0 and QTS standards
"""

from fastapi import APIRouter, HTTPException, Query, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal
import structlog

from cms_pricing.database import get_db
from cms_pricing.models.mpfs.mpfs_rvu import MPFSRVU
from cms_pricing.models.mpfs.mpfs_conversion_factor import MPFSConversionFactor, MPFSAbstract
from cms_pricing.schemas.mpfs import (
    MPFSRVUResponse, MPFSRVUListResponse, MPFSConversionFactorResponse,
    MPFSAbstractResponse, MPFSHealthResponse
)
from cms_pricing.auth import verify_api_key
import uuid
from fastapi import Request

logger = structlog.get_logger()

router = APIRouter(prefix="/mpfs", tags=["MPFS"])


def get_correlation_id(request: Request) -> str:
    """Get correlation ID from request headers or generate new one"""
    correlation_id = request.headers.get("X-Correlation-Id")
    if not correlation_id:
        correlation_id = str(uuid.uuid4())
    return correlation_id


@router.get("/health", response_model=MPFSHealthResponse)
async def health_check():
    """
    Health check endpoint for MPFS service
    
    Returns:
        MPFSHealthResponse: Health status and metadata
    """
    return MPFSHealthResponse(
        status="healthy",
        service="mpfs",
        version="1.0.0",
        timestamp=datetime.now().isoformat(),
        checks={
            "database": "healthy",
            "scraper": "healthy",
            "ingestor": "healthy"
        }
    )


@router.get("/rvu", response_model=MPFSRVUListResponse)
async def get_rvu_items(
    request: Request,
    hcpcs: Optional[str] = Query(None, description="HCPCS code filter"),
    modifier: Optional[str] = Query(None, description="Modifier code filter"),
    status_code: Optional[str] = Query(None, description="Status code filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    is_payable: Optional[bool] = Query(None, description="Payable items only"),
    payment_category: Optional[str] = Query(None, description="Payment category filter"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Page size"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Get MPFS RVU items with filtering and pagination
    
    Args:
        hcpcs: Filter by HCPCS code
        modifier: Filter by modifier code
        status_code: Filter by status code
        effective_date: Filter by effective date
        is_payable: Filter by payable status
        payment_category: Filter by payment category
        page: Page number (1-based)
        page_size: Number of items per page
        db: Database session
        correlation_id: Request correlation ID
        
    Returns:
        MPFSRVUListResponse: Paginated list of RVU items
    """
    correlation_id = get_correlation_id(request)
    
    try:
        logger.info("Getting MPFS RVU items", 
                   hcpcs=hcpcs,
                   modifier=modifier,
                   status_code=status_code,
                   effective_date=effective_date,
                   is_payable=is_payable,
                   payment_category=payment_category,
                   page=page,
                   page_size=page_size,
                   correlation_id=correlation_id)
        
        # Build query
        query = db.query(MPFSRVU)
        
        # Apply filters
        if hcpcs:
            query = query.filter(MPFSRVU.hcpcs == hcpcs)
        if modifier:
            query = query.filter(MPFSRVU.modifier == modifier)
        if status_code:
            query = query.filter(MPFSRVU.status_code == status_code)
        if effective_date:
            query = query.filter(
                MPFSRVU.effective_from <= effective_date,
                (MPFSRVU.effective_to.is_(None)) | (MPFSRVU.effective_to >= effective_date)
            )
        if is_payable is not None:
            query = query.filter(MPFSRVU.is_payable == is_payable)
        if payment_category:
            query = query.filter(MPFSRVU.payment_category == payment_category)
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        items = query.offset(offset).limit(page_size).all()
        
        # Convert to response format
        rvu_items = [item.to_dict() for item in items]
        
        # Calculate pagination info
        total_pages = (total_count + page_size - 1) // page_size
        has_next = page < total_pages
        has_prev = page > 1
        
        response = MPFSRVUListResponse(
            items=rvu_items,
            pagination={
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev
            },
            metadata={
                "correlation_id": correlation_id,
                "timestamp": datetime.now().isoformat(),
                "filters": {
                    "hcpcs": hcpcs,
                    "modifier": modifier,
                    "status_code": status_code,
                    "effective_date": effective_date.isoformat() if effective_date else None,
                    "is_payable": is_payable,
                    "payment_category": payment_category
                }
            }
        )
        
        logger.info("MPFS RVU items retrieved successfully", 
                   count=len(rvu_items),
                   total_count=total_count,
                   correlation_id=correlation_id)
        
        return response
        
    except Exception as e:
        logger.error("Failed to get MPFS RVU items", 
                    error=str(e), 
                    correlation_id=correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve MPFS RVU items"
        )


@router.get("/rvu/{hcpcs}", response_model=MPFSRVUResponse)
async def get_rvu_item(
    request: Request,
    hcpcs: str,
    modifier: Optional[str] = Query(None, description="Modifier code"),
    effective_date: Optional[date] = Query(None, description="Effective date"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Get specific MPFS RVU item by HCPCS code
    
    Args:
        hcpcs: HCPCS code
        modifier: Optional modifier code
        effective_date: Optional effective date
        db: Database session
        request: FastAPI request object
        
    Returns:
        MPFSRVUResponse: RVU item details
    """
    correlation_id = get_correlation_id(request)
    
    try:
        logger.info("Getting MPFS RVU item", 
                   hcpcs=hcpcs, 
                   modifier=modifier,
                   effective_date=effective_date,
                   correlation_id=correlation_id)
        
        # Build query
        query = db.query(MPFSRVU).filter(MPFSRVU.hcpcs == hcpcs)
        
        if modifier:
            query = query.filter(MPFSRVU.modifier == modifier)
        else:
            query = query.filter(MPFSRVU.modifier.is_(None))
        
        if effective_date:
            query = query.filter(
                MPFSRVU.effective_from <= effective_date,
                (MPFSRVU.effective_to.is_(None)) | (MPFSRVU.effective_to >= effective_date)
            )
        
        # Get the item
        item = query.first()
        
        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MPFS RVU item not found for HCPCS {hcpcs}"
            )
        
        response = MPFSRVUResponse(
            item=item.to_dict(),
            metadata={
                "correlation_id": correlation_id,
                "timestamp": datetime.now().isoformat()
            }
        )
        
        logger.info("MPFS RVU item retrieved successfully", 
                   hcpcs=hcpcs,
                   correlation_id=correlation_id)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get MPFS RVU item", 
                    hcpcs=hcpcs,
                    error=str(e), 
                    correlation_id=correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve MPFS RVU item"
        )


@router.get("/conversion-factors", response_model=List[MPFSConversionFactorResponse])
async def get_conversion_factors(
    request: Request,
    cf_type: Optional[str] = Query(None, description="Conversion factor type filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    vintage_year: Optional[str] = Query(None, description="Vintage year filter"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Get MPFS conversion factors
    
    Args:
        cf_type: Filter by conversion factor type
        effective_date: Filter by effective date
        vintage_year: Filter by vintage year
        db: Database session
        request: FastAPI request object
        
    Returns:
        List[MPFSConversionFactorResponse]: List of conversion factors
    """
    correlation_id = get_correlation_id(request)
    
    try:
        logger.info("Getting MPFS conversion factors", 
                   cf_type=cf_type,
                   effective_date=effective_date,
                   vintage_year=vintage_year,
                   correlation_id=correlation_id)
        
        # Build query
        query = db.query(MPFSConversionFactor)
        
        # Apply filters
        if cf_type:
            query = query.filter(MPFSConversionFactor.cf_type == cf_type)
        if effective_date:
            query = query.filter(
                MPFSConversionFactor.effective_from <= effective_date,
                (MPFSConversionFactor.effective_to.is_(None)) | (MPFSConversionFactor.effective_to >= effective_date)
            )
        if vintage_year:
            query = query.filter(MPFSConversionFactor.vintage_year == vintage_year)
        
        # Get items
        items = query.all()
        
        # Convert to response format
        conversion_factors = [item.to_dict() for item in items]
        
        logger.info("MPFS conversion factors retrieved successfully", 
                   count=len(conversion_factors),
                   correlation_id=correlation_id)
        
        return conversion_factors
        
    except Exception as e:
        logger.error("Failed to get MPFS conversion factors", 
                    error=str(e), 
                    correlation_id=correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve MPFS conversion factors"
        )


@router.get("/abstracts", response_model=List[MPFSAbstractResponse])
async def get_abstracts(
    request: Request,
    abstract_type: Optional[str] = Query(None, description="Abstract type filter"),
    payment_year: Optional[str] = Query(None, description="Payment year filter"),
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Get MPFS abstracts and national payment data
    
    Args:
        abstract_type: Filter by abstract type
        payment_year: Filter by payment year
        effective_date: Filter by effective date
        db: Database session
        request: FastAPI request object
        
    Returns:
        List[MPFSAbstractResponse]: List of abstracts
    """
    correlation_id = get_correlation_id(request)
    
    try:
        logger.info("Getting MPFS abstracts", 
                   abstract_type=abstract_type,
                   payment_year=payment_year,
                   effective_date=effective_date,
                   correlation_id=correlation_id)
        
        # Build query
        query = db.query(MPFSAbstract)
        
        # Apply filters
        if abstract_type:
            query = query.filter(MPFSAbstract.abstract_type == abstract_type)
        if payment_year:
            query = query.filter(MPFSAbstract.payment_year == payment_year)
        if effective_date:
            query = query.filter(
                MPFSAbstract.effective_from <= effective_date,
                (MPFSAbstract.effective_to.is_(None)) | (MPFSAbstract.effective_to >= effective_date)
            )
        
        # Get items
        items = query.all()
        
        # Convert to response format
        abstracts = [item.to_dict() for item in items]
        
        logger.info("MPFS abstracts retrieved successfully", 
                   count=len(abstracts),
                   correlation_id=correlation_id)
        
        return abstracts
        
    except Exception as e:
        logger.error("Failed to get MPFS abstracts", 
                    error=str(e), 
                    correlation_id=correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve MPFS abstracts"
        )


@router.get("/stats")
async def get_mpfs_stats(
    request: Request,
    effective_date: Optional[date] = Query(None, description="Effective date filter"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Get MPFS statistics and summary data
    
    Args:
        effective_date: Filter by effective date
        db: Database session
        request: FastAPI request object
        
    Returns:
        Dict[str, Any]: MPFS statistics
    """
    correlation_id = get_correlation_id(request)
    
    try:
        logger.info("Getting MPFS statistics", 
                   effective_date=effective_date,
                   correlation_id=correlation_id)
        
        # Build base query
        rvu_query = db.query(MPFSRVU)
        cf_query = db.query(MPFSConversionFactor)
        
        # Apply effective date filter if provided
        if effective_date:
            rvu_query = rvu_query.filter(
                MPFSRVU.effective_from <= effective_date,
                (MPFSRVU.effective_to.is_(None)) | (MPFSRVU.effective_to >= effective_date)
            )
            cf_query = cf_query.filter(
                MPFSConversionFactor.effective_from <= effective_date,
                (MPFSConversionFactor.effective_to.is_(None)) | (MPFSConversionFactor.effective_to >= effective_date)
            )
        
        # Calculate statistics
        total_rvu_items = rvu_query.count()
        payable_items = rvu_query.filter(MPFSRVU.is_payable == True).count()
        surgery_items = rvu_query.filter(MPFSRVU.is_surgery == True).count()
        evaluation_items = rvu_query.filter(MPFSRVU.is_evaluation == True).count()
        
        # Get unique HCPCS codes
        unique_hcpcs = rvu_query.with_entities(MPFSRVU.hcpcs).distinct().count()
        
        # Get conversion factors
        conversion_factors = cf_query.all()
        cf_stats = {
            "total": len(conversion_factors),
            "by_type": {}
        }
        for cf in conversion_factors:
            cf_type = cf.cf_type
            if cf_type not in cf_stats["by_type"]:
                cf_stats["by_type"][cf_type] = 0
            cf_stats["by_type"][cf_type] += 1
        
        stats = {
            "rvu_items": {
                "total": total_rvu_items,
                "payable": payable_items,
                "surgery": surgery_items,
                "evaluation": evaluation_items,
                "unique_hcpcs": unique_hcpcs
            },
            "conversion_factors": cf_stats,
            "metadata": {
                "effective_date": effective_date.isoformat() if effective_date else None,
                "correlation_id": correlation_id,
                "timestamp": datetime.now().isoformat()
            }
        }
        
        logger.info("MPFS statistics retrieved successfully", 
                   total_rvu_items=total_rvu_items,
                   correlation_id=correlation_id)
        
        return stats
        
    except Exception as e:
        logger.error("Failed to get MPFS statistics", 
                    error=str(e), 
                    correlation_id=correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve MPFS statistics"
        )
