#!/usr/bin/env python3
"""
OPPS API Router
===============

FastAPI router for OPPS data endpoints following Global API Program v1.0 standards.

Author: CMS Pricing Platform Team
Version: 1.0.0
Global API Program Compliance: v1.0
"""

import uuid
from datetime import date, datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import nullslast
from pydantic import BaseModel, Field

from cms_pricing.database import get_db
from cms_pricing.auth import verify_api_key
from cms_pricing.models.opps import (
    OPPSAPCPayment, 
    OPPSHCPCSCrosswalk, 
    OPPSRatesEnriched, 
    RefSILookup
)
from cms_pricing.models.fee_schedules import WageIndex


router = APIRouter()


# Pydantic models for API responses
class OPPSAPCPaymentResponse(BaseModel):
    """OPPS APC Payment response model."""
    id: int
    year: int
    quarter: int
    apc_code: str
    apc_description: Optional[str]
    payment_rate_usd: Decimal
    relative_weight: Decimal
    packaging_flag: Optional[str]
    effective_from: date
    effective_to: Optional[date]
    release_id: str
    batch_id: str
    created_at: date
    updated_at: date
    
    class Config:
        from_attributes = True


class OPPSHCPCSCrosswalkResponse(BaseModel):
    """OPPS HCPCS Crosswalk response model."""
    id: int
    year: int
    quarter: int
    hcpcs_code: str
    modifier: Optional[str]
    status_indicator: str
    apc_code: Optional[str]
    payment_context: Optional[str]
    effective_from: date
    effective_to: Optional[date]
    release_id: str
    batch_id: str
    created_at: date
    updated_at: date
    
    class Config:
        from_attributes = True


class OPPSRatesEnrichedResponse(BaseModel):
    """OPPS Rates Enriched response model."""
    id: int
    year: int
    quarter: int
    apc_code: str
    ccn: Optional[str]
    cbsa_code: Optional[str]
    wage_index: Optional[Decimal]
    payment_rate_usd: Decimal
    wage_adjusted_rate_usd: Optional[Decimal]
    effective_from: date
    effective_to: Optional[date]
    release_id: str
    batch_id: str
    created_at: date
    updated_at: date
    
    class Config:
        from_attributes = True


class RefSILookupResponse(BaseModel):
    """Reference SI Lookup response model."""
    id: int
    status_indicator: str
    description: str
    payment_category: Optional[str]
    effective_from: date
    effective_to: Optional[date]
    created_at: date
    updated_at: date
    
    class Config:
        from_attributes = True


class OPPSListResponse(BaseModel):
    """Generic list response model."""
    items: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int
    has_next: bool
    has_prev: bool


class OPPSHealthResponse(BaseModel):
    """OPPS health response model."""
    status: str
    dataset_name: str
    release_cadence: str
    data_classification: str
    cpt_masking_enabled: bool
    validation_rules_count: int
    last_updated: Optional[datetime]
    correlation_id: str


class OPPSErrorResponse(BaseModel):
    """OPPS error response model."""
    error: str
    code: str
    correlation_id: str
    timestamp: datetime


class WageIndexItem(BaseModel):
    """Wage index item response model."""
    id: str
    year: int
    quarter: Optional[str]
    cbsa: str
    wage_index: Decimal
    effective_from: date
    effective_to: Optional[date]
    release_id: Optional[str] = Field(None, description="Release identifier. Will be None until Phase 2 provenance migration is complete.")
    batch_id: Optional[str] = Field(None, description="Batch identifier. Will be None until Phase 2 provenance migration is complete.")
    
    class Config:
        from_attributes = True


class WageIndexListResponse(BaseModel):
    """Wage index list response model."""
    items: List[WageIndexItem]
    total: int
    page: int
    page_size: int
    has_next: bool
    has_prev: bool
    correlation_id: str


def get_correlation_id(request: Request) -> str:
    """Get correlation ID from request headers or generate new one."""
    correlation_id = request.headers.get("X-Correlation-Id")
    if not correlation_id:
        correlation_id = str(uuid.uuid4())
    return correlation_id


@router.get("/health", response_model=OPPSHealthResponse)
async def get_health(
    request: Request,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get OPPS ingester health status."""
    correlation_id = get_correlation_id(request)
    
    try:
        # Get basic health information
        health_info = {
            "status": "healthy",
            "dataset_name": "cms_opps",
            "release_cadence": "quarterly",
            "data_classification": "public",
            "cpt_masking_enabled": True,
            "validation_rules_count": 12,  # This would be dynamic in real implementation
            "last_updated": datetime.utcnow(),
            "correlation_id": correlation_id
        }
        
        return JSONResponse(
            content=health_info,
            headers={"X-Correlation-Id": correlation_id}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error="Health check failed",
                code="HEALTH_CHECK_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )


@router.get("/apc-payments", response_model=OPPSListResponse)
async def get_apc_payments(
    request: Request,
    year: Optional[int] = Query(None, description="Filter by year"),
    quarter: Optional[int] = Query(None, ge=1, le=4, description="Filter by quarter (1-4)"),
    apc_code: Optional[str] = Query(None, description="Filter by APC code"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get OPPS APC payment rates."""
    correlation_id = get_correlation_id(request)
    
    try:
        # Build query
        query = db.query(OPPSAPCPayment)
        
        if year:
            query = query.filter(OPPSAPCPayment.year == year)
        if quarter:
            query = query.filter(OPPSAPCPayment.quarter == quarter)
        if apc_code:
            query = query.filter(OPPSAPCPayment.apc_code == apc_code)
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        items = query.offset(offset).limit(page_size).all()
        
        # Convert to response format
        response_items = [item.to_dict() for item in items]
        
        return JSONResponse(
            content=OPPSListResponse(
                items=response_items,
                total=total,
                page=page,
                page_size=page_size,
                has_next=offset + page_size < total,
                has_prev=page > 1
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error=f"Failed to retrieve APC payments: {str(e)}",
                code="APC_PAYMENTS_RETRIEVAL_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )


@router.get("/hcpcs-crosswalk", response_model=OPPSListResponse)
async def get_hcpcs_crosswalk(
    request: Request,
    year: Optional[int] = Query(None, description="Filter by year"),
    quarter: Optional[int] = Query(None, ge=1, le=4, description="Filter by quarter (1-4)"),
    hcpcs_code: Optional[str] = Query(None, description="Filter by HCPCS code"),
    modifier: Optional[str] = Query(None, description="Filter by modifier"),
    status_indicator: Optional[str] = Query(None, description="Filter by status indicator"),
    apc_code: Optional[str] = Query(None, description="Filter by APC code"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get OPPS HCPCS crosswalk data."""
    correlation_id = get_correlation_id(request)
    
    try:
        # Build query
        query = db.query(OPPSHCPCSCrosswalk)
        
        if year:
            query = query.filter(OPPSHCPCSCrosswalk.year == year)
        if quarter:
            query = query.filter(OPPSHCPCSCrosswalk.quarter == quarter)
        if hcpcs_code:
            query = query.filter(OPPSHCPCSCrosswalk.hcpcs_code == hcpcs_code)
        if modifier:
            query = query.filter(OPPSHCPCSCrosswalk.modifier == modifier)
        if status_indicator:
            query = query.filter(OPPSHCPCSCrosswalk.status_indicator == status_indicator)
        if apc_code:
            query = query.filter(OPPSHCPCSCrosswalk.apc_code == apc_code)
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        items = query.offset(offset).limit(page_size).all()
        
        # Convert to response format
        response_items = [item.to_dict() for item in items]
        
        return JSONResponse(
            content=OPPSListResponse(
                items=response_items,
                total=total,
                page=page,
                page_size=page_size,
                has_next=offset + page_size < total,
                has_prev=page > 1
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error=f"Failed to retrieve HCPCS crosswalk: {str(e)}",
                code="HCPCS_CROSSWALK_RETRIEVAL_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )


@router.get("/rates-enriched", response_model=OPPSListResponse)
async def get_rates_enriched(
    request: Request,
    year: Optional[int] = Query(None, description="Filter by year"),
    quarter: Optional[int] = Query(None, ge=1, le=4, description="Filter by quarter (1-4)"),
    apc_code: Optional[str] = Query(None, description="Filter by APC code"),
    ccn: Optional[str] = Query(None, description="Filter by CCN"),
    cbsa_code: Optional[str] = Query(None, description="Filter by CBSA code"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get OPPS enriched rates with wage index data."""
    correlation_id = get_correlation_id(request)
    
    try:
        # Build query
        query = db.query(OPPSRatesEnriched)
        
        if year:
            query = query.filter(OPPSRatesEnriched.year == year)
        if quarter:
            query = query.filter(OPPSRatesEnriched.quarter == quarter)
        if apc_code:
            query = query.filter(OPPSRatesEnriched.apc_code == apc_code)
        if ccn:
            query = query.filter(OPPSRatesEnriched.ccn == ccn)
        if cbsa_code:
            query = query.filter(OPPSRatesEnriched.cbsa_code == cbsa_code)
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        items = query.offset(offset).limit(page_size).all()
        
        # Convert to response format
        response_items = [item.to_dict() for item in items]
        
        return JSONResponse(
            content=OPPSListResponse(
                items=response_items,
                total=total,
                page=page,
                page_size=page_size,
                has_next=offset + page_size < total,
                has_prev=page > 1
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error=f"Failed to retrieve enriched rates: {str(e)}",
                code="RATES_ENRICHED_RETRIEVAL_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )


@router.get("/si-lookup", response_model=OPPSListResponse)
async def get_si_lookup(
    request: Request,
    status_indicator: Optional[str] = Query(None, description="Filter by status indicator"),
    payment_category: Optional[str] = Query(None, description="Filter by payment category"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get OPPS status indicator lookup data."""
    correlation_id = get_correlation_id(request)
    
    try:
        # Build query
        query = db.query(RefSILookup)
        
        if status_indicator:
            query = query.filter(RefSILookup.status_indicator == status_indicator)
        if payment_category:
            query = query.filter(RefSILookup.payment_category == payment_category)
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        items = query.offset(offset).limit(page_size).all()
        
        # Convert to response format
        response_items = [item.to_dict() for item in items]
        
        return JSONResponse(
            content=OPPSListResponse(
                items=response_items,
                total=total,
                page=page,
                page_size=page_size,
                has_next=offset + page_size < total,
                has_prev=page > 1
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error=f"Failed to retrieve SI lookup: {str(e)}",
                code="SI_LOOKUP_RETRIEVAL_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )


@router.get("/stats", response_model=Dict[str, Any])
async def get_stats(
    request: Request,
    year: Optional[int] = Query(None, description="Filter by year"),
    quarter: Optional[int] = Query(None, ge=1, le=4, description="Filter by quarter (1-4)"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get OPPS dataset statistics."""
    correlation_id = get_correlation_id(request)
    
    try:
        # Build base filters
        apc_filters = []
        hcpcs_filters = []
        rates_filters = []
        
        if year:
            apc_filters.append(OPPSAPCPayment.year == year)
            hcpcs_filters.append(OPPSHCPCSCrosswalk.year == year)
            rates_filters.append(OPPSRatesEnriched.year == year)
        
        if quarter:
            apc_filters.append(OPPSAPCPayment.quarter == quarter)
            hcpcs_filters.append(OPPSHCPCSCrosswalk.quarter == quarter)
            rates_filters.append(OPPSRatesEnriched.quarter == quarter)
        
        # Get counts
        apc_count = db.query(OPPSAPCPayment).filter(*apc_filters).count()
        hcpcs_count = db.query(OPPSHCPCSCrosswalk).filter(*hcpcs_filters).count()
        rates_count = db.query(OPPSRatesEnriched).filter(*rates_filters).count()
        si_count = db.query(RefSILookup).count()
        
        # Get unique counts
        unique_apc_codes = db.query(OPPSAPCPayment.apc_code).filter(*apc_filters).distinct().count()
        unique_hcpcs_codes = db.query(OPPSHCPCSCrosswalk.hcpcs_code).filter(*hcpcs_filters).distinct().count()
        unique_ccns = db.query(OPPSRatesEnriched.ccn).filter(*rates_filters).distinct().count()
        
        stats = {
            "total_records": {
                "apc_payments": apc_count,
                "hcpcs_crosswalk": hcpcs_count,
                "rates_enriched": rates_count,
                "si_lookup": si_count
            },
            "unique_codes": {
                "apc_codes": unique_apc_codes,
                "hcpcs_codes": unique_hcpcs_codes,
                "ccns": unique_ccns
            },
            "filters_applied": {
                "year": year,
                "quarter": quarter
            },
            "generated_at": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        }
        
        return JSONResponse(
            content=stats,
            headers={"X-Correlation-Id": correlation_id}
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error=f"Failed to retrieve stats: {str(e)}",
                code="STATS_RETRIEVAL_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )


@router.get("/wage-index", response_model=WageIndexListResponse)
async def get_wage_index(
    request: Request,
    cbsa: Optional[str] = Query(None, description="Filter by CBSA code (5 characters)"),
    year: Optional[int] = Query(None, description="Filter by year"),
    quarter: Optional[str] = Query(None, pattern='^[1-4]$', description="Filter by quarter (1-4)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Page size"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """
    Get facility-level wage index data for OPPS and IPPS.
    
    This endpoint provides wage index lookups by CBSA for hospital bill validation.
    Wage index values are used to adjust national payment rates for geographic variation.
    """
    correlation_id = get_correlation_id(request)
    
    try:
        from sqlalchemy import and_, or_
        
        # Build query
        query = db.query(WageIndex)
        
        filters = []
        if year:
            filters.append(WageIndex.year == year)
        if quarter:
            # Coalesce quarter value
            quarter_value = quarter if quarter else "1"
            filters.append(WageIndex.quarter == quarter_value)
        elif year:
            # If year specified but no quarter, return all quarters for that year
            pass
        if cbsa:
            if len(cbsa) != 5 or not cbsa.isdigit():
                raise HTTPException(
                    status_code=400,
                    detail="CBSA must be exactly 5 digits"
                )
            filters.append(WageIndex.cbsa == cbsa)
        
        if filters:
            query = query.filter(and_(*filters))
        
        # Get total count
        total = query.count()
        
        # Single-record lookup mode: if all filters provided, return just latest matching record
        single_lookup = cbsa and year and quarter
        
        if single_lookup:
            # Get latest matching record
            # Build order_by clauses - only include created_at if column exists
            ordering = [WageIndex.effective_from.desc()]
            if hasattr(WageIndex, 'created_at'):
                ordering.append(WageIndex.created_at.desc())
            
            item = query.order_by(*ordering).first()
            
            if not item:
                raise HTTPException(
                    status_code=404,
                    detail=f"No wage index found for CBSA {cbsa}, year {year}, quarter {quarter}"
                )
            
            # Convert to response format
            wage_item = WageIndexItem(
                id=str(item.id),
                year=item.year,
                quarter=item.quarter,
                cbsa=item.cbsa,
                wage_index=Decimal(str(item.wage_index)),
                effective_from=item.effective_from,
                effective_to=item.effective_to,
                release_id=getattr(item, 'release_id', None),
                batch_id=getattr(item, 'batch_id', None)
            )
            
            return WageIndexListResponse(
                items=[wage_item],
                total=1,
                page=1,
                page_size=1,
                has_next=False,
                has_prev=False,
                correlation_id=correlation_id
            )
        
        # Multi-record paginated response
        offset = (page - 1) * page_size
        
        # Order by year desc, then quarter desc (nulls last for IPPS annual records), then CBSA asc
        items = query.order_by(
            WageIndex.year.desc(),
            nullslast(WageIndex.quarter.desc()),
            WageIndex.cbsa.asc()
        ).offset(offset).limit(page_size).all()
        
        # Convert to response format
        response_items = [
            WageIndexItem(
                id=str(item.id),
                year=item.year,
                quarter=item.quarter,
                cbsa=item.cbsa,
                wage_index=Decimal(str(item.wage_index)),
                effective_from=item.effective_from,
                effective_to=item.effective_to,
                release_id=getattr(item, 'release_id', None),
                batch_id=getattr(item, 'batch_id', None)
            )
            for item in items
        ]
        
        return WageIndexListResponse(
            items=response_items,
            total=total,
            page=page,
            page_size=page_size,
            has_next=offset + page_size < total,
            has_prev=page > 1,
            correlation_id=correlation_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=OPPSErrorResponse(
                error=f"Failed to retrieve wage index: {str(e)}",
                code="WAGE_INDEX_RETRIEVAL_FAILED",
                correlation_id=correlation_id,
                timestamp=datetime.utcnow()
            ).dict(),
            headers={"X-Correlation-Id": correlation_id}
        )
