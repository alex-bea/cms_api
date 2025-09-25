"""Pricing endpoints"""

from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from slowapi import Limiter
from slowapi.util import get_remote_address

from cms_pricing.schemas.pricing import (
    PricingRequest, PricingResponse, ComparisonRequest, ComparisonResponse
)
from cms_pricing.auth import verify_api_key
from cms_pricing.services.pricing import PricingService

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/codes/price")
@limiter.limit("120/minute")
async def price_single_code(
    zip: str,
    code: str,
    setting: str,
    year: int,
    quarter: Optional[str] = None,
    ccn: Optional[str] = None,
    payer: Optional[str] = None,
    plan: Optional[str] = None,
    api_key: str = Depends(verify_api_key)
):
    """Price a single code/component"""
    
    # Validate inputs
    if not zip.isdigit() or len(zip) != 5:
        raise HTTPException(status_code=400, detail="ZIP must be exactly 5 digits")
    
    if not code or len(code) > 5:
        raise HTTPException(status_code=400, detail="Code must be 1-5 characters")
    
    if setting not in ['MPFS', 'OPPS', 'ASC', 'IPPS', 'CLFS', 'DMEPOS']:
        raise HTTPException(status_code=400, detail="Invalid setting")
    
    if year < 2020 or year > 2030:
        raise HTTPException(status_code=400, detail="Year must be between 2020 and 2030")
    
    if quarter and quarter not in ['1', '2', '3', '4']:
        raise HTTPException(status_code=400, detail="Quarter must be 1-4")
    
    if ccn and (not ccn.isdigit() or len(ccn) != 6):
        raise HTTPException(status_code=400, detail="CCN must be exactly 6 digits")
    
    try:
        pricing_service = PricingService()
        result = await pricing_service.price_single_code(
            zip=zip,
            code=code,
            setting=setting,
            year=year,
            quarter=quarter,
            ccn=ccn,
            payer=payer,
            plan=plan
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Pricing failed: {str(e)}"
        )


@router.post("/price", response_model=PricingResponse)
@limiter.limit("60/minute")
async def price_plan(
    request: PricingRequest,
    api_key: str = Depends(verify_api_key)
):
    """Price a complete treatment plan"""
    
    try:
        pricing_service = PricingService()
        result = await pricing_service.price_plan(request)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Plan pricing failed: {str(e)}"
        )


@router.post("/compare", response_model=ComparisonResponse)
@limiter.limit("30/minute")
async def compare_locations(
    request: ComparisonRequest,
    api_key: str = Depends(verify_api_key)
):
    """Compare pricing between two locations"""
    
    try:
        pricing_service = PricingService()
        result = await pricing_service.compare_locations(request)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Comparison failed: {str(e)}"
        )
