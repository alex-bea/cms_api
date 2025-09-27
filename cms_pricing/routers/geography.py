"""Geography resolution endpoints"""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from cms_pricing.schemas.geography import GeographyResolveRequest, GeographyResolveResponse, GeographyCandidate
from cms_pricing.auth import verify_api_key
from cms_pricing.services.geography import GeographyService

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/resolve", response_model=GeographyResolveResponse)
async def resolve_geography(
    request: Request,
    zip: str,
    api_key: str = Depends(verify_api_key)
):
    """Resolve ZIP code to locality/CBSA with ambiguity handling"""
    
    if not zip.isdigit() or len(zip) != 5:
        raise HTTPException(
            status_code=400,
            detail="ZIP must be exactly 5 digits"
        )
    
    try:
        geography_service = GeographyService()
        result = await geography_service.resolve_zip(zip)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Geography resolution failed: {str(e)}"
        )
