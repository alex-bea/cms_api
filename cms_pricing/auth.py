"""Authentication utilities"""

from typing import Optional
from fastapi import HTTPException, Header
from cms_pricing.config import settings


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """Verify API key from header"""
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="API key required"
        )
    
    valid_keys = settings.get_api_keys()
    if x_api_key not in valid_keys:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    
    return x_api_key


def is_admin_key(api_key: str) -> bool:
    """Check if API key has admin privileges"""
    # For now, admin keys are those ending with "admin"
    return api_key.endswith("admin") or api_key.endswith("456")
