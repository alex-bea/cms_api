"""Common Pydantic schemas"""

from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class MoneyResponse(BaseModel):
    """Money amount in cents with optional decimal format"""
    cents: int = Field(..., description="Amount in cents")
    currency: str = Field(default="USD", description="Currency code")
    scale: int = Field(default=2, description="Decimal places")
    
    @property
    def decimal(self) -> float:
        """Convert cents to decimal amount"""
        return self.cents / (10 ** self.scale)


class ErrorResponse(BaseModel):
    """Error response schema"""
    error: str = Field(..., description="Error message")
    code: Optional[str] = Field(None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    trace_id: Optional[str] = Field(None, description="Request trace ID")


class PaginationResponse(BaseModel):
    """Pagination metadata"""
    page: int = Field(..., description="Current page number")
    limit: int = Field(..., description="Items per page")
    total: int = Field(..., description="Total number of items")
    next_page_token: Optional[str] = Field(None, description="Token for next page")
    has_next: bool = Field(..., description="Whether there are more pages")
