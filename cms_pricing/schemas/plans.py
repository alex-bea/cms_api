"""Plan-related Pydantic schemas"""

from typing import List, Optional, Dict, Any
from datetime import date
from pydantic import BaseModel, Field, validator
from uuid import UUID


class PlanComponentCreate(BaseModel):
    """Schema for creating a plan component"""
    code: str = Field(..., min_length=1, max_length=5, description="HCPCS/CPT code")
    setting: str = Field(..., description="Payment setting (MPFS, OPPS, ASC, IPPS, CLFS, DMEPOS)")
    units: float = Field(default=1.0, gt=0, description="Number of units")
    utilization_weight: float = Field(default=1.0, gt=0, description="Utilization weight")
    professional_component: bool = Field(default=True, description="Include professional component")
    facility_component: bool = Field(default=True, description="Include facility component")
    modifiers: Optional[List[str]] = Field(None, description="List of modifiers")
    pos: Optional[str] = Field(None, max_length=2, description="Place of service code")
    ndc11: Optional[str] = Field(None, max_length=11, description="NDC for drug components")
    wastage_units: float = Field(default=0.0, ge=0, description="Wastage units (future use)")
    sequence: int = Field(default=1, gt=0, description="Order within plan")
    
    @validator('setting')
    def validate_setting(cls, v):
        allowed_settings = {'MPFS', 'OPPS', 'ASC', 'IPPS', 'CLFS', 'DMEPOS'}
        if v.upper() not in allowed_settings:
            raise ValueError(f'Setting must be one of: {", ".join(allowed_settings)}')
        return v.upper()
    
    @validator('modifiers')
    def validate_modifiers(cls, v):
        if v is None:
            return v
        allowed_modifiers = {'-26', '-TC', '-50', '-51', '-59', '-XS', '-XE', '-XP', '-XU'}
        for modifier in v:
            if modifier not in allowed_modifiers:
                raise ValueError(f'Invalid modifier: {modifier}')
        return v


class PlanComponentResponse(BaseModel):
    """Schema for plan component response"""
    id: UUID
    code: str
    setting: str
    units: float
    utilization_weight: float
    professional_component: bool
    facility_component: bool
    modifiers: Optional[List[str]]
    pos: Optional[str]
    ndc11: Optional[str]
    wastage_units: float
    sequence: int
    created_at: date


class PlanCreate(BaseModel):
    """Schema for creating a plan"""
    name: str = Field(..., min_length=1, max_length=200, description="Plan name")
    description: Optional[str] = Field(None, description="Plan description")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    components: List[PlanComponentCreate] = Field(..., min_items=1, description="Plan components")
    created_by: Optional[str] = Field(None, description="Creator identifier")


class PlanUpdate(BaseModel):
    """Schema for updating a plan"""
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    components: Optional[List[PlanComponentCreate]] = None


class PlanResponse(BaseModel):
    """Schema for plan response"""
    id: UUID
    name: str
    description: Optional[str]
    metadata: Optional[Dict[str, Any]]
    created_by: Optional[str]
    created_at: date
    updated_at: Optional[date]
    components: List[PlanComponentResponse]
    
    class Config:
        from_attributes = True


class PlanSummary(BaseModel):
    """Schema for plan summary in list responses"""
    id: UUID
    name: str
    description: Optional[str]
    component_count: int
    created_by: Optional[str]
    created_at: date
    updated_at: Optional[date]
    
    class Config:
        from_attributes = True
