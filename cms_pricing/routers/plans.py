"""Plan management endpoints"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from slowapi import Limiter
from slowapi.util import get_remote_address

from cms_pricing.database import get_db
from cms_pricing.models.plans import Plan, PlanComponent
from cms_pricing.schemas.plans import (
    PlanCreate, PlanUpdate, PlanResponse, PlanSummary, PlanComponentCreate, PlanComponentResponse
)
from cms_pricing.auth import verify_api_key

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.post("/", response_model=PlanResponse)
async def create_plan(
    request: Request,
    plan_data: PlanCreate,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Create a new treatment plan"""
    
    # Create plan
    plan = Plan(
        name=plan_data.name,
        description=plan_data.description,
        metadata_json=json.dumps(plan_data.metadata) if plan_data.metadata else None,
        created_by=plan_data.created_by,
        created_at=date.today()
    )
    
    db.add(plan)
    db.flush()  # Get the ID
    
    # Create components
    for comp_data in plan_data.components:
        component = PlanComponent(
            plan_id=plan.id,
            code=comp_data.code,
            setting=comp_data.setting,
            units=comp_data.units,
            utilization_weight=comp_data.utilization_weight,
            professional_component=comp_data.professional_component,
            facility_component=comp_data.facility_component,
            modifiers=comp_data.modifiers,
            pos=comp_data.pos,
            ndc11=comp_data.ndc11,
            wastage_units=comp_data.wastage_units,
            sequence=comp_data.sequence,
            created_at=date.today()
        )
        db.add(component)
    
    db.commit()
    db.refresh(plan)

    return PlanResponse(
        id=plan.id,
        name=plan.name,
        description=plan.description,
        metadata=json.loads(plan.metadata_json) if plan.metadata_json else None,
        created_by=plan.created_by,
        created_at=plan.created_at,
        updated_at=plan.updated_at,
        components=[
            PlanComponentResponse(
                id=c.id,
                code=c.code,
                setting=c.setting,
                units=c.units,
                utilization_weight=c.utilization_weight,
                professional_component=c.professional_component,
                facility_component=c.facility_component,
                modifiers=c.modifiers,
                pos=c.pos,
                ndc11=c.ndc11,
                wastage_units=c.wastage_units,
                sequence=c.sequence,
                created_at=c.created_at,
            ) for c in plan.components
        ]
    )


@router.get("/", response_model=List[PlanSummary])
async def list_plans(
    request: Request,
    skip: int = Query(0, ge=0, description="Number of plans to skip"),
    limit: int = Query(20, ge=1, le=200, description="Number of plans to return"),
    search: Optional[str] = Query(None, description="Search by name or description"),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """List treatment plans with pagination"""
    
    query = db.query(Plan)
    
    if search:
        query = query.filter(
            Plan.name.ilike(f"%{search}%") | 
            Plan.description.ilike(f"%{search}%")
        )
    
    # Get total count
    total = query.count()
    
    # Get plans with component count
    plans = query.offset(skip).limit(limit).all()
    
    result = []
    for plan in plans:
        component_count = db.query(PlanComponent).filter(
            PlanComponent.plan_id == plan.id
        ).count()
        
        result.append(PlanSummary(
            id=plan.id,
            name=plan.name,
            description=plan.description,
            component_count=component_count,
            created_by=plan.created_by,
            created_at=plan.created_at,
            updated_at=plan.updated_at
        ))
    
    return result


@router.get("/{plan_id}", response_model=PlanResponse)
async def get_plan(
    request: Request,
    plan_id: UUID,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Get a specific treatment plan"""
    
    plan = db.query(Plan).filter(Plan.id == plan_id).first()
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    # Return PlanResponse mapping metadata_json -> metadata
    return PlanResponse(
        id=plan.id,
        name=plan.name,
        description=plan.description,
        metadata=json.loads(plan.metadata_json) if plan.metadata_json else None,
        created_by=plan.created_by,
        created_at=plan.created_at,
        updated_at=plan.updated_at,
        components=[
            PlanComponentResponse(
                id=c.id,
                code=c.code,
                setting=c.setting,
                units=c.units,
                utilization_weight=c.utilization_weight,
                professional_component=c.professional_component,
                facility_component=c.facility_component,
                modifiers=c.modifiers,
                pos=c.pos,
                ndc11=c.ndc11,
                wastage_units=c.wastage_units,
                sequence=c.sequence,
                created_at=c.created_at,
            ) for c in plan.components
        ]
    )


@router.put("/{plan_id}", response_model=PlanResponse)
async def update_plan(
    request: Request,
    plan_id: UUID,
    plan_data: PlanUpdate,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Update a treatment plan"""
    
    plan = db.query(Plan).filter(Plan.id == plan_id).first()
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    # Update plan fields
    if plan_data.name is not None:
        plan.name = plan_data.name
    if plan_data.description is not None:
        plan.description = plan_data.description
    if plan_data.metadata is not None:
        plan.metadata_json = plan_data.metadata
    
    plan.updated_at = date.today()
    
    # Update components if provided
    if plan_data.components is not None:
        # Delete existing components
        db.query(PlanComponent).filter(PlanComponent.plan_id == plan_id).delete()
        
        # Add new components
        for comp_data in plan_data.components:
            component = PlanComponent(
                plan_id=plan.id,
                code=comp_data.code,
                setting=comp_data.setting,
                units=comp_data.units,
                utilization_weight=comp_data.utilization_weight,
                professional_component=comp_data.professional_component,
                facility_component=comp_data.facility_component,
                modifiers=comp_data.modifiers,
                pos=comp_data.pos,
                ndc11=comp_data.ndc11,
                wastage_units=comp_data.wastage_units,
                sequence=comp_data.sequence,
                created_at=date.today()
            )
            db.add(component)
    
    db.commit()
    db.refresh(plan)

    # Build response mapping metadata_json -> metadata for API compatibility
    return PlanResponse(
        id=plan.id,
        name=plan.name,
        description=plan.description,
        metadata=json.loads(plan.metadata_json) if plan.metadata_json else None,
        created_by=plan.created_by,
        created_at=plan.created_at,
        updated_at=plan.updated_at,
        components=[
            PlanComponentResponse(
                id=c.id,
                code=c.code,
                setting=c.setting,
                units=c.units,
                utilization_weight=c.utilization_weight,
                professional_component=c.professional_component,
                facility_component=c.facility_component,
                modifiers=c.modifiers,
                pos=c.pos,
                ndc11=c.ndc11,
                wastage_units=c.wastage_units,
                sequence=c.sequence,
                created_at=c.created_at,
            ) for c in plan.components
        ]
    )


@router.delete("/{plan_id}")
async def delete_plan(
    request: Request,
    plan_id: UUID,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    """Delete a treatment plan"""
    
    plan = db.query(Plan).filter(Plan.id == plan_id).first()
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    db.delete(plan)
    db.commit()
    
    return {"message": "Plan deleted successfully"}
