"""Plan and component models for treatment plans"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Text, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
from cms_pricing.database import Base
import uuid


class Plan(Base):
    """Treatment plan definitions"""
    
    __tablename__ = "plans"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    metadata_json = Column("metadata", Text, nullable=True)  # JSON metadata stored in DB column 'metadata'
    created_by = Column(String(100), nullable=True)
    created_at = Column(Date, nullable=False)
    updated_at = Column(Date, nullable=True)
    
    # Relationships
    components = relationship("PlanComponent", back_populates="plan", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index("idx_plans_name", "name"),
        Index("idx_plans_created_at", "created_at"),
    )

    # Note: avoid defining an attribute named `metadata` on the class since
    # SQLAlchemy uses that name for the MetaData object at the class level.
    # Use `metadata_json` to store JSON metadata from the application.


class PlanComponent(Base):
    """Individual components within a treatment plan"""
    
    __tablename__ = "plan_components"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plan_id = Column(UUID(as_uuid=True), ForeignKey("plans.id"), nullable=False, index=True)
    code = Column(String(5), nullable=False, index=True)  # HCPCS/CPT
    setting = Column(String(20), nullable=False)  # MPFS, OPPS, ASC, IPPS, CLFS, DMEPOS
    units = Column(Float, nullable=False, default=1.0)
    utilization_weight = Column(Float, nullable=False, default=1.0)
    professional_component = Column(Boolean, nullable=False, default=True)
    facility_component = Column(Boolean, nullable=False, default=True)
    modifiers = Column(ARRAY(String), nullable=True)  # Array of modifiers like -26, -TC, -50
    pos = Column(String(2), nullable=True)  # Place of service code
    ndc11 = Column(String(11), nullable=True)  # NDC for drug components
    wastage_units = Column(Float, nullable=False, default=0.0)  # Future use
    sequence = Column(Integer, nullable=False, default=1)  # Order within plan
    created_at = Column(Date, nullable=False)
    
    # Relationships
    plan = relationship("Plan", back_populates="components")
    
    # Indexes
    __table_args__ = (
        Index("idx_plan_components_plan_code", "plan_id", "code"),
        Index("idx_plan_components_setting", "setting"),
    )
