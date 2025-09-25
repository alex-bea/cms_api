"""Code models for HCPCS/CPT codes and status indicators"""

from sqlalchemy import Column, String, Integer, Date, Boolean, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class Code(Base):
    """HCPCS/CPT codes with metadata"""
    
    __tablename__ = "codes"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hcpcs = Column(String(5), nullable=False, index=True)
    description = Column(Text, nullable=True)
    short_description = Column(String(100), nullable=True)
    global_days = Column(Integer, nullable=True)  # Global period days
    status_indicator = Column(String(2), nullable=True)
    setting_flags = Column(String(20), nullable=True)  # JSON string of allowed settings
    professional_component = Column(Boolean, nullable=False, default=True)
    facility_component = Column(Boolean, nullable=False, default=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    created_at = Column(Date, nullable=False)
    
    # Indexes
    __table_args__ = (
        Index("idx_codes_hcpcs_effective", "hcpcs", "effective_from", "effective_to"),
        Index("idx_codes_status_indicator", "status_indicator", "effective_from"),
    )


class CodeStatus(Base):
    """Status indicators and their meanings"""
    
    __tablename__ = "code_status"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status_indicator = Column(String(2), nullable=False, unique=True)
    description = Column(Text, nullable=False)
    packaging_rule = Column(String(50), nullable=True)  # J1, N, Q1, etc.
    payment_rule = Column(String(100), nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
