"""Facility-specific rates from MRF and TiC files"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class HospitalMRFRate(Base):
    """Hospital Machine Readable File negotiated rates"""
    
    __tablename__ = "hospital_mrf_rates"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ccn = Column(String(6), nullable=False, index=True)  # CMS Certification Number
    payer_name = Column(String(200), nullable=True, index=True)
    plan_name = Column(String(200), nullable=True, index=True)
    code = Column(String(5), nullable=False, index=True)  # HCPCS/CPT/DRG
    code_type = Column(String(10), nullable=False)  # HCPCS, CPT, DRG, REV
    amount_type = Column(String(10), nullable=False)  # USD, PERCENT
    amount_value = Column(Float, nullable=False)
    charge_type = Column(String(20), nullable=True)  # negotiated, cash, etc.
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    fetched_at = Column(Date, nullable=False)
    mrf_active = Column(Boolean, nullable=False, default=True)  # Soft delete flag
    
    # Indexes
    __table_args__ = (
        Index("idx_mrf_ccn_code", "ccn", "code", "mrf_active"),
        Index("idx_mrf_payer_plan", "payer_name", "plan_name"),
        Index("idx_mrf_effective", "effective_from", "effective_to"),
    )
