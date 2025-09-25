"""Drug pricing models for Part B ASP and NADAC"""

from sqlalchemy import Column, String, Integer, Float, Date, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class DrugASP(Base):
    """Part B Average Sales Price drug data"""
    
    __tablename__ = "drugs_asp"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(1), nullable=False, index=True)
    hcpcs = Column(String(5), nullable=False, index=True)
    asp_per_unit = Column(Float, nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_asp_year_quarter_hcpcs", "year", "quarter", "hcpcs"),
    )


class DrugNADAC(Base):
    """NADAC (National Average Drug Acquisition Cost) reference pricing"""
    
    __tablename__ = "drugs_nadac"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    as_of = Column(Date, nullable=False, index=True)
    ndc11 = Column(String(11), nullable=False, index=True)
    unit_price = Column(Float, nullable=True)
    unit_type = Column(String(20), nullable=True)  # ML, TABLET, etc.
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_nadac_as_of_ndc", "as_of", "ndc11"),
    )


class NDCHCPCSXwalk(Base):
    """NDC to HCPCS crosswalk with unit conversions"""
    
    __tablename__ = "ndc_hcpcs_xwalk"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ndc11 = Column(String(11), nullable=False, index=True)
    hcpcs = Column(String(5), nullable=False, index=True)
    units_per_hcpcs = Column(Float, nullable=False, default=1.0)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_ndc_hcpcs_xwalk", "ndc11", "hcpcs"),
    )
