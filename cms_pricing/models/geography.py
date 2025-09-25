"""Geography models for ZIP to locality/CBSA mapping"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class Geography(Base):
    """ZIP code to locality/CBSA mapping with population shares"""
    
    __tablename__ = "geography"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zip5 = Column(String(5), nullable=False, index=True)
    locality_id = Column(String(10), nullable=True, index=True)
    locality_name = Column(String(100), nullable=True)
    cbsa = Column(String(5), nullable=True, index=True)
    cbsa_name = Column(String(100), nullable=True)
    county_fips = Column(String(5), nullable=True)
    state_code = Column(String(2), nullable=True)
    population_share = Column(Float, nullable=False, default=1.0)
    is_rural_dmepos = Column(Boolean, nullable=False, default=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    created_at = Column(Date, nullable=False)
    
    # Indexes for efficient lookups
    __table_args__ = (
        Index("idx_geography_zip_effective", "zip5", "effective_from", "effective_to"),
        Index("idx_geography_locality", "locality_id", "effective_from"),
        Index("idx_geography_cbsa", "cbsa", "effective_from"),
    )
