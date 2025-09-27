"""Geography models for ZIP to locality/CBSA mapping"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class Geography(Base):
    """ZIP code to locality mapping for Medicare pricing (ZIP+4-first per PRD)"""
    
    __tablename__ = "geography"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zip5 = Column(String(5), nullable=False, index=True)
    plus4 = Column(String(4), nullable=True, index=True)  # ZIP+4 add-on
    has_plus4 = Column(Integer, nullable=False, default=0)  # 1 if plus4 present, 0 if ZIP5-only
    state = Column(String(2), nullable=True, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    locality_name = Column(String(100), nullable=True)
    carrier = Column(String(10), nullable=True)  # MAC/Carrier jurisdiction code
    rural_flag = Column(String(1), nullable=True)  # R, B, or blank for DMEPOS/rural logic
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    dataset_id = Column(String(20), nullable=False, default="ZIP_LOCALITY")
    dataset_digest = Column(String(64), nullable=False)  # SHA256 of source data
    created_at = Column(Date, nullable=False)
    
    # Indexes per PRD section 7.1
    __table_args__ = (
        Index("idx_geo_zip5", "zip5"),
        Index("idx_geo_zip5_plus4", "zip5", "plus4"),
        Index("idx_geo_effective", "effective_from", "effective_to"),
        Index("idx_geo_locality", "locality_id"),
        Index("idx_geo_dataset", "dataset_id", "dataset_digest"),
    )
