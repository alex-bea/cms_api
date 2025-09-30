"""ZIP code geometry model for geographic calculations"""

from sqlalchemy import Column, String, Float, Boolean, Date, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class ZipGeometry(Base):
    """ZIP code geometry data for distance calculations"""
    
    __tablename__ = "zip_geometry"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zip5 = Column(String(5), nullable=False, index=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    state = Column(String(2), nullable=False, index=True)
    is_pobox = Column(Boolean, nullable=False, default=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Composite index for efficient geographic queries
    __table_args__ = (
        Index('idx_zip_geometry_state_effective', 'state', 'effective_from', 'effective_to'),
        Index('idx_zip_geometry_coords', 'lat', 'lon'),
    )
    
    def __repr__(self):
        return f"<ZipGeometry(zip5='{self.zip5}', lat={self.lat}, lon={self.lon}, state='{self.state}', is_pobox={self.is_pobox})>"

