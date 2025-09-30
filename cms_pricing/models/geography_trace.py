"""Geography resolution trace model for structured logging"""

from sqlalchemy import Column, String, Float, DateTime, Text, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from cms_pricing.database import Base
import uuid
from datetime import datetime


class GeographyResolutionTrace(Base):
    """Trace record for geography resolution calls"""
    
    __tablename__ = "geography_resolution_traces"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Input parameters
    zip5 = Column(String(5), nullable=False, index=True)
    plus4 = Column(String(4), nullable=True)
    valuation_year = Column(String(4), nullable=True)
    quarter = Column(String(1), nullable=True)
    valuation_date = Column(String(10), nullable=True)  # ISO date string
    strict = Column(String(5), nullable=False)  # "true" or "false"
    
    # Resolution results
    match_level = Column(String(20), nullable=False, index=True)  # "zip+4", "zip5", "nearest", "error"
    locality_id = Column(String(10), nullable=True)
    state = Column(String(2), nullable=True)
    rural_flag = Column(String(1), nullable=True)
    
    # Nearest fallback details (if applicable)
    nearest_zip = Column(String(5), nullable=True)
    distance_miles = Column(Float, nullable=True)
    
    # Dataset information
    dataset_digest = Column(String(64), nullable=True, index=True)
    
    # Performance metrics
    latency_ms = Column(Float, nullable=False)
    
    # Service information
    service_version = Column(String(20), nullable=False)
    
    # Timestamps
    resolved_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    
    # Raw input/output for debugging (JSON)
    inputs_json = Column(JSONB, nullable=True)
    output_json = Column(JSONB, nullable=True)
    
    # Error information (if applicable)
    error_message = Column(Text, nullable=True)
    error_code = Column(String(50), nullable=True)
    
    # Composite indexes for efficient querying
    __table_args__ = (
        Index('idx_geo_trace_zip5_resolved_at', 'zip5', 'resolved_at'),
        Index('idx_geo_trace_match_level_resolved_at', 'match_level', 'resolved_at'),
        Index('idx_geo_trace_dataset_digest_resolved_at', 'dataset_digest', 'resolved_at'),
        Index('idx_geo_trace_state_resolved_at', 'state', 'resolved_at'),
    )
    
    def __repr__(self):
        return f"<GeographyResolutionTrace(zip5='{self.zip5}', match_level='{self.match_level}', latency_ms={self.latency_ms})>"

