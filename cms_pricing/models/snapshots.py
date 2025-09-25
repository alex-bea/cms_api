"""Dataset snapshots and versioning"""

from sqlalchemy import Column, String, Date, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class Snapshot(Base):
    """Dataset snapshots with versioning and digests"""
    
    __tablename__ = "snapshots"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(String(50), nullable=False, index=True)  # MPFS, OPPS, etc.
    effective_from = Column(Date, nullable=False, index=True)
    effective_to = Column(Date, nullable=True, index=True)
    digest = Column(String(64), nullable=False, index=True)  # SHA256 digest
    source_url = Column(String(500), nullable=True)
    manifest_json = Column(Text, nullable=True)  # Full manifest as JSON
    created_at = Column(Date, nullable=False)
    
    # Indexes
    __table_args__ = (
        Index("idx_snapshots_dataset_effective", "dataset_id", "effective_from", "effective_to"),
        Index("idx_snapshots_digest", "digest"),
    )
