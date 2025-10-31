"""Dataset snapshots model for provenance registry

This model represents a registry of available dataset versions, enabling
deterministic snapshot selection for pricing calculations.

Part of Quick Win #1: Dataset Snapshots Table
"""

from sqlalchemy import Column, String, Date, DateTime, Index, text
from datetime import date
from typing import Dict, Any, Optional
from cms_pricing.database import Base


class DatasetSnapshot(Base):
    """Registry of dataset snapshots for deterministic provenance selection"""
    
    __tablename__ = "dataset_snapshots"
    
    # Composite primary key: (dataset_id, release_id)
    dataset_id = Column(String(50), primary_key=True, comment="Dataset identifier (e.g., MPFS, OPPS, ASC)")
    release_id = Column(String(50), primary_key=True, comment="Release identifier matching fee schedule tables")
    
    # Provenance metadata
    digest = Column(String(64), nullable=False, comment="SHA256 digest of dataset content")
    effective_from = Column(Date, nullable=False, comment="Date when snapshot becomes effective")
    effective_to = Column(Date, nullable=True, comment="Date when snapshot expires (None for current)")
    manifest_url = Column(String(500), nullable=True, comment="URL to dataset manifest/metadata")
    created_at = Column(DateTime, nullable=False, server_default=text('NOW()'), comment="Timestamp when snapshot was registered")
    
    # Indexes for efficient queries
    # Use unique names to avoid conflict with existing snapshots table indexes
    __table_args__ = (
        Index("idx_dataset_snapshots_dataset_effective", "dataset_id", "effective_from", "effective_to"),
        Index("idx_dataset_snapshots_digest", "digest"),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            "dataset_id": self.dataset_id,
            "release_id": self.release_id,
            "digest": self.digest,
            "effective_from": self.effective_from.isoformat() if self.effective_from else None,
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
            "manifest_url": self.manifest_url,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
    
    def is_active(self, valuation_date: Optional[date] = None) -> bool:
        """Check if snapshot is active for given valuation date"""
        if valuation_date is None:
            valuation_date = date.today()
        
        if self.effective_from > valuation_date:
            return False
        
        if self.effective_to and self.effective_to < valuation_date:
            return False
        
        return True
    
    def __repr__(self) -> str:
        return f"<DatasetSnapshot(dataset_id={self.dataset_id}, release_id={self.release_id}, effective_from={self.effective_from})>"

