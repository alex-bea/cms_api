"""
OPPS HCPCS Crosswalk Model
==========================

SQLAlchemy model for HCPCS to APC crosswalk with Status Indicators (Addendum B data).

Author: CMS Pricing Platform Team
Version: 1.0.0
"""

from datetime import date
from typing import Optional

from sqlalchemy import Column, Integer, String, Date, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OPPSHCPCSCrosswalk(Base):
    """
    OPPS HCPCS to APC Crosswalk (Addendum B).
    
    Maps HCPCS codes to APC codes with status indicators and payment context
    from CMS OPPS quarterly releases.
    """
    
    __tablename__ = "opps_hcpcs_crosswalk"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Temporal dimensions
    year = Column(Integer, nullable=False, comment="Calendar year")
    quarter = Column(Integer, nullable=False, comment="Quarter (1-4)")
    effective_from = Column(Date, nullable=False, comment="Effective start date")
    effective_to = Column(Date, nullable=True, comment="Effective end date")
    
    # HCPCS identification
    hcpcs_code = Column(String(5), nullable=False, comment="HCPCS code (5 characters)")
    modifier = Column(String(2), nullable=True, comment="Modifier code")
    
    # Status and mapping
    status_indicator = Column(String(1), nullable=False, comment="Status indicator")
    apc_code = Column(String(4), nullable=True, comment="APC code (4 digits)")
    payment_context = Column(String(100), nullable=True, comment="Payment context")
    
    # Batch tracking
    release_id = Column(String(50), nullable=False, comment="Release identifier")
    batch_id = Column(String(50), nullable=False, comment="Batch identifier")
    
    # Metadata
    created_at = Column(Date, nullable=False, comment="Record creation date")
    updated_at = Column(Date, nullable=False, comment="Record update date")
    
    # Indexes
    __table_args__ = (
        Index('idx_opps_hcpcs_crosswalk_year_quarter', 'year', 'quarter'),
        Index('idx_opps_hcpcs_crosswalk_hcpcs', 'hcpcs_code'),
        Index('idx_opps_hcpcs_crosswalk_apc', 'apc_code'),
        Index('idx_opps_hcpcs_crosswalk_status', 'status_indicator'),
        Index('idx_opps_hcpcs_crosswalk_effective', 'effective_from', 'effective_to'),
        Index('idx_opps_hcpcs_crosswalk_release', 'release_id'),
        Index('idx_opps_hcpcs_crosswalk_batch', 'batch_id'),
        # Unique constraint on natural key
        Index('idx_opps_hcpcs_crosswalk_natural_key', 
              'year', 'quarter', 'hcpcs_code', 'modifier', 'effective_from', 
              unique=True)
    )
    
    def __repr__(self):
        return (f"<OPPSHCPCSCrosswalk("
                f"year={self.year}, "
                f"quarter={self.quarter}, "
                f"hcpcs_code='{self.hcpcs_code}', "
                f"modifier='{self.modifier}', "
                f"status_indicator='{self.status_indicator}', "
                f"apc_code='{self.apc_code}', "
                f"effective_from={self.effective_from}"
                f")>")
    
    def to_dict(self):
        """Convert to dictionary for serialization."""
        return {
            'id': self.id,
            'year': self.year,
            'quarter': self.quarter,
            'effective_from': self.effective_from.isoformat() if self.effective_from else None,
            'effective_to': self.effective_to.isoformat() if self.effective_to else None,
            'hcpcs_code': self.hcpcs_code,
            'modifier': self.modifier,
            'status_indicator': self.status_indicator,
            'apc_code': self.apc_code,
            'payment_context': self.payment_context,
            'release_id': self.release_id,
            'batch_id': self.batch_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
