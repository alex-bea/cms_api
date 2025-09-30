"""
OPPS APC Payment Model
======================

SQLAlchemy model for OPPS APC payment rates (Addendum A data).

Author: CMS Pricing Platform Team
Version: 1.0.0
"""

from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import Column, Integer, String, Date, Numeric, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OPPSAPCPayment(Base):
    """
    OPPS APC Payment Rates (Addendum A).
    
    Stores APC payment rates, relative weights, and packaging information
    from CMS OPPS quarterly releases.
    """
    
    __tablename__ = "opps_apc_payment"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Temporal dimensions
    year = Column(Integer, nullable=False, comment="Calendar year")
    quarter = Column(Integer, nullable=False, comment="Quarter (1-4)")
    effective_from = Column(Date, nullable=False, comment="Effective start date")
    effective_to = Column(Date, nullable=True, comment="Effective end date")
    
    # APC identification
    apc_code = Column(String(4), nullable=False, comment="APC code (4 digits)")
    apc_description = Column(String(500), nullable=True, comment="APC description")
    
    # Payment information
    payment_rate_usd = Column(
        Numeric(10, 2), 
        nullable=False, 
        comment="Payment rate in USD"
    )
    relative_weight = Column(
        Numeric(8, 4), 
        nullable=False, 
        comment="Relative weight"
    )
    
    # Packaging and flags
    packaging_flag = Column(String(1), nullable=True, comment="Packaging flag")
    
    # Batch tracking
    release_id = Column(String(50), nullable=False, comment="Release identifier")
    batch_id = Column(String(50), nullable=False, comment="Batch identifier")
    
    # Metadata
    created_at = Column(Date, nullable=False, comment="Record creation date")
    updated_at = Column(Date, nullable=False, comment="Record update date")
    
    # Indexes
    __table_args__ = (
        Index('idx_opps_apc_payment_year_quarter', 'year', 'quarter'),
        Index('idx_opps_apc_payment_apc_code', 'apc_code'),
        Index('idx_opps_apc_payment_effective', 'effective_from', 'effective_to'),
        Index('idx_opps_apc_payment_release', 'release_id'),
        Index('idx_opps_apc_payment_batch', 'batch_id'),
        # Unique constraint on natural key
        Index('idx_opps_apc_payment_natural_key', 
              'year', 'quarter', 'apc_code', 'effective_from', 
              unique=True)
    )
    
    def __repr__(self):
        return (f"<OPPSAPCPayment("
                f"year={self.year}, "
                f"quarter={self.quarter}, "
                f"apc_code='{self.apc_code}', "
                f"payment_rate_usd={self.payment_rate_usd}, "
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
            'apc_code': self.apc_code,
            'apc_description': self.apc_description,
            'payment_rate_usd': float(self.payment_rate_usd) if self.payment_rate_usd else None,
            'relative_weight': float(self.relative_weight) if self.relative_weight else None,
            'packaging_flag': self.packaging_flag,
            'release_id': self.release_id,
            'batch_id': self.batch_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
