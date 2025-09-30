"""
Reference SI Lookup Model
=========================

SQLAlchemy model for Status Indicator lookup reference data.

Author: CMS Pricing Platform Team
Version: 1.0.0
"""

from datetime import date
from typing import Optional

from sqlalchemy import Column, Integer, String, Date, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class RefSILookup(Base):
    """
    Status Indicator Lookup Reference Table.
    
    Provides descriptions and payment categories for OPPS status indicators
    used in HCPCS crosswalk data.
    """
    
    __tablename__ = "ref_si_lookup"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Status indicator
    status_indicator = Column(String(1), nullable=False, comment="Status indicator code")
    
    # Description and categorization
    description = Column(String(500), nullable=False, comment="Status indicator description")
    payment_category = Column(String(100), nullable=True, comment="Payment category")
    
    # Temporal dimensions
    effective_from = Column(Date, nullable=False, comment="Effective start date")
    effective_to = Column(Date, nullable=True, comment="Effective end date")
    
    # Metadata
    created_at = Column(Date, nullable=False, comment="Record creation date")
    updated_at = Column(Date, nullable=False, comment="Record update date")
    
    # Indexes
    __table_args__ = (
        Index('idx_ref_si_lookup_status', 'status_indicator'),
        Index('idx_ref_si_lookup_effective', 'effective_from', 'effective_to'),
        # Unique constraint on natural key
        Index('idx_ref_si_lookup_natural_key', 
              'status_indicator', 'effective_from', 
              unique=True)
    )
    
    def __repr__(self):
        return (f"<RefSILookup("
                f"status_indicator='{self.status_indicator}', "
                f"description='{self.description[:50]}...', "
                f"payment_category='{self.payment_category}', "
                f"effective_from={self.effective_from}"
                f")>")
    
    def to_dict(self):
        """Convert to dictionary for serialization."""
        return {
            'id': self.id,
            'status_indicator': self.status_indicator,
            'description': self.description,
            'payment_category': self.payment_category,
            'effective_from': self.effective_from.isoformat() if self.effective_from else None,
            'effective_to': self.effective_to.isoformat() if self.effective_to else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
