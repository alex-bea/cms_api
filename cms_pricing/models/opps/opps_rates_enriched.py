"""
OPPS Rates Enriched Model
=========================

SQLAlchemy model for enriched OPPS rates with wage index data.

Author: CMS Pricing Platform Team
Version: 1.0.0
"""

from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import Column, Integer, String, Date, Numeric, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OPPSRatesEnriched(Base):
    """
    OPPS Rates Enriched with Wage Index Data.
    
    Enriches APC payment rates with wage index information for facility-specific
    payment calculations.
    """
    
    __tablename__ = "opps_rates_enriched"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Temporal dimensions
    year = Column(Integer, nullable=False, comment="Calendar year")
    quarter = Column(Integer, nullable=False, comment="Quarter (1-4)")
    effective_from = Column(Date, nullable=False, comment="Effective start date")
    effective_to = Column(Date, nullable=True, comment="Effective end date")
    
    # APC identification
    apc_code = Column(String(4), nullable=False, comment="APC code (4 digits)")
    
    # Facility identification
    ccn = Column(String(6), nullable=True, comment="CMS Certification Number")
    cbsa_code = Column(String(5), nullable=True, comment="CBSA code")
    
    # Wage index data
    wage_index = Column(
        Numeric(6, 3), 
        nullable=True, 
        comment="Wage index for facility"
    )
    
    # Payment rates
    payment_rate_usd = Column(
        Numeric(10, 2), 
        nullable=False, 
        comment="Base payment rate in USD"
    )
    wage_adjusted_rate_usd = Column(
        Numeric(10, 2), 
        nullable=True, 
        comment="Wage-adjusted payment rate in USD"
    )
    
    # Batch tracking
    release_id = Column(String(50), nullable=False, comment="Release identifier")
    batch_id = Column(String(50), nullable=False, comment="Batch identifier")
    
    # Metadata
    created_at = Column(Date, nullable=False, comment="Record creation date")
    updated_at = Column(Date, nullable=False, comment="Record update date")
    
    # Indexes
    __table_args__ = (
        Index('idx_opps_rates_enriched_year_quarter', 'year', 'quarter'),
        Index('idx_opps_rates_enriched_apc', 'apc_code'),
        Index('idx_opps_rates_enriched_ccn', 'ccn'),
        Index('idx_opps_rates_enriched_cbsa', 'cbsa_code'),
        Index('idx_opps_rates_enriched_effective', 'effective_from', 'effective_to'),
        Index('idx_opps_rates_enriched_release', 'release_id'),
        Index('idx_opps_rates_enriched_batch', 'batch_id'),
        # Unique constraint on natural key
        Index('idx_opps_rates_enriched_natural_key', 
              'year', 'quarter', 'apc_code', 'ccn', 'effective_from', 
              unique=True)
    )
    
    def __repr__(self):
        return (f"<OPPSRatesEnriched("
                f"year={self.year}, "
                f"quarter={self.quarter}, "
                f"apc_code='{self.apc_code}', "
                f"ccn='{self.ccn}', "
                f"payment_rate_usd={self.payment_rate_usd}, "
                f"wage_adjusted_rate_usd={self.wage_adjusted_rate_usd}, "
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
            'ccn': self.ccn,
            'cbsa_code': self.cbsa_code,
            'wage_index': float(self.wage_index) if self.wage_index else None,
            'payment_rate_usd': float(self.payment_rate_usd) if self.payment_rate_usd else None,
            'wage_adjusted_rate_usd': float(self.wage_adjusted_rate_usd) if self.wage_adjusted_rate_usd else None,
            'release_id': self.release_id,
            'batch_id': self.batch_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
