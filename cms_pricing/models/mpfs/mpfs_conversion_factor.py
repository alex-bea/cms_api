"""
MPFS Conversion Factor Model

This model stores MPFS conversion factors (both physician and anesthesia)
that are used in payment calculations.
"""

from sqlalchemy import Column, String, Numeric, Date, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

Base = declarative_base()


class MPFSConversionFactor(Base):
    """
    MPFS Conversion Factor model
    
    Stores conversion factors for MPFS payment calculations, including
    both physician and anesthesia conversion factors.
    """
    
    __tablename__ = "mpfs_conversion_factor"
    
    # Primary key
    id = Column(String(50), primary_key=True, comment="Unique identifier")
    
    # Conversion factor details
    cf_type = Column(String(20), nullable=False, index=True, comment="Conversion factor type (physician, anesthesia)")
    cf_value = Column(Numeric(10, 4), nullable=False, comment="Conversion factor value")
    cf_description = Column(Text, nullable=True, comment="Description of the conversion factor")
    
    # Temporal fields
    effective_from = Column(Date, nullable=False, index=True, comment="Effective from date")
    effective_to = Column(Date, nullable=True, comment="Effective to date")
    
    # Release information
    release_id = Column(String(50), nullable=False, comment="Release identifier")
    vintage_year = Column(String(4), nullable=False, index=True, comment="Vintage year")
    
    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Record creation timestamp")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, comment="Record update timestamp")
    batch_id = Column(String(50), nullable=False, comment="Ingestion batch identifier")
    
    def __repr__(self):
        return f"<MPFSConversionFactor(type='{self.cf_type}', value={self.cf_value}, effective_from='{self.effective_from}')>"
    
    def is_current(self, as_of_date: Optional[date] = None) -> bool:
        """Check if this conversion factor is current as of the given date"""
        if as_of_date is None:
            as_of_date = date.today()
        
        return (
            self.effective_from <= as_of_date and
            (self.effective_to is None or self.effective_to >= as_of_date)
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'cf_type': self.cf_type,
            'cf_value': float(self.cf_value) if self.cf_value else None,
            'cf_description': self.cf_description,
            'effective_from': self.effective_from.isoformat() if self.effective_from else None,
            'effective_to': self.effective_to.isoformat() if self.effective_to else None,
            'release_id': self.release_id,
            'vintage_year': self.vintage_year,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class MPFSAbstract(Base):
    """
    MPFS Abstract model
    
    Stores abstract information and national payment data for MPFS.
    """
    
    __tablename__ = "mpfs_abstract"
    
    # Primary key
    id = Column(String(50), primary_key=True, comment="Unique identifier")
    
    # Abstract details
    abstract_type = Column(String(20), nullable=False, index=True, comment="Abstract type (national, summary, etc.)")
    title = Column(String(200), nullable=False, comment="Abstract title")
    content = Column(Text, nullable=True, comment="Abstract content")
    
    # Payment information
    national_payment_total = Column(Numeric(15, 2), nullable=True, comment="Total national payment amount")
    payment_year = Column(String(4), nullable=False, index=True, comment="Payment year")
    
    # Temporal fields
    effective_from = Column(Date, nullable=False, index=True, comment="Effective from date")
    effective_to = Column(Date, nullable=True, comment="Effective to date")
    
    # Release information
    release_id = Column(String(50), nullable=False, comment="Release identifier")
    
    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Record creation timestamp")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, comment="Record update timestamp")
    batch_id = Column(String(50), nullable=False, comment="Ingestion batch identifier")
    
    def __repr__(self):
        return f"<MPFSAbstract(type='{self.abstract_type}', title='{self.title}', year='{self.payment_year}')>"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'abstract_type': self.abstract_type,
            'title': self.title,
            'content': self.content,
            'national_payment_total': float(self.national_payment_total) if self.national_payment_total else None,
            'payment_year': self.payment_year,
            'effective_from': self.effective_from.isoformat() if self.effective_from else None,
            'effective_to': self.effective_to.isoformat() if self.effective_to else None,
            'release_id': self.release_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
