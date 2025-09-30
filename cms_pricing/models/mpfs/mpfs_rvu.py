"""
MPFS RVU Model - Curated view referencing PPRRVU data

This model creates a curated view of RVU data specifically for MPFS use cases,
referencing the existing PPRRVU table while adding MPFS-specific fields and logic.
"""

from sqlalchemy import Column, String, Integer, Numeric, Boolean, Date, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

Base = declarative_base()


class MPFSRVU(Base):
    """
    MPFS RVU curated view referencing PPRRVU data
    
    This table provides a curated view of RVU data specifically for MPFS pricing
    calculations, referencing the existing PPRRVU table while adding MPFS-specific
    fields and business logic.
    """
    
    __tablename__ = "mpfs_rvu"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural key components (references PPRRVU)
    hcpcs = Column(String(5), nullable=False, index=True, comment="HCPCS code")
    modifier = Column(String(2), nullable=True, index=True, comment="Modifier code")
    effective_from = Column(Date, nullable=False, index=True, comment="Effective from date")
    
    # RVU components (references PPRRVU)
    rvu_work = Column(Numeric(8, 3), nullable=True, comment="Work RVU")
    rvu_pe_nonfac = Column(Numeric(8, 3), nullable=True, comment="PE RVU non-facility")
    rvu_pe_fac = Column(Numeric(8, 3), nullable=True, comment="PE RVU facility")
    rvu_malp = Column(Numeric(8, 3), nullable=True, comment="Malpractice RVU")
    
    # Status and policy indicators (references PPRRVU)
    status_code = Column(String(2), nullable=False, comment="Status indicator")
    global_days = Column(String(3), nullable=True, comment="Global period days")
    na_indicator = Column(String(1), nullable=True, comment="Not applicable indicator")
    opps_cap_applicable = Column(Boolean, nullable=False, default=False, comment="OPPS cap applies")
    
    # MPFS-specific fields
    is_payable = Column(Boolean, nullable=False, comment="Whether item is payable under MPFS")
    payment_category = Column(String(20), nullable=True, comment="Payment category (e.g., 'surgery', 'evaluation')")
    bilateral_indicator = Column(Boolean, nullable=False, default=False, comment="Bilateral surgery indicator")
    multiple_procedure_indicator = Column(Boolean, nullable=False, default=False, comment="Multiple procedure indicator")
    assistant_surgery_indicator = Column(Boolean, nullable=False, default=False, comment="Assistant surgery indicator")
    co_surgeon_indicator = Column(Boolean, nullable=False, default=False, comment="Co-surgeon indicator")
    team_surgery_indicator = Column(Boolean, nullable=False, default=False, comment="Team surgery indicator")
    
    # Temporal fields
    effective_to = Column(Date, nullable=True, comment="Effective to date")
    release_id = Column(String(50), nullable=False, comment="Release identifier")
    
    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Record creation timestamp")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, comment="Record update timestamp")
    batch_id = Column(String(50), nullable=False, comment="Ingestion batch identifier")
    
    # Computed fields
    total_rvu = Column(Numeric(8, 3), nullable=True, comment="Total RVU (work + pe + malp)")
    is_surgery = Column(Boolean, nullable=False, default=False, comment="Whether this is a surgical procedure")
    is_evaluation = Column(Boolean, nullable=False, default=False, comment="Whether this is an evaluation service")
    is_procedure = Column(Boolean, nullable=False, default=False, comment="Whether this is a procedure")
    
    def __repr__(self):
        return f"<MPFSRVU(hcpcs='{self.hcpcs}', modifier='{self.modifier}', effective_from='{self.effective_from}')>"
    
    def calculate_total_rvu(self) -> Optional[Decimal]:
        """Calculate total RVU from components"""
        if all(x is not None for x in [self.rvu_work, self.rvu_pe_nonfac, self.rvu_malp]):
            return self.rvu_work + self.rvu_pe_nonfac + self.rvu_malp
        return None
    
    def is_payable_item(self) -> bool:
        """Determine if this item is payable under MPFS"""
        # Basic payable logic - can be enhanced
        return (
            self.status_code in ['A', 'R', 'T'] and
            self.rvu_work is not None and
            self.rvu_work > 0
        )
    
    def get_payment_category(self) -> str:
        """Determine payment category based on status and RVU components"""
        if self.status_code == 'A':
            if self.rvu_work and self.rvu_work > 0:
                return 'surgery'
            else:
                return 'evaluation'
        elif self.status_code == 'R':
            return 'radiology'
        elif self.status_code == 'T':
            return 'therapy'
        else:
            return 'other'
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses"""
        return {
            'hcpcs': self.hcpcs,
            'modifier': self.modifier,
            'effective_from': self.effective_from.isoformat() if self.effective_from else None,
            'effective_to': self.effective_to.isoformat() if self.effective_to else None,
            'rvu_work': float(self.rvu_work) if self.rvu_work else None,
            'rvu_pe_nonfac': float(self.rvu_pe_nonfac) if self.rvu_pe_nonfac else None,
            'rvu_pe_fac': float(self.rvu_pe_fac) if self.rvu_pe_fac else None,
            'rvu_malp': float(self.rvu_malp) if self.rvu_malp else None,
            'status_code': self.status_code,
            'global_days': self.global_days,
            'na_indicator': self.na_indicator,
            'opps_cap_applicable': self.opps_cap_applicable,
            'is_payable': self.is_payable,
            'payment_category': self.payment_category,
            'bilateral_indicator': self.bilateral_indicator,
            'multiple_procedure_indicator': self.multiple_procedure_indicator,
            'assistant_surgery_indicator': self.assistant_surgery_indicator,
            'co_surgeon_indicator': self.co_surgeon_indicator,
            'team_surgery_indicator': self.team_surgery_indicator,
            'total_rvu': float(self.total_rvu) if self.total_rvu else None,
            'is_surgery': self.is_surgery,
            'is_evaluation': self.is_evaluation,
            'is_procedure': self.is_procedure,
            'release_id': self.release_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
