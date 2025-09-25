"""Fee schedule models for all CMS payment systems"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Index
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class FeeMPFS(Base):
    """Medicare Physician Fee Schedule RVUs and rates"""
    
    __tablename__ = "fee_mpfs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    revision = Column(String(1), nullable=False, default="A")  # A, B, C revisions
    locality_id = Column(String(10), nullable=False, index=True)
    hcpcs = Column(String(5), nullable=False, index=True)
    work_rvu = Column(Float, nullable=True)
    pe_nf_rvu = Column(Float, nullable=True)  # Non-facility PE RVU
    pe_fac_rvu = Column(Float, nullable=True)  # Facility PE RVU
    mp_rvu = Column(Float, nullable=True)  # Malpractice RVU
    global_days = Column(Integer, nullable=True)
    status_indicator = Column(String(2), nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_mpfs_year_locality_hcpcs", "year", "locality_id", "hcpcs"),
        Index("idx_mpfs_effective", "effective_from", "effective_to"),
    )


class GPCI(Base):
    """Geographic Practice Cost Indices"""
    
    __tablename__ = "gpci"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    locality_name = Column(String(100), nullable=False)
    gpci_work = Column(Float, nullable=False)
    gpci_pe = Column(Float, nullable=False)
    gpci_mp = Column(Float, nullable=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_gpci_year_locality", "year", "locality_id"),
    )


class ConversionFactor(Base):
    """Medicare conversion factors"""
    
    __tablename__ = "conversion_factors"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    cf = Column(Float, nullable=False)  # Conversion factor
    source = Column(String(50), nullable=False)  # MPFS, ASC, etc.
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)


class FeeOPPS(Base):
    """Outpatient Prospective Payment System rates"""
    
    __tablename__ = "fee_opps"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(1), nullable=False, index=True)
    hcpcs = Column(String(5), nullable=False, index=True)
    status_indicator = Column(String(2), nullable=True)
    apc = Column(String(5), nullable=True)
    national_unadj_rate = Column(Float, nullable=True)
    packaging_flag = Column(String(10), nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_opps_year_quarter_hcpcs", "year", "quarter", "hcpcs"),
        Index("idx_opps_effective", "effective_from", "effective_to"),
    )


class WageIndex(Base):
    """Wage index by CBSA for OPPS and IPPS"""
    
    __tablename__ = "wage_index"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(1), nullable=True, index=True)  # None for IPPS
    cbsa = Column(String(5), nullable=False, index=True)
    wage_index = Column(Float, nullable=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_wage_year_cbsa", "year", "cbsa"),
    )


class FeeASC(Base):
    """Ambulatory Surgical Center fee schedule"""
    
    __tablename__ = "fee_asc"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(1), nullable=False, index=True)
    hcpcs = Column(String(5), nullable=False, index=True)
    asc_rate = Column(Float, nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_asc_year_quarter_hcpcs", "year", "quarter", "hcpcs"),
    )


class FeeIPPS(Base):
    """Inpatient Prospective Payment System DRG weights"""
    
    __tablename__ = "fee_ipps"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    fy = Column(Integer, nullable=False, index=True)  # Fiscal year
    drg = Column(String(3), nullable=False, index=True)
    weight = Column(Float, nullable=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_ipps_fy_drg", "fy", "drg"),
    )


class IPPSBaseRate(Base):
    """IPPS base rates by fiscal year"""
    
    __tablename__ = "ipps_base_rates"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    fy = Column(Integer, nullable=False, index=True)
    operating_base = Column(Float, nullable=False)
    capital_base = Column(Float, nullable=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)


class FeeCLFS(Base):
    """Clinical Laboratory Fee Schedule"""
    
    __tablename__ = "fee_clfs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(1), nullable=False, index=True)
    hcpcs = Column(String(5), nullable=False, index=True)
    fee = Column(Float, nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_clfs_year_quarter_hcpcs", "year", "quarter", "hcpcs"),
    )


class FeeDMEPOS(Base):
    """Durable Medical Equipment, Prosthetics, Orthotics, and Supplies fee schedule"""
    
    __tablename__ = "fee_dmepos"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(1), nullable=False, index=True)
    code = Column(String(5), nullable=False, index=True)
    rural_flag = Column(Boolean, nullable=False, default=False)
    fee = Column(Float, nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_dmepos_year_quarter_code", "year", "quarter", "code"),
    )
