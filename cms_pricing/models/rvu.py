"""RVU data models for PPRRVU, GPCI, OPPSCAP, ANES, and Locality-County data"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Text, Index, Numeric, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
from cms_pricing.database import Base
import uuid


class Release(Base):
    """RVU data release information"""
    
    __tablename__ = "releases"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(String(20), nullable=False)  # RVU_FULL, GPCI, OPPSCAP, etc.
    source_version = Column(String(10), nullable=False)  # 2025D, etc.
    imported_at = Column(Date, nullable=False)
    notes = Column(Text, nullable=True)
    
    # Relationships
    rvu_items = relationship("RVUItem", back_populates="release")
    gpci_indices = relationship("GPCIIndex", back_populates="release")
    opps_caps = relationship("OPPSCap", back_populates="release")
    anes_cfs = relationship("AnesCF", back_populates="release")
    locality_counties = relationship("LocalityCounty", back_populates="release")
    
    # Indexes
    __table_args__ = (
        Index("idx_releases_type", "type"),
        Index("idx_releases_source_version", "source_version"),
        Index("idx_releases_imported_at", "imported_at"),
    )


class RVUItem(Base):
    """PPRRVU data items"""
    
    __tablename__ = "rvu_items"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id = Column(UUID(as_uuid=True), ForeignKey('releases.id'), nullable=False, index=True)
    hcpcs_code = Column(String(5), nullable=False, index=True)
    modifiers = Column(ARRAY(String), nullable=True)
    modifier_key = Column(String(10), nullable=True, index=True)  # Normalized modifier key
    description = Column(Text, nullable=True)
    status_code = Column(String(2), nullable=True, index=True)
    work_rvu = Column(Numeric(10, 4), nullable=True)
    pe_rvu_nonfac = Column(Numeric(10, 4), nullable=True)
    pe_rvu_fac = Column(Numeric(10, 4), nullable=True)
    mp_rvu = Column(Numeric(10, 4), nullable=True)
    na_indicator = Column(String(1), nullable=True)
    global_days = Column(String(3), nullable=True)
    bilateral_ind = Column(String(1), nullable=True)
    multiple_proc_ind = Column(String(1), nullable=True)
    assistant_surg_ind = Column(String(1), nullable=True)
    co_surg_ind = Column(String(1), nullable=True)
    team_surg_ind = Column(String(1), nullable=True)
    endoscopic_base = Column(String(1), nullable=True)
    conversion_factor = Column(Numeric(10, 4), nullable=True)
    physician_supervision = Column(String(2), nullable=True)
    diag_imaging_family = Column(String(10), nullable=True)
    total_nonfac = Column(Numeric(10, 2), nullable=True)
    total_fac = Column(Numeric(10, 2), nullable=True)
    effective_start = Column(Date, nullable=True)
    effective_end = Column(Date, nullable=True)
    source_file = Column(String(100), nullable=True)
    row_num = Column(Integer, nullable=True)
    
    # Relationships
    release = relationship("Release", back_populates="rvu_items")
    
    # Indexes
    __table_args__ = (
        Index("idx_rvu_items_hcpcs", "hcpcs_code"),
        Index("idx_rvu_items_status", "status_code"),
        Index("idx_rvu_items_effective", "effective_start", "effective_end"),
        Index("idx_rvu_items_release_hcpcs", "release_id", "hcpcs_code"),
    )


class GPCIIndex(Base):
    """GPCI indices by MAC and locality"""
    
    __tablename__ = "gpci_indices"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id = Column(UUID(as_uuid=True), ForeignKey('releases.id'), nullable=False, index=True)
    mac = Column(String(10), nullable=False, index=True)
    state = Column(String(2), nullable=False, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    locality_name = Column(String(100), nullable=True)
    work_gpci = Column(Numeric(10, 4), nullable=True)
    pe_gpci = Column(Numeric(10, 4), nullable=True)
    mp_gpci = Column(Numeric(10, 4), nullable=True)
    effective_start = Column(Date, nullable=True)
    effective_end = Column(Date, nullable=True)
    source_file = Column(String(100), nullable=True)
    row_num = Column(Integer, nullable=True)
    
    # Relationships
    release = relationship("Release", back_populates="gpci_indices")
    
    # Indexes
    __table_args__ = (
        Index("idx_gpci_mac_locality", "mac", "locality_id"),
        Index("idx_gpci_state", "state"),
        Index("idx_gpci_effective", "effective_start", "effective_end"),
        Index("idx_gpci_release_mac", "release_id", "mac"),
    )


class OPPSCap(Base):
    """OPPS caps by HCPCS and locality"""
    
    __tablename__ = "opps_caps"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id = Column(UUID(as_uuid=True), ForeignKey('releases.id'), nullable=False, index=True)
    hcpcs_code = Column(String(5), nullable=False, index=True)
    modifier = Column(String(2), nullable=True, index=True)
    proc_status = Column(String(2), nullable=True)
    mac = Column(String(10), nullable=False, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    price_fac = Column(Numeric(10, 2), nullable=True)
    price_nonfac = Column(Numeric(10, 2), nullable=True)
    effective_start = Column(Date, nullable=True)
    effective_end = Column(Date, nullable=True)
    source_file = Column(String(100), nullable=True)
    row_num = Column(Integer, nullable=True)
    
    # Relationships
    release = relationship("Release", back_populates="opps_caps")
    
    # Indexes
    __table_args__ = (
        Index("idx_opps_hcpcs", "hcpcs_code"),
        Index("idx_opps_mac_locality", "mac", "locality_id"),
        Index("idx_opps_effective", "effective_start", "effective_end"),
        Index("idx_opps_release_hcpcs", "release_id", "hcpcs_code"),
    )


class AnesCF(Base):
    """Anesthesia conversion factors by MAC and locality"""
    
    __tablename__ = "anes_cfs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id = Column(UUID(as_uuid=True), ForeignKey('releases.id'), nullable=False, index=True)
    mac = Column(String(10), nullable=False, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    locality_name = Column(String(100), nullable=True)
    anesthesia_cf = Column(Numeric(10, 4), nullable=True)
    effective_start = Column(Date, nullable=True)
    effective_end = Column(Date, nullable=True)
    source_file = Column(String(100), nullable=True)
    row_num = Column(Integer, nullable=True)
    
    # Relationships
    release = relationship("Release", back_populates="anes_cfs")
    
    # Indexes
    __table_args__ = (
        Index("idx_anes_mac_locality", "mac", "locality_id"),
        Index("idx_anes_effective", "effective_start", "effective_end"),
        Index("idx_anes_release_mac", "release_id", "mac"),
    )


class LocalityCounty(Base):
    """Locality to county crosswalk"""
    
    __tablename__ = "locality_counties"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id = Column(UUID(as_uuid=True), ForeignKey('releases.id'), nullable=False, index=True)
    mac = Column(String(10), nullable=False, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    state = Column(String(2), nullable=False, index=True)
    fee_schedule_area = Column(String(10), nullable=True)
    county_name = Column(String(100), nullable=True)
    effective_start = Column(Date, nullable=True)
    effective_end = Column(Date, nullable=True)
    source_file = Column(String(100), nullable=True)
    row_num = Column(Integer, nullable=True)
    
    # Relationships
    release = relationship("Release", back_populates="locality_counties")
    
    # Indexes
    __table_args__ = (
        Index("idx_locco_mac_locality", "mac", "locality_id"),
        Index("idx_locco_state", "state"),
        Index("idx_locco_effective", "effective_start", "effective_end"),
        Index("idx_locco_release_mac", "release_id", "mac"),
    )
