"""Data models for nearest ZIP resolver per PRD v1.0"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Index, Text, CHAR, NUMERIC, TIMESTAMP, BIGINT
from sqlalchemy.dialects.postgresql import UUID
from cms_pricing.database import Base
import uuid


class ZCTACoords(Base):
    """Gazetteer centroids for ZCTA5 areas"""
    
    __tablename__ = "zcta_coords"
    
    zcta5 = Column(CHAR(5), primary_key=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_zcta_coords_vintage", "vintage"),
        Index("idx_zcta_coords_coords", "lat", "lon"),
    )


class ZipToZCTA(Base):
    """ZIP ↔ ZCTA crosswalk from UDS/GeoCare"""
    
    __tablename__ = "zip_to_zcta"
    
    zip5 = Column(CHAR(5), primary_key=True)
    zcta5 = Column(CHAR(5), nullable=False)
    relationship = Column(Text, nullable=True)
    weight = Column(NUMERIC, nullable=True)
    city = Column(Text, nullable=True)
    state = Column(Text, nullable=True)
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_zip_to_zcta_zcta5", "zcta5"),
        Index("idx_zip_to_zcta_vintage", "vintage"),
        Index("idx_zip_to_zcta_relationship", "relationship"),
    )


class CMSZipLocality(Base):
    """CMS ZIP5 → locality/state mapping"""
    
    __tablename__ = "cms_zip_locality"
    
    zip5 = Column(CHAR(5), primary_key=True)
    state = Column(CHAR(2), nullable=False)
    locality = Column(String(10), nullable=False)
    carrier_mac = Column(String(10), nullable=True)
    rural_flag = Column(Boolean, nullable=True)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_cms_zip_locality_state", "state"),
        Index("idx_cms_zip_locality_locality", "locality"),
        Index("idx_cms_zip_locality_effective", "effective_from", "effective_to"),
        Index("idx_cms_zip_locality_vintage", "vintage"),
    )


class ZIP9Overrides(Base):
    """CMS ZIP9 override ranges"""
    
    __tablename__ = "zip9_overrides"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zip9_low = Column(CHAR(9), nullable=False, index=True)
    zip9_high = Column(CHAR(9), nullable=False, index=True)
    state = Column(CHAR(2), nullable=False)
    locality = Column(String(10), nullable=False)
    rural_flag = Column(Boolean, nullable=True)
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_zip9_overrides_state", "state"),
        Index("idx_zip9_overrides_locality", "locality"),
        Index("idx_zip9_overrides_vintage", "vintage"),
        Index("idx_zip9_overrides_range", "zip9_low", "zip9_high", unique=True)
    )


class ZCTADistances(Base):
    """Optional: NBER distances (subset import by radius)"""
    
    __tablename__ = "zcta_distances"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zcta5_a = Column(CHAR(5), nullable=False, index=True)
    zcta5_b = Column(CHAR(5), nullable=False, index=True)
    miles = Column(Float, nullable=False)
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_zcta_distances_vintage", "vintage"),
        Index("idx_zcta_distances_miles", "miles"),
        Index("idx_zcta_distances_pair", "zcta5_a", "zcta5_b", unique=True)
    )


class NBERCentroids(Base):
    """NBER ZCTA centroids (fallback for missing Gazetteer data)"""
    
    __tablename__ = "nber_centroids"
    
    zcta5 = Column(CHAR(5), primary_key=True, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_nber_centroids_vintage", "vintage"),
    )


class ZipMetadata(Base):
    """SimpleMaps ZIP metadata (for PO Box flag)"""
    
    __tablename__ = "zip_metadata"
    
    zip5 = Column(CHAR(5), primary_key=True)
    zcta_bool = Column(Boolean, nullable=True)
    parent_zcta = Column(CHAR(5), nullable=True)
    military_bool = Column(Boolean, nullable=True)
    population = Column(Integer, nullable=True)
    is_pobox = Column(Boolean, nullable=False, default=False)  # Computed field
    vintage = Column(String(10), nullable=False)
    source_filename = Column(Text, nullable=True)
    ingest_run_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_zip_metadata_is_pobox", "is_pobox"),
        Index("idx_zip_metadata_vintage", "vintage"),
        Index("idx_zip_metadata_population", "population"),
    )


class IngestRun(Base):
    """Ingest run provenance"""
    
    __tablename__ = "ingest_runs"
    
    run_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_url = Column(Text, nullable=False)
    filename = Column(Text, nullable=True)
    sha256 = Column(CHAR(64), nullable=True)
    bytes = Column(BIGINT, nullable=True)
    started_at = Column(TIMESTAMP(timezone=True), nullable=False)
    finished_at = Column(TIMESTAMP(timezone=True), nullable=True)
    row_count = Column(BIGINT, nullable=True)
    tool_version = Column(Text, nullable=True)
    status = Column(String(20), nullable=False)  # success, failed, partial
    notes = Column(Text, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index("idx_ingest_runs_status", "status"),
        Index("idx_ingest_runs_started_at", "started_at"),
        Index("idx_ingest_runs_source_url", "source_url"),
    )


class NearestZipTrace(Base):
    """Trace data for nearest ZIP resolver lookups"""
    
    __tablename__ = "nearest_zip_traces"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    input_zip = Column(String(20), nullable=False)  # Original input
    input_zip5 = Column(CHAR(5), nullable=False)
    input_zip9 = Column(CHAR(9), nullable=True)
    result_zip = Column(CHAR(5), nullable=False)
    distance_miles = Column(Float, nullable=False)
    trace_json = Column(Text, nullable=False)  # Full trace as JSON
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    
    # Indexes
    __table_args__ = (
        Index("idx_nearest_zip_traces_input", "input_zip5"),
        Index("idx_nearest_zip_traces_result", "result_zip"),
        Index("idx_nearest_zip_traces_created_at", "created_at"),
    )
