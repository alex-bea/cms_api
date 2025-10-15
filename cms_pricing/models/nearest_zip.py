"""Data models for nearest ZIP resolver per PRD v1.0"""

from sqlalchemy import Column, String, Integer, Float, Date, Boolean, Index, Text, CHAR, NUMERIC, TIMESTAMP, BIGINT, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSON
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
    
    # DIS-required metadata fields
    data_quality_score = Column(Float, nullable=True)
    validation_results = Column(JSON, nullable=True)
    processing_timestamp = Column(DateTime, nullable=True)
    file_checksum = Column(String(64), nullable=True)
    record_count = Column(Integer, nullable=True)
    schema_version = Column(String(20), nullable=True)
    business_rules_applied = Column(JSON, nullable=True)
    
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
    
    # DIS-required metadata fields
    effective_from = Column(Date, nullable=True)
    effective_to = Column(Date, nullable=True)
    data_quality_score = Column(Float, nullable=True)
    validation_results = Column(JSON, nullable=True)
    processing_timestamp = Column(DateTime, nullable=True)
    file_checksum = Column(String(64), nullable=True)
    record_count = Column(Integer, nullable=True)
    schema_version = Column(String(20), nullable=True)
    business_rules_applied = Column(JSON, nullable=True)
    
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
    """
    Ingest run tracking with Five Pillar Metrics (DIS v1.0 compliant)
    
    Tracks ingestion runs following Data Ingestion Standard with comprehensive
    observability metrics across Freshness, Volume, Schema, Quality, and Lineage.
    
    Uses tiered approach:
    - ~50 explicit columns for frequently-queried fields
    - JSON columns for extended/optional metadata
    """
    
    __tablename__ = "ingest_runs"
    
    # === CORE IDENTITY === (11 fields)
    run_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, 
                   comment="Unique run identifier")
    dataset_name = Column(String(50), nullable=False, index=True,
                         comment="Dataset name (mpfs, rvu, opps)")
    dataset_type = Column(String(50), nullable=True,
                         comment="Dataset type (pricing, geography, reference)")
    release_id = Column(String(50), nullable=False, index=True,
                       comment="Release identifier (e.g., mpfs_2025_q4_20251015)")
    
    # Vintage tracking (three vintage fields per PRD)
    vintage_date = Column(DateTime, nullable=False, index=True,
                         comment="When data was published (timestamp)")
    product_year = Column(String(4), nullable=False, index=True,
                         comment="Product year (e.g., 2025)")
    quarter_vintage = Column(String(10), nullable=True,
                            comment="Quarter vintage (e.g., 2025Q4, 2025_annual)")
    
    build_id = Column(String(50), nullable=True,
                     comment="Build/batch identifier")
    revision = Column(String(10), nullable=True,
                     comment="Revision letter (A, B, C, D)")
    
    # === EXECUTION CONTEXT === (10 fields)
    started_at = Column(TIMESTAMP(timezone=True), nullable=False, index=True,
                       comment="Run start timestamp")
    finished_at = Column(TIMESTAMP(timezone=True), nullable=True,
                        comment="Run completion timestamp")
    duration_seconds = Column(NUMERIC(10, 2), nullable=True,
                             comment="Total duration in seconds")
    status = Column(String(20), nullable=False, index=True,
                   comment="Run status: started, completed, failed, quarantined")
    environment = Column(String(20), nullable=True,
                        comment="Environment: dev, staging, prod")
    executor = Column(String(50), nullable=True,
                     comment="Execution mode: manual, scheduled, triggered")
    triggered_by = Column(String(100), nullable=True,
                         comment="User/system that initiated run")
    tool_version = Column(Text, nullable=True,
                         comment="Ingestor tool version")
    
    # Extended execution metadata (JSON)
    execution_details = Column(JSON, nullable=True,
                              comment="hostname, process_id, retry_count, etc.")
    
    # === SOURCE METADATA === (6 fields + JSON)
    source = Column(String(100), nullable=True,
                   comment="Source system name")
    source_url = Column(Text, nullable=False, index=True,
                       comment="Primary source URL")
    source_published_at = Column(DateTime, nullable=True,
                                comment="When CMS published data (Pillar 1: Freshness)")
    data_lag_hours = Column(NUMERIC(10, 2), nullable=True,
                           comment="Hours between publication and ingestion (Pillar 1)")
    scraper_version = Column(String(50), nullable=True,
                            comment="Scraper version used")
    
    # Extended source metadata (JSON)
    source_metadata = Column(JSON, nullable=True,
                            comment="source_urls (list), cms_publication_date, cms_effective_date, etc.")
    
    # === FILE TRACKING === (8 fields + JSON)
    filename = Column(Text, nullable=True,
                     comment="Primary filename processed")
    sha256 = Column(CHAR(64), nullable=True,
                   comment="Primary file checksum")
    dataset_digest = Column(CHAR(64), nullable=True,
                           comment="Overall dataset digest")
    bytes = Column(BIGINT, nullable=True,
                  comment="Total bytes processed")
    content_type = Column(String(100), nullable=True,
                         comment="Primary file content type")
    files_discovered = Column(Integer, nullable=True,
                             comment="Total files discovered")
    files_downloaded = Column(Integer, nullable=True,
                             comment="Files successfully downloaded")
    files_parsed = Column(Integer, nullable=True,
                         comment="Files successfully parsed")
    
    # Extended file metadata (JSON)
    files_metadata = Column(JSON, nullable=True,
                           comment="files (list with url, size, checksum per file), files_failed, etc.")
    
    # === VOLUME METRICS === (Pillar 2) (6 fields)
    row_count = Column(BIGINT, nullable=True,
                      comment="Legacy total row count")
    rows_discovered = Column(BIGINT, nullable=True,
                            comment="Total rows found in source (Pillar 2)")
    rows_ingested = Column(BIGINT, nullable=True,
                          comment="Rows successfully stored (Pillar 2)")
    rows_rejected = Column(BIGINT, nullable=True,
                          comment="Rows failed validation (Pillar 2)")
    rows_quarantined = Column(BIGINT, nullable=True,
                             comment="Rows with soft failures (Pillar 2)")
    bytes_processed = Column(BIGINT, nullable=True,
                            comment="Total bytes processed (Pillar 2)")
    
    # === SCHEMA METRICS === (Pillar 3) (3 fields + JSON)
    schema_version = Column(String(20), nullable=True,
                           comment="Schema contract version (Pillar 3)")
    schema_drift_detected = Column(Boolean, nullable=False, default=False,
                                  comment="Schema drift flag (Pillar 3)")
    schema_drift_details = Column(JSON, nullable=True,
                                 comment="Schema drift details (Pillar 3)")
    
    # === QUALITY METRICS === (Pillar 4) (6 fields + JSON)
    validation_errors = Column(Integer, nullable=True,
                              comment="Blocking validation errors (Pillar 4)")
    validation_warnings = Column(Integer, nullable=True,
                                comment="Warning-level validation issues (Pillar 4)")
    validation_rules_executed = Column(Integer, nullable=True,
                                      comment="Total validation rules run")
    validation_rules_passed = Column(Integer, nullable=True,
                                    comment="Validation rules passed")
    null_rate_max = Column(NUMERIC(5, 4), nullable=True,
                          comment="Max null rate across columns (Pillar 4)")
    distribution_drift_pct = Column(NUMERIC(5, 2), nullable=True,
                                   comment="Statistical drift percentage (Pillar 4)")
    
    # Extended quality metadata (JSON)
    quality_metrics = Column(JSON, nullable=True,
                            comment="null_rate_avg, duplicate_rate, completeness_rate, quality_score, etc.")
    
    # === LINEAGE METRICS === (Pillar 5) (2 fields + JSON)
    downstream_notified = Column(Boolean, nullable=False, default=False,
                                comment="Downstream consumers notified (Pillar 5)")
    
    # Extended lineage metadata (JSON)
    lineage_metadata = Column(JSON, nullable=True,
                             comment="dependencies, upstream_runs, downstream_consumers, data_lineage_graph (Pillar 5)")
    
    # === STAGE TIMING === (JSON)
    stage_timings = Column(JSON, nullable=True,
                          comment="discover_duration_sec, land_duration_sec, validate_duration_sec, etc.")
    
    # === STAGE RESULTS === (JSON)
    stage_results = Column(JSON, nullable=True,
                          comment="land_bytes_downloaded, validate_schema_validated, publish_tables_updated, etc.")
    
    # === OUTCOMES & ARTIFACTS === (5 fields + JSON)
    curated_views_created = Column(JSON, nullable=True,
                                  comment="List of curated view names created")
    parquet_files_created = Column(JSON, nullable=True,
                                  comment="List of parquet file paths created")
    diff_report_path = Column(String(200), nullable=True,
                             comment="Path to diff report vs prior vintage")
    diff_summary = Column(JSON, nullable=True,
                         comment="Diff summary: added/removed counts, delta percentiles, etc.")
    manifest_path = Column(String(200), nullable=True,
                          comment="Path to ingestion manifest")
    
    # === COST TRACKING === (JSON)
    cost_tracking = Column(JSON, nullable=True,
                          comment="compute_cost_usd, storage_cost_usd, network_cost_usd, total_cost_usd")
    
    # === ERROR HANDLING === (3 fields + JSON)
    error_message = Column(String(500), nullable=True,
                          comment="Primary error message if failed")
    error_type = Column(String(100), nullable=True,
                       comment="Error type/category")
    
    # Extended error metadata (JSON)
    error_details = Column(JSON, nullable=True,
                          comment="error_stacktrace, quarantine_path, recovery_attempted, etc.")
    
    # === COMPLIANCE & AUDIT === (2 fields + JSON)
    notes = Column(Text, nullable=True,
                  comment="Free-text notes about run")
    
    # Extended compliance metadata (JSON)
    compliance_metadata = Column(JSON, nullable=True,
                                comment="license, attribution_required, audit_log_path, approval info, etc.")
    
    # === ALERTS & NOTIFICATIONS === (JSON)
    alerts_metadata = Column(JSON, nullable=True,
                            comment="alerts_triggered, notifications_sent, escalation_level, incident_ids, etc.")
    
    # Indexes
    __table_args__ = (
        Index("idx_ingest_runs_status", "status"),
        Index("idx_ingest_runs_started_at", "started_at"),
        Index("idx_ingest_runs_source_url", "source_url"),
        Index("idx_ingest_runs_dataset_name", "dataset_name"),
        Index("idx_ingest_runs_release_id", "release_id"),
        Index("idx_ingest_runs_vintage_date", "vintage_date"),
        Index("idx_ingest_runs_product_year", "product_year"),
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
