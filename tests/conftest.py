"""
Test configuration and fixtures for the CMS API test suite.

This module provides shared test fixtures and configuration that follows
the QA Testing Standard (QTS) v1.0 requirements.
"""

import pytest
import asyncio
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import importlib.util

# Import all models to ensure they're registered
from cms_pricing.models.rvu import Base as RVUBase
from cms_pricing.models.nearest_zip import Base as NearestZipBase
from cms_pricing.models.geography_trace import Base as GeographyTraceBase
from cms_pricing.models.plans import Base as PlansBase
from cms_pricing.models.snapshots import Base as SnapshotsBase
from cms_pricing.models.runs import Base as RunsBase
from cms_pricing.models.benefits import Base as BenefitsBase
from cms_pricing.models.codes import Base as CodesBase


GEOGRAPHY_MODULE = "cms_pricing.ingestion.geography"
SCHEDULER_MODULE = "cms_pricing.ingestion.scheduler"
NEAREST_ZIP_MODULE = "cms_pricing.ingestion.nearest_zip_ingestion"
ZIP9_MODULE = "cms_pricing.ingestion.ingestors.cms_zip9_ingester"

GEOGRAPHY_AVAILABLE = importlib.util.find_spec(GEOGRAPHY_MODULE) is not None
SCHEDULER_AVAILABLE = importlib.util.find_spec(SCHEDULER_MODULE) is not None
NEAREST_ZIP_AVAILABLE = importlib.util.find_spec(NEAREST_ZIP_MODULE) is not None
ZIP9_AVAILABLE = importlib.util.find_spec(ZIP9_MODULE) is not None


DOMAIN_MARKER_PATTERNS = {
    "prd_docs": ("tests/prd_docs", "doc_catalog", "doc_metadata", "doc_links", "doc_dependencies"),
    "scraper": ("tests/scrapers", "_scraper", "scraper_"),
    "ingestor": ("tests/ingestors", "ingestor", "ingestion_pipeline", "/ingestion/"),
    "geography": ("tests/geography", "geography", "nearest_zip", "zip9"),
    "api": ("tests/api", "_api", "router"),
}
class SQLiteCompatibleModels:
    """Create SQLite-compatible versions of models that use JSONB"""
    
    @staticmethod
    def create_test_models():
        """Create test-compatible model definitions"""
        from sqlalchemy import Column, String, Text, DateTime, Integer, Float, Boolean
        from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON
        from sqlalchemy.dialects.postgresql import UUID
        import uuid
        
        # Create a test-compatible version of geography_trace
        class GeographyResolutionTrace:
            __tablename__ = 'geography_resolution_traces'
            
            id = Column(Integer, primary_key=True)
            trace_id = Column(String(36), nullable=False, default=lambda: str(uuid.uuid4()))
            input_zip = Column(String(5), nullable=False)
            resolved_zip = Column(String(5), nullable=True)
            resolved_locality = Column(String(10), nullable=True)
            resolved_state = Column(String(2), nullable=True)
            distance_miles = Column(Float, nullable=True)
            resolution_method = Column(String(50), nullable=True)
            confidence_score = Column(Float, nullable=True)
            processing_time_ms = Column(Float, nullable=True)
            created_at = Column(DateTime, nullable=False)
            
            # Use SQLite-compatible JSON instead of JSONB
            inputs_json = Column(SQLiteJSON, nullable=True)
            output_json = Column(SQLiteJSON, nullable=True)
        
        # Create a test-compatible version of runs
        class IngestionRun:
            __tablename__ = 'ingestion_runs'
            
            id = Column(Integer, primary_key=True)
            run_id = Column(String(36), nullable=False, default=lambda: str(uuid.uuid4()))
            dataset_name = Column(String(100), nullable=False)
            release_id = Column(String(100), nullable=False)
            batch_id = Column(String(36), nullable=False)
            status = Column(String(20), nullable=False)
            started_at = Column(DateTime, nullable=False)
            completed_at = Column(DateTime, nullable=True)
            record_count = Column(Integer, nullable=True)
            quality_score = Column(Float, nullable=True)
            error_message = Column(Text, nullable=True)
            
            # Use SQLite-compatible JSON instead of JSONB
            request_json = Column(SQLiteJSON, nullable=True)
            response_json = Column(SQLiteJSON, nullable=True)
        
        class TraceRecord:
            __tablename__ = 'trace_records'
            
            id = Column(Integer, primary_key=True)
            trace_id = Column(String(36), nullable=False)
            step_name = Column(String(100), nullable=False)
            step_order = Column(Integer, nullable=False)
            status = Column(String(20), nullable=False)
            started_at = Column(DateTime, nullable=False)
            completed_at = Column(DateTime, nullable=True)
            duration_ms = Column(Float, nullable=True)
            error_message = Column(Text, nullable=True)
            
            # Use SQLite-compatible JSON instead of JSONB
            trace_refs = Column(SQLiteJSON, nullable=True)
            trace_data = Column(SQLiteJSON, nullable=False)
        
        return GeographyResolutionTrace, IngestionRun, TraceRecord


@pytest.fixture(scope="session")
def test_engine():
    """Create a test database engine with SQLite compatibility"""
    # Create in-memory SQLite database
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False}
    )
    
    # Create test-compatible models
    GeographyResolutionTrace, IngestionRun, TraceRecord = SQLiteCompatibleModels.create_test_models()
    
    # Create all tables
    RVUBase.metadata.create_all(engine)
    NearestZipBase.metadata.create_all(engine)
    PlansBase.metadata.create_all(engine)
    SnapshotsBase.metadata.create_all(engine)
    BenefitsBase.metadata.create_all(engine)
    CodesBase.metadata.create_all(engine)
    
    # Create test-compatible tables
    GeographyResolutionTrace.__table__.create(engine, checkfirst=True)
    IngestionRun.__table__.create(engine, checkfirst=True)
    TraceRecord.__table__.create(engine, checkfirst=True)
    
    return engine


@pytest.fixture(scope="function")
def test_db_session(test_engine):
    """Create a test database session"""
    Session = sessionmaker(bind=test_engine)
    session = Session()
    
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def test_data_dir():
    """Create test data directory with sample data"""
    from tests.fixtures.rvu.test_dataset_creator import RVUTestDatasetCreator
    
    creator = RVUTestDatasetCreator("tests/fixtures/rvu/test_data")
    data_dir = creator.create_all()
    return data_dir


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "mpfs: mark test as MPFS scenario"
    )
    config.addinivalue_line(
        "markers", "opps: mark test as OPPS scenario"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file paths"""
    for item in items:
        fspath = str(item.fspath)

        if not GEOGRAPHY_AVAILABLE and 'geography' in fspath:
            item.add_marker(pytest.mark.skip(reason='geography ingestion module unavailable'))
            continue
        if not SCHEDULER_AVAILABLE and 'scheduler' in fspath:
            item.add_marker(pytest.mark.skip(reason='ingestion scheduler unavailable'))
            continue
        if not NEAREST_ZIP_AVAILABLE and ('nearest_zip' in fspath or 'zip9' in fspath):
            item.add_marker(pytest.mark.skip(reason='nearest zip ingestion modules unavailable'))
            continue
        if not ZIP9_AVAILABLE and 'zip9' in fspath:
            item.add_marker(pytest.mark.skip(reason='zip9 ingester unavailable'))
            continue

        if 'integration' in fspath:
            item.add_marker(pytest.mark.integration)
        elif 'unit' in fspath:
            item.add_marker(pytest.mark.unit)
        elif 'e2e' in fspath:
            item.add_marker(pytest.mark.e2e)

        if 'performance' in fspath or 'load' in fspath:
            item.add_marker(pytest.mark.slow)

        for marker_name, patterns in DOMAIN_MARKER_PATTERNS.items():
            if any(pattern in fspath for pattern in patterns):
                item.add_marker(getattr(pytest.mark, marker_name))