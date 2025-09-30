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

# Import all models to ensure they're registered
from cms_pricing.models.rvu import Base as RVUBase
from cms_pricing.models.nearest_zip import Base as NearestZipBase
from cms_pricing.models.geography_trace import Base as GeographyTraceBase
from cms_pricing.models.plans import Base as PlansBase
from cms_pricing.models.snapshots import Base as SnapshotsBase
from cms_pricing.models.runs import Base as RunsBase
from cms_pricing.models.benefits import Base as BenefitsBase
from cms_pricing.models.codes import Base as CodesBase


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


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file paths"""
    for item in items:
        # Add markers based on file path
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)
        
        # Add slow marker for tests that take > 5 seconds
        if "performance" in str(item.fspath) or "load" in str(item.fspath):
            item.add_marker(pytest.mark.slow)