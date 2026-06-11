"""
Test configuration and fixtures for the CMS API test suite.

This module provides shared test fixtures and configuration that follows
the QA Testing Standard (QTS) v1.0 requirements.
"""

import pytest
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import ProgrammingError, OperationalError
import importlib.util
from typing import Dict, Any

from fastapi.testclient import TestClient

from cms_pricing.main import app
from cms_pricing.config import settings
from cms_pricing.database import get_db

# Import all models to ensure they're registered
from cms_pricing.models.plans import Base as PlansBase


GEOGRAPHY_MODULE = "cms_pricing.ingestion.geography"
SCHEDULER_MODULE = "cms_pricing.ingestion.scheduler"
NEAREST_ZIP_MODULE = "cms_pricing.ingestion.nearest_zip_ingestion"
ZIP9_MODULE = "cms_pricing.ingestion.ingestors.cms_zip9_ingester"

GEOGRAPHY_AVAILABLE = importlib.util.find_spec(GEOGRAPHY_MODULE) is not None
SCHEDULER_AVAILABLE = importlib.util.find_spec(SCHEDULER_MODULE) is not None
NEAREST_ZIP_AVAILABLE = importlib.util.find_spec(NEAREST_ZIP_MODULE) is not None
ZIP9_AVAILABLE = importlib.util.find_spec(ZIP9_MODULE) is not None

LEGACY_GEOGRAPHY_INGESTION_TESTS = (
    "test_geography_ingestion.py",
    "test_geography_automation.py",
    "test_geography_integration.py",
    "test_geography_gaps.py",
)


DOMAIN_MARKER_PATTERNS = {
    "prd_docs": ("tests/prd_docs", "doc_catalog", "doc_metadata", "doc_links", "doc_dependencies"),
    "scraper": ("tests/scrapers", "_scraper", "scraper_"),
    "ingestor": ("tests/ingestors", "ingestor", "ingestion_pipeline", "/ingestion/"),
    "geography": ("tests/geography", "geography", "nearest_zip", "zip9"),
    "api": ("tests/api", "_api", "router"),
}


@pytest.fixture(scope="session")
def test_engine():
    """Create a test database engine with PostgreSQL compatibility"""
    # Use PostgreSQL test database (same as test_with_postgres.sh)
    database_url = settings.test_database_url
    engine = create_engine(
        database_url,
        echo=False,
        pool_size=5,
        max_overflow=10
    )
    
    # Tables are already created by Alembic migrations in bootstrap_test_db.py
    # No need to create them again
    
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


@pytest.fixture(scope="function")
def api_key() -> str:
    """Provide a valid API key for authenticated requests"""
    keys = settings.get_api_keys()
    if not keys:
        raise RuntimeError("No API keys configured for tests")
    return keys[0]


@pytest.fixture(scope="function")
def client(api_key: str, test_db_session) -> TestClient:
    """FastAPI TestClient with default auth headers"""

    def _override_get_db():
        try:
            yield test_db_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db

    with TestClient(app) as test_client:
        default_headers: Dict[str, str] = {
            "X-API-Key": api_key,
        }

        # Merge default headers into each request by wrapping the original call
        original_request = test_client.request

        def request_with_auth(method, url, **kwargs):  # type: ignore[override]
            headers = kwargs.pop("headers", None) or {}
            merged_headers = {**default_headers, **headers}
            return original_request(method, url, headers=merged_headers, **kwargs)

        test_client.request = request_with_auth  # type: ignore[assignment]
        yield test_client

    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(scope="function")
def sample_plan_data() -> Dict[str, Any]:
    """Provide sample treatment plan payload for tests"""

    return {
        "name": "Sample Knee Replacement Plan",
        "description": "Comprehensive knee replacement bundle",
        "created_by": "unit-test",
        "metadata": {"category": "orthopedic", "version": "1.0"},
        "components": [
            {
                "sequence": 1,
                "code": "27447",
                "setting": "OPPS",
                "units": 1,
                "utilization_weight": 1.0,
                "professional_component": False,
                "facility_component": True,
                "modifiers": ["-TC"],
                "pos": "22",
                "ndc11": None,
            },
            {
                "sequence": 2,
                "code": "99213",
                "setting": "MPFS",
                "units": 1,
                "utilization_weight": 1.0,
                "professional_component": True,
                "facility_component": False,
                "modifiers": ["-26"],
                "pos": "11",
                "ndc11": None,
            },
        ],
    }


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # Don't close the loop here - let pytest-asyncio handle it
    # loop.close()


@pytest.fixture(scope="function")
def test_data_dir():
    """Create test data directory with sample data under tmp or RVU_TEST_DATA_DIR."""
    import os
    import tempfile
    from tests.fixtures.rvu.test_dataset_creator import RVUTestDatasetCreator

    base_dir = os.getenv("RVU_TEST_DATA_DIR")
    if not base_dir:
        base_dir = tempfile.mkdtemp(prefix="rvu_tests.")

    creator = RVUTestDatasetCreator(base_dir)
    data_dir = creator.create_all()
    return data_dir


@pytest.fixture(scope="function")
def db_requires_plans_table(test_db_session):
    """Check if the plans table exists, skip test if not.
    
    This fixture provides graceful skipping for tests that require database tables
    that may not be set up yet. It checks for the 'plans' table as a proxy for
    whether the database has been migrated.
    
    Usage:
        def test_something(db_requires_plans_table, client, ...):
            # Test will skip if plans table doesn't exist
            ...
    """
    try:
        # Check if plans table exists by querying it
        test_db_session.execute(text("SELECT 1 FROM plans LIMIT 1"))
        yield test_db_session
    except (ProgrammingError, OperationalError) as e:
        if "does not exist" in str(e) or "relation" in str(e).lower():
            pytest.skip(
                "Database tables not initialized. Run: "
                "python tests/scripts/bootstrap_test_db.py --database-url $TEST_DATABASE_URL"
            )
        raise


def require_db_table(test_db_session, table_name: str):
    """Helper function to check if a table exists, skip test if not.
    
    Can be used in tests to check for specific tables:
        require_db_table(test_db_session, "plans")
    """
    try:
        test_db_session.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
        return True
    except (ProgrammingError, OperationalError) as e:
        if "does not exist" in str(e) or "relation" in str(e).lower():
            pytest.skip(
                f"Table '{table_name}' does not exist. "
                "Run: python tests/scripts/bootstrap_test_db.py --database-url $TEST_DATABASE_URL"
            )
        raise


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

        if not GEOGRAPHY_AVAILABLE and any(
            test_file in fspath for test_file in LEGACY_GEOGRAPHY_INGESTION_TESTS
        ):
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
