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
            headers = kwargs.pop("headers", {})
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
