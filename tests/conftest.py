"""Pytest configuration and fixtures"""

import pytest
import asyncio
from typing import AsyncGenerator, Generator
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cms_pricing.main import app
from cms_pricing.database import get_db, Base
from cms_pricing.config import settings


# Test database URL
TEST_DATABASE_URL = "sqlite:///./test.db"

# Create test engine
test_engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def db_session() -> Generator:
    """Create a test database session."""
    # Create tables
    Base.metadata.create_all(bind=test_engine)
    
    # Create session
    session = TestingSessionLocal()
    
    try:
        yield session
    finally:
        session.close()
        # Drop tables
        Base.metadata.drop_all(bind=test_engine)


@pytest.fixture(scope="function")
def client(db_session) -> TestClient:
    """Create a test client with database override."""
    
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


@pytest.fixture
def api_key():
    """Test API key."""
    return "dev-key-123"


@pytest.fixture
def sample_plan_data():
    """Sample plan data for testing."""
    return {
        "name": "Test Plan",
        "description": "A test treatment plan",
        "components": [
            {
                "code": "99213",
                "setting": "MPFS",
                "units": 1.0,
                "professional_component": True,
                "facility_component": False,
                "sequence": 1
            },
            {
                "code": "27447",
                "setting": "OPPS",
                "units": 1.0,
                "professional_component": True,
                "facility_component": True,
                "sequence": 2
            }
        ]
    }


@pytest.fixture
def sample_pricing_request():
    """Sample pricing request for testing."""
    return {
        "zip": "94110",
        "year": 2025,
        "quarter": "1",
        "include_home_health": False,
        "include_snf": False,
        "apply_sequestration": False
    }
