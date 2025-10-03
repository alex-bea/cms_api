"""Health check endpoint tests"""

import pytest
from fastapi.testclient import TestClient


def test_health_check(client: TestClient):
    """Test basic health check endpoint."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert response.json()["service"] == "cms-pricing-api"


def test_readiness_check(client: TestClient):
    """Test readiness check endpoint."""
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert "dependencies" in response.json()


def test_metrics_endpoint(client: TestClient, api_key: str):
    """Test Prometheus metrics endpoint."""
    response = client.get("/metrics", headers={"X-API-Key": api_key})
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]


def test_metrics_endpoint_requires_auth(client: TestClient):
    """Test that metrics endpoint requires authentication."""
    response = client.get("/metrics")
    assert response.status_code == 401
