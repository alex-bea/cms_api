"""Plan management endpoint tests"""

import pytest
from fastapi.testclient import TestClient


def test_create_plan(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test creating a new plan."""
    response = client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == sample_plan_data["name"]
    assert data["description"] == sample_plan_data["description"]
    assert len(data["components"]) == 2
    assert "id" in data


def test_create_plan_invalid_setting(client: TestClient, api_key: str):
    """Test creating a plan with invalid setting."""
    plan_data = {
        "name": "Invalid Plan",
        "components": [
            {
                "code": "99213",
                "setting": "INVALID",
                "units": 1.0
            }
        ]
    }
    
    response = client.post(
        "/plans/",
        json=plan_data,
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 422  # Validation error


def test_list_plans(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test listing plans."""
    # Create a plan first
    client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    
    # List plans
    response = client.get(
        "/plans/",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1


def test_get_plan(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test getting a specific plan."""
    # Create a plan first
    create_response = client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    plan_id = create_response.json()["id"]
    
    # Get the plan
    response = client.get(
        f"/plans/{plan_id}",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == plan_id
    assert data["name"] == sample_plan_data["name"]


def test_get_plan_not_found(client: TestClient, api_key: str):
    """Test getting a non-existent plan."""
    response = client.get(
        "/plans/00000000-0000-0000-0000-000000000000",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 404


def test_update_plan(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test updating a plan."""
    # Create a plan first
    create_response = client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    plan_id = create_response.json()["id"]
    
    # Update the plan
    update_data = {
        "name": "Updated Plan Name",
        "description": "Updated description"
    }
    
    response = client.put(
        f"/plans/{plan_id}",
        json=update_data,
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Plan Name"
    assert data["description"] == "Updated description"


def test_delete_plan(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test deleting a plan."""
    # Create a plan first
    create_response = client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    plan_id = create_response.json()["id"]
    
    # Delete the plan
    response = client.delete(
        f"/plans/{plan_id}",
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    assert response.json()["message"] == "Plan deleted successfully"
    
    # Verify plan is deleted
    get_response = client.get(
        f"/plans/{plan_id}",
        headers={"X-API-Key": api_key}
    )
    assert get_response.status_code == 404


def test_plan_endpoints_require_auth(client: TestClient, sample_plan_data: dict):
    """Test that plan endpoints require authentication."""
    # Test create
    response = client.post("/plans/", json=sample_plan_data)
    assert response.status_code == 401
    
    # Test list
    response = client.get("/plans/")
    assert response.status_code == 401
    
    # Test get
    response = client.get("/plans/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 401
