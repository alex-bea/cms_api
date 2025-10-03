"""Golden tests for pricing parity validation"""

import json
import pytest
from pathlib import Path
from fastapi.testclient import TestClient


def load_golden_scenarios():
    """Load golden test scenarios from JSONL file."""
    scenarios_file = Path(__file__).parent / "golden" / "test_scenarios.jsonl"
    
    scenarios = []
    with open(scenarios_file) as f:
        for line in f:
            if line.strip():
                scenarios.append(json.loads(line))
    
    return scenarios


@pytest.mark.golden
@pytest.mark.parametrize("scenario", load_golden_scenarios())
def test_pricing_parity(client: TestClient, api_key: str, scenario: dict):
    """Test pricing parity against golden scenarios."""
    
    # Extract scenario data
    scenario_name = scenario["scenario"]
    request_data = scenario["request"]
    expected_allowed_cents = scenario["expected_allowed_cents"]
    expected_beneficiary_cents = scenario["expected_beneficiary_cents"]
    expected_trace_refs = scenario["trace_refs"]
    
    # Make pricing request
    response = client.get(
        "/pricing/codes/price",
        params=request_data,
        headers={"X-API-Key": api_key}
    )
    
    # Verify response
    assert response.status_code == 200, f"Scenario {scenario_name} failed: {response.text}"
    
    data = response.json()
    
    # Verify allowed amount (with tolerance for rounding)
    actual_allowed_cents = data.get("allowed_cents", 0)
    assert abs(actual_allowed_cents - expected_allowed_cents) <= 1, \
        f"Scenario {scenario_name}: Expected {expected_allowed_cents} cents, got {actual_allowed_cents} cents"
    
    # Verify beneficiary cost (with tolerance for rounding)
    actual_beneficiary_cents = data.get("beneficiary_total_cents", 0)
    assert abs(actual_beneficiary_cents - expected_beneficiary_cents) <= 1, \
        f"Scenario {scenario_name}: Expected {expected_beneficiary_cents} beneficiary cents, got {actual_beneficiary_cents} cents"
    
    # Verify trace references
    actual_trace_refs = data.get("trace_refs", [])
    for expected_ref in expected_trace_refs:
        assert expected_ref in actual_trace_refs, \
            f"Scenario {scenario_name}: Missing trace reference {expected_ref}"


@pytest.mark.golden
def test_geography_resolution_parity(client: TestClient, api_key: str):
    """Test geography resolution parity."""
    
    # Test known ZIP codes
    test_cases = [
        {
            "zip": "94110",
            "expected_locality": "01",
            "expected_cbsa": "41860"
        },
        {
            "zip": "73301",
            "expected_locality": "45",
            "expected_cbsa": "19100"
        }
    ]
    
    for case in test_cases:
        response = client.get(
            "/geography/resolve",
            params={"zip": case["zip"]},
            headers={"X-API-Key": api_key}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        assert data["zip5"] == case["zip"]
        assert len(data["candidates"]) > 0
        
        # Check if expected locality is in candidates
        locality_found = any(
            candidate["locality_id"] == case["expected_locality"]
            for candidate in data["candidates"]
        )
        assert locality_found, f"Expected locality {case['expected_locality']} not found for ZIP {case['zip']}"


@pytest.mark.golden
def test_plan_pricing_parity(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test complete plan pricing parity."""
    
    # Create a plan
    create_response = client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    assert create_response.status_code == 200
    plan_id = create_response.json()["id"]
    
    # Price the plan
    pricing_request = {
        "zip": "94110",
        "plan_id": plan_id,
        "year": 2025,
        "quarter": "1"
    }
    
    response = client.post(
        "/pricing/price",
        json=pricing_request,
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    
    data = response.json()
    
    # Verify response structure
    assert "run_id" in data
    assert "plan_id" in data
    assert "geography" in data
    assert "line_items" in data
    assert "total_allowed_cents" in data
    assert "total_beneficiary_cents" in data
    
    # Verify line items
    assert len(data["line_items"]) == 2  # Based on sample plan
    
    # Verify totals are positive
    assert data["total_allowed_cents"] > 0
    assert data["total_beneficiary_cents"] > 0
    
    # Verify geography resolution
    assert data["geography"]["zip5"] == "94110"
    assert data["geography"]["locality_id"] is not None
    assert data["geography"]["cbsa"] is not None


@pytest.mark.golden
def test_comparison_parity(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test location comparison parity."""
    
    # Create a plan
    create_response = client.post(
        "/plans/",
        json=sample_plan_data,
        headers={"X-API-Key": api_key}
    )
    assert create_response.status_code == 200
    plan_id = create_response.json()["id"]
    
    # Compare locations
    comparison_request = {
        "zip_a": "94110",
        "zip_b": "73301",
        "plan_id": plan_id,
        "year": 2025,
        "quarter": "1"
    }
    
    response = client.post(
        "/pricing/compare",
        json=comparison_request,
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    
    data = response.json()
    
    # Verify response structure
    assert "run_id" in data
    assert "location_a" in data
    assert "location_b" in data
    assert "deltas" in data
    assert "parity_report" in data
    
    # Verify parity report
    parity_report = data["parity_report"]
    assert parity_report["valid"] is True
    assert parity_report["snapshots_match"] is True
    assert parity_report["benefits_match"] is True
    assert parity_report["toggles_match"] is True
    assert parity_report["plan_match"] is True
    
    # Verify deltas
    assert len(data["deltas"]) > 0
    assert "total_delta_cents" in data
    assert "total_delta_percent" in data
