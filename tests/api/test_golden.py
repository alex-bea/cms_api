"""Golden tests for pricing parity validation"""

import json
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from sqlalchemy.exc import ProgrammingError, OperationalError


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
    
    # Verify CodePricingItem structure (Quick Win #2)
    assert "code" in data, f"Scenario {scenario_name}: Response should include 'code' field (CodePricingItem)"
    assert "setting" in data, f"Scenario {scenario_name}: Response should include 'setting' field (CodePricingItem)"
    request_setting = request_data.get("setting")
    assert data.get("setting") == request_setting, f"Scenario {scenario_name}: setting should match request setting"
    assert "dataset_id" in data, f"Scenario {scenario_name}: Response should include 'dataset_id' field (CodePricingItem)"
    
    # Verify CodePricingItemWithGeography fields (geography and run_id)
    assert "geography" in data, f"Scenario {scenario_name}: Response should include 'geography' field (CodePricingItemWithGeography)"
    assert "run_id" in data, f"Scenario {scenario_name}: Response should include 'run_id' field (CodePricingItemWithGeography)"
    assert isinstance(data.get("geography"), dict), f"Scenario {scenario_name}: geography should be a dict"
    assert isinstance(data.get("run_id"), str), f"Scenario {scenario_name}: run_id should be a string"
    
    # Verify trace references
    actual_trace_refs = data.get("trace_refs", [])
    for expected_ref in expected_trace_refs:
        assert expected_ref in actual_trace_refs, \
            f"Scenario {scenario_name}: Missing trace reference {expected_ref}"
    
    # Verify provenance fields structure (Phase 2.5)
    # Note: These may be None for legacy data, but fields should exist when provenance is available
    # release_id and batch_id may be None for legacy data
    assert "release_id" in data or "batch_id" in data, \
        f"Scenario {scenario_name}: Response should include provenance fields (release_id or batch_id)"
    
    if "release_id" in data:
        assert data["release_id"] is None or isinstance(data["release_id"], str)
    if "batch_id" in data:
        assert data["batch_id"] is None or isinstance(data["batch_id"], str)
    
    # Verify trace_refs deduplication (Phase 2.5)
    # Should not have duplicates
    assert len(actual_trace_refs) == len(set(actual_trace_refs)), \
        f"Scenario {scenario_name}: trace_refs contains duplicates: {actual_trace_refs}"
    
    # Verify standardized provenance format in trace_refs if present (Phase 2.5)
    provenance_refs = [ref for ref in actual_trace_refs if ref and ':' in ref and len(ref.split(':')) == 3]
    for ref in provenance_refs:
        parts = ref.split(':')
        assert len(parts) == 3, f"Scenario {scenario_name}: Invalid provenance trace_ref format: {ref}"
        dataset_id, provenance_type, value = parts
        assert provenance_type in ["release", "batch"], \
            f"Scenario {scenario_name}: Invalid provenance type in trace_ref: {ref}"
        assert value is not None and value != "", \
            f"Scenario {scenario_name}: Empty provenance value in trace_ref: {ref}"


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
    try:
        create_response = client.post(
            "/plans/",
            json=sample_plan_data,
            headers={"X-API-Key": api_key}
        )
    except Exception as e:
        # Gracefully skip if database tables don't exist
        if "does not exist" in str(e) or "relation" in str(e).lower() or "ProgrammingError" in str(type(e)):
            pytest.skip(
                "Database tables not initialized. Run: "
                "python tests/scripts/bootstrap_test_db.py --database-url $TEST_DATABASE_URL"
            )
        raise
    
    if create_response.status_code == 500:
        # Check if error is due to missing tables
        error_text = create_response.text.lower()
        if "does not exist" in error_text or "relation" in error_text:
            pytest.skip(
                "Database tables not initialized. Run: "
                "python tests/scripts/bootstrap_test_db.py --database-url $TEST_DATABASE_URL"
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
    
    # Verify datasets_used structure (Phase 2.6)
    if "datasets_used" in data:
        assert isinstance(data["datasets_used"], list), \
            "datasets_used should be a list"
        
        # Verify datasets_used includes expected structure with provenance fields
        for dataset_info in data["datasets_used"]:
            assert "dataset_id" in dataset_info, \
                "datasets_used entries must include dataset_id"
            # Provenance fields may be None for legacy data, but should be present in structure
            assert "release_id" in dataset_info or "batch_id" in dataset_info, \
                "datasets_used entries should include release_id or batch_id fields"
            
            # If provenance fields exist, verify they're strings or None
            if "release_id" in dataset_info:
                assert dataset_info["release_id"] is None or isinstance(dataset_info["release_id"], str)
            if "batch_id" in dataset_info:
                assert dataset_info["batch_id"] is None or isinstance(dataset_info["batch_id"], str)
    
    # Verify line items include trace_refs with standardized provenance format (Phase 2.5)
    for line_item in data.get("line_items", []):
        assert "trace_refs" in line_item, \
            "Line items should include trace_refs"
        trace_refs = line_item.get("trace_refs", [])
        
        # Verify deduplication
        assert len(trace_refs) == len(set(trace_refs)), \
            f"Line item trace_refs contains duplicates: {trace_refs}"
        
        # Check if any trace_refs use standardized provenance format
        provenance_refs = [ref for ref in trace_refs if ref and ':' in ref and len(ref.split(':')) == 3]
        if provenance_refs:
            # Verify format: {dataset_id}:release:{release_id} or {dataset_id}:batch:{batch_id}
            for ref in provenance_refs:
                parts = ref.split(':')
                assert len(parts) == 3, f"Invalid provenance trace_ref format: {ref}"
                dataset_id, provenance_type, value = parts
                assert provenance_type in ["release", "batch"], \
                    f"Invalid provenance type in trace_ref: {ref}"
                assert value is not None and value != "", \
                    f"Empty provenance value in trace_ref: {ref}"


@pytest.mark.golden
def test_comparison_parity(client: TestClient, api_key: str, sample_plan_data: dict):
    """Test location comparison parity."""
    
    # Create a plan
    try:
        create_response = client.post(
            "/plans/",
            json=sample_plan_data,
            headers={"X-API-Key": api_key}
        )
    except Exception as e:
        # Gracefully skip if database tables don't exist
        if "does not exist" in str(e) or "relation" in str(e).lower() or "ProgrammingError" in str(type(e)):
            pytest.skip(
                "Database tables not initialized. Run: "
                "python tests/scripts/bootstrap_test_db.py --database-url $TEST_DATABASE_URL"
            )
        raise
    
    if create_response.status_code == 500:
        # Check if error is due to missing tables
        error_text = create_response.text.lower()
        if "does not exist" in error_text or "relation" in error_text:
            pytest.skip(
                "Database tables not initialized. Run: "
                "python tests/scripts/bootstrap_test_db.py --database-url $TEST_DATABASE_URL"
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
    
    # Verify datasets_used structure in both locations (Phase 2.6)
    for location in ["location_a", "location_b"]:
        location_data = data.get(location, {})
        if "datasets_used" in location_data:
            assert isinstance(location_data["datasets_used"], list), \
                f"{location}: datasets_used should be a list"
            for dataset_info in location_data["datasets_used"]:
                assert "dataset_id" in dataset_info, \
                    f"{location}: datasets_used entries must include dataset_id"
                # Provenance fields should be present (may be None for legacy data)
                assert "release_id" in dataset_info or "batch_id" in dataset_info, \
                    f"{location}: datasets_used entries should include provenance fields"
                
                # If provenance fields exist, verify they're strings or None
                if "release_id" in dataset_info:
                    assert dataset_info["release_id"] is None or isinstance(dataset_info["release_id"], str)
                if "batch_id" in dataset_info:
                    assert dataset_info["batch_id"] is None or isinstance(dataset_info["batch_id"], str)
