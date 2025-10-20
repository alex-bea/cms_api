"""
Metrics Contract Validation Helpers

Provides pytest-friendly helpers to validate metrics against the metrics contract.

Usage:
    from tests.helpers.metrics_contract import assert_metrics_contract
    
    def test_my_normalizer():
        result = normalize(raw_df)
        
        # Validate metrics contract
        assert_metrics_contract(result.metrics, 'normalization_pipeline')
        
        # Or get violations for custom assertions
        blocking, warnings = validate_metrics_contract(result.metrics, 'normalization_pipeline')
        assert len(blocking) == 0, f"Metrics violations: {blocking}"
"""

import json
from pathlib import Path
from typing import Dict, Any, Tuple, List

# Load contract once at module level
_CONTRACT_PATH = Path(__file__).parent.parent.parent / 'cms_pricing/ingestion/contracts/metrics_contract_v1.0.json'
_CONTRACT = None


def get_contract() -> Dict[str, Any]:
    """Load metrics contract (cached)."""
    global _CONTRACT
    if _CONTRACT is None:
        with open(_CONTRACT_PATH, 'r') as f:
            _CONTRACT = json.load(f)
    return _CONTRACT


def validate_metrics_contract(
    metrics: Dict[str, Any],
    pipeline_type: str
) -> Tuple[List[str], List[str]]:
    """
    Validate metrics against contract.
    
    Args:
        metrics: Actual metrics dict from parser/normalizer
        pipeline_type: 'parser_metrics', 'normalization_pipeline', or 'enrichment_pipeline'
        
    Returns:
        (blocking_violations, warnings)
    """
    from tools.validate_metrics_contract import validate_metrics
    
    contract = get_contract()
    
    # Map short names to full keys
    type_map = {
        'parser': 'parser_metrics',
        'normalization': 'normalization_pipeline_metrics',
        'normalization_pipeline': 'normalization_pipeline_metrics',
        'enrichment': 'enrichment_pipeline_metrics',
        'enrichment_pipeline': 'enrichment_pipeline_metrics',
    }
    
    full_type = type_map.get(pipeline_type, pipeline_type)
    
    return validate_metrics(metrics, full_type, contract)


def assert_metrics_contract(
    metrics: Dict[str, Any],
    pipeline_type: str,
    allow_warnings: bool = True
):
    """
    Assert metrics conform to contract (pytest-friendly).
    
    Args:
        metrics: Actual metrics dict
        pipeline_type: 'parser', 'normalization', or 'enrichment'
        allow_warnings: If False, warnings also cause assertion failure
        
    Raises:
        AssertionError: If contract violations found
    """
    blocking, warnings = validate_metrics_contract(metrics, pipeline_type)
    
    violations = []
    if blocking:
        violations.extend([f"BLOCK: {v}" for v in blocking])
    if not allow_warnings and warnings:
        violations.extend([f"WARN: {w}" for w in warnings])
    
    assert len(violations) == 0, (
        f"Metrics contract violations:\n" +
        "\n".join(f"  - {v}" for v in violations)
    )


def assert_quarantine_contract(quarantine_df: Any):
    """
    Assert quarantine DataFrame conforms to contract.
    
    Args:
        quarantine_df: Quarantine DataFrame from parser/normalizer
        
    Raises:
        AssertionError: If contract violations found
    """
    from tools.validate_metrics_contract import validate_quarantine_structure
    
    contract = get_contract()
    blocking, warnings = validate_quarantine_structure(quarantine_df, contract)
    
    violations = []
    if blocking:
        violations.extend([f"BLOCK: {v}" for v in blocking])
    # Warnings on quarantine structure are non-blocking
    
    assert len(violations) == 0, (
        f"Quarantine contract violations:\n" +
        "\n".join(f"  - {v}" for v in violations)
    )

