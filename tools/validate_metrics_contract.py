#!/usr/bin/env python3
"""
Validate normalization pipeline metrics against the metrics contract.

Usage:
    python tools/validate_metrics_contract.py <metrics_dict> <contract_path>
    
    # Or from test code:
    from tools.validate_metrics_contract import validate_metrics
    violations = validate_metrics(result.metrics, 'normalization_pipeline')
    assert len(violations) == 0, f"Metrics contract violations: {violations}"

Exit codes:
    0: All validations passed
    1: Blocking violations found
    2: Warnings only (non-blocking)
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple


def load_contract(contract_path: Path) -> Dict[str, Any]:
    """Load metrics contract JSON."""
    with open(contract_path, 'r') as f:
        return json.load(f)


def validate_metrics(
    metrics: Dict[str, Any],
    pipeline_type: str,
    contract: Dict[str, Any]
) -> Tuple[List[str], List[str]]:
    """
    Validate metrics against contract.
    
    Args:
        metrics: Actual metrics dict from parser/normalizer
        pipeline_type: 'parser_metrics', 'normalization_pipeline_metrics', or 'enrichment_pipeline_metrics'
        contract: Loaded metrics contract
        
    Returns:
        (blocking_violations, warnings)
    """
    blocking = []
    warnings = []
    
    pipeline_spec = contract.get(pipeline_type, {})
    required_keys = pipeline_spec.get('required_keys', [])
    
    # R001: Check required keys present
    for key in required_keys:
        if key not in metrics:
            blocking.append(f"R001: Missing required key '{key}' in {pipeline_type}")
    
    # For normalization pipeline, check sub-structures
    if pipeline_type == 'normalization_pipeline_metrics':
        
        # R002: expansion_methods enum
        if 'expansion_methods' in metrics:
            allowed = contract[pipeline_type]['expansion_methods']['allowed_values']
            for method in metrics['expansion_methods'].keys():
                if method not in allowed:
                    warnings.append(
                        f"R002: Unknown expansion_method '{method}'. "
                        f"Allowed: {allowed}"
                    )
        
        # R003: match_methods enum
        if 'match_methods' in metrics:
            allowed = contract[pipeline_type]['match_methods']['allowed_values']
            for method in metrics['match_methods'].keys():
                if method not in allowed:
                    warnings.append(
                        f"R003: Unknown match_method '{method}'. "
                        f"Allowed: {allowed}"
                    )
        
        # R004: authority_fingerprint structure
        if 'authority_fingerprint' in metrics:
            fp = metrics['authority_fingerprint']
            fp_spec = contract[pipeline_type]['authority_fingerprint']
            for required_fp_key in fp_spec.get('required_keys', []):
                if required_fp_key not in fp:
                    blocking.append(
                        f"R004: Missing required key '{required_fp_key}' "
                        f"in authority_fingerprint"
                    )
    
    return blocking, warnings


def validate_quarantine_structure(
    quarantine_df: Any,  # pandas DataFrame
    contract: Dict[str, Any]
) -> Tuple[List[str], List[str]]:
    """
    Validate quarantine DataFrame structure.
    
    Args:
        quarantine_df: Quarantine DataFrame from parser/normalizer
        contract: Loaded metrics contract
        
    Returns:
        (blocking_violations, warnings)
    """
    blocking = []
    warnings = []
    
    quarantine_spec = contract.get('quarantine_structure', {})
    required_cols = quarantine_spec.get('required_columns', [])
    reason_enum = quarantine_spec.get('reason_enum', [])
    
    if len(quarantine_df) == 0:
        return blocking, warnings  # Empty quarantine is valid
    
    # R005: Check required columns
    for col in required_cols:
        if col not in quarantine_df.columns:
            blocking.append(f"R005: Missing required column '{col}' in quarantine DataFrame")
    
    # Check reason values
    if 'reason' in quarantine_df.columns:
        unique_reasons = quarantine_df['reason'].unique()
        for reason in unique_reasons:
            if reason not in reason_enum:
                warnings.append(
                    f"Unknown quarantine reason '{reason}'. "
                    f"Known reasons: {reason_enum}"
                )
    
    return blocking, warnings


def main():
    """CLI entry point for CI validation."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    # For CLI: expect path to metrics JSON file
    metrics_path = Path(sys.argv[1])
    contract_path = Path(sys.argv[2]) if len(sys.argv) > 2 else \
                    Path(__file__).parent.parent / 'cms_pricing/ingestion/contracts/metrics_contract_v1.0.json'
    
    if not metrics_path.exists():
        print(f"Error: Metrics file not found: {metrics_path}")
        sys.exit(1)
    
    if not contract_path.exists():
        print(f"Error: Contract file not found: {contract_path}")
        sys.exit(1)
    
    # Load metrics and contract
    with open(metrics_path, 'r') as f:
        metrics = json.load(f)
    
    contract = load_contract(contract_path)
    
    # Determine pipeline type from metrics structure
    if 'expansion_methods' in metrics:
        pipeline_type = 'normalization_pipeline_metrics'
    elif 'rows_parsed' in metrics:
        pipeline_type = 'parser_metrics'
    elif 'join_rate' in metrics:
        pipeline_type = 'enrichment_pipeline_metrics'
    else:
        print(f"Warning: Cannot determine pipeline type from metrics keys: {list(metrics.keys())}")
        pipeline_type = 'parser_metrics'
    
    # Validate
    blocking, warnings = validate_metrics(metrics, pipeline_type, contract)
    
    # Report
    if blocking:
        print(f"❌ BLOCKING VIOLATIONS ({len(blocking)}):")
        for v in blocking:
            print(f"  - {v}")
    
    if warnings:
        print(f"⚠️  WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")
    
    if not blocking and not warnings:
        print(f"✅ Metrics contract validation passed for {pipeline_type}")
        print(f"   Contract: {contract['version']}")
        print(f"   Keys validated: {len(metrics)}")
    
    # Exit code
    if blocking:
        sys.exit(1)  # Fail CI
    elif warnings:
        sys.exit(2)  # Warnings only
    else:
        sys.exit(0)  # Pass


if __name__ == '__main__':
    main()

