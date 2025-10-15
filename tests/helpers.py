"""
Test helper utilities for CMS API testing.

This module provides reusable test utilities for parser testing,
contract compliance, and data validation per STD-parser-contracts.
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime
import pandas as pd


def validate_parser_output(
    df: pd.DataFrame,
    required_data_columns: List[str],
    required_metadata_columns: List[str] = None,
    natural_key_cols: List[str] = None
) -> List[str]:
    """
    Validate parser output follows STD-parser-contracts requirements.
    
    Use in tests to verify contract compliance.
    
    Args:
        df: Parser output DataFrame
        required_data_columns: Expected data columns from schema
        required_metadata_columns: Expected metadata columns (default standard set)
        natural_key_cols: Natural key for sort verification
        
    Returns:
        List of validation errors (empty if valid)
        
    Example:
        >>> errors = validate_parser_output(
        ...     df,
        ...     required_data_columns=['hcpcs', 'work_rvu'],
        ...     natural_key_cols=['hcpcs']
        ... )
        >>> assert len(errors) == 0, f"Validation failed: {errors}"
    """
    if required_metadata_columns is None:
        # Standard metadata columns per STD-parser-contracts §6.4
        required_metadata_columns = [
            'release_id',
            'vintage_date',
            'product_year',
            'quarter_vintage',
            'source_filename',
            'source_file_sha256',
            'parsed_at',
            'row_content_hash'
        ]
    
    errors = []
    
    # Check data columns present
    missing_data = [c for c in required_data_columns if c not in df.columns]
    if missing_data:
        errors.append(f"Missing data columns: {missing_data}")
    
    # Check metadata columns present
    missing_metadata = [c for c in required_metadata_columns if c not in df.columns]
    if missing_metadata:
        errors.append(f"Missing metadata columns: {missing_metadata}")
    
    # Check row_content_hash exists and is valid format (16-char hex)
    if 'row_content_hash' in df.columns:
        invalid_hash = df[~df['row_content_hash'].str.match(r'^[a-f0-9]{16}$', na=False)]
        if len(invalid_hash) > 0:
            errors.append(f"Invalid hash format: {len(invalid_hash)} rows")
    
    # Check for object dtypes on code columns (should be categorical or string)
    code_columns = ['hcpcs', 'modifier', 'status_code', 'locality_code', 'mac']
    for col in code_columns:
        if col in df.columns and df[col].dtype == 'object':
            errors.append(f"Column '{col}' is object dtype (should be categorical or string)")
    
    # Check sorted by natural key if provided
    if natural_key_cols and all(c in df.columns for c in natural_key_cols):
        sorted_df = df.sort_values(by=natural_key_cols, na_position='last')
        if not df[natural_key_cols].equals(sorted_df[natural_key_cols]):
            errors.append(f"DataFrame not sorted by natural key: {natural_key_cols}")
    
    # Check index is reset (0, 1, 2, ...)
    if not df.index.equals(pd.RangeIndex(len(df))):
        errors.append("Index not reset (should be 0, 1, 2, ...)")
    
    return errors


def verify_hash_determinism(
    parser_func,
    file_content: bytes,
    filename: str,
    metadata: Dict[str, Any],
    schema_columns: List[str]
) -> Tuple[bool, str]:
    """
    Verify parser produces deterministic hashes per STD-parser-contracts §5.2.
    
    Runs parser twice with same input, compares hashes.
    
    Args:
        parser_func: Parser function to test
        file_content: File bytes
        filename: Filename
        metadata: Parser metadata
        schema_columns: Schema columns for hash verification
        
    Returns:
        (is_deterministic, message)
        
    Example:
        >>> from io import BytesIO
        >>> is_det, msg = verify_hash_determinism(
        ...     parse_pprrvu,
        ...     csv_bytes,
        ...     "test.csv",
        ...     metadata,
        ...     ['hcpcs', 'work_rvu']
        ... )
        >>> assert is_det, msg
    """
    from io import BytesIO
    
    # Parse twice
    df1 = parser_func(BytesIO(file_content), filename, metadata)
    df2 = parser_func(BytesIO(file_content), filename, metadata)
    
    # Compare hashes
    if 'row_content_hash' not in df1.columns:
        return False, "Missing row_content_hash column"
    
    if not df1['row_content_hash'].equals(df2['row_content_hash']):
        # Find first mismatch
        mismatches = df1['row_content_hash'] != df2['row_content_hash']
        first_mismatch = mismatches.idxmax() if mismatches.any() else None
        return False, f"Hash mismatch at row {first_mismatch}"
    
    return True, f"Deterministic: {len(df1)} rows, all hashes match"


def create_test_metadata(
    dataset_id: str = 'test',
    release_id: str = 'test_2025_q4_20251015',
    product_year: str = '2025',
    quarter_vintage: str = '2025Q4'
) -> Dict[str, Any]:
    """
    Create standard test metadata for parser testing.
    
    Args:
        dataset_id: Dataset identifier
        release_id: Release identifier
        product_year: Product year
        quarter_vintage: Quarter vintage
        
    Returns:
        Complete metadata dict for parser tests
        
    Example:
        >>> metadata = create_test_metadata('pprrvu')
        >>> df = parse_pprrvu(file_obj, "test.csv", metadata)
    """
    return {
        'dataset_id': dataset_id,
        'release_id': release_id,
        'vintage_date': datetime.utcnow(),
        'product_year': product_year,
        'quarter_vintage': quarter_vintage,
        'source_uri': 'https://test.cms.gov/test.zip',
        'file_sha256': 'test_sha256_abc123',
        'parser_version': 'v1.0.0',
        'schema_id': f'cms_{dataset_id}_v1.0',
    }


def assert_parser_contract_compliance(
    df: pd.DataFrame,
    dataset_name: str,
    required_data_columns: List[str],
    natural_key_cols: List[str],
    schema_columns: List[str]
):
    """
    Assert parser output complies with STD-parser-contracts.
    
    Use in tests as a single assertion that checks all requirements.
    
    Args:
        df: Parser output
        dataset_name: Dataset name for error messages
        required_data_columns: Data columns from schema
        natural_key_cols: Natural key for sort check
        schema_columns: Schema columns for hash check
        
    Raises:
        AssertionError: If any contract requirement violated
        
    Example:
        >>> assert_parser_contract_compliance(
        ...     df,
        ...     'pprrvu',
        ...     required_data_columns=['hcpcs', 'work_rvu'],
        ...     natural_key_cols=['hcpcs'],
        ...     schema_columns=['hcpcs', 'work_rvu', 'status_code']
        ... )
    """
    errors = validate_parser_output(
        df,
        required_data_columns,
        natural_key_cols=natural_key_cols
    )
    
    if errors:
        raise AssertionError(
            f"{dataset_name} parser contract violations:\n" +
            "\n".join(f"  - {err}" for err in errors)
        )
    
    # Additional checks
    assert len(df) > 0, f"{dataset_name}: DataFrame is empty"
    
    assert 'row_content_hash' in df.columns, f"{dataset_name}: Missing row_content_hash"
    
    # Check all schema columns present
    missing_schema = [c for c in schema_columns if c not in df.columns]
    assert not missing_schema, f"{dataset_name}: Missing schema columns: {missing_schema}"


__all__ = [
    'validate_parser_output',
    'verify_hash_determinism',
    'create_test_metadata',
    'assert_parser_contract_compliance',
]

