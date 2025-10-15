#!/usr/bin/env python3
"""
Schema contract validation per STD-parser-contracts v1.1

Enforces:
- All numeric columns have precision, rounding_mode, scale, multipleOf
- row_content_hash pattern is 64-char hex
- hash_spec_version present
- column_order matches columns keys (excluding metadata)
- column_order ∩ hash_metadata_exclusions = ∅
- CF schema v2.0+ does NOT have vintage_year column
"""

import json
import sys
from pathlib import Path

def validate_schema(schema_path: Path) -> list[str]:
    """
    Validate schema contract follows STD-parser-contracts v1.1
    
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    with open(schema_path) as f:
        schema = json.load(f)
    
    dataset_name = schema.get('dataset_name', 'unknown')
    version = schema.get('version', '0')
    
    # Check numeric columns have all required fields
    for col_name, col_def in schema.get('columns', {}).items():
        col_type = col_def.get('type', '')
        if 'float' in col_type or col_type == 'number':
            required_fields = ['precision', 'rounding_mode', 'scale', 'multipleOf']
            missing = [f for f in required_fields if f not in col_def]
            if missing:
                errors.append(
                    f"{col_name}: Numeric column missing {missing}. "
                    f"Required for hash stability per STD-parser-contracts v1.1 §5.2"
                )
    
    # Check row_content_hash field exists and has 64-char pattern
    if 'row_content_hash' in schema.get('columns', {}):
        hash_col = schema['columns']['row_content_hash']
        pattern = hash_col.get('pattern', '')
        if pattern != '^[a-f0-9]{64}$':
            errors.append(
                f"row_content_hash pattern is '{pattern}', "
                f"expected '^[a-f0-9]{{64}}$' (64-char SHA-256)"
            )
    else:
        errors.append("Missing row_content_hash column definition")
    
    # Check hash_spec_version present
    if 'hash_spec_version' not in schema:
        errors.append("Missing 'hash_spec_version' at top level (required for formal hash spec)")
    
    # Check column_order present
    if 'column_order' not in schema:
        errors.append("Missing 'column_order' at top level (required for deterministic hashing)")
    else:
        column_order = set(schema['column_order'])
        
        # Check column_order only includes data columns (not metadata)
        hash_exclusions = set(schema.get('hash_metadata_exclusions', []))
        
        # Check intersection is empty
        intersection = column_order & hash_exclusions
        if intersection:
            errors.append(
                f"column_order ∩ hash_metadata_exclusions must be ∅, "
                f"found overlap: {intersection}"
            )
        
        # Check all columns in column_order exist in columns
        defined_columns = set(schema.get('columns', {}).keys())
        missing_from_schema = column_order - defined_columns
        if missing_from_schema:
            errors.append(
                f"column_order references undefined columns: {missing_from_schema}"
            )
    
    # Check CF schema v2.0+ does NOT have vintage_year
    if dataset_name == 'cms_conversion_factor':
        try:
            version_float = float(version)
            if version_float >= 2.0:
                if 'vintage_year' in schema.get('columns', {}):
                    errors.append(
                        "CF schema v2.0+ must NOT have vintage_year column "
                        "(duplicates metadata fields per STD-parser-contracts v1.1 §6.4)"
                    )
        except ValueError:
            errors.append(f"Invalid version format: {version}")
    
    # Check changelog exists
    if 'changelog' not in schema:
        errors.append("Missing 'changelog' section (required for version tracking)")
    
    return errors


def main():
    """Validate all schema contracts"""
    contracts_dir = Path('cms_pricing/ingestion/contracts')
    
    # Find all schema JSON files
    schema_files = sorted(contracts_dir.glob('cms_*_v*.json'))
    
    if not schema_files:
        print("❌ No schema contract files found")
        sys.exit(1)
    
    print(f"Validating {len(schema_files)} schema contracts...\n")
    
    all_errors = {}
    total_checks = 0
    
    for schema_file in schema_files:
        errors = validate_schema(schema_file)
        total_checks += 1
        
        if errors:
            all_errors[schema_file.name] = errors
    
    # Report results
    if all_errors:
        print(f"❌ Schema validation failed ({len(all_errors)}/{total_checks} schemas with errors):\n")
        for filename, errors in all_errors.items():
            print(f"  {filename}:")
            for error in errors:
                print(f"    - {error}")
        print()
        sys.exit(1)
    else:
        print(f"✅ All {total_checks} schema contracts valid!\n")
        
        # Summary stats
        print("Summary:")
        for schema_file in schema_files:
            with open(schema_file) as f:
                schema = json.load(f)
            
            numeric_cols = sum(
                1 for v in schema.get('columns', {}).values()
                if 'float' in str(v.get('type', '')) or v.get('type') == 'number'
            )
            has_precision = sum(
                1 for v in schema.get('columns', {}).values()
                if 'precision' in v
            )
            
            print(f"  {schema.get('dataset_name', 'unknown'):25s} "
                  f"v{schema.get('version', '?'):4s}  "
                  f"numeric cols: {numeric_cols:2d}  "
                  f"with precision: {has_precision:2d}")
        
        sys.exit(0)


if __name__ == '__main__':
    main()

