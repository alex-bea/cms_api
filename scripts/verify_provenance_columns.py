#!/usr/bin/env python3
"""Verify that parquet files from ingestion include provenance columns (release_id, batch_id)

This script checks normalized parquet files to ensure they contain the expected
provenance columns required for Phase 2 database loading.

Usage:
    python scripts/verify_provenance_columns.py --data-dir ./data
"""

import sys
import argparse
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

import structlog

logger = structlog.get_logger()


def verify_parquet_file(file_path: Path, expected_columns: list[str]) -> dict:
    """
    Verify a parquet file has the expected provenance columns.
    
    Args:
        file_path: Path to parquet file
        expected_columns: List of column names that should exist
        
    Returns:
        Dict with verification results
    """
    result = {
        "file": str(file_path),
        "exists": file_path.exists(),
        "columns_present": [],
        "columns_missing": [],
        "has_data": False,
        "row_count": 0,
        "sample_values": {}
    }
    
    if not result["exists"]:
        return result
    
    try:
        df = pd.read_parquet(file_path)
        result["has_data"] = len(df) > 0
        result["row_count"] = len(df)
        result["actual_columns"] = list(df.columns)
        
        for col in expected_columns:
            if col in df.columns:
                result["columns_present"].append(col)
                # Get sample values if data exists
                if result["has_data"]:
                    non_null_values = df[col].dropna().unique()
                    if len(non_null_values) > 0:
                        result["sample_values"][col] = str(non_null_values[0])
            else:
                result["columns_missing"].append(col)
        
        return result
        
    except Exception as e:
        result["error"] = str(e)
        return result


def check_ingestion_outputs(data_dir: Path, dataset_filter: str = None, build_id_filter: str = None) -> dict:
    """
    Check all normalized parquet files for provenance columns.
    
    Args:
        data_dir: Root data directory containing ingestion outputs
        dataset_filter: Optional dataset name to filter (e.g., 'MPFS', 'OPPS')
        build_id_filter: Optional build ID to filter (checks all datasets if dataset_filter is None)
        
    Returns:
        Dict with verification results by dataset
    """
    results = {}
    expected_columns = ["release_id", "batch_id"]
    
    # Determine which datasets to check
    datasets_to_check = []
    if dataset_filter:
        datasets_to_check = [dataset_filter.upper()]
    else:
        # Check all available datasets
        for item in data_dir.iterdir():
            if item.is_dir() and item.name.upper() in ["MPFS", "OPPS", "ASC", "CLFS", "DMEPOS", "IPPS"]:
                datasets_to_check.append(item.name)
    
    for dataset_name in datasets_to_check:
        dataset_dir = data_dir / dataset_name
        if not dataset_dir.exists():
            continue
        
        dataset_results = []
        for build_dir in dataset_dir.iterdir():
            if not build_dir.is_dir():
                continue
            
            # Filter by build_id if specified
            if build_id_filter and build_dir.name != build_id_filter:
                continue
            
            normalized_dir = build_dir / "normalized"
            if not normalized_dir.exists():
                continue
            
            for parquet_file in normalized_dir.glob("*.parquet"):
                result = verify_parquet_file(parquet_file, expected_columns)
                dataset_results.append(result)
                
                if result["columns_missing"]:
                    logger.warning(
                        "Missing provenance columns",
                        file=str(parquet_file),
                        missing=result["columns_missing"]
                    )
                else:
                    logger.info(
                        "Provenance columns verified",
                        file=str(parquet_file),
                        release_id=result["sample_values"].get("release_id"),
                        batch_id=result["sample_values"].get("batch_id")
                    )
        
        if dataset_results:
            results[dataset_name] = dataset_results
    
    return results


def print_summary(results: dict):
    """Print a summary of verification results."""
    print("\n" + "="*80)
    print("PROVENANCE COLUMN VERIFICATION SUMMARY")
    print("="*80 + "\n")
    
    total_files = 0
    files_with_provenance = 0
    files_missing_provenance = 0
    
    for dataset, file_results in results.items():
        print(f"Dataset: {dataset}")
        print("-" * 80)
        
        for result in file_results:
            total_files += 1
            
            if not result.get("exists"):
                print(f"  ❌ {Path(result['file']).name}: File not found")
                continue
            
            if result.get("error"):
                print(f"  ⚠️  {Path(result['file']).name}: Error - {result['error']}")
                continue
            
            if not result["columns_missing"]:
                files_with_provenance += 1
                release_id = result["sample_values"].get("release_id", "N/A")
                batch_id = result["sample_values"].get("batch_id", "N/A")
                print(f"  ✅ {Path(result['file']).name}: {result['row_count']} rows")
                print(f"      release_id: {release_id}, batch_id: {batch_id}")
            else:
                files_missing_provenance += 1
                print(f"  ❌ {Path(result['file']).name}: Missing {', '.join(result['columns_missing'])}")
        
        print()
    
    print("="*80)
    print(f"Total files checked: {total_files}")
    print(f"Files with provenance: {files_with_provenance}")
    print(f"Files missing provenance: {files_missing_provenance}")
    print("="*80 + "\n")
    
    if files_missing_provenance > 0:
        print("⚠️  WARNING: Some files are missing provenance columns.")
        print("   These files may need to be re-ingested with updated ingestors.")
        return 1
    else:
        print("✅ All checked files have provenance columns.")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Verify provenance columns in ingestion outputs"
    )
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Root data directory containing ingestion outputs"
    )
    parser.add_argument(
        "--dataset",
        choices=["MPFS", "OPPS", "ASC", "CLFS", "DMEPOS", "IPPS"],
        help="Filter to specific dataset (default: check all datasets)"
    )
    parser.add_argument(
        "--build-id",
        help="Filter to specific build ID (can be combined with --dataset)"
    )
    
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    
    if not data_dir.exists():
        logger.error("Data directory not found", path=str(data_dir))
        return 1
    
    results = check_ingestion_outputs(data_dir, 
                                     dataset_filter=args.dataset,
                                     build_id_filter=args.build_id)
    return print_summary(results)


if __name__ == "__main__":
    sys.exit(main())

