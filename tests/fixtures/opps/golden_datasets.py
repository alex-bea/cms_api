#!/usr/bin/env python3
"""
OPPS Golden Datasets Management
===============================

Golden dataset management for OPPS scraper following QTS v1.1 standards.
Includes SemVer versioning, baseline metrics, and backward compatibility.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.1
"""

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class DatasetVersion(Enum):
    """Dataset version enumeration."""
    V1_0_0 = "1.0.0"
    V1_1_0 = "1.1.0"
    V1_2_0 = "1.2.0"


@dataclass
class GoldenDatasetManifest:
    """Manifest for golden datasets."""
    fixture_id: str
    schema_version: str
    source_digest: str
    generated_at: str
    notes: str
    expected_rows: int
    expected_files: List[str]
    validation_rules: Dict[str, Any]
    baseline_metrics: Dict[str, Any]


@dataclass
class BaselineMetrics:
    """Baseline metrics for performance comparison."""
    row_count: int
    distinct_keys: int
    distribution_summary: Dict[str, Any]
    processing_time_sec: float
    memory_usage_mb: float
    generated_at: str
    source_digest: str


class OPPSScraperGoldenDatasets:
    """Golden dataset management for OPPS scraper."""
    
    def __init__(self, fixtures_dir: Path = Path("tests/fixtures/opps/golden")):
        """Initialize golden dataset manager."""
        self.fixtures_dir = fixtures_dir
        self.fixtures_dir.mkdir(parents=True, exist_ok=True)
    
    def create_golden_dataset(self, 
                            version: DatasetVersion,
                            source_data: List[Dict[str, Any]],
                            notes: str = "") -> Path:
        """Create a new golden dataset."""
        # Generate fixture ID
        fixture_id = f"opps-golden@{version.value}"
        
        # Calculate source digest
        source_json = json.dumps(source_data, sort_keys=True)
        source_digest = hashlib.sha256(source_json.encode()).hexdigest()
        
        # Create manifest
        manifest = GoldenDatasetManifest(
            fixture_id=fixture_id,
            schema_version=version.value,
            source_digest=source_digest,
            generated_at=datetime.now(timezone.utc).isoformat(),
            notes=notes,
            expected_rows=len(source_data),
            expected_files=["addendum_a.csv", "addendum_b.csv"],
            validation_rules={
                "required_columns": ["hcpcs", "modifier", "apc_code", "payment_rate"],
                "data_types": {
                    "hcpcs": "string",
                    "modifier": "string",
                    "apc_code": "string",
                    "payment_rate": "decimal"
                },
                "constraints": {
                    "hcpcs": {"min_length": 5, "max_length": 5},
                    "payment_rate": {"min_value": 0.0}
                }
            },
            baseline_metrics={
                "row_count": len(source_data),
                "distinct_hcpcs": len(set(row.get("hcpcs", "") for row in source_data)),
                "distinct_apc_codes": len(set(row.get("apc_code", "") for row in source_data)),
                "avg_payment_rate": sum(row.get("payment_rate", 0) for row in source_data) / len(source_data) if source_data else 0
            }
        )
        
        # Save dataset
        dataset_dir = self.fixtures_dir / version.value
        dataset_dir.mkdir(exist_ok=True)
        
        # Save data
        data_file = dataset_dir / "data.json"
        with open(data_file, 'w') as f:
            json.dump(source_data, f, indent=2)
        
        # Save manifest
        manifest_file = dataset_dir / "manifest.json"
        with open(manifest_file, 'w') as f:
            json.dump(asdict(manifest), f, indent=2)
        
        return dataset_dir
    
    def load_golden_dataset(self, version: DatasetVersion) -> tuple[List[Dict[str, Any]], GoldenDatasetManifest]:
        """Load a golden dataset."""
        dataset_dir = self.fixtures_dir / version.value
        
        # Load manifest
        manifest_file = dataset_dir / "manifest.json"
        if not manifest_file.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_file}")
        
        with open(manifest_file, 'r') as f:
            manifest_data = json.load(f)
        
        manifest = GoldenDatasetManifest(**manifest_data)
        
        # Load data
        data_file = dataset_dir / "data.json"
        if not data_file.exists():
            raise FileNotFoundError(f"Data file not found: {data_file}")
        
        with open(data_file, 'r') as f:
            data = json.load(f)
        
        return data, manifest
    
    def validate_golden_dataset(self, version: DatasetVersion) -> Dict[str, Any]:
        """Validate a golden dataset against its schema."""
        try:
            data, manifest = self.load_golden_dataset(version)
        except FileNotFoundError as e:
            return {"valid": False, "error": str(e)}
        
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        # Validate row count
        if len(data) != manifest.expected_rows:
            validation_results["errors"].append(
                f"Row count mismatch: expected {manifest.expected_rows}, got {len(data)}"
            )
            validation_results["valid"] = False
        
        # Validate required columns
        if data:
            required_columns = manifest.validation_rules["required_columns"]
            sample_row = data[0]
            missing_columns = [col for col in required_columns if col not in sample_row]
            if missing_columns:
                validation_results["errors"].append(
                    f"Missing required columns: {missing_columns}"
                )
                validation_results["valid"] = False
        
        # Validate data types
        if data:
            data_types = manifest.validation_rules["data_types"]
            for row in data:
                for column, expected_type in data_types.items():
                    if column in row:
                        value = row[column]
                        if expected_type == "string" and not isinstance(value, str):
                            validation_results["warnings"].append(
                                f"Column {column} should be string, got {type(value).__name__}"
                            )
                        elif expected_type == "decimal" and not isinstance(value, (int, float)):
                            validation_results["warnings"].append(
                                f"Column {column} should be decimal, got {type(value).__name__}"
                            )
        
        # Validate constraints
        if data:
            constraints = manifest.validation_rules["constraints"]
            for row in data:
                for column, constraint in constraints.items():
                    if column in row:
                        value = row[column]
                        if "min_length" in constraint and len(str(value)) < constraint["min_length"]:
                            validation_results["errors"].append(
                                f"Column {column} violates min_length constraint"
                            )
                            validation_results["valid"] = False
                        if "max_length" in constraint and len(str(value)) > constraint["max_length"]:
                            validation_results["errors"].append(
                                f"Column {column} violates max_length constraint"
                            )
                            validation_results["valid"] = False
                        if "min_value" in constraint and value < constraint["min_value"]:
                            validation_results["errors"].append(
                                f"Column {column} violates min_value constraint"
                            )
                            validation_results["valid"] = False
        
        # Calculate metrics
        if data:
            validation_results["metrics"] = {
                "row_count": len(data),
                "distinct_hcpcs": len(set(row.get("hcpcs", "") for row in data)),
                "distinct_apc_codes": len(set(row.get("apc_code", "") for row in data)),
                "avg_payment_rate": sum(row.get("payment_rate", 0) for row in data) / len(data) if data else 0
            }
        
        return validation_results
    
    def create_baseline_metrics(self, 
                              version: DatasetVersion,
                              processing_time_sec: float,
                              memory_usage_mb: float) -> BaselineMetrics:
        """Create baseline metrics for a dataset version."""
        data, manifest = self.load_golden_dataset(version)
        
        # Calculate distribution summary
        distribution_summary = {
            "hcpcs_lengths": {},
            "payment_rate_ranges": {
                "min": min(row.get("payment_rate", 0) for row in data) if data else 0,
                "max": max(row.get("payment_rate", 0) for row in data) if data else 0,
                "mean": sum(row.get("payment_rate", 0) for row in data) / len(data) if data else 0
            },
            "apc_code_distribution": {}
        }
        
        # Calculate HCPCS length distribution
        for row in data:
            hcpcs = row.get("hcpcs", "")
            length = len(hcpcs)
            distribution_summary["hcpcs_lengths"][str(length)] = distribution_summary["hcpcs_lengths"].get(str(length), 0) + 1
        
        # Calculate APC code distribution
        for row in data:
            apc_code = row.get("apc_code", "")
            distribution_summary["apc_code_distribution"][apc_code] = distribution_summary["apc_code_distribution"].get(apc_code, 0) + 1
        
        baseline = BaselineMetrics(
            row_count=len(data),
            distinct_keys=len(set(row.get("hcpcs", "") for row in data)),
            distribution_summary=distribution_summary,
            processing_time_sec=processing_time_sec,
            memory_usage_mb=memory_usage_mb,
            generated_at=datetime.now(timezone.utc).isoformat(),
            source_digest=manifest.source_digest
        )
        
        # Save baseline
        baseline_file = self.fixtures_dir / version.value / "baseline.json"
        with open(baseline_file, 'w') as f:
            json.dump(asdict(baseline), f, indent=2)
        
        return baseline
    
    def load_baseline_metrics(self, version: DatasetVersion) -> Optional[BaselineMetrics]:
        """Load baseline metrics for a dataset version."""
        baseline_file = self.fixtures_dir / version.value / "baseline.json"
        if not baseline_file.exists():
            return None
        
        with open(baseline_file, 'r') as f:
            baseline_data = json.load(f)
        
        return BaselineMetrics(**baseline_data)
    
    def compare_with_baseline(self, 
                            version: DatasetVersion,
                            current_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Compare current metrics with baseline."""
        baseline = self.load_baseline_metrics(version)
        if not baseline:
            return {"error": "No baseline metrics found"}
        
        comparison = {
            "version": version.value,
            "baseline_date": baseline.generated_at,
            "current_date": datetime.now(timezone.utc).isoformat(),
            "comparisons": {},
            "regressions": [],
            "improvements": []
        }
        
        # Compare row count
        if "row_count" in current_metrics:
            current_count = current_metrics["row_count"]
            baseline_count = baseline.row_count
            diff_pct = ((current_count - baseline_count) / baseline_count) * 100
            
            comparison["comparisons"]["row_count"] = {
                "baseline": baseline_count,
                "current": current_count,
                "difference": current_count - baseline_count,
                "difference_pct": diff_pct
            }
            
            if abs(diff_pct) > 10:  # 10% threshold
                if diff_pct > 0:
                    comparison["improvements"].append(f"Row count increased by {diff_pct:.1f}%")
                else:
                    comparison["regressions"].append(f"Row count decreased by {abs(diff_pct):.1f}%")
        
        # Compare processing time
        if "processing_time_sec" in current_metrics:
            current_time = current_metrics["processing_time_sec"]
            baseline_time = baseline.processing_time_sec
            diff_pct = ((current_time - baseline_time) / baseline_time) * 100
            
            comparison["comparisons"]["processing_time"] = {
                "baseline": baseline_time,
                "current": current_time,
                "difference": current_time - baseline_time,
                "difference_pct": diff_pct
            }
            
            if diff_pct > 20:  # 20% regression threshold
                comparison["regressions"].append(f"Processing time increased by {diff_pct:.1f}%")
            elif diff_pct < -10:  # 10% improvement threshold
                comparison["improvements"].append(f"Processing time decreased by {abs(diff_pct):.1f}%")
        
        # Compare memory usage
        if "memory_usage_mb" in current_metrics:
            current_memory = current_metrics["memory_usage_mb"]
            baseline_memory = baseline.memory_usage_mb
            diff_pct = ((current_memory - baseline_memory) / baseline_memory) * 100
            
            comparison["comparisons"]["memory_usage"] = {
                "baseline": baseline_memory,
                "current": current_memory,
                "difference": current_memory - baseline_memory,
                "difference_pct": diff_pct
            }
            
            if diff_pct > 20:  # 20% regression threshold
                comparison["regressions"].append(f"Memory usage increased by {diff_pct:.1f}%")
            elif diff_pct < -10:  # 10% improvement threshold
                comparison["improvements"].append(f"Memory usage decreased by {abs(diff_pct):.1f}%")
        
        return comparison
    
    def list_available_versions(self) -> List[DatasetVersion]:
        """List all available dataset versions."""
        versions = []
        for version_dir in self.fixtures_dir.iterdir():
            if version_dir.is_dir():
                try:
                    version = DatasetVersion(version_dir.name)
                    versions.append(version)
                except ValueError:
                    continue
        return sorted(versions, key=lambda v: v.value)
    
    def cleanup_old_versions(self, keep_versions: int = 3) -> List[DatasetVersion]:
        """Clean up old dataset versions, keeping only the most recent ones."""
        versions = self.list_available_versions()
        if len(versions) <= keep_versions:
            return []
        
        # Sort by version and keep only the most recent
        versions_to_keep = versions[-keep_versions:]
        versions_to_remove = versions[:-keep_versions]
        
        # Remove old versions
        for version in versions_to_remove:
            version_dir = self.fixtures_dir / version.value
            if version_dir.exists():
                import shutil
                shutil.rmtree(version_dir)
        
        return versions_to_remove


def create_sample_golden_datasets():
    """Create sample golden datasets for testing."""
    manager = OPPSScraperGoldenDatasets()
    
    # Sample data for v1.0.0
    sample_data_v1 = [
        {
            "hcpcs": "99213",
            "modifier": "25",
            "apc_code": "0601",
            "payment_rate": 125.50,
            "effective_date": "2025-01-01"
        },
        {
            "hcpcs": "99214",
            "modifier": "",
            "apc_code": "0602",
            "payment_rate": 185.75,
            "effective_date": "2025-01-01"
        },
        {
            "hcpcs": "99215",
            "modifier": "25",
            "apc_code": "0603",
            "payment_rate": 245.00,
            "effective_date": "2025-01-01"
        }
    ]
    
    # Create v1.0.0 dataset
    v1_path = manager.create_golden_dataset(
        DatasetVersion.V1_0_0,
        sample_data_v1,
        "Initial OPPS golden dataset with basic HCPCS codes"
    )
    
    # Create baseline metrics for v1.0.0
    baseline_v1 = manager.create_baseline_metrics(
        DatasetVersion.V1_0_0,
        processing_time_sec=0.5,
        memory_usage_mb=2.5
    )
    
    print(f"Created golden dataset v1.0.0: {v1_path}")
    print(f"Created baseline metrics: {baseline_v1.generated_at}")
    
    return v1_path


if __name__ == "__main__":
    """Create sample golden datasets."""
    create_sample_golden_datasets()
