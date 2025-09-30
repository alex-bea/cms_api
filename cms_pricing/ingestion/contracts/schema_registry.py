"""
DIS-Compliant Schema Registry
Following Data Ingestion Standard PRD v1.0

This module provides schema contract management, validation, and evolution
capabilities for all DIS-compliant ingestors.
"""

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import structlog

logger = structlog.get_logger()


@dataclass
class ColumnSpec:
    """Column specification following DIS standards"""
    name: str
    type: str  # pandas dtype or SQL type
    nullable: bool
    description: str
    unit: Optional[str] = None
    domain: Optional[List[str]] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    pattern: Optional[str] = None  # regex pattern
    sample_values: Optional[List[Any]] = None


@dataclass
class SchemaContract:
    """Schema contract following DIS requirements"""
    dataset_name: str
    version: str
    generated_at: str
    columns: Dict[str, ColumnSpec]
    primary_keys: List[str]
    partition_columns: List[str]
    business_rules: List[str]
    quality_thresholds: Dict[str, float]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "dataset_name": self.dataset_name,
            "version": self.version,
            "generated_at": self.generated_at,
            "columns": {
                name: {
                    "name": spec.get("name", name) if isinstance(spec, dict) else spec.name,
                    "type": spec.get("type") if isinstance(spec, dict) else spec.type,
                    "nullable": spec.get("nullable") if isinstance(spec, dict) else spec.nullable,
                    "description": spec.get("description") if isinstance(spec, dict) else spec.description,
                    "unit": spec.get("unit") if isinstance(spec, dict) else spec.unit,
                    "domain": spec.get("domain") if isinstance(spec, dict) else spec.domain,
                    "min_value": spec.get("min_value") if isinstance(spec, dict) else spec.min_value,
                    "max_value": spec.get("max_value") if isinstance(spec, dict) else spec.max_value,
                    "pattern": spec.get("pattern") if isinstance(spec, dict) else spec.pattern,
                    "sample_values": spec.get("sample_values") if isinstance(spec, dict) else spec.sample_values
                }
                for name, spec in self.columns.items()
            },
            "primary_keys": self.primary_keys,
            "partition_columns": self.partition_columns,
            "business_rules": self.business_rules,
            "quality_thresholds": self.quality_thresholds
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaContract':
        """Create from dictionary"""
        columns = {}
        for name, col_data in data.get("columns", {}).items():
            columns[name] = ColumnSpec(
                name=col_data["name"],
                type=col_data["type"],
                nullable=col_data["nullable"],
                description=col_data["description"],
                unit=col_data.get("unit"),
                domain=col_data.get("domain"),
                min_value=col_data.get("min_value"),
                max_value=col_data.get("max_value"),
                pattern=col_data.get("pattern"),
                sample_values=col_data.get("sample_values")
            )
        
        return cls(
            dataset_name=data["dataset_name"],
            version=data["version"],
            generated_at=data["generated_at"],
            columns=columns,
            primary_keys=data.get("primary_keys", []),
            partition_columns=data.get("partition_columns", []),
            business_rules=data.get("business_rules", []),
            quality_thresholds=data.get("quality_thresholds", {})
        )


class SchemaRegistry:
    """
    Central registry for schema contracts following DIS standards.
    
    Manages schema versioning, validation, and evolution across all datasets.
    """
    
    def __init__(self, contracts_dir: str = "cms_pricing/ingestion/contracts"):
        self.contracts_dir = Path(contracts_dir)
        self.contracts_dir.mkdir(parents=True, exist_ok=True)
        self._schemas: Dict[str, SchemaContract] = {}
        self._load_existing_schemas()
    
    def _load_existing_schemas(self):
        """Load existing schema contracts from disk"""
        for schema_file in self.contracts_dir.glob("*.json"):
            try:
                with open(schema_file, 'r') as f:
                    data = json.load(f)
                schema = SchemaContract.from_dict(data)
                self._schemas[schema.dataset_name] = schema
                logger.info(f"Loaded schema contract: {schema.dataset_name}")
            except Exception as e:
                logger.error(f"Failed to load schema {schema_file}: {e}")
    
    def register_schema(self, schema: SchemaContract) -> None:
        """Register a new schema contract"""
        self._schemas[schema.dataset_name] = schema
        
        # Save to disk
        schema_file = self.contracts_dir / f"{schema.dataset_name}_v{schema.version}.json"
        with open(schema_file, 'w') as f:
            json.dump(schema.to_dict(), f, indent=2)
        
        logger.info(f"Registered schema contract: {schema.dataset_name} v{schema.version}")
    
    def get_schema(self, dataset_name: str) -> Optional[SchemaContract]:
        """Get schema contract for dataset"""
        return self._schemas.get(dataset_name)
    
    def list_schemas(self) -> List[str]:
        """List all registered dataset names"""
        return list(self._schemas.keys())
    
    def validate_dataframe(self, df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """
        Validate DataFrame against schema contract
        
        Returns validation results with errors, warnings, and metrics
        """
        schema = self.get_schema(dataset_name)
        if not schema:
            return {
                "valid": False,
                "errors": [f"No schema contract found for {dataset_name}"],
                "warnings": [],
                "metrics": {}
            }
        
        errors = []
        warnings = []
        metrics = {}
        
        # Check required columns
        missing_columns = set(schema.columns.keys()) - set(df.columns)
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check column types and constraints
        for col_name, col_spec in schema.columns.items():
            if col_name not in df.columns:
                continue
            
            col_data = df[col_name]
            
            # Check nullability
            null_count = col_data.isnull().sum()
            if not col_spec.nullable and null_count > 0:
                errors.append(f"Column {col_name} has {null_count} null values but is not nullable")
            
            # Check data types
            expected_type = col_spec.type
            actual_type = str(col_data.dtype)
            if expected_type != actual_type:
                warnings.append(f"Column {col_name} type mismatch: expected {expected_type}, got {actual_type}")
            
            # Check domain values
            if col_spec.domain:
                invalid_values = set(col_data.dropna().unique()) - set(col_spec.domain)
                if invalid_values:
                    errors.append(f"Column {col_name} has invalid values: {invalid_values}")
            
            # Check value ranges
            if col_spec.min_value is not None:
                below_min = (col_data < col_spec.min_value).sum()
                if below_min > 0:
                    errors.append(f"Column {col_name} has {below_min} values below minimum {col_spec.min_value}")
            
            if col_spec.max_value is not None:
                above_max = (col_data > col_spec.max_value).sum()
                if above_max > 0:
                    errors.append(f"Column {col_name} has {above_max} values above maximum {col_spec.max_value}")
            
            # Calculate quality metrics
            metrics[f"{col_name}_null_rate"] = null_count / len(df)
            metrics[f"{col_name}_unique_count"] = col_data.nunique()
        
        # Check primary key uniqueness
        if schema.primary_keys:
            pk_columns = [col for col in schema.primary_keys if col in df.columns]
            if pk_columns:
                duplicate_count = df.duplicated(subset=pk_columns).sum()
                if duplicate_count > 0:
                    errors.append(f"Primary key violation: {duplicate_count} duplicate rows")
                metrics["primary_key_duplicates"] = duplicate_count
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "metrics": metrics
        }
    
    def generate_schema_from_dataframe(
        self, 
        df: pd.DataFrame, 
        dataset_name: str,
        version: str = "1.0",
        primary_keys: List[str] = None,
        partition_columns: List[str] = None
    ) -> SchemaContract:
        """Generate schema contract from DataFrame"""
        columns = {}
        
        for col in df.columns:
            col_data = df[col]
            
            # Determine column description
            description = self._get_column_description(col, dataset_name)
            
            # Determine unit
            unit = self._get_column_unit(col)
            
            # Determine domain for categorical columns
            domain = None
            if col_data.dtype == 'object' and col_data.nunique() < 50:
                domain = sorted(col_data.dropna().unique().tolist())
            
            # Determine value ranges for numeric columns
            min_value = None
            max_value = None
            if pd.api.types.is_numeric_dtype(col_data):
                min_value = float(col_data.min()) if not col_data.empty else None
                max_value = float(col_data.max()) if not col_data.empty else None
            
            columns[col] = ColumnSpec(
                name=col,
                type=str(col_data.dtype),
                nullable=col_data.isnull().any(),
                description=description,
                unit=unit,
                domain=domain,
                min_value=min_value,
                max_value=max_value,
                sample_values=col_data.dropna().head(3).tolist() if len(col_data) > 0 else []
            )
        
        return SchemaContract(
            dataset_name=dataset_name,
            version=version,
            generated_at=datetime.utcnow().isoformat(),
            columns=columns,
            primary_keys=primary_keys or [],
            partition_columns=partition_columns or [],
            business_rules=[],
            quality_thresholds={
                "null_rate_threshold": 0.05,
                "duplicate_rate_threshold": 0.0
            }
        )
    
    def _get_column_description(self, col_name: str, dataset_name: str) -> str:
        """Get human-readable description for column"""
        descriptions = {
            "zip5": "5-digit ZIP code",
            "zip9_low": "9-digit ZIP code (low range)",
            "zip9_high": "9-digit ZIP code (high range)",
            "state": "2-letter state code",
            "locality": "2-digit locality code",
            "rural_flag": "Rural area indicator",
            "effective_from": "Effective date",
            "vintage": "Data vintage",
            "source_filename": "Source file name",
            "ingest_run_id": "Ingestion run identifier",
            "batch_id": "Batch execution identifier",
            "release_id": "Release identifier"
        }
        return descriptions.get(col_name, f"Column: {col_name}")
    
    def _get_column_unit(self, col_name: str) -> Optional[str]:
        """Get unit for column if applicable"""
        units = {
            "lat": "degrees",
            "lon": "degrees", 
            "latitude": "degrees",
            "longitude": "degrees",
            "distance": "miles",
            "population": "count",
            "area": "square_miles"
        }
        return units.get(col_name.lower())


# Global schema registry instance
schema_registry = SchemaRegistry()
