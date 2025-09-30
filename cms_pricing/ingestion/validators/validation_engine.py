"""
DIS-Compliant Validation Engine
Following Data Ingestion Standard PRD v1.0

This module provides comprehensive validation capabilities including
structural, domain, statistical, and business rule validation.
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, List, Optional, Callable, Union
import pandas as pd
import numpy as np
from scipy import stats
import structlog

logger = structlog.get_logger()


class ValidationSeverity(Enum):
    """Validation severity levels"""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    rule_name: str
    severity: ValidationSeverity
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    threshold: Optional[float] = None
    actual_value: Optional[float] = None


@dataclass
class ValidationReport:
    """Complete validation report"""
    dataset_name: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    warning_checks: int
    results: List[ValidationResult]
    overall_valid: bool
    quality_score: float


class ValidationRule(ABC):
    """Base class for validation rules"""
    
    def __init__(self, name: str, description: str, severity: ValidationSeverity = ValidationSeverity.ERROR):
        self.name = name
        self.description = description
        self.severity = severity
    
    @abstractmethod
    def validate(self, df: pd.DataFrame, schema: Dict[str, Any] = None) -> ValidationResult:
        """Validate DataFrame and return result"""
        pass


class StructuralValidator:
    """Structural validation following DIS requirements"""
    
    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> ValidationResult:
        """Check that all required columns are present"""
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            return ValidationResult(
                rule_name="required_columns",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Missing required columns: {missing_columns}",
                details={"missing_columns": list(missing_columns)}
            )
        
        return ValidationResult(
            rule_name="required_columns",
            severity=ValidationSeverity.ERROR,
            passed=True,
            message="All required columns present"
        )
    
    @staticmethod
    def validate_data_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> ValidationResult:
        """Check that columns have expected data types"""
        type_mismatches = []
        
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    type_mismatches.append({
                        "column": col,
                        "expected": expected_type,
                        "actual": actual_type
                    })
        
        if type_mismatches:
            return ValidationResult(
                rule_name="data_types",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message=f"Data type mismatches found: {len(type_mismatches)}",
                details={"mismatches": type_mismatches}
            )
        
        return ValidationResult(
            rule_name="data_types",
            severity=ValidationSeverity.WARNING,
            passed=True,
            message="All data types match expected types"
        )
    
    @staticmethod
    def validate_row_count(df: pd.DataFrame, min_rows: int = 1, max_rows: Optional[int] = None) -> ValidationResult:
        """Check that row count is within expected range"""
        row_count = len(df)
        
        if row_count < min_rows:
            return ValidationResult(
                rule_name="row_count",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Too few rows: {row_count} < {min_rows}",
                actual_value=row_count,
                threshold=min_rows
            )
        
        if max_rows and row_count > max_rows:
            return ValidationResult(
                rule_name="row_count",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message=f"More rows than expected: {row_count} > {max_rows}",
                actual_value=row_count,
                threshold=max_rows
            )
        
        return ValidationResult(
            rule_name="row_count",
            severity=ValidationSeverity.ERROR,
            passed=True,
            message=f"Row count within expected range: {row_count}",
            actual_value=row_count
        )


class DomainValidator:
    """Domain validation for business rules"""
    
    @staticmethod
    def validate_state_codes(df: pd.DataFrame, state_column: str = "state") -> ValidationResult:
        """Validate state codes against US state/territory list"""
        if state_column not in df.columns:
            return ValidationResult(
                rule_name="state_codes",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"State column '{state_column}' not found"
            )
        
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC', 'AS', 'GU', 'MP', 'PR', 'VI'
        }
        
        invalid_states = set(df[state_column].dropna().unique()) - valid_states
        
        if invalid_states:
            return ValidationResult(
                rule_name="state_codes",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Invalid state codes found: {invalid_states}",
                details={"invalid_states": list(invalid_states)}
            )
        
        return ValidationResult(
            rule_name="state_codes",
            severity=ValidationSeverity.ERROR,
            passed=True,
            message="All state codes are valid"
        )
    
    @staticmethod
    def validate_zip_codes(df: pd.DataFrame, zip_column: str = "zip5") -> ValidationResult:
        """Validate ZIP5 codes format"""
        if zip_column not in df.columns:
            return ValidationResult(
                rule_name="zip_codes",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"ZIP column '{zip_column}' not found"
            )
        
        zip_pattern = re.compile(r'^\d{5}$')
        invalid_zips = []
        
        for idx, zip_code in df[zip_column].dropna().items():
            if not zip_pattern.match(str(zip_code)):
                invalid_zips.append((idx, zip_code))
        
        if invalid_zips:
            return ValidationResult(
                rule_name="zip_codes",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Invalid ZIP5 format found: {len(invalid_zips)} records",
                details={"invalid_zips": invalid_zips[:10]}  # Show first 10
            )
        
        return ValidationResult(
            rule_name="zip_codes",
            severity=ValidationSeverity.ERROR,
            passed=True,
            message="All ZIP codes have valid format"
        )
    
    @staticmethod
    def validate_locality_codes(df: pd.DataFrame, locality_column: str = "locality") -> ValidationResult:
        """Validate locality codes (2-digit)"""
        if locality_column not in df.columns:
            return ValidationResult(
                rule_name="locality_codes",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Locality column '{locality_column}' not found"
            )
        
        locality_pattern = re.compile(r'^\d{2}$')
        invalid_localities = []
        
        for idx, locality in df[locality_column].dropna().items():
            if not locality_pattern.match(str(locality)):
                invalid_localities.append((idx, locality))
        
        if invalid_localities:
            return ValidationResult(
                rule_name="locality_codes",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Invalid locality format found: {len(invalid_localities)} records",
                details={"invalid_localities": invalid_localities[:10]}
            )
        
        return ValidationResult(
            rule_name="locality_codes",
            severity=ValidationSeverity.ERROR,
            passed=True,
            message="All locality codes have valid format"
        )


class StatisticalValidator:
    """Statistical validation for data quality"""
    
    @staticmethod
    def validate_null_rates(df: pd.DataFrame, max_null_rate: float = 0.05) -> ValidationResult:
        """Check that null rates are within acceptable thresholds"""
        high_null_columns = []
        
        for col in df.columns:
            null_rate = df[col].isnull().sum() / len(df)
            if null_rate > max_null_rate:
                high_null_columns.append({
                    "column": col,
                    "null_rate": null_rate,
                    "threshold": max_null_rate
                })
        
        if high_null_columns:
            return ValidationResult(
                rule_name="null_rates",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message=f"High null rates found in {len(high_null_columns)} columns",
                details={"high_null_columns": high_null_columns},
                threshold=max_null_rate
            )
        
        return ValidationResult(
            rule_name="null_rates",
            severity=ValidationSeverity.WARNING,
            passed=True,
            message="All null rates within acceptable thresholds",
            threshold=max_null_rate
        )
    
    @staticmethod
    def validate_uniqueness(df: pd.DataFrame, key_columns: List[str]) -> ValidationResult:
        """Check uniqueness of key columns"""
        if not key_columns:
            return ValidationResult(
                rule_name="uniqueness",
                severity=ValidationSeverity.ERROR,
                passed=True,
                message="No key columns specified for uniqueness check"
            )
        
        # Check that all key columns exist
        missing_columns = set(key_columns) - set(df.columns)
        if missing_columns:
            return ValidationResult(
                rule_name="uniqueness",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Key columns for uniqueness check not found: {missing_columns}"
            )
        
        duplicate_count = df.duplicated(subset=key_columns).sum()
        
        if duplicate_count > 0:
            return ValidationResult(
                rule_name="uniqueness",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Duplicate records found: {duplicate_count}",
                actual_value=duplicate_count,
                threshold=0
            )
        
        return ValidationResult(
            rule_name="uniqueness",
            severity=ValidationSeverity.ERROR,
            passed=True,
            message="No duplicate records found",
            actual_value=duplicate_count,
            threshold=0
        )
    
    @staticmethod
    def validate_drift(df: pd.DataFrame, reference_df: pd.DataFrame, max_drift: float = 0.15) -> ValidationResult:
        """Check for significant drift from reference data"""
        if len(df) == 0 or len(reference_df) == 0:
            return ValidationResult(
                rule_name="drift",
                severity=ValidationSeverity.WARNING,
                passed=True,
                message="Cannot calculate drift: empty datasets"
            )
        
        # Calculate row count drift
        row_count_drift = abs(len(df) - len(reference_df)) / len(reference_df)
        
        if row_count_drift > max_drift:
            return ValidationResult(
                rule_name="drift",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message=f"Significant row count drift detected: {row_count_drift:.2%}",
                actual_value=row_count_drift,
                threshold=max_drift
            )
        
        return ValidationResult(
            rule_name="drift",
            severity=ValidationSeverity.WARNING,
            passed=True,
            message=f"Row count drift within acceptable range: {row_count_drift:.2%}",
            actual_value=row_count_drift,
            threshold=max_drift
        )


class ValidationEngine:
    """
    Main validation engine that orchestrates all validation rules
    following DIS requirements.
    """
    
    def __init__(self):
        self.structural_validator = StructuralValidator()
        self.domain_validator = DomainValidator()
        self.statistical_validator = StatisticalValidator()
    
    def validate_dataset(
        self, 
        df: pd.DataFrame, 
        dataset_name: str,
        schema: Dict[str, Any] = None,
        reference_df: pd.DataFrame = None
    ) -> ValidationReport:
        """
        Comprehensive dataset validation following DIS standards.
        
        Args:
            df: DataFrame to validate
            dataset_name: Name of the dataset
            schema: Optional schema contract
            reference_df: Optional reference data for drift detection
            
        Returns:
            Complete validation report
        """
        results = []
        
        # Structural validation
        if schema and "required_columns" in schema:
            results.append(
                self.structural_validator.validate_required_columns(
                    df, schema["required_columns"]
                )
            )
        
        if schema and "expected_types" in schema:
            results.append(
                self.structural_validator.validate_data_types(
                    df, schema["expected_types"]
                )
            )
        
        # Always check minimum row count
        results.append(
            self.structural_validator.validate_row_count(df, min_rows=1)
        )
        
        # Domain validation for common fields
        if "state" in df.columns:
            results.append(
                self.domain_validator.validate_state_codes(df)
            )
        
        if "zip5" in df.columns:
            results.append(
                self.domain_validator.validate_zip_codes(df)
            )
        
        if "locality" in df.columns:
            results.append(
                self.domain_validator.validate_locality_codes(df)
            )
        
        # Statistical validation
        results.append(
            self.statistical_validator.validate_null_rates(df)
        )
        
        if schema and "primary_keys" in schema:
            results.append(
                self.statistical_validator.validate_uniqueness(
                    df, schema["primary_keys"]
                )
            )
        
        # Drift detection if reference data provided
        if reference_df is not None:
            results.append(
                self.statistical_validator.validate_drift(df, reference_df)
            )
        
        # Calculate summary statistics
        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.passed)
        failed_checks = sum(1 for r in results if not r.passed and r.severity == ValidationSeverity.ERROR)
        warning_checks = sum(1 for r in results if not r.passed and r.severity == ValidationSeverity.WARNING)
        
        overall_valid = failed_checks == 0
        quality_score = passed_checks / total_checks if total_checks > 0 else 0.0
        
        return ValidationReport(
            dataset_name=dataset_name,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            warning_checks=warning_checks,
            results=results,
            overall_valid=overall_valid,
            quality_score=quality_score
        )
    
    def validate_business_rules(self, df: pd.DataFrame, rules: List[Callable]) -> List[ValidationResult]:
        """Validate custom business rules"""
        results = []
        
        for rule in rules:
            try:
                result = rule(df)
                if isinstance(result, ValidationResult):
                    results.append(result)
                else:
                    # Convert boolean result to ValidationResult
                    results.append(ValidationResult(
                        rule_name=rule.__name__,
                        severity=ValidationSeverity.ERROR,
                        passed=bool(result),
                        message="Business rule validation"
                    ))
            except Exception as e:
                results.append(ValidationResult(
                    rule_name=rule.__name__,
                    severity=ValidationSeverity.ERROR,
                    passed=False,
                    message=f"Business rule validation failed: {str(e)}"
                ))
        
        return results
