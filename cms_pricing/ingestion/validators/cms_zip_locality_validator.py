"""
CMS ZIP Locality Data Validator
Implements DIS-compliant validation rules for CMS ZIP locality data
"""

import json
import re
from datetime import datetime, date
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
from dataclasses import dataclass
from enum import Enum

from ..contracts.ingestor_spec import ValidationSeverity


class ValidationResult(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"


@dataclass
class ValidationRule:
    """Individual validation rule"""
    name: str
    description: str
    severity: ValidationSeverity
    validator_func: callable
    threshold: float = 1.0


@dataclass
class ValidationReport:
    """Complete validation report"""
    dataset_name: str
    total_records: int
    passed_records: int
    failed_records: int
    warning_records: int
    overall_quality_score: float
    validation_results: List[Dict[str, Any]]
    business_rules_applied: List[str]
    processing_timestamp: datetime
    schema_version: str


class CMSZipLocalityValidator:
    """
    DIS-compliant validator for CMS ZIP locality data
    
    Implements all validation rules from the schema contract:
    - Structural validation (required fields, data types)
    - Domain validation (state codes, ZIP formats)
    - Business rule validation (uniqueness, date ranges)
    - Quality threshold validation
    """
    
    def __init__(self, schema_contract_path: str = None):
        """Initialize validator with schema contract"""
        self.schema_contract = self._load_schema_contract(schema_contract_path)
        self.validation_rules = self._create_validation_rules()
        self.valid_states = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC", "PR", "VI", "AS", "GU", "MP"
        }
    
    def _load_schema_contract(self, schema_path: str = None) -> Dict[str, Any]:
        """Load schema contract from JSON file"""
        if schema_path:
            with open(schema_path, 'r') as f:
                return json.load(f)
        
        # Default schema contract
        return {
            "quality_thresholds": {
                "completeness": {
                    "zip5": 1.0,
                    "state": 1.0,
                    "locality": 1.0,
                    "effective_from": 1.0,
                    "vintage": 1.0
                },
                "accuracy": {
                    "zip5_format": 1.0,
                    "state_code_valid": 1.0,
                    "locality_format": 1.0,
                    "date_format": 1.0
                },
                "uniqueness": {
                    "zip5_per_release": 1.0
                }
            }
        }
    
    def _create_validation_rules(self) -> List[ValidationRule]:
        """Create all validation rules"""
        return [
            # Structural validation
            ValidationRule(
                name="required_fields",
                description="All required fields must be present",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_required_fields,
                threshold=1.0
            ),
            ValidationRule(
                name="data_types",
                description="Data types must match schema",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_data_types,
                threshold=1.0
            ),
            
            # Domain validation
            ValidationRule(
                name="zip5_format",
                description="ZIP5 codes must be exactly 5 digits",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_zip5_format,
                threshold=1.0
            ),
            ValidationRule(
                name="state_codes",
                description="State codes must be valid US state/territory codes",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_state_codes,
                threshold=1.0
            ),
            ValidationRule(
                name="locality_format",
                description="Locality codes must be non-empty numeric strings",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_locality_format,
                threshold=1.0
            ),
            ValidationRule(
                name="date_formats",
                description="Date fields must be valid ISO dates",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_date_formats,
                threshold=1.0
            ),
            
            # Business rule validation
            ValidationRule(
                name="uniqueness",
                description="ZIP5 codes must be unique per vintage",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_uniqueness,
                threshold=1.0
            ),
            ValidationRule(
                name="effective_date_range",
                description="Effective end date must be after start date",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_effective_date_range,
                threshold=1.0
            ),
            ValidationRule(
                name="future_dates",
                description="Dates must not be in the future",
                severity=ValidationSeverity.WARNING,
                validator_func=self._validate_future_dates,
                threshold=0.95
            ),
            
            # Quality validation
            ValidationRule(
                name="completeness",
                description="Critical fields must have high completeness",
                severity=ValidationSeverity.CRITICAL,
                validator_func=self._validate_completeness,
                threshold=0.99
            )
        ]
    
    def validate(self, df: pd.DataFrame, vintage: str = None) -> ValidationReport:
        """
        Validate a DataFrame against all rules
        
        Args:
            df: DataFrame to validate
            vintage: Vintage identifier for uniqueness checks
            
        Returns:
            ValidationReport with detailed results
        """
        if df.empty:
            return ValidationReport(
                dataset_name="cms_zip_locality",
                total_records=0,
                passed_records=0,
                failed_records=0,
                warning_records=0,
                overall_quality_score=0.0,
                validation_results=[],
                business_rules_applied=[],
                processing_timestamp=datetime.now(),
                schema_version="1.0"
            )
        
        validation_results = []
        total_records = len(df)
        passed_records = 0
        failed_records = 0
        warning_records = 0
        
        # Run all validation rules
        for rule in self.validation_rules:
            try:
                result = rule.validator_func(df, vintage)
                validation_results.append({
                    "rule_name": rule.name,
                    "description": rule.description,
                    "severity": rule.severity.value,
                    "passed": result["passed"],
                    "passed_count": result["passed_count"],
                    "failed_count": result["failed_count"],
                    "warning_count": result.get("warning_count", 0),
                    "quality_score": result["quality_score"],
                    "threshold": rule.threshold,
                    "details": result.get("details", {}),
                    "sample_failures": result.get("sample_failures", [])
                })
                
                if result["passed"]:
                    passed_records += result["passed_count"]
                else:
                    if rule.severity == ValidationSeverity.CRITICAL:
                        failed_records += result["failed_count"]
                    else:
                        warning_records += result.get("warning_count", 0)
                        
            except Exception as e:
                validation_results.append({
                    "rule_name": rule.name,
                    "description": rule.description,
                    "severity": rule.severity.value,
                    "passed": False,
                    "passed_count": 0,
                    "failed_count": total_records,
                    "warning_count": 0,
                    "quality_score": 0.0,
                    "threshold": rule.threshold,
                    "details": {"error": str(e)},
                    "sample_failures": []
                })
                failed_records += total_records
        
        # Calculate overall quality score
        if validation_results:
            quality_scores = [r["quality_score"] for r in validation_results if r["quality_score"] > 0]
            overall_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
        else:
            overall_quality_score = 0.0
        
        # Get business rules applied
        business_rules_applied = [rule.name for rule in self.validation_rules]
        
        return ValidationReport(
            dataset_name="cms_zip_locality",
            total_records=total_records,
            passed_records=passed_records,
            failed_records=failed_records,
            warning_records=warning_records,
            overall_quality_score=overall_quality_score,
            validation_results=validation_results,
            business_rules_applied=business_rules_applied,
            processing_timestamp=datetime.now(),
            schema_version="1.0"
        )
    
    # Validation rule implementations
    
    def _validate_required_fields(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate all required fields are present"""
        required_fields = ["zip5", "state", "locality", "effective_from", "vintage"]
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            return {
                "passed": False,
                "passed_count": 0,
                "failed_count": len(df),
                "quality_score": 0.0,
                "details": {"missing_fields": missing_fields}
            }
        
        return {
            "passed": True,
            "passed_count": len(df),
            "failed_count": 0,
            "quality_score": 1.0,
            "details": {"all_required_fields_present": True}
        }
    
    def _validate_data_types(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate data types match expected schema"""
        type_issues = []
        
        # Check ZIP5 is string
        if "zip5" in df.columns and not df["zip5"].dtype == "object":
            type_issues.append("zip5 should be string")
        
        # Check state is string
        if "state" in df.columns and not df["state"].dtype == "object":
            type_issues.append("state should be string")
        
        # Check locality is string
        if "locality" in df.columns and not df["locality"].dtype == "object":
            type_issues.append("locality should be string")
        
        if type_issues:
            return {
                "passed": False,
                "passed_count": 0,
                "failed_count": len(df),
                "quality_score": 0.0,
                "details": {"type_issues": type_issues}
            }
        
        return {
            "passed": True,
            "passed_count": len(df),
            "failed_count": 0,
            "quality_score": 1.0,
            "details": {"all_types_correct": True}
        }
    
    def _validate_zip5_format(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate ZIP5 format (exactly 5 digits)"""
        if "zip5" not in df.columns:
            return {"passed": False, "passed_count": 0, "failed_count": len(df), "quality_score": 0.0}
        
        zip5_pattern = re.compile(r'^\d{5}$')
        valid_zips = df["zip5"].astype(str).str.match(zip5_pattern, na=False)
        valid_count = valid_zips.sum()
        invalid_count = len(df) - valid_count
        
        quality_score = valid_count / len(df) if len(df) > 0 else 0.0
        
        sample_failures = []
        if invalid_count > 0:
            invalid_samples = df[~valid_zips]["zip5"].head(5).tolist()
            sample_failures = [{"zip5": str(zip_val)} for zip_val in invalid_samples]
        
        return {
            "passed": invalid_count == 0,
            "passed_count": valid_count,
            "failed_count": invalid_count,
            "quality_score": quality_score,
            "details": {"zip5_pattern": "^\\d{5}$"},
            "sample_failures": sample_failures
        }
    
    def _validate_state_codes(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate state codes are valid US state/territory codes"""
        if "state" not in df.columns:
            return {"passed": False, "passed_count": 0, "failed_count": len(df), "quality_score": 0.0}
        
        valid_states = df["state"].astype(str).str.upper().isin(self.valid_states)
        valid_count = valid_states.sum()
        invalid_count = len(df) - valid_count
        
        quality_score = valid_count / len(df) if len(df) > 0 else 0.0
        
        sample_failures = []
        if invalid_count > 0:
            invalid_samples = df[~valid_states]["state"].head(5).tolist()
            sample_failures = [{"state": str(state_val)} for state_val in invalid_samples]
        
        return {
            "passed": invalid_count == 0,
            "passed_count": valid_count,
            "failed_count": invalid_count,
            "quality_score": quality_score,
            "details": {"valid_states": list(self.valid_states)},
            "sample_failures": sample_failures
        }
    
    def _validate_locality_format(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate locality codes are non-empty numeric strings"""
        if "locality" not in df.columns:
            return {"passed": False, "passed_count": 0, "failed_count": len(df), "quality_score": 0.0}
        
        # Check non-empty and numeric
        locality_str = df["locality"].astype(str)
        non_empty = locality_str != ""
        numeric = locality_str.str.match(r'^\d+$', na=False)
        valid_localities = non_empty & numeric
        
        valid_count = valid_localities.sum()
        invalid_count = len(df) - valid_count
        
        quality_score = valid_count / len(df) if len(df) > 0 else 0.0
        
        sample_failures = []
        if invalid_count > 0:
            invalid_samples = df[~valid_localities]["locality"].head(5).tolist()
            sample_failures = [{"locality": str(loc_val)} for loc_val in invalid_samples]
        
        return {
            "passed": invalid_count == 0,
            "passed_count": valid_count,
            "failed_count": invalid_count,
            "quality_score": quality_score,
            "details": {"locality_pattern": "^\\d+$"},
            "sample_failures": sample_failures
        }
    
    def _validate_date_formats(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate date fields are valid ISO dates"""
        date_fields = ["effective_from", "effective_to", "vintage"]
        all_valid = True
        total_valid = 0
        total_invalid = 0
        
        for field in date_fields:
            if field not in df.columns:
                continue
                
            # Skip fields that are all null (like effective_to)
            if df[field].isnull().all():
                continue
                
            # Try to parse dates
            try:
                # Convert to string first if needed
                field_data = df[field].astype(str)
                parsed_dates = pd.to_datetime(field_data, format="%Y-%m-%d", errors="coerce")
                valid_dates = parsed_dates.notna()
                total_valid += valid_dates.sum()
                total_invalid += (~valid_dates).sum()
                if not valid_dates.all():
                    all_valid = False
            except Exception as e:
                all_valid = False
                total_invalid += len(df)
        
        quality_score = total_valid / (total_valid + total_invalid) if (total_valid + total_invalid) > 0 else 0.0
        
        return {
            "passed": all_valid,
            "passed_count": total_valid,
            "failed_count": total_invalid,
            "quality_score": quality_score,
            "details": {"date_format": "YYYY-MM-DD"}
        }
    
    def _validate_uniqueness(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate ZIP5 codes are unique per vintage"""
        if "zip5" not in df.columns:
            return {"passed": False, "passed_count": 0, "failed_count": len(df), "quality_score": 0.0}
        
        # Check uniqueness within the dataset
        unique_zips = df["zip5"].nunique()
        total_records = len(df)
        duplicates = total_records - unique_zips
        
        quality_score = unique_zips / total_records if total_records > 0 else 0.0
        
        return {
            "passed": duplicates == 0,
            "passed_count": unique_zips,
            "failed_count": duplicates,
            "quality_score": quality_score,
            "details": {"unique_zip5_count": unique_zips, "duplicate_count": duplicates}
        }
    
    def _validate_effective_date_range(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate effective end date is after start date"""
        if "effective_from" not in df.columns or "effective_to" not in df.columns:
            return {"passed": True, "passed_count": len(df), "failed_count": 0, "quality_score": 1.0}
        
        # Convert to datetime for comparison
        try:
            from_dates = pd.to_datetime(df["effective_from"], errors="coerce")
            to_dates = pd.to_datetime(df["effective_to"], errors="coerce")
            
            # Check where effective_to is not null and is before effective_from
            invalid_ranges = (to_dates.notna()) & (to_dates < from_dates)
            invalid_count = invalid_ranges.sum()
            valid_count = len(df) - invalid_count
            
            quality_score = valid_count / len(df) if len(df) > 0 else 0.0
            
            return {
                "passed": invalid_count == 0,
                "passed_count": valid_count,
                "failed_count": invalid_count,
                "quality_score": quality_score,
                "details": {"invalid_date_ranges": invalid_count}
            }
        except:
            return {"passed": False, "passed_count": 0, "failed_count": len(df), "quality_score": 0.0}
    
    def _validate_future_dates(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate dates are not in the future"""
        date_fields = ["effective_from", "effective_to", "vintage"]
        today = date.today()
        total_valid = 0
        total_future = 0
        
        for field in date_fields:
            if field not in df.columns:
                continue
                
            # Skip fields that are all null (like effective_to)
            if df[field].isnull().all():
                continue
                
            try:
                # Convert to datetime, handling different input types
                if field == "effective_from" and hasattr(df[field].iloc[0], 'date'):
                    # Already datetime.date objects
                    field_dates = pd.to_datetime(df[field]).dt.date
                else:
                    # String dates
                    field_dates = pd.to_datetime(df[field], errors="coerce").dt.date
                
                future_dates = field_dates > today
                total_valid += (~future_dates).sum()
                total_future += future_dates.sum()
            except Exception as e:
                # If there's an error, count as future dates
                total_future += len(df)
        
        quality_score = total_valid / (total_valid + total_future) if (total_valid + total_future) > 0 else 0.0
        
        return {
            "passed": total_future == 0,
            "passed_count": total_valid,
            "failed_count": 0,
            "warning_count": total_future,
            "quality_score": quality_score,
            "details": {"future_dates": total_future}
        }
    
    def _validate_completeness(self, df: pd.DataFrame, vintage: str = None) -> Dict[str, Any]:
        """Validate critical fields have high completeness"""
        critical_fields = ["zip5", "state", "locality", "effective_from", "vintage"]
        total_records = len(df)
        
        if total_records == 0:
            return {"passed": False, "passed_count": 0, "failed_count": 0, "quality_score": 0.0}
        
        completeness_scores = []
        for field in critical_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                completeness = non_null_count / total_records
                completeness_scores.append(completeness)
        
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0.0
        threshold = 0.99  # 99% completeness threshold
        
        return {
            "passed": avg_completeness >= threshold,
            "passed_count": int(avg_completeness * total_records),
            "failed_count": int((1 - avg_completeness) * total_records),
            "quality_score": avg_completeness,
            "details": {"completeness_scores": dict(zip(critical_fields, completeness_scores))}
        }
