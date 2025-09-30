"""
ZIP9 Overrides Validator following Data Ingestion Standard (DIS)

Validates ZIP9 override data according to business rules and quality gates.
"""

import pandas as pd
import re
from datetime import datetime, date
from typing import Dict, List, Any, Tuple
import structlog

from cms_pricing.ingestion.contracts.ingestor_spec import ValidationSeverity
from cms_pricing.ingestion.validators.validation_engine import ValidationEngine, ValidationRule, ValidationResult

logger = structlog.get_logger()


class SimpleValidationRule:
    """Simple validation rule for ZIP9 data"""
    def __init__(self, name: str, description: str, validator_func, severity: str = "critical"):
        self.name = name
        self.description = description
        self.validator_func = validator_func
        self.severity = severity


class ZIP9OverridesValidator:
    """Validator for ZIP9 overrides data following DIS standards"""
    
    def __init__(self):
        self.validation_engine = ValidationEngine()
        self.validation_rules = []
        self._setup_validation_rules()
    
    def _setup_validation_rules(self):
        """Setup validation rules for ZIP9 overrides"""
        
        # ZIP9 format validation
        self.validation_rules.append(SimpleValidationRule(
            name="zip9_format_validation",
            description="ZIP9 codes must be exactly 9 digits",
            validator_func=self._validate_zip9_format,
            severity="critical"
        ))
        
        # ZIP9 range validation
        self.validation_rules.append(SimpleValidationRule(
            name="zip9_range_validation",
            description="ZIP9 low must be less than or equal to ZIP9 high",
            validator_func=self._validate_zip9_range,
            severity="critical"
        ))
        
        # State code validation
        self.validation_rules.append(SimpleValidationRule(
            name="state_code_validation",
            description="State code must be valid 2-letter US state code",
            validator_func=self._validate_state_codes,
            severity="critical"
        ))
        
        # Locality code validation
        self.validation_rules.append(SimpleValidationRule(
            name="locality_code_validation",
            description="Locality code must be 2 digits",
            validator_func=self._validate_locality_codes,
            severity="critical"
        ))
        
        # Rural flag validation
        self.validation_rules.append(SimpleValidationRule(
            name="rural_flag_validation",
            description="Rural flag must be A, B, or null",
            validator_func=self._validate_rural_flags,
            severity="warning"
        ))
        
        # Effective dates validation
        self.validation_rules.append(SimpleValidationRule(
            name="effective_dates_validation",
            description="Effective dates must be valid",
            validator_func=self._validate_effective_dates,
            severity="warning"
        ))
        
        # ZIP5 prefix consistency validation
        self.validation_rules.append(SimpleValidationRule(
            name="zip5_prefix_consistency",
            description="ZIP9 codes must have consistent ZIP5 prefix",
            validator_func=self._validate_zip5_prefix_consistency,
            severity="warning"
        ))
        
        # Data completeness validation
        self.validation_rules.append(SimpleValidationRule(
            name="data_completeness",
            description="Required fields must not be null",
            validator_func=self._validate_data_completeness,
            severity="critical"
        ))
    
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate ZIP9 overrides data"""
        logger.info("Starting ZIP9 overrides validation", record_count=len(df))
        
        # Run validation rules
        validation_results = {
            "rules_passed": 0,
            "rules_failed": 0,
            "details": []
        }
        
        for rule in self.validation_rules:
            try:
                passed, errors = rule.validator_func(df)
                if passed:
                    validation_results["rules_passed"] += 1
                    validation_results["details"].append({
                        "rule": rule.name,
                        "status": "passed",
                        "message": rule.description
                    })
                else:
                    validation_results["rules_failed"] += 1
                    validation_results["details"].append({
                        "rule": rule.name,
                        "status": "failed",
                        "message": rule.description,
                        "errors": errors
                    })
            except Exception as e:
                validation_results["rules_failed"] += 1
                validation_results["details"].append({
                    "rule": rule.name,
                    "status": "error",
                    "message": f"Validation error: {str(e)}"
                })
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(validation_results)
        
        # Add additional validation results
        validation_results.update({
            "quality_score": quality_score,
            "record_count": len(df),
            "validation_timestamp": datetime.now().isoformat()
        })
        
        logger.info(
            "ZIP9 overrides validation completed",
            quality_score=quality_score,
            rules_passed=validation_results.get("rules_passed", 0),
            rules_failed=validation_results.get("rules_failed", 0)
        )
        
        return validation_results
    
    def _validate_zip9_format(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate ZIP9 format (9 digits)"""
        errors = []
        
        # Check zip9_low format
        if 'zip9_low' in df.columns:
            invalid_low = df[~df['zip9_low'].astype(str).str.match(r'^\d{9}$')]
            for idx, row in invalid_low.iterrows():
                errors.append({
                    "row": idx,
                    "field": "zip9_low",
                    "value": row['zip9_low'],
                    "error": "ZIP9 low must be exactly 9 digits"
                })
        
        # Check zip9_high format
        if 'zip9_high' in df.columns:
            invalid_high = df[~df['zip9_high'].astype(str).str.match(r'^\d{9}$')]
            for idx, row in invalid_high.iterrows():
                errors.append({
                    "row": idx,
                    "field": "zip9_high",
                    "value": row['zip9_high'],
                    "error": "ZIP9 high must be exactly 9 digits"
                })
        
        return len(errors) == 0, errors
    
    def _validate_zip9_range(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate ZIP9 range (low <= high)"""
        errors = []
        
        if 'zip9_low' in df.columns and 'zip9_high' in df.columns:
            invalid_ranges = df[df['zip9_low'] > df['zip9_high']]
            for idx, row in invalid_ranges.iterrows():
                errors.append({
                    "row": idx,
                    "field": "zip9_range",
                    "value": f"{row['zip9_low']}-{row['zip9_high']}",
                    "error": "ZIP9 low must be less than or equal to ZIP9 high"
                })
        
        return len(errors) == 0, errors
    
    def _validate_state_codes(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate state codes"""
        errors = []
        
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
        }
        
        if 'state' in df.columns:
            invalid_states = df[~df['state'].isin(valid_states)]
            for idx, row in invalid_states.iterrows():
                errors.append({
                    "row": idx,
                    "field": "state",
                    "value": row['state'],
                    "error": f"Invalid state code: {row['state']}"
                })
        
        return len(errors) == 0, errors
    
    def _validate_locality_codes(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate locality codes (2 digits)"""
        errors = []
        
        if 'locality' in df.columns:
            invalid_locality = df[~df['locality'].astype(str).str.match(r'^\d{2}$')]
            for idx, row in invalid_locality.iterrows():
                errors.append({
                    "row": idx,
                    "field": "locality",
                    "value": row['locality'],
                    "error": "Locality code must be 2 digits"
                })
        
        return len(errors) == 0, errors
    
    def _validate_rural_flags(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate rural flags (A, B, or null)"""
        errors = []
        
        if 'rural_flag' in df.columns:
            invalid_flags = df[df['rural_flag'].notna() & ~df['rural_flag'].isin(['A', 'B'])]
            for idx, row in invalid_flags.iterrows():
                errors.append({
                    "row": idx,
                    "field": "rural_flag",
                    "value": row['rural_flag'],
                    "error": "Rural flag must be A, B, or null"
                })
        
        return len(errors) == 0, errors
    
    def _validate_effective_dates(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate effective dates"""
        errors = []
        
        if 'effective_from' in df.columns and 'effective_to' in df.columns:
            # Check for invalid date ranges
            invalid_ranges = df[
                df['effective_from'].notna() & 
                df['effective_to'].notna() & 
                (df['effective_from'] > df['effective_to'])
            ]
            for idx, row in invalid_ranges.iterrows():
                errors.append({
                    "row": idx,
                    "field": "effective_dates",
                    "value": f"{row['effective_from']} to {row['effective_to']}",
                    "error": "Effective from must be less than or equal to effective to"
                })
        
        return len(errors) == 0, errors
    
    def _validate_zip5_prefix_consistency(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate ZIP5 prefix consistency"""
        errors = []
        
        if 'zip9_low' in df.columns and 'zip9_high' in df.columns:
            # Check that ZIP5 prefixes match
            df['zip5_low'] = df['zip9_low'].astype(str).str[:5]
            df['zip5_high'] = df['zip9_high'].astype(str).str[:5]
            
            inconsistent_prefixes = df[df['zip5_low'] != df['zip5_high']]
            for idx, row in inconsistent_prefixes.iterrows():
                errors.append({
                    "row": idx,
                    "field": "zip5_prefix",
                    "value": f"{row['zip5_low']} vs {row['zip5_high']}",
                    "error": "ZIP9 codes must have consistent ZIP5 prefix"
                })
        
        return len(errors) == 0, errors
    
    def _validate_data_completeness(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validate data completeness"""
        errors = []
        
        required_fields = ['zip9_low', 'zip9_high', 'state', 'locality', 'effective_from', 'vintage']
        
        for field in required_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    errors.append({
                        "field": field,
                        "error": f"Required field {field} has {null_count} null values"
                    })
        
        return len(errors) == 0, errors
    
    def _calculate_quality_score(self, validation_results: Dict[str, Any]) -> float:
        """Calculate quality score based on validation results"""
        total_rules = validation_results.get("rules_passed", 0) + validation_results.get("rules_failed", 0)
        if total_rules == 0:
            return 1.0
        
        passed_rules = validation_results.get("rules_passed", 0)
        return passed_rules / total_rules
    
    def validate_zip9_zip5_consistency(self, zip9_df: pd.DataFrame, zip5_df: pd.DataFrame) -> Dict[str, Any]:
        """Validate consistency between ZIP9 and ZIP5 data"""
        logger.info("Validating ZIP9-ZIP5 consistency")
        
        consistency_results = {
            "consistent_count": 0,
            "inconsistent_count": 0,
            "missing_zip5_count": 0,
            "conflicting_mappings": []
        }
        
        # Extract ZIP5 prefixes from ZIP9 data
        zip9_df = zip9_df.copy()
        zip9_df['zip5_prefix'] = zip9_df['zip9_low'].astype(str).str[:5]
        
        # Check for missing ZIP5 references
        missing_zip5 = zip9_df[~zip9_df['zip5_prefix'].isin(zip5_df['zip5'])]
        consistency_results["missing_zip5_count"] = len(missing_zip5)
        
        # Check for conflicting mappings
        merged_df = zip9_df.merge(zip5_df, left_on='zip5_prefix', right_on='zip5', how='inner')
        conflicting = merged_df[
            (merged_df['state_x'] != merged_df['state_y']) |
            (merged_df['locality_x'] != merged_df['locality_y'])
        ]
        
        for _, row in conflicting.iterrows():
            consistency_results["conflicting_mappings"].append({
                "zip9_low": row['zip9_low'],
                "zip9_high": row['zip9_high'],
                "zip5_prefix": row['zip5_prefix'],
                "zip9_state": row['state_x'],
                "zip5_state": row['state_y'],
                "zip9_locality": row['locality_x'],
                "zip5_locality": row['locality_y'],
                "conflict_type": "state_mismatch" if row['state_x'] != row['state_y'] else "locality_mismatch"
            })
        
        consistency_results["inconsistent_count"] = len(conflicting)
        consistency_results["consistent_count"] = len(merged_df) - len(conflicting)
        
        logger.info(
            "ZIP9-ZIP5 consistency validation completed",
            consistent_count=consistency_results["consistent_count"],
            inconsistent_count=consistency_results["inconsistent_count"],
            missing_zip5_count=consistency_results["missing_zip5_count"]
        )
        
        return consistency_results
