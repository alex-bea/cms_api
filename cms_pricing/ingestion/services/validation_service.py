"""
Validation Service Adapter

Thin wrapper around ValidationEngine for consistent initialization
and usage patterns across ingestors.
"""

from typing import Any, Callable, Dict, Optional
import structlog

logger = structlog.get_logger()


class ValidationService:
    """
    Thin adapter for validation engine.
    
    Provides consistent initialization and helper methods for validation
    operations across all DIS ingestors.
    """
    
    @staticmethod
    def create() -> Any:
        """
        Create validation engine with consistent configuration.
        
        Returns:
            ValidationEngine instance
        """
        from ..validators.validation_engine import ValidationEngine
        return ValidationEngine()
    
    @staticmethod
    def register_business_rule(
        engine: Any,
        dataset_name: str,
        rule_name: str,
        rule_func: Callable
    ):
        """
        Register a business rule for a dataset.
        
        Args:
            engine: ValidationEngine instance
            dataset_name: Name of the dataset
            rule_name: Name of the validation rule
            rule_func: Validation function that returns ValidationResult
        """
        try:
            engine.register_business_rule(dataset_name, rule_func)
            logger.debug("Business rule registered", dataset=dataset_name, rule=rule_name)
        except Exception as e:
            logger.error("Failed to register business rule", 
                        dataset=dataset_name, 
                        rule=rule_name, 
                        error=str(e))
            raise

