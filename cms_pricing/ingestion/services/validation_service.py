"""
Validation Service Adapter
--------------------------

Phase 2 Refactoring Context:
    - Step 4: Validation rules extraction
      • Plan: artifacts/phase2_step4_detailed_plan.md
      • Verification: artifacts/phase2_step4_verification_report.md

Thin wrapper around ValidationEngine for consistent initialization
and usage patterns across ingestors.
"""

from typing import Any
import structlog

from ..datasets.spec import DatasetSpec

logger = structlog.get_logger()


class ValidationService:
    """
    Adapter for the validation engine that provides helper methods for
    registering dataset-level business rules and future shared behaviour.
    """

    def __init__(self, validation_engine: Any):
        self._engine = validation_engine

    @property
    def engine(self) -> Any:
        """Expose the underlying validation engine."""
        return self._engine

    def validate_dataframe(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate dataframe validation to the underlying engine."""
        return self._engine.validate_dataframe(*args, **kwargs)

    # Phase 2 Step 4: Validation rules extraction
    # See: artifacts/phase2_step4_detailed_plan.md
    def register_dataset_business_rules(self, dataset_spec: DatasetSpec) -> None:
        """
        Register all business rules defined on a DatasetSpec.

        Args:
            dataset_spec: DatasetSpec containing business_rules callables
        """
        if not dataset_spec.business_rules:
            logger.debug(
                "No business rules to register",
                dataset=dataset_spec.dataset_id
            )
            return

        registered_count = 0
        for rule_func in dataset_spec.business_rules:
            try:
                self._engine.register_business_rule(dataset_spec.dataset_id, rule_func)
                logger.debug(
                    "Business rule registered",
                    dataset=dataset_spec.dataset_id,
                    rule=getattr(rule_func, "__name__", str(rule_func))
                )
                registered_count += 1
            except Exception as exc:
                logger.error(
                    "Failed to register business rule",
                    dataset=dataset_spec.dataset_id,
                    rule=getattr(rule_func, "__name__", str(rule_func)),
                    error=str(exc)
                )
                raise
        
        logger.info(
            "Dataset business rules registered",
            dataset=dataset_spec.dataset_id,
            count=registered_count
        )
