"""
Validation types and data structures
"""

from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum


class ValidationLevel(Enum):
    """Validation severity levels"""
    ERROR = "error"
    WARN = "warn"
    INFO = "info"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    level: ValidationLevel
    rule_name: str
    message: str
    record_id: Optional[str] = None
    field_name: Optional[str] = None
    actual_value: Any = None
    expected_value: Any = None



