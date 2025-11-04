"""
Shared Services Package for DIS Ingestors

This package centralizes service initialization (validation, observability, quarantine,
reference data, schema registry) to eliminate duplication across ingestors.

Services are lazy-loaded by default to avoid unnecessary initialization overhead.
"""

from .service_config import ServiceConfig
from .service_factory import ServiceFactory
from .schema_service import SchemaService
from .validation_service import ValidationService

__all__ = [
    "ServiceConfig",
    "ServiceFactory",
    "SchemaService",
    "ValidationService",
]
