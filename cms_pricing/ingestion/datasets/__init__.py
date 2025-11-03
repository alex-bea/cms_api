"""
Dataset specifications for modular ingestor architecture.

This package provides DatasetSpec interface for plug-in dataset behavior,
enabling ingestors to compose dataset-specific logic instead of using switch statements.
"""

from .spec import DatasetSpec, EnrichmentRule
from .rvu_spec import RVU_DATASETS, get_rvu_dataset_spec, route_file_to_rvu_spec

__all__ = [
    "DatasetSpec",
    "EnrichmentRule",
    "RVU_DATASETS",
    "get_rvu_dataset_spec",
    "route_file_to_rvu_spec",
]

