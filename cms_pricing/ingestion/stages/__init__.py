"""
Stage modules for modular ingestor architecture.

Each stage module implements a single DIS pipeline stage:
- Land: Download and store raw files (DIS §3.2)
- Validate: Structural and domain validation (DIS §3.3)
- Normalize: Adapt and canonicalize data (DIS §3.4)
- Enrich: Join with reference data (DIS §3.5)
- Publish: Store in curated format (DIS §3.6)

Stage modules are reusable across ingestors and can be tested independently.
"""

from .land import execute_land, LandConfig, infer_file_type_from_name, is_guidance_file
from .validate import execute_validate, ValidateConfig
from .normalize import execute_normalize, NormalizeConfig
from .enrich import execute_enrich, EnrichConfig
from .publish import execute_publish, PublishConfig

__all__ = [
    "execute_land", "LandConfig", "infer_file_type_from_name", "is_guidance_file",
    "execute_validate", "ValidateConfig",
    "execute_normalize", "NormalizeConfig",
    "execute_enrich", "EnrichConfig",
    "execute_publish", "PublishConfig",
]

