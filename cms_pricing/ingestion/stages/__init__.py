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

from .enrich import execute_enrich, EnrichConfig

__all__ = ["execute_enrich", "EnrichConfig"]

