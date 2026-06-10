"""Compatibility exports for MPFS ingestion.

Legacy scripts imported ``cms_pricing.ingestion.mpfs.MPFSIngester`` while the
current implementation lives under ``cms_pricing.ingestion.ingestors``.
"""

from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor

MPFSIngester = MPFSIngestor

__all__ = ["MPFSIngestor", "MPFSIngester"]
