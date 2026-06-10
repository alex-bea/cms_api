"""Compatibility exports for OPPS ingestion."""

from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor

OPPSIngester = OPPSIngestor

__all__ = ["OPPSIngestor", "OPPSIngester"]
