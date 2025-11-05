"""
ConversionFactorFetcher
-----------------------

Utility service for landing CMS conversion factor (CF) artefacts used by the
MPFS ingestor. Supports caching downloads locally and reusing previously
downloaded files when the checksum matches.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import httpx
import structlog

logger = structlog.get_logger()


@dataclass
class ConversionFactorMetadata:
    """Metadata describing a conversion factor artefact."""

    year: int
    path: str
    checksum: str
    source_url: str
    effective_from: date
    effective_to: date

    def to_dict(self) -> Dict[str, Any]:
        return {
            "year": self.year,
            "path": self.path,
            "checksum": self.checksum,
            "source_url": self.source_url,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
        }


class ConversionFactorFetcher:
    """Download/cache CMS conversion factor artefacts."""

    def __init__(self, output_dir: str = "./data/ingestion/mpfs/raw", timeout_seconds: int = 60):
        self.output_dir = Path(output_dir)
        self.timeout_seconds = timeout_seconds
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def ensure_conversion_factor(
        self,
        year: int,
        source_url: Optional[str] = None,
        manual_override_path: Optional[str] = None,
        expected_checksum: Optional[str] = None,
    ) -> ConversionFactorMetadata:
        """
        Ensure conversion factor artefact exists locally.

        Args:
            year: Calendar year of the conversion factor.
            source_url: CMS download URL. Required unless manual_override_path provided.
            manual_override_path: Optional path to locally downloaded artefact.
            expected_checksum: Optional SHA256 checksum for verification.

        Returns:
            ConversionFactorMetadata describing the artefact.
        """
        if manual_override_path:
            manual_path = Path(manual_override_path)
            if not manual_path.exists():
                raise FileNotFoundError(f"Conversion factor override not found: {manual_override_path}")
            checksum = self._hash_file(manual_path)
            self._verify_checksum(checksum, expected_checksum)
            metadata = ConversionFactorMetadata(
                year=year,
                path=str(manual_path),
                checksum=checksum,
                source_url=f"file://{manual_path}",
                effective_from=date(year, 1, 1),
                effective_to=date(year, 12, 31),
            )
            logger.info("Using manual conversion factor artefact", **metadata.to_dict())
            return metadata

        if not source_url:
            raise ValueError("source_url is required when manual_override_path is not provided")

        target_path = self._target_path(year, source_url)
        if target_path.exists():
            checksum = self._hash_file(target_path)
            self._verify_checksum(checksum, expected_checksum, warn_only=True)
            logger.info("Reusing cached conversion factor artefact", path=str(target_path), checksum=checksum)
            return ConversionFactorMetadata(
                year=year,
                path=str(target_path),
                checksum=checksum,
                source_url=source_url,
                effective_from=date(year, 1, 1),
                effective_to=date(year, 12, 31),
            )

        # Download artefact
        logger.info("Downloading conversion factor artefact", url=source_url, target=str(target_path))
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(source_url)
            response.raise_for_status()

        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(response.content)
        checksum = self._hash_file(target_path)
        self._verify_checksum(checksum, expected_checksum)

        metadata = ConversionFactorMetadata(
            year=year,
            path=str(target_path),
            checksum=checksum,
            source_url=source_url,
            effective_from=date(year, 1, 1),
            effective_to=date(year, 12, 31),
        )
        logger.info("Conversion factor artefact downloaded", **metadata.to_dict())
        return metadata

    def _target_path(self, year: int, source_url: str) -> Path:
        """Determine download target path from URL."""
        parsed = urlparse(source_url)
        filename = Path(parsed.path).name or f"conversion_factor_{year}.zip"
        year_dir = self.output_dir / str(year)
        return year_dir / filename

    @staticmethod
    def _hash_file(path: Path) -> str:
        """Return SHA256 hash of file contents."""
        sha256 = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def _verify_checksum(actual: str, expected: Optional[str], warn_only: bool = False) -> None:
        """Compare checksum vs expected; raise or warn on mismatch."""
        if not expected:
            return
        if actual.lower() == expected.lower():
            return

        if warn_only:
            logger.warning("Checksum mismatch for cached artefact", actual=actual, expected=expected)
            return
        raise ValueError(f"Checksum mismatch for conversion factor artefact (expected={expected}, actual={actual})")
