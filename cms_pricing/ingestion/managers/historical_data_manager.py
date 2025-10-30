"""
Historical Data Manager for CMS RVU Files
=========================================

Coordinates discovery (and optional downloading) of CMS RVU artifacts using the
CMSRVUScraper and persists discovery manifests for reuse.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

from ..metadata.discovery_manifest import DiscoveryManifestStore
from ..scrapers.cms_rvu_scraper import CMSRVUScraper, RVUFileInfo

logger = structlog.get_logger()


class HistoricalDataManager:
    """Manages discovery and archival of CMS RVU artifacts."""

    def __init__(self, data_dir: str = "./data/historical_rvu") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.scraper = CMSRVUScraper(str(self.data_dir))
        self.manifest_store = DiscoveryManifestStore(
            self.data_dir / "manifests", prefix="cms_rvu_manifest"
        )

    async def download_historical_data(
        self,
        start_year: int = 2003,
        end_year: int = 2025,
        max_concurrent: int = 4,
        download: bool = False,
    ) -> Dict[str, Any]:
        """
        Discover RVU files for the provided year range and optionally download them.

        Args:
            start_year: First year (inclusive) to consider.
            end_year: Last year (inclusive) to consider.
            max_concurrent: Maximum concurrent download requests.
            download: When True, download artifacts after discovery.
        """
        logger.info(
            "rvu.history.discover.start",
            start_year=start_year,
            end_year=end_year,
            download=download,
        )

        files = await self.scraper.scrape_rvu_files(start_year, end_year)

        if not files:
            logger.warning(
                "rvu.history.discover.empty", start_year=start_year, end_year=end_year
            )
            return {
                "status": "no_files_found",
                "files_discovered": 0,
                "downloads_completed": 0,
                "downloads_failed": 0,
                "manifest_path": None,
            }

        downloads_completed = 0
        downloads_failed = 0
        download_results: List[Dict[str, Any]] = []

        if download:
            download_results = await self.scraper.download_all_files(
                files, max_concurrent=max_concurrent
            )
            downloads_completed = sum(
                1 for result in download_results if result.get("status") == "success"
            )
            downloads_failed = sum(
                1 for result in download_results if result.get("status") != "success"
            )

        manifest_path = (
            str(self.scraper.last_manifest_path)
            if self.scraper.last_manifest_path
            else None
        )

        summary = {
            "status": "completed",
            "files_discovered": len(files),
            "downloads_completed": downloads_completed,
            "downloads_failed": downloads_failed,
            "start_year": start_year,
            "end_year": end_year,
            "manifest_path": manifest_path,
            "data_directory": str(self.data_dir),
        }

        logger.info("rvu.history.discover.complete", **summary)
        return summary

    async def download_recent_data(
        self,
        years: int = 3,
        download: bool = False,
    ) -> Dict[str, Any]:
        """
        Discover (and optionally download) RVU data for the most recent N years.
        """
        current_year = datetime.now().year
        start_year = max(2003, current_year - years + 1)

        return await self.download_historical_data(
            start_year=start_year,
            end_year=current_year,
            download=download,
        )

    def get_discovered_files(self) -> List[Dict[str, Any]]:
        """
        Return the most recent discovery manifest entries as dictionaries.
        """
        manifest = self.manifest_store.load_latest()
        if not manifest:
            return []

        return [entry.to_dict() for entry in manifest.files]

    def get_files_by_year(self, year: int) -> List[Dict[str, Any]]:
        """Filter discovered files by year."""
        return [file for file in self.get_discovered_files() if file.get("year") == year]

    def get_latest_files(self) -> List[Dict[str, Any]]:
        """Return discovered files for the most recent year."""
        files = self.get_discovered_files()
        if not files:
            return []
        latest_year = max(file.get("year", 0) for file in files)
        return [file for file in files if file.get("year") == latest_year]

    def check_data_freshness(self) -> Dict[str, Any]:
        """
        Evaluate discovery freshness based on the most recent manifest.
        """
        files = self.get_discovered_files()
        if not files:
            return {"status": "no_data", "latest_year": None, "days_since_latest": None}

        latest_year = max(file.get("year", 0) for file in files)
        current_year = datetime.now().year
        days_since_latest = (current_year - latest_year) * 365

        return {
            "status": "data_available",
            "latest_year": latest_year,
            "days_since_latest": days_since_latest,
            "total_files": len(files),
        }

    async def incremental_update(self, download: bool = False) -> Dict[str, Any]:
        """
        Perform an incremental discovery, fetching files for years not yet captured.
        """
        freshness = self.check_data_freshness()
        current_year = datetime.now().year

        if freshness["status"] == "no_data":
            logger.info("rvu.history.incremental.seed")
            return await self.download_recent_data(years=2, download=download)

        latest_year = freshness["latest_year"]
        if latest_year and latest_year >= current_year:
            logger.info("rvu.history.incremental.up_to_date", latest_year=latest_year)
            return {
                "status": "up_to_date",
                "latest_year": latest_year,
                "message": "Discovery is already current",
            }

        logger.info(
            "rvu.history.incremental.fetch",
            from_year=(latest_year or current_year) + 1 if latest_year else current_year,
            to_year=current_year,
            download=download,
        )

        return await self.download_historical_data(
            start_year=(latest_year or current_year) + 1 if latest_year else current_year,
            end_year=current_year,
            download=download,
        )


async def main() -> None:  # pragma: no cover - convenience CLI
    manager = HistoricalDataManager()
    summary = await manager.download_recent_data(years=2, download=False)
    print("Discovery summary:", summary)
    files = manager.get_latest_files()
    print(f"Latest discovery captured {len(files)} files")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
