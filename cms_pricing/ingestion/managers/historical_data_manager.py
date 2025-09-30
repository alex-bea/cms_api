"""
Historical Data Manager for CMS RVU Files
Manages downloading and organizing historical RVU data
"""

import asyncio
import json
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional
import structlog

from ..scrapers.cms_rvu_scraper import CMSRVUScraper, RVUFileInfo

logger = structlog.get_logger()


class HistoricalDataManager:
    """Manages historical RVU data downloads and organization"""
    
    def __init__(self, data_dir: str = "./data/historical_rvu"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.scraper = CMSRVUScraper(str(self.data_dir))
        
    async def download_historical_data(self, 
                                     start_year: int = 2003, 
                                     end_year: int = 2025,
                                     max_concurrent: int = 3) -> Dict[str, Any]:
        """
        Download historical RVU data for the specified year range
        
        Args:
            start_year: Starting year for historical data
            end_year: Ending year (current year)
            max_concurrent: Maximum concurrent downloads
            
        Returns:
            Summary of download results
        """
        logger.info("Starting historical data download", 
                   start_year=start_year, end_year=end_year)
        
        try:
            # Scrape files from CMS page
            files = await self.scraper.scrape_rvu_files(start_year, end_year)
            
            if not files:
                logger.warning("No RVU files found for the specified year range")
                return {
                    "status": "no_files_found",
                    "files_found": 0,
                    "downloads_completed": 0
                }
            
            # Download files
            download_results = await self.scraper.download_all_files(
                files, max_concurrent=max_concurrent
            )
            
            # Generate manifest
            manifest = self.scraper.generate_manifest(files, download_results)
            
            # Create summary
            successful_downloads = sum(1 for r in download_results 
                                     if isinstance(r, dict) and r.get("status") == "success")
            failed_downloads = len(download_results) - successful_downloads
            
            summary = {
                "status": "completed",
                "files_found": len(files),
                "downloads_completed": successful_downloads,
                "downloads_failed": failed_downloads,
                "start_year": start_year,
                "end_year": end_year,
                "manifest_path": str(self.data_dir / "rvu_files_manifest.json"),
                "data_directory": str(self.data_dir)
            }
            
            logger.info("Historical data download completed", **summary)
            
            return summary
            
        except Exception as e:
            logger.error("Historical data download failed", error=str(e))
            return {
                "status": "failed",
                "error": str(e),
                "files_found": 0,
                "downloads_completed": 0
            }
    
    async def download_recent_data(self, years: int = 3) -> Dict[str, Any]:
        """
        Download recent RVU data (last N years)
        
        Args:
            years: Number of recent years to download
            
        Returns:
            Summary of download results
        """
        current_year = datetime.now().year
        start_year = current_year - years + 1
        
        logger.info("Downloading recent RVU data", 
                   years=years, start_year=start_year, end_year=current_year)
        
        return await self.download_historical_data(start_year, current_year)
    
    def get_downloaded_files(self) -> List[Dict[str, Any]]:
        """Get list of already downloaded files from manifest"""
        manifest_path = self.data_dir / "rvu_files_manifest.json"
        
        if not manifest_path.exists():
            return []
        
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            return manifest.get("files", [])
            
        except Exception as e:
            logger.error("Failed to read manifest", error=str(e))
            return []
    
    def get_files_by_year(self, year: int) -> List[Dict[str, Any]]:
        """Get downloaded files for a specific year"""
        all_files = self.get_downloaded_files()
        return [f for f in all_files if f.get("year") == year]
    
    def get_latest_files(self) -> List[Dict[str, Any]]:
        """Get the most recent RVU files"""
        all_files = self.get_downloaded_files()
        
        if not all_files:
            return []
        
        # Find the latest year
        latest_year = max(f.get("year", 0) for f in all_files)
        
        # Return files from the latest year
        return [f for f in all_files if f.get("year") == latest_year]
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check the freshness of downloaded data"""
        files = self.get_downloaded_files()
        
        if not files:
            return {
                "status": "no_data",
                "latest_year": None,
                "days_since_latest": None
            }
        
        # Find the latest file
        latest_file = max(files, key=lambda x: x.get("year", 0))
        latest_year = latest_file.get("year")
        
        # Calculate days since latest data
        current_year = datetime.now().year
        days_since_latest = (current_year - latest_year) * 365
        
        return {
            "status": "data_available",
            "latest_year": latest_year,
            "days_since_latest": days_since_latest,
            "latest_file": latest_file.get("filename"),
            "total_files": len(files)
        }
    
    async def incremental_update(self) -> Dict[str, Any]:
        """
        Perform incremental update - download only new files since last update
        """
        logger.info("Starting incremental update")
        
        # Check current data freshness
        freshness = self.check_data_freshness()
        
        if freshness["status"] == "no_data":
            # No data exists, download recent data
            logger.info("No existing data found, downloading recent data")
            return await self.download_recent_data(years=2)
        
        # Check if we need to update
        latest_year = freshness["latest_year"]
        current_year = datetime.now().year
        
        if latest_year >= current_year:
            logger.info("Data is up to date", latest_year=latest_year)
            return {
                "status": "up_to_date",
                "latest_year": latest_year,
                "message": "Data is already up to date"
            }
        
        # Download new data
        logger.info("Downloading new data", 
                   from_year=latest_year + 1, to_year=current_year)
        
        return await self.download_historical_data(
            start_year=latest_year + 1, 
            end_year=current_year
        )


async def main():
    """Main function to demonstrate the historical data manager"""
    manager = HistoricalDataManager()
    
    # Check current data status
    freshness = manager.check_data_freshness()
    print(f"Data freshness: {freshness}")
    
    # Download recent data (last 3 years)
    result = await manager.download_recent_data(years=3)
    print(f"Download result: {result}")
    
    # List downloaded files
    files = manager.get_downloaded_files()
    print(f"Downloaded {len(files)} files")
    
    for file_info in files[:5]:  # Show first 5 files
        print(f"  {file_info['year']} {file_info['quarter']}: {file_info['filename']}")


if __name__ == "__main__":
    asyncio.run(main())
