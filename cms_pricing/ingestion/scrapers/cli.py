#!/usr/bin/env python3
"""
CLI interface for CMS RVU scraper operations

This module provides command-line interfaces for discovery, download, and ingestion
operations that can be used by GitHub Actions workflows.
"""

import argparse
import asyncio
import json
import sys
import uuid
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
import structlog

from .cms_rvu_scraper import CMSRVUScraper
from ..ingestors.rvu_ingestor import RVUIngestor
from ..quarantine.dis_quarantine import QuarantineManager
from ..metadata.discovery_manifest import DiscoveryManifest, DiscoveryManifestStore

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

SCRAPER_VERSION = "1.0.0"

class ScraperCLI:
    """CLI interface for scraper operations"""
    
    def __init__(self, output_dir: str, manifest_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_store = DiscoveryManifestStore(Path(manifest_dir), prefix="cms_rvu_manifest")
        
        # Initialize components
        self.scraper = CMSRVUScraper(str(self.output_dir / "scraped_data"))
        self.ingestor = RVUIngestor(str(self.output_dir / "ingested_data"))
        
        # Initialize compliance components
        self.quarantine_manager = QuarantineManager(str(self.output_dir / "quarantine"))
    
    async def discovery_mode(self, 
                           start_year: int = 2025, 
                           end_year: int = 2025,
                           latest_only: bool = True) -> Dict[str, Any]:
        """Run discovery mode - find files and detect changes"""
        logger.info("Starting discovery mode", 
                   start_year=start_year, end_year=end_year, latest_only=latest_only)
        
        try:
            # Discover files using scraper
            scraped_files = await self.scraper.scrape_rvu_files(start_year, end_year)
            
            # Filter to latest files if requested
            if latest_only and scraped_files:
                latest_files = self._filter_latest_files(scraped_files)
                scraped_files = latest_files
                logger.info("Filtered to latest files", 
                           original_count=len(scraped_files), 
                           latest_count=len(latest_files))
            
            manifest = DiscoveryManifest.create(
                source="cms_rvu",
                source_url=self.scraper.rvu_page_url,
                discovered_from=self.scraper.rvu_page_url,
                files=scraped_files,
                metadata={
                    "scraper_version": SCRAPER_VERSION,
                    "discovery_method": "scraper",
                    "total_files": len(scraped_files),
                    "robots_compliant": True,
                    "user_agent": "CMS-Pricing-Scraper/1.0.0 (contact@example.com)",
                },
                license_info={
                    "name": "CMS Open Data",
                    "url": "https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy",
                    "attribution_required": True,
                },
                start_year=start_year,
                end_year=end_year,
                latest_only=latest_only,
                default_content_type="application/zip",
            )

            validation_errors = manifest.validate()
            if validation_errors:
                logger.warning("Manifest validation warnings", errors=validation_errors)

            previous_manifest = self.manifest_store.load_latest()
            changes_detected = not manifest.has_same_files(previous_manifest)

            manifest_path = self.manifest_store.save(manifest)
            snapshot_digest = manifest.digest()
            
            result = {
                "status": "success",
                "files_discovered": len(scraped_files),
                "files_changed": changes_detected,
                "manifest_path": str(manifest_path),
                "snapshot_digest": snapshot_digest,
                "latest_only": latest_only,
                "year_range": f"{start_year}-{end_year}"
            }
            
            logger.info("Discovery completed", **result)
            return result
            
        except Exception as e:
            logger.error("Discovery failed", error=str(e))
            return {
                "status": "failed",
                "error": str(e),
                "files_discovered": 0,
                "files_changed": False
            }
    
    async def download_mode(self, 
                          start_year: int = 2025, 
                          end_year: int = 2025,
                          latest_only: bool = True) -> Dict[str, Any]:
        """Run download mode - download discovered files"""
        logger.info("Starting download mode", 
                   start_year=start_year, end_year=end_year, latest_only=latest_only)
        
        try:
            # Load manifest if it exists
            manifest = self.manifest_store.load_latest()
            if not manifest:
                logger.warning("No manifest found, running discovery first")
                discovery_result = await self.discovery_mode(start_year, end_year, latest_only)
                if discovery_result["status"] != "success":
                    return discovery_result
                manifest = self.manifest_store.load_latest()
            if not manifest:
                return {
                    "status": "failed",
                    "error": "Manifest could not be loaded after discovery",
                    "files_downloaded": 0,
                }
            
            # Download files using scraper
            download_result = await self.scraper.download_files([entry.to_dict() for entry in manifest.files])
            
            # Handle quarantined downloads per DIS §3.2
            if download_result.get("files_failed", 0) > 0:
                quarantine_summary = await self._quarantine_failed_downloads(
                    download_result, 
                    manifest.metadata.get("batch_id", "unknown"),
                    manifest.metadata.get("release_id", "unknown")
                )
                download_result["quarantine_summary"] = quarantine_summary
            
            manifest.metadata["last_download"] = {
                "completed_at": datetime.now().isoformat(),
                "files_downloaded": download_result.get("files_downloaded", 0),
                "files_failed": download_result.get("files_failed", 0),
                "total_size_bytes": download_result.get("total_size_bytes", 0),
            }
            manifest.extras["download_result"] = download_result

            manifest_path = self.manifest_store.save(manifest)
            
            result = {
                "status": "success",
                "files_downloaded": download_result.get("files_downloaded", 0),
                "download_size_bytes": download_result.get("total_size_bytes", 0),
                "manifest_path": str(manifest_path),
                "download_result": download_result
            }
            
            logger.info("Download completed", **result)
            return result
            
        except Exception as e:
            logger.error("Download failed", error=str(e))
            return {
                "status": "failed",
                "error": str(e),
                "files_downloaded": 0
            }
    
    async def ingest_mode(self, 
                         start_year: int = 2025, 
                         end_year: int = 2025,
                         latest_only: bool = True) -> Dict[str, Any]:
        """Run ingestion mode - process downloaded files"""
        logger.info("Starting ingestion mode", 
                   start_year=start_year, end_year=end_year, latest_only=latest_only)
        
        try:
            # Use the ingestor to process files
            result = await self.ingestor.ingest_from_scraped_data(
                release_id=f"scraper-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                batch_id=f"batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                start_year=start_year,
                end_year=end_year,
                latest_only=latest_only
            )
            
            logger.info("Ingestion completed", **result)
            return result
            
        except Exception as e:
            logger.error("Ingestion failed", error=str(e))
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def _filter_latest_files(self, scraped_files: List[Any]) -> List[Any]:
        """Filter files to only include the latest year"""
        if not scraped_files:
            return []
        
        # Group files by year
        files_by_year = {}
        for file_info in scraped_files:
            year = getattr(file_info, 'year', None)
            if not year:
                # Extract year from filename
                year = self._extract_year_from_filename(file_info.filename)
            
            if year:
                if year not in files_by_year:
                    files_by_year[year] = []
                files_by_year[year].append(file_info)
        
        # Get the latest year
        if not files_by_year:
            return []
        
        latest_year = max(files_by_year.keys())
        latest_files = files_by_year[latest_year]
        
        logger.info("Filtered to latest files", 
                   latest_year=latest_year, 
                   files_count=len(latest_files))
        
        return latest_files
    
    def _extract_year_from_filename(self, filename: str) -> Optional[int]:
        """Extract year from RVU filename"""
        import re
        
        pattern = r'rvu(\d{2})[a-z]\.zip'
        match = re.search(pattern, filename.lower())
        
        if match:
            year_2digit = int(match.group(1))
            if year_2digit <= 30:
                return 2000 + year_2digit
            else:
                return 1900 + year_2digit
        
        return None
    
    async def _quarantine_failed_downloads(self, 
                                         download_result: Dict[str, Any], 
                                         batch_id: str, 
                                         release_id: str) -> Dict[str, Any]:
        """Quarantine failed downloads per DIS §3.2"""
        try:
            failed_downloads = download_result.get("download_errors", [])
            if not failed_downloads:
                return {"quarantined": 0, "quarantine_file": None}
            
            # Create quarantine records for failed downloads
            quarantine_records = []
            for error in failed_downloads:
                quarantine_record = {
                    "record_id": str(uuid.uuid4()),
                    "error_code": "DOWNLOAD_FAILED",
                    "error_message": error.get("error", "Download failed"),
                    "sample_data": {
                        "filename": error.get("filename", "unknown"),
                        "download_attempted_at": datetime.now().isoformat()
                    },
                    "validation_stage": "download",
                    "quarantine_timestamp": datetime.now().isoformat(),
                    "batch_id": batch_id,
                    "release_id": release_id,
                    "remediation_guidance": "Check file URL and network connectivity"
                }
                quarantine_records.append(quarantine_record)
            
            # Save quarantine records
            quarantine_file = self.quarantine_manager.quarantine_dir / f"download_failures_{batch_id}.json"
            with open(quarantine_file, 'w') as f:
                json.dump({
                    "batch_id": batch_id,
                    "release_id": release_id,
                    "quarantine_timestamp": datetime.now().isoformat(),
                    "total_records": len(quarantine_records),
                    "records": quarantine_records
                }, f, indent=2, default=str)
            
            logger.info("Failed downloads quarantined", 
                       count=len(quarantine_records),
                       quarantine_file=str(quarantine_file))
            
            return {
                "quarantined": len(quarantine_records),
                "quarantine_file": str(quarantine_file),
                "quarantine_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error("Failed to quarantine downloads", error=str(e))
            return {"quarantined": 0, "quarantine_file": None, "error": str(e)}

async def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="CMS RVU Scraper CLI")
    parser.add_argument("--mode", required=True, 
                       choices=["discovery", "download", "ingest"],
                       help="Operation mode")
    parser.add_argument("--output-dir", required=True,
                       help="Output directory for scraped data")
    parser.add_argument("--manifest-dir", required=True,
                       help="Directory for manifest files")
    parser.add_argument("--start-year", type=int, default=2025,
                       help="Start year for data discovery")
    parser.add_argument("--end-year", type=int, default=2025,
                       help="End year for data discovery")
    parser.add_argument("--latest-only", action="store_true", default=True,
                       help="Only process latest files")
    parser.add_argument("--input-dir", 
                       help="Input directory for ingest mode")
    
    args = parser.parse_args()
    
    # Initialize CLI
    cli = ScraperCLI(args.output_dir, args.manifest_dir)
    
    try:
        if args.mode == "discovery":
            result = await cli.discovery_mode(
                start_year=args.start_year,
                end_year=args.end_year,
                latest_only=args.latest_only
            )
        elif args.mode == "download":
            result = await cli.download_mode(
                start_year=args.start_year,
                end_year=args.end_year,
                latest_only=args.latest_only
            )
        elif args.mode == "ingest":
            result = await cli.ingest_mode(
                start_year=args.start_year,
                end_year=args.end_year,
                latest_only=args.latest_only
            )
        
        # Create standard API response envelope per Global API Program PRD §1.2
        api_response = {
            "data": result,
            "meta": {
                "operation": args.mode,
                "start_year": args.start_year,
                "end_year": args.end_year,
                "latest_only": args.latest_only
            },
            "trace": {
                "correlation_id": str(uuid.uuid4()),
                "vintage": f"{args.start_year}-{args.end_year}",
                "hash": hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()[:16],
                "latency_ms": 0  # Would be calculated in real implementation
            }
        }
        
        # Print result as JSON for GitHub Actions
        print(json.dumps(api_response, indent=2, default=str))
        
        # Set GitHub Actions outputs
        if "files_changed" in result:
            print(f"::set-output name=files_changed::{result['files_changed']}")
        if "manifest_path" in result:
            print(f"::set-output name=manifest_path::{result['manifest_path']}")
        if "snapshot_digest" in result:
            print(f"::set-output name=snapshot_digest::{result['snapshot_digest']}")
        
        # Exit with appropriate code
        if result.get("status") == "success":
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.error("CLI operation failed", error=str(e))
        print(json.dumps({"status": "failed", "error": str(e)}, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
