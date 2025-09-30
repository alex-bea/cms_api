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
from ..contracts.schema_registry import schema_registry
from ..quarantine.dis_quarantine import QuarantineManager
from ..enrichers.dis_reference_data_integration import DISReferenceDataEnricher

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

class ScraperCLI:
    """CLI interface for scraper operations"""
    
    def __init__(self, output_dir: str, manifest_dir: str):
        self.output_dir = Path(output_dir)
        self.manifest_dir = Path(manifest_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.scraper = CMSRVUScraper(str(self.output_dir / "scraped_data"))
        self.ingestor = RVUIngestor(str(self.output_dir / "ingested_data"))
        
        # Initialize compliance components
        self.quarantine_manager = QuarantineManager(str(self.output_dir / "quarantine"))
        self.schema_registry = schema_registry
    
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
            
            # Generate manifest
            manifest = self._generate_manifest(scraped_files, start_year, end_year, latest_only)
            
            # Validate manifest against schema contract
            schema_validation = self._validate_manifest_schema(manifest)
            if not schema_validation["valid"]:
                logger.warning("Manifest schema validation failed", errors=schema_validation["errors"])
            
            # Check for changes
            changes_detected = await self._check_for_changes(manifest)
            
            # Save manifest
            manifest_path = self._save_manifest(manifest)
            
            # Calculate snapshot digest
            snapshot_digest = self._calculate_snapshot_digest(manifest)
            
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
            manifest = self._load_latest_manifest()
            if not manifest:
                logger.warning("No manifest found, running discovery first")
                discovery_result = await self.discovery_mode(start_year, end_year, latest_only)
                if discovery_result["status"] != "success":
                    return discovery_result
                manifest = self._load_latest_manifest()
            
            # Download files using scraper
            download_result = await self.scraper.download_files(manifest.get("files", []))
            
            # Handle quarantined downloads per DIS §3.2
            if download_result.get("files_failed", 0) > 0:
                quarantine_summary = await self._quarantine_failed_downloads(
                    download_result, 
                    manifest.get("batch_id", "unknown"),
                    manifest.get("release_id", "unknown")
                )
                download_result["quarantine_summary"] = quarantine_summary
            
            # Update manifest with download results
            manifest["download_completed_at"] = datetime.now().isoformat()
            manifest["download_result"] = download_result
            
            # Save updated manifest
            manifest_path = self._save_manifest(manifest)
            
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
    
    def _generate_manifest(self, 
                          files: List[Any], 
                          start_year: int, 
                          end_year: int,
                          latest_only: bool) -> Dict[str, Any]:
        """Generate manifest for discovered files per DIS §3.2"""
        manifest = {
            "source": "cms_rvu",
            "discovered_at": datetime.now().isoformat(),
            "fetched_at": datetime.now().isoformat(),
            "start_year": start_year,
            "end_year": end_year,
            "latest_only": latest_only,
            "source_url": "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files",
            "discovered_from": "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files",
            "license": {
                "name": "CMS Open Data",
                "url": "https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy",
                "attribution_required": True
            },
            "files": [],
            "metadata": {
                "scraper_version": "1.0.0",
                "discovery_method": "scraper",
                "total_files": len(files),
                "robots_compliant": True,
                "user_agent": "CMS-Pricing-Scraper/1.0.0 (contact@example.com)"
            }
        }
        
        for file_info in files:
            file_entry = {
                "path": file_info.filename,
                "filename": file_info.filename,
                "url": file_info.url,
                "sha256": getattr(file_info, 'checksum', None),
                "size_bytes": getattr(file_info, 'size_bytes', None),
                "content_type": "application/zip",
                "year": getattr(file_info, 'year', None),
                "quarter": getattr(file_info, 'quarter', None),
                "file_type": getattr(file_info, 'file_type', 'zip'),
                "last_modified": getattr(file_info, 'last_modified', None)
            }
            manifest["files"].append(file_entry)
        
        return manifest
    
    async def _check_for_changes(self, manifest: Dict[str, Any]) -> bool:
        """Check if files have changed since last run"""
        try:
            # Load previous manifest
            previous_manifest = self._load_latest_manifest()
            if not previous_manifest:
                logger.info("No previous manifest found, treating as new")
                return True
            
            # Compare file lists
            current_files = {f["url"]: f for f in manifest["files"]}
            previous_files = {f["url"]: f for f in previous_manifest.get("files", [])}
            
            # Check for new or changed files
            for url, file_info in current_files.items():
                if url not in previous_files:
                    logger.info("New file detected", url=url)
                    return True
                
                # Check if file has changed (size, checksum, last_modified)
                prev_file = previous_files[url]
                if (file_info.get("size_bytes") != prev_file.get("size_bytes") or
                    file_info.get("checksum") != prev_file.get("checksum") or
                    file_info.get("last_modified") != prev_file.get("last_modified")):
                    logger.info("File changed detected", url=url)
                    return True
            
            # Check for removed files
            for url in previous_files:
                if url not in current_files:
                    logger.info("File removed", url=url)
                    return True
            
            logger.info("No changes detected")
            return False
            
        except Exception as e:
            logger.warning("Error checking for changes, treating as changed", error=str(e))
            return True
    
    def _save_manifest(self, manifest: Dict[str, Any]) -> Path:
        """Save manifest to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        manifest_path = self.manifest_dir / f"cms_rvu_manifest_{timestamp}.json"
        
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
        
        logger.info("Manifest saved", path=str(manifest_path))
        return manifest_path
    
    def _load_latest_manifest(self) -> Optional[Dict[str, Any]]:
        """Load the most recent manifest"""
        try:
            manifest_files = list(self.manifest_dir.glob("cms_rvu_manifest_*.json"))
            if not manifest_files:
                return None
            
            # Sort by modification time and get the latest
            latest_manifest = max(manifest_files, key=lambda p: p.stat().st_mtime)
            
            with open(latest_manifest, 'r') as f:
                manifest = json.load(f)
            
            logger.info("Loaded latest manifest", path=str(latest_manifest))
            return manifest
            
        except Exception as e:
            logger.warning("Error loading manifest", error=str(e))
            return None
    
    def _calculate_snapshot_digest(self, manifest: Dict[str, Any]) -> str:
        """Calculate snapshot digest for idempotency"""
        import hashlib
        
        # Create a deterministic string from manifest
        manifest_str = json.dumps(manifest, sort_keys=True, default=str)
        digest = hashlib.sha256(manifest_str.encode()).hexdigest()
        
        return f"sha256:{digest[:16]}"
    
    def _validate_manifest_schema(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Validate manifest against schema contract per DIS §3.1"""
        try:
            # Validate manifest structure
            required_fields = ["source", "discovered_at", "files", "metadata"]
            errors = []
            
            for field in required_fields:
                if field not in manifest:
                    errors.append(f"Required field '{field}' is missing from manifest")
            
            # Validate file entries
            if "files" in manifest:
                for i, file_entry in enumerate(manifest["files"]):
                    file_required_fields = ["filename", "url", "sha256", "size_bytes", "content_type"]
                    for field in file_required_fields:
                        if field not in file_entry:
                            errors.append(f"Required field '{field}' is missing from file {i}")
            
            # Validate against schema registry if available
            schema_validation = {"valid": True, "errors": [], "warnings": []}
            if hasattr(self.schema_registry, 'validate_data'):
                try:
                    schema_validation = self.schema_registry.validate_data("cms_rvu_manifest", manifest)
                except Exception as e:
                    schema_validation["warnings"].append(f"Schema validation failed: {str(e)}")
            
            return {
                "valid": len(errors) == 0 and schema_validation["valid"],
                "errors": errors + schema_validation["errors"],
                "warnings": schema_validation["warnings"]
            }
            
        except Exception as e:
            logger.error("Manifest schema validation failed", error=str(e))
            return {
                "valid": False,
                "errors": [f"Schema validation error: {str(e)}"],
                "warnings": []
            }
    
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
