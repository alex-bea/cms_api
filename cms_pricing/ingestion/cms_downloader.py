"""CMS data downloader for real fee schedule data"""

import asyncio
import httpx
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import structlog

logger = structlog.get_logger()


class CMSDownloader:
    """Downloads data files from CMS.gov"""
    
    def __init__(self, output_dir: str = "./data/cms_raw"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # CMS.gov URLs based on PRD section 4 - actual CMS data sources
        # Reference: https://www.cms.gov/medicare/payment/fee-schedules
        self.cms_base_url = "https://www.cms.gov/medicare/medicare-fee-for-service-payment/prospmedicarefeesvcpmtgen/downloads/"
        
        # Known working CMS URLs based on PRD and actual CMS structure
        self.cms_urls = {
            "geography": {
                # ZIP Code to Carrier Locality files (ZIP9 layout per PRD)
                "zip_code_carrier_locality": "https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip"
            },
            "mpfs": {
                "rvu": "mpfs-rvu-{year}.zip",  # MPFS RVU data
                "conversion_factor": "conversion-factor-{year}.zip"  # Conversion factors
            },
            "opps": {
                "apc": "opps-apc-{year}.zip",  # APC payment rates
                "hcpcs": "opps-hcpcs-{year}.zip"  # HCPCS to APC mapping
            }
        }
        
        # Additional CMS data sources in different directories
        self.other_cms_urls = {
            "geography": {
                # ZIP to locality mapping (alternative location)
                "zip_locality_alt": "zip-locality-mapping-{year}.zip"
            },
            "mpfs": {
                # MPFS data files
                "fee_schedule": "mpfs-fee-schedule-{year}.zip",
                "rvu_data": "mpfs-rvu-data-{year}.zip"
            }
        }
    
    async def check_file_changes(
        self,
        url: str,
        last_etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Check if a file has changed using ETag/Last-Modified headers"""
        
        async with httpx.AsyncClient() as client:
            try:
                # Make HEAD request to check headers
                response = await client.head(url, timeout=timeout)
                
                current_etag = response.headers.get("etag")
                current_modified = response.headers.get("last-modified")
                
                # Check if file has changed
                etag_changed = last_etag and current_etag != last_etag
                modified_changed = last_modified and current_modified != last_modified
                
                has_changes = etag_changed or modified_changed or not last_etag
                
                return {
                    "has_changes": has_changes,
                    "etag": current_etag,
                    "last_modified": current_modified,
                    "etag_changed": etag_changed,
                    "modified_changed": modified_changed,
                    "status_code": response.status_code
                }
                
            except Exception as e:
                logger.error("Error checking file changes", url=url, error=str(e))
                return {
                    "has_changes": True,  # Default to downloading if check fails
                    "error": str(e)
                }

    async def download_file(
        self,
        url: str,
        filename: str,
        max_retries: int = 3,
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Download a single file from CMS.gov"""
        
        output_path = self.output_dir / filename
        
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Downloading CMS file",
                    url=url,
                    filename=filename,
                    attempt=attempt + 1
                )
                
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        # Save the file
                        output_path.write_bytes(response.content)
                        
                        # Calculate checksum
                        file_hash = hashlib.sha256(response.content).hexdigest()
                        
                        logger.info(
                            "File downloaded successfully",
                            filename=filename,
                            size_bytes=len(response.content),
                            checksum=file_hash
                        )
                        
                        return {
                            "success": True,
                            "filename": filename,
                            "url": url,
                            "size_bytes": len(response.content),
                            "checksum": file_hash,
                            "download_time": datetime.utcnow().isoformat(),
                            "local_path": str(output_path)
                        }
                    else:
                        logger.warning(
                            "Failed to download file",
                            url=url,
                            status_code=response.status_code,
                            attempt=attempt + 1
                        )
                        
                        if attempt == max_retries - 1:
                            return {
                                "success": False,
                                "filename": filename,
                                "url": url,
                                "error": f"HTTP {response.status_code}",
                                "attempts": max_retries
                            }
                        
            except Exception as e:
                logger.error(
                    "Error downloading file",
                    url=url,
                    error=str(e),
                    attempt=attempt + 1
                )
                
                if attempt == max_retries - 1:
                    return {
                        "success": False,
                        "filename": filename,
                        "url": url,
                        "error": str(e),
                        "attempts": max_retries
                    }
                
                # Wait before retry
                await asyncio.sleep(2 ** attempt)
        
        return {
            "success": False,
            "filename": filename,
            "url": url,
            "error": "Max retries exceeded"
        }
    
    async def download_dataset(
        self,
        dataset: str,
        year: int,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Download all files for a specific dataset"""
        
        if dataset not in self.cms_urls:
            raise ValueError(f"Unknown dataset: {dataset}")
        
        results = {}
        tasks = []
        
        for file_type, url_template in self.cms_urls[dataset].items():
            # Construct full URL
            if url_template.startswith("http"):
                url = url_template
            else:
                url = self.cms_base_url + url_template.format(year=year)
            
            filename = f"{dataset}_{file_type}_{year}.zip"
            
            task = self.download_file(url, filename)
            tasks.append((file_type, task))
        
        # Download all files concurrently
        for file_type, task in tasks:
            result = await task
            results[file_type] = result
        
        # Check if all downloads were successful
        all_success = all(result["success"] for result in results.values())
        
        return {
            "dataset": dataset,
            "year": year,
            "quarter": quarter,
            "success": all_success,
            "files": results,
            "download_time": datetime.utcnow().isoformat()
        }
    
    async def download_all_current(self, year: int = None) -> Dict[str, Any]:
        """Download all current CMS datasets"""
        
        if year is None:
            year = datetime.now().year
        
        logger.info("Starting CMS data download", year=year)
        
        results = {}
        tasks = []
        
        # Download all datasets concurrently
        for dataset in self.cms_urls.keys():
            task = self.download_dataset(dataset, year)
            tasks.append((dataset, task))
        
        # Wait for all downloads to complete
        for dataset, task in tasks:
            result = await task
            results[dataset] = result
        
        # Check overall success
        all_success = all(result["success"] for result in results.values())
        
        logger.info(
            "CMS data download completed",
            year=year,
            success=all_success,
            datasets=len(results)
        )
        
        return {
            "year": year,
            "success": all_success,
            "datasets": results,
            "download_time": datetime.utcnow().isoformat()
        }
