"""
CMS MPFS Scraper - Orchestrator for MPFS-specific file discovery

This scraper composes with the existing RVU scraper to discover MPFS-specific files
(conversion factors, abstracts, national payment files) while reusing RVU file discovery
for shared artifacts (PPRRVU, GPCI, LocalityCounty).

Following Scraper Standard PRD v1.0 and Data Ingestion Standard PRD v1.0
"""

import asyncio
import httpx
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import structlog
from bs4 import BeautifulSoup

from .cms_rvu_scraper import CMSRVUScraper, RVUFileInfo
from ..metadata.discovery_manifest import DiscoveryManifest, DiscoveryManifestStore

logger = structlog.get_logger()


SCRAPER_VERSION = "1.0.0"


class CMSMPFSScraper:
    """
    MPFS-specific scraper that orchestrates discovery of MPFS files
    
    Composes with RVU scraper for shared artifacts and adds discovery
    for MPFS-specific files like conversion factors and abstracts.
    """
    
    def __init__(self, output_dir: str = "./data/scraped/mpfs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Compose with RVU scraper for shared artifacts
        self.rvu_scraper = CMSRVUScraper(str(self.output_dir.parent / "rvu"))
        self.manifest_store = DiscoveryManifestStore(self.output_dir / "manifests", prefix="cms_mpfs_manifest")
        
        # MPFS-specific URLs and patterns
        self.mpfs_base_url = "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched"
        self.mpfs_download_url = "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads"
        
        # MPFS-specific file patterns
        self.mpfs_file_patterns = {
            "conversion_factors": [
                "conversion-factor-{year}.zip",
                "cf-{year}.zip", 
                "conversion-factor-{year}.xlsx",
                "cf-{year}.xlsx"
            ],
            "abstracts": [
                "pfs-abstract-{year}.zip",
                "abstract-{year}.zip",
                "pfs-abstract-{year}.pdf",
                "abstract-{year}.pdf"
            ],
            "national_payment": [
                "national-payment-{year}.zip",
                "national-{year}.zip",
                "national-payment-{year}.xlsx",
                "national-{year}.xlsx"
            ],
            "policy_updates": [
                "policy-updates-{year}.zip",
                "updates-{year}.zip",
                "policy-updates-{year}.pdf",
                "updates-{year}.pdf"
            ]
        }
        
        # File type mappings
        self.content_type_mapping = {
            '.zip': 'application/zip',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.pdf': 'application/pdf',
            '.txt': 'text/plain',
            '.csv': 'text/csv'
        }
    
    async def scrape_mpfs_files(
        self, 
        start_year: int, 
        end_year: int,
        latest_only: bool = True
    ) -> List[RVUFileInfo]:
        """
        Discover MPFS files for the given year range
        
        Args:
            start_year: Starting year for discovery
            end_year: Ending year for discovery  
            latest_only: If True, only return latest files per type
            
        Returns:
            List of discovered MPFS files
        """
        logger.info("Starting MPFS file discovery", 
                   start_year=start_year, 
                   end_year=end_year,
                   latest_only=latest_only)
        
        all_files = []
        
        # Get shared RVU files (PPRRVU, GPCI, LocalityCounty, etc.)
        try:
            rvu_files = await self.rvu_scraper.scrape_rvu_files(start_year, end_year)
            all_files.extend(rvu_files)
            logger.info("Retrieved RVU files", count=len(rvu_files))
        except Exception as e:
            logger.warning("Failed to retrieve RVU files", error=str(e))
        
        # Discover MPFS-specific files
        for year in range(start_year, end_year + 1):
            try:
                year_files = await self._discover_mpfs_year_files(year)
                all_files.extend(year_files)
                logger.info("Discovered MPFS files for year", year=year, count=len(year_files))
            except Exception as e:
                logger.warning("Failed to discover MPFS files for year", year=year, error=str(e))
        
        # Filter to latest files if requested
        if latest_only:
            all_files = self._filter_latest_files(all_files)
            logger.info("Filtered to latest files", count=len(all_files))
        
        # Save discovery manifest
        await self._save_discovery_manifest(all_files, start_year, end_year)
        
        logger.info("MPFS file discovery completed", total_files=len(all_files))
        return all_files
    
    async def _discover_mpfs_year_files(self, year: int) -> List[RVUFileInfo]:
        """Discover MPFS files for a specific year"""
        files = []
        
        # Try to discover files from MPFS download page
        try:
            page_files = await self._scrape_mpfs_download_page(year)
            files.extend(page_files)
        except Exception as e:
            logger.warning("Failed to scrape MPFS download page", year=year, error=str(e))
        
        # Try to discover files using known patterns
        try:
            pattern_files = await self._discover_by_patterns(year)
            files.extend(pattern_files)
        except Exception as e:
            logger.warning("Failed to discover files by patterns", year=year, error=str(e))
        
        return files
    
    async def _scrape_mpfs_download_page(self, year: int) -> List[RVUFileInfo]:
        """Scrape MPFS download page for files"""
        files = []
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                # Try the main MPFS download page
                response = await client.get(self.mpfs_download_url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for links that might contain MPFS files
                links = soup.find_all('a', href=True)
                
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # Check if this looks like an MPFS file for our year
                    if self._is_mpfs_file_link(href, text, year):
                        file_info = await self._create_file_info_from_link(href, text, year)
                        if file_info:
                            files.append(file_info)
                            
            except Exception as e:
                logger.warning("Failed to scrape MPFS download page", error=str(e))
        
        return files
    
    def _is_mpfs_file_link(self, href: str, text: str, year: int) -> bool:
        """Check if a link looks like an MPFS file for the given year"""
        year_str = str(year)
        
        # Check if year appears in href or text
        if year_str not in href and year_str not in text:
            return False
        
        # Check for MPFS-related keywords
        mpfs_keywords = [
            'conversion', 'factor', 'abstract', 'national', 'payment',
            'pfs', 'mpfs', 'physician', 'fee', 'schedule'
        ]
        
        href_lower = href.lower()
        text_lower = text.lower()
        
        return any(keyword in href_lower or keyword in text_lower for keyword in mpfs_keywords)
    
    async def _create_file_info_from_link(self, href: str, text: str, year: int) -> Optional[RVUFileInfo]:
        """Create ScrapedFileInfo from a discovered link"""
        try:
            # Ensure absolute URL
            if href.startswith('/'):
                href = f"https://www.cms.gov{href}"
            elif not href.startswith('http'):
                href = f"{self.mpfs_download_url}/{href}"
            
            # Get file info
            async with httpx.AsyncClient(timeout=30.0) as client:
                head_response = await client.head(href)
                
                if head_response.status_code == 200:
                    content_length = int(head_response.headers.get('content-length', 0))
                    last_modified = head_response.headers.get('last-modified')
                    
                    if last_modified:
                        last_modified = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                    else:
                        last_modified = datetime.now()
                    
                    # Determine content type
                    content_type = head_response.headers.get('content-type', 'application/octet-stream')
                    
                    # Determine file type
                    file_type = self._determine_file_type(href, text)
                    
                    return RVUFileInfo(
                        url=href,
                        filename=Path(href).name or f"mpfs_{year}_{file_type}",
                        size_bytes=content_length,
                        last_modified=last_modified,
                        content_type=content_type,
                        year=year,
                        file_type=file_type,
                        checksum=None  # Will be calculated when downloaded
                    )
                    
        except Exception as e:
            logger.warning("Failed to create file info from link", href=href, error=str(e))
        
        return None
    
    def _determine_file_type(self, href: str, text: str) -> str:
        """Determine the type of MPFS file based on URL and text"""
        href_lower = href.lower()
        text_lower = text.lower()
        
        if 'conversion' in href_lower or 'conversion' in text_lower or 'cf' in href_lower:
            return 'conversion_factors'
        elif 'abstract' in href_lower or 'abstract' in text_lower:
            return 'abstracts'
        elif 'national' in href_lower or 'national' in text_lower:
            return 'national_payment'
        elif 'policy' in href_lower or 'update' in href_lower or 'policy' in text_lower:
            return 'policy_updates'
        else:
            return 'other'
    
    async def _discover_by_patterns(self, year: int) -> List[RVUFileInfo]:
        """Discover files using known URL patterns"""
        files = []
        
        for file_type, patterns in self.mpfs_file_patterns.items():
            for pattern in patterns:
                try:
                    # Try to construct URL from pattern
                    filename = pattern.format(year=year)
                    url = f"{self.mpfs_download_url}/{filename}"
                    
                    # Check if file exists
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        head_response = await client.head(url)
                        
                        if head_response.status_code == 200:
                            content_length = int(head_response.headers.get('content-length', 0))
                            last_modified = head_response.headers.get('last-modified')
                            
                            if last_modified:
                                last_modified = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                            else:
                                last_modified = datetime.now()
                            
                            content_type = head_response.headers.get('content-type', 'application/octet-stream')
                            
                            file_info = RVUFileInfo(
                                url=url,
                                filename=filename,
                                size_bytes=content_length,
                                last_modified=last_modified,
                                content_type=content_type,
                                year=year,
                                file_type=file_type,
                                checksum=None
                            )
                            
                            files.append(file_info)
                            logger.info("Discovered file by pattern", url=url, file_type=file_type)
                            
                except Exception as e:
                    # Pattern didn't work, continue to next
                    continue
        
        return files
    
    def _filter_latest_files(self, files: List[RVUFileInfo]) -> List[RVUFileInfo]:
        """Filter to latest files per type"""
        if not files:
            return files
        
        # Group by file type and year
        grouped = {}
        for file_info in files:
            key = (file_info.file_type, file_info.year)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(file_info)
        
        # Keep latest file per type per year
        latest_files = []
        for (file_type, year), type_files in grouped.items():
            latest = sorted(
                type_files,
                key=lambda x: x.last_modified or datetime.min,
                reverse=True
            )[0]
            latest_files.append(latest)
        
        return latest_files
    
    async def _save_discovery_manifest(self, files: List[RVUFileInfo], start_year: int, end_year: int) -> None:
        """Persist discovery manifest using shared helper."""
        manifest = DiscoveryManifest.create(
            source="cms_mpfs",
            source_url=self.mpfs_download_url,
            discovered_from=self.mpfs_download_url,
            files=files,
            metadata={
                "scraper_version": SCRAPER_VERSION,
                "discovery_method": "cms_mpfs_scraper",
                "total_files": len(files),
            },
            license_info={
                "name": "CMS Open Data",
                "url": "https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy",
                "attribution_required": True,
            },
            start_year=start_year,
            end_year=end_year,
            default_content_type="application/zip",
        )

        manifest.metadata["includes_rvu_components"] = True
        self.manifest_store.save(manifest)
        logger.info("Saved discovery manifest", source="cms_mpfs", files=len(files))


# Example usage and testing
async def main():
    """Example usage of MPFS scraper"""
    scraper = CMSMPFSScraper()
    
    # Discover MPFS files for 2025
    files = await scraper.scrape_mpfs_files(2025, 2025, latest_only=True)
    
    print(f"Discovered {len(files)} MPFS files:")
    for file_info in files:
        print(f"  - {file_info.filename} ({file_info.file_type}) - {file_info.size_bytes} bytes")


if __name__ == "__main__":
    asyncio.run(main())
