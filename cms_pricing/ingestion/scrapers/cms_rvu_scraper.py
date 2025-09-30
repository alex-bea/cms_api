"""
CMS RVU Files Scraper
Scrapes the official CMS RVU files page and downloads historical data
"""

import asyncio
import re
import hashlib
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import httpx
import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger()


@dataclass
class RVUFileInfo:
    """Information about an RVU file"""
    name: str
    filename: str
    url: str
    year: int
    quarter: str
    file_type: str
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    checksum: Optional[str] = None


class CMSRVUScraper:
    """Scraper for CMS RVU files page"""
    
    def __init__(self, output_dir: str = "./data/cms_rvu"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://www.cms.gov"
        self.rvu_page_url = "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files"
        
    async def scrape_rvu_files(self, start_year: int = 2003, end_year: int = 2025) -> List[RVUFileInfo]:
        """
        Scrape RVU files from the CMS page
        
        Args:
            start_year: Starting year for historical data
            end_year: Ending year (current year)
            
        Returns:
            List of RVUFileInfo objects
        """
        logger.info("Starting RVU files scraping", 
                   start_year=start_year, end_year=end_year)
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Fetch the main RVU page
                response = await client.get(self.rvu_page_url)
                response.raise_for_status()
                
                # Parse HTML
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract file links from the page
                rvu_files = self._extract_file_links(soup, start_year, end_year)
                
                logger.info("RVU files scraped successfully", 
                           files_found=len(rvu_files))
                
                return rvu_files
                
        except Exception as e:
            logger.error("Failed to scrape RVU files", error=str(e))
            raise
    
    async def download_files(self, file_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Download files from the provided file list
        
        Args:
            file_list: List of file information dictionaries
            
        Returns:
            Download results summary
        """
        logger.info("Starting file download", files_count=len(file_list))
        
        download_results = {
            "files_downloaded": 0,
            "files_failed": 0,
            "total_size_bytes": 0,
            "download_errors": [],
            "downloaded_files": []
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                for file_info in file_list:
                    try:
                        # Download file
                        file_result = await self._download_single_file(client, file_info)
                        
                        if file_result["success"]:
                            download_results["files_downloaded"] += 1
                            download_results["total_size_bytes"] += file_result.get("size_bytes", 0)
                            download_results["downloaded_files"].append(file_result)
                        else:
                            download_results["files_failed"] += 1
                            download_results["download_errors"].append({
                                "filename": file_info.get("filename", "unknown"),
                                "error": file_result.get("error", "unknown error")
                            })
                            
                    except Exception as e:
                        logger.error("Failed to download file", 
                                   filename=file_info.get("filename", "unknown"), 
                                   error=str(e))
                        download_results["files_failed"] += 1
                        download_results["download_errors"].append({
                            "filename": file_info.get("filename", "unknown"),
                            "error": str(e)
                        })
            
            logger.info("File download completed", 
                       downloaded=download_results["files_downloaded"],
                       failed=download_results["files_failed"],
                       total_size=download_results["total_size_bytes"])
            
            return download_results
            
        except Exception as e:
            logger.error("File download failed", error=str(e))
            raise
    
    async def _download_single_file(self, client: httpx.AsyncClient, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Download a single file"""
        try:
            url = file_info["url"]
            filename = file_info["filename"]
            
            logger.info("Downloading file", url=url, filename=filename)
            
            # Download file
            response = await client.get(url)
            response.raise_for_status()
            
            # Save file
            file_path = self.output_dir / filename
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            # Calculate checksum
            import hashlib
            with open(file_path, 'rb') as f:
                file_content = f.read()
                checksum = hashlib.sha256(file_content).hexdigest()
            
            result = {
                "success": True,
                "filename": filename,
                "url": url,
                "file_path": str(file_path),
                "size_bytes": len(file_content),
                "checksum": checksum,
                "downloaded_at": datetime.now().isoformat()
            }
            
            logger.info("File downloaded successfully", 
                       filename=filename, 
                       size_bytes=len(file_content))
            
            return result
            
        except Exception as e:
            logger.error("Failed to download single file", 
                       filename=file_info.get("filename", "unknown"), 
                       error=str(e))
            return {
                "success": False,
                "filename": file_info.get("filename", "unknown"),
                "error": str(e)
            }
    
    def _extract_file_links(self, soup: BeautifulSoup, start_year: int, end_year: int) -> List[RVUFileInfo]:
        """Extract file links from the parsed HTML"""
        rvu_files = []
        
        # Look for links that match RVU file patterns
        # Pattern: RVU{YY}{Letter} or similar
        rvu_pattern = re.compile(r'RVU(\d{2,4})([A-Z]*)', re.IGNORECASE)
        
        # Find all links on the page
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Check if this looks like an RVU file
            if 'rvu' in text.lower() or 'rvu' in href.lower():
                # Extract file information
                file_info = self._parse_rvu_link(href, text, start_year, end_year)
                if file_info:
                    rvu_files.append(file_info)
        
        # Also look for direct file links in tables
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                for cell in cells:
                    links_in_cell = cell.find_all('a', href=True)
                    for link in links_in_cell:
                        href = link.get('href', '')
                        text = link.get_text(strip=True)
                        file_info = self._parse_rvu_link(href, text, start_year, end_year)
                        if file_info:
                            rvu_files.append(file_info)
        
        # Remove duplicates and sort by year
        unique_files = self._deduplicate_files(rvu_files)
        unique_files.sort(key=lambda x: (x.year, x.quarter))
        
        return unique_files
    
    def _parse_rvu_link(self, href: str, text: str, start_year: int, end_year: int) -> Optional[RVUFileInfo]:
        """Parse a single RVU link to extract file information"""
        try:
            # Clean up the text
            text = text.strip()
            
            # Extract year and quarter from filename
            rvu_match = re.search(r'RVU(\d{2,4})([A-Z]*)', text, re.IGNORECASE)
            if not rvu_match:
                return None
            
            year_str = rvu_match.group(1)
            quarter = rvu_match.group(2) if rvu_match.group(2) else 'A'
            
            # Convert 2-digit year to 4-digit
            if len(year_str) == 2:
                year = 2000 + int(year_str)
            else:
                year = int(year_str)
            
            # Filter by year range
            if year < start_year or year > end_year:
                return None
            
            # Build full URL
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = self.base_url + href
            else:
                full_url = self.base_url + '/' + href
            
            # Determine file type
            file_type = 'zip' if '.zip' in text.lower() else 'txt'
            
            return RVUFileInfo(
                name=text,
                filename=text,
                url=full_url,
                year=year,
                quarter=quarter,
                file_type=file_type
            )
            
        except Exception as e:
            logger.warning("Failed to parse RVU link", href=href, text=text, error=str(e))
            return None
    
    def _deduplicate_files(self, files: List[RVUFileInfo]) -> List[RVUFileInfo]:
        """Remove duplicate files based on URL"""
        seen_urls = set()
        unique_files = []
        
        for file_info in files:
            if file_info.url not in seen_urls:
                seen_urls.add(file_info.url)
                unique_files.append(file_info)
        
        return unique_files
    
    async def download_file(self, file_info: RVUFileInfo, client: httpx.AsyncClient) -> Dict[str, Any]:
        """Download a single RVU file"""
        try:
            logger.info("Downloading RVU file", 
                       filename=file_info.filename, url=file_info.url)
            
            response = await client.get(file_info.url)
            response.raise_for_status()
            
            content = response.content
            
            # Calculate checksum
            checksum = hashlib.sha256(content).hexdigest()
            
            # Save file
            file_path = self.output_dir / f"{file_info.year}" / file_info.filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                f.write(content)
            
            # Update file info
            file_info.size_bytes = len(content)
            file_info.checksum = checksum
            file_info.last_modified = datetime.now()
            
            logger.info("File downloaded successfully", 
                       filename=file_info.filename, 
                       size_bytes=len(content),
                       checksum=checksum[:16])
            
            return {
                "status": "success",
                "file_info": file_info,
                "file_path": str(file_path),
                "size_bytes": len(content),
                "checksum": checksum
            }
            
        except Exception as e:
            logger.error("Failed to download file", 
                        filename=file_info.filename, error=str(e))
            return {
                "status": "failed",
                "file_info": file_info,
                "error": str(e)
            }
    
    async def download_all_files(self, files: List[RVUFileInfo], 
                                max_concurrent: int = 5) -> List[Dict[str, Any]]:
        """Download all RVU files with concurrency control"""
        logger.info("Starting bulk download", 
                   total_files=len(files), max_concurrent=max_concurrent)
        
        results = []
        
        # Use semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def download_with_semaphore(file_info: RVUFileInfo):
            async with semaphore:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    return await self.download_file(file_info, client)
        
        # Download files in batches
        tasks = [download_with_semaphore(file_info) for file_info in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful_downloads = 0
        failed_downloads = 0
        
        for result in results:
            if isinstance(result, Exception):
                failed_downloads += 1
                logger.error("Download task failed", error=str(result))
            elif result.get("status") == "success":
                successful_downloads += 1
            else:
                failed_downloads += 1
        
        logger.info("Bulk download completed", 
                   successful=successful_downloads, failed=failed_downloads)
        
        return results
    
    def generate_manifest(self, files: List[RVUFileInfo], 
                         download_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a manifest of all downloaded files"""
        manifest = {
            "scraped_at": datetime.now().isoformat(),
            "source_url": self.rvu_page_url,
            "total_files": len(files),
            "files": []
        }
        
        for i, file_info in enumerate(files):
            download_result = download_results[i] if i < len(download_results) else {}
            
            file_entry = {
                "name": file_info.name,
                "filename": file_info.filename,
                "url": file_info.url,
                "year": file_info.year,
                "quarter": file_info.quarter,
                "file_type": file_info.file_type,
                "size_bytes": file_info.size_bytes,
                "checksum": file_info.checksum,
                "last_modified": file_info.last_modified.isoformat() if file_info.last_modified else None,
                "download_status": download_result.get("status", "unknown"),
                "file_path": download_result.get("file_path"),
                "download_error": download_result.get("error")
            }
            
            manifest["files"].append(file_entry)
        
        # Save manifest
        manifest_path = self.output_dir / "rvu_files_manifest.json"
        import json
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info("Manifest generated", manifest_path=str(manifest_path))
        
        return manifest


async def main():
    """Main function to demonstrate the scraper"""
    scraper = CMSRVUScraper()
    
    # Scrape files from 2020 to 2025 (recent data)
    files = await scraper.scrape_rvu_files(start_year=2020, end_year=2025)
    
    print(f"Found {len(files)} RVU files:")
    for file_info in files:
        print(f"  {file_info.year} {file_info.quarter}: {file_info.filename}")
    
    # Download files (limit to 3 for demo)
    if files:
        demo_files = files[:3]  # Just download first 3 files for demo
        results = await scraper.download_all_files(demo_files, max_concurrent=2)
        
        # Generate manifest
        manifest = scraper.generate_manifest(files, results)
        print(f"\nManifest generated with {len(manifest['files'])} files")


if __name__ == "__main__":
    asyncio.run(main())
