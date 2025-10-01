#!/usr/bin/env python3
"""
CMS OPPS Scraper
================

Discovers and downloads OPPS quarterly addenda files from CMS.gov.
Follows the same pattern as the RVU scraper with discovery, checksum, and manifest generation.

Author: CMS Pricing Platform Team
Version: 1.0.0
"""

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup
from structlog import get_logger

logger = get_logger()


@dataclass
class ScrapedFileInfo:
    """Information about a scraped file"""
    url: str
    filename: str
    file_type: str
    batch_id: str
    discovered_at: datetime
    source_page: str
    metadata: Dict[str, Any]
    local_path: Optional[Path] = None
    checksum: Optional[str] = None
    downloaded_at: Optional[datetime] = None


class CMSOPPSScraper:
    """
    CMS OPPS Scraper for quarterly addenda discovery and download.
    
    Discovers OPPS Addendum A/B files from the CMS Quarterly Addenda page,
    follows links to specific quarterly releases, and downloads files with
    checksum validation and manifest generation.
    """
    
    def __init__(self, base_url: str = "https://www.cms.gov", output_dir: Path = None):
        self.base_url = base_url
        self.output_dir = output_dir or Path("data/scraped/opps")
        self.opps_base_url = "https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient"
        self.quarterly_addenda_url = "https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates"
        
        # OPPS-specific patterns
        self.quarterly_pattern = re.compile(r'(\d{4})\s*[Qq](\d)', re.IGNORECASE)
        self.addendum_pattern = re.compile(r'addendum\s*([AB])', re.IGNORECASE)
        self.file_patterns = {
            'addendum_a': re.compile(r'addendum\s*a.*\.(csv|xls|xlsx|txt)', re.IGNORECASE),
            'addendum_b': re.compile(r'addendum\s*b.*\.(csv|xls|xlsx|txt)', re.IGNORECASE),
            'zip_files': re.compile(r'\.(zip|gz)$', re.IGNORECASE)
        }
        
        # Quarterly release patterns
        self.quarterly_release_patterns = {
            'q1': re.compile(r'january|jan|q1|first\s*quarter', re.IGNORECASE),
            'q2': re.compile(r'april|apr|q2|second\s*quarter', re.IGNORECASE),
            'q3': re.compile(r'july|jul|q3|third\s*quarter', re.IGNORECASE),
            'q4': re.compile(r'october|oct|q4|fourth\s*quarter', re.IGNORECASE)
        }
    
    async def discover_files(self, max_quarters: int = 8) -> List[ScrapedFileInfo]:
        """
        Discover OPPS quarterly addenda files.
        
        Args:
            max_quarters: Maximum number of quarters to discover (default: 8)
            
        Returns:
            List of discovered file information
        """
        logger.info("Starting OPPS file discovery", max_quarters=max_quarters)
        
        try:
            # Get the main quarterly addenda page
            addenda_links = await self._get_quarterly_addenda_links()
            
            discovered_files = []
            quarters_found = 0
            
            for link_info in addenda_links:
                if quarters_found >= max_quarters:
                    break
                
                try:
                    # Extract quarter info from link
                    quarter_info = self._extract_quarter_info(link_info)
                    if not quarter_info:
                        continue
                    
                    # Get files for this quarter
                    quarter_files = await self._discover_quarter_files(link_info['url'], quarter_info)
                    discovered_files.extend(quarter_files)
                    quarters_found += 1
                    
                    logger.info(
                        "Discovered quarter files",
                        quarter=f"{quarter_info['year']}Q{quarter_info['quarter']}",
                        file_count=len(quarter_files)
                    )
                    
                except Exception as e:
                    logger.error(
                        "Failed to discover quarter files",
                        quarter=link_info.get('text', 'unknown'),
                        error=str(e)
                    )
                    continue
            
            logger.info(
                "OPPS discovery complete",
                total_files=len(discovered_files),
                quarters_discovered=quarters_found
            )
            
            return discovered_files
            
        except Exception as e:
            logger.error("OPPS discovery failed", error=str(e))
            raise
    
    async def _get_quarterly_addenda_links(self) -> List[Dict[str, str]]:
        """Get links to quarterly addenda pages."""
        logger.info("Fetching quarterly addenda page", url=self.quarterly_addenda_url)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self.quarterly_addenda_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find links to quarterly releases
            addenda_links = []
            
            # Look for links containing quarter/year patterns
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Debug logging
                logger.debug("Checking link", href=href, text=text)
                
                # Check if this looks like a quarterly addenda link
                if self._is_quarterly_addenda_link(href, text):
                    full_url = urljoin(self.quarterly_addenda_url, href)
                    addenda_links.append({
                        'url': full_url,
                        'text': text,
                        'href': href
                    })
                    logger.debug("Added quarterly link", url=full_url, text=text)
            
            logger.info("Found quarterly addenda links", count=len(addenda_links))
            return addenda_links
    
    def _is_quarterly_addenda_link(self, href: str, text: str) -> bool:
        """Check if a link points to quarterly addenda."""
        text_lower = text.lower()
        href_lower = href.lower()
        
        # Look for quarter patterns in URL or text
        quarter_indicators = [
            'addendum', 'quarterly', 'q1', 'q2', 'q3', 'q4',
            'january', 'april', 'july', 'october', 'jan', 'apr', 'jul', 'oct'
        ]
        
        # Must contain at least one quarter indicator
        has_quarter_indicator = any(indicator in text_lower or indicator in href_lower 
                                  for indicator in quarter_indicators)
        
        # Must look like a CMS link
        is_cms_link = 'cms.gov' in href_lower or href.startswith('/')
        
        # Must not be a general page (avoid main addendum page and RSS feeds)
        is_not_general = (
            'addendum' not in href_lower or 
            any(q in href_lower for q in ['q1', 'q2', 'q3', 'q4']) or
            any(month in href_lower for month in ['january', 'april', 'july', 'october'])
        )
        
        # Exclude RSS feeds and other non-file links
        is_not_rss = not any(exclude in href_lower for exclude in ['rss', 'feed', 'subscribe'])
        
        # Must have a year pattern (2025, 2024, etc.)
        has_year = bool(re.search(r'20\d{2}', f"{text} {href}"))
        
        return (has_quarter_indicator and is_cms_link and is_not_general and 
                is_not_rss and has_year)
    
    def _extract_quarter_info(self, link_info: Dict[str, str]) -> Optional[Dict[str, int]]:
        """Extract year and quarter from link information."""
        text = link_info['text']
        href = link_info['href']
        
        # Try to extract year and quarter from text or href
        combined_text = f"{text} {href}"
        
        # Look for year pattern - handle both "2025" and "25" formats
        year_match = re.search(r'20(\d{2})', combined_text)
        if not year_match:
            return None
        
        year = int(f"20{year_match.group(1)}")
        
        # Look for quarter pattern using our predefined patterns
        quarter = None
        for q_num, pattern in self.quarterly_release_patterns.items():
            if pattern.search(combined_text):
                quarter = int(q_num[1])  # Extract number from 'q1', 'q2', etc.
                break
        
        # If no quarter found via patterns, try numeric patterns
        if quarter is None:
            quarter_match = re.search(r'[Qq](\d)', combined_text)
            if quarter_match:
                quarter = int(quarter_match.group(1))
        
        # Validate quarter range
        if quarter is None or quarter < 1 or quarter > 4:
            return None
        
        # Additional validation: ensure we have a reasonable year
        if year < 2020 or year > 2030:
            return None
        
        return {
            'year': year,
            'quarter': quarter
        }
    
    async def _discover_quarter_files(self, quarter_url: str, quarter_info: Dict[str, int]) -> List[ScrapedFileInfo]:
        """Discover files for a specific quarter."""
        logger.info(
            "Discovering quarter files",
            url=quarter_url,
            year=quarter_info['year'],
            quarter=quarter_info['quarter']
        )
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(quarter_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            files = []
            year = quarter_info['year']
            quarter = quarter_info['quarter']
            
            # Look for file links
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Check if this is a relevant file
                file_type = self._classify_file(href, text)
                if file_type:
                    initial_url = urljoin(quarter_url, href)
                    
                    # Generate batch ID
                    batch_id = f"opps_{year}q{quarter}_r01"
                    
                    # Handle disclaimer interstitials using tiered strategy
                    final_url = await self._resolve_disclaimer_url(client, initial_url, text)
                    
                    file_info = ScrapedFileInfo(
                        url=final_url,
                        filename=self._extract_filename(href, text),
                        file_type=file_type,
                        batch_id=batch_id,
                        discovered_at=datetime.utcnow(),
                        source_page=quarter_url,
                        metadata={
                            'year': year,
                            'quarter': quarter,
                            'addendum_type': file_type,
                            'original_text': text,
                            'initial_url': initial_url,
                            'disclaimer_resolved': final_url != initial_url
                        }
                    )
                    
                    files.append(file_info)
            
            logger.info(
                "Discovered quarter files",
                quarter=f"{year}Q{quarter}",
                file_count=len(files)
            )
            
            return files
    
    async def _resolve_disclaimer_url(self, client: httpx.AsyncClient, initial_url: str, text: str) -> str:
        """
        Resolve disclaimer interstitials using tiered strategy from PRD.
        
        Tier 1: Direct HTTP GET
        Tier 2: Headless browser disclaimer acceptance
        Tier 3: Quarantine (return original URL with error flag)
        """
        logger.debug("Resolving disclaimer URL", initial_url=initial_url, text=text)
        
        try:
            # Tier 1: Direct HTTP GET with appropriate headers
            headers = {
                'User-Agent': 'DIS-OPPS-Scraper/1.0 (+ops@yourco.com)',
                'Accept': 'application/octet-stream, application/zip, */*',
                'Referer': 'https://www.cms.gov/'
            }
            
            response = await client.get(initial_url, headers=headers, follow_redirects=True)
            
            # Check if we got a disclaimer page (common indicators)
            content_type = response.headers.get('content-type', '').lower()
            content_text = response.text.lower() if response.text else ''
            
            disclaimer_indicators = [
                'license.asp' in initial_url.lower(),
                'disclaimer' in content_text,
                'terms' in content_text,
                'accept' in content_text,
                'agreement' in content_text,
                'click here to accept' in content_text
            ]
            
            if any(disclaimer_indicators):
                logger.warning(
                    "Disclaimer interstitial detected",
                    url=initial_url,
                    content_type=content_type,
                    indicators=disclaimer_indicators
                )
                
                # Tier 2: Headless browser disclaimer acceptance
                resolved_url = await self._handle_disclaimer_with_browser(initial_url, text)
                if resolved_url != initial_url:
                    logger.info(
                        "Successfully resolved disclaimer with browser",
                        original_url=initial_url,
                        resolved_url=resolved_url
                    )
                    return resolved_url
                else:
                    logger.warning(
                        "Browser disclaimer resolution failed, quarantining",
                        url=initial_url
                    )
                    # Tier 3: Quarantine - return original URL with error flag
                    return initial_url
            
            # Check if we got a file (success indicators)
            file_indicators = [
                'application/zip' in content_type,
                'application/octet-stream' in content_type,
                'text/csv' in content_type,
                'application/vnd.ms-excel' in content_type,
                response.headers.get('content-disposition', '').startswith('attachment')
            ]
            
            if any(file_indicators):
                logger.info(
                    "Successfully resolved to file",
                    url=initial_url,
                    content_type=content_type,
                    content_length=response.headers.get('content-length', 'unknown')
                )
                return initial_url
            
            # If we get here, it's unclear what we got
            logger.warning(
                "Unclear response type",
                url=initial_url,
                content_type=content_type,
                status_code=response.status_code
            )
            return initial_url
            
        except Exception as e:
            logger.error(
                "Error resolving disclaimer URL",
                url=initial_url,
                error=str(e)
            )
            return initial_url
    
    async def _handle_disclaimer_with_browser(self, disclaimer_url: str, text: str) -> str:
        """
        Tier 2: Use headless browser to accept disclaimer and get download URL.
        
        Returns the resolved download URL if successful, original URL if failed.
        """
        logger.info("Attempting browser disclaimer acceptance", url=disclaimer_url)
        
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.firefox.options import Options as FirefoxOptions
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            from selenium.common.exceptions import TimeoutException, NoSuchElementException
            
            # Try Chrome first, then Firefox
            driver = None
            
            # Set up headless Chrome options
            try:
                chrome_options = ChromeOptions()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--window-size=1920,1080')
                chrome_options.add_argument('--user-agent=DIS-OPPS-Scraper/1.0 (+ops@yourco.com)')
                
                driver = webdriver.Chrome(options=chrome_options)
                logger.info("Using Chrome WebDriver for disclaimer handling")
                
            except Exception as chrome_error:
                logger.warning("Chrome WebDriver failed, trying Firefox", error=str(chrome_error))
                
                try:
                    # Set up headless Firefox options
                    firefox_options = FirefoxOptions()
                    firefox_options.add_argument('--headless')
                    firefox_options.add_argument('--width=1920')
                    firefox_options.add_argument('--height=1080')
                    
                    driver = webdriver.Firefox(options=firefox_options)
                    logger.info("Using Firefox WebDriver for disclaimer handling")
                    
                except Exception as firefox_error:
                    logger.error("Both Chrome and Firefox WebDriver failed", 
                               chrome_error=str(chrome_error),
                               firefox_error=str(firefox_error))
                    logger.info("To enable browser disclaimer handling, install Chrome or Firefox and their WebDriver")
                    logger.info("Chrome: brew install --cask google-chrome && pip install webdriver-manager")
                    logger.info("Firefox: brew install firefox && pip install webdriver-manager")
                    return disclaimer_url
            
            if not driver:
                logger.error("No WebDriver available")
                return disclaimer_url
            
            try:
                # Navigate to disclaimer page
                driver.get(disclaimer_url)
                
                # Wait for page to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Look for Accept button with various selectors
                accept_selectors = [
                    "//button[contains(text(), 'Accept')]",
                    "//input[@type='submit' and contains(@value, 'Accept')]",
                    "//a[contains(text(), 'Accept')]",
                    "//button[contains(text(), 'I Accept')]",
                    "//input[@type='submit' and contains(@value, 'I Accept')]",
                    "//a[contains(text(), 'I Accept')]",
                    "//button[contains(text(), 'Agree')]",
                    "//input[@type='submit' and contains(@value, 'Agree')]",
                    "//a[contains(text(), 'Agree')]",
                    "//button[@id='accept']",
                    "//input[@id='accept']",
                    "//a[@id='accept']"
                ]
                
                accept_button = None
                for selector in accept_selectors:
                    try:
                        accept_button = driver.find_element(By.XPATH, selector)
                        if accept_button.is_displayed() and accept_button.is_enabled():
                            break
                    except NoSuchElementException:
                        continue
                
                if not accept_button:
                    logger.warning("No Accept button found on disclaimer page", url=disclaimer_url)
                    return disclaimer_url
                
                # Click Accept button
                logger.info("Clicking Accept button", button_text=accept_button.text)
                accept_button.click()
                
                # Wait for redirect or download to start
                WebDriverWait(driver, 10).until(
                    lambda d: d.current_url != disclaimer_url or 
                    any(header in d.page_source.lower() for header in ['content-disposition', 'download'])
                )
                
                # Check if we got redirected to a download URL
                current_url = driver.current_url
                if current_url != disclaimer_url:
                    logger.info("Successfully redirected after disclaimer acceptance", 
                              original_url=disclaimer_url, 
                              new_url=current_url)
                    return current_url
                
                # Check if we're on a download page (might be same URL but different content)
                page_source = driver.page_source.lower()
                if any(indicator in page_source for indicator in ['download', 'content-disposition', 'attachment']):
                    logger.info("Download page detected after disclaimer acceptance", url=disclaimer_url)
                    return disclaimer_url
                
                # If we get here, disclaimer acceptance didn't work as expected
                logger.warning("Disclaimer acceptance completed but no download detected", url=disclaimer_url)
                return disclaimer_url
                
            finally:
                driver.quit()
                
        except ImportError:
            logger.error("Selenium not available for browser automation")
            return disclaimer_url
        except TimeoutException:
            logger.error("Timeout waiting for disclaimer page elements", url=disclaimer_url)
            return disclaimer_url
        except Exception as e:
            logger.error("Error in browser disclaimer handling", url=disclaimer_url, error=str(e))
            return disclaimer_url
    
    def _classify_file(self, href: str, text: str) -> Optional[str]:
        """Classify file type based on URL and text."""
        href_lower = href.lower()
        text_lower = text.lower()
        
        # Check for Addendum A files (APC rates)
        if (self.file_patterns['addendum_a'].search(href) or 
            self.file_patterns['addendum_a'].search(text) or
            'addendum a' in text_lower or
            'addendum-a' in href_lower):
            return 'addendum_a'
        
        # Check for Addendum B files (HCPCS→APC/SI mapping)
        if (self.file_patterns['addendum_b'].search(href) or 
            self.file_patterns['addendum_b'].search(text) or
            'addendum b' in text_lower or
            'addendum-b' in href_lower):
            return 'addendum_b'
        
        # Check for ZIP files that might contain addenda
        if self.file_patterns['zip_files'].search(href):
            # Check if text suggests it contains addenda
            if any(keyword in text_lower for keyword in ['addendum', 'opps', 'quarterly']):
                return 'addendum_zip'
        
        # Check for other OPPS-related files
        if any(keyword in text_lower for keyword in ['opps', 'quarterly', 'addendum']):
            if any(ext in href_lower for ext in ['.csv', '.xls', '.xlsx', '.txt', '.zip']):
                return 'opps_file'
        
        return None
    
    def _extract_filename(self, href: str, text: str) -> str:
        """Extract filename from URL or text."""
        # Try to get filename from URL
        parsed_url = urlparse(href)
        if parsed_url.path:
            filename = Path(parsed_url.path).name
            if filename and '.' in filename:
                return filename
        
        # Fall back to text if it looks like a filename
        if '.' in text and len(text) < 100:
            return text
        
        # Generate a default filename
        return f"opps_file_{hashlib.md5(href.encode()).hexdigest()[:8]}.txt"
    
    async def download_file(self, file_info: ScrapedFileInfo) -> Path:
        """
        Download a single OPPS file.
        
        Args:
            file_info: File information to download
            
        Returns:
            Path to downloaded file
        """
        logger.info(
            "Downloading OPPS file",
            url=file_info.url,
            filename=file_info.filename,
            batch_id=file_info.batch_id
        )
        
        try:
            # Create batch directory
            batch_dir = self.output_dir / "scraped" / file_info.batch_id
            batch_dir.mkdir(parents=True, exist_ok=True)
            
            # Download file
            file_path = await self._download_with_retry(file_info.url, batch_dir / file_info.filename)
            
            # Calculate checksum
            checksum = self._calculate_checksum(file_path)
            
            # Update file info
            file_info.local_path = file_path
            file_info.checksum = checksum
            file_info.downloaded_at = datetime.utcnow()
            
            # Generate manifest
            await self._generate_manifest(file_info, batch_dir)
            
            logger.info(
                "OPPS file downloaded successfully",
                file_path=str(file_path),
                checksum=checksum,
                size=file_path.stat().st_size
            )
            
            return file_path
            
        except Exception as e:
            logger.error(
                "Failed to download OPPS file",
                url=file_info.url,
                error=str(e)
            )
            raise
    
    async def _generate_manifest(self, file_info: ScrapedFileInfo, batch_dir: Path):
        """Generate manifest file for the batch."""
        manifest = {
            'batch_id': file_info.batch_id,
            'discovered_at': file_info.discovered_at.isoformat(),
            'downloaded_at': file_info.downloaded_at.isoformat(),
            'source_page': file_info.source_page,
            'files': [{
                'url': file_info.url,
                'filename': file_info.filename,
                'file_type': file_info.file_type,
                'local_path': str(file_info.local_path),
                'checksum': file_info.checksum,
                'size_bytes': file_info.local_path.stat().st_size,
                'metadata': file_info.metadata
            }],
            'scraper_version': '1.0.0',
            'discovery_method': 'cms_opps_scraper'
        }
        
        manifest_path = batch_dir / 'manifest.json'
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info("Generated OPPS manifest", manifest_path=str(manifest_path))
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    async def _download_with_retry(self, url: str, file_path: Path, max_retries: int = 3) -> Path:
        """Download file with retry logic."""
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    
                    return file_path
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed, retrying", error=str(e))
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    def get_latest_quarters(self, count: int = 4) -> List[str]:
        """Get the latest N quarters for OPPS releases."""
        current_year = datetime.now().year
        current_quarter = ((datetime.now().month - 1) // 3) + 1
        
        quarters = []
        for i in range(count):
            year = current_year
            quarter = current_quarter - i
            
            if quarter <= 0:
                quarter += 4
                year -= 1
            
            quarters.append(f"{year}q{quarter}")
        
        return quarters
    
    async def discover_latest(self, quarters: int = 2) -> List[ScrapedFileInfo]:
        """Discover files for the latest N quarters."""
        logger.info("Discovering latest OPPS quarters", quarters=quarters)
        
        # Get latest quarters
        latest_quarters = self.get_latest_quarters(quarters)
        
        # Discover files for each quarter
        all_files = []
        for quarter_str in latest_quarters:
            year, quarter = quarter_str.split('q')
            year = int(year)
            quarter = int(quarter)
            
            # Look for files in the quarterly addenda
            try:
                quarter_files = await self._discover_quarter_by_date(year, quarter)
                all_files.extend(quarter_files)
            except Exception as e:
                logger.warning(
                    "Failed to discover quarter",
                    year=year,
                    quarter=quarter,
                    error=str(e)
                )
        
        return all_files
    
    async def _discover_quarter_by_date(self, year: int, quarter: int) -> List[ScrapedFileInfo]:
        """Discover files for a specific year/quarter."""
        # This would implement specific logic to find files for a given quarter
        # For now, we'll use the general discovery and filter
        all_files = await self.discover_files(max_quarters=8)
        
        # Filter for the specific quarter
        quarter_files = [
            f for f in all_files
            if f.metadata.get('year') == year and f.metadata.get('quarter') == quarter
        ]
        
        return quarter_files


# CLI interface
async def main():
    """CLI entry point for OPPS scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CMS OPPS Scraper')
    parser.add_argument('--discover', action='store_true', help='Discover OPPS files')
    parser.add_argument('--download', action='store_true', help='Download discovered files')
    parser.add_argument('--latest', type=int, default=2, help='Number of latest quarters to discover')
    parser.add_argument('--max-quarters', type=int, default=8, help='Maximum quarters to discover')
    parser.add_argument('--output-dir', type=Path, default=Path('data/scraped/opps'), help='Output directory')
    
    args = parser.parse_args()
    
    scraper = CMSOPPSScraper(output_dir=args.output_dir)
    
    if args.discover:
        files = await scraper.discover_files(max_quarters=args.max_quarters)
        print(f"Discovered {len(files)} OPPS files")
        
        for file_info in files:
            print(f"  {file_info.batch_id}: {file_info.filename} ({file_info.file_type})")
    
    if args.download:
        files = await scraper.discover_latest(quarters=args.latest)
        print(f"Downloading {len(files)} latest OPPS files")
        
        for file_info in files:
            try:
                await scraper.download_file(file_info)
                print(f"  Downloaded: {file_info.filename}")
            except Exception as e:
                print(f"  Failed to download {file_info.filename}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
