"""
CMS RVU Files Scraper
=====================

Discovery-only scraper that enumerates CMS RVU release pages, follows each
quarterly detail page, and emits metadata-rich file entries suitable for
downstream ingestion.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
import structlog
from bs4 import BeautifulSoup, Tag
import hashlib

from ..metadata.discovery_manifest import (
    DiscoveryManifest,
    DiscoveryManifestStore,
)

logger = structlog.get_logger()

SCRAPER_VERSION = "2.0.0"
USER_AGENT = "DIS-RVU-Scraper/2.0 (+ops@yourco.com)"
DEFAULT_TIMEOUT = 30.0
MAX_CONCURRENT_DETAIL_REQUESTS = 4
SUPPORTED_EXTENSIONS = (".zip", ".csv", ".txt", ".xlsx", ".xls")


@dataclass
class RVUDetailLink:
    """Represents a quarter detail page discovered from the landing page."""

    url: str
    text: str
    year: int
    quarter: str
    revision: Optional[str]
    heading: Optional[str] = None
    context: Optional[str] = None

    @property
    def release_id(self) -> str:
        revision_part = self.revision or ""
        return f"rvu{self.year % 100:02d}{self.quarter.lower()}{revision_part.lower()}"


@dataclass
class RVUFileInfo:
    """Metadata describing a downloadable artifact discovered for RVU."""

    url: str
    filename: str
    content_type: Optional[str]
    year: int
    quarter: str
    revision: Optional[str]
    posted_at: Optional[str]
    file_size: Optional[str]
    detail_url: str
    display_name: str
    version: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    last_modified: Optional[datetime] = None

    def to_manifest_entry(self) -> Dict[str, Any]:
        """Convert into an object consumable by DiscoveryManifest."""
        entry_metadata = {
            "detail_url": self.detail_url,
            "display_name": self.display_name,
            "posted_at": self.posted_at,
            "file_size": self.file_size,
            "version": self.version,
        }
        entry_metadata.update(self.metadata)

        return {
            "url": self.url,
            "filename": self.filename,
            "content_type": self.content_type or "application/octet-stream",
            "year": self.year,
            "quarter": self.quarter,
            "metadata": entry_metadata,
            "size_bytes": self.size_bytes,
            "sha256": self.checksum,
        }


class CMSRVUScraper:
    """Discovery scraper for CMS RVU files."""

    def __init__(self, output_dir: str = "./data/cms_rvu") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.base_url = "https://www.cms.gov"
        self.rvu_page_url = (
            "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files"
        )

        self.manifest_store = DiscoveryManifestStore(
            self.output_dir / "manifests", prefix="cms_rvu_manifest"
        )
        self._last_manifest_path: Optional[Path] = None

    @property
    def last_manifest_path(self) -> Optional[Path]:
        """Return path to the most recently written manifest, if any."""
        return self._last_manifest_path

    async def scrape_rvu_files(
        self,
        start_year: int = 2003,
        end_year: int = 2025,
    ) -> List[RVUFileInfo]:
        """
        Discover RVU files published by CMS between the provided year bounds.

        Returns:
            List of RVUFileInfo entries for downstream ingestion.
        """
        logger.info(
            "rvu.scraper.discover.start",
            start_year=start_year,
            end_year=end_year,
            scraper_version=SCRAPER_VERSION,
        )

        headers = {"User-Agent": USER_AGENT}

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers=headers) as client:
            landing_html = await self._fetch_text(client, self.rvu_page_url)
            detail_links = self._extract_detail_links(landing_html, start_year, end_year)

            logger.info(
                "rvu.scraper.detail_links",
                count=len(detail_links),
                years=sorted({link.year for link in detail_links}),
            )

            files = await self._discover_all_detail_pages(client, detail_links)

        files = self._deduplicate_files(files)
        files.sort(key=lambda item: (item.year, item.quarter, item.revision or "", item.filename))

        manifest = self._build_manifest(
            files=files,
            start_year=start_year,
            end_year=end_year,
        )
        previous = self.manifest_store.load_latest()
        if not manifest.has_same_files(previous):
            manifest.metadata["changes_detected"] = True
        self._last_manifest_path = self.manifest_store.save(manifest)

        logger.info(
            "rvu.scraper.discover.complete",
            files=len(files),
            manifest_path=str(self._last_manifest_path) if self._last_manifest_path else None,
        )

        return files

    # ------------------------------------------------------------------ #
    # Discovery helpers
    # ------------------------------------------------------------------ #
    async def _fetch_text(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(url)
        response.raise_for_status()
        return response.text

    def _extract_detail_links(
        self,
        landing_html: str,
        start_year: int,
        end_year: int,
    ) -> List[RVUDetailLink]:
        soup = BeautifulSoup(landing_html, "html.parser")
        candidates: List[RVUDetailLink] = []

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            text = anchor.get_text(strip=True) or href

            release = self._parse_release_identifier(text) or self._parse_release_identifier(href)
            if release is None:
                continue

            year, quarter, revision = release
            if year < start_year or year > end_year:
                continue

            url = self._normalize_url(href)
            heading = self._nearest_heading(anchor)
            context = anchor.parent.get_text(" ", strip=True) if anchor.parent else None

            candidates.append(
                RVUDetailLink(
                    url=url,
                    text=text,
                    year=year,
                    quarter=quarter,
                    revision=revision,
                    heading=heading,
                    context=context,
                )
            )

        unique: Dict[str, RVUDetailLink] = {}
        for link in candidates:
            unique.setdefault(link.url, link)

        return list(unique.values())

    async def _discover_all_detail_pages(
        self,
        client: httpx.AsyncClient,
        detail_links: Iterable[RVUDetailLink],
    ) -> List[RVUFileInfo]:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DETAIL_REQUESTS)

        async def discover(link: RVUDetailLink) -> List[RVUFileInfo]:
            async with semaphore:
                try:
                    html = await self._fetch_text(client, link.url)
                except Exception as exc:  # pragma: no cover - network failure
                    logger.warning(
                        "rvu.scraper.detail_fetch_failed", url=link.url, error=str(exc)
                    )
                    return []
                files = self._extract_downloads_from_detail(html, link)
                # Validate each discovered file
                validated_files = []
                for file_info in files:
                    is_valid, content_type, size_bytes = await self._validate_download_url(
                        file_info.url, client
                    )
                    if is_valid:
                        # Update with validated metadata
                        if content_type:
                            file_info.content_type = content_type
                        if size_bytes:
                            file_info.size_bytes = size_bytes
                        validated_files.append(file_info)
                    else:
                        logger.warning(
                            "rvu.scraper.file_rejected",
                            url=file_info.url,
                            reason="Failed validation (likely HTML page)",
                        )
                return validated_files

        tasks = [discover(link) for link in detail_links]
        results = await asyncio.gather(*tasks)

        flattened: List[RVUFileInfo] = []
        for items in results:
            flattened.extend(items)
        return flattened

    def _extract_downloads_from_detail(
        self,
        detail_html: str,
        link: RVUDetailLink,
    ) -> List[RVUFileInfo]:
        soup = BeautifulSoup(detail_html, "html.parser")
        downloads: List[RVUFileInfo] = []

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()

            # Accept either explicit file extensions or RVU quarter-style links that often redirect to attachments
            href_lc = href.lower()
            looks_like_quarter = bool(re.search(r"rvu\d{2,4}[a-d](?:-\d+)?$", href_lc))
            is_supported_ext = self._is_supported_asset(href)
            has_zip_hint = (
                (anchor.get("data-file-type") or "").lower().find("zip") >= 0
                or (anchor.get("type") or "").lower().find("zip") >= 0
                or "zip" in (anchor.get_text(strip=True) or "").lower()
            )

            if not (is_supported_ext or looks_like_quarter or has_zip_hint):
                continue

            download_url = self._normalize_url(href)
            display_name = anchor.get_text(strip=True) or link.text
            context_text = anchor.parent.get_text(" ", strip=True) if anchor.parent else ""

            content_type, file_size = self._extract_content_metadata(anchor, context_text)
            posted_at, posted_label = self._extract_posted_date(display_name, context_text)
            version = self._build_version(link, posted_at)
            filename = self._build_filename(link, download_url, posted_at, display_name)

            metadata = {
                "detail_heading": link.heading,
                "detail_context": link.context,
                "anchor_context": context_text,
                "posted_label": posted_label,
            }

            downloads.append(
                RVUFileInfo(
                    url=download_url,
                    filename=filename,
                    content_type=content_type,
                    year=link.year,
                    quarter=link.quarter,
                    revision=link.revision,
                    posted_at=posted_at,
                    file_size=file_size,
                    detail_url=link.url,
                    display_name=display_name,
                    version=version,
                    metadata=metadata,
                )
            )

        return downloads

    async def _validate_download_url(
        self,
        url: str,
        client: httpx.AsyncClient,
    ) -> Tuple[bool, Optional[str], Optional[int]]:
        """
        Validate URL points to actual file, not HTML page.
        
        Returns:
            (is_valid, detected_content_type, detected_size_bytes)
        """
        try:
            # Issue HEAD request to check Content-Type
            head_response = await client.head(url, follow_redirects=True, timeout=DEFAULT_TIMEOUT)
            
            # Check response status
            if head_response.status_code >= 400:
                logger.debug(
                    "rvu.scraper.validation.failed",
                    url=url,
                    reason=f"HTTP {head_response.status_code}",
                )
                return False, None, None
            
            content_type = head_response.headers.get('content-type', '').lower()
            content_length = head_response.headers.get('content-length')
            size_bytes = int(content_length) if content_length else None
            content_disposition = head_response.headers.get('content-disposition', '').lower()
            
            # Validate it's actually a file, not HTML
            file_indicators = [
                'application/zip' in content_type,
                'application/octet-stream' in content_type,
                'text/plain' in content_type,
                'text/csv' in content_type,
                'application/vnd.ms-excel' in content_type or 'spreadsheet' in content_type,
                content_disposition.startswith('attachment'),
            ]
            
            is_html = 'text/html' in content_type or 'text/xhtml' in content_type
            is_valid_file = any(file_indicators) and not is_html
            
            # Validate reasonable file size: reject suspiciously small files (<1MB)
            if is_valid_file and size_bytes is not None:
                if size_bytes < 1_000_000:
                    logger.debug("rvu.scraper.validation.failed", url=url, reason="File size < 1MB (likely HTML)")
                    is_valid_file = False
                elif size_bytes > 1_000_000_000:  # 1GB limit
                    logger.warning(
                        "rvu.scraper.validation.suspicious_size",
                        url=url,
                        size_bytes=size_bytes,
                    )

            # If HEAD was inconclusive, try a GET to check headers after redirects
            if not is_valid_file:
                try:
                    get_response = await client.get(url, follow_redirects=True, timeout=DEFAULT_TIMEOUT)
                    ct2 = get_response.headers.get('content-type', '').lower()
                    cd2 = get_response.headers.get('content-disposition', '').lower()
                    cl2 = get_response.headers.get('content-length')
                    sz2 = int(cl2) if cl2 else None
                    is_html2 = 'text/html' in ct2 or 'text/xhtml' in ct2
                    indicators2 = [
                        'application/zip' in ct2,
                        'application/octet-stream' in ct2,
                        cd2.startswith('attachment'),
                    ]
                    if any(indicators2) and not is_html2 and (sz2 is None or sz2 >= 1_000_000):
                        return True, ct2 or content_type, sz2 or size_bytes
                except Exception:
                    pass
            
            if is_valid_file:
                logger.debug(
                    "rvu.scraper.validation.passed",
                    url=url,
                    content_type=content_type,
                    size_bytes=size_bytes,
                )
                return True, content_type, size_bytes
            else:
                logger.debug(
                    "rvu.scraper.validation.rejected_html",
                    url=url,
                    content_type=content_type,
                )
                return False, content_type, size_bytes
                
        except Exception as exc:
            logger.warning(
                "rvu.scraper.validation.error",
                url=url,
                error=str(exc),
            )
            # On validation error, allow the URL through but log it
            return True, None, None

    # ------------------------------------------------------------------ #
    # Utility helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _nearest_heading(element: Tag) -> Optional[str]:
        current = element
        for _ in range(5):
            current = current.parent  # type: ignore[assignment]
            if current is None:
                break
            if isinstance(current, Tag) and current.name and current.name.lower().startswith("h"):
                return current.get_text(" ", strip=True)
        return None

    def _normalize_url(self, href: str) -> str:
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return urljoin(self.base_url, href)
        return urljoin(self.rvu_page_url, href)

    @staticmethod
    def _parse_release_identifier(value: str) -> Optional[Tuple[int, str, Optional[str]]]:
        match = re.search(r"rvu(\d{2,4})([A-Z]{1,2})", value, flags=re.IGNORECASE)
        if not match:
            return None

        year_fragment = match.group(1)
        suffix = match.group(2).upper()

        year = int(year_fragment[-2:])
        year += 2000 if year < 70 else 1900  # Supports legacy years if needed.

        quarter = suffix[0]
        revision = suffix[1:] if len(suffix) > 1 else None

        return year, quarter.upper(), revision.upper() if revision else None

    @staticmethod
    def _is_supported_asset(href: str) -> bool:
        parsed = urlparse(href)
        path = parsed.path.lower()
        return any(path.endswith(ext) for ext in SUPPORTED_EXTENSIONS)

    @staticmethod
    def _extract_content_metadata(anchor: Tag, context_text: str) -> Tuple[Optional[str], Optional[str]]:
        candidate_texts = [
            anchor.get("data-file-type"),
            anchor.get("type"),
            context_text,
        ]

        content_type = None
        file_size = anchor.get("data-file-size")

        for text in candidate_texts:
            if not text:
                continue
            content_match = re.search(r"Content Type:\s*([A-Za-z0-9/\-+.]+)", text)
            size_match = re.search(r"File Size:\s*([^\s;]+(?:\s?[A-Za-z]+)?)", text)

            if content_match and not content_type:
                content_type = content_match.group(1).strip()
            if size_match and not file_size:
                file_size = size_match.group(1).strip()

        return content_type, file_size

    @staticmethod
    def _extract_posted_date(
        display_text: str,
        context_text: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        combined = f"{display_text} {context_text}"
        match = re.search(r"(Updated|Posted)\s+(\d{2})/(\d{2})/(\d{4})", combined, flags=re.IGNORECASE)
        if not match:
            return None, None

        month, day, year = match.group(2), match.group(3), match.group(4)
        try:
            iso_date = datetime.strptime(f"{month}/{day}/{year}", "%m/%d/%Y").date().isoformat()
        except ValueError:
            return None, None

        return iso_date, match.group(0)

    @staticmethod
    def _build_version(link: RVUDetailLink, posted_at: Optional[str]) -> str:
        revision = link.revision or ""
        version = f"{link.year}{link.quarter.upper()}{revision.upper()}"
        if posted_at:
            version = f"{version}-{posted_at}"
        return version

    @staticmethod
    def _build_filename(
        link: RVUDetailLink,
        download_url: str,
        posted_at: Optional[str],
        display_name: str,
    ) -> str:
        ext = Path(urlparse(download_url).path).suffix or ".zip"
        base = link.release_id

        if posted_at:
            base = f"{base}-{posted_at.replace('-', '')}"
        else:
            normalized_name = re.sub(r"[^A-Za-z0-9]+", "-", display_name).strip("-").lower()
            if normalized_name:
                base = f"{base}-{normalized_name}"

        return f"{base}{ext}"

    @staticmethod
    def _deduplicate_files(files: Iterable[RVUFileInfo]) -> List[RVUFileInfo]:
        seen: Dict[str, RVUFileInfo] = {}
        for file_info in files:
            seen.setdefault(file_info.url, file_info)
        return list(seen.values())

    def _build_manifest(
        self,
        files: List[RVUFileInfo],
        start_year: int,
        end_year: int,
    ) -> DiscoveryManifest:
        manifest_files = [file_info.to_manifest_entry() for file_info in files]

        metadata = {
            "scraper_version": SCRAPER_VERSION,
            "discovery_method": "cms_rvu_scraper",
            "total_files": len(manifest_files),
        }

        license_info = {
            "name": "CMS Open Data",
            "url": "https://www.cms.gov/About-CMS/Agency-Information/Aboutwebsite/Privacy-Policy",
            "attribution_required": True,
        }

        manifest = DiscoveryManifest.create(
            source="cms_rvu",
            source_url=self.rvu_page_url,
            discovered_from=self.rvu_page_url,
            files=manifest_files,
            metadata=metadata,
            license_info=license_info,
            start_year=start_year,
            end_year=end_year,
            default_content_type="application/zip",
        )

        manifest.latest_only = True
        return manifest

    # ------------------------------------------------------------------ #
    # Deprecated download helpers (kept for backward compatibility)
    # ------------------------------------------------------------------ #
    async def download_all_files(
        self,
        files: Iterable[RVUFileInfo],
        max_concurrent: int = 4,
    ) -> List[Dict[str, Any]]:
        """
        Download the provided RVU files.

        Note: Downloading is optional and typically executed only after approval.
        """

        semaphore = asyncio.Semaphore(max_concurrent)
        headers = {"User-Agent": USER_AGENT}

        async def download_single(file_info: RVUFileInfo) -> Dict[str, Any]:
            async with semaphore:
                return await self._download_single_file(file_info, headers=headers)

        tasks = [download_single(file_info) for file_info in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        normalized: List[Dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception):
                normalized.append({"status": "failed", "error": str(result)})
            else:
                normalized.append(result)
        return normalized

    async def _download_single_file(
        self,
        file_info: RVUFileInfo,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers=headers) as client:
            logger.info("rvu.scraper.download.start", filename=file_info.filename, url=file_info.url)

            response = await client.get(file_info.url)
            response.raise_for_status()

            data = response.content
            checksum = hashlib.sha256(data).hexdigest()

            file_path = self._download_path_for(file_info)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(data)

            file_info.size_bytes = len(data)
            file_info.checksum = checksum
            file_info.last_modified = datetime.now(timezone.utc)

            logger.info(
                "rvu.scraper.download.complete",
                filename=file_info.filename,
                bytes=len(data),
                checksum=checksum[:12],
            )

            return {
                "status": "success",
                "file_info": file_info,
                "file_path": str(file_path),
                "size_bytes": len(data),
                "checksum": checksum,
                "downloaded_at": file_info.last_modified.isoformat(),
            }

    def _download_path_for(self, file_info: RVUFileInfo) -> Path:
        year_dir = self.output_dir / "downloads" / str(file_info.year)
        return year_dir / file_info.filename

    def generate_manifest(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:  # pragma: no cover
        raise NotImplementedError(
            "Use the discovery manifest emitted during scrape_rvu_files()."
        )


async def main() -> None:  # pragma: no cover - convenience CLI
    scraper = CMSRVUScraper()
    files = await scraper.scrape_rvu_files(start_year=2024, end_year=2025)
    print(f"Discovered {len(files)} files. Latest manifest: {scraper.last_manifest_path}")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
