#!/usr/bin/env python3
"""
Utility to enumerate CMS RVU landing-page links and discover download URLs.

Usage:
    python tools/collect_rvu_links.py --start-year 2020 --end-year 2025
"""

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, asdict
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


LANDING_URL = "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files"


@dataclass
class RVUDatasetLink:
    text: str
    url: str
    heading: Optional[str]
    context_snippet: Optional[str]


@dataclass
class RVUDownloadLink:
    detail_url: str
    detail_heading: Optional[str]
    download_text: str
    download_url: str
    content_type: Optional[str]
    file_size: Optional[str]


def _normalize_url(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return urljoin("https://www.cms.gov", href)
    return urljoin(LANDING_URL, href)


def _extract_heading(link_tag) -> Optional[str]:
    current = link_tag
    for _ in range(5):
        current = current.parent
        if current is None:
            return None
        if current.name and current.name.startswith("h"):
            return current.get_text(strip=True)
    return None


async def fetch_html(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        response = await client.get(url, timeout=30.0)
        response.raise_for_status()
        return response.text
    except Exception as exc:
        print(f"[collect_rvu_links] Failed to fetch {url}: {exc}", file=sys.stderr)
        return None


def collect_detail_links(landing_html: str, start_year: int, end_year: int) -> List[RVUDatasetLink]:
    soup = BeautifulSoup(landing_html, "html.parser")
    detail_links: List[RVUDatasetLink] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "/pfs-relative-value-files/rvu" not in href.lower():
            continue

        normalized = _normalize_url(href)
        text = anchor.get_text(strip=True)

        year_digits = "".join(ch for ch in text if ch.isdigit())
        if len(year_digits) >= 4:
            year = int(year_digits[:4])
        else:
            year = None

        if year and (year < start_year or year > end_year):
            continue

        heading = _extract_heading(anchor)
        context = anchor.parent.get_text(strip=True) if anchor.parent else None
        detail_links.append(RVUDatasetLink(text=text, url=normalized, heading=heading, context_snippet=context))

    return detail_links


def extract_download_links(detail_html: str, detail_url: str) -> List[RVUDownloadLink]:
    soup = BeautifulSoup(detail_html, "html.parser")
    downloads: List[RVUDownloadLink] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if not any(ext in href.lower() for ext in (".zip", ".txt", ".csv", ".xlsx")):
            continue

        download_url = _normalize_url(href)

        parent = anchor.parent
        heading = _extract_heading(anchor)
        content_type = None
        file_size = None

        if parent and parent.name == "div":
            text = parent.get_text(" ", strip=True)
            if "(" in text and ")" in text:
                meta = text[text.find("(") + 1 : text.find(")")]
                if ";" in meta:
                    parts = [part.strip() for part in meta.split(";")]
                    for part in parts:
                        if part.lower().startswith("file size"):
                            file_size = part.split(":", 1)[-1].strip()
                        if part.lower().startswith("content type"):
                            content_type = part.split(":", 1)[-1].strip()

        downloads.append(
            RVUDownloadLink(
                detail_url=detail_url,
                detail_heading=heading,
                download_text=anchor.get_text(strip=True),
                download_url=download_url,
                content_type=content_type,
                file_size=file_size,
            )
        )

    return downloads


async def main():
    parser = argparse.ArgumentParser(description="Collect RVU landing page and download links.")
    parser.add_argument("--landing-url", default=LANDING_URL, help="Landing page to crawl")
    parser.add_argument("--start-year", type=int, default=2000, help="Earliest year to include")
    parser.add_argument("--end-year", type=int, default=2100, help="Latest year to include")
    parser.add_argument("--output", type=str, default="-", help="Where to write JSON output ('-' for stdout)")
    args = parser.parse_args()

    async with httpx.AsyncClient(headers={"User-Agent": "RVU-Link-Collector/1.0"}) as client:
        landing_html = await fetch_html(client, args.landing_url)
        if not landing_html:
            sys.exit(1)

        detail_links = collect_detail_links(landing_html, args.start_year, args.end_year)
        results = {
            "landing_url": args.landing_url,
            "detail_links": [asdict(link) for link in detail_links],
            "downloads": [],
        }

        for link in detail_links:
            html = await fetch_html(client, link.url)
            if not html:
                continue
            downloads = extract_download_links(html, link.url)
            results["downloads"].extend(asdict(download) for download in downloads)

    output = json.dumps(results, indent=2)

    if args.output == "-" or not args.output:
        print(output)
    else:
        with open(args.output, "w") as fh:
            fh.write(output)


if __name__ == "__main__":
    asyncio.run(main())
