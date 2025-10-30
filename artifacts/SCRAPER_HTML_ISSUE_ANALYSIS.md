# Why Scraper Got HTML Instead of Data Files

**Date:** 2025-10-28  
**Issue:** Scraper downloaded HTML pages instead of actual data files

## The Problem 🔍

### What Happened
```
Pipeline executed successfully
Downloaded 4 files from CMS:
  - RVU25A (189704 bytes) - HTML page
  - RVU25B (189704 bytes) - HTML page  
  - RVU25C (189704 bytes) - HTML page
  - RVU25D (189706 bytes) - HTML page

Result: 0 records processed
```

### Root Cause

**The scraper is parsing page names, not actual download links**

Looking at the scraper code in `cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`:

**Lines 186-227:** `_extract_file_links()`
```python
def _extract_file_links(self, soup: BeautifulSoup, start_year: int, end_year: int):
    # Looks for links containing "rvu" in text or href
    if 'rvu' in text.lower() or 'rvu' in href.lower():
        file_info = self._parse_rvu_link(href, text, start_year, end_year)
```

**The issue:** The scraper is finding page links (e.g., "RVU25A" page) but not the actual ZIP file download links on those pages.

**Example:**
- ✅ Found link: `<a href="/pfs-relative-value-files/rvu25a">RVU25A</a>`
- ❌ This is a **page**, not a download link
- ✅ Should find: `<a href="/files/zip/rvu25a.zip">Download ZIP</a>`

## Why Local Files Were Ignored

The pipeline always runs the scraper by default and doesn't have a "use local files" mode built in.

The ingestor flow is:
1. `discover()` → Runs scraper
2. `land()` → Downloads from scraper results
3. Never checks for local files

## The Fix 🛠️

### Option 1: Improve Scraper (Recommended)

Fix the scraper to follow links and find actual download files:

```python
async def _scrape_rvu_files_with_downloads(self, soup):
    """Follow page links and find actual ZIP files"""
    rvu_files = []
    
    # Find RVU page links
    page_links = soup.find_all('a', href=re.compile(r'rvu25[a-d]', re.I))
    
    for page_link in page_links:
        page_url = page_link['href']
        
        # Follow the page link
        page_response = await client.get(page_url)
        page_soup = BeautifulSoup(page_response.content, 'html.parser')
        
        # Now find actual ZIP file links on the page
        zip_links = page_soup.find_all('a', href=re.compile(r'\.zip$'))
        for zip_link in zip_links:
            # Found actual download link!
            rvu_files.append(...)
```

### Option 2: Bypass Scraper for Local Testing

Add a flag to use provided files instead of scraping:

```python
class RVUIngestor:
    def __init__(self, use_local_files: bool = False, local_manifest: str = None):
        self.use_local_files = use_local_files
        self.local_manifest = local_manifest
    
    async def discover(self):
        if self.use_local_files:
            # Load files from local manifest
            return self._load_local_manifest()
        else:
            # Use scraper (current behavior)
            return dogs await self.scraper.scrape_rvu_files()
```

### Option 3: Manual File Provision

For testing, manually provide files to the pipeline:

```python
# Create SourceFile objects from local files
source_files = [
    SourceFile(
        url=f"file://{path_to_file}",
        filename="PPRRVU25_JAN.txt",
        ...
    )
    for file in sample_data_dir.glob("*.txt")
]

# Pass directly to pipeline
result = await ingestor._land_stage(release_id, batch_id, source_files)
```

## Recommendation

**For immediate testing: Use Option 3 (Manual File Provision)**

Create a simple test script that:
1. Points directly to `sample_data/rvu25a/` files
2. Bypasses the scraper
3. Runs the pipeline with local files
4. Loads data into database

This validates the pipeline works before fixing the scraper.

## Expected Outcome After Fix

With fixed scraper or manual provision:
- ✅ Files downloaded (actual ZIP/TXT, not HTML)
- ✅ Files parsed correctly
- ✅ Records extracted: ~10,000+ RVU items
- ✅ Data loaded to database
- ✅ API endpoints return real data

## Files to Update

1. **`cms_rvu_scraper.py`** - Fix link parsing to find actual downloads
2. **`rvu_ingestor.py`** - Add "use local files" mode
3. **Test script** - Create manual provision script for testing

## Priority

🟡 **MEDIUM** - Pipeline works, just needs correct file inputs

This is a scraper improvement, not a pipeline bug.
