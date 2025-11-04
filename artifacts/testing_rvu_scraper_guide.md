# Testing the RVU Scraper

**Status:** ✅ Complete  
**Last Updated:** 2025-11-03  
**Scraper:** `CMSRVUScraper` in `cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`

---

## 📋 Overview

The RVU scraper (`CMSRVUScraper`) discovers and downloads RVU files from the CMS website. This guide shows how to test it at different levels: unit tests (mocked), integration tests, and real-world testing against the actual CMS website.

---

## 🧪 Test Types

### 1. Unit Tests (Fast, Mocked HTML)

**File:** `tests/scrapers/test_rvu_scraper_methods.py`

**What it tests:**
- HTML parsing logic
- Detail link extraction
- Download URL validation
- File metadata extraction
- Uses mocked HTML and HTTP responses (no real network calls)

**Run:**
```bash
# In Docker (recommended)
docker compose exec api pytest tests/scrapers/test_rvu_scraper_methods.py -xvs

# Or locally (if dependencies work)
pytest tests/scrapers/test_rvu_scraper_methods.py -xvs
```

**Tests:**
- ✅ `test_extract_detail_links_parses_year_quarter_revision()` - Landing page parsing
- ✅ `test_extract_downloads_from_detail_builds_metadata()` - Detail page parsing
- ✅ `test_validate_download_url_accepts_zip()` - ZIP validation (mocked)
- ✅ `test_validate_download_url_rejects_html()` - HTML rejection (mocked)

**Example:**
```python
def test_extract_detail_links_parses_year_quarter_revision(scraper):
    """Test landing page HTML parsing"""
    landing_html = """
    <section>
      <h2>2025 Releases</h2>
      <ul>
        <li><a href="/.../rvu25a">RVU25A</a></li>
        <li><a href="/.../rvu25ar">RVU25AR</a></li>
      </ul>
    </section>
    """
    
    detail_links = scraper._extract_detail_links(landing_html, start_year=2024, end_year=2025)
    assert len(detail_links) == 3
    assert all(link.year in [2024, 2025] for link in detail_links)
```

---

### 2. Integration Tests (Scraper + Ingestor)

**File:** `tests/ingestors/test_rvu_ingestor_e2e.py`

**What it tests:**
- Scraper discovery integration with ingestor
- File discovery workflow
- Mocked scraper responses (doesn't hit real CMS)

**Run:**
```bash
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_scraper_discovery_integration \
  -xvs
```

**What it verifies:**
- Scraper returns `RVUFileInfo` objects
- Files have required metadata (url, filename, size_bytes, etc.)
- Discovery completes within SLO (≤ 30 seconds)

---

### 3. Real-World Testing (Actual CMS Website)

**⚠️ WARNING:** This hits the real CMS website. Use sparingly and be respectful of rate limits.

#### Option A: Quick Discovery Test

Test if scraper can discover files from CMS website:

```bash
# Run in Docker
docker compose exec api python -c "
import asyncio
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
import tempfile

async def test_discovery():
    scraper = CMSRVUScraper(output_dir=tempfile.mkdtemp())
    
    print('🔍 Discovering RVU files from CMS website...')
    files = await scraper.scrape_rvu_files(2025, 2025)
    
    print(f'✅ Found {len(files)} files:')
    for f in files[:5]:  # Show first 5
        print(f'  - {f.filename} ({f.file_type}) - {f.size_bytes} bytes')
        print(f'    URL: {f.url}')
        print(f'    Year: {f.year}, Quarter: {f.quarter}')
        print()

asyncio.run(test_discovery())
"
```

#### Option B: Using the CLI

The scraper has a CLI interface for easy testing:

```python
# Create a test script: tests/ingestors/scripts/test_scraper_real.py
#!/usr/bin/env python3
"""Test RVU scraper against real CMS website"""
import asyncio
import sys
from pathlib import Path
import tempfile

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
from cms_pricing.ingestion.scrapers.cli import ScraperCLI

async def test_scraper_discovery():
    """Test scraper discovery against real CMS website"""
    output_dir = tempfile.mkdtemp(prefix="rvu_scraper_test_")
    manifest_dir = tempfile.mkdtemp(prefix="rvu_manifests_")
    
    print(f"📂 Output directory: {output_dir}")
    print(f"📂 Manifest directory: {manifest_dir}")
    print()
    
    # Test using ScraperCLI
    cli = ScraperCLI(output_dir, manifest_dir)
    
    print("🔍 Running discovery mode (2025 only)...")
    result = await cli.discovery_mode(
        start_year=2025,
        end_year=2025,
        latest_only=True
    )
    
    print(f"✅ Discovery completed:")
    print(f"   Status: {result['status']}")
    print(f"   Files discovered: {result.get('files_discovered', 0)}")
    print(f"   Manifest path: {result.get('manifest_path', 'N/A')}")
    print()
    
    if result['status'] == 'success' and result.get('files_discovered', 0) > 0:
        print("📋 Discovered files:")
        for i, file_info in enumerate(result.get('files', [])[:5], 1):
            print(f"   {i}. {file_info.get('filename', 'N/A')}")
            print(f"      Type: {file_info.get('file_type', 'N/A')}")
            print(f"      Size: {file_info.get('size_bytes', 0):,} bytes")
            print(f"      URL: {file_info.get('url', 'N/A')[:80]}...")
            print()
    
    return result

if __name__ == "__main__":
    result = asyncio.run(test_scraper_discovery())
    sys.exit(0 if result['status'] == 'success' else 1)
```

**Run:**
```bash
# In Docker
docker compose exec api python tests/ingestors/scripts/test_scraper_real.py

# Or with PYTHONPATH
PYTHONPATH=/app docker compose exec api python tests/ingestors/scripts/test_scraper_real.py
```

#### Option C: Direct Scraper Test

Test the scraper directly without the CLI:

```python
# Create: tests/ingestors/scripts/test_scraper_direct.py
#!/usr/bin/env python3
"""Direct scraper test"""
import asyncio
import sys
from pathlib import Path
import tempfile

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper

async def test_direct():
    """Test scraper directly"""
    output_dir = tempfile.mkdtemp()
    scraper = CMSRVUScraper(output_dir=output_dir)
    
    print("🔍 Discovering files...")
    files = await scraper.scrape_rvu_files(2025, 2025)
    
    print(f"✅ Found {len(files)} files")
    for f in files[:3]:
        print(f"\n📄 {f.filename}")
        print(f"   URL: {f.url}")
        print(f"   Type: {f.file_type}")
        print(f"   Size: {f.size_bytes:,} bytes" if f.size_bytes else "   Size: Unknown")
        print(f"   Year: {f.year}, Quarter: {f.quarter}")
        if f.revision:
            print(f"   Revision: {f.revision}")

if __name__ == "__main__":
    asyncio.run(test_direct())
```

---

## 🔍 Manual Testing Commands

### Quick Discovery Test (Real CMS)

```bash
# Test discovery only (doesn't download)
docker compose exec api python -c "
import asyncio
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
import tempfile

async def test():
    scraper = CMSRVUScraper(output_dir=tempfile.mkdtemp())
    files = await scraper.scrape_rvu_files(2025, 2025)
    print(f'Found {len(files)} files')
    for f in files[:3]:
        print(f'  - {f.filename} ({f.file_type})')

asyncio.run(test())
"
```

### Test with CLI (Full Workflow)

```bash
# Test discovery + download workflow
docker compose exec api python -c "
import asyncio
from cms_pricing.ingestion.scrapers.cli import ScraperCLI
import tempfile

async def test():
    cli = ScraperCLI(
        tempfile.mkdtemp(),
        tempfile.mkdtemp()
    )
    result = await cli.discovery_mode(2025, 2025, latest_only=True)
    print(f'Status: {result[\"status\"]}')
    print(f'Files: {result.get(\"files_discovered\", 0)}')

asyncio.run(test())
"
```

---

## ✅ Verification Checklist

After running tests, verify:

- [ ] **Unit tests pass** - HTML parsing works correctly
- [ ] **Integration tests pass** - Scraper integrates with ingestor
- [ ] **Real discovery works** - Can discover files from CMS website
- [ ] **File metadata correct** - URLs, filenames, sizes are valid
- [ ] **File types detected** - ZIP, CSV, TXT files identified correctly
- [ ] **Year/Quarter parsing** - RVU25A, RVU25B, etc. parsed correctly
- [ ] **Revisions handled** - RVU25AR, RVU25BR detected
- [ ] **HTML pages rejected** - Invalid downloads filtered out
- [ ] **Performance acceptable** - Discovery completes within SLO (≤ 30s)

---

## 🐛 Debugging Tips

### Check scraper logs:

The scraper uses structlog. Enable debug logging:

```python
import structlog
structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
logger = structlog.get_logger()
logger.info("test", message="Logging enabled")
```

### Verify CMS website is accessible:

```bash
# Check if CMS website is reachable
docker compose exec api curl -I https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files

# Check specific RVU page
docker compose exec api curl -I "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files"
```

### Test URL validation:

```python
# Test if a specific URL is valid
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
import asyncio
import httpx

async def test_url():
    scraper = CMSRVUScraper()
    async with httpx.AsyncClient() as client:
        is_valid, content_type, size = await scraper._validate_download_url(
            "https://example.com/rvu25a.zip",
            client
        )
        print(f"Valid: {is_valid}, Type: {content_type}, Size: {size}")

asyncio.run(test_url())
```

### Check discovered files:

```python
# Inspect discovered files
files = await scraper.scrape_rvu_files(2025, 2025)

for f in files:
    print(f"\nFile: {f.filename}")
    print(f"  URL: {f.url}")
    print(f"  Type: {f.file_type}")
    print(f"  Content-Type: {f.content_type}")
    print(f"  Size: {f.size_bytes}")
    print(f"  Year: {f.year}, Quarter: {f.quarter}")
    print(f"  Revision: {f.revision}")
    print(f"  Metadata: {f.metadata}")
```

---

## 🚨 Common Issues

### Issue: "No files discovered"

**Possible causes:**
1. CMS website structure changed
2. Network connectivity issues
3. Year/quarter range too narrow

**Fix:**
```bash
# Test with broader year range
files = await scraper.scrape_rvu_files(2024, 2025)

# Check if landing page is accessible
curl "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files"
```

### Issue: "HTML pages returned instead of ZIP files"

**Cause:** CMS website returns HTML pages instead of direct file downloads.

**Fix:** The scraper has validation logic to reject HTML. Check:
```python
# Verify validation is working
is_valid, content_type, size = await scraper._validate_download_url(url, client)
assert not is_valid  # HTML should be rejected
```

### Issue: "Timeout errors"

**Cause:** CMS website is slow or network issues.

**Fix:** Increase timeout:
```python
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import DEFAULT_TIMEOUT
# DEFAULT_TIMEOUT is 30.0 seconds, can be adjusted in scraper config
```

---

## 📚 Related Documentation

- **Scraper Implementation:** `cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`
- **CLI Interface:** `cms_pricing/ingestion/scrapers/cli.py`
- **Unit Tests:** `tests/scrapers/test_rvu_scraper_methods.py`
- **Integration Tests:** `tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_scraper_discovery_integration`
- **Scraper PRD:** `prds/STD-scraper-prd-v1.0.md`

---

## 🚀 Quick Commands Reference

```bash
# Unit tests (fast, mocked)
docker compose exec api pytest tests/scrapers/test_rvu_scraper_methods.py -xvs

# Integration test (mocked scraper)
docker compose exec api pytest \
  tests/ingestors/test_rvu_ingestor_e2e.py::TestRVUIngestorE2E::test_scraper_discovery_integration \
  -xvs

# Real discovery test (hits CMS website)
docker compose exec api python -c "
import asyncio
from cms_pricing.ingestion.scrapers.cms_rvu_scraper import CMSRVUScraper
import tempfile
async def test():
    scraper = CMSRVUScraper(output_dir=tempfile.mkdtemp())
    files = await scraper.scrape_rvu_files(2025, 2025)
    print(f'Found {len(files)} files')
asyncio.run(test())
"

# Test with CLI
docker compose exec api python tests/ingestors/scripts/test_scraper_real.py
```

---

**Last Updated:** 2025-11-03  
**Scraper Version:** 2.0.0  
**CMS Website:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
