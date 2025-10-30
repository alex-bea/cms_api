# RVU Scraper - Changes Needed

## Summary
Create new RVU Scraper PRD and update existing documentation to reflect the refactored implementation.

---

## Changes Required

### 1. CREATE NEW FILE: `prds/PRD-rvu-scraper-prd-v1.0.md`

**Template:** Follow `prds/PRD-opps-scraper-prd-v1.0.md` structure

**Content to include:**

#### Section 0: Overview
- Two-hop discovery pattern (landing → detail → download)
- Discovery-only behavior (downloads optional behind approval gate)
- Validation guards (HEAD requests, content-type checks)
- DIS-compliant manifests

#### Section 1: Goals & Non-Goals
- **Goals:**
  - Discover quarterly A/B/C/D releases + revisions (e.g., RVU25AR)
  - Validate download URLs before acceptance
  - Emit DiscoveryManifest with enriched metadata
  - Support historical backfill (2003-2025)
- **Non-Goals:**
  - No disclaimer handling (unlike OPPS)
  - No automatic downloads (approval required)

#### Section 2: Scope
- **Quarterly cadence:** RVU24A, RVU24B, RVU24C, RVU24D
- **Revisions:** RVU24AR, RVU24BR (correction packages)
- **File formats:** ZIP, TXT, CSV, XLSX
- **Date patterns:** Extract and sanitize `MM/DD/YYYY` dates

#### Section 3: Discovery → Validation → Manifest Flow
1. **Landing Page Discovery:**
   - Navigate to PFS Relative Value Files page
   - Extract detail page links (`/rvu25a`, `/rvu24b`, etc.)
   - Parse year, quarter, revision from link text/href
   - Validate year range (default: 2003-2025)

2. **Detail Page Resolution:**
   - Follow each detail page link
   - Extract download URLs from page
   - Parse update/post dates from anchor text
   - Sanitize filenames (remove slashes, normalize dates)

3. **Validation Guards:**
   - Issue HEAD request to verify URL
   - Check Content-Type header (reject HTML)
   withhold file size (reject 0 bytes, warn on >1GB)
   - Enrich metadata with validated values

4. **Manifest Generation:**
   - Create DiscoveryManifest with all file entries
   - Include: URL, filename, content_type, size_bytes
   - Metadata: version, posted_at, detail_url, display_name
   - Compare with previous manifest for change detection
   - Save to `data/cms_rvu/manifests/`

#### Section 4: Batch Model
- **Release ID:** `rvu{YY}{quarter}{revision}`
  - Examples: `rvu25a`, `rvu24ar`
- **Version string:** `{year}{quarter}{revision}-{posted_date}`
  - Example: `2025A-2025-01-10`
- **Change detection:** Compare manifests to detect updates

#### Section 5: Mandatory Files (for complete release)
- PPRRVU file (main RVU data)
- GPCI file (geographic adjustments)
- OPPSCap file (imaging caps)
- ANES file (anesthesia conversion factors)
- LocalityCounty file (geography mappings)

#### Section 6: File Handling
- **Supported extensions:** `.zip`, `.csv`, `.txt`, `.xlsx`, `.xls`
- **Validation:** Content-Type and file size checks
- **Concurrency:** Max 4 concurrent requests (semaphore)
- **Retry policy:** TODO - Add from OPPS pattern
- **User-Agent:** `DIS-RVU-Scraper/2.0 (+ops@yourco.com)`

#### Section 7: Manifest Requirements
- **Format:** DiscoveryManifest (JSONL)
- **Fields:**
  - `source`: "cms_rvu"
  - `source_url`: Landing page URL
  - `discovered_at`: ISO timestamp
  - `files`: Array of file entries with metadata
  - `scraper_version`: "2.0.0"
- **Storage:** `data/cms_rvu/manifests/cms_rvu_manifest_TIMESTAMP.jsonl`

#### Section 8: Observability
- **Logs (structured):**
  - `rvu.scraper.discover.start`
  - `rvu.scraper.detail_links`
  - `rvu.scraper.validation.passed`
  - `rvu.scraper.validation.rejected_html`
  - `rvu.scraper.file_rejected`
  - `rvu.scraper.discover.complete`
- **Metrics:** TODO - Add discovery counts, validation stats, throughput
- **Alerts:** TODO - Add validation failures, HTML rejections, layout drift

#### Section 9: Testing
- Unit tests with mocked HTML fixtures
- Equivalent tests for all core methods
- Performance requirement: <5 minutes for 3 years

#### Section 10: Data Flow
- Scraper → DiscoveryManifest
- Optional: HistoricalDataManager downloads files
- RVUIngestor consumes manifests
- Final destination: PostgreSQL database
- API exposure: FastAPI endpoints

#### Section 11: Failure Modes
- URL path changes
- Filename format changes
- HTML structure changes
- Layout drift warnings

---

### 2. UPDATE: `prds/STD-scraper-prd-v1.0.md`

**Line 379 - Update RVU row:**
```
| **RVU** | `CMSRVUScraper` | `/pfs-relative-value-files` | Two-hop detail navigation | ZIP/TXT/CSV/XLSX | Quarterly | ✅ Implemented v2.0 |
```

**Add details to Section 23.3.2:**
- Update discovery strategy: "Two-hop navigation (landing → detail)"
- Add validation guards: Content-Type and size checks
- Add version: v2.0 (validated, enhanced metadata)
- Update manifest location path

---

### 3. UPDATE: `CHANGELOG.md`

**Add entry:**
```markdown
### Changed - [Date]

#### cms_pricing/ingestion/scrapers/cms_rvu_scraper.py

- **BREAKING:** Refactored to discovery-only behavior (downloads optional)
- **Added:** Two-hop discovery pattern (landing page → detail page → download URL)
- **Added:** Validation guards (HEAD requests to verify content types)
- **Added:** Automatic metadata enrichment (content-type, size_bytes from HTTP headers)
- **Added:** Filename sanitization (date normalization, slash removal)
- **Added:** DiscoveryManifest integration (DIS-compliant)
- **Changed:** Now emits manifests instead of downloading files by default
- **Deprecated:** `download_files()`, `download_all_files()` (backward compatible adapter added)

**Impact:**
- Scraper no longer downloads files automatically
- HistoricalDataManager handles optional downloads
- RVUIngestor updated to consume DiscoveryManifest
- All discovered files validated via HEAD requests
- Production-ready with validation guards

**Testing:**
- 4 unit tests added (all passing)
- Smoke tests passing against live CMS pages
- 13 files discovered and validated successfully
```

---

### 4. UPDATE: `prds/DOC-master-catalog-prd-v1.0.md`

**Add RVU Scraper entry:**
```markdown
#### RVU Scraper
- **File:** `prds/PRD-rvu-scraper-prd-v1.0.md`
- **Implementation:** `cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`
- **Version:** 2.0.0
- **Status:** Implemented
- **Dependencies:** DiscoveryManifest infrastructure
- **Consumers:** HistoricalDataManager, RVUIngestor
```

---

## Implementation vs Documentation Gap

### Already Implemented ✅ (25 items)
- Two-hop discovery
- Validation guards
- DiscoveryManifest integration
- Filename sanitization
- Metadata enrichment
- Quarterly pattern detection
- Revision handling
- Unit tests
- Smoke tests

### Needs Documentation ⚠️ (20 items)
- Retry policy (from OPPS)
- Change detection (checksum drift)
- Mandatory file requirements
- Observability metrics/alerts
- Scheduling workflow
- Failure mode runbooks
- VCR fixtures
- Performance benchmarks

### Needs Implementation ❌ (5 items)
- Retry policy (exponential backoff)
- GitHub Actions workflow
- VCR fixtures
- Scheduled discovery job
- Dual manifest tracking

---

## Priority Order

1. **Create PRD** - Document existing implementation
2. **Update CHANGELOG** - Record breaking changes
3. **Update STD-scraper PRD** - Mark RVU as v2.0
4. **Add retry policy** - Implement missing feature
5. **Add observability metrics** - Complete logging
6. **Create GitHub workflow** - Automated discovery
7. **Add VCR fixtures** - Offline testing
