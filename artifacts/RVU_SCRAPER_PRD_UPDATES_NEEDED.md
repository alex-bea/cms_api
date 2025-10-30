# RVU Scraper PRD Updates - Comprehensive List

## Status
**No RVU Scraper PRD currently exists.** Need to create `prds/PRD-rvu-scraper-prd-v1.0.md` following the OPPS scraper pattern.

---

## 1. HIGH PRIORITY - Core Discovery Process

### From RVU Scraper Implementation
- ✅ **Two-hop discovery pattern:** Landing page → Detail page → Download URL
- ✅ **Quarterly releases (A/B/C/D):** RVU24A, RVU24B, RVU24C, RVU24D pattern
- ✅ **Revision detection:** RVU24AR for corrections/revisions
- ✅ **Multiple formats:** ZIP, TXT, CSV, XLSX per release
- ✅ **Date sanitization:** Convert URLs like `/files/zip/rvu25a-updated-01/10/2025.zip` to `rvu25a-20250110.zip`

### From OPPS Scraper Learnings
- 🔄 **Tiered disclaimer handling:** Not needed for RVU (CMS doesn't require AMA acceptance)
- ⚠️ **Change detection:** OPPS tracks checksum drift for "CMS Edit" detection - should add for RVU
- ✅ **Dual manifests:** Discovery manifest (pre-download) vs download manifest (post-download)
- ✅ **Provenance tracking:** Track which page each file was discovered from

---

## 2. VALIDATION GUARDS (Implemented but not documented)

### New Features to Document
- **Content-Type validation via HEAD requests:**
  - Issue HEAD request before accepting URLs
  - Reject if `text/html` is returned
  - Accept if `application/zip`, `application/octet-stream`, etc.
  - Validate file size (reject 0 bytes, warn on >1GB)
  
- **Automatic metadata enrichment:**
  - Populate `content_type` from HTTP headers
  - Populate `size_bytes` from `content-length` header
 FIles filenames (sanitize slashes, dates, special chars)
  
### From OPPS Scraper
- ⚠️ **Retry policy:** Should add exponential backoff for 5xx/timeout errors
- ✅ **Concurrency control:** Max 4 concurrent requests with semaphore
- ⚠️ **User-Agent:** Should document `DIS-RVU-Scraper/2.0` for CMS allowlisting

---

## 3. MANIFEST REQUIREMENTS

### DiscoveryManifest Fields (Implemented)
- `source`: "cms_rvu"
- `source_url`: Landing page URL
- `discovered_at`: Timestamp
- `files`: List of file entries with:
  - URL (normalized and absolute)
  - Filename (sanitized)
  - Content type (validated)
  - Size bytes (validated)
  - Metadata (version, posted_at, detail_url, display_name)

### From OPPS Scraper Learnings
- ⚠️ **Change detection metadata:** `changes_detected` flag when manifest differs from previous
- ⚠️ **License info:** Document CMS Open Data license and attribution requirements
- ⚠️ **Scraper version:** Track `scraper_version: "2.0.0"` in metadata

---

## 4. DATA FLOW AFTER DISCOVERY

### Key Differences from OPPS
- **OPPS:** Scraper discovers AND downloads
- **RVU:** Scraper discovers ONLY, downloads are optional (approval gate)

### To Document
- Discovery produces `RVUFileInfo` objects + `DiscoveryManifest`
- HistoricalDataManager can download files (optional)
- RVUIngestor consumes manifests and downloads files
- Downloads save to: `data/historical_rvu/downloads/{year}/{filename}.zip`
- Final destination: PostgreSQL database (via ingestor)

---

## 5. BATCH MODEL & VERSIONING

### RVU-Specific Patterns
- **Release identifier:** `rvu{YY}{quarter}{revision}`
  - Examples: `rvu25a`, `rvu24ar` (A=April, revision=R)
  - Year extraction: Extract 4-digit year from 2-digit pattern (2024 vs 24)
  
- **Version string:** `{year}{quarter}{revision}-{posted_date}`
  - Example: `2025A-2025-01-10` for January 10, 2025 update
  - Allows change detection based on version diffs

### From OPPS Scraper
- ⚠️ **Checksum drift detection:** Track when same filename has different checksum
- ⚠️ **Silent edits:** Promoted revision number (r01 → r02) when checksum changes
- ✅ **Year/quarter/revision tracking:** Implemented in RVU

---

## 6. FILE HANDLING & INTEGRITY

### Implemented Features to Document
- **Supported extensions:** `.zip`, `.csv`, `.txt`, `.xlsx`, `.xls`
- **Content-Type validation:** Already implemented
- **File size validation:** Implemented (0 bytes rejected, >1GB warned)

### From OPPS Scraper Learnings
- ⚠️ **Retry policy:** Should document:
  - 5xx/timeout: 3–5 retries with exponential backoff (1s→3s→9s→20s)
  - 429 rate limit: Honor `Retry-After` header, else backoff 30–60s
  - Reduce concurrency on persistent failures

- ✅ **Concurrency control:** Max 4 concurrent requests documented
- ✅ **User-Agent:** `DIS-RVU-Scraper/2.0 (+ops@yourco.com)` for traceability

---

## 7. STRUCTURAL PRESENCE & QUARANTINE

### RVU-Specific Requirements
- ⚠️ **Mandatory file types:** Should define what makes a "complete" release
  - PPRRVU file (main RVU data)
  - GPCI file (geographic adjustments)
  - OPPSCap file (imaging caps)
  - ANES file (anesthesia CF)
  - LocalityCounty file (geography mappings)

- ⚠️ **Incomplete release handling:** Document when to quarantine vs accept partial

### From OPPS Scraper
- ⚠️ **Quarantine reasons:** Document failure reasons:
  - `missing`: File listed but not found
  - `validation_failed`: Content-Type or size checks failed
  - `disclaimer_failed`: Not applicable to RVU
  - `checksum_mismatch`: Checksum drift detected

---

## 8. OBSERVABILITY & MONITORING

### Implemented Logging to Document
- `rvu.scraper.discover.start`: Discovery begins
- `rvu.scraper.detail_links`: Detail links found
- `rvu.scraper.validation.passed`: URL validated successfully
- `rvu.scraper.validation.rejected_html`: HTML page rejected
- `rvu.scraper.validation.failed`: Validation error (0 bytes, etc.)
- `rvu.scraper.file_rejected`: File failed validation
- `rvu.scraper.discover.complete`: Discovery finished

### From OPPS Scraper - Metrics to Add
- ⚠️ **Discovery counts:** #links found, #files discovered, #validated
- ⚠️ **Validation stats:** Pass/fail rates by reason
- ⚠️ **Content-Type distribution:** What types of files are we finding
- ⚠️ **Layout drift:** Warnings when HTML structure changes
- ⚠️ **Throughput:** Files discovered per second

### Dashboards/Alerts Needed
- ⚠️ **Validation failures:** Alert if >X% of URLs fail validation
- ⚠️ **HTML rejections:** Alert if any HTML pages accepted
- ⚠️ **Zero files discovered:** Alert if discovery finds no files
- ⚠️ **Layout drift warnings:** Alert when HTML structure changes

---

## 9. TESTING & ACCEPTANCE CRITERIA

### Implemented Tests to Document
- ✅ Unit tests with mocked HTML fixtures
- ✅ Landing page link extraction
- ✅ Detail page download extraction
- ✅ Filename sanitization
- ✅ Content-Type validation
- ✅ HTML rejection logic

### From OPPS Scraper
- ⚠️ **Integration tests:** Live CMS pages (with VCR fixtures)
- ⚠️ **Regression tests:** Historical manifest comparison
- ⚠️ **Performance tests:** Should complete in <5 minutes for 3 years of data
- ⚠️ **Coverage requirements:** ≥90% test coverage

### Acceptance Criteria Needed
- ✅ Correctly discovers A/B/C/D quarters for 2023-2025
- ✅ Extracts revision indicators (R suffix)
- ⚠️ Produces valid DiscoveryManifest
- ⚠️ Validates all URLs (no HTML accepted)
- ⚠️ Sanitizes filenames correctly
- ⚠️ Enriches metadata (content-type, size)
- ⚠️ Handles edge cases (missing dates, malformed URLs)

---

## 10. SCHEDULING & TRIGGERS

### Current State
- ⚠️ No scheduled discovery job
- ✅ Manual trigger via `test_cms_rvu_scraper.py`
- ✅ HistoricalDataManager for manual discovery

### From OPPS Scraper
- ⚠️ **Weekly scheduled crawl:** GitHub Actions workflow
- ⚠️ **Manual dispatch:** CLI or webhook for ad-hoc runs
- ⚠️ **Change detection:** Compare manifests to trigger notifications

---

## 11. FAILURE MODES & RUNBOOK HOOKS

### RVU-Specific Issues
- ⚠️ **URL path changes:** CMS moves from `/files/zip/` to different location
- ⚠️ **Filename format changes:** Date format changes from `MM/DD/YYYY` to ISO
- ⚠️ **HTML structure changes:** Detail page layout changes

### From OPPS Scraper
- ⚠️ **DOM changes:** Use strict selectors first, heuristic fallback
- ⚠️ **Layout drift:** Emit warning and continue with reduced concurrency
- ⚠️ **Silent edits:** Checksum drift with same filename → track as revision
- ⚠️ **Rate limits:** Backoff and lower concurrency; persist, rotate IPs

---

## 12. REFERENCE DATA & CROSS-CUTTING CONCERNS

### RVU-Specific
- ⚠️ **Release dependency:** RVU bundles contain multiple files (PPRRVU, GPCI, etc.)
- ⚠️ **File relationships:** GPCI needs locality mapping, OPPSCap needs OPPS data
- ⚠️ **Effective dates:** Document how dates are extracted from filenames/metadata

### From OPPS Scraper
- ⚠️ **Dual manifest rationale:** Discovery shows intent, download shows reality
- ⚠️ **Batch quarantine:** If any file fails, quarantine the entire batch
- ⚠️ **SLOs:** Detect updates within ≤3 days, deliver to ingestion within ≤1 day

---

## 13. DOCUMENTATION UPDATES NEEDED

### Files to Create/Update
1. **NEW:** `prds/PRD-rvu-scraper-prd-v1.0.md` (create from scratch)
2. **UPDATE:** `prds/STD-scraper-prd-v1.0.md` (update RVU row in scraper matrix)
3. **UPDATE:** `CHANGELOG.md` (document refactor and validation guards)
4. **UPDATE:** `prds/DOC-master-catalog-prd-v1.0.md` (add RVU scraper entry)

### Content to Include
- Two-hop discovery flow diagram
- Validation guard flowchart
- Manifest schema example
- Data flow diagram (discovery → download → ingestion → API)
- Failure mode runbook
- Testing strategy
- Performance benchmarks

---

## 14. KEY DIFFERENCES FROM OPPS SCRAPER

| Aspect | OPPS Scraper | RVU Scraper |
|--------|--------------|-------------|
| **Download behavior** | Discovers AND downloads | Discovers ONLY (downloads optional) |
| **Disclaimer handling** | Tiered strategy (3 tiers) | Not needed |
| **Batch model** | `opps_YYYYqN_rNN` | `rvu{YY}{Q}{revision}` |
| **Mandatory files** | Addendum A + B | PPRRVU + GPCI + OPPSCap + ANES + Locality |
| **Release cadence** | Quarterly (Q1-Q4) | Quarterly (A-D) |
| **File size** | Smaller (1-5MB) | Larger (3-4MB ZIPs) |
| **Format variety** | CSV/XLSX/TXT | ZIP/TXT/CSV |
| **Version tracking** | Yes (rNN increments) | Yes (revision suffix) |

---

## 15. PRIORITIZED ACTION ITEMS

### P0 - Critical
1. ✅ Create `PRD-rvu-scraper-prd-v1.0.md`
2. ✅ Document two-hop discovery pattern
3. ✅ Document validation guards (HEAD requests, content-type checks)
4. ✅ Document discovery-only behavior (downloads optional)

### P1 - High Priority
5. ⚠️ Document retry policy (from OPPS pattern)
6. ⚠️ Add change detection and checksum drift tracking
7. ⚠️ Define mandatory file requirements
8. ⚠️ Add observability metrics and alerts

### P2 - Medium Priority
9. ⚠️ Add GitHub Actions workflow for scheduled discovery
10. ⚠️ Create VCR fixtures for offline testing
11. ⚠️ Document performance benchmarks
12. ⚠️ Add runbook for common failure modes

### P3 - Nice to Have
13. ⚠️ Add disclaimer handling (even though not needed now)
14. ⚠️ Add theatless browser support (future-proofing)
15. ⚠️ Document multi-file release dependencies

---

## Summary

**Total items to document:** ~45 across 15 categories

**Status:**
- ✅ Implemented and tested: ~25 items
- ⚠️ Need to document: ~20 items from OPPS learning
- ❌ Not yet implemented: ~5 items (retry policy, scheduled discovery, VCR fixtures)

**Recommendation:** Create comprehensive RVU scraper PRD following OPPS pattern, documenting all implemented features plus planned enhancements.
