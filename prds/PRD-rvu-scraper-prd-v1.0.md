# RVU Scraper PRD

**Status:** Draft v1.0  
**Owners:** Platform/Data Engineering; Medicare SME (review)  
**Consumers:** RVU ingester, HistoricalDataManager, QA, SRE/Ops  
**Change control:** Follow `STD-scraper-prd-v1.0.md` + ADR for selector changes

For complete index see [Master System Catalog](DOC-master-catalog-prd-v1.0.md unlocking).

Links to standards: Main Scraper Standard PRD → `STD-scraper-prd-v1.0.md` (this RVU PRD inherits all baseline rules; only RVU-specific overrides are defined here).

⸻

## 0. Overview

Scrape CMS RVU (Relative Value Units) PFS Relative Value Files to discover quarterly release artifacts (RVU24A, RVU24B, RVU24C, RVU24D) plus correction packages (RVU24AR), producing DIS-ready manifests with validation guards. Implements two-hop discovery (landing page → detail page → download URL), content-type validation via HEAD requests, automatic metadata enrichment, and discovery-only behavior (downloads optional behind approval gate).

**Status:** Draft v1.0 - Implemented and Tested  
**Owners:** Platform/Data Eng (primary), Medicare SME (review)  
**Consumers:** RVU ingester, HistoricalDataManager, QA, SRE/ops  
**Change control:** Follows Main Scraper Standard PRD; ADR for interface/selector changes

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-scraper-prd-v1.0.md:** Main scraper standard and baseline rules
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns
- **PRD-opps-scraper-prd-v1.0.md:** OPPS scraper PRD (reference for tiered strategies)
- **STD-qa-testing-prd-v1.0.md:** Testing requirements for RVU scraper
- **PRD-rvu-gpci-prd-v0.1.md:** RVU ingester requirements

## Data Classification & Stewardship
- **Classification:** Public CMS artifacts (Internal operational telemetry)  
- **License & Attribution:** CMS RVU files (public domain); acknowledgement required in downstream manifests  
- **Data Owner / Steward:** Platform/Data Engineering (scraper operators) with Medicare SME for content validation  
- **Distribution Policy:** Raw artifacts remain Internal; redistribution must retain CMS attribution

## Ingestion Summary (DIS v1.0)
- **Discovery Cadence:** Manual trigger + scheduled weekly poll; monitors CMS PFS Relative Value Files page for new quarterly releases
- **Release Identifier:** `rvu{YY}{quarter}{revision}` (e.g., `rvu25a`, `rvu24ar`) recorded in discovery manifests  
- **Manifest Requirements:** Emit discovery manifests with fields: URL (normalized), filename (sanitized), content-type (validated), size-bytes, version, posted-at, detail-url, display-name. Discovery runs persist `DiscoveryManifest` under `data/cms_rvu/manifests/` for CI enforcement
- **Storage Layout:** Artifacts persisted under `/raw/rvu/{release_id}/files/*` with `manifest.json` meeting DIS §3.2 requirements; manifests mirrored for ops visibility  
- **Validations & Gates:** Content-Type validation via HEAD requests, file size checks (reject 0 bytes, warn >1GB), HTML page rejection. Failures log warnings but allow URL through for manual triage
- **Hand-off to Ingester:** HistoricalDataManager consumes manifests; optional downloads behind approval gate. RVUIngestor downloads files on-demand
- **SLAs:** Detect updates within ≤3 calendar days of CMS posting; deliver manifests to ingestion within ≤1 business day post-detection  
- **Deviations:** Discovery-only behavior (no automatic downloads); differs from OPPS scraper which downloads files automatically

## Delivery & API Readiness
- **Integration:** HistoricalDataManager consumes discovery manifests; RVUIngestor downloads files from manifests  
- **Observability Hooks:** Metrics/log exports to DIS observability stack; alerts on validation failures, HTML rejections  
- **Security Controls:** Credential-less downloads; operational dashboards gated behind internal auth per **STD-api-security-and-auth-prd-v1.0.md**

⸻

## 1. Goals & Non-Goals

Goals
- Discover all quarterly RVU releases (A/B/C/D) plus corrections (AR/BR/etc.) from landing page, following detail pages to extract actual download URLs.  
- Validate all discovered URLs via HEAD requests to verify Content-Type and file size before acceptance.
- Emit discovery manifests with enriched metadata (dates, versions, content types) in DIS-compliant format.
- Support discovery-only mode (downloads optional behind approval gate).
- Generate sanitized filenames for safe local storage (remove slashes, normalize dates).

Non-Goals
- Parsing/ingestion business rules (covered by the RVU ingester PRD).
- Automatic file downloads (handled by HistoricalDataManager or RVUIngestor).
- Disclaimer handling (CMS doesn't require AMA acceptance for RVU files).

### Acceptance Criteria & Hard Gates
- Accepted artifacts must return binary payloads (`application/zip`, `application/octet-stream`, `text/plain`, `text/csv`, Excel MIME types). Any response that resolves to HTML or reports a `Content-Length` ≤ 1024 bytes is rejected and logged with the originating detail URL.
- Discovery manifests SHALL persist `content_type`, `size_bytes`, and (when present) `sha256` for every file before the run is marked successful. Missing attributes block hand-off to RVUIngestor/HistoricalDataManager.
- The most recent quarterly vintage must surface exactly four accepted files (A/B/C/D). When fewer pass validation, the run raises `rvu.scraper.missing_quarter`, sets `changes_detected=false`, and pauses downstream automation.
- Correction drops (e.g., `RVU25AR`) must include the revision marker and posted date inside the manifest `version` field so ingestors can differentiate the corrected payload from the baseline release.

⸻

## 2. Scope & Key Assumptions
- **Scope:** RVU quarterly releases (RVU24A/RVU24B/RVU24C/RVU24D) plus correction packages (RVU24AR, etc.), all quarters from 2003-2025+. Each quarter's files discovered via two-hop navigation (landing → detail → download URL).  
- **Formats:** ZIP, TXT, CSV, XLSX. ZIP files contain multiple datasets (PPRRVU, GPCI, OPPSCap, ANES, LocalityCounty).
- **Discovery vs Download:** Scraper discovers only; downloading is optional via HistoricalDataManager or RVUIngestor. This separates discovery (pure metadata) from file acquisition (requires approval).
- **Robots & posture:** Ignore robots.txt by policy, implement polite throttling/backoff.

⸻

## 3. Discovery → Validation → Manifest (Control Flow)

### 1. Landing Page Discovery:
- Fetch PFS Relative Value Files page (https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)
- Extract detail page links matching `/pfs-relative-value-files/rvu` pattern
- Parse year, quarter (A/B/C/D), and optional revision (R) from link text/href
- Validate year range (default: 2003-2025)
- Deduplicate links based on URL

### 2. Detail Page Resolution:
- Follow each detail page link (e.g., `/rvu25a`)
- Extract download links with supported extensions (`.zip`, `.txt`, `.csv`, `.xlsx`)
- Parse update/post dates from anchor text (e.g., "Updated 01/10/2025")
- Sanitize filenames: convert `rvu25a-updated-01/10/2025.zip` → `rvu25a-20250110.zip`
- Build version strings: `2025A-2025-01-10` (year + quarter + revision + date)
- Extract metadata from page context (headings, surrounding text)

### 3. Validation Guards (NEW v2.0):
- **HEAD Request:** Issue HEAD request to each discovered URL before accepting
- **Content-Type Check:** Reject if `text/html` or `text/xhtml` returned
- **Accept File Types:** `application/zip`, `application/octet-stream`, `text/plain`, `text/csv`, `application/vnd.ms-excel`, etc.
- **Size Validation:** Reject 0-byte files; warn on files >1GB
- **Metadata Enrichment:** Populate `content_type` and `size_bytes` from HTTP headers
- **Error Handling:** On validation error, allow URL through but log warning (graceful degradation)

### 4. Manifest Generation:
- Create `DiscoveryManifest` with all validated file entries
- Include: URL (normalized), filename (sanitized), content-type (validated), size-bytes (validated)
- Metadata: version, posted-at, detail-url, display-name, release-id
- Compare with previous manifest to detect changes
- Save to `data/cms_rvu/manifests/cms_rvu_manifest_TIMESTAMP.jsonl`
- Track `scraper_version: "2.0.0"` in metadata

⸻

## 4. Release Model & Versioning (Scraper perspective)

- **Release ID:** `rvu{YY}{quarter}{revision}` (e.g., `rvu25a`, `rvu24ar`)
  - Year: Extracted from 2-digit or 4-digit pattern in link text
  - Quarter: A (Jan), B (Apr), C (Jul), D (Oct)
  - Revision: Optional suffix (R for corrections/revisions)
  
- **Version String:** `{year}{quarter}{revision}-{posted_date}`
  - Example: `2025A-2025-01-10` for January 10, 2025 update
  - Allows change detection based on version diffs
  
- **Silent edits (filename unchanged):** TBD - Add checksum drift detection similar to OPPS scraper

⸻

## 5. Data Model

### RVUDetailLink
Represents a quarter detail page discovered from the landing page.

- `url`: Detail page URL
- `text`: Link text
- `year`: 4-digit year
- `quarter`: A/B/C/D
- `revision`: Optional revision suffix (e.g., "R")
- `heading`: Nearest heading from HTML structure
- `context`: Parent element text
- `release_id`: Computed property (e.g., "rvu25a")

### RVUFileInfo
Metadata describing a downloadable artifact discovered for RVU.

- `url`: Download URL (validated)
- `filename`: Sanitized filename
- `content_type`: Validated content type (from HEAD request)
- `size_bytes`: Validated file size (from content-length header)
- `year`: Release year
- `quarter`: Release quarter
- `revision`: Optional revision
- `posted_at`: ISO date from CMS
- `detail_url`: Original detail page URL
- `display_name`: Anchor text from CMS
- `version`: Composite version string
- `metadata`: Additional context (heading, context, posted_label, etc.)
- `checksum`: SHA256 (populated on download)
- `last_modified`: Timestamp (populated on download)

⸻

## 6. File Handling & Integrity

- **Supported types:** ZIP, TXT, CSV, XLSX, XLS. Validate Content-Type via HEAD request.
- **Validation:**
  - Reject if Content-Type is HTML
  - Reject if file size is 0 bytes
  - Warn if file size >1GB
  - Enrich metadata with values from HTTP headers
  
- **Retry policy:** TBD - Add exponential backoff for 5xx/timeout errors (from OPPS pattern)

- **Concurrency & UA:**
  - Max 4 concurrent detail page requests (semaphore)
  - User-Agent: `DIS-RVU-Scraper/2.0 (+ops@yourco.com)` for traceability

⸻

## 7. Manifests, Layouts, & Provenance

- **Paths:**
  - `data/cms_rvu/manifests/` — Discovery manifests
  - `data/historical_rvu/downloads/{year}/` — Downloaded files (optional)
  
- **Discovery Manifest Format:**
  - Uses `DiscoveryManifest` class from `cms_pricing.ingestion.metadata.discovery_manifest`
  - Standard fields: `source`, `source_url`, `discovered_at`, `files`, `metadata`
  - File entries include: URL, filename, content_type, size_bytes, metadata (version, dates, etc.)
  - Saved as JSONL with timestamp: `cms_rvu_manifest_TIMESTAMP.jsonl`
  
- **Change Detection:**
  - Compare current manifest with previous manifest using `has_same_files()`
  - Set `changes_detected: true` in metadata when differences found
  
- **Provenance:**
  - Track `detail_url` (where file was discovered from)
  - Track `display_name` (link text from CMS)
  - Track `posted_at` (update date from CMS)

⸻

## 8. Observability (Scraper layer)

**Logs (structured)**
- `rvu.scraper.discover.start` — Discovery begins
- `rvu.scraper.detail_links` — Detail links found with count and years
- `rvu.scraper.validation.passed` — URL validated successfully
- `rvu.scraper.validation.rejected_html` — HTML page rejected
- `rvu.scraper.validation.failed` — Validation error (0 bytes, etc.)
- `rvu.scraper.file_rejected` — File failed validation (reason logged)
- `rvu.scraper.discover.complete` — Discovery finished with file count and manifest path

**Metrics (TBD)**
- Discovery counts: #detail links found, #files discovered, #validated
- Validation stats: Pass/fail rates by reason
- Content-Type distribution
- Throughput: Files discovered per second

**Dashboards/Alerts (TBD)**
- Validation failures: Alert if >X% of URLs fail validation
- HTML rejections: Alert if any HTML pages accepted
- Zero files discovered: Alert if discovery finds no files
- Layout drift: Alert when HTML structure changes

⸻

## 9. Testing & Acceptance Criteria

**Unit Tests:** `tests/scrapers/test_rvu_scraper_methods.py`
- ✅ Landing page link extraction with year/quarter/revision parsing
- ✅ Detail page download extraction with metadata enrichment
- ✅ Filename sanitization (date conversion, special char handling)
- ✅ Content-Type validation (accepts ZIP, rejects HTML)
- All 4 tests passing

**Integration Tests:** `test_cms_rvu_scraper.py` (smoke test)
- ✅ Live CMS discovery (2023-2025)
- ✅ Download validation (13 files validated successfully)
- ✅ Manifest generation verification
- ✅ HistoricalDataManager integration

**Acceptance Criteria:**
- ✅ Correctly discovers A/B/C/D quarters for 2023-2025
- ✅ Extracts revision indicators (R suffix)
- ✅ Validates all URLs via HEAD requests
- ✅ Rejects HTML pages, accepts only actual files
- ✅ Sanitizes filenames correctly (removes slashes, normalizes dates)
- ✅ Enriches metadata (content-type, size from HTTP headers)
- ✅ Produces valid DiscoveryManifest in DIS-compliant format
- ✅ Handles edge cases (missing dates, malformed URLs)

**Performance:** Discovery completes in ~2 seconds for 13 files (2023-2025 range)

⸻

## 10. Scheduling & Triggers

**Current State:**
- Manual trigger via `test_cms_rvu_scraper.py`
- HistoricalDataManager for programmatic discovery

**TBD - From OPPS Scraper:**
- GitHub Actions: Weekly poll (e.g., Monday 06:00 UTC) + manual dispatch
- Change detection: Compare manifests to trigger notifications
- Automated discovery workflow

⸻

## 11. Failure Modes & Runbook Hooks

**RVU-Specific Issues:**
- URL path changes: CMS moves from `/files/zip/` to different location → Log warning, quarantine batch
- Filename format changes: Date format changes from `MM/DD/YYYY` to ISO → Update regex patterns, ADR required
- HTML structure changes: Detail page layout changes → Emit layout-drift warning, continue with reduced concurrency

**Common Patterns (from OPPS):**
- DOM changes: Use strict selectors first, heuristic fallback (anchor-text regex for RVU pattern)
- Layout drift: Emit warning and continue with reduced concurrency
- Rate limits: Backoff and lower concurrency; if persistent, schedule re-try window

⸻

## 12. Integration with Downstream Systems

**HistoricalDataManager:**
- Consumes DiscoveryManifest from scraper
- Optional downloads: Set `download=True` to download files
- Downloads save to: `data/historical_rvu/downloads/{year}/{filename}.zip`

**RVUIngestor:**
- Consumes DiscoveryManifest or RVUFileInfo objects
- Downloads files on-demand if not already downloaded
- Parses ZIP contents and extracts datasets (PPRRVU, GPCI, OPPSCap, ANES, LocalityCounty)
- Loads data into PostgreSQL database

**Data Flow:**
```
CMS.gov → RVU Scraper → DiscoveryManifest
                              ↓
                    [HistoricalDataManager] (optional downloads)
                              ↓
                         RVUIngestor
                              ↓
                    PostgreSQL Database
                              ↓
                       FastAPI Endpoints
```

⸻

## 13. Differences from OPPS Scraper

| Aspect | OPPS Scraper | RVU Scraper |
|--------|--------------|-------------|
| **Download behavior** | Discovers AND downloads | Discovers ONLY (downloads optional) |
| **Disclaimer handling** | Tiered strategy (3 tiers) | Not needed |
| **Release pattern** | `opps_YYYYqN_rNN` | `rvu{YY}{Q}{revision}` |
| **Discovery pattern** | Quarterly navigation | Two-hop (landing → detail → download) |
| **Validation** | Content-Type check | HEAD request + Content-Type + size validation |
| **Mandatory files** | Addendum A + B | PPRRVU + GPCI + OPPSCap + ANES + Locality (bundled in ZIP) |
| **File size** | Smaller (1-5MB) | Larger (3-4MB ZIPs) |
| **Version** | 1.0.0 | 2.0.0 (with validation guards) |

⸻

## 14. References (authoritative)

- **PFS Relative Value Files:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
- **Sample Release:** RVU25A (January 2025)
- **Sample Detail Page:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu25a
- **Sample Download URL:** https://www.cms.gov/files/zip/rvu25a-updated-01/10/2025.zip
- **OPPS Scraper PRD:** For tiered disclaimer strategy reference (not applicable to RVU)

⸻

## Notes on Inheritance

This RVU Scraper PRD inherits robots/terms compliance posture, manifest/idempotency guarantees, integrity checks, and operational controls from the Main Scraper Standard PRD; only RVU-specific behavior is defined here.

**Key Override:** Discovery-only behavior (unlike OPPS which downloads automatically). Downloads are handled by HistoricalDataManager or RVUIngestor, providing an approval gate for data acquisition.
