OPPS Scraper PRD (Draft)

**Status:** Draft v1.0  
**Owners:** Platform/Data Engineering; Medicare SME (review)  
**Consumers:** OPPS ingester, QA, SRE/Ops  
**Change control:** Follow `STD-scraper_prd_v1.0.md` + ADR for selector changes

For complete index see [Master System Catalog](DOC-master-catalog_prd_v1.0.md).  

Links to standards: Main Scraper Standard PRD → `STD-scraper_prd_v1.0.md` (this OPPS PRD inherits all baseline rules; only OPPS-specific overrides are defined here).

⸻

0. Overview

Scrape CMS OPPS Quarterly Addenda Updates to discover and download Addendum A/B artifacts for each quarter (including corrections), producing DIS-ready manifests and checksums for ingestion. Implements a tiered disclaimer-accept strategy, polite throttling, checksum-based drift detection, dual manifests (discovery vs download), and rich observability. Primary discovery index: Quarterly Addenda Updates; each quarter has its own detail page (e.g., “January 2025 Addendum”). Some downloads require accepting an interstitial disclaimer before the file is served.  ￼

Status: Draft for approval
Owners: Platform/Data Eng (primary), Medicare SME (review)
Consumers: OPPS ingester, QA, SRE/ops
Change control: Follows Main Scraper Standard PRD; ADR for interface/selector changes

**Cross-References:**
- **DOC-master-catalog_prd_v1.0.md:** Master system catalog and dependency map
- **STD-scraper_prd_v1.0:** Main scraper standard and baseline rules
- **STD-data-architecture_prd_v1.0:** Data ingestion lifecycle and storage patterns
- **STD-qa-testing_prd_v1.0:** Testing requirements for OPPS scraper

⸻

1. Goals & Non-Goals

Goals
	•	Discover all quarterly OPPS Addendum A & B artifacts (including “updated/correction” rows) from the index, click into the quarter detail page, and fetch the files.  ￼
	•	Handle disclaimer interstitials via a tiered approach (direct HTTP → headless accept → final fallback + quarantine).  ￼
	•	Emit discovery and download manifests, with SHA-256 checksums, sizes, final resolved URLs, and provenance.

Non-Goals
	•	Parsing/ingestion business rules (covered by the OPPS ingester PRD).
	•	ASC (AA/BB) addenda (separate domain).  ￼

⸻

2. Scope & Key Assumptions
	•	Scope: OPPS Addendum A (APC rates) and B (HCPCS→APC/SI), all quarters and corrections. Each quarter’s files are discovered from the index, then gathered from the quarter page.  ￼
	•	Formats: CSV, XLS, XLSX, TXT, and ZIP (plus nested). Files and packaging vary by quarter.  ￼
	•	Wage index enrichment is downstream; scraper does not fetch IPPS wage tables (ingester does), but PRD acknowledges that OPPS uses IPPS wage indices (CCN→CBSA) annually.  ￼
	•	Robots & posture: Ignore robots.txt by policy, but implement polite throttling/backoff.  ￼

⸻

3. Discovery → Detail → Disclaimer → Download (Control Flow)
	1.	Discovery (Index):
	•	Poll Quarterly Addenda Updates index. Diff current vs last-seen: capture new quarters and “updated/correction” rows. Extract links to quarter pages.  ￼
	2.	Quarter Page Crawl:
	•	Parse quarter page (e.g., January 2025 Addendum) to enumerate all listed artifacts (A, B, any additional OPPS addenda for that quarter). Capture nominal filenames/titles and initial hrefs.  ￼
	3.	Disclaimer Interstitial (Tiered Strategy):
	•	Tier 1 — Direct HTTP: Attempt direct GET with appropriate referrer, cookies, and headers. If the server demands a click-through (e.g., AMA/CMS click-agreement), advance to Tier 2.  ￼
	•	Tier 2 — Headless Accept: Use headless browser to load the disclaimer page and click “Accept”, capturing any session cookie/token, then re-try download. When feasible, store only a hash of disclaimer HTML + selector snippet for change-detection (no full body).  ￼
	•	Tier 3 — Final Fallback: Exponential backoff, reduce concurrency; if still blocked, quarantine the artifact with reason disclaimer_failed and fail the batch (structural presence rule).
	4.	Download & Persist:
	•	Stream to /raw/opps/{batch}/ with SHA-256 and size captured; record final resolved URL after redirects for audit/comparisons.

⸻

4. Batch Model & Versioning (Scraper perspective)
	•	Batch key: opps_YYYYqN_rNN (e.g., opps_2025q1_r01), where rNN increments whenever CMS posts an “updated/correction” for that quarter on the index or we detect checksum drift (see below). This preserves lineage, supports bitemporal ingestion, and avoids silent overwrites.  ￼
	•	Silent edits (filename unchanged): If checksum differs from the previously stored artifact under the same nominal filename, promote a new revision (rNN+1), and append a safe suffix to the stored file, e.g.,
Addendum_B_2025_Q1 (CMS-Edit-2025-01-02T10-31Z).xlsx
Discovery & download manifests must include change_reason="CMS Edit - filename unchanged" for audit.

⸻

5. Structural Presence & Quarantine
	•	Mandatory presence: Addendum A and B are required for a quarter to be considered complete. If any listed artifact on the quarter page is missing or fails, quarantine the batch with per-file reasons (e.g., missing, disclaimer_failed, checksum_mismatch).  ￼

⸻

6. File Handling & Integrity
	•	Supported types: CSV, XLS, XLSX, TXT, ZIP (nested). Validate Content-Type, size > 0, and checksum for each file.
	•	Retry policy:
	•	5xx/timeout: 3–5 retries (1s→3s→9s→20s).
	•	429: honor Retry-After, else backoff 30–60s, reduce concurrency.
	•	403/disclaimer loop: escalate to Tier 2 (headless). If unresolved → quarantine.
	•	Concurrency & UA: Max 4 concurrent requests/host with 200–500ms jitter; UA string DIS-OPPS-Scraper/1.0 (+ops@yourco.com) for traceability/allowlisting if needed.

⸻

7. Manifests, Layouts, & Provenance
	•	Paths:
	•	/raw/opps/{batch}/ — artifacts + download manifest
	•	/ops/scraper/opps/{batch}/ — discovery manifest (and a mirrored copy of the download manifest for SRE/AI triage)
	•	Discovery manifest (pre-download): intended files (title, subject=A/B, initial href, quarter, expected patterns).
	•	Download manifest (post-download): final resolved URL, content type, size, sha256, disclaimer mode used (direct/headless), retry counts, timings, correlation_id, and change_reason if any.
	•	Why dual manifests? One diff shows gaps immediately (intended vs fetched) and is ideal for automated triage.

⸻

8. Observability (Scraper layer)

Metrics
	•	Discovery counts (#links found, #new/updated), layout-drift warnings
	•	Attempted downloads, successes/failures, bytes fetched, MB/s throughput
	•	Retry histogram (by error class), % headless used, checksum churn (#same-name new digests “CMS Edit”)
	•	Time to first successful batch per quarter (freshness pre-ingest signal)

Logs (structured)
	•	quarter, subject(A/B), download_url, referrer_page, content_type, content_length, sha256, disclaimer_mode, http_status, retry_after, retries, redirect_chain_depth, cookie_set(bool), headless_used(bool), correlation_id

Dashboards/Alerts
	•	Index drift (selector failure → warn), rising headless ratio, 4xx/5xx spikes, repeated CMS-Edit events

⸻

9. Security, Licensing, & Legal Notes
	•	Pages and files are public program artifacts; some OPPS assets and related CMS pages require click-through acceptance prior to download (e.g., AMA terms). The scraper records acceptance path but does not store full disclaimer HTML, only a hash + selector snippet. If acceptance fails, the artifact is quarantined.  ￼
	•	This scraper PRD does not authorize displaying AMA CPT® descriptors; licensing is handled at product/UI layer (see ingester/API PRDs).  ￼

⸻

10. Scheduling & Triggers
	•	GitHub Actions: Weekly poll (e.g., Monday 06:00 UTC) + manual dispatch with --quarter override for ad-hoc pulls/backfills. Index shows release/updated rows that justify periodic polling.  ￼

⸻

11. Failure Modes & Runbook Hooks
	•	Disclaimer changes: selector fails → escalate to headless; if headless also fails → quarantine; raise layout-drift event for engineering.
	•	DOM changes (index/quarter pages): use strict selectors first, then heuristic fallback (anchor-text regex for “Addendum A|B” and quarter patterns). Emit layout-drift warning and continue with reduced concurrency.
	•	Silent edits: checksum drift with same filename → promote rNN+1, annotate “CMS Edit,” retain both artifacts, notify in ops channel.
	•	Rate limits / blocks: backoff and lower concurrency; if persistent, rotate IPs or schedule re-try window.

⸻

12. Acceptance Criteria (Scraper)
	•	Correctly discovers January, April, July, October quarters and any “updated/correction” rows, clicking through to quarter pages to enumerate A & B files.  ￼
	•	Tiered disclaimer flow succeeds for files requiring acceptance; headless usage < configurable threshold in steady state.  ￼
	•	Produces both manifests; all required fields present; SHA-256 checksums verified.
	•	Enforces structural presence: if any listed artifact fails, batch quarantines with per-file reasons.
	•	Checksum drift promotes new rNN and annotates CMS Edit in manifests and filenames.
	•	Metrics/logs enable SRE/AI triage (retry histograms, headless %, layout-drift, checksum churn).

⸻

13. References (authoritative)
	•	Quarterly Addenda Updates (OPPS index; A/B are quarterly snapshots; updated rows reflect corrections).  ￼
	•	January 2025 Addendum B (example quarter page linking to updated artifact).  ￼
	•	Addendum B listing (sample dynamic page) (illustrates release/update metadata).  ￼
	•	OPPS program page (notes retroactive quarterly corrections).  ￼
	•	I/OCE overview (companion quarterly specs for code/SI changes).  ￼
	•	IPPS Wage Index (annual CCN/CBSA context for downstream enrichment).  ￼
	•	CMS/AMA click-agreement examples (interstitial acceptance language).  ￼

⸻

Notes on inheritance

This OPPS Scraper PRD inherits robots/terms compliance posture, manifest/idempotency guarantees, integrity checks, and operational controls from the Main Scraper Standard PRD; only OPPS-specific behavior is overridden here.
