Scraper Platform PRD (MVP → v1.1)

**Status:** Draft v1.1  
**Owners:** Data Engineering / Platform Tooling  
**Consumers:** Ingestion Engineers, QA Guild, Ops  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-data-architecture-prd-v1.0:** Data ingestion lifecycle and storage patterns
- **STD-observability-monitoring-prd-v1.0:** Scraper monitoring and alerting
- **REF-cms-pricing-source-map-prd-v1.0.md:** Authoritative source map for all CMS datasets

0) Summary

Build a cheap, reliable, and auditable scraper/orchestration layer that discovers and fetches latest files monthly across CMS/HRSA (and future sources), snapshots raw artifacts, normalizes to typed tables (Parquet-first), and exposes dataset digests for downstream pricing and comparisons. Freshness is defined, costs are guarded, and observability + alerts route to Page Alex.

⸻

1) Goals & Non‑Goals

Goals
	•	Monthly “latest-only” discovery & ingestion for MVP sources (CMS ZIP9, MPFS RVU/GPCI/CF, OPPS; HRSA later).
	•	Cheap & simple orchestration using GitHub Actions (GHA) with manual dispatch.
	•	Immutable snapshots (bronze), normalized Parquet (silver/gold), 6‑year retention in S3 with lifecycle.
	•	Freshness SLOs aligned to monthly checks; run‑now button for ad‑hoc fetch.
	•	Idempotent ingestion keyed by digest (hash of artifacts + manifests).
	•	Basic observability: success rates, row deltas, freshness, schema drift; alerts → Alex.

Non‑Goals (MVP)
	•	Full Payer TiC/MRF high‑volume crawling (defer; design stubs allowed).
	•	Complex distributed compute; ECS/Batch/self‑hosted runners only when GHA minutes become a constraint.

⸻

2) Scope & Sources (MVP → v1)

In‑scope (MVP): CMS ZIP9 locality mapping; MPFS RVU/GPCI/CF; OPPS packaging minimal; HRSA (later in MVP timeline).
Future: ASC, ASP/NADAC, CLFS, DMEPOS, Medicaid pilots (1–2 states), MRF/TiC large‑scale, hospital CCN resolver and MRF slice cache.

⸻

3) Orchestration & Scheduling

Choice: GitHub Actions (GHA) cron workflows.
	•	One workflow per source (e.g., cms_zip9.yml, cms_mpfs_rvu.yml).
	•	Cadence: weekly change‑check cron; plus workflow_dispatch for manual runs.
	•	Jobs call Python CLI ingestors or the scraper API (FastAPI) running in Docker.
	•	Scale path: use GHA for discovery → trigger ECS Fargate/Batch/self‑hosted runner for heavy downloads later.

Approval Gate (ask before download/process)
	•	Weekly runs perform discovery + change detection only.
	•	If new/changed artifacts are detected, emit an approval request (Slack/Email) to Ops (Page Alex) containing: source, diffs, estimated size, and links to manifests/logs.
	•	On approval, launch the download + normalize job; otherwise, defer until next run or an on‑demand trigger.

Example GHA skeleton

name: CMS RVU Weekly Discovery
on:
  schedule:
    - cron: '0 9 * * 1' # 09:00 PT, every Monday
  workflow_dispatch:
jobs:
  discover:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt
      - env:
          SCRAPER_API_KEY: ${{ secrets.SCRAPER_API_KEY }}
          AWS_REGION: us-west-2
          AWS_ROLE: ${{ secrets.AWS_ROLE_ARN }}
        run: |
          python -m ingestors.rvu --mode discovery --out render://ORG-pricing-data/bronze/cms_rvu/
      - name: Request approval if changed
        run: python ops/request_approval.py --source cms_rvu --diff artifacts/diff.json


⸻

4) Storage, Retention & Layout (S3)

Bucket: render://<org>-pricing-data/
	•	Encryption: SSE‑S3 (OK) or SSE‑KMS (recommended for key rotation/audit).
	•	Versioning: ON.
	•	Lifecycle: Standard → Standard‑IA @ 90d → Glacier @ 365d; retain 6 years; delete thereafter.

Data zones

bronze/<source>/<year>/<yyyymmdd_HHMMSS>/raw.zip|raw.csv|... + manifest.json
silver/<source>/<year>/<digest>/normalized.parquet (typed schema)
gold/<dataset>/<year>/<digest>/... (joined/derived tables)

Manifest (bronze)

{
  "source": "cms_rvu",
  "discovered_at": "2025-09-01T16:04:22Z",
  "files": [{"url": "https://.../rvu25a.zip", "sha256": "...", "last_modified": "2025-08-30"}],
  "snapshot_digest": "sha256:...",
  "notes": {"quarter_set": ["A","B","C","D"], "version": "v2 if republished"}
}


⸻

5) Backfill Horizon & Modes
	•	Initial backfill: last 3 years per source.
	•	Modes: latest-only (default), range --start_year YYYY --end_year YYYY (off by default for cost control).
	•	Idempotent: skip processing if digest unchanged; log “no change”.

⸻

6) Freshness SLOs (What they mean)

Definition: time between publisher making a new file available and our snapshot published.
	•	Detection: prefer HTTP ETag/Last-Modified; else compute SHA256 and compare against prior manifest.
	•	Proposed SLOs (aligned to weekly discovery):
	•	CMS MPFS/ZIP9/OPPS: ≤72h from detection.
	•	HRSA: ≤7 days from detection.
	•	MRF/TiC (future): ≤7 days.
	•	Manual Run Now supported to override cadence when needed.

⸻

7) Cost Guardrails
	•	Latest-only ingestion by default.
	•	Skip re‑processing when digest unchanged.
	•	“Custom Range” backfills disabled by default.
	•	Polite throttling (≤0.5 RPS) + exponential backoff; obey robots/ToS.

⸻

8) Legal & Politeness
	•	Maintain a per‑source allowlist of domains/paths; scrapers must not fetch outside allowlisted scopes.
	•	Evaluate and record robots.txt compliance status in manifest; respect crawl‑delay/usage policies.
	•	Ingest Hospital MRFs and Payer TiC at scale within ToS; throttle, rotate user‑agents, expose a per‑source kill switch.

⸻

9) PDF Policy (state Medicaid & edge cases)
	•	Human‑in‑the‑loop: on first PDF‑only source discovery, pause behind a feature flag, prototype parser(s), and validate outputs vs. manual spot‑checks before enabling.

⸻

10) Data Formats & Schema Registry
	•	Parquet default for silver/gold; CSV allowed for tiny feeds/QA samples.
	•	Schema Registry (lightweight): YAML per dataset to lock dtypes and validate at ingest time.

Schema YAML example

version: 1
name: cms_rvu
columns:
  - name: hcpcs
    dtype: string
    required: true
  - name: work_rvu
    dtype: decimal(9,4)
  - name: nonfac_pe_rvu
    dtype: decimal(9,4)
  - name: mp_rvu
    dtype: decimal(9,4)
  - name: effective_date
    dtype: date
primary_key: [hcpcs, effective_date]


⸻

11) Alerts → “Page Alex”

Trigger alerts on:
	•	Freshness SLO breach.
	•	HTTP 429/5xx storms or repeated retries exhaustion.
	•	Schema drift vs. registry.
	•	Huge row deltas outside expected bounds.

Routing: Slack/PagerDuty/email (configurable). Payload includes source, run id, link to logs, and the manifest key.

⸻

12) RVU “Latest Files Integration” (hardening)
	•	Year detection: parse filenames (e.g., rvu25a.zip → 2025) with fallback to page metadata (anchor text/table column).
	•	Quarter packs A/B/C/D: treat as a set; record a single snapshot digest that rolls all four.
	•	Corrections: CMS may republish letters (e.g., RVU25B v2) → rely on Last-Modified/ETag + hash compare to decide re‑ingestion.
	•	Security: keep /api/v1/rvu/scraper/* behind API key + rate limit; manifest logged.
	•	Idempotency: short‑circuit on equal digests; log reason.

⸻

13) Architecture & Workflow

Happy path
	1.	Discovery (GHA): enumerate candidate URLs; collect headers (ETag/Last-Modified), compute URL list.
	2.	Change detection: compare against last snapshot manifest; if unchanged → exit early.
	3.	Fetch: download raw artifacts with throttle/backoff; compute sha256 per file.
	4.	Bronze snapshot: write artifacts + manifest.json; compute snapshot_digest.
	5.	Normalize (silver): parse → typed Parquet; validate against schema registry.
	6.	Gold: optional derivations (joins, effective dating).
	7.	Publish: update dataset registry; emit metrics; send success/alerts.

Key components
	•	Python ingestors (CLI), FastAPI admin endpoints, Docker images, S3 client, small schema registry.

⸻

14) Roadmap (Data acquisition → priced comparisons)

Milestone A — Scraper platform (MVP, cheap + monthly)
	•	GHA workflows per source; S3 bronze/silver/gold; snapshot registry; digest checks; latest‑only default.
Exit: datasets appear in /admin/datasets, pin‑able by digest.

Milestone B — Geography done
	•	ZIP9 fixed‑width loader; nearest‑ZIP resolver (same‑state) in /price; daily gap report.
Exit: every run returns a locality or a strict error.

Milestone C — MPFS pricing v1
	•	RVU/GPCI/CF ingested; POS NF/FAC; modifiers (‑26, ‑TC, ‑50, multi‑procedure); Part‑B deductible & 20% coinsurance.
Exit: golden tests pass (~50 codes × 3 localities); end‑to‑end cents stable.

Milestone D — OPPS & ASC minimal
	•	OPPS packaging (J1, Q1/Q2/Q3, N) with warnings; ASC base loader; 20% coinsurance cap.
Exit: mixed plans (MPFS+OPPS/ASC) priced with traceable math.

Milestone E — Drugs/Labs/DMEPOS
	•	ASP + NADAC; CLFS, DMEPOS loaders; NDC↔HCPCS crosswalk; rural flags.
Exit: drugs/labs/DME priced; reference vs allowed surfaced.

Milestone F — Facility & independents
	•	Hospital MRF slice cache; CCN resolver; CSV rate‑card uploads; fallback multiplier (default 225% of Medicare) per provider.
Exit: compare Medicare vs hospital vs uploaded rates by location.

Milestone G — Compare parity & snapshots
	•	/compare hard‑rejects when snapshots/toggles differ; payer/plan filter order; “winner” column; compare‑lock.
Exit: rigorous, auditable comparisons.

Milestone H — Medicaid scaffolding & future MCO TiC
	•	1–2 state Medicaid fee schedules behind a flag; document future MCO TiC parsing.
Exit: optional Medicaid comparator.

Milestone I — Ops polish
	•	Dashboards (freshness, success, deltas); alerts → Page Alex; runbook; CI (lint/tests); OpenAPI examples; Docker images; seed data.

⸻

15) Observability & Metrics
	•	Counters: runs, successes, failures, retries, bytes downloaded.
	•	Gauges: freshness lag (per source), rows ingested, row delta %, time‑to‑normalize, storage usage, next_run_ts, last_run_ts.
	•	Logs: per run id; include source, mode, snapshot_digest, manifest_key.
	•	Dashboards: Prometheus → Grafana (or GHA summary + CloudWatch alternative).
	•	Alert thresholds: failure rate >5% per run; zero files discovered on an expected cadence; checksum mismatch events; robots violations; weekly discovery not executed within 8 days.

⸻

16) Security & Access
	•	API endpoints behind API key; rate‑limited.
	•	User‑Agent: dedicated UA string with contact email; rotate as needed; documented in manifest.
	•	AWS access via OIDC‑assumed role from GHA to S3 (least privilege, write‑only to bucket prefixes).
	•	Secrets in GHA secrets and environment‑specific parameter store/Vault; never commit tokens.
	•	Audit logs: record download attempts (timestamp, URL, response code) and store with run metadata.

⸻

17) Runbook (High Level)
	•	Incident types: SLO breach; 429 storm; schema drift; parse failure.
	•	First response: check last manifest; validate publisher change; pause source via kill switch if needed; file issue with label incident.
	•	Rollback: re‑pin previous digest in dataset registry; republish.

⸻

18) Risks & Mitigations
	•	Brittle URLs → add discovery by page scraping + pattern fallback.
	•	Publisher silent corrections → rely on Last-Modified/ETag + hashing.
	•	Growing volume → ECS/Batch hand‑off; sharded downloads; caching.
	•	PDF‑only feeds → HiTL policy; compare outputs pre‑enable.

⸻

19) Open Decisions (for config lock)
	1.	Proceed with GitHub Actions monthly schedules + manual run; plan ECS hand‑off for heavy jobs.
	2.	Confirm S3 bucket name / KMS key; enable Versioning + 6‑year lifecycle now.
	3.	Approve Freshness SLOs: ≤72h (CMS) and ≤7 days (HRSA) with monthly discovery.
	4.	Alerts routing: Slack, email, or PagerDuty for Alex; include destination details.
	5.	Make latest‑only the default for all CMS sources (ZIP9/GPCI/OPPS) with backfill override.
	6.	Keep /api/v1/rvu/scraper/* internal-only (API key) for now.
	7.	Approve lightweight schema registry (YAML) per dataset for dtype locks.

⸻

20) Acceptance Criteria (MVP exit)
	•	New datasets appear in /admin/datasets with a pin‑able digest.
	•	Re‑runs with unchanged publisher inputs no‑op and log “no change”.
	•	Freshness metrics present; SLO alerts wired to Alex channel.
	•	Bronze/silver/gold artifacts present with 6‑year lifecycle policy attached.
	•	At least one golden test suite passes for MPFS (≈50 codes × 3 localities).

⸻

21) Appendix

A. S3 key examples

bronze/cms_rvu/2025/20250901_160422/rvu25a.zip
bronze/cms_rvu/2025/20250901_160422/manifest.json
silver/cms_rvu/2025/sha256-XYZ/rvu.parquet

B. Snapshot digest
	•	snapshot_digest = sha256(sorted(file_sha256s) + manifest_metadata)

C. Minimal CLI contract

python -m ingestors.rvu --mode latest-only --start_year 2023 --end_year 2025 --out render://... --schema schemas/cms_rvu.yaml

D. Alert payload (example)

{
  "source": "cms_rvu",
  "event": "freshness_slo_breach",
  "detected": "2025-09-04T10:12:00Z",
  "lag_hours": 90,
  "last_manifest": "render://.../bronze/cms_rvu/2025/20250901_160422/manifest.json",
  "run_log": "https://github.com/org/repo/actions/runs/123456789"
}


⸻

22) Addenda: Scraper Standard Details (adopted)
	•	Concurrency & timeouts: async client with bounded concurrency (default ≤4 simultaneous downloads per domain) + retries with exponential backoff.
	•	Size‑drift guard: validate file size vs historical ranges (±20% default) to catch truncations.
	•	Manifest enrichment: include license, discovery_method, scraper_version (semver), robots_compliant flag, anomaly notes.
	•	Error taxonomy & quarantine: standard codes (SCRAPER_INVALID_URL, SCRAPER_FORBIDDEN, SCRAPER_PARSE_ERROR, SCRAPER_TIMEOUT, SCRAPER_CHECKSUM_MISMATCH); quarantine failed artifacts with reason, attempts, remediation guidance.
	•	CI/QA gates: recorded HTML fixtures (VCR); drift‑detection tests that can block merges; target coverage ≥80% for scraper modules.
	•	Change management: semver policy; dry‑run + Ops sign‑off for major changes; changelog maintained.
	•	Audit & compliance: attribution flags (attribution_required); audit logs persisted with run metadata.

(retains 6‑year retention and bronze/silver/gold data zones as previously defined)

⸻

23) CMS Scraper Pattern Implementations

This section provides concrete implementation patterns and templates for all CMS data source scrapers.

## 23.1 Universal Scraper Pattern

All CMS scrapers follow the same **Discovery → Metadata → Manifest** pattern:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Navigate   │────▶│   Extract    │────▶│   Generate   │────▶│    Store     │
│  CMS Website │     │   Metadata   │     │   Manifest   │     │   Manifest   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                     │                     │                     │
    HTTP GET           Parse HTML/Links      Create JSONL          Save to disk
   Follow links         Extract URLs        Standard schema      Version control
```

**What Scrapers DO:**
- ✅ Navigate CMS website structures
- ✅ Discover file URLs and metadata
- ✅ Generate discovery manifests
- ✅ Track checksums and versions

**What Scrapers DON'T DO:**
- ❌ Download the actual data (that's the ingestor)
- ❌ Parse or validate content
- ❌ Transform data structures
- ❌ Write to the database

## 23.2 Scraper Pattern Matrix

| Data Type | Scraper Class | Base URL | Discovery Pattern | File Types | Cadence | Status |
|-----------|---------------|----------|-------------------|------------|---------|--------|
| **MPFS** | `CMSMPFSScraper` | `/physicianfeesched` | Composes RVU + MPFS patterns | ZIP (RVU bundle + CF) | Annual | ✅ Implemented |
| **RVU** | `CMSRVUScraper` | `/pfs-relative-value-files` | Direct file links | ZIP/TXT/CSV/XLSX | Quarterly | ✅ Implemented |
| **OPPS** | `CMSOPPSScraper` | `/hospital-outpatient-pps` | Quarterly addenda navigation | ZIP/CSV/XLSX | Quarterly | ✅ Implemented |
| **ASC** | `CMSASCScraper` | `/ambulatory-surgical-centers` | Quarterly payment page | ZIP/CSV/XLSX | Quarterly | 🔄 **TO CREATE** |
| **IPPS** | `CMSIPPSScraper` | `/inpatient-prospective-payment-system` | Annual impact files | ZIP/XLSX | Annual | 🔄 **TO CREATE** |
| **CLFS** | `CMSCLFSScraper` | `/clinical-laboratory-fee-schedule` | Quarterly updates | ZIP/CSV/XLSX | Quarterly | 🔄 **TO CREATE** |
| **DMEPOS** | `CMSDMEPOSScraper` | `/durable-medical-equipment` | Quarterly fee schedules | ZIP/CSV/XLSX | Quarterly | 🔄 **TO CREATE** |
| **ASP** | `CMSASPScraper` | `/asp-payment-allowance-limits` | Quarterly drug pricing | ZIP/CSV/XLSX | Quarterly | 🔄 **TO CREATE** |
| **NADAC** | `CMSNADACScraper` | `/national-average-drug-acquisition-cost` | Weekly/monthly updates | CSV/XLSX | Weekly | 🔄 **TO CREATE** |
| **Geography** | `CMSGeographyScraper` | `/payment/fee-schedules` | ZIP locality files | ZIP/TXT/XLSX | Quarterly | 🟡 Partial |

## 23.3 Implemented Scrapers (Detailed)

### 23.3.1 MPFS (Medicare Physician Fee Schedule)

**Implementation:** `cms_pricing/ingestion/scrapers/cms_mpfs_scraper.py`

**Source URL:** https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads

**Discovery Strategy:**
- Composition pattern: Reuses RVU scraper for shared files
- Discovers MPFS-specific files (conversion factors, abstracts)
- Generates unified manifest covering all MPFS artifacts

**Files Discovered:**
- RVU bundle (via composition): `RVU25D.zip` containing:
  - PPRRVU2025_Oct.txt (~19,000 HCPCS codes with RVUs)
  - GPCI2025.txt (~115 localities with geographic adjustments)
  - 25LOCCO.txt (~150 county-to-locality mappings)
  - ANES2025.txt (~115 anesthesia conversion factors)
  - OPPSCAP_Oct.txt (~19,000 OPPS cap amounts)
  - RVU25D.pdf (~200 pages of policy documentation)
- Conversion factors: `cf-2025.xlsx`
- Abstracts: `pfs-abstract-2025.pdf`
- National payment: `national-payment-2025.xlsx`

**Manifest Output:** `data/scraped/mpfs/manifests/cms_mpfs_manifest_YYYYMMDD_HHMMSS.jsonl`

### 23.3.2 RVU (Relative Value Units)

**Implementation:** `cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`

**Source URL:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files

**Discovery Strategy:**
- Direct link pattern: Extracts quarterly release links from main page
- Identifies revision packages (e.g., RVU24AR for corrections)
- Supports multiple formats per release

**Files Discovered:**
- Quarterly releases: `RVU24A`, `RVU24B`, `RVU24C`, `RVU24D`
- Revision packages: `RVU24AR`, `RVU24BR`, etc.
- Multiple formats: `.zip`, `.txt`, `.csv`, `.xlsx`

**Manifest Output:** `data/manifests/cms_rvu/cms_rvu_manifest_YYYYMMDD_HHMMSS.jsonl`

### 23.3.3 OPPS (Outpatient Prospective Payment System)

**Implementation:** `cms_pricing/ingestion/scrapers/cms_opps_scraper.py`

**Source URL:** https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates

**Discovery Strategy:**
- Quarterly navigation pattern: Navigates quarterly update pages
- Extracts Addendum A (APC payments) and B (HCPCS crosswalk) links
- Handles AMA license interstitial automatically

**Files Discovered:**
- Addendum A (APC payments): `january-2025-addendum-a.zip`
- Addendum B (HCPCS crosswalk): `january-2025-addendum-b.zip`
- Quarterly releases for Q1-Q4

**Special Handling:**
- AMA license interstitial detection
- Automatic disclaimer acceptance
- Redirect URL tracking

**Manifest Output:** `data/scraped/opps/manifests/cms_opps_manifest_YYYYMMDD_HHMMSS.jsonl`

## 23.4 Scraper Templates (To Be Implemented)

### 23.4.1 ASC (Ambulatory Surgical Center) Template

**Planned File:** `cms_pricing/ingestion/scrapers/cms_asc_scraper.py`

**Source URL:** https://www.cms.gov/medicare/payment/prospective-payment-systems/ambulatory-surgical-centers

**Expected Files:**
- Addendum AA: ASC payment rates by HCPCS (~3,500 procedures)
- Addendum BB: HCPCS to payment indicator mapping
- Addendum DD1: Surgical procedures by payment group
- Quarterly impact files

**Discovery Strategy:** Similar to OPPS (quarterly navigation pattern)

### 23.4.2 IPPS (Inpatient Prospective Payment System) Template

**Planned File:** `cms_pricing/ingestion/scrapers/cms_ipps_scraper.py`

**Source URL:** https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps

**Expected Files:**
- Impact file: Provider-level payment impacts (~3,000 hospitals)
- Wage index: Geographic wage adjustments
- MS-DRG: Diagnosis-related group weights (~750 MS-DRGs)
- Operating standardized amounts
- Capital standardized amounts

**Discovery Strategy:** Annual impact files pattern (fiscal year navigation)

**Cadence:** Annual (October 1 fiscal year)

### 23.4.3 Other Templates (CLFS, DMEPOS, ASP, NADAC)

**CLFS (Clinical Laboratory Fee Schedule):**
- Source: https://www.cms.gov/medicare/payment/fee-schedules/clinical-laboratory-fee-schedule
- Files: National limitation amounts, local fee schedules, gapfill amounts (~1,500 lab test codes)
- Pattern: Quarterly updates

**DMEPOS (Durable Medical Equipment):**
- Source: https://www.cms.gov/medicare/payment/fee-schedules/dmepos
- Files: DMEPOS fee schedule, competitive bidding amounts (~5,000 HCPCS codes)
- Pattern: Quarterly fee schedules

**ASP (Average Sales Price):**
- Source: https://www.cms.gov/medicare/payment/part-b-drugs/asp-payment-allowance-limits
- Files: ASP pricing file (quarterly), payment limits by NDC (~500-800 drugs per quarter)
- Pattern: Quarterly drug pricing

**NADAC (National Average Drug Acquisition Cost):**
- Source: https://data.medicaid.gov/Drug-Pricing-and-Payment/NADAC-National-Average-Drug-Acquisition-Cost
- Files: Weekly NADAC CSV updates, monthly comparison files (~25,000 drugs)
- Pattern: Weekly updates (different domain - external API)

⸻

24) Implementation Checklist

For each new scraper, follow this 4-phase checklist:

**Phase 1: Discovery Strategy**
-  Identify CMS landing page URL
-  Map navigation structure (direct links vs. multi-page)
-  Define file name patterns (regex)
-  Define quarterly/annual cadence patterns
-  Document special handling (licenses, redirects, etc.)

**Phase 2: Scraper Implementation**
-  Create scraper class inheriting base patterns
-  Implement `discover_files()` method
-  Implement page navigation helpers
-  Implement file extraction logic
-  Add checksum/metadata extraction
-  Implement manifest generation
-  Add logging and observability

**Phase 3: Testing**
-  Unit tests for pattern matching
-  Integration tests for discovery
-  Test with historical data (2+ years)
-  Validate manifest schema compliance
-  Performance testing (should complete in <5 minutes)

**Phase 4: Integration**
-  Update `REF-cms-pricing-source-map-prd-v1.0.md`
-  Create schema contracts (`.json` files)
-  Link to corresponding ingestor
-  Add CI/CD workflow for discovery
-  Document in this PRD

⸻

25) Scraper Code Template

The following template provides a standard structure for new CMS scrapers:

```python
#!/usr/bin/env python3
"""
CMS {DATA_TYPE} Scraper
=======================

Discovers and tracks {DATA_TYPE} files from CMS.gov.
Follows DIS discovery standard with manifest generation.

Author: CMS Pricing Platform Team
Version: 1.0.0
"""

import asyncio
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from structlog import get_logger

from ..metadata.discovery_manifest import DiscoveryManifest, DiscoveryManifestStore

logger = get_logger()

SCRAPER_VERSION = "1.0.0"


class CMS{DataType}Scraper:
    """
    CMS {DATA_TYPE} Scraper for {cadence} file discovery.
    
    Discovers {DATA_TYPE} files from the CMS {page description},
    extracts metadata, and generates discovery manifests.
    """
    
    def __init__(self, output_dir: Path = None):
        self.base_url = "https://www.cms.gov/{path}"
        self.output_dir = output_dir or Path("data/scraped/{data_type}")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_store = DiscoveryManifestStore(
            self.output_dir / "manifests",
            prefix="cms_{data_type}_manifest"
        )
        
        # File patterns
        self.file_patterns = {
            'main_file': re.compile(r'pattern.*\.(csv|xlsx|zip)', re.IGNORECASE),
            # Add more patterns
        }
        
        # Quarterly/annual patterns
        self.cadence_patterns = {
            'q1': re.compile(r'january|jan|q1', re.IGNORECASE),
            # Add more patterns
        }
    
    async def discover_files(
        self, 
        start_period: int, 
        end_period: int
    ) -> List[ScrapedFileInfo]:
        """
        Discover {DATA_TYPE} files for the given period range.
        
        Args:
            start_period: Starting year/quarter/month
            end_period: Ending year/quarter/month
            
        Returns:
            List of discovered file information
        """
        logger.info("Starting {DATA_TYPE} file discovery")
        
        all_files = []
        
        # 1. Navigate to main page
        main_page = await self._fetch_page(self.base_url)
        
        # 2. Extract period-specific links
        period_links = self._extract_period_links(main_page)
        
        # 3. For each period, extract file links
        for period_link in period_links:
            if start_period <= period_link.period <= end_period:
                period_files = await self._discover_period_files(period_link)
                all_files.extend(period_files)
        
        # 4. Generate manifest
        await self.manifest_store.save_manifest(all_files)
        
        logger.info("{DATA_TYPE} file discovery completed", 
                   total_files=len(all_files))
        
        return all_files
    
    async def _fetch_page(self, url: str) -> str:
        """Fetch and return page HTML"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text
    
    def _extract_period_links(self, page_html: str) -> List[PeriodLink]:
        """Extract period-specific links from main page"""
        soup = BeautifulSoup(page_html, 'html.parser')
        period_links = []
        
        for link in soup.find_all('a', href=True):
            # Extract period info from link text/href
            period_info = self._parse_period_info(link)
            if period_info:
                period_links.append(period_info)
        
        return period_links
    
    async def _discover_period_files(
        self, 
        period_link: PeriodLink
    ) -> List[ScrapedFileInfo]:
        """Discover files for a specific period"""
        files = []
        
        # Navigate to period page
        period_page = await self._fetch_page(period_link.url)
        
        # Extract file links
        soup = BeautifulSoup(period_page, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True)
            
            # Match against file patterns
            for pattern_name, pattern in self.file_patterns.items():
                if pattern.search(text) or pattern.search(href):
                    file_info = ScrapedFileInfo(
                        url=urljoin(self.base_url, href),
                        filename=Path(href).name,
                        file_type=pattern_name,
                        batch_id=str(uuid.uuid4()),
                        discovered_at=datetime.utcnow(),
                        source_page=period_link.url,
                        metadata={
                            'period': period_link.period,
                            'file_type': pattern_name,
                            # Add more metadata
                        }
                    )
                    files.append(file_info)
        
        return files


# CLI for testing
async def main():
    """Test the scraper"""
    scraper = CMS{DataType}Scraper()
    files = await scraper.discover_files(2024, 2025)
    
    print(f"Discovered {len(files)} files:")
    for f in files:
        print(f"  - {f.filename} ({f.file_type})")


if __name__ == "__main__":
    asyncio.run(main())
```

⸻

26) Common Pattern Reference

### Pattern A: Direct File Links (RVU)
CMS publishes files at predictable URLs. Extract and validate.

**Example:**
- Navigate to RVU files page
- Extract all links matching `/rvu\d{2}[A-D]/i`
- Generate manifest with URLs

### Pattern B: Quarterly Navigation (OPPS, ASC)
Navigate to quarterly update pages, then extract addenda.

**Example:**
- Get quarterly links from main page
- For each quarter, navigate to quarter-specific page
- Extract Addendum A/B links
- Generate manifest

### Pattern C: Annual Impact Files (IPPS)
Navigate to fiscal year pages, extract impact/parameter files.

**Example:**
- Navigate to FY-specific page
- Extract impact file, wage index, MS-DRG
- Generate manifest

### Pattern D: Composition (MPFS)
Reuse existing scrapers + add dataset-specific files.

**Example:**
- Call RVU scraper for shared files
- Discover MPFS-specific files
- Merge into unified manifest

### Pattern E: External Data API (NADAC)
Different domain, API-based discovery instead of web scraping.

**Example:**
- Use data.medicaid.gov API
- Query for weekly updates
- Generate manifest from API response

⸻

27) Success Metrics

Each scraper should achieve:
- ✅ Discovery completeness: 100% of published files
- ✅ Manifest accuracy: All URLs valid and accessible
- ✅ Performance: <5 minutes for full historical discovery
- ✅ Reliability: <1% failure rate over 90 days
- ✅ Observability: Structured logging for all operations

⸻

28) Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| 2025-10-15 | v1.1 | Data Engineering | Added CMS scraper pattern implementations (§23-27): pattern matrix, detailed mappings for MPFS/RVU/OPPS, templates for ASC/IPPS/CLFS/DMEPOS/ASP/NADAC, implementation checklist, code template, and common patterns reference. |
| 2025-09-30 | v1.0 | Data Engineering | Initial draft of scraper platform PRD. |
