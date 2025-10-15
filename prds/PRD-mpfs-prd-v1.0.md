# PRD: MPFS Ingest (RVU / GPCI / CF / Indicators)

**Status:** Draft v1.0  
**Owners:** Pricing Platform Product & Engineering  
**Consumers:** Data Engineering, Pricing API, Analytics, Ops  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns
- **STD-parser-contracts-prd-v1.0.md:** Shared parser contracts for CMS data files
- **STD-qa-testing-prd-v1.0.md:** Testing requirements for MPFS ingestion
- **REF-nearest-zip-resolver-prd-v1.0.md:** ZIP resolver for geography mapping
- **REF-cms-pricing-source-map-prd-v1.0.md:** Source inventory & work-backwards checklist

## Work-Backwards Checklist (Required)
Every MPFS ingester or schema change **must** trace back to **REF-cms-pricing-source-map-prd-v1.0.md**. Confirm the source table entry, authoritative layout, and checklist completion before authoring code or submitting review.

## Data Classification & Stewardship
- **Classification:** Public CMS release (Internal derived metrics and aggregations)  
- **License & Attribution:** CMS MPFS files (public domain); include CMS citation in manifests and curated docs  
- **Data Owner / Steward:** Pricing Platform Product (product owner), Data Engineering (technical steward)  
- **Downstream Visibility:** Curated tables are Internal; external pricing surfaces must pass compliance review and mask non-public enrichments

## Ingestion Summary (DIS v1.0)
- **Sources & Cadence:** Quarterly RVU A/B/C/D files, annual Locality & GPCI tables, annual Conversion Factor notices, CMS abstracts/policy files  
- **Schema Contracts:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json`, `cms_gpci_v1.0.json`, `cms_localitycounty_v1.0.json`, `cms_anescf_v1.0.json`; versions pinned in manifests  
- **Landing Layout:** `/raw/mpfs/{release_id}/files/*` with `manifest.json` recording `source_url`, `fetched_at`, `release_id`, `sha256`, `license`, `notes_url`  
- **Natural Keys & Partitioning:** RVU tables keyed by `(hcpcs, modifier, quarter_vintage)`; locality/GPCI keyed by `(carrier_id, locality_code, valuation_year)`; curated snapshots partitioned by `vintage_date`  
- **Validations & Gates:** Structural (required files), schema contract enforcement, HCPCS format and effective-dating checks, indicator completeness, locality/GPCI join coverage ≥99.5%, quarter-over-quarter diff thresholds (±1% row drift)  
- **Quarantine Policy:** Failed records land in `/stage/mpfs/{release_id}/reject/` with rule code + payload; publish blocks on any critical errors or missing quarters  
- **Enrichment & Crosswalks:** Join to ZIP→Locality resolver for analytics keys; compute effective windows using valuation quarter and CMS effective_from metadata  
- **Outputs:** `/curated/mpfs/{vintage}/mpfs_rvu.parquet`, `mpfs_indicators_all.parquet`, `mpfs_locality.parquet`, `mpfs_gpci.parquet`, `mpfs_cf_vintage.parquet`, plus latest-effective views for API usage  
- **SLAs:** Land + publish ≤7 business days from CMS posting; manifest digests recorded; backfills re-run through identical validations  
- **Deviations:** None; any exceptions require ADR and update to this summary
- **Discovery Manifest & Governance:** MPFS scraper emits manifests via `cms_pricing.ingestion.metadata.discovery_manifest` (`data/scraped/mpfs/manifests/`). CI runs `tools/verify_source_map.py` so `REF-cms-pricing-source-map-prd-v1.0.md` stays synchronized with discovered artifacts.

## API Readiness & Distribution
- **Curated Views:** `mpfs_rvu_latest`, `mpfs_gpci_latest`, and `mpfs_cf_current` provide Latest-Effective semantics for pricing services  
- **Digest Pinning:** APIs must accept `X-Dataset-Digest` / `?digest` matching curated manifest digests  
- **Access Controls:** Internal APIs behind token-based auth per **STD-api-security-and-auth-prd-v1.0.md**; pricing outputs include attribution if surfaced externally

## Objective  
Persist the full Medicare Physician Fee Schedule (MPFS) inputs — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — to support downstream **network + price + access** and analytics use cases. The ingester stores everything; **no price math in-ingest** (computation lives downstream).

## Scope  
- **Included**  
  - RVU quarterly files (A / B / C / D)  
  - National / abstract / payment files (when published)  
  - Locality & GPCI files  
  - Annual Conversion Factors (Physician + Anesthesia)  
  - All published policy / status / indicator columns  

- **Excluded (in v1)**  
  - Price calculation (CF × (RVU × GPCI) etc.)  
  - Sequestration adjustments  
  - Site-neutral transforms / overrides  

- **Adjacent / references**  
  - PFS Look-Up Tool (for parity QC)  
  - OPPS Addendum B (for future site-neutral linkage)

## Sources & Cadence  
- **PFS Relative Value Files** (e.g. RVU25A/B/C/D) — quarterly refreshes  
- **PFS documentation / abstracts**  
- **Localities & GPCI** (yearly sets + occasional updates)  
- **Conversion Factors** (annually)  
- **Update cadence & vintage**  
  - Ingest CY 2023–2025 now; retain 6 years  
  - Monthly scraper to detect updates  
  - Freeze historical quarterlies; diff reports between versions  

### Primary CMS References  
- [CMS Physician Fee Schedule Relative Value Files](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files) — authoritative quarterly RVU bundles (A/B/C/D) and supporting documentation  
- [CMS Physician Fee Schedule Look-Up Tool](https://www.cms.gov/medicare/physician-fee-schedule/search/overview) — parity benchmark for sampled HCPCS pricing checks  
- [CMS Locality Key & GPCI Documentation](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key) — locality metadata and GPCI definitions used in geography joins  
- [CY 2025 Physician Fee Schedule Final Rule / Conversion Factor Release](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures) — current conversion factor guidance  
- [How to Use the Medicare Physician Fee Schedule (CMS Booklet)](https://www.cms.gov/sites/default/files/2021-04/How_to_MPFS_Booklet_ICN901344_0.pdf) — official payment calculation methodology reference  

## Schema / Data Model  

### Raw tables (1:1 with CMS artifacts)  
- `rvu_raw_YYYYQ` — all columns from the corresponding RVU file  
- `pfs_abstract_raw_YYYYQ` — abstracts / national payment snippets  
- `locality_raw_YYYY` — locality metadata (carrier, locality, name, state, etc.)  
- `gpci_raw_YYYY` — GPCI indices (Work, PE, MP) by locality  
- `cf_raw_YYYY` — Physician & Anesthesia conversion factors  

### Curated / derived views  
- `mpfs_rvu` — core RVUs + indicators keyed by `(hcpcs, modifier, quarter_vintage)`  
- `mpfs_indicators_all` — exploded table of policy/indicator flags for analytics  
- `mpfs_locality` — locality dimension (id, code, name)  
- `mpfs_gpci` — GPCI indices per locality and vintage  
- `mpfs_cf_vintage` — CF values per year  
- `mpfs_link_keys` — minimal key set for downstream joins (hcpcs, modifier, quarter, locality_id)  

## Keys & Joins  
- Primary key: `(hcpcs, modifier, quarter_vintage)`  
- Localities keyed by `(carrier_id, locality_code, year)`  
- ZIP linkage: use external ZIP → Locality resolver to derive pricing views (compute price downstream)  

## Quality / QC & Diffing  
- Schema presence checks (no dropped columns)  
- Idempotency (checksum, content type, consistent downloads)  
- Vintage lock (old versions immutable)  
- Diff reports: compare each new quarterly version with prior (additions, deletions, indicator changes)  
- Parity sampling: recompute sample lines vs PFS Look-Up Tool / national values (non-blocking)  
- Dashboards track the DIS five pillars (freshness, volume, schema, quality, lineage) with release-by-release diff summaries  
- Non-blocking parity review compares sampled HCPCS rows against the CMS PFS Look-Up Tool to confirm facility vs non-facility splits  

## Policy / Indicator Handling  
- Retain all status / indicator columns verbatim (global surgery, PC/TC, bilateral, multiple proc, etc.)  
- Document meaning of key indicators in annex (e.g. “Status Indicator A means …”)  

## Design Decisions  
- Do not compute pay amounts in the ingester; the ingester is strictly storage + light QC  
- Price formula (downstream) is:  
    Payment = Conversion_Factor × [ (Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP) ]

- Sequestration or other adjustments are applied after base price  
- Geographic floors / overrides handled as metadata flags (not transforms)  

## Licensing / ToS  
- CMS publishes these as open program data; include attribution in manifests  
- Record source URL, last-modified, robots/ToS metadata  

## Operations & Runbook Hooks  
- Follow the MPFS entry in `prds/Runbook.md` for quarter-end validation, including HCPCS spot checks against CMS RVU documentation and locality/GPCI sampling across MACs  
- Confirm conversion factors (physician and anesthesia) are pinned in `mpfs_cf_vintage` for the current calendar year before enabling downstream consumers  
- Capture release notes and supporting PDFs alongside manifests so operators can reference CMS change summaries during incident triage  

## Roadmap / Future Enhancements  
- Incorporate sequestration reductions downstream  
- Add flags / override logic for geographic floors (e.g. PE floor)  
- Site-neutral analytics via joining into OPPS data  
- Support retroactive updates or corrections (CMS errata)  

# PRD: MPFS Ingest (RVU / GPCI / CF / Indicators)

## Objective  
Persist the full Medicare Physician Fee Schedule (MPFS) inputs — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — to support downstream **network + price + access** and analytics use cases. The ingester stores everything; **no price math in-ingest** (computation lives downstream).

## Scope  
- **Included**  
  - RVU quarterly files (A / B / C / D)  
  - National / abstract / payment files (when published)  
  - Locality & GPCI files  
  - Annual Conversion Factors (Physician + Anesthesia)  
  - All published policy / status / indicator columns  

- **Excluded (in v1)**  
  - Price calculation (CF × (RVU × GPCI) etc.)  
  - Sequestration adjustments  
  - Site-neutral transforms / overrides  

- **Adjacent / references**  
  - PFS Look-Up Tool (for parity QC)  
  - OPPS Addendum B (for future site-neutral linkage)

## Sources & Cadence  
- **PFS Relative Value Files** (e.g. RVU25A/B/C/D) — quarterly refreshes  
- **PFS documentation / abstracts**  
- **Localities & GPCI** (yearly sets + occasional updates)  
- **Conversion Factors** (annually)  
- **Update cadence & vintage**  
  - Ingest CY 2023–2025 now; retain 6 years  
  - Monthly scraper to detect updates  
  - Freeze historical quarterlies; diff reports between versions  

## Schema / Data Model  

### Raw tables (1:1 with CMS artifacts)  
- `rvu_raw_YYYYQ` — all columns from the corresponding RVU file  
- `pfs_abstract_raw_YYYYQ` — abstracts / national payment snippets  
- `locality_raw_YYYY` — locality metadata (carrier, locality, name, state, etc.)  
- `gpci_raw_YYYY` — GPCI indices (Work, PE, MP) by locality  
- `cf_raw_YYYY` — Physician & Anesthesia conversion factors  

### Curated / derived views  
- `mpfs_rvu` — core RVUs + indicators keyed by `(hcpcs, modifier, quarter_vintage)`  
- `mpfs_indicators_all` — exploded table of policy/indicator flags for analytics  
- `mpfs_locality` — locality dimension (id, code, name)  
- `mpfs_gpci` — GPCI indices per locality and vintage  
- `mpfs_cf_vintage` — CF values per year  
- `mpfs_link_keys` — minimal key set for downstream joins (hcpcs, modifier, quarter, locality_id)  

## Keys & Joins  
- Primary key: `(hcpcs, modifier, quarter_vintage)`  
- Localities keyed by `(carrier_id, locality_code, year)`  
- ZIP linkage: use external ZIP → Locality resolver to derive pricing views (compute price downstream)  

## Quality / QC & Diffing  
- Schema presence checks (no dropped columns)  
- Idempotency (checksum, content type, consistent downloads)  
- Vintage lock (old versions immutable)  
- Diff reports: compare each new quarterly version with prior (additions, deletions, indicator changes)  
- Parity sampling: recompute sample lines vs PFS Look-Up Tool / national values (non-blocking)  

## Policy / Indicator Handling  
- Retain all status / indicator columns verbatim (global surgery, PC/TC, bilateral, multiple proc, etc.)  
- Document meaning of key indicators in annex (e.g. “Status Indicator A means …”)  

## Design Decisions  
- Do not compute pay amounts in the ingester; the ingester is strictly storage + light QC  
- Price formula (downstream) is:  
    Payment = Conversion_Factor × [ (Work_RVU × GPCI_Work) + (PE_RVU × GPCI_PE) + (MP_RVU × GPCI_MP) ]

- Sequestration or other adjustments are applied after base price  
- Geographic floors / overrides handled as metadata flags (not transforms)  

## Licensing / ToS  
- CMS publishes these as open program data; include attribution in manifests  
- Record source URL, last-modified, robots/ToS metadata  

## Roadmap / Future Enhancements  
- Incorporate sequestration reductions downstream  
- Add flags / override logic for geographic floors (e.g. PE floor)  
- Site-neutral analytics via joining into OPPS data  
- Support retroactive updates or corrections (CMS errata)  

# PRD: MPFS Ingest (RVU / GPCI / CF / Indicators)

## Objective
Persist all Medicare Physician Fee Schedule (MPFS) inputs — RVUs, policy/status indicators, Localities & GPCIs, and annual Conversion Factors — to power downstream **network + price + access** and analytics. **No price math in-ingest**; compute later.

> Key refs: PFS relative value files (quarterlies RVU25A/B/C/D)  [oai_citation:0‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com), PFS Look-Up Tool for parity checks  [oai_citation:1‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/overview?utm_source=chatgpt.com), locality & GPCI docs  [oai_citation:2‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key?utm_source=chatgpt.com), 2025 final rule/CF context  [oai_citation:3‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com).

## Scope
**Include**
- RVU quarterly files (A/B/C/D) with **all** status/policy indicators.  [oai_citation:4‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)
- National/abstract files (when posted) and documentation pointers.  [oai_citation:5‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician?utm_source=chatgpt.com)
- Locality & GPCI files (annual + updates).  [oai_citation:6‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/documentation?utm_source=chatgpt.com)
- Annual Conversion Factors (Physician + Anesthesia).  [oai_citation:7‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)

**Exclude (v1 transforms)**
- Price calc, sequestration, site-neutral transforms — handled downstream. (Formula refs in CMS docs.)  [oai_citation:8‡Centers for Medicare & Medicaid Services](https://www.cms.gov/sites/default/files/2021-04/How_to_MPFS_Booklet_ICN901344_0.pdf?utm_source=chatgpt.com)

**Adjacent**
- OPPS Addendum B (join later for site-neutral analysis).  [oai_citation:9‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates?utm_source=chatgpt.com)

## Cadence, Vintage, Retention
- Ingest **CY 2023–2025** now; retain **6 years**.
- Preserve quarterlies (A→B→C→D) with **vintage-lock** + diffs.  [oai_citation:10‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)
- **Monthly** poller for postings; quarterly/annual are real change events.  [oai_citation:11‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)

## Data Model (Store-everything → Curated Views)
**Raw (1:1 with CMS artifacts)**
- `rvu_raw_YYYYQ` — all columns from RVU file (Work/PE/MP RVUs; facility vs non-facility PE; status indicators like global days, bilateral, multiple proc, PC/TC, anesthesia base units, etc.).  [oai_citation:12‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)
- `pfs_abstract_raw_YYYYQ` — national/abstract files when posted.  [oai_citation:13‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician?utm_source=chatgpt.com)
- `locality_raw_YYYY` — carrier/locality, names, state/county mapping.  [oai_citation:14‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key?utm_source=chatgpt.com)
- `gpci_raw_YYYY` — Work/PE/MP indices by locality.  [oai_citation:15‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/documentation?utm_source=chatgpt.com)
- `cf_raw_YYYY` — Physician + Anesthesia CF by year (note CY2025 CF context from final rule).  [oai_citation:16‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)

**Curated / Views**
- `mpfs_rvu` — RVUs + indicators keyed by `(hcpcs, modifier, quarter_vintage)`.
- `mpfs_indicators_all` — exploded indicator key→value for analytics.
- `mpfs_locality` — locality dimension (id, code, name).
- `mpfs_gpci` — GPCI indices per `(locality_id, year)`.
- `mpfs_cf_vintage` — Physician + Anesthesia CF by year.
- `mpfs_link_keys` — `(hcpcs, modifier, quarter_vintage, locality_id)` to enable future OPPS joins.

## Keys & Joins
- Primary: `(hcpcs, modifier, quarter_vintage)`.  
- Locality keys: `(carrier_id, locality_code, year)`; use **ZIP→Locality** resolver for ZIP-level views.
- Price math is **downstream** using CMS method:  
  `Payment = CF × [(Work_RVU×GPCIw) + (PE_RVU×GPCIpe) + (MP_RVU×GPCImp)]` (facility vs non-facility PE as published).  [oai_citation:17‡Centers for Medicare & Medicaid Services](https://www.cms.gov/sites/default/files/2021-04/How_to_MPFS_Booklet_ICN901344_0.pdf?utm_source=chatgpt.com)

## Quality / Observability (5 pillars)
- **Freshness/Volume/Schema/Quality/Lineage** dashboards (DIS standard).  
- **Schema presence** (no dropped CMS fields).  
- **Idempotency** (checksum, type, bytes).  
- **Vintage-lock** (prior quarterlies immutable).  
- **Diffs**: JSON + human log for RVU/indicator adds, deletes, flips per quarterly drop.  [oai_citation:18‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files?utm_source=chatgpt.com)
- **Parity checks (non-blocking)**: Sample recompute vs **PFS Look-Up Tool** national amounts (check facility vs non-facility).  [oai_citation:19‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/physician-fee-schedule/search/overview?utm_source=chatgpt.com)

## Design Decisions
- Ingestor is **storage + light QC** only; all pricing/sequestration/site-neutral logic lives later.
- Single source of truth per CMS artifact; MPFS views **reference** RVU tables.

## Runbook Hooks (see `prds/Runbook.md`)
- Spot-check 10–20 HCPCS+modifier per quarter against RVU pages.  [oai_citation:20‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu25a?utm_source=chatgpt.com)
- Validate locality/GPCI (5 samples across MACs).  [oai_citation:21‡Centers for Medicare & Medicaid Services](https://www.cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key?utm_source=chatgpt.com)
- Confirm **CY2025 CF** pinned in `mpfs_cf_vintage`.  [oai_citation:22‡Centers for Medicare & Medicaid Services](https://www.cms.gov/newsroom/press-releases/hhs-finalizes-physician-payment-rule-strengthening-person-centered-care-and-health-quality-measures?utm_source=chatgpt.com)

## Licensing / ToS
CMS public program data; record attribution + robots/ToS in manifests.