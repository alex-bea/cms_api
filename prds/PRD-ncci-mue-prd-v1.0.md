# PRD — NCCI MUE Ingestion (v1.0)

**Status:** Draft v1.0  
**Owners:** Pricing Platform Product & Engineering  
**Consumers:** Data Engineering, Pricing API, Compliance  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns
- **STD-qa-testing-prd-v1.0.md:** Testing requirements for NCCI MUE ingestion

## Data Classification & Stewardship
- **Classification:** Public CMS program data (Internal enriched analytics)  
- **License & Attribution:** CMS NCCI edits/MUE files (public domain with required CMS citation)  
- **Data Owner / Steward:** Pricing Platform Product (owner), Compliance liaison (SME), Data Engineering (technical steward)  
- **Distribution Policy:** External sharing requires CMS attribution; downstream APIs must adhere to **STD-api-security-and-auth-prd-v1.0.md**

## Ingestion Summary (DIS v1.0)
- **Sources & Cadence:** CMS NCCI Quarterly MUE tables (CSV/XLS), procedure-to-procedure edits, and reference documentation PDFs; quarterly cadence with ad hoc corrections  
- **Schema Contracts:** `cms_pricing/ingestion/contracts/ncci_mue_v1.json` (to be finalized) capturing HCPCS, modifier flags, MUE counts, provider types, effective dates  
- **Landing Layout:** `/raw/ncci_mue/{release_id}/files/*` with DIS `manifest.json` capturing `source_url`, `license`, `fetched_at`, `sha256`, `size_bytes`, `release_notes_url`  
- **Natural Keys & Partitioning:** Keyed by `(hcpcs, modifier, provider_type, effective_from)`; curated partitions by `vintage_date` and `edit_type`  
- **Validations & Gates:** Structural file presence, schema contract enforcement, MUE count numeric checks, provider-type enumeration, delta diffs vs prior quarter, alignment with CMS reference counts  
- **Quarantine Policy:** Failed rows quarantined under `/stage/ncci_mue/{release_id}/reject/`; publish blocks on any critical structural/contract violations  
- **Enrichment & Crosswalks:** Optional join to MPFS RVU tables for analytics; track provider category metadata and effective ranges  
- **Outputs:** `/curated/ncci_mue/{vintage}/ncci_mue.parquet`, materialized views `ncci_mue_latest` and `ncci_mue_history` for analytics and compliance review  
- **SLAs:** Land within ≤5 business days of CMS posting; publish within ≤7 business days; manifest records dataset digest for reproducibility  
- **Deviations:** None; finalize schema contract + reference links before GA

## API Readiness & Distribution
- **Warehouse Views:** `vw_ncci_mue_current(date, provider_type)` returns latest-effective limits; history view supports diffing  
- **Digest Pinning:** APIs expose `X-Dataset-Digest`/`?digest` to guarantee reproducibility  
- **Security:** Internal APIs only until compliance approves broader release; follow **STD-api-security-and-auth-prd-v1.0.md** for auth/logging

## Objective  
Ingest and persist CMS National Correct Coding Initiative (NCCI) reference tables—procedure-to-procedure (PTP) edits and Medically Unlikely Edits (MUEs)—to support downstream rule engines, claim validation, and analytics. This PRD focuses on authoritative storage and quality; adjudication logic lives downstream.

## Scope  
- **Included**  
  - PTP edit tables with Column 1 / Column 2 code pairs, modifier indicators, and effective dating  
  - MUE tables covering HCPCS, MUE counts, MAI indicators, provider-type qualifiers, and effective ranges  
  - Supporting CMS documentation (PDFs, errata summaries) retained alongside manifests  
- **Excluded (v1)**  
  - Real-time adjudication logic (bundling enforcement, payer overrides)  
  - Claim-level transformations or rule-evaluation services  

## Sources & Cadence  
- PTP edits refreshed quarterly by CMS, with ad hoc correction releases  
- MUE tables published quarterly or annually depending on program area  
- Monthly monitor job to detect late-breaking updates; retain ≥6 years of vintages for audit  

## Schema / Data Model  

### Raw tables  
- `ncci_ptp_raw_YYYYQ` — direct ingestion of CMS PTP edit files  
- `ncci_mue_raw_YYYYQ` — raw MUE tables per CMS release  

### Curated views  
- `ncci_ptp` — normalized PTP edits with `col1_code`, `col2_code`, `modifier_flag`, `effective_from`, `effective_to`, `notes`, `source_vintage`  
- `ncci_mue` — normalized MUE entries capturing `hcpcs`, `modifier`, `provider_type`, `mue_value`, `mai_indicator`, and effective dating  

## Keys & Joins  
- PTP analytics join MPFS RVU data on `(hcpcs, modifier, quarter_vintage)` to evaluate bundling impacts  
- MUE joins rely on `(hcpcs, modifier, provider_type, effective_from)`; always preserve original fields to avoid losing CMS semantics  
- No business logic embedded in the ingester—downstream services enforce the edits  

## Quality / QC & Diffing  
- Ensure required columns are present for every vintage; fail ingest on schema drift  
- Idempotent downloads with checksum + content-type verification  
- Quarter-to-quarter diff reports highlighting newly added/retired PTP pairs and MUE value changes  
- Store manifest metadata (source URLs, last-modified, license notes) for every release  

## Licensing / ToS  
CMS publishes the NCCI datasets as public program data—record attribution in manifests and note any license language from CMS distribution pages.

## Roadmap / Extensions  
- Link curated tables into downstream rule engines for automated adjudication  
- Track CMS correction tables/errata and fold them into historical retention  
- Explore support for payer-specific variants once CMS baselines are stable
