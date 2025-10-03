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

> TODO: Populate full PRD content (objective, scope, data sources, validations, QA summary).
