# Data Architecture PRD (v1.0)

## 0. Overview
This document defines the **Data Architecture Standard** used across projects. It specifies comprehensive data architecture including ingestion lifecycle, storage patterns, data modeling, database design, quality gates, versioning/vintage rules, security, observability, and operational runbooks. All dataset PRDs must conform to this standard and include a dataset‑specific **Ingestion Summary** (see §12 and template link in project docs).

**Status:** Adopted 1.0  
**Owners:** Platform/Data Engineering  
**Consumers:** Product, Analytics, Services  
**Change control:** ADR + PR review
For complete index see [Master System Catalog](DOC-master-catalog_prd_v1.0.md).  


**Cross-References:**
- **STD-observability-monitoring_prd_v1.0:** Comprehensive monitoring standards, five-pillar framework, and unified SLAs
- **STD-qa-testing_prd_v1.0:** Testing requirements for data pipelines and quality gates
- **STD-api-security-and-auth_prd_v1.0:** Security requirements for data access and audit logging  

## 1. Goals & Non‑Goals
**Goals**
- Provide one canonical, idempotent ingestion process for all external files/scrapes/feeds.
- Guarantee provenance, auditability, and reproducibility for every artifact.
- Enforce schema and domain contracts with measurable quality gates.
- Support historical backfills and rollbacks by vintage.
- Publish curated, join‑ready datasets with clear SLAs.

**Non‑Goals**
- Business logic specific to a single dataset (kept in dataset PRDs).
- Real‑time event ingestion (covered by Streaming Standard).

## 2. Definitions
- **Vintage:** The as‑published release date/window from the source (e.g., 2024-10 CMS).
- **Effective From/To:** The business validity window for a record.
- **Release ID:** Stable identifier for a fetched release (e.g., `cms_rvu_2024q4_r01`).
- **Batch ID:** UUID for a single pipeline execution.
- **Natural Key:** Domain key(s) that uniquely identify records (e.g., `geoid`, `hcpcs`, plus `effective_from`).

## 3. Architecture & Lifecycle
We separate concerns into **Land → Validate → Normalize → Enrich → Publish**, each with explicit contracts.

### 3.1 Data Contracts & Schema Governance
- **Contract-first ingestion:** Every source **must** declare a machine-readable schema (Avro/Protobuf/JSON Schema) with field types, nullability, enums, and semantic descriptions. Store the schema alongside code and reference it in the dataset PRD.
- **Schema Registry:** Register every schema + version. Ingestion resolves the expected version and validates incoming data **before** normalization. Allowed changes follow SemVer rules (backward-compatible additions vs. breaking changes), enforced by CI.
- **Drift policy:** Any uncontracted column, type change, or enum expansion is a **breaking event**. The batch fails or is quarantined; open an incident and coordinate a contract update via ADR.
- **Producer alignment:** Where we control producers (internal APIs), ship contract tests. For external sources, maintain **schema adapters** that map raw headers to the contract; adapters cannot silently coerce types beyond lossless casts.

### 3.2 Land (Raw)
- Download exact source files to immutable `/raw/<source>/<release_id>/files/`.
- Write `manifest.json` with: `source_url`, `license`, `fetched_at`, `sha256`, `size_bytes`, `content_type`, `discovered_from` (for scraped links), and optional `release_notes_url`.
- Raw artifacts are **never mutated**.

### 3.3 Validate
- Structural: required files present, headers/field counts as expected.
- Typing: strong casts (int/decimal/date/bool), locale‑safe parsing.
- Domain: codes within reference enumerations (FIPS, state, CMS codes, etc.).
- Statistical: row counts within tolerance vs prior vintage; null thresholds; basic drift checks.
- Uniqueness: natural keys unique within `release_id`.
- Rejects are quarantined at `/stage/.../reject/` with `error_code` + `error_msg` + sample rows.

### 3.4 Normalize (Stage)
- Canonicalize column names (snake_case), zero‑pad codes (FIPS/ZCTA), standardize units and time semantics.
- Emit a **Schema Contract** (JSON) and **Column Dictionary** (name, type, unit, description, domain).

### 3.5 Enrich
- Join to `/ref/` reference tables (e.g., Census crosswalks, Gazetteer centroids, FIPS codes).
- Apply tie‑breaker logic where mappings are one‑to‑many; compute `mapping_confidence`.

### 3.6 Publish (Curated)
- Snapshot tables partitioned by `vintage_date` and optionally `effective_from`.
- Publish **Latest‑Effective Views** using window functions over `effective_from DESC`.
- Generate export artifacts (Parquet/CSV) and **API caches** as needed.
- **API Readiness**: Support both header and query param for digest pins; header takes precedence
  - Header: `X-Dataset-Digest: sha256:…`
  - Query: `?digest=sha256:…`
  - Selection rule: if no digest provided, choose Latest ≤ valuation_date (per dataset)

## 4. Storage Layout
```
/raw/<source>/<release_id>/
    files/...                # immutable downloads
    manifest.json
/stage/<source>/<release_id>/
    normalized.parquet
    reject/...               # bad rows w/ reasons
/ref/...                     # static reference sets
/curated/<domain>/<dataset>/<vintage>/
    data/*.parquet           # snapshot
    docs/*.md                # data docs for this vintage
```

## 5. Naming & Conventions
- Columns: `snake_case`; units explicitly suffixed (e.g., `*_usd`, `*_pct`, `*_km`).
- Dates: ISO `YYYY-MM-DD`. Ranges are half‑open `[start, end)`.
- Keys: natural key(s) + `effective_from` for temporal uniqueness, plus surrogate `batch_id`.

## 6. Versioning, Vintage & Temporal Semantics
- Each curated snapshot is immutable, addressed by `vintage_date` (source release) and `release_id`.
- Records may have business validity windows (`effective_from`/`effective_to`).
- “Latest‑effective” consumption uses a view that picks the most recent record **effective on or before** the as‑of date.

## 7. Quality Gates (minimum bar)
- Structural: 100% required columns present; delimiter consistent.
- Uniqueness: 0 violations on declared natural keys.
- Completeness: critical columns null‑rate ≤ thresholds defined per dataset.
- Drift: row_count delta within ±15% vs previous vintage (warn) or ±30% (fail) unless flagged in PRD.
- Business sanity checks per dataset (e.g., symmetric distances, plausible ranges).

### 7.1 Error Handling & Quarantine Policy
- **No silent drops.** Records that fail any gate are written to a **Quarantine Zone** at `/stage/<source>/<release_id>/reject/` with: `batch_id`, `violation_rule_id`, `failure_reason`, and sample raw payload.
- **Batch policy:** If failure rate exceeds the dataset threshold (default 1%), the batch is marked **FAILED** and is not published. Otherwise, publish with a **partial badge** and emit a warning.
- **Triage workflow:** On failure, open an incident, assign Data Steward, attach sample rejects, and link to lineage to assess impact.

### 7.2 Idempotent Upserts & Reprocessing
- **Idempotency:** All publish steps use transactional **UPSERT/MERGE** keyed by the natural key + `effective_from/To`. Re-runs and backfills do not create duplicates nor regress valid records.
- **Determinism:** Given the same inputs and schema version, outputs are bitwise-identical (content-addressed artifacts). Seed dedupe uses row content hashes where appropriate.

## 8. Observability & Monitoring
- **References:** STD-observability-monitoring_prd_v1.0 for comprehensive monitoring standards
- **Data-specific monitoring:** Freshness, volume, schema, quality, lineage for data pipelines
- **Data SLAs:** Timeliness ≤ 24h, Completeness ≥ 99.5%, Accuracy ≥ 99.0%, Schema Stability 0 breaking changes, Availability ≥ 99.9%
- **Dashboards:** One dashboard per dataset with five-pillar widgets and last three vintages
- **Alerts:** Pager on freshness breach/schema drift/quality fail; Slack on volume drift warnings

*For detailed implementation: See Observability PRD Section 2.1 (Data Pipeline Freshness), Section 3.1 (Data Pipeline SLAs), and Section 4.1 (Data Pipeline Metrics)*

## 9. Security & Access
- Raw and Stage are read‑only to most roles; Curated is RBAC‑controlled by domain.
- Secrets (source creds) stored in a vault; no secrets in manifests.
- All artifacts carry licensing metadata; redistribution governed by license.

$1

### 10.1 Metadata & Catalog Requirements
- **Ingestion Runs Table:** Persist per-batch metadata: `release_id`, `batch_id`, source URLs, file hashes, row counts (in/out/rejects), schema version, quality scores, runtime, cost, and outcome.
- **Technical Metadata:** Auto-capture schema (column names/types), constraints, PII tags, and lineage edges; publish to a catalog (e.g., OpenMetadata/Amundsen/Unity Catalog).
- **Business Metadata:** Dataset Owner, Data Steward, business glossary for key fields, data classification, license, and intended use. Expose in catalog docs.
- **Automated Classification:** Each dataset declares **Data Classification**: Public / Internal / Confidential / Restricted. Classification drives encryption-at-rest, masking policies, and RBAC at ingestion and in curated views.
- **API Integration Requirements:** All curated datasets must expose API-ready views with trace blocks, vintage information, and digest pins for reproducibility.

## 11. Operations Runbook
**Smoke Test (pre‑merge)**
1) Sample download ≥1k rows/file; 2) run validators; 3) ensure curated_canary materializes.

**Deployment**
- Blue/green publish: write to `/curated_canary` for 24h; promote on green metrics.

**Incident Response**
- Severity classification, on‑call rotation, comms template.
- Standard steps: freeze pointer to “last good,” open incident, identify failing gate, hotfix or revert, backfill, RCA.

**Rollback**
- Curated snapshots are immutable; “current” pointer can be moved back to `last_good_release_id`.

**Backfill**
- Run with **frozen code tag** matching the era’s mapping rules; compare aggregates vs source PDFs.

## 12. Compliance & Licensing
- Every manifest includes `license` and `attribution_required`.
- Curated docs must include an **Attribution Note** when licenses require it.

## 13. Dataset Integration Contract
Every dataset PRD must include an **Ingestion Summary** stating: source spec, schema contract & keys, semantics, validations (with thresholds), crosswalks & tie‑breakers, outputs, SLAs, and any deviations from DIS. **API surfaces or contracts derived from DIS-managed datasets must comply with the API-STD-Architecture_prd_v1.0**.

## 14. Change Management
- Changes to DIS require an ADR: context, decision, alternatives, impact, migration plan.
- DIS version (e.g., v1.0) must be referenced by dataset PRDs.

## 15. QA Summary (per QA & Testing Standard v1.0)
| Item | Details |
| --- | --- |
| **Scope & Ownership** | DIS applies to all ingestion pipelines; owned by Platform/Data Engineering with QA Guild stewardship; consumers include downstream product, analytics, and pricing teams. |
| **Test Tiers & Coverage** | Unit: `tests/test_ingestion_pipeline.py`, `tests/test_effective_date_selection.py`; Component/Data-contract: `tests/test_golden.py` plus schema drift checks embedded in dataset suites; Integration: `tests/test_geography_ingestion.py` exercises end-to-end DIS flow; Scenario: nightly ingestion replay via `ci-nightly`. Target coverage ≥90% for shared ingestion code (current rolling avg 86%, reported in coverage dashboard). |
| **Fixtures & Baselines** | Golden manifests/parquet slices in `tests/golden/test_scenarios.jsonl`; RVU canonical fixtures under `tests/fixtures/rvu/`; baseline metrics tracked via QA warehouse dashboards (freshness, volume drift) with release digests recorded alongside `TESTING_SUMMARY.md`. |
| **Quality Gates** | Merge: `ci-unit.yaml` enforces unit + lint + coverage delta ≤0.5%; `ci-integration.yaml` runs DIS component suites with schema diff blockers; Release: `ci-nightly.yaml` executes replay + data-contract gates prior to tagging. |
| **Production Monitors** | Airflow/Dagster ingestion jobs emit freshness & volume drift metrics; manifest checksum monitor compares `/raw` artifacts vs prior run; PagerDuty alert on SLA breach (ingestion lag >24h) per §8. |
| **Manual QA** | Operator spot-check of new source manifests/runbooks (§11) before first production release; compliance review of licensing requirements (§12). |
| **Outstanding Risks / TODO** | Increase coverage on quarantine/error-handling paths (tracked in `TESTING_SUMMARY.md`); automate validation-warnings suite; finalize self-healing for manifest drift alerts. |

---

### Appendix A — Manifest JSON Schema (abridged)
```json
{
  "release_id": "string",
  "batch_id": "uuid",
  "source": "string",
  "files": [
    {"path":"string","sha256":"string","size_bytes":123,"content_type":"text/csv"}
  ],
  "fetched_at": "datetime",
  "discovered_from": "url",
  "source_url": "url",
  "license": {"name":"string","url":"url","attribution_required": true},
  "notes_url": "url"
}
```

### Appendix B — Column Naming & Types
- `snake_case` only.
- Geography codes zero‑padded: `state_fips` (2), `county_fips` (3), `geoid` (12), `zcta5` (5).
- Decimals: `DECIMAL(9,6)` for lat/lon; int for codes; `DATE` for day‑level.

### Appendix C — Validation Catalog (examples)
- **GE/DBT tests:** not_null, unique, accepted_values, relationships, range, custom drift (KS test / % change).
- **Business rules:** distance symmetry; max haversine discrepancy median ≤ 1.0 mile; GPCI code domains; payability status set.

### Appendix D — Reference Data (§/ref)
- Census Gazetteer, ZCTA, county/state crosswalks, FIPS, NBER distances, HRSA, CMS code sets.

### Appendix E — Sample DAG (pseudo)
```
@job
def ingest_dataset():
    raw = fetch_and_hash(urls)
    struct_ok = structural_validate(raw)
    staged = normalize(struct_ok)
    enriched = enrich(staged, ref)
    qc = run_quality_tests(enriched)
    publish_curated(enriched, qc)
    update_docs_and_metrics()
```

### Appendix F — RACI
- **Responsible:** Data Eng (build & run pipelines)
- **Accountable:** Head of Data
- **Consulted:** Product Analytics, Domain SMEs
- **Informed:** Platform, Domain Leads

### Appendix G — SLAs (defaults)
**Timeliness:** Land→Publish ≤ 24h (standard); Freshness alert at cadence + 3 days.

**Data Quality SLAs (unless overridden):**
- **Completeness:** ≥ 99.5% non-null on critical columns.
- **Validity/Accuracy:** ≥ 99.0% rows pass domain/relationship tests.
- **Schema Stability:** 0 uncontracted breaking changes per month.
- **Availability:** Latest-effective views ≥ 99.9% monthly.

**Monitoring:** Datasets must expose the five observability pillars (Freshness, Volume, Schema, Quality/Distribution, Lineage & Usage) in a standard dashboard.

### Appendix H — Deviation Process
- Any exception to DIS must be documented in the dataset PRD with a rationale and an end date; owner must open an ADR if the deviation persists beyond one vintage.

### Appendix I — Cross-Reference Map

**Related PRDs:**
- **STD-observability-monitoring_prd_v1.0:** Comprehensive monitoring standards and unified SLAs
- **STD-qa-testing_prd_v1.0:** Testing requirements for data pipelines and quality gates
- **STD-api-security-and-auth_prd_v1.0:** Security requirements for data access and audit logging

**Integration Points:**
- **Observability:** DIS Section 8 → Observability PRD Section 2.1 (Data Pipeline Freshness), Section 3.1 (Data Pipeline SLAs), Section 4.1 (Data Pipeline Metrics)
- **Testing:** DIS Section 7 → QTS Section 7 (Quality Gates), Section 2.5 (Test Accuracy Metrics)
- **Security:** DIS Section 9 → Security PRD Section 22.1 (Operational Runbooks), Section 10.4 (Extended Prometheus Metrics)


---

### Appendix I — Dataset Ingestion Summaries (Pre-filled Examples)

> The following DIS-conformant **Ingestion Summaries** are ready to paste into their respective dataset PRDs. Update owners, volumes, and any file-specific details during implementation.

---

# Ingestion Summary — CMS Locality & GPCI (conforms to DIS v1.0)
**Dataset:** CMS Physician Fee Schedule – Localities & GPCI  
**Domain:** payments/medicare  
**Owner:** Data Eng (Platform) + Medicare SME  
**Release cadence:** Annual main + ad-hoc corrections  
**Source type:** Download (ZIP + TXT/CSV/PDF notes)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS Carrier-Specific Files index; annual PFS/GPCI release pages.
- **Files & formats:** Fixed-width TXT (ZIP→Locality, Carrier Locality), CSV/XLS (GPCI indices), PDF notes.
- **Expected volume:** ~3–10 files per release; tens of thousands of rows.
- **Availability checks:** 200 OK, min size > 10KB/file, content-type text/* or application/zip.

## Schema Contract & Keys
- **Columns (core):** `zip5:char(5)`, `locality_code:string`, `carrier_id:string`, `state_fips:char(2)`, `county_fips:char(3)`, `locality_name:string`, `gpci_work:decimal(6,3)`, `gpci_pe:decimal(6,3)`, `gpci_malp:decimal(6,3)`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`locality_code`, `state_fips`, `effective_from`) for locality; (`zip5`, `effective_from`) for ZIP crosswalk; (`state_fips`,`locality_code`,`effective_from`) for GPCI rows.
- **Uniqueness:** Per entity + effective window.
- **Nullability:** `gpci_*` non-null; locality names non-null.

## Semantics & Temporal Logic
- **Vintage:** CMS publication window (e.g., CY2025 final rule + corrections).
- **Effective window:** From CMS “effective from” date; default open-ended until superseded.
- **Reporting period:** Yearly (calendar year), with mid-year corrections possible.

## Validations (Quality Gates)
- **Structural:** Required files present; fixed-width parse success rate ≥ 99.9%.
- **Domain:** `zip5` zero-padded; `gpci_*` within [0.3, 2.0]; locality codes alphanumeric.
- **Statistical:** Row count drift within ±15% vs prior vintage.
- **Business:** ZIP→locality coverage ≥ 95% of active ZIPs; GPCI triples present for each locality.

## Transforms & Normalization
- Header maps from CMS headers → canonical names; zero-pad FIPS/ZIP; cast decimals; derive `state_fips` if absent.

## Crosswalks & Tie-Breakers
- **Reference tables:** `/ref/census/fips_states`, `/ref/census/counties`, `/ref/zip/zcta_crosswalk`.
- **Join keys:** `zip5`, `state_fips`,`county_fips`.
- **Conflict policy:** If ZIP maps to multiple localities, keep all with `mapping_confidence='multi'`; consumers use latest-effective with business tie-breakers.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_pfs_localities/{vintage}`; `/curated/payments/cms_gpci/{vintage}`; `/curated/payments/cms_zip_to_locality/{vintage}`.
- **Views:** `v_latest_cms_locality`, `v_latest_cms_gpci`, `v_latest_zip_to_locality`.
- **Exports:** Parquet + CSV; optional API cache for locality lookups.
- **RBAC:** payments_read; raw/stage restricted.

## SLAs & Observability
- **Freshness:** ≤ 5 business days after CMS post.
- **Quality SLAs:** Completeness ≥ 99.5% criticals; Validity ≥ 99.0%.
- **Dashboards/alerts:** Five-pillar dashboard; pager on schema drift/parse failure.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS PFS RVU Items (PPRRVU) (conforms to DIS v1.0)
**Dataset:** CMS Physician Fee Schedule — RVU Items (PPRRVU)  
**Domain:** payments/medicare  
**Owner:** Data Eng (Platform) + Medicare SME  
**Release cadence:** Annual final + quarterly/adhoc corrections  
**Source type:** Download (ZIP with CSV/TXT; PDF notes)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS RVU release page (PPRRVU files).
- **Files & formats:** CSV/TXT; occasional Excel; accompanying readme/PDF.
- **Expected volume:** 5–10M rows/year across files.
- **Availability checks:** 200 OK, min size per file; header sanity.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `status_code:string`, `global_days:string?`, `rvu_work:decimal(8,3)`, `rvu_pe_nonfac:decimal(8,3)`, `rvu_pe_fac:decimal(8,3)`, `rvu_malp:decimal(8,3)`, `na_indicator:string?`, `opps_cap_applicable:boolean`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per HCPCS(+modifier) per effective window.
- **Nullability:** RVU components non-null when payable.

## Semantics & Temporal Logic
- **Vintage:** CMS publication window (CY).
- **Effective window:** From CMS effective date; superseded by later releases.

## Validations (Quality Gates)
- **Structural:** Required columns present; types cast.
- **Domain:** `status_code` ∈ {A,R,T,...}; `global_days` ∈ accepted set; RVUs within plausible ranges.
- **Statistical:** Row count drift ±20% warn.
- **Business:** For payable items, sum checks; PE nonfac/fac rules.

## Transforms & Normalization
- Canonical column names; trim codes; derive payability; coerce decimals.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS code sets; revenue center/OPPS mappings as needed.
- **Join keys:** `hcpcs`,`modifier`.
- **Conflict policy:** Prefer latest-effective on ties; keep alternates with `mapping_confidence`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_pprrvu/{vintage}`.
- **Views:** `v_latest_cms_pprrvu`.
- **Exports:** Parquet/CSV; API cache for lookups.
- **RBAC:** payments_read.

## SLAs & Observability
- **Freshness:** ≤ 5 business days post-release.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS OPPS-based Caps (conforms to DIS v1.0)
**Dataset:** CMS OPPS-based Payment Caps for PFS Services  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual + ad-hoc  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS supplemental files section.
- **Files & formats:** CSV/XLSX; notes in PDF.
- **Expected volume:** 10–100k rows.
- **Availability checks:** 200 OK; column count match.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `opps_cap_applies:boolean`, `cap_amount_usd:decimal(10,2)?`, `cap_method:string`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CMS publication.
- **Effective window:** As stated; superseded when updated.

## Validations (Quality Gates)
- **Structural:** Columns present; numeric cast success.
- **Domain:** `cap_amount_usd` ≥ 0 when applies.
- **Business:** Cross-check with PPRRVU payability rules.

## Transforms & Normalization
- Normalize dollars; standardize modifiers; boolean coercions.

## Crosswalks & Tie-Breakers
- **Reference tables:** PPRRVU latest-effective to resolve HCPCS universe.
- **Join keys:** `hcpcs`,`modifier`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_opps_caps/{vintage}`.
- **Views:** `v_latest_cms_opps_caps`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS Anesthesia Conversion Factors (conforms to DIS v1.0)
**Dataset:** CMS Anesthesia Conversion Factors (CFS)  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS/Anesthesia release pages.
- **Files & formats:** CSV/XLSX.
- **Expected volume:** Hundreds to thousands of rows.
- **Availability checks:** 200 OK; min size; header match.

## Schema Contract & Keys
- **Columns (core):** `locality_code:string`, `state_fips:char(2)`, `anesthesia_cf_usd:decimal(8,4)`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`state_fips`,`locality_code`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CY publication.
- **Effective window:** As stated; superseded by later.

## Validations (Quality Gates)
- **Domain:** CF within plausible range; locality exists in locality table.
- **Business:** Join coverage with locality universe ≥ 99%.

## Transforms & Normalization
- Normalize dollars; pad codes; link to locality metadata.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS Localities (latest-effective), state FIPS.
- **Join keys:** `state_fips`,`locality_code`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_anesthesia_cf/{vintage}`.
- **Views:** `v_latest_cms_anesthesia_cf`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — ZIP↔ZCTA/County Crosswalk (OpenICPSR) (conforms to DIS v1.0)
**Dataset:** ZIP-to-ZCTA and ZIP-to-County Crosswalks (OpenICPSR V2)  
**Domain:** geography  
**Owner:** Data Eng + Geo SME  
**Release cadence:** Ad-hoc (per new version)  
**Source type:** Download (CSV/TSV)  
**License:** As per OpenICPSR terms; Attribution required: Y

## Source Spec
- **Discovery:** OpenICPSR project 120088 (vintage V2 link); file list scraped.
- **Files & formats:** CSV/TSV; metadata readme.
- **Expected volume:** 10–50MB.
- **Availability checks:** 200 OK; column count; encoding.

## Schema Contract & Keys
- **Columns (core):** `zip5:char(5)`, `zcta5:char(5)`, `county_fips:char(5)`, `share_pct:decimal(5,4)?`, `method:string?`, `vintage:int`, `effective_from:date`, `release_id:string`.
- **Natural key(s):** (`zip5`,`zcta5`,`county_fips`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** Project release version/date.
- **Effective window:** As documented; else use `vintage_date`.

## Validations (Quality Gates)
- **Domain:** zero-pad codes; shares within [0,1]; ZIP/ZCTA format.
- **Business:** Coverage of active ZIPs ≥ 95%; sum of shares per `zip5` ≈ 1±0.01.

## Transforms & Normalization
- Normalize headers; shares to 0–1 scale; compute `mapping_confidence`.

## Crosswalks & Tie-Breakers
- **Reference tables:** FIPS states/counties; Census ZCTA.
- **Conflict policy:** Equal weight or dominant per PRD; preserve many-to-many with weights.

## Outputs & Access
- **Curated path:** `/curated/geo/zip_crosswalks/{vintage}`.
- **Views:** `v_latest_zip_zcta_county_crosswalk`.

## SLAs & Observability
- **Freshness:** N/A (static until new version).
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — HRSA Data Downloads (conforms to DIS v1.0)
**Dataset:** HRSA Facilities & Shortage Areas (e.g., HPSA/MUA/P, NHSC sites)  
**Domain:** providers/access  
**Owner:** Data Eng + Public Health SME  
**Release cadence:** Monthly (varies by table)  
**Source type:** Download (CSV via HRSA data portal)  
**License:** HRSA Open Data; Attribution required: Y

## Source Spec
- **Discovery:** HRSA data portal index pages; latest CSV endpoints.
- **Files & formats:** CSV; UTF-8.
- **Expected volume:** 10–100MB per entity/month.
- **Availability checks:** 200 OK, content-length > 100KB, schema hash check.

## Schema Contract & Keys
- **Columns (core):** `site_id:string`, `site_name:string`, `address:string`, `city:string`, `state:string`, `zip5:char(5)`, `county_fips:char(5)?`, `latitude:decimal(9,6)`, `longitude:decimal(9,6)`, `hpsa_id:string?`, `hpsa_type:string?`, `status:string`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`site_id`, `effective_from`) for facilities; (`hpsa_id`,`hpsa_type`,`effective_from`) for shortage areas.
- **Uniqueness:** Per entity + effective window.
- **Nullability:** lat/lon non-null for geocoded rows.

## Semantics & Temporal Logic
- **Vintage:** HRSA portal “last updated” date per dataset.
- **Effective window:** From dataset’s effective date fields; else use `vintage_date` as `effective_from`.

## Validations (Quality Gates)
- **Structural:** Required columns present; UTF-8 decode success.
- **Domain:** `state` ∈ USPS set; `zip5` valid pattern; lat/lon within US bounding box.
- **Statistical:** Row count drift ±25% warn; >±40% fail unless flagged.
- **Business:** Site geos must resolve to a county via Gazetteer ≥ 99.0%.

## Transforms & Normalization
- Canonicalize addresses; zero-pad ZIP; normalize state to USPS; cast lat/lon decimals.

## Crosswalks & Tie-Breakers
- **Reference tables:** `/ref/census/gazetteer_centroids`, `/ref/census/county_fips`, `/ref/zip/zcta_crosswalk`.
- **Join keys:** `zip5`, `county_fips` (derived), spatial point-in-polygon as fallback.
- **Conflict policy:** Multiple county matches resolved by point-in-polygon; else nearest centroid with `mapping_confidence`.

## Outputs & Access
- **Curated path:** `/curated/providers/hrsa_sites/{vintage}`, `/curated/providers/hrsa_hpsa/{vintage}`.
- **Views:** `v_latest_hrsa_sites`, `v_latest_hrsa_hpsa`.
- **Exports:** Parquet/CSV; optional geojson.
- **RBAC:** providers_read; raw restricted.

## SLAs & Observability
- **Freshness:** ≤ 10 business days after HRSA update.
- **Quality SLAs:** Completeness ≥ 99.5% crits; Validity ≥ 99.0%.
- **Dashboards/alerts:** Five-pillar dashboard; pager on freshness breach.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS PFS RVU Items (PPRRVU) (conforms to DIS v1.0)
**Dataset:** CMS Physician Fee Schedule — RVU Items (PPRRVU)  
**Domain:** payments/medicare  
**Owner:** Data Eng (Platform) + Medicare SME  
**Release cadence:** Annual final + quarterly/adhoc corrections  
**Source type:** Download (ZIP with CSV/TXT; PDF notes)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS RVU release page (PPRRVU files).
- **Files & formats:** CSV/TXT; occasional Excel; accompanying readme/PDF.
- **Expected volume:** 5–10M rows/year across files.
- **Availability checks:** 200 OK, min size per file; header sanity.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `status_code:string`, `global_days:string?`, `rvu_work:decimal(8,3)`, `rvu_pe_nonfac:decimal(8,3)`, `rvu_pe_fac:decimal(8,3)`, `rvu_malp:decimal(8,3)`, `na_indicator:string?`, `opps_cap_applicable:boolean`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per HCPCS(+modifier) per effective window.
- **Nullability:** RVU components non-null when payable.

## Semantics & Temporal Logic
- **Vintage:** CMS publication window (CY).
- **Effective window:** From CMS effective date; superseded by later releases.

## Validations (Quality Gates)
- **Structural:** Required columns present; types cast.
- **Domain:** `status_code` ∈ {A,R,T,...}; `global_days` ∈ accepted set; RVUs within plausible ranges.
- **Statistical:** Row count drift ±20% warn.
- **Business:** For payable items, sum checks; PE nonfac/fac rules.

## Transforms & Normalization
- Canonical column names; trim codes; derive payability; coerce decimals.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS code sets; revenue center/OPPS mappings as needed.
- **Join keys:** `hcpcs`,`modifier`.
- **Conflict policy:** Prefer latest-effective on ties; keep alternates with `mapping_confidence`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_pprrvu/{vintage}`.
- **Views:** `v_latest_cms_pprrvu`.
- **Exports:** Parquet/CSV; API cache for lookups.
- **RBAC:** payments_read.

## SLAs & Observability
- **Freshness:** ≤ 5 business days post-release.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS OPPS-based Caps (conforms to DIS v1.0)
**Dataset:** CMS OPPS-based Payment Caps for PFS Services  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual + ad-hoc  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS supplemental files section.
- **Files & formats:** CSV/XLSX; notes in PDF.
- **Expected volume:** 10–100k rows.
- **Availability checks:** 200 OK; column count match.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `opps_cap_applies:boolean`, `cap_amount_usd:decimal(10,2)?`, `cap_method:string`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CMS publication.
- **Effective window:** As stated; superseded when updated.

## Validations (Quality Gates)
- **Structural:** Columns present; numeric cast success.
- **Domain:** `cap_amount_usd` ≥ 0 when applies.
- **Business:** Cross-check with PPRRVU payability rules.

## Transforms & Normalization
- Normalize dollars; standardize modifiers; boolean coercions.

## Crosswalks & Tie-Breakers
- **Reference tables:** PPRRVU latest-effective to resolve HCPCS universe.
- **Join keys:** `hcpcs`,`modifier`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_opps_caps/{vintage}`.
- **Views:** `v_latest_cms_opps_caps`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS Anesthesia Conversion Factors (conforms to DIS v1.0)
**Dataset:** CMS Anesthesia Conversion Factors (CFS)  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS/Anesthesia release pages.
- **Files & formats:** CSV/XLSX.
- **Expected volume:** Hundreds to thousands of rows.
- **Availability checks:** 200 OK; min size; header match.

## Schema Contract & Keys
- **Columns (core):** `locality_code:string`, `state_fips:char(2)`, `anesthesia_cf_usd:decimal(8,4)`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`state_fips`,`locality_code`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CY publication.
- **Effective window:** As stated; superseded by later.

## Validations (Quality Gates)
- **Domain:** CF within plausible range; locality exists in locality table.
- **Business:** Join coverage with locality universe ≥ 99%.

## Transforms & Normalization
- Normalize dollars; pad codes; link to locality metadata.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS Localities (latest-effective), state FIPS.
- **Join keys:** `state_fips`,`locality_code`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_anesthesia_cf/{vintage}`.
- **Views:** `v_latest_cms_anesthesia_cf`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — ZIP↔ZCTA/County Crosswalk (OpenICPSR) (conforms to DIS v1.0)
**Dataset:** ZIP-to-ZCTA and ZIP-to-County Crosswalks (OpenICPSR V2)  
**Domain:** geography  
**Owner:** Data Eng + Geo SME  
**Release cadence:** Ad-hoc (per new version)  
**Source type:** Download (CSV/TSV)  
**License:** As per OpenICPSR terms; Attribution required: Y

## Source Spec
- **Discovery:** OpenICPSR project 120088 (vintage V2 link); file list scraped.
- **Files & formats:** CSV/TSV; metadata readme.
- **Expected volume:** 10–50MB.
- **Availability checks:** 200 OK; column count; encoding.

## Schema Contract & Keys
- **Columns (core):** `zip5:char(5)`, `zcta5:char(5)`, `county_fips:char(5)`, `share_pct:decimal(5,4)?`, `method:string?`, `vintage:int`, `effective_from:date`, `release_id:string`.
- **Natural key(s):** (`zip5`,`zcta5`,`county_fips`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** Project release version/date.
- **Effective window:** As documented; else use `vintage_date`.

## Validations (Quality Gates)
- **Domain:** zero-pad codes; shares within [0,1]; ZIP/ZCTA format.
- **Business:** Coverage of active ZIPs ≥ 95%; sum of shares per `zip5` ≈ 1±0.01.

## Transforms & Normalization
- Normalize headers; shares to 0–1 scale; compute `mapping_confidence`.

## Crosswalks & Tie-Breakers
- **Reference tables:** FIPS states/counties; Census ZCTA.
- **Conflict policy:** Equal weight or dominant per PRD; preserve many-to-many with weights.

## Outputs & Access
- **Curated path:** `/curated/geo/zip_crosswalks/{vintage}`.
- **Views:** `v_latest_zip_zcta_county_crosswalk`.

## SLAs & Observability
- **Freshness:** N/A (static until new version).
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — Census ZCTA & Gazetteer (conforms to DIS v1.0)
**Dataset:** Census ZCTA (vintage 2020/2023) & Gazetteer Place/County/Zip Centroids  
**Domain:** geography  
**Owner:** Data Eng + Geo SME  
**Release cadence:** Annual (Gazetteer), by-vintage (ZCTA)  
**Source type:** Download (TXT/CSV)  
**License:** U.S. Census terms; Attribution required: Y

## Source Spec
- **Discovery:** Census ZCTA/Gazetteer product pages (latest vintage).
- **Files & formats:** TXT/CSV; fixed columns; UTF-8.
- **Expected volume:** 50–150MB total.
- **Availability checks:** 200 OK, min size per file, column count check.

## Schema Contract & Keys
- **Columns (core):** `zcta5:char(5)`, `geoid:string`, `state_fips:char(2)`, `county_fips:char(3)`, `name:string`, `lat:decimal(9,6)`, `lon:decimal(9,6)`, `population:int?`, `land_area_km2:decimal(12,3)?`, `water_area_km2:decimal(12,3)?`, `vintage:int`, `effective_from:date`, `release_id:string`.
- **Natural key(s):** (`zcta5`,`vintage`) for ZCTA; (`geoid`,`vintage`) for Gazetteer records.
- **Uniqueness:** Per vintage.
- **Nullability:** lat/lon non-null.

## Semantics & Temporal Logic
- **Vintage:** Census product year (e.g., 2023 Gazetteer).
- **Effective window:** `effective_from` = vintage start (Jan 1 of vintage year).

## Validations (Quality Gates)
- **Structural:** All required columns; numeric cast success ≥ 99.99%.
- **Domain:** zero-pad codes; `lat` in [-90,90], `lon` in [-180,180].
- **Statistical:** Row count drift ≤ ±10% vs prior vintage.
- **Business:** GEOGRAPHY codes must exist in FIPS reference; centroid plausibility (haversine to state centroid < 1,000 km).

## Transforms & Normalization
- Unit conversions to km²; consistent decimals; code padding.

## Crosswalks & Tie-Breakers
- **Reference tables:** `/ref/census/fips_states`, `/ref/census/fips_counties`.
- **Join keys:** `state_fips`,`county_fips`,`zcta5`.
- **Conflict policy:** When multiple names per code, prefer official Gazetteer record; mark alternates.

## Outputs & Access
- **Curated path:** `/curated/geo/zcta/{vintage}`, `/curated/geo/gazetteer/{vintage}`.
- **Views:** `v_latest_zcta`, `v_latest_gazetteer`.
- **Exports:** Parquet + CSV.
- **RBAC:** geo_read.

## SLAs & Observability
- **Freshness:** ≤ 15 business days after new vintage.
- **Quality SLAs:** Completeness ≥ 99.9% crits; Validity ≥ 99.5%.
- **Dashboards/alerts:** Five-pillar dashboard; pager on schema drift.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS PFS RVU Items (PPRRVU) (conforms to DIS v1.0)
**Dataset:** CMS Physician Fee Schedule — RVU Items (PPRRVU)  
**Domain:** payments/medicare  
**Owner:** Data Eng (Platform) + Medicare SME  
**Release cadence:** Annual final + quarterly/adhoc corrections  
**Source type:** Download (ZIP with CSV/TXT; PDF notes)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS RVU release page (PPRRVU files).
- **Files & formats:** CSV/TXT; occasional Excel; accompanying readme/PDF.
- **Expected volume:** 5–10M rows/year across files.
- **Availability checks:** 200 OK, min size per file; header sanity.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `status_code:string`, `global_days:string?`, `rvu_work:decimal(8,3)`, `rvu_pe_nonfac:decimal(8,3)`, `rvu_pe_fac:decimal(8,3)`, `rvu_malp:decimal(8,3)`, `na_indicator:string?`, `opps_cap_applicable:boolean`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per HCPCS(+modifier) per effective window.
- **Nullability:** RVU components non-null when payable.

## Semantics & Temporal Logic
- **Vintage:** CMS publication window (CY).
- **Effective window:** From CMS effective date; superseded by later releases.

## Validations (Quality Gates)
- **Structural:** Required columns present; types cast.
- **Domain:** `status_code` ∈ {A,R,T,...}; `global_days` ∈ accepted set; RVUs within plausible ranges.
- **Statistical:** Row count drift ±20% warn.
- **Business:** For payable items, sum checks; PE nonfac/fac rules.

## Transforms & Normalization
- Canonical column names; trim codes; derive payability; coerce decimals.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS code sets; revenue center/OPPS mappings as needed.
- **Join keys:** `hcpcs`,`modifier`.
- **Conflict policy:** Prefer latest-effective on ties; keep alternates with `mapping_confidence`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_pprrvu/{vintage}`.
- **Views:** `v_latest_cms_pprrvu`.
- **Exports:** Parquet/CSV; API cache for lookups.
- **RBAC:** payments_read.

## SLAs & Observability
- **Freshness:** ≤ 5 business days post-release.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS OPPS-based Caps (conforms to DIS v1.0)
**Dataset:** CMS OPPS-based Payment Caps for PFS Services  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual + ad-hoc  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS supplemental files section.
- **Files & formats:** CSV/XLSX; notes in PDF.
- **Expected volume:** 10–100k rows.
- **Availability checks:** 200 OK; column count match.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `opps_cap_applies:boolean`, `cap_amount_usd:decimal(10,2)?`, `cap_method:string`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CMS publication.
- **Effective window:** As stated; superseded when updated.

## Validations (Quality Gates)
- **Structural:** Columns present; numeric cast success.
- **Domain:** `cap_amount_usd` ≥ 0 when applies.
- **Business:** Cross-check with PPRRVU payability rules.

## Transforms & Normalization
- Normalize dollars; standardize modifiers; boolean coercions.

## Crosswalks & Tie-Breakers
- **Reference tables:** PPRRVU latest-effective to resolve HCPCS universe.
- **Join keys:** `hcpcs`,`modifier`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_opps_caps/{vintage}`.
- **Views:** `v_latest_cms_opps_caps`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS Anesthesia Conversion Factors (conforms to DIS v1.0)
**Dataset:** CMS Anesthesia Conversion Factors (CFS)  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS/Anesthesia release pages.
- **Files & formats:** CSV/XLSX.
- **Expected volume:** Hundreds to thousands of rows.
- **Availability checks:** 200 OK; min size; header match.

## Schema Contract & Keys
- **Columns (core):** `locality_code:string`, `state_fips:char(2)`, `anesthesia_cf_usd:decimal(8,4)`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`state_fips`,`locality_code`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CY publication.
- **Effective window:** As stated; superseded by later.

## Validations (Quality Gates)
- **Domain:** CF within plausible range; locality exists in locality table.
- **Business:** Join coverage with locality universe ≥ 99%.

## Transforms & Normalization
- Normalize dollars; pad codes; link to locality metadata.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS Localities (latest-effective), state FIPS.
- **Join keys:** `state_fips`,`locality_code`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_anesthesia_cf/{vintage}`.
- **Views:** `v_latest_cms_anesthesia_cf`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — ZIP↔ZCTA/County Crosswalk (OpenICPSR) (conforms to DIS v1.0)
**Dataset:** ZIP-to-ZCTA and ZIP-to-County Crosswalks (OpenICPSR V2)  
**Domain:** geography  
**Owner:** Data Eng + Geo SME  
**Release cadence:** Ad-hoc (per new version)  
**Source type:** Download (CSV/TSV)  
**License:** As per OpenICPSR terms; Attribution required: Y

## Source Spec
- **Discovery:** OpenICPSR project 120088 (vintage V2 link); file list scraped.
- **Files & formats:** CSV/TSV; metadata readme.
- **Expected volume:** 10–50MB.
- **Availability checks:** 200 OK; column count; encoding.

## Schema Contract & Keys
- **Columns (core):** `zip5:char(5)`, `zcta5:char(5)`, `county_fips:char(5)`, `share_pct:decimal(5,4)?`, `method:string?`, `vintage:int`, `effective_from:date`, `release_id:string`.
- **Natural key(s):** (`zip5`,`zcta5`,`county_fips`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** Project release version/date.
- **Effective window:** As documented; else use `vintage_date`.

## Validations (Quality Gates)
- **Domain:** zero-pad codes; shares within [0,1]; ZIP/ZCTA format.
- **Business:** Coverage of active ZIPs ≥ 95%; sum of shares per `zip5` ≈ 1±0.01.

## Transforms & Normalization
- Normalize headers; shares to 0–1 scale; compute `mapping_confidence`.

## Crosswalks & Tie-Breakers
- **Reference tables:** FIPS states/counties; Census ZCTA.
- **Conflict policy:** Equal weight or dominant per PRD; preserve many-to-many with weights.

## Outputs & Access
- **Curated path:** `/curated/geo/zip_crosswalks/{vintage}`.
- **Views:** `v_latest_zip_zcta_county_crosswalk`.

## SLAs & Observability
- **Freshness:** N/A (static until new version).
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — NBER Precomputed Distances (conforms to DIS v1.0)
**Dataset:** NBER ZIP-to-ZIP Distance Matrix  
**Domain:** geography  
**Owner:** Data Eng + Geo SME  
**Release cadence:** Ad-hoc (static)  
**Source type:** Download (CSV/TXT)  
**License:** NBER terms; Attribution required: Y

## Source Spec
- **Discovery:** NBER distance data page; documented file names.
- **Files & formats:** CSV/TXT; large matrix format.
- **Expected volume:** 100–500MB depending on coverage.
- **Availability checks:** 200 OK, min size, column count.

## Schema Contract & Keys
- **Columns (normalized):** `zip5_a:char(5)`, `zip5_b:char(5)`, `distance_miles:decimal(8,3)`, `method:string` (e.g., centroid), `vintage:int?`, `release_id:string`.
- **Natural key(s):** (`zip5_a`,`zip5_b`); enforce symmetry (`a,b` == `b,a`).
- **Uniqueness:** Global.
- **Nullability:** distances non-null.

## Semantics & Temporal Logic
- **Vintage:** Source publication date; typically static.
- **Effective window:** Open-ended.

## Validations (Quality Gates)
- **Structural:** Two ZIP columns + one distance column present.
- **Domain:** `zip5_*` valid pattern; distances ≥ 0 and < 6,000.
- **Statistical:** Sample triangle inequality checks; symmetric pairs coverage ≥ 99.9%.
- **Business:** Median |NBER − Haversine(ZCTA centroids)| ≤ 1.0 mile.

## Transforms & Normalization
- Canonicalize to long form (`a,b,distance`); generate symmetric pairs if missing; compute `mapping_confidence` vs haversine baseline.

## Crosswalks & Tie-Breakers
- **Reference tables:** `/ref/geo/zcta_centroids`.
- **Join keys:** `zip5`.
- **Conflict policy:** If multiple methods present, prefer “great-circle” or documented official; mark alternates.

## Outputs & Access
- **Curated path:** `/curated/geo/nber_zip_distances/{vintage}`.
- **Views:** `v_zip_distance_latest`.
- **Exports:** Parquet/CSV; optional fast-path key-value cache.
- **RBAC:** geo_read; cache restricted.

## SLAs & Observability
- **Freshness:** N/A (static). Re-ingest only on source update.
- **Quality SLAs:** Symmetry ≥ 99.99%; triangle check failure ≤ 0.1%.
- **Dashboards/alerts:** Volume/schema checks; business check alerts.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS PFS RVU Items (PPRRVU) (conforms to DIS v1.0)
**Dataset:** CMS Physician Fee Schedule — RVU Items (PPRRVU)  
**Domain:** payments/medicare  
**Owner:** Data Eng (Platform) + Medicare SME  
**Release cadence:** Annual final + quarterly/adhoc corrections  
**Source type:** Download (ZIP with CSV/TXT; PDF notes)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS RVU release page (PPRRVU files).
- **Files & formats:** CSV/TXT; occasional Excel; accompanying readme/PDF.
- **Expected volume:** 5–10M rows/year across files.
- **Availability checks:** 200 OK, min size per file; header sanity.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `status_code:string`, `global_days:string?`, `rvu_work:decimal(8,3)`, `rvu_pe_nonfac:decimal(8,3)`, `rvu_pe_fac:decimal(8,3)`, `rvu_malp:decimal(8,3)`, `na_indicator:string?`, `opps_cap_applicable:boolean`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per HCPCS(+modifier) per effective window.
- **Nullability:** RVU components non-null when payable.

## Semantics & Temporal Logic
- **Vintage:** CMS publication window (CY).
- **Effective window:** From CMS effective date; superseded by later releases.

## Validations (Quality Gates)
- **Structural:** Required columns present; types cast.
- **Domain:** `status_code` ∈ {A,R,T,...}; `global_days` ∈ accepted set; RVUs within plausible ranges.
- **Statistical:** Row count drift ±20% warn.
- **Business:** For payable items, sum checks; PE nonfac/fac rules.

## Transforms & Normalization
- Canonical column names; trim codes; derive payability; coerce decimals.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS code sets; revenue center/OPPS mappings as needed.
- **Join keys:** `hcpcs`,`modifier`.
- **Conflict policy:** Prefer latest-effective on ties; keep alternates with `mapping_confidence`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_pprrvu/{vintage}`.
- **Views:** `v_latest_cms_pprrvu`.
- **Exports:** Parquet/CSV; API cache for lookups.
- **RBAC:** payments_read.

## SLAs & Observability
- **Freshness:** ≤ 5 business days post-release.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS OPPS-based Caps (conforms to DIS v1.0)
**Dataset:** CMS OPPS-based Payment Caps for PFS Services  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual + ad-hoc  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS supplemental files section.
- **Files & formats:** CSV/XLSX; notes in PDF.
- **Expected volume:** 10–100k rows.
- **Availability checks:** 200 OK; column count match.

## Schema Contract & Keys
- **Columns (core):** `hcpcs:string`, `modifier:string?`, `opps_cap_applies:boolean`, `cap_amount_usd:decimal(10,2)?`, `cap_method:string`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`hcpcs`,`modifier?`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CMS publication.
- **Effective window:** As stated; superseded when updated.

## Validations (Quality Gates)
- **Structural:** Columns present; numeric cast success.
- **Domain:** `cap_amount_usd` ≥ 0 when applies.
- **Business:** Cross-check with PPRRVU payability rules.

## Transforms & Normalization
- Normalize dollars; standardize modifiers; boolean coercions.

## Crosswalks & Tie-Breakers
- **Reference tables:** PPRRVU latest-effective to resolve HCPCS universe.
- **Join keys:** `hcpcs`,`modifier`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_opps_caps/{vintage}`.
- **Views:** `v_latest_cms_opps_caps`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — CMS Anesthesia Conversion Factors (conforms to DIS v1.0)
**Dataset:** CMS Anesthesia Conversion Factors (CFS)  
**Domain:** payments/medicare  
**Owner:** Data Eng + Medicare SME  
**Release cadence:** Annual  
**Source type:** Download (CSV/XLSX/PDF)  
**License:** CMS Open Data; Attribution required: Y

## Source Spec
- **Discovery:** CMS PFS/Anesthesia release pages.
- **Files & formats:** CSV/XLSX.
- **Expected volume:** Hundreds to thousands of rows.
- **Availability checks:** 200 OK; min size; header match.

## Schema Contract & Keys
- **Columns (core):** `locality_code:string`, `state_fips:char(2)`, `anesthesia_cf_usd:decimal(8,4)`, `effective_from:date`, `effective_to:date?`, `release_id:string`.
- **Natural key(s):** (`state_fips`,`locality_code`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** CY publication.
- **Effective window:** As stated; superseded by later.

## Validations (Quality Gates)
- **Domain:** CF within plausible range; locality exists in locality table.
- **Business:** Join coverage with locality universe ≥ 99%.

## Transforms & Normalization
- Normalize dollars; pad codes; link to locality metadata.

## Crosswalks & Tie-Breakers
- **Reference tables:** CMS Localities (latest-effective), state FIPS.
- **Join keys:** `state_fips`,`locality_code`.

## Outputs & Access
- **Curated path:** `/curated/payments/cms_anesthesia_cf/{vintage}`.
- **Views:** `v_latest_cms_anesthesia_cf`.

## SLAs & Observability
- **Freshness:** ≤ 5 business days.
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

---

# Ingestion Summary — ZIP↔ZCTA/County Crosswalk (OpenICPSR) (conforms to DIS v1.0)
**Dataset:** ZIP-to-ZCTA and ZIP-to-County Crosswalks (OpenICPSR V2)  
**Domain:** geography  
**Owner:** Data Eng + Geo SME  
**Release cadence:** Ad-hoc (per new version)  
**Source type:** Download (CSV/TSV)  
**License:** As per OpenICPSR terms; Attribution required: Y

## Source Spec
- **Discovery:** OpenICPSR project 120088 (vintage V2 link); file list scraped.
- **Files & formats:** CSV/TSV; metadata readme.
- **Expected volume:** 10–50MB.
- **Availability checks:** 200 OK; column count; encoding.

## Schema Contract & Keys
- **Columns (core):** `zip5:char(5)`, `zcta5:char(5)`, `county_fips:char(5)`, `share_pct:decimal(5,4)?`, `method:string?`, `vintage:int`, `effective_from:date`, `release_id:string`.
- **Natural key(s):** (`zip5`,`zcta5`,`county_fips`,`effective_from`).
- **Uniqueness:** Per effective window.

## Semantics & Temporal Logic
- **Vintage:** Project release version/date.
- **Effective window:** As documented; else use `vintage_date`.

## Validations (Quality Gates)
- **Domain:** zero-pad codes; shares within [0,1]; ZIP/ZCTA format.
- **Business:** Coverage of active ZIPs ≥ 95%; sum of shares per `zip5` ≈ 1±0.01.

## Transforms & Normalization
- Normalize headers; shares to 0–1 scale; compute `mapping_confidence`.

## Crosswalks & Tie-Breakers
- **Reference tables:** FIPS states/counties; Census ZCTA.
- **Conflict policy:** Equal weight or dominant per PRD; preserve many-to-many with weights.

## Outputs & Access
- **Curated path:** `/curated/geo/zip_crosswalks/{vintage}`.
- **Views:** `v_latest_zip_zcta_county_crosswalk`.

## SLAs & Observability
- **Freshness:** N/A (static until new version).
- **Quality SLAs:** Completeness ≥ 99.5%; Validity ≥ 99.0%.

## Deviations from DIS
- None.

##Standard repo shape (shared across all ingestors)
ingestion/
  contracts/            # Avro/JSON Schemas (versioned)
  adapters/             # header/type adapters per dataset (no lossy casts)
  validators/           # structural/domain/statistical checks (reused)
  enrichers/            # generic join utilities (FIPS, Gazetteer, crosswalks)
  publishers/           # snapshot + latest-effective, MERGE helpers
  quarantine/           # write + metadata helpers
  observability/        # metrics emitters (five pillars) + alerts
  run/                  # Dagster/Prefect jobs wiring stages together


##Minimal interface each ingestor implements
class IngestorSpec:
    dataset_name: str
    release_cadence: str
    contract_schema_ref: str  # registry key (e.g., "cms.gpci:1.2.0")
    discovery: Callable[[], list[SourceFile]]        # find URLs/files
    adapter: Callable[[RawBatch], AdaptedBatch]      # header/type mapping
    validators: list[ValidationRule]                 # domain/stat checks
    enricher: Callable[[StageFrame, RefData], StageFrame]
    outputs: OutputSpec                              # curated paths, view names
    slas: SlaSpec                                    # freshness, quality targets
    classification: DataClass                        # Public/Internal/…


## APPENDIX
Perfect. I’ve locked the decisions and turned them into a clean appendix you can paste under “Reference Data Integration” in the DIS.

Appendix — Reference Data Integration (Final)

A. Structure & Inventory

/ref/
  census/
    fips_states/<vintage>/{data.parquet, manifest.json}
    fips_counties/<vintage>/{data.parquet, manifest.json}
    gazetteer_centroids/<vintage>/{data.parquet, manifest.json}
    zcta/<vintage>/{data.parquet, manifest.json}
    cbsa_msa/<vintage>/{data.parquet, manifest.json}
    ruca/<vintage>/{data.parquet, manifest.json}
  cms/
    hcpcs_codes/<vintage>/{data.parquet, manifest.json}
    status_indicators/<vintage>/{data.parquet, manifest.json}
    pos_codes/<vintage>/{data.parquet, manifest.json}
    cpt_level_1/<vintage>/{data.parquet, manifest.json}          # licensed (Restricted)
  geo/
    zip_zcta_crosswalk/<vintage>/{data.parquet, manifest.json}
    nber_distances/<vintage>/{data.parquet, manifest.json}
    timezones/<vintage>/{data.parquet, manifest.json}
  providers/
    npi_taxonomy/<vintage>/{data.parquet, manifest.json}
    ein_npi_crosswalk/<vintage>/{data.parquet, manifest.json}     # Confidential

	•	Format: Parquet for production, JSON/CSV seeds allowed only for dev fixtures.
	•	Schema: Each ref set has a contract in the Schema Registry (SemVer).
	•	Vintage: See §C for standard rules.

B. Integration Points (Runtime)
	•	Enrichers read curated /ref snapshots (never in-repo JSON).
	•	Hot-key KV/LRU caches: zip→locality, zip→zcta, zip_pair→distance.
	•	Emit mapping_confidence ∈ {exact,weighted,nearest,pip_fallback,ambiguous} on all geo enrichments.

C. Vintage Semantics
	•	vintage_date = source publication timestamp when available; else use source-provided effective date.
	•	Keep product_year as an attribute for readability (e.g., “2023 Gazetteer”).
	•	Records may also have effective_from/to columns used by latest-effective views.

D. Precedence & Constraint Policy

Why: Conflicting refs change pricing/eligibility; deterministic rules protect consistency and auditability.

D.1 Precedence (highest → lowest)
	1.	CMS official lists (payment codes, localities, POS)
	2.	Gazetteer shapes PIP (true spatial containment)
	3.	Census/ICPSR crosswalks (tabular relations)
	4.	Nearest centroid ≤ 1.0 mile (haversine fallback)
	5.	Internal fallbacks (flagged)

Always retain alternates with confidence labels for audit.

D.2 Constraints (tiered)
	•	Block publish (critical): unknown HCPCS/CPT/POS, invalid FIPS, missing locality/GPCI key.
	•	Warn + quarantine rows (non-critical): ZIP↔ZCTA disagreements, distance anomalies, missing county when other geography still usable.

E. Tie-Breakers & Thresholds

Goal: pick the closest/most faithful mapping while staying explainable.

E.1 Defaults
	•	ZIP→ZCTA: preserve many-to-many with weights; dominant used in “simple” views; else equal split.
	•	ZIP→County: PIP > crosswalk > nearest ≤ 1.0 mi, else ambiguous.
	•	Distance: prefer NBER; if present, keep even when far from haversine but flag deltas.
	•	Code lists: unknown HCPCS/CPT/APC/POS → block in curated; allowed in staging (quarantine) for triage.

E.2 Thresholds
	•	Share sum tolerance (ZIP↔ZCTA): 1.00 ± 0.01.
	•	NBER vs haversine delta: median ≤ 1.0 mi, p95 ≤ 3.0 mi (else alert).
	•	Nearest fallback radius (ZIP→county): ≤ 1.0 mi; beyond → ambiguous, no auto-assign.

F. Classifications & Licensing

Ref set	Classification	Notes
Census FIPS, Gazetteer, ZCTA, CBSA, RUCA	Public	Attribution required in docs/UI
CMS code lists, localities, POS	Public	Attribution required
NBER distances	Internal	Treat conservatively; attribution required
Time zones	Internal	Derived mapping tables
NPI taxonomy	Internal	No PHI/PII; verify license terms
EIN↔NPI crosswalk	Confidential	Access-limited; potential PII/organizational sensitivity
CPT Level I	Restricted	Licensed content; no redistribution; masked exports

RBAC/masking is driven automatically from this classification.

G. Freshness SLAs & Breach Handling

Ref set	Freshness SLA	Breach action
CMS code lists/localities/POS	≤ 5 business days	Block publish if stale or schema drift
CPT Level I	≤ 10 business days	Block publish on schema drift; warn if source timing delayed by vendor
Gazetteer/ZCTA/CBSA/RUCA	≤ 15 business days after vintage	Warn (block only on schema contract failure)
ZIP↔ZCTA/County (ICPSR)	≤ 30 business days	Warn
NBER distances	Static; monthly check	Warn if unexpected change detected

H. Ownership & Stewardship (solo-friendly)

Until more contributors join:
	•	Owner (all ref sets): Platform / Data Eng (Arnina)
	•	Steward role: defaults to Owner. When SMEs exist, assign: Geo SME for Census/NBER/CBSA/RUCA; Medicare SME for CMS; Governance/Licensing for CPT; Provider SME for EIN↔NPI/NPI taxonomy.
	•	Incident paging and ADR authorship default to Owner.

I. Retention
	•	Hot: keep last 3 vintages of each ref set online.
	•	Archive: all older vintages to cold storage with manifests and checksums.
	•	Critical (CMS/CPT): optionally keep up to 12 months hot before archiving.

J. Reference Data Management
	•	Loader enforces schema registry validation, writes manifest.json (url, sha256, size, license, vintage, schema_version).
	•	Freshness monitors per ref set, wired into the five-pillar dashboards.
	•	Conflict metrics: track ambiguous, pip_fallback, distance deltas; expose in dashboards.
	•	Any change to precedence/tie-breaker logic requires an ADR.

K. Tests (minimum per ref set)
	•	NBER: symmetry + triangle spot checks; delta vs haversine thresholds.
	•	FIPS: 100% coverage and valid lengths/zero-padding.
	•	ZIP↔ZCTA: share sum within ±0.01; dominant correctness; fallback radius honored.
	•	CMS/CPT/POS: accepted values; no unknowns; SemVer schema checks.
	•	Gazetteer/ZCTA: lat/lon bounds; PIP containment sanity.

⸻

If you want me to apply this appendix into the DIS canvas and wire the classifications/SLAs into the existing tables, say the word and I’ll slot it in cleanly.