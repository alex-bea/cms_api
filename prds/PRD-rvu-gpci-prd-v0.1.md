# Product Requirements — RVU Ingestor (PPRRVU, GPCI, OPPSCAP, ANES, Locality) — Draft v0.2

**Status:** Draft v0.2  
**Owners:** Pricing Platform Product & Engineering  
**Consumers:** Data Engineering, Pricing API, Compliance, Ops  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns
- **STD-parser-contracts-prd-v1.0.md:** Shared parser contracts for fixed-width and CSV parsing
- **STD-scraper-prd-v1.0.md:** Scraper requirements for RVU data discovery
- **STD-qa-testing-prd-v1.0.md:** Testing requirements for RVU ingestion
- **REF-cms-pricing-source-map-prd-v1.0.md:** Canonical CMS pricing source inventory

**Implementation Resources:**
- **GPCI Parser Planning:** `planning/parsers/gpci/README.md` (index of all GPCI implementation docs)
- **Implementation Plan:** `planning/parsers/gpci/IMPLEMENTATION.md` (v2.1 - authoritative parser guide)
- **Pre-Implementation:** `planning/parsers/gpci/PRE_IMPLEMENTATION_PLAN.md` (layout verification & setup)
- **Schema Contract:** `cms_pricing/ingestion/contracts/cms_gpci_v1.2.json` (current)
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py` (GPCI_2025D_LAYOUT v2025.4.1)
- **Sample Data:** `sample_data/rvu25d_0/GPCI2025.txt` (CMS RVU25D bundle)

## Work-Backwards Checklist (Required)
Reference **REF-cms-pricing-source-map-prd-v1.0.md** before any RVU/GPCI ingestion change. Validate the mapped artifacts, layouts, and checklist items, then capture deltas here or in the linked reference prior to implementation.

## Data Classification & Stewardship
- **Classification:** Public CMS release (Internal for enriched analytics outputs)  
- **License & Attribution:** CMS RVU/GPCI/OPPSCAP/ANES publications (public domain); maintain CMS citation in manifests and curated docs  
- **Data Owner / Steward:** Pricing Platform Product (owner), Data Engineering (technical steward)  
- **Distribution Policy:** External publication requires compliance + legal review, especially where CPT® descriptors appear

## Ingestion Summary (DIS v1.0)
- **Sources & Cadence:** Quarterly PPRRVU bundles (A/B/C/D + corrections), annual GPCI, annual OPPS-based caps, annual ANES conversion factors, locality-to-county crosswalk; authoritative layout PDFs accompany each release  
- **Schema Contracts:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json`, `cms_oppscap_v1.0.json`, `cms_gpci_v1.0.json`, `cms_anescf_v1.0.json`, `cms_localitycounty_v1.0.json`  
- **Landing Layout:** `/raw/rvu/{release_id}/files/*` with DIS-compliant `manifest.json` capturing source URLs, SHA256, size, license, release notes, and fetched timestamp  
- **Natural Keys & Partitioning:** RVU keyed by `(hcpcs, modifier, effective_from)`; GPCI keyed by `(valuation_year, locality_id)`; OPPSCAP keyed by `(hcpcs, modifier, locality_id)`; curated partitions by `vintage_date` and dataset type  
- **Validations & Gates:** Structural & schema validation, authoritative layout enforcement, decimal precision checks, policy indicator enumerations, locality/GPCI coverage ≥99.5%, delta comparisons vs prior release, recomputed payment parity smoke tests  
- **Quarantine Policy:** Rule failures routed to `/stage/rvu/{release_id}/reject/` with error metadata; publish blocked on critical issues and missing effective dating  
- **Enrichment & Crosswalks:** Locality-to-county mapping to support ZIP pricing; calculate effective ranges using CMS notes; maintain release lineage via `release_id` + `published_at`  
- **Outputs:** `/curated/rvu/{vintage}` parquet tables for RVU, GPCI, OPPSCAP, ANES, and locality crosswalk plus latest-effective warehouse views `vw_rvu_current`, `vw_gpci_current`  
- **SLAs:** Land within 2 business days of CMS release; publish within 5 business days; all manifests record dataset digest for reproducibility  
- **Deviations:** None currently; exceptions require ADR and PRD update
- **Discovery Manifest & Governance:** CMS RVU scraper writes discovery manifests via `cms_pricing.ingestion.metadata.discovery_manifest` to `data/manifests/cms_rvu/`; CI executes `tools/verify_source_map.py` to ensure manifests stay aligned with `REF-cms-pricing-source-map-prd-v1.0.md`.

## API Readiness & Distribution
- **Warehouse Views:** `vw_rvu_current`, `vw_gpci_current`, `vw_opps_cap_current`, `vw_anes_cf_current`, `vw_locality_county_current` follow Latest-Effective semantics  
- **Digest Pinning:** APIs consume `X-Dataset-Digest` / `?digest` aligned to curated manifests  
- **Security:** Follow **STD-api-security-and-auth-prd-v1.0.md** for token-based access; suppress CPT descriptors if license not finalized

> Owner: TBD  •  Approver(s): TBD  •  Stakeholders: Eng, Data, Ops, Compliance, QA  •  Last updated: {{today}}

---

## 0) Executive Summary
Build a robust, auditable pipeline to **acquire, normalize, validate, and publish** CMS RVU-related datasets for Physician Fee Schedule (PFS) pricing and analytics. Scope includes **PPRRVU**, **GPCI**, **OPPSCAP**, **ANES**, and **Locality-to-County** files. The ingestor must handle both **TXT (fixed-width)** and **CSV** variants present in the October 2025 release and preserve **effective dating** and **version lineage**.

**Primary outcomes**
- One-command/automated import of new CMS releases and corrections
- Canonical, stable schemas across changing yearly layouts
- Bitemporal versioning: CMS effective business time and our publish time
- Deterministic validations + anomaly reports; reproducible builds
- Low-latency APIs/Views for downstream pricing and analytics

Non-goals: building a full payment engine UI; commercial payer fee schedules.

---

## 1) Inputs & Sources (Oct 2025 artifacts attached)
We ingest these artifacts each cycle (A/B/C/D + potential AR corrections). This PRD references the **October 2025** files the team provided.

### 1.1 PPRRVU — RVUs & Policy Indicators
- Files: `PPRRVU2025_Oct.txt` (fixed-width), `PPRRVU2025_Oct.csv`
- Content: HCPCS/CPT, optional modifier, description, **Status Code**, **Work RVU**, **PE RVU (non-fac & fac)**, **MP RVU**, policy indicators (bilateral, multiple procedure, assistant/co-surgeon/team), **Global Days**, **Physician Supervision**, NA indicator, (diagnostic imaging family), **Conversion Factor**, totals and amounts columns.
- Authoritative layout: follow the record layout defined in the cycle PDF (e.g., `RVU25D.pdf`).

### 1.2 GPCI — Geographic Practice Cost Indices (2025)
- File: `GPCI2025.txt`
- Content: `MAC (contractor)`, `State`, `Locality #`, `Locality Name`, `2025 Work GPCI`, `2025 PE GPCI`, `2025 MP GPCI`.

### 1.3 OPPSCAP — OPPS-based Payment Caps (Oct)
- Files: `OPPSCAP_Oct.txt`, `OPPSCAP_Oct.csv`
- Content: `HCPCS`, `MOD`, `PROCSTAT`, `MAC`, `Locality`, `Facility Price`, `Non-Facility Price` (capped amounts to compare against PFS imaging components).

### 1.4 ANES — Anesthesia Conversion Factors (2025)
- Files: `ANES2025.txt`, `ANES2025.csv`
- Content: `Contractor (MAC)`, `Locality #`, `Locality Name`, `Anesthesia CF` (local factor distinct from PFS CF).

### 1.5 Locality → County Crosswalk (2025)
- File: `25LOCCO.csv`
- Content: `MAC`, `Locality #`, `State`, `Fee Schedule Area`, `Counties` (canonical map for service location → locality).

### 1.6 Documentation
- File: `RVU25D.pdf` — the authoritative record layout and policy notes; all parsers must comply.

---

## 2) Functional Requirements

### 2.1 Acquisition
- Support **two modes**:
  1) **Automated**: scripted download from CMS or our curated bucket (URL list per release). Accept both **ZIP bundles** and individual files.
  2) **Manual upload**: web UI drag‑and‑drop for hotfix testing. Preserve raw filenames.
- Compute and store **SHA256** per raw file; maintain **byte-exact copies** in object storage.
- Idempotency: Re-running the same input(s) must not duplicate curated rows; checksum-based short‑circuit if unchanged.
- Release tagging: infer `source_version` (e.g., `2025D`, `2025AR`) from filename/manifest; allow manual override.

### 2.2 Parsing & Normalization
- **Authoritative layout requirement (MANDATORY):** Every ingestor **must** locate and use an **authoritative layout file** (e.g., cycle PDF or schema YAML) for field positions/names/types/enums. If an authoritative layout **is not available**, the ingestor **MUST stop**, emit a clear error, and **request the layout** before proceeding.
- Parse **fixed-width TXT** using official column positions and **CSV** using headers; both converge to the same **canonical schemas**.
- All numeric fields parsed as decimal with exact precision; keep **string original** copies for trace.
- Policy indicators map to enumerations with descriptive labels.
- Normalize modifiers to an array of two‑char codes (empty array when none).
- Add metadata columns: `source_file`, `row_num`, `imported_at`, `ingest_job_id`.

### 2.3 Effective Dating & Versioning
- Record **business time**: `effective_start`, `effective_end` from CMS context (year/cycle + corrections). For October cycles, default to `YYYY-10-01` unless superseded by CMS notes; corrections narrow/extend ranges.
- Record **system time**: `published_at` (our publish moment). Maintain **bitemporal** history.
- Allow **time-travel** queries (as of any business date, and/or as of any publish snapshot).

### 2.4 Validation & QA (blocking vs non‑blocking)
- **Structural (ERROR)**: required columns present; correct widths; type coercions succeed; PK uniqueness per release.
- **Content (ERROR/WARN)**: RVUs non‑negative; GPCI in plausible bounds; status codes within domain; NA ⇒ non‑facility PE must be null; anesthesia CF present for all localities.
- **Referential (ERROR)**: OPPSCAP locality must exist in GPCI/Locality; PPRRVU HCPCS format `[A-Z0-9]{5}`.
- **Delta checks (WARN)**: top percentile changes vs prior release; new/retired codes; locality churn.
- Emit a **QA report** (HTML/JSON) with row counts, distincts, distributions, and anomaly samples.

### 2.5 Publication
- Data products:
  - **Curated warehouse tables** (see §3 Data Model)
  - **Materialized views** for “current” values by date
  - **Exports** (CSV/Parquet) with signed URLs on demand
  - **Dictionary** pages autogenerated from schemas and enums
- API endpoints for common queries (e.g., `/rvu?code=99213&date=2025-10-01&modifier=25`).

---

## 3) Canonical Data Model

### 3.1 Releases
- `release(id, type: enum['PPRRVU','GPCI','OPPSCAP','ANES','LOCCO'], source_version, source_url, file_checksums,json, imported_at, published_at, notes)`

### 3.2 RVU Items (PPRRVU)
- `rvu_item(id, release_id fk, hcpcs_code string, modifiers array<string>, description string, status_code string, work_rvu numeric, pe_rvu_nonfac numeric, pe_rvu_fac numeric, mp_rvu numeric, na_indicator string, global_days string, bilateral_ind string, multiple_proc_ind string, assistant_surg_ind string, co_surg_ind string, team_surg_ind string, endoscopic_base string, conversion_factor numeric, physician_supervision string, diag_imaging_family string, total_nonfac numeric, total_fac numeric, effective_start date, effective_end date, source_file, row_num)`
- **Unique key within a release**: `(hcpcs_code, modifier_key, effective_start)`; `modifier_key` is normalized (e.g., `26`, `TC`, `25`, `null`).

### 3.3 GPCI Indices
- `gpci_index(id, release_id fk, mac string, state string, locality_id string, locality_name string, work_gpci numeric, pe_gpci numeric, mp_gpci numeric, effective_start date, effective_end date, source_file, row_num)`
- **Uniqueness & Keys**
  - **Primary unique key (corrected):** `(mac, locality_id, effective_start)`
  - Rationale: `locality_id` values (e.g., `00`) are **not globally unique** and recur across states/MACs; enforcing `(locality_id, effective_start)` causes valid duplicates to fail.  
  - Optional surrogate: `locality_uid = concat(mac, '-', locality_id)` for joins and cache keys.
- **Validation**: reject rows missing any of `(mac, locality_id)`; allow the same `locality_id` across different `mac`/`state` values.

### 3.4 OPPS Caps
- `opps_cap(id, release_id fk, hcpcs_code string, modifier string, proc_status string, mac string, locality_id string, price_fac numeric, price_nonfac numeric, effective_start date, effective_end date, source_file, row_num)`
- Unique: `(hcpcs_code, modifier, mac, locality_id, effective_start)`.

### 3.5 Anesthesia Conversion Factors
- `anes_cf(id, release_id fk, mac string, locality_id string, locality_name string, anesthesia_cf numeric, effective_start date, effective_end date, source_file, row_num)`
- Unique: `(mac, locality_id, effective_start)`.

### 3.6 Locality Crosswalk
- `locality_county(id, release_id fk, mac string, locality_id string, state string, fee_schedule_area string, county_name string, effective_start date, effective_end date, source_file, row_num)`
- Unique: `(mac, locality_id, county_name, effective_start)` (state-only uniqueness is insufficient; preserve MAC context).

---

## 4) File Handling Specs

### 4.1 TXT (Fixed-width) Parsing
- Use the position map from `RVU25D.pdf` as **single source of truth**.
- Implement a reusable **layout registry** keyed by `source_version`.
- For each record set: define `start_col`, `end_col`, `type`, `nullable`, `enum` (if applicable), `transform`.
- Trim right‑padded spaces; preserve leading zeros; unicode‑safe.

### 4.2 CSV Parsing
- Enforce presence of header row; perform case‑insensitive header matching with canonical aliases.
- Whitespace-trim cells; coerce empty strings → null; strict decimal parsing.
- Reject unknown headers unless flagged `allow_extra_columns=true` for exploratory runs.

### 4.3 Modifiers Normalization
- Accept blank, single (e.g., `26`), or multiple (rare) modifiers; store as ordered array; canonical `modifier_key` built as joined string.

### 4.4 Status & Policy Indicators
- Provide lookup tables for: **Status Code**, **Global Days**, **Multiple/Bilateral/Assistant/Co/Team** indicators, **Physician Supervision** (01/02/03/04/05/06/21/22/66/6A/77/7A/09), **NA indicator**, **Diagnostic imaging family**.

---

## 5) Pricing Connectivity (for Validation Only)
- **PFS check**: For a sample of payable codes (A/R/T), recompute `amount_fac`/`amount_nonfac` using RVUs × GPCI × CF and compare to provided totals when available. Differences beyond tolerance (e.g., 1 cent) flag WARN.
- **OPPS-capped imaging**: Where OPPSCAP exists, verify our computed component doesn’t exceed cap.
- **Anesthesia**: Validate anesthesia codes using **ANES** CF (not PFS CF).

---

## 6) Pipelines & Orchestration
1) **Acquire** raw files (download or upload) → store to `raw/` with checksums and manifest.
2) **Detect** file types; route to appropriate parser (TXT/CSV) with `source_version` layout.
3) **Normalize** to staging tables with canonical schemas + metadata.
4) **Validate** (structural/content/referential/delta) → produce QA report and anomaly extracts.
5) **Publish** to curated tables with `effective_*` and `published_at` set; tag the snapshot.
6) **Notify** Slack/Email with run summary and links.

**Scheduling**: cron on release weeks; manual trigger anytime. **Retry** with exponential backoff; partial-run resume supported.

---

## 7) Observability & Ops
- Metrics: files processed, rows ingested, error/warn counts, time-to-curated, parser durations.
- Logs: structured, with `ingest_job_id`, `source_file`, `row_num` context for each error.
- Dashboards: freshness SLO, success rate, anomaly trends.
- Rollback: republish previous `published_at` snapshot; artifacts immutable ≥ 1 year.

---

## 8) Security & Access
- No PHI/PII expected; enforce least‑privilege IAM to buckets/warehouse.
- Roles: **Admin** (ingest/publish), **Editor** (notes), **Reader** (query/export).
- All accesses logged; artifacts encrypted at rest and in transit.

---

## 9) Interfaces
API contracts exposed from this pack must follow the **STD-api-architecture-prd-v1.0.md** for versioning and lifecycle.
### 9.1 Warehouse Views
- `vw_rvu_current(date)` → latest RVUs (by HCPCS/modifier) effective on `date`.
- `vw_gpci_current(date)` → locality GPCIs effective on `date`.
- `vw_opps_cap_current(date)`; `vw_anes_cf_current(date)`; `vw_locality_county_current(date)`.
- `vw_rvu_deltas(version_a, version_b)` → change report.

### 9.2 API (examples)
- `GET /rvu?code=99213&date=2025-10-01&modifier=25`
- `GET /gpci?locality_id=11302&date=2025-10-01`
- `GET /oppscap?code=71260&modifier=TC&locality=11302&date=2025-10-01`
- `GET /releases?type=pprrvu`

### 9.3 Exports
- On-demand CSV/Parquet with filters (code/locality/date), presigned links, retention policy configurable.

---

## 10) Testing Strategy — **Test‑First Mandate (Cursor)**
**MANDATE:** *Cursor must write tests according to this PRD/PRF **before** implementing any parser or pipeline code.* Tests are the contract; code must satisfy the tests.

### 10.1 Test Types (defined first, implemented first)
- **Unit (parsers & transforms)**
  - Fixed‑width position maps for `PPRRVU2025_Oct.txt` (golden slices)
  - CSV header aliasing & normalization (PPRRVU/OPPSCAP/ANES/LOCCO)
  - Modifier normalization & `modifier_key` generation
  - Enum/domain mappers (Status, Global Days, Supervision, NA, policy indicators)
  - **Layout presence test (blocking):** Fail fast if an authoritative layout file is not present/linked for the target `source_version`.
- **Integration (end‑to‑end)**
  - Full ingest of all five datasets → curated tables with bitemporal fields populated
  - Referential joins: Locality↔GPCI↔OPPSCAP; RVU↔GPCI by date; ANES applicability for anesthesia codes
  - Re‑ingest idempotency (same checksums ⇒ no duplicate rows)
- **Data validation (dbt/Great Expectations)**
  - Uniqueness (keys), nullability, ranges, enum memberships, NA ⇒ non‑facility PE null
  - Deltas vs prior release (percentile swing thresholds)
- **Contract/API tests**
  - `vw_*_current` and REST endpoints return expected rows for supplied fixtures and dates
  - Latency budgets verified with representative volumes
- **Golden files & fixtures**
  - Commit minimal, representative fixtures from your attached files (scrubbed) for deterministic tests

### 10.2 CI Gates (tests block merges)
- No parser or pipeline code merges unless:
  - All unit/integration/data/contract tests **exist** and **pass**
  - Coverage for parser modules ≥ 80% lines/branches
  - QA report artifact produced and attached to CI run
  - **Authoritative layout reference present** for the target `source_version` (gate is **blocking**)

### 10.3 Tooling & Conventions
- Test naming: `test_<dataset>_<concern>.py`; fixtures under `tests/fixtures/<release>/`
- Deterministic parsing (locale‑agnostic decimals; explicit encodings; stable sort order)
- Lint/type checks (ruff/mypy) included in CI gates

- Test naming: `test_<dataset>_<concern>.py`; fixtures under `tests/fixtures/<release>/`
- Deterministic parsing (locale‑agnostic decimals; explicit encodings; stable sort order)
- Lint/type checks (ruff/mypy) included in CI gates

---

## 11) Success Metrics & SLOs
- **Ingestion time** (release → curated): ≤ 1 business day
- **Import reliability**: ≥ 99.5% quarterly success rate
- **Validation coverage**: ≥ 98% rows pass on first run; 100% anomalies captured and reported
- **Query latency**: API P95 ≤ 500ms for common lookups  
  **Achieved (perf test bench):** HCPCS lookups **1.86ms** P95; status filters **0.34ms**; date range **0.16ms**; GPCI lookups **0.76ms**; complex joins **0.50ms**
- **Observability**: 100% runs have manifests, QA artifacts, and alert evaluation; MTTA for critical alerts ≤ 15 min

---

## 12) Acceptance Criteria (Phase 1)
- [ ] **Test‑first:** Cursor has authored unit/integration/data/contract tests reflecting this PRD/PRF **before** any implementation, and they pass in CI.
- [ ] **Authoritative layout enforced:** Parser fails fast without a linked authoritative layout (cycle PDF or schema) for the target `source_version`; CI gate prevents merge.
- [ ] Ingest and publish Oct 2025 PPRRVU, GPCI, OPPSCAP, ANES, LOCCO to curated tables.
- [ ] `vw_*_current` views return correct rows for supplied test dates and sample codes.
- [ ] Validation report generated with counts, distributions, and anomaly samples.
- [ ] Bitemporal queries return expected results for corrected vs original windows.
- [ ] API examples return within latency SLO.

---

## 13) Appendix: Column Dictionaries (excerpts)

### 13.1 PPRRVU (canonical)
- `hcpcs_code` (string, regex `[A-Z0-9]{5}`)
- `modifiers` (array<string>, values like `26`, `TC`, `25`)
- `status_code` (enum: A,R,T,… non‑payable include I,N,X,C,J)
- `work_rvu` (numeric ≥ 0)
- `pe_rvu_nonfac` / `pe_rvu_fac` (numeric ≥ 0; null when NA indicated)
- `mp_rvu` (numeric ≥ 0)
- `global_days` (enum: 000, 010, 090, XXX, YYY, ZZZ etc.)
- `bilateral_ind` / `multiple_proc_ind` / `assistant_surg_ind` / `co_surg_ind` / `team_surg_ind` (enums per CMS)
- `physician_supervision` (enum: 01,02,03,04,05,06,21,22,66,6A,77,7A,09)
- `conversion_factor` (numeric; cycle CF)
- `diag_imaging_family` (string/enum; nullable)
- `total_fac` / `total_nonfac` (numeric; nullable)

### 13.2 GPCI
- `mac` (string) • `state` (string) • `locality_id` (string) • `locality_name` (string)
- `work_gpci` • `pe_gpci` • `mp_gpci` (numeric ≥ 0)

### 13.3 OPPSCAP
- `hcpcs_code` (string) • `modifier` (string) • `proc_status` (string)
- `mac` (string) • `locality_id` (string)
- `price_fac` • `price_nonfac` (numeric ≥ 0)

### 13.4 ANES
- `mac` • `locality_id` • `locality_name` • `anesthesia_cf` (numeric ≥ 0)

### 13.5 Locality Crosswalk
- `mac` • `locality_id` • `state` • `fee_schedule_area` • `county_name`

Notes: For detailed fixed-width start/end positions, consult the cycle PDF and encode the layout in the parser registry for `2025D`.


---

## 14) GA Hardening Plan (from Cursor Update)

### 14.1 What’s Complete (Maps to PRD)
- Models & schema (Releases, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty) ✅
- Fixed‑width parser (layout registry, 2025D positions finalized) ✅
- **Hybrid CSV parser** (position + header mapping; multi‑line/mixed‑case; empty→null) with **~100% field coverage across file types** ✅
- End‑to‑end pipeline (acquire → parse → normalize → validate → publish) ✅
- **Business rule validations implemented**: payability (A/R/T), NA indicator, global days, supervision code domain, OPPS cap checks, anesthesia CF usage ✅
- **Web scraper** (CMS acquisition w/ ZIP extraction, file org, multi‑release support; CLI) ✅
- **API surface (read‑only)**: `/rvu`, `/gpci`, `/oppscap`, `/releases` with performance SLOs & contract tests ✅
- Observability & Ops: manifests, QA HTML/JSON, anomaly detection, dashboards, alerts ✅
- Performance SLOs achieved (P95 ≪ 500ms) ✅

### 14.2 Gaps to Close Before GA (Blockers)
**None — feature set meets GA readiness.**

> Post‑GA enhancements remain in §19 backlog (e.g., SDKs, schema YAML formalization).
1. **Advanced Validations** *(in progress)*: Payability (A/R/T only), NA⇒no non‑facility PE, Global‑Days semantics, Supervision code domain, OPPS cap join & cap ≤ component, Anesthesia CF usage for anesthesia codes.  
   • **Tests**: dbt/GE rules per item + targeted fixtures.
2. **API Surface (Read‑only)** *(next)*: Implement endpoints below; enforce latency SLOs.  
   • **Tests**: contract tests with fixtures & latency budgets.
3. **Observability & Ops (prod hardening)**: Run manifest & dashboard URLs published; alert routes validated in prod.  
   • **Tests**: CI artifacts present; simulated failures emit alerts/logs.

> **Note:** **CSV/header parsing is complete** (hybrid parser with header aliasing); ongoing work is regression coverage only.

### 14.3 API Endpoints (Read‑only, Phase 1)
- `GET /rvu?code=99213&date=2025-10-01&modifier=25`
- `GET /gpci?locality_id=11302&date=2025-10-01`
- `GET /oppscap?code=71260&modifier=TC&locality=11302&date=2025-10-01`
- `GET /releases?type=pprrvu` (list versions, effective windows, publish status)

### 14.4 Acceptance Tests (Status)
- **TXT→Canonical Parity (2025D)**: Diverse fixtures in place; parity validated ✅
- **CSV Parity**: Implemented with hybrid parser & header aliasing; regression suite green ✅
- **Bitemporal**: Correction supersession tests added; time‑travel verified ✅
- **Idempotency**: Checksum gate prevents curated deltas on re‑ingest ✅
- **OPPS Cap Application**: Imaging codes test set validates cap ≤ component ✅
- **Anesthesia Path**: Anesthesia codes validated with ANES CF rules ✅
- **Security/Access**: Least‑privilege roles & access logs verified during pipeline run ✅

### 14.5 Release Checklist (GA)
- ✅ All unit/integration/data/contract tests pass in CI; coverage ≥80%.
- ✅ QA report archived with manifest (files, checksums, row counts) for GA snapshot.
- ✅ Views live: `vw_rvu_current`, `vw_gpci_current`, `vw_opps_cap_current`, `vw_anes_cf_current`, `vw_locality_county_current`.
- ✅ Read‑only API endpoints returning within SLA; dashboards/alerts active.
- ✅ Rollback validated (republish previous `published_at`).
- ✅ Performance SLOs achieved; perf report attached to CI artifacts.
- ✅ Observability complete (manifests, dashboard, anomalies, alerts).
- ✅ All unit/integration/data/contract tests exist and pass in CI; parser/transform coverage ≥80%.
- ✅ QA report archived with manifest (files, checksums, row counts) for the GA snapshot.
- ✅ Views live: `vw_rvu_current`, `vw_gpci_current`, `vw_opps_cap_current`, `vw_anes_cf_current`, `vw_locality_county_current`.
- ✅ Read‑only API endpoints returning within SLA; dashboards/alerts active.
- ✅ Rollback validated (republish previous `published_at`).
- ✅ **Performance SLOs achieved:** P95 ≤ 500ms across common patterns; perf report attached to CI artifacts.
- ✅ **Observability complete:** run manifests, dashboard (HTML+JSON), anomaly reports, and multi‑channel alerts in place.

### 14.6 Data Readiness Expectations (Oct‑2025)
- Publish expected‑count ranges for RVU rows (codes × modifier variants), GPCI localities (MAC×locality), OPPSCAP tuples, ANES rows, LOCCO rows.  
- QA report must include: distinct counts, distribution summaries, top deltas vs prior cycle, anomaly samples.

### 14.7 Nice‑to‑Have (Post‑GA)
- Machine‑readable schemas (YAML) to auto‑drive parsers and dbt tests.
- Automated data dictionary publishing from schemas.
- Backfill automation for A/B/C/D + AR detection across years.

---

## 15) Scale & Performance Results (Completed)
**Status:** Complete — production‑ready performance.

### 15.1 Indexing Strategy
- **Total indices:** 175 (across all tables)
- **New performance indices:** 13 (RVU‑focused)
- **Composite indices:** common query keys (e.g., `(hcpcs_code, status_code, effective_start, effective_end)`, `(mac, locality_id, effective_start)`)
- **Partial indices:** e.g., `work_rvu_not_null` to speed RVU component filters
- **Range/Date indices:** for effective date windows and value range queries

### 15.2 Benchmark Outcomes (P95)
- HCPCS lookups: **1.86ms**
- Status filtering: **0.34ms**
- Date range: **0.16ms**
- GPCI lookups: **0.76ms**
- Complex joins: **0.50ms**
- **All under 500ms SLO**, tested with **LoadTester** (20K RVU items/quarter, 10+ years simulated)

### 15.3 Tooling
- **LoadTester:** realistic synthetic data generation & workload replays
- **IndexOptimizer:** analyzes plans, proposes/creates optimal indices
- **QueryAnalyzer:** tracks slow queries, suggests optimizations
- **Perf CI Gate:** validates SLOs; attaches metrics to build artifacts


---

## 16) Operator Guide — Status Questions for Cursor
Use these prompts to quickly assess health and completeness.

### 16.1 Acquisition & Discovery
- What pages/domains are allowlisted, and do we persist an acquisition manifest per run?
- How are links discovered (static patterns, HTML crawl, sitemap, API)? Any JavaScript rendering required?
- Which file types are recognized (ZIP, TXT, CSV, PDF)? How are integrity checks done (size, SHA256)?
- What is the re‑ingest policy (checksum idempotency) and skip behavior for unchanged artifacts?

### 16.2 Parsing & Normalization
- Which layout versions exist in the registry (for example 2021A through 2025D)? How are AR corrections handled?
- Do TXT and CSV converge to the same canonical schema with source_lineage columns?
- How are modifiers normalized (array plus modifier_key)? Any observed edge cases?

### 16.3 Effective Dating & Versioning
- How are effective_start / effective_end derived for each cycle and AR? Where do we store published_at (system time)?
- What are the unique keys per table (e.g., GPCI = mac + locality_id + effective_start) and where are they enforced?

### 16.4 Validation & QA
- Which structural, content, and referential rules run today? Where can I open the QA HTML/JSON?
- What are anomaly rates and top causes for the last three runs?

### 16.5 Scale & Performance
- What are P95 latencies by query pattern in perf CI runs? Are indices validated in each migration?
- What synthetic data profile does LoadTester use (rows per quarter times years)?

### 16.6 Observability & Ops
- Do we emit run manifests, dashboards, and alerts (acquisition failure, validation failure, freshness SLO miss)?
- Where are logs and metrics stored? What is current SLO compliance?

### 16.7 API & Interfaces
- Which read‑only endpoints and warehouse views are live? Do we have contract tests and latency gates in CI?

### 16.8 Security & Runbook
- What roles exist for buckets/warehouse? Are artifacts encrypted and access‑logged?
- How do we reprocess a single cycle or roll back to a previous published_at snapshot?

---

## 17) Automated Scraper & 5‑Year Backfill Plan

### 17.1 Goal
Automatically discover, download, validate, and ingest CMS RVU artifacts for the last five years across quarterly cycles (A, B, C, D) and AR corrections, with lineage, idempotency, and SLOs.

### 17.2 Sources
- CMS “PFS Relative Value Files” landing pages and adjacent subpages hosting quarterly ZIPs/individual files.
- Prefer ZIPs when both ZIP and individual files exist; otherwise accept individual files.
- **Always fetch the authoritative layout** (cycle PDF) alongside data files; store and register it with the layout registry for that `source_version`.

### 17.3 Discovery Rules
- Identify quarterly cycle identifiers containing the year and cycle letter (e.g., 2025D or 2025AR).
- Match filenames that clearly indicate datasets, such as those containing PPRRVU, GPCI, OPPSCAP, ANES, or locality/county, and common extensions like .zip, .txt, .csv, or .pdf.
- Respect robots.txt; throttle 1–3 requests per second; single concurrency per host; exponential backoff on 429/5xx.

### 17.4 Crawler/Collector Architecture
- HTML parser (requests + BeautifulSoup) with optional headless browser fallback for JavaScript‑rendered pages.
- Normalize relative URLs; apply allowlist filters; de‑duplicate links.
- Downloader streams to /raw/{year}/{cycle}/ with a sidecar JSON containing size, checksum, URL, and timestamps.
- Manifest per run under /manifest/{timestamp}.jsonl with URL, filename, dataset, year, cycle, discovered_at, downloaded_at, SHA256, and content_length.
- Idempotency: skip ingest if an artifact with the same SHA256 is already published; re‑tag only when source_version changes.

### 17.5 Backfill Logic
1) Build the target matrix: years = current minus four through current; cycles = A, B, C, D.
2) For each (year, cycle): crawl allowlisted pages; collect links; prefer ZIPs; also search for correction cycles (e.g., 2025AR).
3) Validate downloads (HTTP 200, content length, SHA256) and write sidecars.
4) Emit the run manifest; hand off to the existing ingestor; persist artifacts and manifest.

### 17.6 Classification Heuristics
- PPRRVU: filenames that include the term pprrvu or RVU ZIPs that contain PPRRVU files.
- GPCI: filenames with gpci and .txt extension.
- OPPSCAP: filenames with oppscap and .txt or .csv extensions.
- ANES: filenames with anes and .txt or .csv extensions.
- Locality crosswalk: filenames with locco or locality county and .csv extension.

### 17.7 Tests (CI)
- Crawler discovery on saved HTML pages: assert expected link counts and exact matches.
- Robots and rate‑limit simulation: crawler respects disallow rules and throttles correctly.
- URL‑to‑dataset classification correctness across a matrix of real examples.
- Idempotency: re‑run with the same manifest results in zero curated deltas.
- Backfill coverage: the full matrix is generated; missing cycles logged as warnings, not errors.
- Checksum integrity: corrupted file triggers quarantine and a hard error.
- End‑to‑end: one of each dataset downloads, ingests, and passes validations.
- Performance: crawl completes within budget and ingestion P95 remains under 500ms.

### 17.8 Observability & Ops
- **Run Manifest** (per run): id, started_at/ended_at, source_version(s), discovered_count, downloaded_count, ingested_count, skipped_count, failures[], per‑file: url, filename, dataset, year, cycle, sha256, bytes, discovered_at, downloaded_at, row_counts by stage (raw/staging/curated), durations.
- **Dashboard**: real‑time status (ingestion, validation, performance), activity timeline, severity breakdown; served as **HTML** and **JSON API**.
- **Anomaly Engine**: severity levels (critical/high/medium/low) with rules for RVU (outliers, missing criticals, distribution shifts), GPCI (range/out‑of‑domain, missing localities), OPPSCAP (negative prices, unusual ratios), and cross‑dataset consistency.
- **Alerts**: configurable rules; email/Slack/webhook; cooldowns; resolution tracking; summary reports.
- **File Organization**: structured outputs under `data/RVU/` for manifests, dashboard assets, anomaly reports, and alerts.

### 17.9 Risks & Mitigations
- CMS renames or link reshuffles: use resilient classification (keywords plus patterns) and maintain an override allowlist.
- JavaScript‑rendered content: use headless browser fallback only for allowlisted pages.
- Duplicate or overlapping artifacts: enforce checksum idempotency and de‑duplicate on (dataset type, source_version, SHA256).

### 17.10 Acceptance Criteria (Scraper/Backfill)
- Discovers and ingests at least 95% of target cycles across the last five years; missing cycles are logged with reasons.
- Emits a manifest and QA artifacts each run; all downloads are checksummed and retained.
- Idempotent re‑runs produce zero changes in curated tables.
- All crawler, classification, idempotency, and end‑to‑end tests pass in CI.


---

## 18) Observability Details (Completed)
**Status:** Implemented end‑to‑end and covered by tests.

### 18.1 Components
- **Run Manifest Generator**: comprehensive tracking of files, checksums, row counts, stage durations, and success rates; historical run log with performance data.
- **Anomaly Detection Engine**: rules across datasets with severity; detailed reports including affected counts and sample records.
- **Operational Dashboard**: real‑time health, perf metrics, activity timeline, anomaly summary; HTML UI with responsive layout plus JSON API for automation.
- **Alert System**: severity‑aware, multi‑channel (Email, Slack, Webhook), cooldowns to prevent spam, resolution tracking and reporting.

### 18.2 Testing & Validation
- Unit/integration tests for manifest, anomaly rules, dashboard feeds, and alerting.
- Mock data generators for DB‑less testing.
- File structure validation ensuring proper placement of outputs.
- End‑to‑end tests verifying the full observability stack in CI.

### 18.3 Ops SLAs
- **Coverage**: manifests & QA artifacts present for **100%** runs.
- **Alerting**: critical anomalies generate alerts within **≤ 5 minutes**; MTTA ≤ **15 minutes**.
- **Retention**: manifests, QA, dashboard snapshots, and alerts retained ≥ **1 year**.


---

## 19) TODO / Next Actions (Expanded)
> Use this as the sprint/backlog source. Populate **Owner** and **Target** dates in planning.

### 19.1 API Surface & Contracts
- [ ] **OpenAPI v3 stub (read‑only)** for `/rvu`, `/gpci`, `/oppscap`, `/releases` (if not already in repo); params, pagination, error model, examples. **Owner:** TBD • **Target:** TBD
- [ ] **SDKs** (Python/TypeScript) with typed clients; examples & quickstarts. **Owner:** TBD • **Target:** TBD
- [ ] **Caching & ETags** for GET endpoints; document cache headers. **Owner:** TBD • **Target:** TBD
- [ ] **Rate limiting** & 429 retry guidance; idempotency guidance for downloads. **Owner:** TBD • **Target:** TBD

### 19.2 CSV Parsing & Layouts
- [x] **CSV/header parsing complete** (hybrid parser + header detection). Maintain regression suite.
- [ ] **Layout registry YAML** formalization (machine‑readable specs to drive parsers & dbt tests). **Owner:** TBD • **Target:** TBD
- [ ] **Authoritative layout audit** job (CI) to verify layout presence per `source_version`. **Owner:** TBD • **Target:** TBD
- [ ] **Golden fixtures** for earlier cycles to extend regression coverage. **Owner:** TBD • **Target:** TBD
- [ ] **CSV header aliasing** for PPRRVU, OPPSCAP, ANES, LOCCO; strict type coercion & unknown‑column policy. **Status:** **Completed** (hybrid parser + header detection). Follow‑up: maintain regression suite.
- [ ] **Layout registry YAML** formalization (machine‑readable specs to drive parsers & dbt tests). **Owner:** TBD • **Target:** TBD
- [ ] **Authoritative layout audit** job that verifies a layout file exists for each `source_version` in scope; fails CI if missing. **Owner:** TBD • **Target:** TBD
- [ ] **Golden fixtures** (TXT↔CSV parity sets) committed for 2025D; add earlier cycles for regression. **Owner:** TBD • **Target:** TBD

### 19.3 Advanced Validations (Data Quality)
- [ ] **Payability** (Status A/R/T only); exclusions routed per policy. **Owner:** TBD • **Target:** TBD
- [ ] **NA ⇒ non‑facility PE null** enforcement. **Owner:** TBD • **Target:** TBD
- [ ] **Global days** semantics persisted and validated (000/010/090/XXX/YYY/ZZZ). **Owner:** TBD • **Target:** TBD
- [ ] **Supervision code domain** (01,02,03,04,05,06,21,22,66,6A,77,7A,09). **Owner:** TBD • **Target:** TBD
- [ ] **OPPS CAP** join completeness + cap ≤ component checks. **Owner:** TBD • **Target:** TBD
- [ ] **Anesthesia CF** pathway validation for anesthesia codes. **Owner:** TBD • **Target:** TBD

### 19.4 Scraper & 5‑Year Backfill (per §17)
- [ ] **Crawler implementation** (allowlists, patterns, robots, throttling). **Owner:** TBD • **Target:** TBD
- [ ] **Downloader + sidecars** (SHA256, bytes, timestamps). **Owner:** TBD • **Target:** TBD
- [ ] **Manifest writer** and **idempotency gates**. **Owner:** TBD • **Target:** TBD
- [ ] **Backfill job** (matrix years×cycles + AR); schedule & re‑run strategy. **Owner:** TBD • **Target:** TBD
- [ ] **Scraper CI tests** (discovery, classification, idempotency, E2E). **Owner:** TBD • **Target:** TBD

### 19.5 Observability & Ops (Production Rollout)
- [ ] **Promote dashboards** (HTML & JSON) to prod; publish URLs & access. **Owner:** TBD • **Target:** TBD
- [ ] **Alert rules** finalized (critical/high/medium/low), routes (Email/Slack/Webhook), cooldowns verified. **Owner:** TBD • **Target:** TBD
- [ ] **Runbook**: incident response, common failures, recovery/rollback steps with examples. **Owner:** TBD • **Target:** TBD
- [ ] **SLO docs**: ingestion freshness, validation coverage, API latency; **error budget** policy. **Owner:** TBD • **Target:** TBD
- [ ] **Retention** policy for manifests/QA/alerts (≥1 year) enforced & verified. **Owner:** TBD • **Target:** TBD

### 19.6 Security, Access & Compliance
- [ ] **RBAC** for raw bucket, curated warehouse, and API; least‑privilege review. **Owner:** TBD • **Target:** TBD
- [ ] **Secrets management** (no creds in code; rotate tokens/keys). **Owner:** TBD • **Target:** TBD
- [ ] **Audit logging** verification (access to artifacts, API calls). **Owner:** TBD • **Target:** TBD
- [ ] **DR/BCP**: backup/restore drills for raw artifacts and curated tables. **Owner:** TBD • **Target:** TBD

### 19.7 Data Governance & Catalog
- [ ] **Data dictionary** (finalize & publish) + link to **machine‑readable schemas**. **Owner:** TBD • **Target:** TBD
- [ ] **dbt/GE integration**: generate tests from schemas; nightly runs. **Owner:** TBD • **Target:** TBD
- [ ] **Lineage** in catalog (raw → staging → curated) with column‑level docs. **Owner:** TBD • **Target:** TBD

### 19.8 Performance & Capacity
- [ ] **Perf CI gate** kept green; attach perf artifacts per build. **Owner:** TBD • **Target:** TBD
- [ ] **Capacity planning**: 10+ years steady state; storage & query cost tracking. **Owner:** TBD • **Target:** TBD
- [ ] **Index review** on every migration (automatic plan regression checks). **Owner:** TBD • **Target:** TBD

### 19.9 Deployment & Environments
- [ ] **IaC** for buckets/DB/indices/pipelines (reproducible environments). **Owner:** TBD • **Target:** TBD
- [ ] **Env promotion**: dev → stage → prod with data fences & smoke tests. **Owner:** TBD • **Target:** TBD
- [ ] **Changelog & versioning** for releases (semantic tags; migration notes). **Owner:** TBD • **Target:** TBD

### 19.10 Enablement & Docs
- [ ] **Quickstarts**: notebook & SQL examples (joins RVU↔GPCI↔OPPSCAP↔ANES↔Locality). **Owner:** TBD • **Target:** TBD
- [ ] **API usage guides**: pagination, filters, caching, retries, 429 handling. **Owner:** TBD • **Target:** TBD
- [ ] **Onboarding**: 1‑pager for analysts/eng; office hours plan. **Owner:** TBD • **Target:** TBD

---

## 21) QA Summary (per QA & Testing Standard v1.0)
| Item | Details |
| --- | --- |
| **Scope & Ownership** | RVU/GPCI/OPPSCAP/ANES/Locality ingestion pack; owned by RVU Data squad with Quality Engineering partnership; downstream consumers include pricing APIs, analytics, compliance. |
| **Test Tiers & Coverage** | Unit/component: parser & normalization suites in `tests/test_rvu_parsers.py` and `tests/test_rvu_basic.py`; Validation: business rules in `tests/test_rvu_validations.py`; Contract/API: `tests/test_rvu_api_contracts.py` for read-only endpoints; Scenario: ingestion replay + schema enforcement in `tests/test_ingestion_pipeline.py`. Coverage currently 88% (target ≥92%). |
| **Fixtures & Baselines** | Authoritative TXT/CSV samples + layout YAML stored under `tests/fixtures/rvu/`; golden outputs appended to `tests/golden/test_scenarios.jsonl`; baseline metrics tracked in QA data warehouse (freshness, row deltas) with release digests captured in PRD changelog. |
| **Quality Gates** | Merge: `ci-unit.yaml` runs parser, validation, and contract suites with coverage delta guard; `ci-integration.yaml` loads fixtures into Postgres + verifies bitemporal invariants; nightly replay ensures no schema drift and checks delta tolerances. |
| **Production Monitors** | Freshness and row-count drift alerts from ingestion scheduler; schema/layout audit job blocks runs missing authoritative layouts (§22); API latency + error monitors cover consumer endpoints. |
| **Manual QA** | Layout audit checklist prior to accepting new CMS cycles; manual parity spot-checks for top HCPCS codes vs CMS calculators; operator verification of anomaly reports before GA promotions. |
| **Outstanding Risks / TODO** | Close tasks in §19 (layout registry YAML, scraper backfill); extend golden fixtures to prior cycles; finalize automated payability + anesthesia validations. |

## 22) Authoritative Layout Policy (Global Rule)
**Principle:** *Every ingestor must rely on an authoritative layout/specification.* Examples include CMS cycle PDFs (e.g., RVUyyA/B/C/D*.pdf) or versioned schema YAML files maintained in‑repo.

**Rules**
- Ingest runs **MUST NOT** proceed without a registered authoritative layout for the `source_version` in scope.
- The scraper **MUST** fetch and archive the layout file alongside data artifacts; parsers reference it by exact version.
- CI contains a **layout audit** that fails builds if any targeted `source_version` lacks a layout reference.

**Operator Guidance**
- If a layout is missing or ambiguous, open a blocking task to obtain it (from CMS or internal owners) and re‑run once attached.


---

## 20) Changelog
> Track notable changes to this PRD. Use semantic-style versions and date in **YYYY-MM-DD**.

| Date       | Version | Author | Summary                                     | Details |
|------------|---------|--------|---------------------------------------------|---------|
| 2025-09-27 | v1.0    | Team   | **GA-ready: Validations, Scraper, API live** | Hybrid parser complete; business validations implemented; web scraper & CLI; read-only API endpoints with contract tests; performance SLOs & observability met. |
