# CMS OPPS (Hospital Outpatient Prospective Payment System) Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** OPPS Ingester, Imaging/Outpatient Pricing, Compliance, Analytics, QA  
**Change control:** PR review  
**Review cadence:** Quarterly (aligned with CMS OPPS addenda releases)

**Cross-References:**
- **PRD-opps-prd-v1.0.md:** Product requirements for OPPS ingestion & pricing services  
- **STD-parser-contracts-prd-v2.0.md:** Parser contract guidelines  
- **REF-scraper-ingestor-integration-v1.0.md:** Scraper + ingester integration playbook  
- **STD-qa-testing-prd-v1.0.md:** QA standards and harness expectations  
- **SRC-oppscap.md:** Related dataset (OPPS-based caps for MPFS imaging services)

**Last Updated:** 2025-10-23  
**Verified Against CMS Release:** July 2025 OPPS Quarterly Addenda

---

## 1. Overview

**Official CMS Name:** Hospital Outpatient Prospective Payment System (OPPS) Quarterly Addenda  
**Dataset Code:** OPPS  
**Source URL:** [CMS OPPS Quarterly Addenda Updates](https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates)  
**Release Cadence:** Quarterly (Q1/Jan, Q2/Apr, Q3/Jul, Q4/Oct) with periodic correction bulletins (April/July/Oct addenda often include mid-cycle revisions).  
**Business Purpose:** Supplies APC payment rates, wage-adjusted amounts, and HCPCS → APC crosswalks that govern OPPS reimbursement for hospital outpatient services.

**Key Outputs:**
- **Addendum A:** APC payment rates (APC-level).  
- **Addendum B:** HCPCS-to-APC crosswalk with status indicators, wage index flags, device edits, etc.  
- **Status Indicator Lookup:** Supplemental crosswalk (often separate table).  
- **Packaging & Wage Index Tables:** Additional addenda (C, D1, D2) available but currently out of scope.

---

## 2. Acquisition & Formats

### 2.1 Discovery

`CMSOPPSScraper` performs discovery:
1. Scrape quarterly addenda page → identify release links with quarter/year metadata.  
2. Follow release page to gather file URLs (CSV/XLS/XLSX/ZIP).  
3. Persist manifest (`data/scraped/opps/manifests/cms_opps_manifest_*.jsonl`) with checksums, discovered_at, source metadata.

### 2.2 Supported File Types

| Artifact | Extension | Availability | Notes |
|----------|-----------|--------------|-------|
| **Addendum A** | `.csv`, `.xlsx`, `.zip` | ✅ Always | APC payment rates. CSV is Section 508 compliant. ZIP may bundle both. |
| **Addendum B** | `.csv`, `.xlsx`, `.zip` | ✅ Always | HCPCS-level detail with status indicators. |
| **Status Indicator Lookup** | `.csv`, `.xlsx` | ✅ Common | Maps status indicator codes to descriptions. Schema contract `cms_opps_si_lookup_v1.0.json`. |
| **Addenda C/D/E** | `.zip` (varied) | ⚠️ Optional | Packaging, wage index, comprehensive APC references (future scope). |

**Format Quirks:**
- CSVs use comma delimiter, double quotes for text; some historical files use pipe (`|`). Parser auto-detects via sniffing.  
- XLSX workbooks use first sheet; column headers vary between uppercase/lowercase, spaces/underscores.  
- ZIP bundles frequently include both CSV and XLSX plus supporting docs (PDF). Ingester prioritizes CSV, falls back to XLSX if absent.  
- Numeric fields may include currency symbols (`$`), commas, or `*` footnotes—parser strips/normalizes.

---

## 3. Schema & Natural Keys

### 3.1 Contracts

- **Primary Contract:** `cms_pricing/ingestion/contracts/cms_opps_v1.0.json` (v1.1 content).  
  - Table `opps_apc_payment` (Addendum A).  
  - Table `opps_hcpcs_crosswalk` (Addendum B).  
- **Status Indicator Crosswalk:** `cms_pricing/ingestion/contracts/cms_opps_si_lookup_v1.0.json`.

### 3.2 Natural Keys

| Table | NK (per contract/code) | Rationale |
|-------|------------------------|-----------|
| `opps_apc_payment` | (`apc_code`, `quarter_vintage`, `effective_from`) | APC rates change quarterly; `quarter_vintage` distinguishes successive releases within same calendar year. |
| `opps_hcpcs_crosswalk` | (`hcpcs_code`, `quarter_vintage`, `effective_from`) | HCPCS-to-APC mappings vary by quarter; duplicates across vintages allowed. |

> **Note:** Contract currently lists `"apc"`/`"hcpcs"` for NK fields; ingestion normalizes to `apc_code`/`hcpcs_code`. Contract update queued to align naming (tracked in OPS-1421).

### 3.3 Core Columns

**Addendum A (`opps_apc_payment`):**
| Column | Type | Nullable | Validation | Notes |
|--------|------|----------|------------|-------|
| `year` | Integer | N | 2020–2030 | Calendar year of pricing. |
| `quarter` | Integer | N | {1,2,3,4} | OPPS quarter. |
| `quarter_vintage` | String | N | e.g., `2025Q3` | Derived from year/quarter. |
| `apc_code` | String(4) | N | `^\d{4}$` | Ambulatory Payment Classification. |
| `apc_description` | String | Y | ≤500 chars | CMS description. |
| `payment_rate_usd` | Decimal(10,2) | N | ≥0 | Unadjusted national rate. |
| `relative_weight` | Decimal(8,4) | N | ≥0 | OPPS relative weight. |
| `packaging_flag` | Enum | Y | {Y,N,null} | Packaging indicator. |
| `effective_from` | Date | N | Quarter start | Derived from quarter. |
| `effective_to` | Date | Y | Quarter end or null | Set when superseded. |

**Addendum B (`opps_hcpcs_crosswalk`):**
| Column | Type | Nullable | Validation | Notes |
|--------|------|----------|------------|-------|
| `year`, `quarter`, `quarter_vintage` | Same as Addendum A | | | |
| `hcpcs_code` | String(5) | N | `^[A-Z0-9]{5}$` | Includes CPT, HCPCS Level II. |
| `hcpcs_description` | String | Y | ≤500 chars | Service description. |
| `apc_code` | String(4) | N | `^\d{4}$` | Linked APC. |
| `status_indicator` | Enum | N | e.g., `A`, `T`, `Q1` | Combined with SI lookup for semantics. |
| `wage_adjusted_rate_usd` | Decimal(10,2) | Y | ≥0 | Provided when wage index applies. |
| `minimum_unadjusted_copay` | Decimal(10,2) | Y | ≥0 | CMS min copayment. |
| `effective_from` / `effective_to` | Date | N / Y | | |
| `release_id` / `batch_id` | String | N | Pattern `^opps_\d{4}q[1-4]_r\d+$` | Traceability. |

**Status Indicator Lookup:**
| Column | Type | Notes |
|--------|------|-------|
| `status_indicator` | String | Primary key. |
| `long_description` | String | Human-readable text. |
| `payment_flag` | Enum | Payment logic classification. |

---

## 4. Business Rules & Validations

### 4.1 APC Payment Validations
- **Payment Rate:** Must be ≥0; WARN > 100,000 USD (rare but possible for pass-through).  
- **Relative Weight:** Must be ≥0; WARN > 100 (signals potential parsing error).  
- **Packaging Flag Consistency:** If packaging flag = `N`, payment rate must be >0.  
- **Temporal Rules:** `effective_from` must align with quarter start; `effective_to` set to day before next quarter start.

### 4.2 HCPCS Crosswalk Validations
- **Status Indicator:** Validated against status indicator lookup; unknown codes = BLOCK.  
- **APC Reference Integrity:** Every `apc_code` must exist in Addendum A for same quarter.  
- **Device/Composite Flags:** Columns like device edit, composite APC appear in certain releases. Parser maps to boolean/enum fields; unexpected values escalate to WARN.  
- **Modifiers:** Addendum B occasionally includes modifier-specific rows; ingestion keeps modifier column when present (nullable).

### 4.3 Data Quality Thresholds (per contract)
- Completeness ≥ 99.9% for `hcpcs_code`, `apc_code`, `status_indicator`, `payment_rate_usd`.  
- Accuracy (format/domain) ≥ 99.9%.  
- Cross-table referential integrity enforced (HCPCS → APC).  
- Duplicate NK entries result in BLOCK; ingestion fails fast.

---

## 5. Known Data Quality Issues

1. **Quarterly Corrections:** CMS may release mid-quarter corrections (e.g., January Correction Addendum). Scraper captures latest file; ingestion stores `release_id`/`batch_id` for change tracking.  
2. **Currency Formatting:** Some CSVs include leading `$`/commas or trailing footnotes (e.g., `*`). Parser strips non-numeric characters before casting.  
3. **Status Indicator Typos:** Historical data has `Q0` vs `QO` mix-ups. Validator flags unknown indicators for manual review.  
4. **APC Description Line Breaks:** XLSX exports embed line breaks in descriptions; parser normalizes to single-line strings.  
5. **Addendum B Column Drift:** Column order occasionally shifts (especially introduction/removal of device edit columns). Parser relies on header aliases to maintain mapping.

---

## 6. Testing & QA

- **Scraper Tests:** `STD-qa-testing-prd-v1.0` includes scripts verifying discovery patterns and manifest diffs.  
- **Parser Tests:** Planned golden fixtures for Addendum A/B (CSV & XLSX) with deterministic hashing (pending implementation PR).  
- **Integration:** `prds/REF-scraper-ingestor-integration-v1.0.md` outlines end-to-end validation once parser ships.  
- **Observability:** Manifests capture checksum + file metadata; ingestion pipeline logs record counts, NK duplicates, referential-integrity check outcomes.

---

## 7. Operational Guidance

- **Release IDs:** Format `opps_{year}q{quarter}_r{n}` (e.g., `opps_2025q3_r1`). Increment `r` when corrections processed.  
- **Staging:** Raw files stored under `data/scraped/opps/{year}Q{quarter}/`. Processed tables land in curated warehouse prefix `cms_opps`.  
- **Backfill:** To reload history, re-run scraper with `max_quarters` spanning desired period, then ingest sequentially; referential checks will compare each quarter.  
- **Change Detection:** Use manifest diff to identify new/updated files. Downstream jobs should trigger only when new release detected.  
- **Retention:** Maintain raw releases + manifests for audit (7-year retention).

---

## 8. Dependencies & Downstream Usage

- **OPPSCAP (SRC-oppscap.md):** Imaging cap calculations compare MPFS payment vs OPPS Addendum B rates.  
- **Pricing APIs:** Outpatient pricing endpoints rely on Addendum B crosswalk and Addendum A payment rates.  
- **Compliance/Analytics:** Monitor wage index adjustments, packaging changes, payment trends using Addendum A data.  
- **MPFS Integration:** Status indicators inform physician fee schedule cap logic via OPPS CAP dataset.  
- **Reporting:** Contract requirement ensures `release_id`/`batch_id` flow into audit dashboards.

---
