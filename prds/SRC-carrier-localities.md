# CMS ZIP Code → Carrier Locality Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** Geography Services, Nearest ZIP Resolver, Pricing API, MPFS Ops, QA  
**Change control:** PR review  
**Review cadence:** Annual (aligned with CMS ZIP locality refresh; ad-hoc if CMS posts corrections)

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Dataset inventory and lineage
- **REF-geography-source-map-prd-v1.0.md:** End-to-end geography data flow
- **planning/parsers/locality/AUTHORITY_MATRIX.md:** Authority hierarchy for geographic sources
- **STD-parser-contracts-prd-v2.0.md:** Parser/ingester contract requirements
- **RUN-parser-qa-runbook-prd-v1.0.md:** QA workflow for geography datasets
- **SRC-locality.md:** MAC/locality → county explosion (Stage 2 partner)

**Last Updated:** 2025-10-23  
**Verified Against CMS Release:** ZIP Code Carrier Locality File (revised 08/14/2025)

---

## 1. Overview

**Official CMS Name:** ZIP Code to Carrier Locality File  
**Dataset Code:** ZIP_LOCALITY  
**Source URL:** [ZIP Code Carrier Locality File](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)  
**Release Cadence:** Annual (mid-August) with occasional mid-year corrections  
**Business Purpose:** Maps 5-digit ZIP codes to CMS MAC/locality codes—the backbone for translating provider ZIPs into MPFS pricing geographies.

**Typical Characteristics:**
- **File Size:** ~2–5 MB compressed (ZIP).  
- **Row Count:** ~42,000 active ZIPs plus territories.  
- **Effective Dates:** Annual effective period (Jan 1 – Dec 31) plus optional correction vintages.  
- **Downstream Use:** 
  - Nearest ZIP resolver (fallback when ZIP/ZCTA mismatches occur)  
  - Pricing and reimbursement APIs  
  - Compliance reporting (ensuring payments use correct locality)

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Ingestion Support | Notes |
|--------|-----------|--------------|-------------------|-------|
| **ZIP** | `.zip` | ✅ Always | ✅ Implemented | Container with one CSV (sometimes additional README) |
| **CSV** | `.csv` | ✅ Always (inside ZIP) | ✅ Implemented | Canonical structure; column names vary slightly per year |
| **XLSX** | `.xlsx` | ⚠️ Occasional | ⚠️ Planned | CMS occasionally posts Excel companion; not yet productionized |
| **TXT** | `.txt` | ❌ Not used | ❌ N/A | CMS does not publish fixed-width for this dataset |

### 2.2 CSV Details (Authority Format)

- **Filename Pattern:** `zip_locality.csv`, `ZIP_CODE_LOCALITY_YYYY.csv`, or similar.  
- **Headers (2025):** `ZIP_CODE,CARRIER,LOCALITY,STATE,RURAL_IND` (`RURAL_IND` optional).  
- **Delimiter:** Comma.  
- **Encoding:** UTF-8 with occasional BOM.  
- **Quirks:** 
  - CMS sometimes publishes duplicate ZIP rows differing only by effective date; ingestion picks latest by `effective_from`.  
  - RURAL indicator encoded as `R`/`Y` or numeric `1`; normalized to boolean.  
  - Leading zeros on ZIP preserved—ingester forces string type.

### 2.3 LAND/VALIDATE Pipeline

The production ingester (`CMSZipLocalityProductionIngester`) executes a DIS-compliant pipeline:
1. **Land:** Download ZIP, compute SHA-256, persist raw artifact + manifest.  
2. **Validate:** Invoke `CMSZipLocalityValidator` for structure, domain, and business-rule checks.  
3. **Normalize:** Canonicalize column names/types, compute effective dates, standardize `rural_flag`.  
4. **Enrich:** Attach ingest metadata (`release_id`, `vintage`, provenance).  
5. **Publish:** Load into `cms_zip_locality` table with `row_content_hash` for idempotent merges.

---

## 3. Schema & Natural Keys

### 3.1 Natural Keys

```python
BUSINESS_KEYS = ['zip5', 'effective_from']  # per contract; enforced uniqueness per vintage
```

- `zip5` is the core identifier.  
- `effective_from` captures annual refresh/value changes.  
- Warehouse also tracks `vintage` (e.g., `2025`) to allow multi-year auditing without collisions.

### 3.2 Schema Contract

**Location:** `cms_pricing/ingestion/contracts/cms_zip_locality_v1.json`  
**Primary Key:** `zip5` (physical table)  
**Business Key:** (`zip5`, `effective_from`)  
**Hash Order:** `['zip5','state','locality','carrier_mac','rural_flag','effective_from','ingest_run_id']`

| Column | Type | Nullable | Validation | Notes |
|--------|------|----------|------------|-------|
| `zip5` | String(5) | N | `^\d{5}$` | Leading zeros preserved (New England). |
| `state` | String(2) | N | Valid USPS state/territory codes | Includes territories (PR, GU, VI, AS, MP). |
| `locality` | String(2-10) | N | `^\d{2,10}$` | CMS locality identifiers (typically 2 digits today). |
| `carrier_mac` | String(2-10) | Y | `^\d{2,10}$` | Medicare Administrative Contractor identifier (zero-padded when present). |
| `rural_flag` | Boolean | Y | Derived from CMS indicator | True for “rural”/”rest of state” special rules. |
| `effective_from` | Date | N | `YYYY-MM-DD` | Typically January 1 of the release year. |
| `effective_to` | Date | Y | Null unless CMS issues correction; enforced `effective_to >= effective_from`. |
| `vintage` | String | N | e.g., `2025` | Stored separately from effective date for audit/partitioning. |
| `source_filename` | String | N | Provided by ingester | E.g., `zip_locality.csv`. |
| `ingest_run_id` | UUID | N | DIS run tracking | Links to ingestion run metadata. |
| `row_content_hash` | String(64) | N | SHA-256 | Supports idempotent upsert. |

Additional metadata: `data_quality_score`, `validation_results` (JSON), `processing_timestamp`, `schema_version`, `business_rules_applied`.

---

## 4. Business Rules & Validations

### 4.1 Structural & Domain Rules (Validator)

- Required fields: `zip5`, `state`, `locality`, `carrier_mac`, `effective_from`, `vintage`.  
- ZIP must be 5 digits; state must be valid USPS code.  
- Locality must be numeric string (2–10 chars).  
- `effective_to` must be null or ≥ `effective_from`.  
- Uniqueness: one record per (`zip5`, `vintage`). Violations are CRITICAL (ingest halts).  
- Future-dated `effective_from` allowed with WARN (for announced future years).

### 4.2 Data Quality Thresholds

From schema contract:
- **Completeness:** 100% for core fields (`zip5`, `state`, `locality`, `effective_from`, `vintage`).  
- **Accuracy:** 100% for format/domain rules (ZIP format, state codes, locality numeric).  
- **Uniqueness:** 100% per vintage.  
- Validator aggregates quality score; ingestion fails if thresholds not met.

### 4.3 Derived/Enriched Fields

- `rural_flag`: Normalized to boolean based on CMS indicator values (`R`, `Y`, `Yes`, `1`, etc.).  
- `carrier_mac`: Direct rename of CMS “CARRIER” column; zero-padded string.  
- `vintage`: Derived from release naming (e.g., 2025) and stored for partitioning.  
- `effective_to`: Set when successive release ingested (post-processing step).

---

## 5. Known Data Quality Issues

1. **Dual State Assignments (Border ZIPs):** Some ZIPs appear in multiple states across vintages (e.g., military ZIPs). Validator enforces single state per vintage; cross-vintage differences logged for manual review.  
2. **Rural Indicator Drift:** CMS occasionally toggles the rural indicator without locality change. Downstream logic should treat `rural_flag` as advisory; payment logic leans on locality code.  
3. **Carrier Reassignments:** When CMS reassigns carriers, same ZIP/locality combination may persist with new `carrier_mac`. NK handles this because `carrier_mac` is persisted but not part of business key—only state/locality matter for payment.  
4. **Missing Localities for ZIP9:** Rare ZIP codes present in ZIP9 overrides but absent in carrier locality file; handled via `cms_zip9_overrides`.

---

## 6. Testing & QA

- **Unit Fixtures:** `tests/ingestors/test_ingestion_pipeline.py` mocks zipped CSV ingestion and validates DB persistence.  
- **Integration:** `test_cms_ingester_end_to_end.py` exercises production ingester against sample data.  
- **Nearest ZIP Tests:** `tests/geography/test_nearest_zip_*` rely on `CMSZipLocality` records for join coverage.  
- **Validator Coverage:** `cms_pricing/ingestion/validators/cms_zip_locality_validator.py` includes rule-specific unit tests (completeness, uniqueness, state/ZIP formats).

---

## 7. Operational Guidance

- **Release Naming:** Use `cms_zip_locality_YYYYMMDD_HHMMSS` for release IDs; `vintage` is derived from CMS announcement year (e.g., 2025).  
- **Backfill:** Re-run ingester with prior ZIP bundle to rehydrate history; `effective_from` ensures temporal separation.  
- **Propagation:** After ingestion, run nearest-zip recomputation to refresh caches, then audit logs for state-boundary mismatches.  
- **Monitoring:** `nearest_zip_monitoring` job reports `cms_zip_locality` record counts and last ingest timestamp; alert if drift >1.  
- **Data Retention:** Keep raw ZIP archives and manifests for 7 years per compliance.

---

## 8. Dependencies & Downstream Usage

- **Nearest ZIP Resolver:** Primary lookup when mapping provider ZIP to locality (with ZCTA fallback).  
- **ZIP9 Overrides (`cms_zip9_overrides_v1.json`):** Complements dataset with finer-grained overrides.  
- **Locality-County Mapping (SRC-locality.md):** Used downstream to attach county/state FIPS once locality determined.  
- **Pricing APIs:** Response payloads include locality when exposing ZIP-based pricing calculators.  
- **Ops/Compliance:** Audit dashboards rely on `carrier_mac` and `rural_flag` for oversight reporting.

---
