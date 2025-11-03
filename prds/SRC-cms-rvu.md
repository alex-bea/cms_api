# CMS PPRRVU (MPFS RVU) Data Source

**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Consumers:** MPFS Ingester, RVU Services, Pricing API, Analytics, QA  
**Change control:** PR review  
**Review cadence:** Quarterly (aligned with CMS RVU releases)

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master data catalog & dependency map
- **PRD-mpfs-prd-v1.0.md:** MPFS pricing requirements (RVUs × GPCI × CF)
- **STD-parser-contracts-prd-v2.0.md:** Parser contract requirements
- **REF-parser-quality-guardrails-v1.0.md:** Validation tiers
- **RUN-parser-qa-runbook-prd-v1.0.md:** QA harness for source validation
- **SRC-gpci.md / SRC-conversion-factor.md:** Complementary datasets for MPFS pricing

**Last Updated:** 2025-10-23  
**Verified Against CMS Release:** RVU25D (2025 Q4, file `PPRRVU25D`)

---

## 1. Overview

**Official CMS Name:** Physician/Practitioner Relative Value Units (PPRRVU)  
**Dataset Code:** PPRRVU  
**Source URL:** [CMS PFS Relative Value Files](https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files)  
**Release Cadence:** Quarterly (A/B/C/D) with corrective administrative revisions (AR) as needed  
**Business Purpose:** Supplies per-HCPCS work, practice-expense, and malpractice RVUs that drive MPFS payment calculations. Combined with GPCI and conversion factors to calculate allowed amounts.

**Typical Characteristics:**
- **File Size:** 5–20 MB depending on format
- **Row Count:** ~16,000 HCPCS lines per quarter (varies with code updates)
- **Modifier Coverage:** Empty modifier for base lines; `26`, `TC`, `50`, etc. appear when CMS publishes modifier-specific RVUs
- **Effective Dates:** Derived from CMS quarter (Jan/Apr/Jul/Oct 1). CMS files do not embed dates; ingestion injects them from metadata.

**Payment Formula Context:**
```
MPFS Payment = [
    (Work RVU × Work GPCI) +
    (PE RVU × PE GPCI) +
    (MP RVU × MP GPCI)
] × Conversion Factor
```

---

## 2. File Format Variations

### 2.1 Supported Formats

| Format | Extension | Availability | Parser Support | Notes |
|--------|-----------|--------------|----------------|-------|
| **TXT** | `.txt` | ✅ Primary | ✅ Implemented | Fixed-width; authoritative layout in `layout_registry.PPRRVU_2025D_LAYOUT` |
| **CSV** | `.csv` | ✅ Common | ✅ Implemented | Header variations (API exports vs CMS spreadsheets) |
| **XLSX** | `.xlsx` | ✅ Common | ✅ Implemented | Single sheet; often includes multiple quarters |
| **ZIP** | `.zip` | ✅ Frequent | ✅ Implemented | Containers for TXT/CSV/XLSX; parser auto-detects inner format |

### 2.2 Format-Specific Details

**TXT (Fixed-width authority):**
- **Layout Version:** `PPRRVU_2025D_LAYOUT v2025.4.1`
- **Min Line Length:** 165 (actual data lines ~173 chars)
- **Header Rows:** 0 (data begins immediately)
- **Key Columns:** `hcpcs`, `modifier`, `description`, `status_code`, `rvu_*` values, flags (assistant surgery, bilateral, etc.)
- **Parsing:** Layout registry handles column slices; `effective_from` injected from metadata.

**CSV:**
- **Header Drift:** CMS posts multiple variants (`HCPCS`, `HCPCS CODE`, `CPT/HCPCS`, etc.); parser maintains alias map.
- **Delimiter:** `,`
- **Skip Rows:** 0–2 depending on whether CMS includes title lines.
- **Quoting:** `QUOTE_MINIMAL`; numeric columns may include thousands separators—parser canonicalizes with `canonicalize_numeric_col`.

**XLSX:**
- **Sheets:** Typically first sheet; parser selects by detecting canonical headers.
- **Data Types:** Excel coerces RVUs to float; parser re-strings before canonicalizing to protect precision.
- **Multi-quarter:** Some XLSX bundles prior quarter snapshots—`effective_from` injection plus NK guard ensures duplicates are pruned.

**ZIP:**
- **Inner Selection:** Chooses member containing “pprrvu” (case-insensitive); falls back to first data file otherwise.
- **Encoding:** Outer archive keeps CMS defaults; parser reuses ZIP member bytes and dispatches to TXT/CSV/XLSX logic.

---

## 3. Schema & Natural Keys

### 3.1 Natural Keys

```python
NATURAL_KEYS = ['hcpcs', 'modifier', 'status_code', 'effective_from']
```

- `status_code` participates in the NK to differentiate lines where CMS splits RVUs by status (e.g., carrier-priced vs bundled).  
- For legacy schema (`cms_pprrvu_v1.1` JSON), `status_code` is being added to the NK in the next contract bump. The parser already enforces the 4-part NK to match live warehouse expectations.  
- Duplicate detection is BLOCK severity—true NK collisions halt ingestion; fixtures cover regression cases.

### 3.2 Schema Contract

**Location:** `cms_pricing/ingestion/contracts/cms_pprrvu_v1.0.json` (specifies v1.1 content)  
**Hash Order:** `['hcpcs','modifier','status_code','rvu_work','rvu_pe_nonfac','rvu_pe_fac','rvu_malp','global_days','na_indicator','opps_cap_applicable','effective_from']`

| Column | Type | Nullable | Validation | Notes |
|--------|------|----------|------------|-------|
| `hcpcs` | String(5) | N | `^[A-Z0-9]{5}$` | Upper-case HCPCS/CPT code |
| `modifier` | String(2) | Y | `^[A-Z0-9]{2}$` or null | Empty modifiers normalized to `None` |
| `status_code` | Enum | N | `{A,R,T,I,N,...}` | CMS status indicators (Addendum B) |
| `rvu_work` | Decimal(6,2) | Y | 0 – 100 | Precision enforced via canonicalization |
| `rvu_pe_nonfac` | Decimal(6,2) | Y | 0 – 100 | Non-facility PE |
| `rvu_pe_fac` | Decimal(6,2) | Y | 0 – 100 | Facility PE |
| `rvu_malp` | Decimal(6,2) | Y | 0 – 100 | Malpractice RVU |
| `global_days` | Enum | Y | `{000,010,090,XXX,YYY,ZZZ}` | CMS global period mapping |
| `na_indicator` | String(1) | Y | `'Y'/'N'/None` | Not-applicable flag used by CMS |
| `opps_cap_applicable` | String(1) | Y | `'Y'/'N'/None` | Mirrors OPPSCAP eligibility |
| `effective_from` | Date | N | `YYYY-MM-DD` | Injected from metadata (see §4.2) |

Additional columns preserved from source: `multiple_proc_ind`, `assistant_surg_ind`, `physician_supervision`, `total_nonfac`, etc. (nullable, excluded from hash but loaded for downstream analytics).

### 3.3 Loader Alignment

The publish loader (`_load_pprrvu_data`) retains legacy warehouse column names. Before insert we now copy
parser outputs into the expected fields and normalise modifier handling:

| Parser column | Loader column  | Notes |
|---------------|----------------|-------|
| `hcpcs`       | `hcpcs_code`   | Zero-padded uppercase; BLOCK if missing |
| `modifier`    | `modifier_key` | Also used to derive `modifiers` list (`None` when blank) |
| `status_code` | `status_code`  | Passed through unchanged |
| `work_rvu`    | `rvu_work`     | Numeric canonicalisation |
| `pe_rvu_nonfac` | `pe_rvu_nonfac` | Same |
| `pe_rvu_fac`  | `pe_rvu_fac`   | Same |
| `mp_rvu`      | `rvu_malp`     | Same |

**Safety check:** loader now converts pandas `NA` modifiers to `None` so NK evaluation is deterministic (`boolean value of NA` errors are gone).

---

## 4. Business Rules & Validations

### 4.1 RVU Ranges

| Column | Fail Low | Fail High | Warn High | Typical | Notes |
|--------|----------|-----------|-----------|---------|-------|
| `rvu_work` | < 0 | > 100 | > 50 | 0.00–15.00 | Negative values are invalid; most RVUs < 10 |
| `rvu_pe_nonfac` | < 0 | > 150 | > 60 | 0.00–20.00 | Large only for complex imaging |
| `rvu_pe_fac` | < 0 | > 150 | > 60 | 0.00–20.00 | Facility values trend lower |
| `rvu_malp` | < 0 | > 50 | > 20 | 0.00–5.00 | Rare to exceed 10 |

Validation tiers follow `STD-parser-contracts-impl-v2.0`:
- **BLOCK:** Negative RVUs or absurd spikes beyond Fail High (likely parsing error).
- **WARN:** RVUs exceeding Warn High (logged, quarantined only if business rule requests).
- **INFO:** Missing RVUs permitted for certain `status_code` (`N`, `B`, etc.)—handled via categorical validation.

### 4.2 Effective Dates

- CMS files omit explicit effective dates; ingestion maps quarter vintages:  
  - `A` → `YYYY-01-01`  
  - `B` → `YYYY-04-01`  
  - `C` → `YYYY-07-01`  
  - `D` → `YYYY-10-01`  
- Administrative revisions include explicit effective dates in metadata; ingestion overrides accordingly.
- `effective_to` is set in downstream warehouses when the next vintage supersedes the line.

### 4.3 Categorical Guards

- `status_code` validated against Addendum B enumerations; out-of-band values escalate to BLOCK.  
- `global_days` domain enforced; blank entries convert to `None`.  
- `modifier` normalized to uppercase; blanks trimmed to `None` before NK evaluation.

---

## 5. Known Data Quality Issues

### 5.1 Modifier Reuse with Distinct Status Codes

- CMS occasionally publishes the same HCPCS/modifier twice with different status codes (e.g., `A` vs `R`).  
- The parser’s NK includes `status_code` to support this; duplicates on the 4-part NK are true CMS errors and result in BLOCK.

### 5.2 Incomplete RVUs for Carrier-Priced Codes (`status_code = 'C'`/`'R'`)

- CMS sets RVUs to blank for certain status codes.  
- Parser leaves nulls in RVU columns and surfaces WARN metrics; downstream pricing excludes these lines unless custom rules fill values.

### 5.3 Layout Drift

- Fixed-width layouts occasionally shift (historically ±2 chars). Layout registry versions capture empirical offsets.  
- QA monitors row count deltas and column alignment each release; mismatches raise `LayoutMismatchError`.

---

## 6. Testing & QA

- **Golden Fixtures:** `tests/fixtures/pprrvu/golden/` includes TXT/CSV/XLSX snapshots (18-row mini sample + full slice).  
- **Negative Tests:** 
  - Duplicate NK fixture (`pprrvu_duplicate_hcpcs_modifier.txt`) ensures BLOCK behavior.  
  - Layout drift fixture with misaligned decimals triggers `LayoutMismatchError`.
- **Integration:** `tests/ingestion/test_pprrvu_parser_golden.py` exercises all supported formats and asserts deterministic hashing + NK uniqueness.

---

## 7. Operational Guidelines

- **Metadata Requirements:** `release_id`, `schema_id`, `product_year`, `quarter_vintage`, `vintage_date`, `file_sha256`, `layout_version`.  
- **Staging Strategy:** Load raw parse output, then publish to `cms_pprrvu` table after validation. `row_content_hash` ensures idempotent upserts.  
- **Backfill Playbook:** Re-run parser for desired vintage with correct metadata; duplicates are automatically replaced via NK.

---

## 8. Dependencies & Downstream Usage

- **GPCI (SRC-gpci.md):** Combined at pricing time for geographic adjustments.  
- **Conversion Factors (SRC-conversion-factor.md):** Multipliers applied after RVU × GPCI calculations.  
- **OPPSCAP (SRC-oppscap.md):** `opps_cap_applicable` indicator references OPPS cap logic.  
- **Locality Crosswalk (SRC-locality.md / SRC-carrier-localities.md):** Required for zip-to-locality mapping in pricing workflows.  
- **Pricing API:** Exposes RVU components via `/v1/mpfs/{hcpcs}` endpoints.

---
