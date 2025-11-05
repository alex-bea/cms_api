# MPFS & OPPS Ingestor Architecture Plan

**Date:** 2025-01-15  
**Status:** Draft v1.0  
**Owners:** Data Engineering  
**Priority:** 🔴 Critical (ClearBill v1 Launch Blockers)

---

## Executive Summary

This document provides comprehensive architecture plans for completing MPFS and OPPS ingestion pipelines to support ClearBill v1 launch (Q1 2026). Both ingestors have scaffold implementations but require completion of validation logic, normalization adapters, and enrichment workflows.

**Current State:**
- ✅ Database schema ready (provenance columns, indexes, dataset_snapshots table)
- ✅ DIS-compliant structure exists (5-stage pipeline: Land → Validate → Normalize → Enrich → Publish)
- ⚠️ **MPFS:** Placeholder validators, missing adapter implementations
- ⚠️ **OPPS:** Placeholder parse methods, missing wage index enrichment
- ✅ PDF layout files available in `sample_data/rvu25d_0/RVU25D.pdf` for context
- ✅ Test data available in `test_data/ingestion_2025/` and `sample_data/`

**Success Criteria:**
- End-to-end pipeline runs successfully
- Data appears in curated tables (`fee_mpfs`, `fee_opps`)
- Provenance metadata recorded (`release_id`, `batch_id`, `dataset_digest`)
- API endpoints return data (`/v1/mpfs`, `/v1/opps`)
- All of the above

---

## 1. Infrastructure Readiness Assessment

### 1.1 Database Status ✅

**Connection:** ✅ **VERIFIED**
```bash
Database: cms_pricing_db
User: cms_pricing_db_user
PostgreSQL: 17.6 (Debian)
Status: Connected and accessible
```

**Migrations:** ✅ **AT HEAD**
```
Current Version: 98567c0bbfa8 (head)
- ✅ dataset_snapshots table exists
- ✅ fee_mpfs has release_id, batch_id columns
- ✅ fee_opps has release_id, batch_id columns
- ✅ All provenance indexes created
```

**Tables Ready:**
- `fee_mpfs` - Primary MPFS table with provenance columns
- `fee_opps` - Primary OPPS table with provenance columns
- `gpci` - GPCI indices with provenance
- `conversion_factors` - Conversion factors with provenance
- `wage_index` - Wage index data (needs OPPS enrichment)
- `dataset_snapshots` - Snapshot registry for deterministic selection

**Recommendation:** ✅ **READY** - No database migrations needed

### 1.2 Disk Space & Storage

**Data Directories:**
```
data/
├── ingestion/mpfs/     # MPFS ingestion artifacts
├── ingestion/opps/     # OPPS ingestion artifacts
├── raw/                # Raw downloaded files
├── curated/            # Curated parquet outputs
└── test_data/          # Test fixtures (existing)
```

**Estimated Space Requirements:**
- MPFS raw data: ~50-100 MB per release (ZIP files)
- OPPS raw data: ~20-50 MB per quarter (ZIP files)
- Curated parquet: ~10-30 MB per release
- Total for 2025 vintage: ~500 MB

**Recommendation:** ✅ **SUFFICIENT** - Ensure 1GB+ free space for ingestion runs

### 1.3 Environment Configuration

**Required Environment Variables:**
```bash
DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing
# Or from docker-compose
```

**Dependencies:**
- ✅ PostgreSQL 15+ (17.6 available)
- ✅ Python 3.11+ with pandas, httpx, sqlalchemy
- ✅ Alembic for migrations (already at head)

**Recommendation:** ✅ **READY** - Environment is properly configured

---

## 2. MPFS Ingestor Architecture

### 2.1 Current Implementation Status

**File:** `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` (647 lines)

**What Works:**
- ✅ DIS-compliant structure (5-stage pipeline)
- ✅ RVU/GPCI snapshots available via `DatasetSnapshotService`
- ✅ Land stage can download new artifacts
- ✅ Basic schema contracts loaded
- ✅ Observability collector initialized

**What's Broken/Missing:**
- ❌ Validation rules use `lambda x: True` placeholders (lines 158, 164, 170, 176, 182)
- ❌ Adapter factory returns generic adapter (needs MPFS-specific logic)
- ❌ Enricher factory returns generic enricher (needs GPCI/locality joins)
- ❌ Normalize stage doesn't parse RVU/GPCI/CF files
- ❌ Publish stage doesn't write to `fee_mpfs`, `gpci`, `conversion_factors` tables
- ❌ Discovery still references deprecated `CMSMPFSScraper`; must switch to snapshot reuse + CF fetcher

### 2.2 Architecture Design

#### 2.2.1 Data Flow

```
CMS Sources
    ↓
[Discovery] → Reuse RVU/GPCI snapshots + fetch conversion factor (⚙️ New helper)
    ↓
[Land] → Download/resolve CF artefacts or snapshot pointers (⚙️ Needs update)
    ↓
[Validate] → Structural/schema/domain checks (❌ Needs implementation)
    ↓
[Normalize] → Parse CSV/TXT/XLSX → Canonical DataFrames (❌ Needs implementation)
    ↓
[Enrich] → Join with geography, add locality_id (❌ Needs implementation)
    ↓
[Publish] → Write to fee_mpfs, gpci, conversion_factors tables (❌ Needs implementation)
    ↓
[Provenance] → Record in dataset_snapshots, ingestion_runs (⚠️ Needs integration)
```

#### 2.2.2 Key Components

**1. Validation Engine (`_validate_stage`)**

**Current:** Placeholder lambdas return `True` for all rules

**Required Implementation:**
```python
# Structural validation
- File exists and is readable
- ZIP file extracts successfully
- Required files present (PPRRVU, GPCI, CF files)
- File size within expected ranges

# Schema validation
- HCPCS codes are 5 characters
- RVU values are numeric and non-negative
- Status codes are valid CMS codes (A, B, C, D, E, F, G, H, J, K, L, M, N, P, Q, R, S, T, U, V, W, X, Y, Z)
- Effective dates are valid dates

# Domain validation
- RVU components sum correctly (work + PE + MP)
- Global days are valid (0, 10, 90, XXX)
- Locality codes match known localities
- GPCI values are between 0.5 and 2.0

# Statistical validation
- Row count within ±15% of previous vintage
- RVU distribution matches expected patterns
- No duplicate HCPCS+modifier combinations
```

**2. Normalize Stage (`_normalize_stage`)**

**Current:** Returns empty adapted batch

**Required Implementation:**
```python
# Parse RVU bundle files
- PPRRVU2025_Oct.txt/.csv/.xlsx → DataFrame with columns:
  * hcpcs, modifier, status_code, global_days
  * work_rvu, pe_nf_rvu, pe_fac_rvu, mp_rvu
  * effective_from, effective_to
  
- GPCI2025.txt/.csv/.xlsx → DataFrame with columns:
  * locality_id, locality_name
  * gpci_work, gpci_pe, gpci_mp
  * effective_from, effective_to
  
- Conversion factor files → DataFrame with columns:
  * year, cf, source (MPFS/Anesthesia)
  * effective_from, effective_to

# Use PDF layout for parsing guidance
- Reference: sample_data/rvu25d_0/RVU25D.pdf
- Extract fixed-width column positions
- Handle multiple file formats (TXT fixed-width, CSV, XLSX)
```

**3. Enrich Stage (`_enrich_stage`)**

**Current:** Returns same data without enrichment

**Required Implementation:**
```python
# Join with reference data
- Geography: ZIP → Locality mapping (use existing GeographyService)
- Locality dimension: Add locality_id to fee_mpfs rows
- GPCI: Join GPCI data with locality_id
- Conversion factors: Add CF vintage to MPFS data

# Add provenance metadata
- release_id: "mpfs_2025_D_20250115_143022"
- batch_id: UUID from batch
- dataset_digest: SHA256 of curated data
- effective_from/effective_to: From CMS metadata
```

**4. Publish Stage (`_publish_stage`)**

**Current:** Scaffolds outputs but doesn't persist

**Required Implementation:**
```python
# Write to database tables
- fee_mpfs: Insert/update rows with provenance
- gpci: Insert/update GPCI indices
- conversion_factors: Insert/update CF values

# Write curated parquet
- data/curated/mpfs/{release_id}/mpfs_rvu.parquet
- data/curated/mpfs/{release_id}/mpfs_gpci.parquet
- data/curated/mpfs/{release_id}/mpfs_cf.parquet

# Register in dataset_snapshots
- dataset_id: "MPFS"
- release_id: From batch
- digest: SHA256 of curated data
- effective_from/effective_to: From CMS metadata
```

### 2.3 PDF Layout Integration

**Location:** `sample_data/rvu25d_0/RVU25D.pdf`

**Purpose:**
- Provides authoritative column positions for fixed-width files
- Documents field formats and valid values
- Defines edge cases and special handling

**Implementation Strategy:**
1. **Extract layout metadata** from PDF (or use existing `layout_registry.py`)
2. **Reference layout** during parsing to validate column positions
3. **Handle edge cases** documented in PDF (e.g., continuation lines, special codes)
4. **Validate against PDF** to ensure data matches expected structure

**Integration Points:**
- `cms_pricing/ingestion/parsers/layout_registry.py` - Already has RVU25D layout
- Reuse existing layout definitions for MPFS components
- Add MPFS-specific layouts if needed

### 2.4 Dependencies on RVU Ingestor

**MPFS relies on RVU data:**
- MPFS uses RVU tables (`pprrvu`, `gpci`, `locality_counties`) from RVU ingestor
- MPFS ingestor should **reference** RVU data, not re-ingest it
- MPFS adds MPFS-specific data (conversion factors, abstracts)

**Architecture Decision:**
- ✅ **RVU Ingestor** populates `pprrvu`, `gpci`, `locality_counties` tables
- ✅ **MPFS Ingestor** creates curated views referencing RVU data + adds CF/abstracts
- MPFS ingestor should verify RVU data exists before running

---

## 3. OPPS Ingestor Architecture

### 3.1 Current Implementation Status

**File:** `cms_pricing/ingestion/ingestors/opps_ingestor.py` (1022 lines)

**What Works:**
- ✅ DIS-compliant structure (5-stage pipeline)
- ✅ Discovery via `CMSOPPSScraper`
- ✅ Land stage (downloads files, handles license redirects)
- ✅ Basic schema contracts loaded
- ✅ Observability collector initialized

**What's Broken/Missing:**
- ❌ Parse methods return placeholders (`_parse_addendum_a`, `_parse_addendum_b`, etc.)
- ❌ Wage index enrichment not implemented
- ❌ SI lookup data not loaded
- ❌ Publish stage doesn't write to `fee_opps`, `wage_index` tables
- ⚠️ License acceptance automation needed (currently manual)

### 3.2 Architecture Design

#### 3.2.1 Data Flow

```
CMS Sources
    ↓
[Discovery] → Scraper generates manifest (✅ Works)
    ↓
[Land] → Download ZIP files (⚠️ Needs license automation)
    ↓
[Validate] → Structural/schema/domain checks (⚠️ Partial implementation)
    ↓
[Normalize] → Parse Addendum A/B → DataFrames (❌ Needs implementation)
    ↓
[Enrich] → Join with wage index, SI lookup (❌ Needs implementation)
    ↓
[Publish] → Write to fee_opps, wage_index tables (❌ Needs implementation)
    ↓
[Provenance] → Record in dataset_snapshots (⚠️ Needs integration)
```

#### 3.2.2 Key Components

**1. Normalize Stage (`_normalize_stage`)**

**Current:** Placeholder methods return empty DataFrames

**Required Implementation:**
```python
# Parse Addendum A (APC Payment Rates)
- File: Section 508 CSV or XLSX
- Columns: APC, relative_weight, payment_rate, effective_dates
- Output: DataFrame with columns:
  * apc, relative_weight, national_unadj_rate
  * effective_from, effective_to

# Parse Addendum B (HCPCS → APC Crosswalk)
- File: Section 508 CSV or XLSX
- Columns: HCPCS, modifier, status_indicator, APC
- Output: DataFrame with columns:
  * hcpcs, modifier, status_indicator, apc
  * effective_from, effective_to

# Handle multiple file formats
- CSV (Section 508 compliant)
- XLSX (Excel workbooks)
- TXT (fixed-width, if provided)
- ZIP bundles (multiple addenda)
```

**2. Enrich Stage (`_enrich_stage`)**

**Current:** Returns same data without enrichment

**Required Implementation:**
```python
# Wage index enrichment
- Load IPPS wage index data (annual reference)
- Join CCN → CBSA → wage_index
- Calculate wage-adjusted rates:
  * wage_adjusted_rate = national_rate × wage_index
- Handle facility-specific adjustments

# SI lookup enrichment
- Load status indicator lookup table
- Add SI descriptions and payment rules
- Validate SI codes against known values

# Geography enrichment
- Join with ZIP → CBSA mapping
- Add facility-specific wage adjustments
```

**3. Publish Stage (`_publish_stage`)**

**Current:** Scaffolds outputs but doesn't persist

**Required Implementation:**
```python
# Write to database tables
- fee_opps: Insert/update rows with provenance
- wage_index: Insert/update wage index data (if new)
- opps_rates_enriched: Materialized view with wage adjustments

# Write curated parquet
- data/curated/opps/{release_id}/opps_apc_payment.parquet
- data/curated/opps/{release_id}/opps_hcpcs_crosswalk.parquet
- data/curated/opps/{release_id}/opps_rates_enriched.parquet

# Register in dataset_snapshots
- dataset_id: "OPPS"
- release_id: From batch
- digest: SHA256 of curated data
- effective_from/effective_to: From CMS metadata
```

### 3.3 PDF Layout Integration

**OPPS Layout Files:**
- Addendum A/B layout PDFs (if available in CMS downloads)
- I/OCE notes PDF (documentation for status indicators)
- Reference: `prds/PRD-opps-prd-v1.0.md` for field definitions

**Implementation Strategy:**
1. **Extract layout** from CMS documentation or layout PDFs
2. **Reference layout** during parsing for Section 508 CSV/XLSX
3. **Handle edge cases** (e.g., packaging flags, device-dependent codes)
4. **Validate against PDF** for field formats

### 3.4 License Acceptance Automation

**Current Issue:** OPPS downloads require AMA license acceptance (interstitial redirect)

**Required Solution:**
```python
# Automated license acceptance
- Use headless browser (Playwright/Selenium)
- Navigate to license page
- Accept terms automatically
- Capture session cookies
- Use cookies for subsequent downloads

# Alternative: Manual pre-download
- Download files manually once per quarter
- Store in test_data/ingestion_2025/opps/
- Use local files for testing
```

**Recommendation:** Start with **local test data** approach, add automation later

---

## 4. Testing Strategy

### 4.1 Staged Testing Approach

**Phase 1: Unit Tests with Local Test Data**
- Use existing test data in `test_data/ingestion_2025/`
- Use sample data in `sample_data/rvu25d_0/`
- Test each stage independently (validate, normalize, enrich, publish)
- No scraper calls needed

**Phase 2: Integration Tests**
- Full pipeline with local test data
- Verify database writes
- Verify provenance metadata
- Verify curated parquet outputs

**Phase 3: End-to-End Tests**
- Run with real CMS data (optional)
- Verify against CMS PFS Lookup Tool
- Verify API endpoints return data

### 4.2 Test Data Locations

**Existing Test Data:**
```
test_data/ingestion_2025/
├── full/curated/cms_rvu/    # RVU test data
└── raw/cms_rvu/            # Raw RVU ZIP files

sample_data/
├── rvu25d_0/               # Complete RVU bundle (PPRRVU, GPCI, CF, etc.)
│   ├── PPRRVU2025_Oct.txt
│   ├── GPCI2025.txt
│   ├── RVU25D.pdf          # Layout reference
│   └── ...
└── 2025_carrier_files/     # ZIP locality files
```

**Recommendation:**
- ✅ Use local test data for initial development
- ✅ Download OPPS test data manually (avoid license automation initially)
- ✅ Store test data in `test_data/ingestion_2025/opps/` and `test_data/ingestion_2025/mpfs/`

### 4.3 Test Coverage Requirements

**Unit Tests:**
- Validation rules (structural, schema, domain, statistical)
- Parsing logic (TXT fixed-width, CSV, XLSX)
- Enrichment joins (geography, wage index, SI lookup)
- Provenance metadata generation

**Integration Tests:**
- Full pipeline execution
- Database writes
- Curated parquet generation
- Dataset snapshot registration

**End-to-End Tests:**
- API endpoint responses
- Provenance metadata in responses
- Data freshness validation

---

## 5. Implementation Roadmap

### 5.1 MPFS Ingestor (Priority 1)

**Week 1: Validation & Normalization**
- [ ] Implement real validation rules (replace placeholders)
- [ ] Implement parsing logic for PPRRVU, GPCI, CF files
- [ ] Integrate PDF layout reference
- [ ] Unit tests for validation and parsing

**Week 2: Enrichment & Publishing**
- [ ] Implement enrichment (geography joins, locality_id)
- [ ] Implement database writes (fee_mpfs, gpci, conversion_factors)
- [ ] Implement curated parquet output
- [ ] Implement dataset_snapshots registration
- [ ] Integration tests

**Week 3: Testing & Documentation**
- [ ] End-to-end tests with local test data
- [ ] API endpoint validation
- [ ] Runbook documentation
- [ ] Ready for production run

### 5.2 OPPS Ingestor (Priority 2)

**Week 1: Parsing & Normalization**
- [ ] Implement Addendum A parsing (APC payment rates)
- [ ] Implement Addendum B parsing (HCPCS crosswalk)
- [ ] Handle multiple file formats (CSV, XLSX, ZIP)
- [ ] Unit tests for parsing

**Week 2: Enrichment & Publishing**
- [ ] Implement wage index enrichment
- [ ] Implement SI lookup enrichment
- [ ] Implement database writes (fee_opps, wage_index)
- [ ] Implement curated parquet output
- [ ] Integration tests

**Week 3: Testing & Documentation**
- [ ] End-to-end tests with local test data
- [ ] License acceptance automation (or manual workaround)
- [ ] API endpoint validation
- [ ] Runbook documentation
- [ ] Ready for production run

---

## 6. Success Metrics

### 6.1 MPFS Ingestor

**Data Quality:**
- ✅ Parses PPRRVU, GPCI, CF files successfully
- ✅ Validates all required fields
- ✅ Joins with geography correctly (locality_id populated)
- ✅ Writes to fee_mpfs, gpci, conversion_factors tables

**Provenance:**
- ✅ release_id, batch_id populated in all tables
- ✅ dataset_digest calculated and stored
- ✅ Registered in dataset_snapshots table

**API Readiness:**
- ✅ `/v1/mpfs` endpoint returns data
- ✅ Responses include datasets_used metadata
- ✅ Provenance metadata visible in API responses

### 6.2 OPPS Ingestor

**Data Quality:**
- ✅ Parses Addendum A/B successfully
- ✅ Enriches with wage index correctly
- ✅ Writes to fee_opps, wage_index tables

**Provenance:**
- ✅ release_id, batch_id populated in all tables
- ✅ dataset_digest calculated and stored
- ✅ Registered in dataset_snapshots table

**API Readiness:**
- ✅ `/v1/opps` endpoint returns data
- ✅ Responses include datasets_used metadata
- ✅ Provenance metadata visible in API responses

---

## 7. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| PDF layout parsing fails | High | Use existing layout_registry.py, manual validation |
| Validation rules too strict | Medium | Start permissive, tighten after data review |
| Wage index data missing | High | Use existing IPPS data or manual reference |
| License automation fails | Medium | Manual download workaround, local test data |
| Database write performance | Medium | Batch inserts, use COPY for large datasets |
| Provenance metadata incomplete | High | Unit tests for metadata generation, integration tests |

---

## 8. Next Steps

1. **Review this architecture plan** with team
2. **Create detailed implementation plans** (separate documents)
3. **Set up test data** in `test_data/ingestion_2025/mpfs/` and `test_data/ingestion_2025/opps/`
4. **Begin implementation** starting with MPFS validation rules
5. **Iterate with testing** using local test data

---

## 9. References

- **PRD-MPFS:** `prds/PRD-mpfs-prd-v1.0.md`
- **PRD-OPPS:** `prds/PRD-opps-prd-v1.0.md`
- **Source Map:** `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **RVU Ingestor (Reference):** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py`
- **Database Models:** `cms_pricing/models/fee_schedules.py`
- **Readiness Plan:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md`
