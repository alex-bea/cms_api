# Priority Ingestor Plans Summary

**Date:** 2025-01-15  
**Status:** Complete  
**Purpose:** Executive summary of architecture plans, implementation plans, and runbooks for MPFS and OPPS ingestors

---

## Overview

This document summarizes the comprehensive plans created to complete MPFS and OPPS ingestion pipelines for ClearBill v1 launch (Q1 2026).

**Documents Created:**
1. **Architecture Plan** - High-level design and infrastructure assessment
2. **MPFS Implementation Plan** - Detailed step-by-step implementation guide
3. **OPPS Implementation Plan** - Detailed step-by-step implementation guide
4. **Ingestion Runbook** - Step-by-step execution instructions

---

## Current State Assessment

### Infrastructure ✅ READY

- ✅ **Database:** Connected, migrations at head (98567c0bbfa8)
- ✅ **Schema:** `fee_mpfs`, `fee_opps` tables exist with provenance columns
- ✅ **Provenance:** `dataset_snapshots` table ready for registration
- ✅ **Disk Space:** Sufficient for ingestion runs
- ✅ **Environment:** Properly configured

### Implementation Status

**MPFS Ingestor:**
- ✅ DIS-compliant structure exists (5-stage pipeline)
- ✅ Discovery and land stages work
- ❌ Validation rules use placeholders (`lambda x: True`)
- ❌ Normalization stage doesn't parse files
- ❌ Enrichment stage doesn't join data
- ❌ Publish stage doesn't write to database

**OPPS Ingestor:**
- ✅ DIS-compliant structure exists (5-stage pipeline)
- ✅ Discovery and land stages work (with license handling)
- ⚠️ Validation partially implemented
- ❌ Parse methods return placeholders
- ❌ Wage index enrichment not implemented
- ❌ Publish stage doesn't write to database

---

## Architecture Plans

### Key Design Decisions

1. **PDF Layout Integration**
   - Reference `sample_data/rvu25d_0/RVU25D.pdf` for authoritative column positions
   - Use existing `layout_registry.py` for layout definitions
   - Handle edge cases documented in PDF

2. **Staged Testing Approach**
   - Phase 1: Unit tests with local test data
   - Phase 2: Integration tests with full pipeline
   - Phase 3: End-to-end tests with real CMS data (optional)

3. **MPFS Dependencies**
   - MPFS ingestor references RVU data (doesn't re-ingest)
   - MPFS adds MPFS-specific data (conversion factors, abstracts)
   - Verify RVU data exists before running MPFS ingestion

4. **OPPS License Handling**
   - Start with manual download approach (local test data)
   - Future: Automated license acceptance with headless browser

---

## Implementation Roadmap

### MPFS Ingestor (Priority 1)

**Week 1: Validation & Normalization**
- Replace placeholder validators with real logic
- Implement parsing for PPRRVU, GPCI, CF files
- Integrate PDF layout reference
- Unit tests

**Week 2: Enrichment & Publishing**
- Implement enrichment (geography joins, locality_id)
- Implement database writes
- Implement curated parquet output
- Implement dataset_snapshots registration
- Integration tests

**Week 3: Testing & Documentation**
- End-to-end tests
- API endpoint validation
- Runbook documentation

### OPPS Ingestor (Priority 2)

**Week 1: Parsing & Normalization**
- Implement Addendum A parsing (APC payment rates)
- Implement Addendum B parsing (HCPCS crosswalk)
- Handle multiple file formats
- Unit tests

**Week 2: Enrichment & Publishing**
- Implement wage index enrichment
- Implement SI lookup enrichment
- Implement database writes
- Implement curated parquet output
- Integration tests

**Week 3: Testing & Documentation**
- End-to-end tests
- License acceptance automation (or manual workaround)
- API endpoint validation
- Runbook documentation

---

## Success Criteria

### MPFS Ingestor

**Data Quality:**
- ✅ Parses PPRRVU, GPCI, CF files successfully
- ✅ Validates all required fields
- ✅ Joins with geography correctly (locality_id populated)
- ✅ Writes to `fee_mpfs`, `gpci`, `conversion_factors` tables

**Provenance:**
- ✅ `release_id`, `batch_id` populated in all tables
- ✅ `dataset_digest` calculated and stored
- ✅ Registered in `dataset_snapshots` table

**API Readiness:**
- ✅ `/v1/mpfs` endpoint returns data
- ✅ Responses include `datasets_used` metadata
- ✅ Provenance metadata visible in API responses

### OPPS Ingestor

**Data Quality:**
- ✅ Parses Addendum A/B successfully
- ✅ Enriches with wage index correctly
- ✅ Writes to `fee_opps`, `wage_index` tables

**Provenance:**
- ✅ `release_id`, `batch_id` populated in all tables
- ✅ `dataset_digest` calculated and stored
- ✅ Registered in `dataset_snapshots` table

**API Readiness:**
- ✅ `/v1/opps` endpoint returns data
- ✅ Responses include `datasets_used` metadata
- ✅ Provenance metadata visible in API responses

---

## Testing Strategy

### Staged Approach (Recommended)

**Phase 1: Unit Tests with Local Test Data**
- Use existing test data in `test_data/ingestion_2025/`
- Use sample data in `sample_data/rvu25d_0/`
- Test each stage independently
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

### Test Data Locations

**Existing Test Data:**
- `test_data/ingestion_2025/` - RVU test data
- `sample_data/rvu25d_0/` - Complete RVU bundle (PPRRVU, GPCI, CF, PDF layout)

**To Create:**
- `test_data/ingestion_2025/mpfs/` - MPFS test data
- `test_data/ingestion_2025/opps/` - OPPS test data (manually downloaded)

---

## Next Steps

### Immediate Actions

1. **Review Architecture Plans**
   - Review `artifacts/mpfs_opps_architecture_plan.md`
   - Confirm design decisions with team
   - Address any questions or concerns

2. **Set Up Test Data**
   - Verify `sample_data/rvu25d_0/` exists
   - Create `test_data/ingestion_2025/mpfs/` directory
   - Download OPPS test data to `test_data/ingestion_2025/opps/` (manual)

3. **Begin Implementation**
   - Start with MPFS validation rules (Priority 1)
   - Follow `artifacts/mpfs_implementation_plan.md` step-by-step
   - Test with local test data

4. **Iterate with Testing**
   - Use staged testing approach
   - Validate each phase before moving to next
   - Document any issues or blockers

### Follow-Up Actions

1. **Complete MPFS Ingestion**
   - Finish all implementation phases
   - Run end-to-end tests
   - Execute production run using runbook

2. **Complete OPPS Ingestion**
   - Finish all implementation phases
   - Handle license acceptance (manual or automated)
   - Run end-to-end tests

3. **Verify API Endpoints**
   - Test `/v1/mpfs` and `/v1/opps` endpoints
   - Verify provenance metadata in responses
   - Update readiness documentation

4. **Update Documentation**
   - Update `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` with run metrics
   - Update `artifacts/tomorrow_plan.md` with status
   - Create backlog tickets for ASC → NADAC ingestors

---

## Document References

### Architecture & Planning
- **Architecture Plan:** `artifacts/mpfs_opps_architecture_plan.md`
- **Gap Analysis:** `artifacts/ingestor_gap_analysis.md`

### Implementation Guides
- **MPFS Implementation Plan:** `artifacts/mpfs_implementation_plan.md`
- **OPPS Implementation Plan:** `artifacts/opps_implementation_plan.md`

### Execution Guides
- **Ingestion Runbook:** `prds/RUN-mpfs-ingestion-v1.0.md`

### PRD References
- **ClearBill PRD:** `prds/PRD-clearbill-prd-v1.0.md`
- **MPFS PRD:** `prds/PRD-mpfs-prd-v1.0.md`
- **OPPS PRD:** `prds/PRD-opps-prd-v1.0.md`
- **Source Map:** `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Readiness Plan:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md`

### Code References
- **RVU Ingestor (Template):** `cms_pricing/ingestion/ingestors/rvu_ingestor.py`
- **MPFS Ingestor:** `cms_pricing/ingestion/ingestors/mpfs_ingestor.py`
- **OPPS Ingestor:** `cms_pricing/ingestion/ingestors/opps_ingestor.py`
- **Layout Registry:** `cms_pricing/ingestion/parsers/layout_registry.py`
- **Database Models:** `cms_pricing/models/fee_schedules.py`

### Test Data
- **Sample Data:** `sample_data/rvu25d_0/` (includes PDF layout)
- **Test Data:** `test_data/ingestion_2025/`

---

## Summary

This planning effort provides comprehensive architecture plans, detailed implementation guides, and step-by-step runbooks for completing MPFS and OPPS ingestion pipelines. The infrastructure is ready, test data is available, and the implementation roadmap is clear.

**Key Deliverables:**
- ✅ Architecture plan with infrastructure assessment
- ✅ Detailed implementation plans for both ingestors
- ✅ Step-by-step runbooks for execution
- ✅ Testing strategy with staged approach
- ✅ Success criteria and verification steps

**Next Action:** Begin implementation starting with MPFS validation rules, following the detailed implementation plan.
