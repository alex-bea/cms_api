# Ingestor Gap Analysis & MECE Assessment

**Date:** 2025-01-15  
**Status:** Analysis Complete

---

## Executive Summary

**Current State:**
- ✅ **5 ingestors exist** (RVU, MPFS, OPPS, ZIP9, ZIP Locality)
- ⚠️ **2 ingestors need fixes** (MPFS, OPPS - may have broken imports)
- 🟡 **6 ingestors planned post-launch** (ASC, CLFS, DMEPOS, IPPS, ASP, NADAC align with Treatment Plan API scope)

**MECE Status:** ❌ **NOT MECE** - Missing 6 critical ingestors

**ClearBill PRD Alignment:** ✅ **ALIGNED** - All required datasets identified

---

## 1. Artifact Review: Existing Plans

### Findings
- **Detailed plan published for MPFS** in `artifacts/mpfs_implementation_plan.md` (v2) covering ingestion reuse, conversion-factor landing, curated views, and test strategy.
- **No specific plans found** for ASC, CLFS, DMEPOS, IPPS, ASP, or NADAC ingestors in `artifacts/` folder.
- Artifacts focus on:
  - ✅ RVU ingestion (complete)
  - ✅ Phase 2 provenance tracking (complete)
  - ✅ Quick wins (Dataset Snapshots, CodePricingItem)
  - 📝 Post-phase 2 next steps (mentions datasets but not ingestor implementation details)

### Relevant Artifacts
- `artifacts/post_phase2_next_steps.md` - Mentions datasets but no ingestor plans
- `artifacts/quick_wins_detailed_plans.md` - Focuses on API/DB improvements, not ingestion
- `planning/project/INGESTOR_DEVELOPMENT_TASKS.md` - Lists all ingestors but no detailed implementation plans

---

## 2. ClearBill PRD Alignment

### ClearBill PRD (`PRD-clearbill-prd-v1.0.md`)
**Explicitly Mentions:**
- ✅ MPFS (Medicare Physician Fee Schedule)
- ✅ OPPS (Outpatient Prospective Payment System)
- ✅ GPCI adjustments (via RVU datasets)

**Not in scope for v1:**
- ASC, CLFS, DMEPOS, IPPS, ASP, NADAC (no references to endpoints or datasets)

### Treatment Plan API PRD (`PRD-cms-treatment-plan-api-prd-v0.1.md`)
**Explicitly Lists ALL Required Datasets:**
- ✅ **MPFS** (RVUs, GPCI, CF) — annual (+ revisions A/B/C)
- ✅ **OPPS** Addenda A/B & D1 + wage index — quarterly
- ✅ **ASC** addenda — annual + quarterly updates
- ✅ **IPPS** DRG weights, base rates, wage index — FY annual
- ✅ **CLFS** — quarterly
- ✅ **DMEPOS** — quarterly; rural status & former CBA context
- ✅ **ASP (Part B drugs)** — quarterly; NDC↔HCPCS crosswalk
- ✅ **NADAC** — weekly/monthly (as‑of dates)
- ✅ **Geography** (ZIP crosswalks) — ZIP→Locality, ZIP→CBSA (HUD) — periodic
- ✅ **Policy/edits** — NCCI quarterly; OPPS packaging rules; HCPCS quarterly

**Data Freshness SLAs:**
- MPFS: annual
- OPPS/ASC: quarterly
- ASP: quarterly
- NADAC: weekly/monthly
- CLFS/DMEPOS: quarterly
- IPPS: annual (FY)

### Conclusion
- ✅ ClearBill PRD focuses on MPFS + OPPS + GPCI (v1 launch scope).
- ✅ Treatment Plan API PRD is broader and introduces ASC, CLFS, DMEPOS, IPPS, ASP, NADAC, NADAC, NCCI, etc.
- 🔄 Readiness plan should track extras as **post-launch backlog** unless ClearBill scope expands.

---

## 3. MECE Analysis (Mutually Exclusive, Collectively Exhaustive)

### Current Engine Coverage

| Dataset | Engine | Model | Ingester Status | Priority |
|---------|--------|-------|-----------------|----------|
| **MPFS** | `MPFSEngine` ✅ | `FeeMPFS` ✅ | ⚠️ Exists (needs fixes) | 🔴 Critical |
| **OPPS** | `OPPSEngine` ✅ | `FeeOPPS` ✅ | ⚠️ Exists (needs fixes) | 🔴 Critical |
| **ASC** | `ASCEngine` ✅ | `FeeASC` ✅ | 🚧 Planned (post-launch) | 🟡 High |
| **IPPS** | `IPPSEngine` ✅ | `FeeIPPS` ✅ | 🚧 Planned (post-launch) | 🟡 High |
| **CLFS** | `CLFSEngine` ✅ | `FeeCLFS` ✅ | 🚧 Planned (post-launch) | 🟡 High |
| **DMEPOS** | `DMEPOSEngine` ✅ | `FeeDMEPOS` ✅ | 🚧 Planned (post-launch) | 🟡 High |
| **ASP** | `DrugEngine` ✅ | `DrugASP` ✅ | 🚧 Planned (post-launch) | 🟡 High |
| **NADAC** | `DrugEngine` ✅ | `DrugNADAC` ✅ | 🚧 Planned (post-launch) | 🟢 Medium |
| **RVU** | `MPFSEngine` (uses) | `PPRRVU` ✅ | ✅ Complete | ✅ Done |
| **GPCI** | `MPFSEngine` (uses) | `GPCI` ✅ | ✅ (via RVU) | ✅ Done |
| **Geography** | `GeographyService` ✅ | `Geography` ✅ | ✅ Complete | ✅ Done |

### MECE Assessment

**Mutually Exclusive:** ✅ **YES**
- Each dataset has a distinct purpose and data source
- No overlap between ingestors (e.g., ASC ≠ OPPS, ASP ≠ NADAC)
- Clear boundaries: fee schedules vs. drug pricing vs. geography

**Collectively Exhaustive:** 🚧 **In progress**
- **Six ingestors deferred to post-launch backlog** (Treatment Plan API scope):
  1. ASC Ingester
  2. CLFS Ingester
  3. DMEPOS Ingester
  4. IPPS Ingester
  5. ASP Ingester
  6. NADAC Ingester

**Gap Analysis:**
- Engines exist and can query models ✅
- Models exist with correct schema ✅
- **Ingestors missing** - no way to populate models from CMS sources ❌

### Critical Path
- For ClearBill v1 (MPFS/OPPS/GPCI): ensure existing pipelines run and expose provenance.
- For Treatment Plan API expansion: track post-launch delivery of ASC, CLFS, DMEPOS, IPPS, ASP, NADAC.

---

## 4. Launch Readiness Focus (ClearBill v1)

### MPFS Ingestor (build to v2 plan)
- **Scope:** Execute steps from `artifacts/mpfs_implementation_plan.md` so MPFS ingestion reuses RVU/GPCI snapshots, lands conversion-factor artifacts, and publishes curated payment tables with provenance.
- **Key actions:**
  1. Implement scraper reuse so conversion-factor files are the only new download; attach RVU/GPCI digests via `DatasetSnapshotService`.
  2. Fill normalize/enrich/publish stages to output `mpfs_rvu`, `mpfs_indicators_all`, `mpfs_locality`, `mpfs_gpci`, `mpfs_cf_vintage`, `mpfs_payment_curated`, and `mpfs_link_keys`.
  3. Build MPFS payment calculator (facility/non-facility) per CMS formula using RVU + GPCI + CF inputs; persist curated parquet + relational tables.
  4. Extend tests with golden comparisons against CMS PFREV data and contract checks for `/v1/mpfs` provenance.
  5. Update readiness artifacts with run evidence (release/batch IDs, dataset digests, observability metrics).
- **Owners:** Data Engineering + Pricing API (schema consumers).

### OPPS Ingester (stabilize + wage index enrichment)
- **Scope:** Complete `opps_ingestor.py` so Addenda A/B load into parquet + relational tables with wage index joins.
- **Key actions:**
  1. Finish adapters/enrichers that map Section 508 CSVs to schema contract.
  2. **Consider migration to modular architecture:** Follow `artifacts/ingestor_migration_checklist.md` to refactor OPPS ingestor using the same patterns as RVU (DatasetSpec, SchemaService, adapter extraction, etc.)
  3. Automate license interstitial acceptance; confirm scraper grabs quarterly ZIPs.
  4. Run pipeline against latest quarter; verify APC payment + HCPCS crosswalk tables populate with wage index fields.
  5. Exercise `/opps` routers, confirm `CodePricingItem` responses include `datasets_used`.
- **Owners:** Data Engineering + Platform API.

### RVU / Supporting datasets
- **Status:** `RVUIngestor` production-ready; continue monitoring discovery manifests and provenance outputs.
- **Follow-up:** Keep GPCI/locality join checks in regression suite (`tests/ingestors/test_rvu_ingestor_e2e.py`).

### Launch exit criteria
- MPFS + OPPS ingestors run on current vintage with zero critical validation findings.
- API contract tests for `/v1/mpfs`, `/v1/opps`, `/v1/compare` pass with updated provenance fields.
- Observability dashboards show fresh run timestamps (< SLA window).

## 5. Post-Launch Backlog (Treatment Plan API scope)
High-level placeholders—detailed plans deferred until product signals scope expansion. Refer to `prds/REF-cms-pricing-source-map-prd-v1.0.md#direct-artifact-links-2024-2026` for source URLs.

- **ASC Ingester:** Land/normalize quarterly addenda (AA/BB/DD/EE/FF), track AMA license redirects, surface `/v1/asc`.
- **CLFS Ingester:** Parse 508-friendly TXT/CSV, watch for quarterly revisions (`QxV2`), publish lab fee schedule.
- **DMEPOS Ingester:** Join rural indicators and jurisdiction crosswalks; respect quarterly floor/ceiling updates.
- **IPPS Ingester:** Handle FY-based tables (MS-DRG weights, wage index) with multi-workbook parsing.
- **ASP Ingester:** Maintain parallel ingestion for pricing, NOC, and NDC↔HCPCS crosswalk each quarter.
- **NADAC Ingester:** Consume weekly CSV API snapshots, dedupe against monthly “first time” releases, normalize NDC11 format.

---

## 5. Implementation Recommendations

### Immediate Actions

1. **Fix Existing Ingestors First** (Week 1)
   - Fix MPFS ingestor imports/dependencies
   - Fix OPPS ingestor imports/dependencies
   - **Optionally migrate to modular architecture:** Use `artifacts/ingestor_migration_checklist.md` to refactor both ingestors following the RVU Phase 2 pattern (reduces complexity, improves maintainability)
   - Verify both can run end-to-end

2. **Prep Post-Launch Backlog** (Treatment Plan API)
   - Sequence: ASC → CLFS → DMEPOS → IPPS → ASP → NADAC
   - Capture ingestion requirements using the cheat-sheet now recorded in `prds/REF-cms-pricing-source-map-prd-v1.0.md#direct-artifact-links-2024-2026`

3. **Use RVU Ingestor as Template**
   - All ingestors should follow DIS 5-stage pipeline
   - Reuse patterns from `rvu_ingestor.py` (990 lines, down from 4,247 after Phase 2 refactoring)
   - Follow same structure: Land → Validate → Normalize → Enrich → Publish
   - **Migration Guide:** Use `artifacts/ingestor_migration_checklist.md` for step-by-step migration from monolithic to modular architecture

### DIS Compliance Requirements

Each ingester must implement:
1. **Land Stage:** Download and store raw files with manifest
2. **Validate Stage:** Structural, domain, and statistical validation
3. **Normalize Stage:** Canonicalize column names and data types
4. **Enrich Stage:** Join with reference data (geography, crosswalks)
5. **Publish Stage:** Store in curated format with metadata (release_id, batch_id)

### File Structure
```
cms_pricing/ingestion/ingestors/
├── rvu_ingestor.py          ✅ Complete
├── mpfs_ingestor.py          ⚠️ Needs fixes
├── opps_ingestor.py           ⚠️ Needs fixes
├── asc_ingestor.py            ❌ To build
├── clfs_ingestor.py           ❌ To build
├── dmepos_ingestor.py         ❌ To build
├── ipps_ingestor.py           ❌ To build
├── asp_ingestor.py            ❌ To build
└── nadac_ingestor.py          ❌ To build
```

---

## 6. Summary

### Current State
- ✅ **5 ingestors exist** (RVU, MPFS, OPPS, ZIP9, ZIP Locality); MPFS/OPPS require stabilization.
- 🟡 **6 additional ingestors tracked as post-launch backlog** (ASC, CLFS, DMEPOS, IPPS, ASP, NADAC).
- ✅ **Engines/models ready** for ingestion once pipelines land.
- ✅ **PRD alignment clarified:** ClearBill v1 (MPFS/OPPS/GPCI); Treatment Plan API introduces remaining datasets.

### MECE Status
- **Mutually Exclusive:** ✅ Yes (distinct datasets and contracts).
- **Collectively Exhaustive:** 🚧 Launch scope satisfied once MPFS/OPPS fixed; backlog captures remaining datasets.

### Critical Path (Launch)
1. Repair and validate MPFS ingestion run (current vintage, provenance recorded).
2. Finish OPPS ingestion (Addenda + wage index) and verify API contract tests.
3. Confirm RVU/GPCI pipeline freshness and provenance dashboards.

### Backlog Signals (Post-Launch)
- Kick off detailed plans for ASC → NADAC once scope officially expands.
- Leverage new artifact link appendix for source mapping and automation prep.

### Next Steps
1. Schedule MPFS/OPPS ingestion reruns with QA sign-off.
2. Document run evidence in readiness plan and dashboards.
3. Log backlog epics referencing `prds/REF-cms-pricing-source-map-prd-v1.0.md#direct-artifact-links-2024-2026`.

---

## 7. References

- **ClearBill PRD:** `prds/PRD-clearbill-prd-v1.0.md`
- **Treatment Plan API PRD:** `prds/PRD-cms-treatment-plan-api-prd-v0.1.md`
- **Ingestor Development Tasks:** `planning/project/INGESTOR_DEVELOPMENT_TASKS.md`
- **RVU Ingestor Template:** `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (990 lines, modular architecture)
- **Migration Checklist:** `artifacts/ingestor_migration_checklist.md` - Step-by-step guide for migrating monolithic ingestors to modular architecture (DatasetSpec, SchemaService, adapter extraction patterns)
- **Post-Phase 2 Next Steps:** `artifacts/post_phase2_next_steps.md`

## 8. Implementation Plans (NEW - 2025-01-15)

**Detailed plans created for MPFS and OPPS ingestors:**

- **Architecture Plan:** `artifacts/mpfs_opps_architecture_plan.md`
  - Infrastructure readiness assessment (✅ Database ready, ✅ Schema ready)
  - Architecture design for MPFS and OPPS
  - PDF layout integration strategy (reference `sample_data/rvu25d_0/RVU25D.pdf`)
  - Testing strategy with staged approach
  - Success metrics and risks

- **MPFS Implementation Plan:** `artifacts/mpfs_implementation_plan.md`
  - Step-by-step implementation guide (5 phases)
  - Validation, normalization, enrichment, publishing implementations
  - Code examples and testing requirements
  - Estimated time: 2-3 weeks

- **OPPS Implementation Plan:** `artifacts/opps_implementation_plan.md`
  - Step-by-step implementation guide (5 phases)
  - Parsing, enrichment, publishing implementations
  - License acceptance automation (future enhancement)
  - Estimated time: 2-3 weeks

- **Ingestion Runbook:** `artifacts/mpfs_opps_ingestion_runbook.md`
  - Prerequisites and environment setup
  - Step-by-step execution instructions for MPFS and OPPS
  - Verification and troubleshooting guides
  - Provenance evidence capture

- **Summary Document:** `artifacts/priority_ingestor_plans_summary.md`
  - Executive summary of all plans
  - Current state assessment
  - Implementation roadmap
  - Next steps and document references
