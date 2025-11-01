# Plan Review: GPCI v1.3 & OPPS Wage Index Implementation

**Date:** 2025-10-31  
**Reviewer Analysis:** Comprehensive evaluation of proposed implementation plan

---

## Overall Assessment

**Strengths:** ✅ Well-structured, covers both technical and operational concerns, includes regression prevention  
**Areas for Enhancement:** 🔄 Sequence prioritization, environment assumptions, incremental delivery strategy

---

## Issue 1: Restore GPCI v1.3 Load to Render

### ✅ What's Good About This Plan

1. **Diagnostic-first approach** - Reproducing the failure in Render shell is smart before assuming root cause
2. **Documentation focus** - Updating runbooks prevents future operator confusion
3. **Automated verification** - Adding CI smoke tests prevents regressions
4. **Quality gates** - Checking GPCI releases before ClearBill readiness is proper validation

### 🔄 Recommendations & Considerations

#### 1. Verify Segfault Actually Occurs on Render

**Current Plan:** Assume segfault happens on Render, diagnose there

**Recommendation:** Test this hypothesis first:
```bash
# Before diagnosing in Render, try:
# Option A: Render One-Off Job (clean Docker environment)
# This might work immediately without any fixes

# Option B: If it does segfault, then diagnose
# But many segfaults are macOS/local environment specific
```

**Rationale:** The segfault issue is documented as happening in local conda/macOS environments. Render's Docker environment might not have this issue. Testing first saves time.

#### 2. Consider Alternative: Use RVU Ingestor

**Current Plan:** Fix `load_rvu_to_production.py` to work in Render

**Alternative:** The RVU ingestor already handles GPCI parsing and loading with full provenance support. Consider:
- Using `scripts/load_rvu_to_production.py` directly (which already exists)
- This might avoid segfault entirely if it uses different code paths
- Already supports provenance columns (release_id, batch_id)

**Action:** Check if segfault occurs with RVU ingestor or just the old backfill script.

#### 3. CI Smoke Test Scope

**Current Plan:** Exercise `scripts/load_rvu_to_production.py` against containerized Render clone

**Enhancement:** Also test:
- Migration application (alembic upgrade)
- Provenance column population
- API endpoint responses include provenance

**Suggested Test Structure:**
```python
# tests/integration/test_render_loading.py
def test_gpci_load_with_provenance():
    """Test GPCI loading in Render-like environment with provenance"""
    # 1. Run migration
    # 2. Load GPCI via load_rvu_to_production.py
    # 3. Verify release_id/batch_id populated
    # 4. Query API and verify datasets_used contains GPCI provenance
```

### ⚠️ Potential Issues

1. **Timeline Risk:** Diagnosing + patching Docker image might take longer than using existing RVU ingestor
2. **Over-engineering:** If segfault is local-only, fixing Render environment won't help
3. **Missing Verification:** Plan doesn't mention verifying provenance columns are populated

### 📋 Revised Sequence Suggestion

1. **First (15 min):** Try Render One-Off Job with `load_rvu_to_production.py` - does it work?
2. **If yes:** Load data, verify, document success ✅
3. **If no:** Diagnose in Render shell, capture stack trace
4. **Then:** Patch Docker/buildpacks as needed
5. **Finally:** Add CI smoke tests

This sequence maximizes chance of quick win before investing in environment fixes.

---

## Issue 2: Ship OPPS Ingestion with Wage Index Enrichment

### ✅ What's Good About This Plan

1. **Clear scope** - Implementing real parsing vs scaffolding is well-defined
2. **Natural key constraints** - Proper database design consideration
3. **Enrichment wiring** - Completing the enrichment pipeline
4. **Quality gates** - Parquet goldens and regression tests are solid practices
5. **Readiness alignment** - Matches ClearBill requirements

### 🔄 Recommendations & Considerations

#### 1. Current State Assessment (What I Found)

**Already Implemented:**
- ✅ Enrich stage infrastructure (`_enrich_stage()` method)
- ✅ OPPSRatesEnriched model exists with wage_index column
- ✅ Router endpoint `/api/v1/opps/enriched` exists and works
- ✅ OPPS engine has wage index filter logic
- ✅ WageIndex model exists in `fee_schedules.py` with provenance columns

**Scaffolding (Needs Implementation):**
- ❌ `_parse_addendum_a()` - returns empty DataFrame
- ❌ `_parse_addendum_b()` - returns empty DataFrame  
- ❌ `_load_wage_index_data()` - returns empty DataFrame
- ❌ `_enrich_with_wage_index()` - just returns original data

#### 2. Wage Index Data Source Clarification

**Plan Says:** "Persist wage-index reference data with natural-key constraints"

**Question:** Where does wage index data come from?
- Is it in the OPPS Addendum files?
- Separate CMS release?
- IPPS wage index data reused for OPPS?

**Recommendation:** Clarify in plan:
1. Data source for wage index (Addendum C? IPPS? Separate scraper?)
2. Parsing approach (new parser or reuse existing IPPS parser?)
3. Storage strategy (separate table vs enriched view)

**Found Evidence:** `cms_pricing/models/fee_schedules.py` has `WageIndex` model, suggesting it's a separate reference table that gets joined.

#### 3. Incremental Delivery Strategy

**Current Plan:** Finish all adapters → persist → enrich → expose

**Alternative (Lower Risk):**
1. **Phase 1:** Implement Addendum A/B parsing (core data)
   - Parse real files
   - Load to `fee_opps` table
   - Basic API endpoint works
   
2. **Phase 2:** Add wage index enrichment
   - Load wage index reference data (separate task?)
   - Implement `_enrich_with_wage_index()` logic
   - Persist enriched rates

3. **Phase 3:** API enhancements
   - Update router responses
   - Add facility vs non-facility scenarios

**Rationale:** Allows incremental validation, reduces risk of big-bang failure.

#### 4. Database Schema Consideration

**Plan Says:** "Persist wage-index reference data with natural-key constraints"

**Check Needed:** Does `WageIndex` model already have:
- Natural key constraints defined?
- Provenance columns (release_id, batch_id) from Phase 2?
- Proper indexes for joins?

**Action:** Verify migration exists for `wage_index` table or needs creation.

#### 5. Testing Strategy Enhancement

**Plan Includes:** Parquet goldens, regression tests for facility vs non-facility

**Additional Considerations:**
- Test wage index joins with missing CBSA (graceful degradation)
- Test enrichment with partial wage index data
- Verify provenance flows through enrichment pipeline
- Test API responses include wage-adjusted rates in `datasets_used`

### ⚠️ Potential Issues

1. **Scope Creep Risk:** Plan combines parsing + enrichment + API changes. Consider phasing.
2. **Data Dependency:** Wage index data might not be available when OPPS files are. Plan for async loading.
3. **Missing Parser Contracts:** Addendum A/B parsing should follow existing parser patterns (like GPCI parser). Plan doesn't reference parser contracts.
4. **Addendum File Format:** Need to verify actual CMS file formats (fixed-width? CSV? Excel?) before implementing parsers.

### 📋 Enhanced Implementation Sequence

**Phase 1: Core OPPS Data (Week 1)**
1. Implement `_parse_addendum_a()` - APC payment rates
2. Implement `_parse_addendum_b()` - HCPCS crosswalk
3. Load to `fee_opps` table with provenance
4. Basic API endpoint returns OPPS rates (without wage adjustment)
5. Add parquet golden tests

**Phase 2: Wage Index Foundation (Week 2)**
1. Clarify wage index data source (Addendum C? IPPS? Separate?)
2. Implement wage index parser/loader (or reuse existing)
3. Load to `wage_index` table with natural keys
4. Verify database constraints and indexes

**Phase 3: Enrichment (Week 2-3)**
1. Implement `_load_wage_index_data()` to query database
2. Implement `_enrich_with_wage_index()` join logic
3. Persist enriched rates to `opps_rates_enriched` table
4. Update enrich stage to call enrichment

**Phase 4: API & Testing (Week 3)**
1. Update router to use enriched rates
2. Add facility vs non-facility test scenarios
3. Verify provenance in API responses
4. Contract tests for ClearBill comparisons

**Phase 5: Quality Gates (Week 3)**
1. Parquet regression tests
2. CI integration
3. Documentation updates

---

## Cross-Cutting Recommendations

### 1. Provenance Consistency

Both issues should ensure:
- `release_id` and `batch_id` populated for all new data
- API responses include provenance in `datasets_used`
- ClearBill readiness checks verify provenance presence

### 2. Documentation Updates

Update these files as implementation progresses:
- `prds/RUN-render-deployment-prd-v1.0.md` - GPCI loading steps
- `prds/CMS_Pricing_API_Readiness_Plan_for_Cle.md` - OPPS enrichment status
- `INGESTION_GUIDE.md` - OPPS ingestion workflow

### 3. Testing Pyramid

- **Unit:** Parser logic, enrichment functions
- **Integration:** Full ingestion pipeline with database
- **E2E:** API responses with real data
- **CI:** Containerized Render clone for GPCI loading

### 4. Rollback Strategy

Plan should include:
- How to rollback if GPCI load fails mid-process
- How to handle partial OPPS enrichment if wage index missing
- Database migration rollback procedures

---

## Priority Recommendations

### Must-Have (Before Starting)
1. ✅ Verify segfault actually occurs on Render (might be local-only)
2. ✅ Clarify wage index data source for OPPS
3. ✅ Check if wage_index table migration exists

### Should-Have (Early)
1. 🔄 Incremental delivery for OPPS (parse → enrich → API)
2. 🔄 Test provenance flows through both pipelines
3. 🔄 Document data dependencies (wage index availability)

### Nice-to-Have (Can Defer)
1. ⏸ CI smoke tests (add after initial success)
2. ⏸ Extensive regression tests (add incrementally)

---

## Final Verdict

**Overall:** Strong plan with good coverage ✅  
**Main Enhancement:** Sequence optimization - test Render environment first before fixing  
**Risk Mitigation:** Consider incremental delivery for OPPS to reduce big-bang risk  

**Recommended Next Steps:**
1. Quick test: Try Render One-Off Job for GPCI loading
2. Research: Clarify wage index data source for OPPS
3. Plan: Break OPPS work into 2-3 phases
4. Execute: Start with core parsing, then add enrichment

**Confidence Level:** High that plan will succeed with these refinements 🎯

