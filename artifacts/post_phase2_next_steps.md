# Post-Phase 2: Next Steps

**Phase 2 Status:** ✅ COMPLETE (pending E2E validation with test data)  
**Date:** 2025-10-31

---

## Overview

Phase 2 successfully implemented provenance tracking. The next priorities come from the **CMS Pricing API Readiness Plan** which organizes work into four pillars.

---

## Immediate Next Steps (Priority Order)

### 1. Complete Remaining Data Quality & Provenance Items

**Status:** Phase 2 provenance columns ✅, but some related work remains

#### Outstanding Items:
- [ ] **Dataset Snapshots Table** (Section 5.1)
  - Create `dataset_snapshots(dataset_id, release_id, digest, effective_from, effective_to, manifest_url)`
  - Uniqueness on `(dataset_id, release_id)`
  - Purpose: Registry of available dataset versions for deterministic selection

- [ ] **Snapshot Selection Logic** (Section 5.1)
  - Centralize in `cms_pricing/services/pricing.py`
  - Deterministic fallbacks
  - Alert hooks for missing snapshots

- [ ] **Retention/Backfill Playbook** (Section 5.1)
  - Document re-run procedures
  - Digest reconciliation
  - Abort criteria

- [ ] **MPFS Locality Backfill** (Section 5.1)
  - Add `locality_id` to `fee_mpfs` (if not already done)
  - Backfill from RVU locality dimension

#### Estimated Effort: 1-2 days

---

### 2. API Contract & Clients (Section 5.2)

**Goal:** Standardize API responses and enable ClearBill client integration

#### Key Tasks:
- [ ] **Unified Wire Schema**
  - Define `CodePricingItem` Pydantic model
  - Update all pricing endpoints to use it
  - Compatibility adapter for legacy clients

- [ ] **Client Integration Readiness**
  - Contract tests against ClearBill staging
  - SDK examples with provenance parsing
  - Breaking-change notice (if needed)

- [ ] **Error Handling Enhancement**
  - Expand negative test coverage
  - Invalid locality handling
  - Missing snapshot handling

#### Estimated Effort: 2-3 days

**Note:** Quarter validation is already complete (Phase 2 work).

---

### 3. Access & Compliance (Section 5.3)

**Goal:** Secure API access with RBAC and audit trails

#### Key Tasks:
- [ ] **API Key Platform**
  - Create `api_keys` table (salted hash, scopes, tenant)
  - Scope-based middleware enforcement
  - Per-key metrics (`requests_total{scope}`, etc.)

- [ ] **Audit & Compliance**
  - Correlation ID routing
  - Compliance BOM (HIPAA §164.308/§164.312)
  - Data retention & deletion policy

- [ ] **Security Operations**
  - Alerting on auth failures
  - Audit log sampling
  - Compliance attestation package

#### Estimated Effort: 3-5 days

---

### 4. Operability & Support (Section 5.4)

**Goal:** Performance, caching, monitoring, and operational readiness

#### Key Tasks:
- [ ] **Caching Strategy**
  - L1 in-process cache (LRU)
  - L2 Redis cache
  - Cache warmers for top cohorts

- [ ] **Performance Optimization**
  - Load tests (single lookup < 500ms p95, 40-code batch < 2.5s p95)
  - Query optimization
  - Connection pooling tuning

- [ ] **Observability**
  - Prometheus metrics expansion
  - Grafana dashboards
  - Alert configuration (SLO burn, cache misses, etc.)

- [ ] **Operational Readiness**
  - Runbooks (snapshot rollback, cache failures, RBAC issues)
  - On-call training
  - Deployment checklist

#### Estimated Effort: 4-6 days

---

## Recommended Sequencing

### Option A: Complete Data Quality First (Recommended)
**Focus:** Finish the Data Quality & Provenance pillar before moving to other pillars.

1. **Week 1:** Dataset snapshots table + selection logic (1-2 days)
2. **Week 1:** Retention/backfill playbook (0.5 days)
3. **Week 1:** End-to-end provenance validation with test data (1 day)
4. **Week 2:** Move to API Contract & Clients

**Rationale:** Completes the provenance story end-to-end before building client integrations.

### Option B: Parallel Workstreams
**Focus:** Multiple pillars in parallel (different team members).

1. **Team 1:** Complete Data Quality (snapshots, selection logic)
2. **Team 2:** Start API Contract (unified schema, contract tests)
3. **Team 3:** Begin Access & Compliance (API keys table, middleware)

**Rationale:** Faster overall progress, but requires coordination.

### Option C: Client-First Approach
**Focus:** Enable ClearBill integration ASAP, then complete provenance.

1. **Week 1:** API Contract & Clients (unified schema, contract tests)
2. **Week 2:** Access & Compliance (basic RBAC for ClearBill)
3. **Week 3:** Complete Data Quality (snapshots)
4. **Week 4:** Operability & Support

**Rationale:** Unblocks ClearBill team faster, provenance can be completed in parallel.

---

## Critical Path Dependencies

**Before ClearBill Integration:**
- ✅ Provenance tracking (Phase 2) - DONE
- ⏳ Unified API schema (`CodePricingItem`)
- ⏳ Basic RBAC (API keys with scopes)

**Before Production Launch:**
- ✅ Provenance tracking (Phase 2) - DONE
- ⏳ Dataset snapshots table
- ⏳ Performance SLOs met (p95 < 500ms)
- ⏳ Monitoring dashboards
- ⏳ On-call runbooks

---

## Quick Wins (Low Effort, High Value)

1. **Dataset Snapshots Table** (4-6 hours)
   - Simple table creation
   - Clear value for provenance selection
   - Unblocks snapshot selection logic

2. **Unified `CodePricingItem` Schema** (6-8 hours)
   - Single Pydantic model
   - Update 3-4 endpoints
   - Clear value for ClearBill team

3. **Basic API Keys Table** (6-8 hours)
   - Simple auth table
   - Middleware integration
   - Enables multi-tenant support

---

## Long-Term Roadmap (Beyond Readiness Plan)

From README.md and PRDs:

### v1 Features (Next)
- 🔄 MRF facility pricing with caching
- 🔄 NCCI edit enforcement
- 🔄 Enhanced IPPS add-ons (IME/DSH/outlier)
- 🔄 Anesthesia base+time calculations

### v2 Features (Future)
- 🔄 Payer TiC MRF parsing
- 🔄 State Medicaid fee schedules
- 🔄 Advanced scenario modeling
- 🔄 Cohort-weighted pricing mixes

---

## Decision Points

**Which path to take?**
- **If ClearBill needs API access soon:** Option C (Client-First)
- **If you want complete provenance first:** Option A (Complete Data Quality)
- **If you have multiple engineers:** Option B (Parallel Workstreams)

**What's the most critical blocker?**
- **API contract standardization** → Start with Section 5.2
- **Security/compliance requirements** → Start with Section 5.3
- **Performance concerns** → Start with Section 5.4
- **Completing provenance story** → Start with remaining Section 5.1 items

---

## Summary

**Phase 2 Complete:** ✅ Provenance tracking implemented

**Next Priority:** Choose one:
1. **Complete Data Quality** - Finish dataset snapshots + selection logic
2. **Enable ClearBill** - Unified schema + basic RBAC
3. **Operational Readiness** - Caching + performance + monitoring

All paths converge toward the same readiness gates outlined in Section 9 of the readiness plan.

