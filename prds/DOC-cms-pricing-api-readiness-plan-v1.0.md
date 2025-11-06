# CMS_Pricing_API_Readiness_Plan_for_ClearBill-v1.0.md


doc_type: DOC  
normative: false  
requires:  
  - STD-doc-governance-prd-v1.0.2#overview
  - PRD-clearbill-prd-v1.0#0-overview

**Status:** Draft v1.0  
**Owners:** Product Operations  
**Consumers:** Engineering, Product, Compliance  
**Change control:** ADR + PR Review  

---

## 0. Overview
This document defines the readiness plan to bring the CMS Pricing API to an application-ready state for ClearBill (consumer app, advocate dashboards, provider validation tools, external developer integrations).  
The plan is structured around four mutually exclusive and collectively exhaustive pillars—**Data Quality & Provenance, API Contract & Clients, Access & Compliance, Operability & Support**—so each capability carries a single owner, stage gate, and validation path.

**Registration:** This plan will be registered in `DOC-master-catalog-prd-v1.0.md` under Product Docs.

---

## 1. Readiness Pillars & Scope
| Pillar | In scope (v1.0) | Out of scope (v1.0) |
|---|---|---|
| Data Quality & Provenance | MPFS locality fixes; OPPS wage index ingestion; dataset snapshot registry; provenance columns; retention/backfill runbooks | Non-CMS rate blends; predictive pricing models |
| API Contract & Clients | Unified `CodePricingItem` schema; quarter validation; OpenAPI updates; client contract tests; downstream comms | Real-time EOB integrations; LLM or conversational features |
| Access & Compliance | DB-backed API keys & scopes; audit trails; correlation IDs; compliance bill-of-materials | Patient identity management; PHI storage changes |
| Operability & Support | Caching strategy; latency SLOs; dashboards; on-call training; deployment runbooks; cache warmers | Dedicated site reliability automation beyond ClearBill launch scope |

---

## 2. Objectives, KRs & Acceptance Evidence
| Pillar | Key Results | Acceptance Evidence |
|---|---|---|
| Data Quality & Provenance | 100% engine tests pass for locality/quarter; OPPS wage index applied where applicable; every response includes `datasets_used` with `release_id` + `dataset_digest` | Alembic migrations applied with verified backfills; synthetic publish exercise populates provenance; QA reports confirm deterministic snapshot selection |
| API Contract & Clients | All pricing/list routers emit `CodePricingItem`; invalid quarter rejected with HTTP 400; ClearBill client SDKs compile against new schema | Contract test suite green for `/pricing/*` and list endpoints; dual-write telemetry shows zero missing fields; release notes delivered to client teams |
| Access & Compliance | RBAC scopes enforced on every router; per-key metrics live; compliance BOM and audit logs approved | Pen-tests and negative scope tests pass; compliance sign-off of HIPAA safeguards; KMS audit demonstrates secrets rotation policy |
| Operability & Support | p95 < 500 ms single-code lookup; p95 < 2.5 s 40-code batch; dashboards operational; on-call playbook and training completed | Load-test report signed; Grafana boards published with alert thresholds; dry-run of snapshot activation/rollback recorded |

---

## 3. Dependencies & Consumers
**Primary consumers:** ClearBill App backend, Advocate Dashboard, Provider validation tools, external developer integrations.  
**Internal dependencies:** MPFS/OPPS ingestion pipelines (ASC/CLFS/DMEPOS/IPPS/ASP/NADAC pipelines scheduled post-launch), parquet publishers, relational loaders, Redis, Postgres, Prometheus/Grafana, API Gateway, authentication middleware, compliance review board.

---

## 4. Milestones & Stage Gates
| Target Quarter | Pillar Phase | Entry Criteria | Exit Evidence |
|---|---|---|---|
| Q1 | Data Quality & Provenance — Correctness Hardening | Feature work approved, migrations ready, synthetic dataset fixtures prepared | Engine unit/integration suites green; provenance columns populated; retention/backfill runbook reviewed |
| Q1 | API Contract & Clients — Schema Stabilization | `CodePricingItem` draft validated with client teams; OpenAPI branch cut | Canary responses dual-write old/new schema; contract tests pass for ClearBill client SDKs |
| Q2 | Access & Compliance — Secure Access Launch | RBAC schema migrated; middleware feature-flagged; audit log fields defined | External keys rotated; failed-auth smoke tests captured; compliance BOM and attestation package approved |
| Q2 | Operability & Support — Performance & Support Readiness | Redis + dashboards provisioned; load-test scripts configured | Load-test report within SLO; cache warmers scheduled; on-call training completed; pager escalation verified |
| Q2 | Cross-cutting Enablement — Documentation & Governance | Pillar exit artifacts collected; doc owners ready | PRD + Master Catalog updated; rollout checklist approved; change advisory meeting minutes archived |

---

## 5. Workstreams & Deliverables by Pillar

### 5.1 Data Quality & Provenance
- **Data model & migrations**
  - Add `locality_id` to `fee_mpfs`; backfill from RVU locality dim (`alembic/versions/<mpfs_locality_id>.py`).
  - ✅ **COMPLETE:** Append `release_id`, `batch_id`, `dataset_digest` columns to `fee_mpfs`, `fee_opps`, `fee_asc`; index on `(dataset_id, release_id)`. (Phase 2.1 - Migration `8d80f393d0ee`)
  - ✅ **COMPLETE:** Create `dataset_snapshots(dataset_id, release_id, digest, effective_from, effective_to, manifest_url)` with uniqueness on `(dataset_id, release_id)`. (Quick Win #1 - Migration `98567c0bbfa8`)
- **Ingestion & publishers**
  - ✅ **COMPLETE:** Update MPFS and OPPS publishers to populate new provenance fields and enforce natural keys (HCPCS, locality, effective date). (Phase 2.4 - Updated `load_data.py`, `rvu_ingestor.py`, `opps_ingestor.py`)
  - Extend OPPS ingestion to persist wage index table with NK constraints; add facility-specific joins in engines (`cms_pricing/engines/opps.py`).
  - Produce retention/backfill playbook covering re-runs, digest reconciliation, and abort criteria.
- **Snapshot publication**
  - ✅ **COMPLETE:** RVU ingestor registers curated snapshots (`rvu_items`, `gpci_indices`, locality, anescf, oppscap) during publish stage; runbook updated with verification steps.
  - ✅ **COMPLETE:** MPFS ingestor registers curated payment snapshots (`mpfs_payment_curated`, `mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, `mpfs_indicators_all`, `mpfs_locality`, `mpfs_link_keys`) with SHA256 digests for provenance.
  - Capture post-run evidence: RVU & MPFS snapshot check output, manifest path, curated row counts.
- **Snapshot selection & response provenance**
  - ✅ **COMPLETE:** Centralize snapshot selection in `cms_pricing/services/pricing.py` with deterministic fallbacks and alert hooks. (Quick Win #1 - `DatasetSnapshotService.select_snapshot()`, integrated into `PricingService._collect_datasets_used()`)
  - ✅ **COMPLETE:** Ensure `datasets_used` accumulates `dataset_id`, `release_id`, `dataset_digest`, `effective_from`, `effective_to` for every engine response. (Phase 2.6 - `_collect_datasets_used()` queries `DatasetSnapshot` table, falls back to extracting from `trace_refs`)
  - Add synthetic publish integration test: publish fixture manifests → run `/pricing/price` calls → verify cents + provenance metadata.
- **Quality safeguards**
  - ✅ **COMPLETE:** Extend unit tests for locality and quarter selection (MPFS, OPPS, ASC). (Phase 2.7 - Golden tests updated with provenance validation)
  - ✅ **COMPLETE:** MPFS ingestion contract tests verify `/pricing/price` endpoint includes `datasets_used` with `mpfs_cf`, `mpfs_rvu`, `mpfs_gpci` provenance metadata. (Phase 6.3 - Contract tests in `tests/api/test_golden.py` and `tests/services/test_pricing_provenance.py`)
  - Add wage index coverage test verifying indexed and non-indexed states.
  - Instrument checksum verification and failure alerts during ingestion jobs.
- **Quick Win #1: Dataset Snapshots Table (Complete)**
  - ✅ Created `dataset_snapshots` table via Alembic migration `98567c0bbfa8` with composite primary key `(dataset_id, release_id)`
  - ✅ Implemented `DatasetSnapshot` SQLAlchemy model with indexes for efficient queries
  - ✅ Built `DatasetSnapshotService` with `select_snapshot()` method for deterministic snapshot selection based on valuation date
  - ✅ Created registration script (`scripts/register_dataset_snapshots.py`) for batch snapshot registration from fee schedule tables
  - ✅ Added `/snapshots/health` endpoint for snapshot registry visibility
  - ✅ Integrated snapshot selection into `PricingService._collect_datasets_used()` with fallback to trace_refs extraction

### 5.2 API Contract & Clients
- **Unified wire schema**
  - ✅ **COMPLETE:** Define Pydantic `CodePricingItem` with shared serializer; update MPFS/OPPS/ASC routers plus `/pricing/price`, `/pricing/compare`. (Quick Win #2 - All 7 engines return `CodePricingItem`, `/pricing/codes/price` returns `CodePricingItemWithGeography`)
  - ✅ **COMPLETE:** Provide compatibility adapter for legacy clients while dual-running. (Quick Win #2 - `LineItemResponse.from_code_pricing_item()` adapter, `CodePricingItem.from_dict()` class method)
- **Input validation & error handling**
  - ✅ **COMPLETE:** Enforce quarter ∈ {1,2,3,4} at router layer; return `400` with actionable error codes. (Phase 2 - Quarter validation in `PricingRequest`, `ComparisonRequest`, and all router endpoints)
  - Expand negative test coverage for invalid locality, unsupported modifiers, and missing snapshot.
- **Client integration readiness**
  - ✅ **COMPLETE:** Update OpenAPI spec, SDKs, and snippet docs; publish breaking-change notice. (Quick Win #2 - All endpoints use unified schema, OpenAPI docs updated)
  - Stand up contract tests executed against ClearBill staging environment.
  - Establish downstream regression suite (consumer-driven contract or Postman collection) executed before release.
- **Documentation & comms**
  - Produce versioned API change log; include schema diffs.
  - Coordinate client enablement guide with product marketing and developer relations.
- **Phase 2.8: Provenance Documentation (Complete)**
  - ✅ Document provenance fields in OpenAPI schema (`datasets_used`, `trace_refs`) - Enhanced Field descriptions with structure, format, and examples
  - ✅ Update endpoint docstrings with provenance format documentation - Added provenance sections to `/pricing/price`, `/pricing/compare`, `/pricing/codes/price`
  - ✅ Verify OpenAPI schema exports correctly - Validated `/openapi.json` and `/docs` render provenance documentation
  - 📋 Publish standalone API reference guide with provenance examples (optional enhancement)
  - 📋 Update client SDK examples to demonstrate provenance parsing (client-facing documentation)
- **Quick Win #2: Unified CodePricingItem Schema (Complete)**
  - ✅ `CodePricingItem` Pydantic model defined with all common fields and provenance metadata
  - ✅ All 7 pricing engines (MPFS, OPPS, ASC, CLFS, DMEPOS, IPPS, Drugs) return `CodePricingItem` *(ASC/CLFS/DMEPOS/IPPS/Drug datasets continue to source from post-launch ingestion backlog)*
  - ✅ `CodePricingItemWithGeography` subclasses `CodePricingItem` for `/pricing/codes/price` endpoint
  - ✅ Service layer updated to handle `CodePricingItem` throughout
  - ✅ Compatibility adapters ensure backward compatibility for plan pricing
  - ✅ Comprehensive test coverage including integration tests for MPFS, OPPS, and Drug engines

### 5.3 Access & Compliance
- **API key platform**
  - Create `api_keys` table with salted hash, scopes, tenant, attribution fields; schedule rotation job.
  - Implement middleware enforcing scope per router (`cms_pricing/middleware.py`, `cms_pricing/routers/*`).
  - Emit per-key metrics (`requests_total{scope}`, `requests_4xx_total`, `requests_rate_limited_total}`).
- **Audit & compliance artifacts**
  - Route correlation IDs through ingestion and pricing services; ensure no PHI persists outside encrypted stores.
  - Record compliance bill-of-materials (systems, datasets, controls) aligning to HIPAA §164.308/§164.312.
  - Define data retention & deletion policy for pricing snapshots and logs.
- **Security operations**
  - Configure alerting on repeated auth failures, scope escalation attempts.
  - Capture audit log sampling review; provide compliance attestation package.
  - Partner with Compliance for pre-launch tabletop covering incident response.

### 5.4 Operability & Support
- **Caching & performance**
  - Implement L1 in-process cache and L2 Redis cache keyed by `(engine|code|setting|locality|snapshot_digest)`.
  - Develop nightly cache warmers seeded from top ClearBill demand cohorts.
  - Run load tests (single lookup, 20-code, 40-code) with targets p95 < 500 ms / < 2.5 s.
  - ✅ **COMPLETE:** Engine performance optimizations - Session management for connection reuse, column selection via `with_entities()` to reduce memory/network overhead, reusable filter helpers for common query patterns. (2025-01-15 optimization pass)
- **Observability & alerting**
  - Expose metrics: `dataset_snapshot_selected_total`, `pricing_lookup_latency_ms`, `cache_hits_total`/`cache_misses_total`, `requests_total{route, scope}`.
  - Publish Grafana dashboards (latency, cache hit ratio, error budgets, snapshot adoption).
  - Configure alerts for SLO burn, snapshot mismatch, cache miss spikes, ingestion drift.
- **Operational readiness**
  - Author runbooks: snapshot activation/rollback, cache warmer failures, RBAC issue remediation.
  - Deliver on-call training session with recordings and scenario drills; update paging escalation list.
  - Prepare deployment checklist covering feature flag sequencing, dual-write window, and rollback triggers.

---

## 6. Cross-cutting Enablement & Governance
- Update `PRD-clearbill-prd-v1.0.md`, `DOC-master-catalog-prd-v1.0.md`, and associated runbooks once each pillar exits its gate.
- Maintain change log entries and ADR links for schema, security, and infrastructure decisions.
- Schedule release readiness reviews (R2) with product, engineering, compliance, and support; capture minutes in Confluence.
- Coordinate communications plan (internal release notes, partner updates, developer newsletter).

---

## 7. Traceability Matrix
| Pillar Capability | Validation / Test Artifact | Owner |
|---|---|---|
| Data Quality & Provenance — MPFS locality & quarter correctness | `pytest tests/engines/test_mpfs_engine.py::test_locality_selection`; `pytest tests/engines/test_opps_engine.py::test_quarter_filters` | Data Engineering |
| Data Quality & Provenance — Provenance metadata | Synthetic publish CI job + `tests/services/test_pricing_provenance.py` | Data Engineering |
| API Contract & Clients — `CodePricingItem` schema stability | Contract tests (`tests/contracts/test_code_pricing_item.py`) + SDK compilation check | Platform API |
| API Contract & Clients — Error handling | Postman regression suite verifying 400s and error codes | Platform API |
| Access & Compliance — RBAC enforcement | `pytest tests/auth/test_rbac_scopes.py` + penetration test report | Security Engineering |
| Access & Compliance — Audit trail completeness | Quarterly audit review checklist + log sampling report | Compliance |
| Operability & Support — Performance SLOs | Locust load-test report + Grafana snapshot | SRE |
| Operability & Support — Runbook readiness | Dry-run recording + sign-off template archived in Ops wiki | Support Operations |

---

## 8. Risks & Mitigations
| Pillar | Risk | Mitigation |
|---|---|---|
| Data Quality & Provenance | Snapshot mis-selection returns stale rates | Deterministic selector with alert on missing snapshot; manual override documented |
| Data Quality & Provenance | Wage index gaps due to CMS data drift | Pre-publish validation; fail-closed switch surfaces gap to API clients |
| API Contract & Clients | Client apps break on schema switch | Dual-write comparison, feature flags, coordinated rollout with SDK update |
| Access & Compliance | Over-permissive scopes or leaked key | Default-deny posture, rotation checklist, anomaly detection on per-key metrics |
| Access & Compliance | Compliance evidence incomplete at launch | Dedicated BOM artifact, mapped controls, scheduled audit sign-off |
| Operability & Support | Cache stampede or Redis outage | Request coalescing, TTL jitter, pre-warmers, documented cache bypass runbook |
| Operability & Support | On-call team unprepared for incident | Training drills, pager rehearsal, clear escalation tree |

---

## 9. Rollout Sequence & Evidence
1. **Data Quality & Provenance gate** — migrations merged, backfills executed, synthetic publish + provenance tests green, retention runbook signed, RVU + MPFS production ingest evidence (manifests, snapshot checks, curated row counts) archived.
2. **API Contract & Clients gate** — dual-write telemetry clean, contract tests green, client enablement package delivered, OpenAPI version tagged.
3. **Access & Compliance gate** — RBAC enforced in staging, failed-auth tests recorded, compliance BOM + tabletop results approved.
4. **Operability & Support gate** — load-test SLOs met, dashboards live with alerts, on-call training recorded, runbooks published.
5. **GA readiness** — all pillar evidence archived, rollout checklist signed, 14-day canary metrics within thresholds, compliance and product sign-off complete.

---

## 10. Change Log
| Date | Version | Author | Summary |
|---|---|---|---|
| 2025-10-31 | v1.0 (Draft) | Product Operations | Initial readiness plan for CMS Pricing API to support ClearBill app, including pillar structure, migrations, schema standardization, RBAC, caching, testing, and rollout. |
| 2025-10-31 | v1.1 | Engineering | Phase 2.7 (Testing) complete: Added provenance validation to golden tests, graceful database skipping, unit tests for provenance extraction. Phase 2.8 (Documentation) complete: Updated OpenAPI schemas with provenance field documentation (`datasets_used`, `trace_refs`), enhanced router docstrings for all pricing endpoints, validated OpenAPI schema export. |
| 2025-01-15 | v1.2 | Engineering | **Quick Win #1 (Dataset Snapshots Table) Complete:** Created `dataset_snapshots` table (migration `98567c0bbfa8`), `DatasetSnapshot` model, `DatasetSnapshotService` with snapshot selection logic, registration script (`scripts/register_dataset_snapshots.py`), and `/snapshots/health` endpoint. **Quick Win #2 (Unified CodePricingItem Schema) Complete:** Defined `CodePricingItem` Pydantic model, updated all 7 engines to return `CodePricingItem`, created `CodePricingItemWithGeography` subclass, updated service layer and routers, added compatibility adapters. **Engine Performance Optimizations:** Implemented session management (connection reuse), column selection via `with_entities()`, reusable filter helpers, and comprehensive test coverage including DrugEngine integration test. |
