# Compliance & Licensing (call‑out)
- **Code sets:** We do **not** distribute CPT®. We ship only HCPCS/DRG/APC and other non‑CPT code sets.
- **User‑supplied CPT:** Allowed only as input; optional private crosswalk to HCPCS can be enabled behind a flag. No redistribution.
- **Data provenance:** Every dataset used in pricing is recorded in trace with **dataset_id**, **digest (SHA256)**, **effective_from/to**, and **selection_reason**.
- **PHI/PII:** Service is designed to process no PHI. Logs/trace redact user identifiers; API key auth required.
- **Licenses:** CMS/HRSA/HUD datasets used under their public terms; hospital/payer MRFs under transparency rules.

# PRD — CMS Treatment Plan Pricing & Comparison API (v0.1)

**Status:** Draft v0.1  
**Owners:** Pricing Platform Product & Engineering  
**Consumers:** API Engineering, Analytics, QA, Ops  
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **STD-api-contract-management-prd-v1.0:** API contract management and versioning
- **STD-api-security-and-auth-prd-v1.0:** Security requirements for treatment plan API
- **STD-api-architecture-prd-v1.0:** API architecture and layering patterns

**Owner:** <you>  
**Author:** ChatGPT  
**Date:** Sept 17, 2025  
**Status:** Draft (for review)

---

## 1) Summary
Build a Python-based API that produces **ZIP-level, episode-based** treatment plan prices using **Medicare Fee‑For‑Service** as the baseline, with a roadmap to add **facility-specific negotiated prices (hospital MRFs)** and **commercial/Medicaid** comparators. The service must output **granular line items** (professional + facility, per setting), **beneficiary cost sharing**, and an **auditable trace** of sources, formulas, geography mapping, and assumptions. It must support **A vs. B location comparisons** with enforced apples‑to‑apples rules.

---

## 2) Goals & Non‑Goals
**Goals**
- Price complete **treatment plans** (bundles of CPT/HCPCS/DRG + assumptions) by **ZIP**, **year**, and **optional quarter**.
- Cover all major **settings**: Professional (MPFS), HOPD (OPPS), ASC, Inpatient (IPPS), Clinical Lab (CLFS), DMEPOS, Part B drugs (ASP), retail drugs (NADAC reference).
- Allow **facility-specific** overrides when a **CCN** is provided; otherwise use benchmarks.
- Compute **Medicare allowed amount** + **beneficiary deductible/coinsurance** (line-item & totals).
- Provide **traceability**: dataset versions, formulas, geo decisions, and unit conversions per run.
- Handle **ZIP→locality/CBSA** ambiguity by returning **candidates** and a deterministic default.
- Enable **comparisons** between locations with strict snapshot/assumption parity.

**Non‑Goals (initial)**
- Full claims adjudication; payer policies beyond CMS baseline.
- State-by-state Medicaid coverage in MVP (behind feature flag for later).
- Comprehensive ingestion of all hospital/payer MRFs (feature-flagged, targeted use only initially).

---

## 3) Users & Use Cases
**Personas**
- *Health Econ/Strategy*: compare service lines across markets, build models for expansion.
- *Provider Ops*: benchmark facility charges vs. Medicare baseline and local competitors.
- *Payer Contracting*: cross‑check negotiated rates vs. Medicare and regional norms.

**Primary Use Cases**
1. **Price a plan by ZIP** (e.g., outpatient TKA in 94110, CY2025 Q3).  
2. **Compare Location A vs. B** for the same plan and valuation date (e.g., 94110 vs. 73301; with/without CCN).  
3. **Itemize drugs**: show Part B allowed vs. NADAC reference and optional provider-entered NDC costs.  
4. **Toggle post‑acute** (HH/SNF) and observe effect on totals.  
5. **Audit a run**: reproduce prices and prove data provenance to stakeholders.

---

## 4) Success Metrics
- **Accuracy:** ≤1% deviation vs. CMS calculators for targeted test set (unit tests per code/setting).
- **Comparability:** 100% of comparison responses must state snapshot parity & post‑acute flags.
- **Freshness:** Datasets updated within SLA of upstream releases (e.g., ASP quarterly, OPPS quarterly).
- **Performance:** p95 < 1.5s for pricing a 10‑line plan without MRF; p95 < 3.0s with cached MRF slice.
- **Auditability:** 100% of responses include a complete **trace** with dataset digests and formulas.

---

## 5) Functional Requirements
### 5.1 Plans & Components
- CRUD for **plans** with components: `(code, setting, units, utilization_weight, professional_component, facility_component, modifiers[], pos?, wastage_units?)`.
- Episode windows: **surgical** uses **global periods (000/010/090)**; **conditions** default **30 days**, configurable **up to 180**.

### 5.2 Pricing Engine by Setting
- **Professional (MPFS):** Compute allowed = `(Work×GPCIw + PE×GPCIpe + MP×GPCImp) × CF` with **POS** resolved in this order: (1) `plan_components.pos` if provided; (2) infer by setting (HOPD→22, ASC→24, Office→11); (3) default to **facility** with a **trace warning**. Optional strict mode: **require POS** for MPFS office/clinic lines (400 `POS_REQUIRED`).  
- **HOPD (OPPS):** Addendum B + wage index by **CBSA**; implement **packaging**: **J1** comprehensive packaging, **N** always packaged, **Q1/Q2/Q3** conditionally packaged **if a J1 primary exists in the same request** (heuristic claim). Professional read via MPFS when flagged.
- **ASC:** Facility via ASC schedule; if `professional_component=true`, price professional via **MPFS (POS 24)**.
- **Inpatient (IPPS):** Baseline = `DRG_weight × ((operating_base × WI) + (capital_base × WI))`; **no IME/DSH/outlier** in MVP.
- **CLFS:** National fee schedule (quarterly updates; respect OPPS packaging when priced within HOPD episodes).
- **DMEPOS:** Fee schedule; **rural** uses official **CMS rural ZIP list** (table `dmepos_rural_zip`), with a **heuristic fallback** (non‑CBSA) + **trace warning**.
- **Drugs:** 
  - **Part B (ASP):** `ASP × 1.06 × units` (no wastage in totals).  
  - **NADAC** reference by NDC; convert units via **NDC↔HCPCS** when NDC provided.  
  - Support `wastage_units` **as reference only** (traced, excluded from totals in MVP).

### 5.3 Modifiers (MVP scope)
- **-26 / -TC** component splits as applicable.  
- **-50 bilateral:** apply **150%** of unilateral **before** multiple‑procedure ranking (full bilateral indicator table in roadmap).  
- **-51 multiple procedures:** apply standard discounting order (stub in MVP; full family rules in roadmap).  
- **-59 / X{E,P,S,U}:** treated as distinct for edit/packaging separation (no denial logic in MVP).

### 5.4 Geography
- **ZIP→locality/CBSA** resolver returns **all candidates** with `used` flag and an ambiguity indicator.
- `is_rural_dmepos` derived from **official list first**, else heuristic as fallback with trace note.

#### 5.4.1 Geography Fallback Policy (comparisons)
- **Default:** Use the **same comparison ZIP** for all providers (apples‑to‑apples).  
- **Fallback:** If the same ZIP is not usable for a provider, fall back to that provider’s **service ZIP**.
- **Expansion:** If the provider service ZIP is unavailable or outside radius, expand search **stepwise** (25→50→75→100 miles; capped at **100 miles**, all thresholds configurable) to the **nearest usable ZIP**.
- **Trace:** Always emit a note when fallback or expansion occurs, including chosen ZIP and distance.

### 5.5 Versioning, Snapshots & Trace
- All requests accept `year` + optional `quarter` and optional `snapshot_digest`.
- **Temporal Resolution (per dataset/source):**
  1) If `snapshot_digest` is provided → **use that snapshot** (hard‑fail if missing).
  2) Else if `quarter` is provided → choose the **latest effective version ≤ end of that quarter**.
  3) Else → choose the **latest effective version ≤ valuation date (year‑end)**. If none exists, step back: **prior quarter → prior year → nearest earlier effective**.
  4) For **provider‑specific sources** (MRF/TiC/CSV): prefer a slice whose **effective window covers the valuation date**; else apply the same step‑back logic.
  5) **Trace** each dataset’s `chosen_effective_from/to`, selection **reason** (digest/quarter/latest), and any **stepbacks**.

### 5.6 Comparison Semantics
- Comparisons valid only when **plan_id + snapshot parity + post‑acute flags + benefit toggles** match.  
- API enforces parity and returns a **comparison block** with deltas, plus context (benchmark vs facility-specific).

### 5.7 Error Handling & Validation
- Reject inconsistent pro/facility combinations (e.g., duplicate TC + facility tech).  
- Warn on OPPS‑packaged services priced separately; bundle with parent line and set `packaged=true`.

## 6) Non‑Functional Requirements
- **Availability:** 99.9% monthly (single region acceptable MVP).  
- **Scalability:** Handle 25 concurrent pricing calls, burst to 100 with caching.  
- **Security:** No PHI/PII; public reference data + user-entered plan metadata.  
- **Observability:** Structured logs, metrics (latency, cache hit rate), tracing run IDs.  
- **Data Freshness SLAs:** MPFS annual; OPPS/ASC quarterly; ASP quarterly; NADAC weekly/monthly; CLFS/DMEPOS quarterly; IPPS annual (FY).

---

## 7) Data Sources & Cadence (authoritative)
- **MPFS** (RVUs, GPCI, CF) — annual (+ revisions A/B/C).  
- **OPPS** Addenda A/B & D1 + wage index — quarterly.  
- **ASC** addenda — annual + quarterly updates.  
- **IPPS** DRG weights, base rates, wage index — FY annual.  
- **CLFS** — quarterly.  
- **DMEPOS** — quarterly; rural status & former CBA context.  
- **ASP (Part B drugs)** — quarterly; **NDC↔HCPCS** crosswalk.  
- **NADAC** — weekly/monthly (as‑of dates).  
- **ZIP crosswalks** — ZIP→Locality, ZIP→CBSA (HUD) — periodic.  
- **Policy/edits** — NCCI quarterly; OPPS packaging rules; HCPCS quarterly.

---

## 8) Data Model (high level)
**Core tables**
- `geography(zip5, locality_id, cbsa, shares, effective_from, effective_to)`
- `provider_registry(provider_id, ccn?, npi?, tin?, name, address, city, state, zip5, lat, lon, aliases_json, service_zip5?, created_at)`
- `provider_identifiers(provider_id, id_type, id_value, confidence, source, created_at)`
- `plans(plan_id, name, description, tags, owner, created_at, updated_at)`
- `plan_components(plan_id, idx, code, setting, units, utilization_weight, professional_component, facility_component, modifiers_json, pos?, ndc11?, ndc_units?, assume_facility_component?, fallback_multiplier?, wastage_units?, label?, source_prefs_json?)`
- `codes(hcpcs, desc, status_indicators, global_days, setting_flags, effective_from, effective_to)`
- `fee_mpfs(year, locality_id, hcpcs, work_rvu, pe_nf_rvu, pe_fac_rvu, mp_rvu, cf)`
- `fee_opps(year, quarter, hcpcs, si, apc, base_rate, packaging)`
- `fee_asc(year, quarter, hcpcs, asc_rate)`
- `fee_ipps(fy, drg, weight, base_operating, base_capital, wage_index_by_cbsa)`
- `fee_clfs(year, quarter, hcpcs, fee)`
- `fee_dmepos(year, quarter, code, rural_flag, fee)`
- `drugs_asp(year, quarter, hcpcs, asp_per_unit)`
- `drugs_nadac(as_of, ndc11, unit_price)`
- `ndc_hcpcs_xwalk(ndc11, hcpcs, units_per_hcpcs)`
- `independent_provider_rates(provider_id, code, code_system, setting, unit, amount_value, amount_type, effective_from, effective_to, raw_json)`
- `snapshots(dataset_id, effective_from, effective_to, digest, source_url)`
- `runs(run_id, created_at, api_version, year, quarter, snapshot_digest?, options_json)`
- `run_inputs(run_id, jsonb)`
- `run_outputs(run_id, jsonb)`
- `run_trace(run_id, datasets_jsonb, notes_jsonb)`

## 9) API Endpoints (MVP) (MVP)
All endpoints and change management must align with the **STD-api-architecture-prd-v1.0**.
- `POST /plans` — create/update plan definition.  
- `GET /plans` — list plan summaries (cursor pagination: **default limit=20**, **max=200**, `next_page_token`).
- `GET /geography/resolve?zip=` — return locality/CBSA candidates, `requires_resolution` flag.
- `GET /codes/price?zip=&code=&setting=&year=&quarter=&ccn=&payer=&plan=&apply_sequestration=` — price a single component. If `ccn` is present, the engine first looks for a **facility-specific** rate in the active MRF for that CCN (filtered by `payer` and `plan` when provided); otherwise falls back to benchmark schedules with a trace flag. Optional `apply_sequestration=true` shows sequestration‑adjusted **program** payment on that line only.  
- `POST /price?zip=&year=&quarter=&ccn=&payer=&plan=` — price a full plan (body includes `plan` and optional `benefit_params`). Facility‑specific overrides filtered by payer/plan are applied when present. Returns a **run_id** and persists trace.  
- `POST /compare` — compare **two locations** (A vs B) for the same plan; hard parity enforcement; returns **run_id**.  
- `POST /compare/providers` — compare **N providers in one request** (default max **6**, configurable). Body includes the plan, `benefit_params`, and a providers array with `{provider_id|ccn|npi|tin|name,address, source_prefs, fallback_multiplier?}`. Returns a **matrix** (rows = components; columns = {Medicare, Medicaid, Provider1…}), per‑provider **traces**, **coverage summary**, and **winner** flags; returns **run_id**.  
- `GET /trace/{run_id}` — full provenance for prior runs (inputs, outputs, datasets, notes).

**Request body for `POST /price`**
```json
{
  "plan": { "plan_id": "...", "components": [ /* PlanComponent[] */ ] },
  "benefit_params": {
    "part_b": { "annual_deductible_amount": 0, "coinsurance_rate": 0.20 },
    "part_a": { "inpatient_deductible_amount": 0, "deductible_allocation": "parent" },
    "opps": { "apply_cap": true, "coinsurance_cap_per_service": null },
    "policy_toggles": { "apply_sequestration": false, "sequestration_rate": 0.02 }
  }
}
```

**Response must include**
- Totals: `total_allowed_cents` (integer cents), beneficiary totals in cents, and any post‑acute flags.  
- Line items: per‑code `allowed_cents`, beneficiary `deductible_cents`, `coinsurance_cents`, `total_cents`, optional `opps_cap_applied` + `opps_cap_amount_cents`, and—**only when enabled**—`program_payment_after_sequestration_cents`.  
- `money`: `{ "currency": "USD", "scale": 2 }`; add decimal fields when `?format=decimal` is supplied.  
- `geography` block with candidates and `used` selection.  
- `trace` block: datasets + versions, formulas, geo mapping, unit conversions, toggles, and **run_id**.

---

## 10) CRUD Contract — Treatment Plan Resource

### 0. Resource Identity & Data Governance
| Field | Value |
| :--- | :--- |
| **Resource Name (API/Code)** | `plan_id` |
| **Canonical ID Type** | UUIDv4 |
| **Source of Truth (SoT)** | `plans` table (PostgreSQL) |
| **Optimistic Lock** | Integer `version` column (required on `PATCH`)

### 1. Operations & HTTP Contract
| Operation | Method / Path | Idempotency-Key | Response | Default Sort |
| :--- | :--- | :--- | :--- | :--- |
| CREATE | `POST /plans` | Mandatory (`Idempotency-Key`) | 201 Created + full plan | N/A |
| READ (by id)* | `GET /plans/{plan_id}` *(roadmap)* | No | 200 OK + full plan | N/A |
| LIST | `GET /plans?cursor=...` | No | 200 OK (paginated) | `created_at` DESC |
| UPDATE | `PATCH /plans/{plan_id}` *(MVP)* | Yes | 200 OK + full plan | N/A |
| DELETE | — | Not supported | — | — |

### 2. Concurrency, Deletion & Safety
| Constraint | Policy |
| :--- | :--- |
| Concurrency Guard | `PATCH` requires `If-Match: <version>`; mismatch ⇒ 409 `CONCURRENCY_VIOLATION`. |
| Idempotency Conflict | Re-using an `Idempotency-Key` with a different payload ⇒ 409 `IDEMPOTENCY_CONFLICT`. |
| Idempotency Window | 24 hours per tenant/endpoint. |
| Deletion Mode | Not available in MVP (future soft delete, 6-year retention). |
| Deletion Retention | N/A until delete ships. |

### 3. Validation & Invariants
| Type | Policy |
| :--- | :--- |
| Required Fields | `name`, ≥1 `components[]` each with `code`, `setting`, `units`. |
| Uniqueness | `(tenant_id, external_plan_ref)` unique when provided. |
| Immutable Fields | `tenant_id`, `created_by`, `created_at`, `external_plan_ref`. |
| Max Cardinality | `components[]` ≤ 200; `modifiers[]` ≤ 5 per component. |
| Field Sanitization | Trim strings, lowercase payer IDs, UTC timestamps. |
| FK Constraints | Component codes must exist in catalog; violations emit `400 PLAN_VALIDATION_ERROR`. |
| State Machine | `status`: `DRAFT → READY → ACTIVE`; `ACTIVE` cannot revert to `DRAFT`. |

### 4. Authorization & Filtering
| Operation | Required Role(s) | Default Filtering |
| :--- | :--- | :--- |
| CREATE | `pricing:plans.write` | Auto-populate `tenant_id` from auth context. |
| READ/LIST | `pricing:plans.read` | Auto-filter by `tenant_id`; cross-tenant access forbidden. |
| UPDATE | `pricing:plans.write` (admin bypass state checks) | Payload `tenant_id` must match token. |
| DELETE | — | — |

### 5. Audit & Observability
| Type | Policy |
| :--- | :--- |
| Domain Events | Emit `plan.created`, `plan.updated`, `plan.status_changed`. |
| Audit Fields | `created_at/by`, `updated_at/by`, `version`; future `deleted_*` fields. |
| Tracing Link | Correlate `trace_id`/`span_id`; surface in response trace block. |
| Metrics | CRUD request count, latency, error rate (labelled by operation). |

### 6. Non-CRUD Actions (Commands)
| Command | Purpose / Triggers |
| :--- | :--- |
| `POST /plans/{plan_id}:publish` | Promote plan from `READY` → `ACTIVE` after validations. |
| `POST /plans/{plan_id}:clone` *(roadmap)* | Duplicate composition into new draft. |

### 7. API Error Catalog (Examples)
| HTTP Status | Custom Code | Description |
| :--- | :--- | :--- |
| 400 | `PLAN_VALIDATION_ERROR` | Schema or invariant failure. |
| 401 | `AUTH_INVALID_KEY` | Missing/invalid API key. |
| 403 | `ACCESS_DENIED` | Caller lacks required scope. |
| 404 | `PLAN_NOT_FOUND` | Plan ID not found for tenant. |
| 409 | `CONCURRENCY_VIOLATION` | `If-Match` version mismatch. |
| 409 | `IDEMPOTENCY_CONFLICT` | Reused idempotency key with different payload. |
| 422 | `STATE_VIOLATION` | Illegal state transition. |
| 503 | `UPSTREAM_UNAVAILABLE` | Dependency failure. |
| 500 | `SYSTEM_ERROR` | Unhandled exception (trace ID logged). |


