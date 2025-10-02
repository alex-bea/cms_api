# Compliance & Licensing (call‑out)
- **Code sets:** We do **not** distribute CPT®. We ship only HCPCS/DRG/APC and other non‑CPT code sets.
- **User‑supplied CPT:** Allowed only as input; optional private crosswalk to HCPCS can be enabled behind a flag. No redistribution.
- **Data provenance:** Every dataset used in pricing is recorded in trace with **dataset_id**, **digest (SHA256)**, **effective_from/to**, and **selection_reason**.
- **PHI/PII:** Service is designed to process no PHI. Logs/trace redact user identifiers; API key auth required.
- **Licenses:** CMS/HRSA/HUD datasets used under their public terms; hospital/payer MRFs under transparency rules.

# PRD — CMS Treatment Plan Pricing & Comparison API (v0.1)

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
All endpoints and change management must align with the **API-STD-Architecture_prd_v1.0**.
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

## 10) Calculation Rules (by setting)
- **MPFS:** `(WorkRVU×GPCIw + PERVU×GPCIpe + MPRVU×GPCImp) × CF`; apply POS to choose NF vs FAC PE RVU; handle modifiers (-26/TC, bilateral, multiple-procedure discount).  
- **OPPS:** wage‑adjust Addendum B base; apply packaging (status indicators J1, Q1/Q2/Q3, N, etc.); do not double‑count packaged items; add professional read via MPFS if applicable.  
- **ASC:** use ASC rate; professional separated via MPFS as needed.  
- **IPPS:** baseline = `DRG weight × ((OpBase × WI) + (CapBase × WI))`; enhanced mode may add IME/DSH/outlier factors by configuration.  
- **CLFS/DMEPOS:** use schedule rates; DMEPOS rural multiplier where applicable.  
- **Part B Drugs:** `ASP × 1.06 × units` (sequestration toggle later).  
- **NADAC:** `unit_price × units` (reference only; no cost sharing).

---

## 11) Beneficiary Cost Sharing & Policy Toggles
- **Part B deductible & coinsurance (MVP):** Apply an annual **Part B deductible first** across **Part B‑eligible** lines (MPFS, HOPD/OPPS, ASC, CLFS, DMEPOS, Part B drugs). For each line: `deduct_applied = min(remaining_deductible, allowed)`; `coinsurance = coinsurance_rate × (allowed − deduct_applied)`. Update the run‑level deductible accumulator afterward. Default coinsurance rate **20%**, configurable via `benefit_params.part_b.coinsurance_rate`.
- **OPPS coinsurance cap (per service):** If enabled, cap HOPD coinsurance at `benefit_params.opps.coinsurance_cap_per_service`. If the cap is **null/0**, it **defaults to** the **Part A inpatient deductible** you provide in `benefit_params.part_a.inpatient_deductible_amount`. When the cap binds, return `opps_cap_applied=true` and `opps_cap_amount` on that line.
- **IPPS Part A deductible (baseline policy):** Allocate the **entire** inpatient deductible to the **DRG parent line** (not proportional) and include a trace note; proportional allocation is a roadmap option (`deductible_allocation="proportional"`).
- **Sequestration (toggle):** When `benefit_params.policy_toggles.apply_sequestration=true`, compute and return `program_payment_after_sequestration = (allowed − beneficiary_total) × (1 − sequestration_rate)`. **Do not** alter beneficiary amounts. Toggle is **off by default**.

## 12) Ambiguity & Packaging Policies
- **ZIP ambiguity:** return all candidates (`used` true/false); default rule = highest population overlap; allow client to pin.
- **Packaging:** any packaged OPPS items appear as lines with `packaged=true` and `$0` separate payment; rolled into parent line for totals.

---

## 13) Facility‑Specific Pricing via CCN
- On `ccn` input, attempt MRF pull for relevant HCPCS/DRG/Rev‑code subsets.  
- If found, return **facility tech** amount and benchmark side-by-side; set `facility_specific=true` and cite **MRF dataset** in trace.  
- If not found, fallback to benchmark and set `facility_specific_unavailable=true` with reason.

---

## 14) Roadmap & Milestones
**MVP (4–6 weeks)**
- MPFS, OPPS, ASC, CLFS, DMEPOS, ASP, NADAC (read-only), IPPS baseline.  
- ZIP resolver; beneficiary math; plan CRUD; trace; compare A vs B; **/compare/providers** matrix.
- **Temporal Resolution** per‑dataset logic (latest ≤ valuation date) + quarter/digest pinning.
- **Geography fallback** w/ radius expansion to 100 miles (configurable).
- **Provider Registry** (IDs + fuzzy matching + manual override).  
- Independent provider CSV loader with auto‑mapping + raw column preserve.

**v1**
- MRF facility pricing (selected CCNs) with caching; NCCI edit enforcement; OPPS packaging completeness.  
- Enhanced IPPS add‑ons; anesthesia base+time.  
- **Medicaid FFS scaffolding** (start with **Texas**; add state & nationwide defaults upload).

**v2**
- **MCO TiC parsing API** (varied formats like `.json.gz`, etc.) with flexible readers and caching.  
- Payer TiC MRF parsing (select payers); additional state Medicaid schedules.  
- Advanced scenario UI (sensitivity sliders), cohort‑weighted mixes.  
- “Top‑50 default multiplier” computation (12‑month lookback; plan mix → else state spend; sources: Hospital MRF → Payer TiC → CSV/value).

## 15) Risks & Mitigations
- **MRF variability/size:** parse by CCN+code whitelist; cache; expose coverage % and fallback flags.
- **Time misalignment:** enforce valuation date parity across datasets; trace shows versions per line.
- **Double counting (TC vs facility):** validator rules and status indicators; fail fast on conflicts.
- **Drugs unit errors:** dual‑key model with explicit converters; show both HCPCS and NDC math in trace.
- **ZIP ambiguity:** require pinning for compares when ambiguity is material.
- **Licensing (CPT®):** store HCPCS freely; do not distribute CPT content without AMA license.

---

## 16) Acceptance Criteria (examples)
1. **Pricing parity:** For a curated set of 50 CPT/HCPCS across 3 localities, API allowed amounts exactly match CMS reference within ≤1%.
2. **Comparison guardrails:** `/compare` rejects requests where snapshots differ; returns explicit parity report when accepted.
3. **Trace completeness:** Every `/price` response includes dataset IDs, effective dates, and formulas; a rerun with `run_id` reproduces identical totals.
4. **ZIP ambiguity:** `/geography/resolve?zip=73301` returns ≥2 MPFS localities with `used` flags; `/price` echoes the choice.
5. **Facility override:** Passing `ccn` with an available MRF toggles `facility_specific=true` and shows both benchmark and facility amounts.
6. **Drug dual-key:** A Part B J‑code line shows Part B allowed, and—if NDC provided—NADAC reference with unit conversion trace.

---

## 17) Open Questions
- Default rule for allocating IPPS Part A deductible across line items (proportional to allowed vs fixed to the DRG line only)?
- Should we expose a **policy toggles** endpoint to set sequestration ON/OFF per run?  
- What threshold of ZIP ambiguity triggers `requires_resolution=true` (e.g., >20% split)?

---

## 18) Appendix — Loader Schemas (expected columns)
- **MPFS RVU:** `hcpcs, work_rvu, pe_nf_rvu, pe_fac_rvu, mp_rvu, global_days, status_indicator` (+ `year, revision` added).
- **GPCI:** `locality, locality_name, gpci_work, gpci_pe, gpci_mp, year`.
- **CF:** `year, cf, source, effective_from, effective_to`.
- **OPPS:** `year, quarter, hcpcs, status_indicator, apc, national_unadj_rate, packaging_flag`; `wage_index(year, cbsa, wage_index)`.
- **ASC:** `year, quarter, hcpcs, asc_rate`.
- **IPPS:** `fy, drg, weight`; `ipps_base_rates(fy, operating_base, capital_base)`; `wage_index(fy(or year), cbsa, wage_index)`.
- **CLFS:** `year, quarter, hcpcs, fee`.
- **DMEPOS:** `year, quarter, code, rural_flag, fee`.
- **ASP:** `year, quarter, hcpcs, asp_per_unit`; **NADAC:** `as_of, ndc11, unit_price`; **NDC↔HCPCS:** `ndc11, hcpcs, units_per_hcpcs`.
- **Geography:** `zip5, locality_id, share`; `zip5, cbsa, share`.
- **Benefits:** `year, setting, rules_json` (Part A/B deductibles, caps, percentages).

---

## 19) Example Requests & Responses

### Example: Price a plan with OPPS cap defaulting to Part A deductible
**Request**
```http
POST /price?zip=94110&year=2025
Content-Type: application/json

{
  "plan": {
    "plan_id": "knee_bundle",
    "components": [
      {"code": "27447", "setting": "ASC", "units": 1, "professional_component": true},
      {"code": "66984", "setting": "HOPD", "units": 1},
      {"code": "J0171", "setting": "DRUG_PARTB", "units": 10}
    ]
  },
  "benefit_params": {
    "part_b": {"annual_deductible_amount": 240, "coinsurance_rate": 0.2},
    "part_a": {"inpatient_deductible_amount": 1632, "deductible_allocation": "parent"},
    "opps": {"apply_cap": true, "coinsurance_cap_per_service": null},
    "policy_toggles": {"apply_sequestration": false, "sequestration_rate": 0.02}
  }
}
```

**Response (shape)**
```json
{
  "total_allowed": 12345.67,
  "lines": [
    { "code": "66984", "setting": "HOPD", "allowed": 5200.00, "components": {"facility": 5200.00},
      "beneficiary": {"deductible": 240.00, "coinsurance": 104.00, "total": 344.00, "opps_cap_applied": true, "opps_cap_amount": 1632.00 } },
    { "code": "27447", "setting": "ASC", "allowed": 2200.00, "components": {"facility": 1200.00, "professional": 1000.00},
      "beneficiary": {"deductible": 0.00, "coinsurance": 440.00, "total": 440.00 } },
    { "code": "J0171", "setting": "DRUG_PARTB", "allowed": 1000.00, "components": {"allowed": 1000.00},
      "beneficiary": {"deductible": 0.00, "coinsurance": 200.00, "total": 200.00 } }
  ],
  "benefits": {"remaining_part_b_deductible": 0.00},
  "geo": {"zip5": "94110", "locality_id": "...", "cbsa": "...", "is_rural_dmepos": false},
  "trace": {"datasets": [...], "notes": ["OPPS packaged ...", "DMEPOS rural status inferred by heuristic (no CBSA)"]}
}
```

### Example: Sequestration ON (field only appears when enabled)
Set `benefit_params.policy_toggles.apply_sequestration=true`.
Each line will additionally include:
```json
{ "program_payment_after_sequestration": 3872.00 }
```

### Example: Compare two locations (A vs B)
**Request**
```http
POST /compare?zip_a=94110&zip_b=77030&year=2025&payer=ExamplePayer&plan_filter=Commercial%20PPO
Content-Type: application/json

{
  "plan": {
    "plan_id": "knee_bundle",
    "components": [
      {"code": "27447", "setting": "ASC", "units": 1, "professional_component": true},
      {"code": "66984", "setting": "HOPD", "units": 1}
    ]
  },
  "benefit_params": {
    "part_b": {"annual_deductible_amount": 240, "coinsurance_rate": 0.2},
    "part_a": {"inpatient_deductible_amount": 1632, "deductible_allocation": "parent"},
    "opps": {"apply_cap": true, "coinsurance_cap_per_service": null},
    "policy_toggles": {"apply_sequestration": false, "sequestration_rate": 0.02}
  }
}
```

**Response (shape)**
```json
{
  "status": "ok",
  "year": 2025,
  "quarter": null,
  "payer": "ExamplePayer",
  "plan_filter": "Commercial PPO",
  "A": { "zip": "94110", "ccn": null, "result": { "total_allowed": 7400.00, "lines": [...], "trace": {...}, "geo": {...} } },
  "B": { "zip": "77030", "ccn": null, "result": { "total_allowed": 7800.00, "lines": [...], "trace": {...}, "geo": {...} } },
  "delta_total_allowed_B_minus_A": 400.00
}
```

### Example: Compare N providers in one request (matrix)
**Request**
```http
POST /compare/providers?zip=94110&year=2025
Content-Type: application/json

{
  "plan": { "plan_id": "tka" },
  "providers": [
    {"label": "Medicare", "source": "medicare"},
    {"label": "Medicaid (TX FFS)", "source": "medicaid", "medicaid_source": "ffs", "state": "TX"},
    {"label": "Hospital A", "source": "hospital_mrf", "ccn": "450123", "payer": "ExamplePayer", "plan_filter": "PPO"},
    {"label": "Independent Ortho", "source": "independent_csv", "provider_id": "prov-abc", "fallback_multiplier": 2.25}
  ],
  "benefit_params": {"part_b": {"annual_deductible_amount": 240, "coinsurance_rate": 0.2}},
  "options": {"use_provider_zip": false, "max_radius_miles": 100}
}
```

**Response (shape)**
```json
{
  "matrix": {
    "rows": [
      {"code": "27447", "setting": "ASC", "Medicare": 2200.00, "Medicaid (TX FFS)": 1800.00, "Hospital A": 5400.00, "Independent Ortho": 4950.00, "winner": "Medicaid (TX FFS)"},
      {"code": "J0171", "setting": "DRUG_PARTB", "Medicare": 1000.00, "Medicaid (TX FFS)": 950.00, "Hospital A": 2250.00, "Independent Ortho": 2000.00, "winner": "Medicaid (TX FFS)"}
    ],
    "totals": {"Medicare": 3200.00, "Medicaid (TX FFS)": 2750.00, "Hospital A": 7650.00, "Independent Ortho": 6950.00, "winner": "Medicaid (TX FFS)"}
  },
  "coverage_summary": {
    "Hospital A": {"mrf_pct": 80.0, "tic_pct": 10.0, "fallback_pct": 10.0},
    "Independent Ortho": {"csv_pct": 60.0, "fallback_pct": 40.0}
  },
  "trace": {"notes": ["Same ZIP used for all providers", "Fallback multiplier 2.25× used for 1 line (Independent Ortho)"]}
}
```

---


## Changelog (recent)
**2025-09-25**
- Added **Beneficiary Cost Sharing & Policy Toggles** section.
- Updated **API Endpoints (MVP)** for `POST /price` with `benefit_params`; added `POST /compare` and **`POST /compare/providers`**.
- Added **Examples** for pricing, sequestration, A vs B, and **multi-provider matrix**.
- Added **Temporal Resolution** algorithm and **Geography Fallback Policy** (radius expansion up to **100 miles**).
- Added **Data Model** entries for **Provider Registry** and **Independent Provider Rates**.
- Roadmap updated: **Medicaid FFS scaffolding** (start with **Texas**); future **MCO TiC parsing API** supporting `.json.gz` and other formats.


## 3) Architecture & Deployment
- **Language:** Python **3.11** (CI covers 3.11 & 3.12; runtime pinned to 3.11 for MVP).
- **Framework:** FastAPI + Pydantic v2 + Uvicorn/Gunicorn.
- **Persistence:** PostgreSQL (prod/MVP service), SQLite (dev/tests). Optional DuckDB for offline analytics.
- **DB layer:** SQLAlchemy 2.x (Core for heavy ingest; ORM for app models). Alembic for migrations.
- **Containers:** Multi‑stage Docker build; `docker-compose` with `api` + `db` (+ optional `worker`). Cloud‑agnostic image for ECS/EKS/GKE/Cloud Run.
- **Config (env):** `DATABASE_URL`, `API_KEYS`, `MAX_PROVIDERS`, `MAX_RADIUS_MILES`, `DEFAULT_FALLBACK_MULTIPLIER` (2.25), `RETENTION_MONTHS` (13).
- **Auth:** Require API key via header `X-API-Key`. Basic per‑key rate limiting recommended.
- **Money precision:** return **integer cents** by default; optional `?format=decimal` adds doubles. Include `{currency:"USD", scale:2}`.
- **Licensing/Compliance:** No distribution of CPT®. We store and ship **HCPCS/DRG/APC** only. If users upload CPT, treat as user-supplied; optional private crosswalk behind a flag.

## Changelog (recent)
**2025-09-25**
- Added **Architecture & Deployment** section (Python 3.11; FastAPI/Pydantic v2; Postgres prod, SQLite dev; SQLAlchemy 2.x; Dockerized; API key auth; money in integer cents; licensing constraints).
- Expanded **Data Model** with `runs/*` tables for persisted traces and enriched `plan_components` fields (`pos`, `ndc11`, `ndc_units`, `assume_facility_component`, `fallback_multiplier`, `wastage_units`, `label`, `source_prefs`).
- Updated **API Endpoints**: `/plans` pagination (default 20, max 200), return **run_id** from pricing endpoints, and money fields in **cents** with optional decimal format.


## 20) Success Metrics & Testing
### Functional success
- **Cent‑level parity** on golden tests: ≥ **98%** of lines match exactly; remaining within **±1 cent** (known rounding cases only).
- **Trace completeness:** 100% of runs include dataset digests, temporal selection reason, geo fallback notes, and source precedence per line.

### Performance SLOs (MVP)
- **Warm requests (10‑line plan):** p95 **< 250 ms**.
- **Cold start (no cache):** p95 **< 1200 ms**.
- **Throughput target:** 50–100 RPS on 4 vCPU VM.

### Cache targets
- **In‑memory cache hit ratio:** ≥ **0.80** during steady state.
- **Disk cache hit ratio (if enabled):** ≥ **0.60** after warmup.
- **Slice build time (p95):** **< 150 ms** for common filters.

### Test strategy
- **Golden suite:** 50 HCPCS across 3 MPFS localities + 2 OPPS quarters; includes E/M, imaging, surgery, lab, DME, Part B drugs; modifiers: `-26`, `-TC`, `-50`, multiple‑procedure discount cases.
- **Unit tests:** dataset selection (digest/quarter/latest), geo fallback expansion, beneficiary math (Part B + OPPS cap), sequestration flag behavior.
- **Endpoint tests:** `/price`, `/compare`, `/compare/providers`, `/trace/{run_id}`, `/admin/datasets` (RBAC).
- **Load tests:** baseline with cached datasets; monitor p95 + error rate < **0.1%**.

### Observability
- **Logs:** `structlog` JSON with `request_id`, `run_id`, digests, decisions per line; redaction for user identifiers.
- **Metrics:** Prometheus `/metrics` exposing request counts/latency histograms, cache hits, dataset selection, packaging counters, modifier counters.
- **Health:** `/healthz` (liveness) and `/readyz` (DB + required snapshots present + cache writable).

### QA Summary (per QA & Testing Standard v1.0)
| Item | Details |
| --- | --- |
| **Scope & Ownership** | CMS treatment plan pricing & comparison API; owned by Pricing Apps squad with QA partner from Quality Engineering; stakeholders include Product Strategy, Provider Ops, and external clients. |
| **Test Tiers & Coverage** | Unit/component: `tests/test_plans.py` (pricing math, modifiers, beneficiary cost sharing); Contract/API: `tests/test_rvu_api_contracts.py` verifies payload + schema; Integration: `tests/test_geography_resolver.py` + `tests/test_nearest_zip_resolver.py` exercised via API harness; Scenario/E2E: golden run comparisons in `tests/test_plans.py::test_golden_pricing`. Coverage currently 78% (target ≥85% for API layer) tracked in CI coverage report. |
| **Fixtures & Baselines** | Golden treatment plans + expected outputs stored in `tests/golden/test_scenarios.jsonl`; RVU/GPCI fixtures under `tests/fixtures/rvu/`; API response baselines tracked in QA dashboards with digests logged alongside release notes. |
| **Quality Gates** | Merge pipeline executes unit + contract tests (fails on coverage regression >0.5%); `ci-integration.yaml` runs API end-to-end suites against docker-compose; release gating requires passing load harness + acceptance checklist in §16. |
| **Production Monitors** | Synthetic `/price` + `/compare` probes every 5 min; Prometheus latency/error alerts aligned with SLOs above; dataset freshness monitor ensures backing snapshots ≤ 10 days old. |
| **Manual QA** | Pre-release review of multi-provider matrix outputs; manual validation of trace payloads for new modifiers; compliance check for licensing toggles before external enablement. |
| **Outstanding Risks / TODO** | Expand load/SLO automation (per Test strategy); backfill Medicaid comparators coverage; finalize RBAC smoke for `/admin/datasets` before GA. |
