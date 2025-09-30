# Changelog (Pinned)
- **2025-09-26**: Initial draft of Geography PRD with **ZIP+4-first mandate**, effective-dating rules, schema, ingestion steps, QA, and trace requirements.
- **2025-09-26**: Clarifications applied — non-strict default with explicit fallback policy; **nearest fallback constrained to same state**; carrier exposure optional; locality-name dictionary loader added; annual-as-quarter fallback allowed; ZIP+4 normalization; cache strategy clarified (digest-aware by default, TTL optional); conversational strict-mode errors; per-request radius override; daily gap report methodology.

---

# Geography Locality Mapping — PRD (ZIP+4-first)

## 1) Purpose
Define how we ingest and use CMS geography mapping to resolve a service **ZIP(+4)** to a **Medicare MPFS pricing locality** and related attributes used by pricing engines (MPFS, DMEPOS, etc.). This PRD **mandates using ZIP+4 records when available** and specifies fallbacks, effective-dating, validation, and traceability.

## 2) Scope
- **In-scope**: Ingestion of CMS ZIP→Locality mapping (aka ZIP9 layout), quarterly/effective selection, rural and MAC attributes, resolver behavior, persistence schema, indices, and test/QA rules.
- **Out-of-scope (MVP)**: CBSA/imputed locality via geocoding for missing ZIPs, address-to-ZIP+4 parsing, state Medicaid geographies, and county crosswalks (can be future work).

## 3) Definitions
- **ZIP5**: 5-digit USPS ZIP code.
- **ZIP+4**: 9-digit postal code (ZIP5 plus 4-digit add-on) enabling sub-ZIP granularity.
- **Locality (MPFS)**: CMS-defined pricing area used to select **GPCI** factors for MPFS.
- **Rural Indicator**: Flag (e.g., `R`, `B`, or blank) primarily used by other programs (e.g., DMEPOS rural adjustments).
- **MAC/Carrier**: Medicare Administrative Contractor jurisdiction code for Part B.
- **Effective From / Effective To**: Calendar dates that bound when a row is **valid**. Select the row whose window covers the valuation date (`effective_from ≤ valuation_date ≤ effective_to`). For quarterly files (e.g., ZIP→Locality), map Q1/Q2/Q3/Q4 to quarter start/end (e.g., `2025Q1` → `2025-01-01` to `2025-03-31`). For annual files (e.g., GPCI, CF), use `YYYY-01-01` to `YYYY-12-31`. If CMS only ships annual for a year, treat it as covering all quarters when quarter is omitted.

- **ZIP5**: 5-digit USPS ZIP code.
- **ZIP+4**: 9-digit postal code (ZIP5 plus 4-digit add-on) enabling sub-ZIP granularity.
- **Locality (MPFS)**: CMS-defined pricing area used to select **GPCI** factors for MPFS.
- **Rural Indicator**: Flag (e.g., `R`, `B`, or blank) primarily used by other programs (e.g., DMEPOS rural adjustments).
- **MAC/Carrier**: Medicare Administrative Contractor jurisdiction code for Part B.

## 4) Data Sources

> Reference: **CMS Medicare Fee Schedules** — ZIP Code to Carrier Locality files are published here: https://www.cms.gov/medicare/payment/fee-schedules
>
> Reference: **Carrier-Specific Files (PFS)** — jurisdiction-level files (useful for MAC/carrier context, locality names, and QA): https://www.cms.gov/medicare/payment/fee-schedules/physician/carrier-specific-files

## 4.1 Crosswalk Matrix
| Mapping | Purpose | Source | Primary Keys | Effective Dating | Used By | Required |
|---|---|---|---|---|---|---|
| **ZIP+4 → MPFS Locality** | Resolve precise MPFS locality for pricing | CMS ZIP Code to Carrier Locality (ZIP9 layout) | `(zip5, plus4)` → `locality_id` | Year + optional Quarter (choose latest ≤ valuation when quarter omitted) | MPFS | **Yes** (ZIP+4-first)
| **ZIP5 → MPFS Locality** | Fallback when ZIP+4 not available | CMS ZIP Code to Carrier Locality | `zip5` → `locality_id` | Same as above | MPFS | **Yes** (fallback)
| **ZIP(±4) → State** | Enforce in-state nearest fallback; analytics | CMS ZIP Code to Carrier Locality | `(zip5[, plus4])` → `state` | Same as above | Resolver, Analytics | **Yes**
| **ZIP(±4) → Carrier/MAC** | Debugging/jurisdiction analytics; optional API exposure | CMS ZIP Code to Carrier Locality | `(zip5[, plus4])` → `carrier` | Same as above | Resolver, Analytics | Optional (expose via flag)
| **ZIP(±4) → Rural Flag** | DMEPOS/rural-sensitive logic | CMS ZIP Code to Carrier Locality | `(zip5[, plus4])` → `rural_flag` | Same as above | DMEPOS, Analytics | Optional (store)
| **Locality → GPCI (work, PE, MP)** | Apply geographic adjustment to RVUs | CMS GPCI files | `locality_id`, `year` → `gpci_work, gpci_pe, gpci_mp` | Annual | MPFS | **Yes**
| **Valuation Date → Dataset Slice** | Reproducible selection of mapping/GPCI | Snapshots registry (manifests) | `dataset_id`, `year[, quarter]` or `digest` | Versioned by digest + dates | Resolver, Engines | **Yes**
| **ZIP5 → CBSA** | QA/analytics; not for MPFS locality selection | Supplemental CBSA crosswalk (optional) | `zip5` → `cbsa` | Annual | Analytics | Optional
| **Locality → Display Name** | Nicer UI labels | Optional locality-name dictionary | `locality_id` → `locality_name` | Ad-hoc/annual | UI only | Optional

## 5) Hard Requirements (MUST) (MUST)
1. **ZIP+4-first mandate**: When a request includes a plus-four, the resolver **MUST** look up an **exact ZIP+4** record first and use its **locality** if found.
2. **Fallback hierarchy** (default **non-strict**):
   - (a) **ZIP+4 exact** match (if plus-four provided)
   - (b) **ZIP5 exact** match
   - (c) **Nearest ZIP5 within configurable radius** (default 25mi, expand by 10mi steps up to 100mi cap) **and within the same state (MUST NOT cross state lines)**
   - (d) **Plan-defined default locality** (optional) **else** benchmark locality; strict mode will **error** here instead of defaulting
3. **Effective dating**: Select the mapping **effective for valuation (year and optional quarter)**; if quarter omitted or CMS shipped only annual mapping, choose **latest ≤ year-end** for all quarters.
4. **Traceability**: Every resolution emits structured `geo_resolution` with: inputs, `match_level` (`zip+4|zip5|nearest|default|error`), chosen locality, radius used, state constraint, and dataset digest.
5. **Determinism**: Same inputs + same digest ⇒ same output.
6. **Performance**: p95 ≤ 2ms (warm LRU), ≤ 20ms (cold DB).
7. **Input normalization**: Accept `ZIP+4` as either `94110-1234` or `941101234`; coerce to `(zip5, plus4)` internally.

## 6) Nice-to-haves (SHOULD)
- Support multi-locality ZIP5 tie-breaking using carrier/MAC and rural flags (when guidance applies); else warn.
- Return locality metadata (name) for UI; optional loader provided.
- Per-request radius override (`max_radius_miles`, `initial_radius_miles`, `expand_step_miles`).

- Support multi-locality ZIP5 cases by preferring ZIP+4; if only ZIP5 exists with multiple localities, use **carrier/MAC + rural** tie-breakers when CMS guidance applies; else warn.
- Return locality metadata (name) for UI; store but do not require it for pricing.

## 7) Data Model
### 7.1 Tables
**`geography`** (authoritative crosswalk, effective-dated)
- `zip5 TEXT NOT NULL`
- `plus4 TEXT NULL`
- `has_plus4 INTEGER NOT NULL`
- `state TEXT NULL`
- `locality_id TEXT NOT NULL`
- `locality_name TEXT NULL`
- `carrier TEXT NULL`  — **optional to expose** in API when `expose_carrier=true` (default hidden)
- `rural_flag TEXT NULL`
- `effective_from TEXT NOT NULL`
- `effective_to TEXT NOT NULL`
- `dataset_id TEXT NOT NULL DEFAULT 'ZIP_LOCALITY'`
- `dataset_digest TEXT NOT NULL`
- **Indices**: `idx_geo_zip5`, `idx_geo_zip5_plus4`, `idx_geo_effective`, `idx_geo_locality`

**`locality_dict`** (optional loader for UI labels)
- `locality_id TEXT PRIMARY KEY`
- `locality_name TEXT`
- `state TEXT NULL`
- `notes TEXT NULL`

### 7.2 Invariants
- `(zip5, plus4, effective_from, effective_to)` unique per record version.
- For a given `zip5/plus4` and valuation instant, **≤ 1 active record**.

## 8) Ingestion Pipeline

### 8.1 State Code Sourcing & Validation
- **Primary (authoritative):** Use the **State** field from the CMS ZIP→Locality (ZIP9) file for every record. The ZIP9 fixed‑width layout includes `state` (2 chars) per row; persist this value into `geography.state`.
- **Secondary (QA/enrichment):** Optionally ingest **Carrier‑Specific Files (PFS)** to confirm that each `carrier`’s state coverage aligns with `geography.state`. If there is disagreement, **prefer the ZIP9 state** and log a trace warning.
- **Fallback (rare):** If a record is missing a state (or for additional QA), consult an auxiliary **ZIP→State** crosswalk (e.g., ZIP→county→state or ZCTA→state) to validate. Use only for validation or as a last‑resort fill; do not override a present ZIP9 state.
- **Nearest‑ZIP fallback policy:** Enforce **same‑state constraint** using `geography.state` (from ZIP9). Nearest fallback must **not** cross state lines.
- **Edge cases:** Preserve leading zeros in ZIP+4; handle territories (PR, VI, GU, AS, MP) explicitly—return a friendly error if unsupported; APO/FPO/DPO may not map to standard localities and should be rejected with an actionable message.


1. **Download**: Use resilient downloader (ETag/Last-Modified, retries, checksum) to fetch CMS ZIP archives.
2. **Extract**: Pull CSV/TXT (and convert XLS/XLSX to CSV if needed).
3. **Parse**: Map CMS columns → our schema. Required: `zip`/`zip5`, plus-four flag and value (when present), `locality`, `state` (if present), `carrier`, `rural indicator`, `effective period` or implied by file release/quarter.
4. **Normalize**:
   - Coerce ZIPs to 5/4 digits; uppercase locality/carrier/state; standardize rural flags.
   - Set `has_plus4=1` when plus-four present.
   - Set `effective_from/effective_to` based on file year/quarter.
   - Assign `dataset_digest` (rollup SHA256 of normalized files) and `dataset_id='ZIP_LOCALITY'`.
5. **Load**: Upsert into `geography` (effective-dated). Ensure non-overlapping active periods for the same key.
6. **Register snapshot**: Insert `(dataset_id, effective_from, effective_to, digest, source_url)` into `snapshots`.

## 9) Resolver Behavior
Input: `{ zip5: str, plus4: Optional[str], valuation_year: int, quarter: Optional[int], strict: bool=false, max_radius_miles?: int, initial_radius_miles?: int, expand_step_miles?: int, expose_carrier?: bool }`
Algorithm:
1. Select **dataset version** by valuation (year/quarter or snapshot pin); trace `dataset_selection`.
2. If `plus4` present → query `geography` for active `(zip5, plus4)`; if found, use with `match_level='zip+4'`.
3. Else query active ZIP5-only row `(zip5, plus4 IS NULL)` → `match_level='zip5'`.
4. Else compute **nearest ZIP** within the same **state** using configured radius policy (defaults: start 25mi, step 10mi, cap 100mi). If found, `match_level='nearest'` and trace the distance and candidate ZIP chosen.
5. Else: if `strict=true`, **error**; if `strict=false`, use **plan default locality** or **benchmark** → `match_level='default'`.
6. Return `{ locality_id, state?, rural_flag?, carrier? (when expose_carrier), match_level, dataset_digest }`.

Rationale for nearest ZIP choice: pick the **geodesically closest ZIP5 within the same state** having an active mapping for the valuation instant; if ties, choose the one whose locality’s **GPCI variance** to neighboring localities is smallest (to minimize pricing distortion).

## 10) Error Handling & Observability
- **Errors** (strict mode): Conversational message, e.g.,
  > "We require a ZIP+4 for precise locality pricing in this area, but couldn’t find one for **94110**. We looked for a 9‑digit ZIP (e.g., **94110‑1234**) and then a 5‑digit match. Because strict mode is on, we won’t fall back to nearby ZIPs. You can provide a ZIP+4, or retry with `strict=false` to let us use the closest in‑state ZIP."  
  Include rationale: next candidate would be the nearest in‑state ZIP within the radius policy, starting at 25mi and expanding to 100mi.
- **Logs**: Structured JSON with `zip`, `plus4`, `match_level`, `locality`, `state`, `dataset_digest`, `radius`, `latency_ms`.
- **Metrics**: Counters per `match_level`; histogram of resolution latency.

## 11) QA & Validation
- **Schema validation**: Required columns present; row counts > threshold; locality set is non-empty.
- **Effective coverage**: ≥ 99% of ZIP5s in test states have an active record for the valuation period.
- **Functional tests**:
  - Prefer ZIP+4 over ZIP5 when both exist.
  - ZIP at state border uses ZIP+4 to disambiguate.
  - Nearest ZIP fallback triggers with trace and never crosses state lines.
  - Reproducibility: same input + same digest → same output.
- **Golden fixtures**: 10–20 known ZIP+4s across 3 states covering rural/urban and multiple localities.

## 12) Performance Targets
- Warm LRU: p95 ≤ 2ms; Cold DB read: p95 ≤ 20ms.
- Batch plan resolution (100 components): locality lookup adds ≤ 5ms total overhead via cache.
- **Cache policy**: **Digest-aware by default** (invalidate when `dataset_digest` changes). **TTL optional** for fast-changing sources; note TTL may return mixed versions mid-study—prefer digest pins for reproducible research.

## 13) Security & Compliance
- No PHI/PII stored. ZIP codes are not PHI in isolation. Follow CMS terms for data redistribution.

## 14) Rollout Plan
- Phase 1: Load geography for 1–3 states (includes ZIP+4).
- Phase 2: Nationwide load, enable strict ZIP+4 mandate.
- Phase 3: Optional address→ZIP+4 enrichment.
- Phase 4: Add daily **data gap report** pipeline (see §18) and alerting thresholds.

## 15) Implementation Approach (ZIP9 → Locality)
**Goal:** Deterministic, reproducible ingestion of CMS ZIP Code → Carrier Locality (ZIP9) files with **ZIP+4-first** semantics and effective‑dating.

### A) Modules & Responsibilities
- **Downloader:** `cms_pricing/ingestion/cms_downloader.py` — resilient fetch (ETag/Last-Modified, retries), stores raw ZIP and metadata.
- **Extractor:** `cms_pricing/ingestion/zip_handler.py` — extracts `*.txt`/`*.csv`/`*.xlsx` from the CMS ZIP.
- **ZIP9 Parser:** `cms_pricing/ingestion/geography_zip9.py` — fixed‑width reader that yields normalized records.
- **Loader:** `cms_pricing/ingestion/geography_loader.py` — batch upserts into `geography`, creates indexes, registers snapshot.
- **Orchestrator:** `scripts/ingest_real_cms.py` — wires download → extract → parse → load, computes dataset digests.

### B) Fixed‑width layout (authoritative fields)
(1‑indexed CMS layout, implemented with 0‑indexed Python slices)
- State: cols 1–2 → `state`
- ZIP Code: 3–7 → `zip5`
- Carrier (MAC): 8–12 → `carrier`
- Pricing Locality: 13–14 → `locality_id`
- Rural Indicator: 15 → `rural_flag` ("", "R", "B")
- Plus Four Flag: 21 → `has_plus4` ("0"/"1")
- Plus Four: 22–25 → `plus4` (preserve leading zeros)
- Part B Indicator: 32 → `partb_indicator`
- Year/Quarter: 76–80 → `year_quarter` (e.g., `2025Q1`) 

### C) Normalization Rules
- Accept `ZIP+4` as `94110-1234` or `941101234`; store canonical `(zip5, plus4)`; keep **leading zeros** in `plus4`.
- `has_plus4=1` ⇒ `plus4` must be 4 digits; else `plus4=NULL`.
- Effective dating: `2025Q1` → `effective_from=2025-01-01`, `effective_to=2025-03-31` (map for all quarters). If CMS ships annual only, treat as effective for all quarters.
- Record carries `dataset_id='ZIP_LOCALITY'` and a `dataset_digest` (SHA256) for reproducibility.

### D) Loader Behavior
- Stream files line‑by‑line; parse to dicts; **batch `executemany`** inserts.
- Enforce uniqueness on `(zip5, plus4, effective_from, effective_to)`; ignore exact duplicates; collapse overlapping windows at ingest.
- Create/ensure indices: `(zip5)`, `(zip5, plus4)`, `(effective_from, effective_to)`, `(locality_id)`.
- Register snapshot in `snapshots(dataset_id, effective_from, effective_to, digest, source_url)`.

### E) Resolver Contract (recap)
- **ZIP+4-first** → ZIP5 → **Nearest in‑state** (radius expand 25→100mi) → Default/Benchmark (non‑strict) or **Error** (strict).
- Trace `geo_resolution` with: `inputs`, `match_level`, `locality_id`, `state`, `radius_used`, `dataset_digest`.

### F) QA & Tests (ingestion)
- Unit tests: valid ZIP+4, ZIP5-only, bad `year_quarter`, digit validation, quarter mapping.
- Golden fixtures: 10–20 ZIP+4 rows across 3 states (rural/urban, multiple localities).
- Ingest smoke test: parse real file subset → assert row count > N, distinct localities > M, indices exist.

### G) Performance & Ops
- p95 cold read ≤ 20ms; warm LRU ≤ 2ms.
- Idempotent loads: if digest already present, skip with log note.
- Observability: structured logs for file/digest/rows; Prometheus counters by `match_level` after resolver use.

## 16) Open Questions
1. Do we want a strict **per‑row** duplicate detection report (same `(zip5, plus4, window)` across files) in addition to ignoring duplicates at load?
2. Should we store **source filename + line offset** for deep audit (optional columns)?

## 17) Reporting (Data Gaps — initial methodology)
- **Daily report** (CSV + JSON) by state and nationally:
  - Total lookups; share by `match_level` (`zip+4`, `zip5`, `nearest`, `default`, `error`).
  - Top ZIP5s lacking ZIP+4 coverage.
  - Average fallback distance and distribution.
- **Interpretation guide**: Prioritize sourcing ZIP+4 records for ZIPs with high volume and high fallback distance. Use report to drive data acquisition or enrichment.
- **Future**: Add anomaly flags when `zip5` fallback rate spikes vs 7‑day baseline; surface in Ops dashboard.

## 18) Implementation Notes & Risks (ZIP+4 normalization)
- **Leading zeros** in plus‑four must be preserved.
- **Ambiguous/invalid input** → 400 with friendly message.
- **Territories & PO boxes** → validate CMS coverage; friendly error if missing.
- **ZIP changes/retirements** → rely on valuation‑effective mapping and snapshot digests.
- **Overlaps/duplicates** → collapse to single active record per valuation instant.
- **Formatting noise** → trim spaces/dashes; store normalized `(zip5, plus4)`. (Please Confirm)
1. **Strict mode default**: Should non-strict remain default (warn + fallback) while allowing `strict=true` per request to error on non-`zip+4` matches?
2. **Same-state constraint**: Confirm we should never cross state lines in nearest ZIP fallback (even if the nearest is just across the border).
3. **Carrier/MAC usage**: Do we want to expose `carrier` in API responses and traces for analytics/debugging?
4. **Locality name enrichment**: OK to add an optional loader for a locality name dictionary for UX labeling?
5. **Quarter handling**: If CMS provides only annual files in a year, is choosing the latest annual mapping acceptable for all quarters of that year?
6. **ZIP+4 parsing**: Will callers ever provide full 9-digit string without a dash (e.g., `941101234`)? Should the API accept both `94110-1234` and `941101234`?
7. **Stateful resolver cache TTL**: Any constraints on cache invalidation (e.g., rotate when snapshot digest changes only)?

## 16) Acceptance Criteria
- ZIP+4-first resolution enforced; fallbacks per policy and traced.
- Datasets effective-dated and selectable by valuation (year/quarter) and snapshot digest.
- Input normalization for `94110-1234` and `941101234`.
- Unit/integration tests cover all match levels and edge cases.
- Metrics/logs expose match mix and latency.

## 17) Reporting (Data Gaps — initial methodology)
- **Daily report** (CSV + JSON) by state and nationally:
  - Total lookups; share by `match_level` (`zip+4`, `zip5`, `nearest`, `default`, `error`).
  - Top ZIP5s lacking ZIP+4 coverage.
  - Average fallback distance and distribution.
- **Interpretation guide**: Prioritize sourcing ZIP+4 records for ZIPs with high volume and high fallback distance. Use report to drive data acquisition or enrichment.
- **Future**: Add anomaly flags when `zip5` fallback rate spikes vs 7‑day baseline; surface in Ops dashboard.

## 18) Implementation Notes & Risks (ZIP+4 normalization)
- **Leading zeros**: Preserve leading zeros in plus-four (e.g., `00601-0001` → `plus4='0001'`).
- **Ambiguous input**: Accept `94110-1234` and `941101234`; reject mixed/invalid lengths with a clear 400 error.
- **Non-delivery ZIPs/PO Boxes**: Some ZIP+4 ranges map primarily to PO boxes; ensure we still resolve locality correctly (pricing is unaffected).
- **ZIP changes/retirements**: USPS may reassign or retire ZIPs; rely on valuation-effective mapping and dataset digests for reproducibility.
- **Territories**: For PR/VI/GU/AS/MP, confirm presence in CMS mapping; if missing, return friendly error with guidance.
- **Multiple matches**: If CMS provides overlapping effective windows or duplicates, ingestion must collapse to a single active record per valuation instant.
- **Formatting noise**: Trim spaces/dashes/extra characters; store normalized `(zip5, plus4)`.

