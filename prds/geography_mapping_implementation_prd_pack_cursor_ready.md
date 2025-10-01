# Geography Locality Mapping — PRD (ZIP+4‑first, Cursor‑Ready)

## Changelog (Pinned)
- **2025‑09‑28**: Automation implemented — hourly scheduler with **monthly geography checks**, **ETag/Last‑Modified change detection**, smart **ingest‑on‑change**, notification system (log/email/slack/webhook‑ready), and **operator CLI** (`ingest`, `check‑changes`, `notifications`, `status`).
- **2025‑09‑27**: Incorporated implementation clarifications: geometry table for nearest, PO Box handling, strict error codes, resolver API contract, exclusion constraints for effective windows, cache sizing, and expanded acceptance tests.
- **2025‑09‑26**: Clarifications — non‑strict default with explicit fallback policy; nearest fallback constrained to same state; optional carrier exposure; locality‑name dictionary loader; annual‑as‑quarter fallback; ZIP+4 normalization; digest‑aware cache (TTL optional); conversational strict‑mode errors; per‑request radius override; daily gap‑report methodology.
- **2025‑09‑26**: Initial draft with ZIP+4‑first mandate, effective‑dating rules, schema, ingestion steps, QA, and trace requirements.

---

## 1) Executive Summary
**Problem.** Pricing engines (MPFS, DMEPOS) require precise locality selection. CMS publishes ZIP→Locality mappings (ZIP9) and annual GPCI tables. Historically, ZIP+4 handling, effective‑dating, and fallbacks have been inconsistent.

**Solution.** A deterministic, auditable resolver that maps **ZIP(+4)** + **valuation time** → **MPFS locality**, enforcing **ZIP+4‑first**, **same‑state nearest** fallback, and **strict/non‑strict** modes; backed by a canonical, versioned dataset with run manifests, validation gates, and observable behavior.

**Done means…**
- Test‑first CI gates pass (coverage, perf, validation artifacts). 
- Resolver meets latency SLOs and determinism guarantees. 
- Data is reproducible via snapshot digests; rollback is documented and tested. 
- Operator dashboards and alerts are live; GA blockers closed.

**Non‑goals (for now).** Address→ZIP+4 parsing, CBSA/imputation, Medicaid geographies, county crosswalks.

---

## 2) Success Metrics & SLOs (Definition of Done)
- **Freshness**: ZIP9 snapshot ingested within **2 business days** of CMS release; GPCI within **5 business days**.
- **Latency**: Resolver **p95 ≤ 2ms** warm, **p95 ≤ 20ms** cold; **batch 100 lookups ≤ 5ms** overhead via cache.
- **Reliability**: **99.9%** successful resolutions over 7‑day windows (strict errors excluded). 
- **Coverage**: **≥ 99%** of ZIP5s in pilot states active for valuation period; ZIP+4 coverage reported daily by state.
- **Determinism**: Same inputs + same **snapshot_digest** ⇒ identical outputs (hash‑stable contract tests).
- **Ops SLA**: MTTA ≤ 15 min on red alerts; MTTD ≤ 5 min via automated checks.

SLOs are enforced by CI and monitored in production dashboards; **CI gates** (see §12) block merges when targets aren’t met.

---

## 3) Scope & Assumptions
**In‑scope**: Ingest CMS ZIP9, select effective snapshots, resolve ZIP(+4) with same‑state nearest fallback, expose optional carrier/rural, emit structured traces, publish daily gap reports.

**Assumptions**: CMS ZIP9 includes state per record; consumers provide ZIP in `94110-1234` or `941101234` or `94110` form.

**Territory scope for GA**: **50 states + DC + PR** are **in scope** for GA. **VI, GU, AS, MP** are **post‑GA**: if present in a snapshot, they will resolve; if absent, resolver returns friendly guidance (`GEO_NO_COVERAGE_FOR_PERIOD`).

---

## 4) Canonical Data Model (Versioned/Bitemporal)
**Principles.** Canonical first, then code. All tables carry: **business effective** (`effective_from`, `effective_to`) and **dataset versioning** via `dataset_digest` and `published_at`. Raw artifacts are immutable.

### 4.1 Entities
- **snapshots**: registry of dataset packs (`dataset_id='ZIP_LOCALITY'`, window, `dataset_digest`, `source_url`, `published_at`).
- **geography**: effective‑dated crosswalk; authoritative for resolution.
- **locality_dict** (optional): UI labels for localities.
- **zip_geometry**: lat/lon/state/PO Box flag for nearest logic.

### 4.2 Keys & Invariants
- Primary key: `(zip5, plus4_norm, effective_from)`; exclusion constraint forbids overlapping actives for same `(zip5, plus4_norm)`.
- At any valuation instant, **≤ 1** active `geography` row per `(zip5, plus4)`.
- `plus4` is 4 digits with **leading zeros preserved**; `plus4_norm = COALESCE(plus4,'')`.

---

## 5) Interfaces (API, Warehouse & CLI)
**Resolver API** `POST /geo/resolve`
- **Request**
  - Minimal: `{ zip }` → defaults to **current year (annual)** selection.
  - Date-specific: `{ zip, valuation_date: 'YYYY-MM-DD' }`.
  - Quarter-specific: `{ zip, valuation_year: YYYY, quarter: 1|2|3|4 }`.
  - Annual: `{ zip, valuation_year: YYYY }`.
  - Common options: `strict=false`, `radius={initial, step, max}`, `expose_carrier=false`.
- **Precedence**: `valuation_date` **overrides** `{valuation_year, quarter}` which **overrides** `valuation_year`.
- **Response**: `{ locality_id, state, rural_flag, carrier?, match_level, candidate_zip?, candidate_distance_miles?, snapshot_digest, source:'ZIP_LOCALITY' }`
- **Contract**: OpenAPI stub committed **before code**; contract tests enforce shapes, codes, and latency budgets. **Rate limiting (429)** is in scope for GA. **Caching/ETag** behavior is **post-GA** (documented later).
- **Standards**: API contract and lifecycle must conform to the **API Standards & Architecture PRD v1.0**.

**Healthcheck Endpoint** `GET /geo/healthz` … *(unchanged spec above)*

**Warehouse views** (read‑only): `vw_geo_active(valuation_ts, snapshot_digest)` for analytics and QA.

**Operator CLI** (for ingestion & monitoring)
- `python -m cms_pricing.cli.geography ingest [--dry-run]` — manual ingestion trigger.
- `python -m cms_pricing.cli.geography check-changes` — run ETag/Last‑Modified change detection without downloading.
- `python -m cms_pricing.cli.geography notifications --limit 10` — view recent ingestion notifications.
- `python -m cms_pricing.cli.geography status` — show scheduler/ingestion status and last check times.

---

## Local Runbook / Quickstart (Geometry + Resolver)
**Prereqs**
- Python 3.10+, `poetry install` (or `pip install -r requirements.txt`)
- A Postgres with PostGIS (optional but recommended). Set `DATABASE_URL`.
- Storage directory for source files: set `DATA_DIR=./data` (created if missing).
- Optional PO‑Box flags: download **SimpleMaps US ZIP (free)** CSV and set `SIMPLEMAPS_ZIP_PATH=$DATA_DIR/simplemaps/uszips.csv`. Attribution required (see PRD).

**1) Fetch & load geometry sources**
```bash
# ZCTA Gazetteer (latest)
python -m cms_pricing.cli.geometry ingest-zcta \
  --gazetteer-url https://www2.census.gov/geo/docs/maps-data/data/gazetteer/ \
  --out $DATA_DIR/zcta

# ZIP↔ZCTA crosswalk (UDS, latest)
python -m cms_pricing.cli.geometry ingest-crosswalk \
  --crosswalk-path $DATA_DIR/crosswalk/zip_to_zcta.csv

# (Optional) NBER ZCTA distances
python -m cms_pricing.cli.geometry ingest-nber \
  --nber-path $DATA_DIR/nber/zcta_distances.csv

# (Optional) SimpleMaps ZIP types (PO BOX flag)
python -m cms_pricing.cli.geometry ingest-simplemaps \
  --zip-types $SIMPLEMAPS_ZIP_PATH
```

**2) Build `zip_geometry`**
```bash
python -m cms_pricing.cli.geometry build-zip-geometry \
  --state-source cms  \
  --rule dominant_zcta \
  --write-table zip_geometry
```

**3) Run geometry QA (parity + completeness)**
```bash
# Completeness, null checks, state parity
python -m cms_pricing.cli.geometry qa-geometry --table zip_geometry

# Distance parity (Haversine vs NBER), flags ≥10mi, expect p95 < 1.0mi
python -m cms_pricing.cli.geometry qa-distance-parity \
  --zip-geometry zip_geometry \
  --nber-table zcta_distances \
  --tolerance-mi 1.0 --flag-outlier-mi 10.0
```

**4) Seed CMS geography and run resolver smoke tests**
```bash
# (If not already) ingest CMS ZIP→Locality and register snapshot
python -m cms_pricing.cli.geography ingest --year 2025 --quarter 3

# Resolve with effective dating
curl -s -X POST $BASE_URL/geo/resolve \
  -H 'Content-Type: application/json' \
  -d '{"zip":"94110-1234","valuation_year":2025,"quarter":3}' | jq

# Nearest (non-strict), shows candidate and distance
curl -s -X POST $BASE_URL/geo/resolve \
  -H 'Content-Type: application/json' \
  -d '{"zip":"99999","valuation_date":"2025-03-31","strict":false}' | jq
```

**5) Run tests locally**
```bash
pytest -q tests/test_geography_ingestion.py
pytest -q tests/test_geography_resolver.py -k effective_date
pytest -q tests/test_geography_integration.py
# Enable async tests
pip install pytest-asyncio && pytest -q -k async
```

**Docker (optional)**
```bash
# One-liner local stack (db + api) if docker-compose.yaml is present
docker compose up -d db api
# Then run steps 1–5 against the containerized services
```

---

## 6) Validation Rules (with Severities)
- **ERROR** (block promotion): schema/field type mismatch; invalid ZIP/plus4 lengths; overlapping effective windows for a key; missing state; duplicate `(zip5, plus4_norm, digest, window)`; digest mismatch with manifest.
- **WARN** (promote with caution): carrier/state disagreement vs PFS; unusually low ZIP+4 coverage by state; territory gaps.
- **INFO**: row counts by state; distinct localities; leading‑zero samples.
Artefacts are written as machine‑readable reports (JSON) and human summaries (HTML/Markdown) and attached to CI.

---

## 7) Data Dictionaries & Schemas
- **Human dictionary**: fields, types, semantics, examples (included inline in table definitions).
- **Machine schemas**: YAML/JSON for `geography`, `snapshots`, `zip_geometry` driving: 
  - parser column mapping, 
  - dbt/Great Expectations checks, 
  - API model validation.
Schemas live in `/spec/schemas/*.yaml` and are version‑controlled.

---

## 8) Workflows (Ingest → Normalize → Validate → Publish)
1) **Ingest**: resilient downloader (ETag/Last‑Modified, retries). Store raw ZIP **immutably** with checksum.
2) **Extract**: unzip; convert XLS/XLSX → CSV deterministically.
3) **Normalize**: fixed‑width ZIP9 → normalized rows; uppercase categorical fields; compute `dataset_digest` over **normalized** files.
4) **Validate**: run ERROR/WARN/INFO suite; emit reports; block on ERROR.
5) **Load**: upsert `geography` with exclusion guard; upsert `snapshots` with `published_at`.
6) **Publish**: set `ACTIVE_ZIP_LOCALITY_DIGEST`; record a run manifest (inputs, row counts, timings, checksum) for ops.
7) **Rollback**: documented procedure to switch to a prior `snapshot_digest` and republish caches.

---

## 9) Resolver Behavior (Policy)
- **ZIP+4‑first** → **ZIP5** → **Nearest (same state)** → **Default/Benchmark** (non‑strict) | **Error** (strict).
- **Nearest policy**: start **25mi**, expand **+10mi** to **100mi**; restrict to same state; exclude `is_pobox=true` unless no deliverable candidate within `max_miles`. Tie‑break: shortest distance (hook for min‑GPCI variance later). 
- **Effective‑dating**: choose row whose `[effective_from, effective_to)` covers `valuation_ts`; annual‑only years cover all quarters. Accept `valuation_date` or `{year, quarter}`.
- **Normalization**: accept `94110-1234` and `941101234`; store `(zip5, plus4)`; reject invalid lengths with clear 400.
- **Determinism**: response includes `snapshot_digest`; same inputs + digest ⇒ same output.

**Strict‑mode error codes**: `GEO_NEEDS_PLUS4`, `GEO_NOT_FOUND_IN_STATE`, `GEO_NO_COVERAGE_FOR_PERIOD` with human‑friendly copy.

---

## 10) Observability & Ops (Operator‑First)
- **Per‑request trace** (`geo_resolution`): inputs, `match_level`, output, nearest info, `snapshot_digest`, `latency_ms`, service version.
- **Dashboards**: match mix over time by state; nearest distance histograms; coverage heatmap; ingest freshness.
- **Alerts** (red): `zip+4%` ↓ >5pp d/d, `nearest_p95_miles` ↑ >5mi vs 7‑day baseline, no active snapshot for current quarter, resolver p95 breach. 
- **Routing**: Page **Geo Resolver On‑Call** via the platform’s incident system (e.g., PagerDuty service: `geo-resolver`).
- **Ops SLA**: MTTA ≤ 15 min; playbooks linked from dashboard.
- **Run manifests**: stored per deploy/ingest with checksums and counts.**: stored per deploy/ingest with checksums and counts.

---

## 11) Security, Compliance, & Access
- **RBAC**: read‑only for resolver; write access limited to ingestion service roles.
- **Least privilege** DB roles; 
- **Encryption** in transit and at rest; 
- **Audit logs** on writes/role changes; 
- **Retention**: raw artifacts and published snapshots retained **≥ 24 months**; resolver logs retained **≥ 180 days**; 
- **Privacy**: ZIPs aren’t PHI alone; adhere to CMS redistribution terms.**: ZIPs aren’t PHI alone; adhere to CMS redistribution terms.

---

## 12) Testing Strategy (Test‑First + CI Gates)
- **Policy**: *Write tests from this PRD before writing code.*
- **Unit**: normalization paths (dash/no‑dash, leading zeros), strict vs non‑strict.
- **Integration**: effective‑dating selections; ZIP+4 over ZIP5; nearest same‑state; PO Box exclusion.
- **Data**: schema & content validations via dbt/GE; exclusion constraint overlap tests.
- **Contract**: OpenAPI conformance; enumerated errors; latency budgets.
- **Perf**: warm/cold p95; batch overhead. 
- **Golden fixtures**: 10–20 ZIP+4 across 3 states (urban/rural, border, leading zero, PO Box). 

### What is a **CI gate**?
A **CI gate** is an automated check in Continuous Integration that **must pass** before code can merge or deploy. Gates make the PRD enforceable.

### CI Gates (blocking)
- **Coverage ≥ 85%** (resolver + ingest) measured on changed packages.
- **Perf SLOs met** in synthetic CI runs (warm p95 ≤ 2ms; cold p95 ≤ 20ms; batch ≤ 5ms overhead).
- **Validation artifacts present** (schema reports, overlap checks, row counts) and **no ERRORs**.
- **Geometry gates**: completeness ≥ 99%, **no NULL** lat/lon/state, **state parity** with CMS, **distance parity** (sampled p95 < 1.0 mi; **no** outlier ≥ 10 mi untriaged).
- **Contract tests green** for `/geo/resolve` (status codes, shapes, error enums) **and** for `geo_resolution` trace schema and **territory behavior**.


---

## CMS Ingestion — Release Cadence & Package Contents
- **ZIP→Locality (CMS ZIP files):** Released **quarterly**. Each release contains **two logical files**: **ZIP5** and **ZIP9 (ZIP+4)**, usually packaged as CMS **ZIP archives** containing **CSV/TXT** (occasionally XLS/XLSX).
- **MPFS content:** RVU bundles **A/B/C/D** (quarterly) and a **separate GPCI** file per year.
- **Archival note:** CMS pages typically surface the **latest** quarter only; historical ZIP→Locality sets are not guaranteed to remain hosted. We **archive every retrieved package** internally for reproducibility and back‑tests.

## Ingestion Poller & Archival Policy
- **Cadence:** Poll **quarterly** aligned with CMS releases **plus** a **monthly sanity check** to detect late updates or page/layout changes.
- **Per‑release fetch:** Always download **both** ZIP files (ZIP5 and ZIP9) for the target period; for MPFS also fetch the **RVU A/B/C/D** bundle and the **GPCI** file for the year.
- **Archival:** Store every downloaded archive **immutably** with checksum and metadata (source URL, release label, year/quarter). Archives support incident rollback and historical testing.

## Historical Testing Capability
- The API and warehouse support evaluating historical valuations using retained snapshots. Back‑tests of **up to 9 years** are supported **if archived snapshots exist**. If a requested valuation date lacks a snapshot, return `GEO_NO_COVERAGE_FOR_PERIOD` with guidance.

## Retention & Historical Data (Addendum)
- **Hot (DB):** Published snapshots and resolver artifacts retained **≥ 24 months**.
- **Cold archives:** Immutable CMS downloads (ZIP→Locality, RVU, GPCI) retained **≥ 9 years** in low‑cost storage with checksums.
- **Logs:** Resolver logs retained **≥ 180 days**.

## CMS Ingestion Automation (Scheduler, Change Detection, Notifications, CLI)
**Scheduler cadence**
- An **hourly scheduler** evaluates timers and triggers a **monthly** geography check (aligned to CMS cadence) in addition to planned **quarterly** pulls.

**Change detection**
- Before download, perform **HTTP HEAD** to compare **ETag** and **Last‑Modified** with stored values.
- If **no changes**, ingestion is **skipped** with a `no_changes` decision recorded.
- If headers are missing/unreliable or check fails, **fallback** to a safe full download.

**Smart ingest‑on‑change**
- Geography ingestor runs only when change detection indicates deltas; avoids redundant downloads and processing.

**Notifications**
- Emit structured events: `ingestion_started`, `changes_detected`, `ingestion_completed`, `ingestion_error`.
- Channels: log (default), email, Slack, webhook (extensible). Supports filtering and recent‑activity suppression.

**Operator CLI**
- `python -m cms_pricing.cli.geography ingest [--dry-run]` — manual ingestion trigger.
- `python -m cms_pricing.cli.geography check-changes` — run ETag/Last‑Modified check without downloading.
- `python -m cms_pricing.cli.geography notifications --limit 10` — view recent notifications.
- `python -m cms_pricing.cli.geography status` — show scheduler/ingestion status and last check time.

**Observability hooks**
- Dashboards include scheduler health, last successful check timestamps, and counts of `changes_detected` vs `no_changes`.
- Alerts fire on scheduler inactivity > 2h, missing expected CMS files, or repeated `ingestion_error` events.

---

## ZIP Geometry Sourcing & Nearest Logic (ZCTA‑based)
**Goal.** Build `zip_geometry(zip5, lat, lon, state, is_pobox)` to power **same‑state nearest** fallback with auditable sources.

**Sources & choices**
- **Coordinates:** **Most recent Census ZCTA Gazetteer** (representative point per ZCTA).
- **ZIP↔ZCTA crosswalk:** **Most recent UDS Mapper** vintage (from the OpenICPSR page linked in the PRD notes).
- **ZIP→coordinate rule:** **Dominant ZCTA** per ZIP (max weight; if weights absent, treat equally and pick lowest ZCTA deterministically).
- **State SoT:** Populate `zip_geometry.state` from **CMS ZIP9 geography** (ensures consistency with locality selection and same‑state policy). Keep ZCTA/crosswalk state only for QA.
- **PO Box flag:** Augment with **SimpleMaps (free)** `type` field to set `is_pobox = (type = 'PO BOX')`. *Attribution:* “ZIP type © SimpleMaps (free license).”
- **Distances:** Primary runtime = **Haversine** on ZIP centroids; **optional codepath** to read **NBER ZCTA→ZCTA** distances when available.

**Build steps**
1) Ingest latest **ZCTA Gazetteer** → `zcta_coords(zcta5, lat, lon, vintage)`.
2) Ingest latest **UDS ZIP↔ZCTA** → `zip_to_zcta(zip5, zcta5, weight NULL, vintage)`.
3) Compute **dominant ZCTA** per ZIP → `zip_dominant(zip5, zcta5, rule)`.
4) Join to coords → `zip_geom_base(zip5, lat, lon)`.
5) Join CMS ZIP9 state → `zip_state(zip5, state)`.
6) Left‑join SimpleMaps type → set `is_pobox` (default FALSE).
7) Materialize **`zip_geometry`** with indices (and spatial index if PostGIS).
8) (Optional) Load **NBER ZCTA distances** → `zcta_distances(zcta5_a, zcta5_b, miles)` for validation/fast path.

**Resolver usage**
- Candidate set = `zip_geometry` where `state == input_state`.
- Radius policy: start **25mi**, expand by **+10mi** to **100mi**.
- Exclude `is_pobox=TRUE` unless **no deliverable** candidate within `max_miles`.
- Distance = **Haversine** (or **NBER** value via ZIP→ZCTA mapping when present).
- Tie‑break: shortest distance (hook for “min GPCI variance” later).

**Validation & CI gates (geometry)**
- **Completeness:** `zip_geometry` covers **≥ 99%** of active CMS ZIP5s; **no NULL** lat/lon/state.
- **State parity:** `zip_geometry.state` **must equal** CMS ZIP9 state for each ZIP5.
- **Distance parity:** Sampled pairs per state: `| Haversine(ZIPa, ZIPb) − NBER(ZCTAa, ZCTAb) | < 1.0 mi`. **Flag** any case **≥ 10 mi** with ZIP/ZCTA/state for review.
- **PO Box behavior:** Tests prove PO BOX excluded when deliverable exists; included only when no deliverable within `max_miles`.
- **Determinism:** Rebuild with same inputs yields identical `zip_geometry` (digest over table rows).

**Storage, refresh, trade‑offs**
- **Storage:** Archive raw Gazetteer/crosswalk/NBER files immutably with checksums + vintage; load to warehouse tables.
- **Refresh cadence:** **Annual** rebuild (or when a newer Gazetteer/crosswalk vintage appears); record vintage in `zip_geometry.vintage`.
- **Cons to note:** (a) ZCTA ≠ USPS ZIP (approximation); (b) Census vintages lag USPS changes; (c) dominant‑ZCTA may not reflect split ZIP geography perfectly; (d) SimpleMaps free license requires attribution; (e) Dual engines (ZIP Haversine vs ZCTA NBER) create small, expected deltas—gated by <1 mi tolerance and ≥10 mi alerts.

---

## 13) GA Hardening Plan (Updated)
**Blockers to GA** (must be green, with tests):
1) **Effective‑date selection** — support `{year, quarter}` and `valuation_date`; select rows where `[effective_from, effective_to)` covers the instant; annual‑only years cover all quarters. **Tests:** unit + integration + contract.
2) **Geometry‑based nearest** — in‑state only; radius 25→100mi (step 10mi); exclude `is_pobox=true` unless no deliverable candidate within `max_miles`. **Tests:** cross‑border refusal, PO Box exclusion/allowance, response & trace include `candidate_zip` and `candidate_distance_miles`.
3) **Structured tracing** — emit `geo_resolution` for 100% of calls. **Contract tests:** schema fields (`inputs`, `match_level`, `output`, `nearest`, `snapshot_digest`, `latency_ms`, `service_version`) and N API calls ⇒ N traces.
4) **Snapshot/digest pinning** — compute/store `dataset_digest`; resolver selects by configured digest or by valuation date when unset. **Tests:** reproducibility under new ingests; outputs change only when active digest flips.
5) **OpenAPI published** — resolver endpoint documented and validated. **CI:** contract tests for success + error enums; latency budgets enforced.
6) **Exclusion/overlap enforcement** — ingestion rejects overlapping active windows for the same `(zip5, plus4_norm)` with clear errors. **CI:** failing ingest test proves guard.
7) **Territory behavior** — GA scope = 50 states + DC + PR; VI/GU/AS/MP post‑GA. **Tests:** PR success; friendly `GEO_NO_COVERAGE_FOR_PERIOD` when a post‑GA territory is absent in a snapshot.
8) **CI gates configured** — coverage ≥ 85%, perf SLOs, validation artifacts with no ERRORs, resolver/trace/territory contract tests all green.

**Exit criteria:** All blockers above are green; dashboards and red‑alerts live; on‑call playbooks linked; performance SLOs stable for **48h** under shadow/production traffic.

## 14) Risks & Mitigations
- **Naming drift / schema change upstream** → lock parser with machine schemas; alert on schema deltas.
- **Key collisions / overlaps** → exclusion constraints + ingest blocker.
- **Upstream outages** → retries, mirror cache, alert on freshness SLO breach.
- **Scale / cache churn** → digest‑aware cache, memory budget docs, pre‑warm hot states.
- **Border anomalies** → explicit tests; operator report for nearest misuse.

---

## 15) Operator Guide — Health Questions
- *What snapshot digest is active and for what date window?* (health endpoint)
- *What is today’s ZIP+4 coverage by state and the top gaps?* (gap report)
- *Are nearest fallbacks increasing?* (match‑mix panel, distance histogram)
- *Did p95 latency or error rates breach SLOs today?* (SLO panel)
- *Can I reproduce last week’s result?* (pin prior digest and re‑run)

---

## 16) TODO / Next Actions
- **Effective‑dating API & tests** — *Owner:* Eng — *Target:* YYYY‑MM‑DD
- **Nearest (geometry) + PO Box policy** — *Owner:* Eng — *Target:* YYYY‑MM‑DD
- **Structured tracing + dashboards + alerts** — *Owner:* Data/DevEx — *Target:* YYYY‑MM‑DD
- **Snapshot digests & rollback drill** — *Owner:* Eng — *Target:* YYYY‑MM‑DD
- **dbt/GE validations wired to CI** — *Owner:* Data — *Target:* YYYY‑MM‑DD

---

## 16.1) QA Summary (per QA & Testing Standard v1.0)
| Item | Details |
| --- | --- |
| **Scope & Ownership** | Geography locality mapping resolver + ingestion pack; engineering owner the Geography/Locality squad with QA partner from the Quality Engineering guild; consumers include pricing, analytics, and downstream resolvers. |
| **Test Tiers & Coverage** | Unit/component: `tests/test_geography_ingestion.py`, `tests/test_geography_resolver.py`, `tests/test_geometry_nearest_logic.py`; Integration: `tests/test_geography_integration.py`, `tests/test_geography_automation.py` exercising CLI + scheduler; Scenario/E2E: `tests/test_state_boundary_enforcement.py`, `tests/test_state_boundary_simple.py`, gap-report assertions. Coverage currently ~82% for geography modules (target 90%; tracked via coverage dashboard). |
| **Fixtures & Baselines** | CMS ZIP5/ZIP9 and layout fixtures under `sample_data/zplc_oct2025/`; golden gap-report snapshots archived in `tests/golden/test_scenarios.jsonl`; nearest-distance baselines derived from `sample_data/nber_sample_1000.csv` with digests tracked in QA monitoring dashboards. |
| **Quality Gates** | Merge pipeline (`ci-unit.yaml`) blocks on geography unit/component suites and schema diffs; `ci-integration.yaml` provisions Postgres + runs resolver integration tests; nightly `ci-nightly.yaml` replays ingest + nearest scenarios with drift alerts blocking release. |
| **Production Monitors** | Automated scheduler exports snapshot digest + coverage metrics; gap report + match-mix dashboards alert when coverage or nearest fallbacks exceed thresholds; latency SLO monitors watch resolver p95 per §2. |
| **Manual QA** | Operator CLI (`python -m cms_pricing.cli.geography …`) used for pre-cutover smoke; manual review of coverage dashboard + change notifications before enabling new snapshots. |
| **Outstanding Risks / TODO** | Close open TODOs in §16 (effective dating API, geometry policy); expand automated rural flag validation; backfill historical digest comparison for new states. |

## 17) Appendices
### 17.1 Tables (field‑level dictionary)
**snapshots**: `dataset_id`, `effective_from`, `effective_to`, `dataset_digest`, `source_url`, `published_at`, `created_at`

**geography**: `zip5`, `plus4`, `plus4_norm`, `has_plus4`, `state`, `locality_id`, `locality_name?`, `carrier?`, `rural_flag?`, `effective_from`, `effective_to`, `dataset_id`, `dataset_digest`, `created_at`

**zip_geometry**: `zip5`, `lat`, `lon`, `state`, `is_pobox`, `effective_from`, `effective_to`

### 17.2 Error Codes (authoritative)
`GEO_NEEDS_PLUS4`, `GEO_NOT_FOUND_IN_STATE`, `GEO_NO_COVERAGE_FOR_PERIOD` — messages must include remediation and policy summary.
