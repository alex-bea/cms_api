# Nearest ZIP (Same‑State) Resolver & Data Integration — PRD

**Version:** 1.0  
**Date:** 2025‑09‑29  
**Owner:** Arnina / INS/Prawn BBQ  
**Status:** Draft — ready for engineering

---

## 1) Objective
Given a **Starting ZIP** (ZIP5 or ZIP9), return the **nearest non‑PO‑Box ZIP5 in the same CMS state** using Census Gazetteer centroids, with an optional NBER fast‑path; emit a full trace for QA and pricing parity tests.

---

## 1.1) Issues & Critical Risks (NEW)
The resolver depends on multiple external datasets with varying stability. We proactively manage risk via process and architecture.

| PRD Section | Issue/Risk | Guideline/Mitigation |
|:--|:--|:--|
| §5 Data Sources | **Brittle ingest URLs** (gated or revision‑varying links, e.g., SimpleMaps, CMS ZIP/ZIP9). | **Mandatory Ingest Layer**: all source files must be staged in a controlled internal bucket (data lake). `ingest_runs` tracks the **internal URI** (e.g., `s3://…/raw/…`) as the canonical path; public URLs are for provenance only. |
| §4/§8 | **Uncertain population for tiebreakers**: SimpleMaps `population` may be null when `zcta_bool=false`. | **Tie‑Breaker Coalescing**: use `COALESCE(population, 0)` so unknown/zero population ZIPs sort as “smaller”. |
| §8 Step 2 | **Centroid fallback risk**: reliance on NBER centroid when Gazetteer row is missing. | **Fallback Monitoring**: add **Gazetteer Fallback Rate** KPI/alert; if >0.1% over 24h, open a **P1** to investigate Census file integrity. |
| §8/§9 | **Empty candidate set** (e.g., state has only the starting ZIP or all others filtered as PO boxes). | **Defined Error**: return `NO_CANDIDATES_IN_STATE` (HTTP 422) with diagnostics; never index `R[0]` on empty list. |

---

## 2) Scope
**In:**
- Normalization of ZIP5/ZIP9 → ZCTA5
- CMS-state–constrained nearest ZIP lookup
- Distance engine (Haversine primary; NBER fast-path/validation)
- PO Box filtering via SimpleMaps
- Full traces for observability and QA

All HTTP interfaces produced by this resolver must follow the **Global API Program PRDs (v1.0)** for contract, versioning, and change management.

**Out (future):**
- Multi‑state search
- Polygonal distances or road‑network distances
- UI disambiguation/UX

---

## 3) Inputs & Outputs
**Inputs (accepted):**
- `zip`: string — either **ZIP5** (e.g., `94107`) or **ZIP9** (e.g., `94107-1234` / `941071234`)

**Outputs:**
- `nearest_zip` (ZIP5)
- `distance_miles` (float)
- `trace` (JSON) — full pipeline provenance (see §10)

---

## 4) Key Assumptions & Business Rules
- **ZIP↔ZCTA mapping:** Prefer UDS crosswalk rows where **relationship = "Zip matches ZCTA"**; otherwise choose the row with the **largest weight** (persist weight).
- **State source of truth:** **CMS ZIP→Locality** (ZIP9 override takes precedence when applicable). USPS state codes are fallback only.
- **ZIP9 override:** If input is ZIP9 and falls within a CMS ZIP9 range, use ZIP9‑based **locality/state** (set `zip9_hit = true`).
- **PO Box filter:** Compute `is_pobox = (!zcta && !military)` from SimpleMaps; exclude all `is_pobox = true` from candidate set.
- **Threshold flags:** `< 1.0 mi` → `coincident`; `> 10.0 mi` → `far_neighbor`.
- **Ties:** Break by (1) **smaller population** using **`COALESCE(population, 0)`**, then (2) **lexicographic** `zip` ascending.

---

## 5) Data Sources (Latest)
> All links are **landing + direct** (where exposed). Keep raw layouts alongside ETL. Load latest; retain prior vintage with `vintage`/`effective_from`.

| Name | File name (direct) | Direct file URL | Landing URL | Vintage | Format | Primary fields we use | Notes |
|---|---|---|---|---|---|---|---|
| **Census ZCTA Gazetteer (National)** | `2025_Gaz_zcta_national.zip` | https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_zcta_national.zip | https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html | 2025 | ZIP → pipe‑delimited TXT | `GEOID`(ZCTA5), `INTPTLAT`, `INTPTLONG`, `ALAND*`, `AWATER*` | Authoritative centroids for ZCTA5. |
| **UDS/GeoCare ZIP→ZCTA Crosswalk (current)** | `ZIP Code to ZCTA Crosswalk.xlsx` | https://data.hrsa.gov/DataDownload/GeoCareNavigator/ZIP%20Code%20to%20ZCTA%20Crosswalk.xlsx | https://udsmapper.org/zip-code-to-zcta-crosswalk/ | 2023 current | XLSX | `zip5`, `zcta5`, `relationship`, (*`weight` if present*), `city`, `state` | Prefer exact ZIP=ZCTA; else highest weight. |
| **NBER ZCTA Distance (100‑mile CSV)** | `gaz2024zcta5distance100miles.csv` | https://data.nber.org/distance/zip/2024/100miles/gaz2024zcta5distance100miles.csv | https://www.nber.org/research/data/zip-code-distance-database | 2024 (posted 2025) | CSV | `zip1`(ZCTA), `zip2`(ZCTA), `mi_to_zcta5` | Optional fast‑path & validation; also `/5miles`, `/25miles`, `/50miles`, `/500miles`, `/centroid/`. |
| **NBER ZCTA Centroids (fallback)** | (dir listing) | https://data.nber.org/distance/zip/2024/centroid/ | https://www.nber.org/research/data/zip-code-distance-database | 2024 | CSV | `zcta5`, `lat`, `lon` | Use only if Gazetteer missing a row. |
| **SimpleMaps US ZIPs (free)** | `uszips.csv` or `.xlsx` | *Gated download (no stable direct URL); file provided post‑form* | https://simplemaps.com/data/us-zips | 2025‑06‑07 | CSV/XLSX | `zip`, `zcta`(bool), `parent_zcta`, `military`(bool), `population`, `timezone`, county/CBSA fields | Derive `is_pobox = (!zcta && !military)`. **Attribution required** in production. |
| **CMS ZIP→Locality (ZIP5 pack)** | e.g., `Zip Code to Carrier Locality File – Revised 2025-08-14.zip` | *Direct link appears in the CMS page’s Downloads box (URL varies per revision)* | https://www.cms.gov/medicare/payment/fee-schedules | 2025‑08‑14 | ZIP (CSV/TXT) | `zip5`, `state`, `carrier_mac`, `locality`, `rural_flag` | Canonical state for filtering. |
| **CMS ZIP9 add‑on** | e.g., `Zip Codes Requiring 4 Extension – Revised 2025-08-14.zip` | *Direct link appears in the CMS page’s Downloads box (URL varies per revision)* | https://www.cms.gov/medicare/payment/fee-schedules | 2025‑08‑14 | ZIP (CSV/TXT) | `zip9_low`, `zip9_high`, `state`, `locality`, flags | Use to enforce ZIP9 locality/state; set `zip9_hit`. |
| **CMS GPCI (Addendum E)** | e.g., `cy-2025-pfs-final-rule-addenda.zip` (contains Addendum E) | *Direct link posted on CY rule page when finalized* | https://www.cms.gov/medicare/payment/fee-schedules/physician | Annual | ZIP (XLSX/CSV inside) | `locality_id`, `GPCI_work`, `GPCI_PE`, `GPCI_MP` | For downstream pricing parity tests. |
| **CMS RVU bundles A/B/C/D** | `RVU25A.zip`, `RVU25B.zip`, `RVU25C.zip`, `RVU25D.zip` | Direct links on each RVU page | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files | 2025 (Q1–Q4) | ZIP (CSV) | `HCPCS`, `modifier`, RVUs (Work/PE_fac/PE_nonfac/MP), status/policy indicators | For resolver tests & parity. |
| **HUD–USPS ZIP Crosswalks** | per‑geo files | *Behind login; no public direct URLs* | https://www.huduser.gov/portal/datasets/usps_crosswalk.html | 2025‑02 | CSV/XLSX | `ZIP`, target geo IDs + `res`/`bus`/`oth` weights | QA & analytics only. |
| **Census TIGER/Line ZCTA5 Shapefile** | `tl_2022_us_zcta520.zip` | https://www2.census.gov/geo/tiger/TIGER2022/ZCTA520/tl_2022_us_zcta520.zip | https://catalog.data.gov/dataset/tiger-line-shapefile-2022-nation-u-s-2020-census-5-digit-zip-code-tabulation-area-zcta5 | 2022 | Shapefile | `ZCTA5CE20`, geometry | Optional; only if recomputing centroids. |

---

## 6) Local Data Model (Target Tables)
```sql
-- Gazetteer centroids
CREATE TABLE zcta_coords (
  zcta5 CHAR(5) PRIMARY KEY,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  vintage VARCHAR(10) NOT NULL,
  source_filename TEXT,
  ingest_run_id UUID
);

-- ZIP ↔ ZCTA crosswalk
CREATE TABLE zip_to_zcta (
  zip5 CHAR(5) PRIMARY KEY,
  zcta5 CHAR(5) NOT NULL,
  relationship TEXT,
  weight NUMERIC,
  city TEXT,
  state TEXT,
  vintage VARCHAR(10) NOT NULL,
  source_filename TEXT,
  ingest_run_id UUID
);

-- CMS ZIP5 → locality/state
CREATE TABLE cms_zip_locality (
  zip5 CHAR(5) PRIMARY KEY,
  state CHAR(2) NOT NULL,
  locality VARCHAR(10) NOT NULL,
  carrier_mac VARCHAR(10),
  rural_flag BOOLEAN,
  effective_from DATE,
  effective_to DATE,
  vintage VARCHAR(10) NOT NULL,
  source_filename TEXT,
  ingest_run_id UUID
);

-- CMS ZIP9 override ranges
CREATE TABLE zip9_overrides (
  zip9_low CHAR(9) NOT NULL,
  zip9_high CHAR(9) NOT NULL,
  state CHAR(2) NOT NULL,
  locality VARCHAR(10) NOT NULL,
  rural_flag BOOLEAN,
  vintage VARCHAR(10) NOT NULL,
  source_filename TEXT,
  ingest_run_id UUID,
  PRIMARY KEY (zip9_low, zip9_high)
);

-- Optional: NBER distances (subset import by radius)
CREATE TABLE zcta_distances (
  zcta5_a CHAR(5) NOT NULL,
  zcta5_b CHAR(5) NOT NULL,
  miles DOUBLE PRECISION NOT NULL,
  vintage VARCHAR(10) NOT NULL,
  source_filename TEXT,
  ingest_run_id UUID,
  PRIMARY KEY (zcta5_a, zcta5_b)
);

-- SimpleMaps ZIP metadata (for PO Box flag)
CREATE TABLE zip_metadata (
  zip5 CHAR(5) PRIMARY KEY,
  zcta_bool BOOLEAN,
  parent_zcta CHAR(5),
  military_bool BOOLEAN,
  population INTEGER,
  is_pobox BOOLEAN GENERATED ALWAYS AS (
    CASE WHEN zcta_bool = FALSE AND COALESCE(military_bool, FALSE) = FALSE THEN TRUE ELSE FALSE END
  ) STORED,
  vintage VARCHAR(10) NOT NULL,
  source_filename TEXT,
  ingest_run_id UUID
);

-- Ingest run provenance
CREATE TABLE ingest_runs (
  run_id UUID PRIMARY KEY,
  source_url TEXT NOT NULL,
  filename TEXT,
  sha256 CHAR(64),
  bytes BIGINT,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL,
  finished_at TIMESTAMP WITH TIME ZONE,
  row_count BIGINT,
  tool_version TEXT,
  status TEXT CHECK (status IN ('success','failed','partial')),
  notes TEXT
);

-- Helpful indexes
CREATE INDEX idx_cms_zip_locality_state ON cms_zip_locality(state);
CREATE INDEX idx_zip_to_zcta_zcta5 ON zip_to_zcta(zcta5);
CREATE INDEX idx_zcta_coords_vintage ON zcta_coords(vintage);
CREATE INDEX idx_zcta_distances_vintage ON zcta_distances(vintage);
CREATE INDEX idx_zip_metadata_is_pobox ON zip_metadata(is_pobox);
```sql
-- Gazetteer centroids
CREATE TABLE zcta_coords (
  zcta5 CHAR(5) PRIMARY KEY,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  vintage VARCHAR(10) NOT NULL
);

-- ZIP ↔ ZCTA crosswalk
CREATE TABLE zip_to_zcta (
  zip5 CHAR(5) PRIMARY KEY,
  zcta5 CHAR(5) NOT NULL,
  relationship TEXT,
  weight NUMERIC,
  city TEXT,
  state TEXT,
  vintage VARCHAR(10) NOT NULL
);

-- CMS ZIP5 → locality/state
CREATE TABLE cms_zip_locality (
  zip5 CHAR(5) PRIMARY KEY,
  state CHAR(2) NOT NULL,
  locality VARCHAR(10) NOT NULL,
  carrier_mac VARCHAR(10),
  rural_flag BOOLEAN,
  effective_from DATE,
  effective_to DATE,
  vintage VARCHAR(10) NOT NULL
);

-- CMS ZIP9 override ranges
CREATE TABLE zip9_overrides (
  zip9_low CHAR(9) NOT NULL,
  zip9_high CHAR(9) NOT NULL,
  state CHAR(2) NOT NULL,
  locality VARCHAR(10) NOT NULL,
  rural_flag BOOLEAN,
  vintage VARCHAR(10) NOT NULL,
  PRIMARY KEY (zip9_low, zip9_high)
);

-- Optional: NBER distances (subset import by radius)
CREATE TABLE zcta_distances (
  zcta5_a CHAR(5) NOT NULL,
  zcta5_b CHAR(5) NOT NULL,
  miles DOUBLE PRECISION NOT NULL,
  vintage VARCHAR(10) NOT NULL,
  PRIMARY KEY (zcta5_a, zcta5_b)
);

-- SimpleMaps ZIP metadata (for PO Box flag)
CREATE TABLE zip_metadata (
  zip5 CHAR(5) PRIMARY KEY,
  zcta_bool BOOLEAN,
  parent_zcta CHAR(5),
  military_bool BOOLEAN,
  population INTEGER,
  is_pobox BOOLEAN GENERATED ALWAYS AS (
    CASE WHEN zcta_bool = FALSE AND COALESCE(military_bool, FALSE) = FALSE THEN TRUE ELSE FALSE END
  ) STORED,
  vintage VARCHAR(10) NOT NULL
);
```

---

## 7) ETL Overview
1. **Gazetteer (ZCTA):** Load national file → `zcta_coords` (store `vintage`, `source_filename`, `ingest_run_id`).
2. **UDS ZIP↔ZCTA:** Load Excel → `zip_to_zcta` (normalize `relationship`; store provenance fields).
3. **CMS ZIP5:** Load latest ZIP pack → `cms_zip_locality` (preserve `effective_from/to` when present; store provenance).
4. **CMS ZIP9:** Load override ranges → `zip9_overrides` with provenance fields.
5. **SimpleMaps:** Load CSV/XLSX → `zip_metadata` (populate derived `is_pobox`; store provenance; ensure license note).
6. **NBER:** Optionally load radius files → `zcta_distances` (default 100‑mile) with provenance.
7. **Ingest logging:** Each step writes an entry to `ingest_runs` with checksum, byte size, and row counts; tables reference `ingest_run_id`.

---

## 8) Algorithm (Detailed)
**Step 1 — Parse & Normalize**
- Accept input `zip` (ZIP5 or ZIP9). Strip non‑digits. If 9 digits, set `starting_zip5 = first 5`, `starting_zip9 = all 9`; else only `starting_zip5`.
- **State & locality:**
  - If `starting_zip9` exists and falls within `zip9_overrides`, set `state0`, `locality0`, `zip9_hit = true`.
  - Else set `state0`, `locality0` from `cms_zip_locality` by `starting_zip5`, `zip9_hit = false`.
- **ZCTA:** From `zip_to_zcta` prefer `relationship='Zip matches ZCTA'`; otherwise choose row with max `weight`. Persist `zcta_weight`.

**Step 2 — Starting centroid**
- From `zcta_coords` → `INTPTLAT/INTPTLONG` (`lat0`, `lon0`). If missing, fallback to NBER centroid.
- **KPI:** Track **Gazetteer Fallback Rate**; alert P1 if >0.1% daily.

**Step 3 — Candidate universe**
- From `cms_zip_locality`, select all ZIP5 where `state = state0` and `zip5 != starting_zip5`.
- Join to `zip_metadata` and **exclude** `is_pobox = true`.
- Map each candidate ZIP5 to ZCTA via `zip_to_zcta` (same rules as above).
- **Empty set handling:** if no candidates remain, return error `NO_CANDIDATES_IN_STATE` with trace context.

**Step 4 — Distances**
- **Fast‑path:** If `zcta_distances` loaded, try lookup `(zcta0, zcta_c)` (order‑insensitive). If present, `miles_nber`.
- **Primary:** Look up both ZCTAs in `zcta_coords` and compute Haversine `miles_hav`.
- **Chosen:** `miles_final = COALESCE(miles_nber, miles_hav)`; store both when available. If `|miles_nber - miles_hav| > 1.0`, add trace flag `NBER_HAV_DISCREPANCY` and use Haversine as source of truth.

**Step 5 — Selection & Ties**
- Sort candidates by `miles_final ASC`.
- Drop `miles_final = 0.0` (self / identical centroid) rows.
- Ties within **0.1 mi**: prefer **smaller COALESCE(population,0)**; second tiebreaker: lexical `zip`.

---

## 9) Pseudocode
```text
parse_input(s):
  d = digits_only(s)
  if len(d) == 9: return d[:5], d
  if len(d) == 5: return d, null
  error("invalid_zip")

normalize(zip5, zip9):
  if zip9 and in_zip9_overrides(zip9):
    state0, locality0 = cms_zip9_lookup(zip9)
    zip9_hit = true
  else:
    state0, locality0 = cms_zip5_lookup(zip5)
    zip9_hit = false
  zcta0, weight0 = uds_primary_zcta(zip5)
  lat0, lon0 = zcta_coord(zcta0)  # nber fallback if null
  return {zcta0, weight0, state0, locality0, lat0, lon0, zip9_hit}

nearest_same_state(start_zip):
  zip5, zip9 = parse_input(start_zip)
  ctx = normalize(zip5, zip9)
  C = cms_all_zip5_in_state(ctx.state0) 
  C = C - {zip5}
  C = filter_out_pobox(C)  # simplemaps
  if C is empty: error("NO_CANDIDATES_IN_STATE")
  results = []
  for zip_c in C:
    zcta_c, wt_c = uds_primary_zcta(zip_c)
    miles_nber = nber_lookup(ctx.zcta0, zcta_c)
    lat_c, lon_c = zcta_coord(zcta_c)
    miles_hav = haversine(ctx.lat0, ctx.lon0, lat_c, lon_c)
    miles_final = miles_nber if miles_nber is not null else miles_hav
    if miles_nber is not null and abs(miles_nber - miles_hav) > 1.0:
      add_trace_flag("NBER_HAV_DISCREPANCY")
      miles_final = miles_hav
    results.add({zip_c, zcta_c, miles_final, miles_hav, miles_nber, wt_c})
  R = sort_by_distance_and_ties(results)
  return R[0].zip_c, trace(ctx, R)
```

```text
parse_input(s):
  d = digits_only(s)
  if len(d) == 9: return d[:5], d
  if len(d) == 5: return d, null
  error("invalid_zip")

normalize(zip5, zip9):
  if zip9 and in_zip9_overrides(zip9):
    state0, locality0 = cms_zip9_lookup(zip9)
    zip9_hit = true
  else:
    state0, locality0 = cms_zip5_lookup(zip5)
    zip9_hit = false
  zcta0, weight0 = uds_primary_zcta(zip5)
  lat0, lon0 = zcta_coord(zcta0)  # nber fallback if null
  return {zcta0, weight0, state0, locality0, lat0, lon0, zip9_hit}

nearest_same_state(start_zip):
  zip5, zip9 = parse_input(start_zip)
  ctx = normalize(zip5, zip9)
  C = cms_all_zip5_in_state(ctx.state0) \ {zip5}
  C = filter_out_pobox(C)  # simplemaps
  results = []
  for zip_c in C:
    zcta_c, wt_c = uds_primary_zcta(zip_c)
    miles_nber = nber_lookup(ctx.zcta0, zcta_c)
    lat_c, lon_c = zcta_coord(zcta_c)
    miles_hav = haversine(ctx.lat0, ctx.lon0, lat_c, lon_c)
    miles_final = miles_nber if miles_nber is not null else miles_hav
    results.add({zip_c, zcta_c, miles_final, miles_hav, miles_nber, wt_c})
  R = sort_by_distance_and_ties(results)
  return R[0].zip_c, trace(ctx, R)
```

---

## 10) Trace Schema (Observability)
Sample fields to emit per lookup:
```json
{
  "input": {"zip": "94107-1234", "zip5": "94107", "zip9": "941071234"},
  "normalization": {"starting_zcta": "94107", "zcta_weight": 1.0, "state": "CA", "locality": "...", "zip9_hit": true},
  "starting_centroid": {"lat": 37.76, "lon": -122.39, "source": "gazetteer"},
  "candidates": {"state_zip_count": 1497, "excluded_pobox": 312},
  "dist_calc": {"engine": "nber|haversine", "nber_hits": 1420, "fallbacks": 65},
  "result": {"nearest_zip": "94110", "distance_miles": 1.42},
  "flags": {"coincident": false, "far_neighbor": false}
}
```

---

## 11) Validation & QA
### Test-first plan
- **Fixtures:** Tiny hand‑curated CSV/XLSX per source (1–3 rows) + a small multi‑state sample of real rows.
- **ETL tests:** schema compliance; required fields non‑null; enum/domain checks; cross‑table referential checks (every `zip5` maps to a `zcta5`).
- **Algorithm tests:** ZIP9 override (hit/miss); PO‑box filtering; tie‑breakers (population then lexicographic using `COALESCE(population,0)`); NBER vs. Haversine parity thresholds (median |Δ| < 0.2 mi; p95 < 1.0 mi); edge cases (invalid zip, no UDS row, missing centroid → NBER fallback).
- **Golden tests:** Frozen inputs → expected nearest zip, distance, and flags.
- **Property tests:** distance symmetry; self‑distance = 0; same ZCTA neighbors < 1 mi.
- **Asymmetry Warning (NEW):** For a sample, compute A→B nearest, then B→A; if not reciprocal, log **Asymmetry Warning** for analysis (expected in dense boundaries but tracked).
- **PO Box Exclusions Audit (NEW):** Sample 100 excluded ZIPs with `is_pobox=true` and cross‑reference a public PO Box facility list to validate the derivation logic.

### Runtime QA
- **Threshold flags:** emit rates for `<1 mi` and `>10 mi`.
- **ZIP9 coverage:** % of lookups with `zip9_hit = true`.
- **PO Box sanity:** sample excluded ZIPs to confirm PO‑box‑like.

## 12) Performance & Sizing
- **Per‑lookup:** O(N) over same‑state ZIPs (bounded). With NBER fast‑path for most pairs, compute cost dominated by lookups, not trig.
- **Indexes:** btree on `cms_zip_locality.state`; hash/btree on `zip_to_zcta.zip5`; btree on `zcta_coords.zcta5` & `zcta_distances(zcta5_a, zcta5_b)`.
- **Caching:** LRU cache centroids and common `(zcta0, zcta_c)` distances.

### 12.1) Background Precompute (NEW)
- **Guideline:** During ETL, precompute a **same‑state sparse distance matrix** for all pairs `(zcta_a, zcta_b)` in each CMS state using Gazetteer centroids, and persist to a materialized table or cache. Use this as the primary at‑runtime distance source; NBER used for **validation** and backstops.

### Operational hardening
- **Download resilience:** retries with exponential backoff; checksum (SHA‑256) verification; idempotent ETL.
- **Monitoring:** metrics for latency, `nber_hit_rate`, `fallback_rate`, candidate set size; **Gazetteer Fallback Rate** alert if >0.1%/day.
- **Alerts:** schema drift, row deltas vs. previous vintage, missing required fields.
- **Governance:** store raw archives + checksums; preserve record layouts; log tool versions.
- **Security:** read‑only DB role for service; least‑privilege credentials; no PII in logs; vendor license compliance (SimpleMaps attribution).
- **Release safety:** feature flag; canary rollout; golden tests in CI.


- **Per‑lookup:** O(N) over same‑state ZIPs (bounded). With NBER fast‑path for most pairs, compute cost dominated by lookups, not trig.
- **Indexes:** btree on `cms_zip_locality.state`; hash/btree on `zip_to_zcta.zip5`; btree on `zcta_coords.zcta5` & `zcta_distances(zcta5_a, zcta5_b)`.
- **Caching:** LRU cache centroids and common `(zcta0, zcta_c)` distances.

### 12.1) Background Precompute (NEW)
- **Guideline:** During ETL, precompute a **same‑state sparse distance matrix** for all pairs `(zcta_a, zcta_b)` in each CMS state using Gazetteer centroids, and persist to a materialized table or cache. Use this as the primary at‑runtime distance source; NBER used for **validation** and backstops.

### Operational hardening
- **Download resilience:** retries with exponential backoff; checksum (SHA‑256) verification; idempotent ETL.
- **Monitoring:** metrics for latency, `nber_hit_rate`, `fallback_rate`, candidate set size; **Gazetteer Fallback Rate** alert if >0.1%/day.
- **Alerts:** schema drift, row deltas vs. previous vintage, missing required fields.
- **Governance:** store raw archives + checksums; preserve record layouts; log tool versions.
- **Security:** read‑only DB role for service; least‑privilege credentials; no PII in logs; vendor license compliance (SimpleMaps attribution).
- **Release safety:** feature flag; canary rollout; golden tests in CI.

---

## 13) Security, Licensing, Compliance
- **SimpleMaps (free tier) requires visible attribution** in any public UI or export. Include one‑line attribution in app footer and PRD.
- **Attribution Enforcement (NEW):** add a CI/QA check that fails the build if the attribution string is missing from the deployed UI configuration.
- Respect license terms for NBER, Census, CMS, HUD. Store raw archives for audit.

---

## 14) Runbook (Ops)
- **Refresh cadence:** Gazetteer (annual); UDS crosswalk (when updated); CMS ZIP5/ZIP9 (per CMS revision); NBER (per vintage); SimpleMaps (per vendor notes).
- **Multi‑vintage support:** Load new vintages alongside existing rows using `vintage` + `effective_from/to`; never overwrite. Keep an `ACTIVE_VINTAGE` setting and a `*_current` view that pins which vintage is live.
- **Read/Write segregation (NEW):** expose the service through read‑only roles and query only via `zip_data_current` views. ETL runs under a separate writer role; flip the view atomically post‑validation.
- **Roll‑forward:** Load → parity tests → switch the `*_current` view → flip feature flag.
- **Roll‑back:** Keep N‑1 vintage hot; revert `*_current` view + feature flag.
- **Provenance:** Ensure `ingest_runs` has complete metadata (URL, filename, size, sha256, started/finished, row_count, tool_version, status).

---

## 15) Milestones
1. **ETL & schema** (Gazetteer, UDS, CMS ZIP5/ZIP9, SimpleMaps) — 3d  
2. **Distance engine + traces** — 2d  
3. **NBER integration (optional)** — 1d  
4. **QA harness + parity thresholds** — 1d  
5. **Docs & runbook** — 0.5d

---

## 18) Acceptance Checks (Go/No‑Go)
- **ZIP9 override logic:** ZIP+4 inside CMS range uses ZIP9 state/locality (`zip9_hit=true` in trace).
- **Same‑state constraint:** All candidates drawn only from CMS state.
- **PO‑box filter:** SimpleMaps derivation excludes PO boxes; no PO‑box ZIPs in candidates.
- **Distance engine:** NBER chosen when available; Haversine otherwise; parity within thresholds (median |Δ| < 0.2 mi; p95 < 1.0 mi).
- **Ties:** Resolved by smaller `COALESCE(population,0)`, then lexicographic.
- **Trace completeness:** Inputs, normalization, candidate set sizes, both distances, chosen path, flags present.
- **Empty set handling:** Returns `NO_CANDIDATES_IN_STATE` (422) with diagnostics.

---

## 19) Smoke Tests (CLI & API)
```bash
# Health
curl -s http://localhost:8000/nearest-zip/health

# Basic ZIP5
curl -s "http://localhost:8000/nearest-zip/nearest?zip=94107" | jq

# ZIP9 override case (ensure you pick a known override from dataset)
curl -s "http://localhost:8000/nearest-zip/nearest?zip=94107-1234" | jq

# PO box exclusion sanity (use a known PO-box ZIP)
curl -s "http://localhost:8000/nearest-zip/nearest?zip=XXXXX" | jq

# Traces
TRC=$(curl -s "http://localhost:8000/nearest-zip/nearest?zip=30309" | jq -r .trace_id)
curl -s "http://localhost:8000/nearest-zip/traces/$TRC" | jq

# Stats
curl -s http://localhost:8000/nearest-zip/stats | jq
```

**Example response (happy path)**
```json
{
  "nearest_zip": "94110",
  "distance_miles": 1.42,
  "trace_id": "f7e3a5a2-...",
  "flags": { "coincident": false, "far_neighbor": false }
}
```

---

## 20) Data QA Spot‑Checks (SQL)
```sql
-- 1) Every candidate ZIP in same CMS state
SELECT COUNT(*) FROM resolver_candidates
WHERE state != (SELECT state FROM cms_zip_locality WHERE zip5 = :starting_zip);

-- 2) No PO boxes among candidates
SELECT COUNT(*) FROM resolver_candidates c
JOIN zip_metadata m USING (zip5)
WHERE m.is_pobox = TRUE;

-- 3) UDS mapping coverage
SELECT COUNT(*) FROM zip_to_zcta WHERE zcta5 IS NULL;

-- 4) Gazetteer centroid coverage
SELECT COUNT(*) FROM zcta_coords WHERE lat IS NULL OR lon IS NULL;

-- 5) NBER vs Haversine parity (sample)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(miles_hav - miles_nber)) AS p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY ABS(miles_hav - miles_nber)) AS p95
FROM resolver_results_sample;
```

---

## 21) Observability KPIs
- **Latency p50/p95** for `/nearest`.
- **nber_hit_rate** and **fallback_rate** (Haversine misses/backstops).
- **Gazetteer Fallback Rate** (>0.1%/day → P1).
- **Candidate set size** distribution by state.
- **ZIP9 coverage** (% lookups with `zip9_hit=true`).
- **Discrepancies:** count of `NBER_HAV_DISCREPANCY` trace flags.

---

## 21.1) QA Summary (per QA & Testing Standard v1.0)
| Item | Details |
| --- | --- |
| **Scope & Ownership** | Nearest ZIP resolver service + supporting ETL; owned by Resolver/INS squad with QA partner from Quality Engineering; consumers include pricing engines and analytics parity harnesses. |
| **Test Tiers & Coverage** | Unit/component: `tests/test_geometry_nearest_logic.py`, `tests/test_nearest_zip_resolver.py`; Scenario suites cover happy-path + strict-mode errors via `tests/test_nearest_zip_simple.py` and `tests/test_nearest_zip_comprehensive*.py`; Boundary integration with geography and state constraints validated in `tests/test_state_boundary_enforcement.py` and related fixtures. Coverage trending at 84% (target 90%). |
| **Fixtures & Baselines** | Gazetteer/NBER samples under `sample_data/nber_sample_1000.csv`; ZIP9/ZIP5 packs in `sample_data/zplc_oct2025/`; golden traces for parity stored in `tests/golden/test_scenarios.jsonl` with digests captured in QA observability dashboards. |
| **Quality Gates** | Merge: `ci-unit.yaml` runs resolver unit suites + lint; `ci-integration.yaml` spins resolver stack (Postgres + cache) to execute comprehensive tests; Nightly: `ci-nightly.yaml` replays background precompute + drift checks, blocking releases on regression. |
| **Production Monitors** | Synthetic `/nearest` probes every 5 min verifying latency + determinism; fallback-rate and distance-spread alerts wired to Grafana; Gazetteer Fallback Rate P1 defined in §1.1. |
| **Manual QA** | SQL spot-checks in §20 run on staging prior to new datasets; operators review trace samples for top states before enabling feature flags. |
| **Outstanding Risks / TODO** | Track `Non-Blocking Enhancements` in §23 (Top-N API, rate limiting); expand sample coverage for territories (VI/GU) before GA; automate parity harness vs. CMS reference dataset. |

## 22) Rollout Playbook
1. **Migrations:** `alembic upgrade head`.
2. **Load latest data:** `python -m cms_pricing.ingestion.nearest_zip_etl --source all` (checksums, row counts, `ingest_runs`).
3. **Warm cache:** prequery top 5k ZIPs per state.
4. **Canary:** route 1–5% traffic; monitor latency, `nber_hit_rate`, errors.
5. **Full cutover:** flip feature flag; keep N‑1 vintage hot for rollback.
6. **Aftercare:** run parity harness overnight; archive traces for first 24h.

---

## 23) Non‑Blocking Enhancements
- **Top‑N API variant** (`?n=3`) to surface equidistant neighbors for UI.
- **API versioning** (`/v1`) for forward compatibility.
- **Rate limiting & API‑key scopes** for external exposure.
- **Grafana dashboard** for KPIs above.
