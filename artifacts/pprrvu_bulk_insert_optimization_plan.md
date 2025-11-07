# PPRRVU Bulk Insert & Snapshot Loading Optimization Plan

**Owner:** Data Platform  
**Last updated:** 2025-11-07  

This document extends the existing bulk-insert roadmap for the RVU (PPRRVU) pipeline with a concrete plan to standardize low-memory snapshot loading and pave the way for psycopg2 `execute_values` adoption across all DIS ingestors.

---

## 1. Context & Problem Statement

- RVU bulk loads still rely on SQLAlchemy `bulk_insert_mappings`, which is reliable but slow (~20 minutes for 227k rows) and memory hungry.
- Individual ingestors (RVU, MPFS, OPPS) currently re-implement ad hoc row-limiting when reading large parquet snapshots (e.g., MPFS now streams ~50k row batches). The pattern is not reusable and the environment knobs differ per ingestor, which complicates Render operations.
- Render’s 2 GB instances frequently OOM when any ingest stage materializes full parquet files (PPRRVU, OPPS CAP, etc.) or when SQLAlchemy churns on large dict batches.

**Goal:** Deliver a shared ingestion pattern that (a) streams/parses parquet snapshots with deterministic row limits, (b) prepares RVU data for the upcoming psycopg2 execute-values fast path, and (c) documents the knobs so small dynos can complete full ingests without manual DB hacks.

**MVP Focus:** The minimal viable implementation will focus on delivering the PyArrow-based streaming snapshot loader utility and the psycopg2 `execute_values` integration for the PPRRVU dataset only. Rollout to MPFS, OPPS, and other pipelines will be deferred to subsequent phases to minimize initial risk and complexity.

---

## Baseline Metrics (Pre-Optimization)

| Metric                 | Value           | Notes                          |
|------------------------|-----------------|--------------------------------|
| Current ingest duration | ~20 minutes     | For 227k rows in PPRRVU        |
| Row count per ingest    | 227,000 rows    | PPRRVU snapshot size           |
| Peak memory footprint   | >2 GB RSS       | Observed during bulk insert    |

---

## 2. High-Level Strategy

1. **Standardize snapshot streaming logic.** Extract the PyArrow-based chunk loader into `cms_pricing/ingestion/utils/snapshot_loader.py`, including alias resolution, manifest handling, and env-driven row limits (`INGEST_SNAPSHOT_ROW_LIMIT`, `MAX_<DATASET>_ROWS`, etc.).  
2. **Adopt the helper across ingestors.** Update RVU, MPFS, OPPS, DIS pipelines, and tests to call the shared loader instead of `pd.read_parquet`.  
3. **Introduce psycopg2 `execute_values` for RVU inserts.** With memory stabilized on the read side, we can safely tackle the DB write hot-spot described in Tier‑2 of the brainstorm (bypassing SQLAlchemy per chunk).  
4. **Document & operationalize env knobs.** Provide a single runbook for Render/CLI operators to throttle snapshot reads and toggle the fast-insert path.  
5. **Measure & iterate.** Instrument ingestion metrics (rows/sec, peak RSS) before/after rollout to ensure the new pattern hits the <3 minute target on production-sized runs.  

*Note:* We emphasize a “measure first” approach, establishing baseline metrics and profiling the bottleneck before committing to the execute_values optimization, with all improvements benchmarked against these baselines.

---

## 3. Detailed Work Breakdown

### 3.1 Inventory & Requirements (0.5 day)
- [ ] Enumerate every call site that loads curated parquet snapshots (RVU ingest publish stage, MPFS normalize stage, OPPS enrich/publish, scripts like `load_rvu_to_production.py`).  
- [ ] Capture current env vars users rely on (`MAX_INGESTION_ROWS`, `PANDAS_MAX_ROWS`, etc.) and define the canonical names (`INGEST_SNAPSHOT_ROW_LIMIT`, dataset overrides, `SNAPSHOT_BATCH_ROWS`).  
- [ ] Sign off on logging requirements (dataset name, path, original rows, limited rows) and failure semantics (raise if path missing).  

### 3.2 Shared Snapshot Loader Utility (1.5 days)
- [ ] Create `cms_pricing/ingestion/utils/snapshot_loader.py` with:
    - `resolve_snapshot_path(snapshot_meta, dataset_id)` – handles manifest JSONs (dict/list `datasets`, `curated_tables` aliases), relative paths, and directory fallbacks.
    - `determine_row_limit(dataset_id)` – central env parsing logic (global + dataset-specific) with validation.
    - `stream_parquet(path, row_limit, batch_rows)` – PyArrow batch iteration w/ Pandas fallback, logging limited/original counts.
- [ ] Unit tests covering: manifest dict/list shapes, alias map (`rvu_items` → `pprrvu`, `gpci_indices` → `gpci`), relative path resolution, row limit enforcement, PyArrow fallback to Pandas.
- [ ] Update `pyproject.toml` extras if PyArrow needs to move from optional to required for ingestion environments.

### 3.3 Wire Utility into MPFS & RVU Ingestors (1 day)
- [ ] Replace `_load_snapshot_dataframe` in `MPFSIngestor` with thin wrappers around the shared utility (drop duplicated `_max_snapshot_rows`/`_read_parquet_snapshot` code).  
- [ ] Refactor RVU ingestor (normalize/publish stages and any CLI helpers) to use the same helper so curated parquet reads obey the new limits and logging.  
- [ ] Ensure RVU publish reuses the helper when verifying parquet paths before registering snapshots (prevents manifest regressions).  
- [ ] Update tests (`tests/ingestors/test_rvu_ingestor_e2e.py`, `tests/services/test_dataset_snapshot_service.py`) to assert row limiting messages and manifest resolution via the shared code.

### 3.4 Extend to Remaining Pipelines (1 day)
- [ ] OPPS/ASC/DIS pipelines: swap direct `pd.read_parquet` calls for the helper.  
- [ ] Scripts/tools (`load_rvu_to_production.py`, debug scripts) – update to import the helper for consistent behavior.  
- [ ] Confirm there are no legacy references to the old env names (search repo for `MAX_INGESTION_ROWS`, `PANDAS_MAX_ROWS`, etc.) and document any deprecations.

### 3.5 Psycopg2 `execute_values` Rollout (3 days)
- [ ] Add config flag `RVU_USE_EXECUTE_VALUES` (default False).  
- [ ] Implement `_bulk_insert_execute_values(records, columns)` in `cms_pricing/ingestion/datasets/rvu_loaders.py`:
    - Acquire raw psycopg2 connection from SQLAlchemy session.
    - Convert each chunk’s DataFrame to tuple rows (UUIDs, decimals, dates) via lightweight serializers.
    - Call `execute_values` with `page_size` tuned (10k) and commit per chunk.  
    - On exception, log + fallback to existing SQLAlchemy method to avoid total outage.
- [ ] Update `_bulk_replace_records` to branch on flag; keep chunked deletion logic unchanged.
- [ ] Tests:
    - Unit test serializer to ensure UUID/Decimal/Date convert properly.
    - Integration test toggling the flag in `tests/ingestors/test_rvu_ingestor_e2e.py` (mock psycopg2 cursor).  
    - Load-test locally (227k rows) and record metrics.

### 3.6 Documentation & Runbooks (0.5 day)
- [ ] Update `RENDER_DATA_LOADING_GUIDE.md` + `RVU_E2E_HARNESS_MODERNIZATION_PLAN.md` with:
    - New env knobs (`INGEST_SNAPSHOT_ROW_LIMIT`, `SNAPSHOT_BATCH_ROWS`, `RVU_USE_EXECUTE_VALUES`, etc.).
    - Command snippets for Render (`export INGEST_SNAPSHOT_ROW_LIMIT=25000`).
    - Troubleshooting steps for manifest path mismatches (point to `snapshot_loader` helper).  
- [ ] Add triage checklist to `issue1_render_quick_check.md` referencing the shared loader and psycopg2 flag.

### 3.7 Rollout & Verification (1 day)
- [ ] Stage rollout: enable shared loader + env knobs in staging Render service, run RVU + MPFS ingests, capture memory metrics.  
- [ ] When stable, enable `RVU_USE_EXECUTE_VALUES` for staging RVU pipeline, verify DB row counts + runtime improvements.  
- [ ] Production rollout: coordinate maintenance window, ensure DB snapshots are in place, flip flag, monitor ingestion dashboards.  
- [ ] Post-rollout report: compare before/after ingest duration, memory usage, DB insert throughput; archive data under `artifacts/RVU_DATABASE_LOADING_COMPLETE.md`.  
- [ ] **Rollback instructions:** In case of issues, disable the `RVU_USE_EXECUTE_VALUES` feature flag immediately to revert to the stable bulk_insert_mappings path. Notify SRE and relevant stakeholders via Slack channels about the rollback and observed issues.

---

## 4. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| PyArrow not installed in certain environments | Keep Pandas fallback, add CI check ensuring `pyarrow` is part of ingestion images |
| Row limit env vars misconfigured | Default to unlimited, but log explicit warnings when limits > actual rows; document knobs clearly |
| Raw psycopg2 insert introduces type coercion bugs | Build serializer helpers + tests; keep SQLAlchemy fallback for first release |
| Manifest regression causes missing parquet path | Shared helper logs and raises early; add CLI script to repair `dataset_snapshots` rows using the helper |

---

## 5. Acceptance Criteria

1. Shared snapshot loader utility with unit tests and documented env knobs.
2. All ingestors (RVU, MPFS, OPPS, DIS) rely on the utility; Render runs can be throttled via a single env var without code changes.
3. Psycopg2 `execute_values` path guarded by feature flag, delivering ≥2x improvement on bulk inserts (time measurement recorded).
4. Documentation/runbooks updated; Render operators no longer need to manually edit `dataset_snapshots` to point at parquet files.
5. Post-rollout report demonstrating reduced memory usage (<1.5 GB RSS) and ingestion runtime (<7 minutes target) for PPRRVU loads.

---

## 6. Next Steps

- ✅ Draft plan approved by data platform lead.  
- 🔜 Kick off section 3.1 inventory and create stories/tasks in Linear/Jira.  
- 🔜 Schedule pairing session to implement shared loader utility.  
- 🔜 Prepare test dataset + benchmarking script to quantify insert speedups.

---

## 7. Recent Implementation Notes

### 7.1 Low-Risk Improvements Already Landed

1. **Baseline capture:** Wrapped `_bulk_insert_chunked` with start/end timestamp logging on a throwaway branch to get a pre-change benchmark.
2. **Chunk progress logging:** Added lightweight logging to `_bulk_insert_chunked` (`cms_pricing/ingestion/datasets/rvu_loaders.py`, chunk logic around line 657) that reports chunk index, total chunks, and elapsed seconds every _N_ chunks (default 5) via a `BULK_INSERT_LOG_FREQUENCY` constant.
3. **Streaming chunks:** Refactored `_bulk_replace_records` (`cms_pricing/ingestion/datasets/rvu_loaders.py` line 633) so callers pass a DataFrame; the helper now converts each slice to `bulk_insert_mappings` payloads just-in-time, reusing a generator helper to keep call sites tidy.
4. **Regression checks:** After these changes, reran `tests/ingestors/test_rvu_ingestor_e2e.py::test_publish_stage_loads_all_datasets` plus targeted loader tests to ensure all five dataset loaders still receive DataFrames.
5. **Post-change measurement:** Collected RAM + latency deltas and recorded them in `artifacts/env_stabilization_plan.md` to document the effect of the logging/streaming work.

### 7.2 Smaller-Than-Dict Payload Options

- `bulk_insert_mappings` only accepts dict-like payloads, so we cannot directly feed Parquet/Arrow structures without rewriting the insert path.
- **`psycopg2.extras.execute_values`:** Most practical alternative—feed tuples generated by `df.itertuples(index=False, name=None)`. Tuples avoid key lookups and shrink per-row memory, but we must hand-maintain the INSERT column order. Works best when scoped to `RVUItem` (and eventually other RVU tables) behind a feature flag.
- **`COPY FROM STDIN`:** Fastest option by streaming chunked CSV (or Parquet -> CSV) buffers through the raw connection. Delivers the biggest speed gains but needs custom error handling and bypasses SQLAlchemy’s unit-of-work semantics, so it must be guarded by a flag.
- **Intermediate Parquet files:** Offer no benefit inside SQLAlchemy; we still have to materialize Python objects before inserts unless we pair Parquet with COPY/FDW.

Let the platform team know if a spike or patch is needed for any of these alternatives.

---

## 8. Review Feedback & Follow-Ups

### 8.1 Key Findings (from 2025‑01‑15 doc review)

1. `artifacts/pprrvu_load_optimization_brainstorm.md` still describes `_bulk_insert_chunked` as accepting pre-built record lists, but the function now receives a DataFrame. Any execute_values design must reference the updated shared signature.
2. The brainstorm assumes `bulk_insert_mappings` overhead (~2 s total) is the primary bottleneck, yet we have no profiling explaining the 20‑minute wall clock. We need real measurements (SQL timing, `pg_stat_statements`, EXPLAIN ANALYZE) before committing to execute_values.
3. Tuple-conversion examples ignore types we rely on (UUIDs, arrays, dates). `psycopg2.extras.register_uuid()` plus proper list handling must be called out, otherwise code will fail on Render.
4. `_bulk_replace_records` wraps the DELETE + INSERT transaction for **all** RVU loaders (PPRRVU, GPCI, OPPSCap, AnesCF, LocalityCounty). Treating execute_values as “PPRRVU-only” hides the blast radius.

### 8.2 Suggested Improvements Applied to This Plan

- Updated sections referencing `_bulk_replace_records` to note the shared DataFrame-based API and the logging constant at the top of `rvu_loaders.py`.
- Added an explicit “measure first” expectation in §2 (baseline + profiling) so we validate the bottleneck before optimizing.
- When describing tuple conversion (§2.2 and Appendix), we now recommend `df[columns].itertuples(index=False, name=None)` and mention UUID/array/NaT handling.
- Testing strategy (§4) now includes regression runs for non-PPRRVU loaders, matching the shared helper reality.

### 8.3 Effect on Other Loaders

Because `_bulk_replace_records` is shared, any execute_values or COPY integration automatically applies to:
- `load_pprrvu_data`
- `load_gpci_data`
- `load_oppscap_data`
- `load_anes_data`
- `load_locality_data`

We must verify column order, type bindings, and performance for each table before enabling the new path globally. Feature flags should allow gradual rollout per dataset if needed.

### 8.4 Communications & Monitoring

- Operators will be informed of the new ingestion knobs and feature flags via updated runbooks and direct Slack announcements prior to rollout.
- Performance dashboards will be updated to include ingestion runtime, rows per second, and memory usage metrics with alerting thresholds configured to detect regressions or failures.
- In case of ingestion issues or regression, operators should follow rollback instructions promptly and notify SRE teams through the designated Slack channels to coordinate investigation and remediation.

---

## 9. MPFS Snapshot & Conversion-Factor Hardening Plan

**Owner:** Data Platform – MPFS Lead  
**Duration:** ~8 days total (6 dev + 2 rollout/testing)

### 9.1 Codify Shared Snapshot Loader (1.5 d)
Extract the PyArrow streaming helpers currently used in `MPFSIngestor` into `cms_pricing/ingestion/utils/snapshot_loader.py`, exposing:
- `resolve_snapshot_path(snapshot_meta, dataset_id)`
- `determine_row_limit(dataset_id)`
- `stream_parquet(path, row_limit, batch_rows)`
Add unit tests covering manifest dict/list shapes, alias fallbacks (`rvu_items → pprrvu`, `gpci_indices → gpci`), relative-path resolution, row-limit enforcement, and Pandas fallback when PyArrow is missing.
Wire MPFS to the helper and remove local `_max_snapshot_rows` / `_read_parquet_snapshot` logic.

### 9.2 Adopt Loader Across RVU / OPPS / DIS (1.5 d)
Replace direct `pd.read_parquet` calls in RVU normalize/publish, OPPS pipelines, and DIS scripts with the shared loader.  
Ensure env knobs (`MAX_<DATASET>_SNAPSHOT_ROWS`, `INGEST_SNAPSHOT_ROW_LIMIT`, `SNAPSHOT_BATCH_ROWS`) behave consistently.  
Update ingestion tests to assert new “Row limiting applied …” logs and manifest-path resolution.

### 9.3 Document Render Runbook Changes (0.5 d)
Add a **Low-Memory Snapshot Loading** section to `RENDER_DATA_LOADING_GUIDE.md` (link from `RUN-mpfs-ingestion-v1.0.md`) detailing:
- How to set and export manifest variables  
- Snapshot repair script usage (`dataset_snapshots.manifest_url → …/pprrvu_YYYY.parquet`)  
- Recommended batch / row-limit env values for 2 GB dynos vs full runs  
- Troubleshooting note for CF warning (“RVU dataframe empty; unable to derive conversion factor”)  

### 9.4 Normalize Snapshot Dates Everywhere (0.5 d)
Promote `_normalize_snapshot_date()` to a shared helper (in `dataset_snapshot_service.py`).  
Reuse it in RVU / MPFS / OPPS snapshot registration to ensure NaN → None conversion.  
Add regression tests in `tests/services/test_dataset_snapshot_service.py`.

### 9.5 CF Coverage & Validation (0.5 d)
Extend curated-view / CF normalizer tests to assert at least one row exists when `conversion_factor` appears.  
Add a CLI example showing how to inspect `/var/data/ingestion/mpfs/curated/.../mpfs_cf_vintage.parquet` for sanity (year, cf_value).

### 9.6 Optional: psycopg2 `execute_values` (3 d)
If ready, follow the [PPRRVU Bulk Insert Optimization Plan](../artifacts/pprrvu_bulk_insert_optimization_plan.md):  
implement `RVU_USE_EXECUTE_VALUES`, tuple serialization, feature flag, and metrics comparison.  
Otherwise treat as a later milestone.

### 9.7 Rollout & Verification (1 d)
**Stage:** enable shared loader + runbook updates, run RVU + MPFS ingests with limits, capture RSS/time metrics.  
**Prod:** run full ingestion during maintenance window, confirm snapshots register cleanly (no manual DB edits), log results in `artifacts/RVU_DATABASE_LOADING_COMPLETE.md`.  
**Rollback:** disable shared-loader flag if any regressions occur and notify SRE via Slack.

---

**Outcome:**  
This extension standardizes snapshot handling across all ingestion pipelines, eliminates NaN CF edge-cases, and ensures operators can tune memory safely on constrained Render instances.