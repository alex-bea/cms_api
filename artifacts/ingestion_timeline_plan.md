# ClearBill Ingestion Launch Timeline and Implementation Plans

## Timeline Overview

| Day | Focus Block | Priority Items |
|-----|-------------|----------------|
| Day 1 (Morning) | Stabilize OPPS pipeline foundations | Priority 0 – Stabilize OPPS ingestion end-to-end |
| Day 1 (Afternoon) | Complete OPPS validation + begin MPFS sign-off | Priority 0 follow-through, Priority 1 kickoff |
| Day 2 (Morning) | Finalize MPFS readiness and document results | Priority 1 – Verify MPFS ingestion readiness |
| Day 2 (Afternoon) | Build shared snapshot loader foundation | Priority 2 – Ship shared snapshot loader foundation |
| Day 3 (Morning) | Roll loader into remaining pipelines | Priority 3 – Adopt snapshot loader across OPPS and tooling |
| Day 3 (Afternoon) | Schedule execute_values optimization work | Priority 4 – Implement psycopg2 execute_values for RVU |
| Post-Launch | Expand backlog with new ingestors | Deferred Backlog – New ingestors (ASC → NADAC) |

---

## Priority 0 – Stabilize OPPS ingestion end-to-end

### 🧠 AI Implementation Plan Template

## 0. Context Summary (The Why)
OPPS ingestion lacks complete normalization, enrichment, and publish wiring, blocking ClearBill v1 launch readiness.
Missing Addendum handling and wage-index enrichment causes incomplete datasets and invalid schema outputs.
Objective is to finalize land→publish flow with validation coverage to ensure production-grade stability.

## I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 | `cms_pricing/ingestion/ingestors/opps_ingestor.py` | `tests/ingestion/test_opps_ingestor.py`, `cms_pricing/ingestion/utils/wage_index.py` | Complete land discovery logic, normalize Addenda A/B, wire wage-index enrichment, and ensure publish outputs canonical tables & parquet artifacts. | Ensure `_normalize_stage` handles new helper outputs; confirm `_publish_stage` writes correct dataset names. |
| 2 | P0 | `cms_pricing/ingestion/schemas` | `tests/fixtures/opps` | Update schema definitions and seed metadata required for dry-run validation. | Align schema with wage-index enriched data columns. |
| 3 | P1 | `scripts/dry_run_opps.py` | `artifacts/mpfs_opps_ingestion_runbook.md`, `artifacts/opps_ingestor_tomorrow_plan.md` | Create dry-run script and documentation updates for operators. | Capture validation checklist results. |

### Execution Checklist (Day 1 Morning – Stabilize OPPS)
1. Select the target quarter and construct the batch id (e.g., `opps_2025q1_r01`), confirming discovery metadata exists for that year/quarter.
   - Offline dev? Export `OPPS_LOCAL_SAMPLE_DIR="$PWD/sample_data/january_202025_20web_20addendum_20a.12.31.24"` so the scraper loads local Section 508/Excel files instead of hitting CMS.
2. Run the scripted dry-run to exercise land→publish stages and emit JSON evidence:
   ```bash
   python scripts/dry_run_opps.py \
     --batch-id opps_2025q1_r01 \
     --output-dir data/ingestion/opps \
     --pretty
   ```
   - Evidence JSON is stored under `artifacts/opps_dry_runs/` with tables, record counts, and curated parquet paths.
3. Inspect `curated/opps/<batch_id>/` for the Addendum A/B parquet outputs referenced in the evidence file.
4. Open `curated/opps/<batch_id>/metadata.json` to confirm `quarter_vintage`, `source_files[].source_file_sha256`, and `table_artifacts[].row_content_hash` populate per the DIS metadata standard (`STD-data-architecture-impl-v1.0.md` §1.2).
5. If only a subset of addenda is available locally, update `cms_pricing/ingestion/config/ingestor_artifacts.yml` (dataset `opps`) or set `OPPS_ADDENDA_PROFILE` to ensure validation expectations match the files under test (sandbox leniency will downgrade missing required addenda to warnings).
6. Attach the evidence JSON plus curated row counts to the Day 1 ticket before moving to schema/test updates (template example: `artifacts/opps_dry_runs/opps_2025q1_r01_20251107T205317.json`).
7. Afternoon follow-up: re-run the script after implementing schema/test fixes to prove parity and update documentation checkpoints.

## II. Dependency Checklist (The How and Impact)

- Cascading dependencies: `_normalize_stage` depends on accurate parsing from `CMSOPPSScraper.discover_latest()` and Addendum parsers.
- Downstream components: Wage-index utilities, publish pipeline writing to canonical tables, SI schema definitions.
- External integrations: S3 snapshot sources for OPPS, Postgres publish target.
- Pseudo-code snippet:
  ```python
  def _normalize_stage(self, raw_dfs):
      addendum_a = parse_addendum_a(raw_dfs["addendum_a"])
      addendum_b = parse_addendum_b(raw_dfs["addendum_b"])
      wage_index = enrich_with_wage_index(addendum_a, addendum_b)
      return self._apply_schema(wage_index)
  ```
- Critical change location: Modify `_normalize_stage` near the dictionary comprehension around wage-index merging (approx. line 565).

## III. Testing and Validation Guidance (The Verification)

- Unit / Integration Tests:
  - Add normalization tests in `tests/ingestion/test_opps_ingestor.py` verifying Addendum parsing and wage-index enrichment.
  - Example assertion:
    ```python
    assert enriched_df["wage_index"].notnull().all()
    ```
- Fixture Management:
  ```bash
  pytest tests/ingestion/test_opps_ingestor.py --regen-fixtures
  ```
- End-to-End Verification:
  - Run `python scripts/dry_run_opps.py --batch-id <id> --output-dir data/ingestion/opps --pretty` to confirm land→publish stages succeed and capture evidence.
- Acceptance Criteria / Success Metrics:
  - All validation tests pass, parquet outputs materialize under canonical path, wage-index columns populated.

## IV. Guardrails, Risk, and Rollback

- Primary Risk: Schema mismatch or missing wage-index data causing publish failure.
- Pre-Commit / Pre-Deploy Checks: `pytest tests/ingestion/test_opps_ingestor.py`, run dry-run ingest in staging.
- Rollback Strategy:
  ```bash
  git revert <opps_ingestor_commit>
  redeploy ingest-opps job
  ```

---

## Priority 1 – Verify MPFS ingestion readiness

### 🧠 AI Implementation Plan Template

## 0. Context Summary (The Why)
MPFS ingestion is near-complete but needs dependency cleanup and end-to-end validation before launch.
Ensuring current MPFS data runs without errors provides confidence alongside OPPS readiness.
Documentation updates must capture final validation artifacts for sign-off.

## I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `tests/ingestion/test_mpfs_ingestor.py` | Fix any lingering import/dependency issues and confirm pipeline stages align with latest schemas. | Focus on helpers flagged in gap analysis. |
| 2 | P0 | `scripts/run_mpfs_ingest.py` | `docs/mpfs_runbook.md` | Execute full ingest, capture logs and validation outputs. | Use current vintage data snapshot. |
| 3 | P1 | `artifacts/mpfs_readiness.md` | `docs/readiness_checklist.md` | Update readiness artifacts with evidence, note remaining documentation gaps. | Highlight conversion-factor fetcher tests status. |

## II. Dependency Checklist (The How and Impact)

- Cascading dependencies: Import fixes may affect `_publish_stage` and data loaders referencing shared utilities.
- Downstream components: Validation scripts, readiness documentation workflows.
- External integrations: Render deployment environment, Postgres staging database.
- Pseudo-code snippet:
  ```python
  def run_ingest():
      dataset = MPFSIngestor()
      dataset.run()
      assert dataset.latest_publish.success
  ```
- Critical change location: Adjust imports inside `MPFSIngestor` near the stage pipeline definitions around line 210.

## III. Testing and Validation Guidance (The Verification)

- Unit / Integration Tests:
  - Ensure `tests/ingestion/test_mpfs_ingestor.py` covers updated dependency paths.
    ```python
    assert mpfs_ingestor.dependencies_resolved
    ```
- Fixture Management:
  ```bash
  pytest tests/ingestion/test_mpfs_ingestor.py
  ```
- End-to-End Verification:
  - Run `make ingest_mpfs` capturing logs and verifying published tables.
- Acceptance Criteria / Success Metrics:
  - Successful ingest without errors, readiness documentation updated with evidence.

## IV. Guardrails, Risk, and Rollback

- Primary Risk: Dependency regressions causing ingest failures.
- Pre-Commit / Pre-Deploy Checks: MPFS unit tests, staging ingest run, documentation review.
- Rollback Strategy:
  ```bash
  git revert <mpfs_dependency_commit>
  redeploy ingest-mpfs job
  ```

---

## Priority 2 – Ship shared snapshot loader foundation

### 🧠 AI Implementation Plan Template

## 0. Context Summary (The Why)
Current parquet snapshot loading is inconsistent and can cause OOM issues across ingestors.
A shared streaming loader will standardize row limits and batching, unlocking stability improvements.
Goal is to implement utility plus tests, then refactor MPFS and RVU to use it.

## I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 | `cms_pricing/ingestion/utils/snapshot_loader.py` | `tests/ingestion/utils/test_snapshot_loader.py` | Implement shared loader with path resolution, batching via PyArrow, and row-limit enforcement. | Provide streaming interface and logging hooks. |
| 2 | P0 | `cms_pricing/ingestion/ingestors/mpfs_ingestor.py` | `cms_pricing/ingestion/ingestors/rvu_ingestor.py` | Refactor to consume new loader, removing direct `pd.read_parquet` usage. | Ensure consistent row-limit parameters. |
| 3 | P1 | `docs/ingestion_architecture.md` | `configs/ingestion_defaults.yaml` | Document loader usage and expose configuration toggles. | Update operator guidance for row limits. |

## II. Dependency Checklist (The How and Impact)

- Cascading dependencies: Loader must integrate with existing ingestors and respect environment variables for limits.
- Downstream components: RVU and MPFS pipelines, logging/monitoring for ingestion stages.
- External integrations: PyArrow, S3 storage for parquet files.
- Pseudo-code snippet:
  ```python
  for batch in load_snapshot(path, max_rows=MAX_ROWS):
      process(batch)
  ```
- Critical change location: Implement `load_snapshot` inside new utility near batching loop (initial file creation).

## III. Testing and Validation Guidance (The Verification)

- Unit / Integration Tests:
  - Create tests ensuring batching honors row limits and yields expected schema.
    ```python
    assert sum(len(batch) for batch in batches) == total_rows
    ```
- Fixture Management:
  ```bash
  pytest tests/ingestion/utils/test_snapshot_loader.py
  ```
- End-to-End Verification:
  - Run `make ingest_mpfs` and `make ingest_rvu` ensuring memory usage stays within target.
- Acceptance Criteria / Success Metrics:
  - No OOMs, consistent logging of row counts, pipelines complete successfully.

## IV. Guardrails, Risk, and Rollback

- Primary Risk: Loader bugs causing incomplete data ingestion.
- Pre-Commit / Pre-Deploy Checks: Unit tests for loader, regression ingest runs for MPFS/RVU.
- Rollback Strategy:
  ```bash
  git revert <snapshot_loader_commit>
  restore previous loader configuration
  ```

---

## Priority 3 – Adopt snapshot loader across OPPS and tooling

### 🧠 AI Implementation Plan Template

## 0. Context Summary (The Why)
After MPFS/RVU adoption, OPPS and DIS utilities must use the shared loader to maintain consistency.
This ensures unified behavior and eliminates legacy environment variable reliance.
Change completes the infrastructure refactor across pipelines.

## I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 | `cms_pricing/ingestion/ingestors/opps_ingestor.py` | `cms_pricing/ingestion/scripts/dis_utils.py` | Replace `pd.read_parquet` calls with shared loader usage. | Ensure OPPS loader integration respects row limits. |
| 2 | P1 | `docs/opps_ingestion.md` | `docs/dis_loader_guide.md` | Update documentation to describe new loader usage and configuration knobs. | Highlight removal of `MAX_INGESTION_ROWS`. |
| 3 | P2 | `configs/env.sample` | `configs/env.production` | Clean up deprecated environment variables. | Communicate new settings to operators. |

## II. Dependency Checklist (The How and Impact)

- Cascading dependencies: OPPS pipeline must already be stable (Priority 0) before swapping loader.
- Downstream components: DIS utilities, OPPS publish outputs.
- External integrations: None beyond existing S3/parquet usage.
- Pseudo-code snippet:
  ```python
  loader = SnapshotLoader()
  for batch in loader.read(path):
      opps_pipeline.process(batch)
  ```
- Critical change location: Replace direct pandas call within OPPS ingest `_land_stage` around the parquet read block near line 180.

## III. Testing and Validation Guidance (The Verification)

- Unit / Integration Tests:
  - Update OPPS ingestion tests to assert loader integration.
    ```python
    loader_mock.read.assert_called_once()
    ```
- Fixture Management:
  ```bash
  pytest tests/ingestion/test_opps_ingestor.py
  ```
- End-to-End Verification:
  - Run `make ingest_opps` and relevant DIS scripts to verify consistent behavior.
- Acceptance Criteria / Success Metrics:
  - No regressions, environment variables cleaned up, documentation reflects new process.

## IV. Guardrails, Risk, and Rollback

- Primary Risk: Loader mismatch causing OPPS ingest failure.
- Pre-Commit / Pre-Deploy Checks: Regression tests, staging ingest runs.
- Rollback Strategy:
  ```bash
  git revert <opps_loader_commit>
  restore previous env var settings
  ```

---

## Priority 4 – Implement psycopg2 execute_values for RVU

### 🧠 AI Implementation Plan Template

## 0. Context Summary (The Why)
RVU bulk insert performance needs improvement to benefit from snapshot loader chunking.
Introducing `execute_values` offers faster inserts under a feature flag.
Work should commence after shared loader rollout.

## I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 | `cms_pricing/ingestion/datasets/rvu_loaders.py` | `tests/ingestion/datasets/test_rvu_loaders.py` | Implement execute_values fast path with serialization helpers and fallback. | Guard with `RVU_USE_EXECUTE_VALUES`. |
| 2 | P1 | `configs/feature_flags.yaml` | `docs/rvu_loader_rollout.md` | Add feature flag definitions, rollout plan, and documentation. | Include performance metrics tracking. |
| 3 | P1 | `tests/performance/test_rvu_bulk_insert.py` | `scripts/benchmark_rvu_insert.py` | Add performance regression tests and benchmarks. | Capture baseline vs feature flag results. |

## II. Dependency Checklist (The How and Impact)

- Cascading dependencies: Depends on shared snapshot loader chunking for batch sizes.
- Downstream components: RVU ingestion pipeline, any datasets sharing `_bulk_replace_records`.
- External integrations: Postgres database for measuring insert performance.
- Pseudo-code snippet:
  ```python
  if settings.USE_EXECUTE_VALUES:
      execute_values(cursor, sql, rows)
  else:
      cursor.executemany(sql, rows)
  ```
- Critical change location: Modify `_bulk_replace_records` around tuple serialization logic near line 150.

## III. Testing and Validation Guidance (The Verification)

- Unit / Integration Tests:
  - Ensure feature flag toggles both paths and serialization handles UUID/Decimal/date.
    ```python
    assert bulk_loader(rows, use_execute_values=True).success
    ```
- Fixture Management:
  ```bash
  pytest tests/ingestion/datasets/test_rvu_loaders.py
  ```
- End-to-End Verification:
  - Run `make ingest_rvu` with flag enabled, capture timings and ensure no errors.
- Acceptance Criteria / Success Metrics:
  - Insert throughput improves, correctness maintained under both modes.

## IV. Guardrails, Risk, and Rollback

- Primary Risk: Incorrect serialization causing data corruption.
- Pre-Commit / Pre-Deploy Checks: Unit tests, performance benchmarks, staging ingest run with flag off/on.
- Rollback Strategy:
  ```bash
  export RVU_USE_EXECUTE_VALUES=false
  redeploy ingest-rvu job
  ```

---

## Deferred Backlog – New ingestors (ASC → NADAC)

### 🧠 AI Implementation Plan Template

## 0. Context Summary (The Why)
Post-launch backlog includes new ingestors that rely on RVU modular templates.
Work should begin only after launch-critical pipelines are stable.
Preparation ensures smoother onboarding when scope opens up.

## I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P1 | `planning/new_ingestors_plan.md` | `docs/prd_source_map.md` | Capture detailed requirements per dataset leveraging PRD appendix. | Align requirements with DIS 5-stage architecture. |
| 2 | P2 | `cms_pricing/ingestion/templates/rvu_template.py` | `tests/ingestion/templates/test_rvu_template.py` | Adapt modular template for future ingestors. | Reuse RVU architecture components. |

## II. Dependency Checklist (The How and Impact)

- Cascading dependencies: Requires OPPS/MPFS/RVU stability before expansion.
- Downstream components: Future ingestor projects, documentation.
- External integrations: Reference PRDs and data sources per dataset.
- Pseudo-code snippet:
  ```python
  new_ingestor = RVUTemplate.clone(dataset="ASC")
  new_ingestor.configure_from_prd()
  ```
- Critical change location: Planning documents under `planning/` directory.

## III. Testing and Validation Guidance (The Verification)

- Unit / Integration Tests:
  - Future tests should ensure template configurations match dataset requirements.
    ```python
    assert asc_ingestor.pipeline_stages == EXPECTED_STAGES
    ```
- Fixture Management:
  ```bash
  # No fixtures until implementation kicks off
  ```
- End-to-End Verification:
  - None required pre-implementation; focus on documentation readiness.
- Acceptance Criteria / Success Metrics:
  - Requirements captured, templates prepared, ready to execute when prioritized.

## IV. Guardrails, Risk, and Rollback

- Primary Risk: Premature implementation causing distraction from launch blockers.
- Pre-Commit / Pre-Deploy Checks: Review by leads before starting new ingestors.
- Rollback Strategy:
  ```bash
  git revert <new_ingestor_planning_commit>
  ```
