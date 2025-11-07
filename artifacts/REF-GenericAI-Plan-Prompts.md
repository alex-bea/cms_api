REF-Generic Prompts.md


This single block functions as your **generic prompt** for generating detailed coding guidance.

````markdown
# 🧠 AI Implementation Plan Template

> **Prompt:** > Based on this analysis and feedback, generate a detailed, step-by-step implementation plan and checklist.  
> Structure the response into the following **mandatory sections**.  
> The output must be clear, specific, and implementation-ready for engineers.

---

## 0. Context Summary (The Why)
Summarize in 3–4 lines the core issue, feature, or optimization being addressed.  
Include affected components, observed behavior, and intended outcome.

---

## I. Implementation Breakdown (The What and Where)

Create a **prioritized table** listing all required fixes/features.

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 |  |  |  |  |
| 2 | P1 |  |  |  |  |

**Instructions:** - *Primary Files* → main locations where code changes are required.  
- *Secondary Files* → tests, schemas, configs to update or validate.  
- *Priority Levels:* P0 = must ship, P1 = important but can defer, P2 = optional or follow-up.

---

## II. Dependency Checklist (The How and Impact)

- List **cascading dependencies** for the highest-impact (P0) change.  
- Identify **downstream methods/classes/components** affected.  
- Call out **external integrations** (e.g., Render, Postgres, S3, APIs).  
- Provide **pseudo-code or example snippet** of the refactor or change pattern.

> **Crucial Detail for P0:** For the most critical file modification, identify the **function/method name** and a **neighboring line of code or context** where the change must occur (e.g., "Change occurs inside `_normalize_stage` near the dictionary comprehension on line 565").

---

## III. Testing and Validation Guidance (The Verification)

- **Unit / Integration Tests:** - List which specific tests or assertions need to be added or updated.  
  - Include 1–2 lines of pseudo-code showing the modified assertion.

> Example:  
> ```python
> assert snapshot_meta.path.endswith(".parquet")
> ```

- **Fixture Management:** - Provide shell or Python commands to regenerate test fixtures or manifests.  
  - Example:  
  ```bash
  pytest --regenerate-fixtures
  alembic upgrade head
````

  - **End-to-End Verification:** - Describe the manual or automated E2E validation steps.
      - Example: Run `make ingest_rvu && make ingest_mpfs`, then confirm memory \< 1.5 GB.
  - **Acceptance Criteria / Success Metrics:** - Quantitative targets (e.g., runtime \< 3 min, 0 ingestion errors, all parquet paths valid).

-----

## IV. Guardrails, Risk, and Rollback

  - **Primary Risk:** - Describe the main risk (e.g., schema regression, data duplication, ingestion failure).
  - **Pre-Commit / Pre-Deploy Checks:** - List all checks to run before merging (e.g., unit tests, staging dry-run, migrations).
  - **Rollback Strategy:** - Provide explicit steps to revert safely, disable flags, or restore previous behavior.
      - Example:
    <!-- end list -->
    ```bash
    export RVU_USE_EXECUTE_VALUES=false
    redeploy render-service-rvu
    ```

-----

```
```

---

## Example – OPPS Ingestion Stabilization Plan

### 0. Context Summary (The Why)
- OPPS ingestion still fails to complete the normalize → enrich → publish stages, so Addenda A/B rows and wage-index values never land in canonical tables (`artifacts/ingestion_timeline_plan.md:7-74`).
- Schemas/fixtures lag the dataframe shape emitted by `opps_ingestor.py`, causing validation drift and brittle publish jobs.
- Operators lack a scripted dry-run harness that captures evidence, leaving readiness assessments manual and error-prone.

### I. Implementation Breakdown (The What and Where)

| # | Priority | Primary File(s) | Secondary File(s) | Description of Code Change | Notes |
|---|-----------|-----------------|------------------|-----------------------------|-------|
| 1 | P0 | `cms_pricing/ingestion/ingestors/opps_ingestor.py` | `cms_pricing/ingestion/utils/wage_index.py`, `tests/ingestion/test_opps_ingestor.py`, `tests/fixtures/opps/*` | Finish discovery → normalize → enrich pipeline: parse Addenda A/B, merge wage-index lookups, ensure `_publish_stage` writes canonical parquet + DB tables. | Change occurs inside `_normalize_stage` near the wage-index merge dict comprehension (~line 560). |
| 2 | P0 | `cms_pricing/ingestion/schemas/opps.py` | `tests/fixtures/opps/schema/*.json`, `cms_pricing/validation/types.py` | Update schema definitions and metadata to match enriched dataframe columns and provenance defaults. | Regenerate schema fixtures after updates. |
| 3 | P1 | `scripts/dry_run_opps.py` | `docs/opps_ingestion.md`, `artifacts/opps_ingestor_tomorrow_plan.md` | Add operator script that runs OPPS ingest in dry-run mode, summarizes counts, and emits evidence bundles. | Captures parquet paths, DB row counts, timings. |

### II. Dependency Checklist (The How and Impact)
- Cascading dependencies: `_normalize_stage` relies on `CMSOPPSScraper.discover_latest()` outputs, Addendum parsers, and `wage_index.enrich_dataframe`.
- Downstream components: schema adapters, dataset snapshot service, validation scripts, Render deploy jobs invoking OPPS ingestion.
- External integrations: CMS S3 buckets (inputs), Render persistent disk for parquet outputs, Postgres canonical tables.
- Critical change location: `_normalize_stage` in `opps_ingestor.py` near the wage-index merge block (~line 560).
- Example pattern:
  ```python
  def _normalize_stage(self, raw_dfs):
      add_a = self._parse_addendum("A", raw_dfs["addendum_a"])
      add_b = self._parse_addendum("B", raw_dfs["addendum_b"])
      merged = add_a.merge(add_b, on=["hcpcs_code", "modifier"], how="outer")
      enriched = wage_index.enrich_dataframe(merged, self._load_wage_index())
      return self.schema_adapter.apply(enriched)
  ```

### III. Testing and Validation Guidance (The Verification)
- Unit / Integration Tests:
  - Extend `tests/ingestion/test_opps_ingestor.py` to assert Addenda parsing, wage-index merge, and schema compliance.
    ```python
    assert enriched_df["wage_index"].notnull().all()
    assert set(enriched_df.columns).issuperset(EXPECTED_SCHEMA_COLUMNS)
    ```
- Fixture Management:
  ```bash
  pytest tests/ingestion/test_opps_ingestor.py --regen-fixtures
  pytest tests/ingestion/test_schema_opps.py --regen-fixtures
  ```
- End-to-End Verification:
  - Run `scripts/dry_run_opps.py --release-id <id>` (or `make ingest_opps --dry-run`) to execute land → publish and confirm parquet files land under `/mnt/data/releases/opps/<release>/`.
  - Compare Postgres row counts to Addenda totals and ensure wage-index enrichment logs show full coverage.
- Acceptance Criteria / Success Metrics:
  - 0 ingestion errors; schema validation passes; parquet + DB row counts within ±1% of expected totals.
  - Dry-run script completes ≤ 20 minutes on staging hardware and outputs evidence bundle.
  - Wage-index column populated for all applicable rows.

### IV. Guardrails, Risk, and Rollback
- Primary Risk: schema mismatch or enrichment bugs causing publish failures or incorrect reimbursement values.
- Pre-Commit / Pre-Deploy Checks: `pytest tests/ingestion/test_opps_ingestor.py`, `pytest tests/ingestion/test_reference_data_manager.py`, dry-run ingestion on staging.
- Rollback Strategy:
  ```bash
  git revert <opps_ingestor_change_sha>
  make ingest_opps --dry-run  # verify previous behavior
  redeploy ingest-opps worker
  ```
- Keep optional feature flag (e.g., `OPPS_ENABLE_WAGE_ENRICHMENT=false`) to disable enrichment quickly if parity issues surface.
