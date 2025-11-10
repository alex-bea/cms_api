REF-Generic Prompts.md


This single block functions as your **generic prompt** for generating detailed coding guidance.


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

