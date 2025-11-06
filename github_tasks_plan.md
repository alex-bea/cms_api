## Task: Optimize Docker Builds (Builder/Runtime Split, Caching, Flags)

Context
- Current Dockerfile installs build tools in the base/runtime image and copies the entire source after dependency install. We can speed up CI builds and reduce image size by separating build-time deps, improving layer caching, and tightening apt/pip installs.

Goals
- Reduce cold build time by ~2–4 minutes and hot (code-only) rebuilds by ~2–4 minutes in CI.
- Decrease image size (100–300 MB), reduce Render pull time, and keep a clean runtime without build toolchain.

Scope
- Refactor `Dockerfile` to a proper builder/runtime pattern.
- Improve Docker layer caching and CI cache usage.
- Tighten APT and pip flags to avoid unnecessary downloads.
- Ensure behavior parity (healthcheck, user permissions, envs, CMD) with production target.

Acceptance Criteria
- Dockerfile uses a dedicated builder stage (with build tools) and a slim runtime stage (no build tools).
- APT installs use `--no-install-recommends`; packages minimized.
- Dependencies installed before source copy to maximize cache hits.
- CI workflow (`.github/workflows/deploy.yml`) still builds `--target production` with `cache-from/to: gha` and succeeds.
- Image builds and runs locally; `/health` returns 200.
- Image size reduced vs current; CI build time improved on both cold and hot paths (document numbers in PR).
- Render deploy succeeds and service runs with the new image.

Proposed Implementation Steps
1) Create a builder stage:
   - Base: `python:3.11-slim`
   - Install build deps with `apt-get update && apt-get install -y --no-install-recommends build-essential python3-dev curl … && rm -rf /var/lib/apt/lists/*`
   - `WORKDIR /app`
   - `COPY requirements.txt .` then `pip install --upgrade pip` and `pip install -r requirements.txt` (optionally with BuildKit cache mount)
   - If wheels are preferred, build wheels into a wheels directory (optional optimization)
2) Create a runtime stage:
   - Base: `python:3.11-slim`
   - Install only runtime OS deps (if any) with `--no-install-recommends`
   - `WORKDIR /app`
   - Copy site-packages (or wheels install) and entry scripts from builder into runtime
   - `COPY . .` for application code (or a narrowed subset if appropriate)
   - Create non-root `appuser` and set directory ownership for `data/*`
   - Define HEALTHCHECK and final CMD (preserve current behavior: use `$PORT`, workers=1)
3) Revise order for cache efficiency:
   - `COPY requirements*.txt` + install deps → then `COPY . .`
   - Keep dynamic files (e.g., source) late so deps layer gets reused on code-only changes
4) CI updates (if needed):
   - Ensure `docker/build-push-action` still uses `platforms: linux/amd64`, `target: production`, and `cache-from/to: gha`
   - Optionally add `jq` ensure step (already present in runners, but safe)
5) Verify locally:
   - Build: `docker build --target production --platform linux/amd64 -t cms_api:test .`
   - Run: `docker run -p 8000:8000 cms_api:test`
   - Health: `curl localhost:8000/health`
6) Verify in CI:
   - Open PR; compare build times and image size vs baseline in PR description
7) Verify on Render:
   - Tag a test release; confirm Deploy API uses the new image; health OK

Notes / Nice-to-Haves
- Use BuildKit pip cache: `RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt` (guarded; runners typically support BuildKit)
- Consider `python -m compileall` in builder to precompile pyc (optional)
- Ensure `.dockerignore` excludes `data/`, `logs/`, VCS and caches (already in place; review for drift)

Owner
- Platform Engineering (API Enablement)

Effort
- ~1–2 hours including verification across local, CI, and Render

Risk
- Low; functional behavior unchanged. Rollback by reverting Dockerfile if needed.

# GitHub Tasks Plan: CMS API Development Tasks

Generated on: 2025-10-03 09:57:53

## Project Overview

**Name:** CMS API Development Tasks
**Description:** Comprehensive task management for CMS API development, data ingestion, and system enhancement
**Total Tasks:** 63

- 2025-10-30 2025-10-30 19:22:37: Schema-driven validation flow scoped for RVU pipeline (see [RVU Pipeline Completion Plan](artifacts/RVU_PIPELINE_COMPLETION_PLAN_REVIEW.md)).
- 2025-10-31 2025-10-31 03:12:42 ✅ Ensure RVU pipeline metadata (batch_id, release_id, vintage_date, quality metrics) flows through normalize → enrich → publish; add StageFrame wrapper where needed; update tests to assert metadata presence. (Completed 2025-10-31; see `prds/RUN-render-deployment-prd-v1.0.md` for deployment notes.)
- 2025-10-31 2025-10-31 03:12:42 ✅ Publish stage should persist dataset manifests & actual parquet paths when receiving dict payloads; update `_save_data_with_upserts` to return written files and wire into curated table listings. (Completed 2025-10-31; Render manifest verified.)

### Architecture Layers (Data Flow Context)

To keep the task backlog aligned with the system’s runtime architecture, the plan now calls out two cross-cutting layers that every feature should map to:

1. **Parser & Transformation Layer**  
   - Anchored in the DIS lifecycle (**Land → Validate → Normalize → Enrich → Publish**) defined in `prds/STD-data-architecture-prd-v1.0.md`.  
   - Parsers live under `cms_pricing/ingestion/parsers/**` and follow the contract kit so every dataset enforces schema governance, drift detection, and provenance tags.  
   - Layout registries plus parser routing tables make sure new vintages (e.g., RVU, GPCI, OPPS caps) keep deterministic column mappings before anything touches curated storage.  
   - Related tasks must include updates to schema contracts, parser tests (golden + negative), and ingestion manifests so CI can trace exactly which version of a parser produced a dataset artifact.

2. **Database & Modeling Layer**  
   - Normalized parquet outputs land in Postgres via SQLAlchemy models (`cms_pricing/models/**`) and Alembic revisions (`alembic/versions/**`), giving the FastAPI layer strongly typed tables and views.  
   - Standards from `prds/STD-database-platform-prd-v1.0.md` and `RUN-database-migrations-prd-v1.0.md` govern DDL ownership, migration jobs, and least-privilege roles.  
   - Features that add or mutate curated tables must define natural keys, effective dating, and upsert semantics so they remain compatible with the pricing engines and tracing infrastructure.  
   - Observability hooks (Prometheus metrics, trace storage) rely on these models, so task descriptions should link to the specific ORM entities/migrations they touch.

Any new backlog item should mention how it threads through these two layers (e.g., “extend parser X and model Y”) to avoid gaps between ingestion outputs and serving endpoints.

## GitHub Project Setup Instructions

### Step 1: Install GitHub CLI (if not already installed)

```bash
# macOS
brew install gh

# Ubuntu/Debian
sudo apt install gh

# Windows
winget install GitHub.cli
```

### Step 2: Authenticate with GitHub

```bash
gh auth login
```

### Step 3: Create GitHub Project

```bash
# Create the project
gh project create --title "CMS API Development Tasks" --body "Comprehensive task management for CMS API development, data ingestion, and system enhancement"

# Note the project number from the output
PROJECT_NUMBER=<project_number>
```

### Step 4: Set up Project Views

```bash
# Add Board View (default)
gh project item-add $PROJECT_NUMBER --url https://github.com/alex-bea/cms_api

# Add Table View
gh project view-create $PROJECT_NUMBER --view table --title "Table View"

# Add Timeline View  
gh project view-create $PROJECT_NUMBER --view timeline --title "Timeline View"
```

### Step 5: Create Custom Fields

```bash
# Priority field
gh project field-create $PROJECT_NUMBER --name "Priority" --single-select-option "Critical" --single-select-option "High" --single-select-option "Medium" --single-select-option "Low"

# Category field
gh project field-create $PROJECT_NUMBER --name "Category" --single-select-option "Data Ingestion" --single-select-option "API Development" --single-select-option "Testing" --single-select-option "Security" --single-select-option "Performance" --single-select-option "Monitoring" --single-select-option "Documentation" --single-select-option "Database" --single-select-option "DevOps" --single-select-option "General"

# Estimated Time field
gh project field-create $PROJECT_NUMBER --name "Estimated Time" --text

# Phase field
gh project field-create $PROJECT_NUMBER --name "Phase" --single-select-option "Phase 1: Core" --single-select-option "Phase 2: Enhancement" --single-select-option "Phase 3: Optimization"
```

### Step 6: Create Labels

```bash
# Create labels for the repository
gh label create "ingestion" --description "Tasks related to ingestion"
gh label create "api" --description "Tasks related to api"
gh label create "testing" --description "Tasks related to testing"
gh label create "security" --description "Tasks related to security"
gh label create "performance" --description "Tasks related to performance"
gh label create "monitoring" --description "Tasks related to monitoring"
gh label create "documentation" --description "Tasks related to documentation"
gh label create "database" --description "Tasks related to database"
gh label create "devops" --description "Tasks related to devops"
gh label create "critical" --description "Tasks related to critical"
gh label create "high-priority" --description "Tasks related to high-priority"
gh label create "medium-priority" --description "Tasks related to medium-priority"
gh label create "low-priority" --description "Tasks related to low-priority"
gh label create "bug" --description "Tasks related to bug"
gh label create "feature" --description "Tasks related to feature"
gh label create "enhancement" --description "Tasks related to enhancement"

```

## Task List

### Summary by Category

- **Data Ingestion**: 14 tasks
- **Documentation & Standards**: 10 tasks (PRD Improvements)
- **General**: 32 tasks
- **API Development**: 3 tasks
- **Performance**: 3 tasks
- **Security**: 2 tasks
- **Monitoring**: 3 tasks
- **Testing**: 3 tasks
- **Documentation**: 1 tasks
- **Database**: 2 tasks

### Summary by Priority

- **Critical**: 8 tasks
- **High**: 12 tasks
- **Medium**: 16 tasks
- **Low**: 27 tasks

## Detailed Task List

### Task 64: Operationalize RVU Ingestor Pipeline ✅ COMPLETE (Outdated - Superseded)

**Category:** Data Ingestion  
**Priority:** High  
**Estimated Time:** 2-3 days  
**Labels:** ingestion, high-priority, rvu, backend  
**Status:** 🗑️ OUTDATED (Outdated: Replaced with real implementation)

**Context:** ~~`cms_pricing/ingestion/ingestors/rvu_ingestor.py:1098` still emits mock DataFrames~~ **OUTDATED:** This task described replacing mock data, which has been fully completed. The RVU ingestor now has a complete implementation with all parsers wired in.

**Detailed Steps:**
1. ✅ Replace `_adapt_raw_data_sync` to iterate the discovered ZIP payloads, route each member through the appropriate parser, and return real `AdaptedBatch` objects with schema metadata.  
   - **Completed:** `rvu_ingestor.py` has `_adapt_raw_data_sync()` method that routes to all parsers (`parse_pprrvu`, `parse_gpci`, `parse_oppscap`, `parse_anes`, `parse_locality_raw`)
2. ✅ Wire validators and publishers so `Land → Validate → Normalize → Publish` aligns with `prds/STD-data-architecture-prd-v1.0.md` expectations, including provenance fields and discovery manifest integration.  
   - **Completed:** Full DIS pipeline implemented with `land()`, `validate()`, `normalize()`, `enrich()`, `publish()` methods. All stages wired and tested.
3. ✅ Add regression tests under `tests/ingestion/` that simulate a quarterly release using fixture ZIPs to ensure parsers, validation, and publishing execute end-to-end.  
   - **Completed:** `tests/ingestors/test_rvu_ingestor_e2e.py` has comprehensive E2E tests (13 tests, all passing) covering full pipeline.
4. ✅ Update `CHANGELOG.md` and relevant PRDs to record the transition from mock data to full ingestion, and document any new configuration or operational runbooks.
   - **Completed:** Implementation documented in code and tests. PRDs updated with optimization guidance.

---

### Task 65: Expand Layout Registry for Historical Vintages

**Category:** Data Ingestion  
**Priority:** High  
**Estimated Time:** 1-2 days  
**Labels:** ingestion, schema, high-priority, documentation

**Context:** The layout registry (`cms_pricing/ingestion/parsers/layout_registry.py:35-186`) only includes 2025 SemVer entries. Newly scraped historical files fall back to CSV heuristics or fail fixed-width parsing, blocking backfills and quarterly comparisons required by `prds/REF-cms-pricing-source-map-prd-v1.0.md`.

**Detailed Steps:**
**Status:** ✅ COMPLETE
1. Inventory past-year RVU bundles using the discovery manifests (`data/ingestion/cms_simple/raw/.../manifest.json`) and populate layout specs for each dataset/year/quarter combination back to the required baseline.  
2. Diff column widths and positions against schema contracts (e.g., `cms_pricing/ingestion/contracts/cms_gpci_v1.3.json`) to ensure SemVer bumps match breaking vs compatible changes.  
3. Add parser unit tests that load historical fixtures to verify layout selection, and introduce CI drift checks that fail when CMS shifts column positions without a corresponding layout update.  
4. Document the expanded coverage in the source map PRD and update parser plans (e.g., `planning/parsers/oppscap/OPPSCAP_PARSER_PLAN.md`) so future vintages reference the maintained registry.

---

### Task 66: Harden Discovery-to-Curated Test Coverage

**Category:** Testing  
**Priority:** Medium  
**Estimated Time:** 1 week  
**Labels:** testing, ingestion, medium-priority, qa

**Context:** Current tests such as `test_cms_rvu_scraper.py` are demo scripts that print results without assertions, leaving gaps in validating quarterly rollover handling, vintage metadata, and publish stages (`STD-parser-contracts-prd-v2.0.md` requires deterministic outputs and row-content hashes).

**Status:** ✅ COMPLETE
**Detailed Steps:**
1. Convert scraper demos into pytest suites with mocked HTTP responses, asserting discovery manifests contain year/quarter metadata and SHA-256 digests.  
2. Add integration tests that feed discovered files through RVU/MPFS/OPPS ingestors, asserting curated Parquet artifacts include vintage fields, natural-key uniqueness, and hash stability.  
3. Provide fixture data covering multiple quarters/years to verify historical backfill behavior and schema drift alerts.  
4. Update QA documentation (`STD-parser-contracts-prd-v2.0.md` companions) with the new test matrix and ensure CI gates execute the suite before deployment.

---

### Task 67: Fix Connection Pool Configuration for Sync ORM

**Category:** Database  
**Priority:** High  
**Estimated Time:** 0.5 day  
**Labels:** database, performance, high-priority  
**Status:** ✅ COMPLETE

**Context:** `cms_pricing/database.py:12` uses `StaticPool`, forcing all FastAPI requests to share a single connection. Under concurrency this creates lock contention and request pileups. Swapping to SQLAlchemy’s default QueuePool with tuned limits satisfies the pooling standard in `prds/STD-database-platform-prd-v1.0.md`.

**Detailed Steps:**
1. Update `create_engine` to drop `StaticPool`, set sensible `pool_size`, `max_overflow`, and `pool_timeout`, and expose overrides through settings.  
2. Run load tests against `/pricing/price` and `/pricing/codes/price` to confirm connection reuse and absence of pool exhaustion errors.  
3. Document the new pool defaults in the database runbook and wire env overrides for staging/prod.

---

### Task 68: Add Docker Smoke Test to CI Workflow

**Category:** DevOps  
**Priority:** Medium  
**Estimated Time:** 0.5 day  
**Labels:** devops, testing, automation  
**Status:** ✅ COMPLETE

**Context:** The Build-Only workflow now pushes the production image to GHCR but does not perform a runtime validation. Adding an automated smoke test ensures the image boots and the `/health` endpoint responds before artifacts are published.

**Detailed Steps:**
1. Extend `.github/workflows/build-only.yml` (or deployment workflow) with a post-build step guarded to run for the production target: `docker run --rm -e PORT=8080 -p 8080:8080 <image> ...`.
2. Poll `/health` (and optionally `/readyz`) using `curl --retry` inside the workflow to verify a 200 response, then stop the container.
3. Capture container logs on failure to aid debugging and mark the job failed if the smoke test does not succeed.
4. Document the smoke test behavior in the release checklist (`RUN-render-deployment-prd-v1.0.md`).

---

### Task 69: Integrate Container Vulnerability Scanning

**Category:** Security  
**Priority:** Medium  
**Estimated Time:** 1 day  
**Labels:** security, devops, automation  
**Status:** ✅ COMPLETE

**Context:** With the production image now published to GHCR, we need an automated vulnerability scan (Trivy/Grype) to gate releases and surface CVEs aligned with security governance.

**Detailed Steps:**
1. Add a step to the build workflow (production target only) that pulls the freshly built image and runs `aquasecurity/trivy-action@v0` or `anchore/scan-action@v4`.
2. Configure the scanner to fail on High/Critical findings, uploading SARIF output for GitHub code scanning alerts.
3. Store scan reports as workflow artifacts and notify the security channel if issues exceed thresholds.
4. Update `RUN-render-deployment-prd-v1.0.md` with instructions for handling scan failures and remediations.

---

### Task 68: Repair Async DB Connector URL

**Category:** Database  
**Priority:** Medium  
**Estimated Time:** 0.25 day  
**Labels:** database, devops, medium-priority  
**Status:** ✅ COMPLETE

**Context:** `cms_pricing/database.py:35` rewrites the Postgres DSN to `postgresql+asyncpg://`, which `asyncpg.connect` cannot parse. Background tasks therefore fail to connect. Aligning the DSN with asyncpg expectations restores the asynchronous dependency path.

**Detailed Steps:**
1. Remove the `.replace()` shim so `asyncpg.connect` receives the native DSN; add lightweight retry/error logging.  
2. Add an integration smoke test that exercises an async code path (or stub) to prove the connector works.  
3. Ensure deployment secrets and env files carry the same DSN for async workers; update docs if format requirements change.

---

### Task 69: Cache Parsed API Keys

**Category:** Security  
**Priority:** Medium  
**Estimated Time:** 0.25 day  
**Labels:** security, api, medium-priority  
**Status:** ✅ COMPLETE

**Context:** Both `cms_pricing/auth.py:12` and `cms_pricing/middleware.py:68` split the comma-separated API key string on every request. Caching the parsed set at startup reduces overhead and aligns with the RBAC readiness plan.

**Detailed Steps:**
1. Cache the parsed API key list/set on `settings` during startup and reuse it across auth checks.  
2. Update unit tests to cover cached vs reloaded configurations (including env override scenarios).  
3. Add debug metrics or logs to confirm validation hits the cached lookup without repeated parsing.

---

### Task 70: Provide Default Deductible in Cost Sharing Helper

**Category:** API Development  
**Priority:** Medium  
**Estimated Time:** 0.25 day  
**Labels:** api, backend, medium-priority  
**Status:** ✅ COMPLETE

**Context:** `_calculate_beneficiary_cost_sharing` (`cms_pricing/engines/base.py:39`) requires `deductible_remaining`, yet engine callers omit it. The helper raises `TypeError`, preventing correct pricing. Supplying a default unblocks engine usage and matches the pricing PRD.

**Detailed Steps:**
1. Give `_calculate_beneficiary_cost_sharing` a sensible default (e.g., `deductible_remaining: float = 0.0`).  
2. Update engines to pass explicit remaining deductibles when they are known (future-proof for plan data).  
3. Extend engine unit tests to cover defaulted and explicit deductible paths.

---

### Task 71: Normalize Date Filters in Pricing Engines

**Category:** Performance  
**Priority:** Medium  
**Estimated Time:** 0.5 day  
**Labels:** performance, database, medium-priority  
**Status:** ✅ COMPLETE

**Context:** `cms_pricing/engines/mpfs.py:47`, `cms_pricing/engines/opps.py:64`, and `cms_pricing/engines/asc.py:53` compare dates as strings (`"2025-12-31"`). Casting forces table scans and breaks index usage. Switching to `date()` comparisons restores planner optimizations.

**Detailed Steps:**
1. Replace string literals with `date(year, month, day)` helpers and centralize the logic for reuse.  
2. Share a utility function (e.g., `effective_date_filters.py`) so engines stay consistent and testable.  
3. Benchmark representative queries before/after to confirm index usage and update performance notes.

---

### Task 72: Share Request-Scoped Sessions Across Engines

**Category:** Performance  
**Priority:** High  
**Estimated Time:** 1 day  
**Labels:** performance, api, database, high-priority  
**Status:** ✅ COMPLETE

**Context:** `PricingService`, `GeographyService`, `TraceService`, and each engine instantiate `SessionLocal()` independently. Requests therefore hold multiple connections, complicating transaction boundaries and leaking sessions. Injecting the FastAPI-scoped session fixes lifecycle control.

**Detailed Steps:**
1. Refactor services/engines to accept an injected `Session` instead of constructing `SessionLocal()` internally.  
2. Update FastAPI dependency wiring so a single session flows through the entire request and closes cleanly.  
3. Add teardown tests (or instrumentation) ensuring connections release at request end and no extra commits occur.

---

### Task 73: Optimize Nearest-ZIP Candidate Query

**Category:** Performance  
**Priority:** Medium  
**Estimated Time:** 0.5 day  
**Labels:** performance, geography, medium-priority  
**Status:** ✅ COMPLETE

**Context:** `_find_zip_candidates_in_radius` (`cms_pricing/services/geography.py:358`) re-queries the entire state for each radius increment and ignores effective-date filters. This slows ZIP resolution. A single, filtered fetch reused across iterations reduces DB load.

**Detailed Steps:**
1. Fetch eligible ZIP geometries once per resolution (respecting effective-date predicates) and reuse in-memory as radius expands.  
2. Incorporate the effective-date filter logic into the initial query to avoid stale results.  
3. Add profiling logs or tests to validate the reduced query count and improved response time.

---

### Task 74: Add LRU Cache for ZIP Resolutions

**Category:** Performance  
**Priority:** Medium  
**Estimated Time:** 0.5 day  
**Labels:** performance, caching, geography, medium-priority  
**Status:** ✅ COMPLETE

**Context:** Popular ZIP codes trigger repeated DB lookups in `GeographyService.resolve_zip`. A small in-memory LRU/TTL cache (respecting HIPAA constraints) alleviates hot path latency and aligns with readiness caching goals.

**Detailed Steps:**
1. Introduce a TTL/LRU cache keyed by `(zip5, valuation_year, quarter)` with safe eviction policies.  
2. Expose cache statistics via the geography health endpoint for observability.  
3. Create fuzz tests ensuring stale entries expire and cache misses repopulate correctly.

---

### Task 75: Bulk-Insert Trace Inputs and Outputs

**Category:** Monitoring  
**Priority:** Medium  
**Estimated Time:** 0.5 day  
**Labels:** monitoring, performance, medium-priority  
**Status:** ✅ COMPLETE

**Context:** `TraceService.store_run` (`cms_pricing/services/trace.py:89`) calls `self.db.add` in loops, generating many round-trips. Batched inserts or `bulk_save_objects` bring latency in line with observability SLOs.

**Detailed Steps:**
1. Switch to `session.add_all` or SQLAlchemy bulk APIs for `RunInput`/`RunOutput` collections.  
2. Ensure the transaction commits once per run and capture timing metrics to validate improvement.  
3. Update tests to assert row counts and ordering after bulk insertion.

---

### Task 76: Make ORM Usage Async-Friendly

**Category:** Performance  
**Priority:** Medium  
**Estimated Time:** 1 day  
**Labels:** performance, api, async, medium-priority  
**Status:** ✅ COMPLETE

**Context:** Async endpoints call synchronous ORM code (`pricing_service.price_plan`, geography lookups), blocking the event loop. Wrapping database work in `asyncio.to_thread` (short term) or switching to `AsyncSession` preserves responsiveness.

**Detailed Steps:**
1. Wrap blocking ORM sections in `asyncio.to_thread` while planning the longer-term `AsyncSession` migration.  
2. Add concurrency stress tests (or Locust scenarios) to demonstrate improved event-loop responsiveness.  
3. Document the async pattern for future endpoints and enforce via linting/static checks.

---

### Task 1: CMS Website Scraper

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** data-ingestion, critical-priority, from-todos

**Description:**
**Status:** ✅ COMPLETE
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** CMS Website Scraper

**Source:** NEXT_TODOS.md

---

### Task 2: File Download Manager

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** data-ingestion, critical-priority, from-todos

**Status:** ✅ COMPLETE
**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** File Download Manager

**Source:** NEXT_TODOS.md

---

### Task 3: Release Detection System

**Category:** General
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** general, critical-priority, from-todos
**Status:** ✅ COMPLETE

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** Release Detection System

**Source:** NEXT_TODOS.md

---

### Task 4: CMS API Integration

**Category:** API Development
**Priority:** Critical
**Estimated Time:** TBD
**Status:** ✅ COMPLETE
**Labels:** api-development, critical-priority, from-todos

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** CMS API Integration

**Source:** NEXT_TODOS.md

---

### Task 5: Third-Party Data Sources

**Category:** General
**Priority:** Critical
**Status:** ✅ COMPLETE
**Estimated Time:** TBD
**Labels:** general, critical-priority, from-todos

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** Third-Party Data Sources

**Source:** NEXT_TODOS.md

---

### Task 6: Streaming Data Pipeline

**Category:** General
**Status:** ✅ COMPLETE
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Streaming Data Pipeline

**Source:** NEXT_TODOS.md

---

### Task 7: Incremental Updates

**Status:** ✅ COMPLETE
**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Incremental Updates

**Source:** NEXT_TODOS.md

---

### Task 8: Data Analytics Dashboard
**Status:** ✅ COMPLETE

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Data Analytics Dashboard

**Source:** NEXT_TODOS.md

---

**Status:** ❓ UNKNOWN
### Task 9: Predictive Analytics

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Status:** ✅ COMPLETE
**Details:** Predictive Analytics

**Source:** NEXT_TODOS.md

---
**Status:** ❓ UNKNOWN

### Task 10: Advanced Query Capabilities

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement
**Status:** ✅ COMPLETE

**Details:** Advanced Query Capabilities

**Source:** NEXT_TODOS.md

**Status:** ❓ UNKNOWN
---

### Task 11: API Versioning & Backward Compatibility

**Category:** API Development
**Priority:** High
**Estimated Time:** TBD
**Labels:** api-development, high-priority, from-todos

**Description:**
**Status:** ✅ COMPLETE
**Section:** Phase 2: System Enhancement

**Details:** API Versioning & Backward Compatibility

**Source:** NEXT_TODOS.md
**Status:** ❓ UNKNOWN

---

### Task 12: Horizontal Scaling

**Category:** Performance
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** performance, medium-priority, from-todos

**Status:** ✅ COMPLETE
**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Horizontal Scaling

**Status:** ❓ UNKNOWN
**Source:** NEXT_TODOS.md

---

### Task 13: Performance Optimization

**Category:** Performance
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** performance, medium-priority, from-todos

**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Performance Optimization
**Status:** ❓ UNKNOWN

**Source:** NEXT_TODOS.md

---

### Task 14: Security Hardening

**Category:** Security
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** security, medium-priority, from-todos
**Status:** ✅ COMPLETE

**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Status:** ❓ UNKNOWN
**Details:** Security Hardening

**Source:** NEXT_TODOS.md

---

### Task 15: Compliance & Governance

**Category:** Security
**Priority:** Medium
**Estimated Time:** TBD
**Status:** ✅ COMPLETE
**Labels:** security, medium-priority, from-todos

**Description:**
**Section:** Phase 3: Infrastructure & Operations
**Status:** ❓ UNKNOWN

**Details:** Compliance & Governance

**Source:** NEXT_TODOS.md

---

### Task 16: Advanced Monitoring

**Category:** Monitoring
**Priority:** Medium
**Status:** ✅ COMPLETE
**Estimated Time:** TBD
**Labels:** monitoring, medium-priority, from-todos

**Description:**
**Status:** ❓ UNKNOWN
**Section:** Phase 3: Infrastructure & Operations

**Details:** Advanced Monitoring

**Source:** NEXT_TODOS.md

---

### Task 17: Intelligent Alerting

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** monitoring, medium-priority, from-todos

**Status:** ❓ UNKNOWN
**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Intelligent Alerting

**Source:** NEXT_TODOS.md

---

### Task 18: Cross-Dataset Validation

**Category:** Testing
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** testing, medium-priority, from-todos
**Status:** ❓ UNKNOWN

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Cross-Dataset Validation

**Source:** NEXT_TODOS.md

---

### Task 19: Data Lineage & Provenance

**Category:** General
**Priority:** Medium
**Estimated Time:** TBD
**Status:** ❓ UNKNOWN
**Labels:** general, medium-priority, from-todos

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Data Lineage & Provenance

**Source:** NEXT_TODOS.md

---

### Task 20: Automated Testing

**Category:** Testing
**Status:** ✅ COMPLETE
**Priority:** Medium
**Status:** ❓ UNKNOWN
**Estimated Time:** TBD
**Labels:** testing, medium-priority, from-todos

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Automated Testing

**Source:** NEXT_TODOS.md

---

### Task 21: Data Quality Monitoring

**Status:** ✅ COMPLETE
**Category:** Monitoring
**Status:** ❓ UNKNOWN
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** monitoring, medium-priority, from-todos

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Data Quality Monitoring

**Source:** NEXT_TODOS.md

---

### Task 22: Third-Party Integrations

**Status:** ❓ UNKNOWN
**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority, from-todos

**Description:**
**Section:** Phase 5: Integration & Ecosystem

**Details:** Third-Party Integrations

**Source:** NEXT_TODOS.md

---

### Task 23: Data Export & Import
**Status:** ❓ UNKNOWN

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority, from-todos

**Description:**
**Section:** Phase 5: Integration & Ecosystem

**Details:** Data Export & Import

**Source:** NEXT_TODOS.md

---

**Status:** ❓ UNKNOWN
### Task 24: Documentation & Tools

**Category:** Documentation
**Priority:** Low
**Estimated Time:** TBD
**Labels:** documentation, low-priority, from-todos

**Description:**
**Section:** Phase 5: Integration & Ecosystem

**Details:** Documentation & Tools

**Source:** NEXT_TODOS.md

---
**Status:** ✅ COMPLETE

### Task 25: Testing & Development Tools

**Category:** Testing
**Priority:** Low
**Estimated Time:** TBD
**Labels:** testing, low-priority, from-todos

**Description:**
**Section:** Phase 5: Integration & Ecosystem

**Details:** Testing & Development Tools

**Source:** NEXT_TODOS.md

---

**Status:** ✅ COMPLETE
### Task 26: Task 1: MPFS Ingester**

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, critical-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 1: Critical Core CMS Ingestors

**Task:** MPFS Ingester**

**Priority:** Critical

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---
**Status:** ✅ COMPLETE

### Task 27: Task 2: OPPS Ingester**

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, critical-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 1: Critical Core CMS Ingestors

**Task:** OPPS Ingester**

**Priority:** Critical

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

**Status:** ✅ COMPLETE
---

### Task 28: Task 3: ASC Ingester**

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, critical-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 1: Critical Core CMS Ingestors

**Task:** ASC Ingester**

**Priority:** Critical

**Source:** INGESTOR_DEVELOPMENT_TASKS.md
**Status:** ✅ COMPLETE

---

### Task 29: Task 4: CLFS Ingester**

**Category:** Data Ingestion
**Priority:** High
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, high-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 2: Supporting CMS Ingestors

**Task:** CLFS Ingester**

**Priority:** High

**Status:** ✅ COMPLETE
**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 30: Task 5: DMEPOS Ingester**

**Category:** Data Ingestion
**Priority:** High
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, high-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 2: Supporting CMS Ingestors

**Task:** DMEPOS Ingester**

**Priority:** High
**Status:** ✅ COMPLETE

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 31: Task 6: ASP Ingester**

**Category:** Data Ingestion
**Priority:** High
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, high-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 2: Supporting CMS Ingestors

**Task:** ASP Ingester**

**Status:** ✅ COMPLETE
**Priority:** High

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 32: Task 7: IPPS Ingester**

**Category:** Data Ingestion
**Priority:** High
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, high-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 2: Supporting CMS Ingestors

**Status:** ✅ COMPLETE
**Task:** IPPS Ingester**

**Priority:** High

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 33: Task 8: Census Geography Ingester**

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 3: Reference Data Ingestors

**Task:** Census Geography Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 33a: Census Reference Data Scraper (TIGER/Line Counties)

**Category:** Data Infrastructure
**Priority:** Medium
**Estimated Time:** 1-2 days
**Labels:** reference-data, scraper, census, medium-priority

**Description:**
**Phase:** Reference Data Automation

**Task:** Census Reference Data Scraper (TIGER/Line Counties & Gazetteer)

**Purpose:** Automated scraper for Census TIGER/Line county reference data to ensure `us_counties.csv` stays current with annual Census releases.

**Scope:**
- Scraper for Census 2025 Gazetteer Counties National file
- Downloads from: https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_counties_national.zip
- Validates record count (~3,200 counties/equivalents)
- Spot-checks key counties (VA independent cities, LA parishes, AK boroughs, diacritics)
- Generates `us_counties.csv` with authority_version metadata
- Updates manifest.json with SHA-256, record count, download date
- Runs on demand or scheduled (annual refresh)

**Deliverables:**
- `cms_pricing/ingestion/scrapers/census_gazetteer_scraper.py`
- Integration with existing scraper CLI (`cms_pricing/ingestion/scrapers/cli.py`)
- Unit tests for parsing and validation
- Documentation in `SRC-gazetteer.md` (new)

**Benefits:**
**Status:** ✅ COMPLETE
- Ensures reference data stays current with Census annual releases
- Auditable provenance (source URL, download date, SHA-256)
- Aligns with STD-scraper-prd-v1.0.md patterns
- Prevents manual download/conversion errors

**Dependencies:**
- REF-geography-source-map-prd-v1.0.md (existing)
- STD-data-architecture-impl-v1.0.md §4.2 (Dual-Mode Reference Data)
- STD-scraper-prd-v1.0.md (scraper patterns)

**Acceptance Criteria:**
- Scraper downloads 2025 Gazetteer file successfully
- Validates ~3,220 records ±10
- Spot-checks pass (Doña Ana, Alexandria city, Orleans Parish, Bethel Census Area)
- Generates us_counties.csv matching expected schema (8 columns)
- Updates manifest.json with correct metadata
- CLI integration: `python -m cms_pricing.ingestion.scrapers.cli census-counties --year 2025`
**Status:** ✅ COMPLETE
- Unit tests cover parsing, validation, error handling

**Priority:** Medium (annual refresh needed; manual process currently works)

**Source:** Locality Parser Stage 2 implementation (2025-10-17)

---

### Task 34: Task 9: HRSA Ingester**

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 3: Reference Data Ingestors
**Status:** ✅ COMPLETE

**Task:** HRSA Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 35: Task 10: NBER Distance Ingester**
**Status:** ✅ COMPLETE

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Status:** ❓ UNKNOWN
**Phase:** Phase 3: Reference Data Ingestors

**Task:** NBER Distance Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

**Status:** ✅ COMPLETE
### Task 36: Task 11: NADAC Ingester**

**Category:** Data Ingestion
**Status:** ❓ UNKNOWN
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 3: Reference Data Ingestors

**Task:** NADAC Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

**Status:** ✅ COMPLETE
---

### Task 37: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:61`

**Details:** \s*(.+)",
**Status:** ✅ COMPLETE

---

### Task 38: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:62`

**Status:** ✅ COMPLETE
**Details:** \s*(.+)",

---

### Task 39: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:63`
**Status:** ✅ COMPLETE

**Details:** \s*(.+)",

---

### Task 40: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**Status:** ✅ COMPLETE
**File:** `tools/github_tasks_setup.py:64`

**Details:** \s*(.+)",

---

### Task 41: General: Implement actual task processing

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Status:** ✅ COMPLETE
**Description:**
**File:** `cms_pricing/worker.py:55`

**Details:** Implement actual task processing

---

### Task 42: General: Implement cache warming

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority
**Status:** ✅ COMPLETE

**Description:**
**File:** `cms_pricing/main.py:98`

**Details:** Implement cache warming

---

### Task 43: General: Get from benefit params

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Status:** ✅ COMPLETE
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/engines/ipps.py:113`

**Details:** Get from benefit params

---

### Task 44: General: Implement proper tracking

**Category:** General
**Priority:** Low
**Status:** ✅ COMPLETE
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/nearest_zip_monitoring.py:40`

**Details:** Implement proper tracking

---

### Task 45: General: Extract from traces

**Category:** General
**Status:** ❓ UNKNOWN
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:168`

**Details:** Extract from traces

---
**Status:** ✅ COMPLETE

### Task 46: General: Extract from traces

**Status:** ❓ UNKNOWN
**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:169`

**Details:** Extract from traces

**Status:** ✅ COMPLETE
---

### Task 47: General: Extract from traces
**Status:** ✅ COMPLETE

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:170`

**Details:** Extract from traces

**Status:** ✅ COMPLETE
---

**Status:** ✅ COMPLETE
### Task 48: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:171`

**Status:** ✅ COMPLETE
**Details:** Extract from traces
**Status:** ✅ COMPLETE

---
**Status:** ❓ UNKNOWN

### Task 49: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:172`
**Status:** ✅ COMPLETE

**Status:** ✅ COMPLETE
**Details:** Extract from traces

**Status:** ❓ UNKNOWN
---

### Task 50: General: Implement actual replay logic

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**Status:** ✅ COMPLETE
**File:** `cms_pricing/services/trace.py:199`
**Status:** ✅ COMPLETE

**Details:** Implement actual replay logic
**Status:** ❓ UNKNOWN

---

### Task 51: General: Get from config

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Status:** ✅ COMPLETE
**Description:**
**Status:** ✅ COMPLETE
**File:** `cms_pricing/services/geography_trace.py:25`

**Status:** ❓ UNKNOWN
**Details:** Get from config

---

### Task 52: General: Extract from result if available

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority
**Status:** ✅ COMPLETE

**Status:** ✅ COMPLETE
**Description:**
**File:** `cms_pricing/services/geography_trace.py:102`
**Status:** ❓ UNKNOWN

**Details:** Extract from result if available

---

### Task 53: Performance: Check performance SLOs

**Category:** Performance
**Priority:** Medium
**Estimated Time:** TBD
**Status:** ✅ COMPLETE
**Labels:** performance, medium-priority
**Status:** ✅ COMPLETE

**Description:**
**Status:** ❓ UNKNOWN
**File:** `cms_pricing/services/geography_health.py:97`

**Details:** Check performance SLOs

---

### Task 54: General: Implement snapshots table

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Status:** ✅ COMPLETE (2025-01-15)
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:138`

**Details:** Implement snapshots table

**Completion Notes:**
- ✅ Created `dataset_snapshots` table via Alembic migration `98567c0bbfa8`
- ✅ Implemented `DatasetSnapshot` SQLAlchemy model
- ✅ Built `DatasetSnapshotService` with snapshot selection logic
- ✅ Created registration script `scripts/register_dataset_snapshots.py`
- ✅ Added `/snapshots/health` endpoint
- **Quick Win #1:** Dataset Snapshots Table (Complete)

---

### Task 55: General: Implement proper snapshot selection logic

**Category:** General
**Priority:** Low
**Status:** ✅ COMPLETE (2025-01-15)
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:155`

**Details:** Implement proper snapshot selection logic

**Completion Notes:**
- ✅ Implemented `DatasetSnapshotService.select_snapshot()` with deterministic selection based on valuation date
- ✅ Integrated into `PricingService._collect_datasets_used()` with fallback to trace_refs extraction
- ✅ Supports snapshot selection with deterministic ordering (latest effective_from, then created_at)
- **Quick Win #1:** Dataset Snapshots Table (Complete)

---

### Task 56: General: Implement active snapshot management

**Category:** General
**Status:** ✅ COMPLETE (2025-01-15)
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:174`

**Details:** Implement active snapshot management

**Completion Notes:**
- ✅ Snapshot registration script supports batch operations from fee schedule tables
- ✅ Health endpoint provides visibility into active snapshots by dataset
- ✅ Service layer queries snapshot table for provenance metadata
- ✅ Fallback mechanisms ensure pricing works even when snapshots aren't registered
- **Quick Win #1:** Dataset Snapshots Table (Complete)

---

### Task 57: General: Calculate remaining deductible

**Status:** ✅ COMPLETE
**Category:** General
**Priority:** Low
**Status:** 🗑️ OUTDATED
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:212`

**Details:** Calculate remaining deductible

---

### Task 58: General: Collect dataset information

**Category:** General
**Priority:** Low
**Status:** ✅ COMPLETE (2025-01-15)
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:216`

**Details:** Collect dataset information

**Completion Notes:**
- ✅ Implemented `_collect_datasets_used()` in `PricingService` to aggregate provenance from line items
- ✅ Queries `DatasetSnapshot` table for authoritative metadata (digest, effective dates)
- ✅ Falls back to extracting provenance from `trace_refs` when snapshot not found
- ✅ All pricing responses now include `datasets_used` with `dataset_id`, `release_id`, `batch_id`
- **Phase 2.6:** Service Layer Provenance Aggregation (Complete)

---

### Task 59: Database: Implement database loading

**Category:** Database
**Priority:** Low
**Estimated Time:** TBD
**Labels:** database, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:434`

**Status:** ✅ COMPLETE
**Details:** Implement database loading

---

### Task 60: Database: Implement database loading

**Category:** Database
**Priority:** Low
**Estimated Time:** TBD
**Labels:** database, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:439`
**Status:** ✅ COMPLETE

**Details:** Implement database loading

---

### Task 61: API Development: Wire pricing plan persistence and dataset provenance

**Category:** API Development
**Priority:** High
**Status:** 🗑️ OUTDATED (Outdated: Placeholder replaced with actual code)
**Estimated Time:** TBD
**Status:** ✅ COMPLETE (2025-01-15)
**Labels:** api-development, high-priority

**Description:**
**Files:** `cms_pricing/services/pricing.py:212`, `cms_pricing/services/pricing.py:216`, `cms_pricing/services/trace.py:168`, `cms_pricing/services/pricing.py:434`

**Details:** Capture stored plan components and dataset metadata in pricing responses, propagate dataset usage into trace records, and replace placeholder loaders to deliver full parity and auditing.

**Implementation Steps:**
- ✅ Load stored plan definitions by filling in `_load_plan_components`/`_get_plan_name` with SQLAlchemy queries so pricing reuses persisted sequences and metadata.
- ✅ Normalize stored and ad-hoc components through a shared helper before calling the engines to keep pricing inputs consistent.
- ✅ Extend pricing engines (starting with MPFS) to surface dataset identifiers/digests, leveraging revision/effective date data or snapshot lookups.
- ✅ Aggregate those dataset descriptors into `PricingResponse.datasets_used` to support comparison parity checks.
- ✅ Update `TraceService.store_run` to persist dataset usage and facility flags alongside run data for replay/audit trails.
- ✅ Backstop the change with unit/integration coverage that executes a stored plan and asserts dataset metadata and trace persistence.

**Completion Notes:**
- ✅ All 7 pricing engines return `CodePricingItem` with provenance fields (`dataset_id`, `release_id`, `batch_id`, `trace_refs`)
- ✅ `PricingService._collect_datasets_used()` aggregates provenance from line items and snapshot table
- ✅ Pricing responses include `datasets_used` with complete provenance metadata
- ✅ Comprehensive test coverage validates provenance flow end-to-end
- **Phase 2:** Provenance Tracking (Complete)
- **Quick Win #2:** Unified CodePricingItem Schema (Complete)
---

### Task 62: Data Ingestion: Replace placeholder validation/adaptation logic

**Category:** Data Ingestion
**Status:** 🗑️ OUTDATED
**Priority:** High
**Estimated Time:** TBD
**Labels:** data-ingestion, high-priority

**Description:**
**Files:** `cms_pricing/ingestion/ingestors/mpfs_ingestor.py:152`, `cms_pricing/ingestion/ingestors/rvu_ingestor.py:1`

**Details:** Implement real DIS validation rules, adapters, and enrichers in MPFS/RVU ingestors to enforce schema quality and unblock failing ingestion tests.

---

### Task 63: General: Execute automation and cache-warming backlog

**Status:** ✅ COMPLETE
**Category:** General
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** general, medium-priority

**Description:**
**Files:** `NEXT_TODOS.md:11`, `cms_pricing/main.py:95`, `.github/workflows/cms_rvu_discovery.yml`

**Details:** Build the CMS release detection, downloader automation, and cache-warming routines so dataset freshness and observability match roadmap expectations.

---

### Task 69: Data Quality: Add locality_id to fee_mpfs table
**Status:** ✅ COMPLETE

**Category:** Database
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** database, data-quality, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.1

**Details:** Add `locality_id` column to `fee_mpfs` table and backfill from RVU locality dimension table. This enables proper locality joins and improves MPFS engine queries.

**Implementation Steps:**
- Create Alembic migration to add `locality_id` column to `fee_mpfs`
- Backfill `locality_id` from RVU locality dimension using locality code mapping
- Add index on `locality_id` for efficient joins
- Update MPFS engine to use `locality_id` instead of locality code lookups
- Update tests to verify locality_id joins work correctly

**Acceptance Criteria:**
- `fee_mpfs.locality_id` column exists with proper foreign key relationship
- All existing rows backfilled with correct locality_id values
- MPFS engine queries use locality_id for efficient joins
- Tests verify locality resolution works correctly

---

**Status:** ✅ COMPLETE
### Task 70: Data Quality: Extend OPPS ingestion to persist wage index table

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 hours
**Labels:** data-ingestion, opps, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.1

**Details:** Extend OPPS ingestion to persist wage index table with natural key constraints and add facility-specific joins in OPPS engine.

**Implementation Steps:**
- Update OPPS ingestor to persist wage index data to database
- Add natural key constraints on wage index table (CBSA, year, quarter, effective date)
- Update OPPS engine to use wage index joins for facility-specific pricing
- Add tests for wage index ingestion and engine integration
- Verify wage index data flows through to pricing responses

**Acceptance Criteria:**
- Wage index table populated from OPPS ingestion
- Natural key constraints enforced (no duplicates)
- OPPS engine uses wage index for facility calculations
- Tests verify wage index integration end-to-end

---
**Status:** ✅ COMPLETE

### Task 71: Data Quality: Produce retention/backfill playbook

**Category:** Documentation
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** documentation, operations, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.1

**Details:** Create operational playbook covering data retention policies, backfill procedures, digest reconciliation, and abort criteria for dataset snapshots.

**Implementation Steps:**
- Document retention policies for each dataset type
- Define backfill procedures for reprocessing historical data
- Document digest reconciliation process (how to verify data integrity)
- Define abort criteria (when to stop/rollback backfill operations)
- Include examples and troubleshooting guides
- Review with operations team

**Acceptance Criteria:**
- Playbook covers retention policies for all datasets
- Backfill procedures documented with step-by-step instructions
- Digest reconciliation process clearly explained
- Abort criteria defined for various failure scenarios
- Examples and troubleshooting guides included

**Status:** ✅ COMPLETE
---

### Task 72: Testing: Add synthetic publish integration test

**Category:** Testing
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** testing, integration, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.1

**Details:** Create integration test that publishes fixture manifests, runs `/pricing/price` calls, and verifies both cents and provenance metadata are correctly returned.

**Implementation Steps:**
- Create fixture dataset manifests with provenance metadata
- Write integration test that publishes fixtures
- Call `/pricing/price` endpoint with test data
- Verify pricing amounts (cents) are correct
- Verify provenance metadata (`datasets_used`, `release_id`, `batch_id`) present
- Add to CI pipeline

**Acceptance Criteria:**
- Test publishes fixture manifests successfully
- `/pricing/price` calls return correct pricing amounts
- Provenance metadata verified in response
- Test runs in CI pipeline
**Status:** ✅ COMPLETE

---

### Task 73: Testing: Add wage index coverage test

**Category:** Testing
**Priority:** Medium
**Estimated Time:** 1-2 hours
**Labels:** testing, opps, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.1

**Details:** Create test verifying wage index works correctly for both indexed (facility) and non-indexed (non-facility) pricing states.

**Implementation Steps:**
- Create test fixtures with facility and non-facility settings
- Test OPPS pricing with wage index applied (facility)
- Test OPPS pricing without wage index (non-facility)
- Verify correct wage index values retrieved from database
- Verify pricing calculations differ appropriately

**Acceptance Criteria:**
- Test covers facility (wage index applied) scenarios
- Test covers non-facility (no wage index) scenarios
- Verifies correct wage index values used
**Status:** ✅ COMPLETE
- Verifies pricing calculations accurate

---

### Task 74: Data Quality: Instrument checksum verification and failure alerts

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** data-ingestion, observability, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.1

**Details:** Add checksum verification during ingestion jobs and configure failure alerts when checksums don't match expected values.

**Implementation Steps:**
- Calculate checksums during ingestion (SHA-256 of data)
- Compare calculated checksums to expected values from manifests
- Emit alerts/metrics when checksum mismatches detected
- Log checksum verification results
- Configure alerting rules for checksum failures

**Acceptance Criteria:**
- Checksums calculated for all ingested datasets
- Verification compares against expected values
**Status:** ✅ COMPLETE
- Alerts triggered on checksum mismatches
- Logs include checksum verification results

---

### Task 75: Testing: Expand negative test coverage

**Category:** Testing
**Priority:** Medium
**Estimated Time:** 3-4 hours
**Labels:** testing, api-development, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.2

**Details:** Expand negative test coverage for invalid locality codes, unsupported modifiers, and missing snapshot scenarios.

**Implementation Steps:**
- Add tests for invalid locality codes (should return 400 or appropriate error)
- Add tests for unsupported modifiers (verify error handling)
- Add tests for missing snapshot scenarios (verify fallback behavior)
- Verify error messages are actionable and include error codes
- Document expected error responses

**Acceptance Criteria:**
- Tests cover invalid locality scenarios
**Status:** ✅ COMPLETE
- Tests cover unsupported modifier scenarios
- Tests cover missing snapshot scenarios
- Error messages are actionable with error codes

---

### Task 76: Testing: Stand up contract tests for ClearBill staging

**Category:** Testing
**Priority:** High
**Estimated Time:** 4-6 hours
**Labels:** testing, api-development, high-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.2

**Details:** Stand up contract tests executed against ClearBill staging environment to verify API compatibility before production release.

**Implementation Steps:**
- Set up test environment connecting to ClearBill staging
- Create contract test suite validating API responses
- Test all pricing endpoints (`/pricing/price`, `/pricing/compare`, `/pricing/codes/price`)
- Verify schema compatibility (`CodePricingItem` structure)
- Verify provenance fields present and correctly formatted
- Run contract tests in CI/CD pipeline
- Coordinate with ClearBill team for test data access

**Acceptance Criteria:**
- Contract tests execute against ClearBill staging
**Status:** ✅ COMPLETE
- All pricing endpoints validated
- Schema compatibility verified
- Provenance fields validated
- Tests run in CI/CD pipeline

---

### Task 77: Testing: Establish downstream regression suite

**Category:** Testing
**Priority:** Medium
**Estimated Time:** 3-4 hours
**Labels:** testing, api-development, medium-priority, readiness-plan

**Description:**
**Status:** ✅ COMPLETE
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.2

**Details:** Establish downstream regression suite (consumer-driven contract or Postman collection) executed before each release to catch breaking changes.

**Implementation Steps:**
- Create Postman collection or consumer-driven contract tests
- Include critical API scenarios used by downstream consumers
- Validate response schemas match expected structure
- Test backward compatibility scenarios
- Integrate into pre-release validation workflow
- Document how to update collection when API changes

**Acceptance Criteria:**
**Status:** 🗑️ OUTDATED
- Regression suite covers critical API scenarios
- Schema validation included
- Backward compatibility tested
- Integrated into release workflow
- Documentation for maintenance included

---

### Task 78: Documentation: Produce versioned API change log

**Category:** Documentation
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** documentation, api-development, medium-priority, readiness-plan

**Status:** 🗑️ OUTDATED
**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.2

**Details:** Produce versioned API change log documenting schema changes, new endpoints, deprecations, and breaking changes with schema diffs.

**Implementation Steps:**
- Create API change log document or section in existing docs
- Document schema changes (e.g., `CodePricingItem` introduction)
- Include schema diffs showing before/after
- Document new endpoints and their purposes
- Mark deprecated endpoints and removal timelines
- Version the change log (e.g., v1.0, v1.1)
- Keep change log updated with each release

**Status:** ✅ COMPLETE
**Acceptance Criteria:**
- Change log documents all schema changes
- Schema diffs included for major changes
- New endpoints documented
- Deprecations clearly marked
- Change log is versioned and maintained

---

### Task 79: Documentation: Coordinate client enablement guide

**Category:** Documentation
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** documentation, api-development, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.2

**Details:** Coordinate with product marketing and developer relations to produce client enablement guide for API consumers.

**Implementation Steps:**
- Create client enablement guide document
- Include API overview and use cases
- Document authentication and authorization
- Provide code examples for common scenarios
- Include troubleshooting and FAQ sections
- Coordinate review with product marketing
- Coordinate review with developer relations
- Publish guide in accessible location

**Status:** ✅ COMPLETE
**Acceptance Criteria:**
- Enablement guide covers API overview
- Authentication documented
- Code examples provided
- Troubleshooting section included
- Reviewed by product marketing and dev relations
- Published and accessible

---

### Task 80: API Development: Implement L1 in-process cache and L2 Redis cache

**Category:** Performance
**Priority:** High
**Estimated Time:** 4-6 hours
**Labels:** performance, caching, high-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Implement L1 in-process LRU cache and L2 Redis cache keyed by `(engine|code|setting|locality|snapshot_digest)` for improved pricing performance.

**Implementation Steps:**
- Implement L1 in-process LRU cache in pricing service
- Configure Redis connection for L2 cache
- Implement cache key generation for `(engine|code|setting|locality|snapshot_digest)`
- Add cache hit/miss metrics
- Implement cache invalidation strategy
- Add cache warming for top cohorts
- Test cache behavior and performance improvements
**Status:** ✅ COMPLETE

**Acceptance Criteria:**
- L1 cache implemented with LRU eviction
- L2 Redis cache configured and working
- Cache keys generated correctly
- Cache hit/miss metrics exposed
- Cache invalidation works correctly
- Performance improvements measured

---

### Task 81: Performance: Develop nightly cache warmers

**Category:** Performance
**Priority:** Medium
**Estimated Time:** 3-4 hours
**Labels:** performance, caching, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Develop nightly cache warmers seeded from top ClearBill demand cohorts to pre-populate cache with frequently accessed data.

**Implementation Steps:**
- Identify top ClearBill demand cohorts (codes, settings, localities)
- Create cache warmer script/service
- Schedule nightly execution (cron or scheduled job)
- Pre-populate cache with top cohorts
**Status:** ✅ COMPLETE
- Monitor cache hit rates after warming
- Adjust cohorts based on usage patterns

**Acceptance Criteria:**
- Cache warmer identifies top demand cohorts
- Nightly execution scheduled and working
- Cache pre-populated successfully
- Cache hit rates improved
- Cohorts adjusted based on data

---

### Task 82: Performance: Run load tests for SLO validation

**Category:** Performance
**Priority:** High
**Estimated Time:** 4-6 hours
**Labels:** performance, testing, high-priority, readiness-plan
**Status:** ✅ COMPLETE

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Run load tests (single lookup, 20-code batch, 40-code batch) with targets p95 < 500 ms / < 2.5 s to validate performance SLOs.

**Implementation Steps:**
- Set up load testing framework (Locust, k6, or similar)
- Create test scenarios:
  - Single code lookup (target: p95 < 500ms)
  - 20-code batch (target: p95 < 2.5s)
  - 40-code batch (target: p95 < 2.5s)
- Run load tests against staging environment
**Status:** ⚠️ PARTIAL
- Measure and report p50, p95, p99 latencies
- Identify bottlenecks if SLOs not met
- Document results and recommendations

**Acceptance Criteria:**
- Load tests configured for all scenarios
- SLOs met: single lookup p95 < 500ms
- SLOs met: batch requests p95 < 2.5s
- Results documented with recommendations
- Bottlenecks identified and addressed

---

### Task 83: Observability: Expose pricing and cache metrics

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** monitoring, observability, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Expose Prometheus metrics: `dataset_snapshot_selected_total`, `pricing_lookup_latency_ms`, `cache_hits_total`/`cache_misses_total`, `requests_total{route, scope}`.

**Implementation Steps:**
- Add `dataset_snapshot_selected_total` counter metric
**Status:** ✅ COMPLETE
- Add `pricing_lookup_latency_ms` histogram metric
- Add `cache_hits_total` and `cache_misses_total` counter metrics
- Add `requests_total{route, scope}` counter with labels
- Expose metrics via `/metrics` endpoint
- Document metric names and labels

**Acceptance Criteria:**
- All metrics exposed via `/metrics` endpoint
- Metrics include appropriate labels (route, scope)
- Metrics documented
- Prometheus can scrape metrics successfully

---

### Task 84: Observability: Publish Grafana dashboards

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** 3-4 hours
**Status:** ✅ COMPLETE
**Labels:** monitoring, observability, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Publish Grafana dashboards showing latency, cache hit ratio, error budgets, and snapshot adoption metrics.

**Implementation Steps:**
- Set up Grafana instance (or use existing)
- Create dashboard for latency metrics (p50, p95, p99)
**Status:** ⚠️ PARTIAL
- Create dashboard for cache hit ratio
- Create dashboard for error budgets (4xx, 5xx rates)
- Create dashboard for snapshot adoption (which snapshots active)
- Configure alert thresholds
- Document dashboard access and interpretation

**Acceptance Criteria:**
- Grafana dashboards created for all metrics
- Dashboards show latency, cache, errors, snapshots
- Alert thresholds configured
- Dashboards documented
- Team has access to dashboards

---

### Task 85: Observability: Configure alerts for SLO burn and anomalies

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** monitoring, observability, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Configure alerts for SLO burn, snapshot mismatch, cache miss spikes, and ingestion drift.

**Implementation Steps:**
- Configure alert for SLO burn (latency exceeding targets)
**Status:** ✅ COMPLETE
- Configure alert for snapshot mismatch (wrong snapshot selected)
- Configure alert for cache miss spikes (unusual cache miss rate)
- Configure alert for ingestion drift (data freshness issues)
- Set alert severity levels (warning, critical)
- Configure alert channels (email, Slack, PagerDuty)
- Test alert triggering and resolution

**Acceptance Criteria:**
- Alerts configured for all specified scenarios
- Alert severity levels set appropriately
- Alert channels configured
- Alerts tested and working
- Alert runbook documented

---

### Task 86: Operations: Author operational runbooks

**Category:** Documentation
**Priority:** Medium
**Estimated Time:** 3-4 hours
**Labels:** documentation, operations, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Author runbooks for snapshot activation/rollback, cache warmer failures, and RBAC issue remediation.

**Implementation Steps:**
- Create runbook for snapshot activation/rollback:
  - How to activate new snapshot
  - How to rollback to previous snapshot
  - How to verify snapshot selection
- Create runbook for cache warmer failures:
  - How to identify cache warmer failures
  - How to restart cache warmer
**Status:** ✅ COMPLETE
  - How to verify cache population
- Create runbook for RBAC issue remediation:
  - How to identify RBAC failures
  - How to fix scope issues
  - How to rotate API keys
- Include troubleshooting steps and escalation paths
- Review with operations team

**Acceptance Criteria:**
- Runbooks created for all scenarios
- Step-by-step procedures documented
- Troubleshooting sections included
- Escalation paths documented
- Reviewed by operations team

---

### Task 87: Operations: Deliver on-call training

**Category:** Operations
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** operations, training, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Deliver on-call training session with recordings and scenario drills, and update paging escalation list.

**Implementation Steps:**
- Schedule on-call training session
- Create training materials covering:
  - Common incidents and resolution
**Status:** ✅ COMPLETE
  - Alert interpretation
  - Runbook usage
  - Escalation procedures
- Conduct scenario drills (practice incidents)
- Record training session
- Update paging escalation list with current contacts
- Document training completion

**Acceptance Criteria:**
- Training session conducted
- Training materials created
- Scenario drills completed
- Session recorded
- Escalation list updated
- Training documented

---

### Task 88: Operations: Prepare deployment checklist

**Category:** Operations
**Priority:** Medium
**Estimated Time:** 1-2 hours
**Labels:** operations, deployment, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.4

**Details:** Prepare deployment checklist covering feature flag sequencing, dual-write window, and rollback triggers.

**Status:** ✅ COMPLETE
**Implementation Steps:**
- Create deployment checklist template
- Document feature flag sequencing (order of activation)
- Define dual-write window (old/new schema simultaneously)
- Define rollback triggers (conditions for rollback)
- Include pre-deployment checks
- Include post-deployment verification steps
- Review with engineering and operations teams

**Acceptance Criteria:**
- Deployment checklist created
- Feature flag sequencing documented
- Dual-write window defined
- Rollback triggers clearly specified
- Pre and post-deployment checks included
- Reviewed by teams

---

### Task 89: Security: Create api_keys table (Quick Win #3)

**Category:** Security
**Priority:** High
**Estimated Time:** 3-4 hours
**Labels:** security, database, high-priority, readiness-plan, quick-win-3

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3, `artifacts/quick_wins_detailed_plans.md`

**Details:** Create `api_keys` table with salted hash storage, scopes, tenant, and attribution fields. This is Quick Win #3 and enables RBAC functionality.

**Implementation Steps:**
- Create Alembic migration for `api_keys` table with columns:
  - `key_id` (UUID primary key)
**Status:** ✅ COMPLETE
  - `key_hash` (salted SHA-256 hash, not plaintext)
  - `scopes` (array of strings: `app`, `advocate`, `developer`)
  - `tenant_id` (optional, for multi-tenant support)
  - `attribution_fields` (JSONB for metadata like `created_by`, `purpose`)
  - `created_at`, `updated_at`, `expires_at` (optional)
  - `is_active` (boolean flag)
- Create `APIKey` SQLAlchemy model
- Create key rotation job (scheduled task or CLI script)
- Add indexes on `key_hash`, `tenant_id`, `is_active`
- Create helper functions for key generation, hashing, validation

**Acceptance Criteria:**
- `api_keys` table created with proper schema
- Key hashing uses salted SHA-256 (never store plaintext)
- Scopes array supports RBAC requirements
- Key rotation job can deactivate/rotate keys
- Tests verify key storage and retrieval work correctly

---

### Task 90: Security: Implement RBAC middleware enforcing scopes

**Category:** Security
**Priority:** High
**Estimated Time:** 4-6 hours
**Labels:** security, api-development, high-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3

**Details:** Implement middleware enforcing scope-based access control per router. Replace current env-based API key check with database-backed key lookup and scope validation.

**Implementation Steps:**
- Update `cms_pricing/auth.py` to query `api_keys` table instead of env vars
- Create scope decorator/dependency (e.g., `require_scope("app")` or `require_scope(["app", "developer"])`)
**Status:** ✅ COMPLETE
- Update middleware to:
  - Extract API key from request headers
  - Look up key in database (hash comparison)
  - Validate key is active and not expired
  - Check requested scope is in key's allowed scopes
  - Reject with 403 if scope check fails
- Apply scope requirements to all routers:
  - `/pricing/*` endpoints may require `app` scope
  - `/opps/*`, `/mpfs/*` may require `developer` scope
  - Admin endpoints may require special scope
- Add tests for scope enforcement (valid keys, invalid scopes, expired keys)

**Acceptance Criteria:**
- Middleware enforces scope checks on all protected routes
- Invalid or missing scopes return 403 Forbidden
- Scope checks work with database-backed keys
- Tests verify scope enforcement for all scenarios
- No breaking changes to existing API key behavior (backward compatible transition)

---

### Task 91: Observability: Emit per-key metrics for RBAC

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** monitoring, observability, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3
**Status:** ✅ COMPLETE

**Details:** Emit Prometheus metrics per API key scope: `requests_total{scope, key_id}`, `requests_4xx_total{scope}`, `requests_rate_limited_total{scope}`.

**Implementation Steps:**
- Add metrics collection in authentication middleware
- Create counter metrics:
  - `api_requests_total{scope, key_id, route}` (with key_id for attribution)
  - `api_requests_4xx_total{scope, route}` (aggregated, no key_id for privacy)
  - `api_requests_rate_limited_total{scope, route}`
- Emit metrics after successful/failed auth checks
- Ensure key_id is hashed or truncated for privacy (don't expose full UUIDs)
- Document metric names and labels

**Acceptance Criteria:**
- Per-key metrics exposed via `/metrics` endpoint
- Metrics include scope and route labels
- Key identifiers are privacy-safe (hashed/truncated)
- Metrics documented
- Prometheus can scrape metrics successfully

---

### Task 92: Observability: Route correlation IDs through services

**Category:** Observability
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** observability, tracing, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3

**Status:** ✅ COMPLETE
**Details:** Route correlation IDs through ingestion and pricing services to enable end-to-end request tracing. Ensure no PHI persists outside encrypted stores.

**Implementation Steps:**
- Update FastAPI middleware to generate/accept correlation IDs from headers (e.g., `X-Correlation-ID`)
- Pass correlation ID through:
  - Pricing service calls
  - Engine invocations
  - Ingestion pipeline stages
  - Trace service records
- Add correlation ID to structured logs (structlog)
- Verify PHI is never logged (audit log filtering)
- Ensure correlation IDs are included in trace records
- Document correlation ID format and usage

**Acceptance Criteria:**
- Correlation IDs generated/forwarded for all requests
- IDs propagate through all service layers
- IDs included in logs and trace records
- PHI filtering verified (no sensitive data in logs)
- Documentation covers correlation ID usage

---

### Task 93: Compliance: Record compliance bill-of-materials

**Category:** Compliance
**Priority:** Medium
**Estimated Time:** 4-6 hours
**Labels:** compliance, documentation, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3

**Details:** Record compliance bill-of-materials (BOM) documenting systems, datasets, controls aligning to HIPAA §164.308/§164.312 requirements.

**Implementation Steps:**
- Create compliance BOM document template
- Document systems:
  - API servers, databases, caching layer
**Status:** ✅ COMPLETE
  - Ingestion pipelines, data storage
  - Monitoring and logging systems
- Document datasets:
  - CMS datasets (MPFS, OPPS, ASC, etc.)
  - Data sources and update frequencies
  - Data retention policies
- Document controls:
  - Access controls (RBAC, API keys)
  - Encryption at rest and in transit
  - Audit logging and monitoring
  - Incident response procedures
- Map controls to HIPAA requirements (§164.308, §164.312)
- Create compliance attestation package
- Review with compliance team

**Acceptance Criteria:**
- BOM document created covering all systems, datasets, controls
- Controls mapped to HIPAA requirements
- Attestation package prepared
- Reviewed and approved by compliance team
- Document versioned and maintained

---

### Task 94: Compliance: Define data retention & deletion policy

**Category:** Compliance
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** compliance, documentation, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3

**Details:** Define data retention and deletion policy for pricing snapshots, audit logs, and trace records to ensure compliance with data protection regulations.
**Status:** ✅ COMPLETE

**Implementation Steps:**
- Define retention periods for:
  - Dataset snapshots (e.g., keep last 12 releases)
  - Audit logs (e.g., 7 years for compliance)
  - Trace records (e.g., 90 days for debugging)
  - API request logs (e.g., 30 days)
- Document deletion procedures:
  - Automated deletion workflows
  - Manual deletion processes
  - Data archiving before deletion
- Create data retention schedule (when/what to delete)
- Document exceptions and hold procedures (legal holds)
- Review with legal/compliance team

**Acceptance Criteria:**
- Retention policy document created
- Retention periods defined for all data types
- Deletion procedures documented
- Schedule created for automated cleanup
- Exceptions and holds documented
- Reviewed by legal/compliance team

---

### Task 95: Security: Configure alerting on auth failures

**Category:** Security
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** security, monitoring, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3
**Status:** ✅ COMPLETE

**Details:** Configure alerts for repeated authentication failures and scope escalation attempts to detect potential security threats.

**Implementation Steps:**
- Add metrics tracking auth failures per key/IP:
  - `api_auth_failures_total{key_id, reason}`
  - `api_scope_denied_total{key_id, requested_scope}`
- Configure alert rules:
  - Alert on >5 auth failures in 5 minutes from same key
  - Alert on >10 auth failures in 5 minutes from same IP
  - Alert on repeated scope escalation attempts
  - Alert on key rotation anomalies
- Set alert severity (warning for first threshold, critical for sustained attacks)
- Configure alert channels (PagerDuty, Slack, email)
- Create runbook for responding to auth failure alerts
- Test alert triggering

**Acceptance Criteria:**
- Alerts configured for auth failure scenarios
- Alert thresholds set appropriately
- Alert channels configured
- Runbook created for response procedures
- Alerts tested and working

---

### Task 96: Compliance: Capture audit log sampling review

**Category:** Compliance
**Priority:** Medium
**Estimated Time:** 2-3 hours
**Labels:** compliance, auditing, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3
**Status:** ✅ COMPLETE

**Details:** Capture audit log sampling review process and provide compliance attestation package for HIPAA audit requirements.

**Implementation Steps:**
- Define audit log sampling methodology:
  - Frequency (e.g., quarterly reviews)
  - Sample size and selection criteria
  - Review checklist
- Create audit log review checklist:
  - Verify correlation IDs present
  - Verify no PHI in logs
  - Verify scope enforcement logged
  - Verify data access patterns normal
- Document sampling review process
- Create attestation package template
- Schedule initial audit review
- Document findings and remediation if needed

**Acceptance Criteria:**
- Audit log sampling methodology defined
- Review checklist created
- Attestation package template ready
- Initial review completed
- Findings documented

---

### Task 97: Compliance: Partner for pre-launch tabletop exercise

**Category:** Compliance
**Priority:** Medium
**Status:** ✅ COMPLETE
**Estimated Time:** 3-4 hours
**Labels:** compliance, operations, medium-priority, readiness-plan

**Description:**
**Source:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` §5.3

**Details:** Partner with Compliance team for pre-launch tabletop exercise covering incident response scenarios to validate readiness and procedures.

**Implementation Steps:**
- Schedule tabletop exercise with compliance team
- Prepare incident scenarios:
  - Data breach simulation
  - API key compromise
  - Unauthorized access attempt
  - Data retention violation
  - Audit log anomaly
- Document expected response procedures
- Run tabletop exercise:
  - Present scenarios to team
  - Walk through response procedures
  - Identify gaps or improvements
- Document findings and action items
- Update procedures based on findings

**Acceptance Criteria:**
- Tabletop exercise conducted
- Scenarios cover key incident types
- Response procedures validated
- Findings documented
- Action items tracked and resolved

---

---

### Task 64: Testing: Stand up Postgres-backed API test harness

**Category:** Testing
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** testing, database, medium-priority
**Status:** ✅ Completed (2025-10-03)

**Description:**
Stand up a Postgres-backed test harness so API suites that rely on JSONB/ARRAY columns run without SQLite dialect failures.

**Implementation Steps:**
- Provision a dedicated Postgres test database (e.g., docker compose service or pytest-postgresql fixture) and expose its DSN via environment variables for pytest runs.
- Build a bootstrap script (e.g., `tests/scripts/bootstrap_test_db.py`) that runs Alembic migrations and seeds the minimal MPFS/OPPS/geography data required by plan/pricing tests.
- Update `tests/conftest.py` to override `DATABASE_URL`, invoke the bootstrap script once per session, and hand out transactional SQLAlchemy sessions that roll back between tests.
- Adjust the FastAPI `client` fixture to rely on the Postgres-backed dependency instead of SQLite overrides while still auto-attaching API-key headers.
- Document the workflow (start DB, bootstrap, run pytest) in the README so contributors can execute the suite locally.

---


## GitHub CLI Commands for Adding Tasks

### Add Tasks to GitHub Project

```bash
# Set your project number
PROJECT_NUMBER=<your_project_number>

# Add each task (replace with actual task details)

# Task 1: CMS Website Scraper
gh project item-create $PROJECT_NUMBER --title "CMS Website Scraper" --body "Section: Phase 1: Dynamic Data Acquisition  Details: CMS Website Scraper  Source: NEXT_TODOS.md" --field "Category=Data Ingestion" --field "Priority=Critical" --field "Estimated Time=TBD"

# Task 2: File Download Manager
gh project item-create $PROJECT_NUMBER --title "File Download Manager" --body "Section: Phase 1: Dynamic Data Acquisition  Details: File Download Manager  Source: NEXT_TODOS.md" --field "Category=Data Ingestion" --field "Priority=Critical" --field "Estimated Time=TBD"

# Task 3: Release Detection System
gh project item-create $PROJECT_NUMBER --title "Release Detection System" --body "Section: Phase 1: Dynamic Data Acquisition  Details: Release Detection System  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=Critical" --field "Estimated Time=TBD"

# Task 4: CMS API Integration
gh project item-create $PROJECT_NUMBER --title "CMS API Integration" --body "Section: Phase 1: Dynamic Data Acquisition  Details: CMS API Integration  Source: NEXT_TODOS.md" --field "Category=API Development" --field "Priority=Critical" --field "Estimated Time=TBD"

# Task 5: Third-Party Data Sources
gh project item-create $PROJECT_NUMBER --title "Third-Party Data Sources" --body "Section: Phase 1: Dynamic Data Acquisition  Details: Third-Party Data Sources  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=Critical" --field "Estimated Time=TBD"

# Task 6: Streaming Data Pipeline
gh project item-create $PROJECT_NUMBER --title "Streaming Data Pipeline" --body "Section: Phase 2: System Enhancement  Details: Streaming Data Pipeline  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=High" --field "Estimated Time=TBD"

# Task 7: Incremental Updates
gh project item-create $PROJECT_NUMBER --title "Incremental Updates" --body "Section: Phase 2: System Enhancement  Details: Incremental Updates  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=High" --field "Estimated Time=TBD"

# Task 8: Data Analytics Dashboard
gh project item-create $PROJECT_NUMBER --title "Data Analytics Dashboard" --body "Section: Phase 2: System Enhancement  Details: Data Analytics Dashboard  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=High" --field "Estimated Time=TBD"

# Task 9: Predictive Analytics
gh project item-create $PROJECT_NUMBER --title "Predictive Analytics" --body "Section: Phase 2: System Enhancement  Details: Predictive Analytics  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=High" --field "Estimated Time=TBD"

# Task 10: Advanced Query Capabilities
gh project item-create $PROJECT_NUMBER --title "Advanced Query Capabilities" --body "Section: Phase 2: System Enhancement  Details: Advanced Query Capabilities  Source: NEXT_TODOS.md" --field "Category=General" --field "Priority=High" --field "Estimated Time=TBD"

```

### Alternative: Manual Setup

If GitHub CLI is not available, you can:

1. Go to https://github.com/alex-bea/cms_api
2. Click on "Projects" tab
3. Create a new project
4. Add the tasks manually using the web interface

## Next Steps

1. **Set up GitHub Project** using the commands above
2. **Add all tasks** to the project
3. **Assign team members** to tasks
4. **Set up automation** for task updates
5. **Create milestones** for project phases
6. **Set up notifications** for task updates

## Automation Setup

### GitHub Actions Workflow

Create `.github/workflows/task-sync.yml`:

```yaml
name: Task Synchronization
on:
  schedule:
    - cron: '0 9 * * 1'  # Weekly on Monday at 9 AM
  workflow_dispatch:

jobs:
  sync-todos:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Scan for new TODOs
        run: |
          python tools/github_tasks_setup.py --scan-todos
      - name: Update GitHub Tasks
        # Add logic to create/update GitHub tasks
        run: echo "TODO: Implement GitHub API integration"
```

## Recent Accomplishments (2025-10-04)

### ✅ Completed Tasks

#### Database Test Infrastructure
- **PostgreSQL Test Harness**: Fixed `scripts/test_with_postgres.sh` with proper Docker Compose commands, environment variables, and timeout handling
- **Database Test Patterns**: Created `DOC-test-patterns-prd-v1.0.md` with comprehensive guidance for database testing isolation
- **Test Configuration**: Updated `tests/conftest.py` to use PostgreSQL instead of SQLite for JSONB/ARRAY support
- **Migration Chain**: Resolved Alembic migration chain issues and removed duplicate column definitions

#### CI/CD Pipeline Improvements
- **Daily Scheduling**: Added daily cron schedules to all critical test workflows:
  - `qts-compliance.yml`: Daily at 2:00 UTC
  - `contract-tests.yml`: Daily at 3:00 UTC  
  - `openapi-diff.yml`: Daily at 4:00 UTC
  - `api-contract-lint.yml`: Daily at 5:00 UTC
- **Pipeline Fixes**: Resolved event loop issues, missing dependencies (`psutil`), and Docker registry configuration
- **Test Infrastructure**: Enhanced test patterns with proper async handling and fixture management

#### Documentation & Audit Tools
- **Audit Tool Enhancement**: Enhanced `tools/audit_doc_links.py` and `tools/audit_cross_references.py` with:
  - Forward/backward reference validation
  - Integration point verification
  - Cross-reference symmetry checks
- **Documentation Compliance**: Updated all PRDs to reference `DOC-master-catalog-prd-v1.0.md` per governance standards
- **Cross-Reference Validation**: Added comprehensive validation for document relationships and integration points

#### Repository Labels Added
- `source-map-drift`: Automated verification found doc/manifest mismatch
- `manifest-schema-change`: Updates to discovery manifest format/schema  
- `database-test-patterns`: Database testing patterns and isolation improvements
- `ci-cd-monitoring`: CI/CD pipeline monitoring and fixes
- `audit-tool-enhancement`: Documentation audit tool improvements

### 🔄 Current Focus
- **Core Development**: Implementing missing ingestors (ASC, IPPS, CLFS, DMEPOS, ASP, NADAC)
- **API Enhancement**: Ensuring full compliance with API Architecture PRD
- **Observability**: Implementing comprehensive monitoring features

### 📋 Next Steps
1. Implement ASC (Ambulatory Surgical Center) ingestor with quarterly data processing
2. Implement IPPS (Inpatient Prospective Payment System) ingestor with annual data processing
3. Enhance API endpoints and ensure full compliance with API Architecture PRD
4. Implement comprehensive observability and monitoring features
5. Complete end-to-end testing and performance validation

### 🏗️ Architecture Documentation ✅ COMPLETED

#### ✅ Task 1: STD-data-architecture-impl-v1.0.md (Companion Implementation Guide)
**Status:** ✅ Complete (2025-10-15)  
**Category:** Documentation  
**Actual Time:** 3 hours  

**Completed:**
- Created companion implementation guide (1,200+ lines) for `STD-data-architecture-prd-v1.0.md`
- Documented `BaseDISIngestor` interface with line number references
- Covered all centralized components (factories, validators, observability)
- Included schema contracts, validation patterns, operational guidance
- Added 10-step ingestor creation tutorial
- Provided working examples from MPFS, RVU, OPPS
- Included code templates and compliance checklist
- Established companion document naming convention (`-impl` suffix)

**Governance:**
- Updated `STD-doc-governance-prd-v1.0.md` to v1.0.1 with companion conventions
- Updated `DOC-master-catalog-prd-v1.0.md` to v1.0.2
- Enhanced `tools/audit_doc_catalog.py` with companion validation
- All audit tools pass

**Commit:** `f17a88d`

#### ✅ Task 2: REF-scraper-ingestor-integration-v1.0.md
**Priority:** High  
**Category:** Documentation  
**Estimated Time:** 3-4 hours  
**Dependencies:** Task 1 (for cross-references)  

**Description:**
Create reference architecture document detailing integration patterns between scrapers and ingestors, including implementation templates and best practices.

**Implementation Strategy:**
1. **Integration Patterns**: Document how scrapers hand off data to ingestors via discovery manifests
2. **Code Templates**: Provide implementation templates and code examples
3. **Testing Strategies**: Document testing patterns for scraper–ingestor integration
4. **Error Handling**: Detail error handling and recovery mechanisms
5. **Best Practices**: Compile lessons learned and implementation best practices

**Content Outline:**
- Integration patterns and templates
- Discovery manifest handoff mechanisms
- Error handling and recovery patterns
- Data validation and quality gates
- Testing strategies and patterns
- Code templates and examples
- Implementation best practices

**Governance Compliance:**
- Follow `STD-doc-governance-prd-v1.0.md` naming conventions
- Include proper metadata header (Status, Owners, Consumers, Change control)
- Add cross-references to related PRDs
- Ensure compliance with audit requirements

**Acceptance Criteria:**
-  Document follows governance standards
-  Includes comprehensive integration patterns
-  Contains code templates and examples
-  Documents testing strategies and best practices
-  Cross-references related PRDs appropriately
-  Passes audit tool validation

#### Task 3: Update DOC-master-catalog-prd-v1.0.md
**Priority:** High  
**Category:** Documentation  
**Estimated Time:** 2-3 hours  
**Dependencies:** Tasks 1 & 2  

**Description:**
Update the master catalog to include new architecture PRDs with proper cross-references and dependency mapping.

**Implementation Strategy:**
1. **Add New Entries**: Add both new PRDs to appropriate sections in master catalog
2. **Update Dependency Graph**: Update Mermaid diagram to include new PRDs and relationships
3. **Cross-References**: Ensure proper cross-referencing between related documents
4. **Version Control**: Update version and change log appropriately

**Content Updates:**
- Add `STD-ingestion-pipeline-prd-v1.0.md` to Standards section
- Add `REF-scraper-ingestor-integration-prd-v1.0.md` to Reference Architectures section
- Update dependency graph with new relationships
- Add cross-references to existing PRDs
- Update change log with new entries

**Acceptance Criteria:**
-  New PRDs added to appropriate sections
-  Dependency graph updated with new relationships
-  Cross-references added to related documents
-  Change log updated appropriately
-  Passes audit tool validation

#### Task 4: Add Cross-References to Related PRDs
**Priority:** Medium  
**Category:** Documentation  
**Estimated Time:** 2-3 hours  
**Dependencies:** Tasks 1, 2 & 3  

**Description:**
Add cross-references to new architecture PRDs in related documents to ensure proper integration and discoverability.

**Implementation Strategy:**
1. **Update STD-data-architecture-prd-v1.0.md**: Add references to new pipeline standard
2. **Update STD-scraper-prd-v1.0.md**: Add references to integration patterns
3. **Update Existing Ingestor PRDs**: Add references to pipeline and integration standards
4. **Update Runbooks**: Add references to new architecture documentation

**Documents to Update:**
- `STD-data-architecture-prd-v1.0.md`
- `STD-scraper-prd-v1.0.md`
- `PRD-opps-prd-v1.0.md`
- `PRD-mpfs-prd-v1.0.md`
- `PRD-rvu-gpci-prd-v0.1.md`
- `RUN-global-operations-prd-v1.0.md`

**Acceptance Criteria:**
-  All related PRDs reference new architecture documents
-  Cross-references are bidirectional where appropriate
-  References follow governance standards
-  Passes audit tool validation

#### Task 5: Verify PRD Compliance
**Priority:** Medium  
**Category:** Documentation  
**Estimated Time:** 1-2 hours  
**Dependencies:** Tasks 1, 2, 3 & 4  

**Description:**
Run audit tools to verify governance compliance for new architecture PRDs and updated documentation.

**Implementation Strategy:**
1. **Run Audit Tools**: Execute `tools/audit_doc_catalog.py` and `tools/audit_doc_links.py`
2. **Fix Compliance Issues**: Address any governance violations or missing references
3. **Validate Cross-References**: Ensure all cross-references are valid and bidirectional
4. **Verify Master Catalog**: Confirm master catalog is properly updated

**Audit Tools to Run:**
- `tools/audit_doc_catalog.py` - Verify catalog consistency
- `tools/audit_doc_links.py` - Validate cross-references
- `tools/audit_cross_references.py` - Check reference symmetry
- `.github/workflows/doc-catalog-audit.yml` - CI/CD validation

**Acceptance Criteria:**
-  All audit tools pass without errors
-  No governance violations detected
-  All cross-references are valid and bidirectional
-  Master catalog is properly updated
-  CI/CD pipeline passes documentation audits

### 🔗 Recent Commits
- `a20a9d1`: feat: add daily scheduling to all test workflows
- `4fecbcc`: fix: async test mocking issues in OPPS scraper tests
- `ee021bc`: fix: CI/CD pipeline issues - scraper patterns, dependencies, Docker config
- `1d78f00`: feat: Enhance database test patterns and audit tools
- `c55079b`: docs: Add PostgreSQL test harness dependencies to PRDs
- `5a48201`: fix: Resolve Alembic migration chain and test script issues

### 🎯 Architecture Documentation Benefits

**Strategic Value:**
- **Clarity**: Comprehensive end-to-end pipeline documentation for all stakeholders
- **Consistency**: Standardized integration patterns across all scraper–ingestor implementations
- **Reusability**: Code templates and examples for rapid development of new ingestors
- **Compliance**: Full governance compliance with audit requirements
- **Maintainability**: Centralized architecture documentation for long-term maintenance

**Implementation Priority:**
1. **Phase 1**: Create core architecture PRDs (Tasks 1-2)
2. **Phase 2**: Update master catalog and cross-references (Tasks 3-4)
3. **Phase 3**: Verify compliance and run audit tools (Task 5)

**Total Estimated Time:** 12-18 hours across all tasks
**Critical Path:** Tasks 1 → 2 → 3 → 4 → 5 (sequential dependencies)

---

## ✅ COMPLETED: Architecture Documentation

**Status:** All 5 tasks completed on 2025-10-15

**Delivered:**
- ✅ `STD-data-architecture-impl-v1.0.md` - DIS implementation guide (1,200+ lines)
- ✅ `REF-scraper-ingestor-integration-v1.0.md` - Integration patterns (1,050+ lines)
- ✅ `STD-scraper-prd-v1.0.md` - Updated to v1.1 with pattern map (+453 lines)
- ✅ `STD-doc-governance-prd-v1.0.md` - Updated to v1.0.1 with companion docs
- ✅ `DOC-master-catalog-prd-v1.0.md` - Updated to v1.0.3 with new docs

**Audit Tooling Enhancements (2025-10-15):**
- ✅ Phase 1: Critical fixes (dynamic counts, JSON artifacts, strict mode, .PHONY)
- ✅ Phase 2: CI enhancements (caching, concurrency, job summary)
- ✅ New tool: `audit_makefile_phony.py` with auto-fix capability
- ✅ Total: 7/8 audits passing (1 legitimate dependency graph issue)

---

## 🚀 CURRENT PRIORITY: MPFS Ingestor Implementation

**Started:** 2025-10-15  
**Estimated Completion:** 11-16 hours (5 phases)  
**Status:** Phase 0 in progress

### Overview

Complete the MPFS (Medicare Physician Fee Schedule) ingestor to enable end-to-end physician pricing using the Discovery → Ingestion → Serving (DIS) architecture pattern.

### Business Value

- **Enable MPFS Pricing**: Complete implementation of physician fee schedule pricing
- **DIS Pattern Compliance**: Follow established architecture patterns
- **Production-Grade Quality**: Tiered validation, observability, reference data checks
- **Reusable Components**: Shared parsers for RVU and MPFS ingestors

### Implementation Phases

#### Phase 0: Pre-Implementation Foundation (3-4 hours) ✅ **71% COMPLETE**

**Status:** ✅ COMPLETE
**Completed:** 2025-10-16

**Tasks:**
1. ✅ Check database models (completed)
2. ✅ Extract RVU parsing logic to shared `parsers/` module (PPRRVU complete)
3. ✅ Create Schema Registry with SemVer contracts (10 schemas complete)
4. ✅ Build file-name → parser routing table (routing complete)
5. ✅ All parsers complete: PPRRVU, CF, Locality, OPPSCAP, GPCI, ANES (6 of 6 ✅)

**Deliverables:**
- `cms_pricing/ingestion/parsers/` module (infrastructure complete)
  - ✅ `pprrvu_parser.py` - PPRRVU parsing contract (400 lines, 7 tests)
  - ✅ `conversion_factor_parser.py` - CF parsing contract (755 lines, golden + 11 negatives)
  - ✅ `_parser_kit.py` - Shared utilities (17+ functions)
  - ✅ `layout_registry.py` - Fixed-width layouts (v2025.4.1)
  - ✅ `gpci_parser.py` - GPCI parsing contract (v1.3 COMPLETE - 109 localities, 20/20 tests, 0.03s, corrected NK + DB migration ready)
  - ✅ `locality_parser.py` - Locality parsing contract (Stage 1+2 COMPLETE)
  - ✅ `anes_parser.py` - ANES parsing contract (v1.0 COMPLETE - 109 localities, 19/19 tests, schema v1.1, 0.03s)
  - ✅ `oppscap_parser.py` - OPPS cap parsing contract (v1.0.0 COMPLETE - 16K rows, 0.45s, 100% parity)
- ✅ `cms_pricing/ingestion/contracts/` - Schema contracts (10 complete)
- ✅ `cms_pricing/mappers/` - Column transformation layer
- ✅ Parser routing table (`route_to_parser()` implemented)

**Acceptance Criteria:**
- ✅ Parser infrastructure complete (kit, registry, routing)
- ✅ Schema contracts registered for all 10 file types
- ✅ Parser routing table maps filenames → parsers
- ✅ 6 of 6 parsers complete (PPRRVU, CF, Locality, OPPSCAP, GPCI, ANES) - Parser module 100% ✅
**Status:** ✅ COMPLETE
- ✅ Tests pass for completed parsers (18/18 passing)
- ✅ No breaking changes to existing code

**Progress:** 10/14 tasks complete (71%)

**GPCI v1.3 Migration Ready:**
- Status: ✅ Code complete, ⏭️ Deployment pending
- Alembic migrations: `003` (NK unique index), `004` (compat view)
- Backfill script: `scripts/backfill_gpci_v13.py`
- Operator runbook: `.cursor/plans/GPCI_V13_MIGRATION_GUIDE.md`
- **Production Deployment Guide:** [📖 RUN-render-deployment-prd-v1.0.md](prds/RUN-render-deployment-prd-v1.0.md) ⭐
- Est. time: 45 min (Render), rollback available

**Test Results:**
- Parser tests: 68/68 passing (100%) ✅
- Integration tests: 24/24 passing (100%) ✅
- Python environment: Fixed (pandas 2.1.4) ✅
- Audit: 9/12 checks passed ✅

**Deployment Options:**
1. **Render (Recommended):** Clean setup, $7/month, 45 min
   - Guide: `prds/RUN-render-deployment-prd-v1.0.md`
   - Lessons: `.cursor/LESSONS_DATABASE_SETUP.md`
2. **Docker:** Local testing only (init script conflicts identified)
3. **Railway/Supabase:** Alternatives to Render

---

### Task 67: Deploy GPCI v1.3 to Render (Production Database)

**Category:** Infrastructure / Production Deployment  
**Status:** ✅ COMPLETE
**Priority:** HIGH  
**Status:** ✅ COMPLETE
**Estimated Time:** 45 minutes  
**Complexity:** Medium  
**Cost:** $7/month (Starter tier)

**📖 Primary Guide:** [RUN-render-deployment-prd-v1.0.md](prds/RUN-render-deployment-prd-v1.0.md)

**Objective:**
Deploy GPCI v1.3 schema and data to production-ready Render PostgreSQL database.

**Prerequisites:**
- ✅ Code tested (68/68 parser tests passing, 24/24 integration tests passing)
- ✅ Python environment fixed
- ✅ Migration artifacts complete (Alembic 003, 004, backfill script)
- ✅ Documentation comprehensive (11 guides created)
- ⏭️ Render account (sign up with GitHub)
- ⏭️ Payment method (Starter DB: $7/month after trial)
- ⏭️ 45 minutes uninterrupted time

**Implementation Steps:**

**Part 1: Render Account & Database (15 min)**
1. Sign up: https://render.com/ (use GitHub OAuth)
2. Create PostgreSQL database:
   - Name: `cms-pricing-db`
   - Database: `cms_pricing`
   - User: `cms_user`
   - Region: Oregon (US West) or closest
   - Instance: Starter ($7/month)
   - PostgreSQL Version: 16
3. Get External Database URL from dashboard
4. Set `DATABASE_URL` locally
5. Test connection: `psql $DATABASE_URL -c "SELECT 1;"`

**Part 2: Schema Migration (10 min)**
6. Run Alembic migrations: `alembic upgrade head`
7. Verify v1.3 unique index created:
   ```sql
   SELECT indexname FROM pg_indexes 
   WHERE tablename = 'gpci_indices' 
     AND indexname = 'uq_gpci_mac_locality_effective';
   ```
8. Check tables created: `psql $DATABASE_URL -c "\dt"`

**Part 3: Data Load (10 min)**
9. Dry-run backfill: `python scripts/backfill_gpci_v13.py --dry-run`
10. Verify 0 duplicates in dry-run output
11. Commit backfill: `python scripts/backfill_gpci_v13.py --commit`
12. Verify row count: `psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"`
    - Expected: ~109 rows

**Part 4: Verification (10 min)**
13. Run parser tests: `pytest tests/ingestion/test_gpci_parser_golden.py -v`
14. Verify no duplicates on v1.3 NK:
    ```sql
    SELECT mac, locality_id, effective_start, COUNT(*) 
    FROM gpci_indices 
    GROUP BY mac, locality_id, effective_start 
    HAVING COUNT(*) > 1;
    ```
    - Expected: 0 rows
15. Check ambiguous locality_id='00':
    ```sql
    SELECT mac, locality_id, locality_name 
    FROM gpci_indices 
    WHERE locality_id = '00' 
    ORDER BY mac;
    ```
    - Expected: ~15 rows (multiple MACs: AL, AZ, AR, CA, etc.)
16. Check Render dashboard metrics (connections, storage, CPU)

**Deliverables:**
- ✅ Production PostgreSQL database on Render
- ✅ GPCI v1.3 schema deployed (3-field natural key)
- ✅ 109 GPCI localities loaded
- ✅ Zero duplicates verified
- ✅ Monitoring configured
- ✅ Automatic backups enabled

**Acceptance Criteria:**
- Render database provisioned and accessible
- DATABASE_URL works from local machine
- Alembic migrations applied successfully (004 or 6d0f0408be80 at head)
- v1.3 unique index `uq_gpci_mac_locality_effective` exists
- 109 GPCI rows loaded with 0 duplicates
- Parser tests pass against Render database
- Monitoring alerts configured
- DATABASE_URL documented securely (1Password/vault)

**Documentation:**
- Deployment guide: `prds/RUN-render-deployment-prd-v1.0.md` (650 lines, 7-part process)
- Migration guide: `.cursor/plans/GPCI_V13_MIGRATION_GUIDE.md` (updated with fresh DB notes)
- Deployment checklist: `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (465 lines)
- Lessons learned: `.cursor/LESSONS_DATABASE_SETUP.md` (Docker insights)
- Quick start: `.cursor/plans/GPCI_V13_QUICK_START.md`

**Notes:**
- Docker local deployment has init-db.sql conflicts (see LESSONS_DATABASE_SETUP.md)
- Render provides clean setup path (no conflicts)
- SQLAlchemy model already has v1.3 index in `__table_args__`
- For fresh DB: Can create tables from models OR run Alembic
- For existing DB: Run Alembic upgrade → backfill

**Rollback Plan:**
- Alembic downgrade: `alembic downgrade -1` (< 2 min)
- Full restore: Render dashboard → Backups → Restore (5 min)
- Backfill restore: Use automatic backup table (3 min)

**Success Metrics:**
- Row count: 109 (expected)
- Duplicates: 0 on (mac, locality_id, effective_start)
- Locality '00': ~15 different MACs
- Parse time: < 2s
- Reject rate: 0%

**Cost:**
- Starter database: $7/month (1GB, 7-day backups, 97 connections)
- Optional web service: $7/month (API deployment)
- Total: $7-14/month

**Timeline:**
- Render signup: 5 min
- Database creation: 10 min
- Schema migration: 10 min
- Data load: 10 min
- Verification: 10 min
- Total: 45 minutes

**Status:** Ready to execute when deployment window is available

**Links:**
- [Render Deployment Guide](prds/RUN-render-deployment-prd-v1.0.md) ⭐ START HERE
- [Migration Checklist](.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md)
- [Lessons Learned](.cursor/LESSONS_DATABASE_SETUP.md)
- [Quick Start](.cursor/plans/GPCI_V13_QUICK_START.md)

---

#### Phase 1: Parsing Implementation (2-3 hours)

**Status:** Pending Phase 0 completion  
**Commit:** `mpfs-phase1-parsing`

**Tasks:**
1. Import shared parsers in MPFS ingestor
2. Implement `normalize_stage()` using shared parsers
3. Add schema validation gating
4. Extract metadata (vintage_date, product_year, quarter_vintage)
5. Update ingest_run metrics (rows_discovered, etc.)

**Deliverables:**
- Updated `mpfs_ingestor.py` (~200 lines)
- Schema validation integration
- Metadata extraction helpers

**Acceptance Criteria:**
-  MPFS ingestor uses shared parsers (not private RVU methods)
-  Schema validation blocks on contract violations
-  All 3 vintage fields populated correctly
-  Pillar metrics tracked in ingest_run
-  Tests pass with sample data

---

#### Phase 2: Tiered Validation (2-3 hours)

**Status:** Pending Phase 1 completion  
**Commit:** `mpfs-phase2-validation`

**Tasks:**
1. Create `validators/` module with tiered constraints
2. Implement BLOCK validators (HCPCS format, FIPS, locality key)
3. Implement WARN validators (ZIP↔ZCTA, county mapping)
4. Add reference data validation (HCPCS/CPT/POS against ref sets)
5. Implement quarantine table writes
6. Generate validation reports

**Deliverables:**
- `cms_pricing/ingestion/validators/` module (~300 lines)
  - `tiered_validator.py` - Block/warn/quarantine logic
  - `domain_validators.py` - HCPCS/FIPS/locality checks
  - `reference_validators.py` - Ref data validation
- Updated `validate_stage()` in MPFS ingestor

**Acceptance Criteria:**
-  Blocking validation stops ingestion on critical errors
-  Warning validation quarantines rows with soft errors
-  Reference data validated (HCPCS exists in ref set)
-  Validation report generated with counts
-  Quarantine table populated with rejected rows
-  Tests cover block/warn scenarios

---

#### Phase 3: Curated Views Creation (2-3 hours)

**Status:** Pending Phase 2 completion  
**Commit:** `mpfs-phase3-curated-views`

**Tasks:**
1. Implement `_create_curated_views()` for 6 views:
   - `mpfs_rvu` - Core RVUs + indicators
   - `mpfs_indicators_all` - Exploded policy flags
   - `mpfs_locality` - Locality dimension (or reference RVU table)
   - `mpfs_gpci` - GPCI indices (or reference RVU table)
   - `mpfs_cf_vintage` - Conversion factors (physician + anesthesia SEPARATE)
   - `mpfs_link_keys` - Minimal join keys
2. Add helper methods for indicator explosion
3. Implement latest-effective view generation
4. Add natural key validation (hcpcs, modifier, effective_from)

**Deliverables:**
- Updated `_create_curated_views()` (~350 lines)
- Helper methods (`_explode_indicators`, `_extract_anes_cf`, etc.)
- SQL for `v_latest_mpfs_rvu`, `v_latest_mpfs_gpci`, `v_latest_mpfs_link_keys`

**Acceptance Criteria:**
-  All 6 curated views created successfully
-  Natural keys properly defined per PRD
-  Three vintage fields on all rows
-  Physician CF ≠ Anesthesia CF (separate records)
-  Latest-effective views return current data only
-  Tests verify view structure and content

---

#### Phase 4: Persistence & Diff Reports (2-3 hours)

**Status:** Pending Phase 3 completion  
**Commit:** `mpfs-phase4-persistence`

**Tasks:**
1. Implement delete-and-insert upsert strategy
2. Add bulk insert to PostgreSQL (MPFSRVU, MPFSConversionFactor)
3. Export Parquet files for all 6 views
4. Generate vintage diff reports (compare to prior quarter)
5. Update ingest_run with final metrics
6. Create run summary artifact

**Deliverables:**
- Updated `_store_curated_data()` (~150 lines)
- Diff report generation (~100 lines)
- Parquet export logic
- End-to-end integration test

**Diff Report Contents:**
- Added/removed HCPCS codes
- RVU deltas by percentile (p50, p90, p99)
- GPCI percent changes
- Policy indicator churn
- Row count comparisons

**Acceptance Criteria:**
-  Delete-and-insert works correctly (no duplicates)
-  All data persisted to PostgreSQL
-  Parquet files exported to `data/curated/mpfs/{release_id}/`
-  Diff report generated comparing to prior vintage
-  Ingest run record complete with all 5 pillar metrics
-  End-to-end test passes with sample data
-  API can query persisted MPFS data

---

### Five Pillar Metrics Tracked

**1. Freshness**
- `source_published_at` - When CMS published the data
- `data_lag_hours` - Time between publication and ingestion

**2. Volume**
- `rows_discovered` - Total rows found in source files
- `rows_ingested` - Rows successfully stored
- `rows_rejected` - Rows that failed validation
- `rows_quarantined` - Rows with soft failures
- `bytes_processed` - Total data processed

**3. Schema**
- `schema_version` - Schema contract version used
- `schema_drift_detected` - Boolean flag for drift
- `schema_drift_details` - JSON with drift details

**4. Quality/Distribution**
- `validation_errors` - Count of validation errors
- `validation_warnings` - Count of warnings
- `null_rate_max` - Highest null rate across columns
- `distribution_drift_pct` - Statistical drift from prior

**5. Lineage/Usage**
- `source_urls` - List of CMS source URLs
- `dependencies` - Upstream data dependencies
- `downstream_notified` - Whether consumers notified

---

### Database Tables Created/Updated

**New Tables:**
- `ingest_runs` - Run tracking with five pillar metrics
- Quarantine table (for validation failures)

**Populated Tables:**
- `mpfs_rvu` (~19,000 rows per quarter)
- `mpfs_conversion_factor` (~2 rows per year)
- `mpfs_abstract` (if national payment data available)

**Reference Tables (reused):**
- `locality_county` (from RVU ingestor)
- `gpci_indices` or `gpci` (from RVU ingestor)

**Views Created:**
- `v_latest_mpfs_rvu`
- `v_latest_mpfs_gpci`
- `v_latest_mpfs_link_keys`

---

### Testing Strategy

**Golden Fixtures:**
- Sample PPRRVU data (100 rows representative)
- Sample GPCI data (10 localities)
- Sample conversion factor data
- Expected curated view outputs

**Test Coverage:**
1. **Parser Tests** - Each parser independently
2. **Schema Validation** - Contract enforcement
3. **Domain Validation** - HCPCS/FIPS/locality format
4. **Reference Validation** - Ref data existence checks
5. **Quarantine Tests** - Warn-level validation
6. **Upsert Tests** - Delete-and-insert idempotency
7. **Diff Report Tests** - Vintage comparison
8. **End-to-End Test** - Full pipeline with sample data

---

### Key Design Decisions (Per PRD Compliance)

1. **No Price Calculation** - Ingestor stores data only, no pricing math
2. **Three Vintage Fields** - `vintage_date`, `product_year`, `quarter_vintage`
3. **Natural Keys** - `(hcpcs, modifier, effective_from)` per PRD
4. **Tiered Validation** - Block on critical, warn+quarantine on soft
5. **Shared Parsers** - Public contracts, not private methods
6. **CF Separation** - Physician CF ≠ Anesthesia CF
7. **Reference Tables** - Don't duplicate locality/GPCI (use RVU tables)
8. **Upsert Strategy** - Delete-and-insert by `release_id`
9. **Latest-Effective Views** - SQL views for current data
10. **Diff Reports** - Compare curated vintages, not raw

---

### Success Metrics

**Functional:**
- ✅ All 6 curated views created
- ✅ Data persisted to PostgreSQL
- ✅ Parquet exports generated
- ✅ API can query MPFS data
- ✅ End-to-end test passes

**Quality:**
- ✅ Schema validation enforced
- ✅ Reference data validated
- ✅ Quarantine captures soft failures
- ✅ Diff report shows data changes

**Observability:**
- ✅ Five pillar metrics tracked
- ✅ Ingest run record complete
- ✅ Validation report generated
- ✅ Run summary artifact created

**Performance:**
- ✅ Ingestion completes in <5 minutes for quarterly data
- ✅ Upsert strategy prevents duplicates
- ✅ Latest-effective views return instantly

---

### Dependencies

**Upstream:**
- MPFS scraper (completed) - Provides discovery manifest
- Sample data (available) - `sample_data/rvu25d_0/`
- Database models (completed) - MPFSRVU, MPFSConversionFactor

**Downstream:**
- MPFS pricing API endpoints (future)
- MPFS analytics dashboards (future)
- Network adequacy analysis (future)

---

### Risk Mitigation

**Risk 1: RVU Ingestor Changes**
- Mitigation: Shared parsers with public contracts (SemVer)
- Impact: Low (both ingestors use same parsers)

**Risk 2: Schema Drift**
- Mitigation: Schema registry with strict validation
- Impact: Medium (blocks ingestion, requires schema update)

**Risk 3: Reference Data Missing**
- Mitigation: Tiered validation (warn, don't block)
- Impact: Low (quarantine rows, continue processing)

**Risk 4: Vintage Comparison Failure**
- Mitigation: Graceful fallback if no prior vintage
- Impact: Low (skip diff report, log warning)

---

### Related PRDs

- **`PRD-mpfs-prd-v1.0.md`** - MPFS schema and requirements ✅
- **`PRD-rvu-gpci-prd-v0.1.md`** - RVU parsing patterns ✅
- **`STD-data-architecture-prd-v1.0.md`** - DIS architecture ✅
- **`STD-data-architecture-impl-v1.0.md`** - DIS implementation guide ✅
- **`REF-scraper-ingestor-integration-v1.0.md`** - Integration patterns ✅

---

### Commits Summary

| Commit | Phase | Hours | Files | Lines | Status |
|--------|-------|-------|-------|-------|--------|
| `mpfs-phase0-shared-parsers` | Phase 0 | 3-4h | 9 | ~600 | ⏳ In Progress |
| `mpfs-phase1-parsing` | Phase 1 | 2-3h | 2 | ~200 | ⏸️ Pending |
| `mpfs-phase2-validation` | Phase 2 | 2-3h | 4 | ~300 | ⏸️ Pending |
| `mpfs-phase3-curated-views` | Phase 3 | 2-3h | 2 | ~350 | ⏸️ Pending |
| `mpfs-phase4-persistence` | Phase 4 | 2-3h | 3 | ~250 | ⏸️ Pending |

**Total:** 11-16 hours, 20 files, ~1,700 lines

---

## Notes

- This plan includes **97 total tasks** (68 original + 29 new tasks from CMS Pricing API Readiness Plan)
  - Tasks 69-88: Data Quality, Testing, Documentation, Performance, Observability, and Operations (Sections 5.1, 5.2, 5.4)
  - Tasks 89-97: Access & Compliance tasks (Section 5.3) including Quick Win #3 (API Keys table)
- Tasks are categorized by type and priority
- Estimated times are based on complexity analysis
- Dependencies are noted where applicable
- All tasks include source information for traceability (referenced to `DOC-cms-pricing-api-readiness-plan-v1.0.md`)
- Recent accomplishments and current focus updated as of 2025-01-15
- Architecture documentation tasks added as new high-priority items
- Readiness plan tasks fully mapped (Sections 5.1, 5.2, 5.3, 5.4)

---
*Generated by GitHub Tasks Setup Tool*  
*Last Updated: 2025-01-15*

---

## ⚠️ MANDATORY: PRD Review Before Next Parser (Added 2025-10-16)

**Status:** 🔴 **BLOCKER** - Must complete before starting Conversion Factor parser

### Context

PPRRVU parser implementation revealed critical gaps in documentation that caused:
- 2+ hours debugging schema-layout column name mismatches
- Missing natural key columns (modifier, effective_from)
- min_line_length filtering out all valid data
- Confusion about schema vs API naming conventions

### Required Actions

**BEFORE starting next parser (Conversion Factor, GPCI, ANES, OPPSCAP, or Locality):**

1. **Update STD-parser-contracts v1.2 → v1.3** (45 min)
   - Add error taxonomy (ParseError hierarchy)
   - Document schema vs API naming convention
   - Add layout-schema alignment rules
   - Document layout registry API
   - Add common pitfalls section
   - Update implementation template

2. **Update CHANGELOG.md** (5 min)
   - ✅ DONE - PPRRVU marked complete

3. **Review Before Implementation** (10 min)
   - Read updated STD-parser-contracts v1.3
   - Review DOC-parser-build-playbook-v1.0.md (Draft)
   - Verify layout has all schema columns
   - Check min_line_length against actual data
   - Confirm column names match schema exactly

### Why This is Mandatory

**Time ROI:**
- PRD update: 50 min investment
- Saved per parser: 1-2 hours × 5 parsers = 5-10 hours
- **Net benefit: 4-9 hours saved**

**Quality ROI:**
- Prevents repeating same debugging
- Establishes reusable template
- Documents architectural decisions
- Reduces cognitive load for future devs

### Checklist

Before starting next parser:
-  STD-parser-contracts v1.3 published
-  Read new sections (error taxonomy, naming, alignment)
-  Review DOC-parser-build-playbook-v1.0.md (Draft)
-  Verify layout exists and matches schema
-  Measure actual data line length
-  Confirm natural keys in layout

### Related Documents

- `PPRRVU_HANDOFF.md` - Full context on schema vs API naming
- `PPRRVU_FIX_PLAN.md` - Root cause analysis
- `cms_pricing/mappers/__init__.py` - Column transformation pattern
- `CHANGELOG.md` - Phase 1 progress

---

**Priority:** 🔴 Critical  
**Effort:** 50 minutes  
**Impact:** Saves 5-10 hours  
**Owner:** Parser team  
**Blocker for:** All remaining Phase 1 parsers

---

## 🔧 Parser Infrastructure Improvements (Deferred to CF Parser)

**Added:** 2025-10-16  
**Status:** 📋 Documented, ready for implementation  
**Source:** STD-parser-contracts v1.4 feedback integration  
**Effort:** 55 minutes total (fold into CF parser development)

### Overview

These improvements were documented in `STD-parser-contracts-prd-v1.0.md` v1.4 but code implementation is deferred to avoid premature abstraction. They will be implemented during Conversion Factor parser development.

### Tasks

#### Task A: Streaming File Reads (Priority: HIGH)
**Effort:** 15 minutes  
**Impact:** Prevents OOM on large files

**Current Issue:**
- PPRRVU reads entire file into memory: `content = file_obj.read()`
- Works for small files, problematic for large datasets

**Target Pattern:**
```python
# In parser template:
head = file_obj.read(8192)  # Just for BOM/encoding
encoding, _ = detect_encoding(head)
file_obj.seek(0)  # Reset to start
df = pd.read_fwf(file_obj, ...)  # Let pandas stream
```

**Implementation Checklist:**
-  Update `_parser_kit.py`: Ensure `detect_encoding()` accepts bytes (already does)
-  Update `pprrvu_parser.py`: Use head + seek(0) pattern
-  Update `cf_parser.py`: Follow new pattern from start
-  Update template in §21.1 with streaming example
-  Test: `detect_encoding()` works on 8KB head
-  Test: `file_obj.seek(0)` resets for pandas
-  Test: Memory usage bounded (doesn't grow with file size)
-  Test: All existing tests still pass

---

#### Task B: Metadata Preflight Validation (Priority: HIGH)
**Effort:** 10 minutes  
**Impact:** Fail fast, clear errors, saves debugging time

**Current Issue:**
- No upfront validation of required metadata
- Cryptic errors deep in parser (KeyError on `metadata['release_id']`)

**Target Pattern:**
```python
# In parser, Step 0 (before encoding detection):
REQUIRED_META = [
    "release_id", "schema_id", "product_year",
    "quarter_vintage", "file_sha256"
]
missing = [k for k in REQUIRED_META if k not in metadata]
if missing:
    raise ValueError(f"Missing required metadata: {missing}")
```

**Implementation Checklist:**
-  Add `validate_required_metadata()` to `_parser_kit.py`
-  Add tests in `test_parser_kit.py`
-  Call from `cf_parser.py` Step 0
-  Update `pprrvu_parser.py` to include preflight
-  Update template §21.1 with preflight step
-  Test: Clear error message with missing fields
-  Test: All parsers call it first

---

#### Task C: Layout-Schema Alignment Validator (Priority: MEDIUM)
**Effort:** 20 minutes  
**Impact:** Prevents 2-hour debugging (PPRRVU hit this exact issue)

**Current Issue:**
- §7.3 shows validation guard but function doesn't exist
- Manual alignment check in each parser
- PPRRVU spent 2 hours debugging column name mismatches

**Target Pattern:**
```python
# In _parser_kit.py:
def validate_layout_schema_alignment(
    layout: Dict,
    schema: Dict
) -> None:
    """
    Validate layout columns match schema exactly.
    
    Per STD-parser-contracts §7.3.
    
    Raises:
        LayoutMismatchError: If columns don't align
    """
    schema_cols = set(schema['columns'].keys())
    layout_cols = set(layout['columns'].keys())
    
    missing = schema_cols - layout_cols
    if missing:
        raise LayoutMismatchError(
            f"Layout missing schema columns: {missing}"
        )
    
    # Check for API naming anti-pattern
    if 'work_rvu' in layout_cols and 'rvu_work' in schema_cols:
        raise LayoutMismatchError(
            "Layout uses API names (work_rvu) not schema names (rvu_work)"
        )
```

**Implementation Checklist:**
-  Add function to `_parser_kit.py`
-  Add test to `test_parser_kit.py`
-  Call from `cf_parser.py` after loading layout+schema
-  Update template with handshake pattern
-  Test: Catches layout-schema mismatches
-  Test: PPRRVU v2025.4.0 (broken) vs v2025.4.1 (fixed)
-  Test: Called in template

---

#### Task D: Dynamic Skiprows Metrics (Priority: LOW)
**Effort:** 10 minutes  
**Impact:** CI observability for dynamic detection

**Current Issue:**
- Parsers detect data start dynamically but don't report it
- CI can't assert dynamic detection is happening

**Target Pattern:**
```python
# In parser after detecting data start:
data_start_idx, pattern_used = detect_data_start(file_obj, layout)
file_obj.seek(0)

metrics.update({
    "skiprows_dynamic": int(data_start_idx),
    "data_start_pattern": pattern_used  # e.g., "^[A-Z0-9]{5}$"
})
```

**Implementation Checklist:**
-  Add `detect_data_start()` to `_parser_kit.py` (or keep in parser)
-  Update `cf_parser.py` to emit metrics
-  Add CI test per §7.4 (`test_parser_reports_dynamic_skiprows`)
-  Update template with metrics pattern
-  Test: Metrics include `skiprows_dynamic`
-  Test: Metrics include `data_start_pattern`
-  Test: Value makes sense (0-10 typically)

---

#### Task E: Documentation Updates (Priority: LOW)
**Effort:** Included in other tasks  
**Impact:** Complete template for future parsers

**Add to §21.1 template comments:**
```python
def parse_{dataset}(...):
    """
    ...
    
    Special Cases:
    - ZIP files: List members, route each file, aggregate results
    - Excel files: Read as dtype=str to avoid date/float coercion, then cast
    - Large files: Chunked and single-shot parsing MUST produce identical output (hash-verified)
    """
```

**Implementation:** Just documentation (add during next PRD update)

---

### Success Criteria

**Before merging CF parser:**
-  All 4 code tasks (A-D) implemented
-  Template documentation (Task E) updated
-  All tests passing
-  No breaking changes to PPRRVU parser
-  `STD-parser-contracts-prd-v1.0.md` updated if needed

---

### Related Documents

- `STD-parser-contracts-prd-v1.0.md` v1.4 - §7.3, §7.4, §20.1, §21.1
- `PPRRVU_HANDOFF.md` - Real-world debugging context
- `cms_pricing/ingestion/parsers/_parser_kit.py` - Shared utilities
- `cms_pricing/ingestion/parsers/pprrvu_parser.py` - Reference implementation

---

### Time ROI

**Investment:** 55 minutes (spread across CF parser work)  
**Saved per parser:** Prevents 1-2 hours debugging × 4 remaining parsers = 4-8 hours  
**Net benefit:** 3-7 hours saved + improved code quality

---

## 🔬 Parser Testing Enhancement (v2.0 - Future)

**Added:** 2025-10-16  
**Status:** 📋 Deferred to STD-parser-contracts v2.0  
**Effort:** 2-3 hours (apply to all parsers for consistency)

### Overview

Upgrade golden test pattern from manual assertions to Parquet snapshot comparison for all parsers. This provides stricter schema/dtype regression detection and reduces snapshot brittleness.

### Current Pattern (v1.0-v1.6)

**What we do now:**
- Manual assertions (row count, columns, hash length)
- Parse twice for determinism testing
- Fixture integrity via SHA-256

**Used by:**
- PPRRVU parser ✅
- CF parser ✅ (following PPRRVU pattern)

**Pros:**
- Simple, clear assertions
- Works well for small datasets
- No dependencies on Parquet storage

**Cons:**
- Doesn't catch dtype drift (float64 → object)
- Doesn't verify column order
- Doesn't check categorical codes
- Manual maintenance for each assertion

### Proposed Pattern (v2.0)

**Parquet Golden Snapshots:**

```python
# tests/fixtures/{dataset}/golden/{dataset}_expected.parquet
# Stores the EXPECTED parser output with exact dtypes

def test_golden_parquet_comparison():
    """Compare actual vs expected using Parquet snapshot."""
    expected_df = pd.read_parquet(FIXTURES / "expected_output.parquet")
    
    result = parse_{dataset}(fixture_file, filename, metadata)
    
    # Strict comparison (dtypes, categorical codes, column order)
    pd.testing.assert_frame_equal(
        result.data[data_cols],  # Exclude timestamp columns
        expected_df[data_cols],
        check_dtype=True,
        check_categorical=True,
        check_column_type=True,
        check_exact=True  # For precision fields
    )
```

**Benefits:**
- ✅ Catches dtype regressions (float64 → object)
- ✅ Verifies column order (schema contract compliance)
- ✅ Checks categorical codes (domain enforcement)
- ✅ Single assertion (less maintenance)
- ✅ Parquet = no whitespace/newline churn

**Implementation Checklist:**
-  Add §14.2.1 to STD-parser-contracts v1.7 (document pattern)
-  Create Parquet golden for PPRRVU
-  Create Parquet golden for CF
-  Create Parquet golden for GPCI
-  Create Parquet golden for ANES
-  Create Parquet golden for OPPSCAP
-  Create Parquet golden for Locality
-  Update all golden tests to use `assert_frame_equal()`
-  Add CI check that Parquet goldens match manual assertions

### Success Criteria

**Before v2.0 release:**
-  All 6 parsers use Parquet golden pattern
-  STD-parser-contracts §14.2.1 documents pattern
-  CI enforces Parquet golden freshness
-  All tests passing (backward compatible migration)

### Related

- CF parser user feedback (2025-10-16): Parquet snapshots + assert_frame_equal
- PPRRVU parser: Current manual assertion baseline
- STD-parser-contracts v1.6 §14.2: Current golden test requirements

### Timeline

**Target:** STD-parser-contracts v2.0 (Q1 2026 or when all Phase 1 parsers complete)  
**Effort:** 2-3 hours to migrate all 6 parsers  
**Impact:** Catches 90% more regressions, reduces maintenance

---

## 🔧 Parser Shared Utilities Refactor (v2.0 - Future)

**Added:** 2025-10-16  
**Status:** 📋 Deferred to Phase 2 (after all 6 Phase 1 parsers complete)  
**Effort:** 3-4 hours (refactor all parsers + comprehensive testing)

### Overview

Consolidate duplicated parser logic into shared utilities to reduce code drift and improve maintainability. Currently each parser (PPRRVU, CF) has internal helpers that could be shared.

### Current Duplication (Phase 1 Pattern)

**What's duplicated across parsers:**

Each parser has its own:
- `_parse_csv()` - CSV dialect detection, header normalization
- `_parse_xlsx()` - Excel dtype=str handling
- `_parse_zip()` - ZIP member extraction and routing
- `_normalize_column_names()` - Column alias mapping
- `_cast_dtypes()` - Type coercion with precision
- `_is_fixed_width()` - Format detection heuristic

**Used by:**
- PPRRVU parser: All 6 helpers ✅
- CF parser: 4 helpers (no fixed-width) ✅
- Future parsers: Will duplicate again

**Pros (current approach):**
- ✅ Each parser self-contained and testable
- ✅ Easy to customize per-dataset quirks
- ✅ No cross-parser dependencies
- ✅ Ship parsers independently

**Cons:**
- ❌ ~150 lines duplicated per parser × 6 = 900 lines
- ❌ Bug fixes need propagation across 6 files
- ❌ Testing coverage duplication
- ❌ Harder to maintain consistency

### Proposed Refactor (v2.0)

**Create Shared Module:** `cms_pricing/ingestion/parsers/_io_kit.py`

**Shared Functions:**

```python
# _io_kit.py - Shared I/O utilities for all parsers

def read_tabular(
    file_obj: IO[bytes],
    filename: str,
    file_mime: Optional[str] = None,
    expected_columns: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Unified tabular file reader with format sniffing.
    
    Handles:
    - CSV/TSV (delimiter detection, BOM stripping)
    - XLSX (dtype=str to avoid Excel coercion)
    - ZIP (single-member extraction, multi-file validation)
    - Encoding cascade (UTF-8 → CP1252 → Latin-1)
    
    Args:
        file_obj: Binary stream
        filename: For format hints
        file_mime: Optional MIME type (e.g., 'text/csv')
        expected_columns: For validation (optional)
        
    Returns:
        (DataFrame, io_metrics_dict)
        
    Raises:
        ParseError: If format unreadable or multi-file ZIP without pattern match
        
    Per user feedback (2025-10-16 Phase 5 Workstream A).
    """
    io_metrics = {
        'encoding_detected': None,
        'dialect_detected': None,
        'skiprows_dynamic': 0,
        'zip_members': None
    }
    
    # 1. Format detection (MIME → filename → content sniff)
    # 2. ZIP handling (single member or pattern match)
    # 3. Encoding detection (BOM → UTF-8 → CP1252 → Latin-1)
    # 4. Parse with appropriate reader
    # 5. Return DataFrame + io_metrics
    
    # ... implementation ...


def normalize_column_headers(
    df: pd.DataFrame,
    alias_map: Dict[str, str]
) -> pd.DataFrame:
    """
    Normalize column headers with alias mapping.
    
    Steps:
    1. Lowercase, trim, collapse whitespace
    2. Apply dataset-specific alias map
    3. Verify expected columns present
    
    Args:
        df: Raw DataFrame
        alias_map: {'Conversion Factor': 'cf_value', ...}
        
    Returns:
        DataFrame with normalized column names
    """
    # ... implementation ...


def normalize_currency_value(
    series: pd.Series,
    precision: int = 2,
    rounding_mode: str = 'HALF_UP'
) -> pd.Series:
    """
    Normalize currency values (strip $, commas).
    
    Per user feedback (2025-10-16 Phase 5 Workstream B).
    
    Steps:
    1. Strip '$', ','
    2. Cast to float64
    3. Round to precision with rounding_mode
    
    Returns:
        Normalized numeric series
    """
    # ... implementation ...


def validate_date_range(
    df: pd.DataFrame,
    date_col: str,
    product_year: str,
    future_threshold_months: int = 15
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Validate date ranges and warn on future dates.
    
    Per user feedback (2025-10-16 Phase 5 Workstream D).
    
    Warns if effective_date > product_year + 15 months (year+1-03-31).
    
    Returns:
        (valid_df, warnings_list)
    """
    # ... implementation ...
```

**Parser Skeleton Becomes:**

```python
# pprrvu_parser.py (refactored)

from cms_pricing.ingestion.parsers._io_kit import (
    read_tabular,
    normalize_column_headers,
    normalize_currency_value,
    validate_date_range
)

ALIAS_MAP = {
    'HCPCS_CODE': 'hcpcs',
    'WORK_RVU': 'rvu_work',
    # ... PPRRVU-specific aliases
}

def parse_pprrvu(file_obj, filename, metadata) -> ParseResult:
    # Step 1: Read tabular (shared utility)
    df, io_metrics = read_tabular(file_obj, filename)
    
    # Step 2: Normalize headers (shared + dataset-specific aliases)
    df = normalize_column_headers(df, ALIAS_MAP)
    
    # Step 3-9: Dataset-specific logic (schema, validation, etc.)
    # ...
```

### Implementation Checklist

**Phase 1: Shared Module** (60 min)
-  Create `cms_pricing/ingestion/parsers/_io_kit.py`
-  Implement `read_tabular()` with CSV/XLSX/ZIP/TSV support
-  Implement `normalize_column_headers()` with alias mapping
-  Implement `normalize_currency_value()` for money fields
-  Implement `validate_date_range()` with future date warnings
-  Add comprehensive tests (`tests/ingestion/test_io_kit.py`)

**Phase 2: Refactor Parsers** (120 min)
-  Refactor PPRRVU to use `_io_kit` (30 min)
-  Refactor CF to use `_io_kit` (20 min)
-  Refactor GPCI to use `_io_kit` (20 min)
-  Refactor ANES to use `_io_kit` (20 min)
-  Refactor OPPSCAP to use `_io_kit` (15 min)
-  Refactor Locality to use `_io_kit` (15 min)

**Phase 3: Verification** (30 min)
-  All parser tests still pass (no regressions)
-  New `_io_kit` tests pass (80% coverage target)
-  Performance benchmarks unchanged
-  Code reduction: ~900 lines → ~150 lines (shared) + ~100/parser

**Phase 4: Documentation** (10 min)
-  Update STD-parser-contracts v2.0 §16 (directory layout)
-  Add §21.1.1 "Shared I/O Utilities" subsection
-  Document migration pattern
-  Update CHANGELOG.md

### Benefits

**Code Quality:**
- ✅ 750+ lines removed (duplication eliminated)
- ✅ Bug fixes propagate automatically
- ✅ Consistent behavior across parsers
- ✅ Easier to add new parsers (less boilerplate)

**Maintenance:**
- ✅ Single place for CSV dialect logic
- ✅ Single place for ZIP handling
- ✅ Single place for encoding cascade
- ✅ Centralized testing

**Future-Proofing:**
- ✅ Foundation for Parquet golden snapshots (v2.0)
- ✅ Foundation for streaming large files
- ✅ Foundation for parallel parsing

### Risks & Mitigations

**Risk 1: Breaking Changes**
- Mitigation: Comprehensive test suite (all existing tests must pass)
- Impact: Medium (caught by CI)

**Risk 2: Dataset-Specific Quirks**
- Mitigation: Preserve override hooks in each parser
- Impact: Low (shared utilities are baseline, parsers can extend)

**Risk 3: Performance Regression**
- Mitigation: Benchmark before/after
- Impact: Low (kit is already vectorized)

### Success Criteria

**Before merging v2.0:**
-  All 6 parsers use `_io_kit` shared utilities
-  All existing tests pass (0 regressions)
-  New `_io_kit` tests pass (≥80% coverage)
-  Code reduction ≥700 lines
-  Performance benchmarks unchanged (±5%)
-  STD-parser-contracts v2.0 documents pattern

### Timeline

**Target:** STD-parser-contracts v2.0 (after all Phase 1 parsers complete)  
**Effort:** 3-4 hours total (220 min breakdown above)  
**Impact:** 750+ lines removed, 6× easier maintenance, foundation for v2.0 enhancements

### Related

- User feedback (2025-10-16): Phase 5 Workstream A (shared read_tabular)
- PPRRVU parser: Current baseline pattern (internal helpers)
- CF parser: Will follow PPRRVU pattern, refactor in v2.0
- STD-parser-contracts v2.0: Will document shared utilities pattern

### Notes

**Why defer to v2.0:**
1. Ship Phase 1 parsers faster (proven PPRRVU pattern)
2. Learn from 6 parser implementations before abstracting
3. Avoid premature abstraction (might miss edge cases)
4. Refactor all 6 together for consistency

**Why do it in v2.0:**
1. Clear duplication pattern by then (6 parsers)
2. Known edge cases from real implementations
3. Can test migration thoroughly (all parsers at once)
4. Combines well with Parquet golden migration

---


---

## 📚 Documentation & Standards Improvements (Added 2025-10-17)

**Status:** 🔵 **ACTIVE** - PRD improvements from GPCI parser lessons learned

**Context:** During GPCI parser implementation (60% → near-100% test pass rate), we identified 10 critical improvements for parser PRD standards. Expected to reduce future parser implementation time by ~40% and debugging time by ~80%.

**Reference Documents:**
- `planning/parsers/gpci/LESSONS_LEARNED.md` - Detailed analysis of 10 key lessons
- `planning/parsers/gpci/GITHUB_TASKS_PRD_IMPROVEMENTS.md` - Full task specifications

**Task Summary:**
- **P0 (Critical):** 3 tasks - Format verification, CMS characteristics template, error handling standards
- **P1 (High):** 4 tasks - Test coverage matrix, incremental implementation, CMS quirks reference, metrics standards
- **P2 (Medium):** 3 tasks - Format detection, alias map best practices, type handling standards

**Total Effort:** ~20 hours over 3 weeks  
**Expected Impact:** 50% reduction in total parser implementation + debugging time  

**Quick Reference:**
1. **Task 1 (P0):** Update STD-parser-contracts with Format Verification Section (2h)
2. **Task 2 (P0):** Create "CMS Dataset Characteristics" Template (3h)
3. **Task 3 (P0):** Enhance Error Handling Requirements (2h)
4. **Task 4 (P1):** Add Test Coverage Matrix to QA Standard (2h)
5. **Task 5 (P1):** Document Incremental Implementation Best Practice (1.5h)
6. **Task 6 (P1):** Create "Common CMS Data Quirks" Reference (4h)
7. **Task 7 (P1):** Add Metrics Calculation Standards (1h)
8. **Task 8 (P2):** Enhance Format Detection Documentation (1.5h)
9. **Task 9 (P2):** Create Alias Map Best Practices Guide (1h)
10. **Task 10 (P2):** Add Type Handling Standards (1.5h)

**Success Metrics:**
| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Time to first working test | 3h | <2h | 33% faster |
| Debugging time | 5h | <1h | 80% less |
| First-run test pass rate | 60% | >80% | +20pts |

**Detailed task specifications available in:**  
`planning/parsers/gpci/GITHUB_TASKS_PRD_IMPROVEMENTS.md`

---

### Task: Locality Parser - Stage 2: FIPS Normalization

**Status:** ✅ COMPLETE (2025-10-18)  
**Priority:** Medium  
**Actual Time:** 2.3 hours (within 2-2.5h enhanced estimate)  
**Dependencies:** Locality Stage 1 (raw parser complete ✅), Reference data infrastructure (Phase 0 complete ✅)  
**Related PRDs:**
- `planning/parsers/locality/TWO_STAGE_ARCHITECTURE.md` (detailed design)
- `planning/parsers/locality/PHASE_0_REFERENCE_DATA.md` (reference data plan)
- `STD-data-architecture-impl-v1.0.md` §1.3 (transformation boundaries)
- `STD-parser-contracts-prd-v1.0.md` §21.1 (normalization patterns)

**Description:**

Implement Stage 2 of the two-stage Locality parser architecture: Transform raw county NAMES → FIPS codes and explode to one-row-per-county.

**Background:**
- Stage 1 (raw parser) COMPLETE: Parses CMS files preserving state/county NAMES (608 lines, 18 tests, 17 passing)
- Stage 2 (FIPS normalizer): Derives FIPS codes for downstream joins (GPCI, RVU, MPFS)
- Two-stage architecture chosen per `STD-data-architecture-impl §1.3` (transformation boundaries)

**Deliverables:**

**Code:**
- `cms_pricing/ingestion/normalize/normalize_locality_fips.py` (~300 lines)
- `NormalizeResult(data, quarantine, metrics)` API

**Reference Data (FULL coverage):**
- `data/reference/census/fips_counties/2025/us_counties.csv` (3,100+ rows)
- `data/reference/cms/county_aliases/2025/county_aliases.yml` (curated)

**Tests:**
- `tests/normalize/test_locality_fips_normalization.py` (~200 lines)
- 8+ tests: 3 golden, 3 edge, 2 negative
- Coverage ≥90%

**Acceptance Criteria:**

✅ **Coverage:** ≥99.5% of counties successfully derive FIPS codes  
✅ **Parity:** `SUM(exploded_rows) = SUM(county_count(raw))` (no rows lost/gained)  
✅ **Uniqueness:** Natural key `(locality_code, state_fips, county_fips)` enforced  
✅ **Edge Cases:** VA independent cities (5+), LA parishes (64), AK boroughs (29) handled  
✅ **Tests:** 8+ passing (3 golden, 3 edge, 2 negative), coverage ≥90%  
✅ **Determinism:** Sorted output, `row_content_hash`, `mapping_confidence` populated

**Time Estimate:**
- Reference data + alias map: 15 min
- Exact & alias matching: 30 min
- Fuzzy matching: 30 min
- County explosion: 20 min
- Edge cases (VA/LA/AK): 20 min
- Tests: 40 min
- **Total: 155 min (~2.5h with buffer)**

**Cross-References:**
- `tests/parsers/test_locality_parser.py` (Stage 1 tests)
- `cms_pricing/ingestion/parsers/locality_parser.py` (Stage 1 implementation)
- `planning/parsers/locality/TIME_MEASUREMENT.md` (ROI analysis)
- `prds/SRC-locality.md` (source descriptor)

---

### Task: Locality Parser - Complete Quarantine SLO (0% Achievement)

**Status:** ✅ COMPLETE (2025-10-20) - **PERFECT COVERAGE ACHIEVED**  
**Priority:** Medium  
**Actual Time:** 3 hours total (REST OF STATE: 2h, Final fixes: 1h)  
**Category:** Parser Enhancement & Testing  
**Labels:** locality, stage-2-normalization, set-logic, multi-state, medium-priority  
**Issue:** GH-33

**Description:**

Achieve <0.5% quarantine SLO for real CMS Locality files through REST OF STATE expansion, state extraction improvements, and final edge case fixes.

**Final Results:**
- ✅ **Quarantine SLO:** **0.00% (0/3,229 rows)** - **PERFECT COVERAGE**
- ✅ **REST OF STATE:** 16 localities expanded (1,042 counties)
- ✅ **Multi-state localities:** 3 VA independent cities correctly matched in DC locality
- ✅ **State inference:** All REST OF STATE localities have valid states (0 unknown_state)
- ✅ **All tests passing:** 23/23 (17 parser + 6 integration)

**Completed Work:**

**1. REST OF STATE Expansion (GH-33)** ✅ COMPLETE
- **Implementation:** Two-pass complement logic in `normalize_locality_fips.py`
  - First pass: Track explicit county assignments by state
  - Second pass: Expand REST OF STATE to `all_counties - explicit_counties`
- **Pattern detection:** Updated regex to match "ALL OTHER COUNTIES" (actual CMS format)
- **State extraction:** Handles truncated metro names ("WASHINGTON SEATT" → "WASHINGTON")
- **Results:** 16 localities, 1,042 counties expanded across 15 states
- **Impact:** Reduced quarantine from 1.56% → 0.50% (-68%)

**2. Ambiguous County Names** ✅ POLICY DECISION
- **Current:** 15 rows quarantined (all `unknown_county`)
- **Examples:** BUTTE (CA/ID/SD/MT), KINGS (CA/NY), SANTA CRUZ (CA/AZ)
- **Policy:** Quarantine as legitimate data ambiguities (acceptable at 0.50% threshold)
- **Future:** Curated overrides file (`locality_state_overrides.yml`) for manual disambiguation

**3. Independent City Aliases** ✅ NOT NEEDED
- St. Louis LSAD tie-breaking working correctly
- Not contributing to quarantine (matches successfully)

**Deliverables:**
1. ✅ REST OF STATE expansion implemented
2. ✅ Ambiguous county policy: quarantine (documented)
3. ✅ Test xfail removed (test passing)
4. ✅ SRC-locality.md updated with REST OF STATE documentation

**Acceptance Criteria:**
- ✅ Quarantine SLO ≤0.5% on real CMS sample file
- ✅ `test_locality_quarantine_slo_real_source` PASSING (no xfail)
- ✅ All 23/23 locality tests green
- ✅ Policy documented for ambiguous counties

**Follow-up Task Created:**
- "Locality Parser - Ambiguous County Overrides" (Optional enhancement for <0.30% target)

---

### Task: Locality Parser - Fix E2E GPCI Join Test

**Status:** ✅ COMPLETE (2025-10-18)  
**Priority:** Low  
**Actual Time:** 1 hour (mostly Docker cache debugging)  
**Category:** Testing & Validation  
**Labels:** locality, integration-test, test-fixture, low-priority

**Description:**

Fix skipped E2E integration test for Stage 1 → Stage 2 → GPCI join validation.

**Current Issue:**
- Test: `tests/integration/test_locality_e2e.py::test_locality_e2e_gpci_join_smoke`
- Status: Skipped with reason "TODO: Fix GPCI fixture format - needs proper fixed-width alignment"
- Problem: GPCI test fixture doesn't match `GPCI_2025D_LAYOUT` fixed-width specification

**Deliverables:**
1. Create properly aligned GPCI test fixture
   - Use `GPCI_2025D_LAYOUT` column positions
   - Include test data for CA localities (18, 26), VA (01), MO (02)
   - Verify with `tools/verify_layout_positions.py`
2. Update test to use correct fixture
3. Verify test passes (join rate ≥99.5%, CA ROS spot-checks)

**Acceptance Criteria:**
- Test unmarked from skip (no `@pytest.mark.skip`)
- Test passes in Docker CI
- Validates: join rate ≥99.5%, no duplicate NKs, CA ROS excludes LA/Orange, CA 18 includes only LA/Orange

**Files:**
- `tests/integration/test_locality_e2e.py` (line 364)
- `tests/fixtures/integration/gpci_join_sample.txt` (new fixture)

**Cross-References:**
- `cms_pricing/ingestion/parsers/gpci_parser.py` (GPCI parser)
- `cms_pricing/ingestion/parsers/layout_registry.py` (GPCI_2025D_LAYOUT)
- QTS v1.6 Appendix H.5 (Join Validation Patterns)

**Priority:** Low (core functionality works; this is observability/validation enhancement)

---

### Task: Locality Parser - Fix Real-Source Quarantine SLO Test

**Status:** ✅ COMPLETE (2025-10-18)  
**Priority:** Medium  
**Actual Time:** 3.5 hours (state tracking + inference implementation)  
**Category:** Testing & Data Quality  
**Labels:** locality, stage-1-parser, continuation-rows, medium-priority

**Description:**

Fix Stage 1 parser continuation row handling for real CMS files to enable quarantine SLO testing.

**Current Issue:**
- Test: `tests/integration/test_locality_e2e.py::test_locality_quarantine_slo_real_source`
- Status: Skipped with reason "TODO: Fix Stage 1 continuation row parsing - state_name truncated on real file"
- Problem: Continuation rows have state_name field truncated/bleeding into city names (e.g., "SAN F" instead of blank)
- Root cause: Fixed-width column boundaries not handling continuation rows correctly

**Symptoms:**
```
WARNING: Unknown state: "SAN F"  (should be blank, forward-filled)
WARNING: Unknown state: "VALLE"  (Vallejo - should be continuation)
WARNING: Unknown state: "BAKER"  (Bakersfield - should be continuation)
```

**Investigation Needed:**
1. Verify `LOCCO_2025D_LAYOUT` column positions (state_name: cols 19-50)
2. Check if continuation rows have different spacing/alignment
3. Review forward-fill logic in `_parse_txt_fixed_width`
4. Test with `tools/verify_layout_positions.py` on real CMS file

**Deliverables:**
1. Fix `locality_parser.py` continuation row handling
   - Detect continuation rows more robustly
   - Correctly identify when state_name is truly blank vs truncated
   - Ensure forward-fill only applies to blank fields
2. Update `LOCCO_2025D_LAYOUT` if column positions are incorrect
3. Add edge case test for continuation rows with city names
4. Verify quarantine SLO test passes with real file

**Acceptance Criteria:**
- Test unmarked from skip
- Test passes with real `sample_data/rvu25d_0/25LOCCO.txt`
- Quarantine rate ≤0.5%
- All states recognized (≥50 states with data)
- No "Unknown state" warnings for city names

**Files:**
- `cms_pricing/ingestion/parsers/locality_parser.py` (forward-fill logic)
- `cms_pricing/ingestion/parsers/layout_registry.py` (LOCCO_2025D_LAYOUT)
- `tests/integration/test_locality_e2e.py` (line 491)
- `tests/parsers/test_locality_parser.py` (add edge case test)

**Cross-References:**
- `planning/parsers/locality/PHASE_1_RAW_PARSER_PLAN.md` (Stage 1 design)
- STD-parser-contracts-impl-v2.0.md §2.1 (11-step parser template)
- QTS v1.6 Appendix H.4 (Quarantine SLO Enforcement)

**Priority:** Medium (blocks quarantine SLO validation; Stage 2 works correctly)

**Source:** Locality Parser Stage 2 hardening (2025-10-18)

---

### Reference: RVU E2E Harness Modernization

See `artifacts/RVU_E2E_HARNESS_MODERNIZATION_PLAN.md` for the full design and acceptance criteria.

Create the following GitHub issues and link them to the plan document:

1) RVU tests: write to tmp dir and support RVU_TEST_DATA_DIR ✅ COMPLETE
- Update tests to use `tmp_path`/`tmp_path_factory`.
- Add `RVU_TEST_DATA_DIR` env support; default to `/tmp` on Render.
- Acceptance: No writes under `tests/**`; artifacts appear under tmp.
- **Status:** ✅ COMPLETE - `tests/conftest.py` implements `test_data_dir` fixture with `RVU_TEST_DATA_DIR` support and tempfile fallback

2) Async hygiene: remove asyncio.run and await discovery ✅ COMPLETE
- Remove `asyncio.run()` from ingestors/helpers.
- Ensure `_discover_source_files_async` is awaited.
- Configure `pytest.ini` with `asyncio_mode = auto`.
- Acceptance: No running-loop RuntimeError; no un-awaited coroutine warnings.
- **Status:** ✅ COMPLETE - `pytest.ini` has `asyncio_mode = auto`, `_DiscoveryCallable` handles event loop detection, all E2E tests passing with async

3) Standardize stage I/O on RawBatch and add adapters ✅ COMPLETE
- Update `_normalize_stage`, `_enrich_stage`, `_publish_stage` to accept a `RawBatch`-like object.
- Provide adapters for legacy dict fixtures.
- Acceptance: No `'dict' object has no attribute 'metadata'` failures.
- **Status:** ✅ COMPLETE - `rvu_ingestor.py` has `_coerce_raw_batch_like()` method, stages accept RawBatch, all E2E tests passing

4) Discovery allowlist for dataset-scoped tests ⚠️ PARTIAL
- Add dataset allowlist (e.g., `datasets={"anes"}`) to limit discovery during tests.
- Acceptance: No OPPSCAP/PPRRVU logs/errors in ANES-only tests.
- **Status:** ⚠️ PARTIAL - Not fully implemented; tests may still discover all datasets. Consider implementing if test noise is an issue.

5) Reduce parser log noise and filter deprecations ✅ COMPLETE
- Aggregate/reduce decimal formatting warnings in tests.
- Add `pytest.ini` filterwarnings for known third-party deprecations.
- Acceptance: ≥90% warning reduction vs current run.
- **Status:** ✅ COMPLETE - `pytest.ini` has filterwarnings configured for Pydantic and pandas deprecations

6) Render compatibility: avoid read-only paths ✅ COMPLETE
- Route all test outputs to tmp locations in Render.
- Acceptance: No permission errors writing under repo paths.
- **Status:** ✅ COMPLETE - Tests use temp directories via `test_data_dir` fixture with `RVU_TEST_DATA_DIR` support

7) CI split: PR runs ANES-only; nightly runs full RVU ❓ NEEDS VERIFICATION
- Split jobs; temporarily xfail/skip OPPSCAP/PPRRVU E2E until harness is updated.
- Acceptance: PR CI green on ANES-only; nightly tracks full suite.
- **Status:** ❓ NEEDS VERIFICATION - Need to check `.github/workflows/` for CI configuration

8) Deprecation cleanup (backlog) 🟡 PLANNED
- Track Pydantic v2 migration and SQLAlchemy base import updates.
- Acceptance: Separate issues created; not blocking.
- **Status:** 🟡 PLANNED - Comprehensive plan created (see "Update Dependencies and Fix Deprecation Warnings" task below)

---

## Task: Update Dependencies and Fix Deprecation Warnings

**Status:** 🟡 PLANNED  
**Priority:** Medium  
**Estimated Time:** 8-12 hours  
**Category:** Maintenance & Technical Debt  
**Labels:** dependencies, deprecation-warnings, pydantic-v2, sqlalchemy-2, python-3.12, technical-debt

**Description:**

Fix all deprecation warnings identified during E2E test runs to ensure future compatibility and clean test output. This task addresses dependency upgrades and deprecated API usage across the codebase.

**Current Issues:**

Based on E2E test output (`pytest tests/ingestors/test_rvu_ingestor_e2e.py`), we have:

1. **datetime.utcnow() deprecation** (104 occurrences across 42 files)
   - Python 3.12+ recommends `datetime.now(datetime.UTC)` instead
   - Affects: All ingestor stages, parsers, observability, schemas, models

2. **SQLAlchemy declarative_base() deprecation** (multiple files)
   - SQLAlchemy 2.0+ recommends `sqlalchemy.orm.declarative_base()`
   - Affects: `cms_pricing/database.py`, all model files under `models/`

3. **Pydantic V2 migration** (multiple files)
   - `@validator` → `@field_validator` (already used in some files, need consistency)
   - `class Config` → `ConfigDict`
   - `Field(..., extra={})` → `Field(..., json_schema_extra={})`
   - `min_items` → `min_length`
   - `json_encoders` → custom serializers
   - Affects: Schema files, Pydantic models

4. **Pandas DataFrame.applymap() deprecation** (1 occurrence)
   - `applymap()` → `map()` (or use vectorized operations)
   - Affects: `cms_pricing/ingestion/parsers/pprrvu_parser.py:304`

5. **Third-party library deprecations** (filter via pytest.ini)
   - Pydantic V2 migration warnings
   - structlog datetime warnings
   - These are already filtered but can be addressed at source

**Goals:**

- Eliminate all deprecation warnings from test output (target: 0 warnings)
- Upgrade to latest stable versions of core dependencies
- Ensure backward compatibility where needed
- Maintain clean test output for easier debugging

**Scope:**

- Code changes: Update deprecated API calls
- Dependency updates: Pydantic, SQLAlchemy, pandas (if needed)
- Test updates: Verify all tests pass after changes
- Documentation: Update any affected PRDs/docs if API changes

**Deliverables:**

### Phase 1: Quick Wins (2-3 hours)
1. **Fix DataFrame.applymap() deprecation**
   - File: `cms_pricing/ingestion/parsers/pprrvu_parser.py:304`
   - Change: `df.applymap(_clean)` → `df.map(_clean)` (or vectorized equivalent)
   - Test: Run `pytest tests/parsers/test_pprrvu_parser.py -v`

2. **Fix SQLAlchemy declarative_base() imports**
   - Files:
     - `cms_pricing/database.py`
     - `cms_pricing/models/mpfs/mpfs_rvu.py`
     - `cms_pricing/models/mpfs/mpfs_conversion_factor.py`
     - `cms_pricing/models/opps/opps_apc_payment.py`
     - `cms_pricing/models/opps/opps_hcpcs_crosswalk.py`
     - `cms_pricing/models/opps/opps_rates_enriched.py`
     - `cms_pricing/models/opps/ref_si_lookup.py`
   - Change: `from sqlalchemy.ext.declarative import declarative_base` → `from sqlalchemy.orm import declarative_base`
   - Test: Run all model-related tests

3. **Fix datetime.utcnow() in high-impact files** (prioritize most-used)
   - Core files first:
     - `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (11 occurrences)
     - `cms_pricing/ingestion/observability/metrics_collector.py` (5 occurrences)
     - `cms_pricing/ingestion/run/dis_pipeline.py` (1 occurrence)
     - `cms_pricing/ingestion/observability/dis_observability.py` (2 occurrences)
   - Change: `datetime.utcnow()` → `datetime.now(datetime.UTC)` (add `from datetime import UTC` if needed)
   - Test: Run E2E tests to verify functionality

### Phase 2: Pydantic V2 Migration (4-6 hours)
1. **Audit Pydantic usage**
   - Identify all models using V1 patterns:
     - `@validator` decorators
     - `class Config` inner classes
     - `Field(..., extra={})` kwargs
     - `min_items` in Field()
     - `json_encoders` in Config
   - Files to check:
     - `cms_pricing/schemas/nearest_zip.py` (has `@validator`)
     - `cms_pricing/ingestion/adapters/data_adapters.py`
     - `cms_pricing/config.py`
     - All schema files under `cms_pricing/schemas/`

2. **Update Pydantic models systematically**
   - Replace `@validator` with `@field_validator`
   - Replace `class Config` with `model_config = ConfigDict(...)`
   - Replace `Field(..., extra={})` with `Field(..., json_schema_extra={})`
   - Replace `min_items` with `min_length`
   - Replace `json_encoders` with Pydantic V2 serializers
   - Test: Run schema validation tests

3. **Update dependency versions**
   - Review `requirements.txt` or `pyproject.toml`
   - Ensure Pydantic ≥2.8 (latest stable)
   - Ensure SQLAlchemy ≥2.0 (already using 2.0+ patterns)
   - Pin compatible versions

### Phase 3: Remaining datetime.utcnow() fixes (2-3 hours)
1. **Batch update remaining files** (42 files total, ~104 occurrences)
   - Use find/replace with careful review:
     - Pattern: `datetime.utcnow()` → `datetime.now(UTC)` (import `from datetime import UTC, datetime`)
     - Verify timezone-aware behavior is correct
   - Files grouped by module:
     - Parsers: `parsers/*.py`
     - Ingestion: `ingestion/**/*.py`
     - Services: `services/*.py`
     - Models: `models/**/*.py`
     - Schemas: `schemas/*.py`
     - Infrastructure: `infra/*.py`
   - Test: Full test suite

2. **Update structlog processor** (if needed)
   - File: `structlog` configuration
   - May need to update processor to use `datetime.now(UTC)`

### Phase 4: Validation & Cleanup (1-2 hours)
1. **Run full test suite**
   - `pytest tests/ -v` (all tests)
   - Target: 0 deprecation warnings
   - Verify: All tests pass

2. **Update pytest.ini filters** (if needed)
   - Remove filters for warnings we've fixed
   - Keep filters for third-party only

3. **Documentation updates**
   - Update any code examples in PRDs that use deprecated APIs
   - Add migration notes if breaking changes

**Acceptance Criteria:**

- ✅ All E2E tests pass: `pytest tests/ingestors/test_rvu_ingestor_e2e.py -v` (13/13 passing)
- ✅ Zero deprecation warnings in test output (excluding third-party libraries we don't control)
- ✅ All unit tests pass: `pytest tests/ -v`
- ✅ Code review: Changes follow existing patterns, no regressions
- ✅ Dependency versions updated and documented
- ✅ CI/CD pipeline green

**Files to Update:**

**High Priority (Core Functionality):**
- `cms_pricing/database.py` - SQLAlchemy base
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py` - 11 datetime.utcnow()
- `cms_pricing/ingestion/observability/metrics_collector.py` - 5 datetime.utcnow()
- `cms_pricing/ingestion/parsers/pprrvu_parser.py` - applymap() + datetime.utcnow()

**Medium Priority (Parsers & Services):**
- All parser files under `cms_pricing/ingestion/parsers/` - datetime.utcnow()
- `cms_pricing/ingestion/run/dis_pipeline.py` - datetime.utcnow()
- `cms_pricing/services/*.py` - datetime.utcnow()

**Low Priority (Models & Schemas):**
- All model files under `cms_pricing/models/**/*.py` - SQLAlchemy + datetime.utcnow()
- Schema files with Pydantic V1 patterns - Migration to V2

**Implementation Strategy:**

1. **Incremental approach**: Fix one category at a time, verify tests pass after each
2. **Create helper function** (optional): `def utc_now() -> datetime: return datetime.now(UTC)` for consistency
3. **Use search/replace carefully**: Verify context before bulk changes
4. **Test frequently**: Run affected test suites after each major change

**Risk Assessment:**

- **Low Risk**: datetime.utcnow() → datetime.now(UTC) (drop-in replacement, timezone-aware)
- **Low Risk**: SQLAlchemy declarative_base() import change (same function, different module)
- **Medium Risk**: Pydantic V2 migration (requires validation logic review)
- **Low Risk**: DataFrame.applymap() → map() (drop-in replacement)

**Dependencies:**

- Requires: Python 3.12+ (for `datetime.UTC`)
- Requires: Pydantic ≥2.8
- Requires: SQLAlchemy ≥2.0

**Cross-References:**

- `pytest.ini` - Warning filters
- `requirements.txt` / `pyproject.toml` - Dependency versions
- `prds/STD-api-performance-scalability-prd-v1.0.md` - Performance considerations
- E2E test output analysis from 2025-01-XX

**Notes:**

- Consider creating a pre-commit hook to catch new deprecation warnings
- Document any breaking changes if Pydantic V2 migration reveals incompatibilities
- Keep backward compatibility where possible (e.g., support both old and new patterns during transition)

**Owner:**
- Platform Engineering (Technical Debt / Maintenance)

**Related Issues:**
- RVU E2E Harness Modernization (Part 8: Deprecation cleanup backlog item)

---

## Task: RVU Guidance PDF Extraction & Documentation Pipeline

**Context:**
- CMS publishes RVU PDFs alongside machine-readable ZIP files containing payment guidance, formulas, status code legends, and policy notes
- We need to preserve these PDFs and extract structured summaries to feed manifests, observability, and PRDs
- Current implementation stores PDFs and generates summaries from parsed data, but does not extract text/metadata directly from PDFs

**Goals:**
- Keep every CMS RVU PDF alongside machine-readable payloads so we never lose published guidance
- Extract useful context (release dates, formulas, status-code legends, file descriptions) into structured summaries
- Surface summaries wherever operators or consumers need them (RUN books, REF schemas, documentation portal)

**Status Summary:**
- ✅ **COMPLETE (5 tasks):** PDF discovery/tagging, docs storage, summary generation, manifest integration, observability metrics
- ⚠️ **PARTIAL (1 task):** Warning logging for missing guidance
- ❌ **MISSING (9 tasks):** PDF text extraction tool, PDF parsing logic, validation gates, PRD updates, tests, re-run script

**Detailed Task Breakdown:**

### Phase 1: Land Stage ✅ COMPLETE
- ✅ **Task 1.1:** Allow PDFs through discovery, tagged with `file_type="pdf"`
  - **Status:** ✅ COMPLETE
  - **Evidence:** `SourceFile.file_type` field exists; `cms_rvu_scraper.py` infers file_type on line 104
- ✅ **Task 1.2:** On unzip, copy PDFs into `docs/cms_rvu/<release_id>/raw/` and write `docs_manifest.json`
  - **Status:** ✅ COMPLETE
  - **Evidence:** Implemented in `rvu_ingestor.py` lines 2538-2614 and 2703-2754

### Phase 2: Guidance Extraction ⚠️ PARTIAL
- ❌ **Task 2.1:** Create `tools/extract_rvu_pdf_guidance.py` step (run post-land)
  - **Status:** ❌ MISSING
  - **Requires:** pdfminer.six or PyPDF2 dependency, text extraction logic
  - **Effort:** ~4-6 hours (PDF parsing, regex patterns, error handling)
- ❌ **Task 2.2:** Parse and store: release metadata (effective/update dates, CF=32.3465), data file inventory, MPFS payment formula, Work/PE/MP definitions, status indicator legend, global period descriptions, policy warnings, support contacts
  - **Status:** ❌ MISSING
  - **Current:** `generate_guidance_summary` uses hardcoded templates + extracts CF/status/global from DataFrames
  - **Gap:** No actual PDF text extraction; need to parse PDF content directly
  - **Effort:** ~6-8 hours (pattern matching, table extraction, validation)
- ✅ **Task 2.3:** Save outputs under `docs/.../summary.md` and `docs/.../summary.json`
  - **Status:** ✅ COMPLETE
  - **Evidence:** `write_summary_files` function in `cms_pricing/ingestion/docs/guidance_summary.py`

### Phase 3: Normalize Stage ⚠️ PARTIAL
- ⚠️ **Task 3.1:** Skip PDFs for dataset parsing but ensure docs folder exists; log WARN if release ships without guidance
  - **Status:** ⚠️ PARTIAL
  - **Done:** PDFs are filtered out of raw ingestion payloads (line 2538-2574)
  - **Missing:** Explicit WARN logging when expected PDFs are absent
  - **Effort:** ~30 minutes
- ✅ **Task 3.2:** Reset indices before combining frames
  - **Status:** ✅ COMPLETE
  - **Evidence:** `reset_index(drop=True)` on line 1135 in normalize stage

### Phase 4: Publish & Observability ✅ COMPLETE
- ✅ **Task 4.1:** Append `guidance_docs` entries to published `manifest.json`
  - **Status:** ✅ COMPLETE
  - **Evidence:** `manifest_data["guidance_docs"] = guidance_entries` on line 2603
- ✅ **Task 4.2:** Extend observability payload with documentation counts and link(s) to summary file(s)
  - **Status:** ✅ COMPLETE
  - **Evidence:** `metrics_collector.py` lines 352-372 include `guidance_documents_count` and `guidance_documents_size_bytes`
  - **Note:** In `cms_pricing/ingestion/ingestors/rvu_ingestor.py`, observability now includes `observability.guidance = { documents_count, total_size_bytes, summary_generated }`. `summary_generated` will be `false` when no PDFs are discovered; it becomes `true` when PDFs are present and `summary.json` is written.
- ❌ **Task 4.3:** Fail the run if guidance extraction fails while zips succeed
  - **Status:** ❌ MISSING
  - **Requires:** Validation gate in pipeline that checks for expected guidance when PDFs are discovered
  - **Effort:** ~1-2 hours (add validation logic, error handling)

### Phase 5: Documentation Integration ❌ MISSING
- ❌ **Task 5.1:** Update `REF-rvu-database-schema-v1.0.md` to reference generated summaries for current release
  - **Status:** ❌ MISSING
  - **Effort:** ~30 minutes
- ❌ **Task 5.2:** Update `RUN-render-deployment-prd-v1.0.md` to reference generated summaries
  - **Status:** ❌ MISSING (mentioned in PRD line 1084 but may need explicit integration points)
  - **Effort:** ~30 minutes
- ❌ **Task 5.3:** Create `docs/rvu_guidance/README.md` that describes the pipeline
  - **Status:** ❌ MISSING
  - **Effort:** ~1 hour (document structure, workflow, usage examples)

### Phase 6: Automation & Validation ❌ MISSING
- ❌ **Task 6.1:** Add smoke tests ensuring every manifest result has corresponding doc entries when PDFs are discovered
  - **Status:** ❌ MISSING
  - **Evidence:** No test files found matching "guidance" or "docs_manifest"
  - **Effort:** ~2-3 hours (test fixtures, assertions, edge cases)
- ❌ **Task 6.2:** Provide manual re-run script that re-extracts summaries for a specific release if upstream PDF changes
  - **Status:** ❌ MISSING
  - **Effort:** ~2-3 hours (CLI tool, path resolution, idempotency)

**Acceptance Criteria:**
- PDFs are discovered, tagged, and stored alongside data files
- `docs_manifest.json` is created for every release with guidance PDFs
- `summary.json` and `summary.md` are generated from actual PDF text extraction (not just templates)
- Observability metrics include guidance document counts and sizes
- Tests verify guidance pipeline end-to-end
- PRDs reference guidance summary locations
- Re-run script allows regenerating summaries for specific releases

**Implementation Priority:**
1. **High:** Task 2.1 & 2.2 (PDF extraction - core functionality gap)
2. **High:** Task 4.3 (Validation gate - ensure quality)
3. **Medium:** Task 6.1 (Smoke tests - prevent regressions)
4. **Medium:** Task 6.2 (Re-run script - operational support)
5. **Low:** Task 5.1-5.3 (Documentation updates - nice to have)
6. **Low:** Task 3.1 (WARN logging - minor enhancement)

**Estimated Total Effort:**
- Core gaps (2.1, 2.2, 4.3): ~11-16 hours
- Testing & tooling (6.1, 6.2): ~4-6 hours
- Documentation (5.1-5.3, 3.1): ~2-3 hours
- **Total: ~17-25 hours**

**Owner:**
- Data Engineering (RVU Ingestion)

**Dependencies:**
- Requires: pdfminer.six or PyPDF2 (add to requirements.txt)
- Requires: Existing guidance_summary.py infrastructure (✅ already exists)
- Requires: PDF samples for testing extraction patterns

**Cross-References:**
- `cms_pricing/ingestion/ingestors/rvu_ingestor.py` (land stage, generate_guidance_summary)
- `cms_pricing/ingestion/docs/guidance_summary.py` (summary formatter)
- `cms_pricing/ingestion/run/dis_pipeline.py` (pipeline integration)
- `cms_pricing/ingestion/observability/metrics_collector.py` (metrics)
- `prds/REF-rvu-database-schema-v1.0.md` (needs update)
- `prds/RUN-render-deployment-prd-v1.0.md` (needs update)

**Related Issues:**
- RVU E2E Harness Modernization (Part 7: Documentation & guidance extraction)

### Follow-up Checklist (Added 2025-10-31)

- TODO: Verify Render API endpoint returns 2025-10-31 data (spot check `rvu_items`).
- TODO: Monitor Render logs/metrics for ingestion anomalies over next run.
- TODO: Draft backlog item for SQL `ON CONFLICT DO UPDATE` upsert optimization.
- TODO: Update external documentation / CHANGELOG with DB upsert fix summary.
