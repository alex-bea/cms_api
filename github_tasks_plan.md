# GitHub Tasks Plan: CMS API Development Tasks

Generated on: 2025-10-03 09:57:53

## Project Overview

**Name:** CMS API Development Tasks
**Description:** Comprehensive task management for CMS API development, data ingestion, and system enhancement
**Total Tasks:** 63

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

### Task 1: CMS Website Scraper

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** data-ingestion, critical-priority, from-todos

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** CMS Website Scraper

**Source:** NEXT_TODOS.md

---

### Task 2: File Download Manager

**Category:** Data Ingestion
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** data-ingestion, critical-priority, from-todos

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

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** Release Detection System

**Source:** NEXT_TODOS.md

---

### Task 4: CMS API Integration

**Category:** API Development
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** api-development, critical-priority, from-todos

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** CMS API Integration

**Source:** NEXT_TODOS.md

---

### Task 5: Third-Party Data Sources

**Category:** General
**Priority:** Critical
**Estimated Time:** TBD
**Labels:** general, critical-priority, from-todos

**Description:**
**Section:** Phase 1: Dynamic Data Acquisition

**Details:** Third-Party Data Sources

**Source:** NEXT_TODOS.md

---

### Task 6: Streaming Data Pipeline

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Streaming Data Pipeline

**Source:** NEXT_TODOS.md

---

### Task 7: Incremental Updates

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

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Data Analytics Dashboard

**Source:** NEXT_TODOS.md

---

### Task 9: Predictive Analytics

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Predictive Analytics

**Source:** NEXT_TODOS.md

---

### Task 10: Advanced Query Capabilities

**Category:** General
**Priority:** High
**Estimated Time:** TBD
**Labels:** general, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** Advanced Query Capabilities

**Source:** NEXT_TODOS.md

---

### Task 11: API Versioning & Backward Compatibility

**Category:** API Development
**Priority:** High
**Estimated Time:** TBD
**Labels:** api-development, high-priority, from-todos

**Description:**
**Section:** Phase 2: System Enhancement

**Details:** API Versioning & Backward Compatibility

**Source:** NEXT_TODOS.md

---

### Task 12: Horizontal Scaling

**Category:** Performance
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** performance, medium-priority, from-todos

**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Horizontal Scaling

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

**Source:** NEXT_TODOS.md

---

### Task 14: Security Hardening

**Category:** Security
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** security, medium-priority, from-todos

**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Security Hardening

**Source:** NEXT_TODOS.md

---

### Task 15: Compliance & Governance

**Category:** Security
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** security, medium-priority, from-todos

**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Compliance & Governance

**Source:** NEXT_TODOS.md

---

### Task 16: Advanced Monitoring

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** monitoring, medium-priority, from-todos

**Description:**
**Section:** Phase 3: Infrastructure & Operations

**Details:** Advanced Monitoring

**Source:** NEXT_TODOS.md

---

### Task 17: Intelligent Alerting

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** monitoring, medium-priority, from-todos

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

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Cross-Dataset Validation

**Source:** NEXT_TODOS.md

---

### Task 19: Data Lineage & Provenance

**Category:** General
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** general, medium-priority, from-todos

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Data Lineage & Provenance

**Source:** NEXT_TODOS.md

---

### Task 20: Automated Testing

**Category:** Testing
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** testing, medium-priority, from-todos

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Automated Testing

**Source:** NEXT_TODOS.md

---

### Task 21: Data Quality Monitoring

**Category:** Monitoring
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** monitoring, medium-priority, from-todos

**Description:**
**Section:** Phase 4: Data Quality & Validation

**Details:** Data Quality Monitoring

**Source:** NEXT_TODOS.md

---

### Task 22: Third-Party Integrations

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

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority, from-todos

**Description:**
**Section:** Phase 5: Integration & Ecosystem

**Details:** Data Export & Import

**Source:** NEXT_TODOS.md

---

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

### Task 34: Task 9: HRSA Ingester**

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 3: Reference Data Ingestors

**Task:** HRSA Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 35: Task 10: NBER Distance Ingester**

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 3: Reference Data Ingestors

**Task:** NBER Distance Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 36: Task 11: NADAC Ingester**

**Category:** Data Ingestion
**Priority:** Medium
**Estimated Time:** 3-4 days
**Labels:** ingestion, ingester, medium-priority, from-ingestor-tasks

**Description:**
**Phase:** Phase 3: Reference Data Ingestors

**Task:** NADAC Ingester**

**Priority:** Medium

**Source:** INGESTOR_DEVELOPMENT_TASKS.md

---

### Task 37: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:61`

**Details:** \s*(.+)",

---

### Task 38: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:62`

**Details:** \s*(.+)",

---

### Task 39: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:63`

**Details:** \s*(.+)",

---

### Task 40: General: \s*(.+)",

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `tools/github_tasks_setup.py:64`

**Details:** \s*(.+)",

---

### Task 41: General: Implement actual task processing

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/worker.py:55`

**Details:** Implement actual task processing

---

### Task 42: General: Implement cache warming

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/main.py:98`

**Details:** Implement cache warming

---

### Task 43: General: Get from benefit params

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/engines/ipps.py:113`

**Details:** Get from benefit params

---

### Task 44: General: Implement proper tracking

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/nearest_zip_monitoring.py:40`

**Details:** Implement proper tracking

---

### Task 45: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:168`

**Details:** Extract from traces

---

### Task 46: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:169`

**Details:** Extract from traces

---

### Task 47: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:170`

**Details:** Extract from traces

---

### Task 48: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:171`

**Details:** Extract from traces

---

### Task 49: General: Extract from traces

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:172`

**Details:** Extract from traces

---

### Task 50: General: Implement actual replay logic

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/trace.py:199`

**Details:** Implement actual replay logic

---

### Task 51: General: Get from config

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_trace.py:25`

**Details:** Get from config

---

### Task 52: General: Extract from result if available

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_trace.py:102`

**Details:** Extract from result if available

---

### Task 53: Performance: Check performance SLOs

**Category:** Performance
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** performance, medium-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:97`

**Details:** Check performance SLOs

---

### Task 54: General: Implement snapshots table

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:138`

**Details:** Implement snapshots table

---

### Task 55: General: Implement proper snapshot selection logic

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:155`

**Details:** Implement proper snapshot selection logic

---

### Task 56: General: Implement active snapshot management

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/geography_health.py:174`

**Details:** Implement active snapshot management

---

### Task 57: General: Calculate remaining deductible

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:212`

**Details:** Calculate remaining deductible

---

### Task 58: General: Collect dataset information

**Category:** General
**Priority:** Low
**Estimated Time:** TBD
**Labels:** general, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:216`

**Details:** Collect dataset information

---

### Task 59: Database: Implement database loading

**Category:** Database
**Priority:** Low
**Estimated Time:** TBD
**Labels:** database, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:434`

**Details:** Implement database loading

---

### Task 60: Database: Implement database loading

**Category:** Database
**Priority:** Low
**Estimated Time:** TBD
**Labels:** database, low-priority

**Description:**
**File:** `cms_pricing/services/pricing.py:439`

**Details:** Implement database loading

---

### Task 61: API Development: Wire pricing plan persistence and dataset provenance

**Category:** API Development
**Priority:** High
**Estimated Time:** TBD
**Labels:** api-development, high-priority

**Description:**
**Files:** `cms_pricing/services/pricing.py:212`, `cms_pricing/services/pricing.py:216`, `cms_pricing/services/trace.py:168`, `cms_pricing/services/pricing.py:434`

**Details:** Capture stored plan components and dataset metadata in pricing responses, propagate dataset usage into trace records, and replace placeholder loaders to deliver full parity and auditing.

**Implementation Steps:**
- Load stored plan definitions by filling in `_load_plan_components`/`_get_plan_name` with SQLAlchemy queries so pricing reuses persisted sequences and metadata.
- Normalize stored and ad-hoc components through a shared helper before calling the engines to keep pricing inputs consistent.
- Extend pricing engines (starting with MPFS) to surface dataset identifiers/digests, leveraging revision/effective date data or snapshot lookups.
- Aggregate those dataset descriptors into `PricingResponse.datasets_used` to support comparison parity checks.
- Update `TraceService.store_run` to persist dataset usage and facility flags alongside run data for replay/audit trails.
- Backstop the change with unit/integration coverage that executes a stored plan and asserts dataset metadata and trace persistence.

---

### Task 62: Data Ingestion: Replace placeholder validation/adaptation logic

**Category:** Data Ingestion
**Priority:** High
**Estimated Time:** TBD
**Labels:** data-ingestion, high-priority

**Description:**
**Files:** `cms_pricing/ingestion/ingestors/mpfs_ingestor.py:152`, `cms_pricing/ingestion/ingestors/rvu_ingestor.py:1`

**Details:** Implement real DIS validation rules, adapters, and enrichers in MPFS/RVU ingestors to enforce schema quality and unblock failing ingestion tests.

---

### Task 63: General: Execute automation and cache-warming backlog

**Category:** General
**Priority:** Medium
**Estimated Time:** TBD
**Labels:** general, medium-priority

**Description:**
**Files:** `NEXT_TODOS.md:11`, `cms_pricing/main.py:95`, `.github/workflows/cms_rvu_discovery.yml`

**Details:** Build the CMS release detection, downloader automation, and cache-warming routines so dataset freshness and observability match roadmap expectations.

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

## Notes

- This plan includes 63 total tasks
- Tasks are categorized by type and priority
- Estimated times are based on complexity analysis
- Dependencies are noted where applicable
- All tasks include source information for traceability

---
*Generated by GitHub Tasks Setup Tool*
