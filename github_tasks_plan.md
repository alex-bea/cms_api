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
- [ ] Document follows governance standards
- [ ] Includes comprehensive integration patterns
- [ ] Contains code templates and examples
- [ ] Documents testing strategies and best practices
- [ ] Cross-references related PRDs appropriately
- [ ] Passes audit tool validation

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
- [ ] New PRDs added to appropriate sections
- [ ] Dependency graph updated with new relationships
- [ ] Cross-references added to related documents
- [ ] Change log updated appropriately
- [ ] Passes audit tool validation

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
- [ ] All related PRDs reference new architecture documents
- [ ] Cross-references are bidirectional where appropriate
- [ ] References follow governance standards
- [ ] Passes audit tool validation

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
- [ ] All audit tools pass without errors
- [ ] No governance violations detected
- [ ] All cross-references are valid and bidirectional
- [ ] Master catalog is properly updated
- [ ] CI/CD pipeline passes documentation audits

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

#### Phase 0: Pre-Implementation Foundation (3-4 hours) ⏳ IN PROGRESS

**Status:** Setting up contracts and shared infrastructure  
**Commit:** `mpfs-phase0-shared-parsers`

**Tasks:**
1. ✅ Check database models (completed)
2. ⏳ Extract RVU parsing logic to shared `parsers/` module
3. ⏳ Create Schema Registry with SemVer contracts
4. ⏳ Build file-name → parser routing table
5. ⏳ Scaffold `ingest_runs` table with five pillar metrics

**Deliverables:**
- `cms_pricing/ingestion/parsers/` module (7 files, ~600 lines)
  - `pprrvu_parser.py` - PPRRVU parsing contract
  - `gpci_parser.py` - GPCI parsing contract
  - `locality_parser.py` - Locality parsing contract
  - `anes_parser.py` - Anesthesia CF parsing contract
  - `oppscap_parser.py` - OPPS cap parsing contract
  - `conversion_factor_parser.py` - CF parsing contract (NEW)
  - `schema_registry.py` - Schema validation (SemVer)
- `cms_pricing/models/ingest_runs.py` - Run tracking table
- Parser routing table documented

**Acceptance Criteria:**
- [ ] All parsers extracted from RVU ingestor
- [ ] Schema contracts registered for 6 file types
- [ ] Parser routing table maps filenames → parsers
- [ ] `ingest_runs` table created with 5 pillar metrics
- [ ] Tests pass for all parsers
- [ ] No breaking changes to RVU ingestor

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
- [ ] MPFS ingestor uses shared parsers (not private RVU methods)
- [ ] Schema validation blocks on contract violations
- [ ] All 3 vintage fields populated correctly
- [ ] Pillar metrics tracked in ingest_run
- [ ] Tests pass with sample data

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
- [ ] Blocking validation stops ingestion on critical errors
- [ ] Warning validation quarantines rows with soft errors
- [ ] Reference data validated (HCPCS exists in ref set)
- [ ] Validation report generated with counts
- [ ] Quarantine table populated with rejected rows
- [ ] Tests cover block/warn scenarios

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
- [ ] All 6 curated views created successfully
- [ ] Natural keys properly defined per PRD
- [ ] Three vintage fields on all rows
- [ ] Physician CF ≠ Anesthesia CF (separate records)
- [ ] Latest-effective views return current data only
- [ ] Tests verify view structure and content

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
- [ ] Delete-and-insert works correctly (no duplicates)
- [ ] All data persisted to PostgreSQL
- [ ] Parquet files exported to `data/curated/mpfs/{release_id}/`
- [ ] Diff report generated comparing to prior vintage
- [ ] Ingest run record complete with all 5 pillar metrics
- [ ] End-to-end test passes with sample data
- [ ] API can query persisted MPFS data

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

- This plan includes 68 total tasks (63 original + 5 new architecture documentation tasks)
- Tasks are categorized by type and priority
- Estimated times are based on complexity analysis
- Dependencies are noted where applicable
- All tasks include source information for traceability
- Recent accomplishments and current focus updated as of 2025-10-04
- Architecture documentation tasks added as new high-priority items

---
*Generated by GitHub Tasks Setup Tool*  
*Last Updated: 2025-10-04*
