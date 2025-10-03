# QA & Testing Standard PRD (v1.0)

## 0. Overview
This document defines the **QA & Testing Standard (QTS)** that governs validation across ingestion, services, and user-facing experiences. It specifies the canonical lifecycle for tests, minimum coverage and gating rules, artifact expectations, observability, and operational playbooks. Every product or dataset PRD must reference this standard and include a scope-specific **QA Summary** derived from §12.

**Status:** Draft v1.0 (proposed)  
**Owners:** Quality Engineering & Platform Reliability  
**Consumers:** Product, Engineering, Data, Ops  
**Change control:** ADR + QA guild review + PR sign-off

**Cross-References:**
- **DOC-master-catalog_prd_v1.0.md:** Master system catalog and dependency map
- **STD-observability-monitoring_prd_v1.0:** Comprehensive monitoring standards, test observability, and unified SLAs
- **STD-data-architecture_prd_v1.0:** Data pipeline testing requirements and quality gates
- **STD-api-security-and-auth_prd_v1.0:** Security testing requirements and compliance procedures  

## 1. Goals & Non-Goals
**Goals**
- Establish a shared taxonomy and lifecycle for quality practices from local dev to production monitoring.
- Define minimum QA gates (unit, integration, data validation, end-to-end) that block merges and releases.
- Ensure deterministic, reproducible test runs with source-controlled fixtures and golden data.
- Provide operators with actionable visibility into test health, drift, and flakiness metrics.
- Protect compliance boundaries by codifying how regulated or licensed data is exercised in QA.

**Non-Goals**
- Dataset-specific validation logic (covered in dataset PRDs and QA Summaries).
- UX research, UAT sign-offs, or manual QA runbooks beyond automation triggers (tracked separately).

## 2. Definitions
- **Test Tier:** Layered classification (`unit`, `component`, `integration`, `scenario`, `data-contract`, `e2e`, `non-functional`).
- **Gate:** Blocking check enforced by CI/CD (merge gate, deploy gate, release gate).
- **Golden Dataset:** Canonical fixture archived under `/fixtures/golden/<domain>/` with manifest + checksum.
- **SLO Test:** Automated probe verifying an SLO (latency, accuracy, freshness) in CI or production.
- **Observability Probe:** Synthetic monitor executing core flows in production-like envs.
- **Drift Monitor:** Continuous check comparing live metrics vs. baselines with fail-fast thresholds.

## 3. QA Lifecycle & Architecture
We standardize QA into **Plan → Implement → Execute → Observe → Improve** with explicit contracts.

### 3.1 Plan (Contract-First QA)
- Author QA Summary per product/dataset using template in §12.
- Enumerate acceptance criteria, negative cases, and regulatory checks before writing code.
- Register test IDs and map to user stories or compliance requirements.

### 3.2 Implement (Test Assets)
- Co-locate tests with code (`tests/<domain>/<tier>/`).
- Store fixtures/golden data in `tests/fixtures/` with JSON schema or layout docs.
- Encode infrastructure mocks via approved libraries; no ad-hoc shell scripts.

### 3.3 Execute (Pipelines & Environments)
- Local dev: `make test` (unit/component) must pass before PR submission.
- CI merge pipeline: executes unit, component, data-contract, and lint/static analysis in parallel; integration + scenario suites run serially with artifact upload.
- Nightly broad-run: full suite including expensive end-to-end, load, and drift tests.
- Pre-deploy: smoke + health probes against staging using production-like configuration.
- Production: synthetic monitors & drift watchers continuously run; failures page the on-call.

### 3.4 Observe (Reporting & Telemetry)
- Emit structured test results (JUnit XML + JSON metadata) to the QA warehouse and dashboards.
- Track flake rates, duration percentiles, coverage deltas; SLO: **keep flake <1%** week-over-week.
- Require trace IDs linking failing tests to logs, metrics, and data snapshots.

### 3.5 Improve (Feedback Loop)
- Failing gate auto-creates ticket (Severity per §7.2). Owner ack ≤ 24h.
- Post-mortems for Sev1 QA escapes within 3 business days with action plan.
- Quarterly review of coverage gaps and aging skips.

## 4. Test Environment Matrix
```
/dev            # engineer laptops & ephemeral sandboxes
/ci             # containerized CI runners (deterministic inputs)
/staging        # production-config minus external auth tokens (read-only to prod data)
/perf           # load/perf dedicated environment with scaled datasets
/prod           # live environment with synthetic probes & canaries
```
- **Isolation:** No shared mutable state across concurrent test runs; use namespaced databases.
- **Secrets:** Provision via Vault-backed dynamic secrets; never commit test creds.
- **Data:** Stage production snapshots only in `perf` (tokenized, PII stripped) with audit logs.

## 5. Naming & Conventions
- Test files: `<module>_test.py`, `<feature>_spec.ts`, etc.; match language conventions.
- Fixture manifests: `manifest.json` listing columns, schema version, SHA256.
- Mocks/stubs named by contract: `MockGeoResolver`, `StubCMSClient`.
- Tag slow or flaky tests with markers (`@pytest.mark.slow`), documented in QA Summary with budget.

## 6. Versioning, Baselines & Temporal Semantics
- Version fixtures with SemVer; bump when schema fields change.
- Store baseline metrics per release in `/tests/baselines/<metric>.json` with `generated_at`, `source_digest`.
- QA Summary documents effective date of baselines; enforce backward compatibility via change detection tests.
- Golden datasets expire after 12 months unless revalidated.

## 7. Quality Gates (Minimum Bar)
- **Unit:** ≥90% line coverage for core libraries; failures block merge.
- **Component/Integration:** Critical paths (pricing calculations, geography resolution) tested with real schemas; run on every PR; must complete ≤ 20 min.
- **Data-Contract:** Schema diff detection; any breaking change blocks merge, surfaces ADR requirement.
- **Scenario/E2E:** Smoke flows executed pre-deploy; failure blocks release.
- **Non-Functional:** Load/perf + accuracy SLO tests run nightly; regression beyond tolerance opens Sev2.
- **API Contracts:** When surfaces are exposed, contract tests must also satisfy the governance defined in the **STD-api-architecture_prd_v1.0**.

### 7.1 Failure Handling & Quarantine
- Auto quarantine newly failing tests only with explicit override label (`ALLOW_QUARANTINE`) + issue link.
- Quarantined tests must display warning in CI summary and auto-fail after 7 days if unresolved.

### 7.2 Severity Matrix
| Severity | Trigger | Response |
|---|---|---|
| **Sev1** | Gate failure blocking release; production regression reproduced in staging | Immediate page, stop ship, formal RCA |
| **Sev2** | Nightly regression, SLO breach without production impact | Fix within 2 business days |
| **Sev3** | Flake >1%, non-blocking inconsistencies | Track in backlog, resolve within sprint |

## 8. Observability & Monitoring
- **References:** STD-observability-monitoring_prd_v1.0 for comprehensive monitoring standards
- **Test-specific monitoring:** Pass rates, duration, coverage, flake rates
- **Test data freshness:** Ingest snapshots used in tests must lag production ≤ 30 days (unless synthetic)
- **Synthetic checks:** Hit critical APIs every 5 min; latency SLO p95 tracked
- **Reporting:** Weekly QA health email with gate stats, top failures, quarantined tests

*For detailed implementation: See Observability PRD Section 2.5 (Testing Lineage), Section 3.3 (Testing SLAs), and Section 4.3 (Testing Metrics)*

## 9. Security & Access
- Synthetic data preferred; PII/PHI prohibited in fixtures.
- If regulated datasets needed, tokenize identifiers; store encryption keys in HSM/Vault.
- Access to QA environments requires role-based controls; all actions logged.
- Delete temporary data artifacts post-run (TTL 24h) unless flagged for debugging.

## 10. Test Metadata & Catalog Requirements
- Register each test suite in the QA catalog with:
  - `suite_id`, `owner`, `tier`, `environments`, `dependencies`, `last_updated`.
- Maintain lineage mapping from tests to datasets/services (use data catalog tags).
- Include data dictionary for each golden dataset (column, type, description, domain source).

## 11. Operations Runbook
- Describe how to rerun failing suites (`make test TARGET=...`, `pytest tests/...` etc.).
- Provide playbooks for clearing stuck pipelines, refreshing fixtures, rehydrating baseline metrics.
- Include contact rotation, escalation steps, communication templates.

## 12. QA Summary Template (per PRD)
Each PRD must append a QA Summary table covering:
1. **Scope & Ownership**
2. **Test Tiers & Coverage** (per tier, percentage, critical paths)
3. **Fixtures & Baselines** (datasets, schema versions, refresh cadence)
4. **Quality Gates** (merge, pre-deploy, release)
5. **Production Monitors** (synthetic checks, drift alerts)
6. **Manual QA** (if any, scripts + sign-off cadence)
7. **Outstanding Risks / TODO**

## 13. Change Management
- Propose updates via PR with rationale, impact analysis, and migration plan.
- Major changes (coverage thresholds, severity matrix) require architecture review board approval.
- Document deviations in PRD appendix with expiration date and mitigation plan.

## 14. Compliance & Licensing
- Track license obligations for vendor datasets used in tests; store consent forms.
- Ensure HIPAA/PHI compliance audits include QA environments.
- Maintain audit trails for who accessed regulated fixtures.

## 15. Appendix
### Appendix A — Test ID Naming Convention
- `QA-<DOMAIN>-<TIER>-<SEQUENCE>` (e.g., `QA-GEO-UNIT-0123`).

### Appendix B — Fixture Manifest Example
```
{
  "fixture_id": "geo_locality_v2025q1",
  "schema_version": "1.3.0",
  "source_digest": "sha256:...",
  "generated_at": "2025-09-29T12:34:56Z",
  "notes": "Derived from CMS ZIP→Locality 2025Q1; ZIP+4 records truncated to pilot states"
}
```

### Appendix C — CI Pipeline Reference
- `ci-unit.yaml`: runs lint + unit tests, coverage upload, fails on coverage drop >0.5%.
- `ci-integration.yaml`: spins ephemeral Postgres, loads golden data, runs resolver + pricing scenarios.
- `ci-nightly.yaml`: orchestrates long-running suites, perf tests, drift monitors.

### Appendix D — QA Health Dashboard KPIs
- Pass rate, flake rate, mean/median duration, coverage delta, quarantine count, pending Sev issues.

### Appendix E — Deviation Request Form (abridged)
Fields: `requester`, `suite_id`, `deviation_type`, `justification`, `mitigation`, `expiry`, `approver`.

### Appendix F — Cross-Reference Map

**Related PRDs:**
- **STD-observability-monitoring_prd_v1.0:** Comprehensive monitoring standards, test observability, and unified SLAs
- **STD-data-architecture_prd_v1.0:** Data pipeline testing requirements and quality gates
- **STD-api-security-and-auth_prd_v1.0:** Security testing requirements and compliance procedures

**Integration Points:**
- **Observability:** QTS Section 8 → Observability PRD Section 2.5 (Testing Lineage), Section 3.3 (Testing SLAs), Section 4.3 (Testing Metrics)
- **Data Testing:** QTS Section 7 → DIS Section 7 (Quality Gates), Section 8 (Observability & Monitoring)
- **Security Testing:** QTS Section 9 → Security PRD Section 22.1 (Operational Runbooks), Section 18B (Security Test Suites)

always check to see if you've already written tests before writing new ones

APPENDIX 
QA Testing Standard (QTS) — v1.1 with DIS Enhancements

Status: Proposed → Adopt for all ingestion/scraper repos

Owners: QA Guild (primary), Platform/Data Eng (co-owners)

Consumers: Ingestion engineers, Scraper maintainers, DevEx, Compliance

Change control: PR + QA Guild review + Architecture Board sign‑off

⸻

Changelog (v1.1)
	•	Added Section 2.1–2.3 to harden scraper method unit tests, coverage, and test infra.
	•	Added Section 2.1.1 Implementation Analysis Before Testing (REQUIRED) to prevent test-implementation mismatches.
	•	Added Section 2.2.1 Real Data Structure Analysis (REQUIRED) to ensure test expectations match actual behavior.
	•	Added Section 2.3.1 Mocking Strategy Analysis (REQUIRED) to target correct import paths and call patterns.
	•	Added Section 2.4 Test-First Discovery Process with implementation analysis workflow.
	•	Added Section 2.5 Test Accuracy Metrics with validation checklist and quality gates.
	•	Added Phase 3 performance & load testing with SLAs and measurement guidance.
	•	Added Phase 4 DIS metadata & catalog requirements (ingestion_runs table, technical/business metadata, lineage).
	•	Added Phase 5 fixture management (golden datasets, SemVer, baselines, backward compatibility).
	•	Added Phase 6 observability (five‑pillar dashboard + alerting).
	•	Introduced CI gates for QTS compliance and reporting.

⸻

0. Purpose & Scope

This standard defines the minimum bar for QA across all data scrapers and ingestion pipelines that feed DIS‑compliant systems. It codifies what must be tested, how results are measured, and which artifacts must be produced to prove compliance (coverage reports, baselines, metadata, lineage, dashboards).

Applies to:
	•	Web scrapers (e.g., CMS OPPS/MPFS/RVU/GPCI)
	•	Ingestors (Land → Validate → Normalize → Enrich → Publish)
	•	Catalog/metadata sidecars

Non‑goals: Security testing, privacy reviews, and legal license reviews are separate tracks; this doc references them where relevant.

⸻

1. Test Layers & Definitions
	•	Unit tests: Pure function/class tests; no network or filesystem (except tmp fixtures).
	•	Component tests: Module‑level with file IO and HTML parsing; network and browser calls are mocked.
	•	Integration tests: Exercise real disk and parser stacks with local fixtures; still no external network.
	•	End‑to‑end (E2E): Optional smoke against a known public URL with explicit allowlist and backoff; runs nightly only.
	•	Performance/Load: Benchmarks and concurrency tests.
	•	Contract/Baseline: Golden data & backward compatibility checks.

⸻

2. Scraper QA Requirements

2.1 Method Unit Tests (HIGH priority)

### 2.1.1 Implementation Analysis Before Testing (REQUIRED)

Before writing any unit tests, engineers MUST:

1. **Analyze Real Implementation**
   - Read the actual method signatures and return types
   - Understand the complete business logic flow
   - Identify all conditional branches and edge cases
   - Document the actual data structures returned

2. **Create Implementation Contract**
   ```python
   # Example: Document actual behavior before testing
   def _extract_quarter_info(self, link_info: Dict[str, str]) -> Optional[Dict[str, int]]:
       """
       ACTUAL RETURN TYPE: {'year': int, 'quarter': int} or None
       NOT: (year, quarter) tuple as initially assumed
       """
   ```

3. **Test-Driven Discovery Process**
   - Write ONE test case based on assumptions
   - Run it against real implementation
   - Update test expectations to match reality
   - Document the actual behavior
   - Then write comprehensive test suite

4. **Business Logic Documentation**
   - Document all business rules (e.g., "requires year pattern for quarterly addenda links")
   - Identify precedence rules and tie-breakers
   - Document error handling strategies

### 2.1.2 Required Test Coverage

Method	What to test	Cases
_extract_quarter_info(text)	Month→Quarter mapping; malformed strings	"January 2025"→{'year': 2025, 'quarter': 1}, "Apr 2025"→{'year': 2025, 'quarter': 2}, mixed case, extra spaces, non‑English months, None, empty, garbage
_is_quarterly_addenda_link(url, text)	True/false identification incl. edge words	positive phrases ("Addendum A/B", "Quarterly Update"), negatives ("Errata", "Annual"), case and punctuation, **requires year pattern**
_classify_file(href, link_text)	File type enum	zip, csv, xls/xlsx, pdf, unknown/ambiguous
_resolve_disclaimer_url(url)	Tiered resolution strategy	direct pass‑through, license trampoline (/apps/ama/license.asp?file=...), bad/malformed URLs, timeouts
_handle_disclaimer_with_browser(url)	Browser automation flow	detect button text variants, headless mode, popup failures, retry/backoff limits

Implementation notes
	•	Use pytest.mark.parametrize for table‑driven tests and to include negative/edge cases.
	•	Disallow real HTTP: mock httpx/requests.
	•	Disallow real browser: mock selenium/playwright drivers; assert called with expected selectors and timeouts.
	•	**CRITICAL**: Test expectations must match actual implementation return types and behavior

Example

@pytest.mark.parametrize(
    "text,expected",
    [
        ("January 2025 Addendum A", {'year': 2025, 'quarter': 1}),
        ("Apr 2025 OPPS", {'year': 2025, 'quarter': 2}),
        ("  july 2025  ", {'year': 2025, 'quarter': 3}),
        ("Oct 2025 Errata", {'year': 2025, 'quarter': 4}),
        ("N/A", None),
        (None, None),
    ],
)
def test_extract_quarter_info(text, expected):
    # CRITICAL: Test expectations must match actual implementation
    result = scraper._extract_quarter_info({'text': text, 'href': ''})
    assert result == expected

2.2 Coverage & Negative Testing (HIGH)

### 2.2.1 Real Data Structure Analysis (REQUIRED)

Before writing parametrized tests:

1. **Sample Real Data**
   ```python
   # Create test data from actual implementation
   def analyze_real_behavior():
       scraper = CMSOPPSScraper()
       
       # Test actual return types
       result = scraper._extract_quarter_info({'text': 'January 2025', 'href': ''})
       print(f"Actual return type: {type(result)}")
       print(f"Actual value: {result}")
       
       # Test actual boolean logic
       is_valid = scraper._is_quarterly_addenda_link('/test', 'Test')
       print(f"Actual boolean logic: {is_valid}")
   ```

2. **Document Actual Patterns**
   - What patterns actually work vs. what we assumed
   - What edge cases the implementation actually handles
   - What the implementation actually rejects

3. **Update Test Expectations**
   - Modify parametrized test cases to match reality
   - Add cases for actual edge cases discovered
   - Remove cases for scenarios the implementation doesn't handle

### 2.2.2 Coverage Requirements
	•	Coverage bar: ≥ 90% line coverage for core scraper modules (*_scraper.py, HTML classifier helpers). Measured with pytest-cov and enforced in CI.
	•	Negative cases: malformed URLs, network timeouts, 3xx/4xx/5xx, HTML without expected anchors, duplicate links, empty ZIPs, corrupt files.
	•	Mocking:
	•	HTTP: httpx.Client/AsyncClient mocked for status and payloads.
	•	Browser: mock selenium.webdriver / playwright.sync_api calls.
	•	Parsing: mock BeautifulSoup responses where appropriate.

Coverage config (excerpt)

# pyproject.toml
[tool.pytest.ini_options]
addopts = "-q --maxfail=1 --disable-warnings --cov=src --cov-report=xml --cov-fail-under=90"

2.3 Test Infrastructure (HIGH)

### 2.3.1 Mocking Strategy Analysis (REQUIRED)

Before writing mocks:

1. **Analyze Import Structure**
   ```python
   # Check where imports actually happen
   grep -r "from selenium" cms_pricing/ingestion/scrapers/
   grep -r "import selenium" cms_pricing/ingestion/scrapers/
   ```

2. **Understand Dependency Injection**
   - Are dependencies imported at module level or inside methods?
   - Are they passed as parameters or accessed globally?
   - What's the actual call pattern?

3. **Create Accurate Mocks**
   ```python
   # Mock the actual import path, not assumed path
   with patch('selenium.webdriver') as mock_webdriver:  # Correct
   # NOT: with patch('cms_pricing.ingestion.scrapers.cms_opps_scraper.webdriver')  # Wrong
   ```

4. **Test Mock Accuracy**
   - Verify mocks are actually called
   - Ensure mock return values match expected types
   - Test that mocks don't interfere with real logic

### 2.3.2 Infrastructure Requirements
	•	Fixtures with manifests: Each fixture directory must contain a manifest.yaml describing source URL, vintage, checksum, expected rows/files.
	•	Generators: Provide utilities to generate synthetic HTML pages with N links, controlled types, and broken variants.
	•	Isolation: Each test uses a tmp working dir; all temp artifacts cleaned at end. Randomness is seeded.

Fixture manifest (example)

fixture_version: 1.0.0
source:
  url: https://www.cms.gov/apps/ama/license.asp?file=/files/zip/july-2025-opps-addendum.zip
  retrieved_at: 2025-07-12T03:10:11Z
expected:
  files: ["addendum_a.csv", "addendum_b.csv"]
  sha256: "<zip-hash>"
  rows: { addendum_a.csv: 120345, addendum_b.csv: 90211 }
license:
  name: CMS Open Data
  attribution_required: true


⸻

## 2.4 Test-First Discovery Process (NEW)

### 2.4.1 Discovery Workflow

1. **Phase 1: Implementation Analysis**
   ```bash
   # Step 1: Understand the real implementation
   python -c "
   from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper
   scraper = CMSOPPSScraper()
   
   # Test actual behavior
   print('=== Real Implementation Analysis ===')
   result = scraper._extract_quarter_info({'text': 'January 2025', 'href': ''})
   print(f'Return type: {type(result)}')
   print(f'Return value: {result}')
   "
   ```

2. **Phase 2: Behavior Documentation**
   ```python
   # Document actual behavior in test file
   """
   ACTUAL IMPLEMENTATION BEHAVIOR:
   - _extract_quarter_info returns Dict[str, int] or None
   - _is_quarterly_addenda_link requires year pattern in text/href
   - _classify_file requires file extensions in patterns
   """
   ```

3. **Phase 3: Test Suite Creation**
   - Write tests based on documented behavior
   - Include edge cases discovered during analysis
   - Verify all conditional branches are covered

### 2.4.2 Validation Checklist

Before marking tests complete:
- [ ] All test expectations match actual implementation return types
- [ ] All business logic branches are tested
- [ ] All edge cases discovered during analysis are covered
- [ ] Mocks target actual import paths and call patterns
- [ ] Test data reflects real-world scenarios

⸻

## 2.5 Test Accuracy Metrics (NEW)

### 2.5.1 Accuracy Requirements

- **Implementation Alignment**: 100% of test expectations must match actual implementation behavior
- **Business Logic Coverage**: All documented business rules must have test cases
- **Edge Case Discovery**: Tests must include all edge cases found during analysis
- **Mock Accuracy**: All mocks must target actual import paths and call patterns

### 2.5.2 Validation Process

1. **Pre-Test Analysis**: Document actual behavior before writing tests
2. **Test-Implementation Alignment**: Verify all test expectations match reality
3. **Business Logic Validation**: Ensure all business rules are tested
4. **Mock Verification**: Confirm mocks work with actual implementation
5. **Edge Case Coverage**: Verify all discovered edge cases are tested

### 2.5.3 Quality Gates

- **Accuracy Gate**: 100% of test expectations must match implementation
- **Coverage Gate**: ≥90% line coverage on core modules
- **Business Logic Gate**: All documented business rules must have tests
- **Mock Gate**: All mocks must be verified against actual implementation

### 2.5.4 Practical Example: OPPS Scraper Lessons Learned

**Problem**: Tests assumed `_extract_quarter_info` returned `(year, quarter)` tuple, but actual implementation returned `{'year': year, 'quarter': quarter}` dict.

**Solution**: 
1. **Analyze First**: `python -c "from cms_pricing.ingestion.scrapers.cms_opps_scraper import CMSOPPSScraper; scraper = CMSOPPSScraper(); print(scraper._extract_quarter_info({'text': 'January 2025', 'href': ''}))"`
2. **Document Behavior**: `# ACTUAL RETURN TYPE: {'year': int, 'quarter': int} or None`
3. **Update Tests**: Change all test expectations from `(2025, 1)` to `{'year': 2025, 'quarter': 1}`
4. **Verify**: Run tests to confirm 100% alignment

**Key Takeaway**: Always analyze the real implementation before writing tests to prevent costly rewrites.

⸻

3. Performance & Load Testing (MEDIUM)

3.1 Benchmarking Framework
	•	Use pytest-benchmark to measure function‑level performance for classification and parsing.
	•	Metrics captured: latency p50/p95/p99, throughput (files/min), RSS memory peak during large file processing.
	•	Target SLA: < 30s per file end‑to‑end for standard addendum ZIPs (≤50MB) on CI‑class machines.

3.2 Load Profiles
	•	Simulate high‑volume scraping (100–300 files) with concurrency profiles: 1, 5, 10 workers.
	•	Stress disclaimer resolution: 50% of links require browser click‑through.
	•	Measure DB ingestion throughput with backpressure (e.g., Postgres COPY).

3.3 Performance Monitoring
	•	Export benchmark results as JSON; publish time series to observability collector.
	•	Fail CI on >20% regression versus last successful baseline.

Benchmark example

def test_classify_file_benchmark(benchmark, synthetic_links):
    def run():
        for href, text in synthetic_links:
            classify_file(href, text)
    benchmark(run)


⸻

4. DIS Metadata & Catalog Requirements (MEDIUM)

4.1 ingestion_runs Table (Required)

Persist one row per batch/run for auditability.

Schema (Postgres)

create table if not exists ingestion_runs (
  run_id uuid primary key,
  dataset text not null,
  pipeline text not null,
  source_urls jsonb not null,
  file_hashes jsonb not null,
  file_bytes bigint,
  row_count bigint,
  schema_version text,
  quality_score numeric(5,2),
  runtime_sec integer,
  cost_estimate_usd numeric(10,4),
  outcome text check (outcome in ('success','partial','failed')),
  environment text,
  commit_sha text,
  started_at timestamptz not null,
  finished_at timestamptz not null,
  tags jsonb default '{}'::jsonb
);

4.2 Technical Metadata (Auto‑capture)
	•	Schema capture: column names/types, nullability, primary keys/uniques.
	•	Constraint detection: inferred min/max, enum sets, regex.
	•	PII tags: heuristic detectors (SSN, email, phone) flagged to tags.
	•	Lineage: Emit OpenLineage events (job/run/dataset) on publish.

OpenLineage event (minimal)

{
  "eventType": "COMPLETE",
  "job": {"namespace": "pricing.scrapers", "name": "cms_opps_scraper"},
  "run": {"runId": "<uuid>"},
  "inputs": [{"namespace": "web", "name": "cms.gov/opps/addenda"}],
  "outputs": [{"namespace": "warehouse", "name": "curated.opps_addenda"}]
}

4.3 Business Metadata
	•	Ownership: dataset_owner, steward, escalation Slack channel.
	•	Glossary: definitions for Addendum A/B, Conversion Factor, Locality, etc.
	•	Classification: Public / Internal / Confidential / Restricted.
	•	License & Attribution: record license, obligations, and attribution strings.

⸻

5. QTS Fixture Management (MEDIUM)

5.1 Golden Datasets
	•	Maintain comprehensive golden datasets per dataset version with manifest.yaml.
	•	Version using SemVer (e.g., opps-golden@1.3.0).
	•	Validate goldens against schema contracts before use.
	•	Automate refresh when upstream schema/vintage changes.

5.2 Baselines & Backward Compatibility
	•	Store baseline metrics (row counts, distinct keys, distribution summaries) per release.
	•	Add change detection tests that diff current vs baseline; fail on breaking changes unless allowlisted.
	•	Enforce backward compatibility for APIs/curated views (snapshot tests on JSON/CSV outputs).

Snapshot test (example)

import json, pathlib

def test_curated_view_snapshot(tmp_path):
    current = run_curated_view_export()
    snapshot = json.loads(pathlib.Path("tests/snapshots/opps_curated_v1.json").read_text())
    assert current[0:100] == snapshot[0:100]

5.3 Test Environment Matrix
	•	Isolation: No cross‑test contamination; each test creates/cleans its own dirs and temp DB schemas.
	•	Secrets: Fetch dynamically from secret manager in CI; locally, use .env overrides.
	•	Config: Env‑specific settings via config.<env>.yaml with safe defaults.

Matrix (minimum)

Dimension	Values
Python	3.11.x (pin minor), optional 3.12 in canary
OS	Ubuntu LTS in CI; macOS dev supported
Browser	Chromium headless (Playwright) or ChromeDriver (Selenium)
DB	Postgres 14/15
Concurrency	1, 5, 10 workers


⸻

6. Observability & Alerting (MEDIUM)

6.1 Five‑Pillar Dashboard (Required)
	•	Freshness: time since last successful publish vs SLO (e.g., ≤ 14 days for monthly datasets).
	•	Volume: rows/bytes vs expectation (±15% warn, ±30% page).
	•	Schema: drift vs registered contract; field additions/removals.
	•	Quality: null rates, range checks, key uniqueness, categorical distributions.
	•	Lineage: upstream/downstream asset graph with last run status.

6.2 Alerting Rules
	•	Pager: freshness breach, schema drift, failed publish, performance regression (>20%).
	•	Slack: volume drift warnings, quality dips below thresholds.
	•	Ownership routing: alerts must include dataset owner and run URL.

⸻

7. CI Gates & Reporting
	•	Required to merge:
	•	Unit/component coverage ≥ 90% on core modules.
	•	Performance regression < 20% vs last green.
	•	All baseline/contract tests pass (or explicit, approved bypass label).
	•	Ingestion run metadata written; OpenLineage event emitted.
	•	Artifacts: JUnit XML, coverage XML/HTML, benchmark JSON, baseline diffs, fixture manifests.
	•	Badges: Coverage and last‑green status published to README.

CI (excerpt)

# .github/workflows/qts.yml
name: QTS Compliance
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements-dev.txt
      - run: pytest -q --cov=src --cov-report=xml --cov-fail-under=90
      - run: pytest tests/perf -q --benchmark-json=bench.json
      - run: python tools/check_perf_regression.py bench.json .qts/last_bench.json
      - run: python tools/emit_openlineage.py
      - uses: actions/upload-artifact@v4
        with: { name: qts-artifacts, path: "coverage.xml, bench.json, tests/**/manifest.yaml" }


⸻

8. Appendix

A. Selector expectations for disclaimer clicks
	•	Primary button text variants: "I Agree", "Accept & Continue", "Proceed".
	•	Fallback selectors must be configurable; retries: 3; backoff: 1s → 2s → 4s.

B. Quality checks (min set)
	•	File checksum verification on download; reject unknown sizes <10KB.
	•	CSV sniffing: delimiter, quoting; fail on mixed column counts.
	•	Unique key checks on curated outputs; null rate thresholds per field.

C. Business glossary (starter)
	•	Addendum A/B: OPPS hospital outpatient payment addenda.
	•	Conversion Factor (CF): Payment multiplier applied to RVUs.
	•	Locality: Geographic payment area used in MPFS.

⸻

9. Acceptance Criteria (Ready‑to‑Adopt)
	•	This repo implements Sections 2–7 with passing CI.
	•	**NEW**: Implementation analysis completed before writing tests (Section 2.1.1).
	•	**NEW**: Real data structure analysis documented (Section 2.2.1).
	•	**NEW**: Mocking strategy verified against actual import paths (Section 2.3.1).
	•	**NEW**: Test-First Discovery Process followed (Section 2.4).
	•	**NEW**: Test Accuracy Metrics achieved (Section 2.5).
	•	Coverage ≥ 90% on core modules; negative tests present for listed failure modes.
	•	Benchmarks in place with JSON output and regression guard.
	•	ingestion_runs table populated per run; OpenLineage events emitted.
	•	Golden datasets + baseline metrics stored and validated; snapshot/contract tests enabled.
	•	Five‑pillar dashboard live; alerts wired to owner channel.

⸻

End of v1.1