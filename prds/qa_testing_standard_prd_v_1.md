# QA & Testing Standard PRD (v1.0)

## 0. Overview
This document defines the **QA & Testing Standard (QTS)** that governs validation across ingestion, services, and user-facing experiences. It specifies the canonical lifecycle for tests, minimum coverage and gating rules, artifact expectations, observability, and operational playbooks. Every product or dataset PRD must reference this standard and include a scope-specific **QA Summary** derived from §12.

**Status:** Draft v1.0 (proposed)  
**Owners:** Quality Engineering & Platform Reliability  
**Consumers:** Product, Engineering, Data, Ops  
**Change control:** ADR + QA guild review + PR sign-off  

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

### 7.1 Failure Handling & Quarantine
- Auto quarantine newly failing tests only with explicit override label (`ALLOW_QUARANTINE`) + issue link.
- Quarantined tests must display warning in CI summary and auto-fail after 7 days if unresolved.

### 7.2 Severity Matrix
| Severity | Trigger | Response |
|---|---|---|
| **Sev1** | Gate failure blocking release; production regression reproduced in staging | Immediate page, stop ship, formal RCA |
| **Sev2** | Nightly regression, SLO breach without production impact | Fix within 2 business days |
| **Sev3** | Flake >1%, non-blocking inconsistencies | Track in backlog, resolve within sprint |

## 8. Observability, Monitoring & Data SLAs
- Publish dashboards (Looker/Grafana) for pass rates, duration, coverage.
- Alert thresholds: pass rate <95%, flake >1%, duration +50% vs baseline.
- Test data freshness: ingest snapshots used in tests must lag production ≤ 30 days (unless synthetic).

### 8.1 Production Probes & SLAs
- Synthetic checks hit critical APIs every 5 min; latency SLO p95 tracked.
- Data drift monitors compare daily aggregates vs. baselines with 3σ thresholds.

### 8.2 Reporting
- Weekly QA health email with gate stats, top failures, quarantined tests.
- Monthly governance review summarizing compliance-related test outcomes.

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

