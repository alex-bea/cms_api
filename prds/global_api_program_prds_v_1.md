# Global API Program PRDs (v1.0)

> Single source of truth (SSOT): **/api-contracts/openapi.yaml**. All API shape changes are PR’d here first, then propagated to code, tests, and docs. Cross-refs to existing docs: **data\_ingestion\_standard\_prd\_v\_1.md** (DIS) and **nearest\_zip\_resolver\_data\_integration\_prd\_v\_1.md** (NZR-DI).

---

## 1) API Standards & Contract PRD

**Owner:** Platform Eng (API Guild)\
**Scope:** All externally and internally consumed HTTP/JSON APIs.\
**Goal:** Consistent, predictable APIs with explicit change control.

### 1.1 Versioning & Lifecycle

- **Semantic versioning** for OpenAPI: MAJOR.MINOR.PATCH (e.g., 1.3.2).
- **Backward-compatible** changes allowed in MINOR/PATCH. Breaking changes require **MAJOR** and deprecation window ≥ 90 days.
- **Deprecation policy:** publish `X-Deprecation` header + /status endpoint note + docs banner; provide migration guide.

### 1.2 Request/Response Envelope

- **Request**: JSON; UTF-8; snake\_case fields; idempotency via `Idempotency-Key` on mutating endpoints.
- **Response**: JSON object; top-level `data`, `meta`, `trace` where applicable. Errors use standard envelope:

```json
{
  "error": {
    "code": "string",
    "message": "human-readable",
    "details": [{"field":"path","issue":"desc"}]
  },
  "trace": {"correlation_id":"uuid"}
}
```

### 1.3 Pagination, Filtering, Sorting

- Cursor-based pagination: `page_size` (≤100), `next_cursor`.
- Filtering via whitelisted query params; server rejects unknown filters with 400.
- Sorting: `sort=field:asc|desc`.

### 1.4 Errors & Retries

- Standard codes: 400, 401, 403, 404, 409, 422, 429, 5xx.
- **Retry-After** honored for 429/503. Client idempotent retries allowed where specified.

### 1.5 Correlation & Observability

- Require `X-Correlation-Id` (generated if absent).
- Include `trace.vintage`, `trace.hash` for data-backed responses (see §4 Data Governance).

### 1.6 Contract Source & Tooling

- SSOT: `/api-contracts/openapi.yaml`.
- Lint: Spectral; CI blocks on rule violations.
- **Contract tests** auto-generated from SSOT; drift blocks merge.

### 1.7 SDK & Docs Generation

- Auto-generate SDKs (TS/Python) on tag.
- Publish Postman collection; examples must match live sandbox.

### 1.8 Acceptance Criteria

- Endpoint added only if: OpenAPI approved; examples provided; error cases defined; contract tests pass in CI.

### 1.9 OpenAPI SSOT & SemVer Enforcement

- **Single source of truth:** `/api-contracts/openapi.yaml` must be updated first. Code, tests, SDKs, docs, and changelog entries follow that PR.
- **SemVer rules:** MINOR/PATCH for backward-compatible changes; MAJOR for breaking changes with ≥90 day deprecation window. Deprecations advertise via `X-Deprecation` header and docs banner.
- **CI gates:** Spectral lint, contract-test suite, and router/spec drift check block merges on failure.
- **Release workflow:** Git tag (`vMAJOR.MINOR.PATCH`) equals spec version. Tag builds SDKs (TypeScript, Python) and Postman collection as release assets and publishes rendered docs to `/docs/<version>` (with `/docs` tracking main).

**Acceptance Criteria**

- ✅ CI blocks merges when Spectral lint or contract tests fail.
- ✅ Each API code PR references the spec diff (PR link or commit hash).
- ✅ Tagged releases upload SDKs/Postman assets and publish versioned docs + changelog.

---

## 2) Architecture & NFR PRD

**Owner:** Architecture Board\
**Scope:** Service boundaries, performance, resiliency.

### 2.1 Reference Architecture

- API Gateway → Service layer (stateless) → Data layer (immutable reference data w/ vintages) → Observability bus.
- External data sources ingested via DIS (see cross-ref to DIS).

### 2.2 Service Boundaries

- Each business capability = one service; shared utilities via libraries, not side-calls.
- No circular dependencies.

### 2.3 NFR Budgets (Default)

- **Latency:** p95 ≤ 200ms (read), p95 ≤ 400ms (write).
- **Throughput:** declare RPS target per service; perf tests enforce.
- **Availability:** 99.9% baseline.
- **Scalability:** horizontal; stateless workers.

### 2.4 Resiliency

- Timeouts ≤ 2s; retries w/ jitter; circuit breakers; bulkheads.
- Dependency matrix documented; fallbacks defined for P0 paths.

### 2.5 DR/BCP

- RPO ≤ 15m; RTO ≤ 1h for Tier-1.
- Quarterly restore drills with signed report.

### 2.6 Transport Middleware Stack (FastAPI)

- **Middleware order:** request_id → auth (API key + tenant bind) → rate limiting (Redis-backed) → body-size/timeouts → error shaper → metrics → tracing.
- **Correlation IDs:** require `X-Correlation-Id`; generate a UUIDv4 when absent and echo in responses/logs.
- **Auth & rate limits:** enforce API-key auth before rate limiting. Expose `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and on 429 include `Retry-After`.
- **Error envelope:** responses must use the standard error structure defined in the Error Catalog PRD.
- **Defaults:** body size capped at 5 MB unless overridden; request timeout default 30s.
- **Extensibility:** middleware must allow swapping to an external gateway in the future without changing headers/behavior.
- **FastAPI Implementation:** Use `@app.middleware("http")` decorators with proper exception handling, dependency injection for auth, and structured logging with `structlog`.

**Acceptance Criteria**

- ✅ Responses include the correlation ID header and surface it in the trace block.
- ✅ Error responses always follow the standard envelope.
- ✅ Where limits apply, rate-limit headers are present (and 429 adds `Retry-After`).
- ✅ FastAPI middleware stack properly handles exceptions and dependency injection.

### 2.7 Acceptance Criteria

- ADR recorded for new patterns.
- Perf SLOs defined + verified in CI perf smoke.

---

## 3) Security & Compliance PRD

**Owner:** Security Engineering\
**Scope:** AuthN/Z, data protection, audit, third-party data.

### 3.1 AuthN & AuthZ

- **API Key Management:** Simple API keys for external customers with format `cms_<env>_<tenant>_<random>` (e.g., `cms_prod_acme_abc123def456`). Keys stored hashed in database, never plaintext.
- **Key Validation:** Middleware validates key format, checks database for active keys, caches in Redis (5-minute TTL).
- **Key Scoping:** Keys bound to specific tenant/organization with per-key rate limits (1000 req/hour default).
- **Future RBAC:** Role-based access (read-only, admin, etc.) planned for later phases.
- **OAuth2/JWT:** Service-to-service via mTLS + short-lived tokens.
- **RBAC matrix:** Checked in as code; negative tests in CI.

### 3.2 Data Protection

- TLS 1.2+ in transit; AES-256 at rest.
- Secrets via vault; no secrets in env files/PRs.

### 3.3 PII/PHI Handling

- Data classification labels required; masking in logs; field-level encryption where mandated.
- Right-to-delete workflows defined.

### 3.4 Audit & Access

- Immutable audit log of admin and schema changes.
- Just-in-time access; monthly review.

### 3.5 External Data Licensing

- Source attribution retained (per DIS).
- License terms stored alongside dataset vintage; surfaced in `/meta/sources` endpoint.

### 3.6 Acceptance Criteria

- Security tests pass (auth required; PII redaction).
- No High/Critical SCA/Vuln findings at release time.

---

## 4) Data Governance & Vintage PRD

**Owner:** Data Platform\
**Scope:** Data contracts, vintages, provenance, lineage.

### 4.1 Data Contracts

- Schema defined in `/schemas/*.json` with owners and change policy.
- Changes follow MAJOR/MINOR; MUST include migration plan.

### 4.2 Vintages

- Org-wide `ACTIVE_VINTAGE` required for any reference-data-backed response.
- Response includes `trace.vintage` and `trace.hash` (sha256 of source bundle).
- Vintage lock check in CI.

### 4.3 Response Trace Block (Reproducibility)

- Every data-backed endpoint must include a `trace` object:

```
"trace": {
  "correlation_id": "uuid",
  "vintage": "2025Q1",
  "hash": "sha256:…"
}
```

- `trace.vintage` maps to the snapshot selected by the API (“Latest ≤ valuation date” unless a digest pin is provided).
- `trace.hash` is the dataset bundle digest (from DIS publish step) proving the exact data slice.
- Store correlation ID + trace fields in logs for reproducibility; expose them via `/trace/{run_id}`.

**Acceptance Criteria**

- ✅ `/price`, `/compare`, `/plans/*`, `/datasets/*`, `/geo/*` (and any data-backed endpoint) return the trace block.
- ✅ CI asserts presence/format of `trace.vintage` and `trace.hash` for these endpoints.

### 4.4 Provenance & Lineage

- Store `ingest_runs` with: source URL, timestamp, checksum, record counts.
- Expose `/meta/vintage` endpoint for transparency.

### 4.5 Backfills & Reprocessing

- Immutable historical vintages; new vintage creation never mutates prior outputs.
- Parity test required vs prior vintage.

### 4.6 Acceptance Criteria

- Great Expectations suite green on ingest.
- Parity delta within thresholds set in PRD.

---

## 5) Observability & Runbooks PRD

**Owner:** SRE\
**Scope:** Logs, metrics, traces, alerts, incident handling.

### 5.1 Logging

- **JSON logs:** Include `correlation_id`, `user_id` (hashed), `vintage`, `latency_ms`, `status_code`.
- **Correlation ID Middleware:** FastAPI middleware generates UUIDv4 if `X-Correlation-Id` header missing, echoes in all responses and logs.
- **Structured Logging:** Use `structlog` for consistent JSON formatting across all services.
- **No PII/PHI:** Mask sensitive data in logs; use data classification labels to drive masking policies.

### 5.2 Metrics & Golden Signals

- **RED + USE** per service; standard dashboards templatized.
- SLO error budget alerts wired to on-call rotations.

### 5.3 Tracing

- W3C trace-context; 100% sampling for P0 flows in staging; 5–10% in prod unless incident.

### 5.4 Health Probes

- `/healthz` (liveness), `/readyz` (readiness) with dependency checks.

### 5.5 Runbooks

- Each P0 alert has a runbook with: symptoms, probable causes, cmd snippets, rollback steps, comms template.

### 5.6 Acceptance Criteria

- Dashboards + alerts live before prod enablement.
- Game day run completed for new P0 service.

---

## 6) Release & Change Management PRD

**Owner:** Release Engineering\
**Scope:** Branching, CI/CD, feature flags, migrations, deprecation.

### 6.1 Branching & CI Stages

- Trunk-based with short-lived feature branches.
- CI stages: lint/type → unit → contract → integration → coverage gate → perf smoke → security.

### 6.2 Deployments

- Progressive rollout (canary or blue/green) with automated rollback on SLO breach.
- Feature flags for risky changes.

### 6.3 Schema/Data Migrations

- Backward-compatible first; dual-read/write windows documented; cutover scripts versioned.

### 6.4 Deprecation & Migration Playbook

- **Announce:** open an ADR/PRD update, add changelog entry, and publish a `/docs/migrations/<version>.md` guide.
- **Signal:** ship `X-Deprecation: <ISO-date>; info=<URL>` header + docs banner. Dual-run new behavior behind a feature flag (≥2 weeks in staging, ≥90 days for external breaking changes).
- **Instrument:** add metrics/logs to monitor adoption; ensure rollback plan in runbook.
- **Sunset:** remove deprecated behavior after the sunset date; update docs and release notes accordingly.

### 6.5 Acceptance Criteria

- Release checklist signed; rollback proven; change ticket links embedded in release notes.
- ✅ Breaking-change PRs include migration notes, sunset date, and flag plan.
- ✅ Deprecation headers appear in staging prior to production rollout.

---

## 7) QA / Test Strategy PRD

**Owner:** QA Guild\
**Scope:** Testing philosophy, types, data, and CI gates.

> Canonical guidance lives in **qa_testing_standard_prd_v_1.md** (QA & Testing Standard v1.0). This section highlights API-program specifics layered on top of that standard.

### 7.1 Philosophy

- **Shift-left, data-centric, contract-first.** Tests are code-owned and PR-blocking.

### 7.2 Test Types & Minimums

- **Unit** (critical paths)
- **Contract** (OpenAPI compliance)
- **Integration** (e2e across services)
- **Data Quality** (Great Expectations)
- **Parity/Golden** (median haversine ≤ 1.0 mile for NZR)
- **Property-based** (Hypothesis for geo math)
- **Performance** (Locust p95 budgets)
- **Security** (auth/RBAC/PII)

### 7.3 Test Data & Fixtures

- Small, deterministic fixtures versioned by vintage; large datasets fetched via `make` with checksums.

### 7.4 CI Gates

- P0/P1 tests 100% pass; coverage ≥ 85% on critical modules.
- Contract drift blocks merge.
- Vintage lock enforced.
- Perf budget enforced.

### 7.5 Directory & Matrix

```
/tests/{unit,integration,contract,data_quality,perf,_golden,_fixtures,_schemas}
/tests/config/test_matrix.yaml
```

### 7.6 Contract Test Patterns

- **Tooling:** Schemathesis drives OpenAPI-based tests in CI; project-specific pytest suites cover domain goldens/invariants.
- **Coverage:** Every public route must have at least one happy-path (2xx), validation error (4xx), and auth (401/403) test.
- **Docs parity:** Examples in docs/Postman must execute as tests against the mock/sandbox environment during CI.
- **CI integration:** Contract tests run on every PR and in nightly builds; failures block merge.

**Acceptance Criteria**

- ✅ Schemathesis coverage spans all public endpoints in CI.
- ✅ Documentation examples execute successfully against the mock during CI.

---

## 8) Docs & Developer Experience (DX) PRD

**Owner:** DevRel + Platform Docs\
**Scope:** What “good docs” mean and how we ship them.

### 8.1 Required Artifacts per API

- Quickstart in <5 minutes; runnable curl/HTTPie + SDK snippets.
- End-to-end example; error catalog; Postman collection.

### 8.2 Sandbox & Mocks

- Public mock server generated from OpenAPI; deterministic seeds matching vintage.

### 8.3 Changelogs & Deprecation Comms

- Human-readable changelog on every tag; email/webhooks for breaking changes.

### 8.4 Acceptance Criteria

- Docs build green in CI; examples validated against sandbox on every PR.

---

## 9) Cost / FinOps PRD (Optional but Recommended)

**Owner:** FinOps + Platform\
**Scope:** Unit economics, cost budgets, optimization.

### 9.1 Budgets & Attribution

- Cost per request and per tenant measured and reported weekly.
- Tagging policy for all resources.

### 9.2 Optimization Playbooks

- Caching tiers, query optimization, data pruning, right-sizing.
- Perf tests tie to cost deltas.

### 9.3 Acceptance Criteria

- New endpoint includes cost estimate and caching plan; alert on >20% MoM cost drift.

---

## Cross-References & Where It’s Mentioned

- **SSOT OpenAPI** referenced in: API Standards (§1.6), QA (§7.2), Release Mgmt (§6.1), Docs/DX (§8.2).
- **Vintages/Provenance** referenced in: Data Governance (§4), API Standards (§1.5), QA (§7.4), Observability (§5.1).

## Change Control

- Changes via PR; label with `global-prd` + affected section; Architecture Board + API Guild approval for breaking changes.

## Acceptance for this Pack

- All sections present with owners, scope, acceptance criteria.
- Linked from repo README.
- CI jobs created for contract lint, vintage lock, and docs validation.

\
Appendix — External References

- See **API Test & CI Reference Pack (v1.0)** for:
  - Repo layout
  - CI skeleton (GitHub Actions)
  - Minimal vintage lock script
  - Spectral lint config (OpenAPI excerpt)
  - Test matrix YAML for Cursor

---

## Appendix F — Makefile (targets used by CI)

```makefile
.PHONY: setup lint type contract unit integration coverage perf_smoke fetch-fixtures verify-checksums

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

lint:
	ruff check .
	taplo fmt --check || true
	npx @stoplight/spectral-cli lint api-contracts/openapi.yaml --fail-severity=warn

type:
	mypy .

contract:
	pytest tests/contract -q

unit:
	pytest tests/unit -q

integration:
	pytest tests/integration -q

coverage:
	pytest tests -q --cov=src --cov-fail-under=85

perf_smoke:
	locust -f tests/perf/loc
```
