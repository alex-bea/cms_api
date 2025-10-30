# RUN: OpenAPI Docs Maintenance (v1.0)

**Status:** Draft v1.0  
**Owners:** Platform Engineering (API Enablement)  
**Consumers:** Feature teams, Partner Integrations, Docs/Support  
**Change control:** PR + Architecture review  
**Review cadence:** Quarterly or on API-breaking change  
**Cross-References:**  
- `prds/DOC-master-catalog-prd-v1.0.md` (master catalog)  
- `prds/STD-api-docs-prd-v1.0.md` (standards)  
- `prds/PRD-render-hosting-prd-v1.0.md` (deploy expectations)  
- `prds/RUN-render-deployment-prd-v1.0.md` (release process)  
- `prds/STD-data-architecture-prd-v1.0.md` & `STD-api-architecture-prd-v1.0.md`

---

## 1. Goal & Non-Goals

**Goal.** Keep cms_pricing OpenAPI documentation continuously accurate, accessible, and versioned alongside the application so internal/external consumers can self-serve API information.

**Non-Goals.**
- Writing endpoint-specific business logic or schema definitions (handled in code + standards).
- Partner-specific onboarding materials (handled by Solutions Engineering).

---

## 2. Source of Truth

- `/openapi.json` served by FastAPI (`cms_pricing/main.py`) is the canonical schema.  
- Documentation **must** be generated from running code; manual edits to JSON are prohibited.  
- Settings:
  - `settings.debug=True` exposes docs locally (`/docs`, `/openapi.json`).  
  - Production environment keeps docs disabled publicly; CI uses internal access.  
- Store generated specs under `docs/api/openapi.json` in git for review and downstream publishing.

### 2.1 Schema Ownership & Governance

**Schema Field Governance:**
- Schema definitions are governed by **`STD-api-architecture-prd-v1.0.md`** for naming conventions, data types, and versioning policies.
- All schema changes require review by:
  - **Platform Engineering** (API Enablement) - technical compliance
  - **API Domain Owner** - business logic and semantics
- Schema review checklist:
  - ✓ Naming conventions follow STD-api-architecture standards
  - ✓ No duplicate field definitions across endpoints
  - ✓ Schema drift is detected and documented
  - ✓ PRD cross-references are updated (see §4.5)

**Schema Field Provenance:**
- Long-term: Add OpenAPI extensions or YAML comments for field-level ownership:
  - `x-source-prd`: Reference to PRD document (e.g., `STD-...-v1.0.md §3.2`)
  - `x-owner`: Technical/business owner (e.g., "Platform Eng / API Team")
- Short-term: Lint rule requires schema fields to document PRD links in description.

---

## 3. Publishing Workflow

| Trigger | Action | Owner |
|---------|--------|-------|
| Endpoint or schema change (routers, schemas, middleware) | 1. `uvicorn cms_pricing.main:app --host 0.0.0.0 --port 8000`  
2. `curl -H "X-API-Key: <dev key>" http://localhost:8000/openapi.json > docs/api/openapi.json`  
3. Regenerate Markdown endpoint table (`docs/api/endpoints.md`) via `make docs-api`  
4. Run `make lint-openapi` (Spectral) | Authoring engineer |
| Release candidate build | Bundle HTML: `npx redoc-cli bundle docs/api/openapi.json -o docs/api/index.html` | Platform Eng |
| Deploy to Render | Attach latest spec artifacts to release tag; ensure Render static site (if used) updated; verify required curated datasets (e.g., RVU) were published with non-zero record counts before propagating docs | Release manager |

**Automation recommendations**
- Add `make docs-api` target to regenerate JSON + HTML + Markdown endpoint table (writes to `docs/api/endpoints.md`).  
- CI job verifies spec freshness (git diff) when routers/schemas change and fails if `docs/api/endpoints.md` drifts from the regenerated spec.

---

## 4. Documentation Content Requirements

Every published package (JSON/HTML/PDF) must include or link to:
1. **Overview:** API purpose, supported environments (dev/staging/prod URLs), auth scheme (`X-API-Key`), rate-limit policy.  
2. **Versioning:** Semantic or git-tag reference plus change history summary.  
3. **Endpoint grouping:** Plans, Pricing, Geography, Dataset (MPFS/OPPS/RVU), Trace/Health.  
4. **Examples:** Copy-pastable `curl` + request/response JSON for success and common errors (HTTP 4xx/5xx).  
5. **Schemas:** Reference OpenAPI components with descriptions, enums, formats, and sample values; link to relevant PRDs for domain meaning.  
6. **Observability + Ops:** `/metrics`, `/health`, `/readyz`, `/trace/*` availability and usage guidance.  
7. **Testing hooks:** Sandbox keys, pointers to replay endpoints, and how to debug via traces.  
8. **Accessibility:** Provide HTML + downloadable PDF/text; ensure color-safe diagrams where applicable.

---

## 5. CI, Validation & Linting

- **Spectral** (or equivalent) lint must pass before merge. Example config: `.spectral.yaml` referencing Stoplight rules + custom checks for descriptions, tags, and operationId naming.  
- Unit tests should include smoke tests that hit `/openapi.json` to detect runtime errors.  
- CI verifies that `docs/api/openapi.json` matches the live endpoint by comparing `curl` output against the committed file.  
- Optional: integrate `schemathesis` or similar contract tests to validate responses at runtime.

---

## 6. Hosting & Distribution

- **Render primary:** Serve Docs via Render static site or embed within existing dashboard; tie deploy to application release.  
- **Artifacts:** Always store JSON + HTML within the repo to support offline distribution.  
- **Environments:** Document base URLs and headers for dev, staging, prod; highlight any feature flags or preview endpoints.  
- **Access control:** Redact internal-only endpoints (e.g., `/trace/replay`) from public bundles if required; keep internal PDF variant with full surface area.

---

## 7. Versioning & Change Management

- Adopt semantic versioning for the public API (e.g., `v1`). Breaking changes require bump + changelog entry.  
- Each Render deploy tag must reference the spec SHA and include release notes summarizing endpoint updates.  
- Maintain `CHANGELOG.md` entries for doc updates (e.g., "Docs: regenerated OpenAPI spec for new /pricing/compare payload").  
- Ensure backwards compatibility commitments are honored; document deprecation timelines.

### 7.1 Audit & Drift Recovery

**Quarterly Audit Process:**
- **Frequency:** Every quarter (first business Monday)
- **Owners:** Platform Engineering (API Enablement)
- **Scope:** Freshness, completeness, accuracy

**Audit Checklist:**
1. ✓ Spec Freshness:
   - Compare `docs/api/openapi.json` with live `/openapi.json` endpoint
   - Git diff of spec file since last audit
   - Verify all router changes are reflected in spec
2. ✓ Completeness:
   - Scan `cms_pricing/routers/*.py` for undocumented endpoints
   - Check for missing or outdated examples
   - Validate PRD cross-references still exist
3. ✓ Drift Detection:
   - Automated: GitHub Action compares routers vs spec on every commit
   - Manual: Quarterly audit flags stale examples or undocumented endpoints
   - Remediation: Generate corrected spec within 1 business day

**Automated Drift Detection:**
- GitHub Action (`.github/workflows/audit-openapi.yml`):
  - Runs on every PR that touches routers or schemas
  - Compares committed spec against live endpoint
  - Fails CI if drift > 24 hours
  - Sends alert to Platform Eng + API owners on failure

**Drift Recovery:**
- Detection triggers Severity 2 incident
- Platform Eng regenerates spec within 1 business day
- Update `CHANGELOG.md` with audit findings
- Notify partners if external-facing spec was incorrect

---

## 8. External Distribution Policy

### 8.1 Partner-Facing Publishing

**External Partner Documentation:**
- **Hosting:** Render-hosted static site (primary) + downloadable JSON/PDF variants
- **Access:** Public-facing HTML with partner authentication; downloadable artifacts require API key
- **Update Frequency:** Synchronized with application releases; major versions announced via release notes

**Internal vs External Surface Area:**
- **External (Partner) Variant:**
  - Excludes: `/trace/replay`, `/admin/*`, internal-only debug endpoints
  - Includes: All public pricing, geography, plan management endpoints
  - Schema fields marked `x-internal: true` are redacted from partner docs
- **Internal (Full) Variant:**
  - Includes all endpoints including debug/replay/trace capabilities
  - Required for Platform Engineering and on-call use
  - Stored in internal docs portal or GitHub private repo

### 8.2 Redaction & QA Process

**Pre-Distribution Checklist:**
1. ✓ Run automated redaction script to remove internal-only endpoints
2. ✓ Manual QA checklist:
   - Verify no internal endpoints exposed
   - Confirm examples are production-ready (no dev/test URLs)
   - Validate watermarking (DRAFT vs PRODUCTION)
3. ✓ Architecture review approval (Platform Eng + Solutions Eng)
4. ✓ Watermark artifacts with version, build date, and certification status

**Redaction Scripts:**
- Store in `tools/redact_openapi.py` for automated filtering
- CI job validates before release tag creation
- Manual override possible for emergency patches (requires Platform approval)

### 8.3 Partner Notifications

**Release Notification:**
- Automatic: New releases trigger email to partner technical contacts
- Manual: Major version changes require Solutions Engineering pre-approval
- Distribution: Link to Render-hosted HTML + downloadable JSON/PDF
- Content: Include changelog summary, breaking change notices, migration guides

---

## 9. Ownership & Escalation

- **Primary:** Platform Engineering (API Enablement).  
- **Secondary:** Tech Writers / Solutions for partner packaging.  
- **Escalation path:** Notify API owners and SRE when spec changes fail linting or when docs lag >1 release.  
- **Incident response:** If published docs drift from live behavior, treat as Severity 2 (impacting integrators) and remediate within 1 business day by regenerating or issuing corrections.

---

## Appendix

### A.1 Tooling Reference

**Required Tools (install via npm/brew):**
- `redoc-cli` - Generate interactive HTML docs
- `spectral-cli` - OpenAPI linting and validation
- `openapi-generator-cli` - Generate client SDKs (optional)
- `curl` / `jq` - Command-line utilities

**Installation:**
```bash
# via npm
npm install -g @stoplight/spectral-cli
npm install -g redoc-cli

# via brew (macOS)
brew install spectral-cli redoc-cli

# via requirements-dev.txt
pip install openapi-spec-validator
```

**Make Targets (`.make.docs` or `Makefile`):**
- `make docs-api` - Regenerate JSON + HTML + Markdown endpoint table
- `make lint-openapi` - Run Spectral linting on `docs/api/openapi.json`
- `make audit-openapi` - Compare spec against live endpoint (CI use)
- `make redact-openapi` - Remove internal-only endpoints for partner distribution

### A.2 Common Commands

**Generate OpenAPI Spec:**
```bash
# From running application
curl -H "X-API-Key: $DEV_API_KEY" http://localhost:8000/openapi.json > docs/api/openapi.json

# Validate schema
npx @stoplight/spectral-cli lint docs/api/openapi.json
```

**Generate HTML Documentation:**
```bash
# Single-file HTML (portable)
npx redoc-cli bundle docs/api/openapi.json -o docs/api/index.html

# With custom theme
npx redoc-cli bundle docs/api/openapi.json -o docs/api/index.html --theme.openapi.fontSize.body=14
```

**Audit Spec Freshness:**
```bash
# Compare committed spec vs live endpoint
diff <(cat docs/api/openapi.json) <(curl http://localhost:8000/openapi.json)

# Drift detection (CI job)
tools/audit_openapi_drift.py --api-url http://localhost:8000
```

### A.3 Environment URLs

| Environment | Base URL | Docs URL | Auth | Notes |
|-------------|----------|----------|------|-------|
| Development | `http://localhost:8000` | `/docs` | Dev API key | Local development |
| Staging | `https://cms-pricing-api-staging.onrender.com` | `/docs` | Staging API key | Pre-production testing |
| Production | `https://cms-pricing-api.onrender.com` | Internal only | Prod API key | Production (docs disabled) |

**Configuration:**
- Use environment variables (`API_BASE_URL_PROD`, `API_BASE_URL_STAGING`) in examples
- Update `.spectral.yaml` to validate URLs against actual environment configs

### A.4 Related Files

**Code Locations:**
- Routers: `cms_pricing/routers/*.py`
- Schemas: `cms_pricing/schemas/*.py`
- Main App: `cms_pricing/main.py`
- Data Flow Diagram: `docs/architecture/data_flow.mmd`

**Standards & PRDs:**
- `prds/STD-api-architecture-prd-v1.0.md` - Schema governance
- `prds/STD-api-docs-prd-v1.0.md` - Documentation standards
- `prds/PRD-render-hosting-prd-v1.0.md` - Deployment expectations

### A.5 FAQ

- *Should we publish `/trace/replay`?*  
  Default internal-only; include in partner docs only if contractually required.

- *How are examples maintained?*  
  Prefer auto-generated examples (e.g., `openapi-python-client`) or verified manual snippets stored under `docs/api/examples/*.json`. Examples should be production-ready (no dev URLs, realistic test data).

- *Where do partners get doc updates?*  
  Link to Render-hosted HTML plus downloadable JSON/PDF; notify via release notes. Major version changes require Solutions Engineering pre-approval.

- *How to generate curl examples?*  
  Examples can be:
  - Hand-written with verified execution (stored in `docs/api/examples/`)
  - Auto-generated from OpenAPI spec (recommended for consistency)
  - Tool-generated using `openapi-generator` or similar

- *What's the versioning strategy?*  
  Semantic versioning (e.g., `v1`, `v2`) with breaking changes requiring major version bump. Each release tag references spec SHA and includes changelog entry.
