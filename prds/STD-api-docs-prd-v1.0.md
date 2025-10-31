# API Documentation Standard (v1.0)

## 0. Overview
Defines the canonical requirements for producing, validating, and distributing API documentation for cms_pricing services. Ensures every API surface ships with accurate, accessible OpenAPI specs backed by code and supported by operational runbooks.

**Status:** Draft v1.0  
**Owners:** Platform Engineering (API Enablement)  
**Consumers:** Service teams, Docs, Partner Integrations  
**Change control:** ADR + Architecture review  
**Companion Docs:** `prds/RUN-openapi-docs-maintenance-v1.0.md`

**Cross-References:**  
- `prds/DOC-master-catalog-prd-v1.0.md` (master catalog)

## 1. Goals & Non-Goals
- **Goals**:  
  - Guarantee every public/internal API publishes a single source-of-truth specification.  
  - Standardize doc content, versioning, and accessibility so partners can self-serve.  
  - Tie documentation lifecycles to deployment processes and governance (Render, PRDs).
- **Non-Goals**:  
  - Endpoint-specific business logic.  
  - Partner onboarding playbooks.

## 2. Architecture & Lifecycle
- Documentation follows the same lifecycle as data products: design → implement → validate → publish → monitor.  
- Primary spec is generated from FastAPI routes (`cms_pricing/main.py`, `routers/**`).  
- Narrative overlays (Markdown, HTML) reference PRDs for deeper context.  
- Docs must be updated as part of release cadences (Render deployments, Alembic migrations).

## 3. Requirements
1. **Auto-generated spec**: `/openapi.json` generated from running app; stored under `docs/api/`.  
2. **Content checklist**:
   - Overview + auth + rate limits + environment URLs.  
   - Grouped endpoints with descriptions, tags, and operationIds (mirrored in `docs/api/endpoints.md`; see workflow in `prds/RUN-openapi-docs-maintenance-v1.0.md` §3).  
   - Request/response schemas documented via OpenAPI components.  
   - Provenance metadata documented wherever responses include dataset tracking (e.g., `datasets_used`, `trace_refs`), including field descriptions and standardized formats (`{dataset_id}:release:{release_id}`, `{dataset_id}:batch:{batch_id}`).  
   - Copy-pastable examples + common error payloads.  
   - Observability endpoints (`/metrics`, `/health`, `/trace`) and operational guidance.  
   - Versioning + deprecation notes.  
   - Accessibility requirements (HTML + downloadable format).  
3. **Traceability**: link each endpoint to code (GitHub path) and governing PRDs (e.g., `prds/SRC-locality.md`).  
4. **Security**: omit secrets; mark internal-only endpoints; describe auth headers and scopes.  
5. **Change log**: maintain `CHANGELOG.md` entries for doc-impacting changes.

## 4. Quality Gates
- **Linting**: Spectral/Stoplight rules verifying descriptions, tags, schema completeness.  
- **Drift detection**: CI compares committed spec vs live `/openapi.json`.  
- **Review**: PR reviewers ensure new/changed endpoints update both spec and narrative docs.  
- **Testing**: contract tests (schemathesis or similar) recommended for critical flows.

## 5. Operations & Hosting
- Docs must be deployable with the app (Render).  
- Provide static HTML (Redoc/Stoplight) + raw JSON; optional Markdown table for quick reference.  
- Keep environment matrices updated; staging/prod URLs must be explicit.  
- Follow `RUN-openapi-docs-maintenance-v1.0` for regeneration and incident response.

## 6. Compliance & Accessibility
- Meet WCAG-friendly color/contrast in published HTML/PDF.  
- Provide offline-friendly exports (PDF or Markdown).  
- Include contact/escalation info for corrections.  
- Ensure documentation displays data classifications consistent with PRDs (Public/Internal).

## 7. References
- `prds/RUN-openapi-docs-maintenance-v1.0.md`  
- `prds/PRD-render-hosting-prd-v1.0.md`, `prds/RUN-render-deployment-prd-v1.0.md`  
- `prds/STD-api-architecture-prd-v1.0.md`, `prds/STD-data-architecture-prd-v1.0.md`
