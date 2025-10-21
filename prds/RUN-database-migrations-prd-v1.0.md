# Runbook: Database Migrations & Release Operations

**Status:** Stub v0.1 (in progress)  
**Owners:** Platform Engineering (DBA), Service Teams  
**Consumers:** Release Engineering, On-Call, Data Engineering  
**Change control:** PR review + Platform approval  
**Last updated:** 2025-10-21

**Cross-References**
- `prds/STD-database-platform-prd-v1.0.md` (policy requirements)  
- `prds/RUN-render-deployment-prd-v1.0.md` (Render deploy workflow)  
- `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (recent migration example)

---

## 0. Purpose
Provide actionable steps for authoring, testing, and executing database schema changes under the migrations-first standard. This runbook expands §3 of the database platform standard.

---

## 1. Prerequisites & Tooling
- [ ] Alembic configured and `alembic.ini` points to target environment  
- [ ] PgBouncer connection details documented  
- [ ] Access to prod-sized snapshot for dry-run  
- [ ] Change ticket/PR template (TBD)

*(TODO: enumerate command snippets, environment variables, safe sandbox instructions.)*

---

## 2. Authoring Workflow (Developer POV)
1. Generate revision using Alembic (`alembic revision --autogenerate` or manual).  
2. Review generated DDL for compatibility with migrations-first guidelines.  
3. Update seed scripts/fixtures if necessary.  
4. Run unit/integration tests.  
5. Update change ticket with description & risk.  

*(TODO: include checklist for RLS updates, naming convention lint, PHI registry impact.)*

---

## 3. Dry-Run Procedure (Prod-sized Snapshot)
- Acquire/refresh sanitized snapshot (see sanitization runbook).  
- Execute `alembic upgrade --sql` and capture DDL artefact.  
- Apply DDL against snapshot DB; record execution time and locking behavior.  
- Validate no unexpected differences (`\d`, `pg_indexes`, constraint dump).  
- Attach results (DDL script, logs, validation checklist) to PR/change ticket.  

*(TODO: script references, example commands, validation checklist automation.)*

---

## 4. Execution Pipeline (Render/CI Job)
- Migration job identity: `migrate` role.  
- Steps:
  1. Set session safety timeouts (`SET LOCAL lock_timeout = '5s'`, etc.).  
  2. Run `alembic upgrade head`.  
  3. Emit structured logs (start/end, revision IDs, duration).  
  4. Alert on failure (Slack/Incident).  
- Post-checks: verify `alembic_version`, run smoke queries, monitor metrics.  

*(TODO: provide GitHub Actions/Render Job YAML snippets; alert routing.)*

---

## 5. Fresh Database Bootstrap & Stamp Approval
### Option A — Migrations-first (required for prod)
- Initialize DB with `alembic upgrade head` from revision `None`.  
- Verify tables, indexes, constraints via automated script.  

### Option B — Models-first bootstrap (dev/stage only, w/ approval)
- Conditions, validation queries, and approval steps per standard §3.6.  
- Template log entry to document stamp decision.  

*(TODO: add ready-to-copy validation script and change ticket template.)*

---

## 6. Online Migration Patterns & Maintenance Windows
- Patterns: add/backfill/swap, dual-write triggers, index concurrently, partition backfill.  
- Maintenance window request template and approval matrix.  
- Backout plan checklist (PITR cutover, hot standby).  

*(TODO: collect examples, scripts for dual-write toggles.)*

---

## 7. Emergency Migration / Hotfix Procedure
- Incident commander authorization required.  
- Runbook steps for executing single migration with manual oversight.  
- Post-incident actions (log, audit, restore drill if necessary).  

*(TODO: integrate with incident-response standard.)*

---

## 8. Tooling & Automation Backlog
- `lint_schema_names.py` (enforce global naming) — TBD  
- CI check for `Base.metadata.create_all` usage  
- Migration duration telemetry export  

---

## 9. References & Templates
- Change ticket template (link TBD)  
- ADR-001 (migrations-first) and ADR-002 (stamp policy) — pending  
- Sample GitHub Actions workflow (pending)  

---

## Change Log
| Version | Date | Summary |
|---------|------|---------|
| v0.1 (stub) | 2025-10-21 | Initial scaffold created. Sections marked TODO for detailed procedures, scripts, and templates. |

