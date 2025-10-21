# Render Hosting & Release Policy

**Status:** Draft v1.0  
**Owners:** Platform Engineering, SRE  
**Consumers:** Engineering, Release Management, On-Call  
**Change control:** ADR + Architecture review  
**Review cadence:** Quarterly or on Render pricing/feature changes

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog
- **RUN-global-operations-prd-v1.0.md:** Operational runbooks
- **STD-observability-monitoring-prd-v1.0.md:** Monitoring standards
- **Companion Guide:** `.cursor/RENDER_DEPLOYMENT_GUIDE.md` (step-by-step implementation)

**Last Updated:** 2025-10-21  
**Verified Against:** Render platform capabilities as of 2025-10

---

## 1. Goal & Non-Goals

**Goal.** Standardize hosting, builds, and releases on Render to:
- Minimize build pipeline minutes and hosting costs
- Reduce deploy risk through deterministic artifacts and rehearsable rollbacks
- Meet availability, security, and operability baselines for CMS Pricing services

**Non-Goals.**
- Step-by-step deployment instructions (see companion guide: `.cursor/RENDER_DEPLOYMENT_GUIDE.md`)
- Deep Terraform/IaC specification (future work once infra team standardizes Render automation)

---

## 2. Users & Environments

**Primary users:** Engineers shipping features, release managers, on-call responders

**Environments:** Separate Render services & PostgreSQL instances for **dev**, **staging**, **prod**

**Promotion path:** dev → staging (automatic on merge/main), staging → prod (manual approval gated on checks passing)

**Database tiers:**
- Dev: Free tier or Starter (may expire after 90 days on free)
- Staging: Starter tier minimum ($7/month, 7-day backups)
- Prod: Starter or Standard ($20/month, 14-day backups)

---

## 3. Architecture & Hosting Decisions

### 3.1 Service Packaging

**Primary Strategy:** ✅ **Prebuilt Docker images published by CI**
- CI builds image, tags with git SHA
- Pushes to GitHub Container Registry (`ghcr.io/<org>/cms-pricing:<sha>`)
- Render pulls the image (zero on-platform build minutes)

**Fallback:** Render-native build with cache-aware Dockerfile
- Base layer: Dependencies (stable, cached)
- App layer: Application code (changes frequently)
- Pinned hashes in requirements.txt

### 3.2 Deploy Triggers

**Render auto-deploy:** ❌ **DISABLED**

**Deployments initiated via:** CI job on tagged commits (e.g., `release/*`)

**Monorepo change filters:** CI deploy jobs only fire when:
- `cms_pricing/**` changes
- `infra/**` changes  
- `.cursor/RENDER_DEPLOYMENT_GUIDE.md` changes

### 3.3 Health Checks

**Required:** `/health` endpoint
- Must return HTTP 200 within 1s
- Must validate DB connectivity
- Render HTTP health check enabled (mandatory gate)

**Implementation:**
```python
@app.get("/health")
async def health():
    # Test DB connection
    with engine.connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "healthy"}
```

### 3.4 Secrets Policy

**Render environment variables:** For runtime secrets (DATABASE_URL, API_KEYS)

**Local development:** `.env` + `direnv`

**Never:**
- Export long-lived secrets into shell rc files
- Commit secrets to git history
- Hardcode credentials in code

### 3.5 Image Registry

**Registry:** GitHub Container Registry  
**Path:** `ghcr.io/<org>/cms-pricing:<sha>`  
**Retention:** 90 days for images  
**Tags:** Git SHA for immutability, also tag `latest` for dev

---

## 4. Build & Pipeline Minutes Policy

### 4.1 Primary Strategy (Zero Render Build Minutes)

**CI builds Docker image:**
1. GitHub Actions workflow triggered on push to main or release/*
2. Build multi-stage Dockerfile
3. Tag with git SHA
4. Push to GHCR
5. Trigger Render deploy pointing to image

**Benefits:**
- Zero on-platform build minutes (Render just pulls image)
- Faster deploys (image pre-built)
- Consistent across environments

### 4.2 Fallback: Render Native Build

**If image deploy not available:**

**Dockerfile optimization:**
```dockerfile
# Base layer (stable, cached)
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App layer (changes frequently)
COPY cms_pricing/ /app/cms_pricing/
WORKDIR /app
CMD ["uvicorn", "cms_pricing.main:app", "--host", "0.0.0.0", "--port", "$PORT"]
```

**Optional wheelhouse:**
```bash
# CI publishes wheels
pip wheel -r requirements.txt -w wheelhouse/

# Render build uses
pip install --no-index --find-links ./wheelhouse -r requirements.txt
```

### 4.3 Pre-Deploy Hook

**Keep empty** - No migrations, no asset uploads

**Why:**
- Avoid double work
- Prevent pipeline charges
- Migrations run as explicit Job (see §5.1)

### 4.4 Spend Guardrails

**Render account:**
- Spend limit enabled
- Alerts at 50% and 80% of monthly allocation
- Email + Slack notifications

**Cache hygiene:**
- Weekly CI job rebuilds base image with security patches
- Monitor build minute impact

---

## 5. Database & Migrations

### 5.1 Migration Execution

**Alembic runs as explicit Render Job:** `cms-pricing-migrate`

**Workflow:**
1. CI builds image
2. Trigger `cms-pricing-migrate` Job on Render
3. Job runs: `alembic upgrade head`
4. If successful, deploy application
5. If failed, abort deployment

**Benefits:**
- No inline migrations in app startup
- Prevents race conditions
- Explicit migration logs
- Timeout control

### 5.2 No Inline Migrations

**Prohibited:**
- ❌ Migrations in Render pre-deploy hook
- ❌ Migrations in application startup (main.py)

**Why:**
- Prevents race conditions (multiple app instances)
- Avoids cold-start bloat
- Explicit failure handling

### 5.3 Stamp Discipline

**`alembic stamp` only permitted when:**
- Schema is known to match a specific revision
- Documented in migration guide
- Never stamp 'head' on unknown database

**Prefer:** Forward upgrades (`alembic upgrade head`)

### 5.4 Database Roles & Least Privilege

**Roles:**
```sql
-- DDL for migrations
CREATE ROLE migrate NOINHERIT;

-- CRUD for application
CREATE ROLE app_rw NOINHERIT;

-- Read-only for analytics
CREATE ROLE ro NOINHERIT;
```

**Users:**
- `cms_migrate` - Used by migration Job
- `cms_app_rw` - Used by application
- `cms_ro` - Used for reporting/analytics

**Privileges:**
- `migrate`: CREATE, ALTER, DROP, REFERENCES, TRIGGER
- `app_rw`: INSERT, UPDATE, DELETE
- `ro`: SELECT only

**Default privileges configured** so new tables inherit grants.

### 5.5 Backups

**Primary:** Render automated backups
- Starter tier: Daily, 7-day retention
- Standard tier: Daily, 14-day backups
- Pro tier: Daily, 30-day backups

**Secondary:** Manual `pg_dump` before risky changes
```bash
pg_dump $DATABASE_URL > backup_pre_migration_$(date +%Y%m%d).sql
```

---

## 6. Observability & SLOs

### 6.1 Service SLO

**Target:** 99.9% monthly availability for `/api/*` endpoints

**Error budget:**
- Errors outside maintenance windows count against budget
- Planned maintenance announced 24h in advance

### 6.2 Signals Required Before Prod Deploy

**Must pass:**
1. Render health check green (all regions) for candidate build
2. Application logs emitting `db_healthcheck` durations:
   - p50 < 100ms
   - p95 < 500ms
3. Metrics exposed (application-level):
   - `gpci_requests_without_mac_total`
   - `gpci_duplicate_nk_violations`
   - HTTP request counters (rate, duration, errors)

**Metrics stack:**
- Application: Prometheus/StatsD
- Aggregation: Prometheus or Datadog
- Visualization: Grafana or Datadog dashboards

### 6.3 Dashboards & Alerts

**Render metrics dashboard:** Bookmarked for on-call

**External monitoring:**
- Prometheus or Datadog monitors for:
  - 500 error rate
  - Queue depth
  - Migration failures
  - Connection pool exhaustion

### 6.4 Log Retention

**Structured logs shipped to:**
- Logtail, Datadog, or CloudWatch
- 14-day retention minimum
- 90-day retention for audit/compliance

---

## 7. Rollback & Disaster Recovery

### 7.1 Application Rollback

**Process:**
1. Render dashboard → Deploys tab
2. Select previous successful image
3. Click "Redeploy"
4. Verify `/health` endpoint

**ETA:** < 5 minutes

### 7.2 Database Recovery

**Primary:** Restore Render snapshot
1. Render dashboard → Database → Backups
2. Select backup date
3. Click "Restore"
4. ETA: 10-15 minutes

**Secondary:** Alembic downgrade (only if reversible)
```bash
alembic downgrade -1
```

**Always:** Manual `pg_dump` before schema changes

### 7.3 Disaster Recovery

**RTO/RPO Assumptions:**
- Starter tier: RPO up to 24h (daily backups)
- RTO: 15-30 minutes (restore from backup)

**Cross-region replicas:** Evaluate when usage increases

**Runbook:** `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` contains post-migration validation

---

## 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Cache busting increases build times** | Higher pipeline spend | Keep dependency layers stable; monitor weekly build duration |
| **Long-running migrations block deploys** | Deployment delays | Run migrations as Jobs with timeout + backout plan; test in staging |
| **Secret leakage through shells** | Security breach | Enforce `.env` + `direnv`; periodic rotation |
| **Health check mismatch (app green, DB failing)** | False positives | `/health` must exercise DB; synthetic checks |

---

## 9. Acceptance Criteria (Ship Checklist)

**Before production deployment:**

- [ ] Render services created for dev/staging/prod with `/health` checks enabled
- [ ] Auto-deploy disabled; CI workflow deploys on tag + path filters
- [ ] Container image registry path documented
- [ ] Render configured to use image deploy (or cache-friendly native build)
- [ ] Pre-deploy hook empty
- [ ] Alembic migration Job defined, documented, and rehearsed
- [ ] Render spend limit + 50%/80% alerts enabled
- [ ] Secrets policy documented; local `.env` + `direnv` template shared
- [ ] No credentials in shell profiles
- [ ] PostgreSQL roles (`migrate`, `app_rw`, `ro`) created per environment
- [ ] Roles tied to services/jobs appropriately
- [ ] Rollback rehearsed in staging (redeploy prior image + DB snapshot restore)
- [ ] Rollback procedure documented
- [ ] Observability requirements met:
  - Metrics emitted (application-level)
  - Structured logs flowing
  - `/health` integrated with Render check
- [ ] Cross-references included to RUN guide and migration checklist

---

## 10. References

**Implementation Guides:**
- **RUN:** `.cursor/RENDER_DEPLOYMENT_GUIDE.md` (step-by-step deployment, 978 lines)
- **Migration Checklist:** `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (production ops)
- **Secrets & Roles:** `.cursor/RENDER_DEPLOYMENT_GUIDE.md` Part 6 (least-privilege setup)

**CI/CD:**
- **CI Workflows:** `.github/workflows/deploy.yml` (to be created per this PRD)
- **render.yaml:** Infrastructure as code template (see RENDER_DEPLOYMENT_GUIDE.md)

**Related Standards:**
- **STD-observability-monitoring-prd-v1.0.md:** Monitoring requirements
- **STD-api-security-and-auth-prd-v1.0.md:** Security standards
- **RUN-global-operations-prd-v1.0.md:** Operational procedures

**Migration Task:**
- **Task 67:** Deploy GPCI v1.3 to Render (github_tasks_plan.md, lines 1635-1774)

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| **1.0** | 2025-10-21 | Initial Render hosting policy. Defines service packaging (prebuilt images preferred), deploy triggers (CI-driven, auto-deploy disabled), database migration strategy (explicit Jobs, no inline), secrets management (.env + direnv locally, Render env vars for prod), least-privilege roles (migrate, app_rw, ro), observability requirements (99.9% SLO, metrics, logs), rollback procedures, cost management (spend limits, alerts). Based on learnings from GPCI v1.3 deployment preparation and Docker conflict resolution. Companion guide: RENDER_DEPLOYMENT_GUIDE.md. |

---

**End of PRD-render-hosting-prd-v1.0.md**

