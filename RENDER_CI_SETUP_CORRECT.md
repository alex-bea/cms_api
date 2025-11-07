# Render CI/CD Setup - One-Off Jobs (Correct Implementation) ✅

**Date:** 2025-10-22  
**Status:** Implementation Complete - Render One-Off Jobs API  
**Cost:** ~$0.01 per migration run (per-second billing)

## Implementation Overview

This implementation uses **Render One-Off Jobs API** for migrations:
- ✅ Zero-downtime deployments (migrations run AFTER deploy)
- ✅ Per-second billing (not $7/month Background Worker)
- ✅ Uses service's latest successful build artifact + environment
- ✅ Backward-compatible migrations (expand/contract pattern)

> **Platform note:** Render only accepts `linux/amd64` Docker images. Any manual build (local or CI) must include `docker build --platform linux/amd64 ...` to generate a compatible artifact.

## Architecture

### Deployment Flow

```
1. Build & Push Image (SHA-tagged) → GHCR
2. Deploy to Render (Deploy API with imageUrl)
3. Run Migrations (One-Off Job API)
4. Health Check Validation
```

**Key Insight:** One-Off Jobs don't require creating a permanent service - they're created on-demand and billed per-second only while running!

## Prerequisites (Step 0)

Verify these are in place:

- ✅ API is image-backed Web Service (pulls from GHCR)
- ✅ Health check path configured (e.g., `/health`)
- ✅ Service can pull image (public or Registry Credential with PAT `read:packages`)
- ✅ Deploy Hook URL created
- ✅ Render API key created
- ✅ (Optional) Build Filters / Root Dir set for API changes only

## One-Time Setup

### Step 1: Configure GitHub Secrets

**In GitHub Repository → Settings → Secrets and variables → Actions:**

Add these 3 secrets:

#### `RENDER_DEPLOY_HOOK`
- Render Dashboard → Your Service → Settings → Deploy Hook
- Click "Create Deploy Hook"
- Copy the URL

#### `RENDER_API_KEY`
- Render Dashboard → Account Settings → API Keys
- Click "Create API Key"
- Name: "GitHub Actions CI/CD"
- Copy the key (shown once only!)

#### `RENDER_SERVICE_ID`
- Your Render service URL: `https://dashboard.render.com/web/srv-XXXXX`
- Copy the `srv-XXXXX` part

#### `SNAPSHOT_DATABASE_URL`
- Connection string for the staging snapshot registry (read-only credentials are sufficient).
- Used by the GitHub snapshot-parity job to compare RVU vs GPCI release suffixes before triggering MPFS workflows.

### Step 2: Verify Database Environment

**In Render Dashboard → Your Service → Environment:**

Ensure `DATABASE_URL` is set:
- Add from database (if using Render PostgreSQL)
- OR manually set to your external database URL

### Step 3: Test the Pipeline

```bash
# Create a test tag
git tag v0.0.1-test
git push origin v0.0.1-test

# Watch the pipeline
# GitHub: Actions tab → "Build and Deploy to Render"
# Render: Dashboard → Events tab + Jobs page
```

## Deployment Flow (Detailed)

### Step 1: Build & Push Image

```yaml
# Builds multi-stage Docker image for linux/amd64
# Tags with commit SHA: ghcr.io/alex-bea/cms-api:abc123
# Pushes to GitHub Container Registry
```

### Step 2: Deploy with Image Tag

```bash
# Trigger Deploy API with explicit imageUrl (prefer underscore name or pinned digest)
IMAGE_TAG="ghcr.io/alex-bea/cms_api:${COMMIT_SHA}"
curl -X POST "https://api.render.com/v1/services/${SERVICE_ID}/deploys" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"imageUrl\": \"${IMAGE_TAG}\"}"
```

**Why Deploy API?** Eliminates ambiguity with hooks and lets you verify the resolved image post-deploy.

### Step 3: Run Migrations (One-Off Job)

```bash
# Creates one-off job with alembic upgrade head
curl -X POST "https://api.render.com/v1/services/${SERVICE_ID}/jobs" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"startCommand":"alembic upgrade head"}'
```

**What happens:**
- Render creates a temporary job instance
- Uses your service's latest successful build artifact
- Inherits all environment variables (including DATABASE_URL)
- Runs `alembic upgrade head`
- Job is destroyed after completion
- **Billed per-second** only while running

### Step 4: Poll Job Status

```bash
# Poll until succeeded or failed
curl "https://api.render.com/v1/jobs/${JOB_ID}" \
  -H "Authorization: Bearer ${API_KEY}"
```

**Statuses:**
- `running` - Job in progress
- `succeeded` - Migrations completed successfully ✅
- `failed` - Migrations failed (check logs) ❌

## Zero-Downtime Pattern

### Why Migrations Run AFTER Deploy

1. **Old instances** continue running with old code
2. **New deployment** starts with new code
3. **Migrations run** against database
4. **Health checks** validate new instances
5. **Traffic shifts** to new instances only if healthy

**Requirement:** Migrations must be backward-compatible (expand/contract):
- ✅ Add new columns (nullable or with defaults)
- ✅ Add new tables
- ✅ Add new indexes (CONCURRENTLY)
- ❌ Drop columns (requires multi-step migration)
- ❌ Rename columns (requires multi-step migration)

### When to Use Pre-Deploy Migrations

If you need a breaking schema change:
- **Option A:** Use multi-step migrations (expand/contract)
- **Option B:** Use `preDeployCommand` for that specific release
- **Option C:** Run migrations in CI before Deploy Hook

See `prds/RUN-database-migrations-prd-v1.0.md` for detailed patterns.

## Cost Breakdown

### One-Off Jobs Billing

**Per-second rate** based on instance type:
- Starter: ~$0.00002/second
- Standard: ~$0.00006/second

**Example:**
- Migration takes 30 seconds
- Starter instance: 30 × $0.00002 = **$0.0006** (~$0.02/month for daily deploys)
- Standard instance: 30 × $0.00006 = **$0.0018** (~$0.05/month for daily deploys)

**Compare to Background Worker:**
- $7/month for dedicated service
- **Savings: $6.95-6.98/month** using One-Off Jobs

### Optional: Use Cheaper Instance for Migrations

Override plan for migration jobs:

```bash
curl -X POST "https://api.render.com/v1/services/${SERVICE_ID}/jobs" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "startCommand":"alembic upgrade head",
    "planId":"plan-srv-006"
  }'
```

**Note:** Only use cheaper plans if migration memory usage allows.

## Troubleshooting

### Migration Job Fails

**Check logs:**
1. Render Dashboard → Jobs page
2. Find your job by start time
3. View logs for error details

**Common issues:**
- `alembic.util.exc.CommandError` - Check alembic.ini configuration
- `psycopg2.OperationalError` - Check DATABASE_URL is correct
- `ModuleNotFoundError` - Ensure dependencies are in Docker image

**Solution:**
```bash
# Test locally first
export DATABASE_URL="<render_database_url>"
alembic upgrade head
```

### Deploy Hook Returns 404

**Check:**
1. Deploy Hook URL is correct
2. Service is active in Render
3. Deploy Hook hasn't been regenerated

**Fix:**
```bash
# Regenerate in Render Dashboard
# Update RENDER_DEPLOY_HOOK secret in GitHub
```

### One-Off Job Not Found

**Error:** `"message":"not found: service: ..."`

**Cause:** Using wrong API endpoint or service ID

**Fix:**
- Verify `RENDER_SERVICE_ID` matches your service (srv-XXXXX)
- Use `/jobs` endpoint (not `/jobs/run-migrations/runs`)

### Image Pull Fails

**Error:** `Failed to pull image`

**Solutions:**
1. Verify image exists in GHCR: `docker pull ghcr.io/alex-bea/cms_api:latest`
2. Check image visibility (public or Registry Credential configured)
3. Verify PAT has `read:packages` scope
4. Check image URL in Render service settings

## Rollback Procedure

### If Migration Fails

1. **Investigate:** Check job logs in Render Dashboard
2. **Fix:** Correct the migration issue
3. **Retry:** Push a new tag to trigger redeployment

**Previous deployment remains active** - no downtime from failed migration!

### If Deployment Fails

1. **Render automatically** keeps previous deployment running
2. **Check:** Health check logs for failure reason
3. **Fix:** Correct the issue
4. **Redeploy:** Create new tag

### Manual Rollback

```bash
# Option 1: Redeploy previous version from Render Dashboard
# Dashboard → Deploys → Select previous deploy → Redeploy

# Option 2: Deploy previous Git tag
git tag v1.0.0-rollback  # On previous commit
git push origin v1.0.0-rollback
```

## Policy Compliance

### ✅ STD-database-platform-prd-v1.0.md §3
- Migrations-first approach (runs before traffic shifts)
- Migrations via Render Job/CI (One-Off Jobs API)
- No app startup DDL

### ✅ PRD-render-hosting-prd-v1.0.md §3
- Image-based deployment (zero Render build minutes)
- CI-controlled deploys on tags only
- Zero-downtime pattern

### ✅ RUN-database-migrations-prd-v1.0.md §6
- Migrations run via Render Job API
- Fail-fast on migration errors
- Audit trail in Render logs

## Files Modified

### `.github/workflows/deploy.yml`
- Added One-Off Job API call after deployment
- URL-encodes image tag for Deploy Hook
- Polls job status until completion
- Fails pipeline if migrations fail

### `render.yaml`
- Removed `preDeployCommand` (was incorrect approach)
- Added note about One-Off Jobs for migrations
- Environment variables configured

### Documentation
- Created comprehensive setup guide
- Documented zero-downtime pattern
- Added troubleshooting section

## Next Steps

1. ✅ Configure GitHub secrets (Step 1)
2. ✅ Verify database environment (Step 2)
3. ✅ Test with v0.0.1-test tag (Step 3)
4. 🎉 Start using automated zero-downtime deployments!

## Support

**Documentation:**
- This file - Complete setup and troubleshooting
- `.github/workflows/README.md` - CI/CD workflow details
- `prds/RUN-render-deployment-prd-v1.0.md` - Deployment runbook
- `prds/RUN-database-migrations-prd-v1.0.md` - Migration patterns

**Monitoring:**
- GitHub Actions: Repository → Actions tab
- Render Deployments: Dashboard → Events tab
- Render Jobs: Dashboard → Jobs page
- Health: `curl https://cms-pricing-api.onrender.com/health`

**API Reference:**
- [Create One-Off Job](https://api-docs.render.com/reference/create-job)
- [Retrieve Job](https://api-docs.render.com/reference/get-job)
- [Deploy Hooks](https://render.com/docs/deploy-hooks)

---

**Status:** ✅ Ready for deployment  
**Cost:** ~$0.01 per migration (vs $7/month for Background Worker)  
**Approach:** Zero-downtime with One-Off Jobs API
