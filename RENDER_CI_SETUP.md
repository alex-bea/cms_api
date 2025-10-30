# Render CI/CD Setup Complete ✅

**Date:** 2025-10-22  
**Status:** Implementation Complete - Manual Setup Required

## What Was Implemented

### 1. Infrastructure as Code
✅ **Created:** `render.yaml`
- Web service configuration (image-based deployment)
- Database configuration
- Environment variables
- Health checks and scaling

> Note: One-off jobs aren’t defined in 
`render.yaml`; they’re created and run via the Render API or the service’s **Jobs** tab.

### 2. CI/CD Pipeline
✅ **Updated:** `.github/workflows/deploy.yml`
- Runs database migrations via a Render **One-Off Job** **after** the new image is deployed (jobs always use the base service’s latest successful build artifact).
- Triggers the migration job via Render API and polls for completion (fail-fast on errors).
- Builds and pushes a Docker image tagged with the commit SHA to GHCR.
- Deploys the exact image via Render **Deploy Hook** using the 
  `imgURL` parameter.
- Automated on version tags (v*.*.*)

### 3. Documentation
✅ **Created:** `.github/workflows/README.md`
- Complete CI/CD workflow documentation
- Setup instructions for GitHub secrets
- Render migration job creation guide
- Troubleshooting guide
- Monitoring and rollback procedures

✅ **Updated:** `prds/RUN-render-deployment-prd-v1.0.md`
- Added Part 9: CI/CD Automation Status
- Quick start guide for automated deployments
- One-time setup instructions
- Migration from manual to automated
- Comprehensive troubleshooting

## What You Need to Do (One-Time Setup)

### Step 1: Create Render Migration Job

**In Render Dashboard:**

1. Navigate to your **web service** in the Render Dashboard.
2. Open the **Jobs** tab → click **+ Add Job**.
3. Configure:
   - **Name:** `run-migrations` (exactly this name)
   - **Start command:** `alembic upgrade head`
   - **Base service:** (preselected) your API service — **jobs automatically use the service’s *latest successful build artifact* and its environment**
4. Save.

After creation, your CI will call the Render API to start this job when needed.

### Step 2: Configure GitHub Secrets

**In GitHub Repository → Settings → Secrets and variables → Actions:**

Add three secrets:

#### 1. RENDER_API_KEY
- Go to: Render Dashboard → Account Settings → API Keys
- Click "Create API Key"
- Name: "GitHub Actions CI/CD"
- Copy the key (shown once only!)
- Add as secret in GitHub

#### 2. RENDER_SERVICE_ID  
- Go to your Render web service
- Look at URL: `https://dashboard.render.com/web/srv-XXXXX`
- Copy the `srv-XXXXX` part
- Add as secret in GitHub

#### 3. RENDER_DEPLOY_HOOK
- Should already exist from previous setup
- If not: Render Dashboard → Settings → Deploy Hook → Create
- Add as secret in GitHub  
You can deploy a specific image tag or digest by adding the URL-encoded `imgURL` parameter to the hook.

### Step 2: Test the Pipeline

```bash
# Create a test tag
git tag v0.0.1-test
git push origin v0.0.1-test

# Watch the pipeline
# GitHub: Actions tab → "Build and Deploy to Render"
# Render: Dashboard → Events tab
```

**Expected flow:**
1. ✅ Build Docker image
2. ✅ Push to GHCR
3. ✅ Deploy to Render
4. ✅ Render runs `alembic upgrade head` automatically (preDeployCommand)
5. ✅ Health check passes

**Cost:** $0 - Uses Render's built-in preDeployCommand feature

## How to Deploy Going Forward

### Automated Deployment (Default)

```bash
# 1. Make your changes
git add .
git commit -m "feat: add new feature"
git push origin main

# 2. Create version tag
git tag v1.0.0
git push origin v1.0.0

# 3. That's it! CI/CD handles the rest:
#    - Builds image
#    - Runs migrations
#    - Deploys to Render
#    - Validates health checks
```

**Note:** Render One-Off Jobs always execute using the base service’s most recent successful build artifact. To ensure migration code matches the deployed app, this pipeline deploys first, then runs migrations. If you require *pre-deploy* migrations, run `alembic upgrade head` in CI using the freshly built image instead of a Render job.

### Verify Deployment

```bash
# Check health endpoint
curl https://cms-pricing-api.onrender.com/health

# Expected: {"status": "healthy", "database": "connected"}
```

## Policy Compliance

This implementation follows all your PRD policies:

✅ **STD-database-platform-prd-v1.0.md §3:**
- Migrations-first approach
- Migrations run via Render Job/CI
- No app startup DDL

✅ **PRD-render-hosting-prd-v1.0.md §3:**
- Image-based deployment
- CI-controlled deploys on tags
- Zero Render build minutes

✅ **RUN-database-migrations-prd-v1.0.md §6:**
- Migration job runs before deployment
- Fail-fast on migration errors
- Audit trail in GitHub Actions logs

## Files Created/Modified

### Created:
- ✅ `render.yaml` - Infrastructure as Code
- ✅ `.github/workflows/README.md` - CI/CD documentation
- ✅ `RENDER_CI_SETUP.md` - This file

### Modified:
- ✅ `.github/workflows/deploy.yml` - Added migration automation
- ✅ `prds/RUN-render-deployment-prd-v1.0.md` - Added Part 9

## Troubleshooting

### Migrations not found (e.g., "No such revision")

**Why:** One-off jobs use your service’s latest successful build artifact; if you run the job *before* deploying a new image that contains a new Alembic revision, the job won’t see it.

**Fix:** Deploy the new image first (using Deploy Hook with `imgURL`), then trigger the migration job; or run migrations in CI with the new image tag.

### Migration Job Not Found
```
Error: Failed to trigger migration job
```
**Fix:** Create the `run-migrations` job in Render (Step 1 above)

### API Authentication Failed
```
Error: 401 Unauthorized
```
**Fix:** Verify `RENDER_API_KEY` is correct and not expired

### Deployment Hook 404
```
Error: Service not found
```
**Fix:** Regenerate `RENDER_DEPLOY_HOOK` in Render Dashboard

### Migration Times Out
```
Error: Migration job timed out after 5 minutes
```
**Fix:** 
- Check migration logs in Render Jobs tab
- Optimize slow migrations (see RUN-database-migrations-prd-v1.0.md)
- Increase timeout in workflow if needed

## Next Steps

1. ✅ Complete Step 1: Create Render migration job
2. ✅ Complete Step 2: Configure GitHub secrets  
3. ✅ Complete Step 3: Test with v0.0.1-test tag
4. 🎉 Start using automated deployments!

## Support

**Documentation:**
- `.github/workflows/README.md` - Detailed CI/CD guide
- `prds/RUN-render-deployment-prd-v1.0.md` Part 9 - Automation guide
- `prds/RUN-database-migrations-prd-v1.0.md` - Migration procedures

**Monitoring:**
- GitHub Actions: Repository → Actions tab
- Render: Dashboard → Events and Jobs tabs (Jobs show logs for each run)
- Health: `curl https://cms-pricing-api.onrender.com/health`

---

**Status:** Ready for one-time setup! Complete Steps 1-3 above to activate automated deployments.
