# CI/CD Workflows

This directory contains GitHub Actions workflows for automated CI/CD of the CMS Pricing API.

## Workflows

### `deploy.yml` - Build and Deploy to Render

Automated build and deployment pipeline following:
- **STD-database-platform-prd-v1.0.md** §3: Migrations-first approach
- **PRD-render-hosting-prd-v1.0.md** §3: Image-based deployment strategy
- **RUN-render-deployment-prd-v1.0.md** Part 8: CI/CD automation

**Triggers:**
- **Build:** Every push to `main` branch
- **Deploy:** Only on version tags (`v*.*.*`)

**Pipeline Steps:**
1. Build Docker image (multi-stage, production target)
2. Push to GitHub Container Registry (ghcr.io)
3. Tag with SHA, semver, and latest
4. Trigger Render deployment with specific image tag
5. **Run migrations via One-Off Job API** (after deployment)
6. Poll job status until completion

**Required Secrets:**
- `GITHUB_TOKEN` - Auto-provided by GitHub Actions
- `RENDER_DEPLOY_HOOK` - Webhook URL for Render deployment
- `RENDER_API_KEY` - API key for One-Off Jobs API
- `RENDER_SERVICE_ID` - Service ID for your Render web service

## Setup Instructions

### 1. Configure GitHub Secrets

Go to: Repository Settings → Secrets and variables → Actions

Add the following secrets:

#### `RENDER_DEPLOY_HOOK`
Already configured. To regenerate:
1. Render Dashboard → Your Web Service
2. Settings → Deploy Hook
3. Click "Create Deploy Hook"
4. Copy the URL

#### `RENDER_API_KEY`
1. Render Dashboard → Account Settings
2. API Keys tab
3. Create new API key
4. Copy the key (shown once)

#### `RENDER_SERVICE_ID`
1. Render Dashboard → Your Web Service
2. Copy service ID from URL: `https://dashboard.render.com/web/srv-XXXXX`
3. The ID is the `srv-XXXXX` part

### 2. Understand the One-Off Jobs Approach

**How it works:**
- After deployment, CI triggers a One-Off Job via API
- Job runs `alembic upgrade head` using your service's latest build artifact
- Job inherits all environment variables (including DATABASE_URL)
- **Billed per-second** only while running (~$0.01 per migration)
- No permanent Background Worker needed

**Reference:** `RENDER_CI_SETUP_CORRECT.md` for complete details

### 3. Deploy render.yaml Blueprint (Optional)

For infrastructure-as-code approach:

1. Render Dashboard → New → Blueprint
2. Connect to this repository
3. Select `render.yaml`
4. Review configuration
5. Create resources

**Note:** If you already have a database provisioned manually, you may need to adjust the `databases` section in `render.yaml` or remove it.

## Deployment Process

### Automated Deployment (Recommended)

1. Commit and push changes to `main`
2. Create and push a version tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. GitHub Actions will:
   - Build Docker image
   - Push to GHCR
   - Run database migrations
   - Deploy to Render
   - Wait for health checks

### Manual Deployment (Emergency)

If CI/CD fails, deploy manually:

1. **Run migrations first:**
   ```bash
   # Render Dashboard → Jobs → run-migrations → Run Job
   ```
2. **Trigger deployment:**
   ```bash
   curl -X POST "$RENDER_DEPLOY_HOOK"
   ```

## Monitoring

**GitHub Actions:**
- Actions tab → "Build and Deploy to Render" workflow
- View logs for each step
- Check for failures in migration or deployment steps

**Render:**
- Dashboard → Your Web Service → Events
- View deployment history and logs
- Check migration job logs in Jobs tab

**Health Check:**
```bash
curl https://cms-pricing-api.onrender.com/health
```

Expected: `{"status": "healthy", "database": "connected"}`

## Troubleshooting

### Migration Job Fails

**Symptoms:** CI fails at "Run Database Migrations" step

**Solutions:**
1. Check migration job logs in Render Dashboard
2. Verify `DATABASE_URL` is set correctly
3. Test migration locally:
   ```bash
   export DATABASE_URL="<render_database_url>"
   alembic upgrade head
   ```
4. Check for schema conflicts or breaking changes

### Deployment Hook Returns 404

**Symptoms:** CI fails at "Trigger Render Deploy Hook" step

**Solutions:**
1. Regenerate deploy hook in Render Dashboard
2. Update `RENDER_DEPLOY_HOOK` secret in GitHub
3. Verify service is active in Render

### Image Not Found

**Symptoms:** Render logs show "Failed to pull image"

**Solutions:**
1. Verify image exists in GHCR: `ghcr.io/alex-bea/cms-api:<sha>`
2. Check GHCR permissions (public or Render has access)
3. Try pulling image manually:
   ```bash
   docker pull ghcr.io/alex-bea/cms-api:latest
   ```

### Migration Times Out

**Symptoms:** CI fails after 5 minutes waiting for migration

**Solutions:**
1. Increase timeout in workflow (currently 5 minutes = 30 attempts × 10s)
2. Optimize slow migrations (add indexes CONCURRENTLY)
3. Check for blocking locks in database
4. Review migration complexity (see RUN-database-migrations-prd-v1.0.md)

## Policy Compliance

This workflow enforces:

✅ **Migrations-first:** No app startup DDL  
✅ **Image-based deployment:** Zero Render build minutes  
✅ **CI-controlled:** Auto-deploy disabled, CI triggers deploys  
✅ **Tag-based releases:** Only v*.*.* tags deploy to production  
✅ **Health checks:** Deployment waits for health endpoint

## References

- **STD-database-platform-prd-v1.0.md** - Database platform standard
- **PRD-render-hosting-prd-v1.0.md** - Hosting policy
- **RUN-render-deployment-prd-v1.0.md** - Deployment runbook (Part 8: CI/CD)
- **RUN-database-migrations-prd-v1.0.md** - Migration procedures

## Support

For issues:
1. Check workflow logs in GitHub Actions
2. Review Render deployment logs
3. Consult PRDs listed above
4. Contact Platform Engineering team

