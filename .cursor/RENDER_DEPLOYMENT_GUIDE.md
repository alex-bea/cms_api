# Render Deployment Guide - Production Setup

**Purpose:** Deploy CMS Pricing API with PostgreSQL on Render  
**Audience:** First-time deployers  
**Time:** 30-45 minutes  
**Cost:** $7/month (Starter database) + optional web service

**Why Render:**
- ✅ Simpler than Docker (no init script conflicts)
- ✅ Automatic backups and monitoring
- ✅ Clean Alembic migration path
- ✅ Production-grade infrastructure
- ✅ Free trial available

---

## Pre-Deployment Checklist

**Before starting, confirm you have:**

- [ ] GitHub account (Render uses GitHub OAuth)
- [ ] Payment method (Starter DB is $7/month after trial)
- [ ] 30-45 uninterrupted minutes
- [ ] `psql` CLI available (`brew install libpq` and add to `PATH`)
- [ ] Internet access for `pip` **or** pre-downloaded wheels for `pandas`, `sqlalchemy`, `structlog`
- [ ] Secure location (1Password/Vault) to store the Render `DATABASE_URL`
- [ ] Alembic migration `003_gpci_v13_add_mac_to_nk.py` and GPCI backfill script in the repo
- [ ] GPCI migration checklist (`.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md`) reviewed alongside this guide

## Part 1: Render Account Setup (5 minutes)

### Step 1: Sign Up

1. **Go to:** https://render.com/
2. **Click:** "Get Started" or "Sign Up"
3. **Choose:** "Sign in with GitHub" (easiest)
4. **Authorize:** Render to access your GitHub account
5. **Confirm** your email address

**First-time users get:**
- Free trial period
- $5 credit (covers ~1 month of Starter database)

### Step 2: Create a Team (Optional)

1. **Go to:** Dashboard → Settings
2. **Click:** "Create Team"
3. **Name:** Your team name (e.g., "CMS Pricing")
4. **Note:** Free tier allows 1 team

---

## Part 2: PostgreSQL Database Setup (10 minutes)

### Step 1: Create PostgreSQL Service

1. **From Dashboard**, click **"New +"** (top right)
2. **Select:** "PostgreSQL"
3. **Configure Database:**

   | Setting | Recommendation | Notes |
   |---------|----------------|-------|
   | **Name** | `cms-pricing-db` | Internal identifier |
   | **Database** | `cms_pricing` | Actual database name |
   | **User** | `cms_user` | Database username |
   | **Region** | Oregon (US West) | Choose closest to you |
   | **PostgreSQL Version** | 16 | Latest stable |
   | **Instance Type** | **Starter ($7/month)** | Recommended for production |

4. **Click:** "Create Database"
5. **Wait:** 2-3 minutes for provisioning

**Instance Type Guide:**
```
Free:     256MB storage, 90-day expiration (testing only)
Starter:  $7/month, 1GB, 7-day backups (recommended)
Standard: $20/month, 10GB, 14-day backups (scalable)
Pro:      $50/month, 25GB, 30-day backups (production)
```

**Recommendation:** Start with **Starter** ($7/month) - perfect for development and small production.

### Step 2: Get Connection String

After provisioning completes:

1. **Click** on your database (`cms-pricing-db`)
2. **Go to** "Connect" tab
3. **Copy** the **External Database URL**

**You'll see two URLs:**
```
Internal Database URL:  postgresql://cms_user:xxx@dpg-xxx:5432/cms_pricing
External Database URL:  postgresql://cms_user:xxx@oregon-postgres.render.com:5432/cms_pricing
```

**Use External URL** for local development and migrations.

**Save this URL** - you'll need it for all database operations.

PostgreSQL extensions: none required for this project.

### Step 3: Test Connection Locally

```bash
# Set DATABASE_URL (use YOUR external URL from Render)
export DATABASE_URL="postgresql://cms_user:PASTE_YOUR_PASSWORD@oregon-postgres.render.com:5432/cms_pricing"

# Test connection
psql $DATABASE_URL -c "SELECT current_database(), current_user, version();"

# You should see:
#  current_database | current_user | version
# ------------------+--------------+----------------------------------
#  cms_pricing      | cms_user     | PostgreSQL 16.x on x86_64-linux
```

**Troubleshooting:**
- `psql: command not found` → Install: `brew install libpq` and add `/opt/homebrew/opt/libpq/bin` to `PATH`
- `Connection refused` → Ensure you copied the **External** URL, not Internal
- `Password authentication failed` → Regenerate password from Render dashboard
- Working in shared environments? Avoid exporting credentials in shell history; use `env` files or secret managers.

### Step 4: Manage `DATABASE_URL` Securely

**Local development (recommended):**
1. Create a `.env` file in the repo root (**do not commit** this file):

   `.env` (local only)
   ```
   DATABASE_URL=postgresql://cms_user:YOUR_PASSWORD@oregon-postgres.render.com:5432/cms_pricing
   ```

2. Load it automatically with [`direnv`](https://direnv.net/):
   ```bash
   # macOS
   brew install direnv
   echo 'dotenv' > .envrc
   direnv allow
   ```

3. Store the value in 1Password or Vault and rotate it after initial setup.

**On Render (staging/production):**
- Set `DATABASE_URL` in your Web Service Environment panel.
- Do not store credentials in shell profiles or source code.

---

## Part 3: Run Database Migrations (10 minutes)

### Step 1: Initialize Schema with Alembic

```bash
cd cms-api

# Check current state (should be empty)
alembic current

# Run all migrations
alembic upgrade head

# Expected output:
# INFO  [alembic.runtime.migration] Running upgrade -> 001_add_nearest_zip_tables
# INFO  [alembic.runtime.migration] Running upgrade 001 -> 002_add_nber_centroids
# INFO  [alembic.runtime.migration] Running upgrade 002 -> 003_gpci_v13_add_mac_to_nk
# ✅ GPCI v1.3 migration complete
# INFO  [alembic.runtime.migration] Running upgrade 003 -> 004_gpci_v12_compat_view
# ✅ Created GPCI v1.2 compatibility view
# INFO  [alembic.runtime.migration] Running upgrade 004 -> 6d0f0408be80
```

**If migration 003 fails** (table doesn't exist):

```bash
# Create base tables first (on fresh DB)
python -c "from cms_pricing.database import Base, engine; from cms_pricing.models import *; Base.metadata.create_all(bind=engine)"

# Stamp to the matching revision (avoid stamping 'head' on an unknown DB)
# Replace with the exact revision that matches the created schema, e.g.:
#   alembic stamp 002_add_nber_centroids
alembic stamp <matching_revision_id>

# Now run forward migrations
alembic upgrade head
```

Caution — use `alembic stamp` sparingly. Only stamp a revision that exactly matches
the current schema. Use `alembic history` and `alembic current` to pick the correct
revision, then run `alembic upgrade head`.

### Step 2: Verify Schema

```bash
# Check tables were created
psql $DATABASE_URL -c "\dt"

# Check gpci_indices table specifically
psql $DATABASE_URL -c "\d gpci_indices"

# Verify v1.3 unique index exists
psql $DATABASE_URL -c "
  SELECT indexname, indexdef 
  FROM pg_indexes 
  WHERE tablename = 'gpci_indices' 
    AND indexname = 'uq_gpci_mac_locality_effective';
"

# You should see 1 row with the index definition
```

### Step 3: Verify Migration State

```bash
# Check which migrations were applied
alembic current

# You should see:
# 6d0f0408be80 (head)

# Or if you have multiple heads:
# 004_gpci_v12_compat_view, 6d0f0408be80

# List all applied migrations
psql $DATABASE_URL -c "SELECT * FROM alembic_version;"
```

---

## Part 4: Load GPCI Data (5 minutes)

### Step 1: Dry-Run Backfill

```bash
# Test without committing
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --dry-run

# Expected output:
#   ✅ Parsed 109 rows with v1.3
#   ✅ Verified: 0 duplicates on ['mac', 'locality_code', 'effective_from']
#   🔍 DRY RUN: Would load 109 new rows
#   ✅ DRY RUN SUCCESSFUL
```

### Step 2: Commit Backfill

```bash
# Load data into Render database
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --commit

# Expected output:
#   ✅ Parsed 109 rows
#   ✅ Verified: 0 duplicates
#   ✅ Loaded 109 rows
#   ✅ BACKFILL COMPLETE
```

### Step 3: Verify Data Loaded

```bash
# Check row count
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
# Expected: 109

# Check sample data
psql $DATABASE_URL -c "
  SELECT mac, locality_id, locality_name, work_gpci, pe_gpci, mp_gpci 
  FROM gpci_indices 
  LIMIT 10;
"

# Verify no duplicates on v1.3 NK
psql $DATABASE_URL -c "
  SELECT mac, locality_id, effective_start, COUNT(*) 
  FROM gpci_indices 
  GROUP BY mac, locality_id, effective_start 
  HAVING COUNT(*) > 1;
"
# Expected: 0 rows

# Check ambiguous locality_id='00' (multiple states)
psql $DATABASE_URL -c "
  SELECT mac, locality_id, locality_name 
  FROM gpci_indices 
  WHERE locality_id = '00' 
  ORDER BY mac;
"
# Expected: Multiple rows (AL, AZ, AR, CA, CO, etc.)
```

---

## Part 5: Testing & Verification (5 minutes)

### Step 1: Run Parser Tests Against Render DB

> **Prerequisite:** Ensure the Python environment is healthy. If `pip` cannot reach the Internet in your workspace, install dependencies from a wheel cache before running these commands.

```bash
# Parser tests (don't need DB, but good to verify environment)
pytest tests/ingestion/test_gpci_parser_golden.py -v

# Expected: 20/20 passing
```

### Step 2: Test Database Queries

```bash
# Connect to database
psql $DATABASE_URL

# Run some queries
SELECT COUNT(*) FROM gpci_indices;
SELECT DISTINCT mac FROM gpci_indices ORDER BY mac;
SELECT * FROM gpci_indices WHERE locality_id = '00' LIMIT 5;

# Exit
\q
```

### Step 3: Check Render Dashboard

1. **Go to** Render dashboard → Your database
2. **Check** "Metrics" tab:
   - Connections (should be low)
   - Storage used (should be < 10MB)
   - CPU/Memory (should be minimal)

3. **Check** "Logs" tab:
   - No errors
   - Connection logs visible

---

## Part 6: Production Hardening (10 minutes)

### Step 1: Set Up Automatic Backups

**Good News:** Render automatically backs up your database!

**Backup Schedule (Starter tier):**
- Daily backups
- 7-day retention
- Point-in-time recovery may vary by plan — verify in your dashboard.

**To verify:**
1. Database dashboard → "Backups" tab
2. See backup history and schedule

**Manual backup:**
```bash
# Download backup locally
pg_dump $DATABASE_URL > backup_render_$(date +%Y%m%d).sql

# Or use Render's download feature (web dashboard)
```

**Application Rollback**
1. Re-deploy the previous commit from Render (Deploys → select prior build).
2. Run smoke tests (`/health`, a simple query) to confirm.
3. If the schema has changed, restore the DB snapshot from the Backups tab or run the appropriate Alembic downgrade before re-deploying.

### Step 2: Configure Connection Pooling

Render includes connection pooling, but you can optimize:

1. **Go to:** Database → Settings
2. **Check:** "Max Connections" (default: 97 for Starter)
3. **Adjust** if needed (for API deployment)

**For API deployment:**
- Recommended: 20-50 connections
- Render automatically manages pooling

### Step 3: Set Up Monitoring Alerts

1. **Go to:** Database → Notifications
2. **Add Email Alert** for:
   - Database down
   - High CPU usage (> 80%)
   - High storage usage (> 80%)
   - Connection limit reached

3. **Configure** Slack/Discord webhooks (optional)

**Recommended Metrics to Watch:**

| Metric | Alert Condition | Notes |
|--------|-----------------|-------|
| `gpci_requests_without_mac_total` | > 0 sustained for 24h | Expect to trend toward zero post-migration |
| `gpci_duplicate_nk_violations` | > 0 | Indicates natural-key regression |
| API 500 error rate | Above historical baseline | Add Render Health Check or external alert |

Note: `gpci_requests_without_mac_total` and `gpci_duplicate_nk_violations` are application-level counters (e.g., Prometheus). Ensure your API emits these metrics; they are not Render built-ins. Expose them via your logging/metrics stack (Prometheus + Grafana, Datadog, etc.).

### Step 4: Database Connection Guardrails (App)

Configure SQLAlchemy to avoid connection exhaustion and stale sockets:

```python
from sqlalchemy import create_engine
import os

engine = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,  # seconds
)
```

Tune `pool_size`/`max_overflow` based on your workload and Render plan limits.

### Step 5: Enable Query Performance Insights (Pro tier only)

**If on Pro tier:**
1. Go to "Performance" tab
2. Enable slow query logging
3. Review query performance regularly

**For Starter tier:**
- Use application-level logging
- Monitor with structlog in your app

---

## Part 7: Deploy API to Render (Optional, 20 minutes)

---

### Secrets & Access Management

- Store the Render `DATABASE_URL`, API keys, and any service tokens in a centralized secret manager (1Password, Vault, AWS Secrets Manager).  
- Avoid committing `.env` files with production credentials—use Render's environment variable panel for deployment.  
- Rotate the database password after initial setup and update stored secrets accordingly.  
- Limit access to Render dashboard to required team members; review team permissions monthly.

### Database Roles (Least Privilege)

```sql
-- Roles
CREATE ROLE migrate NOINHERIT;
CREATE ROLE app_rw NOINHERIT;
CREATE ROLE ro NOINHERIT;

-- Users
CREATE USER cms_migrate WITH PASSWORD 'strong_password';
CREATE USER cms_app_rw WITH PASSWORD 'strong_password';
CREATE USER cms_ro WITH PASSWORD 'strong_password';

-- Grants
GRANT migrate TO cms_migrate;
GRANT app_rw TO cms_app_rw;
GRANT ro TO cms_ro;

-- Privileges
GRANT CONNECT ON DATABASE cms_pricing TO migrate, app_rw, ro;
GRANT USAGE ON SCHEMA public TO migrate, app_rw, ro;

-- DDL for migrate; DML for app_rw; SELECT for ro
GRANT CREATE, ALTER, DROP, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public TO migrate;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_rw;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ro;

-- Ensure future tables inherit privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, DELETE ON TABLES TO app_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ro;
```

---

**If you want to deploy the FastAPI application too:**

### Optional: Automate Alembic Migrations

To prevent drift, run `alembic upgrade head` automatically:
- **Render Job (recommended):** Create a one-off Job that runs `alembic upgrade head` with the same `DATABASE_URL`. Trigger it before each deploy.
- **CI step:** From your CI, run migrations against the target `DATABASE_URL` prior to promoting the app.
- **App start (fallback):** Gate the app’s startup on a migration check. This increases cold-start time and is less preferred.

### Step 1: Create Web Service

1. **Click:** "New +" → "Web Service"
2. **Connect:** Your GitHub repository
3. **Configure:**
   - Name: `cms-pricing-api`
   - Environment: Python 3
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn cms_pricing.main:app --host 0.0.0.0 --port $PORT`
   - Instance Type: Starter ($7/month)

### Step 2: Set Environment Variables

In Web Service → Environment:
```
DATABASE_URL=postgresql://cms_user:xxx@oregon-postgres.render.com:5432/cms_pricing
REDIS_URL=(if using Redis, create separately)
API_KEYS=your-api-key-here
LOG_LEVEL=INFO
```

### Step 3: Deploy

1. **Click:** "Create Web Service"
2. **Wait:** 5-10 minutes for build and deploy
3. **Access:** Your API at `https://cms-pricing-api.onrender.com`

### Step 4: Test API

```bash
# Test health endpoint
curl https://cms-pricing-api.onrender.com/health

# Test GPCI endpoint
curl "https://cms-pricing-api.onrender.com/api/v1/gpci?mac=01112&locality=54"
```

### Step 5: Enable Health Check

In Render → **Health Checks**, point the path to `/health`. Ensure your `/health` endpoint returns `200` and (optionally) verifies DB connectivity so deploys only go live when dependencies are ready.

---

## Troubleshooting

### Issue: "psql: command not found"

**Solution:**
```bash
brew install libpq
export PATH="/opt/homebrew/opt/libpq/bin:$PATH"
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc
```

```bash
# Debian/Ubuntu
sudo apt update && sudo apt install -y postgresql-client
```

- **Windows:** Install “PostgreSQL Client Tools” from the official PostgreSQL site and add the `psql.exe` directory to your `PATH`.

### Issue: "alembic upgrade head" fails on Render

**Solution:**
```bash
# Create base tables first (on fresh DB)
python -c "from cms_pricing.database import Base, engine; from cms_pricing.models import *; Base.metadata.create_all(bind=engine)"

# Stamp to the matching revision (avoid 'head' on unknown DB)
alembic stamp <matching_revision_id>

# Future migrations will work normally
alembic upgrade head
```

`alembic stamp` should point to the exact revision that matches the current schema. Use `alembic history` and `alembic current` to confirm before upgrading.

### Issue: "Connection refused" to Render database

**Causes:**
1. Using Internal URL instead of External
2. Render database not fully provisioned yet
3. IP allowlist (Render doesn't use this, so unlikely)

**Solution:**
- Verify you're using **External** Database URL
- Wait 5 minutes after creation
- Check Render dashboard for "Available" status

### Issue: "Too many connections"

**Solution:**
```bash
# Check active connections
psql $DATABASE_URL -c "SELECT count(*) FROM pg_stat_activity;"

# Kill idle connections
psql $DATABASE_URL -c "
  SELECT pg_terminate_backend(pid) 
  FROM pg_stat_activity 
  WHERE datname = 'cms_pricing' 
    AND state = 'idle' 
    AND state_change < now() - interval '5 minutes';
"
```

### Issue: Backfill script fails on Render

**Check:**
1. DATABASE_URL is set correctly
2. Source file exists: `sample_data/rvu25d_0/GPCI2025.txt`
3. Python environment has all dependencies

**Debug:**
```bash
# Test connection
psql $DATABASE_URL -c "SELECT 1;"

# Test parser
python -c "from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci; print('Parser OK')"

# Run with verbose logging
python scripts/backfill_gpci_v13.py --dry-run --verbose
```

---

## Cost Management

### **Starter Tier ($7/month)**

**Includes:**
- 1GB storage
- 7-day automated backups
- 97 max connections
- Standard support

**Sufficient for (small production workloads):**
- ~1M GPCI rows
- 100-500 API requests/day
- Development and small production

**Costs:**
- Database: $7/month
- Web Service (optional): $7/month
- **Total:** $7-14/month

### **Monitoring Costs:**

**Check in Render dashboard:**
1. Go to Account → Billing
2. See current usage
3. Set spending limits (optional)

**Optimize:**
- Use Starter tier for development
- Upgrade to Standard when you need more storage
- Free tier good for testing only (90-day expiration)

---

## Security Best Practices

### **1. Environment Variables**

**Never commit DATABASE_URL to git!**

```bash
# ✅ GOOD: Use environment variable
export DATABASE_URL="postgresql://..."

# ❌ BAD: Hardcode in code
DATABASE_URL = "postgresql://..."  # DON'T DO THIS
```

**In Render:**
- Environment variables are encrypted
- Accessible only to authorized team members
- Rotatable (change password easily)

### **2. SSL Connections**

Render enforces SSL by default ✅

**Verify:**
```bash
psql "$DATABASE_URL?sslmode=require" -c "SELECT current_setting('ssl');"
# Should return: on
```

### **3. IP Allowlisting**

Render doesn't require IP allowlisting (easier setup), but they recommend:
- Use strong passwords
- Rotate credentials regularly
- Limit database user permissions

**To create read-only user:**
```sql
CREATE USER readonly_user WITH PASSWORD 'strong_password';
GRANT CONNECT ON DATABASE cms_pricing TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
```

### **4. Secrets Management**

**For API deployment:**
- Store API_KEYS in Render environment variables
- Use separate keys for prod/staging/dev
- Rotate keys quarterly

---

## Backup & Disaster Recovery

### **Automatic Backups (Render)**

**Starter tier:**
- Daily backups
- 7-day retention
- Automated, no scripting

**To restore from backup:**
1. Render dashboard → Database → Backups
2. Select backup date
3. Click "Restore"
4. Creates new database from backup

### **Manual Backups (Recommended)**

**Weekly backup to local machine:**
```bash
# Download backup
pg_dump $DATABASE_URL > backup_render_$(date +%Y%m%d).sql

# Compress
gzip backup_render_$(date +%Y%m%d).sql

# Store in safe location (Google Drive, S3, etc.)
```

**Restore from manual backup:**
```bash
# Restore to Render
psql $DATABASE_URL < backup_render_20251021.sql

# Or restore locally for testing
createdb cms_pricing_restore
psql cms_pricing_restore < backup_render_20251021.sql
```

### **Disaster Recovery Plan**

**Scenario 1: Database corruption**
- Use Render's automated backup (restore in dashboard)
- ETA: 10-15 minutes

**Scenario 2: Accidental data deletion**
- Restore from most recent backup
- Re-run backfill script for latest data

**Scenario 3: Render outage**
- Deploy to backup provider (Railway, Supabase)
- Restore from local backup
- Update DATABASE_URL in app

---

## Monitoring & Observability

### **Render Built-In Monitoring**

**Metrics available:**
- Connection count
- Storage used
- CPU usage
- Memory usage
- Query count (Pro tier)

**Access:** Database dashboard → Metrics tab

### **Custom Monitoring (Recommended)**

**Add to your application:**
```python
import time, structlog
from cms_pricing.database import engine

log = structlog.get_logger()
with engine.connect() as conn:
    t0 = time.time()
    conn.execute("SELECT 1")
    log.info("db_healthcheck", duration_ms=int((time.time() - t0) * 1000))
```

**Set up alerts for:**
- Query duration > 1s
- Connection pool exhausted
- Row count anomalies (expect ~109)
- Duplicate constraint violations (expect 0)

---

## Deployment Workflow Summary

### **Complete End-to-End Process:**

```
1. Render Signup (5 min)
   └── Sign in with GitHub
   
2. Create PostgreSQL (10 min)
   ├── New + → PostgreSQL
   ├── Name: cms-pricing-db
   ├── Instance: Starter ($7/month)
   └── Wait for provisioning
   
3. Get DATABASE_URL (1 min)
   ├── Dashboard → Connect tab
   ├── Copy External Database URL
   └── export DATABASE_URL="..."
   
4. Run Migrations (10 min)
   ├── alembic upgrade head
   ├── Verify tables created
   └── Check v1.3 index exists
   
5. Load Data (5 min)
   ├── backfill dry-run
   ├── backfill commit
   └── Verify row count
   
6. Verify (5 min)
   ├── Run parser tests
   ├── Query sample data
   └── Check Render metrics
   
7. Production Hardening (10 min)
   ├── Set up monitoring alerts
   ├── Test backup/restore
   └── Document DATABASE_URL

Total Time: ~45 minutes
Total Cost: $7/month
```

---

## Existing Deployments (Upgrading an Existing Render DB)

If you already have data in a Render-hosted Postgres instance:

1. **Back Up First**
   ```bash
   pg_dump $DATABASE_URL > backup_render_pre_migration_$(date +%Y%m%d).sql
   ```
2. **Capture Current Metrics** – Run the duplicate/row-count/index queries from `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` so you can compare after the upgrade.
3. **Review Alembic History** – Ensure the database is stamped to the expected revision (`alembic current`). If drifted, resolve before applying new migrations.
4. **Apply Migration & Backfill** – Use `--dry-run` for the GPCI backfill to inspect upserts before committing.
5. **Post-Upgrade Validation** – Re-run API smoke tests, monitor for 409 conflicts, and review Render metrics for the first 24 hours.
6. **Rollback Plan** – Keep the backup and downgrade instructions from the checklist handy if you need to revert.

---

## Next Steps After Deployment

### **Immediate (Same Day):**

- [ ] Run full test suite: `pytest tests/`
- [ ] Verify monitoring alerts configured
- [ ] Test backup/restore process
- [ ] Document DATABASE_URL in team wiki/1Password

### **Within 1 Week:**

- [ ] Monitor query performance
- [ ] Review connection usage
- [ ] Test quarterly RVU ingestion workflow
- [ ] Train team on Render dashboard

### **Monthly:**

- [ ] Review Render costs
- [ ] Check backup retention
- [ ] Audit user permissions
- [ ] Update documentation if workflow changes

---

## Render vs Local Docker Comparison

| Aspect | Render | Local Docker |
|--------|--------|--------------|
| **Setup Time** | 30 min | 60+ min (due to conflicts) |
| **Complexity** | Low (web UI) | Medium (CLI, configs) |
| **Conflicts** | None (clean DB) | init-db.sql issues |
| **Backups** | Automatic | Manual scripting |
| **Monitoring** | Built-in | Manual setup |
| **Cost** | $7/month | Free (electricity/hardware) |
| **Accessibility** | Anywhere | Local only |
| **Production Ready** | Yes | No (development only) |
| **Learning Curve** | Low | Medium |
| **Recommended For** | First deployment | Development |

**Verdict:** **Render is better for your first production deployment** ✅

---

## Files Needed for Render

**Already have:**
- ✅ `requirements.txt` (Python dependencies)
- ✅ `alembic/` (migrations)
- ✅ `cms_pricing/` (application code)
- ✅ `scripts/backfill_gpci_v13.py` (data loader)

**Optional (for API deployment):**
- `render.yaml` (infrastructure as code)
- `Procfile` (start commands)
- `.env.example` (environment variable template)

### render.yaml starter (optional)

```yaml
services:
  - type: web
    name: cms-pricing-api
    env: python
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn cms_pricing.main:app --host 0.0.0.0 --port $PORT
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: cms-pricing-db
          property: connectionString

databases:
  - name: cms-pricing-db
    databaseName: cms_pricing
    user: cms_user
    plan: starter
    region: oregon
    postgresVersion: "16"
```

---

## Render Deployment Checklist

**Before Starting:**
- [ ] Save this guide: `.cursor/RENDER_DEPLOYMENT_GUIDE.md`
- [ ] Review Part 1-7 above
- [ ] Have 45 minutes of focused time
- [ ] Credit card ready (free trial, then $7/month)

**During Deployment:**
- [ ] Follow steps in order
- [ ] Don't skip verification steps
- [ ] Document DATABASE_URL securely
- [ ] Test at each phase

**After Deployment:**
- [ ] Run parser tests
- [ ] Set up monitoring
- [ ] Document for team
- [ ] Schedule first backup test

---

## Support Resources

**Render Documentation:**
- https://render.com/docs/databases
- https://render.com/docs/postgresql

**Community:**
- Render Discord: https://render.com/community
- GitHub Issues: Report bugs

**This Project:**
- Migration checklist: `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md`
- Quick start: `.cursor/GPCI_V13_QUICK_START.md`
- Troubleshooting: `.cursor/DATABASE_SETUP_GUIDE.md`
- Lessons learned: `.cursor/LESSONS_DATABASE_SETUP.md`

---

**Ready to deploy to Render? You have everything you need!** 🚀

**End of Render Deployment Guide**
