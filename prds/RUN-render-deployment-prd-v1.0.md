# Runbook: Render Deployment & Operations

**Status:** Draft v1.0  
**Owners:** Platform Engineering, SRE  
**Consumers:** Release engineers, on-call responders, data platform contributors  
**Change control:** PR review + infra sign-off

**Cross-References:**
- `prds/PRD-render-hosting-prd-v1.0.md` (policy decisions)
- `prds/DOC-master-catalog-prd-v1.0.md` (system map)
- `prds/STD-observability-monitoring-prd-v1.0.md` (monitoring standards)
- `prds/RUN-database-migrations-prd-v1.0.md` (migrations workflow)
- `prds/RUN-database-backup-dr-prd-v1.0.md` (rollback/PITR)
- `prds/RUN-database-sanitization-prd-v1.0.md` (tokenized refresh)
- `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (historical example)

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
   | **PostgreSQL Version** | 16 | Latest stable (Render may provision 17.x - this is fine!) |
   | **Instance Type** | **Starter ($7/month)** | Recommended for production |

4. **Click:** "Create Database"
5. **Wait:** 2-3 minutes for provisioning

**📝 PostgreSQL Version Note (Gap 6):**
- You may request PG 16 but receive 17.6 (or newer minor version)
- **This is expected:** Render provisions the latest stable minor/major version
- **Why it's OK:** PostgreSQL maintains backward compatibility within and across major versions for standard operations
- **No action needed:** Your migrations and queries will work identically
- **To verify version:** `psql $DATABASE_URL -c "SELECT version();"`
- **To pin major version:** Contact Render support if you need a specific major version (rare)

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

**Issue: `psql: command not found` (Gap 4 - Common on macOS)**

Homebrew installs `libpq` but doesn't add it to PATH by default.

**Fix (macOS - temporary for current session):**
```bash
# Add to PATH for this session
export PATH="/opt/homebrew/opt/libpq/bin:$PATH"

# Test it works
psql --version
# Should show: psql (PostgreSQL) 16.x or similar
```

**Fix (macOS - permanent):**
```bash
# Add to your shell profile
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc

# Reload shell
source ~/.zshrc

# Verify
which psql
# Should show: /opt/homebrew/opt/libpq/bin/psql
```

**Fix (Linux):**
```bash
# Usually installed system-wide
sudo apt-get install postgresql-client   # Debian/Ubuntu
sudo yum install postgresql              # CentOS/RHEL
```

**Other common issues:**
- `Connection refused` → Ensure you copied the **External** URL, not Internal
- `Password authentication failed` → Regenerate password from Render dashboard
- `could not translate host name` → Check your internet connection and DNS

### Step 4: Manage `DATABASE_URL` Securely

**⚠️ SECURITY WARNING (Gap 7):**
- **NEVER commit `.env` to git** - it contains production database credentials
- **NEVER log `DATABASE_URL`** in application code or CI output
- **ALWAYS verify** `.gitignore` includes `.env` before creating the file
- **ALWAYS backup** credentials to 1Password/Vault immediately after creation

**Security checklist:**
```bash
# 1. Verify .gitignore coverage BEFORE creating .env
grep -q "^\.env$" .gitignore && echo "✅ .env is ignored" || echo "❌ WARNING: .env NOT in .gitignore!"

# 2. If missing, add it now
echo ".env" >> .gitignore

# 3. Verify no .env files are staged
git status | grep -q ".env" && echo "❌ DANGER: .env is staged!" || echo "✅ Safe to proceed"
```

**Local development (recommended):**
1. **Verify `.gitignore`** (see checklist above)

2. Create a `.env` file in the repo root:

   `.env` (local only, **NEVER COMMIT**)
   ```
   DATABASE_URL=postgresql://cms_user:YOUR_PASSWORD@oregon-postgres.render.com:5432/cms_pricing
   ```

3. **Immediately backup to 1Password/Vault:**
   - Create secure note: "Render CMS Pricing DB"
   - Store full `DATABASE_URL`
   - Tag with project name and environment

4. Load it automatically with [`direnv`](https://direnv.net/):
   ```bash
   # macOS
   brew install direnv
   echo 'dotenv' > .envrc
   direnv allow
   ```

**On Render (staging/production):**
- Set `DATABASE_URL` in your Web Service Environment panel
- Use Render's built-in secrets management (encrypted at rest)
- Do **NOT** store credentials in:
  - Shell profiles (`~/.zshrc`, `~/.bashrc`)
  - Source code or config files
  - CI logs or debug output
  - Slack/chat messages

**Credential rotation (every 90 days for prod, 180 days for non-prod):**
```bash
# 1. Generate new password in Render dashboard
# 2. Update .env and 1Password/Vault
# 3. Update Render Web Service environment
# 4. Test connection with new credentials
# 5. Revoke old password
```

---

## Part 3: Run Database Migrations (10 minutes)

Detailed guidance for planning, validating, and executing migrations now lives in `prds/RUN-database-migrations-prd-v1.0.md`. Use this section as a high-level reminder while the heavy lifting happens via that runbook.

### Step 1: Follow the migrations runbook
- Complete the **Authoring Workflow**, **Dry-Run**, and **Bootstrap/Stamp** steps in the migrations runbook before touching Render.
- Ensure any sanitized snapshot used for dry-run was produced via `prds/RUN-database-sanitization-prd-v1.0.md`.

### Step 2: Execute the migration job
- From the CI/Render job defined in the migrations runbook, run `alembic upgrade head` using the `migrate` role. Do **not** call `Base.metadata.create_all()` in this pipeline.
- If the job reports schema drift or missing tables, fall back to the “Fresh Database Bootstrap & Stamp Approval” section in the migrations runbook instead of ad-hoc fixes.

### Step 3: Post-checks
- Run the verification checklist (`alembic_version`, table/index presence, smoke queries) in the migrations runbook.
- If rollback is required, immediately pivot to `prds/RUN-database-backup-dr-prd-v1.0.md` for PITR/restore procedures.

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

### Troubleshooting: Python Environment Issues (Gap 5)

**Issue: Segmentation Fault (exit 139) when running backfill script**

**Symptom:**
```bash
python scripts/backfill_gpci_v13.py
[1]    12345 segmentation fault  python scripts/backfill_gpci_v13.py
```

**Root cause:** Conda environment conflicts with pandas/numpy on macOS (common issue with Apple Silicon and Intel Macs).

**Diagnosis:**
```bash
# 1. Check Python environment
which python
python --version

# 2. Try importing pandas
python -c "import pandas; print('Pandas version:', pandas.__version__)"
# If this segfaults, environment is broken

# 3. Check if using conda
conda info --envs
```

**Fix Option A: Create Clean venv (Recommended)**

This avoids conda entirely and uses system Python:

```bash
# 1. Use system Python (not conda)
/usr/bin/python3 -m venv .venv-deploy

# 2. Activate venv
source .venv-deploy/bin/activate

# 3. Upgrade pip
pip install --upgrade pip

# 4. Install dependencies
pip install pandas sqlalchemy psycopg2-binary python-dotenv structlog

# 5. Test pandas works
python -c "import pandas; print('✅ Pandas works!', pandas.__version__)"

# 6. Run backfill script
python scripts/backfill_gpci_v13.py --commit
```

**Fix Option B: Fix Conda Environment**

If you prefer to stay with conda:

```bash
# Option B1: Update packages
conda update pandas numpy

# Option B2: Create fresh conda environment
conda create -n cms-pricing-clean python=3.11
conda activate cms-pricing-clean
pip install -r requirements.txt

# Test
python -c "import pandas; print('✅ Works!')"
```

**Fix Option C: Load Data via SQL (Workaround)**

If Python environment issues persist, load data directly:

```bash
# 1. Export GPCI data to CSV
python scripts/export_gpci_to_csv.py > gpci_data.csv

# 2. Load via psql COPY
psql $DATABASE_URL << 'SQL'
\COPY gpci_indices (mac, locality_code, locality_name, work_gpci, pe_gpci, mp_gpci, effective_from, effective_to, release_id)
FROM 'gpci_data.csv' 
CSV HEADER;
SQL

# 3. Verify
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
```

**Prevention:**
- Use system Python + venv for production scripts
- Document Python version in README
- Add `.python-version` file to repo
- Test scripts in clean environment before deployment

**Database-Only Deployment Pattern (Gap 8):**

**What it means:** Deploy and verify database schema **without** loading all data or deploying the API.

**Why it's valid:**
- ✅ Allows schema verification first
- ✅ Data load can be deferred (or done separately)
- ✅ Reduces deployment risk (schema failures caught early)
- ✅ Database is "production-ready" even without data

**When to defer data load:**
1. Python environment issues (like today)
2. Large datasets (load during maintenance window)
3. Schema verification needed first
4. Incremental deployment strategy

**Database states:**
- **Schema-ready:** Tables exist, migrations applied, no data → **VALID for production**
- **Data-ready:** Schema + data loaded → Ready for API deployment
- **API-ready:** Schema + data + API deployed → Fully operational

**To verify schema-ready database:**
```bash
# 1. Verify all tables exist
psql $DATABASE_URL -c "\dt" | wc -l
# Expected: 40+ tables

# 2. Verify critical indexes
psql $DATABASE_URL -c "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public';"
# Expected: 50+ indexes

# 3. Verify Alembic at head
alembic current
# Expected: 6d0f0408be80 (head)

# 4. Run smoke tests (no data needed)
pytest tests/test_database_schema.py
```

**Your database is production-ready even if data load is deferred!**

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
- **Render Job (recommended):** Create a one-off Job that runs `alembic upgrade head` with the same `DATABASE_URL`. Trigger it before each deploy. See Part 8 for detailed setup.
- **CI step:** From your CI, run migrations against the target `DATABASE_URL` prior to promoting the app.
- **App start (fallback):** Gate the app's startup on a migration check. This increases cold-start time and is less preferred.

**See Part 8 below for full CI/CD automation including One-Off Jobs.**

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

In Render → **Health Checks**, point the path to `/health`. Ensure your `/health` endpoint returns `200` and verifies DB connectivity so deploys only go live when dependencies are ready.

**Example health endpoint** (FastAPI with DB check):

```python
from fastapi import FastAPI
from sqlalchemy import text
from cms_pricing.database import engine

app = FastAPI()

@app.get("/health")
def health():
    """
    Health check endpoint with database connectivity test.
    
    Returns HTTP 200 if healthy, 500 if DB unavailable.
    Target: <50ms response time.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}, 500
```

**Performance target:** Keep health checks under 50ms to avoid deployment delays.

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
- [ ] Execute `scripts/load_rvu_to_production.py` (or equivalent ingestion run) and confirm:
  - `/data/ingestion/production/raw/cms_rvu/<release_id>/files` holds binary ZIP artifacts (>1 MB) rather than HTML pages.
  - `data/ingestion/production/cms_rvu_observability_*.json` reports `record_count > 0` before marking the run success.
  - Discovery manifest entries include `content_type` and `size_bytes` for every quarter; missing fields require remediation before promotion.

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

**Option A: Image-based deployment (recommended - zero build minutes)**

```yaml
services:
  - type: web
    name: cms-pricing-api
    runtime: image
    image:
      url: ghcr.io/YOUR_ORG/cms-pricing-api:latest
    plan: starter
    healthCheckPath: /health
    envVars:
      - key: DATABASE_URL
        sync: false  # Prompts for value at creation, never commits secrets
      - key: LOG_LEVEL
        value: INFO

databases:
  - name: cms-pricing-db
    databaseName: cms_pricing
    user: cms_user
    plan: starter
    region: oregon
    postgresVersion: "16"
```

**Option B: Python runtime (fallback)**

```yaml
services:
  - type: web
    name: cms-pricing-api
    runtime: python
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn cms_pricing.main:app --host 0.0.0.0 --port $PORT
    plan: starter
    healthCheckPath: /health
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

**Note:** Image-based deployment (Option A) follows PRD policy §3.1 and eliminates on-platform build minutes. See Part 8 below for full CI/CD setup.

---

## Render Deployment Checklist

**Before Starting:**
- [ ] Save this guide: `prds/RUN-render-deployment-prd-v1.0.md`
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

## Part 7 Troubleshooting: Common Deployment Issues

Based on 2025-10-21 API deployment experience.

### Issue 1: Service Stuck at "Starting service..." (401 Unauthorized)

**Symptoms:**
- Deployment hangs for 8+ minutes
- Logs show: `INFO: "GET /health HTTP/1.1" 401 Unauthorized`
- Service never reaches "Live" status

**Root cause:** Health endpoint requires authentication, but Render health checker can't provide API key.

**Fix:**
```python
# In your authentication middleware (e.g., cms_pricing/middleware.py)
class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip auth for health checks and docs
        if request.url.path in ["/health", "/healthz", "/readyz", "/docs", "/redoc", "/openapi.json"]:
            return await call_next(request)  # ← CRITICAL: Must bypass auth!
        
        # ... rest of auth logic
```

**Prevention:**
- Always test health endpoints WITHOUT authentication
- Add to deployment checklist: `curl http://localhost:8000/health` (should return 200, not 401)
- Document all public endpoints in API security policy

**Health Endpoint Requirements:**
- ✅ MUST be unauthenticated (no API key required)
- ✅ MUST respond in <1 second
- ✅ MUST be lightweight (avoid heavy DB queries)
- ✅ SHOULD check critical dependencies (database ping OK)

---

### Issue 2: Render Using Cached/Old Docker Image

**Symptoms:**
- Deployed with "Deploy latest commit"
- Logs still show old errors you already fixed
- Changes not appearing in deployment

**Root cause:** Render caches `:latest` tag aggressively. Even when new image is pushed to GHCR, Render may pull cached version.

**Fix (Option A - Use :main tag):**
```
In Render Settings → Image URL:
Change from: ghcr.io/alex-bea/cms-api:latest
To: ghcr.io/alex-bea/cms-api:main
```

**Fix (Option B - Clear cache):**
```
In Render Manual Deploy:
Select: "Clear build cache & deploy"
```

**Fix (Option C - Use SHA tags):**
```
For specific commit: ghcr.io/alex-bea/cms-api:sha-abc123
```

**Prevention:**
- Check logs for "Pulling image" timestamp
- Verify image digest matches expected build
- Use `:main` tag instead of `:latest` for active development
- Switch to version tags (`:v1.0.0`) for production

---

### Issue 3: Out of Memory (512MB) - Starter Tier

**Symptoms:**
- Deployment fails with "Out of memory (used over 512MB)"
- Service crashes during startup
- Logs cut off mid-startup

**Root cause:** Default configuration uses too much memory:
- 4 uvicorn workers × 50MB = 200MB
- Eager-loading all schemas at startup = 100MB
- Python runtime + dependencies = 200MB
- **Total: ~650MB** > 512MB limit

**Fix (Stay on Starter tier):**

Optimize memory in Dockerfile:
```dockerfile
# Reduce workers to 1
CMD ["uvicorn", "app:app", "--workers", "1", "--port", "${PORT:-8000}"]
```

Lazy-load schemas:
```python
# In schema_registry.py
def _load_existing_schemas(self):
    """Skip eager loading - load on-demand"""
    pass

def get_schema(self, dataset_name: str):
    """Lazy-load schema on first access"""
    if dataset_name not in self._schemas:
        self._lazy_load_schema(dataset_name)
    return self._schemas.get(dataset_name)
```

**Result:** ~420MB (fits in 512MB Starter tier)

**Alternative: Upgrade tier**
```
Free:     512 MB  - Too small for most APIs
Starter:  512 MB  - OK with optimizations
Standard: 2 GB    - Recommended for production ($25/mo)
Pro:      4 GB    - High traffic ($50/mo)
```

**Memory Sizing Guide:**
- Starter (512MB): 1 worker, lazy-load, < 1000 requests/day
- Standard (2GB): 2-4 workers, can eager-load, < 100k requests/day
- Pro (4GB): 4+ workers, full caching, production traffic

---

### Issue 4: Permission Denied Creating Directories

**Symptoms:**
- App crashes at startup
- Error: `PermissionError: [Errno 13] Permission denied: 'data'`
- Container runs but exits immediately

**Root cause:** Dockerfile switches to non-root user (`appuser`), but directories don't exist or aren't owned by that user.

**Fix:**
```dockerfile
# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Create directories with proper permissions BEFORE switching user
RUN mkdir -p data/observability data/metrics data/quarantine data/cache && \
    chown -R appuser:appuser data

# NOW switch to non-root user
USER appuser
```

**Prevention:**
- All writable directories must be created before `USER` statement
- Test Docker image locally: `docker run --rm <image>` to catch permission errors
- Add to deployment checklist

---

### Issue 5: Schema Contract Validation Failures

**Symptoms:**
- App starts but logs show: `Failed to load schema X: 'generated_at'`
- Multiple schema loading errors
- May cause partial functionality loss

**Root cause:** SchemaContract dataclass requires specific fields, but JSON schemas were missing them.

**Required fields in ALL schema contracts:**
```json
{
  "dataset_name": "cms_example",     ← REQUIRED
  "version": "1.0",                  ← REQUIRED
  "generated_at": "2025-10-21T00:00:00Z",  ← REQUIRED
  "columns": { ... }
}
```

**Fix:**
Add missing fields to all schema JSON files in `cms_pricing/ingestion/contracts/`.

**Prevention:**
- Create schema contract template with all required fields
- Add CI lint: validate all `*_v*.json` files have required fields
- Use JSON schema validation for schema contracts (meta!)

```bash
# CI check to add
python scripts/lint_schema_contracts.py
# Validates: dataset_name, version, generated_at present in all schemas
```

---

## Part 7 Best Practices Summary

Based on today's deployment, always ensure:

1. ✅ Health endpoints are PUBLIC (no authentication)
2. ✅ Test Docker image locally before pushing
3. ✅ Verify all schema contracts have required fields
4. ✅ Create writable directories in Dockerfile (before USER)
5. ✅ Size instance tier appropriately (test memory usage)
6. ✅ Use `:main` or SHA tags (avoid `:latest` caching)
7. ✅ Distinguish deployment workflows from test workflows

---

## Part 8: Automate Deployments with CI/CD (Optional)

**Purpose:** Automate image builds and deployments to achieve zero on-platform build minutes per PRD policy §4.1.

**Audience:** Teams ready to implement CI/CD after successful manual deployment (Parts 1-7).

**Prerequisites:**
- Parts 1-6 completed (database deployed)
- Optional: Part 7 completed (API deployed manually once)
- GitHub repository access
- Docker knowledge
- Render account with payment method

**Time:** 30-45 minutes one-time setup

---

### 8.1: GitHub Container Registry (GHCR) Setup

**Step 1: Enable GHCR for your repository**

1. Go to your GitHub repository
2. Settings → Packages
3. Enable "Improved container support"

**Step 2: Create Personal Access Token (PAT)**

1. GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate new token (classic)
3. Name: "Render Deployment"
4. Scopes:
   - ✅ `write:packages`
   - ✅ `read:packages`
   - ✅ `delete:packages`
5. Copy token (you'll need it once)

**Step 3: Test GHCR authentication locally**

```bash
echo $GITHUB_TOKEN | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
```

---

### 8.2: Create Dockerfile (if not exists)

**Create `Dockerfile` in repository root:**

```dockerfile
# Multi-stage build for smaller images
FROM python:3.11-slim AS builder

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY cms_pricing/ ./cms_pricing/
COPY alembic/ ./alembic/
COPY alembic.ini .

# Add local packages to PATH
ENV PATH=/root/.local/bin:$PATH

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Run application
CMD ["uvicorn", "cms_pricing.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Benefits:**
- Multi-stage build (smaller final image)
- Dependencies cached separately (faster rebuilds)
- No unnecessary build tools in production image

---

### 8.3: GitHub Actions Workflow

**Create `.github/workflows/deploy-api.yml`:**

```yaml
name: Deploy API to Render

on:
  push:
    branches:
      - main
    paths:
      - 'cms_pricing/**'
      - 'requirements.txt'
      - 'Dockerfile'
      - 'alembic/**'
      - 'prds/RUN-render-deployment-prd-v1.0.md'
    tags:
      - 'v*'
      - 'release/*'

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository_owner }}/cms-pricing-api

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Extract metadata (tags, labels)
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=ref,event=branch
            type=ref,event=pr
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=sha,prefix={{branch}}-
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push Docker image
        uses: docker/build-push-action@v5
        with:
          context: .
          file: ./Dockerfile
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max

      - name: Trigger Render deploy
        if: github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/')
        env:
          RENDER_DEPLOY_HOOK_URL: ${{ secrets.RENDER_DEPLOY_HOOK_URL }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          curl -X POST "$RENDER_DEPLOY_HOOK_URL&imgURL=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ env.IMAGE_TAG }}"
```

**Key features:**
- ✅ Path filters (only rebuilds when relevant files change)
- ✅ Multi-arch support ready
- ✅ GitHub Actions cache (faster builds)
- ✅ Automatic tagging (SHA, semver, latest)
- ✅ Deploy hook with specific image tag

---

### 8.4: Render Deploy Hook Setup

**Step 1: Get Deploy Hook URL**

1. Render Dashboard → your Web Service
2. Settings → Deploy Hook
3. Click "Create Deploy Hook"
4. Copy the URL (looks like: `https://api.render.com/deploy/srv-xxx?key=yyy`)

**Step 2: Add to GitHub Secrets**

1. GitHub repo → Settings → Secrets and variables → Actions
2. New repository secret
3. Name: `RENDER_DEPLOY_HOOK_URL`
4. Value: `<paste deploy hook URL>`

**Step 3: Test deploy hook manually**

```bash
# Test with latest tag
curl -X POST "https://api.render.com/deploy/srv-xxx?key=yyy"

# Test with specific image
IMAGE_TAG="ghcr.io/YOUR_ORG/cms-pricing-api:sha-abc123"
ENCODED_IMAGE=$(echo -n "$IMAGE_TAG" | jq -sRr @uri)
curl -X POST "https://api.render.com/deploy/srv-xxx?key=yyy&imgURL=${ENCODED_IMAGE}"
```

**Expected response:**
```json
{"deploy": {"id": "dep-xxx", "status": "pending"}}
```

Tip: Always URL-encode the `imgURL` value before calling the hook; unencoded slashes or SHA prefixes will cause Render to reject the request.

---

### 8.5: Configure Render for Image Deploys

**Update your Render Web Service:**

1. Dashboard → Web Service → Settings
2. **Image URL:** `ghcr.io/YOUR_ORG/cms-pricing-api:latest`
3. **Auto-deploy:** OFF (CI controls deploys)
4. **Root Directory:** (leave empty for image-based)
5. **Build Command:** (empty for image-based)
6. **Start Command:** (inherited from Dockerfile CMD)

**For monorepo setups:**
1. **Root Directory:** `cms-api/` (only if using Python runtime fallback)
2. **Build Filters:** `cms_pricing/**` (prevents unnecessary builds)

---

### 8.6: One-Off Jobs for Migrations

Render’s One-Off Jobs API lets us run Alembic migrations on demand without paying for a permanent Background Worker. There are two supported paths:

**Option A: Manual run from the dashboard**
1. Render Dashboard → open the web service.
2. Jobs tab (or service menu `⋯` → **New Job** / **Create Job**).
3. Enter the start command `alembic upgrade head`. The job automatically reuses the service’s latest build image and environment (including `DATABASE_URL`).
4. Click **Run Job** and monitor logs until it succeeds.

**Option B: Automated run via API (recommended)**

```bash
# Prereqs: RENDER_API_KEY and RENDER_SERVICE_ID (srv-xxxxx)
curl -X POST "https://api.render.com/v1/services/${RENDER_SERVICE_ID}/jobs" \
  -H "Authorization: Bearer ${RENDER_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"startCommand":"alembic upgrade head"}'
```

The response contains the job `id`. Poll `"https://api.render.com/v1/jobs/${JOB_ID}"` until `status` becomes `succeeded` or `failed`.

**CI/CD example (after the deploy hook fires):**

```yaml
      - name: Run database migrations
        env:
          RENDER_API_KEY: ${{ secrets.RENDER_API_KEY }}
          RENDER_SERVICE_ID: ${{ secrets.RENDER_SERVICE_ID }}
        run: |
          JOB_RESPONSE=$(curl -s -X POST \
            "https://api.render.com/v1/services/${RENDER_SERVICE_ID}/jobs" \
            -H "Authorization: Bearer ${RENDER_API_KEY}" \
            -H "Content-Type: application/json" \
            -d '{"startCommand":"alembic upgrade head"}')
          JOB_ID=$(echo "$JOB_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
          # Poll until the job finishes (see .github/workflows/deploy.yml for full script)
```

**Benefits of One-Off Jobs:**
- ✅ No race conditions (single execution)
- ✅ Dedicated logs for troubleshooting
- ✅ Explicit control over migration timing
- ✅ Reuses service environment variables
- ✅ Pay-per-second; no $7/month worker

---

### 8.7: Enhanced render.yaml with CI/CD

**Complete Blueprint with image-based deployment:**

```yaml
# render.yaml - Infrastructure as Code
# Deploy via: Render Dashboard → New → Blueprint

services:
  # API Service (image-based for zero build minutes)
  - type: web
    name: cms-pricing-api
    runtime: image
    image:
      url: ghcr.io/YOUR_ORG/cms-pricing-api:latest
      # For CI: url is updated via deploy hook with specific SHA tag
    plan: starter
    region: oregon
    healthCheckPath: /health
    autoDeploy: false  # CI controls deploys via deploy hook
    
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: cms-pricing-db
          property: connectionString
      - key: LOG_LEVEL
        value: INFO
      - key: ENVIRONMENT
        value: production
    
    # Scaling (optional)
    scaling:
      minInstances: 1
      maxInstances: 3
      targetMemoryPercent: 80
      targetCPUPercent: 80

  # Migration Job (runs alembic)
  - type: job
    name: run-migrations
    runtime: image
    image:
      url: ghcr.io/YOUR_ORG/cms-pricing-api:latest
    plan: starter
    command: alembic upgrade head
    
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
    ipAllowList: []  # Defaults to allow all; restrict in production
```

**Deploy this blueprint:**
1. Commit `render.yaml` to repository
2. Render Dashboard → New → Blueprint
3. Connect repository
4. Select `render.yaml`
5. Review and create

---

### 8.8: Monorepo Best Practices

**If you're in a monorepo:**

**1. Root Directory Configuration**
- Service Settings → Root Directory: `cms-api/`
- Dockerfile location: `cms-api/Dockerfile`
- Only matters for Python runtime (not image-based)

**2. Build Filters**
- Service Settings → Build Filters
- Include: `cms-api/**`
- This prevents rebuilds when other parts of monorepo change
- **Saves significant pipeline minutes in large repos**

**3. Dockerfile Context**

For monorepo, adjust Dockerfile COPY paths:

```dockerfile
# If Dockerfile is in cms-api/ subdirectory
COPY requirements.txt .
COPY cms_pricing/ ./cms_pricing/
COPY alembic/ ./alembic/
```

Or use build context in GitHub Actions:

```yaml
      - name: Build and push Docker image
        uses: docker/build-push-action@v5
        with:
          context: ./cms-api  # Build from subdirectory
          file: ./cms-api/Dockerfile
          push: true
```

**4. Path Filters in GitHub Actions**

```yaml
on:
  push:
    paths:
      - 'cms-api/**'           # Only trigger on cms-api changes
      - '!cms-api/docs/**'     # Ignore docs changes
      - '!cms-api/tests/**'    # Optionally ignore test changes
```

---

### 8.9: Testing the Full Pipeline

**End-to-end test:**

1. **Make a code change**
   ```bash
   # Edit cms_pricing/main.py
   echo "# CI/CD test" >> cms_pricing/main.py
   git add cms_pricing/main.py
   git commit -m "test: CI/CD pipeline"
   git push origin main
   ```

2. **Watch GitHub Actions**
   - Go to Actions tab
   - Watch "Deploy API to Render" workflow
   - Should complete in 3-5 minutes

3. **Verify image built**
   ```bash
   # Check GHCR for new image
   curl -H "Authorization: Bearer $GITHUB_TOKEN" \
     https://api.github.com/users/YOUR_ORG/packages/container/cms-pricing-api/versions
   ```

4. **Watch Render deployment**
   - Dashboard → Web Service → Events
   - Should show "Deploy triggered by Deploy Hook"
   - Wait for "Live" status (2-3 minutes)

5. **Test deployed API**
   ```bash
   curl https://cms-pricing-api.onrender.com/health
   ```

**Expected:** `{"status": "healthy", "database": "connected"}`

---

### 8.10: Troubleshooting CI/CD

**Issue: GitHub Actions failing to push image**

```
Error: denied: permission_denied: write_package
```

**Solution:**
- Check workflow has `packages: write` permission
- Verify GITHUB_TOKEN has access
- Ensure GHCR is enabled for organization

**Issue: Render deploy hook returns 404**

```
{"error": "Service not found"}
```

**Solution:**
- Verify deploy hook URL is correct
- Check service ID in URL matches your service
- Regenerate deploy hook if needed

**Issue: Render pulling wrong image tag**

**Solution:**
- Check deploy hook includes `imgURL` parameter:
  ```bash
  curl -X POST "$HOOK_URL&imgURL=ghcr.io/org/app:sha-abc123"
  ```
- Verify image exists in GHCR
- Check Render service Image URL setting

**Issue: Migration job fails**

```
alembic.util.exc.CommandError: Can't locate revision identified by 'xxx'
```

**Solution:**
- Ensure alembic/ directory is in Docker image
- Verify DATABASE_URL is set correctly
- Check alembic.ini paths are correct
- Run `alembic current` to see current state

---

### 8.11: Cost Impact Analysis

**Before CI/CD (manual deploys):**
- Render build minutes: ~5-10 min per deploy
- Monthly: ~50-100 build minutes
- Cost: Included in free tier or minimal

**After CI/CD (image-based):**
- Render build minutes: ~0 min (just pulls image)
- GitHub Actions minutes: ~3-5 min per deploy
- Monthly: ~30-50 GitHub Actions minutes
- Cost: Free tier usually sufficient

**Savings:**
- ✅ Zero Render build minutes
- ✅ Faster deploys (pulls vs builds)
- ✅ Consistent across environments
- ✅ Better caching (GitHub Actions)

**GitHub Actions free tier:**
- 2,000 minutes/month for public repos
- 500 minutes/month for private repos (macOS)
- Usually plenty for API deploys

---

### 8.12: Security Best Practices

**1. Never commit secrets**
```bash
# .gitignore
.env
.env.*
!.env.example
*.pem
*.key
```

**2. Use GitHub Secrets for:**
- RENDER_DEPLOY_HOOK_URL
- RENDER_API_KEY (if using API)
- Any third-party API keys

**3. Rotate credentials regularly**
- Render database passwords: Every 90 days
- Deploy hooks: After team changes
- API keys: Following service guidelines

**4. Restrict deploy hook access**
- Store only in GitHub Secrets
- Never log deploy hook URLs
- Regenerate if compromised

**5. Image scanning**

Add to GitHub Actions workflow:

```yaml
      - name: Scan image for vulnerabilities
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ github.sha }}
          format: 'table'
          exit-code: '1'
          severity: 'CRITICAL,HIGH'
```

---

### 8.13: Next Steps After CI/CD Setup

**Once automated:**

1. **Monitor first few deploys**
   - Watch GitHub Actions logs
   - Check Render deployment logs
   - Verify health checks pass

2. **Document for team**
   - Update README with CI/CD flow
   - Share deploy hook access (via 1Password)
   - Document rollback procedures

3. **Set up notifications**
   - Slack webhook for deploy events
   - Email alerts for failures
   - PagerDuty for production issues

4. **Implement staging environment**
   - Create staging Render service
   - Deploy on merge to `develop` branch
   - Promote to prod on tags

5. **Add deployment gates**
   - Require tests to pass
   - Code review approval
   - Security scanning

---

**Congratulations!** 🎉

You now have:
- ✅ Production PostgreSQL database
- ✅ Automated Docker builds (GitHub Actions)
- ✅ Zero-build-minute deploys (image-based)
- ✅ CI/CD pipeline (PR → merge → deploy)
- ✅ Migration automation (One-Off Jobs)
- ✅ Monorepo optimization (path filters)
- ✅ Infrastructure as Code (render.yaml)

**Total pipeline minutes:** ~0 on Render, ~3-5 on GitHub Actions per deploy

---

**Ready to deploy to Render? You have everything you need!** 🚀

---

## Part 9: CI/CD Automation Status (Added 2025-10-22)

### 9.1 Automated Deployment Pipeline

**Status:** ✅ **IMPLEMENTED**

The CMS Pricing API now has fully automated CI/CD following all PRD policies:

**What's automated:**
- ✅ Docker image builds on every push to `main`
- ✅ Image push to GitHub Container Registry (ghcr.io)
- ✅ Database migrations run before deployment (Render Job API)
- ✅ Deployment triggered on version tags (v*.*.*)
- ✅ Health checks validated after deployment

**Infrastructure:**
- ✅ `render.yaml` - Infrastructure-as-Code configuration
- ✅ `.github/workflows/deploy.yml` - CI/CD pipeline
- ✅ Render migration job - `run-migrations` one-off job
- ✅ GitHub secrets - API keys and service IDs configured

**Policy Compliance:**
- ✅ **STD-database-platform-prd-v1.0.md §3:** Migrations-first, no app startup DDL
- ✅ **PRD-render-hosting-prd-v1.0.md §3:** Image-based deployment, zero Render build minutes
- ✅ **RUN-database-migrations-prd-v1.0.md §6:** Migrations via Render Job/CI

### 9.2 Quick Start (Automated Deployment)

**For engineers deploying code:**

1. **Make changes and commit:**
   ```bash
   git add .
   git commit -m "feat: add new feature"
   git push origin main
   ```

2. **Create version tag:**
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

3. **Watch automation:**
   - GitHub Actions builds image and runs migrations
   - Render deploys new version automatically
   - Health checks validate deployment

4. **Verify:**
   ```bash
   curl https://cms-pricing-api.onrender.com/health
   ```

**That's it!** No manual Render dashboard interaction needed.

### 9.3 Required One-Time Setup

If this is your first deployment, complete these manual steps once:

#### Step 1: Create Render Migration Job

In Render Dashboard:
1. Navigate to your Web Service → Jobs tab
2. Create one-off job:
   - Name: `run-migrations`
   - Command: `alembic upgrade head`
   - Runtime: Image (same as web service)
   - Image URL: `ghcr.io/alex-bea/cms-api:latest`
3. Save

#### Step 2: Configure GitHub Secrets

In GitHub Repository Settings → Secrets and variables → Actions:

Add three new secrets:

**RENDER_API_KEY:**
- Render Dashboard → Account Settings → API Keys
- Create new API key → Copy the key

**RENDER_SERVICE_ID:**
- Your Render web service URL: `https://dashboard.render.com/web/srv-XXXXX`
- Copy the `srv-XXXXX` part

**RENDER_DEPLOY_HOOK:**
- Should already exist from manual setup
- If not: Render Dashboard → Settings → Deploy Hook → Create

#### Step 3: Deploy render.yaml (Optional)

For infrastructure-as-code approach:
```bash
# Commit render.yaml
git add render.yaml
git commit -m "infra: add Render IaC configuration"
git push origin main
```

Then in Render Dashboard:
1. New → Blueprint
2. Connect repository
3. Select `render.yaml`
4. Review and create

### 9.4 Migration from Manual to Automated

If you previously deployed manually (Parts 1-7), here's how to transition:

**Before:**
- Manual Docker builds
- Manual database migrations via `psql`
- Manual Render deploy hook triggers
- Manual health check verification

**After:**
- ✅ Automated on every version tag
- ✅ Migrations run first (fail-fast)
- ✅ Consistent deployments (image-based)
- ✅ Audit trail in GitHub Actions logs

**Migration steps:**
1. Complete one-time setup (§9.3)
2. Test with a dummy tag:
   ```bash
   git tag v0.0.1-test
   git push origin v0.0.1-test
   ```
3. Watch GitHub Actions → "Build and Deploy to Render"
4. Verify in Render Dashboard → Events
5. Once working, use for all future deployments

### 9.5 Troubleshooting CI/CD

**See:** `.github/workflows/README.md` for comprehensive troubleshooting

**Common issues:**

**Migration job not found:**
```
Failed to trigger migration job
```
→ Create the `run-migrations` job in Render Dashboard (§9.3 Step 1)

**API authentication failed:**
```
401 Unauthorized
```
→ Verify `RENDER_API_KEY` is valid and not expired

**Deployment hook returns 404:**
```
Service not found
```
→ Regenerate `RENDER_DEPLOY_HOOK` in Render Dashboard

**Migration times out:**
```
Migration job timed out after 5 minutes
```
→ Check migration complexity, optimize slow DDL (see RUN-database-migrations-prd-v1.0.md)

### 9.6 Monitoring Automated Deployments

**GitHub Actions:**
- Repository → Actions tab
- "Build and Deploy to Render" workflow
- Real-time logs for each step

**Render:**
- Dashboard → Events tab
- Deployment history with status
- Jobs tab → run-migrations logs

**Health Checks:**
```bash
# Quick check
curl https://cms-pricing-api.onrender.com/health

# Detailed check
curl -v https://cms-pricing-api.onrender.com/health 2>&1 | grep -E "HTTP|status"
```

### 9.7 Rollback Procedure

**If automated deployment fails:**

1. **Check what failed:**
   ```bash
   # GitHub Actions logs
   # Render deployment logs
   # Migration job logs
   ```

2. **Rollback via Render:**
   - Dashboard → Deploys tab
   - Select previous successful deploy
   - Click "Redeploy"

3. **Rollback database (if needed):**
   - See RUN-database-backup-dr-prd-v1.0.md for PITR procedures
   - Forward-fix preferred over downgrade (STD-database-platform-prd-v1.0.md §3)

4. **Fix and redeploy:**
   ```bash
   # Fix the issue
   git add .
   git commit -m "fix: resolve deployment issue"
   
   # Create new tag
   git tag v1.0.1
   git push origin v1.0.1
   ```

---

## Change Log

|| Version | Date | Summary |
||---------|------|---------|
|| **v1.3** | 2025-10-22 | **Added Part 9: CI/CD Automation Status** - Documented fully automated deployment pipeline implementation. Added render.yaml IaC configuration, updated deploy.yml workflow with migration automation, created comprehensive setup guide (§9.3), troubleshooting (§9.5), monitoring (§9.6), and rollback procedures (§9.7). CI/CD now enforces migrations-first policy via Render Job API with fail-fast on migration failures. Zero Render build minutes achieved via image-based deployment. |
|| **v1.2** | 2025-10-21 | **Added Part 7 API deployment troubleshooting** (5 critical issues from production deployment): 401 Unauthorized on health checks (authentication bypass required), Docker image caching on :latest tag (use :main or SHA tags), Out of Memory on Starter tier (memory optimization strategies), Permission denied for data directories (Dockerfile non-root user pattern), Schema contract validation failures (required fields). Added Part 7 Best Practices Summary with 7-point checklist. Based on cms-pricing-api deployment to Render. |
|| **v1.1** | 2025-10-21 | **Added 5 deployment learnings (Gaps 4-8).** Enhanced Part 2 Step 1 with PostgreSQL version note (Gap 6: Render may provision 17.x when 16 requested). Enhanced Part 2 Step 3 with detailed psql PATH setup for macOS/Linux (Gap 4: common "command not found" fix). Enhanced Part 2 Step 4 with strong .env security warnings and checklist (Gap 7: never commit credentials). Added Part 4 troubleshooting for Python segfaults (Gap 5: conda conflicts, venv solution). Added database-only deployment pattern (Gap 8: schema-ready vs data-ready states). Based on 2025-10-21 Render deployment experience. |
|| **v1.0** | 2025-10-21 | Initial production runbook with comprehensive CI/CD automation (Part 8), health checks, monitoring, and 8-part deployment process. |

---

**End of Render Deployment Guide**
