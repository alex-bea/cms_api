# Database Setup Guide - First Deployment

**Purpose:** Get a PostgreSQL database ready for GPCI v1.3 migration  
**Your Situation:** First time deploying, need database connection  
**Estimated Time:** 10-30 minutes (depending on option chosen)

---

## Quick Recommendation 🎯

**For testing the migration:** Use **Option A (Local PostgreSQL)** - Fast, free, no signup  
**For production:** Use **Option B (Render)** or **Option C (Railway)** - Managed, reliable, easy

---

## Option A: Local PostgreSQL (Quickest for Testing) ⚡

**Best for:** Testing migration locally, development work  
**Time:** 10-15 minutes  
**Cost:** Free  
**Complexity:** Low

### Step 1: Install PostgreSQL

**On macOS (using Homebrew):**
```bash
# Install PostgreSQL
brew install postgresql@15

# Start PostgreSQL service
brew services start postgresql@15

# Verify it's running
psql postgres -c "SELECT version();"
```

**Alternative (Postgres.app):**
```bash
# Download from: https://postgresapp.com/
# Drag to Applications folder
# Click "Initialize" to create default server
# PostgreSQL is now running on localhost:5432
```

### Step 2: Create Database

```bash
# Create database
createdb cms_pricing

# Or use psql
psql postgres -c "CREATE DATABASE cms_pricing;"

# Verify
psql -l | grep cms_pricing
```

### Step 3: Set DATABASE_URL

```bash
# Add to your shell profile (~/.zshrc or ~/.bashrc)
export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"

# Or for current session only
export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"

# Test connection
psql $DATABASE_URL -c "SELECT 1;"
```

### Step 4: Initialize Schema

```bash
# Navigate to project
cd /Users/alexanderbea/Cursor/cms-api

# Run Alembic migrations (creates tables)
alembic upgrade head

# Verify tables exist
psql $DATABASE_URL -c "\dt"
```

**Pros:**
- ✅ Fast setup (< 15 min)
- ✅ Free, no signup
- ✅ Full control
- ✅ No network latency
- ✅ Perfect for testing migration

**Cons:**
- ❌ Manual backups required
- ❌ Runs only when Mac is on
- ❌ Not accessible from other machines
- ❌ No automatic scaling

---

## Option B: Render (Managed PostgreSQL) 🚀

**Best for:** Production deployment, managed service  
**Time:** 15-20 minutes (includes signup)  
**Cost:** Free tier available, $7/month for small production DB  
**Complexity:** Low

### Step 1: Sign Up & Create Database

1. **Go to:** https://render.com/
2. **Sign up** with GitHub (easiest)
3. **Create New PostgreSQL:**
   - Click "New +" → "PostgreSQL"
   - Name: `cms-pricing-db`
   - Database: `cms_pricing`
   - User: `cms_user`
   - Region: Choose closest to you
   - Instance Type: Free tier (for testing) or Starter ($7/month)
   - PostgreSQL Version: 15 or 16

### Step 2: Get Connection String

After creation, Render shows:
```
Internal Database URL:  postgresql://cms_user:xxxxx@dpg-xxxxx:5432/cms_pricing
External Database URL:  postgresql://cms_user:xxxxx@oregon-postgres.render.com:5432/cms_pricing
```

**Use External URL** for local development.

### Step 3: Set DATABASE_URL

```bash
# Copy External Database URL from Render dashboard
export DATABASE_URL="postgresql://cms_user:PASTE_PASSWORD_HERE@oregon-postgres.render.com:5432/cms_pricing"

# Test connection
psql $DATABASE_URL -c "SELECT 1;"
```

### Step 4: Initialize Schema

```bash
# Run migrations
alembic upgrade head

# Verify
psql $DATABASE_URL -c "\dt"
```

**Pros:**
- ✅ Managed (automatic backups, monitoring)
- ✅ Free tier available
- ✅ Easy to scale
- ✅ Accessible from anywhere
- ✅ SSL by default
- ✅ GitHub integration

**Cons:**
- ❌ Free tier limited (256MB storage, 90 days retention)
- ❌ Requires internet connection
- ❌ Some latency from local machine

**Pricing:**
- Free: 256MB, expires after 90 days of inactivity
- Starter: $7/month, 1GB, 7-day backups
- Standard: $20/month, 10GB, 14-day backups

---

## Option C: Railway (Developer-Friendly) 🚂

**Best for:** Fast setup, developer experience  
**Time:** 10-15 minutes  
**Cost:** $5 credit free, then usage-based (~$5-10/month for small DB)  
**Complexity:** Very Low

### Step 1: Sign Up & Create Database

1. **Go to:** https://railway.app/
2. **Sign up** with GitHub
3. **New Project** → "Deploy PostgreSQL"
4. **Wait** 30 seconds for deployment

### Step 2: Get Connection String

Railway automatically shows:
```
DATABASE_URL: postgresql://postgres:xxxxx@containers-us-west-xxx.railway.app:7432/railway
```

**Copy the entire DATABASE_URL** from the "Connect" tab.

### Step 3: Set DATABASE_URL

```bash
# Paste Railway's DATABASE_URL
export DATABASE_URL="postgresql://postgres:PASTE_FROM_RAILWAY"

# Test
psql $DATABASE_URL -c "SELECT 1;"
```

### Step 4: Initialize Schema

```bash
alembic upgrade head
psql $DATABASE_URL -c "\dt"
```

**Pros:**
- ✅ Fastest setup (< 10 min)
- ✅ Excellent developer experience
- ✅ Auto-backups
- ✅ Environment variables integrated
- ✅ GitHub deployments easy

**Cons:**
- ❌ No free tier (but $5 free credit)
- ❌ Usage-based pricing (can be unpredictable)

**Pricing:**
- $5 free credit (lasts 1-2 months for small DB)
- ~$5-10/month for 1GB database with backups

---

## Option D: Supabase (PostgreSQL + APIs) 🔋

**Best for:** If you want built-in APIs and auth  
**Time:** 15 minutes  
**Cost:** Free tier (500MB), $25/month for pro  
**Complexity:** Low

### Setup

1. **Go to:** https://supabase.com/
2. **Sign up** with GitHub
3. **New Project**
4. **Copy DATABASE_URL** from Settings → Database

**Pros:**
- ✅ Free tier (500MB, 2GB bandwidth)
- ✅ Built-in APIs (if you want them later)
- ✅ Auth system included
- ✅ Excellent dashboard

**Cons:**
- ❌ More features than you need (just for DB)
- ❌ Free tier limited to 2 projects

---

## Option E: Docker Compose (Local, Containerized) 🐳

**Best for:** Matching production setup, CI/CD  
**Time:** 10 minutes  
**Cost:** Free  
**Complexity:** Medium

### Setup

You already have `docker-compose.yml` in your repo! Let me check if it has PostgreSQL:

```bash
# Check your docker-compose.yml
cat docker-compose.yml | grep -A 10 postgres
```

If it has postgres, just:

```bash
# Start PostgreSQL container
docker-compose up -d postgres

# Get DATABASE_URL (check docker-compose.yml for exact values)
export DATABASE_URL="postgresql://postgres:password@localhost:5432/cms_pricing"

# Initialize
alembic upgrade head
```

**Pros:**
- ✅ Matches production setup
- ✅ Isolated from system
- ✅ Easy to reset
- ✅ Version-controlled config

**Cons:**
- ❌ Requires Docker Desktop
- ❌ Uses system resources

---

## My Recommendation for You 🎯

### **For Testing Migration (Next 30 Minutes):**

**→ Use Option A (Local PostgreSQL)**

**Why:**
- Fastest setup
- No signup required
- Perfect for testing migration
- Can delete easily after testing

**Quick Start:**
```bash
# 1. Install (if not already)
brew install postgresql@15
brew services start postgresql@15

# 2. Create database
createdb cms_pricing

# 3. Set URL
export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"

# 4. Test
psql $DATABASE_URL -c "SELECT 1;"

# 5. Initialize
alembic upgrade head

# 6. You're ready to migrate!
```

### **For Production Deployment (After Testing):**

**→ Use Option B (Render) or Option C (Railway)**

**Why:**
- Managed backups
- Monitoring included
- Reliable uptime
- Easy to scale
- Professional setup

**Render is good if:**
- You want simple, predictable pricing ($7/month)
- You prefer stability over features
- You want GitHub integration

**Railway is good if:**
- You want fastest setup
- You're okay with usage-based pricing
- You like modern developer tools

---

## Step-by-Step: Local PostgreSQL Setup (Recommended First)

### 1. Check if PostgreSQL is Already Installed

```bash
psql --version
```

**If you see a version:**
```
psql (PostgreSQL) 15.x
```
→ Skip to Step 3 (PostgreSQL already installed)

**If you see "command not found":**
→ Continue to Step 2

### 2. Install PostgreSQL

```bash
# Install via Homebrew
brew install postgresql@15

# Start service
brew services start postgresql@15

# Verify
psql --version
```

### 3. Create Database

```bash
# Create database named cms_pricing
createdb cms_pricing

# Verify it was created
psql -l | grep cms_pricing

# You should see:
# cms_pricing | your_username | UTF8 | ...
```

### 4. Test Connection

```bash
# Connect to database
psql cms_pricing

# You should see:
# psql (15.x)
# Type "help" for help.
# cms_pricing=#

# Type \q to exit
```

### 5. Set DATABASE_URL Environment Variable

```bash
# For current terminal session
export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"

# Verify it's set
echo $DATABASE_URL

# Test connection via URL
psql $DATABASE_URL -c "SELECT current_database(), current_user;"

# You should see:
#  current_database | current_user
# ------------------+---------------
#  cms_pricing      | your_username
```

### 6. Make DATABASE_URL Permanent (Optional)

```bash
# Add to your ~/.zshrc (if using zsh)
echo 'export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"' >> ~/.zshrc

# Or add to ~/.bashrc (if using bash)
echo 'export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"' >> ~/.bashrc

# Reload shell
source ~/.zshrc  # or source ~/.bashrc
```

### 7. Initialize Database Schema

```bash
# Navigate to project (if not already there)
cd /Users/alexanderbea/Cursor/cms-api

# Run Alembic migrations (creates all tables)
alembic upgrade head

# You should see:
# INFO  [alembic.runtime.migration] Running upgrade -> 001_add_nearest_zip_tables
# INFO  [alembic.runtime.migration] Running upgrade 001 -> 002_add_nber_centroids
# INFO  [alembic.runtime.migration] Running upgrade 002 -> 003_gpci_v13_add_mac_to_nk
# INFO  [alembic.runtime.migration] Running upgrade 003 -> 004_gpci_v12_compat_view
```

### 8. Verify Tables Created

```bash
# List all tables
psql $DATABASE_URL -c "\dt"

# You should see tables like:
# - gpci_indices
# - releases
# - alembic_version
# - etc.

# Check gpci_indices specifically
psql $DATABASE_URL -c "\d gpci_indices"

# You should see column definitions, indexes, etc.
```

### 9. Verify GPCI v1.3 Migration Applied

```bash
# Check current Alembic version
alembic current

# You should see:
# 004_gpci_v12_compat_view (head)

# Check for v1.3 unique index
psql $DATABASE_URL -c "
  SELECT indexname, indexdef 
  FROM pg_indexes 
  WHERE tablename = 'gpci_indices' 
    AND indexname = 'uq_gpci_mac_locality_effective';
"

# If migration 003 was applied, you'll see the unique index
# If not, the migration will be applied in the migration process
```

### 10. You're Ready!

```bash
# Run preflight check
./scripts/gpci_v13_preflight_check.sh

# Should now show:
# ✅ PASS: Database connection successful
# ✅ ALL CHECKS PASSED
```

---

## DATABASE_URL Format Reference

### General Format:
```
postgresql://[username]:[password]@[host]:[port]/[database]
```

### Examples by Option:

**Local PostgreSQL:**
```bash
# No password (trust authentication)
postgresql://alexanderbea@localhost:5432/cms_pricing

# With password
postgresql://alexanderbea:mypassword@localhost:5432/cms_pricing

# Using default postgres user
postgresql://postgres:postgres@localhost:5432/cms_pricing
```

**Render:**
```bash
postgresql://cms_user:dpg-xxxx-password@dpg-xxxx-oregon-postgres.render.com:5432/cms_pricing
```

**Railway:**
```bash
postgresql://postgres:xxxx-password-xxxx@containers-us-west-12.railway.app:7432/railway
```

**Supabase:**
```bash
postgresql://postgres.xxxx:password@aws-0-us-west-1.pooler.supabase.com:6543/postgres
```

---

## Troubleshooting

### Issue: "psql: command not found"

**Solution:**
```bash
# Install PostgreSQL
brew install postgresql@15

# Add to PATH (if needed)
echo 'export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Issue: "connection refused"

**Solution:**
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql

# If not running, start it
brew services start postgresql@15

# Or restart
brew services restart postgresql@15
```

### Issue: "database does not exist"

**Solution:**
```bash
# Create the database
createdb cms_pricing

# Or
psql postgres -c "CREATE DATABASE cms_pricing;"
```

### Issue: "password authentication failed"

**Solution:**
```bash
# For local PostgreSQL, check pg_hba.conf
# Usually at: /opt/homebrew/var/postgresql@15/pg_hba.conf

# Change authentication method to 'trust' for local connections
# Or create password:
psql postgres -c "ALTER USER $(whoami) WITH PASSWORD 'yourpassword';"

# Then use:
export DATABASE_URL="postgresql://$(whoami):yourpassword@localhost:5432/cms_pricing"
```

### Issue: "Alembic can't find database"

**Solution:**
```bash
# Verify DATABASE_URL is set
echo $DATABASE_URL

# Check alembic.ini
cat alembic.ini | grep sqlalchemy.url

# Should be:
# sqlalchemy.url = %(DATABASE_URL)s

# Or set in alembic.ini directly:
# sqlalchemy.url = postgresql://user@localhost:5432/cms_pricing
```

---

## Complete First-Time Setup Script

**Copy-paste this entire block:**

```bash
#!/bin/bash
# Complete first-time database setup for GPCI v1.3 migration

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║         GPCI v1.3 Migration - Database Setup                        ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "Installing PostgreSQL..."
    brew install postgresql@15
    brew services start postgresql@15
    sleep 5  # Wait for service to start
else
    echo "✅ PostgreSQL already installed"
fi

# Create database
echo ""
echo "Creating database..."
createdb cms_pricing 2>/dev/null && echo "✅ Database created" || echo "✅ Database already exists"

# Set DATABASE_URL
echo ""
echo "Setting DATABASE_URL..."
export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"
echo "✅ DATABASE_URL set to: $DATABASE_URL"

# Test connection
echo ""
echo "Testing connection..."
if psql $DATABASE_URL -c "SELECT 1;" &> /dev/null; then
    echo "✅ Database connection successful"
else
    echo "❌ Connection failed - check troubleshooting section"
    exit 1
fi

# Initialize schema
echo ""
echo "Initializing schema with Alembic..."
cd /Users/alexanderbea/Cursor/cms-api
alembic upgrade head

# Verify
echo ""
echo "Verifying tables created..."
psql $DATABASE_URL -c "\dt" | head -10

# Check for GPCI table
echo ""
echo "Checking GPCI table..."
psql $DATABASE_URL -c "SELECT COUNT(*) as row_count FROM gpci_indices;" 2>/dev/null || echo "ℹ️ GPCI table empty (expected before migration)"

# Check for v1.3 unique index
echo ""
echo "Checking for GPCI v1.3 unique index..."
psql $DATABASE_URL -c "
  SELECT indexname 
  FROM pg_indexes 
  WHERE tablename = 'gpci_indices' 
    AND indexname = 'uq_gpci_mac_locality_effective';
" | grep uq_gpci && echo "✅ v1.3 index present" || echo "ℹ️ Index not yet present (will be created during migration 003)"

# Run preflight check
echo ""
echo "Running preflight check..."
./scripts/gpci_v13_preflight_check.sh

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                     SETUP COMPLETE                                   ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "✅ Database ready for GPCI v1.3 migration"
echo ""
echo "Next steps:"
echo "  1. Review deployment checklist: .cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md"
echo "  2. Run dry-run: python scripts/backfill_gpci_v13.py --dry-run"
echo "  3. Execute migration: Follow quick start guide"
echo ""
```

**Save this as `setup_database.sh` and run:**
```bash
chmod +x setup_database.sh
./setup_database.sh
```

---

## After Database is Ready

### Run the Preflight Check

```bash
./scripts/gpci_v13_preflight_check.sh
```

**Expected output:**
```
✅ PASS: Source file (118 rows)
✅ PASS: Migration files (003, 004)
✅ PASS: Python 3.12.7
✅ PASS: Dependencies (pandas, sqlalchemy, structlog)
✅ PASS: Alembic 1.16.5
✅ PASS: Database connection successful  ← Should now pass!
✅ PASS: Parser v1.3
✅ PASS: Operator runbook

╔══════════════════════════════════════════════════════════════╗
║ ✅ ALL CHECKS PASSED                                         ║
╚══════════════════════════════════════════════════════════════╝
```

### Test the Migration Process

```bash
# Dry-run backfill (no changes committed)
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --dry-run

# Expected output:
#   ✅ Parsed 109 rows with v1.3
#   ✅ Verified: 0 duplicates on ['mac', 'locality_code', 'effective_from']
#   🔍 DRY RUN: Would delete old GPCI rows
#   🔍 DRY RUN: Would load 109 new rows
#   ✅ DRY RUN SUCCESSFUL
```

### Execute the Migration

Follow the deployment checklist:
```bash
cat .cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md
```

Or quick start:
```bash
cat .cursor/plans/GPCI_V13_QUICK_START.md
```

---

## Comparison Matrix

| Feature | Local PostgreSQL | Render | Railway | Supabase |
|---------|------------------|--------|---------|----------|
| **Setup Time** | 10 min | 15 min | 10 min | 15 min |
| **Cost** | Free | Free tier + $7/month | $5 credit + usage | Free tier + $25/month |
| **Best For** | Testing | Production | Developer experience | APIs + DB |
| **Backups** | Manual | Automatic | Automatic | Automatic |
| **Monitoring** | Manual | Included | Included | Excellent |
| **Scalability** | Manual | Easy | Easy | Easy |
| **Complexity** | Low | Low | Very Low | Low |
| **Reliability** | Good | Excellent | Excellent | Excellent |
| **Network Required** | No | Yes | Yes | Yes |

---

## FAQ

**Q: Which option should I choose?**  
A: For testing the migration right now → Local PostgreSQL. For production later → Render or Railway.

**Q: Can I start with local and migrate to cloud later?**  
A: Yes! Just `pg_dump` your local database and `pg_restore` to cloud database.

**Q: Do I need to sign up for anything to test the migration?**  
A: No, local PostgreSQL requires no signup or payment.

**Q: What if I already have PostgreSQL installed?**  
A: Great! Just create the database (`createdb cms_pricing`) and set `DATABASE_URL`.

**Q: Can I use Docker instead?**  
A: Yes, check if `docker-compose.yml` has PostgreSQL config, then `docker-compose up -d`.

**Q: How do I switch between local and cloud databases?**  
A: Just change the `DATABASE_URL` environment variable.

---

## Next Steps After Database Setup

1. ✅ **Database ready**
2. ⏭️ **Run preflight check** (`./scripts/gpci_v13_preflight_check.sh`)
3. ⏭️ **Test backfill dry-run** (`python scripts/backfill_gpci_v13.py --dry-run`)
4. ⏭️ **Review deployment checklist** (`.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md`)
5. ⏭️ **Execute migration** (follow quick start guide)

---

**Ready to set up the database? Let me know which option you'd like to use, and I can guide you through it!**

**End of Database Setup Guide**

