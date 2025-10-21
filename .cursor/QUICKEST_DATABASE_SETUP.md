# Quickest Database Setup - You Already Have Docker! 🐳

**GREAT NEWS:** Your `docker-compose.yml` already has PostgreSQL configured!

**Setup Time:** ⚡ **< 5 MINUTES**  
**Cost:** Free  
**Complexity:** Very Low (just 3 commands)

---

## The Easiest Path (You Already Have Everything!) 🎯

### Quick Start (3 Commands)

```bash
# 1. Start PostgreSQL container
docker-compose up -d db

# 2. Set DATABASE_URL (use localhost since you're connecting from host machine)
export DATABASE_URL="postgresql://cms_user:cms_password@localhost:5432/cms_pricing"

# 3. Initialize schema
alembic upgrade head
```

**That's it!** ✅ You're ready to migrate.

---

## Detailed Step-by-Step

### Step 1: Start PostgreSQL Container

```bash
cd /Users/alexanderbea/Cursor/cms-api

# Start just the database service
docker-compose up -d db

# You should see:
# [+] Running 2/2
#  ✔ Network cms-api_default     Created
#  ✔ Container cms-api-db-1      Started
```

### Step 2: Verify Database is Running

```bash
# Check container status
docker-compose ps

# You should see something like:
# NAME           IMAGE                COMMAND                  SERVICE   STATUS    PORTS
# cms-api-db-1   postgres:15-alpine   "docker-entrypoint.s…"   db        Up        0.0.0.0:5432->5432/tcp

# Test health check
docker-compose exec db pg_isready -U cms_user -d cms_pricing

# You should see:
# /var/lib/postgresql/data/pgdata:5432 - accepting connections
```

### Step 3: Set DATABASE_URL

**Your docker-compose.yml has these credentials:**
- **User:** `cms_user`
- **Password:** `cms_password`
- **Database:** `cms_pricing`
- **Port:** `5432`

```bash
# Set for current session
export DATABASE_URL="postgresql://cms_user:cms_password@localhost:5432/cms_pricing"

# Test connection from host machine
psql $DATABASE_URL -c "SELECT current_database(), current_user;"

# You should see:
#  current_database | current_user
# ------------------+--------------
#  cms_pricing      | cms_user
```

### Step 4: Make DATABASE_URL Permanent (Optional)

```bash
# Add to ~/.zshrc
echo 'export DATABASE_URL="postgresql://cms_user:cms_password@localhost:5432/cms_pricing"' >> ~/.zshrc

# Reload
source ~/.zshrc

# Verify
echo $DATABASE_URL
```

### Step 5: Initialize Database Schema

```bash
# Run all Alembic migrations (including GPCI v1.3)
alembic upgrade head

# Expected output:
# INFO  [alembic.runtime.migration] Running upgrade -> 001_add_nearest_zip_tables
# INFO  [alembic.runtime.migration] Running upgrade 001 -> 002_add_nber_centroids
# INFO  [alembic.runtime.migration] Running upgrade 002 -> 003_gpci_v13_add_mac_to_nk
# ✅ GPCI v1.3 migration complete
# Added unique index: uq_gpci_mac_locality_effective
# INFO  [alembic.runtime.migration] Running upgrade 003 -> 004_gpci_v12_compat_view
# ✅ Created GPCI v1.2 compatibility view
```

### Step 6: Verify GPCI v1.3 is Applied

```bash
# Check current Alembic version
alembic current

# You should see:
# 004_gpci_v12_compat_view (head)

# Verify v1.3 unique index exists
psql $DATABASE_URL -c "
  SELECT indexname, indexdef 
  FROM pg_indexes 
  WHERE tablename = 'gpci_indices' 
    AND indexname = 'uq_gpci_mac_locality_effective';
"

# You should see 1 row with the unique index definition
```

### Step 7: Check Table Structure

```bash
# View GPCI table
psql $DATABASE_URL -c "\d gpci_indices"

# You should see:
# - Column: id (uuid, primary key)
# - Column: mac (character varying)
# - Column: locality_id (character varying)
# - Column: effective_start (date)
# - Index: uq_gpci_mac_locality_effective UNIQUE (mac, locality_id, effective_start)
```

### Step 8: Run Preflight Check

```bash
./scripts/gpci_v13_preflight_check.sh

# Should now show all green:
# ✅ PASS: Database connection successful
# ✅ ALL CHECKS PASSED
```

---

## What Just Happened?

1. ✅ **PostgreSQL started** in Docker container
2. ✅ **Database created** (`cms_pricing`)
3. ✅ **Schema initialized** (all tables created)
4. ✅ **GPCI v1.3 migration applied** (unique index created)
5. ✅ **Compat view created** (optional backwards compatibility)

**Current State:**
- Database is empty (0 rows in `gpci_indices`)
- Schema v1.3 is ready
- Unique index is in place
- Ready to load data via backfill script

---

## Now Run the Backfill

**Since migrations already ran, you just need to load data:**

```bash
# Parse GPCI data and load it
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --commit

# Expected output:
#   ✅ Parsed 109 rows with v1.3
#   ✅ Verified: 0 duplicates on ['mac', 'locality_code', 'effective_from']
#   ✅ Loaded 109 new rows
#   ✅ BACKFILL COMPLETE
```

---

## Verification

```bash
# Check row count
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
# Expected: 109

# Check for duplicates (should be 0)
psql $DATABASE_URL -c "
  SELECT mac, locality_id, effective_start, COUNT(*) 
  FROM gpci_indices 
  GROUP BY mac, locality_id, effective_start 
  HAVING COUNT(*) > 1;
"
# Expected: 0 rows

# Check ambiguous locality_id='00' (should have multiple MACs)
psql $DATABASE_URL -c "
  SELECT mac, locality_id, locality_name 
  FROM gpci_indices 
  WHERE locality_id = '00' 
  ORDER BY mac 
  LIMIT 10;
"
# Expected: Multiple rows (different MACs for different states)

# Run parser tests
pytest tests/ingestion/test_gpci_parser_golden.py -v
# Expected: 20/20 passing
```

---

## Managing Docker Database

### Start Database
```bash
docker-compose up -d db
```

### Stop Database
```bash
docker-compose stop db
```

### Restart Database
```bash
docker-compose restart db
```

### View Logs
```bash
docker-compose logs -f db
```

### Connect Directly to Database
```bash
# Via docker exec
docker-compose exec db psql -U cms_user -d cms_pricing

# Or via psql from host
psql postgresql://cms_user:cms_password@localhost:5432/cms_pricing
```

### Backup Database
```bash
# Via docker exec
docker-compose exec db pg_dump -U cms_user cms_pricing > backup_$(date +%Y%m%d).sql

# Or from host
pg_dump postgresql://cms_user:cms_password@localhost:5432/cms_pricing > backup_$(date +%Y%m%d).sql
```

### Reset Database (Start Fresh)
```bash
# Stop and remove containers + volumes
docker-compose down -v

# Start again (fresh database)
docker-compose up -d db

# Re-initialize schema
alembic upgrade head
```

---

## Advantages of Using Docker

✅ **Already configured** - You have docker-compose.yml ready  
✅ **Isolated** - No conflicts with system PostgreSQL  
✅ **Reproducible** - Same setup on any machine  
✅ **Easy reset** - Just `docker-compose down -v` to start fresh  
✅ **Version pinned** - PostgreSQL 15-alpine (stable)  
✅ **Health checks** - Built-in monitoring  

---

## Complete Setup Script for Docker

```bash
#!/bin/bash
# Quickest database setup using your existing docker-compose.yml

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║         GPCI v1.3 - Quick Database Setup (Docker)                   ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

cd /Users/alexanderbea/Cursor/cms-api

# Start PostgreSQL
echo "Starting PostgreSQL container..."
docker-compose up -d db

# Wait for health check
echo "Waiting for database to be ready..."
sleep 5

# Check health
if docker-compose exec db pg_isready -U cms_user -d cms_pricing &> /dev/null; then
    echo "✅ PostgreSQL ready"
else
    echo "⏳ Waiting a bit longer..."
    sleep 5
fi

# Set DATABASE_URL
export DATABASE_URL="postgresql://cms_user:cms_password@localhost:5432/cms_pricing"
echo "✅ DATABASE_URL set"

# Test connection
if psql $DATABASE_URL -c "SELECT 1;" &> /dev/null; then
    echo "✅ Database connection successful"
else
    echo "❌ Connection failed"
    echo "   Troubleshooting:"
    echo "   - Check if port 5432 is available: lsof -i :5432"
    echo "   - Check container logs: docker-compose logs db"
    exit 1
fi

# Initialize schema
echo ""
echo "Initializing schema..."
alembic upgrade head

# Verify v1.3 index
echo ""
echo "Verifying GPCI v1.3 unique index..."
if psql $DATABASE_URL -c "SELECT 1 FROM pg_indexes WHERE indexname = 'uq_gpci_mac_locality_effective';" | grep -q 1; then
    echo "✅ GPCI v1.3 unique index present"
else
    echo "ℹ️  Index not found (this is okay if migration 003 hasn't run yet)"
fi

# Run preflight check
echo ""
echo "Running preflight check..."
./scripts/gpci_v13_preflight_check.sh

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                     DATABASE READY                                   ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "✅ PostgreSQL running in Docker"
echo "✅ Database: cms_pricing"
echo "✅ Schema initialized"
echo "✅ DATABASE_URL: $DATABASE_URL"
echo ""
echo "Next steps:"
echo "  1. Test backfill: python scripts/backfill_gpci_v13.py --dry-run"
echo "  2. Follow deployment checklist: .cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md"
echo ""
```

**Save as `setup_docker_db.sh` and run:**
```bash
chmod +x setup_docker_db.sh
./setup_docker_db.sh
```

---

## Summary

**You have 3 easy options:**

### **🥇 EASIEST: Use Docker** (< 5 min)
```bash
docker-compose up -d db
export DATABASE_URL="postgresql://cms_user:cms_password@localhost:5432/cms_pricing"
alembic upgrade head
```

### **🥈 FAST: Local PostgreSQL** (10 min)
```bash
brew install postgresql@15
brew services start postgresql@15
createdb cms_pricing
export DATABASE_URL="postgresql://$(whoami)@localhost:5432/cms_pricing"
alembic upgrade head
```

### **🥉 PRODUCTION: Render/Railway** (15 min)
- Signup required
- Managed service
- Best for production deployment

---

**My Recommendation: Start with Docker (you already have it configured!)** 🐳

Would you like me to create the setup script and guide you through the Docker setup?

