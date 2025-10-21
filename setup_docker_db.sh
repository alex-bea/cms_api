#!/bin/bash
#
# Quick Docker Database Setup for GPCI v1.3 Migration
#
# This script:
# 1. Starts PostgreSQL container from your docker-compose.yml
# 2. Waits for database to be ready
# 3. Sets DATABASE_URL
# 4. Runs Alembic migrations (initializes schema)
# 5. Verifies GPCI v1.3 migration applied
# 6. Runs preflight check
#
# Usage: ./setup_docker_db.sh

set -e

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║         GPCI v1.3 - Docker Database Setup                           ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# ============================================================================
# Step 1: Start PostgreSQL Container
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 1: Starting PostgreSQL container..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker-compose up -d db

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ PostgreSQL container started${NC}"
else
    echo -e "${RED}❌ Failed to start PostgreSQL${NC}"
    echo "Troubleshooting:"
    echo "  - Is Docker running? (check Docker Desktop)"
    echo "  - Is port 5432 available? Run: lsof -i :5432"
    exit 1
fi

# ============================================================================
# Step 2: Wait for Database to be Ready
# ============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 2: Waiting for database to be ready..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

MAX_RETRIES=12
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose exec -T db pg_isready -U cms_user -d cms_pricing &> /dev/null; then
        echo -e "${GREEN}✅ Database ready (took $((RETRY_COUNT * 5)) seconds)${NC}"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
            echo -e "${RED}❌ Database not ready after 60 seconds${NC}"
            echo "Check logs: docker-compose logs db"
            exit 1
        fi
        echo "⏳ Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
        sleep 5
    fi
done

# ============================================================================
# Step 3: Set DATABASE_URL
# ============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 3: Setting DATABASE_URL..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

export DATABASE_URL="postgresql://cms_user:cms_password@localhost:5432/cms_pricing"
echo -e "${GREEN}✅ DATABASE_URL set${NC}"
echo "   $DATABASE_URL"

# Test connection from host
echo ""
echo "Testing connection from host machine..."
if psql $DATABASE_URL -c "SELECT current_database(), current_user, version();" &> /dev/null; then
    echo -e "${GREEN}✅ Connection successful${NC}"
    
    # Show database info
    psql $DATABASE_URL -c "SELECT current_database() as database, current_user as user, version();" | head -5
else
    echo -e "${RED}❌ Connection failed${NC}"
    echo "Troubleshooting:"
    echo "  - Ensure psql is installed: brew install postgresql@15"
    echo "  - Check container: docker-compose ps"
    exit 1
fi

# ============================================================================
# Step 4: Initialize Schema
# ============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 4: Initializing database schema..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check current alembic version
CURRENT_VERSION=$(alembic current 2>&1 | grep -v INFO | head -1)
echo "Current Alembic version: $CURRENT_VERSION"

# Run migrations
echo ""
echo "Running Alembic migrations..."
alembic upgrade head

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Schema initialized${NC}"
else
    echo -e "${RED}❌ Migration failed${NC}"
    exit 1
fi

# ============================================================================
# Step 5: Verify GPCI v1.3 Migration
# ============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 5: Verifying GPCI v1.3 migration..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check for v1.3 unique index
if psql $DATABASE_URL -c "SELECT indexname FROM pg_indexes WHERE tablename = 'gpci_indices' AND indexname = 'uq_gpci_mac_locality_effective';" | grep -q uq_gpci; then
    echo -e "${GREEN}✅ GPCI v1.3 unique index present${NC}"
    echo "   Index: uq_gpci_mac_locality_effective"
    echo "   Natural Key: (mac, locality_id, effective_start)"
else
    echo -e "${YELLOW}⚠️  GPCI v1.3 index not found${NC}"
    echo "   This is okay if you haven't run migration 003 yet"
fi

# Check for compat view
if psql $DATABASE_URL -c "SELECT viewname FROM pg_views WHERE viewname = 'gpci_indices_v12_compat';" | grep -q gpci_indices_v12_compat; then
    echo -e "${GREEN}✅ GPCI v1.2 compatibility view present${NC}"
else
    echo -e "${YELLOW}ℹ️  Compatibility view not found (optional)${NC}"
fi

# List tables
echo ""
echo "Tables created:"
psql $DATABASE_URL -c "\dt" | head -15

# ============================================================================
# Step 6: Run Preflight Check
# ============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 6: Running preflight check..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

./scripts/gpci_v13_preflight_check.sh

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                     DATABASE SETUP COMPLETE                          ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo -e "${GREEN}✅ PostgreSQL running in Docker${NC}"
echo -e "${GREEN}✅ Database: cms_pricing${NC}"
echo -e "${GREEN}✅ Schema initialized (all migrations applied)${NC}"
echo -e "${GREEN}✅ DATABASE_URL configured${NC}"
echo ""
echo "Database URL (for your .zshrc or .bashrc):"
echo "  export DATABASE_URL=\"postgresql://cms_user:cms_password@localhost:5432/cms_pricing\""
echo ""
echo "Useful Commands:"
echo "  - Connect: psql \$DATABASE_URL"
echo "  - Backup: docker-compose exec db pg_dump -U cms_user cms_pricing > backup.sql"
echo "  - Restart: docker-compose restart db"
echo "  - Stop: docker-compose stop db"
echo "  - View logs: docker-compose logs -f db"
echo ""
echo "Next steps:"
echo "  1. Add DATABASE_URL to your ~/.zshrc (see above)"
echo "  2. Test backfill: python scripts/backfill_gpci_v13.py --dry-run"
echo "  3. Follow deployment checklist: .cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md"
echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║         Ready to execute GPCI v1.3 migration! 🚀                     ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"

