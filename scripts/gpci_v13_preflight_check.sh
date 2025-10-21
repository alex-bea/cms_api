#!/bin/bash
#
# GPCI v1.3 Pre-Flight Check Script
#
# This script verifies all prerequisites for the GPCI v1.3 migration.
# Run this BEFORE applying the migration to catch issues early.
#
# Usage:
#   ./scripts/gpci_v13_preflight_check.sh
#
# Exit codes:
#   0 - All checks passed, ready to migrate
#   1 - One or more checks failed, review output

set -e

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║           GPCI v1.3 Migration Pre-Flight Check                       ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

FAILED_CHECKS=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Check 1: Source File
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 1: GPCI Source File"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SOURCE_FILE="sample_data/rvu25d_0/GPCI2025.txt"

if [ -f "$SOURCE_FILE" ]; then
    FILE_SIZE=$(stat -f%z "$SOURCE_FILE" 2>/dev/null || stat -c%s "$SOURCE_FILE" 2>/dev/null)
    ROW_COUNT=$(wc -l < "$SOURCE_FILE")
    
    if [ "$ROW_COUNT" -ge 100 ] && [ "$ROW_COUNT" -le 120 ]; then
        echo -e "${GREEN}✅ PASS${NC}: Source file exists (${FILE_SIZE} bytes, ${ROW_COUNT} rows)"
    else
        echo -e "${RED}❌ FAIL${NC}: Row count ${ROW_COUNT} outside expected range [100-120]"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
    fi
else
    echo -e "${RED}❌ FAIL${NC}: Source file not found: $SOURCE_FILE"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
echo ""

# ============================================================================
# Check 2: Migration Files
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 2: Alembic Migration Files"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

MIGRATION_003="alembic/versions/003_gpci_v13_add_mac_to_nk.py"
MIGRATION_004="alembic/versions/004_gpci_v12_compat_view.py"

if [ -f "$MIGRATION_003" ] && [ -f "$MIGRATION_004" ]; then
    echo -e "${GREEN}✅ PASS${NC}: Migration files present"
    echo "       - 003_gpci_v13_add_mac_to_nk.py (unique index)"
    echo "       - 004_gpci_v12_compat_view.py (compat view)"
else
    echo -e "${RED}❌ FAIL${NC}: One or both migration files missing"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
echo ""

# ============================================================================
# Check 3: Backfill Script
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 3: Backfill Script"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

BACKFILL_SCRIPT="scripts/backfill_gpci_v13.py"

if [ -f "$BACKFILL_SCRIPT" ]; then
    if [ -x "$BACKFILL_SCRIPT" ] || head -1 "$BACKFILL_SCRIPT" | grep -q python; then
        echo -e "${GREEN}✅ PASS${NC}: Backfill script present and executable"
    else
        echo -e "${YELLOW}⚠️  WARN${NC}: Backfill script exists but may not be executable"
    fi
else
    echo -e "${RED}❌ FAIL${NC}: Backfill script not found: $BACKFILL_SCRIPT"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
echo ""

# ============================================================================
# Check 4: Python Environment
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 4: Python Environment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
    echo -e "${GREEN}✅ PASS${NC}: Python available (version $PYTHON_VERSION)"
    
    # Check for required packages
    echo "   Checking dependencies..."
    python -c "import pandas, sqlalchemy, structlog" 2>/dev/null && \
        echo -e "   ${GREEN}✅${NC} pandas, sqlalchemy, structlog" || \
        echo -e "   ${RED}❌${NC} Missing: pandas, sqlalchemy, or structlog"
else
    echo -e "${RED}❌ FAIL${NC}: Python not found in PATH"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
echo ""

# ============================================================================
# Check 5: Alembic
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 5: Alembic"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if command -v alembic &> /dev/null; then
    ALEMBIC_VERSION=$(alembic --version 2>&1 || echo "unknown")
    echo -e "${GREEN}✅ PASS${NC}: Alembic available ($ALEMBIC_VERSION)"
else
    echo -e "${RED}❌ FAIL${NC}: Alembic not found in PATH"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
echo ""

# ============================================================================
# Check 6: Database Connection (Optional)
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 6: Database Connection (Optional)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -n "$DATABASE_URL" ]; then
    echo "DATABASE_URL is set"
    
    # Try to connect
    if command -v psql &> /dev/null; then
        if psql "$DATABASE_URL" -c "SELECT 1;" &> /dev/null; then
            echo -e "${GREEN}✅ PASS${NC}: Database connection successful"
        else
            echo -e "${YELLOW}⚠️  WARN${NC}: DATABASE_URL set but connection failed"
            echo "   This check will be required when running the migration"
        fi
    else
        echo -e "${YELLOW}⚠️  WARN${NC}: psql not found, cannot verify connection"
    fi
else
    echo -e "${YELLOW}⚠️  INFO${NC}: DATABASE_URL not set (will be required for migration)"
fi
echo ""

# ============================================================================
# Check 7: GPCI Parser v1.3
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 7: GPCI Parser v1.3"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

PARSER_FILE="cms_pricing/ingestion/parsers/gpci_parser.py"
SCHEMA_FILE="cms_pricing/ingestion/contracts/cms_gpci_v1.3.json"

if [ -f "$PARSER_FILE" ] && [ -f "$SCHEMA_FILE" ]; then
    # Check if parser references v1.3
    if grep -q "cms_gpci_v1.3" "$PARSER_FILE"; then
        echo -e "${GREEN}✅ PASS${NC}: GPCI parser v1.3 present"
        
        # Check natural key definition
        if grep -q "mac.*locality_code.*effective_from" "$PARSER_FILE"; then
            echo "   ✅ Parser uses 3-field NK (mac, locality_code, effective_from)"
        else
            echo -e "   ${YELLOW}⚠️  WARN${NC}: Could not verify NK definition in parser"
        fi
    else
        echo -e "${RED}❌ FAIL${NC}: Parser does not reference cms_gpci_v1.3"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
    fi
else
    echo -e "${RED}❌ FAIL${NC}: Parser or schema file missing"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
echo ""

# ============================================================================
# Check 8: Operator Runbook
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Check 8: Operator Runbook"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

RUNBOOK=".cursor/plans/GPCI_V13_MIGRATION_GUIDE.md"

if [ -f "$RUNBOOK" ]; then
    RUNBOOK_LINES=$(wc -l < "$RUNBOOK")
    echo -e "${GREEN}✅ PASS${NC}: Operator runbook present (${RUNBOOK_LINES} lines)"
    echo "   Read before proceeding: .cursor/plans/GPCI_V13_MIGRATION_GUIDE.md"
else
    echo -e "${YELLOW}⚠️  WARN${NC}: Operator runbook not found (recommended but not required)"
fi
echo ""

# ============================================================================
# Summary
# ============================================================================
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                         PRE-FLIGHT SUMMARY                           ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

if [ $FAILED_CHECKS -eq 0 ]; then
    echo -e "${GREEN}✅ ALL CHECKS PASSED${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Review operator runbook: less .cursor/plans/GPCI_V13_MIGRATION_GUIDE.md"
    echo "  2. Backup database: pg_dump \$DATABASE_URL > backup_\$(date +%Y%m%d).sql"
    echo "  3. Apply migration: alembic upgrade head"
    echo "  4. Run backfill: python scripts/backfill_gpci_v13.py --commit"
    echo ""
    exit 0
else
    echo -e "${RED}❌ ${FAILED_CHECKS} CHECK(S) FAILED${NC}"
    echo ""
    echo "Fix the failed checks above before proceeding with migration."
    echo ""
    exit 1
fi

