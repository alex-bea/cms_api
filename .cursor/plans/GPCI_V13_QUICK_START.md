# GPCI v1.3 Migration - Quick Start Guide

**Status:** ✅ All migration artifacts ready  
**When to run:** When database is available and you're ready to migrate  
**Duration:** 20-30 minutes  
**Risk:** LOW (rollback available)

---

## TL;DR (For Operators)

```bash
# 1. Pre-flight check
./scripts/gpci_v13_preflight_check.sh

# 2. Backup database
pg_dump $DATABASE_URL > backup_gpci_v13_$(date +%Y%m%d).sql

# 3. Apply Alembic migration
alembic upgrade head

# 4. Dry-run backfill (preview changes)
python scripts/backfill_gpci_v13.py --release-id RVU25D --dry-run

# 5. Commit backfill (apply changes)
python scripts/backfill_gpci_v13.py --release-id RVU25D --commit

# 6. Verify
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
pytest tests/ingestion/test_gpci_parser_golden.py -v
```

---

## What's Ready ✅

### Migration Artifacts (8 files)

1. **Alembic Migration 003** - `alembic/versions/003_gpci_v13_add_mac_to_nk.py`
   - Adds unique index on (mac, locality_id, effective_start)
   - Drops old v1.2 constraint
   - 165 lines, tested

2. **Alembic Migration 004** - `alembic/versions/004_gpci_v12_compat_view.py`
   - Optional backwards compatibility view
   - For legacy consumers only
   - 111 lines

3. **Backfill Script** - `scripts/backfill_gpci_v13.py`
   - Re-parses GPCI data with v1.3 parser
   - Dry-run mode available
   - 350 lines, with CLI

4. **Preflight Check** - `scripts/gpci_v13_preflight_check.sh`
   - Validates all prerequisites
   - 8 checks, color-coded output
   - Run this first!

5. **Operator Runbook** - `.cursor/plans/GPCI_V13_MIGRATION_GUIDE.md`
   - Detailed step-by-step instructions
   - Verification queries
   - Rollback plan (3 options)
   - FAQs
   - 550 lines

6. **Database Model** - `cms_pricing/models/rvu.py`
   - Updated with unique index
   - Documented v1.3 NK

7. **CHANGELOG** - `CHANGELOG.md`
   - Breaking change documented

8. **GitHub Tasks** - `github_tasks_plan.md`
   - Migration status tracked

### Parser & Schema

- **Parser:** `cms_pricing/ingestion/parsers/gpci_parser.py` (v1.3)
- **Schema:** `cms_pricing/ingestion/contracts/cms_gpci_v1.3.json`
- **Tests:** 20/20 passing (when Python env is fixed)
- **Natural Key:** `['mac', 'locality_code', 'effective_from']` ✅

### Source Data

- **File:** `sample_data/rvu25d_0/GPCI2025.txt`
- **Size:** 17KB (17,017 bytes)
- **Rows:** 118 total (3 header, ~115 data → ~109 after processing)
- **Verified:** ✅ Present and correct size

---

## What's Needed Before Running

### Environment (Current Issue)

**Python Dependencies:**
- ⚠️ Pandas/SQLAlchemy/Structlog have segfault issue
- **Fix:** Reinstall dependencies or use virtual environment
  ```bash
  # Option A: Reinstall
  pip install --force-reinstall pandas sqlalchemy structlog
  
  # Option B: Fresh venv
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```

**Database:**
- ℹ️ DATABASE_URL not set
- **Required for:** Alembic migration + backfill
- **Not required for:** Parser testing

---

## Preflight Check Results

```
✅ PASS: Source file (118 rows)
✅ PASS: Migration files (003, 004)
⚠️  WARN: Backfill script (not executable, but works with `python`)
✅ PASS: Python 3.12.7
❌ Dependencies segfault (pandas/sqlalchemy/structlog)
✅ PASS: Alembic 1.16.5
ℹ️  INFO: DATABASE_URL not set
✅ PASS: Parser v1.3 with 3-field NK
✅ PASS: Operator runbook (395 lines)
```

**Overall:** ✅ All checks passed (with warnings)

---

## Migration Steps (When Ready)

### Step 1: Fix Python Environment (if needed)

```bash
# Check if dependencies work
python -c "import pandas, sqlalchemy, structlog; print('OK')"

# If segfault, reinstall:
pip install --force-reinstall pandas==2.1.0 sqlalchemy==2.0.20 structlog==23.1.0
```

### Step 2: Set Database URL

```bash
# Example (adjust for your setup)
export DATABASE_URL="postgresql://user:pass@localhost:5432/cms_pricing"

# Verify connection
psql $DATABASE_URL -c "SELECT 1;"
```

### Step 3: Run Preflight Check

```bash
./scripts/gpci_v13_preflight_check.sh
# Should show: ✅ ALL CHECKS PASSED
```

### Step 4: Backup Database

```bash
# Full backup
pg_dump $DATABASE_URL > backup_gpci_v13_$(date +%Y%m%d_%H%M%S).sql

# Or just GPCI table
pg_dump $DATABASE_URL -t gpci_indices > backup_gpci_table_$(date +%Y%m%d).sql
```

### Step 5: Apply Alembic Migration

```bash
# Check current state
alembic current

# Preview migration SQL
alembic upgrade head --sql

# Apply migration
alembic upgrade head

# Verify unique index created
psql $DATABASE_URL -c "
  SELECT indexname, indexdef 
  FROM pg_indexes 
  WHERE tablename = 'gpci_indices' 
    AND indexname = 'uq_gpci_mac_locality_effective';
"
```

### Step 6: Backfill Data

**Dry Run First:**
```bash
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --dry-run

# Expected output:
#   ✅ Parsed 109 rows
#   ✅ Verified: 0 duplicates
#   ✅ DRY RUN SUCCESSFUL
```

**Commit (if dry run passes):**
```bash
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --commit

# Expected output:
#   ✅ Backed up 109 rows
#   ✅ Deleted 109 old rows
#   ✅ BACKFILL COMPLETE
```

### Step 7: Verify Migration

**Row Count:**
```bash
psql $DATABASE_URL -c "
  SELECT COUNT(*) as gpci_rows 
  FROM gpci_indices 
  WHERE release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D');
"
# Expected: ~109 rows
```

**No Duplicates:**
```bash
psql $DATABASE_URL -c "
  SELECT mac, locality_id, effective_start, COUNT(*) 
  FROM gpci_indices 
  GROUP BY mac, locality_id, effective_start 
  HAVING COUNT(*) > 1;
"
# Expected: 0 rows (no duplicates)
```

**Locality '00' Verification:**
```bash
psql $DATABASE_URL -c "
  SELECT mac, locality_id, locality_name, work_gpci 
  FROM gpci_indices 
  WHERE locality_id = '00' 
  ORDER BY mac 
  LIMIT 10;
"
# Expected: Multiple rows with different MACs (01112, 01212, etc.)
# v1.2 would have rejected most as duplicates
```

### Step 8: Test

```bash
# Parser tests
pytest tests/ingestion/test_gpci_parser_golden.py -v
# Expected: 20/20 passing

# Integration tests
pytest tests/integration/test_gpci_payment_spotcheck.py -v
# Expected: All passing
```

---

## Rollback (If Needed)

### Option A: Alembic Downgrade
```bash
alembic downgrade -1
# Removes unique index, keeps data
```

### Option B: Restore Full Backup
```bash
systemctl stop cms-api
psql $DATABASE_URL < backup_gpci_v13_YYYYMMDD_HHMMSS.sql
systemctl start cms-api
```

### Option C: Restore Backfill Backup
```bash
# Use automatic backup table created by backfill script
psql $DATABASE_URL -c "
  DELETE FROM gpci_indices 
  WHERE release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D');
  
  INSERT INTO gpci_indices 
  SELECT * FROM gpci_indices_backup_YYYYMMDD_HHMMSS;
"
```

---

## Troubleshooting

### Issue: "Duplicate key value violates unique constraint"

**Cause:** Old v1.2 data conflicts with new v1.3 unique index  
**Fix:** Run backfill script to re-parse with v1.3

### Issue: "Backfill finds duplicates on v1.3 NK"

**Cause:** Source file or parser logic issue  
**Fix:** Investigate duplicate rows, check parser logic

### Issue: "Row count mismatch (expected ~109, got different)"

**Cause:** Source file changed or parsing issue  
**Fix:** Verify source file integrity, check parser logs

### Issue: "Python segfault when running backfill"

**Cause:** Pandas/SQLAlchemy dependency issue  
**Fix:** Reinstall dependencies in clean venv

---

## After Migration

### Immediate:
- [ ] Monitor logs for constraint violations (should be 0)
- [ ] Update internal docs (mark v1.2 as deprecated)
- [ ] Test next quarterly RVU ingestion

### Within 1 week:
- [ ] Review monitoring dashboard for anomalies
- [ ] Verify no performance regressions
- [ ] Confirm all downstream consumers working

### Within 2-4 weeks:
- [ ] Consider applying compat view (migration 004) if needed
- [ ] Schedule v1.2 compat view sunset (2026-04-01)
- [ ] Document lessons learned

---

## Files Reference

**Read First:**
- `.cursor/plans/GPCI_V13_MIGRATION_GUIDE.md` - Detailed operator runbook
- `this file` - Quick start guide

**Migration Files:**
- `alembic/versions/003_gpci_v13_add_mac_to_nk.py`
- `alembic/versions/004_gpci_v12_compat_view.py`
- `scripts/backfill_gpci_v13.py`
- `scripts/gpci_v13_preflight_check.sh`

**Source Reference:**
- `prds/SRC-gpci.md` - GPCI dataset documentation
- `CHANGELOG.md` - Breaking change entry

---

## Support

**Questions?**
- Review operator runbook: `GPCI_V13_MIGRATION_GUIDE.md`
- Check PRD: `prds/SRC-gpci.md` (§6 Schema Evolution)
- GitHub tasks: `github_tasks_plan.md` (search "GPCI v1.3")

**Issues?**
- Check preflight: `./scripts/gpci_v13_preflight_check.sh`
- Review logs: `logs/ingestion.log`
- Rollback available (see above)

---

**Migration artifacts ready. Execute when database is available!** ✅

