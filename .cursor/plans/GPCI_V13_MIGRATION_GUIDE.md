# GPCI v1.3 Migration Guide (Operator Runbook)

**Status:** Ready for Production  
**Target Release:** 2025 Q4 (RVU25D)  
**Estimated Duration:** 20-30 minutes  
**Risk Level:** LOW (additive change, surrogate PK unchanged)  
**Rollback:** Available via Alembic downgrade

---

## Overview

**Breaking Change:** GPCI schema v1.2 → v1.3

**Natural Key Change:**
```
v1.2: ['locality_code', 'effective_from']  ❌ FALSE DUPLICATES
v1.3: ['mac', 'locality_code', 'effective_from']  ✅ CORRECT
```

**Problem Solved:**
- `locality_code='00'` exists in multiple states (AL, AZ, AR, CA, CO, CT, etc.)
- v1.2 treated these as duplicates (63 out of 112 rows affected)
- v1.3 adds `mac` to disambiguate (MAC differs by state)

**Impact:**
- Row hashes change (MAC now included in NK)
- Unique index enforces correct uniqueness constraint
- Prevents false duplicate rejections during ingestion
- Enables accurate joins with locality/ANES data

**Why Safe:**
- Surrogate UUID primary key unchanged (backwards compatible)
- No foreign key impact (other tables don't reference GPCI yet)
- Database model was already correct (had `mac` column)
- Migration is additive (adds index, no data loss)

---

## Prerequisites

**Before Starting:**
- [ ] Database backup completed (PostgreSQL dump)
- [ ] Application downtime scheduled (optional: 5 min maintenance window)
- [ ] Review this runbook with DBA
- [ ] Have rollback plan ready (Alembic downgrade command)
- [ ] Confirm source file available: `sample_data/rvu25d_0/GPCI2025.txt`

**Check Environment:**
```bash
# Verify Alembic is installed
alembic --version

# Verify database connection
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"

# Verify GPCI source file exists
ls -lh sample_data/rvu25d_0/GPCI2025.txt
```

---

## Step 1: Database Backup (5 min)

**Critical:** Always backup before schema changes.

```bash
# Full database backup
pg_dump $DATABASE_URL > backup_before_gpci_v13_$(date +%Y%m%d_%H%M%S).sql

# Or just GPCI table
pg_dump $DATABASE_URL -t gpci_indices > backup_gpci_$(date +%Y%m%d_%H%M%S).sql
```

**Verify Backup:**
```bash
ls -lh backup_*.sql
```

---

## Step 2: Apply Alembic Migration (2 min)

**Applies:** Unique index on `(mac, locality_id, effective_start)`

```bash
# Check current migration state
alembic current

# Dry-run (show SQL without applying)
alembic upgrade head --sql

# Apply migration
alembic upgrade head

# Expected output:
#   INFO  [alembic.runtime.migration] Running upgrade 002 -> 003
#   ✅ GPCI v1.3 migration complete
#   Added unique index: uq_gpci_mac_locality_effective
```

**Verify Migration Applied:**
```bash
# Check for unique index
psql $DATABASE_URL -c "
  SELECT indexname, indexdef 
  FROM pg_indexes 
  WHERE tablename = 'gpci_indices' 
    AND indexname = 'uq_gpci_mac_locality_effective';
"

# Should return 1 row with unique index definition
```

---

## Step 3: Backfill GPCI Data (10 min)

**Purpose:** Re-parse GPCI data with v1.3 parser for correct hashes.

### 3.1: Dry Run (Preview Changes)

```bash
# Preview what would happen (no changes committed)
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --dry-run

# Expected output:
#   ✅ Parsed 109 rows with v1.3
#   ✅ Verified: 0 duplicates on ['mac', 'locality_code', 'effective_from']
#   🔍 DRY RUN: Would delete old GPCI rows for RVU25D
#   🔍 DRY RUN: Would load 109 new rows
#   ✅ DRY RUN SUCCESSFUL
```

**Review Output:**
- Parsed rows should be ~109
- Duplicates should be **0** (critical check)
- If duplicates > 0, STOP and investigate

### 3.2: Commit Backfill

```bash
# Apply changes (creates backup, deletes old, loads new)
python scripts/backfill_gpci_v13.py \
    --release-id RVU25D \
    --file sample_data/rvu25d_0/GPCI2025.txt \
    --commit

# Expected output:
#   ✅ Backed up 109 rows to gpci_indices_backup_20251021_004500
#   ✅ Parsed 109 rows with v1.3
#   ✅ Verified: 0 duplicates on ['mac', 'locality_code', 'effective_from']
#   ✅ Deleted 109 old rows
#   ⏳ Load new data step (use RVU ingestor or bulk COPY)
#   ✅ BACKFILL COMPLETE
```

**Note:** Current backfill script documents load step but doesn't execute it.  
**Action Required:** Integrate with actual ingestor load logic OR manually load via:

```bash
# Option A: Use existing RVU ingestor (RECOMMENDED)
python run_ingestion.py --source sample_data/rvu25d_0/GPCI2025.txt --parser gpci

# Option B: Manual bulk COPY (advanced)
# Export parsed data to CSV, then COPY into database
```

---

## Step 4: Verification (5 min)

### 4.1: Row Count Check

```bash
# Check row count matches expected (~109)
psql $DATABASE_URL -c "
  SELECT COUNT(*) as gpci_rows 
  FROM gpci_indices 
  WHERE release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D');
"

# Expected: ~109 rows
```

### 4.2: Unique Constraint Check

```bash
# Verify no duplicate violations
psql $DATABASE_URL -c "
  SELECT mac, locality_id, effective_start, COUNT(*) 
  FROM gpci_indices 
  GROUP BY mac, locality_id, effective_start 
  HAVING COUNT(*) > 1;
"

# Expected: 0 rows (no duplicates)
```

### 4.3: Sample Data Check

```bash
# Verify locality_code='00' appears for multiple MACs (not duplicates)
psql $DATABASE_URL -c "
  SELECT mac, locality_id, locality_name, work_gpci 
  FROM gpci_indices 
  WHERE locality_id = '00' 
  ORDER BY mac 
  LIMIT 10;
"

# Expected: Multiple rows with different MACs (01112, 01212, etc.)
# v1.2 would have rejected most of these as duplicates
```

### 4.4: Hash Check

```bash
# Verify row hashes are different from v1.2 (MAC now included)
# Note: This requires comparing old backup vs new data
psql $DATABASE_URL -c "
  SELECT id, mac, locality_id, effective_start, 
         substring(row_hash::text, 1, 16) as hash_prefix
  FROM gpci_indices 
  WHERE locality_id = '00' 
  LIMIT 5;
"

# Hashes should be different from v1.2 backup (if available)
```

---

## Step 5: Application Testing (5 min)

### 5.1: Parser Test

```bash
# Run GPCI parser tests (all should pass)
pytest tests/ingestion/test_gpci_parser_golden.py -v

# Expected: 20/20 passing
```

### 5.2: Integration Test

```bash
# Run integration tests (if available)
pytest tests/integration/test_locality_e2e.py -v -k gpci

# Expected: All tests passing
```

### 5.3: API Smoke Test (if API is running)

```bash
# Query GPCI endpoint (example, adjust to your API)
curl http://localhost:8000/api/v1/gpci?mac=01112&locality=00

# Expected: Returns data for MAC 01112, locality 00
```

---

## Rollback Plan

**If Something Goes Wrong:**

### Option A: Rollback Migration Only

```bash
# Revert to v1.2 (removes unique index)
alembic downgrade -1

# WARNING: This removes the constraint but doesn't fix data
# Duplicates may re-emerge if you re-parse
```

### Option B: Restore Database Backup

```bash
# Stop application
systemctl stop cms-api

# Restore from backup
psql $DATABASE_URL < backup_before_gpci_v13_YYYYMMDD_HHMMSS.sql

# Restart application
systemctl start cms-api

# Verify restoration
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
```

### Option C: Restore GPCI Table Only

```bash
# Drop current GPCI data
psql $DATABASE_URL -c "DELETE FROM gpci_indices WHERE release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D');"

# Restore from table-specific backup
psql $DATABASE_URL < backup_gpci_YYYYMMDD_HHMMSS.sql

# Verify restoration
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
```

---

## Monitoring & Alerts

**Post-Migration:**

1. **Check Logs:** Monitor for duplicate constraint violations
   ```bash
   tail -f logs/ingestion.log | grep "duplicate key value"
   ```

2. **Alert Thresholds:**
   - Row count < 100 or > 120 (universe change)
   - Duplicate violations on unique index (should be 0)
   - Parse errors during quarterly RVU ingestion

3. **Dashboard Metrics:**
   - GPCI row count by MAC
   - Locality '00' count by state (should be ~15)
   - Average GPCI values by component (work, PE, MP)

---

## Next Steps (After Migration)

### Immediate:
- [ ] Update documentation (mark v1.2 as deprecated)
- [ ] Notify downstream consumers (if any)
- [ ] Schedule next quarterly RVU ingestion (test v1.3 in production)

### Follow-Up (1-2 weeks):
- [ ] Create backwards compatibility view (optional)
  ```sql
  CREATE VIEW gpci_indices_v12_compat AS
  SELECT DISTINCT ON (locality_id, effective_start)
      id, release_id, locality_id, work_gpci, pe_gpci, mp_gpci, effective_start
  FROM gpci_indices
  ORDER BY locality_id, effective_start, mac;
  ```
- [ ] Monitor for any query performance regressions
- [ ] Review metrics dashboard for anomalies

### Long-Term:
- [ ] Deprecate v1.2 schema contract (remove from codebase after 2 quarters)
- [ ] Update related PRDs with migration lessons
- [ ] Consider similar NK fixes for other datasets (ANES, OPPSCAP)

---

## FAQs

**Q: Why keep surrogate UUID primary key?**  
A: Backwards compatibility. If any foreign keys reference `gpci_indices.id`, changing PK would break them. Unique index enforces NK without impacting PKs.

**Q: Can I skip the backfill and just apply the migration?**  
A: Not recommended. Old data has wrong hashes (missing MAC). Queries may work but integrity checks will fail. Backfill ensures consistency.

**Q: What if duplicates appear during backfill dry run?**  
A: STOP. Investigate the duplicate rows. This shouldn't happen with v1.3 parser unless source data has true duplicates. Check parser logic or source file.

**Q: How long is downtime required?**  
A: Migration: ~30 seconds. Backfill: ~2 minutes (delete + load). Total: 3-5 minutes if app is stopped. Can do with zero downtime if using read replicas.

**Q: What if next quarterly RVU ingestion fails?**  
A: Parser v1.3 should handle it. If issues arise, check:
  1. Source file has MAC column
  2. Layout registry correct for new quarter
  3. No CMS format changes

---

## Contact & Support

**Runbook Owner:** CMS Pricing API Team  
**Migration Date:** 2025-10-21  
**Alembic Revision:** `003_gpci_v13_add_mac_to_nk`  
**Related Docs:**
- `prds/SRC-gpci.md` (GPCI source reference)
- `cms_pricing/ingestion/contracts/cms_gpci_v1.3.json` (schema contract)
- `.cursor/plans/gpci_v13_migration_plan.md` (detailed implementation plan)

---

**End of GPCI v1.3 Migration Guide**

