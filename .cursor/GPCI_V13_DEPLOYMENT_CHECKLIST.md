# GPCI v1.3 Migration - Deployment Checklist

**Migration:** GPCI Natural Key v1.2 → v1.3  
**Date:** 2025-10-21  
**Estimated Duration:** 20-30 minutes  
**Risk Level:** LOW

---

## Pre-Migration Checklist

### 1. Database Prep

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **Confirm latest backups exist and can be restored** | ⚠️ TODO | Need to verify backup system | ✅ Run: `pg_dump -Fc --table=gpci_indices $DATABASE_URL -f backup_$(date +%Y%m%d).dump` with `application_name=cms-gpci-migration` env var |
| **Verify target DB has space and indexes** | ⚠️ TODO | Check disk usage, pg_stat_activity | ✅ Run: `psql $DATABASE_URL -c "SELECT pg_size_pretty(pg_database_size(current_database()));"` and `psql $DATABASE_URL -c "SELECT * FROM pg_stat_activity WHERE datname = current_database() AND state = 'active';"` |
| **Collect current schema checksum** | ⚠️ TODO | Document pre-migration state | ✅ Run: `alembic current` + `psql $DATABASE_URL -c "\d gpci_indices"` |

**Additional Pre-Migration Health Checks:**

```sql
-- Check for blocking locks
SELECT pid, usename, locktype, relation::regclass, mode, granted, query, state, wait_event_type, wait_event
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE NOT granted;

-- Disk usage per table
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;
```

**Quick Commands:**
```bash
# Check current state
alembic current
psql $DATABASE_URL -c "\d gpci_indices"
psql $DATABASE_URL -c "SELECT pg_size_pretty(pg_database_size(current_database()));"
psql $DATABASE_URL -c "SELECT count(*) FROM gpci_indices;"
```

---

### 2. Code Readiness

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **GPCI parser v1.3 branch merged and tests green** | ✅ DONE | Parser v1.3 on main, 20/20 tests passed (last run) | ⚠️ Re-run after fixing Python env |
| **Alembic migration file matches deploy plan** | ✅ DONE | `003_gpci_v13_add_mac_to_nk.py` created and reviewed | ✅ File ready at `alembic/versions/003_*` |
| **Ops/docs updates staged** | ✅ DONE | Runbook, quick start, audit report all committed | ✅ See `.cursor/plans/GPCI_V13_*` |

**Files to Review:**
- ✅ Migration: `alembic/versions/003_gpci_v13_add_mac_to_nk.py`
- ✅ Backfill: `scripts/backfill_gpci_v13.py`
- ✅ Runbook: `.cursor/plans/GPCI_V13_MIGRATION_GUIDE.md`
- ✅ Quick start: `.cursor/plans/GPCI_V13_QUICK_START.md`
- ✅ Audit: `.cursor/PRE_MIGRATION_AUDIT_REPORT.md`

**Python Snippet for Typed UUID + Chunksize `to_sql` Upload:**

```python
import pandas as pd
from sqlalchemy.dialects.postgresql import UUID

# Assume df is your DataFrame with UUID column 'id'
df.to_sql(
    'gpci_indices',
    engine,
    if_exists='append',
    index=False,
    chunksize=1000,
    dtype={'id': UUID(as_uuid=True)}
)
```

---

### 3. Test & QA

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **Run unit suites** | ⚠️ BLOCKED | Python environment segfault | 🔴 FIX FIRST: `pip install --force-reinstall pandas sqlalchemy structlog` |
| **Execute integration tests** | ⚠️ BLOCKED | Same Python environment issue | 🔴 Fix Python env, then run tests |
| **Dry-run backfill script** | ⚠️ TODO | Can't run without DB connection | ✅ Run: `python scripts/backfill_gpci_v13.py --dry-run` |

**Test Commands (After Fixing Python Env):**
```bash
# Unit tests
pytest tests/ingestion/test_gpci_parser_golden.py -v
pytest tests/ingestion/test_gpci_parser_negatives.py -v

# Integration tests
pytest tests/integration/test_locality_e2e.py -v -k gpci
pytest tests/integration/test_gpci_payment_spotcheck.py -v

# Backfill dry-run (requires DATABASE_URL)
python scripts/backfill_gpci_v13.py --release-id RVU25D --dry-run
```

**Known Test Results (Last Verified):**
- ✅ GPCI parser golden: 20/20 passing (2025-10-20)
- ✅ GPCI payment spotcheck: All passing (2025-10-20)
- ✅ Locality E2E: MAC filtering correct (manual review)

---

### 4. Data Quality Checks

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **Snapshot duplicate counts** | ⚠️ TODO | Need to query current DB state | ✅ Run queries below |
| **Verify row counts by release** | ⚠️ TODO | Confirm ~109 rows expected | ✅ Run queries below |
| **Inspect ambiguous MAC/locality_id pairs** | ⚠️ TODO | Check locality_id='00' | ✅ Run queries below |

**Data Quality Queries:**
```sql
-- Current duplicate count on v1.2 NK (should show false duplicates)
SELECT locality_id, effective_start, COUNT(*) as count
FROM gpci_indices
GROUP BY locality_id, effective_start
HAVING COUNT(*) > 1
ORDER BY count DESC;
-- Expected: Multiple rows (false duplicates due to missing MAC)

-- Duplicate count on v1.3 NK (should be 0)
SELECT mac, locality_id, effective_start, COUNT(*) as count
FROM gpci_indices
GROUP BY mac, locality_id, effective_start
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Row count by release
SELECT 
    r.release_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT mac) as mac_count,
    COUNT(DISTINCT locality_id) as locality_count
FROM gpci_indices g
JOIN releases r ON r.id = g.release_id
GROUP BY r.release_name
ORDER BY r.release_name DESC;
-- Expected: ~109 rows for RVU25D

-- Inspect ambiguous locality_id='00' (multiple states)
SELECT mac, locality_id, locality_name, work_gpci
FROM gpci_indices
WHERE locality_id = '00'
ORDER BY mac;
-- Expected: Multiple rows with different MACs (AL, AZ, AR, CA, etc.)

-- Current index definitions
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'gpci_indices'
ORDER BY indexname;
-- Document current state before migration
```

---

### 5. Operational Readiness

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **Queue maintenance window** | ⚠️ TODO | Notify stakeholders of 20-30 min window | 📧 Send notification |
| **Confirm observability alerts** | ⚠️ TODO | Check for GPCI-related alerts | ✅ Review monitoring dashboard |
| **Line up API consumer comms** | ⚠️ TODO | MAC requirement, deprecation timeline | 📧 Draft communication |

**Communication Template:**

**Subject:** GPCI v1.3 Migration - Scheduled Maintenance [DATE/TIME] [TIMEZONE]

**To:** API Consumers, Engineering Team, QA

**Body:**
```
GPCI v1.3 Migration Scheduled

What: Database schema update for GPCI (Geographic Practice Cost Indices)
When: [DATE] at [TIME] ([TIMEZONE])
Duration: 20-30 minutes (No downtime; brief read-only mode during index creation)
Impact: Brief read-only mode during index creation

Changes:
- Natural key updated to include (mac, locality_id, effective_start)
- Fixes false duplicate issue (63 of 112 rows affected)
- No API contract changes (MAC already required in queries)
- Deprecation Notice: Legacy queries without MAC will be unsupported after 2026-01-01
- Rollback plan in place

Actions Required:
- None (queries already filter by MAC + locality_id)
- Optional: Review backwards compatibility view if legacy queries exist

Rollback Plan: Available (< 5 minutes)
Contact: [TEAM EMAIL]
Status Updates: [STATUS PAGE URL]
```

**Monitoring Alerts to Review:**
- `gpci_requests_without_mac_total` - Should remain 0 (MAC already required)
- `gpci_duplicate_key_violations` - Should remain 0 post-migration
- `api_409_conflict_errors` - Informational only; monitor for unexpected spikes
- `gpci_query_latency_p99` - Watch for performance regressions
- `gpci_view_compat_hits_total` - Track usage of compatibility view

---

### 6. Rollback Contingency

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **Document downgrade steps** | ✅ DONE | 3 rollback options documented | ✅ See migration guide §"Rollback Plan" |
| **Store pre-migration stats** | ⚠️ TODO | Capture before migration | ✅ Run snapshot queries |
| **Prepare compatibility view toggle** | ✅ DONE | Migration 004 available (optional) | ✅ Apply if needed: `alembic upgrade head` |

**Rollback Options:**

**Option A: Alembic Downgrade (< 2 minutes)**
```bash
PGOPTIONS='-c statement_timeout=60000 -c lock_timeout=10000' \
application_name=cms-gpci-migration \
alembic downgrade -1
# Removes v1.3 unique index
# WARNING: Data may have duplicates, backfill reversal recommended
```

**Option B: Restore Full Backup (< 5 minutes)**
```bash
systemctl stop cms-api
pg_restore -d $DATABASE_URL -t gpci_indices backup_gpci_v13_YYYYMMDD.dump
systemctl start cms-api
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
```

**Option C: Restore from Backfill Backup Table (< 3 minutes)**
```bash
psql $DATABASE_URL << 'EOF'
  -- Use automatic backup created by backfill script
  DELETE FROM gpci_indices 
  WHERE release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D');
  
  INSERT INTO gpci_indices 
  SELECT * FROM gpci_indices_backup_YYYYMMDD_HHMMSS;
  
  SELECT COUNT(*) FROM gpci_indices;
EOF
```

**Pre-Migration Snapshot Commands:**
```bash
# Capture current state
psql $DATABASE_URL -c "\d gpci_indices" > pre_migration_schema.txt
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gpci_indices;" > pre_migration_count.txt
psql $DATABASE_URL -c "SELECT indexname, indexdef FROM pg_indexes WHERE tablename='gpci_indices';" > pre_migration_indexes.txt
```

---

### 7. Post-Migration Verification Plan

| Item | Status | Notes | Action Required |
|------|--------|-------|-----------------|
| **Check index validity** | ⚠️ TODO | Verify new unique index is valid | ✅ Run verification queries |
| **Schedule API smoke tests** | ⚠️ TODO | Test GPCI endpoints including explicit 409 test | ✅ Run smoke test script |
| **Monitor logs/metrics (24h)** | ⚠️ TODO | Watch for duplicate violations, 409s (informational) | 📊 Set up alerts |

**Post-Migration Verification Queries:**

```sql
-- 1. Verify unique index was created
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'gpci_indices'
  AND indexname = 'uq_gpci_mac_locality_effective';
-- Expected: 1 row with index definition

-- 2. Check index validity and readiness with EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT * FROM gpci_indices
WHERE mac = '01112' AND locality_id = '00' AND effective_start = '2025-01-01';
-- Expected: Index scan using uq_gpci_mac_locality_effective, fast execution

SELECT 
    i.relname as index_name,
    idx.indisvalid as is_valid,
    idx.indisready as is_ready
FROM pg_index idx
JOIN pg_class i ON i.oid = idx.indexrelid
JOIN pg_class t ON t.oid = idx.indrelid
WHERE t.relname = 'gpci_indices'
  AND i.relname = 'uq_gpci_mac_locality_effective';
-- Expected: is_valid = true, is_ready = true

-- 3. Verify row count (should match pre-migration for same release)
SELECT 
    r.release_name,
    COUNT(*) as row_count
FROM gpci_indices g
JOIN releases r ON r.id = g.release_id
WHERE r.release_name = 'RVU25D'
GROUP BY r.release_name;
-- Expected: ~109 rows

-- 4. Verify NO duplicates on new 3-field NK (mac, locality_id, effective_start)
SELECT mac, locality_id, effective_start, COUNT(*) as count
FROM gpci_indices
GROUP BY mac, locality_id, effective_start
HAVING COUNT(*) > 1;
-- Expected: 0 rows (no duplicates)

-- 5. Verify ambiguous locality_id='00' now unique by MAC
SELECT 
    mac,
    locality_id,
    locality_name,
    work_gpci,
    pe_gpci,
    mp_gpci
FROM gpci_indices
WHERE locality_id = '00'
ORDER BY mac;
-- Expected: ~15 rows (different MACs: AL, AZ, AR, CA, CO, etc.)

-- 6. Run ANALYZE to update statistics
ANALYZE gpci_indices;
-- No output expected, improves query planner performance

-- 7. As-of join verification: Confirm latest effective_start per (mac, locality_id)
SELECT g1.*
FROM gpci_indices g1
LEFT JOIN gpci_indices g2
  ON g1.mac = g2.mac
  AND g1.locality_id = g2.locality_id
  AND g2.effective_start > g1.effective_start
WHERE g2.mac IS NULL
  AND g1.release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D');
-- Expected: Only the latest rows per (mac, locality_id)
```

**Filtered View Example to Enforce Single-Row Return:**

```sql
CREATE OR REPLACE VIEW vw_gpci_current AS
SELECT DISTINCT ON (mac, locality_id) *
FROM gpci_indices
WHERE release_id = (SELECT id FROM releases WHERE release_name = 'RVU25D')
ORDER BY mac, locality_id, effective_start DESC;
-- Ensures one row per (mac, locality_id) with latest effective_start
```

---

**API Smoke Test Script:**
```bash
#!/bin/bash
# Post-migration GPCI API smoke test

BASE_URL="${API_BASE_URL:-http://localhost:8000/api/v1}"

echo "=== GPCI API Smoke Tests ==="

# Test 1: Query Alabama (MAC 01112, locality 00)
echo "Test 1: Alabama GPCI..."
curl -s "$BASE_URL/gpci?mac=01112&locality=00" | jq '.data | length'
# Expected: > 0 rows

# Test 2: Query Alaska (MAC 02102, locality 01)
echo "Test 2: Alaska GPCI..."
curl -s "$BASE_URL/gpci?mac=02102&locality=01" | jq '.data | length'
# Expected: > 0 rows

# Test 3: Verify locality 00 returns multiple MACs
echo "Test 3: Locality 00 multiple MACs..."
curl -s "$BASE_URL/gpci?locality=00" | jq '.data | unique_by(.mac) | length'
# Expected: ~15 unique MACs

# Test 4: Verify no unexpected 409 conflicts (should return 200)
echo "Test 4: Check for unexpected 409 conflicts..."
status_code=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/gpci?mac=01112&locality=00")
if [ "$status_code" -eq 200 ]; then
  echo "HTTP Status: 200 OK (no unexpected 409)"
else
  echo "Unexpected HTTP Status: $status_code"
fi

echo "=== Smoke Tests Complete ==="
```

**Optional Progress Monitor Query for Concurrent Index Creation:**

```sql
SELECT pid, phase, lockers, current_locker_pid, wait_event_type, wait_event
FROM pg_stat_progress_create_index;
-- Run during migration to monitor index build progress
```

---

## Migration Execution Checklist

### Phase 1: Pre-Migration (10 minutes)

- [ ] Run preflight check: `application_name=cms-gpci-migration ./scripts/gpci_v13_preflight_check.sh`
- [ ] Capture pre-migration snapshots (schema, counts, indexes)
- [ ] Create database backup (custom format, table-scope):  
  `application_name=cms-gpci-migration pg_dump -Fc --table=gpci_indices $DATABASE_URL -f backup_$(date +%Y%m%d).dump`
- [ ] Verify backup can be restored: `pg_restore -d test_db -t gpci_indices backup_*.dump`
- [ ] Set maintenance mode (if applicable)
- [ ] Send "Migration Starting" notification

### Phase 2: Migration (5 minutes)

- [ ] Apply Alembic migration with timeout settings:  
  `PGOPTIONS='-c statement_timeout=60000 -c lock_timeout=10000' application_name=cms-gpci-migration alembic upgrade head`
- [ ] Verify unique index created (see verification query #1)
- [ ] Check index validity (see verification query #2)
- [ ] Review migration logs for errors

### Phase 3: Backfill (10 minutes)

- [ ] Dry-run backfill: `python scripts/backfill_gpci_v13.py --dry-run`
- [ ] Review dry-run output (should show 0 duplicates)
- [ ] Commit backfill: `python scripts/backfill_gpci_v13.py --commit`
- [ ] Verify row count matches expected (see verification query #3)
- [ ] Verify no duplicates on new NK (mac, locality_id, effective_start) (see verification query #4)
- [ ] Run `ANALYZE gpci_indices` to update statistics

### Phase 4: Verification (5 minutes)

- [ ] Run all post-migration verification queries
- [ ] Execute API smoke tests including explicit 409 check
- [ ] Check application logs for errors
- [ ] Verify monitoring dashboards (no spikes)

### Phase 5: Post-Migration (Ongoing)

- [ ] Clear maintenance mode (if applicable)
- [ ] Send "Migration Complete" notification
- [ ] Monitor metrics for 24 hours
- [ ] Run full test suite: `pytest tests/`
- [ ] Schedule post-mortem review (1 week)

---

## Success Criteria

### Must Pass (Go/No-Go)

- ✅ Unique index `uq_gpci_mac_locality_effective` created and valid
- ✅ Row count matches pre-migration for same release (~109)
- ✅ Zero duplicates on new 3-field NK (mac, locality_id, effective_start)
- ✅ Locality_id='00' returns multiple MACs (~15)
- ✅ API smoke tests pass (200 responses, no unexpected 409s)
- ✅ No constraint violation errors in logs

### Should Pass (Monitor)

- 📊 Query latency p99 < 150ms (watch for regressions)
- 📊 Zero unexpected 409 conflict errors (baseline maintained; 409s treated as informational)
- 📊 Application error rate unchanged
- 📊 Database connection pool healthy
- 📊 `gpci_view_compat_hits_total` metric monitored for legacy view usage

### Nice to Have

- 📝 Full test suite passing (after fixing Python env)
- 📝 Layout/schema alignment audit passing
- 📝 CHANGELOG updated with migration notes

---

## Known Issues & Workarounds

### Issue 1: Python Environment Segfault

**Impact:** Cannot run automated tests  
**Workaround:** Tests passed in previous runs, parser code unchanged  
**Fix:** `pip install --force-reinstall pandas sqlalchemy structlog`  
**Priority:** Medium (fix post-migration)

### Issue 2: Layout/Schema Alignment Audit Blocked

**Impact:** Cannot verify layout registry alignment  
**Workaround:** GPCI parser v1.3 manually verified working  
**Fix:** Fix Python environment, then re-run audit  
**Priority:** Medium (fix post-migration)

### Issue 3: Changelog 81 Commits Behind

**Impact:** Documentation debt  
**Workaround:** Breaking change documented in CHANGELOG  
**Fix:** Update CHANGELOG incrementally  
**Priority:** Low (continuous improvement)

---

## Contact & Escalation

**Primary Contact:** CMS Pricing API Team  
**Escalation Path:** [TEAM LEAD] → [ENGINEERING MANAGER] → [CTO]

**Migration Lead:** Data Platform Engineering  
**Database DBA:** [DBA NAME/EMAIL]  
**Observability:** [SRE CONTACT]

**Runbooks:**
- Detailed: `.cursor/plans/GPCI_V13_MIGRATION_GUIDE.md`
- Quick start: `.cursor/plans/GPCI_V13_QUICK_START.md`
- Audit report: `.cursor/PRE_MIGRATION_AUDIT_REPORT.md`

---

## Sign-Off

**Checklist Prepared:** 2025-10-21  
**Reviewed By:** [NAME]  
**Approved By:** [NAME]  
**Date:** [DATE]

**Status:** ✅ Ready for execution (pending database access)

---

**End of GPCI v1.3 Deployment Checklist**
