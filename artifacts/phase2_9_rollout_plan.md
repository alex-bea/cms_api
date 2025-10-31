# Phase 2.9: Final Rollout Checks & Migration Validation

## Overview
This phase covers final validation, migration testing, rollout readiness checks, and deployment procedures before moving Phase 2 provenance to production.

## Objectives
1. Validate Alembic migration can run safely on production-like data
2. Verify end-to-end provenance flow from ingestion → engines → API responses
3. Complete rollout readiness checklist
4. Establish rollback procedures
5. Document deployment sequence and validation steps

## 1. Migration Validation

### 1.1 Pre-Migration Checks
**Purpose:** Validate database state before migration

**Tasks:**
- [ ] Identify target databases (staging, production)
- [ ] Verify current Alembic revision on each database
- [ ] Document table row counts for all 10 fee schedule tables:
  - `fee_mpfs`, `fee_opps`, `fee_asc`, `fee_ipps`, `fee_clfs`, `fee_dmepos`
  - `gpci`, `conversion_factors`, `wage_index`, `ipps_base_rates`
- [ ] Check for existing indexes that might conflict (`idx_*_release`, `idx_*_batch`)
- [ ] Verify database connection and transaction isolation settings
- [ ] Confirm backup/restore procedures are tested

**Validation Script:**
```bash
# Check current revision
alembic current

# Count rows per table
psql $DATABASE_URL -c "SELECT 'fee_mpfs' as table_name, COUNT(*) FROM fee_mpfs UNION ALL SELECT 'fee_opps', COUNT(*) FROM fee_opps ..."

# Check for existing indexes
psql $DATABASE_URL -c "SELECT indexname FROM pg_indexes WHERE tablename IN ('fee_mpfs', 'fee_opps', ...) AND indexname LIKE 'idx_%_release' OR indexname LIKE 'idx_%_batch';"
```

### 1.2 Migration Execution on Staging
**Purpose:** Test migration on staging with production-like data volume

**Tasks:**
- [ ] Create staging database backup before migration
- [ ] Run `alembic upgrade head` on staging database
- [ ] Capture migration execution time and lock duration
- [ ] Verify lock_timeout and statement_timeout are adequate (plan: 5s lock, 30s statement)
- [ ] Confirm all 10 tables have `release_id` and `batch_id` columns
- [ ] Verify indexes are created with correct names (`idx_{table}_release`, `idx_{table}_batch`)
- [ ] Check that existing data is unaffected (row counts unchanged, values intact)
- [ ] Test rollback: `alembic downgrade <previous_revision>` then re-apply

**Validation Queries:**
```sql
-- Verify columns exist
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name IN ('fee_mpfs', 'fee_opps', ...)
  AND column_name IN ('release_id', 'batch_id');

-- Verify indexes exist
SELECT indexname, tablename 
FROM pg_indexes 
WHERE tablename IN ('fee_mpfs', 'fee_opps', ...)
  AND (indexname LIKE 'idx_%_release' OR indexname LIKE 'idx_%_batch');

-- Verify no data loss
SELECT COUNT(*) FROM fee_mpfs; -- Compare before/after
```

### 1.3 Migration Performance Validation
**Purpose:** Ensure migration completes within acceptable time windows

**Success Criteria:**
- Migration completes in < 2 minutes for staging dataset
- No lock timeouts observed
- Database remains available during migration (read-only acceptable)
- Downtime window estimate validated (< 5 minutes for production)

**Metrics to Capture:**
- Total migration time
- Time per table (add columns + create indexes)
- Peak lock duration
- Transaction log growth

## 2. End-to-End Provenance Flow Validation

### 2.1 Ingestion Validation
**Purpose:** Verify ingestion pipeline populates provenance correctly

**Tasks:**
- [ ] Run test ingestion for MPFS dataset with known `release_id` and `batch_id`
- [ ] Verify parquet files contain `release_id` and `batch_id` columns
- [ ] Run `scripts/verify_provenance_columns.py` on ingested parquet files
- [ ] Load test data using `scripts/load_data.py` with provenance
- [ ] Verify database rows have correct `release_id` and `batch_id` values

**Test Scenario:**
```bash
# Ingest with known provenance
python -m cms_pricing.ingestion.ingestors.rvu_ingestor --release-id "test_release_001" --batch-id "test_batch_001"

# Verify parquet output
python scripts/verify_provenance_columns.py --dataset MPFS --build-id test_release_001

# Load into database
python scripts/load_data.py --dataset mpfs --build-id test_release_001 --dataset-prefix mpfs

# Query database
psql $DATABASE_URL -c "SELECT release_id, batch_id, COUNT(*) FROM fee_mpfs WHERE release_id = 'test_release_001' GROUP BY release_id, batch_id;"
```

### 2.2 Engine Validation
**Purpose:** Verify engines return provenance in pricing responses

**Tasks:**
- [ ] Run unit tests for all 6 engines (MPFS, OPPS, ASC, CLFS, DMEPOS, IPPS)
- [ ] Verify engines return `release_id`, `batch_id`, `dataset_id` in response
- [ ] Validate `trace_refs` include standardized format: `{dataset}:release:{id}`, `{dataset}:batch:{id}`
- [ ] Confirm deduplication works (no duplicate trace_refs)
- [ ] Test with legacy data (None provenance) - verify engines handle gracefully

**Test Commands:**
```bash
# Run engine tests
pytest tests/services/test_pricing_provenance.py -v
pytest tests/engines/ -k provenance -v

# Verify trace_refs format
pytest tests/services/test_pricing_provenance.py::TestProvenanceTraceRefsDeduplication -v
```

### 2.3 API Response Validation
**Purpose:** Verify provenance appears in API responses

**Tasks:**
- [ ] Make test pricing request to `/pricing/price`
- [ ] Verify response includes `datasets_used` with provenance fields
- [ ] Verify `line_items[].trace_refs` contain standardized provenance format
- [ ] Test `/pricing/compare` endpoint - verify both locations include provenance
- [ ] Test `/pricing/codes/price` endpoint - verify single-code responses include provenance
- [ ] Validate OpenAPI schema includes provenance documentation

**Test Requests:**
```bash
# Price a plan
curl -X POST http://localhost:8000/pricing/price \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"zip": "94110", "plan_id": "...", "year": 2025, "quarter": "1"}'

# Verify response structure
jq '.datasets_used[] | {dataset_id, release_id, batch_id}' response.json
jq '.line_items[].trace_refs[] | select(. | contains(":release:") or contains(":batch:"))' response.json
```

## 3. Rollout Readiness Checklist

### 3.1 Code Readiness
- [ ] All Phase 2.1-2.8 tasks marked complete
- [ ] Migration file reviewed and approved (`8d80f393d0ee`)
- [ ] All tests passing (unit, integration, golden)
- [ ] No linter errors
- [ ] Code reviewed and merged to main/staging branch

### 3.2 Data Readiness
- [ ] Ingestion pipeline tested with provenance
- [ ] Loader scripts validated (`scripts/load_data.py`)
- [ ] Verification scripts operational (`scripts/verify_provenance_columns.py`)
- [ ] Sample data with provenance available for testing

### 3.3 Infrastructure Readiness
- [ ] Staging database accessible and backed up
- [ ] Production database backup procedures confirmed
- [ ] Rollback procedures documented and tested
- [ ] Monitoring/alerting in place for migration execution
- [ ] Database connection pooling configured appropriately

### 3.4 Documentation Readiness
- [ ] OpenAPI schema updated with provenance fields
- [ ] Endpoint documentation includes provenance format
- [ ] Migration runbook documented
- [ ] Rollback procedures documented
- [ ] Change log updated

## 4. Deployment Sequence

### 4.1 Pre-Deployment
1. **Notification:** Notify stakeholders of deployment window
2. **Backup:** Create full database backup
3. **Health Check:** Verify staging environment is healthy
4. **Lock:** Coordinate deployment window (maintenance mode if needed)

### 4.2 Deployment Steps
1. **Staging Migration:**
   ```bash
   # Set timeouts
   export PGPASSWORD=...
   export DATABASE_URL=postgresql://...
   
   # Run migration
   alembic upgrade head
   
   # Verify
   python scripts/verify_migration.py --database-url $DATABASE_URL
   ```

2. **Staging Validation:**
   - Run end-to-end tests on staging
   - Verify API responses include provenance
   - Performance smoke tests

3. **Production Migration** (if staging successful):
   ```bash
   # Production backup first
   pg_dump $PROD_DATABASE_URL > backup_$(date +%Y%m%d_%H%M%S).sql
   
   # Run migration
   alembic upgrade head
   
   # Verify
   python scripts/verify_migration.py --database-url $PROD_DATABASE_URL
   ```

4. **Production Validation:**
   - Smoke test pricing endpoints
   - Verify provenance in responses
   - Monitor error rates and latency

### 4.3 Post-Deployment
1. **Validation:** Run validation queries to confirm migration success
2. **Monitoring:** Watch error logs and metrics for 30 minutes
3. **Communication:** Notify stakeholders of successful deployment
4. **Documentation:** Update deployment log with results

## 5. Rollback Procedures

### 5.1 Migration Rollback
**If migration fails or causes issues:**

```bash
# Rollback to previous revision
alembic downgrade <previous_revision>

# Verify rollback
psql $DATABASE_URL -c "SELECT column_name FROM information_schema.columns WHERE table_name = 'fee_mpfs' AND column_name IN ('release_id', 'batch_id');"
# Should return no rows

# Restore from backup if needed
pg_restore -d $DATABASE_URL backup_*.sql
```

### 5.2 Application Rollback
**If code changes cause issues:**

- Revert code deployment to previous version
- Migration can remain applied (columns will just be unused)
- Legacy data behavior continues to work (None provenance values)

### 5.3 Rollback Validation
- [ ] Rollback script tested on staging
- [ ] Backup restore procedure validated
- [ ] Rollback time estimated (< 5 minutes)
- [ ] Communication plan for rollback execution

## 6. Validation Scripts

### 6.1 Migration Verification Script
**File:** `scripts/verify_migration.py`

**Purpose:** Validate migration applied correctly

**Checks:**
- All 10 tables have `release_id` and `batch_id` columns
- All 20 indexes created (2 per table)
- Column types are VARCHAR(50), nullable
- No data loss (row counts match pre-migration)
- Migration revision is current

### 6.2 Provenance End-to-End Test
**File:** `scripts/test_provenance_e2e.py`

**Purpose:** Validate full provenance flow

**Flow:**
1. Create test data with known `release_id` and `batch_id`
2. Load into database
3. Make pricing API request
4. Verify response contains provenance
5. Validate trace_refs format

## 7. Success Criteria

### 7.1 Migration Success
- [ ] Migration completes without errors on staging
- [ ] All tables have provenance columns
- [ ] All indexes created successfully
- [ ] No data loss (row counts verified)
- [ ] Migration time within acceptable window

### 7.2 Provenance Flow Success
- [ ] Ingestion populates provenance in parquet files
- [ ] Loader populates provenance in database
- [ ] Engines return provenance in responses
- [ ] API responses include provenance in `datasets_used` and `trace_refs`
- [ ] Legacy data handled gracefully (None values)

### 7.3 Documentation Success
- [ ] OpenAPI schema includes provenance documentation
- [ ] Endpoint docs updated
- [ ] Migration runbook complete
- [ ] Rollback procedures documented

## 8. Risk Mitigation

### 8.1 Migration Risks
| Risk | Mitigation |
|------|-----------|
| Migration timeout | Test on staging first; increase timeouts if needed |
| Lock contention | Run during low-traffic window; use lock_timeout |
| Data corruption | Full backup before migration; test rollback |
| Index creation slow | Create indexes concurrently if needed (PostgreSQL 12+) |

### 8.2 Application Risks
| Risk | Mitigation |
|------|-----------|
| API errors after migration | Monitor error rates; rollback code if needed |
| Missing provenance | Verify ingestion pipeline; check loader scripts |
| Performance degradation | Monitor latency; validate index usage |

## 9. Timeline Estimate

- **Pre-migration validation:** 1-2 hours
- **Staging migration & testing:** 2-4 hours
- **Production migration:** 1 hour (including backup)
- **Post-deployment validation:** 1-2 hours
- **Total:** 5-9 hours (excluding waiting/coordination)

## 10. Dependencies

- Phase 2.1-2.8 must be complete
- Staging database with test data
- Production database access and backup capability
- Alembic configuration verified
- Test API keys and authentication

## 11. Next Steps After Phase 2.9

Once Phase 2.9 is complete:
- Monitor production for 1 week
- Collect provenance data usage metrics
- Plan Phase 3 enhancements (if needed)
- Update master catalog and readiness plan

