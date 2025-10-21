# Runbook: Database Backup, Restore & Disaster Recovery

doc_type: RUN
normative: false
requires:
  - prds/STD-database-platform-prd-v1.0.md#5-backup-restore--disaster-recovery

**Status:** Stub v0.1 (in progress)  
**Owners:** Platform Engineering (DBA), SRE  
**Consumers:** On-call responders, Release Mgmt., Security  
**Change control:** PR review + DBA approval  
**Last updated:** 2025-10-21

**Cross-References**
- `prds/STD-database-platform-prd-v1.0.md` §5 (policy)  
- `prds/RUN-render-deployment-prd-v1.0.md` (deployment hardening)  
- `prds/RUN-database-migrations-prd-v1.0.md` (backout references)  
- `prds/STD-observability-monitoring-prd-v1.0.md` (alerting)

---

## 0. Purpose
Operationalise backup retention, manual dumps, restore drills, and PITR cutovers for all PostgreSQL environments.

---

## 1. Backup Inventory & Automation

### 1.1 Render Automated Backup Schedule

**Backup frequency by tier:**
```
Free:     No backups (testing only)
Starter:  Daily backups, 7-day retention
Standard: Daily backups, 14-day retention
Pro:      Daily backups, 30-day retention
```

**For production:** Use Standard ($20/mo) or Pro ($50/mo) tier for adequate retention.

### 1.2 Verify Last Successful Backup

**Via Render Dashboard:**
1. Navigate to your PostgreSQL database
2. Click **"Backups"** tab
3. Verify latest backup shows:
   - ✅ Status: "Completed"
   - 📅 Date: Within last 24 hours
   - 💾 Size: Reasonable (not 0 bytes)

**Via Render API:**
```bash
# Get service ID from dashboard URL
SERVICE_ID="dpg-xxx"

# List recent backups
curl -X GET "https://api.render.com/v1/services/${SERVICE_ID}/backups" \
  -H "Authorization: Bearer ${RENDER_API_KEY}" \
  -H "Content-Type: application/json" | jq '.'

# Check latest backup
curl -X GET "https://api.render.com/v1/services/${SERVICE_ID}/backups" \
  -H "Authorization: Bearer ${RENDER_API_KEY}" \
  | jq '.[-1] | {id, createdAt, status, sizeBytes}'
```

**Via PostgreSQL (backup age check):**
```sql
-- Check database age (should match last backup time)
SELECT 
  pg_database.datname,
  pg_stat_file('base/'||oid||'/PG_VERSION') as file_stat,
  age(now(), (pg_stat_file('base/'||oid||'/PG_VERSION')).modification) as age
FROM pg_database
WHERE datname = current_database();
```

### 1.3 Backup Manifest Export (HIPAA Compliance)

**For HIPAA-compliant storage:**

```bash
# 1. Export backup manifest to S3 with WORM object-lock
DATE=$(date +%Y-%m-%d)
MANIFEST_FILE="backup-manifest-${DATE}.json"

# 2. Create manifest
cat > ${MANIFEST_FILE} << EOF
{
  "backup_date": "${DATE}",
  "database": "cms_pricing_db",
  "service_id": "${SERVICE_ID}",
  "retention_days": 35,
  "backup_method": "Render automated daily",
  "verification_status": "✅ Completed",
  "size_bytes": $(curl -s ... | jq '.sizeBytes'),
  "postgresql_version": "17.6"
}
EOF

# 3. Upload to S3 with object-lock
aws s3 cp ${MANIFEST_FILE} s3://cms-pricing-backups/manifests/${DATE}/ \
  --storage-class GLACIER \
  --metadata retention=2555  # 7 years in days
```

**Retention policy (HIPAA):**
- Backup manifests: ≥6 years
- Audit logs: ≥6 years (see §7)
- Production data backups: 35 days (STD requirement)

---

## 2. Manual `pg_dump` Procedures

### 2.1 When to Use Manual Dumps

**Required before:**
- High-risk migrations (schema changes, data migrations)
- Major releases (>3 migration files)
- Production data exports
- Database version upgrades

**Optional for:**
- Low-risk migrations (add column with default)
- Development/staging deployments
- Read-only queries

### 2.2 Command Templates

**⚠️ Important:** Match `pg_dump` version to PostgreSQL server version:
```bash
# Check server version
psql $DATABASE_URL -c "SELECT version();"
# PostgreSQL 17.6 → use pg_dump 17.x

# Check client version
pg_dump --version
# If mismatch, install matching version
```

**Schema-only dump (pre-migration safety):**
```bash
# For change ticket: TICKET-1234
DATE=$(date +%Y%m%d_%H%M%S)
TICKET="TICKET-1234"
OUTPUT_DIR="backups/${TICKET}"
mkdir -p ${OUTPUT_DIR}

# Dump schema only
pg_dump $DATABASE_URL \
  --schema-only \
  --no-owner \
  --no-privileges \
  --file="${OUTPUT_DIR}/schema_${DATE}.sql"

# Verify
head -20 "${OUTPUT_DIR}/schema_${DATE}.sql"
# Should show CREATE TABLE statements
```

**Full database dump (complete backup):**
```bash
# Full dump with data (compressed)
pg_dump $DATABASE_URL \
  --format=custom \
  --compress=9 \
  --no-owner \
  --no-privileges \
  --file="${OUTPUT_DIR}/full_${DATE}.backup"

# Verify dump is valid
pg_restore --list "${OUTPUT_DIR}/full_${DATE}.backup" | head -20

# Check file size
ls -lh "${OUTPUT_DIR}/full_${DATE}.backup"
# Should be >1MB for production database
```

**Data-only dump (for data migration):**
```bash
# Data only (useful for testing migrations)
pg_dump $DATABASE_URL \
  --data-only \
  --no-owner \
  --no-privileges \
  --file="${OUTPUT_DIR}/data_${DATE}.sql"
```

### 2.3 Storage & Retention

**Local storage (temporary):**
```bash
# Store locally during migration
./backups/TICKET-1234/
  - schema_20251021_143022.sql
  - full_20251021_143022.backup
  - checksums.sha256
```

**Render object storage (long-term, HIPAA-compliant):**
```bash
# Upload encrypted dumps to Render-managed storage with retention controls
render backups upload ${OUTPUT_DIR}/schema_${DATE}.sql \
  --destination render://backups/prod/manual-dumps/${TICKET}/ \
  --retention-days 2555
render backups upload ${OUTPUT_DIR}/full_${DATE}.backup \
  --destination render://backups/prod/manual-dumps/${TICKET}/ \
  --retention-days 2555

# Verify upload
render backups ls render://backups/prod/manual-dumps/${TICKET}/
```

### 2.4 Integrity Verification

**Generate checksums:**
```bash
# SHA256 checksums for all dumps
cd ${OUTPUT_DIR}
sha256sum *.sql *.backup > checksums.sha256

# Verify checksums
sha256sum -c checksums.sha256
# All lines should show "OK"
```

**Test restore (dry-run):**
```bash
# Create temporary test database
createdb test_restore_${DATE}

# Test restore from custom format
pg_restore \
  --dbname=test_restore_${DATE} \
  --no-owner \
  --no-privileges \
  full_${DATE}.backup

# Verify table count matches
psql test_restore_${DATE} -c "\dt" | wc -l
# Should match production

# Clean up
dropdb test_restore_${DATE}
```

### 2.5 Change Ticket Template

```markdown
## Pre-Migration Backup - TICKET-1234

**Migration:** Add GPCI v1.3 MAC column to natural key
**Risk Level:** High (schema change + data migration)
**Database:** cms_pricing_db (Render production)
**Performed by:** Platform Engineering
**Date:** 2025-10-21 14:30:00 UTC

### Backup Details
- **Schema dump:** `s3://cms-pricing-backups/manual-dumps/TICKET-1234/schema_20251021_143022.sql`
- **Full dump:** `s3://cms-pricing-backups/manual-dumps/TICKET-1234/full_20251021_143022.backup`
- **Size:** 45 MB (compressed)
- **Checksums:** Verified ✅
- **Test restore:** Passed ✅

### Rollback Plan
1. Identify last known good state (pre-migration backup)
2. Restore from `full_20251021_143022.backup`
3. Verify data integrity (see §3.1)
4. Update DNS/app config to restored instance
5. RTO: ≤2 hours, RPO: 0 (point-in-time backup)

### Post-Migration Verification
- [ ] Alembic version matches expected revision
- [ ] Row counts match pre-migration state
- [ ] Critical queries return expected results
- [ ] Application smoke tests pass
```

---

## 3. Restore & PITR Playbooks

### 3.1 Snapshot Restore (Render UI/API)

**Use case:** Restore from daily automated backup (major incident, data corruption)

**Steps:**

1. **Identify backup timestamp**
   ```bash
   # Via Render Dashboard: Database → Backups tab
   # Choose most recent "good" backup (before incident)
   
   # Example: 2025-10-21 02:00:00 UTC (automated daily)
   BACKUP_ID="backup-abc123"
   BACKUP_TIMESTAMP="2025-10-21T02:00:00Z"
   ```

2. **Initiate restore to NEW instance**
   ```
   ⚠️ NEVER restore directly to production!
   Always restore to a new instance first.
   ```

   **Via Render Dashboard:**
   - Navigate to Database → Backups
   - Select backup
   - Click "Restore to New Database"
   - Name: `cms-pricing-db-restore-${DATE}`
   - Instance type: Match production (Starter/Standard/Pro)
   - Wait 5-10 minutes for restore

   **Via Render API:**
   ```bash
   curl -X POST "https://api.render.com/v1/services/${SERVICE_ID}/backups/${BACKUP_ID}/restore" \
     -H "Authorization: Bearer ${RENDER_API_KEY}" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "cms-pricing-db-restore-20251021",
       "plan": "starter"
     }'
   ```

3. **Validate data integrity**
   ```bash
   # Get connection string for restored database
   RESTORE_URL="postgresql://..."

   # Check table counts
   psql $RESTORE_URL -c "
     SELECT 
       schemaname,
       tablename,
       n_live_tup as row_count
     FROM pg_stat_user_tables
     ORDER BY n_live_tup DESC
     LIMIT 20;
   "
   
   # Verify critical data exists
   psql $RESTORE_URL -c "SELECT COUNT(*) FROM gpci_indices;"
   # Compare with known good state
   
   # Check Alembic version
   psql $RESTORE_URL -c "SELECT * FROM alembic_version;"
   
   # Run smoke tests against restored DB
   export DATABASE_URL=$RESTORE_URL
   pytest tests/test_database_smoke.py
   ```

4. **Cutover procedure**
   ```bash
   # Option A: DNS cutover (recommended)
   # Update Render Web Service environment variable
   # DATABASE_URL → point to restored database
   # Redeploy web service
   
   # Option B: Connection string update
   # Update .env, 1Password, etc.
   # Restart application
   
   # Option C: Rename databases (Render support required)
   # Contact support to rename:
   #   cms-pricing-db → cms-pricing-db-old
   #   cms-pricing-db-restore-20251021 → cms-pricing-db
   ```

5. **Post-cutover verification**
   ```bash
   # Verify application is using restored database
   curl https://api.example.com/health
   # Should show: database: connected
   
   # Check application logs for errors
   # Monitor error rates for 1 hour
   
   # Once stable, delete old database
   ```

**Timeline:** RTO ≤2 hours (restore + validation + cutover)

### 3.2 PITR (Point-In-Time Recovery)

**Use case:** Recover to specific moment (e.g., 5 minutes before bad deployment)

**Prerequisites:**
- WAL archiving enabled (automatic on Render)
- Target time within WAL retention (35 days for Pro tier)
- Know exact recovery time

**Steps:**

1. **Select target timestamp**
   ```bash
   # Identify last known good state
   # Example: 2025-10-21 14:25:00 UTC
   # (5 minutes before bad migration at 14:30)
   
   TARGET_TIME="2025-10-21T14:25:00Z"
   ```

2. **Request PITR via Render**
   ```bash
   # PITR is currently manual on Render
   # Contact Render support with:
   
   Support ticket template:
   ---
   Subject: PITR Request for cms-pricing-db
   
   Database: cms-pricing-db (dpg-xxx)
   Service ID: srv-yyy
   Target Time: 2025-10-21 14:25:00 UTC
   Reason: Rollback bad migration
   Urgency: High (production incident)
   
   Restore to: New instance (do NOT overwrite production)
   Name: cms-pricing-db-pitr-20251021-1425
   ---
   
   # Render SLA: 
   # - Standard: 24-48 hours
   # - Emergency (paid plan): 1-4 hours
   ```

3. **Validate WAL replay**
   ```bash
   # Once restore completes, verify recovery point
   PITR_URL="postgresql://..."
   
   # Check recovery target time
   psql $PITR_URL -c "
     SELECT pg_last_xact_replay_timestamp() as recovery_time;
   "
   # Should show: 2025-10-21 14:25:00+00
   
   # Verify data state matches expectations
   psql $PITR_URL -c "SELECT * FROM alembic_version;"
   # Should show: revision BEFORE bad migration
   
   # Check row counts
   psql $PITR_URL -c "SELECT COUNT(*) FROM gpci_indices;"
   ```

4. **Re-enable backups and monitoring**
   ```bash
   # Render automatically enables backups on new instance
   # Verify backup schedule is active
   
   # Update monitoring to point to PITR instance if cutover
   ```

**Timeline:** RTO 2-4 hours (support response + restore + validation)

**Decision: Snapshot vs PITR?**

| Scenario | Use Snapshot | Use PITR |
|----------|--------------|----------|
| Data corruption from bad migration | ✅ If daily backup OK | ✅ If need specific time |
| Accidental data deletion | ❌ Might not catch it | ✅ Restore to before deletion |
| Application bug writing bad data | ❌ Might not catch it | ✅ Restore to before bug |
| Hardware failure | ✅ Latest backup | ❌ Not needed |
| Need specific recovery point | ❌ Only daily snapshots | ✅ Minute-level precision |
| Speed of recovery | ✅ Faster (5-10 min) | ❌ Slower (1-4 hours) |

---

## 4. Quarterly Restore Drill Checklist
- Pre-drill preparation (choose backup, allocate sandbox).  
- Step-by-step execution log.  
- Success criteria (RTO ≤2 h, data integrity checks).  
- Post-drill report template with findings & remediation tasks.  

*(TODO: integrate with compliance calendar.)*

---

## 5. Rollback vs Restore Decision Matrix
- Fast rollback options (feature toggles, cutover reversal).  
- When to choose PITR vs restore vs forward fix.  
- Incident communication plan (who to notify, templates).  

*(TODO: embed flowchart, link to incident response standard.)*

---

## 6. Monitoring & Alerting
- Metrics: backup age, wal_archiving status, disk usage, replication lag.  
- Alert thresholds (backup age >36h, WAL backlog >1 GB, PITR failure).  
- Dashboard links and on-call responsibilities.  

*(TODO: provide example detector configs.)*

---

## 7. Compliance & Audit Logging
- BAA requirements (Render, storage vendor).  
- Audit log retention (≥6 years) and storage location.  
- Quarterly access review checklist.  

*(TODO: include pointers to security documentation.)*

---

## 8. Tooling & Automation Backlog
- Automated backup verification script (compare checksums).  
- Restore drill automation to ephemeral env.  
- Slack bot for backup status.  

---

## 9. References & Templates
- Change ticket template for restore/migration.  
- Incident response doc reference (`prds/STD-incident-response-prd-v1.0.md`).  
- Sample S3 lifecycle policy for WORM storage.  

---

## Change Log
| Version | Date | Summary |
|---------|------|---------|
| v0.1 (stub) | 2025-10-21 | Initial scaffold created. Sections marked TODO for detailed procedures, scripts, and checklists. |
