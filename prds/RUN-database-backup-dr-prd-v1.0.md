# Runbook: Database Backup, Restore & Disaster Recovery
# Runbook: Database Backup, Restore & Disaster Recovery

**Status:** Stub v0.1 (in progress)  
**Owners:** Platform Engineering (DBA), SRE  
**Consumers:** On-call responders, Release Mgmt., Security  
**Change control:** PR review + DBA approval  
**Last updated:** 2025-10-21

**Cross-References**
- `prds/STD-database-platform-prd-v1.0.md` §5 (policy)  
- `prds/RUN-render-deployment-prd-v1.0.md` (deployment hardening)  
- `prds/STD-observability-monitoring-prd-v1.0.md` (alerting)

---

## 0. Purpose
Operationalise backup retention, manual dumps, restore drills, and PITR cutovers for all PostgreSQL environments.

---

## 1. Backup Inventory & Automation
- Document Render automated backup schedule per env (dev/staging/prod).  
- Commands/API calls to verify last successful backup (`psql`, Render API).  
- Procedure to export backup manifest to immutable storage (S3 object-lock).  

*(TODO: include screenshots/CLI commands.)*

---

## 2. Manual `pg_dump` Procedures
- When required (high-risk migrations, data export).  
- Command templates for schema-only vs full dumps; ensure client version matches server.  
- Storage location: `s3://[env]-postgres-ddl-dumps/CHANGE_TICKET_ID/`.  
- Integrity check (`pg_restore --list`, checksums).  

*(TODO: automation script, sample change ticket text.)*

---

## 3. Restore & PITR Playbooks
### 3.1 Snapshot Restore (Render UI/API)
1. Identify backup timestamp.  
2. Initiate restore to new instance (guard against overwriting prod).  
3. Validate data integrity (run smoke queries, checksum critical tables).  
4. Cutover procedure (DNS/app config update).  

### 3.2 PITR (Point-In-Time Recovery)
1. Select target timestamp (within WAL retention).  
2. Request PITR via Render support/API.  
3. Validate WAL applied; check `pg_last_xlog_replay_location()`.  
4. Re-enable backups and monitoring.  

*(TODO: include decision tree and example timelines.)*

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

