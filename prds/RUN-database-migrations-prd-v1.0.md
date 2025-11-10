# Runbook: Database Migrations & Release Operations

doc_type: RUN
normative: false
requires:
  - prds/STD-database-platform-prd-v1.0.md#3-schema-lifecycle--migrations

**Status:** Draft v0.1 (in progress)  
**Owners:** Platform Engineering (DBA), Service Teams  
**Consumers:** Release Engineering, On-Call, Data Engineering  
**Change control:** PR review + Platform approval  
**Last updated:** 2025-10-21

**Cross-References**
- `prds/STD-database-platform-prd-v1.0.md` (policy requirements)  
- `prds/RUN-render-deployment-prd-v1.0.md` (Render deploy workflow)  
- `prds/RUN-database-sanitization-prd-v1.0.md` (tokenized snapshot refresh)  
- `prds/RUN-database-backup-dr-prd-v1.0.md` (rollback/PITR procedures)  
- `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (recent migration example)
- `prds/DOC-master-catalog-prd-v1.0.md` (master documentation catalog)

---

## 0. Purpose
Provide actionable steps for authoring, testing, and executing database schema changes under the migrations-first standard. This runbook expands §3 of the database platform standard.

---

## 1. Prerequisites & Tooling
- Alembic configured and `alembic.ini` points to target environment  
- PgBouncer connection details documented  
- Access to prod-sized snapshot for dry-run  
- Change ticket/PR template (TBD)
- Snapshot utilities installed (`cms_pricing.ops.audit_snapshot_paths`, `cms_pricing.ops.repair_snapshot_paths`) for verifying/repairing dataset snapshot metadata pre/post migration when manifests move locations.

- PgBouncer mode noted (transaction pooling): use **SET LOCAL** for any session state (e.g., tenant GUC) and avoid server-side prepares unless specifically enabled (see §6).
- Alembic offline DDL ready: generate SQL via 
`alembic upgrade --sql` and attach to the PR/change ticket for review (see §3).
- Observability: clients set 
`application_name` per job/role; migration job logs revision IDs and duration.

*(TODO: enumerate command snippets, environment variables, safe sandbox instructions.)*

---

## 2. Authoring Workflow (Developer POV)
1. Generate revision using Alembic (`alembic revision --autogenerate` or manual).  
2. Review generated DDL for compatibility with migrations-first guidelines.  
3. Update seed scripts/fixtures if necessary.  
4. Run unit/integration tests.  
5. Update change ticket with description & risk.  

*(TODO: include checklist for RLS updates, naming convention lint, PHI registry impact.)*

---

## 3. Dry-Run Procedure (Prod-sized Snapshot)
- Acquire/refresh sanitized snapshot (see sanitization runbook).
- Generate offline SQL for review:

```bash
# Produce upgrade SQL without executing it
alembic upgrade head --sql > /tmp/ddl.sql
```

- Apply the SQL to the snapshot and measure locks/runtime:

```bash
/usr/bin/time -l psql $SNAPSHOT_DATABASE_URL -f /tmp/ddl.sql
```

- Validate no unexpected differences (\d, pg_indexes, constraints).
- Capture lock-safety evidence (no long ACCESS EXCLUSIVE waits).
- Attach results (DDL script, logs, validation checklist) to PR/change ticket.

*(TODO: script references, example commands, validation checklist automation.)*

---

## 4. Decision Tree: Stamp vs Upgrade vs Fix

**Critical operational decision:** When should you use `alembic stamp`, `alembic upgrade`, or fix the mismatch?

```
┌─────────────────────────────────────────┐
│ Is this a brand new, empty database?    │
└───────────────┬─────────────────────────┘
                │
        ┌───────┴───────┐
        │ YES           │ NO
        ▼               ▼
┌───────────────┐ ┌────────────────────────────────┐
│ Go to §5      │ │ Does physical schema match     │
│ Bootstrap     │ │ any Alembic revision exactly?  │
└───────────────┘ └────────┬───────────────────────┘
                           │
                   ┌───────┴───────┐
                   │ YES           │ NO/UNKNOWN
                   ▼               ▼
         ┌──────────────────┐ ┌──────────────────────┐
         │ Use alembic      │ │ DANGER: Schema drift │
         │ stamp <revision> │ │ detected!            │
         │ (with approval)  │ │                      │
         │ → Go to §5.3     │ │ → Fix drift first:   │
         │                  │ │  - Compare schema    │
         │                  │ │  - Create migration  │
         │                  │ │  - Test on snapshot  │
         │                  │ │  - Never stamp!      │
         └──────────────────┘ └──────────────────────┘
```

**Golden Rules:**
1. **Stamp ONLY IF:** Physical schema EXACTLY matches target revision + Platform approval + documented
2. **Never stamp** on unknown/mismatched state → creates permanent drift
3. **Production:** Always migrations-first (§5.1) → never stamp
4. For any approved **stamp**, the offline SQL diff (`alembic upgrade --sql`) and schema validation outputs **must be attached** to the change ticket.

---

## 5. Fresh Database Bootstrap & Stamp Approval

### 5.1 Option A — Migrations-First (Required for Production)

**When to use:** All production databases, new staging/dev environments (recommended)

**Prerequisites:**
- [ ] Migration `000_initial_schema.py` exists (creates all base tables)
- [ ] Database is completely empty (verify with `\dt`)
- [ ] `DATABASE_URL` environment variable set
- [ ] `alembic.ini` configured for target environment

**Steps:**

```bash
# 1. Verify database is empty
psql $DATABASE_URL -c "\dt"
# Expected: "Did not find any tables."

# 2. Check current Alembic revision
alembic current
# Expected: No revision (empty database)

# 3. Run migrations from scratch
alembic upgrade head

# 4. Verify all tables created
psql $DATABASE_URL -c "\dt" | wc -l
# Expected: 40+ tables

# 5. Verify Alembic tracking
alembic current
# Expected: Shows head revision (e.g., 6d0f0408be80)

# 6. Verify critical indexes
psql $DATABASE_URL -c "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname;" | head -20

# 7. Run smoke tests
pytest tests/test_database_smoke.py
```

**Success criteria:**
- ✅ All migrations applied without errors
- ✅ Alembic version table shows head revision
- ✅ All expected tables, indexes, constraints exist
- ✅ Smoke tests pass

---

### 5.2 Option B — Models-First Bootstrap (Dev/Staging Only)

**When to use:**
- Non-production environments ONLY
- When migrations don't exist yet for current models
- Quick local development setup
- Platform approval required

**⚠️ WARNING:** Never use in production! Creates risk of schema drift.

**Prerequisites:**
- [ ] Platform approval obtained (change ticket required)
- [ ] Environment is non-production (dev/staging)
- [ ] Will document stamp decision in deployment log
- [ ] Understand this is technical debt to be migrated later

**Steps:**

```bash
# 1. Verify database is empty
psql $DATABASE_URL -c "\dt"
# Expected: "Did not find any tables."

# 2. Run create_tables.py to bootstrap from models
python scripts/create_tables.py

# Example output:
# ✅ Created 43 tables from SQLAlchemy models:
#  - alembic_version
#  - gpci_indices
#  - releases
#  ... (full table list)

# 3. Validate schema before stamp (§5.3)
# Run full validation checklist

# 4. Stamp head ONLY after validation passes
alembic stamp head

# 5. Document decision
cat << EOF >> .cursor/DEPLOYMENT_LOG.md
## Models-First Bootstrap - $(date +%Y-%m-%d)

**Environment:** staging
**Approver:** Platform Team (Ticket #1234)
**Revision stamped:** $(alembic current)
**Reason:** Initial setup, migrations to be created retroactively
**Validation:** All §5.3 checks passed
EOF
```

**Post-stamp obligations:**
1. Create migration `000_initial_schema.py` to match current state
2. Remove `Base.metadata.create_all()` from app startup
3. Add CI check to prevent future models-first usage
4. Document in ADR

---

### 5.3 Validation Checklist Before Stamp

**⚠️ CRITICAL:** Never `stamp` without completing this checklist. Stamping incorrect state creates permanent schema drift.

```bash
# 1. Verify ALL expected tables exist
psql $DATABASE_URL -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;" > /tmp/actual_tables.txt
# Compare against expected list from migrations
diff /tmp/actual_tables.txt expected_tables.txt

# 2. Verify critical indexes exist
psql $DATABASE_URL << 'SQL'
SELECT 
  schemaname, 
  tablename, 
  indexname, 
  indexdef
FROM pg_indexes 
WHERE schemaname = 'public'
AND indexname IN (
  'uq_gpci_mac_locality_effective',
  'idx_gpci_effective',
  'idx_releases_dataset_version',
  -- Add all critical indexes
)
ORDER BY tablename, indexname;
SQL

# 3. Verify foreign keys
psql $DATABASE_URL << 'SQL'
SELECT 
  conname AS constraint_name,
  conrelid::regclass AS source_table,
  confrelid::regclass AS referenced_table,
  a.attname AS source_column
FROM pg_constraint
JOIN pg_attribute a ON a.attnum = ANY(conkey) AND a.attrelid = conrelid
WHERE contype = 'f'
AND connamespace = 'public'::regnamespace
ORDER BY source_table, constraint_name;
SQL

# 4. Verify unique constraints match natural keys
psql $DATABASE_URL -c "SELECT conname, conrelid::regclass FROM pg_constraint WHERE contype = 'u' ORDER BY conrelid::regclass;"

# 5. Compare schema with target migration DDL
# Extract DDL from migration file
grep "op.create_table" alembic/versions/$(alembic current).py | wc -l
# Should match table count

# 6. Verify no extra/missing tables
psql $DATABASE_URL -c "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';"
# Compare with expected count from migrations

# 7. Run automated schema diff (if available)
alembic check  # If supported by Alembic version
```

**Approval template (change ticket):**

```markdown
## Alembic Stamp Request

**Environment:** [dev/staging]
**Current state:** Fresh database with tables created from models
**Target revision:** [revision_id - $(alembic heads)]
**Validation:** All §5.3 checks passed (attach logs)
**Risk:** Low (non-prod environment, documented for migration)
**Approver:** Platform Engineering
**Documentation:** [Link to deployment log]

### Validation Results:
- ✅ 43 tables match expected list
- ✅ All critical indexes present
- ✅ Foreign keys validated
- ✅ Unique constraints match natural keys
- ✅ No schema drift detected

### Post-stamp plan:
1. Create retroactive migration 000_initial_schema.py
2. Remove create_all() from app code
3. Add CI enforcement
```

---

### 5.4 Case Study: 2025-10-21 Render Deployment

**Problem:** Models defined v1.3 schema (MAC in GPCI natural key), but migrations assumed v1.2 existed. Running `alembic upgrade` failed with "relation does not exist" because base tables weren't created.

**Root cause:** Architectural mismatch:
- Models define schema via SQLAlchemy (created by `create_all()` on app startup)
- Migrations modify schema via Alembic (assumed tables exist)
- Database-only deployment → app never started → tables never created

**Solution:**
1. Created `scripts/create_tables.py` to manually run `Base.metadata.create_all()`
2. Fixed duplicate index name (`idx_opps_effective` in two models)
3. Validated schema exactly matched target revision
4. Ran `alembic stamp head` with Platform approval
5. Documented decision in `.cursor/RENDER_DEPLOYMENT_LOG.md`

**Lesson:** Migrations-first prevents this conflict. Use models-first bootstrap only for non-prod with approval.

**Migration to migrations-first:**
1. Create `alembic/versions/000_initial_schema.py` with all `op.create_table()` statements
2. Remove `Base.metadata.create_all()` from `cms_pricing/main.py`
3. Add CI check: fail if `Base.metadata.create_all` detected outside test code
4. Document transition in ADR-001

---

## 6. Execution Pipeline (Render/CI Job)
- Migration job identity: `migrate` role.  
- Steps:
  1. Set session safety timeouts at job start and in each migration transaction:

```sql
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
```

  2. (If using PgBouncer transaction pooling) ensure any GUCs use 
`SET LOCAL` and disable server-side prepares or use the driver's simple protocol.
  3. Run 
`alembic upgrade head` and emit structured logs (start/end, revision IDs, duration, success/failure).
  4. Alert on failure (Slack/Incident).
- Post-checks: verify `alembic_version`, run smoke queries, monitor metrics.
## 6.1 Migration Safety Prologue (for Alembic revisions)

```python
# Prepend in each migration to limit lock blast radius
from alembic import op
def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    # ... your DDL here
```

**Note:** Use `SET LOCAL` so settings apply only to the current transaction, which is safe under PgBouncer transaction pooling.

*(TODO: provide GitHub Actions/Render Job YAML snippets; alert routing.)*

---

## 7. Online Migration Patterns & Maintenance Windows

### 7.1 Zero‑Downtime DDL Rules (Prod)
- **Indexes:** always use 
`CREATE INDEX CONCURRENTLY` / `DROP INDEX CONCURRENTLY`. After creation, verify validity:

```sql
SELECT i.schemaname, i.tablename, i.indexname
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_index x ON x.indexrelid = c.oid
WHERE NOT x.indisvalid;
```

- **Adds:** on PG ≥ 11, `ADD COLUMN ... DEFAULT <const>` is metadata‑only; otherwise stage: add nullable → backfill → set DEFAULT → set NOT NULL.
- **Renames:** add new column/table → dual‑write/backfill → cut reads → drop old (staged swap).
- **Dangerous:** large `ALTER TYPE`/rewrites without shadow table or view‑swap (avoid).

### 7.2 Schema vs Data Migrations
- Alembic revisions are for **schema**.
- Large data backfills run as separate jobs with throttling/checkpoints; never gate deploy on multi‑hour writes.

### 7.3 Partitioning (when and how)
- Prefer native declarative partitions (time/tenant). For live tables, use shadow table + backfill + swap or a view‑swap to avoid long locks.

### 7.4 Maintenance Windows & Backout
- Template + approval matrix.
- Backout plan: PITR cutover or flip to hot standby per RUN‑backup‑dr.

---

## 8. Emergency Migration / Hotfix Procedure
- Incident commander authorization required; notify on-call and Platform immediately.  
- Execute the controlled migration steps here, then pivot to `prds/RUN-database-backup-dr-prd-v1.0.md` if rollback or PITR is required.  
- Post-incident actions (log, audit trail, restore drill follow-up) captured in the change ticket.  

*(TODO: integrate with incident-response standard and add communication templates.)*

---

## 9. Tooling & CI Enforcement
- CI: fail if a prod migration uses non‑concurrent index ops.
- CI: flag `ALTER TABLE ... TYPE` on large tables without an approved online plan.
- CI: flag `DROP COLUMN`/`RENAME` without staged swap plan.
- CI: detect `Base.metadata.create_all` usage outside tests.
- Emit migration duration/revision metrics for SLOs.

---

## 10. Troubleshooting: Common Migration Errors

This section documents errors encountered during today's deployment and their fixes.

### 10.1 "relation does not exist" During Migration

**Error:**
```
psycopg2.errors.UndefinedTable: relation "gpci_indices" does not exist
  at alembic/versions/004_gpci_v12_compat_view.py
```

**Root cause:** Migration assumes table exists, but tables weren't created yet.

**Diagnosis steps:**
```bash
# 1. Check which tables exist
psql $DATABASE_URL -c "\dt"

# 2. Check Alembic revision
alembic current

# 3. Check migration history
alembic history --verbose
```

**Fix options:**

**Option A:** Models-first bootstrap (dev/staging only)
```bash
# Create tables from models first
python scripts/create_tables.py

# Validate schema (§5.3)
# Then stamp
alembic stamp head
```

**Option B:** Fix migration order (production approach)
```bash
# Create missing 000_initial_schema.py that creates base tables
alembic revision -m "initial_schema"

# Edit migration to create all base tables
# Run from scratch
alembic upgrade head
```

---

### 10.2 "duplicate index/constraint name" During create_all()

**Error:**
```
psycopg2.errors.DuplicateTable: relation "idx_opps_effective" already exists
```

**Root cause:** Two different SQLAlchemy models defined an index with the same name.

**Diagnosis:**
```bash
# Search for duplicate index names in models
grep -r "Index(" cms_pricing/models/ | grep "idx_" | sort | uniq -d

# Example found:
# cms_pricing/models/rvu.py:    Index("idx_opps_effective", ...)
# cms_pricing/models/fee_schedules.py:    Index("idx_opps_effective", ...)
```

**Fix:**
```python
# Rename to be table-specific
# Before:
class OPPSCap:
    __table_args__ = (Index("idx_opps_effective", "effective_start"),)

class FeeOPPS:
    __table_args__ = (Index("idx_opps_effective", "effective_from"),)

# After:
class OPPSCap:
    __table_args__ = (Index("idx_opps_cap_effective", "effective_start"),)

class FeeOPPS:
    __table_args__ = (Index("idx_fee_opps_effective", "effective_from"),)
```

**Prevention:** Add CI lint (see §9) to catch duplicates before merge.

---

### 10.3 Python Environment: Segmentation Fault (exit 139)

**Error:**
```bash
python scripts/backfill_gpci_v13.py
[1]    12345 segmentation fault  python scripts/backfill_gpci_v13.py
```

**Root cause:** Conda environment conflicts with pandas/numpy on macOS (common issue).

**Diagnosis:**
```bash
# Check Python environment
which python
python --version

# Try importing pandas
python -c "import pandas; print(pandas.__version__)"
# If this segfaults, environment is broken
```

**Fix options:**

**Option A:** Create clean venv (recommended)
```bash
# Use system Python instead of conda
python3 -m venv .venv-clean
source .venv-clean/bin/activate
pip install -r requirements.txt
python scripts/backfill_gpci_v13.py --commit
```

**Option B:** Fix conda environment
```bash
conda update pandas numpy
# OR
conda create -n cms-pricing-clean python=3.11
conda activate cms-pricing-clean
pip install -r requirements.txt
```

**Option C:** Load data via SQL instead
```bash
# Export data to CSV
# Load via psql COPY
psql $DATABASE_URL -c "\COPY gpci_indices FROM 'data.csv' CSV HEADER"
```

---

### 10.4 Migration Hangs / Long Lock Wait

**Error:** Migration runs for minutes without completing.

**Diagnosis:**
```bash
# Check for blocking locks
psql $DATABASE_URL << 'SQL'
SELECT 
  pid,
  usename,
  pg_blocking_pids(pid) AS blocked_by,
  query AS blocked_query
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0;
SQL

# Check migration process
ps aux | grep alembic
```

**Fix:**
```bash
# Set safety timeouts (add to migration)
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

# For stuck migration
# 1. Cancel gracefully
SELECT pg_cancel_backend(<pid>);

# 2. If needed, terminate
SELECT pg_terminate_backend(<pid>);

# 3. Verify no partial changes
alembic current
psql $DATABASE_URL -c "\dt"

# 4. Fix migration to use CONCURRENTLY
# Then retry
```

---

### 10.5 Foreign Key Constraint Violation During Migration

**Error:**
```
psycopg2.errors.ForeignKeyViolation: insert or update on table "X" violates foreign key constraint "fk_X_Y"
```

**Diagnosis:**
```bash
# Find orphaned rows
psql $DATABASE_URL << 'SQL'
SELECT x.id, x.y_id
FROM x
LEFT JOIN y ON x.y_id = y.id
WHERE y.id IS NULL;
SQL
```

**Fix:**
```bash
# Option A: Clean orphaned rows before migration
DELETE FROM x WHERE y_id NOT IN (SELECT id FROM y);

# Option B: Add to migration
op.execute("DELETE FROM x WHERE y_id NOT IN (SELECT id FROM y)")
op.create_foreign_key("fk_x_y", "x", "y", ["y_id"], ["id"])
```

---

### 10.6 Alembic Version Table Missing or Corrupted

**Error:**
```
sqlalchemy.exc.ProgrammingError: (psycopg2.errors.UndefinedTable) relation "alembic_version" does not exist
```

**Diagnosis:**
```bash
# Check if table exists
psql $DATABASE_URL -c "SELECT * FROM alembic_version;"

# Check database state
psql $DATABASE_URL -c "\dt" | grep alembic
```

**Fix:**
```bash
# Recreate version table
alembic stamp head

# If that fails, manual fix:
psql $DATABASE_URL << 'SQL'
CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
INSERT INTO alembic_version VALUES ('head_revision_id');
SQL
```

---

### 10.7 Migration Rollback / Downgrade Failed

**Error:** Migration succeeded, but downgrade fails or causes data loss.

**Policy:** Don't use downgrades in production (see STD §3).

**Recovery:**
```bash
# Don't attempt downgrade
# Instead, restore from backup:

# 1. Identify last good state
# 2. Restore snapshot (see RUN-database-backup-dr §3)
# 3. OR create forward-fix migration

# Forward fix example:
alembic revision -m "revert_bad_change"
# Edit to undo the bad change
alembic upgrade head
```

---

### 10.8 Schema Drift Detected (models ≠ migrations)

**Symptom:** Tests fail, app crashes with "column does not exist" or similar.

**Diagnosis:**
```bash
# Generate autogenerate diff
alembic revision --autogenerate -m "detect_drift"
cat alembic/versions/XXXX_detect_drift.py
# If it shows changes, you have drift

# Manual comparison
psql $DATABASE_URL -c "\d tablename"
# Compare with model definition
```

**Fix:**
```bash
# Option A: Create migration to match models
alembic revision -m "fix_drift"
# Add DDL to align database with models
alembic upgrade head

# Option B: Revert models to match database
git diff cms_pricing/models/
# Revert unauthorized model changes

# Prevention:
# 1. Enforce migrations-first via CI
# 2. Block direct model changes without migration
# 3. Add pre-commit hook
```

---

## 11. References & Templates
- Change ticket template (link TBD)  
- ADR-001 (migrations-first) and ADR-002 (stamp policy) — pending  
- Sample GitHub Actions workflow (pending)  

---

## Change Log
| Version | Date | Summary |
|---------|------|---------|
| v0.2 | 2025-10-21 | Added offline DDL flow, zero‑downtime rules, migration safety prologue, PgBouncer notes, partitioning guidance, and CI enforcement checks. |
| v0.1 (stub) | 2025-10-21 | Initial scaffold created. Sections marked TODO for detailed procedures, scripts, and templates. |
