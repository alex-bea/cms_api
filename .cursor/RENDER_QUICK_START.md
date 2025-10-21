# Render Deployment - Quick Start Guide

**Objective:** Deploy GPCI v1.3 to production PostgreSQL on Render in 45 minutes

---

## ⚡ Quick Reference

| Part | Task | Time | Status |
|------|------|------|--------|
| 1 | Render Account Setup | 5 min | ⏸️ |
| 2 | PostgreSQL Database | 10 min | ⏸️ |
| 3 | Run Migrations | 10 min | ⏸️ |
| 4 | Load GPCI Data | 5 min | ⏸️ |
| 5 | Testing & Verification | 5 min | ⏸️ |
| 6 | Production Hardening | 10 min | ⏸️ |
| 7 | Deploy API (Optional) | 20 min | ⏸️ SKIP |

**Total: 45 minutes**

---

## 🚀 Part 1: Render Account (5 min)

**Go to:** https://render.com/

**Actions:**
1. Sign up with GitHub OAuth
2. Confirm email
3. Get $5 credit

**When done:** Proceed to Part 2

---

## 💾 Part 2: PostgreSQL Database (10 min)

**In Render Dashboard:**

1. Click **"New +"** → PostgreSQL
2. Configure:
   - Name: `cms-pricing-db`
   - Database: `cms_pricing`
   - User: `cms_user`
   - Region: Oregon (US West)
   - Version: PostgreSQL 16
   - Instance: **Starter ($7/month)**
3. Click "Create Database"
4. Wait 2-3 minutes for provisioning

**After provisioning:**

1. Go to "Connect" tab
2. Copy **External Database URL**
3. Store in 1Password/Vault
4. Create `.env` file:

```bash
DATABASE_URL=<paste_here>
```

**Test connection:**

```bash
psql $DATABASE_URL -c "SELECT version();"
```

**Expected:** PostgreSQL version info

**When done:** Proceed to Part 3

---

## 📦 Part 3: Run Migrations (10 min)

**Set environment:**

```bash
export DATABASE_URL=$(cat .env | grep DATABASE_URL | cut -d'=' -f2-)
```

**Run migrations:**

```bash
alembic upgrade head
```

**Expected output:**
```
INFO  [alembic.runtime.migration] Running upgrade  -> 001_initial_schema
INFO  [alembic.runtime.migration] Running upgrade 001 -> 002_add_mpfs_tables
INFO  [alembic.runtime.migration] Running upgrade 002 -> 003_gpci_v13_add_mac_to_nk
```

**Verify tables:**

```bash
psql $DATABASE_URL -c "\dt"
```

**Expected tables:**
- cms_locality
- cms_gpci
- cms_anes_cf
- cms_conversion_factor
- cms_oppscap
- alembic_version

**Verify GPCI v1.3 index:**

```bash
psql $DATABASE_URL -c "\d cms_gpci"
```

**Look for:**
- Unique index on (mac, locality_code, effective_from)

**When done:** Proceed to Part 4

---

## 📊 Part 4: Load GPCI Data (5 min)

**Dry-run first (preview):**

```bash
python scripts/backfill_gpci_v13.py --dry-run
```

**Expected:** Shows 109 rows to be inserted

**Execute backfill:**

```bash
python scripts/backfill_gpci_v13.py --commit
```

**Verify row count:**

```bash
psql $DATABASE_URL -c "SELECT COUNT(*) FROM cms_gpci;"
```

**Expected:** 109

**Check for duplicates:**

```bash
psql $DATABASE_URL -c "
SELECT mac, locality_code, effective_from, COUNT(*)
FROM cms_gpci
GROUP BY mac, locality_code, effective_from
HAVING COUNT(*) > 1;"
```

**Expected:** No rows (0 duplicates)

**When done:** Proceed to Part 5

---

## ✅ Part 5: Testing & Verification (5 min)

**Run parser tests:**

```bash
pytest tests/test_gpci_parser.py -v
```

**Expected:** 20/20 tests passing

**Test v1.3 natural key lookup:**

```bash
psql $DATABASE_URL -c "
SELECT mac, locality_code, effective_from, work_gpci, pe_gpci, mp_gpci
FROM cms_gpci
WHERE mac = '12101' 
  AND locality_code = '01' 
  AND effective_from = '2025-01-01'::date;"
```

**Expected:** 1 row returned

**Check Render dashboard:**
- Go to your database → Metrics
- Verify connections showing
- Check CPU/Memory usage

**When done:** Proceed to Part 6

---

## 🔒 Part 6: Production Hardening (10 min)

### 6.1: Verify Backups

In Render Dashboard:
1. Go to your database
2. Click "Backups" tab
3. Verify: Daily backups enabled, 7-day retention

### 6.2: Create Database Roles

```bash
psql $DATABASE_URL << 'SQL'
-- Create roles
CREATE ROLE migrate NOINHERIT;
CREATE ROLE app_rw NOINHERIT;
CREATE ROLE ro NOINHERIT;

-- Create users
CREATE USER cms_migrate WITH PASSWORD 'generate_secure_password_1';
GRANT migrate TO cms_migrate;
GRANT ALL ON SCHEMA public TO migrate;

CREATE USER cms_app_rw WITH PASSWORD 'generate_secure_password_2';
GRANT app_rw TO cms_app_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_rw;

CREATE USER cms_ro WITH PASSWORD 'generate_secure_password_3';
GRANT ro TO cms_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ro;

-- Default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO migrate;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ro;
SQL
```

### 6.3: Connection Pooling

In Render Dashboard:
1. Go to database → Settings
2. Find "Connection Pooling"
3. Recommended settings:
   - pool_size: 5
   - max_overflow: 10
   - pool_pre_ping: true

### 6.4: Monitoring

In Render Dashboard:
1. Go to database → Metrics
2. Enable alerts:
   - CPU > 80%
   - Memory > 80%
   - Slow queries > 1s
3. Bookmark dashboard URL

**When done:** Deployment complete! 🎉

---

## 🎉 Part 7: Deploy API (Optional - Skip for now)

**Decision:** Deploying database only for now.

**Rationale:** Foundation-first approach. API can be deployed later.

**To deploy API later:**
- See `prds/RUN-render-deployment-prd-v1.0.md` Part 7 (lines 445-562)
- Estimated time: 20 minutes
- Can use prebuilt images per PRD policy

---

## 📋 Post-Deployment Checklist

After completing Parts 1-6:

- [ ] Render account created
- [ ] PostgreSQL database provisioned (Starter tier)
- [ ] DATABASE_URL stored securely (1Password/Vault)
- [ ] All 3 migrations applied successfully
- [ ] GPCI v1.3 index exists (mac, locality_code, effective_from)
- [ ] 109 GPCI rows loaded
- [ ] 0 duplicate violations
- [ ] Parser tests passing (20/20)
- [ ] Automatic backups verified (7-day retention)
- [ ] Database roles created (migrate, app_rw, ro)
- [ ] Connection pooling configured
- [ ] Monitoring dashboard bookmarked

**Cost:** $7/month (Starter tier)  
**Risk:** LOW (managed service, daily backups, rollback available)  
**Status:** Production-ready database! ✅

---

## 🆘 Troubleshooting

### Connection Issues

```bash
# Test basic connectivity
psql $DATABASE_URL -c "SELECT 1;"

# Check active connections
psql $DATABASE_URL -c "SELECT count(*) FROM pg_stat_activity;"
```

### Migration Issues

```bash
# Check current revision
alembic current

# If needed, downgrade one step
alembic downgrade -1

# Then upgrade again
alembic upgrade head
```

### Data Loading Issues

```bash
# Check if data already exists
psql $DATABASE_URL -c "SELECT COUNT(*) FROM cms_gpci;"

# If needed, clear and reload
psql $DATABASE_URL -c "TRUNCATE cms_gpci CASCADE;"
python scripts/backfill_gpci_v13.py --commit
```

---

## 📚 Full Documentation

- **PRD:** `prds/PRD-render-hosting-prd-v1.0.md` (policy)
- **RUN:** `prds/RUN-render-deployment-prd-v1.0.md` (detailed guide)
- **Checklist:** `.cursor/GPCI_V13_DEPLOYMENT_CHECKLIST.md` (ops)
- **Log:** `.cursor/RENDER_DEPLOYMENT_LOG.md` (track progress)

---

**Ready? Let's deploy! 🚀**

Start with Part 1: https://render.com/

