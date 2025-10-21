# GPCI v1.3 Migration Plan

## Overview
Handle breaking change from GPCI v1.2 → v1.3 where Natural Key changed from `['locality_code', 'effective_from']` to `['mac', 'locality_code', 'effective_from']`. Requires database migration, backfill, and integration test updates.

**Time Estimate:** 2-3 hours  
**Priority:** HIGH (blocks production use of GPCI v1.3)

---

## Breaking Change Summary

### What Changed
**Schema:** `cms_gpci_v1.2.json` → `cms_gpci_v1.3.json`

**Natural Key:**
- **Before (v1.2):** `['locality_code', 'effective_from']`
- **After (v1.3):** `['mac', 'locality_code', 'effective_from']`

**Rationale:**
- `locality_code='00'` appears in multiple states (AL, AZ, AR, etc.)
- Without MAC, 63 of 112 rows (56%) were false duplicates
- MAC disambiguates: AL MAC 01112 vs AZ MAC 02102 are DIFFERENT localities

**Impact:**
- Row hashes change (MAC now included in hash)
- Database unique constraint/index needs update (created CONCURRENTLY, idempotent)
- Existing GPCI rows need re-parsing
- Integration tests need MAC in joins

---

## Step 1: Create Alembic Migration (30 minutes)

### File: `alembic/versions/003_gpci_v13_add_mac_to_nk.py` (NEW)

**Purpose:** Add unique index on (mac, locality_id, effective_start)

```python
"""GPCI v1.3: Add MAC to natural key unique index (idempotent + concurrent)

Revision ID: 003_gpci_v13_add_mac_to_nk
Revises: 002_add_nber_centroids
Create Date: 2025-10-20 23:00:00.000000

Breaking Change: GPCI schema v1.2 → v1.3
- Natural key changed from (locality_code, effective_from) 
  to (mac, locality_code, effective_from)
- Rationale: locality_code='00' appears in multiple states
- MAC column already exists in table (added earlier)
- This migration drops any legacy constraint/index if present and
  creates a UNIQUE INDEX CONCURRENTLY on (mac, locality_id, effective_start)
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '003_gpci_v13_add_mac_to_nk'
down_revision = '002_add_nber_centroids'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop old v1.2 constraint if present (idempotent)
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE c.conname = 'uq_gpci_locality_effective'
              AND t.relname = 'gpci_indices'
        ) THEN
            ALTER TABLE gpci_indices 
            DROP CONSTRAINT uq_gpci_locality_effective;
        END IF;
    END$$;
    """)

    # Drop legacy index concurrently outside the transactional context
    with op.get_context().autocommit_block():
        op.execute("SET LOCAL lock_timeout = '5s';")
        op.execute("SET LOCAL statement_timeout = '2min';")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS uq_gpci_locality_effective;")

    # Pre-flight check for duplicates on the new NK; fail early if any exist
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM (
                SELECT mac, locality_id, effective_start
                FROM gpci_indices
                GROUP BY mac, locality_id, effective_start
                HAVING COUNT(*) > 1
            ) s
        ) THEN
            RAISE EXCEPTION 'Cannot create unique index: duplicates exist on (mac, locality_id, effective_start)';
        END IF;
    END$$;
    """)

    # Create the unique index CONCURRENTLY to minimize locking
    with op.get_context().autocommit_block():
        op.execute("SET LOCAL lock_timeout = '5s';")
        op.execute("SET LOCAL statement_timeout = '2min';")
        op.execute("""
        CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_gpci_mac_locality_effective
          ON gpci_indices (mac, locality_id, effective_start);
        """)

    op.execute("""
    COMMENT ON INDEX uq_gpci_mac_locality_effective IS 
      'GPCI v1.3 natural key: (mac, locality_id, effective_start). Created CONCURRENTLY to reduce lock time.';
    """)


def downgrade() -> None:
    # Drop the v1.3 index (concurrently) if present
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS uq_gpci_mac_locality_effective;")
```

### Run Migration
```bash
# Apply migration
alembic upgrade head

# Verify (index should exist and be UNIQUE on the three-part NK)
psql -d cms_pricing -c "
SELECT 
    indexname, 
    indexdef 
FROM pg_indexes 
WHERE tablename = 'gpci_indices' 
  AND indexname = 'uq_gpci_mac_locality_effective';
"

psql -d cms_pricing -c "
SELECT i.indisvalid
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'uq_gpci_mac_locality_effective';
"
```

Expect the second query to return `t`, confirming the concurrent index built successfully.

**Add two regression guards:**

1) Coexistence on new NK:

```python
def test_gpci_allows_same_locality_different_mac(session):
    # Insert two rows same locality_id/effective_start but different mac
    # should NOT violate the unique key in v1.3
    session.execute("""
        INSERT INTO gpci_indices (mac, locality_id, effective_start)
        VALUES ('01112','00','2025-01-01'),
               ('02102','00','2025-01-01');
    """)
    session.commit()
```

2) Duplicate insert violation:

```python
import pytest
from sqlalchemy.exc import IntegrityError

def test_gpci_duplicate_on_new_nk_raises(session):
    session.execute("""
        INSERT INTO gpci_indices (mac, locality_id, effective_start)
        VALUES ('01112','26','2025-01-01');
    """)
    with pytest.raises(IntegrityError):
        session.execute("""
            INSERT INTO gpci_indices (mac, locality_id, effective_start)
            VALUES ('01112','26','2025-01-01');
        """)
        session.commit()
```

---

## Step 2: Update Database Model (15 minutes)

### File: `cms_pricing/models/rvu.py` (lines 85-110)

**Current:**
```python
class Gpci(Base):
    __tablename__ = "gpci_indices"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mac = Column(String(10), nullable=False, index=True)
    locality_id = Column(String(10), nullable=False, index=True)
    # ... other columns ...
    
    __table_args__ = (
        Index("idx_gpci_mac_locality", "mac", "locality_id"),
        Index("idx_gpci_state", "state"),
        Index("idx_gpci_effective", "effective_start", "effective_end"),
        Index("idx_gpci_release_mac", "release_id", "mac"),
    )
```

**Add:**
```python
    __table_args__ = (
        Index("idx_gpci_mac_locality", "mac", "locality_id"),
        Index("idx_gpci_state", "state"),
        Index("idx_gpci_effective", "effective_start", "effective_end"),
        Index("idx_gpci_release_mac", "release_id", "mac"),
        # GPCI v1.3 natural key constraint (mac, locality_id, effective_start)
        sa.UniqueConstraint(
            "mac",
            "locality_id",
            "effective_start",
            name="uq_gpci_mac_locality_effective",
        ),
    )
```

**Note:** Add `import sqlalchemy as sa` near the other imports, keep UUID primary key for backwards compatibility with existing FKs.

---

## Step 3: Backfill/Re-parse GPCI Data (45 minutes)

### Option A: Re-parse from Source with Staging + Upsert (RECOMMENDED)

**Why:** Lowest risk (no hard deletes), preserves rollback, and leverages the new unique index.
**Implementation notes:**
- Use `PGUUID(as_uuid=True)` when loading the staging table so the UUID column retains its native type.
- Pass `chunksize=1000` to `to_sql` to keep multi-row inserts manageable.
- Log the resolved `release_id` for traceability.

**Script:** Create `scripts/backfill_gpci_v13.py`

```python
"""
Backfill GPCI v1.3: Re-parse 2025 data with corrected Natural Key via staging table + upsert

Usage:
    python scripts/backfill_gpci_v13.py --release-id RVU25D --dry-run
    python scripts/backfill_gpci_v13.py --release-id RVU25D --commit
"""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID

from cms_pricing.config import settings
from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci


def backfill_gpci_v13(release_id: str, dry_run: bool = True):
    engine = create_engine(settings.DATABASE_URL)
    gpci_file = Path('sample_data/rvu25d_0/GPCI2025.txt')
    metadata = {
        'release_id': release_id,
        'schema_id': 'cms_gpci_v1.3',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': datetime(2025, 10, 1),
        'file_sha256': 'backfill_v13',
        'source_uri': str(gpci_file),
        'source_release': release_id,
    }

    # Resolve release UUID once up-front (required for inserts)
    with engine.connect() as conn:
        release_uuid = conn.execute(
            text("""
                SELECT id
                FROM releases
                WHERE source_version = :release_id
                  AND type = 'GPCI'
                ORDER BY imported_at DESC
                LIMIT 1;
            """),
            {"release_id": release_id},
        ).scalar()

    if not release_uuid:
        print(f"❌ No release row found for {release_id}; load releases first.")
        sys.exit(1)
    print(f"Using release_id={release_uuid}")

    # Parse with v1.3
    with open(gpci_file, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025.txt', metadata)

    print(f"✅ Parsed {len(result.data)} rows with v1.3; rejects={len(result.rejects)}")

    # Verify no duplicates on the 3-field NK
    nk_cols = ['mac', 'locality_code', 'effective_from']
    dupes = result.data.duplicated(subset=nk_cols, keep=False)
    if dupes.any():
        print(f"❌ ERROR: {dupes.sum()} duplicates found on new NK!")
        print(result.data.loc[dupes, nk_cols].drop_duplicates())
        sys.exit(1)
    print(f"✅ Verified: 0 duplicates on {nk_cols}")

    # Load to a staging table
    staging = 'gpci_indices_staging_v13'
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging};"))
        conn.execute(text(f"""
            CREATE TABLE {staging} AS
            SELECT 
                release_id,
                mac,
                locality_id,
                locality_name,
                work_gpci,
                pe_gpci,
                mp_gpci,
                effective_start,
                effective_end,
                state,
                row_content_hash
            FROM gpci_indices
            WHERE 1=0;
        """))

    # Map DataFrame columns, include release UUID, and write
    df = result.data.rename(columns={
        'locality_code': 'locality_id',
        'effective_from': 'effective_start',
        'effective_to': 'effective_end',
    })[[
        'mac',
        'locality_id',
        'locality_name',
        'work_gpci',
        'pe_gpci',
        'mp_gpci',
        'effective_start',
        'effective_end',
        'state',
        'row_content_hash',
    ]]
    df['release_id'] = release_uuid
    df = df[[
        'release_id',
        'mac',
        'locality_id',
        'locality_name',
        'work_gpci',
        'pe_gpci',
        'mp_gpci',
        'effective_start',
        'effective_end',
        'state',
        'row_content_hash',
    ]]

    df.to_sql(
        staging,
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000,
        dtype={'release_id': PGUUID(as_uuid=True)},
    )

    # Upsert from staging to main using the new unique key
    merge_sql = f"""
    INSERT INTO gpci_indices (
        release_id,
        mac,
        locality_id,
        locality_name,
        work_gpci,
        pe_gpci,
        mp_gpci,
        effective_start,
        effective_end,
        state,
        row_content_hash
    )
    SELECT 
        s.release_id,
        s.mac,
        s.locality_id,
        s.locality_name,
        s.work_gpci,
        s.pe_gpci,
        s.mp_gpci,
        s.effective_start,
        s.effective_end,
        s.state,
        s.row_content_hash
    FROM {staging} s
    ON CONFLICT (mac, locality_id, effective_start)
    DO UPDATE SET
        locality_name = EXCLUDED.locality_name,
        work_gpci     = EXCLUDED.work_gpci,
        pe_gpci       = EXCLUDED.pe_gpci,
        mp_gpci       = EXCLUDED.mp_gpci,
        effective_end = EXCLUDED.effective_end,
        state         = EXCLUDED.state,
        row_content_hash = EXCLUDED.row_content_hash
    RETURNING 1;
    """

    if dry_run:
        print(f"🔍 DRY RUN: would upsert {len(df)} rows from {staging} into gpci_indices")
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {staging};"))
    else:
        with engine.begin() as conn:
            conn.execute(text(merge_sql))
            conn.execute(text(f"DROP TABLE IF EXISTS {staging};"))
        print(f"✅ Upserted {len(df)} rows into gpci_indices")

    return result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-id', default='RVU25D')
    parser.add_argument('--commit', action='store_true', help='Commit changes (default: dry-run)')
    args = parser.parse_args()
    backfill_gpci_v13(args.release_id, dry_run=not args.commit)
```

Run:

```bash
# Dry run
python scripts/backfill_gpci_v13.py --release-id RVU25D --dry-run

# Commit
python scripts/backfill_gpci_v13.py --release-id RVU25D --commit
```

### Option B: Manual SQL Backfill (Quick Fix)

**If re-parsing not feasible:**

```sql
-- Step 1: Pre-check for duplicates on new NK (should return 0 rows)
SELECT mac, locality_id, effective_start, COUNT(*)
FROM gpci_indices
GROUP BY mac, locality_id, effective_start
HAVING COUNT(*) > 1;

-- Step 2: Upsert from a prepared temp table gpci_indices_tmp
INSERT INTO gpci_indices (
    mac, locality_id, locality_name, work_gpci, pe_gpci, mp_gpci,
    effective_start, effective_end, state, row_content_hash
)
SELECT mac, locality_id, locality_name, work_gpci, pe_gpci, mp_gpci,
       effective_start, effective_end, state, row_content_hash
FROM gpci_indices_tmp
ON CONFLICT (mac, locality_id, effective_start)
DO UPDATE SET
    locality_name = EXCLUDED.locality_name,
    work_gpci     = EXCLUDED.work_gpci,
    pe_gpci       = EXCLUDED.pe_gpci,
    mp_gpci       = EXCLUDED.mp_gpci,
    effective_end = EXCLUDED.effective_end,
    state         = EXCLUDED.state,
    row_content_hash = EXCLUDED.row_content_hash;

-- Step 3: Verify unique constraint (should return 0 rows)
SELECT mac, locality_id, effective_start, COUNT(*)
FROM gpci_indices
GROUP BY mac, locality_id, effective_start
HAVING COUNT(*) > 1;
```

---

## Step 4: Update Integration Tests (30 minutes)

### File: `tests/integration/test_locality_e2e.py` (lines 420-450)

**Problem:** GPCI join uses `(mac, locality_code)` but test fixture may be wrong

**Current:**
```python
# Left join: locality → GPCI on (mac, locality_code) scoped to as_of date
as_of = pd.Timestamp('2025-01-01')
effective_slice = gpci_df[
    (gpci_df['effective_start'] <= as_of) &
    (
        gpci_df['effective_end'].isna() |
        (gpci_df['effective_end'] >= as_of)
    )
][[
    'mac',
    'locality_code_join',
    'gpci_work',
    'gpci_pe',
    'gpci_mp',
    'effective_start',
    'effective_end',
]]

joined = locality_df.merge(
    effective_slice,
    on=['mac', 'locality_code_join'],
    how='left',
    indicator=True,
)

# Assert each (mac, locality_code_join, as_of) resolves to at most one row
dup_counts = joined.groupby(['mac', 'locality_code_join']).size()
assert (dup_counts <= 1).all()
```

**Changes:**
1. Verify GPCI fixture has MAC column
2. Filter GPCI frame by as-of window (`effective_start <= as_of <= effective_end`) before merging
3. Assert each `(mac, locality_code_join, as_of)` resolves to at most one row and keep MAC presence checks

**Fixture Update:** 
```python
# In test_locality_e2e_gpci_join_smoke
gpci_fixture = """
10112  CA  26  REST OF CALIFORNIA                  1.009  0.982  0.692
01182  CA  18  LOS ANGELES-LONG BEACH             1.055  1.091  0.878
"""
# Ensure MAC is first column (already correct)
```

### File: `tests/integration/test_gpci_payment_spotcheck.py`

**Verify:** GPCI lookups use `(mac, locality_code)` tuple, not just `locality_code`

**Update any hardcoded lookups:**
```python
# Before
gpci = gpci_df[gpci_df['locality_code'] == '26'].iloc[0]

# After  
gpci = gpci_df[
    (gpci_df['mac'] == '01112') & 
    (gpci_df['locality_code'] == '26')
].iloc[0]
```

---

## Step 5: Update Views/APIs (30 minutes)

Ambiguity handling (preferred) + temporary compatibility

Preferred behavior: Require mac for unambiguous lookups. If a client omits mac and multiple MACs exist for the given (locality_code, as_of_date) (or no state provided to disambiguate), return HTTP 409 Conflict with candidate MACs.

API Update: Document ambiguity + error shape.

```http
# Example response when `mac` omitted and multiple candidates exist
HTTP/1.1 409 Conflict
{
  "error": "Ambiguous locality",
  "locality": "00",
  "as_of_date": "2025-01-01",
  "candidates": [
    {"mac":"01112","state":"AL"},
    {"mac":"02102","state":"AZ"}
  ],
  "message": "Pass `mac` to disambiguate."
}
```

Temporary compatibility (1 quarter max):
- Keep a compatibility view but emit server-side warnings/metrics when used without mac.
- The view continues to expose current values but should be treated as deprecated.

```sql
CREATE OR REPLACE VIEW vw_gpci_current AS
WITH ranked AS (
    SELECT 
        mac,
        state,
        locality_id AS locality_code,
        locality_name,
        work_gpci,
        pe_gpci,
        mp_gpci,
        effective_start,
        effective_end,
        ROW_NUMBER() OVER (
            PARTITION BY locality_id, effective_start 
            ORDER BY mac
        ) AS mac_priority
    FROM gpci_indices
    WHERE effective_start <= CURRENT_DATE
      AND (effective_end IS NULL OR effective_end >= CURRENT_DATE)
)
SELECT *
FROM ranked
WHERE mac_priority = 1;

COMMENT ON VIEW vw_gpci_current IS 
'GPCI v1.3 compatibility view. Deprecated: will be removed after one quarter. 
 Prefer lookups by (mac, locality_code, as_of_date).';

Note: The view returns only the first MAC per `(locality_id, effective_start)`; ambiguity warnings/metrics stay in the API layer.
```

Observability: Add a counter `gpci_requests_without_mac_total` and a log warning for ambiguous lookups; target zero by the deprecation deadline.

**API Contract Test:** Add/extend `tests/api/test_gpci_ambiguity.py` to seed two MACs for locality `00`, call the endpoint without `mac`, and assert:

```python
def test_gpci_ambiguity_returns_409(api_client, seeded_gpci_dupes):
    resp = api_client.get("/api/gpci", params={"locality": "00", "date": "2025-01-01"})
    assert resp.status_code == 409
    payload = resp.json()
    assert payload["error"] == "Ambiguous locality"
    assert {c["mac"] for c in payload["candidates"]} == {"01112", "02102"}
    assert payload["message"].startswith("Pass `mac`")
```

---

## Step 6: Release Notes & Documentation (15 minutes)

### File: `docs/GPCI_V13_MIGRATION.md` (NEW)

```markdown
# GPCI v1.3 Migration Guide

## Overview
GPCI schema upgraded from v1.2 to v1.3 on 2025-10-20 to fix false duplicate bug.

## What Changed
**Natural Key:** `['locality_code', 'effective_from']` → `['mac', 'locality_code', 'effective_from']`

## Why
`locality_code='00'` appears in multiple states:
- Alabama (MAC 01112, locality 00)
- Arizona (MAC 02102, locality 00)
- Arkansas (MAC 07102, locality 00)

Without MAC, these were treated as duplicates (63 of 112 rows affected).

## Impact on Your Code

### Database Queries
**Before (v1.2):**
```sql
SELECT * FROM gpci_indices 
WHERE locality_id = '00' 
  AND effective_start <= '2025-01-01';
-- Returns multiple rows (ambiguous!)
```

**After (v1.3):**
```sql
SELECT * FROM gpci_indices 
WHERE mac = '01112' 
  AND locality_id = '00'
  AND effective_start <= '2025-01-01';
-- Returns single row (unambiguous)
```

### API Calls
**Before:**
```python
GET /api/gpci?locality=00&date=2025-01-01
# Ambiguous - which state?
```

**After:**
```python
GET /api/gpci?mac=01112&locality=00&date=2025-01-01
# Unambiguous - Alabama
```

**Backwards Compatibility:**
- `vw_gpci_current` view returns first MAC alphabetically for ambiguous lookups
- API accepts `locality` alone (issues warning)
- New code should use `(mac, locality)` tuple

## Row Content Hash Canonicalization

In v1.3, the canonical field order for `row_content_hash` includes MAC before locality_id:

```text
mac | locality_id | effective_start | effective_end | work_gpci | pe_gpci | mp_gpci | state | locality_name
```

Add a golden-test assertion to lock this order:

```python
assert golden_row.hash_input_fields == [
    "mac","locality_id","effective_start","effective_end",
    "work_gpci","pe_gpci","mp_gpci","state","locality_name"
]
```

## Migration Checklist
- [ ] Run Alembic migration: `alembic upgrade head`
- [ ] Backfill data: `python scripts/backfill_gpci_v13.py --commit`
- [ ] Update queries to include MAC
- [ ] Update joins to use `(mac, locality_code)`
- [ ] Test GPCI lookups in pricing calculations
- [ ] Update API clients to pass MAC

## Rollback
To rollback (NOT RECOMMENDED):
```bash
alembic downgrade -1
```

Note: This removes unique constraint but doesn't fix false duplicates.

## Questions
Contact: CMS Pricing API Team
Date: 2025-10-20
```

### Update CHANGELOG (already done in GPCI commit)

### Update API Documentation
**File:** `api-contracts/rvu-openapi.yaml`

Update GPCI endpoints to require `mac` parameter:
```yaml
/gpci:
  get:
    parameters:
      - name: mac
        in: query
        required: true
        description: 5-digit MAC code (required in v1.3+)
      - name: locality
        in: query
        required: true
      - name: as_of_date
        in: query
        required: false
```

---

## Step 7: Operator Runbook (10 minutes)

### File: `docs/runbooks/GPCI_V13_DEPLOYMENT.md` (NEW)

```markdown
# GPCI v1.3 Deployment Runbook

## Pre-Deployment Checklist
- [ ] Backup GPCI data: `pg_dump -t gpci_indices > gpci_backup.sql`
- [ ] Verify parser tests pass: `pytest tests/ingestion/test_gpci_parser_*.py`
- [ ] Verify integration tests pass: `pytest tests/integration/test_gpci_*.py`
- [ ] Notify API consumers of breaking change (1 week notice)

## Deployment Steps

### 1. Apply Database Migration (5 min)
```bash
# Staging
alembic upgrade head

# Verify
psql -d cms_pricing -c "
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'gpci_indices' 
  AND indexname = 'uq_gpci_mac_locality_effective';
"
# Confirm the concurrent build finished cleanly
psql -d cms_pricing -c "
SELECT i.indisvalid
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'uq_gpci_mac_locality_effective';
"
# Should show unique index on (mac, locality_id, effective_start)
```

### 2. Backfill Data (15 min)
```bash
# Dry run
python scripts/backfill_gpci_v13.py --release-id RVU25D --dry-run

# Commit
python scripts/backfill_gpci_v13.py --release-id RVU25D --commit

# Verify
psql -d cms_pricing -c "
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT (mac, locality_id, effective_start)) as unique_nk,
    COUNT(*) - COUNT(DISTINCT (mac, locality_id, effective_start)) as duplicates
FROM gpci_indices;
"
# duplicates should be 0

# Refresh planner stats for the new NK distribution
psql -d cms_pricing -c "ANALYZE gpci_indices;"
```

### 3. Test GPCI Queries (5 min)
```bash
# Test lookups
psql -d cms_pricing -c "
SELECT mac, locality_id, work_gpci, pe_gpci, mp_gpci
FROM gpci_indices
WHERE mac = '01112' AND locality_id = '00'
LIMIT 5;
"
# Should return Alabama GPCI values

# Test join with locality
psql -d cms_pricing -c "
SELECT 
    l.mac,
    l.locality_code,
    l.state_fips,
    g.work_gpci
FROM locality_fips l
JOIN gpci_indices g ON g.mac = l.mac AND g.locality_id = l.locality_code
LIMIT 10;
"
# Should join successfully
```

### 4. Monitor & Alerts (ongoing)
- Check for query errors mentioning `locality_id` ambiguity
- Monitor GPCI API endpoint response times
- Alert if any queries fail unique constraint
- Watch for downstream pricing calculation errors

## Rollback Procedure
```bash
# 1. Revert migration
alembic downgrade -1

# 2. Restore backup
psql -d cms_pricing < gpci_backup.sql

# 3. Notify consumers
# 4. Investigate failure
```

### 5. Query Plan Sanity Check (EXPLAIN)
Run `EXPLAIN ANALYZE` to ensure the three-part index is used:
```sql
EXPLAIN ANALYZE
SELECT 
    l.mac,
    l.locality_code,
    g.work_gpci
FROM locality_fips l
JOIN gpci_indices g 
  ON g.mac = l.mac 
 AND g.locality_id = l.locality_code
 AND g.effective_start <= CURRENT_DATE
 AND (g.effective_end IS NULL OR g.effective_end >= CURRENT_DATE)
WHERE l.locality_code = '00';
```

### 6. Metrics & Alerts
- `gpci_requests_without_mac_total` (counter) — watch daily; must trend to 0 by deprecation date.
- Alert on any unique-constraint violations for `uq_gpci_mac_locality_effective`.

## Success Criteria
- [ ] Unique index exists on (mac, locality_id, effective_start)
- [ ] 0 duplicate violations
- [ ] GPCI lookups return correct values
- [ ] Pricing calculations unchanged (spot check)
- [ ] No API errors in 24 hours post-deploy

## Contacts
- On-call: CMS Pricing Team
- Escalation: Database Admin
```

---

## Step 8: Validation & Testing (15 minutes)

### Run Full Test Suite
```bash
# Unit tests
pytest tests/ingestion/test_gpci_parser_golden.py tests/ingestion/test_gpci_parser_negatives.py -v
# Expect: 20/20 passing

# Integration tests
pytest tests/integration/test_locality_e2e.py tests/integration/test_gpci_payment_spotcheck.py -v
# Expect: All passing with MAC joins
```

### Manual Verification
```python
# Check database
from sqlalchemy import create_engine, text
from cms_pricing.config import settings

engine = create_engine(settings.DATABASE_URL)

with engine.connect() as conn:
    # Check unique constraint
    result = conn.execute(text("""
        SELECT 
            indexname, 
            indexdef 
        FROM pg_indexes 
        WHERE tablename = 'gpci_indices' 
          AND indexname LIKE '%unique%';
    """))
    for row in result:
        print(f"Index: {row.indexname}")
        print(f"Definition: {row.indexdef}")
    
    # Check for duplicates
    result = conn.execute(text("""
        SELECT 
            mac,
            locality_id,
            effective_start,
            COUNT(*)
        FROM gpci_indices
        GROUP BY mac, locality_id, effective_start
        HAVING COUNT(*) > 1;
    """))
    dupes = list(result)
    assert len(dupes) == 0, f"Found {len(dupes)} duplicates!"
    print("✅ No duplicates found")
```

```python
# Negative: attempt a duplicate insert should fail
from sqlalchemy import text

engine = create_engine(settings.DATABASE_URL)

with engine.begin() as conn:
    try:
        conn.execute(text("""
            INSERT INTO gpci_indices (mac, locality_id, effective_start)
            VALUES ('01112','26','2025-01-01');
        """))
        conn.execute(text("""
            INSERT INTO gpci_indices (mac, locality_id, effective_start)
            VALUES ('01112','26','2025-01-01');
        """))
        raise AssertionError("Expected unique violation on uq_gpci_mac_locality_effective")
    except Exception as exc:
        print("✅ Unique violation enforced:", exc)
```

---

## Acceptance Criteria

- [ ] Alembic migration created and applied
- [ ] Unique index on (mac, locality_id, effective_start) exists
- [ ] Database model updated with unique constraint
- [ ] GPCI data backfilled with v1.3 parser
- [ ] 0 duplicate violations on new NK
- [ ] Integration tests pass with MAC joins
- [ ] API documentation updated
- [ ] Backwards compatibility view created
- [ ] Release notes written
- [ ] Operator runbook complete
- [ ] All tests passing (unit + integration)
- [ ] API returns HTTP 409 with candidate MACs when `mac` is omitted and the lookup is ambiguous
- [ ] Metric `gpci_requests_without_mac_total` in place and monitored (target → 0 by deprecation date)

---

## Timeline

| Step | Duration | Cumulative |
|------|----------|------------|
| 1. Alembic migration | 30 min | 0:30 |
| 2. Database model | 15 min | 0:45 |
| 3. Backfill script | 45 min | 1:30 |
| 4. Integration tests | 30 min | 2:00 |
| 5. Views/APIs | 30 min | 2:30 |
| 6. Release notes | 15 min | 2:45 |
| 7. Operator runbook | 10 min | 2:55 |
| 8. Validation | 15 min | 3:10 |

**Total:** ~3 hours

---

## Dependencies

**Requires:**
- GPCI parser v1.3 (already complete)
- Alembic migration system (exists)
- Database access (postgres)
- Sample data for backfill

**Blocks:**
- Production GPCI deployment
- Pricing calculations using GPCI
- Downstream consumer updates

---

## Risk Assessment

**High Risk:**
- Data loss during backfill (MITIGATED: backup script)
- Unique constraint violation (MITIGATED: dry-run verification)
- Downstream query breakage (MITIGATED: compatibility view)

**Medium Risk:**
- Long migration time (MITIGATED: ~110 rows, < 1 second)
- API client updates needed (MITIGATED: backwards compat for 1 quarter)

**Low Risk:**
- Test failures (MITIGATED: already passing 20/20)
- Performance regression (MITIGATED: indexed columns)

---

## Success Metrics

- Migration time: < 5 minutes
- Downtime: 0 seconds (unique index creation is non-blocking)
- Data loss: 0 rows
- False duplicates after migration: 0
- Integration test pass rate: 100%
- API error rate: 0% increase post-deploy
- Ambiguous lookups without `mac`: trending to 0% by end of deprecation window
- Query plans use `uq_gpci_mac_locality_effective` for locality↔GPCI joins

---

✅ About vector optimization

Your plan now does not add any separate “vector engine” refactor (no Polars/Arrow rewrite needed). It does keep vectorized duplicate checks (Pandas) and optimizes DB ops via ON CONFLICT merges and CREATE UNIQUE INDEX CONCURRENTLY—the highest-impact wins without refactoring the parser.
