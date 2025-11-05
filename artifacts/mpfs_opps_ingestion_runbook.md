# OPPS Ingestion Runbook (Legacy Stub)

**Date:** 2025-01-15  
**Status:** Draft v0.2 (MPFS content relocated 2025-11-04)  
**Purpose:** Step-by-step guide for executing OPPS ingestion pipeline. MPFS runbook moved to `prds/RUN-mpfs-ingestion-v1.0.md`.

---

## Prerequisites

### 1. Environment Setup

**Verify Database Connection:**
```bash
# Check database is running
docker-compose ps db

# Test connection
python -c "from cms_pricing.database import SessionLocal; from sqlalchemy import text; db = SessionLocal(); result = db.execute(text('SELECT current_database()')); print('✅ Connected:', result.fetchone()); db.close()"
```

**Expected Output:**
```
✅ Connected: ('cms_pricing_db',)
```

**Verify Migrations:**
```bash
alembic current
```

**Expected Output:**
```
98567c0bbfa8 (head)
```

### 2. Test Data Preparation

**OPPS Test Data:**
```bash
# Create OPPS test data directory
mkdir -p test_data/ingestion_2025/opps/2025q1

# Download OPPS files manually from CMS (if needed)
# Store in: test_data/ingestion_2025/opps/2025q1/
```

### 3. Disk Space Check

```bash
# Check available disk space
df -h data/

# Ensure at least 1GB free space
```

---

# OPPS Ingestion Playbook

### Step 1: Confirm Current Vintages

**Target Release:** OPPS 2025Q1 (example)

**Check Source URLs:**
- Reference: `prds/REF-cms-pricing-source-map-prd-v1.0.md#opps`
- CMS OPPS Addenda bundle (A/B, D1) for target quarter
- Wage index file source (if separate)

**Snapshot Expected File Hashes:**
```bash
# Calculate expected hashes (if available)
sha256sum sample_data/opps25q1/AddendumA.txt
sha256sum sample_data/opps25q1/AddendumB.txt
```

**Document in staging note:**
```markdown
# OPPS 2025Q1 Ingestion Plan
- Release: OPPS25Q1
- Expected Files:
  - Addendum A/B (SHA256: ...)
  - Wage index CSV (SHA256: ...)
  - Packaging rules (if published)
```

---

### Step 2: Prepare Environment

**Validate DB Connectivity:**
```bash
# Already verified in Prerequisites
```

**Check Disk Space:**
```bash
# Already verified in Prerequisites
```

**Clear Prior Run Directories (Optional):**
```bash
mv data/ingestion/opps/raw data/ingestion/opps/raw_$(date +%Y%m%d_%H%M%S)_backup
mkdir -p data/ingestion/opps/{raw,stage,curated,quarantine}
```

**Verify OPPS Config:**
```python
python - <<'PY'
from cms_pricing.ingestion.services.dataset_snapshot_service import DatasetSnapshotService
service = DatasetSnapshotService()
opps_snapshot = service.get_latest_snapshot('opps_payment')
print('OPPS snapshot (if exists):', getattr(opps_snapshot, 'release_id', None))
service.close()
PY
```

---

### Step 3: Run OPPS Ingestion

Use the dedicated OPPS helper script (placeholder):
```bash
python scripts/run_opps_ingestion.py \
  --quarter 2025Q1 \
  --output-dir data/ingestion/opps \
  [--wage-index-path sample_data/opps25q1/WageIndex2025Q1.csv]
```

> **TODO:** Update once OPPS ingestor is fully refactored. Track progress in `artifacts/opps_implementation_plan.md`.

---

### Step 4: Verify Pipeline Summary

**Check Logs:**
```bash
tail -f logs/ingestion_opps_*.log
```

Confirm:
- Files discovered/downloaded count.
- Validation warnings and severity.
- Published tables (Addendum A/B, wage index joins).

---

### Step 5: Post-Run Validation

- Query curated parquet for expected HCPCS counts.
- Run `/v1/opps` API contract test to ensure `datasets_used` includes OPPS datasets.
- Document run evidence in operational ticket.

---

## Change Log

| Version | Date | Summary |
|---------|------|---------|
| v0.2 | 2025-11-04 | Removed MPFS content after split into dedicated runbook. |
| v0.1 | 2025-01-15 | Initial combined MPFS/OPPS draft. |
**Expected Output:**
```
Starting MPFS ingestion year=2025 quarter=D
Files discovered: 3
Files downloaded: 3
Validation passed: True
Validation warnings: 0
PPRRVU records: 10,000+
GPCI records: 100+
CF records: 2
Tables published: ['mpfs_rvu', 'mpfs_gpci', 'mpfs_cf']
Records published: 10,102
```

**Inspect Curated Outputs:**
```bash
# Check curated parquet files
ls -lh data/ingestion/mpfs/curated/mpfs_2025_D_*/mpfs_rvu.parquet
ls -lh data/ingestion/mpfs/curated/mpfs_2025_D_*/mpfs_gpci.parquet
ls -lh data/ingestion/mpfs/curated/mpfs_2025_D_*/mpfs_cf.parquet

# Verify parquet files are readable
python -c "
import pandas as pd
df = pd.read_parquet('data/ingestion/mpfs/curated/mpfs_2025_D_*/mpfs_rvu.parquet')
print(f'✅ Parquet readable: {len(df)} rows')
print(f'Columns: {list(df.columns)}')
"
```

**Verify Database Writes:**
```sql
-- Check fee_mpfs table
SELECT COUNT(*) as mpfs_count, 
       COUNT(DISTINCT release_id) as release_count,
       MIN(effective_from) as earliest_date,
       MAX(effective_to) as latest_date
FROM fee_mpfs
WHERE release_id LIKE 'mpfs_2025_D%';

-- Check gpci table
SELECT COUNT(*) as gpci_count,
       COUNT(DISTINCT locality_id) as locality_count
FROM gpci
WHERE release_id LIKE 'mpfs_2025_D%';

-- Check conversion_factors table
SELECT year, cf, source, release_id
FROM conversion_factors
WHERE release_id LIKE 'mpfs_2025_D%';
```

**Save Artifacts:**
```bash
# Save ingestion log snippet
tail -100 logs/ingestion_mpfs_*.log > artifacts/mpfs_ingestion_$(date +%Y%m%d_%H%M%S).log

# Save manifest path
echo "data/ingestion/mpfs/raw/mpfs_2025_D_*/manifest.json" > artifacts/mpfs_manifest_path.txt

# Save record/validation stats
python -c "
import json
stats = {
    'release_id': 'mpfs_2025_D_20250115_143022',
    'pprrvu_records': 10000,
    'gpci_records': 109,
    'cf_records': 2,
    'validation_warnings': 0,
    'validation_errors': 0
}
with open('artifacts/mpfs_ingestion_stats.json', 'w') as f:
    json.dump(stats, f, indent=2)
"
```

---

### Step 5: Capture Provenance Evidence

**Query Dataset Snapshots:**
```sql
-- Check dataset_snapshots table
SELECT dataset_id, release_id, digest, effective_from, effective_to, created_at
FROM dataset_snapshots
WHERE dataset_id = 'MPFS'
ORDER BY created_at DESC
LIMIT 5;
```

**Query Ingestion Runs:**
```sql
-- Check ingestion_runs table (if exists)
SELECT batch_id, release_id, status, started_at, completed_at
FROM ingestion_runs
WHERE release_id LIKE 'mpfs_2025_D%'
ORDER BY started_at DESC
LIMIT 5;
```

**Take Screenshots or Export:**
```bash
# Export dataset_snapshots to CSV
psql $DATABASE_URL -c "
COPY (
    SELECT dataset_id, release_id, digest, effective_from, effective_to, created_at
    FROM dataset_snapshots
    WHERE dataset_id = 'MPFS'
    ORDER BY created_at DESC
) TO STDOUT WITH CSV HEADER
" > artifacts/mpfs_dataset_snapshots.csv

# Export fee_mpfs provenance sample
psql $DATABASE_URL -c "
SELECT hcpcs, release_id, batch_id, effective_from, effective_to
FROM fee_mpfs
WHERE release_id LIKE 'mpfs_2025_D%'
LIMIT 10
" > artifacts/mpfs_provenance_sample.txt
```

**Attach Evidence to Readiness Checklist:**
- Update `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md` with run metrics
- Include run timestamps, batch IDs, record counts
- Add links to manifest files and curated outputs

---

## OPPS Ingestion Runbook

### Step 1: Confirm Current Vintages

**Target Release:** OPPS Q4 2025 (October 2025)

**Check Source URLs:**
- Reference: `prds/REF-cms-pricing-source-map-prd-v1.0.md#direct-artifact-links-2024-2026`
- OPPS Addenda: `https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates`
- Expected files: `july-2025-opps-addendum.zip` or similar

**Handle License Redirect:**
- OPPS downloads require AMA license acceptance
- Manual download recommended for initial runs
- Store in `test_data/ingestion_2025/opps/2025q4/`

**Snapshot Expected File Hashes:**
```bash
# Calculate expected hashes (if available)
sha256sum test_data/ingestion_2025/opps/2025q4/addendum_a.csv
sha256sum test_data/ingestion_2025/opps/2025q4/addendum_b.csv
```

---

### Step 2: Prepare Environment

**Same as MPFS (Step 2)**

**Additional: OPPS License Handling**
```bash
# If using local test data (recommended)
ls -la test_data/ingestion_2025/opps/2025q4/

# Should see:
# - addendum_a.csv (or .xlsx)
# - addendum_b.csv (or .xlsx)
# - Or addendum.zip containing both
```

---

### Step 3: Run OPPS Ingestion

**Option A: Using CLI (if available)**
```bash
python -m cms_pricing.cli.opps_cli ingest \
    --batch-id opps_2025q4_r01 \
    --year 2025 \
    --quarter 4 \
    --output-dir data/ingestion/opps
```

**Option B: Using Python Script**
```python
#!/usr/bin/env python3
"""Run OPPS ingestion"""
import asyncio
from cms_pricing.ingestion.ingestors.opps_ingestor import OPPSIngestor
from pathlib import Path

async def main():
    ingestor = OPPSIngestor(
        output_dir=Path("./data/ingestion/opps"),
        database_url="postgresql://cms_user:cms_password@localhost:5432/cms_pricing",
        cpt_masking_enabled=True
    )
    
    batch_id = "opps_2025q4_r01"
    result = await ingestor.ingest_batch(batch_id)
    
    print("✅ OPPS ingestion completed")
    print(f"Status: {result.get('status')}")
    print(f"Batch ID: {result.get('batch_id')}")
    print(f"Tables published: {result.get('tables_published', [])}")
    print(f"Records published: {result.get('records_published', 0)}")

if __name__ == "__main__":
    asyncio.run(main())
```

**Save as:** `scripts/run_opps_ingestion.py`

**Execute:**
```bash
python scripts/run_opps_ingestion.py
```

---

### Step 4: Verify Pipeline Summary

**Check Logs:**
```bash
# Check ingestion logs
tail -f logs/ingestion_opps_*.log

# Or check console output for:
# - Files downloaded count
# - Validation warnings
# - Record totals
# - Wage index enrichment status
```

**Expected Output:**
```
Starting OPPS batch ingestion batch_id=opps_2025q4_r01
Files discovered: 2
Files downloaded: 2
Validation passed: True
Validation warnings: 0
Addendum A records: 1,000+
Addendum B records: 10,000+
Wage index enriched: True
Tables published: ['opps_apc_payment', 'opps_hcpcs_crosswalk', 'opps_rates_enriched']
Records published: 11,000+
```

**Inspect Curated Outputs:**
```bash
# Check curated parquet files
ls -lh data/ingestion/opps/curated/opps_2025q4_r01/*.parquet

# Verify parquet files are readable
python -c "
import pandas as pd
df = pd.read_parquet('data/ingestion/opps/curated/opps_2025q4_r01/opps_apc_payment.parquet')
print(f'✅ Parquet readable: {len(df)} rows')
"
```

**Spot-Check Addendum A/B Data:**
```sql
-- Check APC payment rates
SELECT apc, relative_weight, national_unadj_rate, effective_from
FROM fee_opps
WHERE release_id LIKE 'opps_2025q4%'
LIMIT 10;

-- Check HCPCS crosswalk
SELECT hcpcs, status_indicator, apc, effective_from
FROM fee_opps
WHERE release_id LIKE 'opps_2025q4%'
  AND hcpcs IS NOT NULL
LIMIT 10;
```

**Verify Wage Index Enrichment:**
```sql
-- Check wage index data (if separate table)
SELECT cbsa, wage_index, year, effective_from
FROM wage_index
WHERE year = 2025
LIMIT 10;
```

**Save Artifacts:**
```bash
# Save ingestion log snippet
tail -100 logs/ingestion_opps_*.log > artifacts/opps_ingestion_$(date +%Y%m%d_%H%M%S).log

# Save record/validation stats
python -c "
import json
stats = {
    'batch_id': 'opps_2025q4_r01',
    'addendum_a_records': 1000,
    'addendum_b_records': 10000,
    'wage_index_enriched': True,
    'validation_warnings': 0,
    'validation_errors': 0
}
with open('artifacts/opps_ingestion_stats.json', 'w') as f:
    json.dump(stats, f, indent=2)
"
```

---

### Step 5: Capture Provenance Evidence

**Same as MPFS (Step 5), but for OPPS:**

```sql
-- Check dataset_snapshots table
SELECT dataset_id, release_id, digest, effective_from, effective_to, created_at
FROM dataset_snapshots
WHERE dataset_id = 'OPPS'
ORDER BY created_at DESC
LIMIT 5;
```

**Export Provenance:**
```bash
# Export dataset_snapshots to CSV
psql $DATABASE_URL -c "
COPY (
    SELECT dataset_id, release_id, digest, effective_from, effective_to, created_at
    FROM dataset_snapshots
    WHERE dataset_id = 'OPPS'
    ORDER BY created_at DESC
) TO STDOUT WITH CSV HEADER
" > artifacts/opps_dataset_snapshots.csv
```

---

## Post-Ingestion Verification

### Step 1: API Endpoint Validation

**Test MPFS Endpoint:**
```bash
# Start API server (if not running)
python -m cms_pricing.main

# Test endpoint
curl "http://localhost:8000/v1/mpfs?code=99213&year=2025&locality=CA01" | jq

# Verify response includes:
# - Pricing data
# - datasets_used metadata
# - Provenance fields (release_id, batch_id, dataset_digest)
```

**Test OPPS Endpoint:**
```bash
# Test endpoint
curl "http://localhost:8000/v1/opps?apc=5071&year=2025" | jq

# Verify response includes:
# - Pricing data
# - datasets_used metadata
# - Provenance fields
```

### Step 2: Update Documentation

**Update Readiness Plan:**
```bash
# Edit prds/DOC-cms-pricing-api-readiness-plan-v1.0.md
# Add section:
# ## MPFS Ingestion Run Results
# - Release ID: mpfs_2025_D_20250115_143022
# - Records loaded: 10,102
# - Provenance: ✅ Complete
# - API Status: ✅ Functional
```

**Update Tomorrow Plan:**
```bash
# Edit artifacts/tomorrow_plan.md
# Add note:
# - ✅ MPFS ingestion completed for 2025D
# - ✅ OPPS ingestion completed for 2025Q4
# - Next: Verify API endpoints, run regression tests
```

### Step 3: Sync with Backlog

**Once product confirms scope:**
- Create backlog tickets for ASC → NADAC ingestors
- Reference `prds/REF-cms-pricing-source-map-prd-v1.0.md#direct-artifact-links-2024-2026`
- Link source-map appendix as reference

---

## Troubleshooting

### Common Issues

**1. Database Connection Failed**
```bash
# Check docker-compose
docker-compose ps db

# Restart database
docker-compose restart db

# Verify connection
python -c "from cms_pricing.database import SessionLocal; db = SessionLocal(); db.close()"
```

**2. Validation Errors**
```bash
# Check validation logs
grep -i "validation" logs/ingestion_*.log

# Review quarantined records
ls -la data/ingestion/*/quarantine/*/
```

**3. Parsing Errors**
```bash
# Check file format
file data/ingestion/*/raw/*/files/*.txt

# Verify file encoding
head -5 data/ingestion/*/raw/*/files/*.txt
```

**4. Missing PDF Layout**
```bash
# Verify PDF exists
ls -la sample_data/rvu25d_0/RVU25D.pdf

# Extract layout information (if needed)
# Reference: cms_pricing/ingestion/parsers/layout_registry.py
```

---

## Success Criteria Checklist

- [ ] MPFS ingestion runs end-to-end
- [ ] OPPS ingestion runs end-to-end
- [ ] Data appears in curated tables (`fee_mpfs`, `fee_opps`)
- [ ] Provenance metadata recorded (`release_id`, `batch_id`, `dataset_digest`)
- [ ] API endpoints return data (`/v1/mpfs`, `/v1/opps`)
- [ ] Provenance metadata visible in API responses
- [ ] Documentation updated with run metrics
- [ ] Evidence captured for readiness checklist

---

## References

- **Architecture Plan:** `artifacts/mpfs_opps_architecture_plan.md`
- **MPFS Implementation Plan:** `artifacts/mpfs_implementation_plan.md`
- **OPPS Implementation Plan:** `artifacts/opps_implementation_plan.md`
- **Source Map:** `prds/REF-cms-pricing-source-map-prd-v1.0.md`
- **Readiness Plan:** `prds/DOC-cms-pricing-api-readiness-plan-v1.0.md`
