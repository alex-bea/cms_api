# Testing Render Deployment Data

Quick guide to test if your Render deployment has real data uploaded.

## Quick Start

### Option 1: Python Script (Recommended)

```bash
# In Render shell, run:
python3 check_render_data.py
```

This will:
- ✅ Test database connection
- ✅ Show record counts for all tables
- ✅ Display recent releases
- ✅ Show ingestion run statistics
- ✅ Display sample data
- ✅ Check data freshness

### Option 2: Bash Script

```bash
# In Render shell, run:
bash test_render_deployment.sh
```

## Manual Commands

If you prefer to run commands manually, here are the key ones:

### 1. Quick Database Check

```bash
python3 -c "
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    # Check RVU items
    rvu_count = db.execute(text('SELECT COUNT(*) FROM rvu_items')).fetchone()[0]
    print(f'RVU Items: {rvu_count:,}')
    
    # Check GPCI indices
    gpci_count = db.execute(text('SELECT COUNT(*) FROM gpci_indices')).fetchone()[0]
    print(f'GPCI Indices: {gpci_count:,}')
    
    # Check OPPS caps
    opps_count = db.execute(text('SELECT COUNT(*) FROM opps_caps')).fetchone()[0]
    print(f'OPPS Caps: {opps_count:,}')
    
    # Check releases
    release_count = db.execute(text('SELECT COUNT(*) FROM releases')).fetchone()[0]
    print(f'Releases: {release_count:,}')
finally:
    db.close()
"
```

### 2. Check Recent Releases

```bash
python3 -c "
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    result = db.execute(text('''
        SELECT release_id, created_at 
        FROM releases 
        ORDER BY created_at DESC 
        LIMIT 5
    '''))
    
    for row in result.fetchall():
        print(f'{row[0]} | {row[1]}')
finally:
    db.close()
"
```

### 3. Check Ingestion Runs

```bash
python3 -c "
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    result = db.execute(text('''
        SELECT release_id, status, output_record_count, created_at
        FROM ingest_runs 
        ORDER BY created_at DESC 
        LIMIT 5
    '''))
    
    for row in result.fetchall():
        print(f'{row[0]} | {row[1]} | {row[2]:,} records | {row[3]}')
finally:
    db.close()
"
```

### 4. Check Sample Data

```bash
python3 -c "
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    # Sample RVU items
    result = db.execute(text('SELECT hcpcs_code, modifier, work_rvu FROM rvu_items LIMIT 5'))
    print('RVU Items Sample:')
    for row in result.fetchall():
        print(f'  {row[0]} | {row[1]} | Work RVU: {row[2]}')
    
    # Sample GPCI indices
    result = db.execute(text('SELECT mac, locality_id, work_gpci FROM gpci_indices LIMIT 5'))
    print('\nGPCI Indices Sample:')
    for row in result.fetchall():
        print(f'  MAC: {row[0]} | Locality: {row[1]} | Work GPCI: {row[2]}')
finally:
    db.close()
"
```

### 5. Direct PostgreSQL Query (if you have psql access)

```bash
# Connect to database
psql $DATABASE_URL

# Then run:
SELECT COUNT(*) FROM rvu_items;
SELECT COUNT(*) FROM gpci_indices;
SELECT COUNT(*) FROM opps_caps;
SELECT COUNT(*) FROM releases;
SELECT COUNT(*) FROM ingest_runs;

-- Check recent releases
SELECT release_id, created_at FROM releases ORDER BY created_at DESC LIMIT 5;

-- Check recent ingestion runs
SELECT release_id, status, output_record_count, created_at 
FROM ingest_runs 
ORDER BY created_at DESC 
LIMIT 5;
```

## What to Look For

### ✅ Healthy Deployment Signs:
- **Record counts > 0** in main tables (rvu_items, gpci_indices, opps_caps)
- **Recent releases** (within last 7 days)
- **Successful ingestion runs** with output_record_count > 0
- **Sample data** shows valid HCPCS codes, localities, etc.

### ❌ Issues to Watch For:
- **Empty tables** (all counts = 0)
- **Failed ingestion runs** (status = 'failed')
- **Old data** (no releases in last 7 days)
- **Database connection errors**

## Testing API Endpoints

If your API service is running, you can also test the endpoints:

```bash
# Health check
curl $RENDER_SERVICE_URL/health

# RVU endpoint (requires API key)
curl -H "X-API-Key: YOUR_API_KEY" $RENDER_SERVICE_URL/api/v1/rvu?page_size=5

# Releases endpoint
curl $RENDER_SERVICE_URL/api/v1/rvu/releases
```

## Troubleshooting

### No Data Found?
1. Check if ingestion has been run: `SELECT * FROM ingest_runs ORDER BY created_at DESC LIMIT 1;`
2. Check for errors in ingestion runs: `SELECT * FROM ingest_runs WHERE status = 'failed';`
3. Verify database connection: `python3 -c "from cms_pricing.database import engine; print(engine.url)"`

### Database Connection Issues?
1. Verify `DATABASE_URL` environment variable is set
2. Check Render dashboard for database connection status
3. Test connection: `python3 -c "from cms_pricing.database import engine; engine.connect()"`

## Next Steps

If data is missing:
1. Trigger an ingestion run (via API or CLI)
2. Check ingestion logs for errors
3. Verify source data files are accessible
4. Review ingestion run status and quality scores

