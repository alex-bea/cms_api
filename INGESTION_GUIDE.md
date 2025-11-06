# CMS Pricing API Ingestion System Guide

This guide explains how to use the comprehensive data ingestion system for the CMS Pricing API.

## 📚 **Architecture Documentation**

For detailed architecture and implementation guidance, see:

- **[STD-data-architecture-prd-v1.0.md](prds/STD-data-architecture-prd-v1.0.md)** - DIS pipeline architecture and requirements
- **[STD-data-architecture-impl-v1.0.md](prds/STD-data-architecture-impl-v1.0.md)** - Implementation guide with code templates and examples
- **[REF-scraper-ingestor-integration-v1.0.md](prds/REF-scraper-ingestor-integration-v1.0.md)** - Scraper→ingestor handoff patterns
- **[STD-scraper-prd-v1.0.md](prds/STD-scraper-prd-v1.0.md)** - Scraper patterns and discovery manifests

## 🏗️ **System Overview**

The ingestion system automatically fetches, normalizes, and validates CMS datasets with:
- **Individual Ingesters** for each dataset type (MPFS, OPPS, ASC, etc.)
- **Automated Scheduling** with task management
- **Data Validation** and quality checks
- **Audit Trails** with manifest generation
- **CLI Tools** for management and monitoring

The system follows the **DIS (Discovery → Ingestion → Serving) pipeline** architecture defined in the standards above.

## 🚀 **Quick Start**

### 1. Run Individual Ingesters

```python
from cms_pricing.ingestion.mpfs import MPFSIngester

# Ingest MPFS data for 2025
ingester = MPFSIngester("./data")
result = await ingester.ingest(2025, None)

print(f"✅ Ingested {result['dataset_id']} with digest {result['digest']}")
```

### 2. Use the CLI Tool

```bash
# List available datasets
python -m cms_pricing.cli ingestion --list

# Run a single ingestion
python -m cms_pricing.cli ingestion ingest --dataset MPFS --year 2025

# List ingestion tasks
python -m cms_pricing.cli ingestion list-tasks

# Show system status
python -m cms_pricing.cli status
```

### 3. Comprehensive Ingestion Script

```bash
# Ingest all datasets for 2025
python scripts/ingest_all.py --all --year 2025

# Ingest specific dataset
python scripts/ingest_all.py --dataset OPPS --year 2025 --quarter 1

# Schedule ingestion task
python scripts/ingest_all.py --schedule --dataset MPFS --year 2025
```

## 📋 **Available Datasets**

| Dataset | Type | Frequency | Ingester | Status |
|---------|------|-----------|----------|--------|
| MPFS | Annual | Yearly | MPFSIngester | ✅ Complete |
| OPPS | Quarterly | Q1-Q4 | OPPSIngester | ✅ Complete |
| ASC | Quarterly | Q1-Q4 | ASCIngester | 🔄 To Create |
| IPPS | Annual | FY | IPPSIngester | 🔄 To Create |
| CLFS | Quarterly | Q1-Q4 | CLFSEngine | 🔄 To Create |
| DMEPOS | Quarterly | Q1-Q4 | DMEPOSIngester | 🔄 To Create |
| ASP | Quarterly | Q1-Q4 | ASPIngester | 🔄 To Create |
| NADAC | Weekly | Weekly | NADACIngester | 🔄 To Create |

## 🔧 **Shared Services Architecture (ServiceFactory)**

The ingestion system uses a centralized `ServiceFactory` to provide shared services (validation, observability, quarantine, reference data, schema registry) to all DIS ingestors. This eliminates duplication and ensures consistent service configuration.

**Current Migration Status:**
- ✅ **RVU ingestor** uses ServiceFactory (reference implementation)
- ⏳ **MPFS, OPPS, ZIP9 ingestors** pending migration to ServiceFactory

### ServiceFactory Usage Pattern

All DIS ingestors should use `ServiceFactory` to access shared services:

```python
from cms_pricing.ingestion.services import ServiceFactory, ServiceConfig

class MyIngestor(BaseDISIngestor):
    def __init__(self, output_dir: str, db_session=None):
        super().__init__(output_dir, db_session)
        
        # Configure shared services
        service_config = ServiceConfig(
            output_dir=output_dir,
            dataset_name="my_dataset",
            enable_observability=True,
            enable_quarantine=True,
            enable_reference_data=True,
            enable_validation=True,
            enable_schema_registry=True,
            lazy_init=True,  # Services created on first access
            db_session=db_session
        )
        
        # Create service factory
        self.services = ServiceFactory(service_config)
        
        # Access services via factory (lazy initialization)
        # self.services.observability_collector
        # self.services.validation_service
        # self.services.quarantine_manager
        # self.services.reference_data_manager
        # self.services.schema_registry
```

### ServiceConfig Options

- `output_dir`: Base output directory for ingestion artifacts
- `dataset_name`: Name of the dataset (e.g., "cms_rvu", "cms_mpfs")
- `enable_observability`: Enable observability collection (default: True)
- `enable_quarantine`: Enable quarantine management (default: True)
- `enable_reference_data`: Enable reference data management (default: True)
- `enable_validation`: Enable validation engine (default: True)
- `enable_schema_registry`: Enable schema registry (default: True)
- `lazy_init`: Lazy-load services on first access (default: True)
- `db_session`: Optional database session for services that need DB access

### Lazy vs Eager Initialization

**Lazy Initialization (default):**
- Services are created only when first accessed
- Reduces startup time and memory usage
- Recommended for most use cases

```python
service_config = ServiceConfig(..., lazy_init=True)
factory = ServiceFactory(service_config)
# Services not created yet

collector = factory.observability_collector  # Created on first access
```

**Eager Initialization:**
- All enabled services created immediately
- Useful when you know all services will be needed

```python
service_config = ServiceConfig(..., lazy_init=False)
factory = ServiceFactory(service_config)
# All services created immediately
```

### Guardrails

1. **Consistent Factory Surface Area**: Services are accessed via stable property names (`self.services.validation_service`, `self.services.observability_collector`, etc.)

2. **Single-Point Schema Bootstrap**: Schema registration happens once via `schema_service.bootstrap_*_schemas()` to avoid duplicate registration

3. **Lazy Initialization**: Services are created only when accessed unless `lazy_init=False`

4. **NotImplementedError Guardrails**: Disabled services raise descriptive `NotImplementedError` with fix guidance:
   ```python
   config = ServiceConfig(enable_validation=False, ...)
   factory = ServiceFactory(config)
   try:
       _ = factory.validation_engine
   except NotImplementedError as e:
       # Error message: "Validation engine is disabled in ServiceConfig. Set enable_validation=True to use this service."
   ```

### Example: RVU Ingestor

See `cms_pricing/ingestion/ingestors/rvu_ingestor.py` for a complete example of ServiceFactory integration.

## 🏗️ **Creating New Ingesters**

### Step 1: Create the Ingester Class

```python
# cms_pricing/ingestion/asc.py
from cms_pricing.ingestion.base import BaseIngester
import pandas as pd
import httpx

class ASCIngester(BaseIngester):
    """Ingester for Ambulatory Surgical Center fee schedule"""
    
    def get_dataset_id(self) -> str:
        return "ASC"
    
    async def fetch_data(self, valuation_year: int, quarter: Optional[str] = None):
        """Fetch ASC data from CMS"""
        # Implementation here
        pass
    
    def normalize_data(self, raw_data: Dict[str, Any], valuation_year: int, quarter: Optional[str] = None):
        """Normalize ASC data into DataFrames"""
        # Implementation here
        pass
    
    def validate_data(self, normalized_data: Dict[str, pd.DataFrame]) -> List[str]:
        """Validate ASC data"""
        # Implementation here
        pass
```

### Step 2: Add to Scheduler

```python
# cms_pricing/ingestion/scheduler.py
from cms_pricing.ingestion.asc import ASCIngester

class IngestionScheduler:
    def __init__(self, data_dir: str = "./data"):
        self.ingesters = {
            "MPFS": MPFSIngester,
            "OPPS": OPPSIngester,
            "ASC": ASCIngester,  # Add new ingester
            # ... etc
        }
```

### Step 3: Test the Ingester

```bash
# Test the new ingester
python -m cms_pricing.cli ingestion ingest --dataset ASC --year 2025 --quarter 1
```

## 🤖 **Automated Ingestion**

### 1. Set Up Cron Jobs

```bash
# Run the setup script
./scripts/setup_cron.sh
```

This creates cron jobs for:
- **MPFS**: January 1st at 2 AM (annual)
- **OPPS**: 1st day of each quarter at 3 AM
- **ASC**: 1st day of each quarter at 4 AM
- **CLFS**: 1st day of each quarter at 5 AM
- **DMEPOS**: 1st day of each quarter at 6 AM
- **ASP**: 1st day of each quarter at 7 AM
- **NADAC**: Every Monday at 8 AM (weekly)
- **Full Check**: 15th of each month at 9 AM

### 2. Monitor Automated Ingestion

```bash
# Check cron jobs
crontab -l

# View logs
tail -f logs/ingestion.log

# Check task status
python -m cms_pricing.cli ingestion list-tasks

# Check snapshots
python -m cms_pricing.cli snapshots list-snapshots
```

## 📊 **Data Storage Structure**

```
data/
├── MPFS/
│   └── 2025-01-15_123456_MPFS/
│       ├── raw/
│       │   ├── rvu_data_2025.csv
│       │   ├── gpci_data_2025.csv
│       │   └── cf_data_2025.csv
│       ├── normalized/
│       │   ├── fee_mpfs.parquet
│       │   ├── gpci.parquet
│       │   └── conversion_factors.parquet
│       └── manifest.json
├── OPPS/
│   └── 2025-01-15_123457_OPPS/
│       ├── raw/
│       ├── normalized/
│       └── manifest.json
└── cache/
    └── MPFS/
        └── a1b2c3d4e5f6/
            └── slice_key.parquet
```

## 🔍 **Data Validation**

The system includes comprehensive validation:

### Automatic Checks
- ✅ Required columns present
- ✅ Data types correct
- ✅ HCPCS codes valid (5 characters)
- ✅ CBSA codes valid (5 digits)
- ✅ Status indicators valid
- ✅ Wage index values reasonable (0.5 - 2.0)
- ✅ No negative rates
- ✅ No null critical values

### Custom Validation
Override the `validate_data` method in your ingester:

```python
def validate_data(self, normalized_data: Dict[str, pd.DataFrame]) -> List[str]:
    warnings = []
    
    for table_name, df in normalized_data.items():
        # Add your custom validation logic
        if table_name == 'fee_asc':
            # Check for specific ASC requirements
            pass
    
    return warnings
```

## 📈 **Monitoring and Troubleshooting**

### 1. Check Ingestion Status

```bash
# System status
python -m cms_pricing.cli status

# Task details
python -m cms_pricing.cli ingestion list-tasks --status failed

# Snapshot details
python -m cms_pricing.cli snapshots show-snapshot <digest>
```

### 2. Common Issues

**Issue**: Ingestion fails with network error
```bash
# Check network connectivity
curl -I https://www.cms.gov/files/zip/mpfs-rvu-2025.zip

# Retry failed task
python -m cms_pricing.cli ingestion retry-task <task-id>
```

**Issue**: Data validation warnings
```bash
# Check warnings in logs
grep "WARNING" logs/ingestion.log

# Review specific snapshot
python -m cms_pricing.cli snapshots show-snapshot <digest>
```

**Issue**: Disk space
```bash
# Check data directory size
du -sh data/

# Clean up old data (keep last 13 months)
find data/ -type d -mtime +390 -exec rm -rf {} +
```

### 3. Performance Optimization

```bash
# Use disk caching
export DATA_CACHE_DIR="./data/cache"

# Increase cache size
export CACHE_MAX_BYTES=2147483648  # 2GB

# Parallel ingestion
python scripts/ingest_all.py --all --year 2025 &
```

## 🔄 **Production Deployment**

### 1. Environment Setup

```bash
# Production environment variables
export DATABASE_URL="postgresql://user:pass@prod-db:5432/cms_pricing"
export DATA_CACHE_DIR="/opt/cms-pricing/data/cache"
export LOG_LEVEL="INFO"
export S3_BUCKET="cms-pricing-prod-data"
```

### 2. Docker Deployment

```bash
# Build production image
docker build -t cms-pricing-api:latest .

# Run with ingestion
docker run -d \
  --name cms-pricing-api \
  -e DATABASE_URL="$DATABASE_URL" \
  -e DATA_CACHE_DIR="/app/data/cache" \
  -v /opt/cms-pricing/data:/app/data \
  cms-pricing-api:latest
```

### 3. Kubernetes Deployment

```yaml
# k8s/ingestion-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cms-ingestion
spec:
  schedule: "0 2 1 1,4,7,10 *"  # Quarterly
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: ingestion
            image: cms-pricing-api:latest
            command: ["python", "scripts/ingest_all.py", "--all", "--year", "2025"]
            env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: cms-pricing-secrets
                  key: database-url
```

## 📚 **Examples and Tutorials**

### Run Examples

```bash
# Run comprehensive examples
python examples/ingestion_example.py

# Test individual components
python -c "
import asyncio
from cms_pricing.ingestion.mpfs import MPFSIngester
ingester = MPFSIngester('./data')
result = asyncio.run(ingester.ingest(2025, None))
print(f'Digest: {result[\"digest\"]}')
"
```

### Integration Examples

```python
# Load ingested data into database
import pandas as pd
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.fee_schedules import FeeMPFS

def load_mpfs_data(parquet_file: str):
    df = pd.read_parquet(parquet_file)
    db = SessionLocal()
    
    try:
        for _, row in df.iterrows():
            mpfs_record = FeeMPFS(
                year=row['year'],
                locality_id=row['locality_id'],
                hcpcs=row['hcpcs'],
                work_rvu=row['work_rvu'],
                # ... etc
            )
            db.add(mpfs_record)
        
        db.commit()
        print(f"Loaded {len(df)} MPFS records")
    
    finally:
        db.close()

# Load data
load_mpfs_data("./data/MPFS/2025-01-15_123456_MPFS/normalized/fee_mpfs.parquet")
```

## 🎯 **Next Steps**

1. **Complete Ingesters**: Create remaining ingesters for ASC, IPPS, CLFS, DMEPOS, ASP, NADAC
2. **Real Data Sources**: Replace placeholder URLs with actual CMS endpoints
3. **Enhanced Validation**: Add business-specific validation rules
4. **Monitoring**: Set up alerts for ingestion failures
5. **Performance**: Optimize for large datasets and high frequency
6. **Integration**: Connect with your existing data pipeline

## 📞 **Support**

- **Documentation**: Check the main README.md
- **API Docs**: http://localhost:8000/docs (when running)
- **Logs**: Check `logs/ingestion.log` for detailed information
- **Issues**: Use the CLI tool to diagnose problems

The ingestion system is designed to be robust, scalable, and production-ready. It provides comprehensive tooling for managing CMS data ingestion with proper validation, monitoring, and audit trails.
