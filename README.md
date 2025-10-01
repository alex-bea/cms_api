# CMS Treatment Plan Pricing & Comparison API

A Python-based API that produces ZIP-level, episode-based treatment plan prices using Medicare Fee-For-Service as the baseline, with support for facility-specific negotiated prices and commercial/Medicaid comparators.

## Features

- **Complete Treatment Plan Pricing**: Price bundles of CPT/HCPCS/DRG codes across all major settings
- **Multi-Setting Support**: MPFS, OPPS, ASC, IPPS, CLFS, DMEPOS, Part B drugs (ASP), NADAC reference
- **Geographic Resolution**: ZIP→locality/CBSA mapping with ambiguity handling
- **Facility-Specific Pricing**: CCN-based overrides with MRF support
- **Beneficiary Cost Sharing**: Medicare allowed amounts + deductibles/coinsurance
- **Audit Trail**: Complete traceability with dataset versions, formulas, and assumptions
- **Location Comparisons**: A vs B comparisons with strict parity enforcement
- **High Performance**: LRU caching, structured logging, Prometheus metrics

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Redis (optional, for caching)
- Docker & Docker Compose (recommended)

### Using Docker Compose

1. **Clone and start services**:
   ```bash
   git clone <repository>
   cd cms-pricing-api
   docker-compose up -d
   ```

2. **Run database migrations**:
   ```bash
   docker-compose exec api alembic upgrade head
   ```

3. **Verify the API**:
   ```bash
   curl -H "X-API-Key: dev-key-123" http://localhost:8000/healthz
   ```

### Manual Setup

1. **Install dependencies**:
   ```bash
   pip install poetry
   poetry install
   ```

2. **Set up environment**:
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

3. **Start PostgreSQL and Redis**:
   ```bash
   # Using Docker
   docker run -d --name postgres -e POSTGRES_DB=cms_pricing -e POSTGRES_USER=cms_user -e POSTGRES_PASSWORD=cms_password -p 5432:5432 postgres:15-alpine
   docker run -d --name redis -p 6379:6379 redis:7-alpine
   ```

4. **Run migrations**:
   ```bash
   alembic upgrade head
   ```

5. **Start the API**:
   ```bash
   uvicorn cms_pricing.main:app --host 0.0.0.0 --port 8000 --reload
   ```

## API Usage

### Authentication

All endpoints require an API key in the `X-API-Key` header:

```bash
curl -H "X-API-Key: dev-key-123" http://localhost:8000/plans
```

### Core Endpoints

#### 1. Create a Treatment Plan

```bash
curl -X POST -H "X-API-Key: dev-key-123" -H "Content-Type: application/json" \
  http://localhost:8000/plans \
  -d '{
    "name": "Outpatient TKA",
    "description": "Total knee arthroscopy outpatient procedure",
    "components": [
      {
        "code": "27447",
        "setting": "OPPS",
        "units": 1,
        "professional_component": true,
        "facility_component": true
      },
      {
        "code": "99213",
        "setting": "MPFS",
        "units": 1,
        "professional_component": true,
        "facility_component": false
      }
    ]
  }'
```

#### 2. Price a Plan

```bash
curl -X POST -H "X-API-Key: dev-key-123" -H "Content-Type: application/json" \
  http://localhost:8000/pricing/price \
  -d '{
    "zip": "94110",
    "plan_id": "your-plan-id",
    "year": 2025,
    "quarter": "1"
  }'
```

#### 3. Compare Locations

```bash
curl -X POST -H "X-API-Key: dev-key-123" -H "Content-Type: application/json" \
  http://localhost:8000/pricing/compare \
  -d '{
    "zip_a": "94110",
    "zip_b": "73301",
    "plan_id": "your-plan-id",
    "year": 2025,
    "quarter": "1"
  }'
```

#### 4. Resolve Geography

```bash
curl -H "X-API-Key: dev-key-123" \
  "http://localhost:8000/geography/resolve?zip=94110"
```

#### 5. Get Trace Information

```bash
curl -H "X-API-Key: dev-key-123" \
  "http://localhost:8000/trace/your-run-id"
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `API_KEYS` | Comma-separated API keys | `dev-key-123` |
| `RATE_LIMIT_PER_MINUTE` | API rate limit | `120` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `CACHE_TTL_SECONDS` | Cache TTL | `3600` |
| `MAX_CONCURRENT_REQUESTS` | Max concurrent requests | `25` |

### Data Sources

The API supports ingestion from multiple CMS data sources:

- **MPFS**: Medicare Physician Fee Schedule (annual)
- **OPPS**: Outpatient Prospective Payment System (quarterly)
- **ASC**: Ambulatory Surgical Center fee schedule (quarterly)
- **IPPS**: Inpatient Prospective Payment System (annual)
- **CLFS**: Clinical Laboratory Fee Schedule (quarterly)
- **DMEPOS**: Durable Medical Equipment fee schedule (quarterly)
- **ASP**: Average Sales Price for Part B drugs (quarterly)
- **NADAC**: National Average Drug Acquisition Cost (weekly/monthly)

## Architecture

### Core Components

- **FastAPI Application**: REST API with automatic OpenAPI documentation
- **SQLAlchemy Models**: Database models for all CMS data types
- **Pricing Engines**: Specialized engines for each payment system
- **Geography Service**: ZIP code resolution with ambiguity handling
- **Cache Manager**: LRU in-memory + disk caching with digest verification
- **Trace Service**: Comprehensive audit trail and run tracking
- **Data Ingestion**: Automated fetching and normalization of CMS datasets

### Database Schema

The API uses PostgreSQL with the following main tables:

- `geography`: ZIP→locality/CBSA mappings
- `codes`: HCPCS/CPT codes with metadata
- `fee_*`: Fee schedules for each payment system
- `drugs_*`: Drug pricing data (ASP, NADAC)
- `plans`: Treatment plan definitions
- `snapshots`: Dataset versioning and digests
- `runs`: Pricing run tracking and audit trail

### Pricing Logic

1. **Geography Resolution**: ZIP → locality/CBSA with ambiguity handling
2. **Dataset Selection**: Choose appropriate fee schedule based on year/quarter
3. **Rate Calculation**: Apply RVUs, GPCI, conversion factors, wage indices
4. **Modifier Application**: Handle -26, -TC, -50, -51, etc.
5. **Cost Sharing**: Calculate deductibles and coinsurance
6. **Facility Overrides**: Apply MRF rates when available
7. **Trace Generation**: Record all decisions and data sources

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run golden tests for parity validation
pytest tests/test_golden.py -v

# Run with coverage
pytest --cov=cms_pricing
```

### Code Quality

```bash
# Format code
black cms_pricing/
isort cms_pricing/

# Lint code
flake8 cms_pricing/
mypy cms_pricing/
```

### Database Migrations

```bash
# Create new migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1
```

## Deployment

### Docker

The API is containerized with multi-stage builds:

```bash
# Build image
docker build -t cms-pricing-api .

# Run container
docker run -p 8000:8000 \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  -e API_KEYS=your-api-key \
  cms-pricing-api
```

### Cloud Deployment

The API is cloud-ready and can be deployed to:

- **AWS**: ECS, EKS, Lambda
- **GCP**: Cloud Run, GKE
- **Azure**: Container Apps, AKS

### Monitoring

The API exposes Prometheus metrics at `/metrics`:

- HTTP request metrics
- Pricing engine performance
- Cache hit/miss ratios
- Dataset selection tracking

Health checks are available at:
- `/healthz`: Basic health check
- `/readyz`: Readiness check with dependencies

## Compliance

### CPT® Licensing

This API stores and processes HCPCS codes only. CPT® codes are not distributed in the repository or API responses. If users provide CPT® codes in their requests, they are treated as user-supplied data and not redistributed.

### Data Privacy

- No PHI/PII is stored or processed
- Only public reference data and user-entered plan metadata
- All data is anonymized and aggregated

## Roadmap

### MVP (Current)
- ✅ Core pricing engines (MPFS, OPPS, ASC, IPPS, CLFS, DMEPOS, ASP)
- ✅ ZIP resolution and geography handling
- ✅ Plan management and pricing
- ✅ Location comparisons with parity enforcement
- ✅ Comprehensive trace and audit system

### v1 (Next)
- 🔄 MRF facility pricing with caching
- 🔄 NCCI edit enforcement
- 🔄 Enhanced IPPS add-ons (IME/DSH/outlier)
- 🔄 Anesthesia base+time calculations

### v2 (Future)
- 🔄 Payer TiC MRF parsing
- 🔄 State Medicaid fee schedules
- 🔄 Advanced scenario modeling
- 🔄 Cohort-weighted pricing mixes

## Support

For questions, issues, or contributions:

1. Check the [API documentation](http://localhost:8000/docs) when running locally
2. Review the [test fixtures](tests/fixtures/) for examples
3. Examine the [golden tests](tests/test_golden.py) for expected behavior
4. Submit issues or pull requests to the repository

## License

This project is licensed under the MIT License. See LICENSE file for details.

---

**Note**: This API is designed for healthcare pricing analysis and should not be used for actual claims processing without proper validation and compliance review.
## PRD Catalog

This project follows comprehensive Product Requirements Documents (PRDs) that define standards, architecture, and best practices across all components:

### Core Architecture PRDs

- **[Data Architecture PRD v1.0](prds/data_architecture_prd_v_1.md)**: Comprehensive data architecture including ingestion lifecycle, storage patterns, data modeling, database design, quality gates, versioning, security, and observability. Defines the Data Ingestion Standard (DIS) for all data pipelines.

- **[API Standards & Architecture PRD v1.0](prds/api_standards_architecture_prd_v_1.md)**: Unified API design and architecture standards covering contracts, versioning, request/response envelopes, pagination, errors, correlation, layer responsibilities, dependency flow, and release discipline.

### Security & Quality PRDs

- **[API Security & Auth PRD v1.0](prds/prd-api-security-and-auth.md)**: Comprehensive security standards including authentication, authorization, API key management, RBAC, PII/PHI handling, rate limiting, security middleware, and operational runbooks.

- **[QA Testing Standard (QTS) v1.0](prds/qa_testing_standard_prd_v_1.md)**: Comprehensive testing standards including test tiers, quality gates, test environments, naming conventions, versioning, observability, reporting, and test accuracy metrics.

### Performance & Operations PRDs

- **[Observability & Monitoring PRD v1.0](prds/observability_monitoring_prd_v_1.md)**: Unified monitoring standards with five-pillar framework (Freshness, Volume, Schema, Quality, Lineage), SLAs for data pipelines and API services, metrics, alerting, and incident response.

- **[API Performance & Scalability PRD v1.0](prds/api_performance_scalability_prd_v_1.md)**: Performance budgets, caching strategies, scaling patterns, load testing, performance monitoring, and optimization guidelines.

- **[API Contract Management PRD v1.0](prds/api_contract_management_prd_v_1.md)**: Schema evolution, versioning, compatibility rules, contract governance, OpenAPI SSOT, and automated contract testing.

### Specialized PRDs

- **[Scraper Standard PRD v1.0](prds/scraper_standard_prd_v_1.md)**: Web scraping standards for automated data discovery, disclaimer handling, orchestration, and compliance with data ingestion requirements.

- **[OPPS Scraper PRD](prds/prd-opps-scraper.md)**: Specific requirements for CMS OPPS data scraping including quarterly addenda discovery, disclaimer acceptance, and file classification.

### PRD Compliance

All components in this project are designed to comply with these PRDs:
- **Data pipelines** follow the Data Architecture PRD (DIS)
- **API endpoints** comply with API Standards & Architecture PRD
- **Security** implements API Security & Auth PRD requirements
- **Testing** follows QA Testing Standard (QTS) guidelines
- **Monitoring** adheres to Observability & Monitoring PRD
- **Performance** meets API Performance & Scalability PRD standards
- **Contracts** are managed per API Contract Management PRD
- **Scrapers** follow Scraper Standard PRD requirements

For implementation details, see the individual PRD files in the `prds/` directory.
