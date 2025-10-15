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

### Architecture Documentation

The system architecture is comprehensively documented in PRDs following governance standards:

**Core Standards:**
- [STD-data-architecture-prd-v1.0.md](prds/STD-data-architecture-prd-v1.0.md) - DIS pipeline architecture and requirements
- [STD-data-architecture-impl-v1.0.md](prds/STD-data-architecture-impl-v1.0.md) - Implementation guide (companion)
- [STD-scraper-prd-v1.0.md](prds/STD-scraper-prd-v1.0.md) - Scraper patterns and discovery
- [STD-api-architecture-prd-v1.0.md](prds/STD-api-architecture-prd-v1.0.md) - API design standards
- [STD-qa-testing-prd-v1.0.md](prds/STD-qa-testing-prd-v1.0.md) - Testing standards
- [STD-observability-monitoring-prd-v1.0.md](prds/STD-observability-monitoring-prd-v1.0.md) - Observability framework

**Reference Architectures:**
- [REF-scraper-ingestor-integration-v1.0.md](prds/REF-scraper-ingestor-integration-v1.0.md) - Scraper→ingestor handoff
- [REF-cms-pricing-source-map-prd-v1.0.md](prds/REF-cms-pricing-source-map-prd-v1.0.md) - CMS data sources
- [REF-geography-source-map-prd-v1.0.md](prds/REF-geography-source-map-prd-v1.0.md) - Geography data sources

**Master Catalog:**
- [DOC-master-catalog-prd-v1.0.md](prds/DOC-master-catalog-prd-v1.0.md) - Complete documentation index

For governance and naming conventions, see [STD-doc-governance-prd-v1.0.md](prds/STD-doc-governance-prd-v1.0.md).

### Core Components

- **FastAPI Application**: REST API with automatic OpenAPI documentation
- **SQLAlchemy Models**: Database models for all CMS data types
- **Pricing Engines**: Specialized engines for each payment system
- **Geography Service**: ZIP code resolution with ambiguity handling
- **Cache Manager**: LRU in-memory + disk caching with digest verification
- **Trace Service**: Comprehensive audit trail and run tracking
- **Data Ingestion**: Automated fetching and normalization of CMS datasets (DIS pipeline)

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

We organize the suite by domain to match the QA Testing Standard. Each directory has a matching pytest marker so you can target specific layers:

```bash
# Documentation catalog audits
pytest tests/prd_docs -m prd_docs

# Scraper unit/component + performance suites
pytest tests/scrapers -m scraper

# DIS ingestion pipelines (requires Postgres)
pytest tests/ingestors -m ingestor

# API contract, health, and parity checks
pytest tests/api -m api

# Geography & nearest-zip resolver suites
pytest tests/geography -m geography
```

Combine markers as needed (e.g., `pytest -m "scraper or prd_docs"`) or run the full suite with `pytest`.

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

- **[Data Architecture PRD v1.0](prds/STD-data-architecture-prd-v1.0.md)**: Comprehensive data architecture including ingestion lifecycle, storage patterns, data modeling, database design, quality gates, versioning, security, and observability. Defines the Data Ingestion Standard (DIS) for all data pipelines.

- **[API Standards & Architecture PRD v1.0](prds/STD-api-architecture-prd-v1.0.md)**: Unified API design and architecture standards covering contracts, versioning, request/response envelopes, pagination, errors, correlation, layer responsibilities, dependency flow, and release discipline.

### Security & Quality PRDs

- **[API Security & Auth PRD v1.0](prds/STD-api-security-and-auth-prd-v1.0.md)**: Comprehensive security standards including authentication, authorization, API key management, RBAC, PII/PHI handling, rate limiting, security middleware, and operational runbooks.

- **[QA Testing Standard (QTS) v1.0](prds/STD-qa-testing-prd-v1.0.md)**: Comprehensive testing standards including test tiers, quality gates, test environments, naming conventions, versioning, observability, reporting, and test accuracy metrics.

### Performance & Operations PRDs

- **[Observability & Monitoring PRD v1.0](prds/STD-observability-monitoring-prd-v1.0.md)**: Unified monitoring standards with five-pillar framework (Freshness, Volume, Schema, Quality, Lineage), SLAs for data pipelines and API services, metrics, alerting, and incident response.

- **[API Performance & Scalability PRD v1.0](prds/STD-api-performance-scalability-prd-v1.0.md)**: Performance budgets, caching strategies, scaling patterns, load testing, performance monitoring, and optimization guidelines.

- **[API Contract Management PRD v1.0](prds/STD-api-contract-management-prd-v1.0.md)**: Schema evolution, versioning, compatibility rules, contract governance, OpenAPI SSOT, and automated contract testing.

### Blueprints & Implementation Packs

- **[Geography Mapping Implementation Pack](prds/REF-geography-mapping-cursor-prd-v1.0.md)**: Comprehensive geography service implementation including ingestion, resolver, and operations with ZIP+4-first mandate.

- **[Nearest ZIP Resolver Implementation Pack](prds/REF-nearest-zip-resolver-prd-v1.0.md)**: State-constrained nearest ZIP lookup algorithm with Haversine distance calculation and PO Box filtering.

### Product & Dataset PRDs

- **[MPFS PRD v1.0](prds/PRD-mpfs-prd-v1.0.md)**: Medicare Physician Fee Schedule data ingestion and processing requirements.

- **[OPPS PRD v1.0](prds/PRD-opps-prd-v1.0.md)**: Hospital Outpatient Prospective Payment System data requirements and processing standards.

- **[NCCI MUE PRD v1.0](prds/PRD-ncci-mue-prd-v1.0.md)**: National Correct Coding Initiative Medically Unlikely Edits requirements.

- **[CMS Treatment Plan API PRD v0.1](prds/PRD-cms-treatment-plan-api-prd-v0.1.md)**: Treatment plan pricing comparison API specifications and requirements.

- **[Geography Locality Mapping PRD v1.0](prds/PRD-geography-locality-mapping-prd-v1.0.md)**: ZIP+4-first geography locality mapping with business requirements and validation rules.

- **[RVU GPCI PRD v0.1](prds/PRD-rvu-gpci-prd-v0.1.md)**: Resource-Based Relative Value Units and Geographic Practice Cost Index data requirements.

### Operational Runbooks

- **[Global Operations Runbook](prds/RUN-global-operations-prd-v1.0.md)**: Operational procedures, troubleshooting guides, and maintenance protocols.

### Specialized PRDs

- **[Scraper Standard PRD v1.0](prds/STD-scraper-prd-v1.0.md)**: Web scraping standards for automated data discovery, disclaimer handling, orchestration, and compliance with data ingestion requirements.

- **[OPPS Scraper PRD](prds/PRD-opps-scraper-prd-v1.0.md)**: Specific requirements for CMS OPPS data scraping including quarterly addenda discovery, disclaimer acceptance, and file classification.

### PRD Compliance

All components in this project are designed to comply with these PRDs:
- **Data pipelines** follow the STD-data-architecture-prd-v1.0 (DIS)
- **API endpoints** comply with STD-api-architecture-prd-v1.0
- **Security** implements STD-api-security-and-auth-prd-v1.0 requirements
- **Testing** follows STD-qa-testing-prd-v1.0 guidelines
- **Monitoring** adheres to STD-observability-monitoring-prd-v1.0
- **Performance** meets STD-api-performance-scalability-prd-v1.0 standards
- **Contracts** are managed per STD-api-contract-management-prd-v1.0
- **Scrapers** follow STD-scraper-prd-v1.0 requirements

For implementation details, see the individual PRD files in the `prds/` directory.
