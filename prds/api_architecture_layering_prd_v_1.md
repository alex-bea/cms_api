# API Architecture & Layering PRD (v1.0)

## 0. Overview
This document defines the **API Architecture & Layering Standard** for the CMS Pricing API. It specifies layer responsibilities, dependency flow, error handling, testing strategy, and implementation patterns. All API development must conform to this standard.

**Status:** Adopted v1.0  
**Owners:** Platform Engineering (API Guild)  
**Consumers:** Product, Engineering, Data, Ops  
**Change control:** ADR + Architecture Board review  

## 1. Goals & Non-Goals

**Goals**
- Establish clear layer boundaries with single responsibility per layer
- Enable contract-first development with OpenAPI as SSOT
- Ensure testable, maintainable, and scalable API architecture
- Provide consistent error handling and observability across all layers
- Support external customer APIs with proper security and rate limiting

**Non-Goals**
- Business logic specific to pricing calculations (covered in domain engines)
- Real-time streaming APIs (covered by separate streaming standard)
- Internal-only APIs without external customer requirements

## 2. Layer Responsibilities

### 2.1 Transport Layer (Routers)
**Location:** `cms_pricing/routers/`

**Responsibilities:**
- HTTP request/response handling
- Request validation using Pydantic schemas
- Authentication and authorization enforcement
- Rate limiting and security middleware
- Error shaping and HTTP status code mapping
- Response formatting and serialization
- Correlation ID management

**What Goes Here:**
- FastAPI route handlers (`@router.get`, `@router.post`)
- Pydantic request/response models
- HTTP-specific validation logic
- API key verification
- Rate limiting configuration
- CORS and security headers

**What Doesn't:**
- Business logic or calculations
- Direct database access
- Domain-specific pricing rules
- Complex data transformations

**Example:**
```python
@router.get("/codes/price")
async def price_single_code(
    request: Request,
    zip: str,
    code: str,
    setting: str,
    year: int,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    # Input validation
    if not zip.isdigit() or len(zip) != 5:
        raise HTTPException(status_code=400, detail="ZIP must be exactly 5 digits")
    
    # Delegate to service layer
    pricing_service = PricingService(db)
    result = await pricing_service.price_single_code(zip, code, setting, year)
    return result
```

### 2.2 Service Layer
**Location:** `cms_pricing/services/`

**Responsibilities:**
- Business orchestration and workflow coordination
- Multi-engine coordination (MPFS, OPPS, ASC, etc.)
- Transaction management and database session handling
- Business rule enforcement and validation
- Cross-cutting concerns (logging, tracing, metrics)
- Error handling and exception mapping

**What Goes Here:**
- Service classes that coordinate multiple engines
- Business workflow orchestration
- Transaction boundaries
- Cross-service communication
- Business validation rules
- Audit logging and tracing

**What Doesn't:**
- HTTP-specific concerns
- Direct database queries (use repositories)
- Domain calculations (use engines)
- Request/response formatting

**Example:**
```python
class PricingService:
    def __init__(self, db: Session):
        self.geography_service = GeographyService(db)
        self.engines = {
            'MPFS': MPSFEngine(),
            'OPPS': OPPSEngine(),
            # ...
        }
    
    async def price_single_code(self, zip: str, code: str, setting: str, year: int):
        # Orchestrate geography resolution
        geography_result = await self.geography_service.resolve_zip(zip)
        
        # Get appropriate engine
        engine = self.engines.get(setting)
        if not engine:
            raise ValueError(f"Unknown setting: {setting}")
        
        # Delegate to domain engine
        return await engine.price_code(code, zip, year, geography=geography_result)
```

### 2.3 Domain Layer (Engines)
**Location:** `cms_pricing/engines/`

**Responsibilities:**
- Core business logic and calculations
- Domain-specific pricing rules
- Algorithm implementation
- Business invariants and constraints
- Pure business logic (side-effect free)

**What Goes Here:**
- Pricing calculation algorithms
- Business rule implementations
- Domain-specific validations
- Mathematical computations
- Business logic that's independent of infrastructure

**What Doesn't:**
- HTTP concerns
- Database access (use repositories)
- External API calls
- Infrastructure dependencies
- Request/response formatting

**Example:**
```python
class MPSFEngine(BasePricingEngine):
    async def price_code(self, code: str, zip: str, year: int, geography: GeographyResult):
        # Pure business logic - no HTTP, no DB imports
        # Calculate pricing based on domain rules
        base_rate = self._calculate_base_rate(code, year)
        locality_multiplier = self._get_locality_multiplier(geography.locality)
        return base_rate * locality_multiplier
```

### 2.4 Data Access Layer
**Location:** `cms_pricing/models/`, `cms_pricing/repositories/` (future)

**Responsibilities:**
- Database operations and queries
- Data mapping and transformations
- Repository pattern implementation
- Query optimization
- Database session management

**What Goes Here:**
- SQLAlchemy models
- Repository interfaces and implementations
- Database queries and operations
- Data mapping between DB and domain
- Connection pooling and session management

**What Doesn't:**
- Business logic
- HTTP concerns
- Domain calculations
- Service orchestration

## 3. Dependency Flow & Boundaries

### 3.1 Allowed Dependencies
```
Routers → Services → Engines → Repositories → Models
```

**Routers can depend on:**
- Services (via dependency injection)
- Pydantic schemas
- Auth middleware
- Rate limiting

**Services can depend on:**
- Engines (domain logic)
- Repositories (data access)
- Other services
- Configuration

**Engines can depend on:**
- Domain models (not ORM models)
- Other engines
- Pure Python libraries

**Repositories can depend on:**
- SQLAlchemy models
- Database sessions
- Query builders

### 3.2 Forbidden Dependencies
- **Routers cannot directly import engines** (must go through services)
- **Routers cannot directly import models** (must go through services)
- **Engines cannot import HTTP libraries** (FastAPI, requests, etc.)
- **Engines cannot import database libraries** (SQLAlchemy, etc.)
- **Services cannot import HTTP libraries** (except for external API calls)

### 3.3 Dependency Injection Pattern
```python
# FastAPI dependency injection
@router.get("/price")
async def price_endpoint(
    db: Session = Depends(get_db),
    pricing_service: PricingService = Depends(get_pricing_service),
    api_key: str = Depends(verify_api_key)
):
    return await pricing_service.price_code(...)

# Service factory
def get_pricing_service(db: Session = Depends(get_db)) -> PricingService:
    return PricingService(db)
```

## 4. Error Handling Strategy

### 4.1 Error Hierarchy
```
DomainException (base)
├── ValidationError
├── BusinessRuleError
├── NotFoundError
└── ExternalServiceError
```

### 4.2 Error Mapping
**Routers map domain exceptions to HTTP status codes:**
- `ValidationError` → 400 Bad Request
- `BusinessRuleError` → 422 Unprocessable Entity
- `NotFoundError` → 404 Not Found
- `ExternalServiceError` → 502 Bad Gateway
- `AuthenticationError` → 401 Unauthorized
- `AuthorizationError` → 403 Forbidden
- `RateLimitError` → 429 Too Many Requests
- `InternalError` → 500 Internal Server Error

### 4.3 Error Response Format
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "ZIP must be exactly 5 digits",
    "details": [
      {
        "field": "zip",
        "issue": "Invalid format"
      }
    ]
  },
  "trace": {
    "correlation_id": "uuid",
    "vintage": "2025-01",
    "hash": "sha256:..."
  }
}
```

### 4.4 Centralized Error Handling
```python
# Global exception handler
@app.exception_handler(DomainException)
async def domain_exception_handler(request: Request, exc: DomainException):
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "error": {
                "code": exc.error_code,
                "message": exc.message,
                "details": exc.details
            },
            "trace": {
                "correlation_id": request.state.correlation_id,
                "vintage": get_current_vintage(),
                "hash": get_current_hash()
            }
        }
    )
```

## 5. Async/Sync Patterns

### 5.1 Async Standardization
**Decision:** Standardize on async/await throughout the stack

**Rationale:**
- Your codebase already uses async/await consistently
- Better performance for I/O-bound operations
- Easier to add concurrency later
- Consistent with FastAPI patterns

### 5.2 Implementation Pattern
```python
# Routers - async
@router.get("/price")
async def price_endpoint(...):
    result = await pricing_service.price_code(...)
    return result

# Services - async
class PricingService:
    async def price_code(self, ...):
        geography = await self.geography_service.resolve_zip(...)
        result = await self.engine.price_code(...)
        return result

# Engines - async
class MPSFEngine:
    async def price_code(self, ...):
        # Pure business logic
        return calculation_result

# Repositories - sync (SQLAlchemy)
class PricingRepository:
    def get_rate(self, code: str, year: int):
        return self.db.query(FeeMPFS).filter(...).first()
```

## 6. Transaction Management

### 6.1 Transaction Boundaries
**Decision:** Per-request transactions in service layer

**Pattern:**
```python
class PricingService:
    async def price_plan(self, request: PricingRequest):
        # Start transaction
        async with self.db.begin():
            # Multiple operations within transaction
            geography = await self.geography_service.resolve_zip(...)
            pricing_result = await self.engine.price_code(...)
            
            # Transaction commits automatically on success
            return pricing_result
```

### 6.2 Read vs Write Segregation
- **Read operations:** Use read-only database sessions
- **Write operations:** Use read-write database sessions
- **Cross-service calls:** Use distributed transactions when needed

## 7. Caching Strategy

### 7.1 Multi-Tier Caching
**Current Implementation:** You have `CacheManager` with disk cache

**Recommended Tiers:**
1. **In-memory LRU:** Hot data (GPCI lookups, geography resolution)
2. **Redis:** Shared cache across instances
3. **Database:** Persistent storage

### 7.2 Cache Patterns
```python
class GeographyService:
    async def resolve_zip(self, zip: str):
        # Check cache first
        cache_key = f"geo:{zip}"
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Compute and cache
        result = await self._compute_geography(zip)
        await self.cache.set(cache_key, result, ttl=3600)
        return result
```

### 7.3 Cache Provenance
Include cache hit/miss information in trace blocks:
```json
{
  "trace": {
    "correlation_id": "uuid",
    "cache_hits": ["geo:90210", "gpc:12345"],
    "cache_misses": ["geo:12345"],
    "vintage": "2025-01",
    "hash": "sha256:..."
  }
}
```

## 8. Configuration Management

### 8.1 Typed Settings
**Current Implementation:** You use Pydantic BaseSettings

**Pattern:**
```python
class Settings(BaseSettings):
    app_name: str = "CMS Pricing API"
    database_url: str
    redis_url: Optional[str] = None
    rate_limit_per_minute: int = 1000
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
```

### 8.2 Environment Overlays
- **Development:** `.env.dev`
- **Staging:** `.env.staging`
- **Production:** Environment variables only

### 8.3 Secrets Management
- **Local:** `.env` files (gitignored)
- **CI/CD:** GitHub Secrets
- **Production:** Vault or cloud secret manager

## 9. Performance & SLOs

### 9.1 Latency Budgets
**Default SLOs:**
- **Read operations:** p95 ≤ 200ms
- **Write operations:** p95 ≤ 400ms
- **Complex calculations:** p95 ≤ 1000ms

### 9.2 Performance Monitoring
**Current Implementation:** You have Prometheus metrics

**Metrics:**
```python
REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration',
    ['method', 'endpoint']
)

PRICING_CALCULATIONS = Histogram(
    'pricing_calculation_duration_seconds',
    'Pricing calculation duration',
    ['engine', 'code_type']
)
```

### 9.3 Caching Strategy
- **Hot data:** In-memory LRU (GPCI, geography)
- **Warm data:** Redis (pricing rates, reference data)
- **Cold data:** Database only

## 10. Security Patterns

### 10.1 API Key Management
**Current Implementation:** Simple API key validation

**Pattern:**
```python
def verify_api_key(api_key: str = Header(..., alias="X-API-Key")):
    # Validate key format: cms_<env>_<tenant>_<random>
    if not api_key.startswith("cms_"):
        raise HTTPException(status_code=401, detail="Invalid API key format")
    
    # Check database for active key
    key_record = get_api_key(api_key)
    if not key_record or not key_record.is_active:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key")
    
    return api_key
```

### 10.2 Tenant Enforcement
**Decision Needed:** Where to enforce tenant filtering

**Options:**
1. **Service layer:** Filter data by tenant in services
2. **Repository layer:** Filter data by tenant in repositories
3. **Database layer:** Row-level security (RLS)

**Recommendation:** Repository layer for data filtering, service layer for business rules

### 10.3 Rate Limiting
**Current Implementation:** You use SlowAPI

**Pattern:**
```python
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@router.get("/price")
@limiter.limit("100/minute")
async def price_endpoint(request: Request, ...):
    # Rate limited endpoint
```

## 11. Testing Strategy

### 11.1 Testing Pyramid
```
Unit Tests (Engines) ≫ Integration Tests (Services) ≫ Contract Tests (API) ≫ E2E Tests
```

### 11.2 Unit Tests (Engines)
- **Focus:** Pure business logic
- **No dependencies:** Mock all external dependencies
- **Coverage:** ≥90% line coverage
- **Speed:** <100ms per test

```python
def test_mpfs_engine_calculation():
    engine = MPSFEngine()
    result = await engine.price_code("12345", "90210", 2025)
    assert result.base_rate == 150.00
    assert result.locality_multiplier == 1.2
```

### 11.3 Integration Tests (Services)
- **Focus:** Service orchestration
- **Dependencies:** Real database, mocked external services
- **Coverage:** Critical paths only
- **Speed:** <1s per test

```python
async def test_pricing_service_integration():
    service = PricingService(test_db)
    result = await service.price_single_code("90210", "12345", "MPFS", 2025)
    assert result.geography.locality == "Los Angeles"
```

### 11.4 Contract Tests (API)
- **Focus:** OpenAPI compliance
- **Tool:** Schemathesis
- **Coverage:** All public endpoints
- **Speed:** <5s per test

```python
def test_api_contracts():
    # Auto-generated from OpenAPI spec
    schema = load_openapi_spec()
    test_endpoints(schema)
```

## 12. Folder Structure & Module Rules

### 12.1 Current Structure
```
cms_pricing/
├── routers/          # Transport layer
├── services/         # Service layer
├── engines/          # Domain layer
├── models/           # Data access layer
├── schemas/          # Pydantic DTOs
├── config.py         # Configuration
├── database.py       # Database setup
├── auth.py           # Authentication
├── middleware/       # Cross-cutting concerns
└── tests/            # Test suites
```

### 12.2 Module Import Rules
**Enforced by linting:**
- Routers cannot import engines directly
- Routers cannot import models directly
- Engines cannot import HTTP libraries
- Engines cannot import database libraries

**Lint Configuration:**
```python
# .flake8 or ruff.toml
forbidden-imports = [
    "cms_pricing.routers.*:cms_pricing.engines",
    "cms_pricing.routers.*:cms_pricing.models",
    "cms_pricing.engines.*:fastapi",
    "cms_pricing.engines.*:sqlalchemy",
]
```

## 13. Observability & Monitoring

### 13.1 Correlation IDs
**Current Implementation:** You have correlation ID middleware

**Pattern:**
```python
@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-Id", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    
    response = await call_next(request)
    response.headers["X-Correlation-Id"] = correlation_id
    return response
```

### 13.2 Structured Logging
**Current Implementation:** You use structlog

**Pattern:**
```python
logger = structlog.get_logger()

logger.info(
    "Pricing calculation completed",
    correlation_id=request.state.correlation_id,
    zip=zip,
    code=code,
    duration_ms=duration_ms,
    result_count=len(results)
)
```

### 13.3 Metrics Collection
**Current Implementation:** You have Prometheus metrics

**Metrics:**
- Request rate, duration, error rate
- Business metrics (pricing calculations, geography lookups)
- Infrastructure metrics (database connections, cache hit rates)

## 14. Migration & Compatibility

### 14.1 Backward Compatibility
**Decision Needed:** Migration strategy for breaking changes

**Recommendation:**
1. **Additive changes:** Always backward compatible
2. **Breaking changes:** MAJOR version bump with 90-day deprecation
3. **Dual-read/write:** Support both old and new formats during transition

### 14.2 Schema Evolution
**Database migrations:**
- **Additive first:** Add new columns, don't remove old ones
- **Dual-write:** Write to both old and new formats
- **Cleanup:** Remove old format after migration complete

### 14.3 API Versioning
**Strategy:** URL versioning (`/api/v1/`, `/api/v2/`)
- **Minor changes:** Same version
- **Major changes:** New version with deprecation timeline

## 15. Release Discipline

### 15.1 ADR Requirements
**Decision Needed:** When to require ADRs

**Recommendation:**
- **New services:** ADR required
- **Cross-layer changes:** ADR required
- **Breaking changes:** ADR required
- **Performance changes:** ADR required

### 15.2 Semver Enforcement
**Decision Needed:** Semver policy for API changes

**Recommendation:**
- **OpenAPI changes:** Semver bump required
- **Breaking changes:** MAJOR version bump
- **New endpoints:** MINOR version bump
- **Bug fixes:** PATCH version bump

## 16. Acceptance Criteria

### 16.1 Implementation Checklist
- [ ] All routers follow thin router pattern (no business logic)
- [ ] All services orchestrate engines and repositories
- [ ] All engines are pure business logic (no HTTP/DB imports)
- [ ] All data access goes through repositories
- [ ] All errors follow standardized error hierarchy
- [ ] All endpoints include correlation IDs and trace blocks
- [ ] All async/await patterns are consistent
- [ ] All dependencies follow allowed dependency flow
- [ ] All tests follow testing pyramid
- [ ] All performance SLOs are met

### 16.2 Compliance Gates
- **Lint checks:** Enforce import rules
- **Unit tests:** ≥90% coverage on engines
- **Integration tests:** Critical paths covered
- **Contract tests:** All endpoints compliant
- **Performance tests:** SLOs met
- **Security tests:** Auth and rate limiting work

## 17. Change Management

### 17.1 PRD Updates
- Changes require ADR for architectural decisions
- Breaking changes require migration plan
- Performance changes require SLO impact analysis

### 17.2 Implementation Timeline
- **Phase 1:** Refactor existing mixed-layer code
- **Phase 2:** Implement standardized error handling
- **Phase 3:** Add comprehensive testing
- **Phase 4:** Implement observability and monitoring

---

## Appendix A — Decision Matrix

| Decision | Current State | Recommendation | Rationale |
|----------|---------------|----------------|-----------|
| Async/Sync | Async throughout | ✅ Keep async | Already implemented, better performance |
| Transaction Boundaries | Per-request | ✅ Keep per-request | Simple, works well with FastAPI |
| DTO Mapping | Pydantic schemas | ❓ Need decision | Explicit adapters vs inline mapping |
| Repository Pattern | SQLAlchemy models | ❓ Need decision | Repository interfaces vs direct models |
| Caching | Multi-tier | ✅ Keep multi-tier | Good performance, already implemented |
| Idempotency | None | ❓ Need decision | Required for external APIs |
| Tenant Enforcement | API keys only | ❓ Need decision | Repository vs service layer |
| Error Handling | HTTPException | ✅ Standardize | Good pattern, needs hierarchy |
| Background Work | None | ❓ Need decision | Task queue vs synchronous |
| Schema Migration | Alembic | ❓ Need decision | Zero-downtime vs simple migrations |
| Release Discipline | None | ❓ Need decision | ADR requirements and semver policy |

## Appendix B — Implementation Examples

### B.1 Thin Router Example
```python
@router.get("/codes/price")
async def price_single_code(
    request: Request,
    zip: str,
    code: str,
    setting: str,
    year: int,
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    # Input validation only
    if not zip.isdigit() or len(zip) != 5:
        raise HTTPException(status_code=400, detail="ZIP must be exactly 5 digits")
    
    # Delegate to service
    pricing_service = PricingService(db)
    result = await pricing_service.price_single_code(zip, code, setting, year)
    return result
```

### B.2 Service Orchestration Example
```python
class PricingService:
    async def price_single_code(self, zip: str, code: str, setting: str, year: int):
        # Orchestrate multiple engines
        geography = await self.geography_service.resolve_zip(zip)
        engine = self.engines.get(setting)
        if not engine:
            raise ValueError(f"Unknown setting: {setting}")
        
        result = await engine.price_code(code, zip, year, geography=geography)
        return result
```

### B.3 Pure Domain Engine Example
```python
class MPSFEngine(BasePricingEngine):
    async def price_code(self, code: str, zip: str, year: int, geography: GeographyResult):
        # Pure business logic - no HTTP, no DB imports
        base_rate = self._calculate_base_rate(code, year)
        locality_multiplier = self._get_locality_multiplier(geography.locality)
        return {
            "base_rate": base_rate,
            "locality_multiplier": locality_multiplier,
            "total_rate": base_rate * locality_multiplier
        }
```

---

**End of API Architecture & Layering PRD v1.0**
