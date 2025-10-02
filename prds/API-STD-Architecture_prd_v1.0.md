# API Standards & Architecture PRD (v1.0)

## 0. Overview
This document defines the **API Standards & Architecture** for the CMS Pricing API. It consolidates API design standards, layer architecture, contracts, and implementation patterns into a single authoritative source. All API development must conform to this standard.

**Status:** Adopted v1.0  
**Owners:** Platform Engineering (API Guild)  
**Consumers:** Product, Engineering, Data, Ops  
**Change control:** ADR + Architecture Board review  

**Cross-References:**
- **STD-observability-monitoring_prd_v1.0:** API service monitoring and observability
- **STD-api-security-and-auth_prd_v1.0:** Authentication, authorization, and security requirements
- **STD-qa-testing_prd_v1.0:** API testing requirements and quality gates
- **STD-Data-Architecture_prd_v1.0:** Data pipeline integration and observability

## 1. Goals & Non-Goals

**Goals**
- Establish clear layer boundaries with single responsibility per layer
- Enable contract-first development with OpenAPI as SSOT
- Ensure testable, maintainable, and scalable API architecture
- Provide consistent error handling and observability across all layers
- Support external customer APIs with proper security and rate limiting
- Define comprehensive API standards for contracts, versioning, and lifecycle

**Non-Goals**
- Business logic specific to pricing calculations (covered in domain engines)
- Real-time streaming APIs (covered by separate streaming standard)
- Internal-only APIs without external customer requirements
- Security implementation details (covered in Security PRD)

## 2. API Standards & Contracts

### 2.1 OpenAPI SSOT & SemVer Enforcement

**Single Source of Truth:** `/api-contracts/openapi.yaml` must be updated first. Code, tests, SDKs, docs, and changelog entries follow that PR.

**SemVer Rules:**
- MINOR/PATCH for backward-compatible changes
- MAJOR for breaking changes with ≥90 day deprecation window
- Deprecations advertise via `X-Deprecation` header and docs banner

**CI Gates:**
- Spectral lint, contract-test suite, and router/spec drift check block merges on failure
- Contract tests auto-generated from SSOT; drift blocks merge

**Release Workflow:**
- Git tag (`vMAJOR.MINOR.PATCH`) equals spec version
- Tag builds SDKs (TypeScript, Python) and Postman collection as release assets
- Publishes rendered docs to `/docs/<version>` (with `/docs` tracking main)

### 2.2 Request/Response Standards

**Request Format:**
- JSON; UTF-8; snake_case fields
- Idempotency via `Idempotency-Key` on mutating endpoints
- Content-Type: `application/json`

**Response Format:**
- JSON object with top-level `data`, `meta`, `trace` where applicable
- Standard error envelope:

```json
{
  "error": {
    "code": "string",
    "message": "human-readable",
    "details": [{"field":"path","issue":"desc"}]
  },
  "trace": {"correlation_id":"uuid"}
}
```

**Data-Backed Responses:**
- Include `trace.vintage` and `trace.hash` for reproducibility
- `trace.vintage` maps to snapshot selected by API
- `trace.hash` is dataset bundle digest proving exact data slice

### 2.3 Pagination, Filtering, Sorting

**Pagination:**
- Cursor-based pagination: `page_size` (≤100), `next_cursor`
- Consistent across all list endpoints

**Filtering:**
- Whitelisted query params only
- Server rejects unknown filters with 400
- Document all supported filters in OpenAPI

**Sorting:**
- Format: `sort=field:asc|desc`
- Support multiple fields: `sort=name:asc,date:desc`

### 2.4 Error Handling & Retries

**Standard HTTP Codes:**
- 400: Bad Request (validation errors)
- 401: Unauthorized (missing/invalid API key)
- 403: Forbidden (insufficient permissions)
- 404: Not Found (resource not found)
- 409: Conflict (idempotency key conflict)
- 422: Unprocessable Entity (business rule violations)
- 429: Too Many Requests (rate limit exceeded)
- 5xx: Server errors

**Retry Behavior:**
- Honor `Retry-After` header for 429/503
- Client idempotent retries allowed where specified
- Exponential backoff recommended

### 2.5 Correlation & Observability

**Correlation IDs:**
- Require `X-Correlation-Id` (generated if absent)
- Echo in all responses and logs
- Use UUIDv4 format

**Observability Integration:**
- Structured logging with correlation IDs
- Metrics collection for all endpoints
- Trace context propagation
- Performance monitoring

## 3. Layer Architecture

### 3.1 Transport Layer (Routers)
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

### 3.2 Service Layer
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
        self.db = db
        self.mpfs_engine = MPFSEngine(db)
        self.opps_engine = OPPSEngine(db)
    
    async def price_single_code(self, zip: str, code: str, setting: str, year: int):
        # Business orchestration
        if setting == "office":
            return await self.mpfs_engine.calculate_price(zip, code, year)
        elif setting == "outpatient":
            return await self.opps_engine.calculate_price(zip, code, year)
        else:
            raise ValueError(f"Unknown setting: {setting}")
```

### 3.3 Domain Layer (Engines)
**Location:** `cms_pricing/engines/`

**Responsibilities:**
- Core business logic and calculations
- Domain-specific algorithms and rules
- Data validation and business constraints
- Domain model management
- Pricing calculations and formulas

**What Goes Here:**
- Pricing calculation engines (MPFS, OPPS, ASC, etc.)
- Domain models and value objects
- Business rule implementations
- Calculation algorithms
- Domain validation logic

**What Doesn't:**
- HTTP concerns
- Database access (use repositories)
- External service calls
- Infrastructure concerns

**Example:**
```python
class MPFSEngine:
    def __init__(self, db: Session):
        self.db = db
        self.rvu_repo = RVURepository(db)
        self.gpci_repo = GPCIRepository(db)
    
    async def calculate_price(self, zip: str, code: str, year: int):
        # Domain logic
        rvu = await self.rvu_repo.get_rvu(code, year)
        gpci = await self.gpci_repo.get_gpci(zip, year)
        
        if not rvu or not gpci:
            raise ValueError("Required data not found")
        
        # Pricing calculation
        price = rvu.total_rvu * gpci.total_gpci * self.get_conversion_factor(year)
        return PriceResult(code=code, price=price, components=rvu, gpci=gpci)
```

### 3.4 Data Access Layer
**Location:** `cms_pricing/models/` and repositories

**Responsibilities:**
- Database access and ORM management
- Data persistence and retrieval
- Query optimization and performance
- Data mapping and transformation
- Transaction management

**What Goes Here:**
- SQLAlchemy models
- Repository pattern implementations
- Database queries and operations
- Data mapping logic
- Connection management

**What Doesn't:**
- Business logic
- HTTP concerns
- Domain calculations
- External service calls

## 4. Dependency Flow & Boundaries

### 4.1 Allowed Dependencies
- **Transport → Service:** Routers can call services
- **Service → Domain:** Services can call engines
- **Service → Data:** Services can call repositories
- **Domain → Data:** Engines can call repositories
- **All → Observability:** All layers can emit metrics/logs

### 4.2 Forbidden Dependencies
- **Transport → Domain:** Routers cannot call engines directly
- **Transport → Data:** Routers cannot access database directly
- **Domain → Service:** Engines cannot call services
- **Data → Domain:** Repositories cannot call engines
- **Data → Service:** Repositories cannot call services

### 4.3 Dependency Injection Pattern
```python
# Service layer with dependency injection
class PricingService:
    def __init__(
        self,
        mpfs_engine: MPFSEngine,
        opps_engine: OPPSEngine,
        observability: ObservabilityService
    ):
        self.mpfs_engine = mpfs_engine
        self.opps_engine = opps_engine
        self.observability = observability
```

## 5. Performance & Scalability

### 5.1 Performance Budgets
- **Latency:** p95 ≤ 200ms (read), p95 ≤ 400ms (write)
- **Throughput:** Declare RPS target per service; perf tests enforce
- **Availability:** 99.9% baseline monthly
- **Response Time:** p95 ≤ 300ms for reference endpoints

### 5.2 Scalability Patterns
- Horizontal scaling with stateless workers
- Database connection pooling
- Caching strategies for reference data
- Async/await for I/O operations

### 5.3 Monitoring & Alerting
- RED metrics (Rate, Errors, Duration)
- Custom business metrics
- Performance regression detection
- Capacity planning metrics

## 6. Testing Strategy

### 6.1 Test Layers
- **Unit Tests:** Individual components in isolation
- **Integration Tests:** Service-to-service communication
- **Contract Tests:** OpenAPI compliance verification
- **End-to-End Tests:** Full API workflow validation

### 6.2 Testing Requirements
- All endpoints must have unit tests
- Contract tests auto-generated from OpenAPI
- Integration tests for critical paths
- Performance tests for SLA validation

*Reference: QTS PRD Section 7 for detailed testing requirements*

## 7. SDK & Documentation

### 7.1 SDK Generation
- Auto-generate SDKs (TypeScript, Python) on tag
- Publish Postman collection
- Examples must match live sandbox

### 7.2 Documentation Standards
- OpenAPI-first documentation
- Interactive API explorer
- Code examples for all endpoints
- Migration guides for breaking changes

## 8. Acceptance Criteria

### 8.1 Development Gates
- Endpoint added only if: OpenAPI approved; examples provided; error cases defined; contract tests pass in CI
- All layers follow dependency boundaries
- Performance budgets met
- Security requirements satisfied

### 8.2 Release Gates
- CI blocks merges when Spectral lint or contract tests fail
- Each API code PR references the spec diff
- Tagged releases upload SDKs/Postman assets and publish versioned docs

## 9. Cross-Reference Map

### Related PRDs
- **STD-observability-monitoring_prd_v1.0:** API service monitoring, performance metrics, and alerting
- **STD-api-security-and-auth_prd_v1.0:** Authentication, authorization, rate limiting, and security middleware
- **STD-qa-testing_prd_v1.0:** API testing requirements, contract testing, and quality gates
- **STD-Data-Architecture_prd_v1.0:** Data pipeline integration and data-backed response requirements

### Integration Points
- **API Observability:** This PRD Section 5.3 → Observability PRD Section 2.2 (API Service Volume)
- **API Security:** This PRD Section 3.1 → Security PRD Section 3.1 (AuthN & AuthZ)
- **API Testing:** This PRD Section 6 → QTS PRD Section 7 (Test Types & Minimums)
- **Data Integration:** This PRD Section 2.2 → DIS PRD Section 3.6 (API Readiness)

---

**End of API Standards & Architecture PRD v1.0**
