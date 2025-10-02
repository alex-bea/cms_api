# API Contract Management PRD (v1.0)

## 0. Overview
This document defines the **API Contract Management Standard** for the CMS Pricing API. It specifies schema evolution, versioning strategies, compatibility rules, and contract governance to ensure API stability and backward compatibility while enabling controlled evolution.

**Status:** Adopted v1.0  
**Owners:** Platform Engineering (API Guild)  
**Consumers:** Product, Engineering, Data, Ops  
**Change control:** ADR + Architecture Board review  

**Cross-References:**
- **API-STD-Architecture_prd_v1.0:** OpenAPI SSOT and contract-first development
- **Observability & Monitoring PRD v1.0:** Contract monitoring and drift detection
- **QA Testing Standard (QTS) v1.0:** Contract testing requirements
- **Data Architecture PRD v1.0:** Data schema contracts and evolution

## 1. Goals & Non-Goals

**Goals**
- Establish OpenAPI as single source of truth for all API contracts
- Define clear schema evolution and versioning strategies
- Ensure backward compatibility and controlled breaking changes
- Provide automated contract validation and testing
- Enable client SDK generation and documentation

**Non-Goals**
- Database schema management (covered by Data Architecture PRD)
- Internal service contracts (covered by separate microservices standard)
- Real-time streaming contracts (covered by separate streaming standard)

## 2. Contract Governance

### 2.1 OpenAPI as SSOT
**Contract Location:** `/api-contracts/openapi.yaml`

**Contract Ownership:**
- **API Guild:** Overall contract governance and standards
- **Product Teams:** Domain-specific contract requirements
- **Platform Engineering:** Contract tooling and automation

**Contract Lifecycle:**
1. **Design:** Contract designed in OpenAPI spec
2. **Review:** Architecture Board review for breaking changes
3. **Implementation:** Code implementation follows contract
4. **Testing:** Contract tests validate implementation
5. **Release:** Contract versioned and released
6. **Deprecation:** Breaking changes follow deprecation process

### 2.2 Contract Validation Rules
**Schema Validation:**
- All endpoints must have OpenAPI definitions
- Request/response schemas must be complete
- Examples must be provided for all schemas
- Error responses must be documented
- All 4xx/5xx responses must reference the canonical error schema via `components.responses`; ad-hoc error bodies are forbidden.

**Implementation Validation:**
- Code must match OpenAPI contract
- Contract tests must pass
- SDK generation must succeed
- Documentation must be generated

### 2.3 Contract Review Process
**Non-Breaking Changes:**
- Team lead approval
- Automated contract validation
- CI/CD pipeline validation

**Breaking Changes:**
- Architecture Board review required
- 90-day deprecation window
- Migration guide required
- Client notification process

### 2.4 Canonical Error Contract

All 4xx/5xx responses MUST conform to a single, canonical error schema. No ad-hoc error bodies are allowed. Every error response MUST include a stable machine code, a human-readable message, and a `trace_id` that correlates with server logs. Where useful, include a `docs_url` to the relevant troubleshooting page.

```yaml
components:
  schemas:
    Error:
      type: object
      required: [code, message, trace_id]
      properties:
        code:
          type: string
          description: Stable, documented machine code (e.g., "CREDITS_EXHAUSTED")
        message:
          type: string
          description: Human-readable summary (safe for logs)
        details:
          type: array
          items:
            type: object
            properties:
              field: { type: string }
              issue: { type: string }
        trace_id:
          type: string
          description: Correlates with server logs (e.g., W3C traceparent)
        docs_url:
          type: string
          format: uri
  responses:
    BadRequest:
      description: Bad Request
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/Error'
    Unauthorized:
      description: Unauthorized
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/Error'
    PaymentRequired:
      description: Payment Required (credits)
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/Error'
    NotFound:
      description: Not Found
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/Error'
    TooManyRequests:
      description: Too Many Requests
      headers:
        Retry-After:
          schema:
            type: string
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/Error'
```

**Standard usage in paths (example):**
```yaml
paths:
  /pricing/estimate:
    get:
      responses:
        '200':
          description: OK
        '400':
          $ref: '#/components/responses/BadRequest'
        '401':
          $ref: '#/components/responses/Unauthorized'
        '402':
          $ref: '#/components/responses/PaymentRequired'
        '404':
          $ref: '#/components/responses/NotFound'
        '429':
          $ref: '#/components/responses/TooManyRequests'
```

## 3. Schema Evolution & Versioning

### 3.1 Semantic Versioning
**Version Format:** `MAJOR.MINOR.PATCH`

**Version Rules:**
- **MAJOR:** Breaking changes (incompatible API changes)
- **MINOR:** Backward-compatible additions (new endpoints, optional fields)
- **PATCH:** Backward-compatible bug fixes (documentation, examples)

**Version Examples:**
- `v1.0.0` → `v1.1.0`: Add new optional field
- `v1.1.0` → `v2.0.0`: Remove deprecated field
- `v1.1.0` → `v1.1.1`: Fix documentation typo

### 3.2 Backward Compatibility Rules
**Allowed Changes (MINOR/PATCH):**
- Add new optional fields
- Add new endpoints
- Add new enum values
- Improve documentation
- Add examples

**Breaking Changes (MAJOR):**
- Remove fields
- Change field types
- Make optional fields required
- Remove endpoints
- Change endpoint behavior

### 3.3 Schema Evolution Patterns
**Additive Changes:**
```yaml
# Before
components:
  schemas:
    PricingRequest:
      type: object
      properties:
        zip_code:
          type: string
        hcpcs_code:
          type: string
      required: [zip_code, hcpcs_code]

# After (MINOR version)
components:
  schemas:
    PricingRequest:
      type: object
      properties:
        zip_code:
          type: string
        hcpcs_code:
          type: string
        modifier:          # New optional field
          type: string
      required: [zip_code, hcpcs_code]
```

**Deprecation Pattern:**
```yaml
# Deprecate field
components:
  schemas:
    PricingRequest:
      type: object
      properties:
        zip_code:
          type: string
        hcpcs_code:
          type: string
        old_field:           # Deprecated field
          type: string
          deprecated: true
          x-deprecation-date: "2025-06-01"
          x-sunset-date: "2025-09-01"
```

### 3.4 Compatibility Matrix

| Change Type                          | Example                                    | Allowed w/o Major? | Client Action Needed | Notes |
|-------------------------------------|--------------------------------------------|---------------------|----------------------|------|
| Add optional response field         | `PricingResponse.unit`                      | ✅ MINOR            | None                 | Clients must tolerate unknown fields. |
| Add enum value                      | `PricingTier: "ENTERPRISE"`                 | ⚠️ MINOR*           | Maybe                | Breaking for strict switch/case—clients must default. |
| Add query param (optional)          | `?modifier=LT`                              | ✅ MINOR            | None                 | Server must preserve behavior if omitted. |
| Change default value                | default sort from `code`→`date`             | ❌ MAJOR            | Yes                  | Behavior change. |
| Make optional → required            | `zip_code`                                  | ❌ MAJOR            | Yes                  | N/A  |
| Remove field/endpoint               | `old_field`                                 | ❌ MAJOR            | Yes                  | Follow deprecation §7. |
| Tighten validation                  | max length 64→32                            | ❌ MAJOR            | Yes                  | N/A  |
| Add error code                      | introduce `429` rate-limit                  | ✅ MINOR            | None                 | Must be documented in error catalog. |



## 4. Contract Testing & Validation

### 4.1 Contract Testing Strategy
**Test Types:**
- **Contract Tests:** Validate implementation matches contract
- **Compatibility Tests:** Ensure backward compatibility
- **Regression Tests:** Detect breaking changes
- **Client Tests:** Validate SDK generation
- **Error Contract Tests:** Verify that all 4xx/5xx responses conform to `components.schemas.Error`.

**Testing Tools:**
- **Schemathesis:** OpenAPI-based contract testing
- **Dredd:** API contract testing
- **Pact:** Consumer-driven contract testing

### 4.2 Contract Test Implementation
```python
# Schemathesis contract test example
import schemathesis

schema = schemathesis.from_file("api-contracts/openapi.yaml")

@schema.parametrize()
def test_api_contract(case):
    response = case.call()
    case.validate_response(response)

# Custom contract validation
def test_response_schema():
    response = client.get("/codes/price?zip=90210&code=99213")
    assert response.status_code == 200
    
    # Validate response matches OpenAPI schema
    schema = load_openapi_schema("PricingResponse")
    validate_response(response.json(), schema)
```

### 4.3 CI/CD Integration
**Contract Validation Pipeline:**
```yaml
# GitHub Actions workflow
name: Contract Validation
on: [push, pull_request]

jobs:
  contract-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: pip install schemathesis
      - name: Run contract tests
        run: schemathesis run api-contracts/openapi.yaml --base-url=http://localhost:8000
      - name: Validate schema
        run: python scripts/validate_schema.py
```

## 5. Client SDK Generation

### 5.1 SDK Generation Strategy
**Supported Languages:**
- **TypeScript:** Web and Node.js clients
- **Python:** Data science and backend clients
- **Java:** Enterprise clients
- **Go:** High-performance clients

**Generation Tools:**
- **OpenAPI Generator:** Multi-language SDK generation
- **Swagger Codegen:** Legacy tool (deprecated)
- **Custom Generators:** Domain-specific clients

### 5.2 SDK Generation Implementation
```yaml
# OpenAPI Generator configuration
openapi-generator-cli:
  generate:
    - generator: typescript-fetch
      input: api-contracts/openapi.yaml
      output: clients/typescript
      config:
        npmName: "@cms-pricing/api-client"
        npmVersion: "1.0.0"
    
    - generator: python
      input: api-contracts/openapi.yaml
      output: clients/python
      config:
        packageName: cms_pricing_api
        packageVersion: "1.0.0"
```

### 5.3 SDK Release Process
**Release Automation:**
- SDKs generated on every contract change
- Versioned with API contract version
- Published to package registries (npm, PyPI, Maven)
- Documentation generated and published

**Release Pipeline:**
```yaml
# SDK release workflow
name: SDK Release
on:
  push:
    tags: ['v*']

jobs:
  generate-sdks:
    runs-on: ubuntu-latest
    steps:
      - name: Generate TypeScript SDK
        run: openapi-generator-cli generate -i api-contracts/openapi.yaml -g typescript-fetch -o clients/typescript
      - name: Generate Python SDK
        run: openapi-generator-cli generate -i api-contracts/openapi.yaml -g python -o clients/python
      - name: Publish SDKs
        run: |
          cd clients/typescript && npm publish
          cd clients/python && python setup.py sdist bdist_wheel && twine upload dist/*
```

## 6. Documentation Generation

### 6.1 Documentation Strategy
**Documentation Types:**
- **API Reference:** Auto-generated from OpenAPI
- **Getting Started:** Quick start guides
- **Code Examples:** Language-specific examples
- **Migration Guides:** Breaking change guides

**Documentation Tools:**
- **Swagger UI:** Interactive API explorer
- **ReDoc:** Clean API documentation
- **GitBook:** Comprehensive guides
- **Custom Docs:** Domain-specific documentation

#### Error Catalog (excerpt)
- `CREDITS_EXHAUSTED` → 402: Balance is 0; includes top-up link.
- `RATE_LIMITED` → 429: `Retry-After` header included; exponential backoff advised.
- `VALIDATION_FAILED` → 400: `details[]` lists (field, issue).
- `UNAUTHORIZED` → 401: Invalid or missing credentials.

### 6.2 Documentation Implementation
```yaml
# Swagger UI configuration
swagger-ui:
  title: "CMS Pricing API"
  version: "1.0.0"
  description: "Healthcare pricing and comparison API"
  servers:
    - url: "https://api.cms-pricing.com"
      description: "Production server"
    - url: "https://staging-api.cms-pricing.com"
      description: "Staging server"
```

### 6.3 Documentation Publishing
**Publishing Strategy:**
- Documentation hosted on GitHub Pages
- Versioned documentation for each API version
- Automatic updates on contract changes
- Search and navigation features

## 7. Breaking Change Management

### 7.1 Deprecation Process
**Deprecation Timeline:**
1. **Announcement:** Add deprecation notice to contract
2. **Communication:** Notify all clients
3. **Grace Period:** 90-day deprecation window
4. **Sunset:** Remove deprecated functionality

**Deprecation Implementation:**
```yaml
# Deprecation in OpenAPI
paths:
  /deprecated-endpoint:
    get:
      deprecated: true
      x-deprecation-date: "2025-01-01"
      x-sunset-date: "2025-04-01"
      x-migration-guide: "https://docs.cms-pricing.com/migration/v2"
```

### 7.2 Migration Support
**Migration Tools:**
- **Migration Guides:** Step-by-step instructions
- **Code Examples:** Before/after examples
- **SDK Updates:** Automatic migration in SDKs
- **Support:** Dedicated migration support

**Migration Timeline:**
- **Month 1:** Announce deprecation
- **Month 2:** Provide migration guide
- **Month 3:** Sunset deprecated functionality
- **Month 4:** Remove deprecated code

**Deprecation headers**
Deprecation: true
Sunset: Wed, 01 Jan 2026 00:00:00 GMT
Link: <https://docs.cms-pricing.com/migration/v2>; rel="deprecation"


**OpenAPI extension on deprecated elements:**
x-deprecation:
  announced_at: "2025-10-01"
  sunset_at: "2026-01-01"
  replacement: "/v2/pricing"
  migration_doc: "https://docs.cms-pricing.com/migration/v2"

  

## 8. Contract Monitoring & Observability

### 8.1 Contract Drift Detection
**Drift Types:**
- **Schema Drift:** Implementation doesn't match contract
- **Behavior Drift:** Response behavior changes
- **Performance Drift:** Response times change
- **Error Drift:** Error responses change

**Monitoring Implementation:**
```python
# Contract drift detection
import jsonschema
from prometheus_client import Counter, Histogram

CONTRACT_VIOLATIONS = Counter('api_contract_violations_total', 'Contract violations', ['endpoint', 'violation_type'])
RESPONSE_VALIDATION_TIME = Histogram('api_response_validation_seconds', 'Response validation time')

def validate_response(response_data, schema):
    start_time = time.time()
    
    try:
        jsonschema.validate(response_data, schema)
    except jsonschema.ValidationError as e:
        CONTRACT_VIOLATIONS.labels(
            endpoint=request.endpoint,
            violation_type='schema'
        ).inc()
        raise
    
    RESPONSE_VALIDATION_TIME.observe(time.time() - start_time)
```

### 8.2 Contract Analytics
**Analytics Metrics:**
- Contract usage patterns
- Client adoption rates
- Breaking change impact
- Migration success rates

**Analytics Dashboard:**
- Contract version adoption
- Client SDK usage
- Error rate by contract version
- Performance by contract version

## 9. Acceptance Criteria

### 9.1 Contract Requirements
- ✅ OpenAPI spec is complete and valid
- ✅ All endpoints have proper schemas
- ✅ Contract tests pass in CI
- ✅ SDK generation succeeds

### 9.2 Governance Requirements
- ✅ Contract review process defined
- ✅ Breaking change process documented
- ✅ Deprecation timeline established
- ✅ Migration support provided

### 9.3 Monitoring Requirements
- ✅ Contract drift detection active
- ✅ Analytics dashboard available
- ✅ Alerting configured for violations
- ✅ Performance monitoring integrated

## 10. Cross-Reference Map

### Related PRDs
- **API-STD-Architecture_prd_v1.0:** OpenAPI SSOT and contract-first development
- **Observability & Monitoring PRD v1.0:** Contract monitoring, drift detection, and analytics
- **QA Testing Standard (QTS) v1.0:** Contract testing requirements and validation
- **Data Ingestion Standard (DIS) v1.0:** Data schema contracts and evolution patterns

### Integration Points
- **Contract Testing:** This PRD Section 4 → QTS PRD Section 7 (Contract Test Patterns)
- **Contract Monitoring:** This PRD Section 8 → Observability PRD Section 2.3 (Schema)
- **API Standards:** This PRD Section 2 → API Standards PRD Section 2.1 (OpenAPI SSOT)
- **Data Contracts:** This PRD Section 3 → DIS PRD Section 3.1 (Data Contracts & Schema Governance)

---

**End of API Contract Management PRD v1.0**
