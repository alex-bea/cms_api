# API Performance & Scalability PRD (v1.0)

## 0. Overview
This document defines the **API Performance & Scalability Standard** for the CMS Pricing API. It specifies performance budgets, caching strategies, scaling patterns, and capacity planning to ensure the API can handle production workloads efficiently and cost-effectively.

**Status:** Adopted v1.0  
**Owners:** Platform Engineering (Performance Guild)  
**Consumers:** Product, Engineering, Data, Ops  
**Change control:** ADR + Architecture Board review  

**Cross-References:**
- **API-STD-Architecture_prd_v1.0:** Layer architecture and performance budgets
- **STD-observability-monitoring_prd_v1.0:** Performance monitoring and alerting
- **STD-api-security-and-auth_prd_v1.0:** Rate limiting and security performance impact
- **STD-Data-Architecture_prd_v1.0:** Data pipeline performance requirements

## 1. Goals & Non-Goals

**Goals**
- Define clear performance budgets and SLAs for all API endpoints
- Establish caching strategies to optimize response times and reduce costs
- Provide scaling patterns for horizontal and vertical growth
- Enable capacity planning and resource optimization
- Ensure performance regression detection and prevention

**Non-Goals**
- Real-time streaming performance (covered by separate streaming standard)
- Database optimization (covered by STD-Data-Architecture_prd_v1.0)
- Infrastructure provisioning (covered by separate infrastructure PRD)

## 2. Performance Budgets & SLAs

### 2.1 Response Time SLAs
- **Read Operations:** p95 ≤ 200ms, p99 ≤ 500ms
- **Write Operations:** p95 ≤ 400ms, p99 ≤ 1000ms
- **Reference Data Lookups:** p95 ≤ 100ms, p99 ≤ 200ms
- **Complex Pricing Calculations:** p95 ≤ 300ms, p99 ≤ 800ms

### 2.2 Throughput SLAs
- **Standard Endpoints:** 1000 RPS per instance
- **Reference Data Endpoints:** 5000 RPS per instance
- **Pricing Calculation Endpoints:** 500 RPS per instance
- **Bulk Operations:** 100 RPS per instance

### 2.3 Availability SLAs
- **API Availability:** 99.9% monthly uptime
- **Data Freshness:** Reference data ≤ 5 minutes stale
- **Cache Hit Rate:** ≥ 90% for reference data endpoints
- **Error Rate:** ≤ 0.1% 5xx errors

### 2.4 Resource Utilization SLAs
- **CPU Usage:** ≤ 70% average, ≤ 90% peak
- **Memory Usage:** ≤ 80% average, ≤ 95% peak
- **Database Connections:** ≤ 80% of pool capacity
- **Network Bandwidth:** ≤ 80% of instance capacity

## 3. Caching Strategy

### 3.1 Cache Layers
```
Client Cache (Browser/CDN) → API Gateway Cache → Application Cache → Database Cache
```

### 3.2 Cache Types & TTLs

**Reference Data Cache:**
- **TTL:** 1 hour (configurable)
- **Scope:** ZIP codes, localities, GPCI data
- **Storage:** Redis cluster
- **Invalidation:** Manual or time-based

**Pricing Results Cache:**
- **TTL:** 15 minutes
- **Scope:** Calculated pricing results
- **Storage:** Redis cluster
- **Invalidation:** Data change events

**Static Content Cache:**
- **TTL:** 24 hours
- **Scope:** API documentation, schemas, static assets
- **Storage:** CDN (CloudFront)
- **Invalidation:** Manual deployment

### 3.3 Cache Implementation
```python
from functools import wraps
import redis
import json
import hashlib

redis_client = redis.Redis(host='cache-cluster', port=6379, db=0)

def cache_result(ttl: int = 3600):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key_data = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            cache_key = f"api:{hashlib.md5(key_data.encode()).hexdigest()}"
            
            # Try cache first
            cached = redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Cache result
            redis_client.setex(cache_key, ttl, json.dumps(result))
            return result
        return wrapper
    return decorator

# Usage
@cache_result(ttl=3600)
async def get_locality_data(zip_code: str):
    # Expensive database query
    return await db.query("SELECT * FROM localities WHERE zip = ?", zip_code)
```

### 3.4 Cache Invalidation Strategy
- **Time-based:** Automatic expiration
- **Event-based:** Invalidate on data changes
- **Manual:** Admin interface for cache clearing
- **Pattern-based:** Invalidate related cache entries

## 4. Scaling Patterns

### 4.1 Horizontal Scaling
**Stateless Design:**
- No session state in application
- External session storage (Redis)
- Load balancer distribution

**Auto-scaling Configuration:**
```yaml
# Kubernetes HPA configuration
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: cms-pricing-api
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

### 4.2 Vertical Scaling
**Resource Limits:**
- CPU: 2-8 cores per instance
- Memory: 4-16 GB per instance
- Storage: 20-100 GB per instance

**Scaling Triggers:**
- CPU utilization > 70% for 5 minutes
- Memory utilization > 80% for 5 minutes
- Response time p95 > SLA threshold
- Error rate > 0.1%

### 4.3 Database Scaling
**Read Replicas:**
- Separate read-only instances
- Automatic failover
- Load balancing across replicas

**Connection Pooling:**
```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,          # Base connections
    max_overflow=30,       # Additional connections
    pool_pre_ping=True,    # Validate connections
    pool_recycle=3600      # Recycle connections hourly
)
```

## 5. Performance Monitoring

### 5.1 Key Metrics
**Response Time Metrics:**
- p50, p95, p99 latencies
- Average response time
- Request duration histograms

**Throughput Metrics:**
- Requests per second (RPS)
- Requests per minute (RPM)
- Concurrent connections

**Error Metrics:**
- Error rate by status code
- Timeout rate
- Circuit breaker activations

**Resource Metrics:**
- CPU utilization
- Memory usage
- Database connection pool usage
- Cache hit/miss ratios

### 5.2 Monitoring Implementation
```python
from prometheus_client import Counter, Histogram, Gauge
import time

# Metrics definitions
REQUEST_COUNT = Counter('api_requests_total', 'Total API requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('api_request_duration_seconds', 'Request duration', ['method', 'endpoint'])
ACTIVE_CONNECTIONS = Gauge('api_active_connections', 'Active connections')
CACHE_HITS = Counter('api_cache_hits_total', 'Cache hits', ['cache_type'])
CACHE_MISSES = Counter('api_cache_misses_total', 'Cache misses', ['cache_type'])

# Middleware for metrics collection
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    
    response = await call_next(request)
    
    duration = time.time() - start_time
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    
    REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    return response
```

### 5.3 Alerting Rules
**Critical Alerts:**
- Response time p95 > SLA threshold
- Error rate > 0.1%
- Availability < 99.9%
- Cache hit rate < 90%

**Warning Alerts:**
- Response time p95 > 80% of SLA threshold
- CPU utilization > 70%
- Memory utilization > 80%
- Database connection pool > 80%

## 6. Performance Testing

### 6.1 Load Testing Strategy
**Test Types:**
- **Smoke Tests:** Basic functionality under minimal load
- **Load Tests:** Expected production load
- **Stress Tests:** Beyond expected capacity
- **Spike Tests:** Sudden load increases
- **Volume Tests:** Large data sets

**Tools:**
- **Locust:** Python-based load testing
- **Artillery:** Node.js-based testing
- **JMeter:** Java-based testing

### 6.2 Load Testing Implementation
```python
# Locust load test example
from locust import HttpUser, task, between

class PricingAPIUser(HttpUser):
    wait_time = between(1, 3)
    
    @task(3)
    def price_single_code(self):
        self.client.get("/codes/price?zip=90210&code=99213&setting=office&year=2025")
    
    @task(1)
    def get_reference_data(self):
        self.client.get("/reference/localities?zip=90210")
    
    @task(1)
    def health_check(self):
        self.client.get("/health")
```

### 6.3 Performance Regression Testing
**CI Integration:**
- Automated performance tests on every PR
- Baseline comparison with previous runs
- Performance budget enforcement
- Regression detection and blocking

**Performance Budgets:**
- Response time regression > 10%
- Throughput regression > 5%
- Resource utilization increase > 15%

## 7. Capacity Planning

### 7.1 Growth Projections
**User Growth:**
- Current: 100 concurrent users
- 6 months: 500 concurrent users
- 12 months: 2000 concurrent users
- 24 months: 10000 concurrent users

**Data Growth:**
- Current: 1M pricing records
- 6 months: 5M pricing records
- 12 months: 20M pricing records
- 24 months: 100M pricing records

### 7.2 Resource Planning
**Instance Sizing:**
- **Development:** 2 instances, 2 CPU, 4GB RAM
- **Staging:** 3 instances, 4 CPU, 8GB RAM
- **Production:** 5+ instances, 8 CPU, 16GB RAM

**Database Sizing:**
- **Development:** Single instance, 2 CPU, 8GB RAM
- **Staging:** Primary + 1 replica, 4 CPU, 16GB RAM
- **Production:** Primary + 3 replicas, 8 CPU, 32GB RAM

### 7.3 Cost Optimization
**Resource Optimization:**
- Right-size instances based on actual usage
- Use spot instances for non-critical workloads
- Implement auto-scaling to reduce idle costs
- Monitor and optimize database queries

**Caching Benefits:**
- Reduce database load by 80%
- Improve response times by 60%
- Reduce infrastructure costs by 40%

## 8. Performance Optimization

### 8.1 Code Optimization
**Async/Await Patterns:**
```python
# Good: Async database operations
async def get_pricing_data(zip_code: str, code: str):
    async with get_db_session() as db:
        locality = await db.execute(
            "SELECT * FROM localities WHERE zip = ?", zip_code
        )
        rvu = await db.execute(
            "SELECT * FROM rvu_data WHERE code = ?", code
        )
        return calculate_price(locality, rvu)

# Bad: Synchronous operations
def get_pricing_data(zip_code: str, code: str):
    locality = db.execute("SELECT * FROM localities WHERE zip = ?", zip_code)
    rvu = db.execute("SELECT * FROM rvu_data WHERE code = ?", code)
    return calculate_price(locality, rvu)
```

**Database Query Optimization:**
- Use prepared statements
- Implement connection pooling
- Optimize query patterns
- Use database indexes effectively

### 8.2 Infrastructure Optimization
**CDN Configuration:**
- Cache static assets
- Compress responses
- Use HTTP/2
- Implement proper cache headers

**Load Balancer Configuration:**
- Health check configuration
- Session affinity (if needed)
- SSL termination
- Rate limiting

## 9. Acceptance Criteria

### 9.1 Performance Requirements
- ✅ All endpoints meet response time SLAs
- ✅ Throughput targets achieved under load
- ✅ Availability targets met
- ✅ Resource utilization within limits

### 9.2 Monitoring Requirements
- ✅ Performance metrics collected and exposed
- ✅ Alerting rules configured and tested
- ✅ Dashboards available for monitoring
- ✅ Performance regression detection active

### 9.3 Testing Requirements
- ✅ Load testing suite implemented
- ✅ Performance regression testing in CI
- ✅ Capacity planning documented
- ✅ Cost optimization strategies implemented

## 10. Cross-Reference Map

### Related PRDs
- **API-STD-Architecture_prd_v1.0:** Layer architecture and performance budgets
- **STD-observability-monitoring_prd_v1.0:** Performance monitoring, alerting, and SLAs
- **STD-api-security-and-auth_prd_v1.0:** Rate limiting performance impact and security overhead
- **STD-Data-Architecture_prd_v1.0:** Data pipeline performance requirements

### Integration Points
- **Performance Monitoring:** This PRD Section 5 → Observability PRD Section 2.2 (API Service Volume)
- **Performance SLAs:** This PRD Section 2 → Observability PRD Section 3.2 (API Service SLAs)
- **Rate Limiting:** This PRD Section 4 → Security PRD Section 3.4 (Rate Limiting & Throttling)
- **Data Performance:** This PRD Section 7 → DIS PRD Section 8 (Observability & Monitoring)

---

**End of API Performance & Scalability PRD v1.0**
