"""Main FastAPI application for CMS Pricing API"""

import time
import uuid
from contextlib import asynccontextmanager
from typing import Dict, Any

import structlog
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response as StarletteResponse

from cms_pricing.config import settings
from cms_pricing.database import engine, Base
from cms_pricing.middleware import LoggingMiddleware, SecurityMiddleware
from cms_pricing.routers import plans, pricing, geography, trace, health
from cms_pricing.cache import CacheManager
from cms_pricing.auth import verify_api_key

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer() if settings.log_format == "json" else structlog.dev.ConsoleRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Rate limiting
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{settings.rate_limit_per_minute}/minute"]
)

# Prometheus metrics
REQUEST_COUNT = Counter(
    'http_requests_total', 
    'Total HTTP requests', 
    ['method', 'endpoint', 'status_code']
)
REQUEST_DURATION = Histogram(
    'http_request_duration_seconds', 
    'HTTP request duration', 
    ['method', 'endpoint']
)
PRICING_LINES = Counter(
    'pricing_lines_total', 
    'Total pricing lines processed', 
    ['source']
)
DATASET_SELECTIONS = Counter(
    'dataset_snapshot_selected_total', 
    'Dataset snapshots selected', 
    ['dataset_id', 'digest']
)
CACHE_HITS = Counter('cache_hits_total', 'Cache hits', ['tier'])
CACHE_MISSES = Counter('cache_misses_total', 'Cache misses', ['tier'])

# Global cache manager
cache_manager = CacheManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting CMS Pricing API", version=settings.app_version)
    
    # Create database tables
    Base.metadata.create_all(bind=engine)
    
    # Initialize cache
    await cache_manager.initialize()
    
    # Warm caches if configured
    warm_slices = settings.get_warm_slices()
    if warm_slices:
        logger.info("Warming caches", slices=warm_slices)
        # TODO: Implement cache warming
    
    logger.info("CMS Pricing API started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down CMS Pricing API")
    await cache_manager.close()


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="CMS Treatment Plan Pricing & Comparison API",
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    openapi_url="/openapi.json" if settings.debug else None,
    lifespan=lifespan
)

# Add rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.debug else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(LoggingMiddleware)
app.add_middleware(SecurityMiddleware)

# Include routers
app.include_router(plans.router, prefix="/plans", tags=["plans"])
app.include_router(pricing.router, prefix="/pricing", tags=["pricing"])
app.include_router(geography.router, prefix="/geography", tags=["geography"])
app.include_router(trace.router, prefix="/trace", tags=["trace"])
app.include_router(health.router, prefix="", tags=["health"])


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware to collect Prometheus metrics"""
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    # Record metrics
    duration = time.time() - start_time
    REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status_code=response.status_code
    ).inc()
    
    return response


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    # Verify API key for metrics endpoint
    # This will be implemented in the security middleware
    return StarletteResponse(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Global HTTP exception handler"""
    run_id = getattr(request.state, 'run_id', str(uuid.uuid4()))
    
    logger.error(
        "HTTP exception",
        run_id=run_id,
        status_code=exc.status_code,
        detail=exc.detail,
        path=request.url.path,
        method=request.method
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "code": f"HTTP_{exc.status_code}",
            "trace_id": run_id
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    run_id = getattr(request.state, 'run_id', str(uuid.uuid4()))
    
    logger.error(
        "Unhandled exception",
        run_id=run_id,
        exception=str(exc),
        path=request.url.path,
        method=request.method,
        exc_info=True
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "code": "INTERNAL_ERROR",
            "trace_id": run_id
        }
    )


# Dependency to get cache manager
def get_cache_manager() -> CacheManager:
    """Dependency to get cache manager"""
    return cache_manager


# Dependency to get logger
def get_logger():
    """Dependency to get structured logger"""
    return logger


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "cms_pricing.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )
