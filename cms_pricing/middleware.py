"""Custom middleware for CMS Pricing API"""

import time
import uuid
from typing import Callable
import structlog

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from cms_pricing.config import settings

logger = structlog.get_logger()


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for structured request/response logging"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate run ID
        run_id = str(uuid.uuid4())
        request.state.run_id = run_id
        
        # Log request
        start_time = time.time()
        logger.info(
            "Request started",
            run_id=run_id,
            method=request.method,
            path=request.url.path,
            query_params=dict(request.query_params),
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
        
        # Process request
        try:
            response = await call_next(request)
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Log response
            logger.info(
                "Request completed",
                run_id=run_id,
                status_code=response.status_code,
                duration_ms=duration_ms,
            )
            
            # Add run ID to response headers
            response.headers["X-Run-ID"] = run_id
            
            return response
            
        except Exception as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            
            logger.error(
                "Request failed",
                run_id=run_id,
                exception=str(exc),
                duration_ms=duration_ms,
                exc_info=True
            )
            
            raise


class SecurityMiddleware(BaseHTTPMiddleware):
    """Middleware for API key authentication and security headers"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip auth for health checks and docs
        if request.url.path in ["/healthz", "/readyz", "/docs", "/redoc", "/openapi.json"]:
            return await call_next(request)
        
        # Verify API key
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return JSONResponse(
                status_code=401,
                content={"error": "API key required", "code": "MISSING_API_KEY"}
            )
        
        valid_keys = settings.get_api_keys()
        if api_key not in valid_keys:
            return JSONResponse(
                status_code=401,
                content={"error": "Invalid API key", "code": "INVALID_API_KEY"}
            )
        
        # Add security headers
        response = await call_next(request)
        
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        # Add CORS headers for API endpoints
        if request.url.path.startswith("/api/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-API-Key"
        
        return response
