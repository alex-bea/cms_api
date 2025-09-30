"""Configuration management for CMS Pricing API"""

import os
from typing import List, Optional
try:
    # pydantic v2+ moved BaseSettings to pydantic-settings package
    from pydantic_settings import BaseSettings
    from pydantic import Field
except Exception:
    # Fallback for older installations where BaseSettings is in pydantic
    from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings"""
    
    # Database
    database_url: str = Field(default="postgresql://cms_user:cms_password@localhost:5432/cms_pricing", env="DATABASE_URL")
    redis_url: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    
    # API Configuration - using simple string field
    api_keys_str: str = Field(default="dev-key-123,admin-key-456,worker-key-789", env="API_KEYS")
    rate_limit_per_minute: int = Field(default=120, env="RATE_LIMIT_PER_MINUTE")
    
    # Data Configuration
    max_providers: int = Field(default=100, env="MAX_PROVIDERS")
    max_radius_miles: int = Field(default=25, env="MAX_RADIUS_MILES")
    default_fallback_multiplier: float = Field(default=1.0, env="DEFAULT_FALLBACK_MULTIPLIER")
    retention_months: int = Field(default=13, env="RETENTION_MONTHS")
    
    # Storage Configuration
    data_cache_dir: str = Field(default="./data/cache", env="DATA_CACHE_DIR")
    s3_bucket: Optional[str] = Field(default=None, env="S3_BUCKET")
    s3_prefix: str = Field(default="datasets", env="S3_PREFIX")
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field(default="us-east-1", env="AWS_REGION")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    trace_verbose: bool = Field(default=False, env="TRACE_VERBOSE")
    log_format: str = Field(default="json", env="LOG_FORMAT")
    
    # Cache Configuration
    cache_ttl_seconds: int = Field(default=3600, env="CACHE_TTL_SECONDS")
    cache_max_items: int = Field(default=512, env="CACHE_MAX_ITEMS")
    cache_max_bytes: int = Field(default=1073741824, env="CACHE_MAX_BYTES")  # 1GB
    
    # Security Configuration
    secret_key: str = Field(default="your-secret-key-here", env="SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    jwt_expire_minutes: int = Field(default=60, env="JWT_EXPIRE_MINUTES")
    
    # Performance Configuration
    warm_slices: str = Field(default="", env="WARM_SLICES")
    max_concurrent_requests: int = Field(default=25, env="MAX_CONCURRENT_REQUESTS")
    burst_limit: int = Field(default=100, env="BURST_LIMIT")
    
    # Application Configuration
    app_name: str = "CMS Pricing API"
    app_version: str = "0.1.0"
    debug: bool = Field(default=False, env="DEBUG")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields in environment
        
    def get_api_keys(self) -> List[str]:
        """Parse comma-separated API keys"""
        return [key.strip() for key in self.api_keys_str.split(",")]
    
    def get_warm_slices(self) -> dict:
        """Parse warm slices configuration"""
        if not self.warm_slices:
            return {}
        
        slices = {}
        for slice_config in self.warm_slices.split(","):
            if ":" in slice_config:
                dataset, year_quarter = slice_config.split(":", 1)
                slices[dataset.strip()] = year_quarter.strip()
        return slices


# Global settings instance
settings = Settings()