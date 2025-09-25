"""Caching system for dataset slices and computed results"""

import asyncio
import hashlib
import json
import os
import pickle
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timedelta
import structlog

from cms_pricing.config import settings

logger = structlog.get_logger()


class LRUCache:
    """Simple LRU cache implementation"""
    
    def __init__(self, max_items: int = 512, max_bytes: int = 1073741824):
        self.max_items = max_items
        self.max_bytes = max_bytes
        self.cache: Dict[str, Tuple[Any, datetime]] = {}
        self.access_times: Dict[str, datetime] = {}
        self.current_bytes = 0
    
    def _estimate_size(self, obj: Any) -> int:
        """Estimate object size in bytes"""
        try:
            return len(pickle.dumps(obj))
        except:
            return 1024  # Default estimate
    
    def _evict_oldest(self):
        """Evict oldest accessed items"""
        if not self.access_times:
            return
        
        # Sort by access time
        sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
        
        for key, _ in sorted_items:
            if key in self.cache:
                del self.cache[key]
                del self.access_times[key]
                break
    
    def get(self, key: str) -> Optional[Any]:
        """Get item from cache"""
        if key not in self.cache:
            return None
        
        # Update access time
        self.access_times[key] = datetime.utcnow()
        
        # Return value
        value, _ = self.cache[key]
        return value
    
    def put(self, key: str, value: Any, ttl_seconds: int = 3600):
        """Put item in cache"""
        # Estimate size
        size = self._estimate_size(value)
        
        # Evict if necessary
        while (len(self.cache) >= self.max_items or 
               self.current_bytes + size > self.max_bytes):
            self._evict_oldest()
        
        # Store with expiration
        expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
        self.cache[key] = (value, expires_at)
        self.access_times[key] = datetime.utcnow()
        self.current_bytes += size
    
    def _cleanup_expired(self):
        """Remove expired items"""
        now = datetime.utcnow()
        expired_keys = [
            key for key, (_, expires_at) in self.cache.items()
            if expires_at < now
        ]
        
        for key in expired_keys:
            del self.cache[key]
            if key in self.access_times:
                del self.access_times[key]
    
    def clear(self):
        """Clear all items"""
        self.cache.clear()
        self.access_times.clear()
        self.current_bytes = 0


class DiskCache:
    """Disk-based cache with digest verification"""
    
    def __init__(self, cache_dir: str = "./data/cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        """Get cache file path"""
        # Create subdirectory based on key hash
        key_hash = hashlib.md5(key.encode()).hexdigest()[:2]
        subdir = os.path.join(self.cache_dir, key_hash)
        os.makedirs(subdir, exist_ok=True)
        
        return os.path.join(subdir, f"{key_hash}.pkl")
    
    def get(self, key: str, expected_digest: Optional[str] = None) -> Optional[Any]:
        """Get item from disk cache"""
        cache_path = self._get_cache_path(key)
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            
            # Verify digest if provided
            if expected_digest and data.get('digest') != expected_digest:
                logger.warning(
                    "Cache digest mismatch",
                    key=key,
                    expected=expected_digest,
                    actual=data.get('digest')
                )
                return None
            
            # Check expiration
            if data.get('expires_at') and datetime.fromisoformat(data['expires_at']) < datetime.utcnow():
                os.remove(cache_path)
                return None
            
            return data['value']
            
        except Exception as e:
            logger.warning("Failed to load from disk cache", key=key, error=str(e))
            return None
    
    def put(self, key: str, value: Any, digest: Optional[str] = None, ttl_seconds: int = 3600):
        """Put item in disk cache"""
        cache_path = self._get_cache_path(key)
        
        try:
            data = {
                'value': value,
                'digest': digest,
                'expires_at': (datetime.utcnow() + timedelta(seconds=ttl_seconds)).isoformat(),
                'created_at': datetime.utcnow().isoformat()
            }
            
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
                
        except Exception as e:
            logger.warning("Failed to save to disk cache", key=key, error=str(e))


class CacheManager:
    """Unified cache manager"""
    
    def __init__(self):
        self.memory_cache = LRUCache(
            max_items=settings.cache_max_items,
            max_bytes=settings.cache_max_bytes
        )
        self.disk_cache = DiskCache(settings.data_cache_dir)
        self.locks: Dict[str, asyncio.Lock] = {}
    
    async def initialize(self):
        """Initialize cache manager"""
        logger.info("Initializing cache manager")
        # Clean up expired items
        self.memory_cache._cleanup_expired()
    
    async def close(self):
        """Close cache manager"""
        logger.info("Closing cache manager")
        # Clean up locks
        self.locks.clear()
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create lock for key"""
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        return self.locks[key]
    
    async def get(self, key: str, expected_digest: Optional[str] = None) -> Optional[Any]:
        """Get item from cache (memory first, then disk)"""
        # Try memory cache first
        value = self.memory_cache.get(key)
        if value is not None:
            return value
        
        # Try disk cache
        value = self.disk_cache.get(key, expected_digest)
        if value is not None:
            # Promote to memory cache
            self.memory_cache.put(key, value)
            return value
        
        return None
    
    async def put(self, key: str, value: Any, digest: Optional[str] = None, ttl_seconds: int = None):
        """Put item in cache (both memory and disk)"""
        if ttl_seconds is None:
            ttl_seconds = settings.cache_ttl_seconds
        
        # Put in memory cache
        self.memory_cache.put(key, value, ttl_seconds)
        
        # Put in disk cache
        self.disk_cache.put(key, value, digest, ttl_seconds)
    
    async def get_or_set(self, key: str, factory_func, expected_digest: Optional[str] = None, ttl_seconds: int = None) -> Any:
        """Get from cache or compute and set"""
        # Try to get from cache
        value = await self.get(key, expected_digest)
        if value is not None:
            return value
        
        # Use lock to prevent duplicate computation
        lock = self._get_lock(key)
        async with lock:
            # Double-check after acquiring lock
            value = await self.get(key, expected_digest)
            if value is not None:
                return value
            
            # Compute value
            value = await factory_func()
            
            # Store in cache
            await self.put(key, value, expected_digest, ttl_seconds)
            
            return value
    
    def clear(self):
        """Clear all caches"""
        self.memory_cache.clear()
        # Note: We don't clear disk cache as it may be shared
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'memory_items': len(self.memory_cache.cache),
            'memory_bytes': self.memory_cache.current_bytes,
            'memory_max_items': self.memory_cache.max_items,
            'memory_max_bytes': self.memory_cache.max_bytes,
            'disk_dir': self.disk_cache.cache_dir,
            'active_locks': len(self.locks)
        }
