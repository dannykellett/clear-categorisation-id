import asyncio
import json
from typing import Optional, Any, Dict
from datetime import datetime, timedelta
import redis.asyncio as redis
from app.core.config import config
from app.core.logging import get_logger, log_with_context

logger = get_logger(__name__)


class CacheManager:
    """Manages caching for taxonomy data with Redis fallback to in-memory"""
    
    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self._memory_cache: Dict[str, Any] = {}
        self._use_redis = getattr(config, 'use_redis', True)
        self._redis_url = getattr(config, 'redis_url', 'redis://localhost:6379')
    
    async def initialize(self):
        """Initialize Redis connection if available"""
        if self._use_redis:
            try:
                self._redis_client = redis.from_url(
                    self._redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test connection
                await self._redis_client.ping()
                log_with_context(
                    logger, "info", "Redis cache initialized successfully",
                    redis_url=self._redis_url
                )
            except Exception as e:
                log_with_context(
                    logger, "warning", "Failed to connect to Redis, using in-memory cache",
                    error=str(e),
                    redis_url=self._redis_url
                )
                self._redis_client = None
        else:
            log_with_context(
                logger, "info", "Using in-memory cache (Redis disabled)"
            )
    
    async def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """
        Set a value in cache
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl_seconds: Time to live in seconds (None for no expiration)
            
        Returns:
            True if successfully cached
        """
        try:
            # Serialize value to JSON
            serialized_value = json.dumps(value, default=self._json_serializer)
            
            if self._redis_client:
                if ttl_seconds:
                    await self._redis_client.setex(key, ttl_seconds, serialized_value)
                else:
                    await self._redis_client.set(key, serialized_value)
                
                log_with_context(
                    logger, "debug", "Value cached in Redis",
                    key=key,
                    ttl_seconds=ttl_seconds
                )
            else:
                # Use in-memory cache
                cache_entry = {
                    "value": serialized_value,
                    "expires_at": datetime.utcnow() + timedelta(seconds=ttl_seconds) if ttl_seconds else None
                }
                self._memory_cache[key] = cache_entry
                
                log_with_context(
                    logger, "debug", "Value cached in memory",
                    key=key,
                    ttl_seconds=ttl_seconds
                )
            
            return True
            
        except Exception as e:
            log_with_context(
                logger, "error", "Failed to set cache value",
                key=key,
                error=str(e)
            )
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        try:
            if self._redis_client:
                value = await self._redis_client.get(key)
                if value:
                    return json.loads(value)
                return None
            else:
                # Use in-memory cache
                cache_entry = self._memory_cache.get(key)
                if cache_entry:
                    # Check expiration
                    if cache_entry["expires_at"] and datetime.utcnow() > cache_entry["expires_at"]:
                        del self._memory_cache[key]
                        return None
                    
                    return json.loads(cache_entry["value"])
                return None
                
        except Exception as e:
            log_with_context(
                logger, "error", "Failed to get cache value",
                key=key,
                error=str(e)
            )
            return None
    
    async def delete(self, key: str) -> bool:
        """
        Delete a value from cache
        
        Args:
            key: Cache key
            
        Returns:
            True if successfully deleted
        """
        try:
            if self._redis_client:
                await self._redis_client.delete(key)
            else:
                self._memory_cache.pop(key, None)
            
            log_with_context(
                logger, "debug", "Cache key deleted",
                key=key
            )
            return True
            
        except Exception as e:
            log_with_context(
                logger, "error", "Failed to delete cache key",
                key=key,
                error=str(e)
            )
            return False
    
    async def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching a pattern
        
        Args:
            pattern: Key pattern (e.g., "taxonomy:*")
            
        Returns:
            Number of keys deleted
        """
        try:
            if self._redis_client:
                keys = await self._redis_client.keys(pattern)
                if keys:
                    deleted = await self._redis_client.delete(*keys)
                    log_with_context(
                        logger, "info", "Cleared cache keys by pattern",
                        pattern=pattern,
                        deleted_count=deleted
                    )
                    return deleted
                return 0
            else:
                # Simple pattern matching for in-memory cache
                import fnmatch
                keys_to_delete = [k for k in self._memory_cache.keys() if fnmatch.fnmatch(k, pattern)]
                for key in keys_to_delete:
                    del self._memory_cache[key]
                
                log_with_context(
                    logger, "info", "Cleared cache keys by pattern",
                    pattern=pattern,
                    deleted_count=len(keys_to_delete)
                )
                return len(keys_to_delete)
                
        except Exception as e:
            log_with_context(
                logger, "error", "Failed to clear cache pattern",
                pattern=pattern,
                error=str(e)
            )
            return 0
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    async def close(self):
        """Close Redis connection"""
        if self._redis_client:
            await self._redis_client.aclose()


# Global instance
cache_manager = CacheManager()