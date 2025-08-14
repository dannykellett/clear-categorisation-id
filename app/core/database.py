"""
Database connection management for PostgreSQL.

Provides connection pooling and database utilities for the application.
"""

import logging
from typing import Optional
import asyncpg
from asyncpg import Pool

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Global pool instance
_db_pool: Optional[Pool] = None


async def get_db_pool() -> Pool:
    """
    Get or create database connection pool.
    
    Returns:
        AsyncPG connection pool
    """
    global _db_pool
    
    if _db_pool is None:
        settings = get_settings()
        
        # Build database URL from settings
        database_url = (
            f"postgresql://{settings.database_user}:{settings.database_password}"
            f"@{settings.database_host}:{settings.database_port}/{settings.database_name}"
        )
        
        logger.info(f"Creating database pool for {settings.database_host}:{settings.database_port}")
        
        _db_pool = await asyncpg.create_pool(
            database_url,
            min_size=getattr(settings, 'database_pool_min_size', 5),
            max_size=getattr(settings, 'database_pool_max_size', 20),
            command_timeout=getattr(settings, 'database_command_timeout', 60),
            server_settings={
                'jit': 'off'  # Disable JIT for faster connection startup
            }
        )
        
        logger.info("Database pool created successfully")
    
    return _db_pool


async def close_db_pool() -> None:
    """Close the database connection pool."""
    global _db_pool
    
    if _db_pool is not None:
        logger.info("Closing database pool")
        await _db_pool.close()
        _db_pool = None
        logger.info("Database pool closed")


async def health_check_db() -> bool:
    """
    Perform database health check.
    
    Returns:
        True if database is healthy, False otherwise
    """
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
            return result == 1
            
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False