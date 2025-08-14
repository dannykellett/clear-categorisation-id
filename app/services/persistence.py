"""
Database persistence layer for Kafka message processing.

Provides idempotent writes for article data and classification results
to support at-least-once delivery guarantees.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
import json

import asyncpg
from asyncpg import Pool

from app.schemas import ScrapedMessage, ClassifiedMessage
from app.core.config import get_settings
from app.core.database import get_db_pool

logger = logging.getLogger(__name__)


class KafkaPersistenceService:
    """
    Persistence service for Kafka message processing.
    
    Handles idempotent writes for articles and classifications
    to support at-least-once delivery semantics.
    """
    
    def __init__(self, db_pool: Optional[Pool] = None):
        self.db_pool = db_pool
        self.settings = get_settings()
        
        logger.info("Initialized Kafka persistence service")
    
    async def _get_pool(self) -> Pool:
        """Get database pool, creating if necessary."""
        if self.db_pool is None:
            self.db_pool = await get_db_pool()
        return self.db_pool
    
    async def persist_article(self, message: ScrapedMessage) -> bool:
        """
        Persist article data with idempotent writes by article_id.
        
        Args:
            message: ScrapedMessage containing article data
            
        Returns:
            True if record was inserted/updated, False if unchanged
            
        Raises:
            DatabaseError: If persistence fails
        """
        try:
            pool = await self._get_pool()
            
            # Prepare JSON attributes
            attrs_json = json.dumps(message.attrs) if message.attrs else None
            
            async with pool.acquire() as conn:
                # Use INSERT ... ON CONFLICT DO UPDATE for idempotent writes
                query = """
                    INSERT INTO articles (
                        article_id, source, ts, text, attrs, ingested_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6
                    )
                    ON CONFLICT (article_id) DO UPDATE SET
                        source = EXCLUDED.source,
                        ts = EXCLUDED.ts,
                        text = EXCLUDED.text,
                        attrs = EXCLUDED.attrs,
                        ingested_at = EXCLUDED.ingested_at
                    WHERE 
                        articles.ts < EXCLUDED.ts OR
                        articles.text != EXCLUDED.text OR
                        articles.attrs IS DISTINCT FROM EXCLUDED.attrs
                    RETURNING xmax
                """
                
                result = await conn.fetchrow(
                    query,
                    message.article_id,
                    message.source,
                    message.ts,
                    message.text,
                    attrs_json,
                    datetime.now(timezone.utc)
                )
                
                # xmax = 0 means INSERT, xmax > 0 means UPDATE
                was_updated = result['xmax'] != 0 if result else False
                
                logger.debug(
                    f"Persisted article {message.article_id}: "
                    f"{'updated' if was_updated else 'inserted'}"
                )
                
                return True
                
        except Exception as e:
            logger.error(
                f"Failed to persist article {message.article_id}: {e}",
                exc_info=True
            )
            raise DatabaseError(f"Article persistence failed: {e}") from e
    
    async def persist_classification(
        self,
        message: ClassifiedMessage,
        force_update: bool = False
    ) -> bool:
        """
        Persist classification results with idempotent writes.
        
        Args:
            message: ClassifiedMessage containing classification data
            force_update: If True, update even if newer record exists
            
        Returns:
            True if record was inserted/updated, False if unchanged
            
        Raises:
            DatabaseError: If persistence fails
        """
        try:
            pool = await self._get_pool()
            
            # Prepare JSON data
            classification_json = json.dumps([
                {
                    "tier_1": cat.tier_1,
                    "tier_2": cat.tier_2,
                    "tier_3": cat.tier_3,
                    "tier_4": cat.tier_4,
                    "confidence": cat.confidence,
                    "reasoning": cat.reasoning
                }
                for cat in message.classification
            ])
            
            usage_json = json.dumps(message.usage) if message.usage else None
            
            async with pool.acquire() as conn:
                if force_update:
                    # Force update - ignore timestamp checks
                    query = """
                        INSERT INTO classifications (
                            id, article_id, classification, provider, model, 
                            usage, taxonomy_version, processed_ts
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8
                        )
                        ON CONFLICT (article_id) DO UPDATE SET
                            id = EXCLUDED.id,
                            classification = EXCLUDED.classification,
                            provider = EXCLUDED.provider,
                            model = EXCLUDED.model,
                            usage = EXCLUDED.usage,
                            taxonomy_version = EXCLUDED.taxonomy_version,
                            processed_ts = EXCLUDED.processed_ts
                        RETURNING xmax
                    """
                else:
                    # Idempotent update - only update if newer or different
                    query = """
                        INSERT INTO classifications (
                            id, article_id, classification, provider, model, 
                            usage, taxonomy_version, processed_ts
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8
                        )
                        ON CONFLICT (article_id) DO UPDATE SET
                            id = EXCLUDED.id,
                            classification = EXCLUDED.classification,
                            provider = EXCLUDED.provider,
                            model = EXCLUDED.model,
                            usage = EXCLUDED.usage,
                            taxonomy_version = EXCLUDED.taxonomy_version,
                            processed_ts = EXCLUDED.processed_ts
                        WHERE 
                            classifications.processed_ts < EXCLUDED.processed_ts OR
                            classifications.classification::text != EXCLUDED.classification::text OR
                            classifications.taxonomy_version != EXCLUDED.taxonomy_version
                        RETURNING xmax
                    """
                
                result = await conn.fetchrow(
                    query,
                    message.id,
                    message.article_id,
                    classification_json,
                    message.provider,
                    message.model,
                    usage_json,
                    message.taxonomy_version,
                    message.processed_ts
                )
                
                was_updated = result['xmax'] != 0 if result else False
                
                logger.debug(
                    f"Persisted classification for article {message.article_id}: "
                    f"{'updated' if was_updated else 'inserted'}"
                )
                
                return True
                
        except Exception as e:
            logger.error(
                f"Failed to persist classification for article {message.article_id}: {e}",
                exc_info=True
            )
            raise DatabaseError(f"Classification persistence failed: {e}") from e
    
    async def persist_message_pair(
        self,
        scraped_message: ScrapedMessage,
        classified_message: ClassifiedMessage
    ) -> Tuple[bool, bool]:
        """
        Persist both article and classification data in a transaction.
        
        Args:
            scraped_message: Original scraped message
            classified_message: Classification result
            
        Returns:
            Tuple of (article_persisted, classification_persisted)
            
        Raises:
            DatabaseError: If persistence fails
        """
        try:
            pool = await self._get_pool()
            
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Persist article first
                    article_result = await self._persist_article_in_transaction(
                        conn, scraped_message
                    )
                    
                    # Then persist classification
                    classification_result = await self._persist_classification_in_transaction(
                        conn, classified_message
                    )
                    
                    logger.debug(
                        f"Persisted message pair for article {scraped_message.article_id}: "
                        f"article={'updated' if article_result else 'unchanged'}, "
                        f"classification={'updated' if classification_result else 'unchanged'}"
                    )
                    
                    return article_result, classification_result
                    
        except Exception as e:
            logger.error(
                f"Failed to persist message pair for article {scraped_message.article_id}: {e}",
                exc_info=True
            )
            raise DatabaseError(f"Message pair persistence failed: {e}") from e
    
    async def _persist_article_in_transaction(
        self, 
        conn: asyncpg.Connection, 
        message: ScrapedMessage
    ) -> bool:
        """Persist article within an existing transaction."""
        attrs_json = json.dumps(message.attrs) if message.attrs else None
        
        query = """
            INSERT INTO articles (
                article_id, source, ts, text, attrs, ingested_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ON CONFLICT (article_id) DO UPDATE SET
                source = EXCLUDED.source,
                ts = EXCLUDED.ts,
                text = EXCLUDED.text,
                attrs = EXCLUDED.attrs,
                ingested_at = EXCLUDED.ingested_at
            WHERE 
                articles.ts < EXCLUDED.ts OR
                articles.text != EXCLUDED.text OR
                articles.attrs IS DISTINCT FROM EXCLUDED.attrs
            RETURNING xmax
        """
        
        result = await conn.fetchrow(
            query,
            message.article_id,
            message.source,
            message.ts,
            message.text,
            attrs_json,
            datetime.now(timezone.utc)
        )
        
        return result['xmax'] != 0 if result else False
    
    async def _persist_classification_in_transaction(
        self,
        conn: asyncpg.Connection,
        message: ClassifiedMessage
    ) -> bool:
        """Persist classification within an existing transaction."""
        classification_json = json.dumps([
            {
                "tier_1": cat.tier_1,
                "tier_2": cat.tier_2,
                "tier_3": cat.tier_3,
                "tier_4": cat.tier_4,
                "confidence": cat.confidence,
                "reasoning": cat.reasoning
            }
            for cat in message.classification
        ])
        
        usage_json = json.dumps(message.usage) if message.usage else None
        
        query = """
            INSERT INTO classifications (
                id, article_id, classification, provider, model, 
                usage, taxonomy_version, processed_ts
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (article_id) DO UPDATE SET
                id = EXCLUDED.id,
                classification = EXCLUDED.classification,
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                usage = EXCLUDED.usage,
                taxonomy_version = EXCLUDED.taxonomy_version,
                processed_ts = EXCLUDED.processed_ts
            WHERE 
                classifications.processed_ts < EXCLUDED.processed_ts OR
                classifications.classification::text != EXCLUDED.classification::text OR
                classifications.taxonomy_version != EXCLUDED.taxonomy_version
            RETURNING xmax
        """
        
        result = await conn.fetchrow(
            query,
            message.id,
            message.article_id,
            classification_json,
            message.provider,
            message.model,
            usage_json,
            message.taxonomy_version,
            message.processed_ts
        )
        
        return result['xmax'] != 0 if result else False
    
    async def get_article_status(self, article_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of an article in the database.
        
        Args:
            article_id: Article identifier
            
        Returns:
            Dictionary with article status or None if not found
        """
        try:
            pool = await self._get_pool()
            
            async with pool.acquire() as conn:
                query = """
                    SELECT 
                        a.article_id,
                        a.source,
                        a.ts,
                        a.ingested_at,
                        CASE WHEN c.article_id IS NOT NULL THEN true ELSE false END as has_classification,
                        c.taxonomy_version,
                        c.processed_ts
                    FROM articles a
                    LEFT JOIN classifications c ON a.article_id = c.article_id
                    WHERE a.article_id = $1
                """
                
                result = await conn.fetchrow(query, article_id)
                
                if result:
                    return {
                        'article_id': result['article_id'],
                        'source': result['source'],
                        'ts': result['ts'],
                        'ingested_at': result['ingested_at'],
                        'has_classification': result['has_classification'],
                        'taxonomy_version': result['taxonomy_version'],
                        'processed_ts': result['processed_ts']
                    }
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get article status for {article_id}: {e}")
            raise DatabaseError(f"Article status query failed: {e}") from e
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on persistence service.
        
        Returns:
            Health check results
        """
        health = {
            'status': 'healthy',
            'database_connected': False,
            'pool_ready': False
        }
        
        try:
            pool = await self._get_pool()
            
            async with pool.acquire() as conn:
                # Simple connectivity test
                result = await conn.fetchval("SELECT 1")
                
                health['database_connected'] = result == 1
                health['pool_ready'] = True
                
                # Check table existence
                tables_query = """
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name IN ('articles', 'classifications')
                """
                table_count = await conn.fetchval(tables_query)
                health['required_tables_exist'] = table_count == 2
                
                if not health['required_tables_exist']:
                    health['status'] = 'degraded'
                    
        except Exception as e:
            logger.error(f"Persistence service health check failed: {e}")
            health['status'] = 'unhealthy'
            health['error'] = str(e)
        
        return health


class DatabaseError(Exception):
    """Raised when database operations fail."""
    pass


# Global instance
def get_persistence_service(db_pool: Optional[Pool] = None) -> KafkaPersistenceService:
    """Get persistence service instance."""
    return KafkaPersistenceService(db_pool=db_pool)