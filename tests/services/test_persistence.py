"""
Tests for Kafka persistence service functionality.
"""

import pytest
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.persistence import KafkaPersistenceService, DatabaseError
from app.schemas import ScrapedMessage, ClassifiedMessage, CategoryPath


class TestKafkaPersistenceService:
    """Tests for KafkaPersistenceService class."""
    
    def test_init(self):
        """Test service initialization."""
        mock_pool = MagicMock()
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        assert service.db_pool == mock_pool
    
    def test_init_without_pool(self):
        """Test service initialization without pool."""
        service = KafkaPersistenceService()
        
        assert service.db_pool is None
    
    @pytest.mark.asyncio
    async def test_persist_article_success(self):
        """Test successful article persistence."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock successful insert (xmax = 0)
        mock_conn.fetchrow.return_value = {'xmax': 0}
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        message = ScrapedMessage(
            id="test-123",
            article_id="article-456",
            text="Test content",
            source="test-source",
            ts=datetime.now(timezone.utc),
            attrs={"lang": "en", "author": "test"}
        )
        
        result = await service.persist_article(message)
        
        assert result is True
        mock_conn.fetchrow.assert_called_once()
        
        # Verify query parameters
        call_args = mock_conn.fetchrow.call_args
        assert call_args[0][1] == "article-456"  # article_id
        assert call_args[0][2] == "test-source"  # source
        assert call_args[0][4] == "Test content"  # text
        
        # Verify JSON serialization of attrs
        attrs_json = call_args[0][5]
        parsed_attrs = json.loads(attrs_json)
        assert parsed_attrs == {"lang": "en", "author": "test"}
    
    @pytest.mark.asyncio
    async def test_persist_article_update(self):
        """Test article persistence with update."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock update (xmax > 0)
        mock_conn.fetchrow.return_value = {'xmax': 123}
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        message = ScrapedMessage(
            id="test-123",
            article_id="article-456",
            text="Updated content",
            source="test-source",
            ts=datetime.now(timezone.utc)
        )
        
        result = await service.persist_article(message)
        
        assert result is True
        mock_conn.fetchrow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_persist_article_failure(self):
        """Test article persistence failure."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        mock_conn.fetchrow.side_effect = Exception("Database error")
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        message = ScrapedMessage(
            id="test-123",
            article_id="article-456",
            text="Test content",
            source="test-source",
            ts=datetime.now(timezone.utc)
        )
        
        with pytest.raises(DatabaseError, match="Article persistence failed"):
            await service.persist_article(message)
    
    @pytest.mark.asyncio
    async def test_persist_classification_success(self):
        """Test successful classification persistence."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock successful insert
        mock_conn.fetchrow.return_value = {'xmax': 0}
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        categories = [
            CategoryPath(
                tier_1="technology",
                tier_2="software",
                tier_3="web-development",
                tier_4=None,
                confidence=0.85,
                reasoning="Contains web dev terms"
            )
        ]
        
        message = ClassifiedMessage(
            id="test-123",
            article_id="article-456",
            classification=categories,
            provider="openai",
            model="gpt-4",
            usage={"input_tokens": 50, "output_tokens": 30, "total_tokens": 80},
            taxonomy_version="tax_v20250814_123456",
            processed_ts=datetime.now(timezone.utc)
        )
        
        result = await service.persist_classification(message)
        
        assert result is True
        mock_conn.fetchrow.assert_called_once()
        
        # Verify query parameters
        call_args = mock_conn.fetchrow.call_args
        assert call_args[0][1] == "test-123"  # id
        assert call_args[0][2] == "article-456"  # article_id
        assert call_args[0][4] == "openai"  # provider
        assert call_args[0][5] == "gpt-4"  # model
        
        # Verify JSON serialization of classification
        classification_json = call_args[0][3]
        parsed_classification = json.loads(classification_json)
        assert len(parsed_classification) == 1
        assert parsed_classification[0]["tier_1"] == "technology"
        assert parsed_classification[0]["tier_2"] == "software"
        assert parsed_classification[0]["confidence"] == 0.85
    
    @pytest.mark.asyncio
    async def test_persist_classification_force_update(self):
        """Test classification persistence with force update."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        mock_conn.fetchrow.return_value = {'xmax': 456}
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        categories = [
            CategoryPath(
                tier_1="general",
                tier_2=None,
                tier_3=None,
                tier_4=None,
                confidence=0.6,
                reasoning="Generic content"
            )
        ]
        
        message = ClassifiedMessage(
            id="test-123",
            article_id="article-456",
            classification=categories,
            provider="openai",
            model="gpt-4",
            usage={"input_tokens": 20, "output_tokens": 15, "total_tokens": 35},
            taxonomy_version="tax_v20250814_123456",
            processed_ts=datetime.now(timezone.utc)
        )
        
        result = await service.persist_classification(message, force_update=True)
        
        assert result is True
        mock_conn.fetchrow.assert_called_once()
        
        # Verify force update query doesn't have timestamp conditions
        call_args = mock_conn.fetchrow.call_args
        query = call_args[0][0]
        assert "WHERE" not in query or "processed_ts" not in query
    
    @pytest.mark.asyncio
    async def test_persist_message_pair_success(self):
        """Test successful message pair persistence."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_transaction = AsyncMock()
        mock_conn.transaction.return_value.__aenter__.return_value = mock_transaction
        
        # Mock both operations returning success
        mock_conn.fetchrow.side_effect = [
            {'xmax': 0},  # Article insert
            {'xmax': 0}   # Classification insert
        ]
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        scraped_message = ScrapedMessage(
            id="test-123",
            article_id="article-456",
            text="Test content",
            source="test-source",
            ts=datetime.now(timezone.utc)
        )
        
        categories = [
            CategoryPath(
                tier_1="technology",
                tier_2=None,
                tier_3=None,
                tier_4=None,
                confidence=0.75,
                reasoning="Tech content"
            )
        ]
        
        classified_message = ClassifiedMessage(
            id="test-123",
            article_id="article-456",
            classification=categories,
            provider="openai",
            model="gpt-4",
            usage={"input_tokens": 30, "output_tokens": 20, "total_tokens": 50},
            taxonomy_version="tax_v20250814_123456",
            processed_ts=datetime.now(timezone.utc)
        )
        
        article_result, classification_result = await service.persist_message_pair(
            scraped_message, classified_message
        )
        
        assert article_result is True
        assert classification_result is True
        
        # Verify transaction was used
        mock_conn.transaction.assert_called_once()
        
        # Verify both queries were called
        assert mock_conn.fetchrow.call_count == 2
    
    @pytest.mark.asyncio
    async def test_persist_message_pair_failure(self):
        """Test message pair persistence failure."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_transaction = AsyncMock()
        mock_conn.transaction.return_value.__aenter__.return_value = mock_transaction
        
        # Mock article insert failure
        mock_conn.fetchrow.side_effect = Exception("Transaction failed")
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        scraped_message = ScrapedMessage(
            id="test-123",
            article_id="article-456",
            text="Test content",
            source="test-source",
            ts=datetime.now(timezone.utc)
        )
        
        categories = [CategoryPath(
            tier_1="general",
            tier_2=None,
            tier_3=None,
            tier_4=None,
            confidence=0.6,
            reasoning="Generic"
        )]
        
        classified_message = ClassifiedMessage(
            id="test-123",
            article_id="article-456",
            classification=categories,
            provider="openai",
            model="gpt-4",
            usage={"input_tokens": 20, "output_tokens": 10, "total_tokens": 30},
            taxonomy_version="tax_v20250814_123456",
            processed_ts=datetime.now(timezone.utc)
        )
        
        with pytest.raises(DatabaseError, match="Message pair persistence failed"):
            await service.persist_message_pair(scraped_message, classified_message)
    
    @pytest.mark.asyncio
    async def test_get_article_status_found(self):
        """Test getting article status when found."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        mock_result = {
            'article_id': 'article-456',
            'source': 'test-source',
            'ts': datetime.now(timezone.utc),
            'ingested_at': datetime.now(timezone.utc),
            'has_classification': True,
            'taxonomy_version': 'tax_v20250814_123456',
            'processed_ts': datetime.now(timezone.utc)
        }
        mock_conn.fetchrow.return_value = mock_result
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        result = await service.get_article_status("article-456")
        
        assert result is not None
        assert result['article_id'] == 'article-456'
        assert result['source'] == 'test-source'
        assert result['has_classification'] is True
        assert result['taxonomy_version'] == 'tax_v20250814_123456'
    
    @pytest.mark.asyncio
    async def test_get_article_status_not_found(self):
        """Test getting article status when not found."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        mock_conn.fetchrow.return_value = None
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        result = await service.get_article_status("nonexistent-article")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_article_status_failure(self):
        """Test getting article status with database error."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        mock_conn.fetchrow.side_effect = Exception("Database error")
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        with pytest.raises(DatabaseError, match="Article status query failed"):
            await service.get_article_status("article-456")
    
    @pytest.mark.asyncio
    async def test_health_check_healthy(self):
        """Test health check with healthy database."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock connectivity test
        mock_conn.fetchval.side_effect = [1, 2]  # connectivity test, table count
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        health = await service.health_check()
        
        assert health['status'] == 'healthy'
        assert health['database_connected'] is True
        assert health['pool_ready'] is True
        assert health['required_tables_exist'] is True
    
    @pytest.mark.asyncio
    async def test_health_check_missing_tables(self):
        """Test health check with missing tables."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock connectivity test passing, but missing tables
        mock_conn.fetchval.side_effect = [1, 1]  # connectivity test, partial table count
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        health = await service.health_check()
        
        assert health['status'] == 'degraded'
        assert health['database_connected'] is True
        assert health['required_tables_exist'] is False
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self):
        """Test health check with database failure."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        mock_conn.fetchval.side_effect = Exception("Connection failed")
        
        service = KafkaPersistenceService(db_pool=mock_pool)
        
        health = await service.health_check()
        
        assert health['status'] == 'unhealthy'
        assert health['database_connected'] is False
        assert 'error' in health
    
    @pytest.mark.asyncio
    async def test_get_pool_creation(self):
        """Test database pool creation when not provided."""
        with patch('app.services.persistence.get_db_pool') as mock_get_pool:
            mock_pool = MagicMock()
            mock_get_pool.return_value = mock_pool
            
            service = KafkaPersistenceService()
            
            pool = await service._get_pool()
            
            assert pool == mock_pool
            assert service.db_pool == mock_pool
            mock_get_pool.assert_called_once()


def test_get_persistence_service():
    """Test persistence service factory function."""
    from app.services.persistence import get_persistence_service
    
    # Test without pool
    service1 = get_persistence_service()
    assert isinstance(service1, KafkaPersistenceService)
    assert service1.db_pool is None
    
    # Test with pool
    mock_pool = MagicMock()
    service2 = get_persistence_service(db_pool=mock_pool)
    assert isinstance(service2, KafkaPersistenceService)
    assert service2.db_pool == mock_pool


class TestDatabaseError:
    """Tests for DatabaseError exception."""
    
    def test_database_error(self):
        """Test DatabaseError creation."""
        error = DatabaseError("Test database error")
        assert str(error) == "Test database error"
        
        # Test with chained exception
        original_error = Exception("Original error")
        try:
            raise DatabaseError("Wrapper error") from original_error
        except DatabaseError as chained_error:
            assert str(chained_error) == "Wrapper error"
            assert chained_error.__cause__ == original_error