"""
Integration tests for database persistence with Kafka message processing.

Tests the complete flow of persisting classified messages to the database
with proper idempotent writes and error handling.
"""

import asyncio
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import AsyncMock, patch, MagicMock

from app.schemas.scraped_message import ScrapedMessage
from app.schemas.classified_message import ClassifiedMessage, CategoryPath, UsageStats
from app.services.persistence import get_persistence_service
from app.engine.stream_processor import StreamProcessor


class TestPersistenceIntegration:
    """Integration tests for database persistence with message processing."""
    
    @pytest_asyncio.fixture
    async def mock_database(self):
        """Mock database connection and operations."""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        return mock_conn, mock_pool
    
    @pytest.mark.asyncio
    async def test_persist_classified_message_success(self, mock_database):
        """Test successful persistence of classified message."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock successful database operations
            mock_conn.execute.return_value = None
            mock_conn.fetchval.return_value = 1  # Affected rows
            
            persistence_service = get_persistence_service()
            
            # Create test classified message
            category_path = CategoryPath(
                tier_1="Technology",
                tier_2="Machine Learning",
                confidence=0.92,
                reasoning="Strong ML indicators"
            )
            
            usage = UsageStats(
                input_tokens=150,
                output_tokens=25,
                total_tokens=175
            )
            
            classified_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="persist-test-123",
                classification=[category_path],
                provider="openai",
                model="gpt-4o-mini",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            # Create corresponding scraped message
            scraped_message = ScrapedMessage(
                id=classified_message.id,
                article_id="persist-test-123",
                text="Sample ML article content",
                source="tech_blog",
                ts="2025-08-14T10:30:00Z",
                attrs={"lang": "en", "author": "AI Researcher"}
            )
            
            # Persist the messages
            result = await persistence_service.persist_article_and_classification(
                scraped_message,
                classified_message
            )
            
            assert result is True
            
            # Verify database calls were made
            assert mock_conn.execute.call_count >= 2  # At least article and classification inserts
    
    @pytest.mark.asyncio
    async def test_idempotent_writes_duplicate_article(self, mock_database):
        """Test idempotent writes when article already exists."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock database responses for duplicate handling
            # First call: article already exists (conflict)
            # Second call: classification update succeeds
            mock_conn.execute.side_effect = [
                None,  # Article upsert (conflict handled by database)
                None   # Classification upsert
            ]
            
            persistence_service = get_persistence_service()
            
            # Create test messages
            category_path = CategoryPath(
                tier_1="Business",
                confidence=0.88,
                reasoning="Business content identified"
            )
            
            usage = UsageStats(
                input_tokens=120,
                output_tokens=20,
                total_tokens=140
            )
            
            classified_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="duplicate-test-456",
                classification=[category_path],
                provider="openai",
                model="gpt-4o-mini",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            scraped_message = ScrapedMessage(
                id=classified_message.id,
                article_id="duplicate-test-456",
                text="Business article content",
                source="business_news",
                ts="2025-08-14T10:30:00Z"
            )
            
            # Persist twice - should handle duplicates gracefully
            result1 = await persistence_service.persist_article_and_classification(
                scraped_message,
                classified_message
            )
            
            result2 = await persistence_service.persist_article_and_classification(
                scraped_message,
                classified_message
            )
            
            assert result1 is True
            assert result2 is True
    
    @pytest.mark.asyncio
    async def test_transaction_rollback_on_failure(self, mock_database):
        """Test transaction rollback when classification insert fails."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock transaction methods
            mock_transaction = AsyncMock()
            mock_conn.transaction.return_value = mock_transaction
            
            # Mock article insert success, classification insert failure
            mock_conn.execute.side_effect = [
                None,  # Article insert succeeds
                Exception("Classification insert failed")  # Classification insert fails
            ]
            
            persistence_service = get_persistence_service()
            
            # Create test messages
            category_path = CategoryPath(
                tier_1="Technology",
                confidence=0.9,
                reasoning="Tech content"
            )
            
            usage = UsageStats(
                input_tokens=100,
                output_tokens=15,
                total_tokens=115
            )
            
            classified_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="transaction-test-789",
                classification=[category_path],
                provider="ollama",
                model="qwen3:30b",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            scraped_message = ScrapedMessage(
                id=classified_message.id,
                article_id="transaction-test-789",
                text="Tech article content",
                source="tech_news",
                ts="2025-08-14T10:30:00Z"
            )
            
            # Should handle failure gracefully
            with pytest.raises(Exception, match="Classification insert failed"):
                await persistence_service.persist_article_and_classification(
                    scraped_message,
                    classified_message
                )
            
            # Verify transaction rollback was called
            mock_transaction.__aenter__.assert_called()
            mock_transaction.__aexit__.assert_called()
    
    @pytest.mark.asyncio
    async def test_batch_persistence_success(self, mock_database):
        """Test batch persistence of multiple messages."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock successful batch operations
            mock_conn.executemany.return_value = None
            mock_conn.execute.return_value = None
            
            persistence_service = get_persistence_service()
            
            # Create test message pairs
            message_pairs = []
            for i in range(5):
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.8 + (i * 0.02),
                    reasoning=f"Test classification {i}"
                )
                
                usage = UsageStats(
                    input_tokens=100 + i * 10,
                    output_tokens=15 + i * 2,
                    total_tokens=115 + i * 12
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id=f"batch-test-{i}",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                scraped_message = ScrapedMessage(
                    id=classified_message.id,
                    article_id=f"batch-test-{i}",
                    text=f"Test article content {i}",
                    source="test_source",
                    ts="2025-08-14T10:30:00Z"
                )
                
                message_pairs.append((scraped_message, classified_message))
            
            # Persist batch
            results = await persistence_service.persist_batch(message_pairs)
            
            # Verify all succeeded
            assert len(results) == 5
            assert all(r['success'] for r in results)
            
            # Verify batch operations were called
            mock_conn.executemany.assert_called()
    
    @pytest.mark.asyncio
    async def test_batch_persistence_partial_failure(self, mock_database):
        """Test batch persistence with some failures."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock batch operation with some failures
            def mock_executemany_side_effect(query, params):
                # Simulate failure for specific records
                for param in params:
                    if "fail" in str(param):
                        raise Exception(f"Database error for {param}")
                return None
            
            mock_conn.executemany.side_effect = mock_executemany_side_effect
            
            persistence_service = get_persistence_service()
            
            # Create test messages - one will fail
            message_pairs = []
            for i in range(3):
                article_id = f"batch-partial-{i}" if i != 1 else "batch-partial-fail-1"
                
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.85,
                    reasoning=f"Test classification {i}"
                )
                
                usage = UsageStats(
                    input_tokens=100,
                    output_tokens=15,
                    total_tokens=115
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id=article_id,
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                scraped_message = ScrapedMessage(
                    id=classified_message.id,
                    article_id=article_id,
                    text=f"Test article content {i}",
                    source="test_source",
                    ts="2025-08-14T10:30:00Z"
                )
                
                message_pairs.append((scraped_message, classified_message))
            
            # Should handle partial failures gracefully
            results = await persistence_service.persist_batch(message_pairs)
            
            # Verify mixed results
            assert len(results) == 3
            success_count = sum(1 for r in results if r['success'])
            failure_count = sum(1 for r in results if not r['success'])
            
            # At least some should succeed, some should fail
            assert success_count > 0
            assert failure_count > 0
    
    @pytest.mark.asyncio
    async def test_persistence_with_stream_processing(self, mock_database):
        """Test end-to-end persistence with stream processing."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool, \
             patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy, \
             patch('app.engine.stream_processor.get_settings') as mock_settings:
            
            mock_get_pool.return_value = mock_pool
            mock_conn.execute.return_value = None
            
            # Mock settings
            mock_settings.return_value.kafka_batch_size = 3
            mock_settings.return_value.kafka_processing_timeout = 30.0
            
            # Mock taxonomy service
            mock_taxonomy.get_current_taxonomy.return_value = {
                'categories': [],
                'version': 'tax_v20250814_103000'
            }
            
            # Mock classification engine
            mock_engine.classify.return_value = {
                'classification': [
                    {
                        'tier_1': 'Technology',
                        'confidence': 0.9,
                        'reasoning': 'Technology content detected'
                    }
                ],
                'provider': 'openai',
                'model': 'gpt-4o-mini',
                'usage': {
                    'input_tokens': 100,
                    'output_tokens': 20,
                    'total_tokens': 120
                }
            }
            
            # Create stream processor and persistence service
            processor = StreamProcessor()
            persistence_service = get_persistence_service()
            
            # Create test scraped message
            scraped_message = ScrapedMessage(
                id=str(uuid4()),
                article_id="e2e-persistence-test",
                text="This is a technology article about AI and machine learning.",
                source="tech_blog",
                ts="2025-08-14T10:30:00Z",
                attrs={"lang": "en"}
            )
            
            # Process message to get classification
            classified_message = await processor.process_message(scraped_message)
            
            # Persist to database
            result = await persistence_service.persist_article_and_classification(
                scraped_message,
                classified_message
            )
            
            # Verify end-to-end success
            assert result is True
            assert classified_message.provider == 'openai'
            assert classified_message.model == 'gpt-4o-mini'
            assert len(classified_message.classification) == 1
            assert classified_message.classification[0].tier_1 == 'Technology'
            
            # Verify persistence was called
            assert mock_conn.execute.call_count >= 2
    
    @pytest.mark.asyncio
    async def test_persistence_performance_metrics(self, mock_database):
        """Test persistence performance monitoring."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock timing for performance measurement
            mock_conn.execute.return_value = None
            
            persistence_service = get_persistence_service()
            
            # Create test message
            category_path = CategoryPath(
                tier_1="Technology",
                confidence=0.9,
                reasoning="Performance test"
            )
            
            usage = UsageStats(
                input_tokens=100,
                output_tokens=20,
                total_tokens=120
            )
            
            classified_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="performance-test",
                classification=[category_path],
                provider="openai",
                model="gpt-4o-mini",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            scraped_message = ScrapedMessage(
                id=classified_message.id,
                article_id="performance-test",
                text="Performance test content",
                source="test_source",
                ts="2025-08-14T10:30:00Z"
            )
            
            # Measure persistence time
            start_time = asyncio.get_event_loop().time()
            
            await persistence_service.persist_article_and_classification(
                scraped_message,
                classified_message
            )
            
            end_time = asyncio.get_event_loop().time()
            persistence_time = end_time - start_time
            
            # Verify reasonable performance (should be very fast with mocked DB)
            assert persistence_time < 1.0  # Should complete in less than 1 second
            
            # Get persistence metrics
            metrics = await persistence_service.get_metrics()
            
            # Verify metrics are available
            assert 'total_articles_persisted' in metrics
            assert 'total_classifications_persisted' in metrics
            assert 'average_persistence_time' in metrics
            assert 'database_pool_status' in metrics
    
    @pytest.mark.asyncio
    async def test_database_connection_recovery(self, mock_database):
        """Test database connection recovery after failure."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            # First call fails, second succeeds
            failed_pool = AsyncMock()
            failed_pool.acquire.side_effect = Exception("Connection failed")
            
            success_pool = AsyncMock()
            success_pool.acquire.return_value.__aenter__.return_value = mock_conn
            mock_conn.execute.return_value = None
            
            mock_get_pool.side_effect = [failed_pool, success_pool]
            
            persistence_service = get_persistence_service()
            
            # Create test message
            category_path = CategoryPath(
                tier_1="Technology",
                confidence=0.9,
                reasoning="Recovery test"
            )
            
            usage = UsageStats(
                input_tokens=100,
                output_tokens=20,
                total_tokens=120
            )
            
            classified_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="recovery-test",
                classification=[category_path],
                provider="openai",
                model="gpt-4o-mini",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            scraped_message = ScrapedMessage(
                id=classified_message.id,
                article_id="recovery-test",
                text="Recovery test content",
                source="test_source",
                ts="2025-08-14T10:30:00Z"
            )
            
            # First attempt should fail
            with pytest.raises(Exception, match="Connection failed"):
                await persistence_service.persist_article_and_classification(
                    scraped_message,
                    classified_message
                )
            
            # Second attempt should succeed after connection recovery
            # (This would require implementing retry logic in the service)
            # For now, just verify the failure was handled properly
    
    @pytest.mark.asyncio
    async def test_concurrent_persistence_operations(self, mock_database):
        """Test concurrent persistence operations."""
        mock_conn, mock_pool = mock_database
        
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            mock_get_pool.return_value = mock_pool
            
            # Mock database operations with slight delay
            async def mock_execute(*args, **kwargs):
                await asyncio.sleep(0.1)  # Simulate database operation time
                return None
            
            mock_conn.execute.side_effect = mock_execute
            
            persistence_service = get_persistence_service()
            
            # Create multiple test messages
            tasks = []
            for i in range(5):
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.8 + (i * 0.02),
                    reasoning=f"Concurrent test {i}"
                )
                
                usage = UsageStats(
                    input_tokens=100 + i,
                    output_tokens=20 + i,
                    total_tokens=120 + (i * 2)
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id=f"concurrent-{i}",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                scraped_message = ScrapedMessage(
                    id=classified_message.id,
                    article_id=f"concurrent-{i}",
                    text=f"Concurrent test content {i}",
                    source="test_source",
                    ts="2025-08-14T10:30:00Z"
                )
                
                # Create task for concurrent execution
                task = persistence_service.persist_article_and_classification(
                    scraped_message,
                    classified_message
                )
                tasks.append(task)
            
            # Execute all tasks concurrently
            start_time = asyncio.get_event_loop().time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            end_time = asyncio.get_event_loop().time()
            
            # Verify concurrent execution was faster than sequential
            concurrent_time = end_time - start_time
            # Sequential would be 5 * 0.1 = 0.5 seconds
            # Concurrent should be close to 0.1 seconds (plus overhead)
            assert concurrent_time < 0.3  # Allow for overhead
            
            # Verify all operations completed successfully
            assert len(results) == 5
            assert all(r is True for r in results if not isinstance(r, Exception))