"""
Tests for stream processor functionality.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.engine.stream_processor import StreamProcessor, ClassificationError
from app.schemas import ScrapedMessage, ClassifiedMessage, CategoryPath


class TestStreamProcessor:
    """Tests for StreamProcessor class."""
    
    def test_init(self):
        """Test processor initialization."""
        processor = StreamProcessor()
        
        assert processor.batch_size == 10
        assert processor.processing_timeout == 30.0
    
    @pytest.mark.asyncio
    async def test_process_message_success(self):
        """Test successful message processing."""
        with patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            
            # Mock classification engine response
            mock_categories = [
                MagicMock(
                    path="technology/software/web-development",
                    confidence=0.85,
                    reasoning="Contains web development terms"
                )
            ]
            mock_usage = MagicMock(
                provider="openai",
                model="gpt-4",
                input_tokens=50,
                output_tokens=30
            )
            mock_engine.classify_text.return_value = (mock_categories, mock_usage)
            
            # Mock taxonomy service
            mock_version = MagicMock(version_string="tax_v20250814_123456")
            mock_taxonomy.get_current_version.return_value = mock_version
            
            # Create test message
            scraped_message = ScrapedMessage(
                id="test-123",
                article_id="article-456",
                text="This is about web development and APIs",
                source="test-source",
                ts=datetime.now(timezone.utc),
                attrs={"lang": "en"}
            )
            
            processor = StreamProcessor()
            result = await processor.process_message(scraped_message)
            
            # Verify result
            assert isinstance(result, ClassifiedMessage)
            assert result.id == "test-123"
            assert result.article_id == "article-456"
            assert result.provider == "openai"
            assert result.model == "gpt-4"
            assert result.taxonomy_version == "tax_v20250814_123456"
            assert len(result.classification) == 1
            
            classification = result.classification[0]
            assert classification.tier_1 == "technology"
            assert classification.tier_2 == "software"
            assert classification.tier_3 == "web-development"
            assert classification.tier_4 is None
            assert classification.confidence == 0.85
            assert classification.reasoning == "Contains web development terms"
            
            # Verify usage stats
            assert result.usage["input_tokens"] == 50
            assert result.usage["output_tokens"] == 30
            assert result.usage["total_tokens"] == 80
    
    @pytest.mark.asyncio
    async def test_process_message_classification_failure(self):
        """Test message processing with classification failure."""
        with patch('app.engine.stream_processor.classification_engine') as mock_engine:
            mock_engine.classify_text.side_effect = Exception("Classification failed")
            
            scraped_message = ScrapedMessage(
                id="test-123",
                article_id="article-456",
                text="Test text",
                source="test-source",
                ts=datetime.now(timezone.utc)
            )
            
            processor = StreamProcessor()
            
            with pytest.raises(ClassificationError, match="Classification failed for article article-456"):
                await processor.process_message(scraped_message)
    
    @pytest.mark.asyncio
    async def test_process_batch_success(self):
        """Test successful batch processing."""
        with patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            
            # Mock responses
            mock_categories = [MagicMock(
                path="general/uncategorized",
                confidence=0.6,
                reasoning="Generic content"
            )]
            mock_usage = MagicMock(
                provider="openai",
                model="gpt-4",
                input_tokens=20,
                output_tokens=15
            )
            mock_engine.classify_text.return_value = (mock_categories, mock_usage)
            
            mock_version = MagicMock(version_string="tax_v20250814_123456")
            mock_taxonomy.get_current_version.return_value = mock_version
            
            # Create test messages
            messages = [
                ScrapedMessage(
                    id=f"test-{i}",
                    article_id=f"article-{i}",
                    text=f"Test text {i}",
                    source="test-source",
                    ts=datetime.now(timezone.utc)
                )
                for i in range(3)
            ]
            
            processor = StreamProcessor()
            successful_results, failed_messages = await processor.process_batch(messages)
            
            # Verify results
            assert len(successful_results) == 3
            assert len(failed_messages) == 0
            
            for i, result in enumerate(successful_results):
                assert result.id == f"test-{i}"
                assert result.article_id == f"article-{i}"
                assert len(result.classification) == 1
    
    @pytest.mark.asyncio
    async def test_process_batch_partial_failure(self):
        """Test batch processing with some failures."""
        with patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            
            # Mock responses - fail on second message
            def classify_side_effect(text, provider=None, model=None):
                if "fail" in text:
                    raise Exception("Classification failed")
                
                mock_categories = [MagicMock(
                    path="general/uncategorized",
                    confidence=0.6,
                    reasoning="Generic content"
                )]
                mock_usage = MagicMock(
                    provider="openai",
                    model="gpt-4",
                    input_tokens=20,
                    output_tokens=15
                )
                return mock_categories, mock_usage
            
            mock_engine.classify_text.side_effect = classify_side_effect
            
            mock_version = MagicMock(version_string="tax_v20250814_123456")
            mock_taxonomy.get_current_version.return_value = mock_version
            
            # Create test messages - one will fail
            messages = [
                ScrapedMessage(
                    id="test-1",
                    article_id="article-1",
                    text="Test text 1",
                    source="test-source",
                    ts=datetime.now(timezone.utc)
                ),
                ScrapedMessage(
                    id="test-2",
                    article_id="article-2",
                    text="This should fail",
                    source="test-source",
                    ts=datetime.now(timezone.utc)
                ),
                ScrapedMessage(
                    id="test-3",
                    article_id="article-3",
                    text="Test text 3",
                    source="test-source",
                    ts=datetime.now(timezone.utc)
                )
            ]
            
            processor = StreamProcessor()
            successful_results, failed_messages = await processor.process_batch(messages)
            
            # Verify results
            assert len(successful_results) == 2
            assert len(failed_messages) == 1
            
            # Check successful results
            successful_ids = [r.id for r in successful_results]
            assert "test-1" in successful_ids
            assert "test-3" in successful_ids
            
            # Check failed message
            failed_message, error = failed_messages[0]
            assert failed_message.id == "test-2"
            assert "Classification failed" in str(error)
    
    @pytest.mark.asyncio
    async def test_process_with_retry_success(self):
        """Test retry logic with eventual success."""
        with patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            
            # Mock to fail first two attempts, succeed on third
            call_count = 0
            def classify_side_effect(text, provider=None, model=None):
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise Exception("Temporary failure")
                
                mock_categories = [MagicMock(
                    path="general/uncategorized",
                    confidence=0.6,
                    reasoning="Generic content"
                )]
                mock_usage = MagicMock(
                    provider="openai",
                    model="gpt-4",
                    input_tokens=20,
                    output_tokens=15
                )
                return mock_categories, mock_usage
            
            mock_engine.classify_text.side_effect = classify_side_effect
            
            mock_version = MagicMock(version_string="tax_v20250814_123456")
            mock_taxonomy.get_current_version.return_value = mock_version
            
            scraped_message = ScrapedMessage(
                id="test-123",
                article_id="article-456",
                text="Test text",
                source="test-source",
                ts=datetime.now(timezone.utc)
            )
            
            processor = StreamProcessor()
            result = await processor.process_with_retry(
                scraped_message,
                max_retries=3,
                backoff_factor=0.1  # Small delay for testing
            )
            
            # Should succeed after retries
            assert result is not None
            assert result.id == "test-123"
            assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_process_with_retry_failure(self):
        """Test retry logic with all attempts failing."""
        with patch('app.engine.stream_processor.classification_engine') as mock_engine:
            mock_engine.classify_text.side_effect = Exception("Persistent failure")
            
            scraped_message = ScrapedMessage(
                id="test-123",
                article_id="article-456",
                text="Test text",
                source="test-source",
                ts=datetime.now(timezone.utc)
            )
            
            processor = StreamProcessor()
            result = await processor.process_with_retry(
                scraped_message,
                max_retries=2,
                backoff_factor=0.1  # Small delay for testing
            )
            
            # Should fail after all retries
            assert result is None
    
    def test_get_metrics(self):
        """Test metrics collection."""
        processor = StreamProcessor()
        metrics = processor.get_metrics()
        
        assert 'batch_size' in metrics
        assert 'processing_timeout' in metrics
        assert 'classification_engine_ready' in metrics
        assert 'taxonomy_service_ready' in metrics
        
        assert metrics['batch_size'] == 10
        assert metrics['processing_timeout'] == 30.0
    
    @pytest.mark.asyncio
    async def test_health_check_healthy(self):
        """Test health check with healthy services."""
        with patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            mock_version = MagicMock(version_string="tax_v20250814_123456")
            mock_taxonomy.get_current_version.return_value = mock_version
            
            processor = StreamProcessor()
            health = await processor.health_check()
            
            assert health['status'] == 'healthy'
            assert health['processor_ready'] is True
            assert health['classification_engine_ready'] is True
            assert health['taxonomy_service_ready'] is True
            assert health['taxonomy_version'] == "tax_v20250814_123456"
    
    @pytest.mark.asyncio
    async def test_health_check_degraded(self):
        """Test health check with degraded services."""
        with patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            mock_taxonomy.get_current_version.return_value = None
            
            processor = StreamProcessor()
            health = await processor.health_check()
            
            assert health['status'] == 'degraded'
            assert health['processor_ready'] is True
            assert health['classification_engine_ready'] is True
            assert health['taxonomy_service_ready'] is False
    
    @pytest.mark.asyncio
    async def test_health_check_taxonomy_exception(self):
        """Test health check with taxonomy service exception."""
        with patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy:
            mock_taxonomy.get_current_version.side_effect = Exception("Service error")
            
            processor = StreamProcessor()
            health = await processor.health_check()
            
            assert health['status'] == 'degraded'
            assert health['taxonomy_service_ready'] is False


class TestClassificationError:
    """Tests for ClassificationError exception."""
    
    def test_classification_error(self):
        """Test ClassificationError creation."""
        error = ClassificationError("Test error message")
        assert str(error) == "Test error message"
        
        # Test with chained exception
        original_error = ValueError("Original error")
        try:
            raise ClassificationError("Wrapper error") from original_error
        except ClassificationError as chained_error:
            assert str(chained_error) == "Wrapper error"
            assert chained_error.__cause__ == original_error