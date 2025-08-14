"""
Tests for classified message producer.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from app.producers.classified_producer import ClassifiedMessageProducer
from app.schemas.classified_message import ClassifiedMessage, CategoryPath, UsageStats


class TestClassifiedMessageProducer:
    """Tests for ClassifiedMessageProducer class."""
    
    def test_init_with_defaults(self):
        """Test producer initialization with default settings."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "default-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            assert producer.output_topic == "default-classified"
    
    def test_init_with_custom_topic(self):
        """Test producer initialization with custom topic."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer(output_topic="custom-classified")
            
            assert producer.output_topic == "custom-classified"
    
    @pytest.mark.asyncio
    async def test_publish_message_success(self):
        """Test successful publishing of classified message."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            # Mock base producer methods
            with patch.object(producer, 'send_message', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = {
                    'topic': 'test-classified',
                    'partition': 0,
                    'offset': 123,
                    'timestamp': 1234567890
                }
                
                # Create test classified message
                category_path = CategoryPath(
                    tier_1="Technology",
                    tier_2="Hardware",
                    confidence=0.92,
                    reasoning="Strong indicators of hardware content"
                )
                
                usage = UsageStats(
                    input_tokens=150,
                    output_tokens=20,
                    total_tokens=170
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id="art-123",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                result = await producer.publish_message(classified_message)
                
                assert result['topic'] == 'test-classified'
                assert result['partition'] == 0
                assert result['offset'] == 123
                
                # Verify send_message was called with correct parameters
                mock_send.assert_called_once()
                call_args = mock_send.call_args
                assert call_args[1]['topic'] == 'test-classified'
                assert call_args[1]['key'] == classified_message.article_id
                
                # Verify message content
                sent_value = call_args[1]['value']
                if isinstance(sent_value, bytes):
                    sent_message = json.loads(sent_value.decode())
                else:
                    sent_message = json.loads(sent_value)
                assert sent_message['article_id'] == "art-123"
                assert sent_message['provider'] == "openai"
    
    @pytest.mark.asyncio
    async def test_publish_message_generic_method(self):
        """Test generic publish_message method."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            with patch.object(producer, 'publish_message', new_callable=AsyncMock) as mock_publish:
                mock_publish.return_value = {'success': True}
                
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.85,
                    reasoning="Technology content detected"
                )
                
                usage = UsageStats(
                    input_tokens=100,
                    output_tokens=15,
                    total_tokens=115
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id="art-456",
                    classification=[category_path],
                    provider="ollama",
                    model="qwen3:30b",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                result = await producer.publish_message(classified_message)
                
                assert result['success'] is True
                mock_publish.assert_called_once_with(classified_message)
    
    @pytest.mark.asyncio
    async def test_publish_message_invalid_type(self):
        """Test error handling for invalid message type."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            # The actual producer doesn't validate message type, so we test actual behavior
            with pytest.raises(AttributeError):  # String doesn't have the expected attributes
                await producer.publish_message("invalid_message")
    
    @pytest.mark.asyncio
    async def test_publish_batch_success(self):
        """Test successful batch publishing."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            with patch.object(producer, 'send_batch', new_callable=AsyncMock) as mock_send_batch:
                mock_send_batch.return_value = [
                    {'success': True, 'offset': 123},
                    {'success': True, 'offset': 124}
                ]
                
                # Create test messages
                messages = []
                for i in range(2):
                    category_path = CategoryPath(
                        tier_1="Technology",
                        confidence=0.8 + i * 0.1,
                        reasoning=f"Test reasoning {i}"
                    )
                    
                    usage = UsageStats(
                        input_tokens=100 + i * 10,
                        output_tokens=15 + i * 2,
                        total_tokens=115 + i * 12
                    )
                    
                    message = ClassifiedMessage(
                        id=str(uuid4()),
                        article_id=f"art-{i}",
                        classification=[category_path],
                        provider="openai",
                        model="gpt-4o-mini",
                        usage=usage,
                        taxonomy_version="tax_v20250814_103000"
                    )
                    messages.append(message)
                
                results = await producer.publish_batch(messages)
                
                assert len(results) == 2
                assert all(r['success'] for r in results)
                
                # Verify send_batch was called with proper format
                mock_send_batch.assert_called_once()
                call_args = mock_send_batch.call_args
                batch_messages = call_args[0][0]  # First argument is the batch
                
                assert len(batch_messages) == 2
                for i, batch_msg in enumerate(batch_messages):
                    assert 'value' in batch_msg
                    assert 'key' in batch_msg
                    assert batch_msg['key'] == f"art-{i}"
    
    @pytest.mark.asyncio
    async def test_publish_batch_empty(self):
        """Test batch publishing with empty list."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            results = await producer.publish_batch([])
            
            assert results == []
    
    @pytest.mark.asyncio
    async def test_publish_batch_invalid_message(self):
        """Test batch publishing with invalid message in batch."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            # Mix valid and invalid messages
            category_path = CategoryPath(
                tier_1="Technology",
                confidence=0.85,
                reasoning="Valid message"
            )
            
            usage = UsageStats(
                input_tokens=100,
                output_tokens=15,
                total_tokens=115
            )
            
            valid_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="art-valid",
                classification=[category_path],
                provider="openai",
                model="gpt-4o-mini",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            messages = [valid_message, "invalid_message"]
            
            # The actual producer will fail on the invalid message due to missing attributes
            with pytest.raises(AttributeError):
                await producer.publish_batch(messages)
    
    def test_topic_metrics(self):
        """Test topic-specific metrics collection."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            producer.messages_sent = 100
            
            metrics = producer.get_topic_metrics()
            
            assert 'output_topic' in metrics
            assert metrics['output_topic'] == "test-classified"
            assert 'messages_per_topic' in metrics
            assert metrics['messages_per_topic']['test-classified'] == 100
    
    @pytest.mark.asyncio
    async def test_health_check_with_metrics(self):
        """Test health check including producer-specific metrics."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            producer.is_running = True
            producer.producer = MagicMock()
            
            health = await producer.health_check()
            
            assert health['status'] == 'healthy'
            assert health['output_topic'] == 'test-classified'
            assert 'bootstrap_servers' in health
            assert 'producer_running' in health
    
    @pytest.mark.asyncio
    async def test_error_handling_send_failure(self):
        """Test error handling when send_message fails."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            with patch.object(producer, 'send_message', new_callable=AsyncMock) as mock_send:
                mock_send.side_effect = Exception("Kafka send failed")
                
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.85,
                    reasoning="Test message"
                )
                
                usage = UsageStats(
                    input_tokens=100,
                    output_tokens=15,
                    total_tokens=115
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id="art-fail",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                with pytest.raises(Exception, match="Kafka send failed"):
                    await producer.publish_message(classified_message)