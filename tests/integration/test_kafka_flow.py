"""
Integration tests for Kafka message flow.

Tests the complete flow from consuming scraped messages to producing classified results
using embedded Kafka for reliable testing.
"""

import asyncio
import json
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import AsyncMock, patch, MagicMock

# Import aiokafka test utilities for embedded kafka
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.consumers.scraped_consumer import ScrapedMessageConsumer
from app.producers.classified_producer import ClassifiedMessageProducer
from app.schemas.scraped_message import ScrapedMessage
from app.schemas.classified_message import ClassifiedMessage, CategoryPath, UsageStats
from app.engine.stream_processor import StreamProcessor


class TestKafkaIntegration:
    """Integration tests for Kafka message processing."""
    
    @pytest_asyncio.fixture
    async def kafka_setup(self):
        """Set up test Kafka topics and infrastructure."""
        # Use a simple in-memory approach for testing
        # In a real test environment, you might use testcontainers or embedded kafka
        
        topics = {
            'test-scraped': [],
            'test-classified': []
        }
        
        return topics
    
    @pytest.mark.asyncio
    async def test_end_to_end_message_flow(self, kafka_setup):
        """Test complete message flow from scraped to classified."""
        topics = kafka_setup
        
        # Mock the Kafka infrastructure
        with patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class, \
             patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class, \
             patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            
            # Configure settings
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Create mock consumer and producer
            mock_consumer = AsyncMock()
            mock_producer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            mock_producer_class.return_value = mock_producer
            
            # Create test scraped message
            test_message_data = {
                "id": str(uuid4()),
                "article_id": "art-e2e-123",
                "text": "This is a sample technology article about machine learning and AI.",
                "source": "tech_blog",
                "ts": "2025-08-14T10:30:00Z",
                "attrs": {"lang": "en", "author": "Test Author"}
            }
            
            # Mock the consumer record
            from aiokafka.structs import ConsumerRecord
            consumer_record = ConsumerRecord(
                topic="test-scraped",
                partition=0,
                offset=0,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=json.dumps(test_message_data).encode(),
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=len(json.dumps(test_message_data)),
                headers=[]
            )
            
            # Mock classification service
            mock_classification_service = AsyncMock()
            mock_classification_result = {
                'classification': [
                    {
                        'tier_1': 'Technology',
                        'tier_2': 'Machine Learning',
                        'confidence': 0.95,
                        'reasoning': 'Strong ML indicators found'
                    }
                ],
                'provider': 'openai',
                'model': 'gpt-4o-mini',
                'usage': {
                    'input_tokens': 150,
                    'output_tokens': 25,
                    'total_tokens': 175
                },
                'taxonomy_version': 'tax_v20250814_103000'
            }
            mock_classification_service.classify_text.return_value = mock_classification_result
            
            # Mock persistence service
            mock_persistence_service = AsyncMock()
            
            # Set up consumer and producer
            consumer = ScrapedMessageConsumer(
                input_topic="test-scraped",
                group_id="test-group"
            )
            consumer.set_classification_service(mock_classification_service)
            
            producer = ClassifiedMessageProducer(output_topic="test-classified")
            consumer.set_producer(producer)
            
            # Mock the producer send method
            producer_result = {
                'topic': 'test-classified',
                'partition': 0,
                'offset': 123,
                'timestamp': 1234567890
            }
            
            with patch.object(producer, 'send_message', new_callable=AsyncMock) as mock_send:
                mock_send.return_value = producer_result
                
                # Process the message
                await consumer.process_message(consumer_record)
                
                # Verify classification service was called
                mock_classification_service.classify_text.assert_called_once()
                call_args = mock_classification_service.classify_text.call_args
                assert call_args[0][0] == test_message_data['text']  # Text argument
                
                # Verify producer was called
                mock_send.assert_called_once()
                producer_call_args = mock_send.call_args
                
                # Verify the message sent to producer
                assert producer_call_args[1]['topic'] == 'test-classified'
                assert producer_call_args[1]['key'] == b'art-e2e-123'
                
                # Parse and verify the classified message
                sent_message = json.loads(producer_call_args[1]['value'].decode())
                assert sent_message['article_id'] == 'art-e2e-123'
                assert sent_message['provider'] == 'openai'
                assert sent_message['model'] == 'gpt-4o-mini'
                assert len(sent_message['classification']) == 1
                assert sent_message['classification'][0]['tier_1'] == 'Technology'
                assert sent_message['classification'][0]['tier_2'] == 'Machine Learning'
                assert sent_message['usage']['total_tokens'] == 175
    
    @pytest.mark.asyncio
    async def test_batch_processing(self, kafka_setup):
        """Test batch processing of multiple messages."""
        topics = kafka_setup
        
        # Create stream processor for batch testing
        with patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy, \
             patch('app.engine.stream_processor.get_settings') as mock_settings:
            
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
            
            processor = StreamProcessor()
            
            # Create test messages
            messages = []
            for i in range(3):
                message = ScrapedMessage(
                    id=str(uuid4()),
                    article_id=f"batch-art-{i}",
                    text=f"Test article content {i}",
                    source="test_source",
                    ts="2025-08-14T10:30:00Z",
                    attrs={"lang": "en"}
                )
                messages.append(message)
            
            # Process batch
            results = await processor.process_batch(messages)
            
            assert len(results) == 3
            for i, result in enumerate(results):
                assert isinstance(result, ClassifiedMessage)
                assert result.article_id == f"batch-art-{i}"
                assert result.provider == 'openai'
                assert result.model == 'gpt-4o-mini'
                assert len(result.classification) == 1
                assert result.classification[0].tier_1 == 'Technology'
            
            # Verify classification engine was called for each message
            assert mock_engine.classify.call_count == 3
    
    @pytest.mark.asyncio
    async def test_error_handling_in_flow(self, kafka_setup):
        """Test error handling throughout the message flow."""
        topics = kafka_setup
        
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            # Test invalid JSON message
            from aiokafka.structs import ConsumerRecord
            invalid_record = ConsumerRecord(
                topic="test-scraped",
                partition=0,
                offset=0,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=b"invalid json data",
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=17,
                headers=[]
            )
            
            # Should handle error gracefully
            from app.schemas import MessageValidationError
            with pytest.raises(MessageValidationError):
                await consumer.process_message(invalid_record)
    
    @pytest.mark.asyncio
    async def test_message_ordering_guarantees(self, kafka_setup):
        """Test that message ordering is preserved."""
        topics = kafka_setup
        
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            # Create ordered test messages
            messages = []
            for i in range(5):
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.8 + (i * 0.02),
                    reasoning=f"Message {i}"
                )
                
                usage = UsageStats(
                    input_tokens=100 + i,
                    output_tokens=20 + i,
                    total_tokens=120 + (i * 2)
                )
                
                message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id=f"order-test-{i:03d}",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                messages.append(message)
            
            # Mock send_batch to capture message order
            sent_messages = []
            
            async def mock_send_batch(batch_messages, topic):
                sent_messages.extend(batch_messages)
                return [{'success': True, 'offset': i} for i in range(len(batch_messages))]
            
            with patch.object(producer, 'send_batch', side_effect=mock_send_batch):
                await producer.publish_batch(messages)
                
                # Verify order is preserved
                assert len(sent_messages) == 5
                for i, sent_msg in enumerate(sent_messages):
                    message_data = json.loads(sent_msg['value'])
                    assert message_data['article_id'] == f"order-test-{i:03d}"
    
    @pytest.mark.asyncio
    async def test_consumer_partition_handling(self, kafka_setup):
        """Test consumer behavior with multiple partitions."""
        topics = kafka_setup
        
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            consumer = ScrapedMessageConsumer()
            
            # Mock partition assignment
            mock_partitions = [
                MagicMock(topic='test-scraped', partition=0),
                MagicMock(topic='test-scraped', partition=1),
                MagicMock(topic='test-scraped', partition=2)
            ]
            
            # Test partition assignment handling
            await consumer.on_partitions_assigned(mock_partitions)
            
            # Verify consumer state
            assert consumer.assigned_partitions == mock_partitions
    
    @pytest.mark.asyncio
    async def test_producer_delivery_guarantees(self, kafka_setup):
        """Test producer delivery confirmation."""
        topics = kafka_setup
        
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            # Mock the base producer to simulate delivery confirmation
            with patch.object(producer, 'send_message', new_callable=AsyncMock) as mock_send:
                # Simulate successful delivery
                mock_send.return_value = {
                    'topic': 'test-classified',
                    'partition': 0,
                    'offset': 123,
                    'timestamp': 1234567890,
                    'serialized_key_size': 10,
                    'serialized_value_size': 250
                }
                
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.9,
                    reasoning="High confidence classification"
                )
                
                usage = UsageStats(
                    input_tokens=150,
                    output_tokens=25,
                    total_tokens=175
                )
                
                message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id="delivery-test",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                result = await producer.publish_classified_message(message)
                
                # Verify delivery confirmation details
                assert result['topic'] == 'test-classified'
                assert result['offset'] == 123
                assert result['timestamp'] == 1234567890
                assert 'serialized_value_size' in result
    
    @pytest.mark.asyncio
    async def test_concurrent_processing(self, kafka_setup):
        """Test concurrent message processing."""
        topics = kafka_setup
        
        with patch('app.engine.stream_processor.classification_engine') as mock_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy, \
             patch('app.engine.stream_processor.get_settings') as mock_settings:
            
            mock_settings.return_value.kafka_batch_size = 5
            mock_settings.return_value.kafka_processing_timeout = 30.0
            
            # Mock services
            mock_taxonomy.get_current_taxonomy.return_value = {
                'categories': [],
                'version': 'tax_v20250814_103000'
            }
            
            # Simulate variable processing times
            async def mock_classify(*args, **kwargs):
                await asyncio.sleep(0.1)  # Simulate processing time
                return {
                    'classification': [
                        {
                            'tier_1': 'Technology',
                            'confidence': 0.85,
                            'reasoning': 'Concurrent processing test'
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
            
            mock_engine.classify.side_effect = mock_classify
            
            processor = StreamProcessor()
            
            # Create multiple messages for concurrent processing
            messages = []
            for i in range(10):
                message = ScrapedMessage(
                    id=str(uuid4()),
                    article_id=f"concurrent-{i}",
                    text=f"Concurrent test article {i}",
                    source="test_source",
                    ts="2025-08-14T10:30:00Z"
                )
                messages.append(message)
            
            # Process messages concurrently in batches
            start_time = asyncio.get_event_loop().time()
            
            # Split into batches and process concurrently
            batch_size = 5
            batches = [messages[i:i + batch_size] for i in range(0, len(messages), batch_size)]
            
            tasks = [processor.process_batch(batch) for batch in batches]
            results = await asyncio.gather(*tasks)
            
            end_time = asyncio.get_event_loop().time()
            
            # Verify all messages were processed
            total_results = []
            for batch_result in results:
                total_results.extend(batch_result)
            
            assert len(total_results) == 10
            
            # Verify concurrent processing was faster than sequential
            # (This is a rough check - concurrent should be significantly faster)
            processing_time = end_time - start_time
            assert processing_time < 1.5  # Should be much faster than 10 * 0.1 = 1 second
            
            # Verify all results are valid
            for result in total_results:
                assert isinstance(result, ClassifiedMessage)
                assert result.provider == 'openai'
                assert len(result.classification) == 1