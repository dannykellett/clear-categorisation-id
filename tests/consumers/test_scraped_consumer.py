"""
Tests for scraped message consumer.
"""

import json
import pytest
from unittest.mock import AsyncMock, patch
from aiokafka.structs import ConsumerRecord
from uuid import uuid4

from app.consumers.scraped_consumer import ScrapedMessageConsumer, create_and_run_consumer
from app.schemas import MessageValidationError


class TestScrapedMessageConsumer:
    """Tests for ScrapedMessageConsumer class."""
    
    def test_init_with_defaults(self):
        """Test consumer initialization with default settings."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "default-scraped"
            mock_settings.return_value.kafka_group_id = "default-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            assert consumer.input_topic == "default-scraped"
            assert consumer.group_id == "default-group"
            assert consumer.topics == ["default-scraped"]
    
    def test_init_with_custom_values(self):
        """Test consumer initialization with custom values."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer(
                input_topic="custom-topic",
                group_id="custom-group"
            )
            
            assert consumer.input_topic == "custom-topic"
            assert consumer.group_id == "custom-group"
            assert consumer.topics == ["custom-topic"]
    
    @pytest.mark.asyncio
    async def test_process_message_valid(self):
        """Test processing a valid scraped message."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.engine.stream_processor.classification_engine') as mock_classification_engine, \
             patch('app.engine.stream_processor.taxonomy_service') as mock_taxonomy, \
             patch('app.consumers.scraped_consumer.get_persistence_service') as mock_persistence_service, \
             patch('app.consumers.scraped_consumer.ClassifiedMessageProducer') as mock_producer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Mock classification engine to return valid results
            from app.models.responses import CategoryPath, UsageInfo
            from unittest.mock import Mock, AsyncMock
            
            mock_categories = [
                CategoryPath(
                    path="technology/software/web-development",
                    confidence=0.85,
                    reasoning="Test classification"
                )
            ]
            mock_usage = UsageInfo(
                provider="openai",
                model="gpt-4o-mini",
                input_tokens=10,
                output_tokens=5
            )
            
            mock_classification_engine.classify_text = AsyncMock(return_value=(mock_categories, mock_usage))
            
            # Mock taxonomy service to return valid version
            mock_version = Mock()
            mock_version.version_string = "tax_v20250814_103000"
            mock_taxonomy.get_current_version.return_value = mock_version
            
            # Mock persistence service
            mock_persistence = AsyncMock()
            mock_persistence.persist_message_pair = AsyncMock(return_value=(True, True))
            mock_persistence_service.return_value = mock_persistence
            
            # Mock producer
            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            mock_producer.publish_message = AsyncMock(return_value={"partition": 0, "offset": 123})
            mock_producer.output_topic = "test-output-topic"
            mock_producer_class.return_value = mock_producer
            
            consumer = ScrapedMessageConsumer()
            
            # Manually set the producer instead of calling start()
            consumer.classified_producer = mock_producer
            
            # Create valid message
            message_data = {
                "id": str(uuid4()),
                "article_id": "art-123",
                "text": "Sample text for classification",
                "source": "tech_blog",
                "ts": "2025-08-14T10:30:00Z",
                "attrs": {"lang": "en"}
            }
            
            message = ConsumerRecord(
                topic="test-topic",
                partition=0,
                offset=123,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=json.dumps(message_data).encode(),
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=len(json.dumps(message_data)),
                headers=[]
            )
            
            # Should not raise any exceptions
            await consumer.process_message(message)
    
    @pytest.mark.asyncio
    async def test_process_message_invalid_json(self):
        """Test processing message with invalid JSON."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            message = ConsumerRecord(
                topic="test-topic",
                partition=0,
                offset=123,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=b"invalid json",
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=12,
                headers=[]
            )
            
            with pytest.raises(MessageValidationError):
                await consumer.process_message(message)
    
    @pytest.mark.asyncio
    async def test_process_message_invalid_schema(self):
        """Test processing message with invalid schema."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            # Missing required fields
            message_data = {
                "id": "invalid-uuid",
                "text": "",  # Empty text should fail validation
            }
            
            message = ConsumerRecord(
                topic="test-topic",
                partition=0,
                offset=123,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=json.dumps(message_data).encode(),
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=len(json.dumps(message_data)),
                headers=[]
            )
            
            with pytest.raises(MessageValidationError):
                await consumer.process_message(message)
    
    @pytest.mark.asyncio
    async def test_handle_message_error_validation(self):
        """Test error handling for validation errors."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            message = ConsumerRecord(
                topic="test-topic",
                partition=0,
                offset=123,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=b"invalid json",
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=12,
                headers=[]
            )
            
            error = MessageValidationError("Test validation error")
            
            # Should not raise exception
            await consumer.handle_message_error(message, error)
    
    @pytest.mark.asyncio
    async def test_handle_message_error_generic(self):
        """Test error handling for generic errors."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            message = ConsumerRecord(
                topic="test-topic",
                partition=0,
                offset=123,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=b"test value",
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=10,
                headers=[]
            )
            
            error = Exception("Generic error")
            
            # Should not raise exception
            await consumer.handle_message_error(message, error)
    
    def test_set_classification_service(self):
        """Test setting classification service."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            mock_service = AsyncMock()
            
            consumer.set_classification_service(mock_service)
            
            assert consumer.classification_service == mock_service
    
    def test_set_producer(self):
        """Test setting message producer."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            mock_producer = AsyncMock()
            
            consumer.set_producer(mock_producer)
            
            assert consumer.producer == mock_producer
    
    @pytest.mark.asyncio
    async def test_run_method(self):
        """Test the run method lifecycle."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            with patch.object(consumer, 'start') as mock_start, \
                 patch.object(consumer, 'consume_messages') as mock_consume, \
                 patch.object(consumer, 'stop') as mock_stop:
                
                # Mock consume_messages to return quickly
                mock_consume.return_value = None
                
                await consumer.run()
                
                mock_start.assert_called_once()
                mock_consume.assert_called_once()
                mock_stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_with_exception(self):
        """Test run method with exception handling."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-topic"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            with patch.object(consumer, 'start') as mock_start, \
                 patch.object(consumer, 'consume_messages') as mock_consume, \
                 patch.object(consumer, 'stop') as mock_stop:
                
                # Mock consume_messages to raise exception
                mock_consume.side_effect = Exception("Test error")
                
                with pytest.raises(Exception, match="Test error"):
                    await consumer.run()
                
                mock_start.assert_called_once()
                mock_consume.assert_called_once()
                mock_stop.assert_called_once()


class TestCreateAndRunConsumer:
    """Tests for convenience function."""
    
    @pytest.mark.asyncio
    async def test_create_and_run_consumer_without_shutdown(self):
        """Test creating and running consumer without shutdown event."""
        with patch('app.consumers.scraped_consumer.ScrapedMessageConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            await create_and_run_consumer(
                input_topic="test-topic",
                group_id="test-group"
            )
            
            mock_consumer_class.assert_called_once_with(
                input_topic="test-topic",
                group_id="test-group"
            )
            mock_consumer.run.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_and_run_consumer_with_shutdown(self):
        """Test creating and running consumer with shutdown event."""
        import asyncio
        
        with patch('app.consumers.scraped_consumer.ScrapedMessageConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            shutdown_event = asyncio.Event()
            
            await create_and_run_consumer(
                input_topic="test-topic",
                group_id="test-group",
                shutdown_event=shutdown_event
            )
            
            mock_consumer_class.assert_called_once_with(
                input_topic="test-topic",
                group_id="test-group"
            )
            mock_consumer.run_with_graceful_shutdown.assert_called_once_with(shutdown_event)