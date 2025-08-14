"""
Tests for base Kafka consumer functionality.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.structs import ConsumerRecord, TopicPartition

from app.consumers.base_consumer import BaseKafkaConsumer


class TestableConsumer(BaseKafkaConsumer):
    """Testable implementation of BaseKafkaConsumer."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_messages = []
        self.processing_errors = []
    
    async def process_message(self, message: ConsumerRecord) -> None:
        """Test implementation that stores processed messages."""
        if message.value == b"error":
            raise ValueError("Test error")
        self.processed_messages.append(message)
    
    async def handle_message_error(self, message: ConsumerRecord, error: Exception) -> None:
        """Store errors for testing."""
        self.processing_errors.append((message, error))


class TestBaseKafkaConsumer:
    """Tests for BaseKafkaConsumer class."""
    
    def test_init(self):
        """Test consumer initialization."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        assert consumer.topics == ["test-topic"]
        assert consumer.group_id == "test-group"
        assert consumer.bootstrap_servers == "localhost:9092"
        assert not consumer.is_running
        assert not consumer.is_stopping
        assert consumer.messages_processed == 0
        assert consumer.messages_failed == 0
    
    def test_init_with_settings(self):
        """Test consumer initialization using settings."""
        with patch('app.consumers.base_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_brokers = "test-broker:9092"
            
            consumer = TestableConsumer(
                topics=["test-topic"],
                group_id="test-group"
            )
            
            assert consumer.bootstrap_servers == "test-broker:9092"
    
    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test consumer start and stop lifecycle."""
        with patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            mock_consumer = AsyncMock()
            mock_consumer.subscribe = MagicMock()  # subscribe is not async
            mock_consumer_class.return_value = mock_consumer
            
            consumer = TestableConsumer(
                topics=["test-topic"],
                group_id="test-group",
                bootstrap_servers="localhost:9092"
            )
            
            # Test start
            await consumer.start()
            
            assert consumer.is_running
            assert not consumer.is_stopping
            mock_consumer.start.assert_called_once()
            mock_consumer.subscribe.assert_called_once_with(["test-topic"])
            
            # Test stop
            await consumer.stop()
            
            assert not consumer.is_running
            assert not consumer.is_stopping
            mock_consumer.stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_start_already_running(self):
        """Test starting consumer when already running."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        consumer.is_running = True
        
        with patch('app.consumers.base_consumer.AIOKafkaConsumer'):
            await consumer.start()
            # Should not create new consumer
            assert consumer.consumer is None
    
    @pytest.mark.asyncio
    async def test_handle_message_success(self):
        """Test successful message handling."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        message = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=123,
            timestamp=1234567890,
            timestamp_type=1,
            key=b"test-key",
            value=b"test-value",
            checksum=None,
            serialized_key_size=8,
            serialized_value_size=10,
            headers=[]
        )
        
        await consumer._handle_message(message)
        
        assert len(consumer.processed_messages) == 1
        assert consumer.processed_messages[0] == message
        assert consumer.messages_processed == 1
        assert consumer.messages_failed == 0
        assert consumer.last_message_timestamp == 1234567890
    
    @pytest.mark.asyncio
    async def test_handle_message_error(self):
        """Test message handling with processing error."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        message = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=123,
            timestamp=1234567890,
            timestamp_type=1,
            key=b"test-key",
            value=b"error",  # This triggers error in TestableConsumer
            checksum=None,
            serialized_key_size=8,
            serialized_value_size=5,
            headers=[]
        )
        
        await consumer._handle_message(message)
        
        assert len(consumer.processed_messages) == 0
        assert len(consumer.processing_errors) == 1
        assert consumer.messages_processed == 0
        assert consumer.messages_failed == 1
        
        error_message, error = consumer.processing_errors[0]
        assert error_message == message
        assert isinstance(error, ValueError)
        assert str(error) == "Test error"
    
    def test_get_metrics(self):
        """Test metrics collection."""
        consumer = TestableConsumer(
            topics=["test-topic1", "test-topic2"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        # Set some test metrics
        consumer.messages_processed = 100
        consumer.messages_failed = 5
        consumer.last_message_timestamp = 1234567890
        consumer.is_running = True
        
        metrics = consumer.get_metrics()
        
        assert metrics['topics'] == ["test-topic1", "test-topic2"]
        assert metrics['group_id'] == "test-group"
        assert metrics['is_running'] is True
        assert metrics['messages_processed'] == 100
        assert metrics['messages_failed'] == 5
        assert metrics['last_message_timestamp'] == 1234567890
        assert metrics['error_rate'] == 5 / 105  # 5 failed out of 105 total
    
    def test_get_metrics_no_messages(self):
        """Test metrics with no messages processed."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        metrics = consumer.get_metrics()
        assert metrics['error_rate'] == 0
    
    @pytest.mark.asyncio
    async def test_get_consumer_lag_no_consumer(self):
        """Test consumer lag when no consumer is set."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        lag = await consumer.get_consumer_lag()
        assert lag == {}
    
    @pytest.mark.asyncio
    async def test_get_consumer_lag_with_consumer(self):
        """Test consumer lag calculation."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        
        # Mock consumer with assignment
        mock_consumer = AsyncMock()
        tp1 = TopicPartition("test-topic", 0)
        tp2 = TopicPartition("test-topic", 1)
        
        mock_consumer.assignment = MagicMock(return_value={tp1, tp2})
        mock_consumer.end_offsets.return_value = {tp1: 1000, tp2: 500}
        mock_consumer.position = AsyncMock(side_effect=lambda tp: 900 if tp == tp1 else 400)
        
        consumer.consumer = mock_consumer
        
        lag = await consumer.get_consumer_lag()
        
        assert lag == {
            "test-topic-0": 100,  # 1000 - 900
            "test-topic-1": 100   # 500 - 400
        }
    
    @pytest.mark.asyncio
    async def test_health_check_healthy(self):
        """Test health check for healthy consumer."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        consumer.is_running = True
        
        # Mock consumer with assignment
        mock_consumer = MagicMock()
        tp = TopicPartition("test-topic", 0)
        mock_consumer.assignment.return_value = {tp}
        consumer.consumer = mock_consumer
        
        health = await consumer.health_check()
        
        assert health['status'] == 'healthy'
        assert health['consumer_running'] is True
        assert health['consumer_stopping'] is False
        assert health['topics_subscribed'] == ["test-topic"]
        assert health['group_id'] == "test-group"
        assert health['partitions_assigned'] == 1
        assert health['partition_details'] == ["test-topic-0"]
    
    @pytest.mark.asyncio
    async def test_health_check_unhealthy(self):
        """Test health check for unhealthy consumer."""
        consumer = TestableConsumer(
            topics=["test-topic"],
            group_id="test-group",
            bootstrap_servers="localhost:9092"
        )
        consumer.is_running = False
        
        health = await consumer.health_check()
        
        assert health['status'] == 'unhealthy'
        assert health['consumer_running'] is False