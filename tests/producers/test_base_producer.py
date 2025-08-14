"""
Tests for base Kafka producer functionality.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.producers.base_producer import BaseKafkaProducer


class TestableProducer(BaseKafkaProducer):
    """Testable implementation of BaseKafkaProducer."""
    
    async def publish_message(self, message):
        """Test implementation."""
        return await self.send_message(
            topic="test-topic",
            value=str(message),
            key="test-key"
        )


class TestBaseKafkaProducer:
    """Tests for BaseKafkaProducer class."""
    
    def test_init(self):
        """Test producer initialization."""
        producer = TestableProducer(
            bootstrap_servers="localhost:9092",
            acks='all',
            retries=5
        )
        
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.producer_config['acks'] == 'all'
        assert producer.producer_config['retries'] == 5
        assert not producer.is_running
        assert not producer.is_stopping
        assert producer.messages_sent == 0
        assert producer.messages_failed == 0
    
    def test_init_with_settings(self):
        """Test producer initialization using settings."""
        with patch('app.producers.base_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_brokers = "test-broker:9092"
            
            producer = TestableProducer()
            
            assert producer.bootstrap_servers == "test-broker:9092"
    
    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test producer start and stop lifecycle."""
        with patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            producer = TestableProducer(bootstrap_servers="localhost:9092")
            
            # Test start
            await producer.start()
            
            assert producer.is_running
            assert not producer.is_stopping
            mock_producer.start.assert_called_once()
            
            # Test stop
            await producer.stop()
            
            assert not producer.is_running
            assert not producer.is_stopping
            mock_producer.stop.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_start_already_running(self):
        """Test starting producer when already running."""
        producer = TestableProducer(bootstrap_servers="localhost:9092")
        producer.is_running = True
        
        with patch('app.producers.base_producer.AIOKafkaProducer'):
            await producer.start()
            # Should not create new producer
            assert producer.producer is None
    
    @pytest.mark.asyncio
    async def test_send_message_success(self):
        """
        Test successful message sending with proper aiokafka Future handling.
        
        This test validates the complex async pattern used by aiokafka where:
        1. producer.send() returns a Future (not awaitable directly)
        2. The Future must be awaited separately to get delivery confirmation
        3. Record metadata contains partition assignment and offset information
        4. Producer tracks metrics for successful deliveries
        
        Mock Strategy:
        - AIOKafkaProducer.send() returns a Future object
        - Future resolves to RecordMetadata with delivery details
        - Producer lifecycle (start/stop) properly managed
        - Message serialization handled transparently
        
        Key Pattern Being Tested:
        ```
        future = producer.send(topic, value, key)  # Returns Future
        record_metadata = await future            # Await the Future
        ```
        
        This pattern is critical for proper Kafka integration and was a source
        of AsyncMock-related test failures that required specific mock handling.
        """
        with patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Mock record metadata
            mock_record_metadata = MagicMock()
            mock_record_metadata.topic = "test-topic"
            mock_record_metadata.partition = 0
            mock_record_metadata.offset = 123
            mock_record_metadata.timestamp = 1234567890
            mock_record_metadata.serialized_key_size = 8
            mock_record_metadata.serialized_value_size = 10
            
            # Mock send method to return an awaitable that resolves to record metadata
            def create_future(*args, **kwargs):
                import asyncio
                future = asyncio.Future()
                future.set_result(mock_record_metadata)
                return future
            
            # Replace the send method entirely with our mock function
            mock_producer.send = create_future
            
            producer = TestableProducer(bootstrap_servers="localhost:9092")
            producer.producer = mock_producer
            producer.is_running = True
            
            # Test send message
            result = await producer.send_message(
                topic="test-topic",
                value="test-value",
                key="test-key"
            )
            
            assert producer.messages_sent == 1
            assert producer.messages_failed == 0
            assert producer.bytes_sent == 10  # Length of "test-value"
            assert producer.last_send_timestamp == 1234567890
            
            assert result['topic'] == "test-topic"
            assert result['partition'] == 0
            assert result['offset'] == 123
    
    @pytest.mark.asyncio
    async def test_send_message_not_running(self):
        """Test sending message when producer not running."""
        producer = TestableProducer(bootstrap_servers="localhost:9092")
        
        with pytest.raises(RuntimeError, match="Producer not started"):
            await producer.send_message(
                topic="test-topic",
                value="test-value"
            )
    
    @pytest.mark.asyncio
    async def test_send_message_string_conversion(self):
        """Test automatic string to bytes conversion."""
        with patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            mock_record_metadata = MagicMock()
            mock_record_metadata.topic = "test-topic"
            mock_record_metadata.partition = 0
            mock_record_metadata.offset = 123
            mock_record_metadata.timestamp = 1234567890
            mock_record_metadata.serialized_key_size = 8
            mock_record_metadata.serialized_value_size = 10
            
            # Mock send method to return an awaitable that resolves to record metadata
            def create_future_func(*args, **kwargs):
                import asyncio
                future_func = asyncio.Future()
                future_func.set_result(mock_record_metadata)
                return future_func
            
            # Create a mock wrapper that tracks calls
            send_mock = MagicMock(side_effect=create_future_func)
            mock_producer.send = send_mock
            
            producer = TestableProducer(bootstrap_servers="localhost:9092")
            producer.producer = mock_producer
            producer.is_running = True
            
            await producer.send_message(
                topic="test-topic",
                value="test-value",
                key="test-key"
            )
            
            # Verify that strings were converted to bytes
            send_mock.assert_called_once()
            call_args = send_mock.call_args
            assert call_args[1]['value'] == b"test-value"
            assert call_args[1]['key'] == b"test-key"
    
    @pytest.mark.asyncio
    async def test_send_batch(self):
        """Test batch message sending."""
        with patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            mock_record_metadata = MagicMock()
            mock_record_metadata.topic = "test-topic"
            mock_record_metadata.partition = 0
            mock_record_metadata.offset = 123
            mock_record_metadata.timestamp = 1234567890
            mock_record_metadata.serialized_key_size = 8
            mock_record_metadata.serialized_value_size = 10
            
            # Mock send method to return an awaitable that resolves to record metadata
            def create_future_func(*args, **kwargs):
                import asyncio
                future_func = asyncio.Future()
                future_func.set_result(mock_record_metadata)
                return future_func
            
            # Replace the send method entirely with our mock function
            mock_producer.send = create_future_func
            
            producer = TestableProducer(bootstrap_servers="localhost:9092")
            producer.producer = mock_producer
            producer.is_running = True
            
            messages = [
                {'value': 'message1', 'key': 'key1'},
                {'value': 'message2', 'key': 'key2'}
            ]
            
            results = await producer.send_batch(messages, "test-topic")
            
            assert len(results) == 2
            assert all(r['success'] for r in results)
            assert producer.messages_sent == 2
    
    @pytest.mark.asyncio
    async def test_flush(self):
        """Test producer flush."""
        with patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            producer = TestableProducer(bootstrap_servers="localhost:9092")
            producer.producer = mock_producer
            
            await producer.flush()
            
            mock_producer.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_flush_timeout(self):
        """Test producer flush with timeout."""
        with patch('app.producers.base_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            # Mock flush to take longer than timeout
            async def slow_flush():
                await asyncio.sleep(2)
            mock_producer.flush = AsyncMock(side_effect=slow_flush)
            
            producer = TestableProducer(bootstrap_servers="localhost:9092")
            producer.producer = mock_producer
            
            with pytest.raises(asyncio.TimeoutError):
                await producer.flush(timeout=0.1)
    
    def test_get_metrics(self):
        """Test metrics collection."""
        producer = TestableProducer(bootstrap_servers="localhost:9092")
        
        # Set some test metrics
        producer.messages_sent = 100
        producer.messages_failed = 5
        producer.bytes_sent = 1024
        producer.last_send_timestamp = 1234567890
        producer.is_running = True
        
        metrics = producer.get_metrics()
        
        assert metrics['bootstrap_servers'] == "localhost:9092"
        assert metrics['is_running'] is True
        assert metrics['messages_sent'] == 100
        assert metrics['messages_failed'] == 5
        assert metrics['bytes_sent'] == 1024
        assert metrics['last_send_timestamp'] == 1234567890
        assert metrics['error_rate'] == 5 / 105  # 5 failed out of 105 total
    
    def test_get_metrics_no_messages(self):
        """Test metrics with no messages sent."""
        producer = TestableProducer(bootstrap_servers="localhost:9092")
        
        metrics = producer.get_metrics()
        assert metrics['error_rate'] == 0
    
    @pytest.mark.asyncio
    async def test_health_check_healthy(self):
        """Test health check for healthy producer."""
        producer = TestableProducer(bootstrap_servers="localhost:9092")
        producer.is_running = True
        producer.producer = MagicMock()  # Mock producer
        
        health = await producer.health_check()
        
        assert health['status'] == 'healthy'
        assert health['producer_running'] is True
        assert health['producer_stopping'] is False
        assert health['bootstrap_servers'] == "localhost:9092"
        assert health['client_ready'] is True
    
    @pytest.mark.asyncio
    async def test_health_check_unhealthy(self):
        """Test health check for unhealthy producer."""
        producer = TestableProducer(bootstrap_servers="localhost:9092")
        producer.is_running = False
        
        health = await producer.health_check()
        
        assert health['status'] == 'unhealthy'
        assert health['producer_running'] is False
        assert health['client_ready'] is False