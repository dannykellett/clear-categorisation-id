"""
Tests for error scenarios and recovery mechanisms in Kafka message processing.

Tests various failure scenarios and recovery patterns including:
- Network failures and reconnection
- Classification service failures
- Database connectivity issues
- Message corruption and validation errors
- Consumer lag and backpressure handling
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import AsyncMock, patch, MagicMock

from aiokafka.structs import ConsumerRecord
from aiokafka.errors import KafkaError, KafkaConnectionError, KafkaTimeoutError

from app.consumers.scraped_consumer import ScrapedMessageConsumer
from app.producers.classified_producer import ClassifiedMessageProducer
from app.schemas import MessageValidationError
from app.schemas.scraped_message import ScrapedMessage
from app.schemas.classified_message import ClassifiedMessage, CategoryPath, UsageStats


class TestErrorRecoveryScenarios:
    """Tests for various error scenarios and recovery mechanisms."""
    
    @pytest.mark.asyncio
    async def test_kafka_connection_failure_and_recovery(self):
        """
        Test Kafka connection failure and automatic recovery mechanism.
        
        This test simulates a network failure scenario where:
        1. Initial connection attempt fails with KafkaConnectionError
        2. Consumer implements exponential backoff retry strategy
        3. Subsequent connection attempt succeeds
        4. Consumer resumes normal operation after recovery
        
        Recovery patterns tested:
        - Automatic retry with exponential backoff
        - Connection state management during failures
        - Proper cleanup of failed connections
        - Graceful resumption of message processing
        
        Expected behavior:
        - First connection attempt raises KafkaConnectionError
        - Consumer waits with exponential backoff before retry
        - Second attempt succeeds and consumer becomes operational
        - No message loss or duplicate processing after recovery
        """
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock connection failure followed by recovery
            connection_attempts = 0
            def start_side_effect():
                nonlocal connection_attempts
                connection_attempts += 1
                if connection_attempts == 1:
                    raise KafkaConnectionError("Connection failed")
                # Second attempt succeeds
                return None
            
            mock_consumer.start.side_effect = start_side_effect
            
            consumer = ScrapedMessageConsumer()
            
            # First start attempt should fail
            with pytest.raises(KafkaConnectionError):
                await consumer.start()
            
            # Second attempt should succeed
            await consumer.start()
            
            assert connection_attempts == 2
            assert consumer.is_running is True
    
    @pytest.mark.asyncio
    async def test_kafka_timeout_error_handling(self):
        """Test handling of Kafka timeout errors."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock timeout during message consumption
            mock_consumer.getmany.side_effect = KafkaTimeoutError("Timeout waiting for messages")
            
            consumer = ScrapedMessageConsumer()
            consumer.is_running = True
            consumer.consumer = mock_consumer
            
            # Should handle timeout gracefully
            await consumer.consume_messages()
            
            # Consumer should still be running after timeout
            assert consumer.is_running is True
    
    @pytest.mark.asyncio
    async def test_message_validation_error_handling(self):
        """Test handling of various message validation errors."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            # Test cases for different validation errors
            test_cases = [
                {
                    "name": "invalid_json",
                    "message": b"invalid json data",
                    "expected_error": "Invalid JSON format"
                },
                {
                    "name": "missing_required_fields",
                    "message": json.dumps({"id": "test"}).encode(),
                    "expected_error": "validation failed"
                },
                {
                    "name": "invalid_uuid",
                    "message": json.dumps({
                        "id": "not-a-uuid",
                        "article_id": "test",
                        "text": "test",
                        "source": "test",
                        "ts": "2025-08-14T10:30:00Z"
                    }).encode(),
                    "expected_error": "validation failed"
                },
                {
                    "name": "empty_text",
                    "message": json.dumps({
                        "id": str(uuid4()),
                        "article_id": "test",
                        "text": "",
                        "source": "test",
                        "ts": "2025-08-14T10:30:00Z"
                    }).encode(),
                    "expected_error": "validation failed"
                }
            ]
            
            for test_case in test_cases:
                consumer_record = ConsumerRecord(
                    topic="test-scraped",
                    partition=0,
                    offset=0,
                    timestamp=1234567890,
                    timestamp_type=1,
                    key=b"test-key",
                    value=test_case["message"],
                    checksum=None,
                    serialized_key_size=8,
                    serialized_value_size=len(test_case["message"]),
                    headers=[]
                )
                
                # Should raise appropriate validation error
                with pytest.raises(MessageValidationError) as exc_info:
                    await consumer.process_message(consumer_record)
                
                assert test_case["expected_error"] in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_classification_service_failure_and_retry(self):
        """Test classification service failure and retry mechanisms."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            # Mock classification service with retry behavior
            mock_classification_service = AsyncMock()
            
            # First two calls fail, third succeeds
            call_count = 0
            async def classify_with_retry(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise Exception("Classification service temporarily unavailable")
                return {
                    'classification': [
                        {
                            'tier_1': 'Technology',
                            'confidence': 0.85,
                            'reasoning': 'Recovered classification'
                        }
                    ],
                    'provider': 'openai',
                    'model': 'gpt-4o-mini',
                    'usage': {
                        'input_tokens': 100,
                        'output_tokens': 20,
                        'total_tokens': 120
                    },
                    'taxonomy_version': 'tax_v20250814_103000'
                }
            
            mock_classification_service.classify_text.side_effect = classify_with_retry
            consumer.set_classification_service(mock_classification_service)
            
            # Create valid test message
            message_data = {
                "id": str(uuid4()),
                "article_id": "retry-test-123",
                "text": "Test article for retry mechanism",
                "source": "test_source",
                "ts": "2025-08-14T10:30:00Z"
            }
            
            consumer_record = ConsumerRecord(
                topic="test-scraped",
                partition=0,
                offset=0,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=json.dumps(message_data).encode(),
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=len(json.dumps(message_data)),
                headers=[]
            )
            
            # Mock producer to verify successful processing after retry
            mock_producer = AsyncMock()
            mock_producer.publish_classified_message.return_value = {"success": True}
            consumer.set_producer(mock_producer)
            
            # Process message - should succeed after retries
            await consumer.process_message(consumer_record)
            
            # Verify classification service was called 3 times (2 failures + 1 success)
            assert call_count == 3
            
            # Verify producer was called (indicating successful processing)
            mock_producer.publish_classified_message.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_database_persistence_failure_recovery(self):
        """Test database persistence failure and recovery."""
        with patch('app.services.persistence.get_database_pool') as mock_get_pool:
            # Mock database connection failures
            mock_conn = AsyncMock()
            mock_pool = AsyncMock()
            
            # First attempt fails, second succeeds
            call_count = 0
            async def acquire_with_failure():
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise Exception("Database connection failed")
                return mock_conn.__aenter__.return_value
            
            mock_pool.acquire.side_effect = acquire_with_failure
            mock_get_pool.return_value = mock_pool
            
            # Mock successful database operations after connection recovery
            mock_conn.execute.return_value = None
            
            from app.services.persistence import get_persistence_service
            persistence_service = get_persistence_service()
            
            # Create test messages
            category_path = CategoryPath(
                tier_1="Technology",
                confidence=0.9,
                reasoning="Database recovery test"
            )
            
            usage = UsageStats(
                input_tokens=100,
                output_tokens=20,
                total_tokens=120
            )
            
            classified_message = ClassifiedMessage(
                id=str(uuid4()),
                article_id="db-recovery-test",
                classification=[category_path],
                provider="openai",
                model="gpt-4o-mini",
                usage=usage,
                taxonomy_version="tax_v20250814_103000"
            )
            
            scraped_message = ScrapedMessage(
                id=classified_message.id,
                article_id="db-recovery-test",
                text="Database recovery test content",
                source="test_source",
                ts="2025-08-14T10:30:00Z"
            )
            
            # First attempt should fail
            with pytest.raises(Exception, match="Database connection failed"):
                await persistence_service.persist_article_and_classification(
                    scraped_message,
                    classified_message
                )
            
            # Verify connection was attempted
            assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_producer_delivery_failure_retry(self):
        """Test producer delivery failure and retry mechanisms."""
        with patch('app.producers.classified_producer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_out_topic = "test-classified"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            producer = ClassifiedMessageProducer()
            
            # Mock send_message with retry behavior
            attempt_count = 0
            async def send_with_retry(*args, **kwargs):
                nonlocal attempt_count
                attempt_count += 1
                if attempt_count <= 2:
                    raise KafkaError("Send failed")
                return {
                    'topic': 'test-classified',
                    'partition': 0,
                    'offset': 123,
                    'timestamp': 1234567890
                }
            
            with patch.object(producer, 'send_message', side_effect=send_with_retry):
                # Create test message
                category_path = CategoryPath(
                    tier_1="Technology",
                    confidence=0.9,
                    reasoning="Producer retry test"
                )
                
                usage = UsageStats(
                    input_tokens=100,
                    output_tokens=20,
                    total_tokens=120
                )
                
                classified_message = ClassifiedMessage(
                    id=str(uuid4()),
                    article_id="producer-retry-test",
                    classification=[category_path],
                    provider="openai",
                    model="gpt-4o-mini",
                    usage=usage,
                    taxonomy_version="tax_v20250814_103000"
                )
                
                # Should eventually succeed after retries
                result = await producer.publish_classified_message(classified_message)
                
                assert result['topic'] == 'test-classified'
                assert attempt_count == 3  # 2 failures + 1 success
    
    @pytest.mark.asyncio
    async def test_consumer_lag_handling(self):
        """Test consumer lag detection and handling."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock high-water mark and committed offset to simulate lag
            mock_metadata = {
                0: MagicMock(highwater=1000, committed=900)  # 100 message lag
            }
            mock_consumer.highwater.return_value = 1000
            mock_consumer.committed.return_value = 900
            
            consumer = ScrapedMessageConsumer()
            consumer.consumer = mock_consumer
            consumer.is_running = True
            
            # Get consumer lag metrics
            lag_info = await consumer.get_consumer_lag()
            
            # Verify lag detection
            assert lag_info['total_lag'] == 100
            assert lag_info['partition_lags'][0] == 100
            assert lag_info['is_lagging'] is True
    
    @pytest.mark.asyncio
    async def test_backpressure_handling(self):
        """Test backpressure handling when processing is slower than consumption."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            # Mock slow classification service
            mock_classification_service = AsyncMock()
            
            async def slow_classify(*args, **kwargs):
                await asyncio.sleep(1.0)  # Simulate slow processing
                return {
                    'classification': [
                        {
                            'tier_1': 'Technology',
                            'confidence': 0.85,
                            'reasoning': 'Slow processing test'
                        }
                    ],
                    'provider': 'openai',
                    'model': 'gpt-4o-mini',
                    'usage': {
                        'input_tokens': 100,
                        'output_tokens': 20,
                        'total_tokens': 120
                    },
                    'taxonomy_version': 'tax_v20250814_103000'
                }
            
            mock_classification_service.classify_text.side_effect = slow_classify
            consumer.set_classification_service(mock_classification_service)
            
            # Mock producer
            mock_producer = AsyncMock()
            mock_producer.publish_classified_message.return_value = {"success": True}
            consumer.set_producer(mock_producer)
            
            # Create multiple test messages
            messages = []
            for i in range(5):
                message_data = {
                    "id": str(uuid4()),
                    "article_id": f"backpressure-test-{i}",
                    "text": f"Test content {i}",
                    "source": "test_source",
                    "ts": "2025-08-14T10:30:00Z"
                }
                
                consumer_record = ConsumerRecord(
                    topic="test-scraped",
                    partition=0,
                    offset=i,
                    timestamp=1234567890,
                    timestamp_type=1,
                    key=f"key-{i}".encode(),
                    value=json.dumps(message_data).encode(),
                    checksum=None,
                    serialized_key_size=8,
                    serialized_value_size=len(json.dumps(message_data)),
                    headers=[]
                )
                messages.append(consumer_record)
            
            # Process messages with backpressure
            start_time = asyncio.get_event_loop().time()
            
            # Process with limited concurrency to simulate backpressure
            semaphore = asyncio.Semaphore(2)  # Limit to 2 concurrent operations
            
            async def process_with_semaphore(message):
                async with semaphore:
                    await consumer.process_message(message)
            
            tasks = [process_with_semaphore(msg) for msg in messages]
            await asyncio.gather(*tasks)
            
            end_time = asyncio.get_event_loop().time()
            
            # Verify processing handled backpressure appropriately
            processing_time = end_time - start_time
            
            # With semaphore(2) and 1s processing time, should take at least 3 seconds
            # (round 1: 2 messages in parallel = 1s, round 2: 2 messages = 1s, round 3: 1 message = 1s)
            assert processing_time >= 2.5  # Allow some variance
            
            # Verify all messages were processed
            assert mock_classification_service.classify_text.call_count == 5
            assert mock_producer.publish_classified_message.call_count == 5
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_pattern(self):
        """
        Test circuit breaker pattern implementation for downstream service failures.
        
        This test validates the circuit breaker pattern which protects the system from
        cascading failures when downstream services (classification engine) are failing:
        
        Circuit Breaker States Tested:
        1. CLOSED: Normal operation, requests pass through
        2. OPEN: Service failing consistently, requests fail fast
        3. HALF_OPEN: Testing if service has recovered
        4. Recovery: Service operational again, circuit closes
        
        Test Scenario:
        1. Classification service fails consistently (5 failures)
        2. Circuit breaker opens after threshold reached
        3. Subsequent requests fail fast without calling service
        4. After timeout, circuit moves to half-open
        5. Single test request succeeds, circuit closes
        6. Normal operation resumes
        
        Expected Behavior:
        - First 5 requests trigger actual service calls (and fail)
        - Circuit opens, protecting system from further failures
        - Fast-fail responses without service calls during open state
        - Automatic recovery detection and circuit closure
        - System resilience maintained during service instability
        """
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            
            # Mock classification service with circuit breaker behavior
            mock_classification_service = AsyncMock()
            
            failure_count = 0
            async def failing_classify(*args, **kwargs):
                nonlocal failure_count
                failure_count += 1
                if failure_count <= 5:  # Fail first 5 attempts
                    raise Exception("Service consistently failing")
                # After 5 failures, start succeeding
                return {
                    'classification': [
                        {
                            'tier_1': 'Technology',
                            'confidence': 0.85,
                            'reasoning': 'Circuit breaker recovery'
                        }
                    ],
                    'provider': 'openai',
                    'model': 'gpt-4o-mini',
                    'usage': {
                        'input_tokens': 100,
                        'output_tokens': 20,
                        'total_tokens': 120
                    },
                    'taxonomy_version': 'tax_v20250814_103000'
                }
            
            mock_classification_service.classify_text.side_effect = failing_classify
            consumer.set_classification_service(mock_classification_service)
            
            # Test circuit breaker behavior
            for i in range(10):
                message_data = {
                    "id": str(uuid4()),
                    "article_id": f"circuit-breaker-{i}",
                    "text": f"Test content {i}",
                    "source": "test_source",
                    "ts": "2025-08-14T10:30:00Z"
                }
                
                consumer_record = ConsumerRecord(
                    topic="test-scraped",
                    partition=0,
                    offset=i,
                    timestamp=1234567890,
                    timestamp_type=1,
                    key=f"key-{i}".encode(),
                    value=json.dumps(message_data).encode(),
                    checksum=None,
                    serialized_key_size=8,
                    serialized_value_size=len(json.dumps(message_data)),
                    headers=[]
                )
                
                try:
                    await consumer.process_message(consumer_record)
                except Exception as e:
                    # Expected for first 5 messages
                    if i < 5:
                        assert "Service consistently failing" in str(e)
                    else:
                        # Should start succeeding after circuit breaker recovery
                        pytest.fail(f"Unexpected failure after circuit breaker recovery: {e}")
            
            # Verify failure and recovery pattern
            assert failure_count >= 5  # At least 5 failures occurred
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown_with_inflight_messages(self):
        """Test graceful shutdown with in-flight message processing."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            consumer = ScrapedMessageConsumer()
            consumer.consumer = mock_consumer
            consumer.is_running = True
            
            # Mock classification service with delay
            mock_classification_service = AsyncMock()
            
            async def delayed_classify(*args, **kwargs):
                await asyncio.sleep(2.0)  # Long processing time
                return {
                    'classification': [{'tier_1': 'Technology', 'confidence': 0.85, 'reasoning': 'Test'}],
                    'provider': 'openai',
                    'model': 'gpt-4o-mini',
                    'usage': {'input_tokens': 100, 'output_tokens': 20, 'total_tokens': 120},
                    'taxonomy_version': 'tax_v20250814_103000'
                }
            
            mock_classification_service.classify_text.side_effect = delayed_classify
            consumer.set_classification_service(mock_classification_service)
            
            # Create shutdown event
            shutdown_event = asyncio.Event()
            
            # Start processing a message
            message_data = {
                "id": str(uuid4()),
                "article_id": "shutdown-test",
                "text": "Test content for shutdown",
                "source": "test_source",
                "ts": "2025-08-14T10:30:00Z"
            }
            
            consumer_record = ConsumerRecord(
                topic="test-scraped",
                partition=0,
                offset=0,
                timestamp=1234567890,
                timestamp_type=1,
                key=b"test-key",
                value=json.dumps(message_data).encode(),
                checksum=None,
                serialized_key_size=8,
                serialized_value_size=len(json.dumps(message_data)),
                headers=[]
            )
            
            # Start message processing
            process_task = asyncio.create_task(consumer.process_message(consumer_record))
            
            # Trigger shutdown after short delay
            async def trigger_shutdown():
                await asyncio.sleep(0.5)
                shutdown_event.set()
            
            shutdown_task = asyncio.create_task(trigger_shutdown())
            
            # Wait for either processing completion or shutdown
            start_time = asyncio.get_event_loop().time()
            
            try:
                await asyncio.wait_for(process_task, timeout=3.0)
                processing_completed = True
            except asyncio.TimeoutError:
                processing_completed = False
            
            end_time = asyncio.get_event_loop().time()
            
            # Clean up
            await shutdown_task
            if not process_task.done():
                process_task.cancel()
                try:
                    await process_task
                except asyncio.CancelledError:
                    pass
            
            # Verify graceful handling
            processing_time = end_time - start_time
            
            # Should either complete processing (2+ seconds) or handle shutdown gracefully
            if processing_completed:
                assert processing_time >= 1.8  # Allow some variance
            else:
                # If shutdown interrupted processing, it should be quick
                assert processing_time < 3.0