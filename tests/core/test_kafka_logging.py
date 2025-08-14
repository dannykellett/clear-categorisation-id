"""
Tests for Kafka logging utilities.
"""

import pytest
import time
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from app.core.kafka_logging import KafkaMessageLogger, log_structured_event


class TestKafkaMessageLogger:
    """Tests for KafkaMessageLogger class."""
    
    def test_init(self):
        """Test logger initialization."""
        logger = KafkaMessageLogger("test.logger")
        
        assert logger._processing_start_times == {}
        assert logger.logger.name == "test.logger"
    
    def test_log_message_start(self):
        """Test message start logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            kafka_logger.log_message_start(
                message_id="test-123",
                article_id="article-456",
                source="test-source",
                text_length=1000,
                topic="scraped",
                partition=0,
                offset=12345
            )
            
            # Verify processing time tracking
            assert "test-123" in kafka_logger._processing_start_times
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Starting processing for message test-123" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "message_processing_start"
            assert context["message_id"] == "test-123"
            assert context["article_id"] == "article-456"
            assert context["source"] == "test-source"
            assert context["text_length"] == 1000
            assert context["kafka"]["topic"] == "scraped"
            assert context["kafka"]["partition"] == 0
            assert context["kafka"]["offset"] == 12345
    
    def test_log_message_success(self):
        """Test successful message logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            # Set up processing start time
            start_time = time.time()
            kafka_logger._processing_start_times["test-123"] = start_time
            
            # Wait a small amount to ensure processing time > 0
            time.sleep(0.001)
            
            delivery_info = {
                "topic": "scraped-classified",
                "partition": 1,
                "offset": 67890
            }
            
            kafka_logger.log_message_success(
                message_id="test-123",
                article_id="article-456",
                classification_count=2,
                taxonomy_version="tax_v20250814_123456",
                delivery_info=delivery_info
            )
            
            # Verify processing time was removed
            assert "test-123" not in kafka_logger._processing_start_times
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Successfully processed message test-123" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "message_processing_success"
            assert context["message_id"] == "test-123"
            assert context["article_id"] == "article-456"
            assert context["classification_count"] == 2
            assert context["taxonomy_version"] == "tax_v20250814_123456"
            assert context["processing_time_ms"] > 0
            assert context["delivery"]["topic"] == "scraped-classified"
            assert context["delivery"]["partition"] == 1
            assert context["delivery"]["offset"] == 67890
    
    def test_log_message_failure(self):
        """Test message failure logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            # Set up processing start time
            start_time = time.time()
            kafka_logger._processing_start_times["test-123"] = start_time
            
            # Wait a small amount to ensure processing time > 0
            time.sleep(0.001)
            
            error = ValueError("Test error message")
            kafka_metadata = {
                "topic": "scraped",
                "partition": 0,
                "offset": 12345
            }
            
            kafka_logger.log_message_failure(
                message_id="test-123",
                article_id="article-456",
                error=error,
                stage="classification",
                kafka_metadata=kafka_metadata
            )
            
            # Verify processing time was removed
            assert "test-123" not in kafka_logger._processing_start_times
            
            # Verify logging call
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args
            
            assert "Failed to process message test-123" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "message_processing_failure"
            assert context["message_id"] == "test-123"
            assert context["article_id"] == "article-456"
            assert context["error"]["type"] == "ValueError"
            assert context["error"]["message"] == "Test error message"
            assert context["error"]["stage"] == "classification"
            assert context["processing_time_ms"] > 0
            assert context["kafka"]["topic"] == "scraped"
    
    def test_log_batch_start(self):
        """Test batch start logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            kafka_logger.log_batch_start(
                batch_id="batch-123",
                batch_size=10,
                topic="scraped"
            )
            
            # Verify batch timing tracking
            assert "batch-123" in kafka_logger._processing_start_times
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Starting batch processing: 10 messages" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "batch_processing_start"
            assert context["batch_id"] == "batch-123"
            assert context["batch_size"] == 10
            assert context["topic"] == "scraped"
    
    def test_log_batch_complete(self):
        """Test batch completion logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            # Set up batch start time
            start_time = time.time()
            kafka_logger._processing_start_times["batch-123"] = start_time
            
            # Wait a small amount to ensure processing time > 0
            time.sleep(0.001)
            
            kafka_logger.log_batch_complete(
                batch_id="batch-123",
                successful_count=8,
                failed_count=2
            )
            
            # Verify batch timing was removed
            assert "batch-123" not in kafka_logger._processing_start_times
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Batch processing complete: 8/10 successful" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "batch_processing_complete"
            assert context["batch_id"] == "batch-123"
            assert context["total_messages"] == 10
            assert context["successful_count"] == 8
            assert context["failed_count"] == 2
            assert context["success_rate_percent"] == 80.0
            assert context["processing_time_ms"] > 0
            assert context["avg_time_per_message_ms"] > 0
    
    def test_log_consumer_event(self):
        """Test consumer event logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            details = {"partitions_assigned": [0, 1, 2]}
            
            kafka_logger.log_consumer_event(
                event_type="rebalance",
                consumer_group="test-group",
                topic="scraped",
                details=details
            )
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Consumer rebalance: test-group on topic scraped" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "consumer_rebalance"
            assert context["consumer_group"] == "test-group"
            assert context["topic"] == "scraped"
            assert context["details"]["partitions_assigned"] == [0, 1, 2]
    
    def test_log_producer_event(self):
        """Test producer event logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            details = {"messages_sent": 100, "bytes_sent": 50000}
            
            kafka_logger.log_producer_event(
                event_type="flush",
                topic="scraped-classified",
                details=details
            )
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Producer flush: topic scraped-classified" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "producer_flush"
            assert context["topic"] == "scraped-classified"
            assert context["details"]["messages_sent"] == 100
            assert context["details"]["bytes_sent"] == 50000
    
    def test_log_dlq_candidate(self):
        """Test DLQ candidate logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            kafka_metadata = {
                "topic": "scraped",
                "partition": 0,
                "offset": 12345,
                "timestamp": 1692000000000
            }
            
            kafka_logger.log_dlq_candidate(
                message_id="test-123",
                article_id="article-456",
                error_type="ValidationError",
                kafka_metadata=kafka_metadata
            )
            
            # Verify logging call
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            
            assert "DLQ candidate: ValidationError for message test-123" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "dlq_candidate"
            assert context["message_id"] == "test-123"
            assert context["article_id"] == "article-456"
            assert context["error_type"] == "ValidationError"
            assert context["kafka"]["topic"] == "scraped"
    
    def test_log_performance_metrics(self):
        """Test performance metrics logging."""
        with patch('app.core.kafka_logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            kafka_logger = KafkaMessageLogger("test.logger")
            
            metrics = {
                "messages_processed": 1000,
                "avg_processing_time_ms": 45.5,
                "error_rate": 0.01
            }
            
            kafka_logger.log_performance_metrics(
                component="consumer",
                metrics=metrics
            )
            
            # Verify logging call
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args
            
            assert "Performance metrics for consumer" in call_args[0][0]
            assert "kafka_context" in call_args[1]["extra"]
            
            context = call_args[1]["extra"]["kafka_context"]
            assert context["event"] == "performance_metrics"
            assert context["component"] == "consumer"
            assert context["metrics"]["messages_processed"] == 1000
            assert context["metrics"]["avg_processing_time_ms"] == 45.5
            assert context["metrics"]["error_rate"] == 0.01


def test_log_structured_event():
    """Test structured event logging helper."""
    mock_logger = MagicMock()
    
    context = {
        "component": "test",
        "action": "test_action",
        "status": "success"
    }
    
    log_structured_event(
        logger=mock_logger,
        level="info",
        event_type="test_event",
        message="Test message",
        context=context
    )
    
    # Verify logging call
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args
    
    assert call_args[0][0] == "Test message"
    assert "structured_context" in call_args[1]["extra"]
    
    structured_context = call_args[1]["extra"]["structured_context"]
    assert structured_context["event_type"] == "test_event"
    assert structured_context["component"] == "test"
    assert structured_context["action"] == "test_action"
    assert structured_context["status"] == "success"
    assert "timestamp" in structured_context


def test_log_structured_event_invalid_level():
    """Test structured event logging with invalid level."""
    mock_logger = MagicMock()
    
    # Mock that invalid level falls back to info
    mock_logger.invalid_level = None
    
    context = {"test": "value"}
    
    log_structured_event(
        logger=mock_logger,
        level="invalid_level",
        event_type="test_event",
        message="Test message",
        context=context
    )
    
    # Should fall back to info level
    mock_logger.info.assert_called_once()


def test_module_level_loggers():
    """Test module-level logger instances."""
    from app.core.kafka_logging import consumer_logger, producer_logger, processor_logger
    
    assert isinstance(consumer_logger, KafkaMessageLogger)
    assert isinstance(producer_logger, KafkaMessageLogger)
    assert isinstance(processor_logger, KafkaMessageLogger)
    
    # Verify logger names
    assert consumer_logger.logger.name == "kafka.consumer"
    assert producer_logger.logger.name == "kafka.producer"
    assert processor_logger.logger.name == "kafka.processor"