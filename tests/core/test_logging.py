import pytest
import json
import logging
from app.core.logging import StructuredFormatter, set_correlation_id, get_logger, log_with_context


class TestStructuredLogging:
    """Test structured logging functionality"""
    
    def test_structured_formatter(self):
        """Test structured JSON formatter"""
        formatter = StructuredFormatter()
        
        # Create a log record
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Format the record
        formatted = formatter.format(record)
        
        # Parse as JSON
        log_data = json.loads(formatted)
        
        assert "timestamp" in log_data
        assert log_data["level"] == "INFO"
        assert log_data["service"] == "classification-api"
        assert log_data["logger"] == "test.logger"
        assert log_data["message"] == "Test message"
    
    def test_correlation_id_context(self):
        """Test correlation ID context management"""
        # Set correlation ID
        corr_id = set_correlation_id("test-123")
        assert corr_id == "test-123"
        
        # Test that formatter includes correlation ID
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data["correlation_id"] == "test-123"
    
    def test_log_with_context(self):
        """Test logging with extra context data"""
        logger = get_logger("test.logger")
        
        # Create a test handler to capture log output
        import io
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        # Log with extra context
        log_with_context(
            logger, "info", "Test message",
            user_id="123",
            action="test_action"
        )
        
        # Get the log output
        log_output = log_capture.getvalue()
        log_data = json.loads(log_output.strip())
        
        assert log_data["message"] == "Test message"
        assert log_data["user_id"] == "123"
        assert log_data["action"] == "test_action"