"""Enhanced logging with correlation ID support"""
import logging
import uuid
from contextvars import ContextVar
from typing import Optional, Dict, Any
import json

# Context variable for correlation ID
correlation_id: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "classification-api",
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add correlation ID if available
        if corr_id := correlation_id.get():
            log_data["correlation_id"] = corr_id
            
        # Add extra fields if present
        if hasattr(record, 'extra_data'):
            log_data.update(record.extra_data)
            
        return json.dumps(log_data)


def setup_logging(log_level: str = "INFO") -> None:
    """Configure structured logging"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(message)s',  # StructuredFormatter handles the format
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Apply structured formatter to all handlers
    for handler in logging.root.handlers:
        handler.setFormatter(StructuredFormatter())


def get_logger(name: str) -> logging.Logger:
    """Get logger with structured formatting"""
    return logging.getLogger(name)


def set_correlation_id(corr_id: Optional[str] = None) -> str:
    """Set correlation ID for current context"""
    if corr_id is None:
        corr_id = str(uuid.uuid4())
    correlation_id.set(corr_id)
    return corr_id


def log_with_context(logger: logging.Logger, level: str, message: str, **extra_data: Any) -> None:
    """Log message with additional context data"""
    record = logging.LogRecord(
        name=logger.name,
        level=getattr(logging, level.upper()),
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None
    )
    record.extra_data = extra_data
    logger.handle(record)