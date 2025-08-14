"""
Message validation utilities for Kafka schemas.

Provides validation functions and error handling for message
serialization and deserialization.
"""

import json
import logging
from typing import Any, Dict, Optional, Union

from pydantic import ValidationError

from app.schemas.scraped_message import ScrapedMessage
from app.schemas.classified_message import ClassifiedMessage

logger = logging.getLogger(__name__)


class MessageValidationError(Exception):
    """Raised when message validation fails."""
    
    def __init__(self, message: str, validation_errors: Optional[list] = None):
        super().__init__(message)
        self.validation_errors = validation_errors or []


def validate_scraped_message(data: Union[str, bytes, Dict[str, Any]]) -> ScrapedMessage:
    """
    Validate and parse a scraped message from Kafka.
    
    Args:
        data: Raw message data (JSON string, bytes, or dict)
        
    Returns:
        Validated ScrapedMessage instance
        
    Raises:
        MessageValidationError: If validation fails
    """
    try:
        # Handle different input types
        if isinstance(data, (str, bytes)):
            parsed_data = json.loads(data)
        elif isinstance(data, dict):
            parsed_data = data
        else:
            raise MessageValidationError(f"Unsupported data type: {type(data)}")
        
        # Validate against schema
        return ScrapedMessage(**parsed_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise MessageValidationError(f"Invalid JSON format: {e}")
    
    except ValidationError as e:
        logger.error(f"Scraped message validation error: {e}")
        validation_errors = [
            f"{'.'.join(error['loc'])}: {error['msg']}"
            for error in e.errors()
        ]
        raise MessageValidationError(
            f"Scraped message validation failed: {validation_errors}",
            validation_errors
        )
    
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        raise MessageValidationError(f"Unexpected validation error: {e}")


def validate_classified_message(data: Union[str, bytes, Dict[str, Any]]) -> ClassifiedMessage:
    """
    Validate and parse a classified message for Kafka publishing.
    
    Args:
        data: Message data (JSON string, bytes, or dict)
        
    Returns:
        Validated ClassifiedMessage instance
        
    Raises:
        MessageValidationError: If validation fails
    """
    try:
        # Handle different input types
        if isinstance(data, (str, bytes)):
            parsed_data = json.loads(data)
        elif isinstance(data, dict):
            parsed_data = data
        else:
            raise MessageValidationError(f"Unsupported data type: {type(data)}")
        
        # Validate against schema
        return ClassifiedMessage(**parsed_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise MessageValidationError(f"Invalid JSON format: {e}")
    
    except ValidationError as e:
        logger.error(f"Classified message validation error: {e}")
        validation_errors = [
            f"{'.'.join(error['loc'])}: {error['msg']}"
            for error in e.errors()
        ]
        raise MessageValidationError(
            f"Classified message validation failed: {validation_errors}",
            validation_errors
        )
    
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        raise MessageValidationError(f"Unexpected validation error: {e}")


def serialize_message(message: Union[ScrapedMessage, ClassifiedMessage]) -> str:
    """
    Serialize a message to JSON string for Kafka publishing.
    
    Args:
        message: Message instance to serialize
        
    Returns:
        JSON string representation
        
    Raises:
        MessageValidationError: If serialization fails
    """
    try:
        return message.model_dump_json()
    except Exception as e:
        logger.error(f"Message serialization error: {e}")
        raise MessageValidationError(f"Failed to serialize message: {e}")


def deserialize_message(
    data: Union[str, bytes], 
    message_type: str
) -> Union[ScrapedMessage, ClassifiedMessage]:
    """
    Deserialize and validate a message from Kafka.
    
    Args:
        data: Raw message data
        message_type: Type of message ('scraped' or 'classified')
        
    Returns:
        Validated message instance
        
    Raises:
        MessageValidationError: If deserialization/validation fails
    """
    if message_type == 'scraped':
        return validate_scraped_message(data)
    elif message_type == 'classified':
        return validate_classified_message(data)
    else:
        raise MessageValidationError(f"Unknown message type: {message_type}")