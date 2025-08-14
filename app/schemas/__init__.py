# Kafka message schemas for ClearCategorisationIQ

from .scraped_message import ScrapedMessage
from .classified_message import ClassifiedMessage, UsageStats, CategoryPath
from .validation import (
    MessageValidationError,
    validate_scraped_message,
    validate_classified_message,
    serialize_message,
    deserialize_message
)

__all__ = [
    'ScrapedMessage',
    'ClassifiedMessage', 
    'CategoryPath',
    'UsageStats',
    'MessageValidationError',
    'validate_scraped_message',
    'validate_classified_message',
    'serialize_message',
    'deserialize_message'
]