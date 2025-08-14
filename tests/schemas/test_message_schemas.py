"""
Tests for Kafka message schemas and validation.
"""

import json
import pytest
from datetime import datetime
from uuid import uuid4

from pydantic import ValidationError

from app.schemas.scraped_message import ScrapedMessage
from app.schemas.classified_message import ClassifiedMessage, UsageStats, CategoryPath
from app.schemas.validation import (
    MessageValidationError,
    validate_scraped_message,
    validate_classified_message,
    serialize_message,
    deserialize_message
)


class TestScrapedMessage:
    """Tests for ScrapedMessage schema validation."""
    
    def test_valid_scraped_message(self):
        """Test creation of valid scraped message."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "text": "Sample text content for classification",
            "source": "tech_blog",
            "ts": "2025-08-14T10:30:00Z",
            "attrs": {"lang": "en", "author": "John Doe"}
        }
        
        message = ScrapedMessage(**data)
        assert message.id == data["id"]
        assert message.article_id == "art-123"
        assert message.text == "Sample text content for classification"
        assert message.source == "tech_blog"
        assert message.attrs == {"lang": "en", "author": "John Doe"}
    
    def test_scraped_message_with_minimal_data(self):
        """Test scraped message with minimal required fields."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-456",
            "text": "Minimal text",
            "source": "social",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        message = ScrapedMessage(**data)
        assert message.attrs == {}  # Default empty dict
    
    def test_invalid_uuid_format(self):
        """Test validation error for invalid UUID format."""
        data = {
            "id": "invalid-uuid",
            "article_id": "art-123",
            "text": "Sample text",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            ScrapedMessage(**data)
        
        assert "id must be a valid UUID format" in str(exc_info.value)
    
    def test_empty_article_id(self):
        """Test validation error for empty article_id."""
        data = {
            "id": str(uuid4()),
            "article_id": "",
            "text": "Sample text",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            ScrapedMessage(**data)
        
        assert "article_id cannot be empty" in str(exc_info.value)
    
    def test_empty_text(self):
        """Test validation error for empty text."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "text": "",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            ScrapedMessage(**data)
        
        assert "String should have at least 1 character" in str(exc_info.value)
    
    def test_whitespace_trimming(self):
        """Test that whitespace is properly trimmed."""
        data = {
            "id": str(uuid4()),
            "article_id": "  art-123  ",
            "text": "  Sample text  ",
            "source": "  blog  ",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        message = ScrapedMessage(**data)
        assert message.article_id == "art-123"
        assert message.text == "Sample text"
        assert message.source == "blog"


class TestClassifiedMessage:
    """Tests for ClassifiedMessage schema validation."""
    
    def test_valid_classified_message(self):
        """Test creation of valid classified message."""
        category_path = CategoryPath(
            tier_1="Technology",
            tier_2="Hardware",
            confidence=0.92,
            reasoning="Test reasoning"
        )
        
        usage = UsageStats(
            input_tokens=150,
            output_tokens=20,
            total_tokens=170
        )
        
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "classification": [category_path.model_dump()],
            "provider": "openai",
            "model": "gpt-4o-mini",
            "usage": usage.model_dump(),
            "taxonomy_version": "tax_v20250814_103000"
        }
        
        message = ClassifiedMessage(**data)
        assert message.provider == "openai"
        assert message.model == "gpt-4o-mini"
        assert message.taxonomy_version == "tax_v20250814_103000"
        assert len(message.classification) == 1
    
    def test_empty_classification(self):
        """Test classified message with empty classification list."""
        usage = UsageStats(
            input_tokens=100,
            output_tokens=10,
            total_tokens=110
        )
        
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "classification": [],
            "provider": "ollama",
            "model": "qwen3:30b",
            "usage": usage.model_dump(),
            "taxonomy_version": "tax_v20250814_103000"
        }
        
        message = ClassifiedMessage(**data)
        assert message.classification == []
    
    def test_invalid_provider(self):
        """Test validation error for unsupported provider."""
        usage = UsageStats(
            input_tokens=100,
            output_tokens=10,
            total_tokens=110
        )
        
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "classification": [],
            "provider": "invalid_provider",
            "model": "some-model",
            "usage": usage.model_dump(),
            "taxonomy_version": "tax_v20250814_103000"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            ClassifiedMessage(**data)
        
        assert "provider must be one of" in str(exc_info.value)
    
    def test_invalid_taxonomy_version_format(self):
        """Test validation error for invalid taxonomy version format."""
        usage = UsageStats(
            input_tokens=100,
            output_tokens=10,
            total_tokens=110
        )
        
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "classification": [],
            "provider": "openai",
            "model": "gpt-4o-mini",
            "usage": usage.model_dump(),
            "taxonomy_version": "invalid_format"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            ClassifiedMessage(**data)
        
        assert "taxonomy_version must start with 'tax_v'" in str(exc_info.value)


class TestUsageStats:
    """Tests for UsageStats validation."""
    
    def test_valid_usage_stats(self):
        """Test creation of valid usage stats."""
        usage = UsageStats(
            input_tokens=150,
            output_tokens=20,
            total_tokens=170
        )
        
        assert usage.input_tokens == 150
        assert usage.output_tokens == 20
        assert usage.total_tokens == 170
    
    def test_invalid_total_tokens(self):
        """Test validation error when total doesn't match sum."""
        with pytest.raises(ValidationError) as exc_info:
            UsageStats(
                input_tokens=150,
                output_tokens=20,
                total_tokens=200  # Should be 170
            )
        
        assert "total_tokens" in str(exc_info.value)
        assert "must equal input_tokens + output_tokens" in str(exc_info.value)
    
    def test_negative_tokens(self):
        """Test validation error for negative token counts."""
        with pytest.raises(ValidationError):
            UsageStats(
                input_tokens=-10,
                output_tokens=20,
                total_tokens=10
            )


class TestMessageValidation:
    """Tests for message validation functions."""
    
    def test_validate_scraped_message_from_dict(self):
        """Test validating scraped message from dictionary."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "text": "Sample text",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        message = validate_scraped_message(data)
        assert isinstance(message, ScrapedMessage)
        assert message.article_id == "art-123"
    
    def test_validate_scraped_message_from_json_string(self):
        """Test validating scraped message from JSON string."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "text": "Sample text",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        json_string = json.dumps(data)
        
        message = validate_scraped_message(json_string)
        assert isinstance(message, ScrapedMessage)
        assert message.article_id == "art-123"
    
    def test_validate_scraped_message_invalid_json(self):
        """Test validation error for invalid JSON."""
        with pytest.raises(MessageValidationError) as exc_info:
            validate_scraped_message("invalid json")
        
        assert "Invalid JSON format" in str(exc_info.value)
    
    def test_validate_scraped_message_validation_error(self):
        """Test validation error for invalid message data."""
        data = {
            "id": "invalid-uuid",
            "article_id": "",
            "text": "",
            "source": "",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        with pytest.raises(MessageValidationError) as exc_info:
            validate_scraped_message(data)
        
        assert "validation failed" in str(exc_info.value)
        assert len(exc_info.value.validation_errors) > 0
    
    def test_serialize_message(self):
        """Test message serialization to JSON."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "text": "Sample text",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        
        message = ScrapedMessage(**data)
        json_string = serialize_message(message)
        
        # Verify it's valid JSON and can be parsed back
        parsed = json.loads(json_string)
        assert parsed["article_id"] == "art-123"
    
    def test_deserialize_message_scraped(self):
        """Test deserializing scraped message."""
        data = {
            "id": str(uuid4()),
            "article_id": "art-123",
            "text": "Sample text",
            "source": "blog",
            "ts": "2025-08-14T10:30:00Z"
        }
        json_string = json.dumps(data)
        
        message = deserialize_message(json_string, 'scraped')
        assert isinstance(message, ScrapedMessage)
        assert message.article_id == "art-123"
    
    def test_deserialize_message_unknown_type(self):
        """Test error for unknown message type."""
        with pytest.raises(MessageValidationError) as exc_info:
            deserialize_message("{}", "unknown")
        
        assert "Unknown message type" in str(exc_info.value)