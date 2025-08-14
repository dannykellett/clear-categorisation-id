"""
Scraped message schema for input Kafka topic.

Defines the structure and validation for messages consumed from the 'scraped' topic.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, ConfigDict, field_serializer


class ScrapedMessage(BaseModel):
    """
    Schema for messages consumed from the 'scraped' Kafka topic.
    
    Represents unstructured text content to be classified, with metadata
    for tracking and processing.
    """
    
    id: str = Field(..., description="Unique message identifier (UUID format)")
    article_id: str = Field(..., description="Article identifier for database persistence")
    text: str = Field(..., min_length=1, description="Text content to be classified")
    source: str = Field(..., description="Source of the content (e.g., blog, social, press_release)")
    ts: datetime = Field(..., description="Timestamp when content was scraped (ISO-8601)")
    attrs: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional attributes (e.g., language, author)"
    )
    
    @field_validator('id')
    @classmethod
    def validate_id_format(cls, v: str) -> str:
        """Validate that id is a valid UUID string format."""
        try:
            UUID(v)
            return v
        except ValueError:
            raise ValueError(f"id must be a valid UUID format, got: {v}")
    
    @field_validator('article_id')
    @classmethod
    def validate_article_id(cls, v: str) -> str:
        """Validate article_id is not empty."""
        if not v or not v.strip():
            raise ValueError("article_id cannot be empty")
        return v.strip()
    
    @field_validator('text')
    @classmethod
    def validate_text_content(cls, v: str) -> str:
        """Validate text content is meaningful."""
        if not v or not v.strip():
            raise ValueError("text content cannot be empty")
        return v.strip()
    
    @field_validator('source')
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Validate source is not empty."""
        if not v or not v.strip():
            raise ValueError("source cannot be empty")
        return v.strip()
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "article_id": "art-123",
                "text": "Apple unveils new M-series chips at WWDC...",
                "source": "tech_blog",
                "ts": "2025-08-14T10:30:00Z",
                "attrs": {
                    "lang": "en",
                    "author": "John Doe",
                    "url": "https://example.com/article"
                }
            }
        }
    )
    
    @field_serializer('ts')
    def serialize_ts(self, value: datetime) -> str:
        """Serialize datetime to ISO format."""
        return value.isoformat()