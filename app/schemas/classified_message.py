"""
Classified message schema for output Kafka topic.

Defines the structure for messages published to the 'scraped-classified' topic
after text classification processing.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, ConfigDict, field_serializer


class CategoryPath(BaseModel):
    """
    Category classification result with hierarchical tiers.
    
    Represents a single classification path with confidence and reasoning.
    Supports up to 4 hierarchical tiers as specified in the PRD.
    """
    
    tier_1: str = Field(..., description="First tier of classification hierarchy")
    tier_2: Optional[str] = Field(None, description="Second tier of classification hierarchy")
    tier_3: Optional[str] = Field(None, description="Third tier of classification hierarchy")
    tier_4: Optional[str] = Field(None, description="Fourth tier of classification hierarchy")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")
    reasoning: str = Field(..., max_length=240, description="Human-readable reasoning (max 240 chars)")
    
    @field_validator('reasoning')
    @classmethod
    def validate_reasoning_length(cls, v: str) -> str:
        """Validate reasoning length doesn't exceed 240 characters."""
        if len(v) > 240:
            raise ValueError("reasoning must be 240 characters or less")
        return v



class UsageStats(BaseModel):
    """Token usage statistics for AI provider calls."""
    
    input_tokens: int = Field(..., ge=0, description="Number of input tokens consumed")
    output_tokens: int = Field(..., ge=0, description="Number of output tokens generated")
    total_tokens: int = Field(..., ge=0, description="Total tokens used")
    
    @field_validator('total_tokens')
    @classmethod
    def validate_total_tokens(cls, v: int, info) -> int:
        """Validate total_tokens equals input + output tokens."""
        data = info.data
        input_tokens = data.get('input_tokens', 0)
        output_tokens = data.get('output_tokens', 0)
        expected_total = input_tokens + output_tokens
        
        if v != expected_total:
            raise ValueError(
                f"total_tokens ({v}) must equal input_tokens + output_tokens ({expected_total})"
            )
        return v


class ClassifiedMessage(BaseModel):
    """
    Schema for messages published to the 'scraped-classified' Kafka topic.
    
    Contains the original message identifiers plus classification results,
    provider information, and usage statistics.
    """
    
    id: str = Field(..., description="Original message ID for joinability")
    article_id: str = Field(..., description="Original article ID for database persistence")
    classification: List[CategoryPath] = Field(
        default_factory=list,
        description="List of classified category paths with confidence scores"
    )
    provider: str = Field(..., description="AI provider used (openai, ollama)")
    model: str = Field(..., description="Specific model used for classification")
    usage: UsageStats = Field(..., description="Token usage statistics")
    taxonomy_version: str = Field(..., description="Taxonomy version used for classification")
    processed_ts: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when classification was processed"
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
    
    @field_validator('provider')
    @classmethod
    def validate_provider(cls, v: str) -> str:
        """Validate provider is supported."""
        supported_providers = {'openai', 'ollama'}
        if v not in supported_providers:
            raise ValueError(f"provider must be one of {supported_providers}, got: {v}")
        return v
    
    @field_validator('model')
    @classmethod
    def validate_model(cls, v: str) -> str:
        """Validate model is not empty."""
        if not v or not v.strip():
            raise ValueError("model cannot be empty")
        return v.strip()
    
    @field_validator('taxonomy_version')
    @classmethod
    def validate_taxonomy_version(cls, v: str) -> str:
        """Validate taxonomy version format."""
        if not v or not v.strip():
            raise ValueError("taxonomy_version cannot be empty")
        
        # Check for expected format: tax_vYYYYMMDD_HHMMSS
        if not v.startswith('tax_v'):
            raise ValueError(f"taxonomy_version must start with 'tax_v', got: {v}")
        
        return v.strip()
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "article_id": "art-123",
                "classification": [
                    {
                        "tier_1": "Technology",
                        "tier_2": "Hardware",
                        "tier_3": "Processors",
                        "tier_4": "Mobile Chips",
                        "confidence": 0.92,
                        "reasoning": "Keywords 'M-series chips' and 'WWDC' indicate Apple processor announcement"
                    }
                ],
                "provider": "openai",
                "model": "gpt-4o-mini",
                "usage": {
                    "input_tokens": 150,
                    "output_tokens": 20,
                    "total_tokens": 170
                },
                "taxonomy_version": "tax_v20250814_103000",
                "processed_ts": "2025-08-14T10:35:00Z"
            }
        }
    )
    
    @field_serializer('processed_ts')
    def serialize_processed_ts(self, value: datetime) -> str:
        """Serialize datetime to ISO format."""
        return value.isoformat()