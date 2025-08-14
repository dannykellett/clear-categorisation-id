from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class CategoryPath(BaseModel):
    """Single category path with confidence and reasoning"""
    path: str = Field(..., description="Hierarchical category path (e.g., 'tier1/tier2/tier3')")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")
    reasoning: str = Field(..., description="Human-readable reasoning for this classification")


class UsageInfo(BaseModel):
    """Usage tracking information"""
    provider: str = Field(..., description="AI provider used")
    model: str = Field(..., description="Model name used")
    input_tokens: Optional[int] = Field(None, description="Number of input tokens used")
    output_tokens: Optional[int] = Field(None, description="Number of output tokens generated")


class ClassificationResponse(BaseModel):
    """Single text classification response"""
    categories: List[CategoryPath] = Field(..., description="List of applicable category paths")
    usage: UsageInfo = Field(..., description="Usage tracking information")


class BatchClassificationResponse(BaseModel):
    """Batch text classification response"""
    results: List[ClassificationResponse] = Field(..., description="Classification results for each input text")
    total_usage: UsageInfo = Field(..., description="Aggregated usage information")