from typing import List, Optional
from pydantic import BaseModel, Field


class ClassificationRequest(BaseModel):
    """Single text classification request"""
    text: str = Field(..., description="Text to classify", min_length=1)
    provider: Optional[str] = Field(None, description="AI provider to use (e.g., 'openai', 'ollama')")
    model: Optional[str] = Field(None, description="Model name to use")


class BatchClassificationRequest(BaseModel):
    """Batch text classification request"""
    texts: List[str] = Field(..., description="List of texts to classify", min_length=1)
    provider: Optional[str] = Field(None, description="AI provider to use (e.g., 'openai', 'ollama')")
    model: Optional[str] = Field(None, description="Model name to use")