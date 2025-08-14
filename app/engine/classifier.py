from typing import List, Optional
import time
from app.core.config import config
from app.core.logging import get_logger, log_with_context
from app.models.responses import CategoryPath, UsageInfo

logger = get_logger(__name__)


class ClassificationEngine:
    """Main classification orchestrator"""
    
    def __init__(self):
        self.default_provider = config.default_provider
        self.default_model = config.default_model
    
    async def classify_text(
        self, 
        text: str, 
        provider: Optional[str] = None, 
        model: Optional[str] = None
    ) -> tuple[List[CategoryPath], UsageInfo]:
        """
        Classify a single text and return categories with usage info
        """
        start_time = time.time()
        
        # Use provided or default provider/model
        actual_provider = provider or self.default_provider
        actual_model = model or self.default_model
        
        log_with_context(
            logger, "info", "Starting text classification",
            provider=actual_provider,
            model=actual_model,
            text_length=len(text)
        )
        
        # Mock classification logic - will be replaced with real implementation
        categories = await self._mock_classify(text)
        
        # Calculate token usage (mock implementation)
        input_tokens = len(text.split())
        output_tokens = sum(len(cat.reasoning.split()) for cat in categories)
        
        usage = UsageInfo(
            provider=actual_provider,
            model=actual_model,
            input_tokens=input_tokens,
            output_tokens=output_tokens
        )
        
        processing_time = time.time() - start_time
        log_with_context(
            logger, "info", "Text classification completed",
            provider=actual_provider,
            model=actual_model,
            processing_time_ms=round(processing_time * 1000, 2),
            categories_count=len(categories),
            input_tokens=input_tokens,
            output_tokens=output_tokens
        )
        
        return categories, usage
    
    async def classify_batch(
        self,
        texts: List[str],
        provider: Optional[str] = None,
        model: Optional[str] = None
    ) -> tuple[List[tuple[List[CategoryPath], UsageInfo]], UsageInfo]:
        """
        Classify multiple texts and return individual results plus total usage
        """
        results = []
        total_input_tokens = 0
        total_output_tokens = 0
        
        actual_provider = provider or self.default_provider
        actual_model = model or self.default_model
        
        for text in texts:
            categories, usage = await self.classify_text(text, provider, model)
            results.append((categories, usage))
            total_input_tokens += usage.input_tokens or 0
            total_output_tokens += usage.output_tokens or 0
        
        total_usage = UsageInfo(
            provider=actual_provider,
            model=actual_model,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens
        )
        
        return results, total_usage
    
    async def _mock_classify(self, text: str) -> List[CategoryPath]:
        """Mock classification implementation"""
        # Simple keyword-based mock classification
        text_lower = text.lower()
        
        if any(word in text_lower for word in ['api', 'web', 'server', 'http']):
            return [
                CategoryPath(
                    path="technology/software/web-development",
                    confidence=0.85,
                    reasoning="Text contains web development related terms"
                )
            ]
        elif any(word in text_lower for word in ['data', 'analysis', 'statistics']):
            return [
                CategoryPath(
                    path="technology/data-science/analysis",
                    confidence=0.78,
                    reasoning="Text contains data analysis related terms"
                )
            ]
        else:
            return [
                CategoryPath(
                    path="general/uncategorized",
                    confidence=0.60,
                    reasoning="Text does not match specific category patterns"
                )
            ]


# Global instance
classification_engine = ClassificationEngine()