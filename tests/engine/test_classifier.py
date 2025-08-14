import pytest
from app.engine.classifier import ClassificationEngine, classification_engine
from app.models.responses import CategoryPath, UsageInfo


class TestClassificationEngine:
    """Test classification engine functionality"""
    
    @pytest.mark.asyncio
    async def test_classify_text_web_content(self):
        """Test classification of web development content"""
        engine = ClassificationEngine()
        
        text = "Building REST APIs with FastAPI and handling HTTP requests"
        categories, usage = await engine.classify_text(text)
        
        # Verify categories
        assert isinstance(categories, list)
        assert len(categories) > 0
        
        category = categories[0]
        assert isinstance(category, CategoryPath)
        assert "technology/software/web-development" in category.path
        assert 0.0 <= category.confidence <= 1.0
        assert len(category.reasoning) > 0
        
        # Verify usage
        assert isinstance(usage, UsageInfo)
        assert usage.provider == "mock"
        assert usage.model == "mock-model"
        assert usage.input_tokens > 0
        assert usage.output_tokens > 0
    
    @pytest.mark.asyncio
    async def test_classify_text_data_content(self):
        """Test classification of data science content"""
        engine = ClassificationEngine()
        
        text = "Performing statistical analysis on large datasets"
        categories, usage = await engine.classify_text(text)
        
        # Verify categories
        assert isinstance(categories, list)
        assert len(categories) > 0
        
        category = categories[0]
        assert "technology/data-science/analysis" in category.path
        assert 0.0 <= category.confidence <= 1.0
    
    @pytest.mark.asyncio
    async def test_classify_text_uncategorized(self):
        """Test classification of uncategorized content"""
        engine = ClassificationEngine()
        
        text = "Random text that doesn't match any patterns"
        categories, usage = await engine.classify_text(text)
        
        # Verify categories
        assert isinstance(categories, list)
        assert len(categories) > 0
        
        category = categories[0]
        assert "general/uncategorized" in category.path
        assert 0.0 <= category.confidence <= 1.0
    
    @pytest.mark.asyncio
    async def test_classify_with_custom_provider(self):
        """Test classification with custom provider and model"""
        engine = ClassificationEngine()
        
        text = "Test text"
        categories, usage = await engine.classify_text(
            text, 
            provider="custom-provider", 
            model="custom-model"
        )
        
        # Verify custom provider/model are used
        assert usage.provider == "custom-provider"
        assert usage.model == "custom-model"
    
    @pytest.mark.asyncio
    async def test_classify_batch(self):
        """Test batch classification"""
        engine = ClassificationEngine()
        
        texts = [
            "Building web APIs",
            "Data analysis",
            "Random content"
        ]
        
        results, total_usage = await engine.classify_batch(texts)
        
        # Verify results
        assert len(results) == 3
        
        for categories, usage in results:
            assert isinstance(categories, list)
            assert len(categories) > 0
            assert isinstance(usage, UsageInfo)
        
        # Verify total usage
        assert isinstance(total_usage, UsageInfo)
        assert total_usage.provider == "mock"
        assert total_usage.model == "mock-model"
        assert total_usage.input_tokens > 0
        assert total_usage.output_tokens > 0
    
    @pytest.mark.asyncio
    async def test_classify_batch_with_provider(self):
        """Test batch classification with custom provider"""
        engine = ClassificationEngine()
        
        texts = ["Text 1", "Text 2"]
        
        results, total_usage = await engine.classify_batch(
            texts,
            provider="batch-provider",
            model="batch-model"
        )
        
        # Verify provider/model in results
        for categories, usage in results:
            assert usage.provider == "batch-provider"
            assert usage.model == "batch-model"
        
        assert total_usage.provider == "batch-provider"
        assert total_usage.model == "batch-model"
    
    def test_global_engine_instance(self):
        """Test that global engine instance exists"""
        assert classification_engine is not None
        assert isinstance(classification_engine, ClassificationEngine)