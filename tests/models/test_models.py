import pytest
from pydantic import ValidationError
from app.models.requests import ClassificationRequest, BatchClassificationRequest
from app.models.responses import CategoryPath, UsageInfo, ClassificationResponse, BatchClassificationResponse


class TestRequestModels:
    """Test request model validation"""
    
    def test_classification_request_valid(self):
        """Test valid classification request"""
        request = ClassificationRequest(
            text="This is a test text",
            provider="openai",
            model="gpt-4"
        )
        
        assert request.text == "This is a test text"
        assert request.provider == "openai"
        assert request.model == "gpt-4"
    
    def test_classification_request_minimal(self):
        """Test minimal valid classification request"""
        request = ClassificationRequest(text="Test")
        
        assert request.text == "Test"
        assert request.provider is None
        assert request.model is None
    
    def test_classification_request_empty_text(self):
        """Test classification request with empty text"""
        with pytest.raises(ValidationError):
            ClassificationRequest(text="")
    
    def test_batch_classification_request_valid(self):
        """Test valid batch classification request"""
        request = BatchClassificationRequest(
            texts=["Text 1", "Text 2", "Text 3"],
            provider="ollama",
            model="llama2"
        )
        
        assert len(request.texts) == 3
        assert request.provider == "ollama"
        assert request.model == "llama2"
    
    def test_batch_classification_request_empty_list(self):
        """Test batch classification request with empty list"""
        with pytest.raises(ValidationError):
            BatchClassificationRequest(texts=[])


class TestResponseModels:
    """Test response model validation"""
    
    def test_category_path_valid(self):
        """Test valid category path"""
        category = CategoryPath(
            path="tech/software/web",
            confidence=0.85,
            reasoning="Contains web development terms"
        )
        
        assert category.path == "tech/software/web"
        assert category.confidence == 0.85
        assert category.reasoning == "Contains web development terms"
    
    def test_category_path_invalid_confidence(self):
        """Test category path with invalid confidence"""
        with pytest.raises(ValidationError):
            CategoryPath(
                path="tech/software/web",
                confidence=1.5,  # Invalid: > 1.0
                reasoning="Test"
            )
        
        with pytest.raises(ValidationError):
            CategoryPath(
                path="tech/software/web",
                confidence=-0.1,  # Invalid: < 0.0
                reasoning="Test"
            )
    
    def test_usage_info_valid(self):
        """Test valid usage info"""
        usage = UsageInfo(
            provider="openai",
            model="gpt-4",
            input_tokens=100,
            output_tokens=50
        )
        
        assert usage.provider == "openai"
        assert usage.model == "gpt-4"
        assert usage.input_tokens == 100
        assert usage.output_tokens == 50
    
    def test_usage_info_optional_tokens(self):
        """Test usage info with optional token fields"""
        usage = UsageInfo(
            provider="mock",
            model="mock-model"
        )
        
        assert usage.provider == "mock"
        assert usage.model == "mock-model"
        assert usage.input_tokens is None
        assert usage.output_tokens is None
    
    def test_classification_response_valid(self):
        """Test valid classification response"""
        category = CategoryPath(
            path="tech/ai",
            confidence=0.9,
            reasoning="AI related content"
        )
        
        usage = UsageInfo(
            provider="openai",
            model="gpt-4",
            input_tokens=50,
            output_tokens=25
        )
        
        response = ClassificationResponse(
            categories=[category],
            usage=usage
        )
        
        assert len(response.categories) == 1
        assert response.categories[0].path == "tech/ai"
        assert response.usage.provider == "openai"
    
    def test_batch_classification_response_valid(self):
        """Test valid batch classification response"""
        category = CategoryPath(
            path="tech/ai",
            confidence=0.9,
            reasoning="AI related content"
        )
        
        usage = UsageInfo(
            provider="openai",
            model="gpt-4",
            input_tokens=50,
            output_tokens=25
        )
        
        individual_response = ClassificationResponse(
            categories=[category],
            usage=usage
        )
        
        total_usage = UsageInfo(
            provider="openai",
            model="gpt-4",
            input_tokens=150,
            output_tokens=75
        )
        
        batch_response = BatchClassificationResponse(
            results=[individual_response, individual_response],
            total_usage=total_usage
        )
        
        assert len(batch_response.results) == 2
        assert batch_response.total_usage.input_tokens == 150