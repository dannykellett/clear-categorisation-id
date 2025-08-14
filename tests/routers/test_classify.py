import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


class TestClassifyEndpoints:
    """Test classification endpoints"""
    
    def test_classify_single_text_success(self):
        """Test successful single text classification"""
        payload = {
            "text": "Building a web API with FastAPI"
        }
        
        response = client.post("/classify/", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "categories" in data
        assert "usage" in data
        assert isinstance(data["categories"], list)
        assert len(data["categories"]) > 0
        
        # Verify category structure
        category = data["categories"][0]
        assert "path" in category
        assert "confidence" in category
        assert "reasoning" in category
        assert 0.0 <= category["confidence"] <= 1.0
        
        # Verify usage structure
        usage = data["usage"]
        assert "provider" in usage
        assert "model" in usage
        assert "input_tokens" in usage
        assert "output_tokens" in usage
    
    def test_classify_single_text_with_provider(self):
        """Test single text classification with provider selection"""
        payload = {
            "text": "Data analysis and machine learning",
            "provider": "custom-provider",
            "model": "custom-model"
        }
        
        response = client.post("/classify/", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify provider/model were used
        usage = data["usage"]
        assert usage["provider"] == "custom-provider"
        assert usage["model"] == "custom-model"
    
    def test_classify_batch_success(self):
        """Test successful batch text classification"""
        payload = {
            "texts": [
                "Building web APIs with Python",
                "Data science and analytics",
                "Machine learning models"
            ]
        }
        
        response = client.post("/classify/batch", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "results" in data
        assert "total_usage" in data
        assert isinstance(data["results"], list)
        assert len(data["results"]) == 3
        
        # Verify each result
        for result in data["results"]:
            assert "categories" in result
            assert "usage" in result
            assert isinstance(result["categories"], list)
            assert len(result["categories"]) > 0
        
        # Verify total usage
        total_usage = data["total_usage"]
        assert "provider" in total_usage
        assert "model" in total_usage
        assert "input_tokens" in total_usage
        assert "output_tokens" in total_usage
    
    def test_classify_empty_text_validation(self):
        """Test validation of empty text"""
        payload = {
            "text": ""
        }
        
        response = client.post("/classify/", json=payload)
        
        assert response.status_code == 422  # Validation error
    
    def test_classify_missing_text_validation(self):
        """Test validation of missing text field"""
        payload = {}
        
        response = client.post("/classify/", json=payload)
        
        assert response.status_code == 422  # Validation error
    
    def test_classify_batch_empty_list_validation(self):
        """Test validation of empty text list"""
        payload = {
            "texts": []
        }
        
        response = client.post("/classify/batch", json=payload)
        
        assert response.status_code == 422  # Validation error
    
    def test_classify_batch_missing_texts_validation(self):
        """Test validation of missing texts field"""
        payload = {}
        
        response = client.post("/classify/batch", json=payload)
        
        assert response.status_code == 422  # Validation error