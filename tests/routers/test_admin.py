import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


class TestHealthEndpoints:
    """Test health check endpoints"""
    
    def test_health_check_success(self):
        """Test successful health check"""
        response = client.get("/healthz")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "status" in data
        assert "timestamp" in data
        assert "service" in data
        assert "version" in data
        
        # Verify values
        assert data["status"] == "healthy"
        assert data["service"] == "classification-api"
        assert data["version"] == "0.1.0"
        assert isinstance(data["timestamp"], (int, float))
    
    def test_readiness_check_success(self):
        """Test successful readiness check"""
        response = client.get("/readyz")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "status" in data
        assert "timestamp" in data
        assert "dependencies" in data
        
        # Verify values
        assert data["status"] == "ready"
        assert isinstance(data["timestamp"], (int, float))
        assert isinstance(data["dependencies"], dict)
        
        # Verify dependencies
        deps = data["dependencies"]
        assert "api" in deps
        assert "models" in deps
        assert deps["api"] is True
        assert deps["models"] is True


class TestRootEndpoint:
    """Test root endpoint"""
    
    def test_root_endpoint(self):
        """Test root endpoint returns welcome message"""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert data["message"] == "ClearCategorisationIQ API is running"