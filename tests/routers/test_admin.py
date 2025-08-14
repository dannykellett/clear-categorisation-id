import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient
from app.main import app
from app.models.taxonomy import TaxonomyVersion, ValidationResult
from datetime import datetime

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


class TestTaxonomyEndpoints:
    """Test taxonomy management endpoints"""
    
    def test_get_taxonomy_version_with_data(self):
        """Test taxonomy version endpoint with active data"""
        mock_version = TaxonomyVersion(
            version_string="tax_v20250814_120000",
            created_at=datetime(2025, 8, 14, 12, 0, 0)
        )
        
        with patch('app.routers.admin.taxonomy_service') as mock_service:
            mock_service.get_current_version.return_value = mock_version
            
            response = client.get("/taxonomy/version")
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["version"] == "tax_v20250814_120000"
            assert data["status"] == "active"
            assert data["created_at"] == "2025-08-14T12:00:00"
            assert data["metadata"]["service"] == "classification-api"
    
    def test_get_taxonomy_version_no_data(self):
        """Test taxonomy version endpoint with no data"""
        with patch('app.routers.admin.taxonomy_service') as mock_service:
            mock_service.get_current_version.return_value = None
            
            response = client.get("/taxonomy/version")
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["version"] is None
            assert data["status"] == "no_data"
            assert data["created_at"] is None
            assert "No taxonomy data available" in data["metadata"]["message"]
    
    def test_get_taxonomy_preview(self):
        """Test taxonomy preview endpoint"""
        mock_preview = {
            "version": "tax_v20250814_120000",
            "status": "active",
            "structure": {
                "Technology": {
                    "Software": {
                        "Web Development": ["APIs", "Frameworks"]
                    }
                }
            },
            "metadata": {
                "total_paths": 2,
                "created_at": "2025-08-14T12:00:00"
            }
        }
        
        with patch('app.routers.admin.taxonomy_service') as mock_service:
            mock_service.get_taxonomy_preview.return_value = mock_preview
            
            response = client.get("/taxonomy/preview")
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["version"] == "tax_v20250814_120000"
            assert data["status"] == "active"
            assert "Technology" in data["structure"]
            assert data["metadata"]["total_paths"] == 2
    
    def test_trigger_taxonomy_sync_success(self):
        """Test manual taxonomy sync trigger - success case"""
        mock_version = TaxonomyVersion(
            version_string="tax_v20250814_120000",
            created_at=datetime(2025, 8, 14, 12, 0, 0)
        )
        
        mock_validation_result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[]
        )
        
        async def mock_sync_taxonomy(force=False):
            return True, mock_validation_result
        
        with patch('app.routers.admin.taxonomy_service') as mock_service:
            mock_service.sync_taxonomy = mock_sync_taxonomy
            mock_service.get_current_version.return_value = mock_version
            
            response = client.post("/taxonomy/sync")
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["success"] is True
            assert data["version"] == "tax_v20250814_120000"
            assert data["validation_result"]["is_valid"] is True
            assert "completed successfully" in data["message"]
    
    def test_trigger_taxonomy_sync_failure(self):
        """Test manual taxonomy sync trigger - failure case"""
        mock_validation_result = ValidationResult(
            is_valid=False,
            errors=["Test validation error", "Another error"],
            warnings=[]
        )
        
        async def mock_sync_taxonomy(force=False):
            return False, mock_validation_result
        
        with patch('app.routers.admin.taxonomy_service') as mock_service:
            mock_service.sync_taxonomy = mock_sync_taxonomy
            mock_service.get_current_version.return_value = None
            
            response = client.post("/taxonomy/sync")
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["success"] is False
            assert data["version"] is None
            assert data["validation_result"]["is_valid"] is False
            assert len(data["validation_result"]["errors"]) == 2
            assert "sync failed" in data["message"]
            assert "2 errors found" in data["message"]