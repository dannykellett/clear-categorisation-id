import pytest
import os
from app.core.config import AppConfig


class TestAppConfig:
    """Test application configuration"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = AppConfig()
        
        assert config.title == "ClearCategorisationIQ"
        assert config.version == "0.1.0"
        assert config.log_level == "INFO"
        assert config.host == "0.0.0.0"
        assert config.port == 8000
        assert config.default_provider == "mock"
        assert config.default_model == "mock-model"
    
    def test_config_from_env(self, monkeypatch):
        """Test configuration from environment variables"""
        # Set environment variables
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("HOST", "127.0.0.1")
        monkeypatch.setenv("PORT", "9000")
        monkeypatch.setenv("RELOAD", "false")
        monkeypatch.setenv("DEFAULT_PROVIDER", "openai")
        monkeypatch.setenv("DEFAULT_MODEL", "gpt-4")
        
        config = AppConfig.from_env()
        
        assert config.log_level == "DEBUG"
        assert config.host == "127.0.0.1"
        assert config.port == 9000
        assert config.reload is False
        assert config.default_provider == "openai"
        assert config.default_model == "gpt-4"