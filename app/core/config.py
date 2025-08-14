"""Application configuration management"""
from typing import Optional
from pydantic import BaseModel
import os


class AppConfig(BaseModel):
    """Application configuration settings"""
    
    # Application settings
    title: str = "ClearCategorisationIQ"
    description: str = "Text classification API with hierarchical taxonomy support"
    version: str = "0.1.0"
    
    # Logging settings
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = True
    
    # Classification engine settings
    default_provider: str = "mock"
    default_model: str = "mock-model"
    
    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create config from environment variables"""
        return cls(
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            host=os.getenv("HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "8000")),
            reload=os.getenv("RELOAD", "true").lower() == "true",
            default_provider=os.getenv("DEFAULT_PROVIDER", "mock"),
            default_model=os.getenv("DEFAULT_MODEL", "mock-model"),
        )


# Global config instance
config = AppConfig.from_env()